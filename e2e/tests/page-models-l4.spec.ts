import { test, expect } from "@playwright/test";
import { execFileSync } from "node:child_process";
import { createReadyOrg, personalUser, addMember } from "../lib/isolation";
import { waitForComputed, refreshPath } from "../lib/readModels";
import { CONTAINERS, CONTROL_DB, REMOTE } from "../stack/config.mjs";

async function waitForIndex(api: { json: (method: "GET" | "POST", path: string) => Promise<any> }) {
  await api.json("POST", "/v1/load/oceans/refresh");
  let index: any;
  await expect.poll(async () => {
    index = await api.json("GET", "/v1/load/oceans");
    expect(index).not.toHaveProperty("computed_at");
    return index.components.length > 0 && index.components.every((c: any) => c.stored.computed_at !== null && !c.stored.stale);
  }, { timeout: 120_000, intervals: [250, 500, 1000] }).toBe(true);
  return index;
}

function sql(db: string, query: string): string {
  if (REMOTE) throw new Error("L4 fixtures require the owned local stack");
  return execFileSync("docker", ["exec", "-i", CONTAINERS.pg, "psql", "-XAt", "-v", "ON_ERROR_STOP=1", "-U", "postgres", "-d", db, "-c", query], { encoding: "utf8", timeout: 15_000 }).trim().split("\n")[0];
}

// Real Clerk JWTs, serving roles, retained ingest and actual refresh workers.
// These API contracts do not stand in for the L5 browser conversion.
for (const plane of ["personal", "org"] as const) {
  test(`${plane}: L4 sections fill through workers and preserve interactive pagination`, async ({ request }) => {
    test.setTimeout(240_000);
    const org = plane === "org" ? await createReadyOrg(request, "l4-models") : undefined;
    const personal = org ? undefined : await personalUser(request, "l4-models");
    const api = org?.adminInOrg ?? personal!.api;
    const graph = (await api.graphs()).find(g => g.kind === "member" || g.kind === "personal")!;
    expect(graph).toBeTruthy();
    const db = org ? sql(CONTROL_DB, `SELECT 'coralswarm_tenant_'||replace(slug,'-','_') FROM tenant_provisioning_jobs WHERE clerk_org_id='${org.orgId}'`) : CONTROL_DB;
    const sourceId = `l4-retained-${crypto.randomUUID()}`;
    // R2-backed notes have no inline plaintext title witness. Retain the
    // named source participant through actual ingest, rather than asserting a
    // raw node label with no admitted support.
    const ingest = await api.json("POST", "/v1/ingest/atom", {
      graph_id: graph.id, source_type: "note", source_id: sourceId,
      content: "L4Fixture retains a source-backed page contract.",
      participants: ["L4Fixture"], timestamp: new Date().toISOString(),
    });
    expect(ingest.atom_ids.length).toBeGreaterThan(0);
    const captured = { sourceId, atomIds: ingest.atom_ids as string[] };
    const account = sql(db, `SELECT account_id FROM graphs WHERE id='${graph.id}'`);
    const actor = sql(CONTROL_DB, `SELECT owner_user_id FROM accounts WHERE id='${account}'`);
    // Adversarial owned fixture writes still run the real invalidation triggers.
    const node = sql(db, `INSERT INTO nodes(graph_id,type,title,body) VALUES('${graph.id}','entity','L4Fixture','{}') RETURNING node_id`);
    sql(db, `INSERT INTO node_atoms(node_id,atom_id) VALUES('${node}','${captured.atomIds[0]}')`);
    const conversation = sql(db, `INSERT INTO conversations(account_id,graph_id,user_id,library_scope) VALUES('${account}','${graph.id}','${actor}','{"graph_ids":["${graph.id}"]}') RETURNING id`);
    sql(db, `INSERT INTO conversation_messages(conversation_id,seq,role,text) VALUES('${conversation}',1,'user','L4 retained question')`);
    const items = await api.json("GET", `/v1/sources/items?graph_id=${graph.id}&atom_id=${captured.atomIds[0]}`);
    expect(items.items.length).toBeGreaterThan(0);
    const source = items.items.find((s: any) => s.source_id === captured.sourceId);
    expect(source).toBeTruthy();
    const models = [
      ["conversation_detail", `/v1/load/conversations/${conversation}`],
      ["chat_empty", "/v1/load/chats/empty?limit=5&since_days=30"],
      ["oceans_index", "/v1/load/oceans"],
      ["reef_page", `/v1/load/reefs?graph=${graph.id}&page_size=20`],
      ["atlas", `/v1/graphs/${graph.id}/atlas`],
      ["topic_page", `/v1/load/topics/${node}`],
      ["coral_detail", `/v1/load/corals/${captured.atomIds[0]}`],
      ["capture_detail", `/v1/load/captures?graph=${graph.id}&source=${source.source_item_id}`],
      ["usage_summary", "/v1/usage/summary"],
      ...(org ? [["members_admin", "/v1/load/org/members"]] : []),
    ];
    for (const [name, path] of models) {
      const response = await api.call("GET", path);
      expect(response.status(), await response.text()).toBe(200);
      expect(response.headers()["server-timing"]).toContain('class;desc="page_read"');
      const miss = await response.json();
      if (name === "oceans_index") {
        expect(miss).not.toHaveProperty("computed_at");
        expect(miss.components.length).toBeGreaterThan(0);
        for (const c of miss.components) { expect(c.stored.model).toBe(name); expect(c.stored.computed_at).toBeNull(); }
      } else { expect(miss.model).toBe(name); expect(miss.computed_at).toBeNull(); }
    }
    const payloads = new Map<string, any>();
    for (const [name, path] of models) payloads.set(name, name === "oceans_index" ? await waitForIndex(api) : (await waitForComputed(api, path)).payload);
    expect(payloads.get("conversation_detail").resolved_scope.graph_ids).toEqual([graph.id]);
    expect(payloads.get("conversation_detail").messages[0].text).toBe("L4 retained question");
    expect(payloads.get("reef_page").nodes.some((n: any) => n.node_id === node && n.title === "L4Fixture")).toBe(true);
    expect(payloads.get("atlas").nodes.some((n: any) => n.id === node && n.label === "L4Fixture")).toBe(true);
    expect(payloads.get("topic_page").node.title).toBe("L4Fixture");
    expect(payloads.get("coral_detail").atom.content).toContain("L4Fixture");
    expect(payloads.get("capture_detail").evidence.source_item_id).toBe(source.source_item_id);
    const interactive = await api.call("GET", `/v1/nodes?graph_id=${graph.id}&page=2&page_size=1`);
    expect(interactive.status()).toBe(200); expect(Array.isArray(await interactive.json())).toBe(true);
    expect(interactive.headers()["server-timing"]).toContain('class;desc="other"');
    for (const [name, path] of models) {
      expect((await api.call("POST", refreshPath(path))).status()).toBe(202);
      if (name === "oceans_index") await waitForIndex(api);
      else expect((await waitForComputed(api, path, false)).model).toBe(name);
    }
    // Real Clerk + real worker historical/focused deep link. This version is
    // outside the twenty newest summaries, and focus is past the first100 atoms.
    const historical = sql(db, `WITH made AS (INSERT INTO source_versions(graph_id,source_item_id,source_type,provider_namespace,source_id,raw_inline,extraction_version,extraction_status,captured_at) SELECT graph_id,source_item_id,source_type,provider_namespace,source_id,'Historical deep link fixture','fixture-v1','done',now()-n*interval '1 hour' FROM source_items CROSS JOIN generate_series(1,25) n WHERE source_item_id='${source.source_item_id}' RETURNING version_id,captured_at) SELECT version_id FROM made ORDER BY captured_at LIMIT 1`);
    sql(db, `INSERT INTO semantic_atoms(graph_id,source_version_id,ordinal,kind,statement,extraction_version,model) SELECT '${graph.id}','${historical}',n,'fact','Historical focused fact '||n,'fixture-v1','fixture' FROM generate_series(0,259) n`);
    const focus = sql(db, `SELECT semantic_atom_id FROM semantic_atoms WHERE source_version_id='${historical}' AND ordinal=150`);
    const selectedPath = `/v1/load/captures?graph=${graph.id}&source=${source.source_item_id}&version_id=${historical}&atom_id=${focus}`;
    const selectedMiss = await api.json("GET", selectedPath);
    expect(selectedMiss.computed_at).toBeNull();
    const selected = (await waitForComputed(api, selectedPath)).payload.evidence;
    expect(selected.selected_version.version_id).toBe(historical);
    expect(selected.selected_version.atoms[0].atom_id).toBe(focus);
    expect(selected.selected_version.atoms).toHaveLength(100);
    expect(selected.versions.some((v: any) => v.version_id === historical)).toBe(true);
    const later = await api.call("GET", `/v1/sources/evidence?graph_id=${graph.id}&source_item_id=${source.source_item_id}&version_id=${historical}&atoms_after=${selected.selected_version.atoms_next_cursor}`);
    expect(later.status()).toBe(200);
    expect(later.headers()["server-timing"]).toContain('class;desc="other"');
    expect((await later.json()).selected_version.atoms).toHaveLength(10);
    for (const [query, status] of [[`version_id=${crypto.randomUUID()}`,404],[`version_id=${source.current_version_id}&atom_id=${focus}`,400]] as const) {
      const bad = `/v1/load/captures?graph=${graph.id}&source=${source.source_item_id}&${query}`;
      expect((await api.call("GET",bad)).status()).toBe(status);
      expect((await api.call("POST",refreshPath(bad))).status()).toBe(status);
    }
    const counts = await api.json("GET", "/v1/e2e/read-model-computes");
    expect(counts.worker_computes).toBeGreaterThan(0); expect(counts.request_computes).toBe(0);
    if (!org) {
      expect((await api.call("GET", "/v1/load/org/members")).status()).toBe(403);
      expect((await api.call("POST", "/v1/load/org/members/refresh")).status()).toBe(403);
    }
  });
}


test("org: whole-org private metering and warmed admin-role revocation use live authority", async ({ request }) => {
  test.setTimeout(240_000);
  const org = await createReadyOrg(request, "l4-usage-auth");
  const member = await addMember(request, org, "l4-usage-member");
  const privateGraph = (await member.inOrg.graphs()).find(g => g.kind === "member")!;
  expect(privateGraph).toBeTruthy();
  expect((await org.adminInOrg.graphs()).some(g => g.id === privateGraph.id)).toBe(false);
  const db = sql(CONTROL_DB, `SELECT 'coralswarm_tenant_'||replace(slug,'-','_') FROM tenant_provisioning_jobs WHERE clerk_org_id='${org.orgId}'`);
  sql(db, `INSERT INTO llm_usage(account_id,graph_id,task,model,input_tokens,output_tokens) VALUES(NULL,'${privateGraph.id}','synthesize','claude-sonnet-5',1000000,1000000)`);
  const usage = (await waitForComputed(org.adminInOrg, "/v1/usage/summary", true,
    p => p.by_workspace.some((w: any) => w.graph_id === privateGraph.id))).payload;
  expect(usage.by_workspace.find((w: any) => w.graph_id === privateGraph.id).cost_usd).toBeGreaterThan(0);
  const members = await waitForComputed(org.adminInOrg, "/v1/load/org/members");
  expect(members.payload.members.length).toBe(2);
  for (const path of ["/v1/usage/summary", "/v1/load/org/members"]) {
    expect((await member.inOrg.call("GET", path)).status()).toBe(403);
    expect((await member.inOrg.call("POST", refreshPath(path))).status()).toBe(403);
  }
  // Retain the real Clerk admin session/claim while changing the authoritative
  // control role. A cached privileged payload must not become a role proof.
  sql(CONTROL_DB, `UPDATE account_members SET role='member' WHERE account_id=(SELECT id FROM accounts WHERE clerk_org_id='${org.orgId}') AND user_id=(SELECT id FROM users WHERE clerk_user_id='${org.admin.user.id}')`);
  for (const path of ["/v1/usage/summary", "/v1/load/org/members"]) {
    expect((await org.adminInOrg.call("GET", path)).status()).toBe(403);
    expect((await org.adminInOrg.call("POST", refreshPath(path))).status()).toBe(403);
  }
});

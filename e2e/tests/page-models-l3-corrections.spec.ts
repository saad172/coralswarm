import { test, expect } from "@playwright/test";
import { execFileSync, spawn } from "node:child_process";
import { createReadyOrg, personalUser, ingestNote, orgApi } from "../lib/isolation";
import { clerkApi } from "../lib/clerk";
import { waitForComputed } from "../lib/readModels";
import { CONTAINERS, CONTROL_DB } from "../stack/config.mjs";

// Adversarial database mutations have no public API. Responses below always
// traverse the actual authenticated HTTP handlers under nonowner application RLS.
function sql(db: string, query: string): string {
  return execFileSync("docker", ["exec", "-i", CONTAINERS.pg, "psql", "-XAt", "-v", "ON_ERROR_STOP=1", "-U", "postgres", "-d", db, "-c", query], { encoding: "utf8", timeout: 15_000 }).trim().split("\n")[0];
}
async function workerBarrier(db: string) {
  const child = spawn("docker", ["exec", "-i", CONTAINERS.pg, "psql", "-XAt", "-v", "ON_ERROR_STOP=1", "-U", "postgres", "-d", db], { stdio: ["pipe", "pipe", "pipe"] });
  await new Promise<void>((resolve, reject) => {
    child.stdout.on("data", data => { if (String(data).includes("locked")) resolve(); });
    child.once("error", reject);
    child.stdin.write("BEGIN; SET LOCAL lock_timeout='3s'; LOCK TABLE nodes IN ACCESS EXCLUSIVE MODE; SELECT 'locked';\n");
  });
  return async () => { const done = new Promise<void>(resolve => child.once("exit", () => resolve())); child.stdin.end("ROLLBACK;\n"); await done; };
}
test("personal Clerk org retains manageable add-on metadata in stored ConnectorDetail", async ({ request }) => {
  test.setTimeout(120_000);
  const personal = await personalUser(request, "l3-personal-addon");
  const graph = (await personal.api.graphs()).find(g => g.kind === "member" || g.kind === "personal")!;
  const account = sql(CONTROL_DB, `SELECT account_id FROM graphs WHERE id='${graph.id}'`);
  const actor = sql(CONTROL_DB, `SELECT owner_user_id FROM accounts WHERE id='${account}'`);
  const tokenHash = `fixture-personal-addon-${crypto.randomUUID()}`;
  const token = sql(CONTROL_DB, `INSERT INTO addin_tokens(account_id,owner_user_id,token_hash,name) VALUES('${account}','${actor}','${tokenHash}','Personal fixture token') RETURNING id`);
  const path = "/v1/connectors/google";
  const legacy = await waitForComputed(personal.api, path, true);
  expect(legacy.payload.addin_tokens.tokens.map((t: any) => t.id)).toEqual([token]);
  const organization = await clerkApi.organizations.createOrganization({ name: "E2E personal add-on", createdBy: personal.user.id });
  sql(CONTROL_DB, `BEGIN; SET CONSTRAINTS account_members_account_org_fk DEFERRED; SELECT set_config('app.account_id','${account}',true); SELECT billing_enter_account('${account}'); INSERT INTO personal_orgs(clerk_org_id,clerk_user_id) VALUES('${organization.id}','${personal.user.id}'); UPDATE accounts SET clerk_org_id='${organization.id}' WHERE id='${account}'; UPDATE account_members SET account_is_org_account=true,seat_tier_id=(SELECT id FROM seat_tiers WHERE code='standard' AND is_active=true) WHERE account_id='${account}'; COMMIT`);
  const adoptedApi = await orgApi(request, personal.user.id, organization.id);
  expect((await adoptedApi.json("GET", "/v1/me")).is_personal_org).toBe(true);
  const adopted = await waitForComputed(adoptedApi, path, true);
  expect(adopted.payload.addin_tokens.tokens.map((t: any) => t.id)).toEqual([token]);
  expect(Object.keys(adopted.payload.addin_tokens.tokens[0]).sort()).toEqual(["created_at", "id", "last_used_at", "name"]);
  expect(JSON.stringify(adopted)).not.toContain(tokenHash);
  const revoked = await adoptedApi.call("DELETE", `/v1/addin/tokens/${token}`);
  expect(revoked.status()).toBe(204);
  const after = await waitForComputed(adoptedApi, path, true, p => p.addin_tokens.tokens.length === 0);
  expect(after.payload.addin_tokens.tokens).toEqual([]);
});
for (const plane of ["personal", "org"] as const) {
  test(`${plane}: Teams scope reduction revokes authoritative captures with absent and stale page rows`, async ({ request }) => {
    test.setTimeout(180_000);
    const org = plane === "org" ? await createReadyOrg(request, "l3-fix-meet") : undefined;
    const personal = org ? undefined : await personalUser(request, "l3-fix-meet");
    const api = org?.adminInOrg ?? personal!.api;
    const graph = (await api.graphs()).find(g => g.kind === "member" || g.kind === "personal")!;
    const db = org ? sql(CONTROL_DB, `SELECT 'coralswarm_tenant_'||replace(slug,'-','_') FROM tenant_provisioning_jobs WHERE clerk_org_id='${org.orgId}'`) : CONTROL_DB;
    const account = sql(db, `SELECT account_id FROM graphs WHERE id='${graph.id}'`);
    const actor = sql(CONTROL_DB, `SELECT owner_user_id FROM accounts WHERE id='${account}'`);
    const tid = "22222222-1111-1111-1111-111111111111", aad = "33333333-1111-1111-1111-111111111111";
    sql(CONTROL_DB, `INSERT INTO oauth_tokens(account_id,provider,scope,owner_user_id,access_token,provider_workspace_id,provider_user_id) VALUES('${account}','microsoft','OnlineMeetingTranscript.Read.All',${org ? `'${actor}'` : "NULL"},'\\x00'::bytea,'${tid}','${aad}')`);
    const path = "/v1/connectors/microsoft/teams/transcripts/captures";
    const insert = (meeting: string) => sql(db, `INSERT INTO teams_meeting_captures(account_id,user_id,aad_id,tenant_id,meeting_id,occurrence_start,occurrence_end,graph_id,source,next_poll_at) VALUES('${account}','${actor}','${aad}','${tid}','${meeting}',now()-interval '1 hour',now()+interval '1 hour','${graph.id}','explicit_meeting',now()+interval '1 day') RETURNING id`);
    for (const state of ["absent", "stale"]) {
      const capture = insert(`correction-${state}`);
      if (state === "stale") {
        const warm = await waitForComputed(api, path, true, p => p.captures.some((c: any) => c.capture_id === capture));
        expect(warm.payload.captures).toHaveLength(1);
      } else sql(db, `DELETE FROM read_models WHERE account_id='${account}' AND model='teams_transcript_captures'`);
      if (state === "stale") {
        sql(db, `UPDATE teams_meeting_captures SET last_checked_at=now() WHERE id='${capture}'`);
        // A held in-flight claim is a deterministic worker barrier; it does
        // not lock any authoritative table needed by the mutation itself.
        sql(db, `UPDATE read_models SET refresh_claimed_at=now(),claim_token=gen_random_uuid() WHERE account_id='${account}' AND model='teams_transcript_captures'`);
        // A reducing content write serves the warm answer stale, never as current.
        const behind = await api.json("GET", path);
        expect(behind.computed_at).not.toBeNull(); expect(behind.stale).toBe(true);
      }
      const revision = () => sql(CONTROL_DB, `SELECT COALESCE((SELECT revision FROM read_model_control_revisions WHERE account_id='${account}'),0)`);
      const keyed = revision();
      const response = await api.call("PUT", "/v1/connectors/microsoft/sources/teams_meetings/scope", { mode: "pick", item_ids: [] });
      expect(response.status(), await response.text()).toBe(200);
      expect(sql(db, `SELECT revoked_at IS NOT NULL FROM teams_meeting_captures WHERE id='${capture}'`)).toBe("t");
      // The revocation is a reducing content write on `teams_meeting_captures`.
      // The PUT re-keys the model (a control-revision bump) only when its
      // `connector_sources` upsert changes authority: the absent pass inserts
      // the row, the stale pass rewrites the same mode, which
      // `read_model_control_fact_changed` treats as a no-op. So the absent pass
      // reads a never-computed key, and the stale pass reads the warm answer
      // served stale, never as current, until the recompute drops the capture.
      const after = await api.json("GET", path);
      if (state === "stale") {
        expect(revision(), "a same-mode scope PUT must not re-key the model").toBe(keyed);
        expect(after.computed_at).not.toBeNull(); expect(after.stale).toBe(true);
        expect(after.payload.captures.map((c: any) => c.capture_id)).toEqual([capture]);
      } else expect(after.computed_at).toBeNull();
      expect(sql(db, `SELECT count(*) FROM teams_meeting_captures WHERE account_id='${account}' AND user_id='${actor}' AND revoked_at IS NULL`)).toBe("0");
      sql(db, `UPDATE read_models SET refresh_claimed_at=NULL,claim_token=NULL WHERE account_id='${account}' AND model='teams_transcript_captures'`);
      const settled = await waitForComputed(api, path, true, p => p.captures.every((c: any) => c.capture_id !== capture));
      expect(settled.payload.captures).toEqual([]);
    }
  });

  test(`${plane}: actual ConnectorDetail and WorkspaceActivity omit obsolete semantic titles and mark unlinked titles stale until recompute`, async ({ request }) => {
    test.setTimeout(180_000);
    const org = plane === "org" ? await createReadyOrg(request, "l3-fix-title") : undefined;
    const personal = org ? undefined : await personalUser(request, "l3-fix-title");
    const api = org?.adminInOrg ?? personal!.api;
    const graphs = await api.graphs();
    const own = graphs.find(g => g.kind === "member" || g.kind === "personal")!;
    const shared = org ? org.oceanId : sql(CONTROL_DB, `INSERT INTO graphs(account_id,name,kind,owner_user_id) SELECT account_id,'Correction shared fixture','org',NULL FROM graphs WHERE id='${own.id}' RETURNING id`);
    const db = org ? sql(CONTROL_DB, `SELECT 'coralswarm_tenant_'||replace(slug,'-','_') FROM tenant_provisioning_jobs WHERE clerk_org_id='${org.orgId}'`) : CONTROL_DB;
    const account = sql(db, `SELECT account_id FROM graphs WHERE id='${own.id}'`);
    const actor = sql(CONTROL_DB, `SELECT owner_user_id FROM accounts WHERE id='${account}'`);
    const node = sql(db, `INSERT INTO nodes(graph_id,type,title) VALUES('${shared}','entity','OBSOLETE_HTTP_SECRET') RETURNING node_id`);
    const atom = sql(db, `INSERT INTO atoms(graph_id,source_type,source_id,content_blob_key,content_inline,timestamp,ingested_by) VALUES('${shared}','slack','fixture-unrelated','fixture','unrelated admitted content',now(),'${actor}') RETURNING atom_id`);
    sql(db, `INSERT INTO node_atoms(node_id,atom_id) VALUES('${node}','${atom}')`);
    const item = sql(db, `INSERT INTO source_items(graph_id,source_type,provider_namespace,source_id) VALUES('${shared}','note','fixture','obsolete-http') RETURNING source_item_id`);
    const version = sql(db, `INSERT INTO source_versions(graph_id,source_item_id,source_type,provider_namespace,source_id,extraction_version,extraction_status) VALUES('${shared}','${item}','note','fixture','obsolete-http','current','done') RETURNING version_id`);
    sql(db, `UPDATE source_items SET current_version_id='${version}' WHERE source_item_id='${item}'`);
    const semantic = sql(db, `INSERT INTO semantic_atoms(graph_id,source_version_id,ordinal,kind,statement,extraction_version,model,entities) VALUES('${shared}','${version}',0,'fact','unrelated','obsolete','fixture','[{"mention":"OBSOLETE_HTTP_SECRET"}]') RETURNING semantic_atom_id`);
    sql(db, `INSERT INTO node_semantic_atoms(graph_id,node_id,semantic_atom_id) VALUES('${shared}','${node}','${semantic}')`);
    const detail = "/v1/connectors/slack", workspace = "/v1/home/workspace-activity";
    for (const path of [detail, workspace]) expect.soft(JSON.stringify((await waitForComputed(api, path, true)).payload)).not.toContain("OBSOLETE_HTTP_SECRET");
    sql(db, `UPDATE semantic_atoms SET extraction_version='current' WHERE semantic_atom_id='${semantic}'`);
    for (const path of [detail, workspace]) expect.soft(JSON.stringify((await waitForComputed(api, path, true, p => JSON.stringify(p).includes("OBSOLETE_HTTP_SECRET"))).payload)).toContain("OBSOLETE_HTTP_SECRET");
    sql(db, `DELETE FROM node_semantic_atoms WHERE node_id='${node}' AND semantic_atom_id='${semantic}'`);
    const unlock = await workerBarrier(db);
    // The unlink is a reducing write: the last answer (still naming the title)
    // is served stale, never as current, until the recompute drops the title.
    try { for (const path of [detail, workspace]) { const behind = await api.json("GET", path); expect(behind.computed_at).not.toBeNull(); expect(behind.stale).toBe(true); } }
    finally { await unlock(); }
    for (const path of [detail, workspace]) expect(JSON.stringify((await waitForComputed(api, path, true, p => !JSON.stringify(p).includes("OBSOLETE_HTTP_SECRET"))).payload)).not.toContain("OBSOLETE_HTTP_SECRET");
  });
  test(`${plane}: sustained ordinary control telemetry keeps useful Today answers and workers converge`, async ({ request }) => {
    test.setTimeout(180_000);
    const org = plane === "org" ? await createReadyOrg(request, "l3-fix-churn") : undefined;
    const personal = org ? undefined : await personalUser(request, "l3-fix-churn");
    const api = org?.adminInOrg ?? personal!.api;
    const graph = (await api.graphs()).find(g => g.kind === "member" || g.kind === "personal")!;
    const db = org ? sql(CONTROL_DB, `SELECT 'coralswarm_tenant_'||replace(slug,'-','_') FROM tenant_provisioning_jobs WHERE clerk_org_id='${org.orgId}'`) : CONTROL_DB;
    const account = sql(db, `SELECT account_id FROM graphs WHERE id='${graph.id}'`);
    const actor = sql(CONTROL_DB, `SELECT owner_user_id FROM accounts WHERE id='${account}'`);
    const note = await ingestNote(api, graph.id, "Useful current evidence retained through routine MCP reads.");
    await api.waitForExtraction(graph.id, note.sourceId, 60_000);
    sql(CONTROL_DB, `INSERT INTO oauth_tokens(account_id,provider,scope,owner_user_id,access_token) VALUES('${account}','google_gmail','gmail.readonly','${actor}','\\x00'::bytea)`);
    const token = sql(CONTROL_DB, `INSERT INTO addin_tokens(account_id,owner_user_id,token_hash) VALUES('${account}','${actor}',gen_random_uuid()::text) RETURNING id`);
    const range = `from=${encodeURIComponent(new Date(Date.now()-86400000).toISOString())}&to=${encodeURIComponent(new Date(Date.now()+3600000).toISOString())}&who=mine&tz=UTC`;
    const paths = [`/v1/journal/activities?${range}`, "/v1/connectors/google"];
    const initial = [];
    for (const path of paths) initial.push(await waitForComputed(api, path, true));
    expect(initial[0].payload.activities.length).toBeGreaterThan(0);
    const advanced = paths.map(() => false);
    const started = Date.now(); let writes = 0;
    // Longer than MAX_DELAY: no quiet period is available to the worker.
    while (Date.now() - started < 65_000) {
      sql(CONTROL_DB, `BEGIN; INSERT INTO mcp_tool_calls(account_id,user_id,tool,scope_used,outcome) VALUES('${account}','${actor}','ocean_list','read','ok'); UPDATE addin_tokens SET last_used_at=now() WHERE id='${token}'; UPDATE oauth_tokens SET access_token=decode(md5(random()::text),'hex'),expires_at=now()+interval '1 hour',updated_at=now() WHERE account_id='${account}'; COMMIT`);
      writes++;
      for (const [i,path] of paths.entries()) {
        const stored = await api.json("GET", path);
        expect(stored.computed_at, `pending under routine telemetry: ${path}`).not.toBeNull();
        if (stored.computed_at === initial[i].computed_at) expect(stored.payload).toEqual(initial[i].payload);
        else advanced[i] = true;
        if (i === 0) expect(stored.payload.activities.length).toBeGreaterThan(0);
      }
      await new Promise(resolve => setTimeout(resolve, 400));
    }
    expect(advanced).toEqual([true,true]);
    expect(writes).toBeGreaterThan(20);
    expect(sql(db, `SELECT count(*) FROM read_models WHERE account_id='${account}' AND model IN ('journal_activities','connector_detail')`)).toBe("2");
    const unlock = await workerBarrier(db);
    try {
      sql(CONTROL_DB, `UPDATE oauth_tokens SET credential_invalid_at=now(),credential_invalid_reason='revoked' WHERE account_id='${account}'`);
      for (const path of paths) expect((await api.json("GET", path)).computed_at).toBeNull();
    } finally { await unlock(); }
    console.log(JSON.stringify({scenario:"control_telemetry_convergence",plane,duration_ms:Date.now()-started,writes,advanced}));
  });

}

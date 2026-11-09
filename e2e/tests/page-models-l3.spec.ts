import { test, expect } from "@playwright/test";
import { execFileSync, spawn } from "node:child_process";
import { createReadyOrg, personalUser, ingestNote } from "../lib/isolation";
import { waitForComputed } from "../lib/readModels";
import { API_BASE, CONTAINERS, CONTROL_DB } from "../stack/config.mjs";

function sql(db: string, query: string): string {
  return execFileSync("docker", ["exec", "-i", CONTAINERS.pg, "psql", "-XAt", "-v", "ON_ERROR_STOP=1", "-U", "postgres", "-d", db, "-c", query], { encoding: "utf8", timeout: 15_000 }).trim();
}

test("personal: control grant commits withhold former facts and isolate the other viewer", async ({ request }) => {
  test.setTimeout(180_000);
  const { api } = await personalUser(request, "l3-grant");
  await api.graphs();
  const path = "/v1/connectors/slack";
  expect((await waitForComputed(api, path)).payload.status).toBe("not_connected");
  const { state } = await api.json("GET", "/v1/oauth/state");
  const unlock = await lock(CONTROL_DB, "atoms");
  try {
    const callback = await request.get(`${API_BASE}/v1/oauth/slack/callback?state=${encodeURIComponent(state)}&code=team-l3${Date.now().toString(36)}`, { maxRedirects: 0 });
    expect(callback.status()).toBe(307);
    expect(callback.headers().location).toContain("connected=slack");
    expect((await api.json("GET", path)).computed_at).toBeNull();
  } finally { await unlock(); }
  expect((await waitForComputed(api, path)).payload.status).not.toBe("not_connected");
  const other = await personalUser(request, "l3-other-grant");
  await other.api.graphs();
  expect((await waitForComputed(other.api, path)).payload.status).toBe("not_connected");
  const stop = await lock(CONTROL_DB, "atoms");
  try {
    expect((await api.call("DELETE", path)).ok()).toBeTruthy();
    const withheld = await api.json("GET", path);
    expect(withheld.computed_at).toBeNull();
    expect(withheld.payload.status).toBe("not_connected");
  } finally { await stop(); }
  expect((await waitForComputed(api, path)).payload.status).toBe("not_connected");
});
async function lock(db: string, table: "atoms" | "nodes") {
  const process = spawn("docker", ["exec", "-i", CONTAINERS.pg, "psql", "-XAt", "-v", "ON_ERROR_STOP=1", "-U", "postgres", "-d", db], { stdio: ["pipe", "pipe", "pipe"] });
  await new Promise<void>((resolve, reject) => {
    process.stdout.on("data", data => { if (String(data).includes("locked")) resolve(); });
    process.once("error", reject);
    process.stdin.write(`BEGIN; SET LOCAL lock_timeout='3s'; LOCK TABLE ${table} IN ACCESS EXCLUSIVE MODE; SELECT 'locked';\n`);
  });
  return async () => {
    const exit = new Promise<void>(resolve => process.once("exit", () => resolve()));
    process.stdin.end("ROLLBACK;\n"); await exit;
  };
}

for (const plane of ["personal", "org"] as const) {
  test(`${plane}: eleven L3 models are stored-only and refresh through real workers`, async ({ request }) => {
    test.setTimeout(240_000);
    const org = plane === "org" ? await createReadyOrg(request, "l3-models") : undefined;
    const personal = org ? undefined : await personalUser(request, "l3-models");
    const api = org?.adminInOrg ?? personal!.api;
    const graph = (await api.graphs()).find(g => g.kind === "member" || g.kind === "personal")!;
    const db = org ? sql(CONTROL_DB, `SELECT 'coralswarm_tenant_'||replace(slug,'-','_') FROM tenant_provisioning_jobs WHERE clerk_org_id='${org.orgId}'`) : CONTROL_DB;
    const now = new Date();
    const range = `from=${encodeURIComponent(new Date(now.getTime() - 86_400_000).toISOString())}&to=${encodeURIComponent(new Date(now.getTime() + 60_000).toISOString())}`;
    const models = [
      ["home_metrics", "/v1/home/metrics?days=7&tz=UTC"],
      ["workspace_activity", "/v1/home/workspace-activity?limit=5&since_days=30"],
      ["journal_activities", `/v1/journal/activities?${range}&who=mine&tz=UTC`],
      ["journal_strip", "/v1/journal/strip?tz=UTC"],
      ["journal_gist", "/v1/journal/gist?tz=UTC"],
      ["journal_rollups", `/v1/journal?${range}`],
      ["connectors_list", "/v1/connectors"],
      ["connector_detail", "/v1/connectors/google"],
      ["connector_status", "/v1/connectors/status"],
      ["teams_transcript_captures", "/v1/connectors/microsoft/teams/transcripts/captures"],
      ["conversation_list", "/v1/conversations"],
    ];
    await ingestNote(api, graph.id, "Synthetic L3 fixture evidence for stored Home and Today sections.");
    const before = await api.json("GET", "/v1/e2e/read-model-computes");
    const unlock = await lock(db, "atoms");
    try {
      for (const [name, path] of models) {
        const start = Date.now(); const response = await api.call("GET", path);
        expect(response.status(), await response.text()).toBe(200);
        expect(Date.now() - start).toBeLessThan(1500);
        expect(response.headers()["server-timing"]).toContain('class;desc="page_read"');
        const body = await response.json();
        expect(body.model).toBe(name); expect(body.computed_at).toBeNull(); expect(body.stale).toBe(true);
      }
    } finally { await unlock(); }
    const results = new Map<string, any>();
    for (const [name, path] of models) {
      const ready = name === "home_metrics" ? (p: any) => p.kpis.corals_ingested.total > 0
        : name === "journal_activities" ? (p: any) => p.activities.length > 0 : () => true;
      results.set(name, await waitForComputed(api, path, true, ready));
    }
    const requestCosts: Record<string, { p50_ms: number; p95_ms: number; samples: number }> = {};
    for (const [name, path] of models) {
      const times: number[] = [];
      for (let n = 0; n < 20; n++) {
        const response = await api.call("GET", path);
        expect(response.status()).toBe(200);
        const timing = response.headers()["server-timing"] ?? "";
        const total = timing.match(/total;dur=([0-9.]+)/);
        expect(total, timing).toBeTruthy();
        times.push(Number(total![1]));
      }
      times.sort((a, b) => a - b);
      requestCosts[name] = { p50_ms: times[9], p95_ms: times[18], samples: times.length };
    }
    expect(results.get("home_metrics").payload.kpis.corals_ingested.total).toBeGreaterThan(0);
    expect(results.get("journal_activities").payload.activities.length).toBeGreaterThan(0);
    expect(results.get("connector_detail").payload).toHaveProperty("captures.items");
    expect(results.get("connector_detail").payload).toHaveProperty("watched.threads");
    expect(results.get("connector_detail").payload).toHaveProperty("addin_tokens.tokens");
    expect(results.get("conversation_list").payload).toEqual({ conversations: [], shared: [] });
    const account = sql(db, `SELECT account_id FROM graphs WHERE id='${graph.id}'`);
    const actor = sql(CONTROL_DB, `SELECT owner_user_id FROM accounts WHERE id='${account}'`);
    const conversation = sql(db, `INSERT INTO conversations(account_id,graph_id,user_id,title) VALUES('${account}','${graph.id}','${actor}','Unproven generated fixture title') RETURNING id`).split("\n")[0];
    sql(db, `INSERT INTO conversation_messages(conversation_id,seq,role,text) VALUES('${conversation}',1,'user','Permission-filtered fixture question')`);
    const chats = (await waitForComputed(api, "/v1/conversations", true,
      p => p.conversations.some((c: any) => c.id === conversation))).payload;
    const chat = chats.conversations.find((c: any) => c.id === conversation);
    expect(chat.title).toBe("Permission-filtered fixture question");
    expect(chat.mine).toBe(true); expect(chat.oceans).toEqual([{ ocean_id: graph.id, ocean_name: graph.name }]);
    const first = await waitForComputed(api, models[0][1]);
    // Real additive fixture mutation through all statement triggers. Ingest
    // also binds evidence, whose authority change conservatively withholds.
    sql(db, `INSERT INTO nodes(graph_id,type,title,body) VALUES('${graph.id}','entity','Additive fixture node','{}')`);
    const stop = await lock(db, "nodes");
    try {
      const stale = await api.json("GET", models[0][1]);
      expect(stale.computed_at).toBe(first.computed_at); expect(stale.stale).toBe(true);
    } finally { await stop(); }
    sql(db, `UPDATE source_items SET access_state='restricted' WHERE graph_id='${graph.id}'`);
    const stopReduced = await lock(db, "nodes");
    try {
      // A reducing write serves the last answer stale, never as current; the
      // recompute below drops the restricted evidence. Pre-reduction content
      // is asserted as a positive count rather than equality with `first`:
      // background admission can republish between the two reads.
      const behind = await api.json("GET", models[0][1]);
      expect(behind.computed_at).not.toBeNull(); expect(behind.stale).toBe(true);
      expect(behind.payload.kpis.corals_ingested.total).toBeGreaterThan(0);
    } finally { await stopReduced(); }
    const reduced = await waitForComputed(api, models[0][1]);
    expect(reduced.payload.kpis.corals_ingested.total).toBe(0);
    const after = await api.json("GET", "/v1/e2e/read-model-computes");
    expect(after.worker_computes).toBeGreaterThan(before.worker_computes); expect(after.request_computes).toBe(0);
    expect((await api.call("GET", "/v1/home/metrics?compute_as=forged")).status()).toBe(400);
    expect((await api.call("GET", "/v1/connectors/google?provider=microsoft")).status()).toBe(400);
    console.log(JSON.stringify({ scenario: "l3_models", plane, models: models.map(([name]) => name), computes: after, stored_request_costs: requestCosts }));
  });
}

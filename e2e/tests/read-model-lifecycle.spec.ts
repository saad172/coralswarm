import { test, expect } from "@playwright/test";
import { execFileSync, spawn } from "node:child_process";
import { createReadyOrg, personalUser, ingestNote } from "../lib/isolation";
import { CONTAINERS, CONTROL_DB, PORTS, FAKE } from "../stack/config.mjs";
function sql(db: string, query: string): string {
  return execFileSync("docker", ["exec", "-i", CONTAINERS.pg, "psql", "-XAt", "-v", "ON_ERROR_STOP=1", "-U", "postgres", "-d", db, "-c", query], { encoding: "utf8" }).trim();
}
async function lockAtoms(db: string, table = "atoms") {
  const child = spawn("docker", ["exec", "-i", CONTAINERS.pg, "psql", "-XAt", "-v", "ON_ERROR_STOP=1", "-U", "postgres", "-d", db], { stdio: ["pipe", "pipe", "pipe"] });
  await new Promise<void>((resolve, reject) => {
    child.stdout.on("data", (data) => { if (String(data).includes("locked")) resolve(); });
    child.once("error", reject);
    child.stdin.write(`BEGIN; SET LOCAL lock_timeout='3s'; LOCK TABLE ${table} IN ACCESS EXCLUSIVE MODE; SELECT 'locked';\n`);
  });
  return async () => { const done = new Promise<void>((r) => child.once("exit", () => r())); child.stdin.end("ROLLBACK;\n"); await done; };
}
for (const plane of ["personal", "org"] as const) {
  test(`${plane} stored workspace counts compute only on workers, refresh and serve revocations stale until recompute`, async ({ request }) => {
    test.setTimeout(150_000);
    const org = plane === "org" ? await createReadyOrg(request, "read-model") : undefined;
    const personal = org ? undefined : await personalUser(request, "read-model-personal");
    const api = org?.adminInOrg ?? personal!.api;
    const graph = (await api.graphs()).find((g) => g.kind === "member" || g.kind === "personal")!;
    expect(graph).toBeTruthy();
    await ingestNote(api, graph.id, "The synthetic read model rollout has one captured note.");
    const db = org ? sql(CONTROL_DB, `SELECT 'coralswarm_tenant_' || replace(slug, '-', '_') FROM tenant_provisioning_jobs WHERE clerk_org_id='${org.orgId}'`) : CONTROL_DB;
    const path = `/v1/graphs/${graph.id}/stats`;
    const before = await api.json("GET", "/v1/e2e/read-model-computes");
    // If the real GET computes, it blocks behind atoms. The stored read must
    // answer while its independent worker is unable to read any content.
    const unlock = await lockAtoms(db);
    try {
      const start = Date.now();
      const initial = await api.call("GET", path);
      expect(initial.status(), await initial.text()).toBe(200);
      expect(Date.now() - start).toBeLessThan(1500);
      expect(initial.headers()["server-timing"]).toContain('class;desc="page_read"');
      const missing = await initial.json();
      expect(missing.computed_at).toBeNull(); expect(missing.stale).toBe(true);
      expect(missing.refresh_eta).not.toBeNull(); expect(missing.payload.atom_count).toBe(0);
    } finally { await unlock(); }
    // Capture acknowledgement precedes asynchronous evidence admission. A valid
    // first stored snapshot may still contain zero; wait for the actual admitted
    // stored answer, keeping the same worker completion deadline.
    await expect.poll(async () => {
      const answer = await api.json("GET", path);
      return answer.computed_at !== null && answer.payload.atom_count > 0;
    }, { timeout: 60_000, intervals: [500, 1000] }).toBe(true);
    const first = await api.json("GET", path);
    expect(first.model).toBe("workspace_counts"); expect(first.version).toBe(1);
    expect(first.payload.atom_count).toBeGreaterThan(0);
    const timeouts = async () => {
      const response = await request.get(`http://localhost:${PORTS.internal}/internal/admin/page-read-health`, { headers: { "X-Internal-Token": FAKE.internalToken } });
      expect(response.ok()).toBeTruthy();
      const health = await response.json();
      return health.per_route.find((r: { route: string }) => r.route === "/v1/graphs/{graph_id}/stats")?.statement_timeouts ?? 0;
    };
    const priorTimeouts = await timeouts();
    const releaseStored = await lockAtoms(db, "read_models");
    try {
      const start = Date.now(); const response = await api.call("GET", path);
      expect(response.status()).toBe(503); expect(Date.now() - start).toBeLessThan(1800);
      await expect.poll(timeouts).toBe(priorTimeouts + 1);
    } finally { await releaseStored(); }
    const refresh = await api.call("POST", `${path}/refresh`);
    expect(refresh.status()).toBe(202); expect(refresh.headers()["server-timing"]).toContain('class;desc="other"');
    await expect.poll(async () => (await api.json("GET", path)).computed_at, { timeout: 60_000, intervals: [500, 1000] }).not.toBe(first.computed_at);
    // Hold the worker at the content boundary while the permission-reducing
    // write commits: an immediate stored read serves the last answer marked
    // stale (never as current), and the recompute replaces it. The write goes
    // first because its dirty trigger reads `nodes`. The assertion is on the
    // content, not on a computed_at captured earlier: background admission can
    // republish at any moment, and only a post-reduction compute answers 0.
    sql(db, `UPDATE source_items SET access_state='restricted' WHERE graph_id='${graph.id}'`);
    const stop = await lockAtoms(db, "nodes");
    try {
      const behind = await api.json("GET", path);
      expect(behind.computed_at).not.toBeNull(); expect(behind.stale).toBe(true);
      expect(behind.payload.atom_count).toBeGreaterThan(0);
    } finally { await stop(); }
    await expect.poll(async () => (await api.json("GET", path)).payload.atom_count, { timeout: 60_000, intervals: [500, 1000] }).toBe(0);
    const reduced = await api.json("GET", path); expect(reduced.payload.atom_count).toBe(0);
    const after = await api.json("GET", "/v1/e2e/read-model-computes");
    expect(after.worker_computes).toBeGreaterThan(before.worker_computes); expect(after.request_computes).toBe(0);
    expect((await api.call("GET", `${path}?compute_as=forged`)).status()).toBe(400);
    console.log(JSON.stringify({ scenario: "read_model_lifecycle", plane, first: first.computed_at, final: reduced.computed_at, computes: after }));
  });
}

// Real HTTP/Clerk/serving-role regression for the SQL behind plain page routes.
// The only privileged connection holds a table lock or observes pg_stat_activity;
// all request SQL runs through the production handler and serving role.
import { test, expect } from "@playwright/test";
import { spawn, execFileSync } from "node:child_process";
import { createReadyOrg, personalUser } from "../lib/isolation";
import { API_BASE, CONTAINERS, CONTROL_DB, FAKE, PORTS, REMOTE } from "../stack/config.mjs";

function sql(db: string, query: string): string {
  return execFileSync("docker", ["exec", "-i", CONTAINERS.pg, "psql", "-XAt", "-v", "ON_ERROR_STOP=1", "-U", "postgres", "-d", db, "-c", query], { encoding: "utf8" }).trim();
}

async function lock(db: string, table: string) {
  const child = spawn("docker", ["exec", "-i", CONTAINERS.pg, "psql", "-XAt", "-v", "ON_ERROR_STOP=1", "-U", "postgres", "-d", db], { stdio: ["pipe", "pipe", "pipe"] });
  let output = "";
  await new Promise<void>((resolve, reject) => {
    child.stdout.on("data", (data) => { output += data; if (output.includes("locked")) resolve(); });
    child.on("error", reject);
    child.on("exit", (code) => { if (!output.includes("locked")) reject(new Error(`lock exited ${code}`)); });
    // Transaction ends on stdin EOF even if the assertion fails.
    child.stdin.write(`BEGIN; SET LOCAL lock_timeout = '3s'; LOCK TABLE ${table} IN ACCESS EXCLUSIVE MODE; SELECT 'locked';\n`);
  });
  return async () => {
    const exited = new Promise<void>((resolve) => child.once("exit", () => resolve()));
    child.stdin.end("ROLLBACK;\n");
    await exited;
  };
}

test("plain graph handlers cancel locked SQL and release their page connections", async ({ request }) => {
  test.skip(REMOTE, "table locks require the isolated local stack");
  const org = await createReadyOrg(request, "page-lock");
  const personal = await personalUser(request, "page-lock-personal");
  await personal.api.graphs();
  const tenantDb = sql(CONTROL_DB, `SELECT 'coralswarm_tenant_' || replace(slug, '-', '_') FROM tenant_provisioning_jobs WHERE clerk_org_id = '${org.orgId}'`);
  expect(sql("postgres", `SELECT count(*) FROM pg_database WHERE datname = '${tenantDb}'`)).toBe("1");
  for (const [db, api] of [[tenantDb, org.adminInOrg], [CONTROL_DB, personal.api]] as const) {
    for (const path of ["/v1/graphs", "/v1/bootstrap"]) {
      await api.json("GET", path); // warm real auth and pools before locking
      const unlock = await lock(db, "graphs");
      const start = Date.now();
      let response;
      try {
        response = await Promise.race([
          api.call("GET", path),
          new Promise<never>((_, reject) => setTimeout(() => reject(new Error(`${path} still blocked after 2200ms (1s SQL budget missing)`)), 2200)),
        ]);
        const elapsed = Date.now() - start;
        console.log(JSON.stringify({ scenario: "locked_page_handler", plane: db === CONTROL_DB ? "personal" : "org", path, elapsed_ms: elapsed, status: response.status() }));
        expect(elapsed, `${path} held a connection past the 1s SQL budget`).toBeLessThan(1800);
        expect(elapsed).toBeGreaterThan(850);
        expect(response.status()).toBe(500);
        // Rollback must have cleared the aborted transaction while the
        // table is STILL locked (no successful read can hide a held slot).
        await expect.poll(() => Number(sql(db, "SELECT count(*) FROM pg_stat_activity WHERE usename IN ('coralswarm_app', 'coralswarm_tenant_app') AND (state LIKE 'idle in transaction%' OR (state <> 'idle' AND query ILIKE '%resolve%reachable%graphs%'))"))).toBe(0);
        if (db === tenantDb && path === "/v1/graphs") {
          // Both reserved slots must be reusable while the lock is still
          // held. One leaked slot would serialize these into ~2 seconds.
          const wave = Date.now();
          const next = await Promise.all([api.call("GET", path), api.call("GET", path)]);
          expect(Date.now() - wave, "page slot leaked after timed-out handler").toBeLessThan(1800);
          expect(next.map((res) => res.status())).toEqual([500, 500]);
        }
      } finally { await unlock(); }
      expect((await api.call("GET", path)).ok()).toBeTruthy();
    }
  }
});

test("control page handlers enforce read-only SQL and retain fallback contracts", async ({ request }) => {
  test.skip(REMOTE, "SQL probes require the isolated local stack");
  const who = await personalUser(request, "control-page-read");
  await who.api.graphs();
  const billing = await who.api.json("GET", "/v1/billing/me");
  const tiers = await who.api.json("GET", "/v1/billing/seat-tiers");
  expect(tiers.length).toBeGreaterThan(0);
  // Keep the real handler query and data. This temporary view adds a probe
  // at its query seam; the probe asserts GUCs AND attempts a persistent write.
  // A setting-only test would miss a helper that sets a misleading local GUC.
  sql(CONTROL_DB, `BEGIN;
    CREATE TABLE l1_write_probe (value integer);
    CREATE FUNCTION l1_read_probe() RETURNS boolean LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path = public, pg_temp AS $$
    BEGIN
      IF current_setting('transaction_read_only') <> 'on' OR current_setting('statement_timeout') <> '1s' THEN
        RAISE EXCEPTION 'plain control query missing read-only/1s budget';
      END IF;
      BEGIN
        INSERT INTO l1_write_probe VALUES (1);
      EXCEPTION WHEN read_only_sql_transaction THEN RETURN true;
      END;
      RAISE EXCEPTION 'page read allowed a persistent write';
    END $$;
    ALTER TABLE seat_tiers RENAME TO l1_original_seat_tiers;
    CREATE VIEW seat_tiers AS SELECT * FROM l1_original_seat_tiers WHERE l1_read_probe();
    GRANT SELECT ON seat_tiers TO coralswarm_app;
    ALTER TABLE plans RENAME TO l1_original_plans;
    CREATE VIEW plans AS SELECT * FROM l1_original_plans WHERE l1_read_probe();
    GRANT SELECT ON plans TO coralswarm_app;
    COMMIT;`);
  try {
    expect(await who.api.json("GET", "/v1/billing/seat-tiers")).toEqual(tiers);
    expect(await who.api.json("GET", "/v1/billing/me")).toEqual(billing);
    expect(sql(CONTROL_DB, "SELECT count(*) FROM l1_write_probe")).toBe("0");
  } finally {
    sql(CONTROL_DB, "BEGIN; DROP VIEW plans; ALTER TABLE l1_original_plans RENAME TO plans; DROP VIEW seat_tiers; ALTER TABLE l1_original_seat_tiers RENAME TO seat_tiers; DROP FUNCTION l1_read_probe(); DROP TABLE l1_write_probe; COMMIT;");
  }

  for (const [path, table, expected] of [
    ["/v1/me", "personal_orgs", 503], // real auth prefetch must be bounded too
    ["/v1/me", "user_active_graph", 200],
    ["/v1/billing/me", "extra_usage_settings", 500],
    ["/v1/billing/summary", "extra_usage_settings", 500],
  ] as const) {
    await who.api.json("GET", path);
    const unlock = await lock(CONTROL_DB, table);
    const started = Date.now();
    try {
      const res = await Promise.race([
        who.api.call("GET", path),
        new Promise<never>((_, reject) => setTimeout(() => reject(new Error(`${path} exceeded its SQL budget`)), 2200)),
      ]);
      const elapsed = Date.now() - started;
      console.log(JSON.stringify({ scenario: "locked_control_handler", path, elapsed_ms: elapsed, status: res.status() }));
      expect(elapsed).toBeGreaterThan(850);
      expect(elapsed).toBeLessThan(1800);
      expect(res.status()).toBe(expected);
      if (table === "user_active_graph") expect((await res.json()).active_graph_id).toBeNull();
    } finally { await unlock(); }
    expect((await who.api.call("GET", path)).ok()).toBeTruthy();
  }
});


test("public prefill keeps its null contract under a locked token lookup", async ({ request }) => {
  test.skip(REMOTE, "table locks require the isolated local stack");
  const timeouts = async () => {
    const response = await request.get(`http://localhost:${PORTS.internal}/internal/admin/page-read-health`, {
      headers: { "X-Internal-Token": FAKE.internalToken },
    });
    expect(response.ok()).toBeTruthy();
    const health = await response.json();
    return health.per_route.find((route: { route: string }) => route.route === "/v1/email/prefill")?.statement_timeouts ?? 0;
  };
  const before = await timeouts();
  const unlock = await lock(CONTROL_DB, "slack_unmatched_email_notices");
  const started = Date.now();
  try {
    const res = await Promise.race([
      request.get(`${API_BASE}/v1/email/prefill?t=page-read-missing-token`),
      new Promise<never>((_, reject) => setTimeout(() => reject(new Error("prefill lookup exceeded its SQL budget")), 2200)),
    ]);
    const elapsed = Date.now() - started;
    console.log(JSON.stringify({ scenario: "locked_public_prefill", elapsed_ms: elapsed, status: res.status() }));
    expect(elapsed).toBeGreaterThan(850);
    expect(elapsed).toBeLessThan(1800);
    expect(res.status()).toBe(200);
    expect(await res.json()).toEqual({ email: null });
    // The handler deliberately swallows the SQL error. The health ledger
    // must still record exactly one cancelled statement for this request.
    await expect.poll(timeouts).toBe(before + 1);
  } finally { await unlock(); }
});

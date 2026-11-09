import { test, expect } from "@playwright/test";
import { test as signedInTest } from "../lib/fixtures";
import { personalUser } from "../lib/isolation";
import { waitForComputed } from "../lib/readModels";
import { API_BASE } from "../stack/config.mjs";

// Product-level companion to read_models_pg's obsolete-row/claim regressions.
// Seed via real OAuth and disconnect APIs; no fabricated cache envelope.
test("personal: connector revocation fences the old revision while real workers refresh", async ({ request }) => {
  test.setTimeout(180_000);
  const { api } = await personalUser(request, "revision-retirement");
  await api.graphs();
  const path = "/v1/connectors/slack";
  const { state } = await api.json("GET", "/v1/oauth/state");
  const callback = await request.get(
    `${API_BASE}/v1/oauth/slack/callback?state=${encodeURIComponent(state)}&code=team-retire${Date.now().toString(36)}`,
    { maxRedirects: 0 },
  );
  expect(callback.status()).toBe(307);
  expect(callback.headers().location).toContain("connected=slack");
  const connected = await waitForComputed(api, path);
  expect(connected.payload.status).not.toBe("not_connected");

  expect((await api.call("DELETE", path)).ok()).toBeTruthy();
  // An already-computed new answer is allowed; the old connected payload is
  // never allowed, including while the replacement has no computed_at yet.
  for (let i = 0; i < 3; i++) {
    const answer = await api.json("GET", path);
    expect(answer.payload.status).toBe("not_connected");
    if (answer.computed_at !== null) expect(answer.computed_at).not.toBe(connected.computed_at);
  }
  const refreshed = await waitForComputed(api, path, false);
  expect(refreshed.payload.status).toBe("not_connected");
  expect(refreshed.computed_at).not.toBe(connected.computed_at);
});

// Real Clerk session + real HTTP handler + real queue/worker. Local stack DML
// only: hold computation after a genuine authority change; never mock a cert.
signedInTest("personal: OAuth scope change withholds N+1 until worker publication and records first-fill stages", async ({ request, page, signedIn }) => {
  const { execFileSync, spawn } = await import("node:child_process");
  const { CONTAINERS, CONTROL_DB } = await import("../stack/config.mjs");
  const sql = (query: string) => execFileSync("docker", ["exec", "-i", CONTAINERS.pg, "psql", "-XAt", "-v", "ON_ERROR_STOP=1", "-U", "postgres", "-d", CONTROL_DB, "-c", query], { encoding: "utf8" }).trim();
  const { api, user } = signedIn;
  await api.graphs();
  const { state } = await api.json("GET", "/v1/oauth/state");
  const callback = await request.get(`${API_BASE}/v1/oauth/slack/callback?state=${encodeURIComponent(state)}&code=team-stages${Date.now().toString(36)}`, { maxRedirects: 0 });
  expect(callback.status()).toBe(307);
  const path = "/v1/connectors";
  const prior = await waitForComputed(api, path, false, () => true, 10_000);
  // Fixtures create Clerk IDs, never arbitrary SQL fragments.
  expect(user.id).toMatch(/^user_[A-Za-z0-9]+$/);
  const account = `SELECT a.id FROM accounts a JOIN users u ON u.id=a.owner_user_id WHERE u.clerk_user_id='${user.id}'`;
  const lock = spawn("docker", ["exec", "-i", CONTAINERS.pg, "psql", "-XAt", "-v", "ON_ERROR_STOP=1", "-U", "postgres", "-d", CONTROL_DB], { stdio: ["pipe", "pipe", "pipe"] });
  const done = new Promise<void>((resolve, reject) => { lock.once("exit", code => code === 0 ? resolve() : reject(new Error("first-fill barrier failed"))); lock.once("error", reject); });
  await new Promise<void>((resolve, reject) => {
    lock.stdout.on("data", data => { if (String(data).includes("stage_locked")) resolve(); });
    lock.once("error", reject);
    lock.stdin.write("BEGIN; SET LOCAL lock_timeout='3s'; LOCK TABLE atoms IN ACCESS EXCLUSIVE MODE; SELECT 'stage_locked';\n");
  });
  try {
    sql(`UPDATE oauth_tokens SET scope=scope||' synthetic_authority_scope' WHERE account_id IN (${account}) AND provider='slack'`);
    const current = await api.json("GET", path);
    expect(current.computed_at).toBeNull();
    expect(current.stale).toBe(true);
    const missingResponse = page.waitForResponse(response => new URL(response.url()).pathname === path && response.request().method() === "GET" && response.status() === 200);
    await page.goto("/connectors", { waitUntil: "domcontentloaded" });
    expect((await (await missingResponse).json()).computed_at).toBeNull();
    await expect(page.locator('[data-page-loaded="false"]').getByRole("main")).toBeVisible();
    await expect.poll(() => sql(`SELECT count(*) FROM read_models WHERE account_id IN (${account}) AND model='connectors_list' AND computed_at IS NULL AND immediate_due_at IS NOT NULL AND refresh_claimed_at>=immediate_due_at`), { timeout: 8_000, intervals: [100, 250] }).toBe("1");
    expect((await api.json("GET", path)).computed_at).toBeNull();
    expect(Number(sql(`SELECT sum(statements) FROM read_model_oauth_changes WHERE account_id IN (${account}) AND column_name='scope' AND semantic_class='authority'`))).toBeGreaterThan(0);
  } finally { lock.stdin.end("ROLLBACK;\n"); await done; }
  const filled = await waitForComputed(api, path, false, () => true, 8_000);
  expect(filled.computed_at).not.toBe(prior.computed_at);
  await expect(page.locator('[data-page-loaded="true"]').getByRole("main")).toBeVisible({ timeout: 8_000 });
  expect(sql(`SELECT count(*) FROM read_models WHERE account_id IN (${account}) AND model='connectors_list' AND computed_at IS NOT NULL AND immediate_due_at IS NULL`)).toBe("1");
});

import { test as base, expect } from "@playwright/test";
import { createReadyOrg, type Org } from "../lib/isolation";
import { test } from "../lib/fixtures";

// Page-load standard L1 (docs/plans/page-load-standard/design.md §4, §10):
// every response names its route class in Server-Timing, and a `page_read`
// route runs on the tenant's own page pool lane, so it keeps answering while
// Other-class requests hold every work-lane connection.

test("every response names its route class in Server-Timing", async ({ signedIn }) => {
  for (const path of [
    "/v1/me", "/v1/bootstrap", "/v1/graphs", "/v1/activity",
    "/v1/billing/me", "/v1/billing/summary", "/v1/billing/seat-tiers",
    "/v1/me/delete-account", "/v1/retractions", "/v1/oauth/connections",
    "/v1/addin/workspace", "/v1/addin/tokens", "/v1/addin/pair/info?code=missing",
    "/v1/shared/missing", "/v1/shared/sessions/missing",
    "/v1/meetings/shares/view/missing", "/v1/email/prefill?t=missing",
  ]) {
    const res = await signedIn.api.call("GET", path);
    // Feature gates / token refusals may be 4xx; the matched route's class
    // must still be visible, and a page route must never 5xx merely from tagging.
    expect(res.status(), path).toBeLessThan(500);
    expect(res.headers()["server-timing"], path).toContain('class;desc="page_read"');
  }
  const mutation = await signedIn.api.call("POST", "/v1/graphs", { name: "route-class mutation" });
  expect(mutation.ok(), await mutation.text()).toBeTruthy();
  expect(mutation.headers()["server-timing"]).toContain('class;desc="other"');
  const missing = await signedIn.api.call("GET", "/v1/route-class-does-not-exist");
  expect(missing.status()).toBe(404);
  expect(missing.headers()["server-timing"]).toContain('class;desc="other"');

  const prefs = await signedIn.api.call("GET", "/v1/prefs");
  expect(prefs.ok()).toBeTruthy();
  expect(prefs.headers()["server-timing"]).toContain('class;desc="other"');
});

// One real self-serve org per worker: its tenant database has the two-lane
// request pool (page 2 / work 3) the proof below saturates.
const orgTest = base.extend<{}, { org: Org }>({
  org: [
    async ({ playwright }, use) => {
      const request = await playwright.request.newContext();
      await use(await createReadyOrg(request, "route-class"));
      await request.dispose();
    },
    { scope: "worker", timeout: 60_000 },
  ],
});

orgTest("a page read answers while the work lane is saturated", async ({ org }) => {
  const api = org.adminInOrg;
  // Warm the pool and the token so the timed call below measures the lane,
  // not a cold connection or a Clerk round trip.
  await api.json("GET", "/v1/graphs");

  // Six Other-class requests, each holding one work-lane connection for 3 s
  // (`GET /v1/e2e/slow`, e2e build only). The work lane has three
  // connections, so three run and three wait on acquire: the lane is full
  // for the whole window.
  const holders = Array.from({ length: 6 }, () => api.call("GET", "/v1/e2e/slow?secs=3"));
  await new Promise((r) => setTimeout(r, 400));

  const started = Date.now();
  const graphs = await api.call("GET", "/v1/graphs");
  const elapsed = Date.now() - started;
  console.log(JSON.stringify({
    scenario: "page_read_saturated_work_lane",
    enforcement: process.env.E2E_CONCURRENCY_ENFORCE === "1",
    elapsed_ms: elapsed,
    server_timing: graphs.headers()["server-timing"],
  }));
  expect(graphs.ok(), await graphs.text()).toBeTruthy();
  expect(graphs.headers()["server-timing"]).toContain('class;desc="page_read"');
  // On one shared five-connection pool this call would wait for a sleeper
  // to finish (about 3 s, or the 5 s acquire timeout). On the page lane it
  // is an indexed read; 1500 ms leaves room for a slow runner.
  expect(elapsed, `page read took ${elapsed} ms behind a saturated work lane`).toBeLessThan(1500);

  const responses = await Promise.all(holders);
  const shed = responses.filter((res) => res.status() === 503);
  if (process.env.E2E_CONCURRENCY_ENFORCE === "1") {
    // Opt-in rehearsal against a stack booted with this same setting.
    expect(shed).toHaveLength(3);
    for (const res of shed) {
      expect(res.headers()["x-coralswarm-admission"]).toBe("concurrency");
      expect(Number(res.headers()["retry-after"])).toBeGreaterThanOrEqual(1);
      expect(res.headers()["server-timing"]).toContain('class;desc="other"');
    }
  } else {
    // Observe-only is the approved L1 default.
    expect(shed).toHaveLength(0);
  }
  for (const res of responses.filter((res) => res.status() !== 503)) {
    expect(res.status(), await res.text()).toBe(200);
    expect(res.headers()["x-coralswarm-admission"]).toBeUndefined();
    expect((await res.json()).class).toBe("other");
  }
});

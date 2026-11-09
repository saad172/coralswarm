import { waitForComputed } from "../lib/readModels";
import { expect, test } from "../lib/fixtures";
import { API_BASE } from "../stack/config.mjs";

// Home dashboard data (`GET /v1/home/metrics`, flag HOME_DASHBOARD_ENABLED,
// set on in stack/config.mjs). API-level: seeded through the real ingest
// route, read back as the owner, and checked to be invisible to a stranger.

test("home metrics count what the user ingested, per ocean and day, and no one else's", async ({ makeUser, request }) => {
  test.setTimeout(180_000);
  const owner = await makeUser("home-owner", "Home Owner");
  const stranger = await makeUser("home-stranger", "Home Stranger");
  const graph = await owner.api.personalGraph();

  const before = (await waitForComputed(owner.api, "/v1/home/metrics?days=7&tz=America/Chicago")).payload;
  expect(before.window.days).toBe(7);
  expect(before.window.tz).toBe("America/Chicago");
  expect(before.kpis.corals_ingested.daily).toHaveLength(7);
  expect(before.ingest_by_day).toHaveLength(7);
  expect(before.source_type_groups.map((g: any) => g.id)).toEqual([
    "chat",
    "meetings",
    "agent_sessions",
    "docs_mail",
    "code_tickets",
  ]);

  await owner.api.ingest(graph.id, "Home metrics e2e: the Orchid rollout checklist is owned by Priya.");
  await owner.api.ingest(graph.id, "Home metrics e2e: the Orchid launch moved to March 14.");

  const after = (await waitForComputed(owner.api, "/v1/home/metrics?days=7", true, p => p.kpis.corals_ingested.total >= 2)).payload;
  expect(after.kpis.corals_ingested.total).toBeGreaterThanOrEqual(2);
  // A note is a Docs & mail coral; today is the last bucket.
  expect(after.source_type_totals.docs_mail).toBeGreaterThanOrEqual(2);
  expect(after.kpis.corals_ingested.daily.at(-1)).toBeGreaterThanOrEqual(2);
  const ocean = after.oceans.find((o: any) => o.graph_id === graph.id);
  expect(ocean, JSON.stringify(after.oceans)).toBeTruthy();
  expect(ocean.total).toBe(after.oceans.reduce((n: number, o: any) => n + o.total, 0));
  const today = after.ingest_by_day.at(-1).by_graph.find((g: any) => g.graph_id === graph.id);
  expect(today.by_source_type.docs_mail).toBeGreaterThanOrEqual(2);

  // The stranger's own Home does not include the owner's ocean or corals.
  const theirs = (await waitForComputed(stranger.api, "/v1/home/metrics?days=7")).payload;
  expect(JSON.stringify(theirs)).not.toContain(graph.id);
  expect(theirs.kpis.corals_ingested.total).toBe(0);

  // Bad ranges and zones are refused; no token is refused.
  for (const bad of ["days=6", "days=61", "tz=Mars/Base"]) {
    const res = await owner.api.call("GET", `/v1/home/metrics?${bad}`);
    expect(res.status(), bad).toBe(400);
  }
  expect((await request.get(`${API_BASE}/v1/home/metrics`)).status()).toBe(401);
});

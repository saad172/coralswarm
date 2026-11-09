import { afterEach, expect, it, vi } from "vitest";
import { ApiError, setTokenRefresher } from "./api";
import { __resetTransportForTests } from "./transport";
import {
  homeMetricsPath,
  kpiDeltaPct,
  type HomeMetrics,
  type HomeMetricsParams,
} from "./pageSectionTypes/home";
import { homeMetricsQueryOptions } from "./appQueries";

afterEach(() => {
  setTokenRefresher(null);
  vi.unstubAllGlobals();
  __resetTransportForTests();
});

async function read(params: HomeMetricsParams = {}) {
  const options = homeMetricsQueryOptions({ scope: "fixture", getAuthToken: () => "tok" }, params);
  const value = options.select(await options.queryFn({ signal: new AbortController().signal }));
  if (!value) throw new Error("Expected computed fixture");
  return value;
}

const body: HomeMetrics = {
  window: { start: "2026-09-30T00:00:00Z", end: "2026-10-30T00:00:00Z", days: 30, tz: "UTC" },
  kpis: {
    corals_ingested: { total: 3, prior_total: 2, daily: new Array(30).fill(0) },
    reefs_formed: { total: 0, prior_total: 0, daily: new Array(30).fill(0) },
    questions_asked: { total: 0, prior_total: 0, daily: new Array(30).fill(0) },
    agent_sessions: { total: 0, prior_total: 0, daily: new Array(30).fill(0) },
    meetings_captured: { total: 0, prior_total: 0, daily: new Array(30).fill(0) },
  },
  ingest_by_day: [],
  oceans: [],
  source_type_totals: { chat: 3, meetings: 0, agent_sessions: 0, docs_mail: 0, code_tickets: 0 },
  source_type_groups: [],
  fallback_group: "docs_mail",
};

it("builds the path with only the params given and rejects out-of-range days", () => {
  expect(homeMetricsPath()).toBe("/v1/home/metrics");
  expect(homeMetricsPath({ days: 7, tz: "America/Chicago" })).toBe("/v1/home/metrics?days=7&tz=America%2FChicago");
  expect(() => homeMetricsPath({ days: 6 })).toThrow();
  expect(() => homeMetricsPath({ days: 61 })).toThrow();
  expect(() => homeMetricsPath({ days: 7.5 })).toThrow();
});

it("hides the delta when there is no prior window and rounds otherwise", () => {
  expect(kpiDeltaPct({ total: 5, prior_total: 0, daily: [] })).toBeNull();
  expect(kpiDeltaPct({ total: 3, prior_total: 2, daily: [] })).toBe(50);
  expect(kpiDeltaPct({ total: 1, prior_total: 3, daily: [] })).toBe(-67);
});

it("fetches with the bearer and returns the typed body", async () => {
  const fetch = vi.fn().mockResolvedValue(new Response(JSON.stringify({ model: "home_metrics", version: 1, payload: body, computed_at: "2026-10-31T00:00:00Z", stale: false, refresh_eta: null })));
  vi.stubGlobal("fetch", fetch);
  const got = await read({ days: 30, tz: "UTC" });
  expect(got.kpis.corals_ingested.total).toBe(3);
  const [url, init] = fetch.mock.calls[0];
  expect(String(url)).toContain("/v1/home/metrics?days=30&tz=UTC");
  expect(new Headers(init.headers).get("Authorization")).toBe("Bearer tok");
});

it("retains a 404 refusal for the existing feature-off view", async () => {
  vi.stubGlobal("fetch", vi.fn().mockResolvedValue(new Response("{}", { status: 404 })));
  await expect(read()).rejects.toBeInstanceOf(ApiError);
});

it("keys the query by scope and params, never the bearer", () => {
  const ctx = { getAuthToken: () => "secret", scope: "u1:o1" };
  const a = homeMetricsQueryOptions(ctx, { days: 30, tz: "UTC" });
  const b = homeMetricsQueryOptions(ctx, { days: 7 });
  expect(a.queryKey).toEqual(["home-metrics", "u1:o1", expect.objectContaining({ days: 30, tz: "UTC" })]);
  expect(b.queryKey).not.toEqual(a.queryKey);
  expect(JSON.stringify(a.queryKey)).not.toContain("secret");
});

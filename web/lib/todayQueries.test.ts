import { beforeEach, expect, it, vi } from "vitest";
import { ApiError } from "./api";
import type { QueryCtx } from "./appQueries";
import { todayActivitiesQueryOptions } from "./todayQueries";

const wire = vi.hoisted(() => ({ fetch: vi.fn() }));
vi.mock("./transport", async (original) => ({ ...await original<typeof import("./transport")>(), guardedFetch: wire.fetch }));

const ctx: QueryCtx = { getAuthToken: () => "tok", scope: "user_1:org_1" };
const params = { from: "2026-10-29T05:00:00Z", to: "2026-10-30T05:00:00Z" };
const run = (c: QueryCtx = ctx, who: "mine" | "team" = "mine") => {
  const o = todayActivitiesQueryOptions(c, { ...params, who, ...(who === "team" ? { graphId: "11111111-1111-4111-8111-111111111111" } : {}) });
  return (o.queryFn as (x: { signal: AbortSignal }) => Promise<unknown>)({ signal: new AbortController().signal });
};

beforeEach(() => wire.fetch.mockReset());

it("returns the real Stored envelope and selects its computed payload", async () => {
  const body = { activities: [], day_activity: [], truncated: false, window: { from: "a", to: "b", tz: "UTC", who: "mine" as const } };
  wire.fetch.mockResolvedValue(new Response(JSON.stringify({ model: "journal_activities", version: 1, payload: body, computed_at: "2026-10-31T00:00:00Z", stale: false, refresh_eta: null })));
  const stored = await run();
  expect(stored).toMatchObject({ model: "journal_activities", payload: body, computed_at: "2026-10-31T00:00:00Z" });
  expect(todayActivitiesQueryOptions(ctx, params).select!(stored as import("./pageSections").Stored<typeof body>)).toEqual(body);
});

it("keeps team and personal refusals as errors without a fallback read", async () => {
  wire.fetch.mockResolvedValue(new Response(JSON.stringify({ error: "not found" }), { status: 404 }));
  await expect(run(ctx, "team")).rejects.toBeInstanceOf(ApiError);
  await expect(run()).rejects.toBeInstanceOf(ApiError);
  wire.fetch.mockResolvedValue(new Response(JSON.stringify({ error: "forbidden" }), { status: 403 }));
  await expect(run(ctx, "team")).rejects.toBeInstanceOf(ApiError);
  await expect(run()).rejects.toBeInstanceOf(ApiError);
});

it("still throws every other failure", async () => {
  wire.fetch.mockResolvedValue(new Response(JSON.stringify({ error: "boom" }), { status: 500 }));
  await expect(run()).rejects.toBeInstanceOf(ApiError);
  await expect(run({ getAuthToken: () => "", scope: "s" })).rejects.toThrow("Not authenticated");
});

it("keeps the journal prefix but its own key, apart from Home's feed", () => {
  const key = todayActivitiesQueryOptions(ctx, { ...params, who: "team", graphId: "g1" }).queryKey;
  expect(key.slice(0, 3)).toEqual(["journal", "user_1:org_1", "activities-today"]);
  expect(key[3]).toMatchObject({ who: "team", graphId: "g1" });
});

import { beforeEach, expect, it, vi } from "vitest";
import { ApiError } from "./api";
import type { QueryCtx } from "./appQueries";
import {
  activitiesQuery,
  fetchJournalActivities,
  fetchJournalActivity,
  journalActivitiesQueryOptions,
  journalActivityQueryOptions,
  type ActivitiesResponse,
} from "./journalActivitiesApi";

const wire = vi.hoisted(() => ({ fetch: vi.fn() }));
vi.mock("./transport", async (original) => ({ ...await original<typeof import("./transport")>(), guardedFetch: wire.fetch }));

const ctx: QueryCtx = { getAuthToken: () => "tok", scope: "user_1:org_1" };
const response: ActivitiesResponse = {
  activities: [],
  day_activity: [],
  truncated: false,
  window: { from: "2026-10-29T00:00:00Z", to: "2026-10-30T00:00:00Z", tz: "UTC", who: "mine" },
};

beforeEach(() => wire.fetch.mockReset().mockResolvedValue(new Response(JSON.stringify(response))));

it("builds the list query with who defaulting to mine and only the set filters", () => {
  const q = new URLSearchParams(activitiesQuery({ from: "a", to: "b" }));
  expect(Object.fromEntries(q)).toEqual({ from: "a", to: "b", who: "mine" });
  const full = new URLSearchParams(activitiesQuery({ from: "a", to: "b", who: "team", graphId: "g1", tz: "Europe/London" }));
  expect(Object.fromEntries(full)).toEqual({ from: "a", to: "b", who: "team", graph_id: "g1", tz: "Europe/London" });
});

it("sends the bearer at fetch time with no-store and returns the body", async () => {
  const out = await fetchJournalActivities("tok", { from: "2026-10-29T00:00:00Z", to: "2026-10-30T00:00:00Z", who: "team" });
  expect(out).toEqual(response);
  const [path, init] = wire.fetch.mock.calls[0];
  expect(path).toBe("/v1/journal/activities?from=2026-10-29T00%3A00%3A00Z&to=2026-10-30T00%3A00%3A00Z&who=team");
  expect(init).toMatchObject({ method: "GET", cache: "no-store", headers: { Authorization: "Bearer tok" } });
});

it("url-encodes the opaque key and carries the ocean and zone", async () => {
  wire.fetch.mockResolvedValue(new Response("{}"));
  await fetchJournalActivity("tok", "a/b+c=", "g1", { tz: "UTC" });
  expect(wire.fetch.mock.calls[0][0]).toBe("/v1/journal/activities/a%2Fb%2Bc%3D?graph_id=g1&tz=UTC");
});

it("throws an ApiError with the server message and status (404 = flag off or no access)", async () => {
  wire.fetch.mockResolvedValue(new Response(JSON.stringify({ error: "not found" }), { status: 404 }));
  const error = await fetchJournalActivities("tok", { from: "a", to: "b" }).catch((e: unknown) => e);
  expect(error).toBeInstanceOf(ApiError);
  expect(error).toMatchObject({ status: 404, message: "not found" });
  wire.fetch.mockResolvedValue(new Response("<html>", { status: 502 }));
  expect(await fetchJournalActivities("tok", { from: "a", to: "b" }).catch((e: unknown) => e)).toMatchObject({ status: 502, message: "HTTP 502" });
});

it("refuses to call without a token", async () => {
  await expect(fetchJournalActivities("", { from: "a", to: "b" })).rejects.toThrow("Not authenticated");
  expect(wire.fetch).not.toHaveBeenCalled();
});

it("keys start [journal, scope, ...] so appInvalidation.journal() reaches them, and never hold the bearer", () => {
  const list = journalActivitiesQueryOptions(ctx, { from: "a", to: "b" });
  expect(list.queryKey.slice(0, 3)).toEqual(["journal", "user_1:org_1", "activities"]);
  expect(list.queryKey[3]).toEqual({ from: "a", to: "b", who: "mine", graphId: null, tz: null });
  expect(JSON.stringify(list.queryKey)).not.toContain("tok");
  const team = journalActivitiesQueryOptions(ctx, { from: "a", to: "b", who: "team" });
  expect(team.queryKey).not.toEqual(list.queryKey);
  const detail = journalActivityQueryOptions(ctx, "k", "g1");
  expect(detail.queryKey.slice(0, 4)).toEqual(["journal", "user_1:org_1", "activity", "k"]);
});

it("the query functions read the token when they run", async () => {
  let token: string | null = null;
  const lazy: QueryCtx = { getAuthToken: () => token, scope: "s" };
  const opts = journalActivitiesQueryOptions(lazy, { from: "a", to: "b" });
  const run = opts.queryFn as (c: { signal: AbortSignal }) => Promise<unknown>;
  await expect(run({ signal: new AbortController().signal })).rejects.toThrow("Not authenticated");
  token = "later";
  wire.fetch.mockResolvedValue(new Response(JSON.stringify({ model: "journal_activities", version: 1, payload: response, computed_at: "2026-10-31T00:00:00Z", stale: false, refresh_eta: null })));
  await run({ signal: new AbortController().signal });
  expect(new Headers(wire.fetch.mock.calls[0][1].headers).get("Authorization")).toBe("Bearer later");
});

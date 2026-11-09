import { journalGistQueryOptions } from "./todayQueries";
import { beforeEach, expect, it, vi } from "vitest";
import { ApiError } from "./api";
import type { QueryCtx } from "./appQueries";
import { gistQuery, gistRows, type GistResponse } from "./pageSectionTypes/todayGist";

const wire = vi.hoisted(() => ({ fetch: vi.fn() }));
vi.mock("./transport", async (original) => ({ ...await original<typeof import("./transport")>(), guardedFetch: wire.fetch }));

const ctx: QueryCtx = { getAuthToken: () => "tok", scope: "user_1:org_1" };
const response: GistResponse = {
  period_start: "2026-10-29T00:00:00Z",
  period_end: "2026-10-30T00:00:00Z",
  open: true,
  who: "mine",
  granularity: "day",
  tz: "UTC",
  gist: { done: ["Proration fix up as PR #482"], decided: [], open: ["Usage tier still unpriced"] },
  compiled_at: "2026-10-29T14:10:00Z",
  span: { first_activity: "2026-10-29T08:04:00Z", last_activity: "2026-10-29T13:51:00Z" },
};

async function read(token: string, params: import("./pageSectionTypes/todayGist").GistParams) {
  const options = journalGistQueryOptions({ scope: "fixture", getAuthToken: () => token }, params);
  return options.select(await options.queryFn({ signal: new AbortController().signal }));
}

beforeEach(() => wire.fetch.mockReset().mockResolvedValue(new Response(JSON.stringify({ model: "journal_gist", version: 1, payload: response, computed_at: "2026-10-31T00:00:00Z", stale: false, refresh_eta: null }))));

it("builds the query with day/mine defaults and only the set filters", () => {
  expect(Object.fromEntries(new URLSearchParams(gistQuery({})))).toEqual({ granularity: "day", who: "mine" });
  const full = new URLSearchParams(gistQuery({ granularity: "week", anchor: "2026-10-28", who: "team", graphId: "g1", tz: "Europe/London" }));
  expect(Object.fromEntries(full)).toEqual({ granularity: "week", who: "team", anchor: "2026-10-28", graph_id: "g1", tz: "Europe/London" });
});

it("sends the bearer at fetch time with no-store and returns the body", async () => {
  const out = await read("tok", { anchor: "2026-10-29", tz: "UTC" });
  expect(out).toEqual(response);
  const [path, init] = wire.fetch.mock.calls[0];
  expect(path).toBe("/v1/journal/gist?anchor=2026-10-29&granularity=day&tz=UTC&who=mine");
  expect(init).toMatchObject({ cache: "no-store", headers: { Authorization: "Bearer tok" } });
});

it("refuses a team read without an ocean, and any read without a token", async () => {
  await expect(read("tok", { who: "team" })).rejects.toThrow("Team gist needs an ocean");
  await expect(read("", {})).rejects.toThrow("Not authenticated");
  expect(wire.fetch).not.toHaveBeenCalled();
});

it("throws an ApiError with the server message and status (404 = flag off or no access)", async () => {
  wire.fetch.mockResolvedValue(new Response(JSON.stringify({ error: "not found" }), { status: 404 }));
  const error = await read("tok", {}).catch((e: unknown) => e);
  expect(error).toBeInstanceOf(ApiError);
  expect(error).toMatchObject({ status: 404, message: "not found" });
});

it("renders rows in DONE, DECIDED, OPEN order and skips empty groups", () => {
  expect(gistRows(response.gist).map((r) => r.label)).toEqual(["DONE", "OPEN"]);
  expect(gistRows(null)).toEqual([]);
});

it("keys start [journal, scope, gist] and never hold the bearer; 404 is not retried", () => {
  const opts = journalGistQueryOptions(ctx, { anchor: "2026-10-29" });
  expect(opts.queryKey.slice(0, 3)).toEqual(["journal", "user_1:org_1", "gist"]);
  expect(opts.queryKey[3]).toEqual({ granularity: "day", anchor: "2026-10-29", who: "mine", graphId: null, tz: null });
  expect(JSON.stringify(opts.queryKey)).not.toContain("tok");
  expect(journalGistQueryOptions(ctx, { who: "team", graphId: "g1" }).queryKey).not.toEqual(opts.queryKey);
  expect(opts.retry).toBe(false); // Transport is the sole retry authority.
});

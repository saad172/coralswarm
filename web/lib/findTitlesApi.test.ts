import { beforeEach, expect, it, vi } from "vitest";
import { ApiError } from "./api";
import type { QueryCtx } from "./appQueries";
import { fetchFindTitles, findTitlesQuery, findTitlesQueryOptions, type FindTitlesResponse } from "./findTitlesApi";

const wire = vi.hoisted(() => ({ fetch: vi.fn() }));
vi.mock("./transport", () => ({ guardedFetch: wire.fetch }));

const ctx: QueryCtx = { getAuthToken: () => "tok", scope: "user_1:org_1" };
const response: FindTitlesResponse = {
  query: "acme",
  results: [
    {
      source_item_id: "i1",
      kind: "meeting",
      kind_label: "meeting",
      title: "Acme pricing review",
      outcome: null,
      occurred_at: "2026-10-15T15:00:00Z",
      duration_ms: 2_520_000,
      people: ["Tom Becker"],
      people_count: 1,
      graph: { id: "g1", name: "GTM" },
      best: false,
      decisions: [],
      reefs: [],
      quotes: [],
    },
  ],
};

beforeEach(() => wire.fetch.mockReset().mockResolvedValue(new Response(JSON.stringify(response))));

it("trims the query and sends the limit only when set", () => {
  expect(Object.fromEntries(new URLSearchParams(findTitlesQuery("  acme ")))).toEqual({ q: "acme" });
  expect(Object.fromEntries(new URLSearchParams(findTitlesQuery("a&b", 5)))).toEqual({ q: "a&b", limit: "5" });
});

it("sends the bearer at fetch time with no-store and returns the body", async () => {
  const out = await fetchFindTitles("tok", "acme", 3);
  expect(out).toEqual(response);
  const [path, init] = wire.fetch.mock.calls[0];
  expect(path).toBe("/v1/find/titles?q=acme&limit=3");
  expect(init).toMatchObject({ method: "GET", cache: "no-store", headers: { Authorization: "Bearer tok" } });
});

it("throws an ApiError with the server message and status (404 = flag off)", async () => {
  wire.fetch.mockResolvedValue(new Response(JSON.stringify({ error: "not found" }), { status: 404 }));
  const error = await fetchFindTitles("tok", "acme").catch((e: unknown) => e);
  expect(error).toBeInstanceOf(ApiError);
  expect(error).toMatchObject({ status: 404, message: "not found" });
  wire.fetch.mockResolvedValue(new Response("<html>", { status: 502 }));
  expect(await fetchFindTitles("tok", "acme").catch((e: unknown) => e)).toMatchObject({ status: 502, message: "HTTP 502" });
});

it("refuses to call without a token", async () => {
  await expect(fetchFindTitles("", "acme")).rejects.toThrow("Not authenticated");
  expect(wire.fetch).not.toHaveBeenCalled();
});

it("keys carry the scope and query but never the bearer; a blank query is disabled", () => {
  const opts = findTitlesQueryOptions(ctx, " acme ", 5);
  expect(opts.queryKey).toEqual(["find", "user_1:org_1", "titles", { q: "acme", limit: 5 }]);
  expect(JSON.stringify(opts.queryKey)).not.toContain("tok");
  expect(opts.enabled).toBe(true);
  expect(findTitlesQueryOptions(ctx, "   ").enabled).toBe(false);
  expect(findTitlesQueryOptions(ctx, "x".repeat(201)).enabled).toBe(false);
});

it("the query function reads the token when it runs", async () => {
  let token: string | null = null;
  const lazy: QueryCtx = { getAuthToken: () => token, scope: "s" };
  const run = findTitlesQueryOptions(lazy, "acme").queryFn as (c: { signal: AbortSignal }) => Promise<unknown>;
  await expect(run({ signal: new AbortController().signal })).rejects.toThrow("Not authenticated");
  token = "later";
  await run({ signal: new AbortController().signal });
  expect(wire.fetch.mock.calls[0][1]).toMatchObject({ headers: { Authorization: "Bearer later" } });
});

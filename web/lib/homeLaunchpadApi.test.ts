import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import {
  homeAttentionQueryOptions,
  homeContinueQueryOptions,
  homeWhatsNewQueryOptions,
} from "./homeLaunchpadApi";
import { appInvalidation } from "./appQueries";
import { partialMatchKey } from "@tanstack/query-core";
import { PLAIN_LOAD_PATHS } from "./pageShellPaths";

const wire = vi.hoisted(() => ({ fetch: vi.fn() }));
vi.mock("./transport", async (original) => ({ ...await original<typeof import("./transport")>(), guardedFetch: wire.fetch }));

beforeEach(() => wire.fetch.mockReset());
afterEach(() => vi.restoreAllMocks());

const ctx = { scope: "fixture", getAuthToken: () => "tok" };
const factories = [
  ["continue", homeContinueQueryOptions, "/v1/home/continue", { items: [], generated_at: "2026-11-03T12:00:00Z" }],
  ["whatsNew", homeWhatsNewQueryOptions, "/v1/home/whats-new", { items: [], window_days: 7, generated_at: "2026-11-03T12:00:00Z" }],
  ["attention", homeAttentionQueryOptions, "/v1/home/attention", { has_content: false, has_source: false, has_mcp: false, attention: null, generated_at: "2026-11-03T12:00:00Z" }],
] as const;

describe("Home launchpad reads", () => {
  it.each(factories)("%s is a declared plain page read", (_name, factory, path) => {
    expect(Object.values(PLAIN_LOAD_PATHS)).toContain(path);
    expect(factory(ctx).meta).toMatchObject({ pageRead: true, plain: path, priority: "critical" });
  });

  it.each(factories)("%s re-reads on every visit, refetches on a stale tab return, and never polls", (_name, factory) => {
    const options = factory(ctx) as Record<string, unknown>;
    expect(options.refetchOnMount).toBe("always");
    expect(options.staleTime).toBe(5 * 60_000);
    expect(options.refetchOnWindowFocus).toBe(true);
    expect(options.refetchInterval).toBeUndefined();
    // Previous data across a key change would carry another identity's rows.
    expect(options.placeholderData).toBeUndefined();
  });

  it("keys each read as its own resource, scope second, so a mutation can invalidate just that one", () => {
    const keys = factories.map(([, factory]) => factory(ctx).queryKey);
    for (const key of keys) expect(key[1]).toBe("fixture");
    expect(new Set(keys.map((k) => JSON.stringify(k))).size).toBe(3);
    expect(homeContinueQueryOptions({ ...ctx, scope: "other" }).queryKey).not.toEqual(homeContinueQueryOptions(ctx).queryKey);
    const [cont, news, att] = keys;
    expect(partialMatchKey(cont, [...appInvalidation.homeContinue()])).toBe(true);
    expect(partialMatchKey(news, [...appInvalidation.homeWhatsNew()])).toBe(true);
    expect(partialMatchKey(att, [...appInvalidation.homeAttention()])).toBe(true);
    expect(partialMatchKey(cont, [...appInvalidation.homeAttention()])).toBe(false);
  });

  it.each(factories)("%s GETs its path with the bearer and critical priority", async (_name, factory, path, body) => {
    wire.fetch.mockResolvedValue(new Response(JSON.stringify(body)));
    const signal = new AbortController().signal;
    const out = await factory(ctx).queryFn({ signal });
    const [requested, init, options] = wire.fetch.mock.calls[0];
    expect(requested).toBe(path);
    expect(init).toMatchObject({ headers: { Authorization: "Bearer tok" } });
    expect(options).toMatchObject({ priority: "critical", signal });
    expect(out).toEqual(body);
  });

  it("warms in the background when the sidebar prefetches", () => {
    expect(homeWhatsNewQueryOptions(ctx, "background").meta).toMatchObject({ priority: "background" });
  });

  it("refuses an empty token without a request", async () => {
    await expect(homeAttentionQueryOptions({ ...ctx, getAuthToken: () => null }).queryFn({ signal: new AbortController().signal })).rejects.toThrow();
    expect(wire.fetch).not.toHaveBeenCalled();
  });
});

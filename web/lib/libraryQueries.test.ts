// perf/library-tab-cache — tests for the Library query-key/option builders.
//
// What actually matters here, in order:
//   1. A key never contains the token (`lib/providers.tsx`'s `AppState.token`
//      warning, the root cause the whole PR exists to fix).
//   2. A key IS a function of every filter/page/facet param a list uses,
//      including `graphId` — two different oceans/filters must never share a
//      cache entry.
//   3. The URL's own canonical-form rewrite (`router.replace` dropping a
//      redundant `?ws=all`) must not mint a second key for the same resolved
//      filters — that was the double `GET /v1/library` bug.
//   4. `staleTime`/`refetchOnWindowFocus`/`placeholderData` are set exactly
//      once, here, so every page and the hover-prefetch agree by construction
//      rather than by six independently-typed literals staying in sync.
//   5. Invalidation reaches every param combination for its own list (a
//      PREFIX match) and never a sibling list's — same contract
//      `lib/queryKeys.test.ts` proves for the Reefs half of the hierarchy.

import { describe, expect, it, vi } from "vitest";
import { QueryClient, keepPreviousData } from "@tanstack/react-query";
import {
  LIBRARY_STALE_TIME,
  agentSessionsQueryOptions,
  codingSessionsQueryOptions,
  libraryInvalidation,
  peopleQueryKey,
  peopleQueryOptions,
  prefetchLibraryTab,
  sessionsQueryKey,
  sourceGroupQueryKey,
  sourceGroupQueryOptions,
  sourceGroupsQueryKey,
  sourceGroupsQueryOptions,
  topicsQueryKey,
  topicsQueryOptions,
  type TopicsParams,
} from "./libraryQueries";

const G = "3f346a2a-71ca-4779-9701-baf2e5e7bddd";
const G2 = "3f346a2a-71ca-4779-9701-ffffffffffff";

/** Does invalidating `target` reach `key`? Same helper (and same reasoning)
 *  as `lib/queryKeys.test.ts`'s: exercise the REAL matcher, not a hand-rolled
 *  prefix compare. */
function reaches(target: readonly unknown[], key: readonly unknown[]): boolean {
  const qc = new QueryClient();
  qc.setQueryData(key as unknown[], "cached");
  qc.invalidateQueries({ queryKey: target as unknown[] });
  return qc.getQueryState(key as unknown[])?.isInvalidated === true;
}

const tokenA = () => "token-A";
const tokenB = () => "a-totally-different-token-B";

describe("topicsQueryKey — excludes the token, includes every filter", () => {
  const params: TopicsParams = { graphId: G, entityType: "topic", q: "roadmap", page: 2 };

  it("is identical across two different `getAuthToken` closures", () => {
    const a = topicsQueryOptions({ getAuthToken: tokenA, enabled: true, params });
    const b = topicsQueryOptions({ getAuthToken: tokenB, enabled: true, params });
    expect(a.queryKey).toEqual(b.queryKey);
  });

  it("changes when graphId, entityType, q, or page changes", () => {
    const base = topicsQueryKey(params);
    expect(topicsQueryKey({ ...params, graphId: G2 })).not.toEqual(base);
    expect(topicsQueryKey({ ...params, graphId: null })).not.toEqual(base);
    expect(topicsQueryKey({ ...params, entityType: "person" })).not.toEqual(base);
    expect(topicsQueryKey({ ...params, entityType: null })).not.toEqual(base);
    expect(topicsQueryKey({ ...params, q: "something else" })).not.toEqual(base);
    expect(topicsQueryKey({ ...params, page: 3 })).not.toEqual(base);
  });

  it("the SAME resolved params produce the SAME key regardless of URL normalisation", () => {
    // `/library` (no query params at all) — a fresh, never-normalised URL.
    const freshUrlParams = new URLSearchParams("");
    // `/library?ws=all&kind=all&page=1` — a hypothetical PRE-normalisation
    // URL carrying the default values explicitly (what `router.replace`'s
    // own canonical rewrite immediately collapses back down to the URL
    // above). The page resolves both through the identical `ws === "all" ?
    // null : ws` formula BEFORE building the key — that resolve-before-key
    // step is what makes the two below produce one query, not two.
    const preNormalizationUrlParams = new URLSearchParams("ws=all&kind=all&page=1");

    function resolve(sp: URLSearchParams): TopicsParams {
      const ws = sp.get("ws") ?? "all";
      const kind = sp.get("kind") ?? "all";
      const q = sp.get("q") ?? "";
      const page = Math.max(1, Number(sp.get("page") ?? "1") || 1);
      return {
        graphId: ws === "all" ? null : ws,
        entityType: kind === "all" ? null : (kind as TopicsParams["entityType"]),
        q: q.trim(),
        page,
      };
    }

    expect(topicsQueryKey(resolve(freshUrlParams))).toEqual(topicsQueryKey(resolve(preNormalizationUrlParams)));
  });
});

describe("sourceGroupsQueryKey / sourceGroupQueryKey / sessionsQueryKey / peopleQueryKey — token exclusion + graphId inclusion", () => {
  it("source groups (list): same key across different tokens, different key across oceans", () => {
    const a = sourceGroupsQueryOptions({ getAuthToken: tokenA, enabled: true, graphId: G });
    const b = sourceGroupsQueryOptions({ getAuthToken: tokenB, enabled: true, graphId: G });
    expect(a.queryKey).toEqual(b.queryKey);
    expect(sourceGroupsQueryKey(G)).not.toEqual(sourceGroupsQueryKey(G2));
  });

  it("source group (detail): same key across different tokens; graphId, channelKey AND page are all part of the key", () => {
    const a = sourceGroupQueryOptions({ getAuthToken: tokenA, enabled: true, graphId: G, channelKey: "chanA", page: 1 });
    const b = sourceGroupQueryOptions({ getAuthToken: tokenB, enabled: true, graphId: G, channelKey: "chanA", page: 1 });
    expect(a.queryKey).toEqual(b.queryKey);

    const base = sourceGroupQueryKey(G, "chanA", 1);
    expect(sourceGroupQueryKey(G2, "chanA", 1)).not.toEqual(base);
    expect(sourceGroupQueryKey(G, "chanB", 1)).not.toEqual(base);
    expect(sourceGroupQueryKey(G, "chanA", 2)).not.toEqual(base);
  });

  it("source group (detail): percent-encoding lives in the fetch, not the key — a raw Teams-shaped channelKey round-trips", () => {
    const key = "19:abc@thread.tacv2";
    const opts = sourceGroupQueryOptions({ getAuthToken: tokenA, enabled: true, graphId: G, channelKey: key, page: 1 });
    expect(opts.queryKey).toEqual(sourceGroupQueryKey(G, key, 1));
  });

  it("sessions: kind, graphId AND filters are all part of the key; the token is not", () => {
    const filters = { project: "coralswarm" };
    const a = codingSessionsQueryOptions({ getAuthToken: tokenA, enabled: true, graphId: G, filters });
    const b = codingSessionsQueryOptions({ getAuthToken: tokenB, enabled: true, graphId: G, filters });
    expect(a.queryKey).toEqual(b.queryKey);

    expect(sessionsQueryKey("coding", G, filters)).not.toEqual(sessionsQueryKey("agent", G, filters));
    expect(sessionsQueryKey("coding", G, filters)).not.toEqual(sessionsQueryKey("coding", G2, filters));
    expect(sessionsQueryKey("coding", G, filters)).not.toEqual(sessionsQueryKey("coding", G, {}));
  });

  it("agent sessions: same shape as coding sessions", () => {
    const filters = { platform: "github-actions" };
    const opts = agentSessionsQueryOptions({ getAuthToken: tokenA, enabled: true, graphId: G, filters });
    expect(opts.queryKey).toEqual(sessionsQueryKey("agent", G, filters));
  });

  it("people: keyed on the account scope, not a graph — same key across tokens", () => {
    const a = peopleQueryOptions({ getAuthToken: tokenA, enabled: true, scopeId: "acct-1" });
    const b = peopleQueryOptions({ getAuthToken: tokenB, enabled: true, scopeId: "acct-1" });
    expect(a.queryKey).toEqual(b.queryKey);
    expect(peopleQueryKey("acct-1")).not.toEqual(peopleQueryKey("acct-2"));
  });
});

describe("cache-freshness options — set once, here", () => {
  it("every Library list opts into the 60s staleTime, keepPreviousData, and no refetch-on-focus", () => {
    const built = [
      topicsQueryOptions({ getAuthToken: tokenA, enabled: true, params: { graphId: null, entityType: null, q: "", page: 1 } }),
      sourceGroupsQueryOptions({ getAuthToken: tokenA, enabled: true, graphId: G }),
      sourceGroupQueryOptions({ getAuthToken: tokenA, enabled: true, graphId: G, channelKey: "chanA", page: 1 }),
      codingSessionsQueryOptions({ getAuthToken: tokenA, enabled: true, graphId: G, filters: {} }),
      agentSessionsQueryOptions({ getAuthToken: tokenA, enabled: true, graphId: G, filters: {} }),
      peopleQueryOptions({ getAuthToken: tokenA, enabled: true, scopeId: null }),
    ];
    for (const opts of built) {
      expect(opts.staleTime).toBe(60_000);
      expect(opts.staleTime).toBe(LIBRARY_STALE_TIME);
      expect(opts.refetchOnWindowFocus).toBe(false);
      expect(opts.placeholderData).toBe(keepPreviousData);
    }
  });

  it("enabled reflects the caller's gate (authReady / a resolved graphId), not a constant", () => {
    expect(topicsQueryOptions({ getAuthToken: tokenA, enabled: false, params: { graphId: null, entityType: null, q: "", page: 1 } }).enabled).toBe(false);
    expect(sourceGroupsQueryOptions({ getAuthToken: tokenA, enabled: true, graphId: undefined }).enabled).toBe(false);
    expect(sourceGroupQueryOptions({ getAuthToken: tokenA, enabled: true, graphId: undefined, channelKey: "chanA", page: 1 }).enabled).toBe(false);
    expect(sourceGroupQueryOptions({ getAuthToken: tokenA, enabled: true, graphId: G, channelKey: "", page: 1 }).enabled).toBe(false);
    expect(codingSessionsQueryOptions({ getAuthToken: tokenA, enabled: true, graphId: G, filters: {} }).enabled).toBe(true);
  });
});

describe("libraryInvalidation — prefix reaches every descendant, never a sibling", () => {
  it("topics: the broad prefix reaches every graph/filter/page variant", () => {
    expect(reaches(libraryInvalidation.topics(), topicsQueryKey({ graphId: G, entityType: null, q: "", page: 1 }))).toBe(true);
    expect(reaches(libraryInvalidation.topics(), topicsQueryKey({ graphId: G2, entityType: "person", q: "x", page: 4 }))).toBe(true);
  });

  it("source groups (list): reaches only its own graph", () => {
    expect(reaches(libraryInvalidation.sourceGroups(G), sourceGroupsQueryKey(G))).toBe(true);
    expect(reaches(libraryInvalidation.sourceGroups(G), sourceGroupsQueryKey(G2))).toBe(false);
  });

  it("source group (detail): reaches every channelKey/page for its own graph, never a sibling graph", () => {
    const target = libraryInvalidation.sourceGroup(G);
    expect(reaches(target, sourceGroupQueryKey(G, "chanA", 1))).toBe(true);
    expect(reaches(target, sourceGroupQueryKey(G, "chanA", 2))).toBe(true);
    expect(reaches(target, sourceGroupQueryKey(G, "chanB", 1))).toBe(true);
    expect(reaches(target, sourceGroupQueryKey(G2, "chanA", 1))).toBe(false);
  });

  it("sessions: reaches its own kind+graph across every filter combination, never a sibling kind or ocean", () => {
    const target = libraryInvalidation.sessions("coding", G);
    expect(reaches(target, sessionsQueryKey("coding", G, {}))).toBe(true);
    expect(reaches(target, sessionsQueryKey("coding", G, { project: "x" }))).toBe(true);
    expect(reaches(target, sessionsQueryKey("agent", G, {}))).toBe(false);
    expect(reaches(target, sessionsQueryKey("coding", G2, {}))).toBe(false);
  });

  it("people: reaches every scope (account-wide, not per-graph)", () => {
    expect(reaches(libraryInvalidation.people(), peopleQueryKey("acct-1"))).toBe(true);
    expect(reaches(libraryInvalidation.people(), peopleQueryKey(null))).toBe(true);
  });
});

describe("prefetchLibraryTab — hover prefetch uses the SAME key/options the page uses", () => {
  it("does nothing until auth is ready", () => {
    const qc = new QueryClient();
    const spy = vi.spyOn(qc, "prefetchQuery").mockResolvedValue(undefined);
    prefetchLibraryTab(qc, "topics", { getAuthToken: tokenA, authReady: false, graphId: null, accountId: null });
    expect(spy).not.toHaveBeenCalled();
  });

  it("topics: prefetches the default (unfiltered, page 1) key", () => {
    const qc = new QueryClient();
    const spy = vi.spyOn(qc, "prefetchQuery").mockResolvedValue(undefined);
    prefetchLibraryTab(qc, "topics", { getAuthToken: tokenA, authReady: true, graphId: null, accountId: null });
    expect(spy).toHaveBeenCalledTimes(1);
    const opts = spy.mock.calls[0][0];
    expect(opts.queryKey).toEqual(topicsQueryKey({ graphId: null, entityType: null, q: "", page: 1 }));
    expect(opts.staleTime).toBe(LIBRARY_STALE_TIME);
  });

  it("ocean-scoped tabs (sources/sessions/agent-sessions) no-op without a resolved graphId", () => {
    const qc = new QueryClient();
    const spy = vi.spyOn(qc, "prefetchQuery").mockResolvedValue(undefined);
    for (const tab of ["sources", "sessions", "agent-sessions"] as const) {
      prefetchLibraryTab(qc, tab, { getAuthToken: tokenA, authReady: true, graphId: null, accountId: null });
    }
    expect(spy).not.toHaveBeenCalled();
  });

  it("sources: prefetches the current ocean's GROUPS LIST key once a graphId resolves (never the per-group detail — the pill has no channelKey to prefetch)", () => {
    const qc = new QueryClient();
    const spy = vi.spyOn(qc, "prefetchQuery").mockResolvedValue(undefined);
    prefetchLibraryTab(qc, "sources", { getAuthToken: tokenA, authReady: true, graphId: G, accountId: null });
    expect(spy).toHaveBeenCalledTimes(1);
    expect(spy.mock.calls[0][0].queryKey).toEqual(sourceGroupsQueryKey(G));
  });

  it("people: prefetches the account scope, independent of graphId", () => {
    const qc = new QueryClient();
    const spy = vi.spyOn(qc, "prefetchQuery").mockResolvedValue(undefined);
    prefetchLibraryTab(qc, "people", { getAuthToken: tokenA, authReady: true, graphId: null, accountId: "acct-1" });
    expect(spy).toHaveBeenCalledTimes(1);
    expect(spy.mock.calls[0][0].queryKey).toEqual(peopleQueryKey("acct-1"));
  });
});

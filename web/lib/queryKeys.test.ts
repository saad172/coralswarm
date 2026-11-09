// W4 — the cache-invalidation contract, tested (plan §6.4).
//
// A read cache introduces a staleness class this codebase cannot currently
// have, because today every screen refetches from scratch. That makes
// invalidation the one way W4 can make CORRECTNESS worse, not just
// performance — and the original plan omitted it entirely.
//
// The failure mode is what makes it worth a test file of its own: a missed
// invalidation is SILENT. A rename that appears to do nothing is
// indistinguishable, to a user, from a rename that failed. Nothing throws,
// nothing logs, and the bug reproduces only after a specific navigation.

import { describe, expect, it } from "vitest";
import { QueryClient } from "@tanstack/react-query";
import { keys } from "./queryKeys";
import { makeQueryClient, STALE } from "./queryClient";

const G = "3f346a2a-71ca-4779-9701-baf2e5e7bddd";
const G2 = "3f346a2a-71ca-4779-9701-ffffffffffff";

/** Does invalidating `target` reach `key`? Uses the real matcher, not a
 *  hand-rolled prefix compare, so the test exercises the same logic the app
 *  does. */
function reaches(target: readonly unknown[], key: readonly unknown[]): boolean {
  const qc = new QueryClient();
  qc.setQueryData(key, "cached");
  qc.invalidateQueries({ queryKey: target as unknown[] });
  return qc.getQueryState(key as unknown[])?.isInvalidated === true;
}

describe("key hierarchy — a graph prefix must reach every descendant", () => {
  it("invalidating an ocean reaches its reef list, atoms, edges, facets, sources and stats", () => {
    // The whole reason mutations target a PREFIX: if they had to enumerate
    // keys, they would miss the ones added after they were written.
    for (const key of [
      keys.reefsAll(G),
      keys.reefsPage(G, 2, 100),
      keys.atomsAll(G),
      keys.edges(G),
      keys.edgeFacets(G),
      keys.sources(G),
      keys.stats(G),
    ]) {
      expect(reaches(keys.graph(G), key)).toBe(true);
    }
  });

  it("a narrower prefix does NOT reach a sibling", () => {
    // Over-invalidation is a real cost too — invalidating an ocean on every
    // keystroke would refetch a 20,000-edge drain. The hierarchy has to
    // discriminate, not just match everything.
    expect(reaches(keys.reefsAll(G), keys.atomsAll(G))).toBe(false);
    expect(reaches(keys.stats(G), keys.sources(G))).toBe(false);
  });

  it("one ocean's invalidation never touches another's", () => {
    // The multi-tenant version of the same bug: a rename in a personal ocean
    // must not refetch an org ocean the user also has open.
    expect(reaches(keys.graph(G), keys.reefsAll(G2))).toBe(false);
    expect(reaches(keys.graph(G2), keys.reefsAll(G))).toBe(false);
  });
});

describe("client defaults — the two that can bite", () => {
  it("ordinary reads stay fresh for a navigation round trip; live data opts out", () => {
    // A non-zero default is what makes returning to a screen free. It is safe
    // only because mutations invalidate their keys (lib/appQueries.ts's
    // invalidationFor); a
    // resource that must be live on every mount opts into STALE.MUTABLE.
    expect(STALE.READ).toBe(30_000);
    expect(STALE.MUTABLE).toBe(0);
    const qc = makeQueryClient();
    expect(qc.getDefaultOptions().queries?.staleTime).toBe(STALE.READ);
  });

  it("retry is delegated entirely to the transport layer", () => {
    // Two independent retry layers MULTIPLY: 3 transport attempts × 3 query
    // attempts is 9 requests for one user action, aimed at a service that by
    // then is probably already struggling. lib/transport.ts is the single
    // retry authority, with jitter and Retry-After.
    const qc = makeQueryClient();
    expect(qc.getDefaultOptions().queries?.retry).toBe(false);
    expect(qc.getDefaultOptions().mutations?.retry).toBe(false);
  });

  it("does not refetch every query on window focus", () => {
    // A screen holding a dozen queries would issue a dozen requests every
    // time the user alt-tabs back — the exact cost profile this plan removes.
    const qc = makeQueryClient();
    expect(qc.getDefaultOptions().queries?.refetchOnWindowFocus).toBe(false);
  });
});

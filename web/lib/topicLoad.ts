// `/topics`'s production load sequence, extracted out of
// `app/(app)/topics/page.tsx`'s `load()` callback and its related-reefs
// effect into named, importable functions.
//
// WHY THIS FILE EXISTS (CodeRabbit finding on PR #859,
// web/lib/requestBudget.test.ts:294-298): the request-budget test that pins
// this page's 3-flat-request load shape used to call `api.reefs.get` /
// `.atoms` / `.list` directly, by hand — a hand-copied reproduction of what
// the page does, not an invocation OF what the page does. That only proves
// the reproduction makes 3 requests; if the page's own `load()` regressed
// back to draining the whole ocean via `listAll` tomorrow, the test would
// still pass, because nothing it calls is the page's own code. This file is
// the fix: the page and the test both call THESE functions, so a regression
// in the real load path is a regression the test actually exercises.
//
// Split into two (not one function doing everything) because the page
// itself splits this across two effects for a real reason: `loadTopicReef`
// runs once per `nodeId` (the page's `load()`), while `loadRelatedReefs`
// re-runs whenever the resolved reef's `graph_id`/`type` change — including
// after a re-type via curation, which `loadTopicReef` alone would miss since
// nothing re-invokes it on that path. Folding both into one function would
// have made that re-fetch-on-retype behavior impossible without either
// double-fetching on initial mount or reproducing the split some other way.

import { api, type Reef, type ReefAtom, type ReefDetail, type ReefType } from "./api";

/** Resolves the reef this page was asked to show, by id — never from
 *  ambient ocean state (see topics/page.tsx's own file header) — plus its
 *  own atoms, fetched in parallel since `atoms` needs only the node id, not
 *  the ocean `reef.graph_id` resolves. 2 requests, flat regardless of ocean
 *  size. A failure here is fatal to the page (propagates to the caller). */
export async function loadTopicReef(
  token: string,
  nodeId: string,
): Promise<{ reef: ReefDetail; atoms: ReefAtom[] }> {
  const [reef, atoms] = await Promise.all([api.reefs.get(token, nodeId), api.reefs.atoms(token, nodeId)]);
  return { reef, atoms };
}

/** Up to 5 same-type reefs in `graphId` (the reef's OWN ocean) for the
 *  always-visible "related topics" rail — bounded, 1 request, replacing the
 *  ocean-wide `listAll` drain this page used to run on every load just to
 *  compute this. 5 requested, not 4: the subject reef itself is usually
 *  inside its own same-type page and gets filtered back out by the caller,
 *  so 5 keeps a full 4 after that filter.
 *
 *  Non-fatal on failure — degrades to an empty related list rather than
 *  losing the reef/atoms the caller already has, matching the page's
 *  original per-section error isolation. */
export async function loadRelatedReefs(token: string, graphId: string, type: ReefType): Promise<Reef[]> {
  try {
    const { data } = await api.reefs.list(token, graphId, type, 1, 5);
    return data;
  } catch {
    return [];
  }
}

/* A monotonic request-generation guard — the fix for "a stale response
   overwrote a newer one".

   The bug it exists to prevent looks harmless in every diff it appears in:

     const page = await api.sessions.list(token, { graphId: currentGraph.id });
     setSessions(page.sessions);

   Switch oceans while that request is in flight and two requests are now
   racing. Responses do NOT come back in the order they were sent — a slow
   first one lands after a fast second one, writes the previous ocean's rows
   into state, and the screen shows another workspace's sessions under this
   workspace's heading, with no error and no spinner to hint at it. `loading`
   suffers the same way: the stale request's `finally` clears the flag the
   NEWER request just set, so the list renders as "done" while its real
   fetch is still running.

   The pattern is already spelled out inline in this repo — see
   `components/connectors/SlackChannelActions.tsx` (`genRef`),
   and `components/today/HourDrilldown.tsx` (`requestIdRef`). It lives here as a function for one
   reason: this repo has no component-test harness (vitest `include` is
   `.test.ts`, node env, no renderer), so a guard written inline in a page
   cannot be shown to actually drop a stale response. This one can, and is —
   `requestGuard.test.ts` resolves two requests out of order and asserts the
   older loser, against the same code the pages run.

   Deliberately NOT a React hook: no `useRef`, no import from `react`. A hook
   would drag the untestable half back in. Pages hold the guard in a `useRef`
   themselves, which is where the React-ness belongs. */

/** A guard shared by every request that writes the same piece of state. */
export interface RequestGuard {
  /** Start a request, superseding every request started before it.
   *
   *  Returns the predicate to check AFTER each `await` and BEFORE every state
   *  write (including in `finally`): `true` only while this request is still
   *  the newest one started on this guard. */
  begin(): () => boolean;
}

/** Create a [`RequestGuard`]. One per piece of state, shared by every code
 *  path that writes it — a `loadMore` that appends to the same list must
 *  share the guard with the `load` that replaces it, or an ocean switch
 *  mid-`loadMore` appends the old ocean's rows to the new ocean's list. */
export function createRequestGuard(): RequestGuard {
  let newest = 0;
  return {
    begin() {
      const mine = ++newest;
      return () => newest === mine;
    },
  };
}

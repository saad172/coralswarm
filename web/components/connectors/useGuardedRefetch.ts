"use client";

import type { MutableRefObject } from "react";
import { useReturnRefetch, type ReturnRefetch } from "@/lib/useReturnRefetch";

/* Focus/visibility refetch with a rate floor — shared by the two Slack
 * surfaces that each cost a LIVE `conversations.list` server-side.
 *
 * Extracted from SlackOnboardingChecklist's `useSlackOnboarding`, which grew
 * this carefully and alone. The channel-management page then shipped a bare
 * `window.addEventListener("focus", load)`: no floor, no in-flight guard, no
 * `visibilitychange`, so alt-tabbing repeatedly turned into a request storm
 * against Slack's rate limit — the exact failure the checklist's version
 * exists to prevent, reintroduced one page over. Two copies of this logic
 * would have drifted the same way again, so there is one.
 *
 * What this owns: the Slack floor. The event wiring and trailing timer are
 * the shared `lib/useReturnRefetch.ts`. What it does NOT own: single-flight
 * (the caller's `load` must return early while a request is in flight — the
 * floor bounds RATE, not concurrency), account switching, or any state. Callers keep those, because both call sites do
 * them differently and neither difference is accidental. */

// A refetch re-runs a live Slack `conversations.list` server-side, expensive
// enough to need a floor independent of anything React does.
//
// A suppressed refetch DEFERS to this boundary rather than dropping (see
// lib/useReturnRefetch.ts), so this only controls request RATE, never whether
// the user's update arrives at all — worst case they see it this long after coming back. That
// is why it can be generous; 15s is comfortably below "did the app notice I
// came back", and there is no fleet-wide polling pressure to trade against:
// this fires on a real user focus/visibility event, once per client, never on
// a timer.
export const MIN_REFETCH_INTERVAL_MS = 15_000;

export type GuardedRefetch = ReturnRefetch;

/** Refetch on focus/visibility, at most once per [`MIN_REFETCH_INTERVAL_MS`].
 *
 *  `lastFetchAtRef` is the caller's — `load` must stamp it whenever it
 *  actually issues a request, so the floor is measured against real traffic
 *  rather than against when this hook last considered firing. Sharing the ref
 *  (rather than this hook stamping it) is what lets a manual Refresh button
 *  and a mount fetch also push the floor out.
 *
 *  The wiring, floor and trailing timer live in `lib/useReturnRefetch.ts`.
 *  This keeps window `focus` on (a side-by-side window regaining focus is a
 *  real "I came back from Slack" signal here) and `online` off, as before; the
 *  shared hook coalesces the focus + visibilitychange pair of one tab switch
 *  into a single call rather than an immediate call plus a trailing one. */
export function useGuardedRefetch(
  load: () => void,
  lastFetchAtRef: MutableRefObject<number>,
): GuardedRefetch {
  return useReturnRefetch(load, {
    floorMs: MIN_REFETCH_INTERVAL_MS,
    lastRunAtRef: lastFetchAtRef,
    focus: true,
    online: false,
  });
}

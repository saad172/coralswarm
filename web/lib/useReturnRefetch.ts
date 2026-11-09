"use client";

import { useCallback, useEffect, useRef, type MutableRefObject } from "react";

/* Revalidate when the user comes back to the tab — once, rate-floored, and
 * never on top of a request already in flight.
 *
 * Extracted from `components/connectors/useGuardedRefetch.ts` (which now
 * wraps this) so Library reads, open chats and Today share one copy instead
 * of each hand-rolling `focus` + `visibilitychange` + `online` listeners. The
 * hand-rolled copies are what made every data page blank when the user
 * clicked away and back: `focus` and `visibilitychange` both fire on a single
 * return, each listener started its own read, and each read cleared the
 * rendered data first.
 *
 * What this owns: the event wiring, the floor, the trailing timer, the
 * optional visible-only interval. What it does NOT own: what a refetch does
 * to rendered data. Callers must revalidate in the background — keep what is
 * on screen until fresh data or an explicit denial arrives. */

/** Default floor for app reads. Short enough that coming back feels live,
 *  long enough to absorb a burst of tab switches. */
export const RETURN_REFETCH_FLOOR_MS = 5_000;

// Events this close to the previous run are the same return (a tab switch
// fires `visibilitychange` and then `focus`; a reconnect can land beside
// either). They coalesce onto that run instead of arming a trailing one.
const SAME_RETURN_MS = 1_000;

export interface ReturnRefetchOptions {
  /** Minimum gap between runs. A return inside it defers to the boundary. */
  floorMs?: number;
  /** Caller-owned "last request issued at" stamp. Pass it when the caller
   *  also fetches on mount or from a Refresh button, so those push the floor
   *  out too; the caller must stamp it whenever it actually issues a request.
   *  When omitted, this hook stamps its own runs. */
  lastRunAtRef?: MutableRefObject<number>;
  /** True while the caller has a request in flight. A return then is
   *  dropped: the running request is already the fresh read. */
  inFlight?: () => boolean;
  /** Also listen to window `focus`. Off by default: `visibilitychange` is the
   *  return signal, and listening to both is the double-fire this exists to
   *  remove. Kept for callers whose floor deliberately serves window focus. */
  focus?: boolean;
  /** Revalidate on `online`. On by default. */
  online?: boolean;
  /** Also revalidate on this interval, only while the tab is visible. */
  intervalMs?: number;
  /** Off detaches every listener and cancels a pending trailing run. */
  enabled?: boolean;
}

export interface ReturnRefetch {
  /** Cancel a deferred trailing run. Call it when invalidating state out of
   *  band (an account switch), so a timer armed for the outgoing identity
   *  cannot fire into the incoming one. */
  clearPendingRefetchTimer: () => void;
}

/** Run `revalidate` when the tab returns to view, at most once per floor.
 *
 *  Re-subscribes when `revalidate`'s identity changes (and cancels any
 *  trailing run armed for the previous closure), so pass a stable callback
 *  whose identity changes only when its target does. */
export function useReturnRefetch(
  revalidate: () => void,
  {
    floorMs = RETURN_REFETCH_FLOOR_MS,
    lastRunAtRef,
    inFlight,
    focus = false,
    online = true,
    intervalMs,
    enabled = true,
  }: ReturnRefetchOptions = {},
): ReturnRefetch {
  const ownLastRunAt = useRef(0);
  const lastRunAt = lastRunAtRef ?? ownLastRunAt;
  const stampsOwnRuns = !lastRunAtRef;
  const inFlightRef = useRef(inFlight);
  inFlightRef.current = inFlight;
  // At most one deferred run at a time, so re-arriving events coalesce onto
  // it instead of each queuing their own.
  const pendingRefetchTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  const clearPendingRefetchTimer = useCallback(() => {
    if (pendingRefetchTimerRef.current !== null) {
      clearTimeout(pendingRefetchTimerRef.current);
      pendingRefetchTimerRef.current = null;
    }
  }, []);

  // Why DEFER rather than drop inside the floor: a user who switches away,
  // changes something and comes straight back arrives inside the floor.
  // Dropping there leaves nothing to re-trigger the read, so the page would
  // stay stale until some unrelated later return. Scheduling at
  // `floorMs - elapsed` still delivers it, a few seconds later.
  useEffect(() => {
    if (!enabled) return;
    const run = () => {
      if (stampsOwnRuns) lastRunAt.current = Date.now();
      revalidate();
    };
    const runTrailingRefetch = () => {
      pendingRefetchTimerRef.current = null;
      // The tab may have gone hidden again while this waited. That is not a
      // return; the next event re-evaluates from scratch.
      if (document.visibilityState !== "visible" || inFlightRef.current?.()) return;
      run();
    };
    const maybeRefetch = () => {
      if (document.visibilityState !== "visible") return;
      if (pendingRefetchTimerRef.current !== null) return;
      if (inFlightRef.current?.()) return;
      const elapsed = Date.now() - lastRunAt.current;
      if (elapsed < SAME_RETURN_MS) return;
      if (elapsed >= floorMs) {
        run();
        return;
      }
      pendingRefetchTimerRef.current = setTimeout(runTrailingRefetch, floorMs - elapsed);
    };

    document.addEventListener("visibilitychange", maybeRefetch);
    if (focus) window.addEventListener("focus", maybeRefetch);
    if (online) window.addEventListener("online", maybeRefetch);
    const timer = intervalMs ? window.setInterval(maybeRefetch, intervalMs) : null;
    return () => {
      document.removeEventListener("visibilitychange", maybeRefetch);
      if (focus) window.removeEventListener("focus", maybeRefetch);
      if (online) window.removeEventListener("online", maybeRefetch);
      if (timer !== null) window.clearInterval(timer);
      // Runs on every re-subscribe AND on unmount, so a trailing timer never
      // fires against an unmounted component or a superseded closure.
      clearPendingRefetchTimer();
    };
  }, [revalidate, enabled, floorMs, focus, online, intervalMs, lastRunAt, stampsOwnRuns, clearPendingRefetchTimer]);

  return { clearPendingRefetchTimer };
}

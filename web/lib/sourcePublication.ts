import type { SourceConnection } from "./api";
export function connectionState(c: SourceConnection): string {
  if (c.paused) return "Paused";
  if (c.error_message || c.processing?.state === "attention") return "Needs attention";
  if (c.processing?.state === "processing") return "Publishing and processing";
  if (c.has_pending_evidence) return "Catching up";
  if (!c.last_checked_at) return "Waiting for first check";
  return c.mode === "live" ? "Up to date" : "Published";
}

// ── Poll policy ──────────────────────────────────────────────────────────────
// `SourceConnections` used to re-arm a 5s poll from a `finally` block, forever,
// for every open tab — success or failure, work in flight or long finished
// (CodeRabbit review of #871). The delay decision lives here as a pure function
// so the stop conditions are unit-testable without a render harness, matching
// this project's convention (see `promoteJob.ts`/`useJobPoll.ts`).

/** Steady cadence while a publication is actually moving. */
export const CONNECTION_POLL_MS = 5_000;
/** Cadence for a connected ("live") source that is idle right now: the server
 *  sweeps each connection about once a minute, so nothing is lost by asking
 *  less often than that. */
export const CONNECTION_IDLE_POLL_MS = 30_000;
/** Ceiling on the failure backoff. */
export const CONNECTION_POLL_MAX_BACKOFF_MS = 60_000;
/** Consecutive failures after which the poll gives up entirely rather than
 *  retrying a dead token / down API until the tab is closed. */
export const CONNECTION_POLL_MAX_FAILURES = 6;

/** What the last successful reading says about whether anything can still
 *  change on its own:
 *  - `working`  — a job is running, evidence is queued, or the first sweep
 *                 hasn't happened yet.
 *  - `watching` — nothing in flight, but a connected source can be handed new
 *                 evidence by the server at any time.
 *  - `settled`  — every connection is finished, paused, or waiting on a person
 *                 (a retry, a resume). Nothing changes without a user action,
 *                 and every user action here bumps the poll's revision. */
export type PollActivity = "working" | "watching" | "settled";

/** True while this connection can still change without anyone touching it.
 *  `current_job_id` is deliberately NOT a signal: the backend leaves it set
 *  after the job reaches `ready`, so it never goes back to null. */
export function isConnectionMoving(c: SourceConnection): boolean {
  if (c.paused) return false;
  if (c.error_message || c.processing?.state === "attention") return false;
  return (
    c.has_pending_evidence || c.processing?.state === "processing" || c.last_checked_at === null
  );
}

export function pollActivity(rows: SourceConnection[]): PollActivity {
  if (rows.some(isConnectionMoving)) return "working";
  const watching = rows.some(
    (c) =>
      c.mode === "live" &&
      !c.paused &&
      !c.error_message &&
      c.processing?.state !== "attention",
  );
  return watching ? "watching" : "settled";
}

/** The delay before the next poll, or `null` to stop polling.
 *
 *  Failures dominate: consecutive failures back off 5s → 10s → 20s → 40s → 60s
 *  and then stop, so an expired token or a 5xx costs a bounded number of
 *  requests instead of 12 per minute for as long as the tab stays open. With
 *  no failures the cadence follows the work, and `settled` stops outright. */
export function nextConnectionPollDelayMs(activity: PollActivity, failures: number): number | null {
  if (failures >= CONNECTION_POLL_MAX_FAILURES) return null;
  if (failures > 0) {
    return Math.min(CONNECTION_POLL_MS * 2 ** (failures - 1), CONNECTION_POLL_MAX_BACKOFF_MS);
  }
  if (activity === "settled") return null;
  return activity === "working" ? CONNECTION_POLL_MS : CONNECTION_IDLE_POLL_MS;
}

// ── Poll lifecycle ───────────────────────────────────────────────────────────
// The loop is in exactly ONE of these at a time. The first pass at this fix
// tracked all of them with a single `parked` boolean, which CodeRabbit caught
// on review of #873: "hidden" and "terminal" became indistinguishable, so a
// tab hidden with a timer still armed never refreshed on the way back (the
// timer was left running, and browsers throttle background timers to about
// once a minute), while a SETTLED loop — terminal by definition — restarted on
// every tab focus. Both are transitions, so they live here as pure functions
// rather than as flags inside the effect.
//
//   running  — a timer is pending or a request is in flight.
//   hidden   — suspended because this tab is in the background. The only
//              state that visibility restoration brings back.
//   settled  — terminal. Nothing changes without a user action, and every
//              user action in this panel re-runs the effect that owns the
//              loop. A tab focus is not one of those actions.
//   gaveUp   — terminal after consecutive failures. Recovered only through
//              the explicit "Check again" control.
export type PollMode = "running" | "hidden" | "settled" | "gaveUp";

/** Where the loop lands once a poll has completed and reported what it read. */
export function pollModeAfter(
  activity: PollActivity,
  failures: number,
  isHidden: boolean,
): { mode: PollMode; delayMs: number | null } {
  const delayMs = nextConnectionPollDelayMs(activity, failures);
  // Terminal beats hidden: a finished (or given-up) poll has nothing to come
  // back to, so it must not be resurrected by a tab focus.
  if (delayMs === null) return { mode: failures > 0 ? "gaveUp" : "settled", delayMs: null };
  if (isHidden) return { mode: "hidden", delayMs: null };
  return { mode: "running", delayMs };
}

/** `visibilitychange` → hidden. Only a running loop suspends, and it hands
 *  back `clearTimer` because leaving a timer armed in a background tab is
 *  what made the tab look stale on the way back. */
export function onTabHidden(mode: PollMode): { mode: PollMode; clearTimer: boolean } {
  if (mode !== "running") return { mode, clearTimer: false };
  return { mode: "hidden", clearTimer: true };
}

/** `visibilitychange` → visible. Restores polling ONLY from a hidden
 *  suspension; `settled` and `gaveUp` stay exactly where they are. */
export function onTabVisible(mode: PollMode): { mode: PollMode; pollNow: boolean } {
  if (mode !== "hidden") return { mode, pollNow: false };
  return { mode: "running", pollNow: true };
}

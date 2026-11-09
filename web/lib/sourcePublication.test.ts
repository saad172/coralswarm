import { describe, it, expect } from "vitest";
import {
  CONNECTION_IDLE_POLL_MS,
  CONNECTION_POLL_MAX_BACKOFF_MS,
  CONNECTION_POLL_MAX_FAILURES,
  CONNECTION_POLL_MS,
  connectionState,
  isConnectionMoving,
  nextConnectionPollDelayMs,
  onTabHidden,
  onTabVisible,
  pollActivity,
  pollModeAfter,
  type PollActivity,
  type PollMode,
} from "./sourcePublication";
import type { SourceConnection } from "./api";
const sample: SourceConnection = {
  id: "c",
  source_graph_id: "s",
  target_graph_id: "d",
  label: "Channel",
  mode: "live",
  paused: false,
  source_type: "slack",
  source_id: "C1/",
  current_job_id: "j",
  last_checked_at: "2026-09-10T00:00:00Z",
  error_message: null,
  has_pending_evidence: false,
  processing: { state: "ready", stages: [] },
};
describe("source publication status", () => {
  it("does not say up to date while another evidence batch is waiting", () => {
    expect(connectionState({ ...sample, has_pending_evidence: true })).toBe("Catching up");
    expect(connectionState(sample)).toBe("Up to date");
  });
  it("keeps failures and paused consent distinguishable from completion", () => {
    expect(connectionState({ ...sample, processing: { state: "attention", stages: [] } })).toBe(
      "Needs attention",
    );
    expect(connectionState({ ...sample, paused: true })).toBe("Paused");
    expect(connectionState({ ...sample, mode: "once" })).toBe("Published");
  });
});

// ── Poll policy (CodeRabbit review of #871) ─────────────────────────────────
// The old loop re-armed a fixed 5s timer from a `finally` block for every open
// tab, forever: 12 requests/minute whether the token had expired, the API was
// 5xx-ing, or the publication had finished hours ago. These cover the two
// things that has to stop doing.

const moving: SourceConnection = { ...sample, processing: { state: "processing", stages: [] } };
const doneOnce: SourceConnection = { ...sample, mode: "once" };

describe("isConnectionMoving", () => {
  it("is true only while something changes without a person touching it", () => {
    expect(isConnectionMoving(moving)).toBe(true);
    expect(isConnectionMoving({ ...sample, has_pending_evidence: true })).toBe(true);
    // Created, never swept: the UI would otherwise freeze on "Waiting for
    // first check" because it stopped polling before the first reading.
    expect(isConnectionMoving({ ...sample, last_checked_at: null })).toBe(true);
  });

  it("is false for anything waiting on a person", () => {
    // Each of these resumes through a user action, and every user action in
    // this panel bumps the poll's revision — so waiting on a timer is pure
    // cost.
    expect(isConnectionMoving({ ...moving, paused: true })).toBe(false);
    expect(isConnectionMoving({ ...sample, processing: { state: "attention", stages: [] } })).toBe(
      false,
    );
    expect(isConnectionMoving({ ...sample, error_message: "nope" })).toBe(false);
    expect(isConnectionMoving(doneOnce)).toBe(false);
  });
});

describe("pollActivity", () => {
  it("separates work in flight from a connected source that is merely idle", () => {
    expect(pollActivity([moving, doneOnce])).toBe("working");
    expect(pollActivity([sample])).toBe("watching");
    expect(pollActivity([doneOnce])).toBe("settled");
  });

  it("a finished one-time publication settles, and an empty page settles", () => {
    expect(pollActivity([doneOnce, { ...doneOnce, id: "c2", paused: true }])).toBe("settled");
    expect(pollActivity([])).toBe("settled");
  });

  it("a paused or broken live connection does not hold the page open", () => {
    expect(pollActivity([{ ...sample, paused: true }])).toBe("settled");
    expect(pollActivity([{ ...sample, error_message: "nope" }])).toBe("settled");
  });
});

describe("nextConnectionPollDelayMs", () => {
  it("follows the work while everything is healthy", () => {
    expect(nextConnectionPollDelayMs("working", 0)).toBe(CONNECTION_POLL_MS);
    expect(nextConnectionPollDelayMs("watching", 0)).toBe(CONNECTION_IDLE_POLL_MS);
  });

  it("STOPS once nothing can change on its own", () => {
    expect(nextConnectionPollDelayMs("settled", 0)).toBeNull();
  });

  it("backs off on consecutive failures and then gives up", () => {
    const schedule = [1, 2, 3, 4, 5, 6].map((f) => nextConnectionPollDelayMs("working", f));
    expect(schedule).toEqual([5_000, 10_000, 20_000, 40_000, CONNECTION_POLL_MAX_BACKOFF_MS, null]);
    expect(CONNECTION_POLL_MAX_FAILURES).toBe(6);
    // Failures dominate the activity: a "working" page that cannot be read is
    // not a page worth asking twelve times a minute.
    expect(nextConnectionPollDelayMs("working", 7)).toBeNull();
    expect(nextConnectionPollDelayMs("watching", 2)).toBe(10_000);
  });
});

/** Runs the same decision loop the component's effect runs, on a fake clock:
 *  one request per reading, failures counted consecutively, stop when the
 *  policy returns null. Returns how many requests the page would make. */
function drive(readings: (SourceConnection[] | "error")[]) {
  let failures = 0;
  let activity: PollActivity = "working";
  let requests = 0;
  let elapsed = 0;
  const delays: (number | null)[] = [];
  // Generous ceiling: a policy that never stops trips this instead of hanging.
  for (let i = 0; i < 500; i++) {
    const reading = readings[Math.min(i, readings.length - 1)];
    requests += 1;
    if (reading === "error") failures += 1;
    else {
      failures = 0;
      activity = pollActivity(reading);
    }
    const delay = nextConnectionPollDelayMs(activity, failures);
    delays.push(delay);
    if (delay === null) return { requests, elapsed, delays, stopped: true };
    elapsed += delay;
  }
  return { requests, elapsed, delays, stopped: false };
}

describe("the poll terminates", () => {
  it("stops after the publication finishes instead of polling the tab's lifetime", () => {
    const run = drive([[moving], [moving], [doneOnce]]);
    expect(run.stopped).toBe(true);
    expect(run.requests).toBe(3);
    expect(run.delays).toEqual([CONNECTION_POLL_MS, CONNECTION_POLL_MS, null]);
  });

  it("stops on a page that was already finished when it loaded", () => {
    expect(drive([[doneOnce]])).toMatchObject({ stopped: true, requests: 1 });
  });

  it("gives up on a persistently failing API after a bounded number of requests", () => {
    const run = drive(["error"]);
    expect(run.stopped).toBe(true);
    expect(run.requests).toBe(CONNECTION_POLL_MAX_FAILURES);
    // ~2 minutes of retrying, not 720 requests an hour.
    expect(run.elapsed).toBe(135_000);
  });

  it("resets the backoff on the first success after a blip", () => {
    const run = drive(["error", "error", [moving], [doneOnce]]);
    expect(run.delays).toEqual([5_000, 10_000, CONNECTION_POLL_MS, null]);
  });

  it("keeps watching a connected source, but only twice a minute", () => {
    const run = drive([[sample]]);
    expect(run.stopped).toBe(false);
    expect(run.delays.every((d) => d === CONNECTION_IDLE_POLL_MS)).toBe(true);
  });
});

// ── Poll lifecycle (CodeRabbit review of #873) ──────────────────────────────
// The first pass tracked "hidden", "settled" and "gave up" with one `parked`
// boolean. Two bugs fell out of the conflation, and both are covered below:
//   1. Hiding a tab left its pending timer armed, and coming back before that
//      timer fired did nothing — so the page sat stale for as long as the
//      browser's background throttling delayed it.
//   2. A settled (terminal) poll set the same flag, so a tab focus restarted
//      a loop that `nextConnectionPollDelayMs` had already stopped for good.

describe("pollModeAfter", () => {
  it("routes a healthy reading to a running loop with its cadence", () => {
    expect(pollModeAfter("working", 0, false)).toEqual({ mode: "running", delayMs: 5_000 });
    expect(pollModeAfter("watching", 0, false)).toEqual({
      mode: "running",
      delayMs: CONNECTION_IDLE_POLL_MS,
    });
  });

  it("suspends a still-live loop that finished while the tab was hidden", () => {
    expect(pollModeAfter("working", 0, true)).toEqual({ mode: "hidden", delayMs: null });
  });

  it("keeps the two terminal states distinct, and terminal even while hidden", () => {
    expect(pollModeAfter("settled", 0, false)).toEqual({ mode: "settled", delayMs: null });
    expect(pollModeAfter("settled", 0, true)).toEqual({ mode: "settled", delayMs: null });
    expect(pollModeAfter("working", CONNECTION_POLL_MAX_FAILURES, false)).toEqual({
      mode: "gaveUp",
      delayMs: null,
    });
    // Terminal beats hidden: a finished poll must not come back as a merely
    // suspended one, because suspension is what a tab focus restores.
    expect(pollModeAfter("working", CONNECTION_POLL_MAX_FAILURES, true).mode).toBe("gaveUp");
  });
});

describe("onTabHidden / onTabVisible", () => {
  it("suspends a running loop and drops its pending timer", () => {
    expect(onTabHidden("running")).toEqual({ mode: "hidden", clearTimer: true });
  });

  it("leaves every non-running state exactly where it is", () => {
    for (const mode of ["hidden", "settled", "gaveUp"] as PollMode[]) {
      expect(onTabHidden(mode)).toEqual({ mode, clearTimer: false });
    }
  });

  it("restores polling ONLY from a hidden suspension", () => {
    expect(onTabVisible("hidden")).toEqual({ mode: "running", pollNow: true });
  });

  it("does not resurrect a terminal loop on a tab focus", () => {
    // The whole point of the terminal states. A settled page is finished, and
    // a given-up page recovers through "Check again" — neither is something
    // alt-tabbing should restart.
    expect(onTabVisible("settled")).toEqual({ mode: "settled", pollNow: false });
    expect(onTabVisible("gaveUp")).toEqual({ mode: "gaveUp", pollNow: false });
    // Already running: no second poll on top of the one in flight.
    expect(onTabVisible("running")).toEqual({ mode: "running", pollNow: false });
  });
});

type LoopStep = SourceConnection[] | "error" | "hide" | "show";

/** Drives the same transitions the effect drives, on a fake clock: a reading
 *  step is "the armed timer fires and the request runs", `hide`/`show` are
 *  visibility events. Uses the exported transitions rather than restating
 *  them, so a regression in the real code shows up here. */
function runLoop(script: LoopStep[]) {
  let mode: PollMode = "running";
  let failures = 0;
  let activity: PollActivity = "working";
  let isHidden = false;
  // The effect fires its first poll on mount, so the loop starts armed.
  let timerArmed = true;
  let requests = 0;
  const log: string[] = [];

  for (const step of script) {
    if (step === "hide") {
      isHidden = true;
      const next = onTabHidden(mode);
      if (next.clearTimer) timerArmed = false;
      mode = next.mode;
      log.push(`hide -> ${mode}${next.clearTimer ? " +cleared" : ""}`);
      continue;
    }
    if (step === "show") {
      isHidden = false;
      const next = onTabVisible(mode);
      mode = next.mode;
      if (next.pollNow) timerArmed = true;
      log.push(`show -> ${mode}${next.pollNow ? " +poll" : ""}`);
      continue;
    }
    if (mode !== "running" || !timerArmed) {
      // Nothing is armed, so no request can happen — this is the state the
      // whole fix is about.
      log.push(`idle (${mode})`);
      continue;
    }
    requests += 1;
    timerArmed = false;
    if (step === "error") failures += 1;
    else {
      failures = 0;
      activity = pollActivity(step);
    }
    const next = pollModeAfter(activity, failures, isHidden);
    mode = next.mode;
    timerArmed = next.delayMs !== null;
    log.push(`poll -> ${mode}${next.delayMs === null ? "" : ` @${next.delayMs}`}`);
  }
  return { mode, requests, timerArmed, log };
}

describe("the poll loop across tab switches", () => {
  it("clears the pending timer when the tab goes away", () => {
    const run = runLoop([[moving], "hide"]);
    expect(run.log).toEqual(["poll -> running @5000", "hide -> hidden +cleared"]);
    expect(run.timerArmed).toBe(false);
    expect(run.mode).toBe("hidden");
  });

  it("takes a fresh reading when the tab comes back, not whenever the old timer happens to fire", () => {
    // The bug: with one `parked` flag, hiding left the timer armed and
    // `parked` false, so coming back before it fired did nothing at all and
    // the page stayed stale — for up to a minute, since browsers throttle
    // background timers well past the 5s the delay claims.
    const run = runLoop([[moving], "hide", "show"]);
    expect(run.log.slice(-2)).toEqual(["hide -> hidden +cleared", "show -> running +poll"]);
    expect(run.timerArmed).toBe(true);
    expect(run.mode).toBe("running");
  });

  it("issues no requests at all while hidden, however long it stays away", () => {
    const run = runLoop([[moving], "hide", [moving], [moving], [moving], "show"]);
    // One request, the one before the tab went away.
    expect(run.requests).toBe(1);
    expect(run.log.slice(1, -1)).toEqual([
      "hide -> hidden +cleared",
      "idle (hidden)",
      "idle (hidden)",
      "idle (hidden)",
    ]);
  });

  it("a settled poll stays settled across a tab switch", () => {
    const run = runLoop([[moving], [doneOnce], "hide", "show", [doneOnce]]);
    expect(run.log).toEqual([
      "poll -> running @5000",
      "poll -> settled",
      "hide -> settled",
      "show -> settled",
      "idle (settled)",
    ]);
    expect(run.mode).toBe("settled");
    // Two readings, then nothing — the tab switch adds no traffic to a page
    // that is finished.
    expect(run.requests).toBe(2);
  });

  it("giving up survives a tab switch — only Check again brings it back", () => {
    const errors: LoopStep[] = Array(CONNECTION_POLL_MAX_FAILURES).fill("error");
    const run = runLoop([[moving], ...errors, "hide", "show", "error"]);
    expect(run.requests).toBe(CONNECTION_POLL_MAX_FAILURES + 1);
    expect(run.mode).toBe("gaveUp");
    expect(run.log.slice(-3)).toEqual(["hide -> gaveUp", "show -> gaveUp", "idle (gaveUp)"]);
  });
});

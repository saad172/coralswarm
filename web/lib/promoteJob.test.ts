import { describe, expect, it } from "vitest";
import type { JobStatus } from "./api";
import {
  activeJobIds,
  allBatchJobIds,
  allLegsTerminal,
  applyJobPollError,
  applyJobPollErrorToBatches,
  applyJobUpdate,
  applyJobUpdateToBatches,
  batchIndicatorLabel,
  batchLinkTargetNodeId,
  batchPhase,
  batchProgressCounts,
  batchProgressPct,
  clearPromoteBatch,
  dismissBatch,
  indicatorExpandAriaLabel,
  indicatorSummaryLabel,
  indicatorSummaryPhase,
  indicatorTriggerLabel,
  isLegTerminal,
  legFromPromoteResult,
  legPhaseWord,
  legProgressPct,
  legStatusLabel,
  processingStageLine,
  processingStageTitle,
  promoteCardItemTitle,
  promoteLegHeading,
  succeededCoralLabel,
  listPromoteBatchReefIds,
  loadPromoteBatch,
  nextPromotePollDelayMs,
  PROMOTE_POLL_INITIAL_MS,
  PROMOTE_POLL_LONG_AFTER_MS,
  PROMOTE_POLL_LONG_MS,
  PROMOTE_POLL_STEP_AFTER_MS,
  PROMOTE_POLL_STEP_MS,
  type PromoteBatch,
  type PromoteBatchMap,
  type PromoteLeg,
  promoteFailureMessage,
  promoteJobStorageKey,
  promotePanelPhase,
  promoteSuccessMessage,
  removeBatch,
  savePromoteBatch,
  summarizeLegs,
  upsertBatch,
  visibleBatches,
  type ListableKVStorage,
} from "./promoteJob";

/** Minimal in-memory `ListableKVStorage` — this project's vitest config runs
 *  a Node environment with no `window`/`localStorage` (see
 *  vitest.config.ts), so the storage-facing functions below are exercised
 *  against this fake rather than the real thing. Mirrors what
 *  `promoteJobLocalStorage()` hands `PromoteJobsProvider`/`PromoteModal` in
 *  the browser — including enumeration (`.length`/`.key`), which the real
 *  `localStorage` also natively supports. */
function fakeStorage(): ListableKVStorage & { data: Map<string, string> } {
  const data = new Map<string, string>();
  return {
    data,
    getItem: (k) => data.get(k) ?? null,
    setItem: (k, v) => void data.set(k, v),
    removeItem: (k) => void data.delete(k),
    get length() {
      return data.size;
    },
    key: (i) => Array.from(data.keys())[i] ?? null,
  };
}

const batch = (over: Partial<PromoteBatch> = {}): PromoteBatch => ({
  reefNodeId: "reef-1",
  reefTitle: "Q3 roadmap priorities",
  legs: [],
  dismissed: false,
  ...over,
});

function jobRes(over: Partial<JobStatus> = {}): JobStatus {
  return {
    id: "job-1",
    kind: "PromoteNode",
    status: "running",
    progress_current: null,
    progress_total: null,
    error_message: null,
    created_at: "2026-09-08T00:00:00Z",
    started_at: "2026-09-08T00:00:01Z",
    finished_at: null,
    ...over,
  };
}

const jobLeg = (over: Partial<Extract<PromoteLeg, { phase: "job" }>> = {}): Extract<PromoteLeg, { phase: "job" }> => ({
  oceanId: "ocean-1",
  oceanName: "Coralswarm Team",
  phase: "job",
  jobId: "job-1",
  status: "pending",
  current: null,
  total: null,
  error: null,
  ...over,
});

describe("legFromPromoteResult", () => {
  it("becomes a job leg when the response carries a job_id", () => {
    const leg = legFromPromoteResult("o1", "Team", { ok: true, jobId: "j1", coralsRequested: 3129 });
    expect(leg).toEqual({
      oceanId: "o1",
      oceanName: "Team",
      phase: "job",
      jobId: "j1",
      status: "pending",
      current: null,
      total: null,
      error: null,
    });
  });

  it("becomes an immediate success when job_id is null (nothing to promote)", () => {
    const leg = legFromPromoteResult("o1", "Team", { ok: true, jobId: null, coralsRequested: 0 });
    expect(leg).toEqual({ oceanId: "o1", oceanName: "Team", phase: "succeeded", coralsRequested: 0 });
  });

  it("becomes an immediate success reporting the requested count even without a job", () => {
    // Real contract only nulls job_id at corals_requested === 0, but this
    // function doesn't assume that invariant — it just reports what it's told.
    const leg = legFromPromoteResult("o1", "Team", { ok: true, jobId: null, coralsRequested: 5 });
    expect(leg).toMatchObject({ phase: "succeeded", coralsRequested: 5 });
  });

  it("becomes a failed leg when the promote call itself rejected", () => {
    const leg = legFromPromoteResult("o1", "Team", { ok: false, error: "HTTP 504" });
    expect(leg).toEqual({ oceanId: "o1", oceanName: "Team", phase: "failed", error: "HTTP 504" });
  });

  // Regression coverage for the bug this file's earlier 85 tests all missed:
  // `oceanId` is the destination OCEAN, not the destination REEF — nothing
  // above ever exercised the destination reef id at all. `src-1`/`dst-1`
  // (rather than reusing one fixture value for both, e.g. via a shared "id")
  // means a leg that accidentally echoes the wrong one back fails loudly
  // instead of a self-comparison silently passing.
  it("carries the response's target_node_id (the destination topic, distinct from any source id) onto a job leg", () => {
    const leg = legFromPromoteResult("o1", "Team", { ok: true, jobId: "j1", coralsRequested: 3129, targetNodeId: "dst-1" });
    expect(leg).toMatchObject({ phase: "job", targetNodeId: "dst-1" });
    if (leg.phase === "job") expect(leg.targetNodeId).not.toBe("src-1");
  });

  it("carries target_node_id onto an immediate-success leg too (no job needed)", () => {
    const leg = legFromPromoteResult("o1", "Team", { ok: true, jobId: null, coralsRequested: 0, targetNodeId: "dst-1" });
    expect(leg).toMatchObject({ phase: "succeeded", targetNodeId: "dst-1" });
  });

  it("a failed leg never carries a targetNodeId — the promote call itself never returned one", () => {
    const leg = legFromPromoteResult("o1", "Team", { ok: false, error: "HTTP 504" });
    expect((leg as { targetNodeId?: string }).targetNodeId).toBeUndefined();
  });
});

describe("applyJobUpdate", () => {
  it("moves a job leg to succeeded on status done, using progress_total as the count", () => {
    const leg = applyJobUpdate(jobLeg(), jobRes({ status: "done", progress_current: 3129, progress_total: 3129 }));
    expect(leg).toEqual({ oceanId: "ocean-1", oceanName: "Coralswarm Team", phase: "succeeded", coralsRequested: 3129 });
  });

  it("done with no progress_total reported falls back to 0, not a fabricated number", () => {
    const leg = applyJobUpdate(jobLeg(), jobRes({ status: "done" }));
    expect(leg).toMatchObject({ phase: "succeeded", coralsRequested: 0 });
  });

  it("carries a job leg's targetNodeId forward onto the succeeded leg it becomes — this is the only place a job leg's destination becomes link-eligible", () => {
    const leg = applyJobUpdate(jobLeg({ targetNodeId: "dst-1" }), jobRes({ status: "done", progress_total: 3129 }));
    expect(leg).toMatchObject({ phase: "succeeded", targetNodeId: "dst-1" });
  });

  it("a job leg that fails never gains a targetNodeId, even if the job leg itself had one — a failed promote isn't a place to land", () => {
    const leg = applyJobUpdate(jobLeg({ targetNodeId: "dst-1" }), jobRes({ status: "failed", error_message: "boom" }));
    expect((leg as { targetNodeId?: string }).targetNodeId).toBeUndefined();
  });

  it("moves a job leg to failed on status failed, carrying error_message", () => {
    const leg = applyJobUpdate(jobLeg(), jobRes({ status: "failed", error_message: "batch insert timed out" }));
    expect(leg).toEqual({ oceanId: "ocean-1", oceanName: "Coralswarm Team", phase: "failed", error: "batch insert timed out" });
  });

  it("moves a job leg to failed on status dead", () => {
    const leg = applyJobUpdate(jobLeg(), jobRes({ status: "dead", error_message: null }));
    expect(leg).toEqual({ oceanId: "ocean-1", oceanName: "Coralswarm Team", phase: "failed", error: null });
  });

  it("updates progress fields in place while still running", () => {
    const leg = applyJobUpdate(jobLeg({ status: "running" }), jobRes({ status: "running", progress_current: 500, progress_total: 3129 }));
    expect(leg).toEqual(jobLeg({ status: "running", current: 500, total: 3129, error: null, pollUnavailable: false }));
  });

  it("pending -> running transition is reflected", () => {
    const leg = applyJobUpdate(jobLeg({ status: "pending" }), jobRes({ status: "running" }));
    expect(leg.phase).toBe("job");
    if (leg.phase === "job") expect(leg.status).toBe("running");
  });

  it("is a no-op on an already-terminal (succeeded) leg — a stale response can't resurrect it", () => {
    const succeeded: PromoteLeg = { oceanId: "o1", oceanName: "Team", phase: "succeeded", coralsRequested: 3 };
    expect(applyJobUpdate(succeeded, jobRes({ status: "running" }))).toBe(succeeded);
  });

  it("is a no-op on an already-terminal (failed) leg", () => {
    const failed: PromoteLeg = { oceanId: "o1", oceanName: "Team", phase: "failed", error: "x" };
    expect(applyJobUpdate(failed, jobRes({ status: "done" }))).toBe(failed);
  });
});

describe("applyJobPollError", () => {
  it("a 404 is terminal — the job id is gone, don't keep polling it", () => {
    const leg = applyJobPollError(jobLeg(), 404);
    expect(leg.phase).toBe("failed");
    if (leg.phase === "failed") expect(leg.error).toMatch(/could no longer be found/i);
  });

  it("any other status (or none, e.g. a network error) is keeps polling and shows that status is unavailable", () => {
    expect(applyJobPollError(jobLeg(), 500)).toMatchObject({ phase: "job", pollUnavailable: true });
    expect(applyJobPollError(jobLeg(), undefined)).toMatchObject({ phase: "job", pollUnavailable: true });
  });

  it("is a no-op on an already-terminal leg", () => {
    const succeeded: PromoteLeg = { oceanId: "o1", oceanName: "Team", phase: "succeeded", coralsRequested: 3 };
    expect(applyJobPollError(succeeded, 404)).toBe(succeeded);
  });
});

describe("isLegTerminal / allLegsTerminal", () => {
  it("a job leg is not terminal; succeeded/failed are", () => {
    expect(isLegTerminal(jobLeg())).toBe(false);
    expect(isLegTerminal({ oceanId: "o", oceanName: "n", phase: "succeeded", coralsRequested: 0 })).toBe(true);
    expect(isLegTerminal({ oceanId: "o", oceanName: "n", phase: "failed", error: null })).toBe(true);
  });

  it("allLegsTerminal is false while any leg is still a job", () => {
    const legs: PromoteLeg[] = [
      { oceanId: "o1", oceanName: "a", phase: "succeeded", coralsRequested: 1 },
      jobLeg({ oceanId: "o2" }),
    ];
    expect(allLegsTerminal(legs)).toBe(false);
  });

  it("allLegsTerminal is true once every leg has landed", () => {
    const legs: PromoteLeg[] = [
      { oceanId: "o1", oceanName: "a", phase: "succeeded", coralsRequested: 1 },
      { oceanId: "o2", oceanName: "b", phase: "failed", error: "x" },
    ];
    expect(allLegsTerminal(legs)).toBe(true);
  });

  it("an empty leg set is (vacuously) terminal", () => {
    expect(allLegsTerminal([])).toBe(true);
  });
});

describe("activeJobIds", () => {
  it("returns only the job ids of non-terminal legs", () => {
    const legs: PromoteLeg[] = [
      jobLeg({ oceanId: "o1", jobId: "j1" }),
      { oceanId: "o2", oceanName: "b", phase: "succeeded", coralsRequested: 1 },
      jobLeg({ oceanId: "o3", jobId: "j3" }),
    ];
    expect(activeJobIds(legs)).toEqual(["j1", "j3"]);
  });

  it("is empty once every leg is terminal — the signal useJobPoll uses to stop", () => {
    const legs: PromoteLeg[] = [{ oceanId: "o1", oceanName: "a", phase: "succeeded", coralsRequested: 1 }];
    expect(activeJobIds(legs)).toEqual([]);
  });
});

describe("legProgressPct", () => {
  it("computes a rounded percentage when both bounds are present", () => {
    expect(legProgressPct(jobLeg({ current: 500, total: 3129 }))).toBe(16); // 15.98% rounds to 16
  });

  it("is null (indeterminate) when current is missing", () => {
    expect(legProgressPct(jobLeg({ current: null, total: 3129 }))).toBeNull();
  });

  it("is null (indeterminate) when total is missing", () => {
    expect(legProgressPct(jobLeg({ current: 5, total: null }))).toBeNull();
  });

  it("is null when total is zero or negative — never divides by zero", () => {
    expect(legProgressPct(jobLeg({ current: 0, total: 0 }))).toBeNull();
    expect(legProgressPct(jobLeg({ current: 0, total: -1 }))).toBeNull();
  });

  it("clamps to [0, 100]", () => {
    expect(legProgressPct(jobLeg({ current: 3200, total: 3129 }))).toBe(100);
    expect(legProgressPct(jobLeg({ current: 0, total: 3129 }))).toBe(0);
  });
});

describe("summarizeLegs", () => {
  it("splits succeeded/failed names and sums items across succeeded legs only", () => {
    const legs: PromoteLeg[] = [
      { oceanId: "o1", oceanName: "Coralswarm Team", phase: "succeeded", coralsRequested: 3129 },
      { oceanId: "o2", oceanName: "Acme Org", phase: "failed", error: "boom" },
    ];
    expect(summarizeLegs(legs)).toEqual({
      succeededNames: ["Coralswarm Team"],
      failedNames: ["Acme Org"],
      succeededLegs: 1,
      failedLegs: 1,
      coralsPromoted: 3129,
      allSucceeded: false,
      someSucceeded: true,
    });
  });

  // A bulk promote fans out one leg per (ocean x reef) pair, so three topics
  // into one workspace produced three legs all named "Acme" — and every
  // consumer joins these names into a sentence ("Promoted to Acme, Acme,
  // Acme"). The PAIR count has to survive the dedupe: it is what the caller
  // reports as `reefs`.
  it("reports each workspace once for a bulk promote, and keeps the pair count separately", () => {
    const legs: PromoteLeg[] = [
      { oceanId: "o1", oceanName: "Acme", phase: "succeeded", coralsRequested: 4 },
      { oceanId: "o1", oceanName: "Acme", phase: "succeeded", coralsRequested: 5 },
      { oceanId: "o1", oceanName: "Acme", phase: "succeeded", coralsRequested: 6 },
    ];
    const out = summarizeLegs(legs);
    expect(out.succeededNames).toEqual(["Acme"]);
    expect(out.succeededLegs).toBe(3);
    expect(out.coralsPromoted).toBe(15);
  });

  it("dedupes failed workspace names too, and counts the failed pairs", () => {
    const legs: PromoteLeg[] = [
      { oceanId: "o1", oceanName: "Acme", phase: "failed", error: "x" },
      { oceanId: "o1", oceanName: "Acme", phase: "failed", error: "y" },
      { oceanId: "o2", oceanName: "Beta", phase: "succeeded", coralsRequested: 1 },
      { oceanId: "o2", oceanName: "Beta", phase: "succeeded", coralsRequested: 2 },
    ];
    const out = summarizeLegs(legs);
    expect(out.failedNames).toEqual(["Acme"]);
    expect(out.failedLegs).toBe(2);
    expect(out.succeededNames).toEqual(["Beta"]);
    expect(out.succeededLegs).toBe(2);
  });

  it("keeps distinct workspaces distinct, in first-success order", () => {
    const legs: PromoteLeg[] = [
      { oceanId: "o2", oceanName: "Beta", phase: "succeeded", coralsRequested: 1 },
      { oceanId: "o1", oceanName: "Acme", phase: "succeeded", coralsRequested: 1 },
      { oceanId: "o2", oceanName: "Beta", phase: "succeeded", coralsRequested: 1 },
    ];
    expect(summarizeLegs(legs).succeededNames).toEqual(["Beta", "Acme"]);
  });

  it("allSucceeded is true only with zero failures, even zero legs", () => {
    expect(summarizeLegs([]).allSucceeded).toBe(true);
    expect(summarizeLegs([]).someSucceeded).toBe(false);
  });

  it("a still-running job leg counts toward neither succeeded nor failed", () => {
    const legs: PromoteLeg[] = [jobLeg()];
    const out = summarizeLegs(legs);
    expect(out.succeededNames).toEqual([]);
    expect(out.failedNames).toEqual([]);
    expect(out.succeededLegs).toBe(0);
    expect(out.failedLegs).toBe(0);
    expect(out.coralsPromoted).toBe(0);
  });
});

describe("promoteFailureMessage", () => {
  it("is null when nothing failed", () => {
    expect(promoteFailureMessage(summarizeLegs([{ oceanId: "o", oceanName: "Team", phase: "succeeded", coralsRequested: 1 }]))).toBeNull();
  });

  it("reports a pure failure without a success clause", () => {
    const outcome = summarizeLegs([{ oceanId: "o", oceanName: "Acme Org", phase: "failed", error: "x" }]);
    expect(promoteFailureMessage(outcome)).toBe("Couldn't promote to Acme Org — please retry.");
  });

  it("reports a partial failure with both clauses, matching the pre-async wording", () => {
    const outcome = summarizeLegs([
      { oceanId: "o1", oceanName: "Coralswarm Team", phase: "succeeded", coralsRequested: 10 },
      { oceanId: "o2", oceanName: "Acme Org", phase: "failed", error: "x" },
    ]);
    expect(promoteFailureMessage(outcome)).toBe(
      "Promoted to Coralswarm Team. Couldn't promote to Acme Org — please retry.",
    );
  });
});

describe("promoteSuccessMessage", () => {
  it("names a bulk promote's single destination once", () => {
    const outcome = summarizeLegs([
      { oceanId: "o1", oceanName: "Acme", phase: "succeeded", coralsRequested: 2 },
      { oceanId: "o1", oceanName: "Acme", phase: "succeeded", coralsRequested: 3 },
    ]);
    expect(promoteSuccessMessage("selected topics", outcome)).toBe(
      "Promoted selected topics to Acme — 5 items travelled with it.",
    );
  });

  it("pluralises items correctly and lists every succeeded workspace", () => {
    const outcome = summarizeLegs([
      { oceanId: "o1", oceanName: "Coralswarm Team", phase: "succeeded", coralsRequested: 3129 },
      { oceanId: "o2", oceanName: "Acme Org", phase: "succeeded", coralsRequested: 0 },
    ]);
    expect(promoteSuccessMessage("Q3 roadmap", outcome)).toBe(
      "Promoted Q3 roadmap to Coralswarm Team, Acme Org — 3,129 items travelled with it.",
    );
  });

  it("singular for exactly one item", () => {
    const outcome = summarizeLegs([{ oceanId: "o1", oceanName: "Team", phase: "succeeded", coralsRequested: 1 }]);
    expect(promoteSuccessMessage("Launch plan", outcome)).toBe("Promoted Launch plan to Team — 1 item travelled with it.");
  });

  it("reads 'no items', not '0 items', for a reef-only promote", () => {
    const outcome = summarizeLegs([{ oceanId: "o1", oceanName: "Team", phase: "succeeded", coralsRequested: 0 }]);
    expect(promoteSuccessMessage("Launch plan", outcome)).toBe("Promoted Launch plan to Team — no items travelled with it.");
  });
});

describe("legStatusLabel", () => {
  it("shows the formatted counts once both are known", () => {
    expect(legStatusLabel(jobLeg({ current: 500, total: 3129 }))).toBe("500 of 3,129 items");
  });

  it("reads 'queued…' while pending with no progress signal", () => {
    expect(legStatusLabel(jobLeg({ status: "pending", current: null, total: null }))).toBe("queued…");
  });

  it("reads 'copying items…' while running with no progress signal (indeterminate)", () => {
    expect(legStatusLabel(jobLeg({ status: "running", current: null, total: null }))).toBe("copying items…");
  });
});

describe("promotePanelPhase", () => {
  it("is idle before anything is submitted", () => {
    expect(promotePanelPhase(null, false)).toBe("idle");
  });

  it("is promoting the instant submit fires, before any leg exists", () => {
    expect(promotePanelPhase(null, true)).toBe("promoting");
  });

  it("is promoting while any leg is still a job", () => {
    expect(promotePanelPhase([jobLeg()], false)).toBe("promoting");
  });

  it("is done once every leg succeeded", () => {
    expect(promotePanelPhase([{ oceanId: "o", oceanName: "Team", phase: "succeeded", coralsRequested: 1 }], false)).toBe("done");
  });

  it("is error once terminal with at least one failure", () => {
    const legs: PromoteLeg[] = [
      { oceanId: "o1", oceanName: "Team", phase: "succeeded", coralsRequested: 1 },
      { oceanId: "o2", oceanName: "Org", phase: "failed", error: "x" },
    ];
    expect(promotePanelPhase(legs, false)).toBe("error");
  });
});

describe("nextPromotePollDelayMs", () => {
  it("starts responsive", () => {
    expect(nextPromotePollDelayMs(0)).toBe(PROMOTE_POLL_INITIAL_MS);
    expect(nextPromotePollDelayMs(PROMOTE_POLL_STEP_AFTER_MS - 1)).toBe(PROMOTE_POLL_INITIAL_MS);
  });

  it("steps down in frequency once a job has run for a while", () => {
    expect(nextPromotePollDelayMs(PROMOTE_POLL_STEP_AFTER_MS)).toBe(PROMOTE_POLL_STEP_MS);
    expect(nextPromotePollDelayMs(PROMOTE_POLL_LONG_AFTER_MS - 1)).toBe(PROMOTE_POLL_STEP_MS);
  });

  it("backs off further on a genuinely long-running job, never gives up", () => {
    expect(nextPromotePollDelayMs(PROMOTE_POLL_LONG_AFTER_MS)).toBe(PROMOTE_POLL_LONG_MS);
    expect(nextPromotePollDelayMs(PROMOTE_POLL_LONG_AFTER_MS * 100)).toBe(PROMOTE_POLL_LONG_MS);
  });

  it("every step is slower than or equal to the previous one — cadence never speeds back up", () => {
    expect(PROMOTE_POLL_INITIAL_MS).toBeLessThanOrEqual(PROMOTE_POLL_STEP_MS);
    expect(PROMOTE_POLL_STEP_MS).toBeLessThanOrEqual(PROMOTE_POLL_LONG_MS);
  });
});

describe("promoteJobStorageKey", () => {
  it("namespaces by topic node id", () => {
    expect(promoteJobStorageKey("reef-abc")).toBe("cs:promoteJob:reef-abc");
    expect(promoteJobStorageKey("reef-abc")).not.toBe(promoteJobStorageKey("reef-xyz"));
  });
});

describe("save/load/clearPromoteBatch — resilience to navigating away and back", () => {
  it("round-trips an in-flight batch (title + legs), resuming a job leg at a safe 'pending, unknown progress' state", () => {
    const storage = fakeStorage();
    const legs: PromoteLeg[] = [
      jobLeg({ oceanId: "o1", jobId: "j1", status: "running", current: 500, total: 3129 }),
      { oceanId: "o2", oceanName: "Acme Org", phase: "succeeded", coralsRequested: 4 },
    ];
    savePromoteBatch(storage, "reef-1", "Q3 roadmap priorities", legs);
    const resumed = loadPromoteBatch(storage, "reef-1");
    expect(resumed).toEqual({
      reefTitle: "Q3 roadmap priorities",
      legs: [
        jobLeg({ oceanId: "o1", jobId: "j1", status: "pending", current: null, total: null }),
        { oceanId: "o2", oceanName: "Acme Org", phase: "succeeded", coralsRequested: 4 },
      ],
    });
  });

  it("clears the entry once the whole batch is terminal — nothing left to resume", () => {
    const storage = fakeStorage();
    savePromoteBatch(storage, "reef-1", "Q3 roadmap priorities", [jobLeg()]);
    expect(loadPromoteBatch(storage, "reef-1")).not.toBeNull();
    savePromoteBatch(storage, "reef-1", "Q3 roadmap priorities", [
      { oceanId: "o1", oceanName: "Team", phase: "succeeded", coralsRequested: 1 },
    ]);
    expect(loadPromoteBatch(storage, "reef-1")).toBeNull();
  });

  it("saving an empty leg list clears rather than storing an empty batch", () => {
    const storage = fakeStorage();
    savePromoteBatch(storage, "reef-1", "Q3 roadmap priorities", [jobLeg()]);
    savePromoteBatch(storage, "reef-1", "Q3 roadmap priorities", []);
    expect(loadPromoteBatch(storage, "reef-1")).toBeNull();
  });

  it("returns null when nothing was ever stored for this topic", () => {
    const storage = fakeStorage();
    expect(loadPromoteBatch(storage, "reef-never-promoted")).toBeNull();
  });

  it("treats corrupt JSON as nothing to resume, not a throw", () => {
    const storage = fakeStorage();
    storage.setItem(promoteJobStorageKey("reef-1"), "{not json");
    expect(loadPromoteBatch(storage, "reef-1")).toBeNull();
  });

  it("treats a foreign/mismatched schema version as nothing to resume", () => {
    const storage = fakeStorage();
    storage.setItem(promoteJobStorageKey("reef-1"), JSON.stringify({ v: 4, reefNodeId: "reef-1", reefTitle: "x", legs: [] }));
    expect(loadPromoteBatch(storage, "reef-1")).toBeNull();
  });

  it("drops a pre-indicator v1 entry (no reefTitle) rather than guessing a title", () => {
    const storage = fakeStorage();
    storage.setItem(promoteJobStorageKey("reef-1"), JSON.stringify({ v: 1, reefNodeId: "reef-1", legs: [{ oceanId: "o", oceanName: "n", phase: "job", jobId: "j" }] }));
    expect(loadPromoteBatch(storage, "reef-1")).toBeNull();
  });

  // Regression coverage for the promote-pill-links-to-the-wrong-topic bug:
  // a v2 entry predates `targetNodeId` entirely, so every `succeeded`/`job`
  // leg it carries would resume with no way to ever link correctly. Rather
  // than resume one anyway (unlinked, but ONE MORE SPOT that must remember
  // "no targetNodeId means no link" instead of the shape simply not
  // existing), it's dropped outright — same treatment as v1.
  it("drops a pre-targetNodeId v2 entry rather than resume a batch that can never link", () => {
    const storage = fakeStorage();
    storage.setItem(
      promoteJobStorageKey("reef-1"),
      JSON.stringify({
        v: 2,
        reefNodeId: "reef-1",
        reefTitle: "Q3 roadmap priorities",
        legs: [{ oceanId: "o1", oceanName: "Team", phase: "succeeded", coralsRequested: 4 }],
      }),
    );
    expect(loadPromoteBatch(storage, "reef-1")).toBeNull();
  });

  it("round-trips a leg's targetNodeId — the destination topic id, distinct from the source topic this batch is keyed by", () => {
    const storage = fakeStorage();
    const legs: PromoteLeg[] = [
      { oceanId: "o1", oceanName: "Team", phase: "succeeded", coralsRequested: 4, targetNodeId: "dst-1" },
      jobLeg({ oceanId: "o2", jobId: "j1", targetNodeId: "dst-2" }),
    ];
    // The batch itself stays keyed by the SOURCE reef id — promoting never
    // changes what key this batch resumes under.
    savePromoteBatch(storage, "src-1", "Q3 roadmap priorities", legs);
    const resumed = loadPromoteBatch(storage, "src-1");
    expect(resumed?.legs.find((l) => l.oceanId === "o1")).toMatchObject({ targetNodeId: "dst-1" });
    expect(resumed?.legs.find((l) => l.oceanId === "o2")).toMatchObject({ targetNodeId: "dst-2" });
    // And "src-1" was never mistaken for either destination.
    expect(resumed?.legs.every((l) => (l as { targetNodeId?: string }).targetNodeId !== "src-1")).toBe(true);
  });

  it("two topics' batches never collide", () => {
    const storage = fakeStorage();
    savePromoteBatch(storage, "reef-1", "Topic one", [jobLeg({ oceanId: "a" })]);
    savePromoteBatch(storage, "reef-2", "Topic two", [jobLeg({ oceanId: "b" })]);
    expect(loadPromoteBatch(storage, "reef-1")?.legs[0]).toMatchObject({ oceanId: "a" });
    expect(loadPromoteBatch(storage, "reef-2")?.legs[0]).toMatchObject({ oceanId: "b" });
  });

  it("clearPromoteBatch removes an entry outright", () => {
    const storage = fakeStorage();
    savePromoteBatch(storage, "reef-1", "Q3 roadmap priorities", [jobLeg()]);
    clearPromoteBatch(storage, "reef-1");
    expect(loadPromoteBatch(storage, "reef-1")).toBeNull();
  });
});

describe("listPromoteBatchReefIds", () => {
  it("finds every stored topic id, ignoring unrelated keys", () => {
    const storage = fakeStorage();
    savePromoteBatch(storage, "reef-1", "Topic one", [jobLeg()]);
    savePromoteBatch(storage, "reef-2", "Topic two", [jobLeg()]);
    storage.setItem("cs_device_id", "some-other-app-key");
    expect(listPromoteBatchReefIds(storage).sort()).toEqual(["reef-1", "reef-2"]);
  });

  it("is empty when nothing is stored", () => {
    expect(listPromoteBatchReefIds(fakeStorage())).toEqual([]);
  });

  it("still reports an id whose value doesn't parse — the caller (loadPromoteBatch) is what discards it", () => {
    const storage = fakeStorage();
    storage.setItem(promoteJobStorageKey("reef-corrupt"), "{not json");
    expect(listPromoteBatchReefIds(storage)).toEqual(["reef-corrupt"]);
  });
});

describe("upsertBatch / removeBatch / dismissBatch — the multi-reef map", () => {
  it("upsertBatch adds a new entry, dismissed:false", () => {
    const map = upsertBatch({}, "reef-1", "Q3 roadmap priorities", [jobLeg()]);
    expect(map["reef-1"]).toEqual(batch({ legs: [jobLeg()] }));
  });

  it("upsertBatch REPLACES an existing entry for the same topic rather than merging — a retry is a fresh batch", () => {
    const withOld = upsertBatch({}, "reef-1", "Q3 roadmap priorities", [jobLeg({ jobId: "old" })]);
    const withNew = upsertBatch(withOld, "reef-1", "Q3 roadmap priorities", [jobLeg({ jobId: "new" })]);
    expect(withNew["reef-1"].legs).toEqual([jobLeg({ jobId: "new" })]);
  });

  it("different topics never collide in the map", () => {
    let map = upsertBatch({}, "reef-1", "Topic one", [jobLeg({ oceanId: "a" })]);
    map = upsertBatch(map, "reef-2", "Topic two", [jobLeg({ oceanId: "b" })]);
    expect(Object.keys(map).sort()).toEqual(["reef-1", "reef-2"]);
    expect(map["reef-1"].legs[0]).toMatchObject({ oceanId: "a" });
    expect(map["reef-2"].legs[0]).toMatchObject({ oceanId: "b" });
  });

  it("removeBatch deletes the entry; is a no-op (same reference) if absent", () => {
    const map = upsertBatch({}, "reef-1", "Q3 roadmap priorities", [jobLeg()]);
    const removed = removeBatch(map, "reef-1");
    expect(removed).toEqual({});
    expect(removeBatch(removed, "reef-1")).toBe(removed);
  });

  it("dismissBatch flips the flag; is a no-op (same reference) if absent or already dismissed", () => {
    const map = upsertBatch({}, "reef-1", "Q3 roadmap priorities", [{ oceanId: "o", oceanName: "n", phase: "succeeded", coralsRequested: 1 }]);
    const dismissed = dismissBatch(map, "reef-1");
    expect(dismissed["reef-1"].dismissed).toBe(true);
    expect(dismissBatch(dismissed, "reef-1")).toBe(dismissed);
    expect(dismissBatch({}, "reef-missing")).toEqual({});
  });
});

describe("applyJobUpdateToBatches / applyJobPollErrorToBatches", () => {
  it("routes an update to the leg that owns the job id, across multiple batches", () => {
    let map: PromoteBatchMap = upsertBatch({}, "reef-1", "Topic one", [jobLeg({ jobId: "j1", oceanId: "a" })]);
    map = upsertBatch(map, "reef-2", "Topic two", [jobLeg({ jobId: "j2", oceanId: "b" })]);
    const next = applyJobUpdateToBatches(map, "j2", jobRes({ status: "running", progress_current: 10, progress_total: 20 }));
    expect(next["reef-1"]).toBe(map["reef-1"]); // untouched, same reference
    expect(next["reef-2"].legs[0]).toMatchObject({ phase: "job", current: 10, total: 20 });
  });

  it("terminal update moves the owning leg to succeeded/failed without touching other batches", () => {
    let map: PromoteBatchMap = upsertBatch({}, "reef-1", "Topic one", [jobLeg({ jobId: "j1" })]);
    map = upsertBatch(map, "reef-2", "Topic two", [jobLeg({ jobId: "j2" })]);
    const next = applyJobUpdateToBatches(map, "j1", jobRes({ status: "done", progress_total: 42 }));
    expect(next["reef-1"].legs[0]).toEqual({ oceanId: "ocean-1", oceanName: "Coralswarm Team", phase: "succeeded", coralsRequested: 42 });
    expect(next["reef-2"]).toBe(map["reef-2"]);
  });

  it("is a no-op (same reference) when no leg anywhere owns the job id — a stray/late response can't resurrect anything", () => {
    const map = upsertBatch({}, "reef-1", "Topic one", [jobLeg({ jobId: "j1" })]);
    expect(applyJobUpdateToBatches(map, "unknown-job", jobRes())).toBe(map);
  });

  it("applyJobPollErrorToBatches routes a 404 to the owning leg as terminal-failed", () => {
    const map = upsertBatch({}, "reef-1", "Topic one", [jobLeg({ jobId: "j1" })]);
    const next = applyJobPollErrorToBatches(map, "j1", 404);
    expect(next["reef-1"].legs[0].phase).toBe("failed");
  });

  it("applyJobPollErrorToBatches is a no-op on a miss, and on a transient (non-404) error", () => {
    const map = upsertBatch({}, "reef-1", "Topic one", [jobLeg({ jobId: "j1" })]);
    expect(applyJobPollErrorToBatches(map, "unknown-job", 404)).toBe(map);
    expect(applyJobPollErrorToBatches(map, "j1", 500)).not.toEqual(map);
  });
});

describe("allBatchJobIds / visibleBatches", () => {
  it("flattens every non-terminal leg's job id across every batch", () => {
    let map: PromoteBatchMap = upsertBatch({}, "reef-1", "Topic one", [jobLeg({ jobId: "j1" }), jobLeg({ jobId: "j2", oceanId: "o2" })]);
    map = upsertBatch(map, "reef-2", "Topic two", [{ oceanId: "o", oceanName: "n", phase: "succeeded", coralsRequested: 1 }]);
    expect(allBatchJobIds(map).sort()).toEqual(["j1", "j2"]);
  });

  it("empties out once every leg everywhere has landed", () => {
    const map = upsertBatch({}, "reef-1", "Topic one", [{ oceanId: "o", oceanName: "n", phase: "succeeded", coralsRequested: 1 }]);
    expect(allBatchJobIds(map)).toEqual([]);
  });

  it("visibleBatches excludes dismissed entries only", () => {
    let map = upsertBatch({}, "reef-1", "Topic one", [jobLeg()]);
    map = upsertBatch(map, "reef-2", "Topic two", [jobLeg()]);
    map = dismissBatch(map, "reef-1");
    expect(visibleBatchesReefIds(map)).toEqual(["reef-2"]);
  });
});

function visibleBatchesReefIds(map: PromoteBatchMap): string[] {
  return visibleBatches(map)
    .map((b) => b.reefNodeId)
    .sort();
}

describe("batchPhase / batchProgressCounts / batchProgressPct", () => {
  it("is promoting while any leg is still a job", () => {
    expect(batchPhase(batch({ legs: [jobLeg()] }))).toBe("promoting");
  });

  it("is done once every leg succeeded", () => {
    expect(batchPhase(batch({ legs: [{ oceanId: "o", oceanName: "n", phase: "succeeded", coralsRequested: 1 }] }))).toBe("done");
  });

  it("is error once terminal with at least one failure", () => {
    expect(
      batchPhase(
        batch({
          legs: [
            { oceanId: "o1", oceanName: "a", phase: "succeeded", coralsRequested: 1 },
            { oceanId: "o2", oceanName: "b", phase: "failed", error: "x" },
          ],
        }),
      ),
    ).toBe("error");
  });

  it("sums progress across every DETERMINATE running leg, ignoring an indeterminate one", () => {
    const b = batch({
      legs: [
        jobLeg({ oceanId: "o1", current: 500, total: 1000 }),
        jobLeg({ oceanId: "o2", current: 200, total: 500 }),
        jobLeg({ oceanId: "o3", current: null, total: null }),
      ],
    });
    expect(batchProgressCounts(b)).toEqual({ current: 700, total: 1500 });
    expect(batchProgressPct(b)).toBe(47); // 700/1500 = 46.67% rounds to 47
  });

  it("is null (indeterminate) when NO leg has reported progress yet", () => {
    const b = batch({ legs: [jobLeg({ current: null, total: null })] });
    expect(batchProgressCounts(b)).toBeNull();
    expect(batchProgressPct(b)).toBeNull();
  });

  it("ignores a leg that has already landed — it contributes nothing further", () => {
    const b = batch({
      legs: [jobLeg({ current: 500, total: 1000 }), { oceanId: "o2", oceanName: "b", phase: "succeeded", coralsRequested: 999 }],
    });
    expect(batchProgressCounts(b)).toEqual({ current: 500, total: 1000 });
  });
});

// Regression coverage for the promote-pill-links-to-the-wrong-topic bug:
// `PromoteJobsIndicator`'s row must link to the DESTINATION reef, and only
// when exactly one leg has landed on a known one. Every fixture below uses
// visibly distinct source/destination values (`src-1` for the batch's own
// key, `dst-*` for a leg's `targetNodeId`) so a regression that echoes the
// wrong id back fails these tests instead of silently comparing a value to
// itself.
describe("batchLinkTargetNodeId — the one destination worth linking a batch's row to", () => {
  it("links to the single succeeded leg's targetNodeId", () => {
    const b = batch({
      reefNodeId: "src-1",
      legs: [{ oceanId: "o1", oceanName: "Team", phase: "succeeded", coralsRequested: 4, targetNodeId: "dst-1" }],
    });
    expect(batchLinkTargetNodeId(b)).toBe("dst-1");
  });

  it("is null while the batch is still promoting — never falls back to the source topic id", () => {
    const b = batch({ reefNodeId: "src-1", legs: [jobLeg({ targetNodeId: "dst-1" })] });
    expect(batchLinkTargetNodeId(b)).toBeNull();
  });

  it("is null for a job leg's targetNodeId even though it's already known — items are still copying (the mid-job empty-state bug)", () => {
    // The destination reef row exists (target_node_id came back synchronously)
    // but the background coral-copy job for it hasn't landed yet, so this
    // must NOT be link-eligible until the leg is terminal.
    const b = batch({
      reefNodeId: "src-1",
      legs: [jobLeg({ oceanId: "o1", jobId: "j1", targetNodeId: "dst-1" })],
    });
    expect(batchLinkTargetNodeId(b)).toBeNull();
  });

  it("is null once every leg failed — nothing was ever promoted anywhere", () => {
    const b = batch({ reefNodeId: "src-1", legs: [{ oceanId: "o1", oceanName: "Team", phase: "failed", error: "boom" }] });
    expect(batchLinkTargetNodeId(b)).toBeNull();
  });

  it("is null when two+ legs succeeded to different workspaces — no single destination to send a click to", () => {
    const b = batch({
      reefNodeId: "src-1",
      legs: [
        { oceanId: "o1", oceanName: "Team", phase: "succeeded", coralsRequested: 4, targetNodeId: "dst-1" },
        { oceanId: "o2", oceanName: "Acme Org", phase: "succeeded", coralsRequested: 2, targetNodeId: "dst-2" },
      ],
    });
    expect(batchLinkTargetNodeId(b)).toBeNull();
  });

  // Regression for a CodeRabbit finding on this PR: the first cut of this
  // function counted KNOWN ids (legs whose targetNodeId survived the
  // `!!id` filter), not succeeded legs — so two succeeded legs where only
  // one carries a targetNodeId (e.g. one leg rehydrated from a legacy
  // persisted batch, or any other path that lost the id) collapsed to "one
  // candidate" and confidently linked to it, even though a second ocean
  // ALSO succeeded and this row isn't uniquely about the one with a known
  // id. Distinct oceanId/targetNodeId fixtures on both legs so this can't
  // pass by comparing a value to itself.
  it("is null when two legs succeeded but only one carries a targetNodeId — a missing id must make the batch LESS linkable, never count as 'the one'", () => {
    const b = batch({
      reefNodeId: "src-1",
      legs: [
        { oceanId: "o1", oceanName: "Team", phase: "succeeded", coralsRequested: 4, targetNodeId: "dst-1" },
        { oceanId: "o2", oceanName: "Acme Org", phase: "succeeded", coralsRequested: 2 },
      ],
    });
    expect(batchLinkTargetNodeId(b)).toBeNull();
  });

  it("is null for a succeeded leg with no targetNodeId — the persisted-shape migration case (a legacy leg predating this field)", () => {
    const b = batch({
      reefNodeId: "src-1",
      legs: [{ oceanId: "o1", oceanName: "Team", phase: "succeeded", coralsRequested: 4 }],
    });
    expect(batchLinkTargetNodeId(b)).toBeNull();
  });

  it("one succeeded leg with a targetNodeId plus one failed leg still links to the succeeded destination", () => {
    const b = batch({
      reefNodeId: "src-1",
      legs: [
        { oceanId: "o1", oceanName: "Team", phase: "succeeded", coralsRequested: 4, targetNodeId: "dst-1" },
        { oceanId: "o2", oceanName: "Acme Org", phase: "failed", error: "boom" },
      ],
    });
    expect(batchLinkTargetNodeId(b)).toBe("dst-1");
  });
});

describe("batchIndicatorLabel / indicatorSummaryLabel", () => {
  it("shows a live count once any leg has reported one", () => {
    const b = batch({ legs: [jobLeg({ current: 1600, total: 3129 })] });
    expect(batchIndicatorLabel(b)).toBe("Promoting Q3 roadmap priorities… 1,600 / 3,129");
  });

  it("falls back to a plain verb while every leg is still indeterminate", () => {
    const b = batch({ legs: [jobLeg({ current: null, total: null })] });
    expect(batchIndicatorLabel(b)).toBe("Promoting Q3 roadmap priorities…");
  });

  it("reads a success label once done", () => {
    const b = batch({ legs: [{ oceanId: "o", oceanName: "n", phase: "succeeded", coralsRequested: 1 }] });
    expect(batchIndicatorLabel(b)).toBe("Promoted Q3 roadmap priorities");
  });

  it("reads a failure label once errored", () => {
    const b = batch({ legs: [{ oceanId: "o", oceanName: "n", phase: "failed", error: "x" }] });
    expect(batchIndicatorLabel(b)).toBe("Couldn't promote Q3 roadmap priorities");
  });

  it("indicatorSummaryLabel counts phases across batches, omitting zero counts", () => {
    const batches = [
      batch({ reefNodeId: "r1", legs: [jobLeg()] }),
      batch({ reefNodeId: "r2", legs: [jobLeg()] }),
      batch({ reefNodeId: "r3", legs: [{ oceanId: "o", oceanName: "n", phase: "failed", error: "x" }] }),
    ];
    expect(indicatorSummaryLabel(batches)).toBe("2 promoting · 1 failed");
  });

  it("indicatorSummaryLabel omits a phase with zero batches entirely (no '0 done')", () => {
    const batches = [batch({ reefNodeId: "r1", legs: [jobLeg()] })];
    expect(indicatorSummaryLabel(batches)).toBe("1 promoting");
  });

  it("indicatorTriggerLabel drops the live count on a single promoting batch — that belongs in the expanded panel", () => {
    const one = [batch({ legs: [jobLeg({ current: 1600, total: 3129 })] })];
    expect(indicatorTriggerLabel(one)).toBe("Promoting Q3 roadmap priorities…");
    const many = [batch({ reefNodeId: "r1", legs: [jobLeg()] }), batch({ reefNodeId: "r2", legs: [jobLeg()] })];
    expect(indicatorTriggerLabel(many)).toBe("2 promoting");
  });

  it("indicatorTriggerLabel keeps the full done/error sentence for a single terminal batch", () => {
    expect(indicatorTriggerLabel([batch({ legs: [{ oceanId: "o", oceanName: "n", phase: "succeeded", coralsRequested: 1 }] })])).toBe(
      "Promoted Q3 roadmap priorities",
    );
    expect(indicatorTriggerLabel([batch({ legs: [{ oceanId: "o", oceanName: "n", phase: "failed", error: "x" }] })])).toBe(
      "Couldn't promote Q3 roadmap priorities",
    );
  });

  it("indicatorSummaryPhase reports promoting over error over done", () => {
    expect(indicatorSummaryPhase([batch({ legs: [jobLeg()] }), batch({ legs: [{ oceanId: "o", oceanName: "n", phase: "failed", error: "x" }] })])).toBe("promoting");
    expect(indicatorSummaryPhase([batch({ legs: [{ oceanId: "o", oceanName: "n", phase: "failed", error: "x" }] }), batch({ legs: [{ oceanId: "o", oceanName: "n", phase: "succeeded", coralsRequested: 1 }] })])).toBe("error");
    expect(indicatorSummaryPhase([batch({ legs: [{ oceanId: "o", oceanName: "n", phase: "succeeded", coralsRequested: 1 }] })])).toBe("done");
  });

  it("indicatorExpandAriaLabel names the expand affordance — progress while anything is still running, details once everything landed", () => {
    expect(indicatorExpandAriaLabel([batch({ legs: [jobLeg({ current: 1600, total: 3129 })] })])).toBe(
      "Promoting Q3 roadmap priorities… 1,600 / 3,129. Expand for progress.",
    );
    expect(indicatorExpandAriaLabel([batch({ legs: [{ oceanId: "o", oceanName: "n", phase: "succeeded", coralsRequested: 1 }] })])).toBe(
      "Promoted Q3 roadmap priorities. Expand for details.",
    );
    expect(indicatorExpandAriaLabel([batch({ reefNodeId: "r1", legs: [jobLeg()] }), batch({ reefNodeId: "r2", legs: [jobLeg()] })])).toBe(
      "2 promoting. Expand for progress.",
    );
  });
});

describe("legPhaseWord / succeededCoralLabel / promoteLegHeading / processing stages", () => {
  it("legPhaseWord covers starting, queued, in-progress, done, and failed", () => {
    expect(legPhaseWord(null)).toBe("starting…");
    expect(legPhaseWord(jobLeg({ status: "pending" }))).toBe("queued");
    expect(legPhaseWord(jobLeg({ status: "running" }))).toBe("in progress");
    expect(legPhaseWord({ oceanId: "o", oceanName: "n", phase: "succeeded", coralsRequested: 1 })).toBe("✓ done");
    expect(legPhaseWord({ oceanId: "o", oceanName: "n", phase: "failed", error: "x" })).toBe("✗ failed");
  });

  it("succeededCoralLabel reads 'no items' at zero and pluralizes after", () => {
    expect(succeededCoralLabel(0)).toBe("no items");
    expect(succeededCoralLabel(1)).toBe("1 item");
    expect(succeededCoralLabel(3129)).toBe("3,129 items");
  });

  it("promoteLegHeading only adds the topic title when a bulk promote supplied one", () => {
    expect(promoteLegHeading("Coralswarm Team")).toBe("Coralswarm Team");
    expect(promoteLegHeading("Coralswarm Team", "Q3 roadmap")).toBe("Coralswarm Team · Q3 roadmap");
  });

  it("promoteCardItemTitle hides the topic when the card is already under that topic's heading", () => {
    expect(promoteCardItemTitle("Q3 roadmap priorities", "Q3 roadmap priorities")).toBeUndefined();
    expect(promoteCardItemTitle("Q3 roadmap priorities")).toBeUndefined();
    expect(promoteCardItemTitle("3 topics", "Q3 roadmap priorities")).toBe("Q3 roadmap priorities");
  });

  it("processingStageTitle / processingStageLine keep the modal and the expanded pill on the same copy", () => {
    expect(processingStageTitle("copying")).toBe("Copy evidence");
    expect(processingStageTitle("building")).toBe("Build topics and relationships");
    expect(processingStageTitle("checking")).toBe("Check and resolve relationships");
    expect(processingStageLine({ stage: "copying", total: 1, completed: 1, failed: 0, skipped: 0 })).toBe("✓ Copy evidence: 1/1");
    expect(processingStageLine({ stage: "building", total: 2, completed: 1, failed: 0, skipped: 1 })).toBe(
      "◦ Build topics and relationships: 1/2 · 1 needed no changes",
    );
  });
});


describe("publication processing", () => {
  it("does not finish when copying finishes before derivation", () => {
    const processing = { state: "processing" as const, stages: [
      { stage: "copying" as const, total: 1, completed: 1, failed: 0, skipped: 0 },
      { stage: "building" as const, total: 2, completed: 1, failed: 0, skipped: 0 },
    ] };
    const leg = applyJobUpdate(jobLeg(), jobRes({ status: "done", progress_current: 100, progress_total: 100, processing }));
    expect(leg.phase).toBe("job");
    if (leg.phase === "job") expect(legStatusLabel(leg)).toContain("Building topics and relationships");
    expect(applyJobUpdate(leg, jobRes({ status: "done", processing: { ...processing, state: "ready" } })).phase).toBe("succeeded");
  });
  it("keeps downstream failures visible after the copy succeeded", () => {
    const leg = applyJobUpdate(jobLeg(), jobRes({ status: "done", processing: { state: "attention", stages: [] } }));
    expect(leg.phase).toBe("failed");
  });
});

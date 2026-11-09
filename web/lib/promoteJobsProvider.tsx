"use client";

// Shared, cross-page ownership of in-flight "reef" promotes — the fix behind
// the persistent indicator (`components/PromoteJobsIndicator.tsx`). Mounted
// ONCE, in `app/(app)/layout.tsx`, wrapping `AppShell` — so it (and its poll)
// outlives any one `PromoteModal` mount. Before this, the batch/poll lived
// entirely inside `PromoteModal`'s own React state: closing the modal (the
// ordinary way anyone dismisses it) tore the poll down along with it, so
// nothing survived to show anywhere else, and a job that finished while the
// modal was closed produced no signal until the user manually reopened
// Promote on that exact reef. `PromoteModal` is a CONSUMER of this now
// (`usePromoteJobs()`), not the owner — see its own comments at the call
// sites below.
//
// All the actual decision logic (leg/batch transitions, poll cadence,
// storage schema, indicator copy) is in lib/promoteJob.ts as plain functions
// — this file is the thin React wiring around it: state, the poll effect,
// persistence effect, boot-time rehydration, and auto-dismiss scheduling.

import { createContext, useCallback, useContext, useEffect, useMemo, useRef, useState, type ReactNode } from "react";
import type { PromoteLeg } from "./promoteJob";
import {
  allBatchJobIds,
  batchPhase,
  dismissBatch,
  listPromoteBatchReefIds,
  loadPromoteBatch,
  promoteJobLocalStorage,
  PROMOTE_INDICATOR_AUTO_DISMISS_MS,
  removeBatch,
  savePromoteBatch,
  upsertBatch,
  applyJobPollErrorToBatches,
  applyJobUpdateToBatches,
  type PromoteBatchMap,
} from "./promoteJob";
import { useJobPoll } from "./useJobPoll";
import { useQueryClient } from "@tanstack/react-query";
import { invalidate, invalidationFor } from "./appQueries";

export interface PromoteJobsContextValue {
  batches: PromoteBatchMap;
  /** Start (or replace) the batch for `reefNodeId` — called by
   *  `PromoteModal` right after its `POST /promote` calls settle. A retry
   *  calls this again with a fresh set of legs; see `upsertBatch`'s own
   *  "replace, don't merge" contract. */
  startBatch: (reefNodeId: string, reefTitle: string, legs: PromoteLeg[]) => void;
  /** Acknowledge a terminal batch off the indicator without forgetting it
   *  happened — `PromoteModal` itself never needs this (it always shows the
   *  real state regardless of `dismissed`); only the indicator's own
   *  dismiss controls and the success auto-dismiss timer call it. */
  dismissBatch: (reefNodeId: string) => void;
  /** Fully forget a reef's batch — used by `PromoteModal`'s Retry (which
   *  then starts a brand-new one) and by the indicator's failure dismiss. */
  clearBatch: (reefNodeId: string) => void;
}

const PromoteJobsContext = createContext<PromoteJobsContextValue | null>(null);

export function PromoteJobsProvider({ token, children }: { token: string | null; children: ReactNode }) {
  const [batches, setBatches] = useState<PromoteBatchMap>({});

  // ── Boot-time rehydration ──────────────────────────────────────────────
  // Runs once: finds every `cs:promoteJob:*` entry left in storage — from a
  // hard reload, or simply this provider's first mount after sign-in — and
  // resumes each. Without this, a hard reload would behave exactly like the
  // pre-indicator world (silently drop everything) even though the SAME
  // localStorage-based resilience already exists per-reef; this is just the
  // provider discovering what to resume without already knowing which reefs
  // to look for (`PromoteModal`'s own per-reef resume, before this file
  // existed, only ever had ONE reef id to check: its own `target.reef`).
  const rehydratedRef = useRef(false);
  useEffect(() => {
    if (rehydratedRef.current) return;
    rehydratedRef.current = true;
    const storage = promoteJobLocalStorage();
    if (!storage) return;
    const ids = listPromoteBatchReefIds(storage);
    if (ids.length === 0) return;
    setBatches((cur) => {
      let next = cur;
      for (const reefNodeId of ids) {
        const loaded = loadPromoteBatch(storage, reefNodeId);
        if (loaded) next = upsertBatch(next, reefNodeId, loaded.reefTitle, loaded.legs);
      }
      return next;
    });
  }, []);

  // ── Persistence ──────────────────────────────────────────────────────────
  // Mirrors every batch's current legs to storage on every change —
  // `savePromoteBatch` itself clears the entry once a batch is fully
  // terminal, so a batch that just finished stops being resumable the
  // instant there's nothing left to resume.
  useEffect(() => {
    const storage = promoteJobLocalStorage();
    if (!storage) return;
    for (const b of Object.values(batches)) {
      savePromoteBatch(storage, b.reefNodeId, b.reefTitle, b.legs);
    }
  }, [batches]);

  // ── Polling ──────────────────────────────────────────────────────────────
  // One poll loop for every non-terminal leg across every batch — the
  // multi-job counterpart of what `PromoteModal` used to run for its own
  // single reef. `allBatchJobIds` shrinks as legs land, which tears this
  // down naturally (see `useJobPoll`) with no batch-count bookkeeping here.
  const jobIds = useMemo(() => allBatchJobIds(batches), [batches]);
  useJobPoll(
    token,
    jobIds,
    useCallback((jobId, res) => setBatches((cur) => applyJobUpdateToBatches(cur, jobId, res)), []),
    useCallback((jobId, status) => setBatches((cur) => applyJobPollErrorToBatches(cur, jobId, status)), []),
  );

  // ── Cache ────────────────────────────────────────────────────────────────
  // A leg that lands `succeeded` (at submit, or when its job finishes, even
  // with no modal open) changed its destination ocean, so that ocean's cached
  // reads are refetched. Leg objects are immutable, so a WeakSet of the ones
  // already handled keeps this to once per landing.
  const queryClient = useQueryClient();
  const invalidatedLegs = useRef(new WeakSet<PromoteLeg>());
  useEffect(() => {
    const landed: string[] = [];
    for (const b of Object.values(batches)) {
      for (const leg of b.legs) {
        if (leg.phase !== "succeeded" || invalidatedLegs.current.has(leg)) continue;
        invalidatedLegs.current.add(leg);
        landed.push(leg.oceanId);
      }
    }
    if (landed.length > 0) void invalidate(queryClient, invalidationFor.promote([...new Set(landed)]));
  }, [batches, queryClient]);

  // ── Actions ──────────────────────────────────────────────────────────────
  const autoDismissTimers = useRef<Map<string, ReturnType<typeof setTimeout>>>(new Map());

  const clearAutoDismissTimer = useCallback((reefNodeId: string) => {
    const t = autoDismissTimers.current.get(reefNodeId);
    if (t) clearTimeout(t);
    autoDismissTimers.current.delete(reefNodeId);
  }, []);

  const dismissBatchAction = useCallback(
    (reefNodeId: string) => {
      clearAutoDismissTimer(reefNodeId);
      setBatches((cur) => dismissBatch(cur, reefNodeId));
    },
    [clearAutoDismissTimer],
  );

  const startBatch = useCallback(
    (reefNodeId: string, reefTitle: string, legs: PromoteLeg[]) => {
      // A fresh submit (including a retry) always supersedes whatever
      // countdown was pending for this reef's PREVIOUS batch.
      clearAutoDismissTimer(reefNodeId);
      setBatches((cur) => upsertBatch(cur, reefNodeId, reefTitle, legs));
    },
    [clearAutoDismissTimer],
  );

  const clearBatch = useCallback(
    (reefNodeId: string) => {
      clearAutoDismissTimer(reefNodeId);
      setBatches((cur) => removeBatch(cur, reefNodeId));
      const storage = promoteJobLocalStorage();
      if (storage) storage.removeItem(`cs:promoteJob:${reefNodeId}`);
    },
    [clearAutoDismissTimer],
  );

  // ── Auto-dismiss a batch that lands DONE while nobody's watching ────────
  // Only schedules a timer the first time a batch is seen done-and-not-yet-
  // dismissed (guarded by `autoDismissTimers` already having an entry), so a
  // poll tick for an unrelated batch can't restart another one's countdown.
  // A FAILURE is deliberately excluded — see PROMOTE_INDICATOR_AUTO_DISMISS_MS's
  // own doc comment on why success and failure get different lifetimes.
  useEffect(() => {
    for (const b of Object.values(batches)) {
      if (b.dismissed || batchPhase(b) !== "done") continue;
      if (autoDismissTimers.current.has(b.reefNodeId)) continue;
      const reefNodeId = b.reefNodeId;
      const id = setTimeout(() => {
        autoDismissTimers.current.delete(reefNodeId);
        setBatches((cur) => dismissBatch(cur, reefNodeId));
      }, PROMOTE_INDICATOR_AUTO_DISMISS_MS);
      autoDismissTimers.current.set(reefNodeId, id);
    }
  }, [batches]);

  // Provider-unmount cleanup (e.g. sign-out tears down the whole app shell).
  useEffect(() => {
    const timers = autoDismissTimers.current;
    return () => {
      for (const t of timers.values()) clearTimeout(t);
    };
  }, []);

  const value = useMemo(
    () => ({ batches, startBatch, dismissBatch: dismissBatchAction, clearBatch }),
    [batches, startBatch, dismissBatchAction, clearBatch],
  );

  return <PromoteJobsContext.Provider value={value}>{children}</PromoteJobsContext.Provider>;
}

/** Throws outside a `PromoteJobsProvider` — every real mount of
 *  `PromoteModal` is under `app/(app)/layout.tsx`, which always provides
 *  one; a throw here means a NEW call site was added outside that layout
 *  (or a test/demo harness forgot to wrap it), which is a wiring bug worth
 *  surfacing loudly rather than silently rendering with no persistence and
 *  no indicator. */
export function usePromoteJobs(): PromoteJobsContextValue {
  const ctx = useContext(PromoteJobsContext);
  if (!ctx) throw new Error("usePromoteJobs() must be called within a PromoteJobsProvider");
  return ctx;
}

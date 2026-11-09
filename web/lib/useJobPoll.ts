"use client";

// Polls a set of background job ids (`GET /v1/jobs/{id}`) on a
// self-scheduling, backing-off timer until every one of them is removed from
// `jobIds` by the caller (i.e. the caller has decided it landed a terminal
// status). Uses a self-scheduling `setTimeout` rather than
// `meeting/page.tsx`'s fixed `setInterval` — a promote job's cadence needs to
// SLOW DOWN over elapsed wall-clock time (`nextPromotePollDelayMs`), which a
// fixed-period interval can't express.
//
// Deliberately thin: every actual decision (what changed, whether a leg is
// now terminal, what delay comes next) lives in lib/promoteJob.ts's pure
// functions, which are what's unit tested — this hook is just the
// setTimeout/fetch wiring around them, consistent with this project's
// "no component-render harness" convention (see PromoteModal.tsx's and
// promoteJob.ts's file-level comments).

import { useEffect, useRef } from "react";
import { api, ApiError, type JobStatus } from "./api";
import { nextPromotePollDelayMs } from "./promoteJob";

export function useJobPoll(
  token: string | null,
  jobIds: string[],
  onTick: (jobId: string, res: JobStatus) => void,
  onError: (jobId: string, status: number | undefined) => void,
): void {
  // Held in a ref, not effect deps, so a parent re-render with fresh
  // callback identities (every caller here passes inline closures) doesn't
  // tear down and restart the timer — same reasoning as Modal.tsx's
  // `onCloseRef`. Only `signature` below should ever restart the loop.
  const latest = useRef({ token, jobIds, onTick, onError });
  latest.current = { token, jobIds, onTick, onError };

  // Stable string so the effect only restarts when the SET of ids to poll
  // actually changes (a leg landing terminal removes it from `jobIds`), not
  // on every render that happens to pass a new array of the same ids.
  const signature = jobIds.join(",");

  useEffect(() => {
    if (!token || jobIds.length === 0) return;
    let cancelled = false;
    let timer: ReturnType<typeof setTimeout> | null = null;
    const startedAt = Date.now();

    async function tick() {
      const { token: t, jobIds: ids, onTick: cb, onError: eb } = latest.current;
      if (!t) return;
      await Promise.all(
        ids.map(async (id) => {
          try {
            const res = await api.jobs.get(t, id);
            if (!cancelled) cb(id, res);
          } catch (e) {
            if (cancelled) return;
            eb(id, e instanceof ApiError ? e.status : undefined);
          }
        }),
      );
      if (cancelled) return;
      timer = setTimeout(() => void tick(), nextPromotePollDelayMs(Date.now() - startedAt));
    }

    // First poll fires immediately (a fresh job or a resumed one both want
    // the current status right away, not after a full interval's delay).
    void tick();

    return () => {
      cancelled = true;
      if (timer) clearTimeout(timer);
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [token, signature]);
}

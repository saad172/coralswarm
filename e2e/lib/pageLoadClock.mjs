import { performance } from 'node:perf_hooks';

/** One epoch-anchored monotonic clock for E2E ordering and elapsed policies.
 * Raw wall time is retained separately for server-ISO correlation/forensics.
 * A host clock adjustment must not shorten polls, quiet windows or deadlines.
 * Sources are injectable only for deterministic isolated clock regressions. */
export function createPageLoadClock({ elapsedNow = () => performance.now(), wallNow = () => Date.now() } = {}) {
  const anchor = wallNow();
  const elapsedAnchor = elapsedNow();
  return {
    now: () => anchor + elapsedNow() - elapsedAnchor,
    wallNow,
  };
}

// All browser recorders and their setup phases in this worker share this domain.
export const pageLoadClock = createPageLoadClock();

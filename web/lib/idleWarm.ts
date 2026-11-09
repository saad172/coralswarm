export const IDLE_WARM_COUNT = 3;
const IDLE_TIMEOUT_MS = 2_000;
// Safari has no requestIdleCallback.
const IDLE_FALLBACK_MS = 500;

export type IdleEnv = {
  /** Run `cb` when the browser is idle; returns a cancel. */
  idle: (cb: () => void) => () => void;
  /** Visible, online, and not asking to save data. */
  eligible: () => boolean;
};

export const browserIdleEnv: IdleEnv = {
  idle: (cb) => {
    if (typeof window.requestIdleCallback === "function") {
      const handle = window.requestIdleCallback(cb, { timeout: IDLE_TIMEOUT_MS });
      return () => window.cancelIdleCallback(handle);
    }
    const handle = setTimeout(cb, IDLE_FALLBACK_MS);
    return () => clearTimeout(handle);
  },
  eligible: () => {
    const connection = (navigator as Navigator & { connection?: { saveData?: boolean } }).connection;
    return document.visibilityState !== "hidden" && navigator.onLine && connection?.saveData !== true;
  },
};

export type IdleWarmSteps = {
  warm: (id: string) => Promise<unknown>;
  skip: (id: string) => boolean;
  /** Another read is in flight; wait for the next idle slot instead of competing. */
  busy: () => boolean;
};

/** Warms `ids` in order, one at a time, each in its own idle slot. Stops for
 *  good when the env turns ineligible; the caller restarts it. Returns a cancel. */
export function warmWhenIdle(ids: readonly string[], steps: IdleWarmSteps, env: IdleEnv): () => void {
  let cancelled = false;
  let cancelIdle = () => {};
  const next = (i: number) => {
    cancelIdle = env.idle(() => {
      if (cancelled || !env.eligible()) return;
      if (steps.busy()) return next(i);
      const at = ids.findIndex((id, j) => j >= i && !steps.skip(id));
      if (at < 0) return;
      void steps.warm(ids[at]).then(() => {
        if (!cancelled) next(at + 1);
      });
    });
  };
  next(0);
  return () => {
    cancelled = true;
    cancelIdle();
  };
}

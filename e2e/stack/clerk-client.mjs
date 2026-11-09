// Every Clerk Backend API call the harness makes goes through this wrapper.
// The dev instance rate-limits per 10 s window across every shard, laptop and
// cleanup sharing it, and answers 429 `too_many_requests` with a Retry-After.
import { setTimeout as sleep } from 'node:timers/promises';

/** How long one call waits out rate limiting before its 429 surfaces. */
export const RATE_LIMIT_WAIT_MS = 150_000;

export function isRateLimited(e) {
  const errors = e?.errors;
  return e?.status === 429 || (Array.isArray(errors) && errors.some((x) => x?.code === 'too_many_requests'));
}

/** Wait before retry `attempt` (0-based): the Retry-After Clerk sent, else
 *  1 s doubling to 10 s with jitter so waiting callers do not retry in lockstep. */
export function rateLimitBackoffMs(e, attempt, random = Math.random) {
  if (typeof e?.retryAfter === 'number' && e.retryAfter > 0) return e.retryAfter * 1000;
  return Math.min(1_000 * 2 ** attempt, 10_000) * (0.5 + random() / 2);
}

/** @typedef {{ waitMs?: number; sleep?: (ms: number) => Promise<unknown>; now?: () => number; onWait?: (ms: number) => void }} RetryOptions */

/** A client whose API methods (`users.createUser`, `sessions.getToken`, ...)
 *  retry 429 answers, honoring Retry-After, for at most `waitMs` per call.
 *  Any other error, including 403 `user_quota_exceeded`, surfaces at once.
 *  @template T @param {T} client @param {RetryOptions} [options] @returns {T} */
export function withRateLimitRetry(client, { waitMs = RATE_LIMIT_WAIT_MS, sleep: wait = sleep, now = Date.now, onWait } = {}) {
  const retrying = (method, api) => async (...args) => {
    const deadline = now() + waitMs;
    for (let attempt = 0; ; attempt++) {
      try {
        return await method.apply(api, args);
      } catch (e) {
        if (!isRateLimited(e)) throw e;
        const ms = Math.min(rateLimitBackoffMs(e, attempt), deadline - now());
        if (ms <= 0) throw e;
        onWait?.(ms);
        await wait(ms);
      }
    }
  };
  const retryingApi = (api) => new Proxy(api, {
    get(target, name) {
      const value = Reflect.get(target, name);
      return typeof value === 'function' ? retrying(value, target) : value;
    },
  });
  return new Proxy(client, {
    get(target, name) {
      const value = Reflect.get(target, name);
      return value !== null && typeof value === 'object' ? retryingApi(value) : value;
    },
  });
}

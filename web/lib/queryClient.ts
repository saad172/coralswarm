// W4 — the shared QueryClient.
//
// Plan: docs/plans/request-reliability-and-scale.md §6.4 (decision §12-E).
//
// WHAT ADOPTING THIS BUYS, and why it is worth a dependency: it makes the
// Reefs bug class **inexpressible** rather than fixed. The original failure was
// an effect that depended on the state it set — `enrich` was in the dependency
// list, and the updater always allocated a `new Map(cur)`, so a rejected fetch
// re-fired the effect that produced it. That shape cannot be written with a
// declarative query: the cache key is stable data, not an object identity, so
// there is nothing for a failed fetch to spin on.
//
// It also supplies, by construction, the four things the 22 hand-rolled
// `useEffect` + `api.*` + `useState` blocks each re-implement (and each get
// subtly differently): request dedup, cancel-on-unmount, a retry policy, and
// sequence-number race guards.
//
// DIVISION OF LABOUR WITH W1. This layer owns *what* is fetched and *when it
// is stale*. `lib/transport.ts` owns *whether and when a request goes out* —
// concurrency, timeouts, retry, coalescing, the circuit breaker. They are not
// redundant, and the config below is written to keep them from fighting:
// retries are DISABLED here because the transport already retries with full
// jitter and honours `Retry-After`, and two independent retry layers multiply
// (3 × 3 = 9 attempts for one user action, against a service that is by then
// probably struggling).

import { QueryClient, type DefaultError, type EnsureQueryDataOptions, type FetchQueryOptions, type QueryKey } from "@tanstack/react-query";
import { watchPageReadTiming } from "./pageReadTimingReceipt";
import { pageReadImperativeCompletion } from "./pageReadAuthority";

/** Production must create clients through makeQueryClient. Factory hooks fence
 * observer/cache publication; this public subclass also fences every imperative
 * completion, including callers joining TanStack's raw retryer promise.
 * Inherited fetch/ensureInfiniteQueryData and prefetch methods dispatch through
 * these overrides. Ordinary queries without page-read authority pass through. */
class AppQueryClient extends QueryClient {
  override fetchQuery<TQueryFnData, TError = DefaultError, TData = TQueryFnData, TQueryKey extends QueryKey = QueryKey, TPageParam = never>(
    options: FetchQueryOptions<TQueryFnData, TError, TData, TQueryKey, TPageParam>,
  ): Promise<TData> {
    const defaults = this.defaultQueryOptions(options);
    if (defaults.retry === undefined) defaults.retry = false;
    const query = this.getQueryCache().build(this, defaults);
    const staleTime = typeof defaults.staleTime === "function" ? defaults.staleTime(query) : defaults.staleTime;
    const cached = !query.isStaleByTime(staleTime);
    if (cached && defaults.meta?.pageRead === true) query.setOptions(defaults);
    const completion = pageReadImperativeCompletion(this, query, cached);
    try {
      // Same public fetch/cache branches as TanStack 5.101.4, resolving a
      // functional staleTime once. Query.fetch still owns dedupe and retries.
      const result = cached ? Promise.resolve(query.state.data as TData) : query.fetch(defaults);
      return result.then(completion.validate);
    } finally { completion.finish(query.promise); }
  }

  override ensureQueryData<TQueryFnData, TError = DefaultError, TData = TQueryFnData, TQueryKey extends QueryKey = QueryKey>(
    options: EnsureQueryDataOptions<TQueryFnData, TError, TData, TQueryKey>,
  ): Promise<TData> {
    const defaults = this.defaultQueryOptions(options);
    const query = this.getQueryCache().build(this, defaults);
    if (query.state.data === undefined) return super.ensureQueryData(options); // Delegates to guarded fetchQuery.
    if (defaults.meta?.pageRead === true) query.setOptions(defaults);
    const completion = pageReadImperativeCompletion(this, query, true);
    return super.ensureQueryData(options).then(completion.validate);
  }
}

/** Resource classes and their staleness. */
export const STALE = {
  /** The default for an ordinary read. Thirty seconds means navigating away
   *  and straight back paints from cache with zero requests, which is what
   *  the app felt like it was missing: every screen used to refetch (and
   *  most showed a spinner) on every visit. The rename-appears-to-fail risk
   *  that once argued for zero is closed by the other half of the contract:
   *  every mutation INVALIDATES the keys it affects (lib/queryKeys.ts,
   *  lib/appQueries.ts), so a stale window only ever covers changes made
   *  somewhere else — another tab, another device, a background ingest. */
  READ: 30_000,
  /** Opt-in for a resource that genuinely must be live on every mount (a
   *  value another actor changes and the user acts on immediately). Use it
   *  per query, with the reason next to it. */
  MUTABLE: 0,
  /** Data that only changes via ingest, which the user does not drive from
   *  this screen. Ten seconds is short enough that a background ingest shows
   *  up promptly and long enough to absorb a burst of navigation. */
  INGESTED: 10_000,
  /** A record opened from a list, which the user may have changed from
   *  another surface moments ago. Long enough that hover-then-click is one
   *  request, short enough that a reopen catches that change. */
  OPENED: 10_000,
} as const;

/** Retention for an opened record once nothing shows it, so a session's
 *  chats stay one paint away without the default five-minute drop. */
export const OPENED_GC_TIME = 30 * 60_000;

export function makeQueryClient(): QueryClient {
  const client = new AppQueryClient({
    defaultOptions: {
      queries: {
        // See the module comment: the transport layer is the single retry
        // authority. Stacking retries here would multiply attempts.
        retry: false,
        staleTime: STALE.READ,
        // Refetching every query on every window focus is a request storm on
        // a screen that holds a dozen of them — the exact cost profile this
        // plan exists to remove. Screens that genuinely need freshness on
        // focus opt in individually.
        refetchOnWindowFocus: false,
        // Keep unused data for five minutes so back-navigation is instant
        // without holding a whole ocean's drained edges forever.
        gcTime: 5 * 60_000,
      },
      mutations: {
        retry: false,
      },
    },
  });
  if (process.env.NEXT_PUBLIC_E2E_PAGE_READ_RECEIPTS === "1") watchPageReadTiming(client);
  return client;
}

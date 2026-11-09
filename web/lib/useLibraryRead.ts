"use client";

import { useCallback, useId, useRef } from "react";
import { useQuery } from "@tanstack/react-query";
import { transientFailure } from "./api";
import { TransportError } from "./transport";
import { useReturnRefetch } from "./useReturnRefetch";

/** Transport messages are for logs. The reader sees why the panel failed and what to do. */
export function libraryReadFailure(error: unknown): string {
  if (error instanceof TransportError)
    return error.kind === "timeout"
      ? "Library did not respond in time. Retry in a moment."
      : "Library could not be reached. Check the connection and retry.";
  return error instanceof Error ? error.message : "Library could not verify access. Try again.";
}

/** A settled read is either data or an explicit denial. A denial RESOLVES the
 * query rather than rejecting it, so it replaces the cached result outright;
 * a transient failure rejects, and TanStack keeps the last data beside it. */
type Settled<T> = { data: T } | { denied: string };

/** Stale-while-revalidate Library read.
 *
 * Rendered data is dropped only on an explicit signal: the request is denied
 * (401/403/404 or failed verification), the identity/scope `key` changes, or
 * `enabled` goes false (sign-out). A background revalidation — tab return,
 * reconnect, the visible-only minute tick, `refresh()` — keeps the result on
 * screen; a transient failure keeps it too and reports `refreshError`.
 *
 * The cache entry is scoped to this hook instance AND the full `key`, with no
 * cross-key placeholder, so a key change shows nothing from the previous key
 * in the same render, and a late response for a superseded key lands in a
 * query nobody observes (and is aborted with it). `gcTime: 0` keeps the old
 * no-private-cache intent: an unobserved result is discarded immediately. */
export function useLibraryRead<T>(
  key: string,
  enabled: boolean,
  load: (signal: AbortSignal) => Promise<T>,
) {
  const loader = useRef(load);
  loader.current = load;
  // Two panels may build the same key string for different loaders; the
  // instance id keeps their entries apart.
  const instance = useId();
  const lastRunAt = useRef(0);
  // Each time the read is (re-)enabled it gets a fresh entry, decided during
  // render: a sign-out/sign-in cycle must not re-show the pre-sign-out result,
  // even if its entry has not been collected yet.
  const epoch = useRef({ enabled, value: 0 });
  if (enabled !== epoch.current.enabled)
    epoch.current = { enabled, value: epoch.current.value + (enabled ? 1 : 0) };
  const query = useQuery<Settled<T>>({
    queryKey: ["library-read", instance, key, epoch.current.value],
    enabled,
    gcTime: 0,
    // lib/transport.ts decides whether a request goes out. TanStack's own
    // online tracking would instead PAUSE the read after an `offline` event
    // (process-wide, until `online`), so a re-check would neither run nor
    // fail; here it runs, fails as a transient TransportError, and keeps the
    // last verified result with a note.
    networkMode: "always",
    queryFn: async ({ signal }) => {
      lastRunAt.current = Date.now();
      try {
        return { data: await loader.current(signal) };
      } catch (error) {
        if (signal.aborted || transientFailure(error)) throw error;
        return { denied: libraryReadFailure(error) };
      }
    },
  });
  const settled = enabled ? query.data : undefined;
  const data = settled && "data" in settled ? settled.data : undefined;
  const denied = settled && "denied" in settled ? settled.denied : undefined;
  const failure = enabled && query.error ? libraryReadFailure(query.error) : undefined;
  // Blocking only while there is nothing to show for this key.
  const loading = enabled && data === undefined && (query.isPending || query.isFetching);
  const refetch = query.refetch;
  const live = useRef({ enabled, fetching: query.isFetching });
  live.current = { enabled, fetching: query.isFetching };
  const revalidate = useCallback(() => {
    // Joins a read already in flight instead of restarting it.
    if (live.current.enabled) void refetch({ cancelRefetch: false });
  }, [refetch]);
  const refresh = useCallback(() => {
    if (live.current.enabled) void refetch();
  }, [refetch]);
  useReturnRefetch(revalidate, {
    enabled,
    lastRunAtRef: lastRunAt,
    inFlight: () => live.current.fetching,
    intervalMs: 60_000,
  });
  return {
    data,
    /** Blocking failure: set only when there is no data for this key. */
    error: data === undefined && !loading ? (failure ?? denied) : undefined,
    loading,
    /** A background revalidation is running while data stays on screen. */
    refreshing: data !== undefined && enabled && query.isFetching,
    /** The last revalidation failed transiently; `data` is the last verified result. */
    refreshError: data !== undefined ? failure : undefined,
    refresh,
  };
}

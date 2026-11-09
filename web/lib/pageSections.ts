"use client";

import { useCallback, useEffect, useSyncExternalStore } from "react";
import { useQuery, useQueryClient, type Query, type QueryKey, type UseQueryOptions } from "@tanstack/react-query";
import { apiRequest } from "./api";
import { beginPageReadTiming, admitPageReadTiming } from "./pageReadTimingReceipt";
import { manualPageRefresh } from "./pageManualRefresh";
import { pageReadPriority, hasCriticalPageObserver } from "./pageReadPriority";
import { existingPageRefreshCycles, pageRefreshCycle, pageRefreshInProgress, watchPageRefreshObservers, PageRefreshExpiredError } from "./pageRefreshPolicy";
import { authorizedPageRead, refusePageRead, pageReadToken, verifyPageReadToken, terminalPageReadError, pageReadPublicationBoundary, type PageReadContext } from "./pageReadAuthority";
import { STALE } from "./queryClient";
import { useApp } from "./providers";
import { type RequestPriority } from "./transport";
import { plainPageSectionQuery, type PlainLoadPath } from "./pageShell";

export { typedStored, canonicalSectionParams, pageSectionPath, refreshOwed } from "./pageSectionWire";
export type { PageModel, SectionParams, Stored } from "./pageSectionWire";
import { typedStored, canonicalSectionParams, pageSectionPath, pageSectionReadIdentity, refreshOwed, type PageModel, type SectionParams, type Stored } from "./pageSectionWire";

export type SectionTokenGetter = () => string | null | Promise<string | null>;
export interface SectionQueryOptions { queryKey?: QueryKey; priority?: RequestPriority }

/** Keys carry the verified app scope, never a bearer or a wall-clock timestamp.
 * Polls remain critical reads. TanStack owns observer dedupe and cancellation;
 * transport owns retries, including the credential-safe single 401 refresh. */
function storedSectionQuery<P>(
  model: PageModel,
  scope: string,
  params: SectionParams = {},
  getToken: SectionTokenGetter,
  options: SectionQueryOptions = {},
) {
  const canonical = canonicalSectionParams(params);
  const readIdentity = pageSectionReadIdentity(model, scope, canonical);
  const publication = pageReadPublicationBoundary();
  return {
    queryKey: options.queryKey ?? [model, scope, canonical],
    structuralSharing: process.env.NEXT_PUBLIC_E2E_PAGE_READ_RECEIPTS === "1" ? function(this: object, previous: unknown, value: unknown) {
      const admitted = publication.structuralSharing.call(this, previous, value);
      admitPageReadTiming(this, admitted, performance.now()); // AFTER the unchanged authority guard, BEFORE success dispatch.
      return admitted;
    } : publication.structuralSharing,
    queryFn: async (context: PageReadContext) => {
      publication.observe(context);
      const pageReadTiming = process.env.NEXT_PUBLIC_E2E_PAGE_READ_RECEIPTS === "1"
        ? beginPageReadTiming(context.client, context.queryKey, pageSectionPath(model, canonical)) : undefined;
      const { signal } = context;
      watchPageRefreshObservers(context.client);
      const query = context.queryKey ? context.client?.getQueryCache().find({ queryKey: context.queryKey, exact: true }) : undefined;
      let cycle: ReturnType<typeof pageRefreshCycle> | undefined;
      if (query) {
        // Limits are required only after a real response establishes refresh
        // owed; ordinary warm reads do not depend on scheduler metadata.
        if (options.priority !== "background" || hasCriticalPageObserver(query)) {
          // Refusal clears data but leaves the stopped recovery cycle. An
          // explicit retry must rearm that cycle before its verified response.
          cycle = existingPageRefreshCycles(query)[0] ??
            (refreshOwed(query.state.data as Stored<unknown> | undefined) ? pageRefreshCycle(query, model) : undefined);
        }
        cycle?.beforeRead(Date.now());
      }
      const priority = pageReadPriority(context, cycle ? "critical" : options.priority ?? "critical");
      const read = async () => {
        try {
          const token = await pageReadToken(getToken);
          const body = await apiRequest<unknown>(pageSectionPath(model, canonical), token, { cache: "no-store" }, false,
            { signal, priority: priority.source.current(), prioritySource: priority.source, ...(pageReadTiming ? { pageReadTiming } : {}) });
          verifyPageReadToken(token, await pageReadToken(getToken));
          return typedStored<P>(body, model, canonical);
        } catch (error) {
          if (terminalPageReadError(error)) cycle?.stop();
          else if (!(error instanceof PageRefreshExpiredError) && !signal.aborted) cycle?.failure();
          throw error;
        }
      };
      try {
        return await authorizedPageRead(context, readIdentity, read, {
          publishStored: true,
          withheld: (stored) => stored.computed_at === null,
          verified: (stored) => {
            if (query && refreshOwed(stored) && priority.source.current() === "critical") {
              cycle ??= pageRefreshCycle(query, model);
              cycle.observe(stored, Date.now());
            } else cycle?.observe(stored, Date.now());
          },
        });
      } finally { priority.release(); }
    },
    meta: { pageRead: true, model, scope, readIdentity, priority: options.priority ?? "critical" },
    staleTime: STALE.READ,
    retry: false,
    refetchOnMount: (query) => refreshOwed(query.state.data) && query.getObserversCount() <= 1 ? "always" : true,
    refetchOnWindowFocus: false,
    refetchOnReconnect: false,
    refetchInterval: (query: Query<Stored<P>>) => {
      if (terminalPageReadError(query.state.error) || query.state.error instanceof PageRefreshExpiredError || !refreshOwed(query.state.data)) return false;
      try { return pageRefreshCycle(query, model).interval(Date.now()); }
      catch { return false; } // Missing generated metadata is surfaced by the query, never a render-time throw.
    },
  } satisfies UseQueryOptions<Stored<P>>;
}

/** Plain and materialized reads share the page-query entry point. Plain
 * payloads carry no synthetic Stored certificate. */
export function pageSectionQuery<P>(model: PageModel, scope: string, params: SectionParams, getToken: SectionTokenGetter, options?: SectionQueryOptions): ReturnType<typeof storedSectionQuery<P>>;
export function pageSectionQuery<P>(path: PlainLoadPath, scope: string, params: SectionParams, getToken: SectionTokenGetter, options: { plain: true; public?: boolean; queryKey?: QueryKey }): ReturnType<typeof plainPageSectionQuery<P>>;
export function pageSectionQuery<P>(source: PageModel | PlainLoadPath, scope: string, params: SectionParams, getToken: SectionTokenGetter,
  options: SectionQueryOptions | { plain: true; public?: boolean; queryKey?: QueryKey } = {}) {
  if ("plain" in options) return plainPageSectionQuery<P>(source as PlainLoadPath, scope, params, getToken, options);
  return storedSectionQuery<P>(source as PageModel, scope, params, getToken, options);
}

/** Existing presentational consumers may select the payload, but an uncomputed
 * or withheld envelope always selects undefined, never its empty placeholder. */
export function pagePayloadQuery<P>(
  model: PageModel, scope: string, params: SectionParams,
  getToken: SectionTokenGetter, options: SectionQueryOptions = {},
) {
  return {
    ...pageSectionQuery<P>(model, scope, params, getToken, options),
    select: (stored: Stored<P>): P | undefined => stored.computed_at === null ? undefined : stored.payload,
  };
}

export interface PageSectionState<P> {
  data: P | undefined;
  computedAt: Date | null;
  stale: boolean;
  isPending: boolean;
  isRefreshing: boolean;
  error: Error | null;
  refresh: () => Promise<void>;
}

export function usePageSection<P>(model: PageModel, params: SectionParams = {}, enabled = true, queryKey?: QueryKey): PageSectionState<P> {
  const { queryScope, getAuthToken, authReady } = useApp();
  const qc = useQueryClient();
  const options = pageSectionQuery<P>(model, queryScope, params, getAuthToken, { queryKey });
  const query = useQuery({ ...options, enabled: authReady && enabled });
  const paramKey = JSON.stringify(canonicalSectionParams(params));
  const readIdentity = options.meta.readIdentity;
  const sharedManual = manualPageRefresh(qc, readIdentity);
  const manualState = useSyncExternalStore(sharedManual.subscribe, sharedManual.snapshot, sharedManual.snapshot);
  useEffect(() => authReady && enabled ? sharedManual.observe() : undefined, [sharedManual, authReady, enabled]);
  const refresh = useCallback(async () => {
    if (!authReady || !enabled) return;
    return sharedManual.run(async (signal) => {
      if (signal.aborted) return;
      try {
        const token = await pageReadToken(getAuthToken);
        const path = pageSectionPath(model, JSON.parse(paramKey), true);
        await apiRequest<{ refresh_eta: string | null }>(path, token, { method: "POST" }, false, { priority: "normal", signal });
        verifyPageReadToken(token, await pageReadToken(getAuthToken));
        // A peer may outlive the button that started this action. Re-read one
        // currently admitted key; publishing its real certificate updates all
        // alternate factory observers without another POST or a fake answer.
        const active = qc.getQueryCache().findAll({ predicate: (candidate) => candidate.meta?.readIdentity === readIdentity && hasCriticalPageObserver(candidate) })[0];
        if (!signal.aborted && active) await qc.invalidateQueries({ queryKey: active.queryKey, exact: true }, { cancelRefetch: false });
      } catch (error) {
        if (!signal.aborted && terminalPageReadError(error)) await refusePageRead(qc, readIdentity, error as Error);
        throw error;
      }
    });
  }, [qc, sharedManual, authReady, enabled, getAuthToken, model, paramKey, readIdentity]);
  const stored = query.data;
  const admitted = authReady && enabled;
  const data = admitted && stored?.computed_at ? stored.payload : undefined;
  return {
    data,
    computedAt: admitted && stored?.computed_at ? new Date(stored.computed_at) : null,
    stale: stored?.stale ?? true,
    isPending: admitted && !query.error && data === undefined,
    isRefreshing: admitted && (manualState.busy || query.isFetching ||
      pageRefreshInProgress(qc.getQueryCache().find({ queryKey: options.queryKey, exact: true }))),
    error: admitted ? query.error ?? manualState.error : null,
    refresh,
  };
}

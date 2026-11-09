import type { Query } from "@tanstack/react-query";
import { beginPageReadTiming, admitPageReadTiming } from "./pageReadTimingReceipt";
import { authorizedPageRead, pageReadToken, verifyPageReadToken, terminalPageReadError, pageReadPublicationBoundary, type PageReadContext } from "./pageReadAuthority";
import { existingPageRefreshCycles, retainPageRefreshCycles, pageRefreshCycle, watchPageRefreshObservers, PageRefreshExpiredError } from "./pageRefreshPolicy";
import { apiRequest, type ArchivedTeam, type Graph, type Member, type OceanStats, type TeamMemberRow } from "./api";
import type { RequestPriority } from "./transport";
import { pageReadPriority, hasCriticalPageObserver } from "./pageReadPriority";
import { refreshOwed } from "./pageSectionWire";
import { FAN_IN_LOAD_PATHS } from "./pageShellPaths";
import { STALE } from "./queryClient";
import type { QueryCtx } from "./appQueries";

import { parseOceansIndex as parseWire, type OceansIndex as WireIndex, type OceanComponent as WireComponent } from "./pageSectionTypes/oceans";
export interface OceansPayload {
  oceans: { graph: Graph; counts: OceanStats }[];
  members: Member[];
  team_members: Record<string, TeamMemberRow[]>;
  archived: ArchivedTeam[] | null;
}
export type OceansIndex = WireIndex<OceansPayload>;
export type OceanComponent = WireComponent<OceansPayload>;
export const parseOceansIndex = (body: unknown) => parseWire<OceansPayload>(body);
export const oceansRefreshOwed = (index: OceansIndex | undefined) => index?.components.some((c) => refreshOwed(c.stored)) === true;

export function oceansIndexQueryOptions(ctx: QueryCtx, priority: RequestPriority = "critical") {
  const readIdentity = JSON.stringify([ctx.scope, FAN_IN_LOAD_PATHS.oceansIndex]);
  const publication = pageReadPublicationBoundary();
  return {
    queryKey: ["oceansIndex", ctx.scope] as const,
    structuralSharing: process.env.NEXT_PUBLIC_E2E_PAGE_READ_RECEIPTS === "1" ? function(this: object, previous: unknown, value: unknown) {
      const admitted = publication.structuralSharing.call(this, previous, value);
      admitPageReadTiming(this, admitted, performance.now());
      return admitted;
    } : publication.structuralSharing,
    queryFn: async (context: PageReadContext) => {
      publication.observe(context);
      const pageReadTiming = process.env.NEXT_PUBLIC_E2E_PAGE_READ_RECEIPTS === "1"
        ? beginPageReadTiming(context.client, context.queryKey, FAN_IN_LOAD_PATHS.oceansIndex) : undefined;
      const { signal } = context;
      watchPageRefreshObservers(context.client);
      const query = context.queryKey ? context.client?.getQueryCache().find({ queryKey: context.queryKey, exact: true }) : undefined;
      const required = priority !== "background" || query && hasCriticalPageObserver(query);
      if (query && required) try {
        const cycles = existingPageRefreshCycles(query);
        // Automatic reads stop when every constituent is stopped. A read
        // after that point is a new recovery; sibling polls preserve each
        // other's exhausted budgets while any constituent remains active.
        const restartStopped = cycles.every((cycle) => cycle.stopped);
        for (const cycle of cycles) cycle.beforeRead(Date.now(), restartStopped);
      }
      catch (error) { for (const cycle of existingPageRefreshCycles(query)) cycle.stop(); throw error; }
      const scheduling = pageReadPriority(context, priority);
      try { return await authorizedPageRead(context, readIdentity, async () => {
        try {
          const token = await pageReadToken(ctx.getAuthToken);
          const body = await apiRequest(FAN_IN_LOAD_PATHS.oceansIndex, token, { cache: "no-store" }, false, { signal,
            priority: scheduling.source.current(), prioritySource: scheduling.source, ...(pageReadTiming ? { pageReadTiming } : {}) });
          verifyPageReadToken(token, await pageReadToken(ctx.getAuthToken));
          return parseOceansIndex(body);
        } catch (error) {
          if (query) for (const cycle of existingPageRefreshCycles(query)) {
            if (terminalPageReadError(error)) cycle.stop(); else if (!signal.aborted) cycle.failure();
          }
          throw error;
        }
      }, { publishStored: true, withheld: (index) => index.components.some((c) => c.stored.computed_at === null), verified: (index) => {
        if (!query || !required && !hasCriticalPageObserver(query)) return;
        const owed = index.components.filter((c) => refreshOwed(c.stored));
        retainPageRefreshCycles(query, new Set(owed.map((c) => c.account_id)));
        for (const component of owed) {
          const cycle = pageRefreshCycle(query, "oceans_index", component.account_id);
          cycle.observe(component.stored, Date.now());
        }
      } }); } finally { scheduling.release(); }
    },
    meta: { pageRead: true, model: "oceans_index", scope: ctx.scope, readIdentity, priority },
    staleTime: STALE.READ,
    retry: false,
    refetchOnWindowFocus: false,
    refetchOnReconnect: false,
    refetchInterval: (query: Query<OceansIndex>) => {
      if (terminalPageReadError(query.state.error) || query.state.error instanceof PageRefreshExpiredError) return false;
      const intervals = existingPageRefreshCycles(query).map((cycle) => cycle.interval(Date.now())).filter((ms): ms is number => ms !== false);
      return intervals.length ? Math.min(...intervals) : false;
    },
  };
}

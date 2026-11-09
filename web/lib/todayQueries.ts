import type { GistParams, GistResponse } from "./pageSectionTypes/todayGist";
import { pagePayloadQuery } from "./pageSections";
// Bootstrap selects disabled models before dispatch; refusals remain errors.
import type { QueryCtx } from "./appQueries";
import { type ActivitiesParams, type ActivitiesResponse } from "./journalActivitiesApi";

export function todayActivitiesQueryOptions(ctx: QueryCtx, params: ActivitiesParams) {
  const who = params.who ?? "mine";
  return pagePayloadQuery<ActivitiesResponse>("journal_activities", ctx.scope,
    { from: params.from, to: params.to, who, graph_id: params.graphId, tz: params.tz }, ctx.getAuthToken,
    { queryKey: ["journal", ctx.scope, "activities-today", { from: params.from, to: params.to, who, graphId: params.graphId ?? null, tz: params.tz ?? null }] });
}

/** One period's gist. The sweep recompiles an open day at most hourly, so a
 *  few minutes of staleness is fine; a 404 (flag off) is not retried. */
export function journalGistQueryOptions(ctx: QueryCtx, params: GistParams) {
  const key = {
    granularity: params.granularity ?? "day",
    anchor: params.anchor ?? null,
    who: params.who ?? "mine",
    graphId: params.graphId ?? null,
    tz: params.tz ?? null,
  };
  const options = pagePayloadQuery<GistResponse>("journal_gist", ctx.scope,
    { granularity: key.granularity, anchor: key.anchor, who: key.who, graph_id: key.graphId, tz: key.tz }, ctx.getAuthToken,
    { queryKey: ["journal", ctx.scope, "gist", key] });
  return { ...options, queryFn: async (context: { signal: AbortSignal }) => {
    if (!ctx.getAuthToken()) throw new Error("Not authenticated");
    if (key.who === "team" && !key.graphId) throw new Error("Team gist needs an ocean");
    return options.queryFn(context);
  } };
}

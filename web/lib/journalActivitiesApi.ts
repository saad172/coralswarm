import { pagePayloadQuery } from "./pageSections";
// Typed client for the Today activity cards:
//
//   GET /v1/journal/activities?from=&to=&who=&graph_id=&tz=
//   GET /v1/journal/activities/{key}?graph_id=&tz=
//
// Contract: docs/implementation/journal-activities-api.md. Backend:
// backend/src/services/journal_activities.rs (module doc: grouping, kind
// mapping, access). Both routes answer 404 while TODAY_ACTIVITIES_ENABLED is
// unset on the server, which `ApiError.status === 404` carries here.
//
// The query options follow lib/appQueries.ts: the bearer is read at fetch time
// and never part of a key; the key is `[resource, scope, ...]`. The resource
// is "journal" on purpose, so the existing `appInvalidation.journal()` prefix
// (ingest, promote) refreshes cards together with the hour rows.

import { queryOptions } from "@tanstack/react-query";
import { ApiError } from "./api";
import type { QueryCtx } from "./appQueries";
import { CLIENT_HEADER, clientName } from "./clientHeader";
import { guardedFetch } from "./transport";

export type ActivityKind = "meeting" | "slack" | "agent" | "doc" | "note";
export type ActivityWho = "mine" | "team";

export interface ActivityOwner {
  user_id: string | null;
  name: string | null;
}

export interface ActivityGraph {
  id: string;
  name: string;
}

export interface ActivityTopic {
  node_id: string;
  title: string;
}

export interface Activity {
  /** Opaque. Pair with `graph.id` for the detail read. */
  key: string;
  kind: ActivityKind;
  /** "meeting" | "slack thread" | "agent session" | "doc session" | "note" */
  kind_label: string;
  owner: ActivityOwner;
  /** RFC3339 UTC. */
  start: string;
  end: string;
  graph: ActivityGraph;
  title: string;
  /** One terse line; null when nothing was stored to say it. */
  outcome: string | null;
  /** At most 8 on a card (up to 100 in the detail). */
  people: string[];
  /** Total distinct people, which can exceed `people.length`. */
  people_count: number;
  topics: ActivityTopic[];
  decision_count: number;
  /** Corals (ingested atoms) behind the card. */
  source_count: number;
}

export interface DayActivity {
  /** `YYYY-MM-DD`, a local day in the request's zone. */
  day: string;
  count: number;
}

export interface ActivitiesWindow {
  from: string;
  to: string;
  tz: string;
  who: ActivityWho;
}

export interface ActivitiesResponse {
  /** Sorted by start. */
  activities: Activity[];
  /** Always 14 entries, ascending, ending with the day before `to`. */
  day_activity: DayActivity[];
  /** More than 1000 matched; the earliest 1000 are returned. */
  truncated: boolean;
  window: ActivitiesWindow;
}

export interface ActivitySource {
  /** The card's kind label. */
  kind: string;
  quote: string;
  /** "MEETING · 14:52" */
  meta_label: string;
  source_item_id: string | null;
  atom_id: string | null;
  occurred_at: string | null;
}

export interface ActivityDetail extends Activity {
  decisions: string[];
  /** At most 5. */
  sources: ActivitySource[];
  reefs: ActivityTopic[];
}

export interface ActivitiesParams {
  /** RFC3339; `to` is exclusive; the span is at most 62 days. */
  from: string;
  to: string;
  who?: ActivityWho;
  /** Narrow to one ocean. */
  graphId?: string | null;
  /** IANA zone; the server falls back to the user's stored zone. */
  tz?: string | null;
}

export const ACTIVITY_MAX_WINDOW_DAYS = 62;

/** The query string of a list read. Exported for tests. */
export function activitiesQuery(p: ActivitiesParams): string {
  const qs = new URLSearchParams({ from: p.from, to: p.to, who: p.who ?? "mine" });
  if (p.graphId) qs.set("graph_id", p.graphId);
  if (p.tz) qs.set("tz", p.tz);
  return qs.toString();
}

async function get<T>(path: string, token: string, signal?: AbortSignal): Promise<T> {
  if (!token) throw new Error("Not authenticated");
  const res = await guardedFetch(
    path,
    { method: "GET", cache: "no-store", signal, headers: { Authorization: `Bearer ${token}`, [CLIENT_HEADER]: clientName() } },
    { signal },
  );
  if (!res.ok) {
    let message = `HTTP ${res.status}`;
    try {
      const body: unknown = await res.json();
      if (body && typeof body === "object" && typeof (body as { error?: unknown }).error === "string") {
        message = (body as { error: string }).error;
      }
    } catch {
      /* keep the status text */
    }
    throw new ApiError(message, res.status);
  }
  return (await res.json()) as T;
}

/** `GET /v1/journal/activities`. */
export function fetchJournalActivities(token: string, params: ActivitiesParams, signal?: AbortSignal): Promise<ActivitiesResponse> {
  return get<ActivitiesResponse>(`/v1/journal/activities?${activitiesQuery(params)}`, token, signal);
}

/** `GET /v1/journal/activities/{key}`: the detail side panel. */
export function fetchJournalActivity(
  token: string,
  key: string,
  graphId: string,
  opts?: { tz?: string | null },
  signal?: AbortSignal,
): Promise<ActivityDetail> {
  const qs = new URLSearchParams({ graph_id: graphId });
  if (opts?.tz) qs.set("tz", opts.tz);
  return get<ActivityDetail>(`/v1/journal/activities/${encodeURIComponent(key)}?${qs.toString()}`, token, signal);
}

function bearer(ctx: QueryCtx): string {
  const token = ctx.getAuthToken();
  if (!token) throw new Error("Not authenticated");
  return token;
}

/** A window of cards. A key is a different window, who or ocean, so there is
 *  no `keepPreviousData` (yesterday's cards under today's header are wrong,
 *  not merely stale). */
export function journalActivitiesQueryOptions(ctx: QueryCtx, params: ActivitiesParams) {
  const who = params.who ?? "mine";
  return pagePayloadQuery<ActivitiesResponse>("journal_activities", ctx.scope,
    { from: params.from, to: params.to, who, graph_id: params.graphId, tz: params.tz }, ctx.getAuthToken,
    { queryKey: ["journal", ctx.scope, "activities", { from: params.from, to: params.to, who, graphId: params.graphId ?? null, tz: params.tz ?? null }] });
}

/** One card in full, for the side panel. */
export function journalActivityQueryOptions(ctx: QueryCtx, key: string, graphId: string, tz?: string | null) {
  return queryOptions<ActivityDetail>({
    queryKey: ["journal", ctx.scope, "activity", key, { graphId, tz: tz ?? null }] as const,
    queryFn: async ({ signal }) => fetchJournalActivity(bearer(ctx), key, graphId, { tz }, signal),
  });
}

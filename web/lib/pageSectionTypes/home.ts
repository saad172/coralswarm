import { pageSectionPath } from "../pageSectionWire";
// Typed client for `GET /v1/home/metrics` (the Home dashboard's data).
// Contract: docs/implementation/home-metrics-api.md. The server does the
// local-day bucketing and the source-type grouping; this file only types the
// response and fetches it.

/** The five display groups, in display order (server ids). */
export type SourceGroupId = "chat" | "meetings" | "agent_sessions" | "docs_mail" | "code_tickets";

export const HOME_MIN_DAYS = 7;
export const HOME_MAX_DAYS = 60;
export const HOME_DEFAULT_DAYS = 30;

export interface HomeMetricsWindow {
  /** UTC instant of the first local day's midnight. */
  start: string;
  /** UTC instant of the midnight after today; the window is [start, end). */
  end: string;
  days: number;
  tz: string;
}

export interface HomeKpi {
  total: number;
  /** The preceding window of equal length, for the delta. */
  prior_total: number;
  /** One entry per local day, oldest first; length === window.days. */
  daily: number[];
}

export interface HomeKpis {
  corals_ingested: HomeKpi;
  reefs_formed: HomeKpi;
  questions_asked: HomeKpi;
  agent_sessions: HomeKpi;
  meetings_captured: HomeKpi;
}

export interface HomeGraphDay {
  graph_id: string;
  /** Corals ingested that day, by display group id. Absent groups are 0. */
  by_source_type: Partial<Record<SourceGroupId, number>>;
}

export interface HomeIngestDay {
  /** Local date, YYYY-MM-DD. One entry per day of the window. */
  day: string;
  /** Only oceans with corals that day. */
  by_graph: HomeGraphDay[];
}

export interface HomeOcean {
  graph_id: string;
  name: string;
  /** "member" (private) | "org" | "personal" | "team". */
  kind: string;
  /** A stored colour, when the ocean has one. None do today. */
  hue?: string;
  total: number;
  by_source_type: Partial<Record<SourceGroupId, number>>;
}

export interface HomeSourceGroup {
  id: SourceGroupId;
  label: string;
  /** Raw source types mapped here; `prefix*` entries are families. */
  source_types: string[];
}

export interface HomeMetrics {
  window: HomeMetricsWindow;
  kpis: HomeKpis;
  ingest_by_day: HomeIngestDay[];
  /** Sorted by total, descending. */
  oceans: HomeOcean[];
  /** Window corals by display group; every group is present. */
  source_type_totals: Record<SourceGroupId, number>;
  source_type_groups: HomeSourceGroup[];
  /** Where a raw type with no rule lands. */
  fallback_group: SourceGroupId;
}

export interface HomeMetricsParams {
  days?: number;
  /** IANA zone for the day buckets. Defaults to UTC on the server. */
  tz?: string;
}

/** Whether the viewer's browser zone is a usable `tz` value. */
export function browserTimeZone(): string | undefined {
  try {
    return Intl.DateTimeFormat().resolvedOptions().timeZone || undefined;
  } catch {
    return undefined;
  }
}

export function homeMetricsPath(params: HomeMetricsParams = {}): string {
  const q = new URLSearchParams();
  if (params.days !== undefined) {
    if (!Number.isInteger(params.days) || params.days < HOME_MIN_DAYS || params.days > HOME_MAX_DAYS) {
      throw new Error(`days must be an integer from ${HOME_MIN_DAYS} to ${HOME_MAX_DAYS}`);
    }
    q.set("days", String(params.days));
  }
  if (params.tz) q.set("tz", params.tz);
  const qs = q.toString();
  return pageSectionPath("home_metrics", Object.fromEntries(new URLSearchParams(qs)));
}

/** Percent change against the prior window; null when there is no prior to
 *  compare with (the design hides the delta then). */
export function kpiDeltaPct(kpi: HomeKpi): number | null {
  if (kpi.prior_total <= 0) return null;
  return Math.round(((kpi.total - kpi.prior_total) / kpi.prior_total) * 100);
}


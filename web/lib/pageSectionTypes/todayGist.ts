// Typed client for the Today workstream gist:
//
//   GET /v1/journal/gist?granularity=day|week|month&anchor=YYYY-MM-DD&who=mine|team&graph_id=&tz=
//
// Contract: docs/implementation/journal-gist-api.md. Backend:
// backend/src/services/journal_gist.rs. The route answers 404 while
// TODAY_GIST_ENABLED is unset on the server (and for an ocean the caller cannot
// read), which `ApiError.status === 404` carries here: the panel hides the gist
// and keeps its stats and charts (design-handoff fallback table).
//
// The query options follow lib/appQueries.ts: the bearer is read at fetch time
// and never part of a key; the key is `[resource, scope, ...]`. The resource is
// "journal" on purpose, so the existing `appInvalidation.journal()` prefix
// refreshes the gist together with the hour rows and activity cards.


export type GistGranularity = "day" | "week" | "month";
export type GistWho = "mine" | "team";

/** 3-5 terse results in total. An empty group is `[]`: render no row for it. */
export interface Gist {
  done: string[];
  decided: string[];
  open: string[];
}

export interface GistSpan {
  /** RFC3339 UTC: the first and last captured activity in the period. */
  first_activity: string;
  last_activity: string;
}

export interface GistResponse {
  /** RFC3339 UTC bounds of the period containing `anchor` in `tz`. */
  period_start: string;
  period_end: string;
  /** The period contains now (Today): the gist is "so far". */
  open: boolean;
  who: GistWho;
  granularity: GistGranularity;
  tz: string;
  /** `null` until the sweep has compiled this period (or nothing happened). */
  gist: Gist | null;
  compiled_at: string | null;
  /** `null` when nothing was captured in the period. */
  span: GistSpan | null;
}

export interface GistParams {
  granularity?: GistGranularity;
  /** A local date (`YYYY-MM-DD`) inside the period; default today. */
  anchor?: string | null;
  who?: GistWho;
  /** One ocean. Required for `who: "team"`. */
  graphId?: string | null;
  /** IANA zone; the server falls back to the user's stored zone. */
  tz?: string | null;
}

/** The query string. Exported for tests. */
export function gistQuery(p: GistParams): string {
  const qs = new URLSearchParams({ granularity: p.granularity ?? "day", who: p.who ?? "mine" });
  if (p.anchor) qs.set("anchor", p.anchor);
  if (p.graphId) qs.set("graph_id", p.graphId);
  if (p.tz) qs.set("tz", p.tz);
  return qs.toString();
}

/** The rows the panel renders, in design order, skipping empty groups. */
export function gistRows(gist: Gist | null): { label: "DONE" | "DECIDED" | "OPEN"; items: string[] }[] {
  if (!gist) return [];
  const rows: { label: "DONE" | "DECIDED" | "OPEN"; items: string[] }[] = [
    { label: "DONE", items: gist.done },
    { label: "DECIDED", items: gist.decided },
    { label: "OPEN", items: gist.open },
  ];
  return rows.filter((r) => r.items.length > 0);
}


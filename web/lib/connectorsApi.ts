// Typed generic connector client. Contracts: B1 summary.rs, B2 connect routes,
// B3 scope routes and B4 history routes in connector_catalog.rs.
//
// The backend ships problem CODES and params, never sentences; the words live
// in components/connectors/statusModel.ts, the same split as `reconsentModel`.

import { apiRequest } from "./api";

export type ConnectorStatus =
  | "not_connected"
  | "setting_up"
  | "active"
  | "importing"
  | "needs_attention"
  | "reconnect_needed";

export type SourceStatus = "off" | "active" | "importing" | "paused" | "needs_attention" | "stopped";

export type ScopeShape = "items" | "pick_only" | "everything_only";

/** Where an unfinished setup resumes. */
export type ResumeStep = "sources" | "scope" | "history";

export type ProblemParams = Record<string, string | number>;

export interface Problem {
  /** A stable machine code, e.g. `credential_invalid`, `missing_scopes`. */
  code: string;
  /** The source it belongs to, or null for the whole connector. */
  source: string | null;
  /** ISO timestamp the problem was first recorded. */
  since: string;
  params: ProblemParams;
}

export interface ScopeView {
  mode: "pick" | "everything";
  /** Items picked; null until the backend stores them (B3). */
  count: number | null;
  /** The plural noun the picker counts; null for a source with no items. */
  noun: string | null;
}

/** `supported: false` carries a reason CODE (e.g. a toolkit captures from the
 *  moment you connect), not a sentence. */
export type HistoryView =
  | { supported: true; max_days: number }
  | { supported: false; reason: string };

export interface BackfillView {
  /** null is indeterminate. */
  percent: number | null;
  /** 0 is an automatic, unbounded window. */
  window_days: number;
}

export interface SourceSummary {
  key: string;
  label: string;
  status: SourceStatus;
  enabled: boolean;
  paused: boolean;
  scope_shape: ScopeShape;
  scope: ScopeView | null;
  history: HistoryView;
  history_days: number | null;
  /** Durable acceptance, not ingestion completion. */
  history_requested_at: string | null;
  setup_completed_at: string | null;
  last_captured_at: string | null;
  /** Atoms captured per day over the last seven days, oldest first. */
  week: number[];
  backfill: BackfillView | null;
}

export interface ImportingView {
  source: string;
  window_days: number;
  percent: number | null;
}

export interface ConnectorSummary {
  key: string;
  name: string;
  beta: boolean;
  status: ConnectorStatus;
  sources_on: number;
  sources_total: number;
  last_captured_at: string | null;
  importing: ImportingView | null;
  problem: Problem | null;
  resume_step: ResumeStep | null;
  sources: SourceSummary[];
}

export interface TopicSummary {
  id: string;
  label: string;
  total: number;
  last_7_days: number;
}

export interface ConnectorDetail extends ConnectorSummary {
  captures: CapturePage;
  watched: { threads: import("./api").WatchedThread[]; next_cursor: string | null };
  addin_tokens: { tokens: import("./api").AddinToken[] };
  account_label: string | null;
  connected_at: string | null;
  topics: TopicSummary[];
}

export interface ConnectorList {
  connectors: ConnectorSummary[];
}

export interface ScopeSelection { mode: "pick" | "everything"; item_ids?: string[] }
export interface ScopeItem { id: string; label: string; meta: string | null; selected: boolean }
export interface ScopeItemsPage { items: ScopeItem[]; next_cursor: string | null; total: number | null }
const connectorPath = (p: string) => `/v1/connectors/${encodeURIComponent(p)}`;
const sourcePath = (p: string, s: string) => `${connectorPath(p)}/sources/${encodeURIComponent(s)}`;
const json = (method: string, body: unknown) => ({ method, body: JSON.stringify(body) });
export interface Capture { atom_id: string; source: string; title: string | null; excerpt: string | null; author: string | null; captured_at: string; meta: unknown; topics: TopicSummary[] }
export interface CapturePage { items: Capture[]; next_cursor: string | null }
export const connectorsApi = {
  disconnect: (t: string, p: string) => apiRequest<void>(connectorPath(p), t, { method: "DELETE" }),
  captures: (t: string, p: string, params: { source?: string; cursor?: string }) => apiRequest<CapturePage>(`${connectorPath(p)}/captures?${new URLSearchParams(Object.entries(params).filter((entry): entry is [string, string] => entry[1] !== undefined))}`, t),
  connect: (t: string, p: string, returnTo: "wizard" | "onboarding" = "wizard") => apiRequest<{ authorize_url: string; expires_at: string; pending_id?: string | null }>(`${connectorPath(p)}/connect`, t, json("POST", { return_to: returnTo })),
  complete: (t: string, p: string, pendingId: string) => apiRequest<ConnectorSummary>(`${connectorPath(p)}/connect/complete`, t, json("POST", { pending_id: pendingId })),
  patchSource: (t: string, p: string, s: string, patch: { enabled?: boolean; paused?: boolean }) => apiRequest<SourceSummary>(sourcePath(p, s), t, json("PATCH", patch)),
  scopeItems: (t: string, p: string, s: string, params: { q?: string; cursor?: string } = {}, signal?: AbortSignal) => apiRequest<ScopeItemsPage>(`${sourcePath(p, s)}/scope-items?${new URLSearchParams(Object.entries(params).filter((entry): entry is [string, string] => entry[1] !== undefined))}`, t, undefined, false, { priority: "normal", signal }),
  setScope: (t: string, p: string, s: string, selection: ScopeSelection) => apiRequest<SourceSummary>(`${sourcePath(p, s)}/scope`, t, json("PUT", selection)),
  backfill: (t: string, p: string, s: string, days: 0 | 7 | 30 | 90) => apiRequest<SourceSummary>(`${sourcePath(p, s)}/backfill`, t, json("POST", { days })),
};

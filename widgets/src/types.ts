// Wire shapes mirrored from the backend's real `structuredContent`.
//
// Sources (read these before touching this file):
//   - backend/src/mcp/tools/read.rs
//       CitationDetailOutput, AskOceanOutput, SearchAtomResultOutput,
//       SearchAtomsOutput (the registered #[tool(output_schema = ...)] structs
//       — these are schema-only, but a drift test in that file
//       (`ask_ocean_result_shape_matches_get_task_result_registered_schema`,
//       `search_atoms_output_shape_matches_registered_schema`) asserts every
//       key the real hand-built `json!({...})` response emits is declared
//       here, so the schema is a trustworthy mirror of the runtime shape).
//   - backend/src/services/ask.rs (AskResult, AskMode)
//   - backend/src/routes/queries.rs (synthesis_atom_json, citation_details,
//     score_parts_json)
//   - backend/src/mcp/tools/read.rs get_reef/get_atom/recent_activity bodies
//     (hand-built json!({...}) — no registered output_schema for these three)
//
// Field names/optionality below are intentionally verbatim, not
// widget-convenience-renamed.

export type SourceType =
  | "transcript"
  | "note"
  | "mcp"
  | "email"
  | "chat"
  | "document"
  | "meeting"
  | "coding_session"
  | "slack"
  | "zoom"
  | "meet"
  | "teams_transcript"
  | "notion"
  | "teams_channel"
  | "teams_chat"
  | "gdrive"
  | (string & {});

export interface ScoreParts {
  vector: number;
  keyword: number;
  recency: number;
}

/** One `ask_ocean`/`get_task_result` `citation_details[i]` entry. */
export interface CitationDetail {
  atom_id: string;
  /** Absent (not merely null) when citation_details could not resolve this
   *  id at all (hallucinated id, or outside the caller's authorized scope). */
  source_type?: SourceType;
  /** `null` when content could not be resolved by any fallback tier. */
  content: string | null;
  /** Only present when the id resolved. Guaranteed non-empty when present. */
  title?: string;
  /** Only present when the id resolved. */
  body_omitted?: boolean;
  timestamp?: string | null;
  age_days?: number | null;
  score: number | null;
  score_parts: ScoreParts | null;
  channel_name?: string | null;
  author_name?: string | null;
  reefs?: string[];
  superseded_by?: string | null;
}

export type AskMode = "synthesized" | "sources" | "no_answer";

/** The completed shape of ask_ocean/get_task_result — `run_ask_to_json`'s
 *  success arm (`ask_result_json`). Never carries `status`. */
export interface AskOceanCompleted {
  mode: AskMode;
  answer: string | null;
  citations: string[];
  citation_details: CitationDetail[];
  unreachable_oceans: string[];
  status?: undefined;
}

export type TaskStatus = "working" | "completed" | "failed" | "cancelled";

export interface TaskError {
  code: string;
  message: string;
}

/** The polling envelope get_task_result returns for a non-completed task,
 *  and the immediate reply legacy-era `ask_ocean` returns while a task is
 *  still running (`{task_id, status: "working", poll_with: "get_task_result"}`
 *  is the literal ask_ocean reply shape; `get_task_result` itself never
 *  carries `task_id`, `poll_with` — see get_task_result's own doc). */
export interface AskOceanWorking {
  status: Exclude<TaskStatus, "completed">;
  task_id?: string;
  poll_with?: "get_task_result";
  created_at?: string;
  age_seconds?: number;
  error?: TaskError;
}

/** A caller-side application-level failure (`AskError`), surfaced as an
 *  isError CallToolResult body — no `status`, no `mode`, just `error`. */
export interface AskOceanErrorBody {
  error: TaskError;
  status?: undefined;
  mode?: undefined;
}

/** A TERMINAL polling envelope: `get_task_result` will never produce a
 *  completed result for this task again. `"failed"` carries the real
 *  `TaskError`; `"cancelled"` is reserved by the schema (no task-based
 *  ask_ocean path emits it today) and carries none. Distinguished from
 *  `AskOceanWorking` (which only ever means `status === "working"`) so a
 *  caller can never mistake "stop polling, nothing more is coming" for
 *  "still in flight" — see `isAskOceanWorking`'s own doc for why that
 *  distinction is load-bearing. */
export interface AskOceanTerminal {
  status: "failed" | "cancelled";
  task_id?: string;
  created_at?: string;
  age_seconds?: number;
  error?: TaskError;
}

export type AskOceanOutput = AskOceanCompleted | AskOceanWorking | AskOceanErrorBody;

export function isAskOceanCompleted(v: AskOceanOutput | null | undefined): v is AskOceanCompleted {
  return !!v && v.status === undefined && (v as AskOceanCompleted).mode !== undefined;
}

/** `true` ONLY for `status === "working"` — a poller's cue to keep polling.
 *  `"failed"`/`"cancelled"` are TERMINAL (see `isAskOceanTerminal`): treating
 *  them as "still working" would leave a caller polling a task that will
 *  never complete. */
export function isAskOceanWorking(v: AskOceanOutput | null | undefined): v is AskOceanWorking {
  return !!v && (v as AskOceanWorking).status === "working";
}

export function isAskOceanTerminal(v: AskOceanOutput | null | undefined): v is AskOceanTerminal {
  const status = (v as AskOceanTerminal | undefined)?.status;
  return status === "failed" || status === "cancelled";
}

export function isAskOceanErrorBody(v: AskOceanOutput | null | undefined): v is AskOceanErrorBody {
  return !!v && v.status === undefined && (v as AskOceanErrorBody).error !== undefined && (v as AskOceanCompleted).mode === undefined;
}

/** `get_atom`'s full structuredContent — read only when a citation carries
 *  `body_omitted: true` (per the founder's override; chip expand otherwise
 *  renders inline from `citation_details` alone, never calling get_atom). */
export interface AtomDetail {
  atom_id: string;
  graph_id: string;
  source_type: SourceType;
  source_id: string;
  content: string | null;
  content_truncated: boolean;
  participants: string[] | null;
  timestamp: string | null;
  created_at: string;
  scope: string;
  fts_language: string | null;
  channel_id: string | null;
  channel_name: string | null;
  author_id: string | null;
  author_name: string | null;
  project: string | null;
  mcp_client_name: string | null;
  title: string;
  excerpt: string | null;
  linked_reefs: { node_id: string; type: string; title: string }[];
}

/** One `search_atoms` `results[i]` entry. */
export interface SearchAtomResult {
  atom_id: string;
  ocean_id: string | null;
  score: number;
  score_parts: ScoreParts;
  snippet: string;
  title: string;
  body_omitted: boolean;
  source_type: SourceType;
  channel_name: string | null;
  author_name: string | null;
  project: string | null;
  mcp_client_name: string | null;
  timestamp: string | null;
  age_days: number | null;
  reefs: string[];
  superseded_by?: string | null;
}

export interface SearchAtomsOutput {
  results: SearchAtomResult[];
  unreachable_oceans: string[];
}

/** One `get_reef` `corals[i]` entry. */
export interface ReefCoral {
  atom_id: string;
  source_type: SourceType;
  source_id: string;
  timestamp: string | null;
  participants: string[] | null;
}

/** One `get_reef` `relationships[i]` entry. */
export interface ReefRelationship {
  edge_id: string;
  subject: string;
  predicate: string;
  object: string;
  inferred: boolean;
  confirmed: boolean;
  confidence: number | null;
}

/** `body->'digest'` shape (`workers::digest::regenerate`'s `digest_json`). */
export interface ReefDigestExcerpt {
  atom_id: string;
  excerpt: string;
  contribution: string | null;
}

export interface ReefDigest {
  atoms_key?: string;
  digested_title?: string | null;
  summary: string | null;
  excerpts: ReefDigestExcerpt[];
}

export type ReefType = "entity" | "decision" | "thread" | "reference";

/** `get_reef`'s full structuredContent. */
export interface GetReefOutput {
  reef_id: string;
  ocean_id: string;
  type: ReefType;
  title: string;
  scope: string;
  atom_count: number;
  created_at: string;
  updated_at: string;
  digest: ReefDigest | null;
  corals: ReefCoral[];
  relationships: ReefRelationship[];
}

/** Known `recent_activity` `event_type` values — the ONLY four
 *  `insert_activity_event` call sites in the backend today
 *  (`routes/ingest.rs`, `routes/queries.rs` x2, `services/supersession.rs`,
 *  `routes/nodes.rs` x2). See widgets/README.md "Backend fields needed" —
 *  the design's richer vocabulary (decision/ask/session/conflict/meeting)
 *  does not exist in the real event log yet. Kept as a loose string so an
 *  unrecognized future event_type still renders (generic fallback), not a
 *  union type that would need updating in lockstep with the backend.
 */
export type ActivityEventType =
  | "atom_ingested"
  | "query_run"
  | "supersession_created"
  | "promote"
  | (string & {});

/** One `recent_activity` `events[i]` entry. `payload`'s shape depends on
 *  `event_type` — see each literal's own inline comment. */
export interface ActivityEvent {
  id: string;
  user_id: string;
  graph_id: string | null;
  event_type: ActivityEventType;
  /** Truncated to ACTIVITY_PAYLOAD_CAP bytes server-side; shape depends on
   *  event_type:
   *    atom_ingested       -> { atom_count, chunks_created, source_type }
   *    query_run           -> { citation_count, mode } |
   *                           { citation_count, input_tokens, output_tokens }
   *    supersession_created -> { superseder_id, superseded_id, source? }
   *    promote             -> { promoted_atom_count, source_graph_id } |
   *                           { requested_atom_count, source_graph_id, job_id }
   */
  payload: Record<string, unknown>;
  payload_truncated: boolean;
  created_at: string;
}

export interface RecentActivityOutput {
  events: ActivityEvent[];
  page: number;
  page_size: number;
  has_more: boolean;
}

// ── Tool inputs (window.openai.toolInput) ───────────────────────────────────

export interface AtomFilterFields {
  source_type?: string;
  scope?: string;
  since?: string;
  until?: string;
  project?: string;
  session_id?: string;
}

export interface AskOceanInput extends AtomFilterFields {
  question: string;
  ocean_ids?: string[];
  top_k?: number;
}

export interface SearchAtomsInput extends AtomFilterFields {
  query: string;
  ocean_ids?: string[];
  limit?: number;
}

export interface GetReefInput {
  reef_id: string;
}

export interface RecentActivityInput {
  ocean_id?: string;
  event_type?: string;
  page?: number;
  page_size?: number;
}

export interface GetTaskResultInput {
  task_id: string;
}

export type ToolName =
  | "ask_ocean"
  | "search_atoms"
  | "get_reef"
  | "recent_activity"
  | "get_task_result"
  | "get_atom";

export type TemplateName = "ask_ocean" | "search_atoms" | "get_reef" | "recent_activity";

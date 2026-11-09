import { ApiError, type LibrarySessionKey, type SourceOrigin, type SourceEvidencePage } from "./api";
import { guardedFetch } from "./transport";
import { CLIENT_HEADER, clientName } from "./clientHeader";

export interface LibraryScope {
  graph_ids: string[];
  outcome_versions?: "current" | "all_retained" | null;
  collection?: "meetings" | "agent_sessions" | "work_sessions" | null;
  service_id?: string | null;
  connection?: { kind: "oauth_grant" | "connector_install"; id: string } | null;
  container?: { kind: string; id: string; provider_namespace: string } | null;
  content_kind?: string | null;
  source_item_id?: string | null;
  source_version_id?: string | null;
  node_id?: string | null;
}
export interface LibrarySearchRequest {
  scope: LibraryScope;
  query?: string;
  page_size?: number;
  sources_after?: string | null;
  outcomes_after?: string | null;
  nodes_after?: string | null;
}
export interface LibraryPage<T> {
  items: T[];
  total: number;
  next_cursor: string | null;
}
export interface LibraryFacet {
  key: string;
  value: string | Record<string, unknown>;
  label: string | null;
  source_count: number;
}
export interface LibraryEvidenceCounts {
  unknown_independence_record_count?: number;
  source_support_count: number;
  semantic_atom_count: number;
  unmapped_legacy_chunk_count: number;
}
export interface LibrarySource {
  graph_id: string;
  source_item_id: string;
  source_type: string;
  provider_namespace: string;
  source_id: string;
  title: string | null;
  current_version_id: string | null;
  current_captured_at?: string | null;
  current_extraction_status?: string | null;
  current_extraction_reason?: string | null;
  selected_extraction_reason?: string | null;
  selected_extraction_status?: string | null;
  selected_version_id: string | null;
  origin: SourceOrigin | null;
  version_origin: SourceOrigin | null;
  representation: string | null;
  occurred_at: string | null;
  captured_at: string | null;
  updated_at: string;
  semantic_atom_count: number;
  legacy_chunk_count: number;
  synthetic_demo: boolean;
  /** The selected version's session; servers before COR-94 omit it. */
  session?: LibrarySessionKey | null;
}
export interface LibraryCurrentCapture {
  source_count: number;
  current_version_count: number;
  unavailable_version_count: number;
  pending_version_count: number;
  incomplete_version_count: number;
  current_semantic_atom_count: number;
  latest_captured_at: string | null;
  status: "ready" | "pending" | "incomplete" | "unavailable";
}
export interface LibraryNode {
  graph_id: string;
  current_capture?: LibraryCurrentCapture;
  node_id: string;
  type: string;
  entity_type: string | null;
  title: string | null;
  contribution: LibraryEvidenceCounts;
  wider_context: LibraryEvidenceCounts;
  latest_captured_at: string | null;
  unresolved_disagreement: boolean;
  current_understanding_status: "evidence_only" | "unresolved_disagreement";
  synthetic_demo: boolean;
}
export interface LibraryCitation {
  kind: "source_passage";
  graph_id: string;
  source_item_id: string;
  source_version_id: string;
  passage_id: string;
  semantic_atom_id: string;
  start_char: number;
  end_char: number;
  quote: string;
  quote_truncated: boolean;
  representation: string;
  occurred_at: string | null;
  captured_at: string;
  /** Only Ask `retained_citations` carry these; Library search and temporal
   * citations, and messages stored before they were added, do not. */
  source_type?: string;
  title?: string | null;
  /** Jev cite verdict (#1353): only on Ask citations the answer cited and Jev
   *  checked. Parse with `parseJevVerdict`. */
  jev_verdict?: string;
}
export interface LibraryOutcome {
  graph_id: string;
  source_version_is_current?: boolean;
  semantic_atom_id: string;
  kind: "fact" | "decision" | "action" | "observation";
  statement: string;
  source_item_id: string;
  source_version_id: string;
  origin: SourceOrigin | null;
  occurred_at: string | null;
  captured_at: string;
  nodes: { node_id: string; title: string | null; type: string }[];
  citations: LibraryCitation[];
  nodes_truncated: boolean;
  citations_truncated: boolean;
  /** The session that produced `source_version_id`; servers before COR-94 omit it. */
  session?: LibrarySessionKey | null;
  /** Fact supersession (#1346): only on superseded facts, only when the
   *  backend runs FACT_SUPERSESSION_MODE=live. Parse with `parseSupersession`. */
  supersession?: unknown;
}
export interface LibrarySearchResponse {
  scope: LibraryScope;
  sources: LibraryPage<LibrarySource>;
  outcomes: LibraryPage<LibraryOutcome>;
  nodes: LibraryPage<LibraryNode>;
  prominent_work: LibraryNode[];
  facets: {
    services: LibraryFacet[];
    content_kinds: LibraryFacet[];
    connections: LibraryFacet[];
    containers: LibraryFacet[];
    unknown_origin_count: number;
    truncated: boolean;
  };
}

const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const bounded = (value: unknown, max: number): value is string =>
  typeof value === "string" &&
  value.length > 0 &&
  Array.from(value).length <= max &&
  value.trim() === value &&
  !/[\u0000-\u001f\u007f]/.test(value);

/** Client bounds prevent malformed saved links from minting broad requests.
 * They are not authorization: every read is checked again by the server. */
export function validateLibraryRequest(request: LibrarySearchRequest): void {
  const s = request.scope;
  if (s.outcome_versions != null && !["current", "all_retained"].includes(s.outcome_versions))
    throw new Error("This captured-version selection is invalid.");
  // "work_sessions" is a read-only legacy value: saved chats may still carry it until the work-session data purge.
  if (
    s.collection != null &&
    !["meetings", "agent_sessions", "work_sessions"].includes(s.collection)
  )
    throw new Error("This Library collection is invalid.");
  if (
    !Array.isArray(s.graph_ids) ||
    s.graph_ids.length < 1 ||
    s.graph_ids.length > 32 ||
    s.graph_ids.some((id) => !UUID.test(id))
  )
    throw new Error("Choose an accessible workspace.");
  for (const id of [s.source_item_id, s.source_version_id, s.node_id])
    if (id != null && !UUID.test(id))
      throw new Error("This Library link has an invalid object identifier.");
  if (s.source_version_id && !s.source_item_id)
    throw new Error("A source version needs its source item.");
  if ((s.connection != null || s.container != null) && !s.service_id)
    throw new Error("Choose an origin before an account or container.");
  for (const value of [s.service_id, s.content_kind])
    if (value != null && (!bounded(value, 100) || !/^[a-z0-9_-]+$/.test(value)))
      throw new Error("This Library filter is invalid.");
  if (
    s.connection != null &&
    (!["oauth_grant", "connector_install"].includes(s.connection.kind) ||
      !bounded(s.connection.id, 512))
  )
    throw new Error("This captured-account filter is invalid.");
  if (
    s.container != null &&
    (![s.container.id, s.container.provider_namespace].every((v) => bounded(v, 512)) ||
      !bounded(s.container.kind, 100))
  )
    throw new Error("This container filter is invalid.");
  if (
    request.query != null &&
    (typeof request.query !== "string" || Array.from(request.query).length > 200)
  )
    throw new Error("Search is limited to 200 characters.");
  if (
    request.page_size != null &&
    (!Number.isInteger(request.page_size) || request.page_size < 1 || request.page_size > 50)
  )
    throw new Error("This Library page size is invalid.");
  for (const cursor of [request.sources_after, request.outcomes_after, request.nodes_after])
    if (cursor != null && !bounded(cursor, 8192))
      throw new Error("This saved Library page is invalid.");
}

function record(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}
function uuid(value: unknown): boolean {
  return typeof value === "string" && UUID.test(value);
}
function nullableUuid(value: unknown): boolean {
  return value === null || uuid(value);
}
function nullableString(value: unknown): boolean {
  return value === null || typeof value === "string";
}
function count(value: unknown): boolean {
  return Number.isSafeInteger(value) && typeof value === "number" && value >= 0;
}
function origin(value: unknown): boolean {
  if (value === null) return true;
  if (!record(value)) return false;
  const connection = value.connection;
  const container = value.container;
  return (
    typeof value.service_id === "string" &&
    ["native", "composio", "synthetic"].includes(String(value.transport)) &&
    typeof value.content_kind === "string" &&
    nullableString(value.capture_surface) &&
    (connection === null ||
      (record(connection) &&
        ["oauth_grant", "connector_install"].includes(String(connection.kind)) &&
        typeof connection.id === "string")) &&
    (container === null ||
      (record(container) &&
        ["kind", "id", "provider_namespace"].every((key) => typeof container[key] === "string") &&
        nullableString(container.label)))
  );
}
function evidenceCounts(value: unknown): boolean {
  return (
    record(value) &&
    (value.unknown_independence_record_count === undefined || count(value.unknown_independence_record_count)) &&
    ["source_support_count", "semantic_atom_count", "unmapped_legacy_chunk_count"].every((key) =>
      count(value[key]),
    )
  );
}
function node(value: unknown): boolean {
  return (
    record(value) &&
    ["graph_id", "node_id"].every((key) => uuid(value[key])) &&
    typeof value.type === "string" &&
    nullableString(value.title) &&
    nullableString(value.entity_type) &&
    evidenceCounts(value.contribution) &&
    evidenceCounts(value.wider_context) &&
    nullableString(value.latest_captured_at) &&
    typeof value.unresolved_disagreement === "boolean" &&
    typeof value.synthetic_demo === "boolean" &&
    (value.current_capture === undefined || currentCapture(value.current_capture)) &&
    ["evidence_only", "unresolved_disagreement"].includes(
      String(value.current_understanding_status),
    )
  );
}
function currentCapture(value: unknown): boolean {
  return record(value) && ["source_count", "current_version_count", "unavailable_version_count", "pending_version_count", "incomplete_version_count", "current_semantic_atom_count"].every((key) => count(value[key])) && nullableString(value.latest_captured_at) && ["ready", "pending", "incomplete", "unavailable"].includes(String(value.status));
}
function source(value: unknown): boolean {
  return (
    record(value) &&
    [
      "source_type",
      "provider_namespace",
      "source_id",
      "updated_at",
    ].every((key) => typeof value[key] === "string") &&
    ["graph_id", "source_item_id"].every((key) => uuid(value[key])) &&
    ["current_version_id", "selected_version_id"].every((key) => nullableUuid(value[key])) &&
    [
      "title",
      "representation",
      "occurred_at",
      "captured_at",
    ].every((key) => nullableString(value[key])) &&
    ["current_captured_at", "current_extraction_status", "selected_extraction_status", "current_extraction_reason", "selected_extraction_reason"].every((key) => value[key] === undefined || nullableString(value[key])) &&
    origin(value.origin) &&
    origin(value.version_origin) &&
    count(value.semantic_atom_count) &&
    count(value.legacy_chunk_count) &&
    typeof value.synthetic_demo === "boolean" &&
    validSessionKey(value.session, value.graph_id)
  );
}
/** A key belongs to its own row's graph; anything else rejects the response. */
export function validSessionKey(value: unknown, graphId: unknown): boolean {
  return value === undefined || value === null || (
    record(value) &&
    typeof graphId === "string" &&
    typeof value.graph_id === "string" &&
    value.graph_id.toLowerCase() === graphId.toLowerCase() &&
    bounded(value.provider_namespace, 512) &&
    typeof value.session_id === "string" &&
    value.session_id.length > 0 &&
    Array.from(value.session_id).length <= 512 &&
    !/[\u0000-\u001f\u007f]/.test(value.session_id)
  );
}
function citation(value: unknown): boolean {
  return (
    record(value) &&
    value.kind === "source_passage" &&
    ["graph_id", "source_item_id", "source_version_id", "passage_id", "semantic_atom_id"]
      .every((key) => uuid(value[key])) &&
    [
      "quote",
      "representation",
      "captured_at",
    ].every((key) => typeof value[key] === "string") &&
    count(value.start_char) &&
    count(value.end_char) &&
    Number(value.end_char) > Number(value.start_char) &&
    nullableString(value.occurred_at) &&
    typeof value.quote_truncated === "boolean"
  );
}
function outcome(value: unknown): boolean {
  return (
    record(value) &&
    ["statement", "captured_at"].every((key) => typeof value[key] === "string") &&
    ["graph_id", "semantic_atom_id", "source_item_id", "source_version_id"].every((key) => uuid(value[key])) &&
    (value.source_version_is_current === undefined || typeof value.source_version_is_current === "boolean") &&
    ["fact", "decision", "action", "observation"].includes(String(value.kind)) &&
    origin(value.origin) &&
    nullableString(value.occurred_at) &&
    Array.isArray(value.nodes) &&
    value.nodes.every(
      (ref) =>
        record(ref) &&
        uuid(ref.node_id) &&
        typeof ref.type === "string" &&
        nullableString(ref.title),
    ) &&
    Array.isArray(value.citations) &&
    value.citations.every(citation) &&
    typeof value.nodes_truncated === "boolean" &&
    typeof value.citations_truncated === "boolean" &&
    validSessionKey(value.session, value.graph_id)
  );
}
export function validLibraryFacet(value: unknown): value is LibraryFacet {
  return (
    record(value) &&
    typeof value.key === "string" &&
    (typeof value.value === "string" || record(value.value)) &&
    nullableString(value.label) &&
    count(value.source_count)
  );
}
function validPage(value: unknown, validItem: (item: unknown) => boolean): boolean {
  if (!value || typeof value !== "object") return false;
  const page = value as LibraryPage<unknown>;
  return (
    Array.isArray(page.items) &&
    page.items.every(validItem) &&
    count(page.total) &&
    (page.next_cursor === null || bounded(page.next_cursor, 8192))
  );
}

// P4 normalizes UUID spelling, graph ordering/duplicates and absent null fields.
// Keep the authorized response tied to the requested scope, not a broad fallback.
export function normalizedLibraryScope(scope: LibraryScope): LibraryScope {
  // Scope serialization contains identifiers/selectors only, even if a newer
  // response includes metadata we have not admitted into this client contract.
  const next: LibraryScope = { graph_ids: [...scope.graph_ids], outcome_versions: scope.source_version_id ? "all_retained" : scope.outcome_versions || "all_retained" };
  for (const key of ["collection", "service_id", "content_kind", "source_item_id", "source_version_id", "node_id"] as const)
    if (scope[key] !== undefined) Object.assign(next, { [key]: scope[key] });
  if (scope.connection !== undefined) next.connection = scope.connection ? { kind: scope.connection.kind, id: scope.connection.id } : null;
  if (scope.container !== undefined) next.container = scope.container ? { kind: scope.container.kind, id: scope.container.id, provider_namespace: scope.container.provider_namespace } : null;
  return next;
}
function scopeKey(scope: LibraryScope): string {
  return JSON.stringify([
    [...new Set(scope.graph_ids.map((id) => id.toLowerCase()))].sort(),
    scope.collection ?? null, scope.service_id ?? null, scope.content_kind ?? null,
    scope.connection ? [scope.connection.kind, scope.connection.id] : null,
    scope.container ? [scope.container.kind, scope.container.id, scope.container.provider_namespace] : null,
    scope.source_item_id?.toLowerCase() ?? null,
    scope.source_version_id?.toLowerCase() ?? null,
    scope.node_id?.toLowerCase() ?? null,
    normalizedLibraryScope(scope).outcome_versions,
  ]);
}
export function matchingLibraryScope(value: unknown, requested: LibraryScope): boolean {
  if (!record(value)) return false;
  try {
    const scope = value as unknown as LibraryScope;
    validateLibraryRequest({ scope });
    return scopeKey(scope) === scopeKey(requested);
  } catch { return false; }
}

export async function searchLibrary(
  token: string,
  request: LibrarySearchRequest,
  signal: AbortSignal,
): Promise<LibrarySearchResponse> {
  validateLibraryRequest(request);
  const res = await guardedFetch(
    "/v1/library/search",
    {
      method: "POST",
      cache: "no-store",
      signal,
      headers: {
        Authorization: `Bearer ${token}`,
        "Content-Type": "application/json",
        [CLIENT_HEADER]: clientName(),
      },
      body: JSON.stringify({ ...request, scope: normalizedLibraryScope(request.scope) }),
    },
    { signal },
  );
  if (!res.ok)
    throw new ApiError(
      res.status === 404
        ? "This saved source or node is unavailable or cannot be accessed."
        : res.status === 503
          ? "Library storage is unavailable. Try again when the connection is restored."
          : res.status === 401
            ? "Sign in again to read Library."
            : "Library could not verify this request. Try again.",
      res.status,
    );
  const result = (await res.json()) as LibrarySearchResponse;
  if (
    !result ||
    !matchingLibraryScope(result.scope, request.scope) ||
    !validPage(result.sources, source) ||
    !validPage(result.outcomes, outcome) ||
    !validPage(result.nodes, node) ||
    !Array.isArray(result.prominent_work) ||
    !result.prominent_work.every(node) ||
    ![...result.sources.items, ...result.outcomes.items, ...result.nodes.items, ...result.prominent_work]
      .every((item) => request.scope.graph_ids.some((graph) => graph.toLowerCase() === item.graph_id.toLowerCase())) ||
    !result.sources.items.every((item) =>
      (!request.scope.source_item_id || item.source_item_id.toLowerCase() === request.scope.source_item_id.toLowerCase()) &&
      (!request.scope.source_version_id || item.selected_version_id?.toLowerCase() === request.scope.source_version_id.toLowerCase())) ||
    !result.outcomes.items.every((item) => item.citations.every((ref) =>
      ref.graph_id.toLowerCase() === item.graph_id.toLowerCase() &&
      ref.source_item_id.toLowerCase() === item.source_item_id.toLowerCase() &&
      ref.source_version_id.toLowerCase() === item.source_version_id.toLowerCase() &&
      ref.semantic_atom_id.toLowerCase() === item.semantic_atom_id.toLowerCase())) ||
    !result.outcomes.items.every((item) =>
      (normalizedLibraryScope(request.scope).outcome_versions !== "current" || item.source_version_is_current === true) &&
      (!request.scope.source_item_id || item.source_item_id.toLowerCase() === request.scope.source_item_id.toLowerCase()) &&
      (!request.scope.source_version_id || item.source_version_id.toLowerCase() === request.scope.source_version_id.toLowerCase())) ||
    !result.facets ||
    ![
      result.facets.services,
      result.facets.connections,
      result.facets.containers,
      result.facets.content_kinds,
    ].every((facets) => Array.isArray(facets) && facets.every(validLibraryFacet)) ||
    !count(result.facets.unknown_origin_count) ||
    typeof result.facets.truncated !== "boolean"
  )
    throw new Error("Library returned an unreadable response. Try again.");
  return result;
}


/** Inline evidence is a live authorized read. Never let the generic API client's
 * global 401 refresher admit a different credential behind this identity guard. */
export async function readLibrarySourceEvidence(
  token: string,
  args: { graphId: string; sourceItemId: string; versionId?: string; atomId?: string },
  signal: AbortSignal,
): Promise<SourceEvidencePage> {
  validateLibraryRequest({ scope: {
    graph_ids: [args.graphId], source_item_id: args.sourceItemId, source_version_id: args.versionId,
  } });
  if (args.atomId && !uuid(args.atomId)) throw new Error("This Library passage link is invalid.");
  const params = new URLSearchParams({ graph_id: args.graphId, source_item_id: args.sourceItemId });
  if (args.versionId) params.set("version_id", args.versionId);
  if (args.atomId) params.set("atom_id", args.atomId);
  const res = await guardedFetch(`/v1/sources/evidence?${params}`, {
    cache: "no-store", signal,
    headers: { Authorization: `Bearer ${token}`, [CLIENT_HEADER]: clientName() },
  }, { signal });
  if (!res.ok) throw new ApiError(
    res.status === 401 ? "Sign in again to read this capture." :
      res.status === 404 || res.status === 403 ? "This source is unavailable or cannot be accessed." :
        "Captured text could not be verified. Retry when the connection is restored.",
    res.status,
  );
  return await res.json() as SourceEvidencePage;
}

/** Shared structural validators; authorization remains server-owned. */
export const validLibraryOutcome = outcome;
export const validLibraryOrigin = origin;

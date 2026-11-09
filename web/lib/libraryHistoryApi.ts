import { ApiError, type LibrarySessionKey } from "./api";
import { CLIENT_HEADER, clientName } from "./clientHeader";
import { guardedFetch } from "./transport";
import { matchingLibraryScope, normalizedLibraryScope, validateLibraryRequest, validLibraryFacet, validLibraryOutcome, type LibraryFacet, type LibraryOutcome, type LibraryScope } from "./libraryMemoryApi";

export type LibraryFacetDimension = "services" | "content_kinds" | "connections" | "containers";
export interface LibraryFacetRequest { scope: LibraryScope; dimension: LibraryFacetDimension; option_query?: string; page_size?: number; after?: string | null }
export interface LibraryFacetPage { scope: LibraryScope; dimension: LibraryFacetDimension; items: LibraryFacet[]; total: number; next_cursor: string | null; truncated: boolean }
export interface LibraryNeighborhood {
  scope: LibraryScope;
  node_id: string;
  items: {
    edge_id: string;
    predicate: string;
    origin: "stated" | "inferred";
    status: "confirmed" | "suggested";
    confidence: number;
    reviewed_by: string | null;
    src: { node_id: string; title: string | null; type: string };
    dst: { node_id: string; title: string | null; type: string };
    support: ({ source_item_id: string; source_version_id: string; source_version_is_current?: boolean } | { kind: "legacy_chunk"; graph_id: string; atom_id: string })[];
    support_truncated: boolean;
    /** Edge validity (#1339): RFC 3339 UTC instants, null when the source stated none. v2 anchors a stated date at UTC midnight. */
    valid_from?: string | null;
    valid_until?: string | null;
  }[];
  next_cursor: string | null;
  truncated: boolean;
}
export interface LibraryEffectiveInterval { state_key: string; operation: "assert" | "transition" | "correct" | "withdraw"; value: string | null; valid_from: string | null; valid_until: string | null; recorded_at: string; passage_id: string }
export interface LibrarySideEvidence { outcome: LibraryOutcome; knowledge_time: string | null; effective_time: LibraryEffectiveInterval[]; effective_time_truncated: boolean }
export interface LibraryDisagreementSide { node_id: string; type: string; title: string | null; evidence: LibrarySideEvidence[]; evidence_truncated: boolean }
/** Opposing sides the caller cannot read exist only as `withheld.count`. */
export interface LibraryDisagreementPage {
  graph_id: string;
  node_id: string;
  anchor: LibraryDisagreementSide;
  items: { recorded_at: string; other: LibraryDisagreementSide }[];
  withheld: { count: number; complete: boolean };
  next_cursor: string | null;
  truncated: boolean;
}
export interface LibraryNodeSession extends LibrarySessionKey {
  title: string | null;
  version_count: number;
  last_captured_at: string;
  latest_version_id: string;
}
export interface LibraryNodeSessions {
  scope: LibraryScope;
  node_id: string;
  total: number;
  items: LibraryNodeSession[];
  next_cursor: string | null;
  truncated: boolean;
}
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const record = (value: unknown): value is Record<string, unknown> => !!value && typeof value === "object" && !Array.isArray(value);
const uuid = (value: unknown): value is string => typeof value === "string" && UUID.test(value);
const sameId = (value: unknown, id: string) => typeof value === "string" && value.toLowerCase() === id.toLowerCase();
const text = (value: unknown, max: number): value is string => typeof value === "string" && Array.from(value).length > 0 && Array.from(value).length <= max && !/[\u0000-\u001f\u007f]/.test(value);
const count = (value: unknown) => Number.isSafeInteger(value) && Number(value) >= 0;
const cursor = (value: unknown) => value === null || text(value, 8192);
function pageBounds(pageSize: number | undefined, after: string | null | undefined, max: number) {
  if (pageSize != null && (!Number.isInteger(pageSize) || pageSize < 1 || pageSize > max)) throw new Error("This Library page size is invalid.");
  if (after != null && !text(after, 8192)) throw new Error("This saved Library page is invalid.");
}
async function post(path: string, token: string, body: object, signal: AbortSignal): Promise<unknown> {
  if (!token) throw new Error("Verify the current Library account.");
  const response = await guardedFetch(path, { method: "POST", cache: "no-store", signal, headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json", [CLIENT_HEADER]: clientName() }, body: JSON.stringify(body) }, { signal });
  if (!response.ok) throw new ApiError(response.status === 401 ? "Sign in again to read Library." : response.status === 403 || response.status === 404 ? "This Library scope is unavailable or cannot be accessed." : "Library is unavailable right now. Retry when the connection is restored.", response.status);
  return response.json();
}
function facetValue(value: unknown, dimension: LibraryFacetDimension) {
  if (dimension === "services" || dimension === "content_kinds") return text(value, 100) && /^[a-z0-9_-]+$/.test(value);
  return record(value) && text(value.id, 512) && (dimension === "connections" ? ["oauth_grant", "connector_install"].includes(String(value.kind)) : text(value.kind, 100) && text(value.provider_namespace, 512));
}
export async function discoverLibraryFacets(token: string, request: LibraryFacetRequest, signal: AbortSignal): Promise<LibraryFacetPage> {
  validateLibraryRequest({ scope: request.scope });
  if (!["services", "content_kinds", "connections", "containers"].includes(request.dimension) || (request.option_query != null && (typeof request.option_query !== "string" || Array.from(request.option_query).length > 100))) throw new Error("This facet search is invalid.");
  pageBounds(request.page_size, request.after, 100);
  const result = await post("/v1/library/facets", token, { ...request, scope: normalizedLibraryScope(request.scope) }, signal);
  if (!record(result) || !matchingLibraryScope(result.scope, request.scope) || result.dimension !== request.dimension || !Array.isArray(result.items) || result.items.length > (request.page_size || 50) || !result.items.every((item) => validLibraryFacet(item) && text(item.key, 8192) && facetValue(item.value, request.dimension)) || !count(result.total) || result.items.length > Number(result.total) || !cursor(result.next_cursor) || typeof result.truncated !== "boolean") throw new Error("The captured facet response could not be verified.");
  const valueForScope = (value: LibraryFacet["value"]): LibraryFacet["value"] => {
    if (typeof value === "string") return value;
    return request.dimension === "connections" ? { kind: value.kind, id: value.id } : { kind: value.kind, id: value.id, provider_namespace: value.provider_namespace };
  };
  return { ...(result as unknown as LibraryFacetPage), scope: normalizedLibraryScope(result.scope as unknown as LibraryScope), items: (result.items as LibraryFacet[]).map((item) => ({ key: item.key, label: item.label, source_count: item.source_count, value: valueForScope(item.value) })) };
}
export async function readLibraryNeighborhood(token: string, request: { scope: LibraryScope; node_id: string; page_size?: number; after?: string | null }, signal: AbortSignal): Promise<LibraryNeighborhood> {
  validateLibraryRequest({ scope: request.scope });
  if (!uuid(request.node_id) || request.scope.graph_ids.length !== 1 || (request.scope.node_id && !sameId(request.scope.node_id, request.node_id))) throw new Error("Choose one verified node workspace for its connections.");
  pageBounds(request.page_size, request.after, 50);
  const result = await post("/v1/library/neighborhood", token, { ...request, scope: normalizedLibraryScope(request.scope) }, signal);
  const graph = request.scope.graph_ids[0];
  const allowsLegacySupport = !request.scope.service_id && !request.scope.connection && !request.scope.container && !request.scope.content_kind && !request.scope.source_item_id && !request.scope.source_version_id && !request.scope.collection;
  const endpoint = (value: unknown) => record(value) && uuid(value.node_id) && (value.graph_id === undefined || sameId(value.graph_id, graph)) && (value.title === null || typeof value.title === "string") && typeof value.type === "string";
  const support = (value: unknown) => record(value) && (value.kind === "legacy_chunk" ? allowsLegacySupport && sameId(value.graph_id, graph) && uuid(value.atom_id) : value.kind === undefined && (value.graph_id === undefined || sameId(value.graph_id, graph)) && uuid(value.source_item_id) && uuid(value.source_version_id) && (value.source_version_is_current === undefined || typeof value.source_version_is_current === "boolean") && (!request.scope.source_item_id || sameId(value.source_item_id, request.scope.source_item_id)) && (!request.scope.source_version_id || sameId(value.source_version_id, request.scope.source_version_id)));
  if (!record(result) || !matchingLibraryScope(result.scope, request.scope) || !sameId(result.node_id, request.node_id) || !Array.isArray(result.items) || result.items.length > (request.page_size || 20) || !result.items.every((edge) => record(edge) && uuid(edge.edge_id) && text(edge.predicate, 512) && ["stated", "inferred"].includes(String(edge.origin)) && ["confirmed", "suggested"].includes(String(edge.status)) && typeof edge.confidence === "number" && Number.isFinite(edge.confidence) && edge.confidence >= 0 && edge.confidence <= 1 && (edge.reviewed_by === null || typeof edge.reviewed_by === "string") && endpoint(edge.src) && endpoint(edge.dst) && ((record(edge.src) && sameId(edge.src.node_id, request.node_id)) || (record(edge.dst) && sameId(edge.dst.node_id, request.node_id))) && Array.isArray(edge.support) && edge.support.length > 0 && edge.support.length <= 64 && edge.support.every(support) && typeof edge.support_truncated === "boolean") || !cursor(result.next_cursor) || typeof result.truncated !== "boolean") throw new Error("This node’s retained connections could not be verified.");
  return result as unknown as LibraryNeighborhood;
}
const instant = (value: unknown): value is string => typeof value === "string" && !Number.isNaN(Date.parse(value));
const nullableInstant = (value: unknown) => value === null || instant(value);
export async function readLibraryDisagreements(token: string, request: { graph_id: string; node_id: string; page_size?: number; after?: string | null }, signal: AbortSignal): Promise<LibraryDisagreementPage> {
  if (!uuid(request.graph_id) || !uuid(request.node_id)) throw new Error("Choose one verified node for its disagreement.");
  pageBounds(request.page_size, request.after, 10);
  const result = await post("/v1/library/disagreements", token, request, signal);
  const interval = (value: unknown) => record(value) && text(value.state_key, 64) && ["assert", "transition", "correct", "withdraw"].includes(String(value.operation)) && (value.value === null || text(value.value, 512)) && nullableInstant(value.valid_from) && nullableInstant(value.valid_until) && instant(value.recorded_at) && uuid(value.passage_id);
  const evidence = (value: unknown) => record(value) && validLibraryOutcome(value.outcome) && record(value.outcome) && sameId(value.outcome.graph_id, request.graph_id) && nullableInstant(value.knowledge_time) && Array.isArray(value.effective_time) && value.effective_time.length <= 4 && value.effective_time.every(interval) && typeof value.effective_time_truncated === "boolean";
  const side = (value: unknown) => record(value) && uuid(value.node_id) && typeof value.type === "string" && (value.title === null || typeof value.title === "string") && Array.isArray(value.evidence) && value.evidence.length <= 3 && value.evidence.every(evidence) && typeof value.evidence_truncated === "boolean";
  const others = record(result) && Array.isArray(result.items) ? result.items.map((item) => record(item) && record(item.other) ? String(item.other.node_id).toLowerCase() : "") : [];
  if (!record(result) || !sameId(result.graph_id, request.graph_id) || !sameId(result.node_id, request.node_id) || !side(result.anchor) || !record(result.anchor) || !sameId(result.anchor.node_id, request.node_id) || !Array.isArray(result.items) || result.items.length > (request.page_size || 5) || !result.items.every((item) => record(item) && instant(item.recorded_at) && side(item.other)) || others.includes(request.node_id.toLowerCase()) || new Set(others).size !== others.length || !record(result.withheld) || !count(result.withheld.count) || Number(result.withheld.count) > 200 || typeof result.withheld.complete !== "boolean" || !cursor(result.next_cursor) || typeof result.truncated !== "boolean") throw new Error("This node’s unresolved disagreement could not be verified.");
  return result as unknown as LibraryDisagreementPage;
}
/** Sessions whose permitted versions support the node, newest supporting capture first. */
export async function readLibraryNodeSessions(token: string, request: { scope: LibraryScope; node_id: string; page_size?: number; after?: string | null }, signal: AbortSignal): Promise<LibraryNodeSessions> {
  validateLibraryRequest({ scope: request.scope });
  if (!uuid(request.node_id) || request.scope.graph_ids.length !== 1 || (request.scope.node_id && !sameId(request.scope.node_id, request.node_id))) throw new Error("Choose one verified node workspace for its sessions.");
  pageBounds(request.page_size, request.after, 50);
  const result = await post("/v1/library/node-sessions", token, { ...request, scope: normalizedLibraryScope(request.scope) }, signal);
  const graph = request.scope.graph_ids[0];
  const version = request.scope.source_version_id;
  const session = (value: unknown) => record(value) && sameId(value.graph_id, graph) && text(value.provider_namespace, 512) && value.provider_namespace.trim() === value.provider_namespace && text(value.session_id, 512)
    && (value.title === null || (typeof value.title === "string" && Array.from(value.title).length <= 512)) && count(value.version_count) && Number(value.version_count) > 0
    && typeof value.last_captured_at === "string" && Number.isFinite(Date.parse(value.last_captured_at)) && uuid(value.latest_version_id) && (!version || sameId(value.latest_version_id, version));
  if (!record(result) || !matchingLibraryScope(result.scope, request.scope) || !sameId(result.node_id, request.node_id) || !Array.isArray(result.items) || result.items.length > (request.page_size || 20) || !result.items.every(session) || !count(result.total) || result.items.length > Number(result.total) || !cursor(result.next_cursor) || typeof result.truncated !== "boolean") throw new Error("This node’s captured sessions could not be verified.");
  const items = (result.items as LibraryNodeSession[]).map((s) => ({ graph_id: s.graph_id, provider_namespace: s.provider_namespace, session_id: s.session_id, title: s.title, version_count: s.version_count, last_captured_at: s.last_captured_at, latest_version_id: s.latest_version_id }));
  return { scope: normalizedLibraryScope(result.scope as unknown as LibraryScope), node_id: request.node_id, total: result.total as number, items, next_cursor: result.next_cursor as string | null, truncated: result.truncated };
}

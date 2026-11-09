import { ApiError, type MeetingEvidencePage, type SourceEvidencePage, type NodeMeetingAtomsPage, type SourceItemsPage } from "./api";
import { guardedFetch } from "./transport";
import { CLIENT_HEADER, clientName } from "./clientHeader";

export interface EvidenceReadArgs {
  graphId: string;
  sourceItemId?: string;
  sourceId?: string;
  versionId?: string;
  atomId?: string;
  before?: string;
  atomsAfter?: string;
}
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const isRecord = (value: unknown): value is Record<string, unknown> => !!value && typeof value === "object" && !Array.isArray(value);
const uuid = (value: unknown) => typeof value === "string" && UUID.test(value);
const nullableString = (value: unknown) => value === null || typeof value === "string";
const bounded = (value: unknown, max: number) => typeof value === "string" && value.length > 0 && Array.from(value).length <= max && !/[\u0000-\u001f\u007f]/.test(value);
const sameId = (a: unknown, b: string) => typeof a === "string" && a.toLowerCase() === b.toLowerCase();

function checkArgs(args: EvidenceReadArgs) {
  if (!uuid(args.graphId) || (!args.sourceItemId && !args.sourceId) || (!!args.sourceItemId && !!args.sourceId)) throw new Error("This evidence link has an invalid source scope.");
  for (const id of [args.sourceItemId, args.versionId, args.atomId]) if (id != null && !uuid(id)) throw new Error("This evidence link has an invalid retained identifier.");
  if (args.sourceId != null && !bounded(args.sourceId, 1024)) throw new Error("This meeting source identifier is invalid.");
  for (const cursor of [args.before, args.atomsAfter]) if (cursor != null && !bounded(cursor, 8192)) throw new Error("This retained evidence page is invalid.");
}
async function fixedRequest(path: string, token: string, signal: AbortSignal, init?: RequestInit): Promise<unknown> {
  if (!token) throw new Error("Sign in to read this evidence.");
  const res = await guardedFetch(path, { ...init, cache: "no-store", signal, headers: { Authorization: `Bearer ${token}`, [CLIENT_HEADER]: clientName(), ...(init?.body ? { "Content-Type": "application/json" } : {}) } }, { signal });
  if (!res.ok) throw new ApiError(res.status === 401 ? "Sign in again to read this evidence." : res.status === 403 || res.status === 404 ? "This evidence is unavailable or cannot be accessed." : "Evidence could not be verified. Retry when the connection is restored.", res.status);
  return res.status === 204 ? null : await res.json();
}
function validPassage(value: unknown): boolean {
  return isRecord(value) && uuid(value.passage_id) && typeof value.quote === "string" && Number.isInteger(value.start_char) && Number.isInteger(value.end_char) && Number(value.start_char) >= 0 && Number(value.end_char) > Number(value.start_char);
}
function validVersion(value: unknown, graph: string): boolean {
  if (!isRecord(value)) return false;
  return uuid(value.version_id) && typeof value.captured_at === "string" && typeof value.is_legacy_baseline === "boolean" && nullableString(value.raw_text) && ["available", "unavailable"].includes(String(value.text_status)) && ["not_started", "pending", "running", "done", "failed", "blocked"].includes(String(value.extraction_status)) && nullableString(value.extraction_error) && typeof value.can_retry === "boolean" && (value.atoms_next_cursor === null || bounded(value.atoms_next_cursor, 8192)) && Array.isArray(value.atoms) && value.atoms.every((atom) => isRecord(atom) && uuid(atom.atom_id) && ["fact", "decision", "action", "observation"].includes(String(atom.kind)) && typeof atom.statement === "string" && Array.isArray(atom.passages) && atom.passages.every(validPassage) && Array.isArray(atom.nodes) && atom.nodes.every((node) => isRecord(node) && uuid(node.node_id) && sameId(node.graph_id, graph) && nullableString(node.title) && typeof node.type === "string"));
}
export function assertEvidenceResponse(value: unknown, args: EvidenceReadArgs): asserts value is MeetingEvidencePage | SourceEvidencePage {
  if (!isRecord(value) || !sameId(value.graph_id, args.graphId) || (args.sourceItemId ? !sameId(value.source_item_id, args.sourceItemId) : value.source_id !== args.sourceId) || !Array.isArray(value.versions) || !value.versions.every((version) => isRecord(version) && uuid(version.version_id) && typeof version.captured_at === "string" && typeof version.is_current === "boolean" && typeof version.is_legacy_baseline === "boolean") || (value.next_cursor !== null && !bounded(value.next_cursor, 8192)) || (value.selected_version !== null && !validVersion(value.selected_version, args.graphId))) throw new Error("The retained evidence response could not be matched to this source.");
  if (args.versionId && (!isRecord(value.selected_version) || !sameId(value.selected_version.version_id, args.versionId))) throw new Error("The selected retained version is unavailable.");
  if (args.sourceItemId && (!isRecord(value.source) || !sameId(value.source.source_item_id, args.sourceItemId) || (value.source.graph_id !== undefined && !sameId(value.source.graph_id, args.graphId)))) throw new Error("The retained source belongs to a different scope.");
  if (args.sourceItemId && isRecord(value.selected_version) && value.selected_version.source_item_id != null && !sameId(value.selected_version.source_item_id, args.sourceItemId)) throw new Error("The retained version belongs to a different source.");
  if (isRecord(value.selected_version) && value.selected_version.graph_id !== undefined && !sameId(value.selected_version.graph_id, args.graphId)) throw new Error("The retained version belongs to a different workspace.");
}

/** Fixed request credential: never invoke the generic client's global401
 * refresher, which may have moved on to another identity or selected scope. */
export async function readP6Evidence(token: string, args: EvidenceReadArgs, signal: AbortSignal): Promise<MeetingEvidencePage | SourceEvidencePage> {
  checkArgs(args);
  const params = new URLSearchParams({ graph_id: args.graphId, ...(args.sourceItemId ? { source_item_id: args.sourceItemId } : { source_id: args.sourceId || "" }) });
  for (const [key, value] of [["version_id", args.versionId], ["atom_id", args.atomId], ["before", args.before], ["atoms_after", args.atomsAfter]] as const) if (value) params.set(key, value);
  const value = await fixedRequest(`/v1/${args.sourceItemId ? "sources" : "meetings"}/evidence?${params}`, token, signal);
  assertEvidenceResponse(value, args);
  return value;
}
export async function retryP6Evidence(token: string, args: EvidenceReadArgs & { versionId: string }, signal: AbortSignal): Promise<{ accepted: boolean }> {
  checkArgs(args);
  const value = await fixedRequest(`/v1/${args.sourceItemId ? "sources" : "meetings"}/evidence/retry`, token, signal, { method: "POST", body: JSON.stringify({ graph_id: args.graphId, version_id: args.versionId, ...(args.sourceItemId ? { source_item_id: args.sourceItemId } : { source_id: args.sourceId }) }) });
  if (!isRecord(value) || typeof value.accepted !== "boolean") throw new Error("Extraction status could not be verified.");
  return { accepted: value.accepted };
}
export async function readP6NodeAtoms(token: string, args: { graphId: string; nodeId: string; before?: string }, signal: AbortSignal): Promise<NodeMeetingAtomsPage> {
  if (!uuid(args.graphId) || !uuid(args.nodeId) || (args.before != null && !bounded(args.before, 8192))) throw new Error("This node evidence link is invalid.");
  const params = new URLSearchParams({ graph_id: args.graphId });
  if (args.before) params.set("before", args.before);
  const value = await fixedRequest(`/v1/nodes/${encodeURIComponent(args.nodeId)}/semantic-atoms?${params}`, token, signal);
  if (!isRecord(value) || !sameId(value.graph_id, args.graphId) || !sameId(value.node_id, args.nodeId) || !Array.isArray(value.items) || !value.items.every((atom) => isRecord(atom) && uuid(atom.atom_id) && uuid(atom.source_version_id) && (atom.source_item_id == null || uuid(atom.source_item_id)) && typeof atom.statement === "string" && typeof atom.captured_at === "string" && Array.isArray(atom.passages) && atom.passages.every(validPassage)) || (value.next_cursor !== null && !bounded(value.next_cursor, 8192))) throw new Error("The node evidence response could not be verified.");
  return value as unknown as NodeMeetingAtomsPage;
}
export async function readP6SourceItems(token: string, args: { graphId: string; before?: string; atomId?: string }, signal: AbortSignal): Promise<SourceItemsPage> {
  if (!uuid(args.graphId) || (args.atomId != null && !uuid(args.atomId)) || (args.before != null && !bounded(args.before, 8192))) throw new Error("This source inventory link is invalid.");
  const params = new URLSearchParams({ graph_id: args.graphId });
  if (args.before) params.set("before", args.before);
  if (args.atomId) params.set("atom_id", args.atomId);
  const value = await fixedRequest(`/v1/sources/items?${params}`, token, signal);
  if (!isRecord(value) || !sameId(value.graph_id, args.graphId) || !Array.isArray(value.items) || !value.items.every((item) => isRecord(item) && sameId(item.graph_id, args.graphId) && uuid(item.source_item_id) && (item.current_version_id === null || uuid(item.current_version_id)) && (item.mapped_version_id === null || uuid(item.mapped_version_id))) || (value.next_cursor !== null && !bounded(value.next_cursor, 8192))) throw new Error("The source inventory response could not be verified.");
  return value as unknown as SourceItemsPage;
}


export interface RetainedSourcePointer { graph_id: string; source_item_id: string; source_version_id: string; checkpoint_atom_id?: string | null }
export interface LegacyCaptureProjection {
  projection_status: "unverified_lineage";
  source_items: RetainedSourcePointer[];
  occurred_at: string | null;
}
/** Only the proven source mapping is admitted. Unattributed legacy descriptors,
 * summaries, and parent claims are never a fallback for this projection. */
export async function readP6LegacyCapture(token: string, args: { graphId: string; id: string }, signal: AbortSignal): Promise<LegacyCaptureProjection> {
  if (!uuid(args.graphId) || !bounded(args.id, 1024)) throw new Error("This capture link has an invalid scope.");
  const params = new URLSearchParams({ graph_id: args.graphId, source_id: args.id });
  const value = await fixedRequest(`/v1/meetings/summary?${params}`, token, signal);
  return parseP6LegacyCapture(value, args);
}

export function parseP6LegacyCapture(value: unknown, args: { graphId: string; id: string }): LegacyCaptureProjection {
  if (!isRecord(value) || value.projection_status !== "unverified_lineage" || (value.graph_id !== undefined && !sameId(value.graph_id, args.graphId)) || value.source_id !== args.id || !Array.isArray(value.source_items) || value.source_items.length > 200 || !value.source_items.every((item) => isRecord(item) && sameId(item.graph_id, args.graphId) && uuid(item.source_item_id) && uuid(item.source_version_id) && (item.checkpoint_atom_id == null || uuid(item.checkpoint_atom_id)))) throw new Error("This capture’s retained source mapping could not be verified.");
  if (value.occurred_at !== undefined && !nullableString(value.occurred_at)) throw new Error("This capture’s event time could not be verified.");
  // Project only the narrow admitted fields, even if an older server also sent
  // a private title/body or future descriptors we have not yet audited.
  const sourceItems = (value.source_items as Record<string, unknown>[]).map((item): RetainedSourcePointer => ({ graph_id: item.graph_id as string, source_item_id: item.source_item_id as string, source_version_id: item.source_version_id as string, ...(item.checkpoint_atom_id !== undefined ? { checkpoint_atom_id: item.checkpoint_atom_id as string | null } : {}) }));
  return { projection_status: "unverified_lineage", source_items: sourceItems, occurred_at: (value.occurred_at ?? null) as string | null };
}


export async function resolveP6NodeGraph(token: string, nodeId: string, graphId: string | undefined, signal: AbortSignal): Promise<string> {
  if (!uuid(nodeId) || (graphId != null && !uuid(graphId))) throw new Error("This node link has an invalid scope.");
  const value = await fixedRequest(`/v1/nodes/${encodeURIComponent(nodeId)}`, token, signal);
  if (!isRecord(value) || !sameId(value.node_id, nodeId) || !uuid(value.graph_id) || (graphId && !sameId(value.graph_id, graphId))) throw new Error("This node’s workspace could not be verified.");
  // The compatibility route proves only the address. Never project its stored
  // title, body, aliases, digest or synthesized timeline into the new detail.
  return value.graph_id as string;
}

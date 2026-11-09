import { afterEach, expect, it, vi } from "vitest";
import { guardedFetch } from "./transport";
import { setTokenRefresher } from "./api";
import { discoverLibraryFacets, readLibraryDisagreements, readLibraryNeighborhood, readLibraryNodeSessions, type LibraryDisagreementPage, type LibraryDisagreementSide } from "./libraryHistoryApi";
vi.mock("./transport", async () => ({ ...(await vi.importActual<typeof import("./transport")>("./transport")), guardedFetch: vi.fn() }));
const graph = "11111111-1111-4111-8111-111111111111";
const node = "22222222-2222-4222-8222-222222222222";
const item = "33333333-3333-4333-8333-333333333333";
const version = "44444444-4444-4444-8444-444444444444";
const scope = { graph_ids: [graph], node_id: node, outcome_versions: "current" as const };
const signal = () => new AbortController().signal;
function reply(value: unknown, status = 200) { vi.mocked(guardedFetch).mockResolvedValue(new Response(JSON.stringify(value), { status })); }
function neighborhood() { return { scope, node_id: node, items: [{ edge_id: version, predicate: "depends_on", origin: "inferred", status: "confirmed", confidence: 0.8, reviewed_by: null, src: { node_id: node, title: null, type: "entity" }, dst: { node_id: item, title: "Shipping", type: "entity" }, support: [{ source_item_id: item, source_version_id: version, source_version_is_current: false }], support_truncated: false }], next_cursor: null, truncated: false }; }
afterEach(() => { setTokenRefresher(null); vi.resetAllMocks(); });

it("discovers beyond the summary cap with literal option search, cursor and no-store", async () => {
  reply({ scope, dimension: "services", items: [{ key: "last-option", value: "linear", label: "Linear", source_count: 71 }], total: 237, next_cursor: "opaque/+?", truncated: true });
  const result = await discoverLibraryFacets("fixed", { scope, dimension: "services", option_query: "literal / + &", after: "page-after-first100", page_size: 50 }, signal());
  expect(result.total).toBe(237); expect(result.items[0].source_count).toBe(71);
  const [path, init] = vi.mocked(guardedFetch).mock.calls[0];
  expect(path).toBe("/v1/library/facets"); expect(init).toMatchObject({ cache: "no-store", headers: { Authorization: "Bearer fixed" } });
  expect(JSON.parse(String(init?.body))).toMatchObject({ scope, option_query: "literal / + &", after: "page-after-first100" });
});
it.each(["scope", "dimension", "identity"])("rejects a mismatched %s facet response", async (kind) => {
  const value = { scope: { ...scope, graph_ids: [...scope.graph_ids] }, dimension: "connections", items: [{ key: "connection", value: { kind: "oauth_grant", id: "connection" }, label: null, source_count: 1 }], total: 1, next_cursor: null, truncated: false };
  if (kind === "scope") Object.assign(value.scope, { outcome_versions: "all_retained" });
  if (kind === "dimension") value.dimension = "services";
  if (kind === "identity") value.items[0].value.kind = "guessed";
  reply(value);
  await expect(discoverLibraryFacets("fixed", { scope, dimension: "connections" }, signal())).rejects.toThrow(/verified/);
});
it("refuses oversized option queries/cursors before any request", async () => {
  await expect(discoverLibraryFacets("fixed", { scope, dimension: "services", option_query: "😀".repeat(101) }, signal())).rejects.toThrow();
  await expect(discoverLibraryFacets("fixed", { scope, dimension: "services", after: "x".repeat(8193) }, signal())).rejects.toThrow();
  expect(guardedFetch).not.toHaveBeenCalled();
});
it("projects option values to identifiers only before they can enter filter URLs", async () => {
  reply({ scope, dimension: "connections", items: [{ key: "saved", value: { kind: "oauth_grant", id: "connection", title: "Private extra metadata", body: "Never persist" }, label: "Readable connection", source_count: 1 }], total: 1, next_cursor: null, truncated: false });
  const result = await discoverLibraryFacets("fixed", { scope, dimension: "connections" }, signal());
  expect(result.items[0].value).toEqual({ kind: "oauth_grant", id: "connection" });
});
it("reads retained typed connections in current-outcome mode without converting support flags into validity", async () => {
  reply(neighborhood());
  const result = await readLibraryNeighborhood("fixed", { scope, node_id: node, after: "edge-page" }, signal());
  expect(result.items[0].support[0]).toMatchObject({ source_version_is_current: false });
  expect(JSON.parse(String(vi.mocked(guardedFetch).mock.calls[0][1]?.body))).toMatchObject({ scope, node_id: node, after: "edge-page" });
});
it.each(["exact item", "exact version", "connector", "collection"])("refuses unmapped legacy support in a narrowed %s scope", async (kind) => {
  const narrowed = { ...scope, ...(kind === "exact item" ? { source_item_id: item } : kind === "exact version" ? { source_item_id: item, source_version_id: version } : kind === "connector" ? { service_id: "linear" } : { collection: "agent_sessions" as const }) };
  const value = neighborhood(); Object.assign(value, { scope: narrowed });
  Object.assign(value.items[0], { support: [{ kind: "legacy_chunk", graph_id: graph, atom_id: item }] });
  reply(value);
  await expect(readLibraryNeighborhood("fixed", { scope: narrowed, node_id: node }, signal())).rejects.toThrow(/verified/);
});
it("retains genuine unmapped legacy support in explicitly broad node context", async () => {
  const value = neighborhood(); Object.assign(value.items[0], { support: [{ kind: "legacy_chunk", graph_id: graph, atom_id: item }] });
  reply(value);
  await expect(readLibraryNeighborhood("fixed", { scope, node_id: node }, signal())).resolves.toMatchObject({ items: [{ support: [{ kind: "legacy_chunk", atom_id: item }] }] });
});
it.each(["node", "adjacency", "support", "graph", "kind", "confidence"])("rejects unverified %s neighborhood payloads", async (kind) => {
  const value = neighborhood();
  if (kind === "node") value.node_id = item;
  if (kind === "adjacency") value.items[0].src.node_id = version;
  if (kind === "support") value.items[0].support = [];
  if (kind === "graph") Object.assign(value.items[0].support[0], { graph_id: item });
  if (kind === "kind") Object.assign(value.items[0].support[0], { kind: "guess" });
  if (kind === "confidence") value.items[0].confidence = 1.5;
  reply(value);
  await expect(readLibraryNeighborhood("fixed", { scope, node_id: node }, signal())).rejects.toThrow(/verified/);
});
it.each([401, 403, 404, 503])("fails closed on%s with no global refresher or legacy fallback", async (status) => {
  const refresh = vi.fn(async () => "foreign"); setTokenRefresher(refresh); reply({}, status);
  await expect(readLibraryNeighborhood("fixed", { scope, node_id: node }, signal())).rejects.toMatchObject({ status });
  expect(refresh).not.toHaveBeenCalled(); expect(guardedFetch).toHaveBeenCalledTimes(1);
});

const other = "55555555-5555-4555-8555-555555555555";
function side(id: string, title: string | null): LibraryDisagreementSide {
  return { node_id: id, type: "decision", title, evidence_truncated: false, evidence: [{ knowledge_time: null, effective_time: [], effective_time_truncated: false, outcome: { graph_id: graph, semantic_atom_id: id, source_item_id: item, source_version_id: version, kind: "decision", statement: "Ship in March.", origin: null, occurred_at: null, captured_at: "2026-10-12T00:00:00Z", nodes: [], citations: [], nodes_truncated: false, citations_truncated: false } }] };
}
function disagreement(): LibraryDisagreementPage {
  return { graph_id: graph, node_id: node, anchor: side(node, "Atlas"), items: [{ recorded_at: "2026-09-01T00:00:00Z", other: side(other, null) }], withheld: { count: 1, complete: true }, next_cursor: null, truncated: false };
}
it("reads one node's disagreement with the exact bounded request and no-store", async () => {
  reply(disagreement());
  await expect(readLibraryDisagreements("fixed", { graph_id: graph, node_id: node, page_size: 5, after: "pair-page" }, signal())).resolves.toEqual(disagreement());
  const [path, init] = vi.mocked(guardedFetch).mock.calls[0];
  expect(path).toBe("/v1/library/disagreements"); expect(init).toMatchObject({ method: "POST", cache: "no-store", headers: { Authorization: "Bearer fixed" } });
  expect(JSON.parse(String(init?.body))).toEqual({ graph_id: graph, node_id: node, page_size: 5, after: "pair-page" });
});
it.each<[string, (v: LibraryDisagreementPage) => void]>([
  ["an opposing side equal to the anchor", (v) => { v.items[0].other.node_id = node; }],
  ["duplicate opposing sides", (v) => { v.items.push(structuredClone(v.items[0])); }],
  ["more items than the page size", (v) => { for (const n of [1, 2, 3, 4, 5]) v.items.push({ recorded_at: "2026-09-01T00:00:00Z", other: side(`66666666-6666-4666-8666-00000000000${n}`, null) }); }],
  ["more than three evidence items", (v) => { v.anchor.evidence = Array(4).fill(v.anchor.evidence[0]); }],
  ["more than four effective intervals", (v) => { v.anchor.evidence[0].effective_time = Array(5).fill({ state_key: "status", operation: "assert", value: "on", valid_from: null, valid_until: null, recorded_at: "2026-09-01T00:00:00Z", passage_id: item }); }],
  ["an effective interval without its operation", (v) => { v.anchor.evidence[0].effective_time = [{ state_key: "status", value: "on", valid_from: null, valid_until: null, recorded_at: "2026-09-01T00:00:00Z", passage_id: item } as unknown as LibraryDisagreementSide["evidence"][0]["effective_time"][0]]; }],
  ["an outcome from another graph", (v) => { v.items[0].other.evidence[0].outcome.graph_id = other; }],
  ["a negative withheld count", (v) => { v.withheld.count = -1; }],
  ["a fractional withheld count", (v) => { v.withheld.count = 1.5; }],
  ["a missing knowledge time", (v) => { delete (v.anchor.evidence[0] as Partial<LibraryDisagreementPage["anchor"]["evidence"][0]>).knowledge_time; }],
  ["an unparseable knowledge time", (v) => { v.anchor.evidence[0].knowledge_time = "yesterday"; }],
  ["a different anchor", (v) => { v.anchor.node_id = other; }],
])("rejects %s", async (_case, mutate) => {
  const value = disagreement(); mutate(value); reply(value);
  await expect(readLibraryDisagreements("fixed", { graph_id: graph, node_id: node, page_size: 5 }, signal())).rejects.toThrow(/verified/);
});
it("refuses an invalid page size before reading", async () => {
  await expect(readLibraryDisagreements("fixed", { graph_id: graph, node_id: node, page_size: 11 }, signal())).rejects.toThrow(/page size/);
  expect(guardedFetch).not.toHaveBeenCalled();
});
function nodeSessions() { return { scope: { ...scope }, node_id: node, total: 3, items: [{ graph_id: graph, provider_namespace: "history:A", session_id: "fixture%_:session", title: "Reported A", version_count: 2, last_captured_at: "2026-10-12T00:00:00Z", latest_version_id: version }], next_cursor: "sessions-next", truncated: true }; }
it("reads node sessions through the bounded endpoint and keeps only the key and display fields", async () => {
  const value = nodeSessions(); Object.assign(value.items[0], { capture_context: { secret: true } });
  reply(value);
  const result = await readLibraryNodeSessions("fixed", { scope, node_id: node, page_size: 20, after: "sessions-page" }, signal());
  expect(result.items).toEqual([{ graph_id: graph, provider_namespace: "history:A", session_id: "fixture%_:session", title: "Reported A", version_count: 2, last_captured_at: "2026-10-12T00:00:00Z", latest_version_id: version }]);
  expect(result).toMatchObject({ total: 3, next_cursor: "sessions-next", truncated: true });
  const [path, init] = vi.mocked(guardedFetch).mock.calls[0];
  expect(path).toBe("/v1/library/node-sessions"); expect(init).toMatchObject({ cache: "no-store" });
  expect(JSON.parse(String(init?.body))).toMatchObject({ scope, node_id: node, page_size: 20, after: "sessions-page" });
});
it.each(["scope", "node", "page size", "latest version", "graph", "version count", "exact version", "total"])("rejects an unverified %s node sessions payload", async (kind) => {
  const value = nodeSessions();
  let request: { scope: typeof scope & { source_item_id?: string; source_version_id?: string }; node_id: string; page_size?: number } = { scope, node_id: node };
  if (kind === "scope") Object.assign(value.scope, { outcome_versions: "all_retained" });
  if (kind === "node") value.node_id = item;
  if (kind === "page size") { value.items.push({ ...value.items[0], session_id: "other" }); request = { ...request, page_size: 1 }; }
  if (kind === "latest version") value.items[0].latest_version_id = "not-a-uuid";
  if (kind === "graph") value.items[0].graph_id = "55555555-5555-4555-8555-555555555555";
  if (kind === "version count") value.items[0].version_count = 1.5;
  if (kind === "exact version") { request = { ...request, scope: { ...scope, source_item_id: item, source_version_id: item } }; Object.assign(value, { scope: request.scope }); }
  if (kind === "total") value.total = 0;
  reply(value);
  await expect(readLibraryNodeSessions("fixed", request, signal())).rejects.toThrow(/verified/);
});
it("refuses a node sessions read whose scope names another node before any request", async () => {
  await expect(readLibraryNodeSessions("fixed", { scope, node_id: item }, signal())).rejects.toThrow();
  await expect(readLibraryNodeSessions("fixed", { scope, node_id: node, page_size: 51 }, signal())).rejects.toThrow();
  expect(guardedFetch).not.toHaveBeenCalled();
});

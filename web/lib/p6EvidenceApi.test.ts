import { afterEach, expect, it, vi } from "vitest";
import { ApiError, setTokenRefresher } from "./api";
import { guardedFetch } from "./transport";
import { readP6Evidence, readP6LegacyCapture, resolveP6NodeGraph } from "./p6EvidenceApi";

vi.mock("./transport", async () => ({ ...(await vi.importActual<typeof import("./transport")>("./transport")), guardedFetch: vi.fn() }));
const graph = "10000000-0000-4000-8000-000000000001";
const item = "10000000-0000-4000-8000-000000000002";
const version = "10000000-0000-4000-8000-000000000003";
const node = "10000000-0000-4000-8000-000000000004";
const signal = () => new AbortController().signal;
function payload() { return { graph_id: graph, source_item_id: item, source: { source_item_id: item, source_id: "opaque" }, versions: [{ version_id: version, captured_at: "2026-10-12T00:00:00Z", is_current: true, is_legacy_baseline: false }], next_cursor: null, selected_version: { version_id: version, source_item_id: item, captured_at: "2026-10-12T00:00:00Z", is_legacy_baseline: false, raw_text: "Saved evidence", text_status: "available", extraction_status: "done", extraction_error: null, can_retry: false, atoms: [], atoms_next_cursor: null } }; }
function reply(value: unknown, status = 200) { vi.mocked(guardedFetch).mockResolvedValue(new Response(JSON.stringify(value), { status })); }
afterEach(() => { setTokenRefresher(null); vi.resetAllMocks(); });

it("uses the exact retained scope and immutable bearer with no-store", async () => {
  reply(payload());
  await readP6Evidence("fixed-token", { graphId: graph, sourceItemId: item, versionId: version, before: "opaque/+?cursor" }, signal());
  const [path, init] = vi.mocked(guardedFetch).mock.calls[0];
  const params = new URL(String(path), "https://example.test").searchParams;
  expect(Object.fromEntries(params)).toEqual({ graph_id: graph, source_item_id: item, version_id: version, before: "opaque/+?cursor" });
  expect(init).toMatchObject({ cache: "no-store", headers: { Authorization: "Bearer fixed-token" } });
});
it.each([401, 403, 404, 503])("fails closed on HTTP%s without substituting a global bearer", async (status) => {
  const refresh = vi.fn(async () => "other-account"); setTokenRefresher(refresh); reply({}, status);
  await expect(readP6Evidence("fixed-token", { graphId: graph, sourceItemId: item }, signal())).rejects.toBeInstanceOf(ApiError);
  expect(refresh).not.toHaveBeenCalled(); expect(guardedFetch).toHaveBeenCalledTimes(1);
});
it.each(["graph", "source", "nested source", "version", "version owner", "version graph", "passage"])("rejects mismatched or malformed %s evidence", async (kind) => {
  const value = payload();
  if (kind === "graph") value.graph_id = node;
  if (kind === "source") value.source_item_id = node;
  if (kind === "nested source") value.source.source_item_id = node;
  if (kind === "version") value.selected_version.version_id = node;
  if (kind === "version owner") value.selected_version.source_item_id = node;
  if (kind === "version graph") Object.assign(value.selected_version, { graph_id: node });
  if (kind === "passage") Object.assign(value.selected_version, { atoms: [{ atom_id: node, kind: "fact", statement: "Supported", nodes: [], passages: [{ passage_id: node, quote: "Saved", start_char: 4, end_char: 2 }] }] });
  reply(value);
  await expect(readP6Evidence("fixed-token", { graphId: graph, sourceItemId: item, versionId: version }, signal())).rejects.toThrow();
});
it("refuses invalid source scopes before any request", async () => {
  await expect(readP6Evidence("fixed-token", { graphId: graph, sourceItemId: item, sourceId: "other" }, signal())).rejects.toThrow();
  expect(guardedFetch).not.toHaveBeenCalled();
});
it("projects only proven legacy source pointers and admitted event clocks", async () => {
  reply({ source_id: "opaque meeting", graph_id: graph, projection_status: "unverified_lineage", title: "PRIVATE stored fallback", body: "PRIVATE", occurred_at: null, source_items: [{ graph_id: graph, source_item_id: item, source_version_id: version, checkpoint_atom_id: null, title: "PRIVATE nested alias", body: "PRIVATE nested content", hints: { parent: "PRIVATE" } }] });
  const result = await readP6LegacyCapture("fixed-token", { graphId: graph, id: "opaque meeting" }, signal());
  expect(result).not.toHaveProperty("title"); expect(result).not.toHaveProperty("body");
  expect(result.occurred_at).toBeNull(); expect(result.source_items[0].source_version_id).toBe(version);
  expect(result.source_items[0]).toEqual({ graph_id: graph, source_item_id: item, source_version_id: version, checkpoint_atom_id: null });
});
it.each(["envelope graph", "source item", "source version", "checkpoint"])("rejects a contradictory or malformed legacy %s pointer", async (kind) => {
  const value = { source_id: "meeting", graph_id: graph, projection_status: "unverified_lineage", source_items: [{ graph_id: graph, source_item_id: item, source_version_id: version, checkpoint_atom_id: node }] };
  if (kind === "envelope graph") value.graph_id = node;
  if (kind === "source item") value.source_items[0].source_item_id = "invalid";
  if (kind === "source version") value.source_items[0].source_version_id = "invalid";
  if (kind === "checkpoint") value.source_items[0].checkpoint_atom_id = "invalid";
  reply(value);
  await expect(readP6LegacyCapture("fixed-token", { graphId: graph, id: "meeting" }, signal())).rejects.toThrow();
});
it("rejects an old summary payload and cross-workspace source mappings", async () => {
  reply({ source_id: "meeting", summary: "PRIVATE" });
  await expect(readP6LegacyCapture("fixed-token", { graphId: graph, id: "meeting" }, signal())).rejects.toThrow();
  reply({ source_id: "meeting", projection_status: "unverified_lineage", source_items: [{ graph_id: node, source_item_id: item, source_version_id: version }] });
  await expect(readP6LegacyCapture("fixed-token", { graphId: graph, id: "meeting" }, signal())).rejects.toThrow();
});
it("resolves only a legacy node address and refuses a foreign workspace echo", async () => {
  reply({ node_id: node, graph_id: graph, body: "PRIVATE fallback" });
  await expect(resolveP6NodeGraph("fixed-token", node, graph, signal())).resolves.toBe(graph);
  await expect(resolveP6NodeGraph("fixed-token", node, item, signal())).rejects.toThrow();
});

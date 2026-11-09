import { afterEach, expect, it, vi } from "vitest";
import { api } from "./api";
import { sourceEvidenceHref } from "./provenance";
afterEach(() => vi.unstubAllGlobals());
it("encodes source-item, version, semantic atom, and paging scopes independently", async () => {
  const fetchMock = vi.fn().mockResolvedValue({ ok: true, status: 200, json: async () => ({}) }); vi.stubGlobal("fetch", fetchMock);
  await api.sources.evidence("bearer", { graphId: "graph/&", sourceItemId: "item:+?", versionId: "version/&", atomId: "atom:+?", atomsAfter: "cursor/+?" });
  const url = new URL(fetchMock.mock.calls[0][0], "https://example.test");
  expect(url.pathname).toBe("/v1/sources/evidence"); expect(Object.fromEntries(url.searchParams)).toEqual({ graph_id: "graph/&", source_item_id: "item:+?", version_id: "version/&", atom_id: "atom:+?", atoms_after: "cursor/+?" });
  await api.sources.retryExtraction("bearer", { graphId: "graph/&", sourceItemId: "item:+?", versionId: "version/&" });
  expect(JSON.parse(fetchMock.mock.calls[1][1].body)).toEqual({ graph_id: "graph/&", source_item_id: "item:+?", version_id: "version/&" });
  await api.sources.items("bearer", { graphId: "graph/&", atomId: "chunk:+?", before: "next/+?" });
  const inventory = new URL(fetchMock.mock.calls[2][0], "https://example.test"); expect(Object.fromEntries(inventory.searchParams)).toEqual({ graph_id: "graph/&", atom_id: "chunk:+?", before: "next/+?" });
  const href = new URL(sourceEvidenceHref("item:+?", "graph/&", "version/&", "semantic/+?", "passage/+?"), "https://example.test");
  expect(href.pathname).toBe("/source"); expect(href.searchParams.get("semantic_atom_id")).toBe("semantic/+?"); expect(href.searchParams.get("passage_id")).toBe("passage/+?");
});

import { afterEach, describe, expect, it, vi } from "vitest";
import { api } from "./api";

afterEach(() => vi.unstubAllGlobals());

describe("recorded meeting API", () => {
  it("encodes source, version and pagination for preserved evidence", async () => {
    const fetchMock = vi.fn().mockResolvedValue({ ok: true, status: 200, json: async () => ({}) });
    vi.stubGlobal("fetch", fetchMock);
    await api.meetings.evidence("bearer", { graphId: "graph/&", sourceId: "meeting:+?", versionId: "version/&", before: "cursor:+?" });
    const url = new URL(fetchMock.mock.calls[0][0], "https://example.test");
    expect(url.pathname).toBe("/v1/meetings/evidence");
    expect(Object.fromEntries(url.searchParams)).toEqual({ graph_id: "graph/&", source_id: "meeting:+?", version_id: "version/&", before: "cursor:+?" });
    await api.meetings.retryExtraction("bearer", { graphId: "graph/&", sourceId: "meeting:+?", versionId: "version/&" });
    const [, init] = fetchMock.mock.calls[1] as [string, RequestInit];
    expect(init.method).toBe("POST");
    expect(JSON.parse(init.body as string)).toEqual({ graph_id: "graph/&", source_id: "meeting:+?", version_id: "version/&" });
  });

  it("serializes workspace and opaque cursor independently and reads the paginated response", async () => {
    const page = { items: [], has_more: true, next_cursor: "next:cursor" };
    const fetchMock = vi.fn().mockResolvedValue({ ok: true, status: 200, json: async () => page });
    vi.stubGlobal("fetch", fetchMock);
    const controller = new AbortController();
    expect(await api.meetings.list("bearer", { graphId: "graph-id", limit: 20, cursor: "cursor:+/?&=" }, controller.signal)).toEqual(page);
    const [address, init] = fetchMock.mock.calls[0] as [string, RequestInit];
    const url = new URL(address, "https://example.test");
    expect(url.pathname).toBe("/v1/meetings");
    expect(Object.fromEntries(url.searchParams)).toEqual({ graph_id: "graph-id", limit: "20", cursor: "cursor:+/?&=" });
    expect(new Headers(init.headers).get("Authorization")).toBe("Bearer bearer");
  });
});

import { afterEach, expect, it, vi } from "vitest";
import { searchLibrary, validateLibraryRequest, readLibrarySourceEvidence, normalizedLibraryScope, validSessionKey } from "./libraryMemoryApi";
import { setTokenRefresher } from "./api";
import { __resetTransportForTests } from "./transport";

const graph = "11111111-1111-4111-8111-111111111111";
const request = {
  scope: {
    graph_ids: [graph],
    collection: "agent_sessions" as const,
    service_id: "linear",
    container: { kind: "project", id: "same-id", provider_namespace: "workspace-two" },
  },
  query: "a literal / + & query",
  sources_after: "opaque/+?",
  page_size: 20,
};
afterEach(() => {
  setTokenRefresher(null);
  vi.unstubAllGlobals();
  __resetTransportForTests();
});

function versionResponse(current?: boolean) {
  const group = { items: [], total: 0, next_cursor: null };
  return { scope: { graph_ids: [graph], outcome_versions: "current" }, sources: group, nodes: group, prominent_work: [], outcomes: { items: [{ graph_id: graph, semantic_atom_id: graph, source_item_id: graph, source_version_id: graph, kind: "decision", statement: "A saved fixture decision.", origin: null, occurred_at: null, captured_at: "2026-10-12T00:00:00Z", nodes: [], citations: [], nodes_truncated: false, citations_truncated: false, ...(current !== undefined ? { source_version_is_current: current } : {}) }], total: 1, next_cursor: null }, facets: { services: [], connections: [], containers: [], content_kinds: [], unknown_origin_count: 1, truncated: false } };
}
it.each([false, undefined])("refuses current-outcome responses with pointer flag %s instead of widening to retained history", async (current) => {
  const fetch = vi.fn().mockResolvedValue(new Response(JSON.stringify(versionResponse(current)))); vi.stubGlobal("fetch", fetch);
  await expect(searchLibrary("fixed", { scope: { graph_ids: [graph], outcome_versions: "current" } }, new AbortController().signal)).rejects.toThrow(/unreadable/);
  expect(fetch).toHaveBeenCalledTimes(1);
});
it("accepts the default legacy all-retained payload with absent pointer metadata as unknown", async () => {
  const value = versionResponse(); Object.assign(value.scope, { outcome_versions: undefined });
  vi.stubGlobal("fetch", vi.fn().mockResolvedValue(new Response(JSON.stringify(value))));
  const result = await searchLibrary("fixed", { scope: { graph_ids: [graph] } }, new AbortController().signal);
  expect(result.outcomes.items[0].source_version_is_current).toBeUndefined(); expect(result.outcomes.items[0].occurred_at).toBeNull();
});
it("keeps exact historical source selectors while normalizing an aggregate current request", async () => {
  const value = versionResponse(false); Object.assign(value.scope, { source_item_id: graph, source_version_id: graph, outcome_versions: "all_retained" });
  const fetch = vi.fn().mockResolvedValue(new Response(JSON.stringify(value))); vi.stubGlobal("fetch", fetch);
  const result = await searchLibrary("fixed", { scope: { graph_ids: [graph], source_item_id: graph, source_version_id: graph, outcome_versions: "current" } }, new AbortController().signal);
  expect(result.outcomes.items[0].source_version_is_current).toBe(false);
  expect(JSON.parse(fetch.mock.calls[0][1].body).scope).toMatchObject({ source_item_id: graph, source_version_id: graph, outcome_versions: "all_retained" });
});
it("normalizes serialized scopes to selector fields without private metadata", () => {
  const scope = { graph_ids: [graph], body: "Private stray metadata", connection: { kind: "oauth_grant" as const, id: "connection", title: "Private account label" } };
  expect(normalizedLibraryScope(scope)).toEqual({ graph_ids: [graph], outcome_versions: "all_retained", connection: { kind: "oauth_grant", id: "connection" } });
});

it("sends independent exact scope and cursor values to the authorized endpoint without caching", async () => {
  const group = { items: [], total: 0, next_cursor: null };
  const response = {
    scope: request.scope,
    sources: group,
    outcomes: group,
    nodes: group,
    prominent_work: [],
    facets: {
      services: [],
      connections: [],
      containers: [],
      content_kinds: [],
      unknown_origin_count: 0,
      truncated: false,
    },
  };
  const fetch = vi.fn().mockResolvedValue(new Response(JSON.stringify(response), { status: 200 }));
  vi.stubGlobal("fetch", fetch);
  expect(await searchLibrary("secret", request, new AbortController().signal)).toEqual(response);
  expect(fetch).toHaveBeenCalledTimes(1);
  expect(String(fetch.mock.calls[0][0])).toContain("/v1/library/search");
  expect(fetch.mock.calls[0][1]).toMatchObject({
    method: "POST",
    cache: "no-store",
  });
  expect(new Headers(fetch.mock.calls[0][1].headers).get("Authorization")).toBe("Bearer secret");
  expect(new Headers(fetch.mock.calls[0][1].headers).get("X-CoralSwarm-Client")).toBe("web");
  expect(JSON.parse(fetch.mock.calls[0][1].body)).toEqual({ ...request, scope: normalizedLibraryScope(request.scope) });
});

it.each([403, 404, 503])(
  "fails closed on %s without querying a legacy fallback",
  async (status) => {
    const fetch = vi.fn().mockResolvedValue(new Response("{}", { status }));
    vi.stubGlobal("fetch", fetch);
    await expect(
      searchLibrary("secret", request, new AbortController().signal),
    ).rejects.toMatchObject({ status });
    expect(fetch).toHaveBeenCalledTimes(1);
  },
);

it("rejects malformed exact selectors and account/container filters before any request", () => {
  expect(() =>
    validateLibraryRequest({ scope: { graph_ids: [graph], source_item_id: "bad" } }),
  ).toThrow(/object identifier/);
  expect(() =>
    validateLibraryRequest({ scope: { graph_ids: [graph], container: request.scope.container } }),
  ).toThrow(/origin/);
  expect(() =>
    validateLibraryRequest({ scope: { graph_ids: [graph], source_version_id: graph } }),
  ).toThrow(/source item/);
  expect(() => validateLibraryRequest({ scope: { graph_ids: [] } })).toThrow(/workspace/);
  expect(() =>
    validateLibraryRequest({ scope: { graph_ids: [graph] }, query: "😀".repeat(201) }),
  ).toThrow(/200 characters/);
});

it("does not interpret a malformed server payload as an empty Library", async () => {
  vi.stubGlobal(
    "fetch",
    vi
      .fn()
      .mockResolvedValue(
        new Response(JSON.stringify({ sources: {}, outcomes: [], nodes: null }), { status: 200 }),
      ),
  );
  await expect(searchLibrary("secret", request, new AbortController().signal)).rejects.toThrow(
    /unreadable response/,
  );
});

it("rejects malformed source rows rather than displaying guessed metadata", async () => {
  const group = { items: [], total: 0, next_cursor: null };
  const response = {
    scope: request.scope,
    sources: { ...group, items: [{ title: "Secret fallback" }] },
    outcomes: group,
    nodes: group,
    prominent_work: [],
    facets: {
      services: [],
      connections: [],
      containers: [],
      content_kinds: [],
      unknown_origin_count: 0,
      truncated: false,
    },
  };
  vi.stubGlobal(
    "fetch",
    vi.fn().mockResolvedValue(new Response(JSON.stringify(response), { status: 200 })),
  );
  await expect(searchLibrary("secret", request, new AbortController().signal)).rejects.toThrow(
    /unreadable response/,
  );
});


it.each(["missing", "different graph", "different filter"])("rejects %s response scope rather than treating it as authorized", async (kind) => {
  const group = { items: [], total: 0, next_cursor: null };
  const scope = kind === "missing" ? undefined : kind === "different graph"
    ? { ...request.scope, graph_ids: ["22222222-2222-4222-8222-222222222222"] }
    : { ...request.scope, service_id: "slack" };
  vi.stubGlobal("fetch", vi.fn().mockResolvedValue(new Response(JSON.stringify({
    scope, sources: group, outcomes: group, nodes: group, prominent_work: [],
    facets: { services: [], connections: [], containers: [], content_kinds: [], unknown_origin_count: 0, truncated: false },
  }), { status: 200 })));
  await expect(searchLibrary("secret", request, new AbortController().signal)).rejects.toThrow(/unreadable response/);
});

it("accepts normalized UUIDs and omitted-null selectors without changing the requested scope", async () => {
  const group = { items: [], total: 0, next_cursor: null };
  const response = {
    scope: { ...request.scope, graph_ids: [graph], source_item_id: null, node_id: null },
    sources: group, outcomes: group, nodes: group, prominent_work: [],
    facets: { services: [], connections: [], containers: [], content_kinds: [], unknown_origin_count: 0, truncated: false },
  };
  vi.stubGlobal("fetch", vi.fn().mockResolvedValue(new Response(JSON.stringify(response), { status: 200 })));
  await expect(searchLibrary("secret", { ...request, scope: { ...request.scope, graph_ids: [graph.toUpperCase(), graph] } }, new AbortController().signal)).resolves.toEqual(response);
});


it("rejects a well-shaped source from outside the verified request graphs", async () => {
  const group = { items: [], total: 0, next_cursor: null };
  vi.stubGlobal("fetch", vi.fn().mockResolvedValue(new Response(JSON.stringify({
    scope: request.scope,
    sources: { ...group, total: 1, items: [{
      graph_id: "22222222-2222-4222-8222-222222222222", source_item_id: graph,
      source_type: "note", provider_namespace: "fixture", source_id: "foreign-source", title: "Foreign private title",
      current_version_id: graph, selected_version_id: graph, origin: null, version_origin: null,
      representation: "original_text", occurred_at: null, captured_at: null, updated_at: "2026-10-12T12:00:00Z",
      semantic_atom_count: 0, legacy_chunk_count: 0, synthetic_demo: false,
    }] },
    outcomes: group, nodes: group, prominent_work: [],
    facets: { services: [], connections: [], containers: [], content_kinds: [], unknown_origin_count: 0, truncated: false },
  }), { status: 200 })));
  await expect(searchLibrary("secret", request, new AbortController().signal)).rejects.toThrow(/unreadable response/);
});


it("inline retained text uses no-store and never retries401 through another identity's global credential", async () => {
  const refresh = vi.fn().mockResolvedValue("foreign-identity-bearer");
  setTokenRefresher(refresh);
  const fetch = vi.fn().mockResolvedValue(new Response("{}", { status: 401 }));
  vi.stubGlobal("fetch", fetch);
  await expect(readLibrarySourceEvidence("current-identity-bearer", {
    graphId: graph, sourceItemId: graph, versionId: graph, atomId: graph,
  }, new AbortController().signal)).rejects.toMatchObject({ status: 401 });
  expect(fetch).toHaveBeenCalledTimes(1);
  expect(refresh).not.toHaveBeenCalled();
  expect(fetch.mock.calls[0][1]).toMatchObject({ cache: "no-store" });
  expect(new Headers(fetch.mock.calls[0][1].headers).get("Authorization")).toBe("Bearer current-identity-bearer");
  const url = new URL(String(fetch.mock.calls[0][0]));
  expect(url.pathname).toBe("/v1/sources/evidence");
  expect(Object.fromEntries(url.searchParams)).toEqual({ graph_id: graph, source_item_id: graph, version_id: graph, atom_id: graph });
});

const sessionKey = { graph_id: graph, provider_namespace: "history:A", session_id: "fixture%_:session" };
it.each([
  ["absent", undefined, true],
  ["null", null, true],
  ["own graph", sessionKey, true],
  ["upper-case own graph", { ...sessionKey, graph_id: graph.toUpperCase() }, true],
  ["another graph", { ...sessionKey, graph_id: "22222222-2222-4222-8222-222222222222" }, false],
  ["empty session", { ...sessionKey, session_id: "" }, false],
  ["oversized session", { ...sessionKey, session_id: "s".repeat(513) }, false],
  ["NUL in session", { ...sessionKey, session_id: "a\u0000b" }, false],
  ["untrimmed namespace", { ...sessionKey, provider_namespace: " history:A" }, false],
  ["missing namespace", { graph_id: graph, session_id: "s" }, false],
  ["string", "fixture%_:session", false],
])("validSessionKey %s", (_name, value, valid) => {
  expect(validSessionKey(value, graph)).toBe(valid);
});
it.each(["outcome", "source"])("fails closed on a cross-graph %s session key", async (row) => {
  const value = versionResponse(true);
  const foreign = { ...sessionKey, graph_id: "22222222-2222-4222-8222-222222222222" };
  if (row === "outcome") Object.assign(value.outcomes.items[0], { session: foreign });
  else Object.assign(value, { sources: { items: [{ graph_id: graph, source_item_id: graph, source_type: "agent_session", provider_namespace: "history:A", source_id: "s", title: null, current_version_id: graph, selected_version_id: graph, origin: null, version_origin: null, representation: null, occurred_at: null, captured_at: null, updated_at: "2026-10-12T00:00:00Z", semantic_atom_count: 1, legacy_chunk_count: 0, synthetic_demo: false, session: foreign }], total: 1, next_cursor: null } });
  vi.stubGlobal("fetch", vi.fn().mockResolvedValue(new Response(JSON.stringify(value))));
  await expect(searchLibrary("fixed", { scope: { graph_ids: [graph], outcome_versions: "current" } }, new AbortController().signal)).rejects.toThrow(/unreadable/);
  Object.assign(row === "outcome" ? value.outcomes.items[0] : value.sources.items[0], { session: sessionKey });
  vi.stubGlobal("fetch", vi.fn().mockResolvedValue(new Response(JSON.stringify(value))));
  const result = await searchLibrary("fixed", { scope: { graph_ids: [graph], outcome_versions: "current" } }, new AbortController().signal);
  expect(row === "outcome" ? result.outcomes.items[0].session : result.sources.items[0].session).toEqual(sessionKey);
});

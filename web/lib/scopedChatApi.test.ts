import { afterEach, describe, expect, it, vi } from "vitest";
import { askScoped, readScopedConversation, readScopedThread, resolveScopedChatScope, validateScopedAnswer } from "./scopedChatApi";
import { defaultChatGraphIds, defaultChatScope, isNarrowedChatScope, narrowedScopeLabel, readChatScope, scopedChatHref } from "./scopedChatScope";
const graph = "11111111-1111-4111-8111-111111111111";
const source = "22222222-2222-4222-8222-222222222222";
const version = "33333333-3333-4333-8333-333333333333";
const passage = "44444444-4444-4444-8444-444444444444";
const atom = "55555555-5555-4555-8555-555555555555";
const conversation = "66666666-6666-4666-8666-666666666666";
const scope = { graph_ids: [graph], source_item_id: source, source_version_id: version, outcome_versions: "all_retained" as const };
const citation = { kind: "source_passage", graph_id: graph, source_item_id: source, source_version_id: version, passage_id: passage, semantic_atom_id: atom,
  quote: "A😀", start_char: 4, end_char: 6, quote_truncated: false, representation: "exported_text", occurred_at: null, captured_at: "2026-10-11T12:00:00Z" };
function result() { return { mode: "retrieval_only", support_status: "complete", answer: null, text: "", library_scope: scope,
  previous_library_scope: null, scope_changed: true, retained_citations: [citation], outcomes: [], legacy_evidence: [],
  answer_status: "supported", disagreement_status: "not_resolved", truncated: false, truncation_reason: null,
  conversation_id: conversation, message_id: atom, query_id: passage, new_conversation: true, title: "Question", contains_synthetic_demo_evidence: true, egress_blocked: false }; }
afterEach(() => vi.unstubAllGlobals());
describe("scoped Chats contract", () => {
  it("normalizes exact retained versions without dropping selector constraints or Back context", () => {
    const href = scopedChatHref({ ...scope, outcome_versions: "current", service_id: "linear", connection: { kind: "connector_install", id: "account" } }, "/home?outcomes_after=opaque&page=3");
    const params = new URL(href, "https://test.invalid").searchParams;
    expect(readChatScope(params, [graph])).toEqual({ ...scope, service_id: "linear", connection: { kind: "connector_install", id: "account" } });
    expect(params.get("returnLibrary")).toBe("/home?outcomes_after=opaque&page=3");
    expect(() => readChatScope(new URLSearchParams("scope=all&source_type=meeting"), [graph])).toThrow("not removed");
    expect(() => readChatScope(new URLSearchParams({ library_scope: JSON.stringify({ ...scope, private_body: "not a selector" }) }), [graph])).toThrow("unsupported");
  });
  it("defaults to every readable workspace, keeping the first 32 of a larger account", () => {
    expect(readChatScope(new URLSearchParams(), [graph, source])).toEqual({ graph_ids: [graph, source], outcome_versions: "all_retained" });
    expect(readChatScope(new URLSearchParams(), [])).toBeNull();
    const many = Array.from({ length: 40 }, (_, i) => `00000000-0000-4000-8000-${String(i).padStart(12, "0")}`);
    expect(defaultChatGraphIds(many)).toEqual(many.slice(0, 32));
    expect(readChatScope(new URLSearchParams(), many)?.graph_ids).toEqual(many.slice(0, 32));
    expect(defaultChatScope([graph, source])).toEqual({ graph_ids: [graph, source], outcome_versions: "all_retained" });
    expect(defaultChatScope([])).toBeNull();
  });
  it("treats any selector below the default all-workspaces scope as narrowing", () => {
    const readable = [graph, source];
    const ws = { graph_ids: [graph, source] };
    expect(isNarrowedChatScope(ws, readable)).toBe(false);
    expect(isNarrowedChatScope({ ...ws, outcome_versions: "all_retained" }, readable)).toBe(false);
    expect(narrowedScopeLabel({ ...ws, outcome_versions: "all_retained" }, [])).toBeNull();
    for (const narrowed of [{ source_item_id: source }, { source_version_id: version }, { collection: "meetings" as const }, { service_id: "linear" },
      { connection: { kind: "oauth_grant" as const, id: "a" } }, { container: { kind: "channel", id: "c", provider_namespace: "slack" } },
      { content_kind: "transcript" }, { node_id: atom }, { outcome_versions: "current" as const }])
      expect(isNarrowedChatScope({ ...ws, ...narrowed }, readable)).toBe(true);
    expect(narrowedScopeLabel({ ...ws, source_item_id: source }, [])).toBe("one source");
    expect(narrowedScopeLabel(scope, [])).toBe("one capture version");
    expect(narrowedScopeLabel({ ...ws, collection: "meetings", outcome_versions: "current" }, [])).toBe("meetings · current versions");
  });
  it("treats a workspace subset as narrowed, but not the default set in another order or case", () => {
    const graphs = [{ id: graph, name: "My Ocean" }, { id: source, name: "Team" }, { id: version, name: "Org" }];
    const readable = graphs.map((g) => g.id);
    expect(isNarrowedChatScope({ graph_ids: [version, graph, source] }, readable)).toBe(false);
    expect(isNarrowedChatScope({ graph_ids: [source.toUpperCase(), version, graph] }, readable)).toBe(false);
    expect(isNarrowedChatScope({ graph_ids: [graph] }, readable)).toBe(true);
    expect(isNarrowedChatScope({ graph_ids: [graph, source] }, readable)).toBe(true);
    expect(isNarrowedChatScope({ graph_ids: [graph, source, version, atom] }, readable)).toBe(true);
    expect(narrowedScopeLabel({ graph_ids: [graph], outcome_versions: "all_retained" }, graphs)).toBe("My Ocean");
    expect(narrowedScopeLabel({ graph_ids: [graph, source] }, graphs)).toBe("2 workspaces");
    expect(narrowedScopeLabel({ graph_ids: [atom] }, graphs)).toBe("1 workspace");
    expect(narrowedScopeLabel({ graph_ids: [source], source_item_id: atom }, graphs)).toBe("Team · one source");
    // Before admission there is no default to compare against.
    expect(isNarrowedChatScope({ graph_ids: [graph] }, [])).toBe(false);
    // Past 32, the default is the first 32; the 33rd alone is a subset.
    const many = Array.from({ length: 33 }, (_, i) => `00000000-0000-4000-8000-${String(i).padStart(12, "0")}`);
    expect(isNarrowedChatScope({ graph_ids: many.slice(0, 32).reverse() }, many)).toBe(false);
    expect(isNarrowedChatScope({ graph_ids: [many[32]] }, many)).toBe(true);
  });
  it.each(["item", "version", "graph_ids", "origin", "collection", "container"])("refuses unsupported %s selectors instead of admitting the ambient workspace", (key) => {
    expect(() => readChatScope(new URLSearchParams({ [key]: source }), [graph])).toThrow("not removed");
    expect(() => readChatScope(new URLSearchParams({ library_scope: JSON.stringify(scope), [key]: source }), [graph])).toThrow("not removed");
  });
  it("does not widen an empty explicit scope into the ambient graph and preserves ordinary route context", () => {
    expect(() => readChatScope(new URLSearchParams("library_scope="), [graph])).toThrow("empty");
    expect(readChatScope(new URLSearchParams({ id: conversation, q: "Reviewed draft", returnLibrary: "/home?g=" + graph, graph_id: graph }), [source])).toEqual({ graph_ids: [graph], outcome_versions: "all_retained" });
    expect(() => readChatScope(new URLSearchParams("graph_id="), [graph])).toThrow("empty");
  });
  it("defaults a selector-free chat to every readable workspace, capped at 32", () => {
    expect(readChatScope(new URLSearchParams({ q: "hi" }), [graph, source])).toEqual({ graph_ids: [graph, source], outcome_versions: "all_retained" });
    expect(readChatScope(new URLSearchParams(), [])).toBeNull();
    const many = Array.from({ length: 40 }, (_, i) => `${String(i).padStart(8, "0")}-1111-4111-8111-111111111111`);
    expect(readChatScope(new URLSearchParams(), many)?.graph_ids).toEqual(many.slice(0, 32));
  });
  it.each(["graph_id", "library_scope"])("rejects repeated or conflicting %s parameters instead of choosing the first selector", (selector) => {
    const values = selector === "graph_id" ? [graph, source] : [JSON.stringify(scope), JSON.stringify({ graph_ids: [graph] })];
    for (const ordered of [values, [...values].reverse(), [values[0], values[0]]]) {
      const params = new URLSearchParams(ordered.map((value) => [selector, value]));
      expect(() => readChatScope(params, [graph])).toThrow("repeats");
    }
  });
  it("admits matching workspace echoes and rejects conflicts with an explicit Library scope", () => {
    expect(readChatScope(new URLSearchParams({ library_scope: JSON.stringify(scope), graph_id: graph }), [source])).toEqual(scope);
    expect(() => readChatScope(new URLSearchParams({ library_scope: JSON.stringify(scope), graph_id: source }), [graph])).toThrow("conflicting");
    expect(() => readChatScope(new URLSearchParams({ library_scope: JSON.stringify({ graph_ids: [graph, source] }), graph_id: graph }), [graph])).toThrow("conflicting");
  });
  it("posts only supported knobs, fixed no-store bearer, exact normalization, then rejects source/scope mismatch", async () => {
    const fetcher = vi.fn().mockResolvedValue(new Response(JSON.stringify(result()))); vi.stubGlobal("fetch", fetcher);
    await askScoped("fixed-token", { text: "Question", scope, model: "smart", style: "sources-only" }, new AbortController().signal);
    const request = fetcher.mock.calls[0][1];
    expect(request.cache).toBe("no-store"); expect(request.headers.Authorization).toBe("Bearer fixed-token");
    expect(JSON.parse(request.body)).toEqual({ text: "Question", library_scope: scope, style: "sources-only" });
    expect(() => validateScopedAnswer({ ...result(), library_scope: { graph_ids: [graph] } }, scope, "sources-only")).toThrow("could not be verified");
    expect(() => validateScopedAnswer({ ...result(), retained_citations: [{ ...citation, source_version_id: atom }] }, scope, "sources-only")).toThrow();
    expect(() => validateScopedAnswer({ ...result(), retained_citations: [{ ...citation, end_char: 7 }] }, scope, "sources-only")).toThrow();
  });
  it("accepts server UUID normalization while retaining exact graph/item/version constraints", async () => {
    const mixed = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa";
    const lowerScope = { ...scope, graph_ids: [mixed], source_item_id: mixed, source_version_id: mixed };
    const upperScope = { ...scope, graph_ids: [mixed.toUpperCase()], source_item_id: mixed.toUpperCase(), source_version_id: mixed.toUpperCase() };
    const response = { ...result(), library_scope: lowerScope, retained_citations: [{ ...citation, graph_id: mixed, source_item_id: mixed, source_version_id: mixed }] };
    expect(validateScopedAnswer(response, upperScope, "sources-only").library_scope).toEqual(lowerScope);
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(new Response(JSON.stringify({ conversation: {id:mixed,title:"Question",scope_status:"resolved",library_scope:lowerScope},messages:[] }))));
    expect((await readScopedConversation("token", mixed.toUpperCase(), new AbortController().signal)).conversation.id).toBe(mixed);
  });
  it("still opens a chat saved against the legacy Work sessions collection", async () => {
    const legacy = { graph_ids: [graph], collection: "work_sessions" as const, outcome_versions: "all_retained" as const };
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(new Response(JSON.stringify({ conversation: { id: conversation, title: "Question", scope_status: "resolved", library_scope: legacy }, messages: [] }))));
    expect((await readScopedConversation("token", conversation, new AbortController().signal)).conversation.library_scope).toEqual(legacy);
    expect(narrowedScopeLabel(legacy, [])).toBe("work sessions");
  });
  it("parses the caller's stored answer feedback and rejects a malformed or misplaced one", async () => {
    const answer = { id: atom, seq: 2, role: "assistant", text: "", mode: null, support_status: "unavailable" };
    const read = (messages: unknown[]) => {
      vi.stubGlobal("fetch", vi.fn().mockResolvedValue(new Response(JSON.stringify({ conversation: { id: conversation, title: "Question", scope_status: "unscoped", library_scope: null }, messages }))));
      return readScopedConversation("token", conversation, new AbortController().signal);
    };
    const feedback = { vote: "down", reasons: ["wrong", "outdated"], note: "stale" };
    expect((await read([{ ...answer, feedback }])).messages[0].feedback).toEqual(feedback);
    expect((await read([{ ...answer, feedback: null }])).messages[0].feedback).toBeNull();
    expect((await read([answer])).messages[0]).not.toHaveProperty("feedback");
    await expect(read([{ ...answer, feedback: { ...feedback, reasons: ["bogus"] } }])).rejects.toThrow("feedback could not be verified");
    await expect(read([{ ...answer, feedback: { ...feedback, vote: "sideways" } }])).rejects.toThrow("feedback could not be verified");
    await expect(read([{ ...answer, role: "user", feedback }])).rejects.toThrow("feedback could not be verified");
  });
  it("inherits accepted scopes on follow-up, sends explicit replacement when requested, and never refreshes a401", async () => {
    const fetcher = vi.fn().mockImplementation(() => Promise.resolve(new Response(JSON.stringify({ ...result(), new_conversation: false })))); vi.stubGlobal("fetch", fetcher);
    await askScoped("token", { text: "Followup", scope, conversationId: conversation, inherit: true, model: "fast", style: "sources-only" }, new AbortController().signal);
    expect(JSON.parse(fetcher.mock.calls[0][1].body)).not.toHaveProperty("library_scope");
    await askScoped("token", { text: "Replace", scope, conversationId: conversation, replace: true, model: "fast", style: "sources-only" }, new AbortController().signal);
    expect(JSON.parse(fetcher.mock.calls[1][1].body).scope_action).toBe("replace");
    fetcher.mockResolvedValue(new Response("denied", { status: 401 }));
    await expect(askScoped("token", { text: "Question", scope, model: "fast", style: "concise" }, new AbortController().signal)).rejects.toMatchObject({ status: 401 });
    expect(fetcher).toHaveBeenCalledTimes(3);
  });
  it("verifies freshly resolved selector context and refuses a broadened scope echo", async () => {
    const fetcher = vi.fn().mockImplementation(() => Promise.resolve(new Response(JSON.stringify({scope})))); vi.stubGlobal("fetch", fetcher);
    expect(await resolveScopedChatScope("fixed", scope, new AbortController().signal)).toEqual(scope);
    expect(JSON.parse(fetcher.mock.calls[0][1].body)).toEqual({scope});
    fetcher.mockImplementation(() => Promise.resolve(new Response(JSON.stringify({scope:{graph_ids:[graph]}}))));
    await expect(resolveScopedChatScope("fixed", scope, new AbortController().signal)).rejects.toThrow("not widened");
  });
  it("requires server support status and does not allow prose in sources-only or a no-evidence response", () => {
    expect(() => validateScopedAnswer({ ...result(), support_status: "unknown", timing: { support_status: "complete" } }, scope, "sources-only")).toThrow();
    expect(() => validateScopedAnswer({ ...result(), text: "Invented answer" }, scope, "sources-only")).toThrow();
    expect(() => validateScopedAnswer({ ...result(), answer_status: "no_evidence" }, scope, "sources-only")).toThrow();
    expect(() => validateScopedAnswer({ ...result(), retained_citations: [{ ...citation, quote: "", end_char: citation.start_char }] }, scope, "sources-only")).toThrow();
  });
  it("accepts an inherited unresolved historical state only without prose and with an unresolved temporal state", () => {
    const unresolved = { ...result(), answer_status: "temporal_unresolved", temporal_state: { mode: "recorded_evidence_state", status: "incomplete" } };
    expect(validateScopedAnswer(unresolved, scope, "sources-only").answer_status).toBe("temporal_unresolved");
    expect(() => validateScopedAnswer({ ...unresolved, temporal_state: { status: "supported" } }, scope, "sources-only")).toThrow();
    expect(() => validateScopedAnswer({ ...unresolved, temporal_state: null }, scope, "sources-only")).toThrow();
    expect(() => validateScopedAnswer({ ...unresolved, mode: "synthesized", text: "Invented", answer: "Invented" }, scope, "concise")).toThrow();
  });
  it.each(["connector", "content", "collection"])("rejects unmapped chunks in narrowed %s scopes but retains broad node legacy evidence", (kind) => {
    const broad = { graph_ids: [graph], node_id: atom, outcome_versions: "all_retained" as const };
    const narrowed = { ...broad, ...(kind === "connector" ? { service_id: "slack" } : kind === "content" ? { content_kind: "message" } : { collection: "agent_sessions" as const }) };
    const legacy = { kind: "legacy_chunk", graph_id: graph, atom_id: atom, representation: "unmapped_legacy_chunk", content: "Retained legacy text", content_truncated: false, occurred_at: null, captured_at: null };
    expect(() => validateScopedAnswer({ ...result(), library_scope: narrowed, legacy_evidence: [legacy] }, narrowed, "sources-only")).toThrow();
    expect(validateScopedAnswer({ ...result(), library_scope: broad, legacy_evidence: [legacy] }, broad, "sources-only").legacy_evidence).toHaveLength(1);
  });
  it("requires an authoritative current pointer in current mode while preserving false for retained history", () => {
    const currentScope = { graph_ids: [graph], outcome_versions: "current" as const };
    const outcome = { graph_id: graph, semantic_atom_id: atom, kind: "fact", statement: "A fact", statement_truncated: false, source_item_id: source, source_version_id: version, source_version_is_current: false,
      occurred_at: null, captured_at: citation.captured_at, representation: "exported_text", confidence: null, synthetic_demo: true, citations: [citation] };
    const current = { ...result(), library_scope: currentScope, outcomes: [outcome] };
    expect(() => validateScopedAnswer(current, currentScope, "sources-only")).toThrow();
    expect(() => validateScopedAnswer({ ...current, outcomes: [{ ...outcome, source_version_is_current: undefined }] }, currentScope, "sources-only")).toThrow();
    expect(validateScopedAnswer({ ...current, outcomes: [{ ...outcome, source_version_is_current: true }] }, currentScope, "sources-only").outcomes[0].source_version_is_current).toBe(true);
    expect(validateScopedAnswer({ ...result(), outcomes: [outcome] }, scope, "sources-only").outcomes[0].source_version_is_current).toBe(false);
  });
  it("does not recover a denied saved exact scope from old ocean IDs", async () => {
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(new Response(JSON.stringify({ conversation: { id: conversation, title: "Masked", scope_status: "unavailable", library_scope: null, ocean_ids: [graph] }, messages: [] }))));
    const detail = await readScopedConversation("token", conversation, new AbortController().signal);
    expect(detail.conversation.library_scope).toBeNull();
  });
  describe("history pages", () => {
    const conv = { id: conversation, title: "Long chat", scope_status: "resolved", library_scope: scope, ocean_ids: [graph] };
    const user = (seq: number) => ({ id: `${String(seq).padStart(8, "0")}-0000-4000-8000-000000000000`, seq, role: "user", text: `question ${seq}` });
    const page = (from: number, to: number, more: boolean, conversation: object = conv) => ({ conversation,
      messages: Array.from({ length: to - from + 1 }, (_, i) => user(from + i)), messages_truncated: more, older_before_seq: more ? from : null });
    const serve = (...pages: unknown[]) => { const fetch = vi.fn(); for (const p of pages) fetch.mockResolvedValueOnce(new Response(JSON.stringify(p))); vi.stubGlobal("fetch", fetch); return fetch; };
    it("requests the page older than a keyset cursor and returns the next cursor", async () => {
      const fetch = serve(page(51, 250, true), page(0, 50, false));
      const newest = await readScopedConversation("token", conversation, new AbortController().signal);
      expect(newest.older_before_seq).toBe(51);
      const older = await readScopedConversation("token", conversation, new AbortController().signal, newest.older_before_seq ?? null);
      expect(String(fetch.mock.calls[1][0])).toMatch(new RegExp(`/v1/conversations/${conversation}\\?before_seq=51$`));
      expect(older.messages.map((m) => m.seq)).toEqual(Array.from({ length: 51 }, (_, i) => i));
      expect(older.older_before_seq).toBeNull();
    });
    it("refuses a page whose cursor or messages are not strictly older than the requested cursor", async () => {
      serve(page(40, 60, false));
      await expect(readScopedConversation("token", conversation, new AbortController().signal, 51)).rejects.toThrow("could not be verified");
      serve({ ...page(51, 250, true), older_before_seq: null });
      await expect(readScopedConversation("token", conversation, new AbortController().signal)).rejects.toThrow("history could not be verified");
      serve({ ...page(1, 50, false), older_before_seq: 7 });
      await expect(readScopedConversation("token", conversation, new AbortController().signal)).rejects.toThrow("history could not be verified");
      await expect(readScopedConversation("token", conversation, new AbortController().signal, 0)).rejects.toThrow("cursor is invalid");
    });
    it("re-reads every loaded page down to the oldest shown message, in order and without overlap", async () => {
      const fetch = serve(page(201, 400, true), page(1, 200, false));
      const thread = await readScopedThread("token", conversation, new AbortController().signal, 150);
      expect(fetch).toHaveBeenCalledTimes(2);
      expect(thread.messages.map((m) => m.seq)).toEqual(Array.from({ length: 400 }, (_, i) => i + 1));
      expect(thread.messages_truncated).toBe(false); expect(thread.older_before_seq).toBeNull();
      const newestOnly = serve(page(201, 400, true));
      expect((await readScopedThread("token", conversation, new AbortController().signal, null)).older_before_seq).toBe(201);
      expect(newestOnly).toHaveBeenCalledTimes(1);
    });
    it("fails closed when an older page reports a different saved scope", async () => {
      serve(page(201, 400, true), page(1, 200, false, { ...conv, scope_status: "unavailable", library_scope: null }));
      await expect(readScopedThread("token", conversation, new AbortController().signal, 1)).rejects.toThrow("scope changed");
    });
  });
});
describe("graph fields on scoped answers (#1353, #1346)", () => {
  it("admits Jev reports, cite verdicts and supersession without treating them as required", () => {
    const outcome = { graph_id: graph, semantic_atom_id: atom, kind: "fact", statement: "Old", statement_truncated: false, source_item_id: source, source_version_id: version,
      source_version_is_current: true, occurred_at: null, captured_at: "2026-10-11T12:00:00Z", representation: "exported_text", confidence: null, synthetic_demo: false,
      citations: [{ ...citation, jev_verdict: "verified" }], supersession: { status: "superseded", superseded_by: [{ semantic_atom_id: passage }] }, jev_role: "historical" };
    const answer = validateScopedAnswer({ ...result(), retained_citations: [{ ...citation, jev_verdict: "verified" }], outcomes: [outcome],
      jev: { pack: "applied", cite: "applied", pack_ms: 3, cite_ms: 9 } }, scope, "concise");
    expect(answer.jev).toEqual({ pack: "applied", cite: "applied", pack_ms: 3, cite_ms: 9 });
    expect(answer.retained_citations[0].jev_verdict).toBe("verified");
    // A pre-#1353 payload (no jev, no verdicts) still validates.
    expect(validateScopedAnswer(result(), scope, "concise").jev).toBeUndefined();
  });
});

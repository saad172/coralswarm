// @vitest-environment jsdom
import { cleanup, fireEvent, screen, waitFor } from "@testing-library/react";
import { render } from "@/lib/testing/queryRender";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { useChatThread } from "./useChatThread";
import { ChatComposer } from "./ChatComposer";

const identity = vi.hoisted(() => ({ key: "personal-user", ready: true, getToken: () => "fixture-token" }));
vi.mock("@/lib/useEvidenceAdmission", () => ({ useEvidenceAdmission: () => ({ ...identity, graphs: [], currentGraphId: "11111111-1111-4111-8111-111111111111" }) }));
const graph = "11111111-1111-4111-8111-111111111111";
const source = "22222222-2222-4222-8222-222222222222";
const version = "33333333-3333-4333-8333-333333333333";
const passage = "44444444-4444-4444-8444-444444444444";
const atom = "55555555-5555-4555-8555-555555555555";
const conversation = "66666666-6666-4666-8666-666666666666";
const scope = { graph_ids: [graph], node_id: atom, outcome_versions: "current" as const };
const params = new URLSearchParams({ library_scope: JSON.stringify(scope) }).toString();
const question = "  Preserve this diagnostic draft exactly  ";
const citation = { kind: "source_passage", graph_id: graph, source_item_id: source, source_version_id: version, passage_id: passage, semantic_atom_id: atom,
  quote: "Exact phrase", start_char: 0, end_char: 12, quote_truncated: false, representation: "exported_text", occurred_at: null, captured_at: "2026-10-11T12:00:00Z" };
const answer = { mode: "retrieval_only", support_status: "complete", answer: null, text: "", library_scope: scope,
  previous_library_scope: null, scope_changed: true, retained_citations: [citation], outcomes: [], legacy_evidence: [],
  answer_status: "supported", disagreement_status: "not_resolved", truncated: false, truncation_reason: null,
  conversation_id: conversation, message_id: atom, query_id: passage, new_conversation: true, title: "Question", contains_synthetic_demo_evidence: false, egress_blocked: false };
const detail = { conversation: { id: conversation, title: "Question", mine: true, scope_status: "resolved", library_scope: scope },
  messages: [{ id: source, seq: 0, role: "user", text: question.trim() }, { ...answer, id: atom, seq: 1, role: "assistant" }] };
let observed: ReturnType<typeof useChatThread>;
function Harness() {
  const chat = useChatThread(null, params);
  observed = chat;
  return <ChatComposer {...chat} disabled={chat.readOnly} initialText={question} scoped onSend={chat.send} onStop={chat.stop} />;
}
beforeEach(() => {
  identity.key = "personal-user"; identity.ready = true;
  window.history.replaceState({}, "", `/chats?${params}`);
  vi.stubGlobal("matchMedia", () => ({ matches: false, addEventListener: () => {}, removeEventListener: () => {} }));
});
afterEach(() => { cleanup(); vi.unstubAllGlobals(); });

describe("Sources-only HTTP query and readback", () => {
  it.each([
    { name: "POST 503 with no readback", post: 503, get: 200, reads: 0, success: false },
    { name: "POST 200 then GET 503", post: 200, get: 503, reads: 1, success: false },
    { name: "POST 200 then GET 200", post: 200, get: 200, reads: 1, success: true },
  ])("$name preserves the admission fence without replay", async ({ post, get, reads, success }) => {
    const fetcher = vi.fn(async (input: string, init: RequestInit) => {
      const path = new URL(input, "https://fixture.invalid").pathname;
      if (path === "/v1/library/resolve-scope") return new Response(JSON.stringify({ scope }));
      if (path === "/v1/conversations") return new Response(JSON.stringify({ conversations: [], shared: [] }));
      if (path === "/v1/queries/stream" && init.method === "POST") return new Response(post === 200 ? `event: final\ndata: ${JSON.stringify(answer)}\n\n` : JSON.stringify(answer), { status: post });
      if (path === `/v1/conversations/${conversation}` && init.method === "GET") return new Response(JSON.stringify(detail), { status: get });
      throw new Error(`Unexpected fixture request ${init.method} ${path}`);
    });
    vi.stubGlobal("fetch", fetcher);
    const view = render(<Harness />);
    await waitFor(() => expect(observed.libraryScope).toEqual(scope));
    fireEvent.click(screen.getByRole("button", { name: "Answer settings" }));
    fireEvent.click(screen.getByRole("tab", { name: "Sources only" }));
    await waitFor(() => expect(observed.style).toBe("sources-only"));
    fireEvent.click(screen.getByRole("button", { name: "Send" }));
    await waitFor(() => expect(success ? observed.activeId : observed.err).toBe(success ? conversation : "The chat could not be verified. Retry when the connection is restored."));
    const queries = fetcher.mock.calls.filter(([url]) => url.endsWith("/v1/queries/stream"));
    expect(queries).toHaveLength(1);
    expect(JSON.parse(String(queries[0][1].body))).toEqual({ text: question.trim(), library_scope: scope, style: "sources-only" });
    expect(fetcher.mock.calls.filter(([url]) => url.endsWith(`/v1/conversations/${conversation}`))).toHaveLength(reads);
    expect((screen.getByRole("textbox") as HTMLTextAreaElement).value).toBe(success ? "" : question);
    expect(observed.busy).toBe(false);
    if (success) {
      expect(observed.messages).toHaveLength(2);
      expect(observed.scopeAccepted).toBe(true);
      expect(observed.messages[1]).toMatchObject({ mode: "retrieval_only", retainedCitations: [citation] });
    } else {
      expect(observed.activeId).toBeNull();
      expect(observed.activeConv).toBeNull();
      expect(observed.messages).toEqual([]);
      expect(observed.libraryScope).toBeNull();
      expect(observed.readOnly).toBe(true);
    }
    identity.ready = false;
    view.rerender(<Harness />);
    expect(observed.activeConv).toBeNull();
    expect(observed.messages).toEqual([]);
    expect(observed.libraryScope).toBeNull();
    expect(observed.getNavigationId()).toBeNull();
    expect(fetcher.mock.calls.filter(([url]) => url.endsWith("/v1/queries/stream"))).toHaveLength(1);
  });
});

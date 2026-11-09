// @vitest-environment jsdom
import { act, cleanup, fireEvent, render, renderHook, screen, waitFor } from "@testing-library/react";
import { onlineManager, QueryClientProvider, type QueryClient } from "@tanstack/react-query";
import type { ReactElement } from "react";
import { afterEach, beforeEach, expect, it, vi } from "vitest";
import { conversationKey, conversationQueryOptions, prefetchConversation } from "@/lib/appQueries";
import { ApiError } from "@/lib/api";
import { makeQueryClient } from "@/lib/queryClient";
import { TransportError } from "@/lib/transport";
import { emitConversationsChanged } from "@/lib/conversationsChanged";
import type { ConversationSummary } from "@/lib/api";
import type { ScopedAnswer, ScopedConversationDetail } from "@/lib/scopedChatApi";
import type { Stored } from "@/lib/pageSectionWire";
import type { ConversationDetailPayload } from "@/lib/pageSectionTypes/conversationDetail";
import type { ConversationListPayload } from "@/lib/pageSectionTypes/chats";
import type { AssistantTurn } from "./types";

const graph = "11111111-1111-4111-8111-111111111111";
const S1 = "22222222-2222-4222-8222-222222222222";
const S2 = "22222222-2222-4222-8222-333333333333";
const atom = "55555555-5555-4555-8555-555555555555";
const conversation = "66666666-6666-4666-8666-666666666666";
const scope = { graph_ids: [graph], outcome_versions: "all_retained" as const };
const admission = { key: "account-A", ready: true, getToken: () => "token", graphs: [{ id: graph, name: "Ocean" }], currentGraphId: graph, queryScope: "scope-A" };
vi.mock("@/lib/useEvidenceAdmission", () => ({ useEvidenceAdmission: () => admission }));
vi.mock("@/lib/providers", () => ({ useApp: () => ({ getAuthToken: () => "token", authReady: true, queryScope: "scope-A" }) }));
const nav = vi.hoisted(() => ({ query: "" }));
vi.mock("next/navigation", () => ({ useSearchParams: () => new URLSearchParams(nav.query), usePathname: () => "/chats" }));
const api = vi.hoisted(() => ({ readScopedConversation: vi.fn(), readScopedThread: vi.fn(), listScopedConversations: vi.fn(), askScopedStream: vi.fn(), resolveScopedChatScope: vi.fn() }));
vi.mock("@/lib/scopedChatApi", async (actual) => ({ ...(await actual<typeof import("@/lib/scopedChatApi")>()), ...api }));

const wire = vi.hoisted(() => ({ read: vi.fn(), list: vi.fn() }));
vi.mock("@/lib/api", async (actual) => ({ ...(await actual<typeof import("@/lib/api")>()), apiRequest: vi.fn((path: string) => {
  if (path === "/v1/conversations") return wire.list();
  if (path.startsWith("/v1/load/conversations/")) return wire.read(path);
  throw new Error(`Unexpected mount read ${path}`);
}) }));

import { useChatThread } from "./useChatThread";
import { useConversations } from "@/components/useConversations";
import ChatHistory from "@/components/ChatHistory";

const quote = (source: string) => ({ kind: "source_passage" as const, graph_id: graph, source_item_id: source, source_version_id: "33333333-3333-4333-8333-333333333333",
  passage_id: "44444444-4444-4444-8444-444444444444", semantic_atom_id: atom, start_char: 0, end_char: 12, quote: "Exact phrase", quote_truncated: false,
  representation: "exported_text", occurred_at: null, captured_at: "2026-10-11T12:00:00Z" });
function detail(sources: string[], extra: ScopedConversationDetail["messages"] = [], id = conversation): Stored<ConversationDetailPayload> {
  return { model: "conversation_detail", version: 1, computed_at: "2026-10-31T00:00:00Z", stale: false, refresh_eta: null, payload: { resolved_scope: scope, conversation: { id, graph_id: graph, title: "Chat", mine: true, pinned: false, shared: false, created_at: "2026-10-11", updated_at: "2026-10-11", scope_status: "resolved", library_scope: scope },
    messages: [{ id: atom, seq: 1, role: "assistant", text: "Answer", mode: "synthesized", support_status: "complete", library_scope: scope, retained_citations: sources.map(quote), outcomes: [], legacy_evidence: [], contains_synthetic_demo_evidence: false }, ...extra],
    messages_truncated: false, older_before_seq: null } };
}
function deferred<T>() { let resolve!: (v: T) => void; let reject!: (e: unknown) => void; const promise = new Promise<T>((r, j) => { resolve = r; reject = j; }); return { promise, resolve, reject }; }
const key = conversationKey("scope-A", conversation);
const cited = (hook: { result: { current: ReturnType<typeof useChatThread> } }) =>
  hook.result.current.messages.map((m) => (m as AssistantTurn).retainedCitations?.map((c) => c.source_item_id));
function open(qc: QueryClient) {
  return renderHook(() => useChatThread(conversation), { wrapper: ({ children }) => <QueryClientProvider client={qc}>{children}</QueryClientProvider> });
}

function listFixture(rows: ConversationSummary[] = []): Stored<ConversationListPayload> {
  return { model: "conversation_list", version: 1, computed_at: "2026-10-31T00:00:00Z", stale: false, refresh_eta: null, payload: { conversations: rows, shared: [] } };
}
beforeEach(() => {
  nav.query = ""; wire.list.mockResolvedValue(listFixture());
  Object.defineProperty(navigator, "connection", { configurable: true, value: { saveData: true } });
});
afterEach(() => { cleanup(); vi.resetAllMocks(); Reflect.deleteProperty(navigator, "connection"); });

it("paints a stale cached chat at once, then replaces it from the re-read", async () => {
  const qc = makeQueryClient();
  qc.setQueryData(key, detail([S1, S2]), { updatedAt: Date.now() - 20_000 });
  const reread = deferred<ReturnType<typeof detail>>();
  wire.read.mockReturnValue(reread.promise);
  const hook = open(qc);
  expect(cited(hook)).toEqual([[S1, S2]]);
  expect(hook.result.current.loadingConv).toBe(false);
  await act(async () => {});
  expect(wire.read).toHaveBeenCalledTimes(1);
  await act(async () => reread.resolve(detail([S1])));
  await waitFor(() => expect(cited(hook)).toEqual([[S1]]));
  expect(qc.getQueryData(key)).toEqual(detail([S1]));
});

it("a fresh cached chat opens with no request", async () => {
  const qc = makeQueryClient();
  qc.setQueryData(key, detail([S1]));
  const hook = open(qc);
  expect(cited(hook)).toEqual([[S1]]);
  await act(async () => {});
  expect(wire.read).not.toHaveBeenCalled();
});

it("a cold open shows the loading state until the read lands, then caches it", async () => {
  const qc = makeQueryClient();
  const read = deferred<ReturnType<typeof detail>>();
  wire.read.mockReturnValue(read.promise);
  const hook = open(qc);
  expect(hook.result.current.loadingConv).toBe(true);
  expect(hook.result.current.messages).toEqual([]);
  await act(async () => read.resolve(detail([S1])));
  await waitFor(() => expect(cited(hook)).toEqual([[S1]]));
  expect(hook.result.current.loadingConv).toBe(false);
  expect(qc.getQueryData(key)).toEqual(detail([S1]));
});

it("a transient failure of the background re-read keeps the cached chat with a note", async () => {
  const qc = makeQueryClient();
  qc.setQueryData(key, detail([S1, S2]), { updatedAt: Date.now() - 20_000 });
  const failure = new TransportError("network", "/v1/conversations", "The chat could not be reached. Retry when the connection is restored.");
  wire.read.mockRejectedValue(failure);
  const hook = open(qc);
  await waitFor(() => expect(hook.result.current.err).toBe(failure.message));
  expect(cited(hook)).toEqual([[S1, S2]]);
  expect(hook.result.current.loadingConv).toBe(false);
  expect(qc.getQueryData(key)).toEqual(detail([S1, S2]));
});

it("a refused background re-read drops the cached chat, so a reopen does not paint it and a hover does not re-ask", async () => {
  const qc = makeQueryClient();
  qc.setQueryData(key, detail([S1, S2]), { updatedAt: Date.now() - 20_000 });
  const refusal = new ApiError("This chat is no longer available.", 404);
  wire.read.mockRejectedValue(refusal);
  const first = open(qc);
  await waitFor(() => expect(first.result.current.err).toBe(refusal.message));
  expect(first.result.current.messages).toEqual([]);
  expect(qc.getQueryData(key)).toBeUndefined();
  first.unmount();
  // Hovering the row afterwards does not retry the refused read.
  await prefetchConversation(qc, { getAuthToken: () => "token", scope: "scope-A", authReady: true }, conversation);
  expect(wire.read).toHaveBeenCalledTimes(1);
  // A reopen paints nothing from the cache: it waits on the server's answer.
  const reread = deferred<ReturnType<typeof detail>>();
  wire.read.mockReturnValue(reread.promise);
  const second = open(qc);
  expect(second.result.current.messages).toEqual([]);
  expect(second.result.current.loadingConv).toBe(true);
  await act(async () => reread.reject(refusal));
  await waitFor(() => expect(second.result.current.err).toBe(refusal.message));
  expect(wire.read).toHaveBeenCalledTimes(2);
});

it("keeps the interactive receipt local until a real Stored reread replaces the cache", async () => {
  const qc = makeQueryClient();
  wire.read.mockResolvedValue(detail([S1]));
  const hook = open(qc);
  await waitFor(() => expect(cited(hook)).toEqual([[S1]]));
  const answer: ScopedAnswer = { support_status: "complete", text: "Follow-up answer", answer: "Follow-up answer", mode: "synthesized", library_scope: scope, previous_library_scope: null, scope_changed: false,
    retained_citations: [quote(S2)], outcomes: [], legacy_evidence: [], answer_status: "supported", disagreement_status: "not_resolved", truncated: false, truncation_reason: null,
    conversation_id: conversation, message_id: "77777777-7777-4777-8777-777777777777", query_id: "88888888-8888-4888-8888-888888888888", new_conversation: false, title: "Chat", contains_synthetic_demo_evidence: false, egress_blocked: false };
  const afterSend = detail([S1], [
    { id: "99999999-9999-4999-8999-999999999999", seq: 2, role: "user", text: "Follow-up", mode: null },
    { id: answer.message_id, seq: 3, role: "assistant", text: answer.text, mode: "synthesized", support_status: "complete", library_scope: scope, retained_citations: [quote(S2)], outcomes: [], legacy_evidence: [], contains_synthetic_demo_evidence: false },
  ]);
  api.askScopedStream.mockResolvedValue(answer);
  api.readScopedThread.mockResolvedValue(afterSend.payload);
  const refreshed = deferred<ReturnType<typeof detail>>();
  wire.read.mockReturnValue(refreshed.promise);
  await act(async () => { expect(await hook.result.current.send("Follow-up")).toBe(true); });
  expect(qc.getQueryData(key)).toEqual(detail([S1]));
  expect(cited(hook)).toEqual([[S1], undefined, [S2]]);
  await act(async () => refreshed.resolve(afterSend));
  await waitFor(() => expect(qc.getQueryData(key)).toEqual(afterSend));
});

it("opening while the browser reports offline fails like a direct read, not a paused one", async () => {
  onlineManager.setOnline(false);
  try {
    const qc = makeQueryClient();
    const failure = new TransportError("network", "/v1/conversations", "The chat could not be reached. Retry when the connection is restored.");
    wire.read.mockRejectedValue(failure);
    const hook = open(qc);
    await waitFor(() => expect(hook.result.current.err).toBe(failure.message));
    expect(hook.result.current.loadingConv).toBe(false);
  } finally {
    onlineManager.setOnline(true);
  }
});

it("deleting a chat drops its cached copy", async () => {
  const qc = makeQueryClient();
  wire.read.mockResolvedValue(detail([S1]));
  await qc.fetchQuery(conversationQueryOptions({ getAuthToken: () => "token", scope: "scope-A" }, conversation));
  renderHook(() => useConversations(), { wrapper: ({ children }) => <QueryClientProvider client={qc}>{children}</QueryClientProvider> });
  act(() => emitConversationsChanged(conversation));
  expect(qc.getQueryData(key)).toBeUndefined();
});

// The sidebar deletes while Home is on screen; a new or renamed chat comes
// from /chats, and Home re-reads on its next visit anyway.
it("a chat deleted from the sidebar marks Home's Continue stale; a new or renamed one does not", () => {
  const qc = makeQueryClient();
  const home = ["homeContinue", "scope-A"];
  qc.setQueryData(home, { items: [], generated_at: "2026-11-03T12:00:00Z" });
  renderHook(() => useConversations(), { wrapper: ({ children }) => <QueryClientProvider client={qc}>{children}</QueryClientProvider> });
  act(() => emitConversationsChanged());
  expect(qc.getQueryState(home)?.isInvalidated).toBe(false);
  act(() => emitConversationsChanged(conversation));
  expect(qc.getQueryState(home)?.isInvalidated).toBe(true);
});

const other = "66666666-6666-4666-8666-777777777777";
const summary = (id: string, title: string): ConversationSummary => ({ id, graph_id: graph, title, pinned: false, shared: false, mine: true, created_at: "2026-10-11", updated_at: "2026-10-11" });
const rows = [summary(conversation, "Alpha"), summary(other, "Beta")];
const wrap = (qc: QueryClient, ui: ReactElement) => render(<QueryClientProvider client={qc}>{ui}</QueryClientProvider>);
const settle = () => act(async () => new Promise((r) => setTimeout(r, 150)));

it.each([
  ["the sidebar history", () => { wire.list.mockResolvedValue(listFixture(rows)); return <ChatHistory />; }],
])("%s warms the row the pointer settles on, not the rows swept over", async (_label, ui) => {
  wire.read.mockImplementation((path: string) => Promise.resolve(detail([S1], [], path.split("/").at(-1))));
  wrap(makeQueryClient(), ui());
  const alpha = await screen.findByRole("link", { name: /Alpha/ });
  const beta = screen.getByRole("link", { name: /Beta/ });
  fireEvent.pointerEnter(alpha);
  fireEvent.pointerLeave(alpha);
  fireEvent.pointerEnter(beta);
  await settle();
  expect(wire.read.mock.calls.map(([path]) => path.split("/").at(-1))).toEqual([other]);
});

it("the open chat's own row is never warmed", async () => {
  nav.query = `id=${conversation}`;
  wire.list.mockResolvedValue(listFixture(rows));
  wire.read.mockResolvedValue(detail([S1]));
  wrap(makeQueryClient(), <ChatHistory />);
  fireEvent.focus(await screen.findByRole("link", { name: /Alpha/ }));
  await settle();
  expect(wire.read).not.toHaveBeenCalled();
});

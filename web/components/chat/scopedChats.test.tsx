// @vitest-environment jsdom
import { act, cleanup, fireEvent, screen, waitFor } from "@testing-library/react";
import { render, renderHook, returnToTab } from "@/lib/testing/queryRender";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { useChatThread, scopedMessageTurn } from "./useChatThread";
import { ChatComposer } from "./ChatComposer";
import ChatsPage from "@/app/(app)/chats/page";
import { CitationPanel } from "./CitationPanel";
import { citationHref } from "@/lib/provenance";
import { evidenceSources } from "./evidence";
import type { ScopedAnswer, ScopedConversationDetail } from "@/lib/scopedChatApi";
import type { AssistantTurn } from "./types";
import { TransportError } from "@/lib/transport";
import { ApiError } from "@/lib/api";
const deps = vi.hoisted(() => ({ graphs: [] as { id: string; name: string }[], key: "user-a", ready: true, getToken: vi.fn(), ask: vi.fn(), read: vi.fn(), stored: vi.fn(), thread: vi.fn(), list: vi.fn(), resolve: vi.fn(), query: "", push: vi.fn(), replace: vi.fn() }));
// The first-run checklist reads Clerk and the org directory; it has its own test.
vi.mock("@/components/chat/FirstRunChecklist", () => ({ FirstRunChecklist: () => null }));
vi.mock("@/components/chat/ChatEmpty", () => ({ ChatEmpty: () => null }));
vi.mock("next/navigation", () => ({ useSearchParams: () => new URLSearchParams(deps.query), useRouter: () => ({ push: deps.push, replace: deps.replace }) }));
vi.mock("@/lib/useEvidenceAdmission", () => ({ useEvidenceAdmission: () => ({ key: deps.key, ready: deps.ready, getToken: deps.getToken, graphs: deps.graphs, currentGraphId: "11111111-1111-4111-8111-111111111111", queryScope: deps.key }) }));
vi.mock("@/lib/scopedChatApi", async (actual) => ({ ...(await actual<typeof import("@/lib/scopedChatApi")>()), readScopedThread: (...args: unknown[]) => deps.thread(...args), askScopedStream: (...args: unknown[]) => deps.ask(...args), readScopedConversation: (...args: unknown[]) => deps.read(...args), listScopedConversations: (...args: unknown[]) => deps.list(...args), resolveScopedChatScope: (...args: unknown[]) => deps.resolve(...args) }));
vi.mock("@/lib/api", async (actual) => ({ ...(await actual<typeof import("@/lib/api")>()), apiRequest: vi.fn((path: string) => {
  if (path === "/v1/conversations") return deps.list();
  if (path.startsWith("/v1/load/conversations/")) return deps.stored(path);
  throw new Error(`Unexpected mount read ${path}`);
}) }));
const graph = "11111111-1111-4111-8111-111111111111";
const source = "22222222-2222-4222-8222-222222222222";
const version = "33333333-3333-4333-8333-333333333333";
const passage = "44444444-4444-4444-8444-444444444444";
const atom = "55555555-5555-4555-8555-555555555555";
const conversation = "66666666-6666-4666-8666-666666666666";
const scope = { graph_ids: [graph], source_item_id: source, source_version_id: version, outcome_versions: "all_retained" as const };
const quote = { kind: "source_passage" as const, graph_id: graph, source_item_id: source, source_version_id: version, passage_id: passage, semantic_atom_id: atom,
  start_char: 0, end_char: 12, quote: "Exact phrase", quote_truncated: false, representation: "exported_text", occurred_at: null, captured_at: "2026-10-11T12:00:00Z" };
function answer(): ScopedAnswer { return { support_status: "complete", text: "Verified answer [1]", answer: "Verified answer [1]", mode: "synthesized", library_scope: scope,
  retained_citations: [quote], outcomes: [], legacy_evidence: [], previous_library_scope: null, scope_changed: true, truncated: false, truncation_reason: null,
  answer_status: "supported", disagreement_status: "not_resolved", conversation_id: conversation, message_id: atom, query_id: passage, new_conversation: true, title: "Question", contains_synthetic_demo_evidence: true, egress_blocked: false }; }
function detail(): ScopedConversationDetail { return { conversation: { id: conversation, graph_id: graph, title: "Verified conversation", mine: true, pinned: false, shared: false, created_at: "2026-10-11", updated_at: "2026-10-11", scope_status: "resolved", library_scope: scope }, messages_truncated: false, older_before_seq: null, messages: [{ id: atom, seq: 0, role: "assistant", text: "Saved private answer", mode: "synthesized", support_status: "complete", library_scope: scope, retained_citations: [quote], outcomes: [], legacy_evidence: [], contains_synthetic_demo_evidence: false }] }; }
function storedFixture(payload: ScopedConversationDetail = detail()) {
  return { model: "conversation_detail", version: 1, computed_at: "2026-10-31T00:00:00Z", stale: false, refresh_eta: null,
    payload: { ...payload, messages_truncated: payload.messages_truncated ?? false, older_before_seq: payload.older_before_seq ?? null,
      resolved_scope: payload.conversation.scope_status === "resolved" ? payload.conversation.library_scope : null } };
}
function listed(rows: unknown[] = []) {
  return { model: "conversation_list", version: 1, computed_at: "2026-10-31T00:00:00Z", stale: false, refresh_eta: null, payload: { conversations: rows, shared: [] } };
}
function deferred<T>() { let resolve!: (value: T) => void; let reject!: (reason: unknown) => void; const promise = new Promise<T>((yes, no) => { resolve = yes; reject = no; }); return { promise, resolve, reject }; }
beforeEach(() => {
  deps.query = ""; deps.graphs = [{ id: graph, name: "Ocean" }];
  vi.stubGlobal("matchMedia", () => ({matches:false,addEventListener:()=>{},removeEventListener:()=>{}}));
  deps.key = "user-a"; deps.ready = true; deps.getToken.mockReset().mockReturnValue("frozen-a");
  deps.ask.mockReset().mockResolvedValue(answer()); deps.read.mockReset().mockResolvedValue(detail()); deps.thread.mockReset().mockImplementation((...args: unknown[]) => deps.read(...args)); deps.list.mockReset().mockResolvedValue(listed()); deps.stored.mockReset().mockResolvedValue(storedFixture()); deps.resolve.mockReset().mockImplementation((_token, scope) => Promise.resolve(scope));
});
afterEach(() => { cleanup(); vi.restoreAllMocks(); vi.unstubAllGlobals(); });
describe("admitted scoped Chats", () => {
  it("recovers server exact scope, inherits a follow-up, and explicitly replaces cleared selectors", async () => {
    const { result } = renderHook(() => useChatThread(conversation));
    await waitFor(() => expect(result.current.messages).toHaveLength(1));
    expect(result.current.libraryScope).toEqual(scope);
    await act(async () => { await result.current.send("Followup"); });
    expect(deps.ask.mock.calls[0][1]).toMatchObject({ inherit: true, replace: false, conversationId: conversation, scope });
    act(() => result.current.clearNarrowing());
    expect(result.current.messages).toEqual([]); expect(result.current.activeConv).toBeNull();
    await waitFor(() => expect(result.current.libraryScope).toEqual({ graph_ids: [graph], outcome_versions: "all_retained" }));
    deps.ask.mockResolvedValue({ ...answer(), library_scope: { graph_ids: [graph], outcome_versions: "all_retained" }, retained_citations: [] });
    deps.read.mockResolvedValue({ ...detail(), conversation: { ...detail().conversation, library_scope: { graph_ids: [graph], outcome_versions: "all_retained" } }, messages: [{ ...detail().messages[0], library_scope: { graph_ids: [graph], outcome_versions: "all_retained" } }] });
    await act(async () => { expect(await result.current.send("Broader question")).toBe(true); });
    expect(deps.ask.mock.calls[1][1]).toMatchObject({ inherit: false, replace: true });
  });
  it("hides loaded titles, answers, citations and selectors during live identity loss before delayed old results settle", async () => {
    const pending = deferred<ScopedAnswer>(); deps.ask.mockReturnValue(pending.promise);
    const paints: unknown[] = [];
    const { result, rerender } = renderHook(() => { const chat = useChatThread(conversation); paints.push({ key: deps.key, text: chat.messages.map((m) => m.text), scope: chat.libraryScope, title: chat.activeConv?.title }); return chat; });
    await waitFor(() => expect(result.current.messages).toHaveLength(1));
    let sending!: Promise<boolean>;
    act(() => { sending = result.current.send("Question"); });
    deps.key = "user-b"; deps.ready = false; deps.getToken.mockImplementation(() => { throw new Error("identity changed"); });
    rerender();
    expect(result.current.messages).toEqual([]); expect(result.current.libraryScope).toBeNull(); expect(result.current.activeConv).toBeNull();
    await act(async () => { pending.resolve(answer()); await sending; });
    expect(paints.filter((v) => (v as {key:string}).key === "user-b").every((v) => { const p = v as {text:string[]; scope:unknown; title:unknown}; return !p.text.length && p.scope === null && p.title === undefined; })).toBe(true);
    expect(result.current.messages).toEqual([]);
  });
  it("drops deferred results after scope changes and rejects lost admission even without a render", async () => {
    const pending = deferred<ScopedAnswer>(); deps.ask.mockReturnValue(pending.promise);
    const { result } = renderHook(() => useChatThread(conversation));
    await waitFor(() => expect(result.current.messages).toHaveLength(1));
    let sending!: Promise<boolean>;
    act(() => { sending = result.current.send("Question"); });
    act(() => result.current.clearNarrowing());
    await act(async () => { pending.resolve(answer()); expect(await sending).toBe(false); });
    expect(result.current.messages).toEqual([]);
    deps.getToken.mockImplementation(() => { throw new Error("live identity lost"); });
    await act(async () => { expect(await result.current.send("Another")).toBe(false); });
    expect(deps.ask).toHaveBeenCalledTimes(1);
  });
  it("keeps the verified chat while offline, then clears it when the reconnect re-check fails closed", async () => {
    const { result } = renderHook(() => useChatThread(conversation));
    await waitFor(() => expect(result.current.messages).toHaveLength(1));
    act(() => window.dispatchEvent(new Event("offline")));
    // Offline is not a revocation: the verified thread stays, with a note.
    expect(result.current.messages).toHaveLength(1); expect(result.current.err).toMatch(/^Offline\./);
    deps.stored.mockRejectedValue(new ApiError("Unavailable", 403));
    act(() => window.dispatchEvent(new Event("online")));
    await waitFor(() => expect(result.current.err).toBe("Unavailable"));
    expect(result.current.messages).toEqual([]); expect(result.current.activeConv).toBeNull();
  });
  it("keeps graph-only legacy links constrained to the saved scope instead of broadening mismatched workspaces", async () => {
    const link = new URLSearchParams({ graph_id: source }).toString();
    window.history.replaceState(null, "", `/chats?${link}`);
    const { result } = renderHook(() => useChatThread(conversation, link));
    await waitFor(() => expect(result.current.err).toContain("not widened"));
    expect(result.current.messages).toEqual([]); expect(result.current.libraryScope).toBeNull();
  });
  it("drops a delayed scoped response when the live address changes before the router renders", async () => {
    const pending = deferred<ScopedAnswer>(); deps.ask.mockReturnValue(pending.promise);
    const link = new URLSearchParams({id:conversation}).toString(); window.history.replaceState(null, "", `/chats?${link}`);
    const { result } = renderHook(() => useChatThread(conversation, link));
    await waitFor(() => expect(result.current.messages).toHaveLength(1));
    let sending!: Promise<boolean>; act(() => { sending = result.current.send("Question"); });
    window.history.replaceState(null, "", "/chats?graph_id=" + source);
    expect(result.current.getNavigationId()).toBeNull();
    await act(async () => { pending.resolve(answer()); expect(await sending).toBe(false); });
    expect(result.current.messages.some((m) => m.text === "Verified answer [1]")).toBe(false);
  });
  it("withholds selector chips and Send while an explicit scope change is delayed or denied", async () => {
    const pending = deferred<typeof scope>(); deps.resolve.mockReturnValue(pending.promise);
    const link = new URLSearchParams({library_scope:JSON.stringify(scope)}).toString();
    window.history.replaceState(null, "", `/chats?${link}`);
    const { result } = renderHook(() => useChatThread(null, link));
    expect(deps.resolve).not.toHaveBeenCalled();
    act(() => result.current.clearNarrowing());
    expect(result.current.libraryScope).toBeNull(); expect(result.current.readOnly).toBe(true);
    await act(async () => { expect(await result.current.send("Not yet")).toBe(false); });
    await act(async () => { pending.reject(new Error("Denied selection")); });
    expect(result.current.libraryScope).toBeNull(); expect(result.current.messages).toEqual([]);
    expect(deps.ask).not.toHaveBeenCalled();
  });
  it("withholds labels and every scope request for an unsupported exact-version route selector", async () => {
    const link = new URLSearchParams({ item: source, version, graph_id: graph }).toString();
    window.history.replaceState(null, "", `/chats?${link}`);
    const { result } = renderHook(() => useChatThread(null, link));
    await act(async () => { expect(await result.current.send("Do not broaden")).toBe(false); });
    expect(result.current.err).toContain("not removed"); expect(result.current.libraryScope).toBeNull();
    expect(deps.resolve).not.toHaveBeenCalled(); expect(deps.read).not.toHaveBeenCalled(); expect(deps.stored).not.toHaveBeenCalled(); expect(deps.ask).not.toHaveBeenCalled();
  });
  it("new chat revalidates its explicit selection even when the URL is already empty", async () => {
    const { result } = renderHook(() => useChatThread(null));
    await waitFor(() => expect(result.current.readOnly).toBe(false));
    act(() => result.current.newChat());
    await waitFor(() => expect(result.current.readOnly).toBe(false));
    expect(deps.resolve).toHaveBeenCalledTimes(1);
  });
  it("never accepts timing as a receipt or a saved exact scope that is unavailable", async () => {
    const masked = scopedMessageTurn({ id: atom, role: "assistant", text: "Unsafe cached answer", mode: "synthesized", support_status: "unknown", retained_citations: [quote] }, scope) as AssistantTurn;
    expect(masked.text).toBe(""); expect(evidenceSources(masked)).toEqual([]);
    deps.stored.mockResolvedValue(storedFixture({ ...detail(), conversation: { ...detail().conversation, scope_status: "unavailable", library_scope: null } }));
    const { result } = renderHook(() => useChatThread(conversation));
    await waitFor(() => expect(result.current.loadingConv).toBe(false));
    expect(result.current.libraryScope).toBeNull(); expect(result.current.readOnly).toBe(true);
    await act(async () => { expect(await result.current.send("Do not widen")).toBe(false); });
    expect(deps.ask).not.toHaveBeenCalled();
  });
});
describe("failed asks", () => {
  const failedDetail = (reason = "db_busy") => ({ ...detail(), messages: [
    { ...detail().messages[0], seq: 0 },
    { id: passage, seq: 1, role: "user" as const, text: "Followup", mode: null },
    { id: source, seq: 2, role: "assistant" as const, text: "", mode: "failed" as const, support_status: "failed" as const, failure_reason: reason, question_message_id: passage },
  ] });
  it("keeps the thread when an ask fails after its question was saved, shows why, and Retry reuses that question", async () => {
    const { result } = renderHook(() => useChatThread(conversation));
    await waitFor(() => expect(result.current.messages).toHaveLength(1));
    deps.ask.mockRejectedValueOnce(new ApiError("The chat could not be verified. Retry when the connection is restored.", 503, null, null, null,
      { error: "scoped ask unavailable", reason: "db_busy", retryable: true, conversation_id: conversation, question_message_id: passage, failed_message_id: source }));
    deps.read.mockResolvedValue(failedDetail());
    await act(async () => { expect(await result.current.send("Followup")).toBe(false); });
    expect(result.current.err).toBeNull();
    const turns = result.current.messages;
    expect(turns.map((m) => m.role)).toEqual(["assistant", "user", "assistant"]);
    expect((turns[2] as AssistantTurn).failure).toEqual({ reason: "db_busy", questionMessageId: passage });
    // Retry: same saved question, no new user bubble.
    deps.ask.mockResolvedValue(answer());
    deps.read.mockResolvedValue({ ...detail(), messages: [{ ...detail().messages[0], seq: 0 }, { id: passage, seq: 1, role: "user", text: "Followup", mode: null }, { ...detail().messages[0], id: atom, seq: 2 }] });
    await act(async () => { expect(await result.current.retry(turns[2].key)).toBe(true); });
    expect(deps.ask.mock.calls[1][1]).toMatchObject({ text: "Followup", conversationId: conversation, retryMessageId: passage });
    expect(result.current.messages.filter((m) => m.role === "user")).toHaveLength(1);
    expect(result.current.messages.some((m) => m.role === "assistant" && !!m.failure)).toBe(false);
  });
  it("shows a failed turn locally when the failure record could not be read back, and Retry still reuses the saved question", async () => {
    const { result } = renderHook(() => useChatThread(conversation));
    await waitFor(() => expect(result.current.messages).toHaveLength(1));
    deps.ask.mockRejectedValueOnce(new ApiError("The chat could not be verified. Retry when the connection is restored.", 503, null, null, null,
      { reason: "db_busy", conversation_id: conversation, question_message_id: passage, failed_message_id: null }));
    deps.thread.mockRejectedValueOnce(new TransportError("network", "/v1/conversations/receipt", "readback unavailable"));
    await act(async () => { expect(await result.current.send("Followup")).toBe(false); });
    expect(result.current.err).toBeNull();
    expect(result.current.messages).toHaveLength(3);
    const failed = result.current.messages[2] as AssistantTurn;
    expect(failed.failure).toEqual({ reason: "db_busy", questionMessageId: passage });
    deps.ask.mockResolvedValue(answer());
    await act(async () => { await result.current.retry(failed.key); });
    expect(deps.ask.mock.calls[1][1]).toMatchObject({ text: "Followup", conversationId: conversation, retryMessageId: passage });
  });
  it("asks a question the server never saved afresh on Retry, replacing the failed pair", async () => {
    const { result } = renderHook(() => useChatThread(conversation));
    await waitFor(() => expect(result.current.messages).toHaveLength(1));
    deps.ask.mockRejectedValueOnce(new ApiError("The chat could not be verified. Retry when the connection is restored.", 503, null, null, null, { reason: "unavailable" }));
    await act(async () => { expect(await result.current.send("Never saved")).toBe(false); });
    expect(result.current.err).toBeNull();
    expect(result.current.messages).toHaveLength(3);
    const failed = result.current.messages[2] as AssistantTurn;
    expect(failed.failure).toEqual({ reason: "unavailable", questionMessageId: null });
    const pending = deferred<ScopedAnswer>(); deps.ask.mockReturnValue(pending.promise);
    let retrying!: Promise<boolean>;
    act(() => { retrying = result.current.retry(failed.key); });
    expect(deps.ask.mock.calls[1][1].retryMessageId).toBeFalsy();
    expect(deps.ask.mock.calls[1][1]).toMatchObject({ text: "Never saved" });
    // Nothing was saved, so the failed pair is replaced by the new one: still one bubble.
    expect(result.current.messages.filter((m) => m.role === "user" && m.text === "Never saved")).toHaveLength(1);
    expect(result.current.messages.some((m) => m.role === "assistant" && !!m.failure)).toBe(false);
    await act(async () => { pending.resolve(answer()); await retrying; });
  });
  it("still wipes the thread when the answer fails verification rather than the ask failing", async () => {
    const { result } = renderHook(() => useChatThread(conversation));
    await waitFor(() => expect(result.current.messages).toHaveLength(1));
    deps.ask.mockRejectedValueOnce(new Error("The answer or its saved scope could not be verified. No answer was displayed."));
    await act(async () => { expect(await result.current.send("Question")).toBe(false); });
    expect(result.current.messages).toEqual([]);
    expect(result.current.err).toContain("could not be verified");
  });
});
describe("scoped evidence and drafts", () => {
  it("shows a restored unanswered question as failed with Retry, which re-asks the saved question and adds no second copy", async () => {
    deps.query = new URLSearchParams({ id: conversation }).toString();
    window.history.replaceState(null, "", `/chats?${deps.query}`);
    const unanswered = { ...detail(), messages: [{ id: passage, seq: 0, role: "user" as const, text: "Unanswered saved question", mode: null }] };
    const answered = { ...detail(), messages: [unanswered.messages[0], { ...detail().messages[0], seq: 1 }] };
    let done = false;
    deps.read.mockImplementation(async () => (done ? answered : unanswered));
    deps.stored.mockImplementation(async () => storedFixture(done ? answered : unanswered));
    deps.ask.mockImplementation(async () => { done = true; return answer(); });
    const { rerender } = render(<ChatsPage />);
    await screen.findByText("This question never got an answer.");
    expect(screen.getAllByText("Unanswered saved question")).toHaveLength(1);
    expect(screen.queryByText("No completed saved answer is available for the latest question.")).toBeNull();
    expect(deps.ask).not.toHaveBeenCalled();
    fireEvent.click(screen.getByRole("button", { name: "Retry" }));
    await screen.findByText("Saved private answer");
    expect(deps.ask).toHaveBeenCalledTimes(1);
    expect(deps.ask.mock.calls[0][1]).toMatchObject({ text: "Unanswered saved question", conversationId: conversation, retryMessageId: passage });
    expect(screen.getAllByText("Unanswered saved question")).toHaveLength(1);
    expect(screen.queryByText("This question never got an answer.")).toBeNull();
    deps.key = "user-b"; deps.ready = false;
    rerender(<ChatsPage />);
    expect(screen.queryByText("Unanswered saved question")).toBeNull();
  });
  it("renders a recorded failed ask with its reason and Retry, and ignores any evidence on it", async () => {
    deps.query = new URLSearchParams({ id: conversation }).toString();
    window.history.replaceState(null, "", `/chats?${deps.query}`);
    deps.stored.mockResolvedValue(storedFixture({ ...detail(), messages: [
      { id: passage, seq: 0, role: "user", text: "Why did the build fail?", mode: null },
      { id: source, seq: 1, role: "assistant", text: "", mode: "failed", support_status: "failed", failure_reason: "db_busy", question_message_id: passage,
        retained_citations: [quote], outcomes: [], legacy_evidence: [] },
    ] }));
    render(<ChatsPage />);
    await screen.findByText("Couldn\u2019t answer: the database was busy.");
    expect(screen.getByRole("button", { name: "Retry" })).toBeTruthy();
    expect(screen.queryByText("Exact phrase")).toBeNull();
    expect(screen.queryByText(/answered from/)).toBeNull();
    expect(screen.getAllByText("Why did the build fail?")).toHaveLength(1);
  });
  it("keeps live answer progress distinct from restored unanswered history", async () => {
    deps.query = new URLSearchParams({ id: conversation }).toString();
    window.history.replaceState(null, "", `/chats?${deps.query}`);
    const pending = deferred<ScopedAnswer>(); deps.ask.mockReturnValue(pending.promise);
    render(<ChatsPage />);
    await screen.findByText("Saved private answer");
    expect(screen.queryByText("This question never got an answer.")).toBeNull();
    fireEvent.change(screen.getByRole("textbox"), { target: { value: "Live question" } });
    fireEvent.click(screen.getByRole("button", { name: "Send" }));
    await screen.findByText("Preparing your question");
    expect(screen.queryByText("No completed saved answer is available for the latest question.")).toBeNull();
    await act(async () => { pending.resolve(answer()); });
    await screen.findByText("Saved private answer");
    expect(screen.queryByText("No completed saved answer is available for the latest question.")).toBeNull();
  });
  it("shows a narrowed chat as one Only chip whose × clears to all workspaces, keeping the draft and Back context", async () => {
    deps.query = new URLSearchParams({ id: conversation, returnLibrary: "/home?outcomes_after=page3&source_item_id=" + source }).toString();
    window.history.replaceState(null, "", `/chats?${deps.query}`);
    render(<ChatsPage />);
    await screen.findByText("Saved private answer");
    expect(screen.getByText("Only: one capture version")).toBeTruthy();
    expect(screen.queryByRole("region", { name: "Chat capture scope" })).toBeNull();
    expect(screen.queryByText(/Verified .* scope/)).toBeNull();
    expect(screen.queryByRole("button", { name: "Broaden to workspace" })).toBeNull();
    fireEvent.click(screen.getByRole("button", {name:"Back to Home"}));
    expect(deps.push).toHaveBeenCalledWith("/home?outcomes_after=page3&source_item_id=" + source);
    const field = screen.getByRole("textbox", {name:"Question"});
    fireEvent.change(field, {target:{value:"Preserve this draft"}});
    fireEvent.click(screen.getByRole("button", {name:"Remove scope"}));
    expect(screen.queryByText("Saved private answer")).toBeNull();
    expect((field as HTMLTextAreaElement).value).toBe("Preserve this draft");
    expect(deps.resolve).toHaveBeenLastCalledWith("frozen-a", { graph_ids: [graph], outcome_versions: "all_retained" }, expect.anything());
    await waitFor(() => expect(screen.queryByText(/^Only:/)).toBeNull());
    expect((screen.getByRole("textbox") as HTMLTextAreaElement).value).toBe("Preserve this draft");
  });
  it("asks across every readable workspace by default, with no workspace picker", async () => {
    deps.graphs = [{ id: graph, name: "My Ocean" }, { id: source, name: "Team" }];
    window.history.replaceState(null, "", "/chats");
    render(<ChatsPage />);
    expect(deps.resolve).not.toHaveBeenCalled();
    await waitFor(() => expect((screen.getByRole("textbox") as HTMLTextAreaElement).disabled).toBe(false));
    expect(screen.queryByText(/^Only:/)).toBeNull();
    expect(screen.queryByRole("button", { name: /workspace|My Ocean|Team/ })).toBeNull();
    expect(screen.queryByRole("checkbox")).toBeNull();
    expect(screen.queryByRole("textbox", { name: "Search workspaces" })).toBeNull();
  });
  it("names the workspace a ?graph_id= link narrows to, and × widens to every readable workspace", async () => {
    deps.graphs = [{ id: graph, name: "My Ocean" }, { id: source, name: "Team" }];
    deps.query = new URLSearchParams({ graph_id: source }).toString();
    window.history.replaceState(null, "", `/chats?${deps.query}`);
    render(<ChatsPage />);
    expect(await screen.findByText("Only: Team")).toBeTruthy();
    expect(deps.resolve).not.toHaveBeenCalled();
    fireEvent.click(screen.getByRole("button", { name: "Remove scope" }));
    expect(deps.resolve).toHaveBeenLastCalledWith("frozen-a", { graph_ids: [graph, source], outcome_versions: "all_retained" }, expect.anything());
    await waitFor(() => expect(screen.queryByText(/^Only:/)).toBeNull());
  });
  it("shows no scope chip or card for an ordinary workspace-wide chat", async () => {
    const wide = { graph_ids: [graph], outcome_versions: "all_retained" as const };
    deps.stored.mockResolvedValue(storedFixture({ ...detail(), conversation: { ...detail().conversation, library_scope: wide }, messages: [{ ...detail().messages[0], library_scope: wide }] }));
    deps.query = new URLSearchParams({ id: conversation }).toString();
    window.history.replaceState(null, "", `/chats?${deps.query}`);
    render(<ChatsPage />);
    await screen.findByText("Saved private answer");
    expect(screen.queryByText(/^Only:/)).toBeNull();
    expect(screen.queryByRole("button", { name: "Remove scope" })).toBeNull();
    expect(screen.queryByRole("region", { name: "Chat capture scope" })).toBeNull();
  });
  it("keeps an edited follow-up draft while the first question persists its server scope and conversation URL", async () => {
    const pending = deferred<ScopedAnswer>(); deps.ask.mockReturnValue(pending.promise);
    deps.query = new URLSearchParams({ library_scope: JSON.stringify(scope), returnLibrary: "/home?item=" + source }).toString();
    window.history.replaceState(null, "", `/chats?${deps.query}`);
    const { rerender } = render(<ChatsPage />);
    await waitFor(() => expect((screen.getByRole("textbox") as HTMLTextAreaElement).disabled).toBe(false));
    fireEvent.change(screen.getByRole("textbox"), {target:{value:"First question"}});
    fireEvent.click(screen.getByRole("button", {name:"Send"}));
    await waitFor(() => expect(deps.ask).toHaveBeenCalledTimes(1));
    fireEvent.change(screen.getByRole("textbox"), {target:{value:"Next draft while waiting"}});
    await act(async () => { pending.resolve(answer()); });
    await screen.findByText("Saved private answer");
    expect(new URL(window.location.href).searchParams.get("id")).toBe(conversation);
    expect(new URL(window.location.href).searchParams.has("library_scope")).toBe(false);
    deps.query = new URL(window.location.href).searchParams.toString();
    rerender(<ChatsPage />);
    expect((screen.getByRole("textbox") as HTMLTextAreaElement).value).toBe("Next draft while waiting");
  });
  it("withholds a buffered final when its new receipt is unavailable in the following fresh read", async () => {
    const { result } = renderHook(() => useChatThread(conversation));
    await waitFor(() => expect(result.current.messages).toHaveLength(1));
    deps.read.mockResolvedValue({ ...detail(), messages: [{ ...detail().messages[0], support_status: "unavailable", text: "masked", retained_citations: [] }] });
    await act(async () => { expect(await result.current.send("Question")).toBe(false); });
    expect(result.current.messages).toEqual([]);
    expect(result.current.err).toContain("no longer available");
  });
  it("replaces every prior projection when a fresh canonical read masks an old answer but admits a new one", async () => {
    const old = detail(); old.messages[0].contains_synthetic_demo_evidence = true;
    old.conversation.title = "Old private generated title";
    deps.stored.mockResolvedValueOnce(storedFixture(old));
    deps.list.mockResolvedValueOnce(listed([{ ...old.conversation, title: "Old private list title" }])).mockResolvedValue(listed());
    const { result } = renderHook(() => useChatThread(conversation));
    await waitFor(() => expect(result.current.messages).toHaveLength(1));
    expect((result.current.messages[0] as AssistantTurn).syntheticDemo).toBe(true);
    expect(result.current.conversations).toHaveLength(1);
    const newId = "77777777-7777-4777-8777-777777777777";
    const questionId = "88888888-8888-4888-8888-888888888888";
    deps.ask.mockResolvedValue({ ...answer(), message_id: newId, contains_synthetic_demo_evidence: false });
    const reconciled: ScopedConversationDetail = { ...detail(), conversation: { ...detail().conversation, title: "Fresh admitted question" }, messages_truncated: true, older_before_seq: 1,
      messages: [{ ...old.messages[0], seq: 1, text: "", support_status: "unavailable", retained_citations: [], outcomes: [], contains_synthetic_demo_evidence: null },
        { id: questionId, seq: 2, role: "user", text: "New independently supported question", mode: null },
        { ...detail().messages[0], seq: 3, id: newId, text: "Fresh independently admitted answer" }] };
    deps.read.mockResolvedValue(reconciled);
    deps.stored.mockResolvedValue(storedFixture(reconciled));
    await act(async () => { expect(await result.current.send("New independently supported question")).toBe(true); });
    expect(result.current.messages.map((m) => m.key)).toEqual([atom, questionId, newId]);
    const masked = result.current.messages[0] as AssistantTurn;
    expect(masked.text).toBe(""); expect(masked.syntheticDemo).toBeFalsy();
    expect(evidenceSources(masked)).toEqual([]); expect(citationHref(masked, 1)).toBeUndefined();
    expect(result.current.messages[2].text).toBe("Fresh independently admitted answer");
    expect((result.current.messages[2] as AssistantTurn).prompt).toBe("New independently supported question");
    expect(result.current.activeConv?.title).toBe("Fresh admitted question");
    await waitFor(() => expect(result.current.conversations).toEqual([])); expect(result.current.messagesTruncated).toBe(true);
  });
  it("opens exact saved version/passages, labels unknown event time independently, and badges synthetic inputs", () => {
    const turn = scopedMessageTurn({ ...detail().messages[0], contains_synthetic_demo_evidence: true }, scope) as AssistantTurn;
    const href = citationHref(turn, 1);
    if (!href) throw new Error("Expected exact passage navigation");
    const params = new URL(href, "https://test.invalid").searchParams;
    expect(params.get("source_item_id")).toBe(source); expect(params.get("version_id")).toBe(version);
    expect(params.get("passage_id")).toBe(passage); expect(href).not.toContain("/coral");
    render(<CitationPanel turn={turn} n={1} onClose={() => {}} />);
    expect(screen.getByText("Exact phrase")).toBeTruthy();

    expect(screen.getByText("Contains synthetic demo evidence")).toBeTruthy();
  });
  it("renders retained malicious HTML and Markdown image references as inert quote text", () => {
    const turn = scopedMessageTurn({ ...detail().messages[0], retained_citations: [{ ...quote, quote: '<img src="https://external.invalid/?private=secret"><script>steal()</script> ![private](https://external.invalid/image)' }] }, scope) as AssistantTurn;
    const view = render(<CitationPanel turn={turn} n={1} onClose={() => {}} />);
    expect(view.container.querySelector("img,script,iframe,object,embed")).toBeNull();
    expect(screen.getByText(/<img src=/)).toBeTruthy();
  });
  it("preserves an edited draft across scope broadening and rejected send, and sends only the chosen style", async () => {
    const send = vi.fn().mockResolvedValue(false); const setStyle = vi.fn(); const clear = vi.fn();
    const props = { model: "smart" as const, setModel: vi.fn(), effort: "medium" as const, setEffort: vi.fn(), style: "detailed" as const, setStyle, busy: false, onSend: send, onStop: vi.fn(), scoped: true };
    const { rerender } = render(<ChatComposer {...props} only={{ label: "Team", onClear: clear }} />);
    fireEvent.change(screen.getByRole("textbox", {name:"Question"}), { target: { value: "Keep my follow-up draft" } });
    rerender(<ChatComposer {...props} />);
    expect((screen.getByRole("textbox") as HTMLTextAreaElement).value).toBe("Keep my follow-up draft");
    fireEvent.keyDown(screen.getByRole("textbox"), {key:"Enter"});
    await waitFor(() => expect(send).toHaveBeenCalledWith("Keep my follow-up draft"));
    expect((screen.getByRole("textbox") as HTMLTextAreaElement).value).toBe("Keep my follow-up draft");
    fireEvent.click(screen.getByRole("button", {name:"Answer settings"})); fireEvent.click(screen.getByText("Sources only"));
    expect(setStyle).toHaveBeenCalledWith("sources-only");
    rerender(<ChatComposer {...props} style="sources-only" />);
    expect(screen.queryByRole("button", {name:/Smart/})).toBeNull(); expect(screen.queryByText("reasoning effort")).toBeNull();
  });
  describe("older history pages", () => {
    const q = (n: number) => `9999999${n}-9999-4999-8999-999999999999`;
    const masked = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa";
    const newest = (): ScopedConversationDetail => ({ ...detail(), messages_truncated: true, older_before_seq: 3,
      messages: [{ id: q(3), seq: 3, role: "user", text: "Newest question", mode: null }, { ...detail().messages[0], seq: 4 }] });
    const older = (): ScopedConversationDetail => ({ ...detail(), messages_truncated: false, older_before_seq: null,
      messages: [{ id: q(1), seq: 1, role: "user", text: "Oldest question", mode: null },
        { id: masked, seq: 2, role: "assistant", text: "[Evidence unavailable: retained support cannot be verified]", mode: null, support_status: "unavailable" }] });
    it("prepends the verified older page, keeps its unsupported answer masked, and revalidates down to it", async () => {
      deps.stored.mockResolvedValue(storedFixture(newest()));
      deps.read.mockImplementation((_t, _id, _s, before?: number | null) => Promise.resolve(before ? older() : newest()));
      const { result } = renderHook(() => useChatThread(conversation));
      await waitFor(() => expect(result.current.canLoadOlder).toBe(true));
      expect(result.current.messagesTruncated).toBe(true);
      await act(async () => { expect(await result.current.loadOlder()).toBe(true); });
      expect(deps.read.mock.calls.at(-1)?.slice(0, 2)).toEqual(["frozen-a", conversation]);
      expect(deps.read.mock.calls.at(-1)?.[3]).toBe(3);
      expect(result.current.messages.map((m) => m.key)).toEqual([q(1), masked, q(3), atom]);
      const unsupported = result.current.messages[1] as AssistantTurn;
      expect(unsupported.text).toBe(""); expect(unsupported.error).toMatch("unavailable"); expect(evidenceSources(unsupported)).toEqual([]);
      expect(result.current.messagesTruncated).toBe(false); expect(result.current.canLoadOlder).toBe(false);
      deps.thread.mockResolvedValue({ ...newest(), messages: [...older().messages, ...newest().messages], messages_truncated: false, older_before_seq: null });
      act(() => returnToTab());
      await waitFor(() => expect(result.current.messages).toHaveLength(4));
      expect(deps.thread.mock.calls.at(-1)?.[3]).toBe(1);
    });
    it("hides the thread when an older page reports a different saved scope", async () => {
      deps.stored.mockResolvedValue(storedFixture(newest()));
      deps.read.mockImplementation((_t, _id, _s, before?: number | null) => Promise.resolve(before
        ? { ...older(), conversation: { ...older().conversation, scope_status: "unavailable", library_scope: null } } : newest()));
      const { result } = renderHook(() => useChatThread(conversation));
      await waitFor(() => expect(result.current.canLoadOlder).toBe(true));
      await act(async () => { expect(await result.current.loadOlder()).toBe(false); });
      expect(result.current.messages).toEqual([]); expect(result.current.activeConv).toBeNull();
      expect(result.current.err).toMatch("scope changed");
    });
    it("keeps the reader's place when Load earlier messages prepends turns", async () => {
      deps.query = new URLSearchParams({ id: conversation }).toString();
      window.history.replaceState(null, "", `/chats?${deps.query}`);
      const page = deferred<ScopedConversationDetail>();
      deps.stored.mockResolvedValue(storedFixture(newest()));
      deps.read.mockImplementation((_t, _id, _s, before?: number | null) => before ? page.promise : Promise.resolve(newest()));
      render(<ChatsPage />);
      const button = await screen.findByRole("button", { name: "Load earlier messages" });
      const scroller = button.closest(".overflow-y-auto") as HTMLElement;
      let height = 1000;
      Object.defineProperty(scroller, "scrollHeight", { configurable: true, get: () => height });
      scroller.scrollTop = 100;
      fireEvent.click(button);
      expect(await screen.findByRole("button", { name: "Verifying earlier messages…" })).toBeTruthy();
      height = 1600;
      await act(async () => { page.resolve(older()); });
      expect(await screen.findByText("Oldest question")).toBeTruthy();
      expect(scroller.scrollTop).toBe(700);
      expect(screen.queryByRole("button", { name: "Load earlier messages" })).toBeNull();
    });
  });
});

it("resets progress for a new attempt, ignores older attempts and fences an aborted final", async () => {
  const pending = deferred<ScopedAnswer>();
  deps.ask.mockReturnValue(pending.promise);
  const { result } = renderHook(() => useChatThread(conversation));
  await waitFor(() => expect(result.current.messages).toHaveLength(1));
  let sending!: Promise<boolean>;
  act(() => { sending = result.current.send("Question"); });
  const progress = deps.ask.mock.calls[0][3];
  act(() => progress({ stage: "retrieved", sources: 12, attempt: 1 }));
  expect(result.current.messages.at(-1)).toMatchObject({ progress: { kind: "retrieved", sources: 12 }, text: "" });
  act(() => progress({ stage: "searching", attempt: 2 }));
  act(() => progress({ stage: "checking", attempt: 1 }));
  expect(result.current.messages.at(-1)).toMatchObject({ attempt: 2, progress: { kind: "searching" } });
  act(() => result.current.stop());
  expect(deps.ask.mock.calls[0][2].aborted).toBe(true);
  expect(result.current.messages.at(-1)).toMatchObject({ stopped: true, pending: false, progress: null, text: "" });
  await act(async () => { pending.resolve(answer()); expect(await sending).toBe(false); });
  expect(result.current.messages.at(-1)).toMatchObject({ stopped: true, text: "" });
});

describe("stopped request reconciliation", () => {
  const savedQuestion = () => ({ ...detail(), messages: [
    ...detail().messages,
    { id: passage, role: "user" as const, text: "Stopped question", mode: null },
  ] });
  async function stopped(id: string | null = conversation) {
    const pending = deferred<ScopedAnswer>();
    deps.ask.mockReturnValueOnce(pending.promise);
    const hook = renderHook(() => useChatThread(id));
    await waitFor(() => expect(hook.result.current.readOnly).toBe(false));
    let sending!: Promise<boolean>;
    act(() => { sending = hook.result.current.send("Stopped question"); });
    act(() => hook.result.current.stop());
    const key = (hook.result.current.messages.at(-1) as AssistantTurn).key;
    return { ...hook, pending, sending, key };
  }
  it("recovers a question saved before cancellation and retries its ID exactly once", async () => {
    const { result, pending, sending, key } = await stopped();
    deps.thread.mockResolvedValue(savedQuestion());
    await act(async () => { expect(await result.current.retry(key)).toBe(false); });
    expect(deps.ask).toHaveBeenCalledTimes(1);
    expect(result.current.messages.at(-1)).toMatchObject({ key: passage, role: "user" });
    await act(async () => {
      pending.resolve(answer());
      expect(await sending).toBe(false);
    });
    expect(result.current.messages.at(-1)?.key).toBe(passage);
    const retryAnswer = deferred<ScopedAnswer>();
    deps.ask.mockReturnValueOnce(retryAnswer.promise);
    let retrying!: Promise<boolean>;
    act(() => { retrying = result.current.retry(passage); });
    await act(async () => { expect(await result.current.retry(passage)).toBe(false); });
    expect(deps.ask).toHaveBeenCalledTimes(2);
    expect(deps.ask.mock.calls[1][1]).toMatchObject({ retryMessageId: passage, conversationId: conversation });
    expect(result.current.messages.filter((m) => m.role === "user")).toHaveLength(1);
    await act(async () => { retryAnswer.resolve(answer()); await retrying; });
  });
  it("restores an already saved final without issuing another ask", async () => {
    const { result, pending, sending, key } = await stopped();
    deps.thread.mockResolvedValue(detail());
    await act(async () => { await result.current.retry(key); await result.current.retry(key); });
    expect(deps.ask).toHaveBeenCalledTimes(1);
    expect(result.current.messages).toHaveLength(1);
    expect(result.current.messages[0].text).toBe("Saved private answer");
    await act(async () => { pending.resolve(answer()); expect(await sending).toBe(false); });
    expect(result.current.messages[0].text).toBe("Saved private answer");
  });
  it("requires history recovery for an unknown ID, including repeated retry and a late final", async () => {
    const { result, pending, sending, key } = await stopped(null);
    await act(async () => { await result.current.retry(key); await result.current.retry(key); });
    expect(result.current.err).toContain("Open it from chat history");
    expect(deps.thread).not.toHaveBeenCalled();
    expect(deps.ask).toHaveBeenCalledTimes(1);
    await act(async () => { pending.resolve(answer()); expect(await sending).toBe(false); });
    expect(result.current.activeId).toBeNull();
    expect(result.current.messages.at(-1)).toMatchObject({ stopped: true, text: "" });
  });
  it.each(["scope", "thread"] as const)("rejects a reconciliation response after a %s change", async (change) => {
    const { result, pending, sending, key } = await stopped();
    const read = deferred<ScopedConversationDetail>();
    deps.thread.mockReturnValueOnce(read.promise);
    let retrying!: Promise<boolean>;
    act(() => { retrying = result.current.retry(key); });
    act(() => { if (change === "scope") result.current.clearNarrowing(); else result.current.newChat(); });
    await act(async () => { read.resolve(savedQuestion()); await retrying; pending.resolve(answer()); await sending; });
    expect(result.current.messages).toEqual([]);
    expect(result.current.activeConv).toBeNull();
    expect(deps.ask).toHaveBeenCalledTimes(1);
  });
});

vi.mock("./ChatEmpty", () => ({ ChatEmpty: () => null }));

it('preserves verified answer decorations when the first Ask adopts its own persisted id, without carrying them to another chat', async () => {
  const initial = new URLSearchParams({ library_scope: JSON.stringify(scope) }).toString();
  window.history.replaceState(null, '', `/chats?${initial}`);
  const hook = renderHook(({ id, params }: { id: string | null; params: string }) => useChatThread(id, params), { initialProps: { id: null as string | null, params: initial } });
  await waitFor(() => expect(hook.result.current.readOnly).toBe(false));
  await act(async () => { expect(await hook.result.current.send('First question')).toBe(true); });
  expect((hook.result.current.messages[0] as AssistantTurn).prompt).toBe('First question');
  const adopted = new URLSearchParams({ id: conversation }).toString();
  window.history.replaceState(null, '', `/chats?${adopted}`);
  hook.rerender({ id: conversation, params: adopted });
  await waitFor(() => expect(hook.result.current.messages).toHaveLength(1));
  expect((hook.result.current.messages[0] as AssistantTurn).prompt).toBe('First question');
  const pending = deferred<ReturnType<typeof storedFixture>>(); deps.stored.mockReturnValue(pending.promise);
  const other = '77777777-7777-4777-8777-777777777777';
  const moved = new URLSearchParams({ id: other }).toString(); window.history.replaceState(null, '', `/chats?${moved}`);
  hook.rerender({ id: other, params: moved });
  expect(hook.result.current.messages).toEqual([]);
});

it('uses Find tiles from the current Stored message and clears them when present omits them without changing computed_at', async () => {
  const tile = { source_item_id: source, kind: 'meeting' as const, kind_label: 'meeting', title: 'Current admitted meeting', outcome: null, occurred_at: null, duration_ms: null,
    people: [], people_count: 0, graph: { id: graph, name: 'Ocean' }, best: true, decisions: [], reefs: [], quotes: [] };
  const withTiles = detail(); withTiles.messages[0].source_results = [tile];
  deps.stored.mockResolvedValue(storedFixture(withTiles));
  const hook = renderHook(() => useChatThread(conversation));
  await waitFor(() => expect((hook.result.current.messages[0] as AssistantTurn)?.sourceResults).toEqual([tile]));
  deps.stored.mockResolvedValue(storedFixture(detail()));
  act(() => returnToTab());
  await waitFor(() => expect((hook.result.current.messages[0] as AssistantTurn)?.sourceResults).toBeUndefined());
  expect(hook.result.current.messages[0].text).toBe('Saved private answer');
});

it('accepts recertified empty Find cards after withholding without resurrecting old cards or rewriting the saved answer', async () => {
  const tile = { source_item_id: source, kind: 'meeting' as const, kind_label: 'meeting', title: 'Withdrawn meeting card', outcome: null, occurred_at: null, duration_ms: null,
    people: [], people_count: 0, graph: { id: graph, name: 'Ocean' }, best: true, decisions: [], reefs: [], quotes: [] };
  const withTiles = detail(); withTiles.messages[0].source_results = [tile];
  deps.stored.mockResolvedValue(storedFixture(withTiles));
  const hook = renderHook(() => useChatThread(conversation));
  await waitFor(() => expect((hook.result.current.messages[0] as AssistantTurn)?.sourceResults).toEqual([tile]));

  deps.stored.mockResolvedValue({ ...storedFixture(), computed_at: null, stale: true, refresh_eta: '2026-10-31T00:00:02Z', payload: {} });
  act(() => returnToTab());
  await waitFor(() => expect(hook.result.current.messages).toEqual([]));
  expect(hook.result.current.loadingConv).toBe(true);

  const recertified = detail(); recertified.messages[0].source_results = [];
  deps.stored.mockResolvedValue({ ...storedFixture(recertified), computed_at: '2026-10-31T00:00:04Z' });
  // A withheld read is already loading: recovery uses the genuine two-second
  // owed poll, rather than a tab-return request that would duplicate it.
  await waitFor(() => expect((hook.result.current.messages[0] as AssistantTurn)?.sourceResults).toEqual([]), { timeout: 3000 });
  const saved = hook.result.current.messages[0] as AssistantTurn;
  expect(saved.text).toBe('Saved private answer');
  expect(saved.supportStatus).toBe('complete');
  expect(saved.retainedCitations).toEqual([quote]);
  expect(hook.result.current.loadingConv).toBe(false);
  expect(hook.result.current.err).toBeNull();
});

it('refuses a Stored Find tile from another graph instead of displaying its private labels', async () => {
  const data = detail(); data.messages[0].source_results = [{ source_item_id: source, kind: 'meeting', kind_label: 'meeting', title: 'Foreign private title', outcome: null, occurred_at: null, duration_ms: null,
    people: [], people_count: 0, graph: { id: '99999999-9999-4999-8999-999999999999', name: 'Other account' }, best: true, decisions: [], reefs: [], quotes: [] }];
  deps.stored.mockResolvedValue(storedFixture(data));
  const hook = renderHook(() => useChatThread(conversation));
  await waitFor(() => expect(hook.result.current.err).toBeTruthy());
  expect(hook.result.current.messages).toEqual([]);
});

// @vitest-environment jsdom
import { act, cleanup, fireEvent, screen, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { render, returnToTab } from "@/lib/testing/queryRender";
import ChatsPage from "@/app/(app)/chats/page";
import ChatHistory from "@/components/ChatHistory";
vi.mock("./ChatEmpty", () => ({ ChatEmpty: () => null }));
import type { ConversationSummary } from "@/lib/api";
import type { ScopedAnswer, ScopedConversationDetail } from "@/lib/scopedChatApi";

/* The composer's placeholder, the landing list's header, and the sidebar list
   catching up with a chat the first question just created (or renamed). */

// Token getters and graphs must be stable across renders: both hooks key
// their loading effects on them.
const deps = vi.hoisted(() => ({ query: "", ask: vi.fn(), thread: vi.fn(), list: vi.fn(), read: vi.fn(), resolve: vi.fn(), sidebar: vi.fn(),
  getToken: () => "frozen-a", getAuthToken: () => "sidebar-token", graphs: [{ id: "11111111-1111-4111-8111-111111111111", name: "Ocean" }] }));
// The first-run checklist reads Clerk and the org directory; it has its own test.
vi.mock("@/components/chat/FirstRunChecklist", () => ({ FirstRunChecklist: () => null }));
vi.mock("next/navigation", () => ({ useSearchParams: () => new URLSearchParams(deps.query), usePathname: () => "/chats", useRouter: () => ({ push: vi.fn(), replace: vi.fn() }) }));
vi.mock("@/lib/useEvidenceAdmission", () => ({ useEvidenceAdmission: () => ({ key: "user-a", ready: true, getToken: deps.getToken, graphs: deps.graphs, currentGraphId: graph, queryScope: "account-A" }) }));
vi.mock("@/lib/providers", () => ({ useApp: () => ({ getAuthToken: deps.getAuthToken, authReady: true, queryScope: "account-A" }) }));
vi.mock("@/lib/api", async (actual) => {
  const real = await actual<typeof import("@/lib/api")>();
  return { ...real, apiRequest: vi.fn((path: string) => {
    if (path === "/v1/conversations") return deps.list();
    if (path.startsWith("/v1/load/conversations/")) return Promise.resolve({ model: "conversation_detail", version: 1, computed_at: "2026-10-31T00:00:00Z", stale: false, refresh_eta: null,
      payload: { ...detail(server[0]?.title ?? "First question"), resolved_scope: scope } });
    throw new Error(`Unexpected mount read ${path}`);
  }), api: { ...real.api, conversations: { ...real.api.conversations, list: (...args: unknown[]) => deps.sidebar(...args) } } };
});
vi.mock("@/lib/scopedChatApi", async (actual) => ({ ...(await actual<typeof import("@/lib/scopedChatApi")>()), askScopedStream: (...args: unknown[]) => deps.ask(...args),
  readScopedThread: (...args: unknown[]) => deps.thread(...args), readScopedConversation: (...args: unknown[]) => deps.read(...args), listScopedConversations: (...args: unknown[]) => deps.list(...args), resolveScopedChatScope: (...args: unknown[]) => deps.resolve(...args) }));

const graph = "11111111-1111-4111-8111-111111111111";
const atom = "55555555-5555-4555-8555-555555555555";
const conversation = "66666666-6666-4666-8666-666666666666";
const scope = { graph_ids: [graph], outcome_versions: "all_retained" as const };
function answer(extra: Partial<ScopedAnswer> = {}): ScopedAnswer { return { support_status: "complete", text: "Verified answer", answer: "Verified answer", mode: "synthesized", library_scope: scope,
  retained_citations: [], outcomes: [], legacy_evidence: [], previous_library_scope: null, scope_changed: false, truncated: false, truncation_reason: null,
  answer_status: "supported", disagreement_status: "not_resolved", conversation_id: conversation, message_id: atom, query_id: atom, new_conversation: true, title: "First question", contains_synthetic_demo_evidence: false, egress_blocked: false, ...extra }; }
function summary(title: string): ConversationSummary { return { id: conversation, graph_id: graph, title, pinned: false, shared: false, mine: true, created_at: "2026-10-23", updated_at: "2026-10-23" }; }
function detail(title: string): ScopedConversationDetail { return { conversation: { ...summary(title), scope_status: "resolved", library_scope: scope },
  messages_truncated: false, older_before_seq: null, messages: [{ id: "77777777-7777-4777-8777-777777777777", seq: 0, role: "user", text: "First question" }, { id: atom, seq: 1, role: "assistant", text: "Saved answer", mode: "synthesized", support_status: "complete", library_scope: scope, retained_citations: [], outcomes: [], legacy_evidence: [], contains_synthetic_demo_evidence: false }] } as ScopedConversationDetail; }
/** What the server lists right now — the sidebar only sees it when it refetches. */
let server: ConversationSummary[] = [];

beforeEach(() => {
  deps.query = ""; server = [];
  Object.defineProperty(navigator, "connection", { configurable: true, value: { saveData: true } });
  window.history.replaceState(null, "", "/chats");
  vi.stubGlobal("matchMedia", () => ({ matches: false, addEventListener: () => {}, removeEventListener: () => {} }));
  deps.ask.mockReset().mockImplementation(async () => { server = [summary("First question")]; return answer(); });
  deps.thread.mockReset().mockResolvedValue(detail("First question"));
  deps.read.mockReset().mockImplementation((...args: unknown[]) => deps.thread(...args));
  // The sidebar and the landing list share one cache entry (lib/appQueries.ts)
  // read through the verified lister, so both see whatever the server lists.
  deps.list.mockReset().mockImplementation(async () => ({ model: "conversation_list", version: 1, computed_at: "2026-10-31T00:00:00Z", stale: false, refresh_eta: null, payload: { conversations: server, shared: [] } }));
  deps.resolve.mockReset().mockImplementation((_token, s) => Promise.resolve(s));
  deps.sidebar.mockReset().mockImplementation(async () => ({ conversations: server, shared: [] }));
});
afterEach(() => { cleanup(); vi.restoreAllMocks(); vi.unstubAllGlobals(); Reflect.deleteProperty(navigator, "connection"); });

async function askFirst(question: string) {
  await waitFor(() => expect((screen.getByRole("textbox") as HTMLTextAreaElement).disabled).toBe(false));
  fireEvent.change(screen.getByRole("textbox"), { target: { value: question } });
  await act(async () => { fireEvent.click(screen.getByRole("button", { name: "Send" })); });
  await screen.findByText("Saved answer");
}

describe("Chats list and composer", () => {
  it("says \"Ask anything…\" on a new chat and \"Ask a follow-up…\" once a question is sent", async () => {
    render(<ChatsPage />);
    expect(screen.getByRole("textbox").getAttribute("placeholder")).toBe("Ask anything…");
    await askFirst("First question");
    expect(screen.getByRole("textbox").getAttribute("placeholder")).toBe("Ask a follow-up…");
  });

  it("lists conversations once on a cold load of /chats with the sidebar mounted", async () => {
    render(<><ChatHistory /><ChatsPage /></>);
    await waitFor(() => expect(deps.list).toHaveBeenCalled());
    await act(async () => {});
    expect(deps.list).toHaveBeenCalledTimes(1);
    expect(deps.sidebar).not.toHaveBeenCalled();
  });

  it("adds a chat the first question creates to the sidebar without a reload", async () => {
    render(<><ChatHistory /><ChatsPage /></>);
    await waitFor(() => expect(deps.list).toHaveBeenCalledTimes(1));
    expect(screen.queryByRole("link", { name: /First question/ })).toBeNull();
    await askFirst("First question");
    expect(await screen.findByRole("link", { name: /First question/ })).toBeTruthy();
  });

  it("renames the sidebar row when a later read sees the chat's generated title", async () => {
    const { rerender } = render(<><ChatHistory /><ChatsPage /></>);
    await askFirst("First question");
    await screen.findByRole("link", { name: /First question/ });
    // The router mirrors the conversation's new ?id= into the page.
    deps.query = new URL(window.location.href).searchParams.toString();
    rerender(<><ChatHistory /><ChatsPage /></>);
    await screen.findByText("Saved answer");
    const calls = deps.list.mock.calls.length;
    server = [summary("Generated title")];
    deps.thread.mockResolvedValue(detail("Generated title"));
    // One tab return re-checks the open chat (lib/useReturnRefetch.ts).
    act(() => returnToTab());
    expect(await screen.findByRole("link", { name: /Generated title/ })).toBeTruthy();
    expect(deps.list.mock.calls.length).toBeGreaterThan(calls);
  });
});

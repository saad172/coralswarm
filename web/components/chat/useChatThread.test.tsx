// @vitest-environment jsdom
import { act, cleanup, waitFor } from "@testing-library/react";
import { renderHook, returnToTab } from "@/lib/testing/queryRender";
import { afterEach, beforeEach, expect, it, vi } from "vitest";
import { ApiError } from "@/lib/api";
import { TransportError } from "@/lib/transport";

// The admission hook reads Clerk + the app provider; the thread only needs its
// identity key, readiness and a token getter. Stable references, as in the app.
const admission = { key: "account-A", ready: true, getToken: () => "token", graphs: [], queryScope: "scope-A" };
vi.mock("@/lib/useEvidenceAdmission", () => ({ useEvidenceAdmission: () => admission }));

const api = vi.hoisted(() => ({
  readScopedConversation: vi.fn(),
  readScopedThread: vi.fn(),
  listScopedConversations: vi.fn(),
  resolveScopedChatScope: vi.fn(),
}));
vi.mock("@/lib/scopedChatApi", async (actual) => ({ ...(await actual<typeof import("@/lib/scopedChatApi")>()), ...api }));

const wire = vi.hoisted(() => ({ read: vi.fn() }));
vi.mock("@/lib/api", async (actual) => ({ ...(await actual<typeof import("@/lib/api")>()), apiRequest: vi.fn((path: string) => {
  if (path === "/v1/conversations") return Promise.resolve({ model: "conversation_list", version: 1, computed_at: "2026-10-31T00:00:00Z", stale: false, refresh_eta: null, payload: { conversations: [], shared: [] } });
  if (path.startsWith("/v1/load/conversations/")) return wire.read(path);
  throw new Error(`Unexpected mount read ${path}`);
}) }));
import { useChatThread } from "./useChatThread";

let visibility: DocumentVisibilityState = "visible";
beforeEach(() => {
  visibility = "visible";
  Object.defineProperty(document, "visibilityState", { configurable: true, get: () => visibility });
  api.listScopedConversations.mockResolvedValue([]);
  wire.read.mockResolvedValue(thread("first question"));
});
afterEach(() => {
  cleanup();
  vi.resetAllMocks();
  vi.restoreAllMocks();
});

const conversation = "66666666-6666-4666-8666-666666666666";
const graph = "11111111-1111-4111-8111-111111111111";
function thread(text: string) {
  return { model: "conversation_detail", version: 1, computed_at: "2026-10-31T00:00:00Z", stale: false, refresh_eta: null,
    payload: { conversation: { id: conversation, title: "Chat", library_scope: { graph_ids: [graph], outcome_versions: "all_retained" }, mine: true, scope_status: "resolved" },
      resolved_scope: { graph_ids: [graph], outcome_versions: "all_retained" },
      messages: [{ id: "77777777-7777-4777-8777-777777777777", role: "user", text, seq: 1 }], messages_truncated: false, older_before_seq: null } };
}

function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (error: unknown) => void;
  const promise = new Promise<T>((r, j) => { resolve = r; reject = j; });
  return { promise, resolve, reject };
}

const texts = (hook: { result: { current: ReturnType<typeof useChatThread> } }) =>
  hook.result.current.messages.map((m) => m.text);

async function openChat() {
  const hook = renderHook(() => useChatThread(conversation));
  await waitFor(() => expect(texts(hook)).toEqual(["first question"]));
  expect(hook.result.current.loadingConv).toBe(false);
  wire.read.mockClear();
  return hook;
}

it("keeps the open chat on screen while a tab return re-checks it, then replaces it in one update", async () => {
  const hook = await openChat();
  const reread = deferred<ReturnType<typeof thread>>();
  wire.read.mockImplementationOnce(() => reread.promise);
  act(() => returnToTab());
  await waitFor(() => expect(wire.read).toHaveBeenCalledTimes(1));
  expect(texts(hook)).toEqual(["first question"]);
  expect(hook.result.current.activeConv?.id).toBe(conversation);
  expect(hook.result.current.loadingConv).toBe(false);
  await act(async () => reread.resolve(thread("re-verified question")));
  await waitFor(() => expect(texts(hook)).toEqual(["re-verified question"]));
  expect(hook.result.current.err).toBeNull();
});

it.each([
  ["403", new ApiError("This chat or scope is unavailable.", 403)],
  ["404", new ApiError("This chat or scope is unavailable.", 404)],

])("clears the chat when the re-check is denied: %s", async (_label, denial) => {
  const hook = await openChat();
  wire.read.mockRejectedValueOnce(denial);
  act(() => returnToTab());
  await waitFor(() => expect(hook.result.current.err).toBe(denial.message));
  expect(hook.result.current.messages).toEqual([]);
  expect(hook.result.current.activeConv).toBeNull();
});

it.each([
  ["network", new TransportError("network", "/v1/chats/c1", "The chat could not be reached. Retry when the connection is restored.")],
  ["503", new ApiError("The chat could not be verified. Retry when the connection is restored.", 503)],
])("keeps the chat with a note when the re-check fails transiently: %s", async (_label, failure) => {
  const hook = await openChat();
  wire.read.mockRejectedValueOnce(failure);
  act(() => returnToTab());
  await waitFor(() => expect(hook.result.current.err).toBe(failure.message));
  expect(texts(hook)).toEqual(["first question"]);
  expect(hook.result.current.activeConv?.id).toBe(conversation);
});

it("going offline keeps the verified chat and says why it may be stale", async () => {
  const hook = await openChat();
  act(() => { window.dispatchEvent(new Event("offline")); });
  expect(texts(hook)).toEqual(["first question"]);
  expect(hook.result.current.err).toMatch(/^Offline\./);
});

it("a single return (visibilitychange + focus together) re-checks exactly once", async () => {
  await openChat();
  wire.read.mockResolvedValue(thread("first question"));
  act(() => returnToTab());
  await act(async () => new Promise((r) => setTimeout(r, 50)));
  expect(wire.read).toHaveBeenCalledTimes(1);
});

it("clears the thread on an invalid Stored certificate and cannot revive it on a later 503", async () => {
  const hook = await openChat();
  wire.read.mockResolvedValueOnce({ ...thread("private"), version: 999 });
  act(() => returnToTab());
  await waitFor(() => expect(hook.result.current.err).toContain("Could not verify"));
  expect(hook.result.current.messages).toEqual([]);
  wire.read.mockRejectedValueOnce(new ApiError("Unavailable", 503));
  act(() => returnToTab());
  await waitFor(() => expect(wire.read).toHaveBeenCalledTimes(2));
  expect(hook.result.current.messages).toEqual([]);
  expect(api.resolveScopedChatScope).not.toHaveBeenCalled();
  expect(api.readScopedConversation).not.toHaveBeenCalled();
});

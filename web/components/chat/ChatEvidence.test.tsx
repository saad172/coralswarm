// @vitest-environment jsdom
import { act, waitFor, cleanup, fireEvent, screen, within } from "@testing-library/react";
import { render, renderHook } from "@/lib/testing/queryRender";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import ChatsPage from "@/app/(app)/chats/page";
import { ThreadTurn } from "./AnswerTurn";
import { CitationPanel } from "./CitationPanel";
import { applyCitation, applyFinal, emptyAssistant, type AssistantTurn } from "./types";
import { evidenceSources } from "./evidence";
import { useNarrowChat } from "./useNarrowChat";

const deps = vi.hoisted(() => ({ chat: {} as Record<string, unknown>, replace: vi.fn() }));
// The first-run checklist reads Clerk and the org directory; it has its own test.
vi.mock("@/components/chat/FirstRunChecklist", () => ({ FirstRunChecklist: () => null }));
vi.mock("next/navigation", () => ({ useSearchParams: () => new URLSearchParams("id=conversation"), useRouter: () => ({ replace: deps.replace }) }));
vi.mock("@/components/chat/ChatIdentityBoundary", () => ({ ChatIdentityBoundary: ({ children }: { children: React.ReactNode }) => children }));
vi.mock("./useChatThread", () => ({ useChatThread: () => deps.chat }));
vi.mock("./ChatHeader", () => ({ ChatHeader: () => <div>Chat header</div> }));
vi.mock("./ChatComposer", () => ({ ChatComposer: () => <div>Chat composer</div> }));

function answer(key: string, title = key): AssistantTurn {
  return {
    ...emptyAssistant(key, "Question"), pending: false, text: "An answer [1]", mode: "synthesized",
    citations: [key], citationDetails: [{ atom_id: key, graph_id: "ocean", graph_name: `Ocean ${key}`, title, content: `Full excerpt for ${key}.\nSecond paragraph of evidence.`, source_type: "slack", timestamp: "2026-10-10T12:00:00Z" }],
  };
}
const actions = { onCite: vi.fn(), onRegenerate: vi.fn(), onAsk: vi.fn() };
function matchMedia(narrow: boolean) {
  vi.stubGlobal("matchMedia", vi.fn(() => ({ matches: narrow, addEventListener: vi.fn(), removeEventListener: vi.fn() })));
}
beforeEach(() => {
  matchMedia(false);
  localStorage.clear();
  deps.chat = {
    activeId: "conversation", activeConv: { title: "Thread" }, messages: [answer("older", "Older source"), answer("latest", "Latest source")],
    graphs: [], conversations: [], loadingConv: false, newChat: vi.fn(), regenerate: vi.fn(), send: vi.fn(), stop: vi.fn(),
  };
});
afterEach(() => { cleanup(); vi.useRealTimers(); vi.unstubAllGlobals(); vi.clearAllMocks(); });

describe("live answer progress", () => {
  it("keeps one elapsed clock across actual stages, then removes it on completed final", () => {
    vi.useFakeTimers(); vi.setSystemTime(new Date("2026-10-11T00:00:00Z"));
    const turn = emptyAssistant("pending", "Question");
    const { rerender } = render(<ThreadTurn turn={{ ...turn, progress: { kind: "searching" } }} {...actions} />);
    expect(screen.getByText("Searching your workspaces").textContent).toBe("Searching your workspaces");
    act(() => vi.advanceTimersByTime(2100));
    expect(screen.getByText("2.1s")).toBeTruthy();
    rerender(<ThreadTurn turn={{ ...turn, progress: { kind: "writing" } }} {...actions} />);
    expect(screen.getByText("Writing answer").textContent).toBe("Writing answer");
    act(() => vi.advanceTimersByTime(1000));
    expect(screen.getByText("3.1s")).toBeTruthy();
    rerender(<ThreadTurn turn={{ ...turn, pending: false, text: "Completed answer" }} {...actions} />);
    expect(screen.queryByRole("status")).toBeNull();
    expect(screen.getByText("Completed answer")).toBeTruthy();
    act(() => vi.advanceTimersByTime(500));
    expect(screen.queryByText("3.6s")).toBeNull();
  });
  it.each(["error", "rateLimited", "outOfCredits", "disconnected", "stopped"] as const)("ends waiting on %s while preserving feedback", (flag) => {
    vi.useFakeTimers();
    const turn = emptyAssistant("pending", "Question");
    const { rerender } = render(<ThreadTurn turn={turn} {...actions} />);
    rerender(<ThreadTurn turn={{ ...turn, pending: false, [flag]: flag === "error" ? "Failed request" : true }} {...actions} />);
    expect(screen.queryByRole("status")).toBeNull();
    act(() => vi.advanceTimersByTime(500));
    expect(screen.queryByText("0.5s")).toBeNull();
  });
  it("stops on a sources-only final without inventing prose", () => {
    vi.useFakeTimers();
    const turn = emptyAssistant("pending", "Question");
    const { rerender } = render(<ThreadTurn turn={turn} {...actions} />);
    const final = applyFinal(turn, { conversation_id: "c", new_conversation: false, title: "Question", message_id: "m", text: "", citations: [], mode: "retrieval_only", timing: { embed_ms: 0, retrieve_ms: 0 }, sources: [{ atom_id: "s", title: "Retrieved source", content: "Evidence", timestamp: null, source_type: "reference" }] });
    rerender(<ThreadTurn turn={final} {...actions} />);
    act(() => vi.advanceTimersByTime(500));
    expect(screen.queryByText("0.5s")).toBeNull();
    expect(screen.getByRole("button", { name: "Open source 1: Retrieved source" })).toBeTruthy();
    expect(screen.getByText("1 source · not synthesized")).toBeTruthy();
  });
});

describe("answer-owned evidence", () => {
  it("opens on Sources, opens older citations, and retains them as a new answer arrives", () => {
    const { rerender } = render(<ChatsPage />);
    expect(screen.queryByRole("dialog")).toBeNull();
    expect(screen.queryByRole("tab", { name: /Context/ })).toBeNull();
    fireEvent.click(screen.getAllByRole("link", { name: "Open source 1" })[0]);
    const panel = screen.getByRole("dialog");

    expect(within(panel).getByRole("heading", { name: "Older source" })).toBeTruthy();
    expect(within(panel).getByRole("link", { name: "View captured source" }).getAttribute("href")).toBe("/coral?id=older&graph_id=ocean");
    expect(within(panel).getByText("Ocean older")).toBeTruthy();
    expect(within(panel).getByText(/Full excerpt for older/)).toBeTruthy();
    deps.chat.messages = [answer("older", "Older source"), answer("latest", "Latest source"), answer("new", "New source")];
    rerender(<ChatsPage />);
    expect(within(panel).getByRole("heading", { name: "Older source" })).toBeTruthy();
    fireEvent.click(screen.getByRole("button", { name: "Open source 1: Latest source" }));
    expect(within(panel).getByRole("heading", { name: "Latest source" })).toBeTruthy();
  });
  it("preserves a closed desktop preference on mount and selects source-only rail evidence by number", () => {
    localStorage.setItem("cs_chat_drawer", "closed");
    deps.chat.messages = [{ ...answer("retrieved"), text: "", mode: "retrieval_only", citations: [], citationDetails: [], sources: [
      { atom_id: "a", title: "First source", content: "First excerpt", source_type: "reference", timestamp: null },
      { atom_id: "b", title: "Second source", content: "Second excerpt", source_type: "meeting", timestamp: null },
    ] }];
    render(<ChatsPage />);
    expect(localStorage.getItem("cs_chat_drawer")).toBe("closed");
    expect(screen.queryByRole("complementary")).toBeNull();
    fireEvent.click(screen.getByRole("button", { name: "Open source 2: Second source" }));
    expect(screen.getByRole("heading", { name: "Second source" })).toBeTruthy();
  });
  it("does not expose source_oceans IDs as ocean names", () => {
    const turn = answer("unnamed");
    turn.citationDetails[0].graph_name = null;
    turn.sourceOceans = { unnamed: "d82e762e-53d8-4637-8f4a-3daa47dd0a92" };
    expect(evidenceSources(turn)[0].ocean).toBeNull();
    turn.citationDetails = [];
    turn.sources = [{ atom_id: "unnamed", title: "Source", content: "Excerpt", source_type: "reference", timestamp: null }];
    expect(evidenceSources(turn)[0].ocean).toBeNull();
  });
  it("does not expose padded citation gaps or renumber later sources", () => {
    const turn = applyCitation(emptyAssistant("gap", "Question"), { n: 3, atom_id: "third", graph_id: "ocean", title: "Third source" });
    expect(evidenceSources(turn).map((source) => [source.n, source.title])).toEqual([[3, "Third source"]]);
  });
});

describe("mobile evidence sheet", () => {
  it("traps focus and closes with Escape, restoring focus and scroll without changing desktop preference", async () => {
    matchMedia(true);
    localStorage.setItem("cs_chat_drawer", "closed");
    render(<ChatsPage />);
    expect(screen.queryByRole("dialog")).toBeNull();
    const trigger = screen.getByRole("button", { name: "Open source 1: Older source" });
    trigger.focus(); fireEvent.click(trigger);
    const dialog = screen.getByRole("dialog");
    expect(dialog.getAttribute("aria-modal")).toBe("true");
    expect(document.body.style.overflow).toBe("hidden");
    expect(dialog.contains(document.activeElement)).toBe(true);
    const buttons = within(dialog).getAllByRole("button");
    const last = buttons[buttons.length - 1];
    last.focus(); fireEvent.keyDown(last, { key: "Tab" });
    expect(document.activeElement).toBe(buttons[0]);
    fireEvent.keyDown(document, { key: "Escape" });
    await waitFor(() => expect(screen.queryByRole("dialog")).toBeNull());
    expect(document.activeElement).toBe(trigger);
    expect(document.body.style.overflow).toBe("");
    expect(localStorage.getItem("cs_chat_drawer")).toBe("closed");
  });
  it("closes on outside mousedown and cleans up on unmount", async () => {
    matchMedia(true);
    const onClose=vi.fn();
    const {unmount}=render(<CitationPanel turn={answer("older")} n={1} onClose={onClose}/>);
    fireEvent.mouseDown(document.body);
    await waitFor(()=>expect(onClose).toHaveBeenCalledOnce());
    unmount();
    expect(document.body.style.overflow).toBe("");
  });
});


describe("older WebKit media-query compatibility", () => {
  it("responds to viewport changes and cleans up with legacy-only listeners", () => {
    let listener = () => {};
    const media = {
      matches: false,
      addListener: vi.fn((next: () => void) => { listener = next; }),
      removeListener: vi.fn(),
    };
    vi.stubGlobal("matchMedia", vi.fn(() => media));
    const { result, unmount } = renderHook(() => useNarrowChat());
    expect(result.current).toBe(false);
    expect(media.addListener).toHaveBeenCalledOnce();
    act(() => { media.matches = true; listener(); });
    expect(result.current).toBe(true);
    act(() => { media.matches = false; listener(); });
    expect(result.current).toBe(false);
    unmount();
    expect(media.removeListener).toHaveBeenCalledExactlyOnceWith(listener);
  });

  it("prefers the modern API and removes the same change listener", () => {
    const media = { matches: true, addEventListener: vi.fn(), removeEventListener: vi.fn(), addListener: vi.fn(), removeListener: vi.fn() };
    vi.stubGlobal("matchMedia", vi.fn(() => media));
    const { result, unmount } = renderHook(() => useNarrowChat());
    expect(result.current).toBe(true);
    expect(media.addEventListener).toHaveBeenCalledWith("change", expect.any(Function));
    expect(media.addListener).not.toHaveBeenCalled();
    const listener = media.addEventListener.mock.calls[0][1];
    unmount();
    expect(media.removeEventListener).toHaveBeenCalledExactlyOnceWith("change", listener);
    expect(media.removeListener).not.toHaveBeenCalled();
  });
});

vi.mock("./ChatEmpty", () => ({ ChatEmpty: () => null }));
vi.mock("./AnswerFeedback", () => ({ AnswerFeedback: () => null, FeedbackAvailability: ({ children }: { children: React.ReactNode }) => children }));

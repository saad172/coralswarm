import { act, fireEvent, render, screen, waitFor } from "@testing-library/preact";
import { afterEach, describe, expect, it, vi } from "vitest";
import { LegacyResultCard } from "./ResultCard";
import { INITIALIZE_ID } from "./bridge";
import { installFakeOpenAi } from "../test/fakeOpenAi";

const answer = {
  mode: "synthesized", answer: "The decision is **ready** [1].",
  citation_details: [{ atom_id: "a1", title: "Decision record", source_type: "note", content: "Keep the real source excerpt.", author_name: "Maya", timestamp: "2026-10-11T12:00:00Z" }],
  citations: ["a1"], unreachable_oceans: [],
};
const disposers: (() => void)[] = [];
afterEach(() => { for (const dispose of disposers.splice(0)) dispose(); });
function parentHost() {
  const iframe = document.createElement("iframe");
  document.body.appendChild(iframe);
  const parent = iframe.contentWindow!;
  const previous = Object.getOwnPropertyDescriptor(window, "parent")!;
  Object.defineProperty(window, "parent", { configurable: true, value: parent });
  const post = vi.spyOn(parent, "postMessage").mockImplementation(() => {});
  disposers.push(() => { post.mockRestore(); Object.defineProperty(window, "parent", previous); iframe.remove(); });
  const receive = (message: object, source: Window = parent, origin = "https://host.example") => act(() => { window.dispatchEvent(new MessageEvent("message", { data: { jsonrpc: "2.0", ...message }, source, origin })); });
  return { parent, post, receive, notify: (method: string, params?: unknown) => receive({ method, params }) };
}

describe("cached result-card-v1 compatibility", () => {
  it("keeps the exact v1 handshake and acknowledges its fixed response id once", () => {
    const host = parentHost();
    render(<LegacyResultCard />);
    expect(host.post).toHaveBeenCalledWith({ jsonrpc: "2.0", id: INITIALIZE_ID, method: "ui/initialize", params: {
      protocolVersion: "2026-01-26", appInfo: { name: "CoralSwarm result card", version: "1.0.0" }, appCapabilities: { availableDisplayModes: ["inline"] },
    } }, "*");
    host.receive({ id: INITIALIZE_ID, result: {} });
    expect(host.post).toHaveBeenLastCalledWith({ jsonrpc: "2.0", method: "ui/notifications/initialized" }, "https://host.example");
    expect(host.post.mock.calls.filter(([message]) => (message as { method: string }).method === "ui/initialize")).toHaveLength(1);
  });
  it("renders synchronous OpenAI data through the actual shared frame/evidence and focuses its source", async () => {
    installFakeOpenAi({ toolOutput: answer, theme: "dark" });
    const { container } = render(<LegacyResultCard />);
    expect(container.querySelector(".csau-card-frame[data-theme=dark]")).not.toBeNull();
    screen.getByRole("button", { name: "Open source 1" }).focus();
    fireEvent.click(screen.getByRole("button", { name: "Open source 1" }));
    expect(screen.getByRole("heading", { name: "Decision record" })).toBe(document.activeElement);
    expect(container.querySelector(".csau-excerpt-text")?.textContent).toBe("Keep the real source excerpt.");
    expect(screen.getByText("Maya")).toBeTruthy();
    fireEvent.click(screen.getByRole("button", { name: "Close source" }));
    await waitFor(() => expect(document.activeElement).toBe(screen.getByRole("button", { name: "Open source 1" })));
  });
  it("supports later OpenAI globals and clears an old result for a new invocation", async () => {
    const api = installFakeOpenAi({ toolOutput: answer });
    render(<LegacyResultCard />);
    act(() => api.setGlobals({ toolOutput: null, theme: "dark" }));
    expect(screen.queryByText("Decision record")).toBeNull();
    expect(screen.getByRole("status").textContent).toBe("Waiting for the tool result…");
    act(() => api.setGlobals({ toolOutput: { results: [] } }));
    await waitFor(() => expect(screen.getByText("No matching corals were supplied.")).toBeTruthy());
  });
  it("accepts old parent tool results before initialization, ignores foreign frames/origins and retains null results", () => {
    const host = parentHost();
    render(<LegacyResultCard />);
    host.receive({ method: "ui/notifications/tool-result", params: { structuredContent: { forged: true } } }, window);
    expect(screen.queryByText(/forged/)).toBeNull();
    host.notify("ui/notifications/tool-result", { structuredContent: answer });
    expect(screen.getByText("Decision record")).toBeTruthy();
    host.receive({ method: "ui/notifications/tool-result", params: { structuredContent: { forged: true } } }, host.parent, "https://other.example");
    host.notify("ui/notifications/tool-result", { structuredContent: null });
    expect(screen.getByText("Decision record")).toBeTruthy();
  });
  it("honors cancellation, new input and host errors without retaining old evidence", () => {
    const host = parentHost();
    render(<LegacyResultCard />);
    host.notify("ui/notifications/tool-result", { structuredContent: answer });
    host.notify("ui/notifications/tool-cancelled");
    expect(screen.getByText("The tool call was cancelled.")).toBeTruthy();
    expect(screen.queryByText("Decision record")).toBeNull();
    host.notify("ui/notifications/tool-input", { arguments: {} });
    expect(screen.getByRole("status").textContent).toBe("Waiting for the tool result…");
    host.notify("ui/notifications/tool-result", { isError: true });
    expect(screen.getByText("The tool could not complete this request.")).toBeTruthy();
  });
  it("does not roll standard results back to late OpenAI output echoes", () => {
    const api = installFakeOpenAi({ toolOutput: { original: true } });
    const host = parentHost();
    render(<LegacyResultCard />);
    host.notify("ui/notifications/tool-result", { structuredContent: answer });
    act(() => api.setGlobals({ toolOutput: { original: true } }));
    expect(screen.getByText("Decision record")).toBeTruthy();
  });
  it("renders unknown historical shapes completely and escapes HTML-shaped content", () => {
    const html = '<img src=x onerror="window.compromised=true">';
    installFakeOpenAi({ toolOutput: { custom_old_result: [html, { nested: true }] } });
    const { container } = render(<LegacyResultCard />);
    expect(container.querySelector("pre")?.textContent).toBe(JSON.stringify({ custom_old_result: [html, { nested: true }] }, null, 2));
    expect(container.querySelector("img")).toBeNull();
  });
  it("escapes answer/source text and disables unresolved citation markers", () => {
    const html = "<script>window.compromised=true</script>";
    installFakeOpenAi({ toolOutput: { ...answer, answer: `${html} [1] [99]`, citation_details: [{ atom_id: "a1", title: html, content: html }] } });
    const { container } = render(<LegacyResultCard />);
    fireEvent.click(screen.getByRole("button", { name: "Open source 1" }));
    expect(container.querySelector("script")).toBeNull();
    expect((screen.getByRole("button", { name: "Open source 99" }) as HTMLButtonElement).disabled).toBe(true);
    expect(screen.getByRole("heading", { name: html })).toBeTruthy();
  });
  it.each([
    [{ results: [{ atom_id: "s1", title: "Search result", snippet: "Search excerpt", source_type: "chat" }] }, "Search corals", "Search result"],
    [{ events: [{ id: "e1", event_type: "query_run", payload: { citation_count: 2 }, created_at: "2026-10-11T12:00:00Z" }] }, "Recent activity", "Query run · 2 citations."],
    [{ reef_id: "r1", title: "Reef summary", digest: { summary: "True digest", excerpts: [{ atom_id: "a1", excerpt: "Digest excerpt" }] }, relationships: [{ inferred: true }] }, "Reef summary", "a1"],
  ])("adopts shared evidence for recognized historical results while retaining reef details", (output, title, sourceTitle) => {
    installFakeOpenAi({ toolOutput: output });
    const { container } = render(<LegacyResultCard />);
    expect(screen.getByRole("heading", { name: title })).toBeTruthy();
    expect(container.querySelector(".csau-evidence-list")).not.toBeNull();
    expect(screen.getByText(sourceTitle)).toBeTruthy();
    if ("reef_id" in output) expect(container.querySelector("pre")?.textContent).toContain("relationships");
  });
  it("polls a real working task through an available old OpenAI caller without guessing backend stages", async () => {
    const api = installFakeOpenAi({ toolOutput: { task_id: "task1", status: "working" }, callToolHandlers: { get_task_result: () => answer } });
    render(<LegacyResultCard />);
    await waitFor(() => expect(screen.getByText("Decision record")).toBeTruthy());
    expect(api.callToolCalls).toEqual([{ name: "get_task_result", args: { task_id: "task1" } }]);
    expect(screen.queryByText(/Retrieving|Synthesizing/)).toBeNull();
  });
  it("a notification-only parent keeps working tasks pending without unsupported tool calls", () => {
    const host = parentHost();
    render(<LegacyResultCard />);
    host.notify("ui/notifications/tool-result", { structuredContent: { status: "working", task_id: "task1" } });
    expect(screen.getByRole("status").textContent).toBe("Waiting for the tool result…");
    expect(host.post.mock.calls.some(([message]) => (message as { method: string }).method === "tools/call")).toBe(false);
    host.notify("ui/notifications/tool-result", { structuredContent: answer });
    expect(screen.getByText("Decision record")).toBeTruthy();
  });
  it("rejects a stale task result after cancellation or a newer tool invocation", async () => {
    let resolve: (value: { structuredContent: unknown }) => void = () => {};
    const api = installFakeOpenAi({ toolOutput: { task_id: "task1", status: "working" } });
    api.callTool = vi.fn(() => new Promise<{ structuredContent: unknown }>(done => { resolve = done; }));
    const host = parentHost();
    render(<LegacyResultCard />);
    await waitFor(() => expect(api.callTool).toHaveBeenCalledOnce());
    host.notify("ui/notifications/tool-cancelled");
    await act(async () => { resolve({ structuredContent: answer }); await Promise.resolve(); });
    expect(screen.getByText("The tool call was cancelled.")).toBeTruthy();
    expect(screen.queryByText("Decision record")).toBeNull();
  });
  it("uses host safe area and height budgets without adding permissions or asset requests", () => {
    const host = parentHost();
    const { container } = render(<LegacyResultCard />);
    host.receive({ id: INITIALIZE_ID, result: { hostContext: { theme: "dark", containerDimensions: { maxHeight: 240 }, safeAreaInsets: { top: 8, right: 9, bottom: 10, left: 11 }, deviceCapabilities: { touch: true } } } });
    const frame = container.querySelector<HTMLElement>(".csau-card-frame")!;
    expect(frame.dataset.theme).toBe("dark");
    expect(frame.style.maxHeight).toBe("240px");
    expect(frame.style.paddingBottom).toBe("10px");
    expect(frame.style.getPropertyValue("--csau-target-size")).toBe("44px");
    expect(container.querySelector("a,link,iframe")).toBeNull();
  });
});

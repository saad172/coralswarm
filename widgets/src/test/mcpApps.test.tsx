import { afterEach, describe, expect, it } from "vitest";
import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/preact";
import { AskOceanTemplate } from "../templates/AskOcean";
import { SearchAtomsTemplate } from "../templates/SearchAtoms";
import { GetReefTemplate } from "../templates/GetReef";
import { RecentActivityTemplate } from "../templates/RecentActivity";
import { callTool, openExternalLink, requestFullscreen, useDisplayMode, useMaxHeight, useTheme, useToolInput, useToolOutput } from "../openai";
import { installMcpHost } from "./fakeMcpHost";
import { installFakeOpenAi } from "./fakeOpenAi";
import ask from "../../fixtures/ask_ocean.synthesized.json";
import emptyAsk from "../../fixtures/ask_ocean.empty.json";
import working from "../../fixtures/ask_ocean.working.json";
import atom from "../../fixtures/get_atom.body_omitted.json";
import search from "../../fixtures/search_atoms.default.json";
import reef from "../../fixtures/get_reef.default.json";
import activity from "../../fixtures/recent_activity.default.json";

let host: ReturnType<typeof installMcpHost> | undefined;
afterEach(() => { cleanup(); host?.close(); host = undefined; });

function Probe() {
  const output = useToolOutput<{ hello: string }>();
  const input = useToolInput<{ question: string }>();
  const theme = useTheme();
  const mode = useDisplayMode();
  const maxHeight = useMaxHeight();
  return <div data-testid="probe">{[output?.hello ?? "none", input?.question ?? "none", theme, mode, maxHeight].join("|")}</div>;
}

async function initialized() {
  await waitFor(() => expect(host?.packets.some((p) => p.method === "ui/notifications/initialized")).toBe(true));
}

describe("MCP Apps bridge using standard JSON-RPC postMessage", () => {
  it("initializes once for all hooks and retains an immediate initial result without window.openai", async () => {
    host = installMcpHost({ context: { theme: "dark", displayMode: "fullscreen", containerDimensions: { maxHeight: 720 } }, onInitialized: () => {
      host!.notify("ui/notifications/tool-input", { arguments: { question: "rate limiter" } });
      host!.notify("ui/notifications/tool-result", { content: [], structuredContent: { hello: "first answer" } });
    } });
    render(<Probe />);
    await screen.findByText("first answer|rate limiter|dark|fullscreen|720");
    expect(window.openai).toBeUndefined();
    expect(host.packets.filter((p) => p.method === "ui/initialize")).toHaveLength(1);
    expect(host.targets[0]).toBe("*");
    expect(host.targets[1]).toBe("https://host.example");
    const init = host.packets.find((p) => p.method === "ui/initialize");
    const capabilities = (init?.params as { appCapabilities?: { availableDisplayModes?: string[] } } | undefined)?.appCapabilities;
    expect(capabilities?.availableDisplayModes).toEqual(["inline", "fullscreen"]);
  });

  it("applies delayed result, partial input and later host context updates", async () => {
    host = installMcpHost();
    render(<Probe />);
    await initialized();
    expect(screen.getByTestId("probe").textContent).toBe("none|none|light|inline|");
    host.notify("ui/notifications/tool-input-partial", { arguments: { question: "What did Tidepool decide?" } });
    host.notify("ui/notifications/tool-result", { content: [], structuredContent: { hello: "100 requests per minute" } });
    host.notify("ui/notifications/host-context-changed", { theme: "dark", containerDimensions: { maxHeight: 480 } });
    await screen.findByText("100 requests per minute|What did Tidepool decide?|dark|inline|480");
  });

  it("does not turn a host's current iframe height into an intrinsic content cap", async () => {
    installFakeOpenAi({ maxHeight: 179 });
    host = installMcpHost({ context: { displayMode: "inline", containerDimensions: { width: 768, height: 179 } } });
    const { container } = render(<AskOceanTemplate />);
    await initialized();
    host.notify("ui/notifications/tool-result", { content: [], structuredContent: emptyAsk.toolOutput });
    await screen.findByText("Nothing in your ocean matches that yet.");
    const wrapper = container.firstElementChild as HTMLElement;
    expect(wrapper.style.maxHeight).toBe("");
    host.notify("ui/notifications/host-context-changed", { containerDimensions: { width: 768, maxHeight: 500 } });
    await waitFor(() => {
      const body = container.querySelector("[data-scroll-body]") as HTMLElement;
      expect(body.getAttribute("data-scroll-body")).toBe("bounded");
      expect(body.style.maxHeight).toBe("412px");
    });
    host.notify("ui/notifications/host-context-changed", { containerDimensions: { width: 768, height: 179 } });
    await waitFor(() => {
      const body = container.querySelector("[data-scroll-body]") as HTMLElement;
      expect(body.getAttribute("data-scroll-body")).toBe("unbounded");
      expect(body.style.maxHeight).toBe("");
    });
  });

  it("keeps legacy height available when the standard host reports only width", async () => {
    const openai = installFakeOpenAi({ maxHeight: 500 });
    host = installMcpHost({ context: { containerDimensions: { width: 700 } } });
    render(<Probe />);
    await initialized();
    await screen.findByText("none|none|light|inline|500");
    openai.setGlobals({ maxHeight: 600 });
    await screen.findByText("none|none|light|inline|600");
    host.notify("ui/notifications/host-context-changed", { containerDimensions: { width: 390 } });
    openai.setGlobals({ maxHeight: 550 });
    await screen.findByText("none|none|light|inline|550");
    host.notify("ui/notifications/host-context-changed", { containerDimensions: { maxHeight: 400 } });
    await screen.findByText("none|none|light|inline|400");
    openai.setGlobals({ maxHeight: 800 });
    expect(screen.getByTestId("probe").textContent).toBe("none|none|light|inline|400");
  });

  it("rejects foreign windows, changed parent origins, and malformed messages", async () => {
    host = installMcpHost();
    render(<Probe />);
    await initialized();
    const notification = { method: "ui/notifications/tool-result", params: { content: [], structuredContent: { hello: "spoofed" } } };
    host.receive(notification, { source: window });
    host.receive(notification, { origin: "https://untrusted.example" });
    host.receive({ jsonrpc: "wrong", ...notification });
    // A genuine subsequent event establishes that queued work was processed.
    host.notify("ui/notifications/host-context-changed", { theme: "dark" });
    await screen.findByText("none|none|dark|inline|");
  });

  it("retains OpenAI compatibility while standard results replace initial globals", async () => {
    const openai = installFakeOpenAi({ toolOutput: { hello: "legacy" } });
    host = installMcpHost();
    render(<Probe />);
    expect(screen.getByTestId("probe").textContent).toContain("legacy");
    await initialized();
    host.notify("ui/notifications/tool-result", { content: [], structuredContent: { hello: "standard" } });
    await screen.findByText("standard|none|light|inline|");
    openai.setGlobals({ toolOutput: { hello: "stale legacy" }, theme: "dark", maxHeight: 600 });
    await screen.findByText("standard|none|light|inline|600");
    // Calls route over MCP Apps after its handshake, even if globals exist.
    await callTool("get_atom", { atom_id: "abc" });
    expect(openai.callToolCalls).toHaveLength(0);
    expect(host.packets.filter((p) => p.method === "tools/call")).toHaveLength(1);
  });

  it.each([
    ["ask ocean", AskOceanTemplate, ask, /Token bucket per API key/],
    ["search atoms", SearchAtomsTemplate, search, /corals match/],
    ["reef", GetReefTemplate, reef, /Rate limiting/],
    ["recent activity", RecentActivityTemplate, activity, /recent activity/],
  ] as const)("renders the designed %s template from standard input/results", async (_name, Template, fixture, text) => {
    host = installMcpHost();
    render(<Template />);
    await initialized();
    host.notify("ui/notifications/tool-input", { arguments: fixture.toolInput });
    host.notify("ui/notifications/tool-result", { content: [], structuredContent: fixture.toolOutput });
    expect((await screen.findAllByText(text)).length).toBeGreaterThan(0);
  });

  it("expands supplied citations inline and retrieves only the omitted body via tools/call", async () => {
    host = installMcpHost({ callTool: () => atom });
    render(<AskOceanTemplate />);
    await initialized();
    host.notify("ui/notifications/tool-input", { arguments: ask.toolInput });
    host.notify("ui/notifications/tool-result", { content: [], structuredContent: ask.toolOutput });
    await screen.findByText(/Token bucket per API key/);
    fireEvent.click(screen.getByRole("button", { name: "2" }));
    expect(host.packets.filter((p) => p.method === "tools/call")).toHaveLength(0);
    fireEvent.click(screen.getByRole("button", { name: "5" }));
    await screen.findByText(atom.content);
    expect(host.packets.filter((p) => p.method === "tools/call").map((p) => ({ name: p.params?.name, arguments: p.params?.arguments }))).toEqual([{ name: "get_atom", arguments: { atom_id: atom.atom_id } }]);
  });

  it("polls legacy task envelopes through the standard tools/call bridge", async () => {
    host = installMcpHost({ callTool: () => ask.toolOutput });
    render(<AskOceanTemplate />);
    await initialized();
    host.notify("ui/notifications/tool-result", { content: [], structuredContent: working.toolOutput });
    await screen.findByText(/Token bucket per API key/);
    expect(host.packets.filter((p) => p.method === "tools/call").map((p) => p.params?.name)).toEqual(["get_task_result"]);
  });

  it("clears the old answer when new tool input arrives and ignores a late old task response", async () => {
    let completeOldPoll: ((value: unknown) => void) | undefined;
    host = installMcpHost({ callTool: () => new Promise((resolve) => { completeOldPoll = resolve; }) });
    render(<AskOceanTemplate />);
    await initialized();
    host.notify("ui/notifications/tool-input", { arguments: ask.toolInput });
    host.notify("ui/notifications/tool-result", { content: [], structuredContent: ask.toolOutput });
    await screen.findByText(/Token bucket per API key/);
    host.notify("ui/notifications/tool-input", { arguments: { question: "A new question" } });
    await screen.findByText(/Waiting for your answer…/);
    expect(screen.queryByText(/Token bucket per API key/)).toBeNull();
    host.notify("ui/notifications/tool-result", { content: [], structuredContent: working.toolOutput });
    await waitFor(() => expect(completeOldPoll).toBeDefined());
    host.notify("ui/notifications/tool-input", { arguments: { question: "The newest question" } });
    completeOldPoll!(ask.toolOutput);
    host.notify("ui/notifications/tool-result", { content: [], isError: true });
    await screen.findByText("The tool could not complete this request.");
    expect(screen.queryByText(/Token bucket per API key/)).toBeNull();
  });

  it.each([
    ["ask ocean", AskOceanTemplate, ask],
    ["search", SearchAtomsTemplate, search],
    ["reef", GetReefTemplate, reef],
    ["activity", RecentActivityTemplate, activity],
  ] as const)("renders standard error and cancellation safely in the designed %s shell", async (_name, Template, fixture) => {
    host = installMcpHost();
    render(<Template />);
    await initialized();
    host.notify("ui/notifications/tool-result", { content: [], structuredContent: fixture.toolOutput });
    host.notify("ui/notifications/tool-result", { content: [], isError: true });
    await screen.findByText("The tool could not complete this request.");
    host.notify("ui/notifications/tool-cancelled", {});
    await screen.findByText("The request was cancelled.");
  });

  it("correlates tool failures, rejects disallowed tools, and sends standard fullscreen/link requests", async () => {
    host = installMcpHost({ callTool: () => Promise.reject(new Error("Host rejected")) });
    render(<Probe />);
    await initialized();
    await expect(callTool("get_atom", { atom_id: "abc" })).rejects.toThrow("Tool failed");
    await expect(callTool("delete_atom", {})).rejects.toThrow("not available to widgets");
    requestFullscreen();
    openExternalLink("https://app.coralswarm.example/chats");
    await screen.findByText("none|none|light|fullscreen|");
    expect(host.packets.some((p) => p.method === "ui/open-link" && p.params?.url === "https://app.coralswarm.example/chats")).toBe(true);
    expect(host.packets.filter((p) => p.method === "tools/call")).toHaveLength(1);
  });

  it("does not request fullscreen after the host reports an empty display-mode list", async () => {
    host = installMcpHost({ context: { availableDisplayModes: [] } });
    render(<Probe />);
    await initialized();
    requestFullscreen();
    await Promise.resolve();
    expect(host.packets.filter((p) => p.method === "ui/request-display-mode")).toHaveLength(0);
    expect(screen.getByTestId("probe").textContent).toBe("none|none|light|inline|");
  });

  it("clears a prior fullscreen capability when the host later reports no modes", async () => {
    host = installMcpHost({ context: { availableDisplayModes: ["inline", "fullscreen"] } });
    render(<Probe />);
    await initialized();
    host.notify("ui/notifications/host-context-changed", { theme: "dark", availableDisplayModes: [] });
    await screen.findByText("none|none|dark|inline|");
    requestFullscreen();
    await Promise.resolve();
    expect(host.packets.filter((p) => p.method === "ui/request-display-mode")).toHaveLength(0);
    expect(screen.getByTestId("probe").textContent).toBe("none|none|dark|inline|");
  });

  it("discards a rejected tool call from the previous invocation", async () => {
    let rejectOldCall: ((error: Error) => void) | undefined;
    host = installMcpHost({ callTool: () => new Promise((_resolve, reject) => { rejectOldCall = reject; }) });
    render(<Probe />);
    await initialized();
    const oldCall = callTool("get_task_result", { task_id: "old" });
    await waitFor(() => expect(rejectOldCall).toBeDefined());
    host.notify("ui/notifications/tool-input", { arguments: { question: "new invocation" } });
    rejectOldCall!(new Error("Previous task failed"));
    await expect(oldCall).resolves.toBeNull();
  });

  it("stops receiving after unmount and remounts without cached prior-user data or duplicate handlers", async () => {
    host = installMcpHost();
    const first = render(<Probe />);
    await initialized();
    host.notify("ui/notifications/tool-result", { content: [], structuredContent: { hello: "old user" } });
    await screen.findByText("old user|none|light|inline|");
    first.unmount();
    host.notify("ui/notifications/tool-result", { content: [], structuredContent: { hello: "stale update" } });
    render(<Probe />);
    await waitFor(() => expect(host!.packets.filter((p) => p.method === "ui/initialize")).toHaveLength(2));
    expect(screen.getByTestId("probe").textContent).toBe("none|none|light|inline|");
  });
});

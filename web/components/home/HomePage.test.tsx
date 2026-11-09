// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import { act, cleanup, fireEvent, render as rtlRender, screen, waitFor, within } from "@testing-library/react";
import { QueryClientProvider } from "@tanstack/react-query";
import { createElement, type AnchorHTMLAttributes, type ComponentProps, type ReactNode } from "react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { render } from "@/lib/testing/queryRender";
import { makeQueryClient } from "@/lib/queryClient";
import { HEADER_META_ID } from "@/components/HeaderMeta";
import HomePage from "@/app/(app)/home/page";
import type { AttentionResponse, ContinueResponse, WhatsNewResponse } from "@/lib/homeLaunchpadApi";

const wire = vi.hoisted(() => ({ fetch: vi.fn() }));
// Counts every render of the checklist, so a one-frame flash is caught.
const checklist = vi.hoisted(() => ({ renders: 0 }));
vi.mock("@/components/home/Launchpad", async (original) => {
  const mod = await original<typeof import("@/components/home/Launchpad")>();
  return {
    ...mod,
    SetupChecklist: (props: ComponentProps<typeof mod.SetupChecklist>) => {
      checklist.renders++;
      return createElement(mod.SetupChecklist, props);
    },
  };
});
const push = vi.hoisted(() => vi.fn());
vi.mock("@/lib/transport", async (original) => ({ ...await original<typeof import("@/lib/transport")>(), guardedFetch: wire.fetch }));
vi.mock("next/navigation", () => ({ useRouter: () => ({ push }) }));
vi.mock("next/link", () => ({
  default: ({ children, href, ...rest }: { children: ReactNode; href: string } & AnchorHTMLAttributes<HTMLAnchorElement>) => (
    <a href={href} {...rest}>{children}</a>
  ),
}));
const session = vi.hoisted(() => ({ authReady: true, firstName: "Dana" }));
vi.mock("@clerk/nextjs", () => ({ useUser: () => ({ user: { firstName: session.firstName, fullName: `${session.firstName} Ross` } }) }));
vi.mock("@/lib/providers", () => ({
  useApp: () => ({ authReady: session.authReady, queryScope: "scope-1", userId: "user_1", getAuthToken: () => "tok" }),
}));

const GENERATED = "2026-11-03T12:00:00Z";
const continueBody = (): ContinueResponse => ({
  generated_at: GENERATED,
  items: [
    { kind: "chat", id: "c1", title: "Why is the read model serving stale reefs?", context: "Platform", count: null, graph_id: null, at: "2026-11-03T11:48:00Z" },
    { kind: "coding", id: "s9", title: "coralswarm · cs-read-model-serve-stale", context: "claude code · last: cache warmup on cold start", count: 3, graph_id: null, at: "2026-11-03T11:20:00Z" },
    { kind: "meeting", id: "src-41", title: "Home redesign review", context: "34 min · 5 people", count: null, graph_id: "g7", at: "2026-11-03T10:00:00Z" },
    { kind: "workspace", id: "g7", title: "Platform", context: "218 reefs · shared", count: null, graph_id: null, at: "2026-11-01T12:00:00Z" },
  ],
});
const whatsNewBody = (): WhatsNewResponse => ({
  generated_at: GENERATED,
  window_days: 7,
  items: [
    { reef_id: "n1", kind: "decision", title: "Usage-based billing starts at each Q1 renewal", ocean: { graph_id: "g2", name: "Go-to-market", kind: "team" }, formed_at: "2026-11-03T07:00:00Z" },
    { reef_id: "n2", kind: "thread", title: "Connector retry and back-off policy", ocean: { graph_id: "g7", name: "Platform", kind: "org" }, formed_at: "2026-11-01T07:00:00Z" },
  ],
});
const attentionBody = (over: Partial<AttentionResponse> = {}): AttentionResponse => ({
  has_content: true, has_source: true, has_mcp: false, has_asked: false, attention: null, generated_at: GENERATED, ...over,
});
const SLACK_BROKEN = { connector_id: "k1", provider: "slack", label: "Slack needs you to sign in again", reason: "reauth" } as const;

const EMPTY_READS = { continue: { items: [], generated_at: GENERATED }, whatsNew: { items: [], window_days: 7, generated_at: GENERATED } };
type Bodies = { continue: unknown; whatsNew: unknown; attention: unknown };
function serve(bodies: Bodies) {
  const answer = (path: string) => {
    const body = path === "/v1/home/continue" ? bodies.continue : path === "/v1/home/whats-new" ? bodies.whatsNew : path === "/v1/home/attention" ? bodies.attention : null;
    if (body === null) return Promise.resolve(new Response(JSON.stringify({ error: "unexpected" }), { status: 404 }));
    return Promise.resolve(new Response(JSON.stringify(body)));
  };
  wire.fetch.mockImplementation(answer);
  return answer;
}
const homeGets = () => wire.fetch.mock.calls.map(([path]) => path as string).filter((p) => p.startsWith("/v1/home/"));

beforeEach(() => {
  session.authReady = true;
  session.firstName = "Dana";
  checklist.renders = 0;
  wire.fetch.mockReset();
  push.mockReset();
  localStorage.clear();
  const slot = document.createElement("span");
  slot.id = HEADER_META_ID;
  document.body.appendChild(slot);
});
afterEach(() => {
  cleanup();
  document.getElementById(HEADER_META_ID)?.remove();
});

describe("Home, State A (populated)", () => {
  it("makes exactly the three launchpad reads and renders every section", async () => {
    localStorage.setItem("cs.home.lastVisit.scope-1", "2026-11-02T12:00:00Z");
    serve({ continue: continueBody(), whatsNew: whatsNewBody(), attention: attentionBody() });
    render(<HomePage />);

    expect(await screen.findByRole("heading", { level: 1 })).toHaveTextContent(/Good (morning|afternoon|evening), Dana/);
    const cont = await screen.findByRole("region", { name: "Continue" });
    const chat = await within(cont).findByRole("link", { name: /Why is the read model serving stale reefs\?/ });
    expect(chat).toHaveAttribute("href", "/chats?id=c1");
    // The group count shows once, as the pill; the context line does not repeat it.
    const grouped = within(cont).getByRole("link", { name: /cs-read-model-serve-stale/ });
    expect(within(grouped).getAllByText(/3 sessions/)).toHaveLength(1);
    expect(within(grouped).getByText("coding · claude code · last: cache warmup on cold start")).toBeInTheDocument();
    // A meeting row opens its transcript in its ocean.
    expect(within(cont).getByRole("link", { name: /Home redesign review/ })).toHaveAttribute("href", "/meeting?source_id=src-41&graph_id=g7");
    expect(within(cont).getByRole("link", { name: /^Platform workspace/ })).toHaveAttribute("href", "/workspaces?id=g7");
    // Freshness is measured from this client's fetch, not the server's
    // generated_at, so a server clock behind this one cannot age fresh data.
    expect(within(cont).getByText("updated just now")).toBeInTheDocument();

    const news = screen.getByRole("region", { name: "What's new" });
    expect(within(news).getByRole("link", { name: /Usage-based billing/ })).toHaveAttribute("href", "/topics?id=n1&graph_id=g2");
    // Only the reef formed after the remembered visit gets the dot.
    expect(within(news).getAllByRole("img", { name: "new since your last visit" })).toHaveLength(1);
    expect(within(news).getByText(/^1 new since your last visit · updated/)).toBeInTheDocument();

    const starters = screen.getByRole("group", { name: "Prompt starters" });
    const chips = within(starters).getAllByRole("button");
    expect(chips.map((b) => b.textContent)).toEqual([
      "What led to “Usage-based billing starts at…”?",
      "Catch me up on cs-read-model-serve-stale",
      "Where did “Connector retry and back-off…” land?",
    ]);
    // A shortened chip carries its whole question.
    expect(chips[0]).toHaveAttribute("title", "What led to “Usage-based billing starts at each Q1 renewal”?");
    expect(chips[1]).not.toHaveAttribute("title");
    expect(screen.queryByRole("region", { name: "Needs attention" })).toBeNull();
    expect(screen.queryByRole("region", { name: "Set up your ocean" })).toBeNull();

    await waitFor(() => expect(homeGets().sort()).toEqual(["/v1/home/attention", "/v1/home/continue", "/v1/home/whats-new"]));
    // The next visit compares against what the server just generated.
    expect(localStorage.getItem("cs.home.lastVisit.scope-1")).toBe(GENERATED);
    expect(localStorage.getItem("cs.home.onboarded.scope-1")).not.toBeNull();
  });

  it("never moves the remembered visit backwards", async () => {
    // A newer visit (another tab) than this, possibly cached, answer.
    localStorage.setItem("cs.home.lastVisit.scope-1", "2026-11-03T12:30:00Z");
    serve({ continue: continueBody(), whatsNew: whatsNewBody(), attention: attentionBody() });
    render(<HomePage />);
    await screen.findByRole("link", { name: /Usage-based billing/ });
    await waitFor(() => expect(homeGets()).toHaveLength(3));
    expect(localStorage.getItem("cs.home.lastVisit.scope-1")).toBe("2026-11-03T12:30:00Z");
  });

  it("shows the needs-attention row only while a connector is broken", async () => {
    serve({
      continue: continueBody(),
      whatsNew: whatsNewBody(),
      attention: attentionBody({ attention: SLACK_BROKEN }),
    });
    render(<HomePage />);
    const row = await screen.findByRole("region", { name: "Needs attention" });
    expect(within(row).getByText("Slack needs you to sign in again")).toBeInTheDocument();
    expect(within(row).getByRole("link", { name: "Fix: Slack needs you to sign in again" })).toHaveAttribute("href", "/connectors/slack");
  });

  it("draws skeletons only before the first answer, then keeps content while refreshing", async () => {
    let release: () => void = () => {};
    const held = new Promise<void>((resolve) => { release = resolve; });
    const answer = serve({ continue: continueBody(), whatsNew: whatsNewBody(), attention: attentionBody() });
    wire.fetch.mockImplementation(async (path: string) => { await held; return answer(path); });
    render(<HomePage />);

    expect(await screen.findByRole("status", { name: "Loading your recent work" })).toBeInTheDocument();
    expect(screen.getByRole("status", { name: "Loading what's new" })).toBeInTheDocument();
    expect(screen.getByText("opening your work…")).toBeInTheDocument();
    await act(async () => release());
    await screen.findByRole("link", { name: /Why is the read model/ });
    expect(screen.queryByRole("status", { name: "Loading your recent work" })).toBeNull();

    // A manual refresh re-runs all three without dropping back to a skeleton.
    let releaseAgain: () => void = () => {};
    const heldAgain = new Promise<void>((resolve) => { releaseAgain = resolve; });
    wire.fetch.mockImplementation(async (path: string) => { await heldAgain; return answer(path); });
    fireEvent.click(screen.getByRole("button", { name: "Refresh home" }));
    expect(await screen.findByRole("button", { name: "Refreshing…" })).toBeDisabled();
    expect(screen.getAllByText(/refreshing…/).length).toBeGreaterThan(0);
    expect(screen.getByRole("link", { name: /Why is the read model/ })).toBeInTheDocument();
    expect(screen.queryByRole("status", { name: "Loading your recent work" })).toBeNull();
    await act(async () => releaseAgain());
    await waitFor(() => expect(homeGets()).toHaveLength(6));
  });

  it("re-reads all three on every visit, painting the cached answer meanwhile", async () => {
    let release: () => void = () => {};
    const held = new Promise<void>((resolve) => { release = resolve; });
    const answer = serve({ continue: continueBody(), whatsNew: whatsNewBody(), attention: attentionBody() });
    wire.fetch.mockImplementation(async (path: string) => { await held; return answer(path); });
    // A visit a moment ago left a fresh answer (inside the 5 minutes) in the cache.
    const client = makeQueryClient();
    client.setQueryData(["homeContinue", "scope-1"], continueBody());
    client.setQueryData(["homeWhatsNew", "scope-1"], whatsNewBody());
    client.setQueryData(["homeAttention", "scope-1"], attentionBody());
    const first = rtlRender(<QueryClientProvider client={client}><HomePage /></QueryClientProvider>);
    // On screen before any read answers: no skeleton, marked refreshing.
    expect(await screen.findByRole("link", { name: /Why is the read model/ })).toBeInTheDocument();
    expect(screen.queryByRole("status", { name: "Loading your recent work" })).toBeNull();
    await waitFor(() => expect(homeGets().sort()).toEqual(["/v1/home/attention", "/v1/home/continue", "/v1/home/whats-new"]));
    expect(screen.getAllByText(/refreshing…/).length).toBeGreaterThan(0);
    await act(async () => release());
    first.unmount();

    // The next visit, still inside the 5 minutes, reads all three again, once each.
    rtlRender(<QueryClientProvider client={client}><HomePage /></QueryClientProvider>);
    await waitFor(() => expect(homeGets()).toHaveLength(6));
    expect(homeGets().slice(3).sort()).toEqual(["/v1/home/attention", "/v1/home/continue", "/v1/home/whats-new"]);
  });

  it("does not refresh before the session can read", async () => {
    session.authReady = false;
    serve({ continue: continueBody(), whatsNew: whatsNewBody(), attention: attentionBody() });
    render(<HomePage />);
    const button = screen.getByRole("button", { name: "Refresh home" });
    expect(button).toBeDisabled();
    fireEvent.click(button);
    await act(async () => {});
    expect(wire.fetch).not.toHaveBeenCalled();
  });

  it("accents the name as its own word, never a match inside the greeting", async () => {
    session.firstName = "Go";
    serve({ continue: continueBody(), whatsNew: whatsNewBody(), attention: attentionBody() });
    render(<HomePage />);
    const title = await screen.findByRole("heading", { level: 1 });
    expect(title).toHaveTextContent(/^Good (morning|afternoon|evening), Go$/);
    const accents = title.querySelectorAll("em");
    expect(accents).toHaveLength(1);
    expect(accents[0]).toHaveTextContent(/^Go$/);
    expect(title.lastChild).toBe(accents[0]);
  });

  it("opens a new chat from the Ask box and from a prompt starter", async () => {
    serve({ continue: continueBody(), whatsNew: whatsNewBody(), attention: attentionBody() });
    render(<HomePage />);
    const box = screen.getByRole("textbox", { name: "Ask your ocean" });
    fireEvent.change(box, { target: { value: "When does the Orchid launch?" } });
    fireEvent.keyDown(box, { key: "Enter" });
    expect(push).toHaveBeenLastCalledWith("/chats?q=When%20does%20the%20Orchid%20launch%3F");
    const starter = await screen.findByRole("button", { name: "Catch me up on cs-read-model-serve-stale" });
    fireEvent.click(starter);
    expect(push).toHaveBeenLastCalledWith("/chats?q=Catch%20me%20up%20on%20cs-read-model-serve-stale");
    // A shortened chip asks the whole question.
    fireEvent.click(screen.getByRole("button", { name: "What led to “Usage-based billing starts at…”?" }));
    expect(push).toHaveBeenLastCalledWith(`/chats?q=${encodeURIComponent("What led to “Usage-based billing starts at each Q1 renewal”?")}`);
  });
});

describe("Home, State B (empty ocean)", () => {
  it("shows the setup checklist and hides Continue and What's new", async () => {
    serve({ continue: { items: [], generated_at: GENERATED }, whatsNew: { items: [], window_days: 7, generated_at: GENERATED }, attention: attentionBody({ has_content: false, has_source: false }) });
    render(<HomePage />);
    const setup = await screen.findByRole("region", { name: "Set up your ocean" });
    expect(within(setup).getByText("0 of 3")).toBeInTheDocument();
    expect(within(setup).getByRole("link", { name: "Connect" })).toHaveAttribute("href", "/connectors");
    expect(within(setup).getByRole("button", { name: "Copy the MCP URL https://api.coralswarm.example/mcp" })).toBeInTheDocument();
    expect(screen.getByText(/Your ocean is empty for now/)).toBeInTheDocument();
    expect(screen.queryByRole("region", { name: "Continue" })).toBeNull();
    expect(screen.queryByRole("region", { name: "What's new" })).toBeNull();
    expect(screen.queryByRole("group", { name: "Prompt starters" })).toBeNull();

    // Step 3 focuses the Ask box.
    fireEvent.click(within(setup).getByRole("button", { name: "Ask your first question" }));
    expect(screen.getByRole("textbox", { name: "Ask your ocean" })).toHaveFocus();
  });

  it("does not move the remembered visit while What's new is not shown", async () => {
    localStorage.setItem("cs.home.lastVisit.scope-1", "2026-10-20T00:00:00Z");
    serve({ ...EMPTY_READS, attention: attentionBody({ has_content: false, has_source: false }) });
    render(<HomePage />);
    await screen.findByRole("region", { name: "Set up your ocean" });
    await waitFor(() => expect(homeGets()).toHaveLength(3));
    await act(async () => {});
    expect(localStorage.getItem("cs.home.lastVisit.scope-1")).toBe("2026-10-20T00:00:00Z");
  });

  it("shows the needs-attention row above the checklist, and step 1 done but not a success", async () => {
    serve({ ...EMPTY_READS, attention: attentionBody({ has_content: false, has_source: true, attention: SLACK_BROKEN }) });
    render(<HomePage />);
    const row = await screen.findByRole("region", { name: "Needs attention" });
    const setup = screen.getByRole("region", { name: "Set up your ocean" });
    expect(row.compareDocumentPosition(setup) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();
    expect(within(row).getByRole("link", { name: "Fix: Slack needs you to sign in again" })).toHaveAttribute("href", "/connectors/slack");
    expect(within(setup).getByText("1 of 3")).toBeInTheDocument();
    const step1 = within(setup).getByText("Connect a source").closest("li");
    if (!step1) throw new Error("step 1 is not a list item");
    expect(within(step1).getByText(/Connected, but a source needs attention/)).toBeInTheDocument();
    expect(within(step1).getByText("(done, needs attention)")).toBeInTheDocument();
    expect(within(step1).queryByText(/A source is connected/)).toBeNull();
  });

  it("takes step 3 from has_asked, and reads a missing has_asked as not done", async () => {
    serve({ ...EMPTY_READS, attention: attentionBody({ has_content: false, has_source: false, has_asked: true }) });
    render(<HomePage />);
    const setup = await screen.findByRole("region", { name: "Set up your ocean" });
    await within(setup).findByText("1 of 3");
    expect(within(setup).getByText("Ask your first question").closest("li")).toHaveTextContent("(done)");
    cleanup();

    const legacy: Partial<AttentionResponse> = attentionBody({ has_content: false, has_source: false });
    delete legacy.has_asked;
    serve({ ...EMPTY_READS, attention: legacy });
    render(<HomePage />);
    await within(await screen.findByRole("region", { name: "Set up your ocean" })).findByText("0 of 3");
  });

  it("takes step 1 from has_source, not from a broken connector", async () => {
    serve({
      continue: { items: [], generated_at: GENERATED },
      whatsNew: { items: [], window_days: 7, generated_at: GENERATED },
      attention: attentionBody({ has_content: false, has_source: false, attention: SLACK_BROKEN }),
    });
    render(<HomePage />);
    const setup = await screen.findByRole("region", { name: "Set up your ocean" });
    expect(within(setup).getByText("0 of 3")).toBeInTheDocument();
    expect(within(setup).getByRole("link", { name: "Connect" })).toBeInTheDocument();
    cleanup();

    serve({
      continue: { items: [], generated_at: GENERATED },
      whatsNew: { items: [], window_days: 7, generated_at: GENERATED },
      attention: attentionBody({ has_content: false, has_source: true }),
    });
    render(<HomePage />);
    const connected = await screen.findByRole("region", { name: "Set up your ocean" });
    await within(connected).findByText("1 of 3");
    expect(within(connected).getByRole("link", { name: "Add another" })).toBeInTheDocument();
    expect(within(connected).getByText("Add your ocean to Claude, Cursor or Codex").closest("li")).toHaveAttribute("aria-current", "step");
  });

  it("takes step 2 from has_mcp, or from copying the MCP URL here", async () => {
    serve({
      continue: { items: [], generated_at: GENERATED },
      whatsNew: { items: [], window_days: 7, generated_at: GENERATED },
      attention: attentionBody({ has_content: false, has_source: false, has_mcp: true }),
    });
    render(<HomePage />);
    await within(await screen.findByRole("region", { name: "Set up your ocean" })).findByText("1 of 3");
    cleanup();

    const writeText = vi.fn().mockResolvedValue(undefined);
    Object.defineProperty(navigator, "clipboard", { value: { writeText }, configurable: true });
    serve({
      continue: { items: [], generated_at: GENERATED },
      whatsNew: { items: [], window_days: 7, generated_at: GENERATED },
      attention: attentionBody({ has_content: false, has_source: false, has_mcp: false }),
    });
    render(<HomePage />);
    const setup = await screen.findByRole("region", { name: "Set up your ocean" });
    await within(setup).findByText("0 of 3");
    fireEvent.click(within(setup).getByRole("button", { name: "Copy the MCP URL https://api.coralswarm.example/mcp" }));
    await within(setup).findByText("1 of 3");
    expect(writeText).toHaveBeenCalledWith("https://api.coralswarm.example/mcp");
    expect(JSON.parse(localStorage.getItem("cs.home.setup.user_1") ?? "{}")).toMatchObject({ mcp: true });
  });

  it("never shows the checklist again once this identity has had content", async () => {
    localStorage.setItem("cs.home.onboarded.scope-1", "2026-11-01T00:00:00Z");
    serve({ continue: continueBody(), whatsNew: whatsNewBody(), attention: attentionBody({ has_content: false }) });
    render(<HomePage />);
    expect(await screen.findByRole("region", { name: "Continue" })).toBeInTheDocument();
    expect(screen.queryByRole("region", { name: "Set up your ocean" })).toBeNull();
    expect(checklist.renders).toBe(0);
  });

  it("keeps State A for the rest of the visit once content was seen, even if a later answer reads empty", async () => {
    serve({ continue: continueBody(), whatsNew: whatsNewBody(), attention: attentionBody() });
    render(<HomePage />);
    await screen.findByRole("link", { name: /Why is the read model/ });
    await waitFor(() => expect(localStorage.getItem("cs.home.onboarded.scope-1")).not.toBeNull());
    serve({ ...EMPTY_READS, attention: attentionBody({ has_content: false, has_source: false }) });
    fireEvent.click(screen.getByRole("button", { name: "Refresh home" }));
    await waitFor(() => expect(homeGets()).toHaveLength(6));
    await screen.findByText(/No reefs formed in your oceans/);
    expect(screen.getByRole("region", { name: "Continue" })).toBeInTheDocument();
    expect(screen.queryByRole("region", { name: "Set up your ocean" })).toBeNull();
    expect(checklist.renders).toBe(0);
  });

  it("does not flash the checklist before the onboarded mark has loaded", async () => {
    // A return inside the 5-minute window: the empty answer is cached and
    // there on the first render, before the browser memory is read.
    localStorage.setItem("cs.home.onboarded.scope-1", "2026-11-01T00:00:00Z");
    const client = makeQueryClient();
    client.setQueryData(["homeAttention", "scope-1"], attentionBody({ has_content: false }));
    serve({ continue: continueBody(), whatsNew: whatsNewBody(), attention: attentionBody({ has_content: false }) });
    rtlRender(<QueryClientProvider client={client}><HomePage /></QueryClientProvider>);
    expect(await screen.findByRole("region", { name: "Continue" })).toBeInTheDocument();
    expect(checklist.renders).toBe(0);
  });
});

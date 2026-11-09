// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import { act, waitFor, cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { DetailPanel, handlePanelKey } from "./DetailPanel";
import { answerBlockPreview, askAboutItemHref, fromActivity, fromActivityDetail, fromSourceResult, type DetailItem } from "./detailModel";
import type { Activity, ActivityDetail } from "@/lib/journalActivitiesApi";
import type { SourceResult } from "@/lib/scopedChatApi";

const motionPreference = vi.hoisted(() => ({ reduced: false }));
vi.mock("motion/react", async (original) => ({ ...await original<typeof import("motion/react")>(), useReducedMotion: () => motionPreference.reduced }));
const push = vi.fn();
vi.mock("next/navigation", () => ({ useRouter: () => ({ push }) }));
vi.mock("next/link", () => ({ default: ({ children, href }: { children: React.ReactNode; href: string }) => <a href={href}>{children}</a> }));

afterEach(() => {
  cleanup();
  push.mockReset();
});

const item: DetailItem = {
  id: "k1",
  kind: "meeting",
  kindLabel: "meeting",
  meta: "thu 15 oct · 42 min",
  graphId: "g1",
  oceanName: "Platform",
  oceanHue: "teal",
  title: "Acme pricing review",
  outcome: "Three usage tiers.",
  decided: ["Ship tier B"],
  people: ["Tom Becker", "Sara Kim"],
  peopleCount: 3,
  sources: [{ quote: "We settled on three tiers", meta: "MEETING · 14:52", kind: "meeting" }],
  reefs: [{ nodeId: "n1", title: "Pricing" }],
};

describe("keyboard", () => {
  const key = (k: string, target: EventTarget | null = document.body) =>
    ({ key: k, target, defaultPrevented: false, metaKey: false, ctrlKey: false, altKey: false }) as const;

  it("Esc closes", () => {
    const onClose = vi.fn();
    expect(handlePanelKey(key("Escape"), { onClose })).toBe(true);
    expect(onClose).toHaveBeenCalled();
  });
  it("arrows step only when the caller supplied stepping", () => {
    const onClose = vi.fn();
    const onPrev = vi.fn();
    const onNext = vi.fn();
    expect(handlePanelKey(key("ArrowDown"), { onClose })).toBe(false);
    expect(handlePanelKey(key("ArrowUp"), { onClose, onPrev, onNext })).toBe(true);
    expect(handlePanelKey(key("ArrowLeft"), { onClose, onPrev, onNext })).toBe(true);
    expect(handlePanelKey(key("ArrowDown"), { onClose, onPrev, onNext })).toBe(true);
    expect(handlePanelKey(key("ArrowRight"), { onClose, onPrev, onNext })).toBe(true);
    expect(onPrev).toHaveBeenCalledTimes(2);
    expect(onNext).toHaveBeenCalledTimes(2);
  });
  it("does not steal arrows while typing, but Esc still closes", () => {
    const input = document.createElement("input");
    const onClose = vi.fn();
    const onNext = vi.fn();
    expect(handlePanelKey(key("ArrowDown", input), { onClose, onNext })).toBe(false);
    expect(onNext).not.toHaveBeenCalled();
    expect(handlePanelKey(key("Escape", input), { onClose })).toBe(true);
  });
  it("ignores modified keys", () => {
    expect(handlePanelKey({ ...key("Escape"), metaKey: true }, { onClose: vi.fn() })).toBe(false);
  });
});

describe("panel", () => {
  it("renders every section and Esc / the arrow keys drive the handlers", async () => {
    const onClose = vi.fn();
    const onPrev = vi.fn();
    const onNext = vi.fn();
    render(<DetailPanel item={item} onClose={onClose} onPrev={onPrev} onNext={onNext} position={{ index: 1, total: 6 }} />);
    expect(screen.getByRole("heading", { name: "Acme pricing review" })).toBeInTheDocument();
    expect(screen.getByText("Ship tier B")).toBeInTheDocument();
    expect(screen.getByText("Tom Becker")).toBeInTheDocument();
    expect(screen.getByText("+1 more")).toBeInTheDocument();
    expect(screen.getByText("We settled on three tiers")).toBeInTheDocument();
    expect(screen.getByRole("link", { name: "Pricing" })).toHaveAttribute("href", "/topics?id=n1&graph_id=g1");
    expect(screen.getByText(/2 of 6/)).toBeInTheDocument();

    fireEvent.keyDown(window, { key: "ArrowDown" });
    fireEvent.keyDown(window, { key: "ArrowUp" });
    fireEvent.keyDown(window, { key: "Escape" });
    expect(onNext).toHaveBeenCalledTimes(1);
    expect(onPrev).toHaveBeenCalledTimes(1);
    await waitFor(() => expect(onClose).toHaveBeenCalledTimes(1));
  });

  it("without stepping props there are no step buttons and arrows do nothing", () => {
    render(<DetailPanel item={item} onClose={vi.fn()} />);
    expect(screen.queryByRole("button", { name: "Next" })).toBeNull();
    fireEvent.keyDown(window, { key: "ArrowDown" });
  });

  it("Ask about this opens a chat in the item's ocean, or calls onAsk", () => {
    const { rerender } = render(<DetailPanel item={item} onClose={vi.fn()} />);
    fireEvent.click(screen.getByRole("button", { name: "Ask about this" }));
    expect(push).toHaveBeenCalledWith(askAboutItemHref(item));
    expect(push.mock.calls[0][0]).toContain("graph_id=g1");
    const onAsk = vi.fn();
    rerender(<DetailPanel item={item} onClose={vi.fn()} onAsk={onAsk} />);
    fireEvent.click(screen.getByRole("button", { name: "Ask about this" }));
    expect(onAsk).toHaveBeenCalledWith(item);
  });

  it("shows a loading line while sources are still on their way", () => {
    render(<DetailPanel item={{ ...item, sources: [] }} loading onClose={vi.fn()} />);
    expect(screen.getByRole("status")).toHaveTextContent(/loading sources/);
  });
});

describe("adapters", () => {
  const act: Activity = {
    key: "k",
    kind: "agent",
    kind_label: "agent session",
    owner: { user_id: null, name: null },
    start: "2026-10-29T14:00:00Z",
    end: "2026-10-29T15:30:00Z",
    graph: { id: "g", name: "Private" },
    title: "Refactor router",
    outcome: "Done",
    people: ["A"],
    people_count: 1,
    topics: [{ node_id: "t", title: "Router" }],
    decision_count: 1,
    source_count: 2,
  };
  it("the list row paints at once, marked partial", () => {
    const d = fromActivity(act, { oceanKind: "member", tz: "UTC" });
    expect(d).toMatchObject({ kind: "agent", meta: "thu 29 oct · 14:00–15:30", oceanHue: "pink", partial: true, sources: [] });
    expect(d.reefs).toEqual([{ nodeId: "t", title: "Router" }]);
  });
  it("the detail read adds DECIDED and CAME FROM", () => {
    const full: ActivityDetail = {
      ...act,
      decisions: ["Use axum"],
      sources: [{ kind: "agent session", quote: "q", meta_label: "AGENT SESSION · 14:02", source_item_id: null, atom_id: null, occurred_at: null }],
      reefs: [{ node_id: "t2", title: "Axum" }],
    };
    const d = fromActivityDetail(full, { tz: "UTC" });
    expect(d.partial).toBe(false);
    expect(d.decided).toEqual(["Use axum"]);
    expect(d.sources[0]).toMatchObject({ quote: "q", meta: "AGENT SESSION · 14:02", kind: "agent" });
    expect(d.reefs).toEqual([{ nodeId: "t2", title: "Axum" }]);
  });
  it("a find tile maps decisions, reefs, duration and quotes", () => {
    const r: SourceResult = {
      source_item_id: "s",
      kind: "meeting",
      kind_label: "meeting",
      title: "Acme pricing review",
      outcome: null,
      occurred_at: "2026-10-15T15:00:00Z",
      duration_ms: 42 * 60_000,
      people: ["Tom"],
      people_count: 2,
      graph: { id: "g", name: "Platform" },
      best: false,
      decisions: ["d"],
      reefs: [{ node_id: "n", title: "Pricing" }],
      quotes: [{ quote: "hello", passage_id: null, occurred_at: "2026-10-15T15:10:00Z" }],
    };
    const d = fromSourceResult(r, { tz: "UTC" });
    expect(d.meta).toBe("thu 15 oct · 42 min");
    expect(d.decided).toEqual(["d"]);
    expect(d.sources[0].meta).toBe("MEETING · 15:10");
    expect(d.peopleCount).toBe(2);
  });
});

it("renders citation verdict and used blocks without inventing people or reefs", () => {
  render(<DetailPanel item={{...item,people:[],peopleCount:0,reefs:[],verdict:"verified",usedFor:["Launch is Friday [1]."]}} onClose={vi.fn()}/>);
  expect(screen.getByText("Jev: verified")).toBeInTheDocument();
  expect(screen.getByRole("region",{name:"Used in this answer for"})).toHaveTextContent("Launch is Friday [1].");
  expect(screen.queryByRole("region",{name:"People"})).toBeNull();
  expect(screen.queryByRole("region",{name:"Reefs touched"})).toBeNull();
});

it("cancels an outside dismissal when another activity replaces the item", async () => {
  vi.useFakeTimers();
  const close = vi.fn();
  const { rerender } = render(<DetailPanel item={item} onClose={close}/>);
  fireEvent.mouseDown(document.body);
  rerender(<DetailPanel item={{...item, id:"replacement", title:"Replacement"}} onClose={close}/>);
  await act(async () => { vi.advanceTimersByTime(500); });
  expect(close).not.toHaveBeenCalled();
  expect(screen.getByRole("heading", {name:"Replacement"})).toBeVisible();
  vi.useRealTimers();
});
it("returns focus on normal unmount but preserves Ask action focus", () => {
  const opener = document.createElement("button");
  const composer = document.createElement("textarea");
  document.body.append(opener, composer);
  opener.focus();
  const first = render(<DetailPanel item={item} onClose={vi.fn()}/>);
  first.unmount();
  expect(opener).toHaveFocus();
  const second = render(<DetailPanel item={item} onClose={vi.fn()} onAsk={() => composer.focus()}/>);
  fireEvent.click(screen.getByRole("button", {name:"Ask about this"}));
  second.unmount();
  expect(composer).toHaveFocus();
  opener.remove(); composer.remove();
});
it("strips answer markup and limits previews to 120 characters", () => {
  expect(answerBlockPreview("## **Hello** [world](https://example.com) [1]")).toBe("Hello world");
  expect(answerBlockPreview("a".repeat(200))).toHaveLength(120);
});
it("switches to a trapped mobile sheet at 759px and releases body scrolling", () => {
  let matches = true;
  let update = () => {};
  vi.stubGlobal("matchMedia", vi.fn((query: string) => ({
    get matches() { return query === "(max-width: 759px)" && matches; },
    addEventListener: (_: string, cb: () => void) => { update = cb; },
    removeEventListener: vi.fn(),
  })));
  const { unmount } = render(<DetailPanel item={item} onClose={vi.fn()}/>);
  expect(screen.getByRole("dialog")).toHaveAttribute("aria-modal", "true");
  const close = screen.getByRole("button", {name:"Close"});
  const ask = screen.getByRole("button", {name:"Ask about this"});
  ask.focus();
  fireEvent.keyDown(document, {key:"Tab"});
  expect(close).toHaveFocus();
  fireEvent.keyDown(document, {key:"Tab", shiftKey:true});
  expect(ask).toHaveFocus();
  act(() => { matches = false; update(); });
  expect(screen.getByRole("dialog")).not.toHaveAttribute("aria-modal");
  expect(document.body.style.overflow).toBe("");
  unmount(); vi.unstubAllGlobals();
});

it("dismisses without the exit delay under reduced motion", async () => {
  vi.useFakeTimers();
  motionPreference.reduced = true;
  const close = vi.fn();
  render(<DetailPanel item={item} onClose={close}/>);
  fireEvent.click(screen.getByRole("button", {name:"Close"}));
  await act(async () => { vi.advanceTimersByTime(0); });
  expect(close).toHaveBeenCalledOnce();
  motionPreference.reduced = false;
  vi.useRealTimers();
});

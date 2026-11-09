// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import { waitFor, cleanup, fireEvent, render, screen, within } from "@testing-library/react";
import { useState } from "react";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { SourceResult } from "@/lib/scopedChatApi";
import { OPEN_COMMAND_PALETTE_EVENT } from "@/lib/commandPalette";
import { DetailPanel } from "../detail/DetailPanel";
import { fromSourceResult } from "../detail/detailModel";
import { ThreadTurn } from "./AnswerTurn";
import { emptyAssistant, type AssistantTurn } from "./types";

vi.mock("next/navigation", () => ({ useRouter: () => ({ push: vi.fn() }) }));
vi.mock("next/link", () => ({ default: ({ children, href }: { children: React.ReactNode; href: string }) => <a href={href}>{children}</a> }));

const actions = { onCite: vi.fn(), onRegenerate: vi.fn(), onAsk: vi.fn() };
afterEach(() => {
  cleanup();
  vi.clearAllMocks();
});

function tile(n: number, extra: Partial<SourceResult> = {}): SourceResult {
  return {
    source_item_id: `item-${n}`,
    kind: "meeting",
    kind_label: "meeting",
    title: `Acme pricing review ${n}`,
    outcome: "Acme pushed back on a hard usage cap. Three tiers proposed.",
    occurred_at: "2026-10-15T12:00:00Z",
    duration_ms: 42 * 60_000,
    people: ["Tom Becker", "Sara Kim"],
    people_count: 2,
    graph: { id: "g1", name: "Go-to-market" },
    best: n === 1,
    decisions: ["Propose three tiers to Acme"],
    reefs: [{ node_id: "n1", title: "Acme renewal" }],
    quotes: [{ quote: "Can we see this as three tiers?", passage_id: null, occurred_at: "2026-10-15T15:18:00Z" }],
    ...extra,
  };
}

function turn(extra: Partial<AssistantTurn> = {}): AssistantTurn {
  return {
    ...emptyAssistant("a", "Find the Acme pricing meeting"),
    pending: false,
    mode: "synthesized",
    supportStatus: "complete",
    text: "It is the Acme pricing review.",
    ...extra,
  };
}

/** Stands in for the chats page, which owns the open-tile state. */
function Harness({ t }: { t: AssistantTurn }) {
  const [openId, setOpenId] = useState<string | null>(null);
  const opened=t.sourceResults?.find(r=>r.source_item_id===openId);
  return <><ThreadTurn turn={t} {...actions} find={{ openId, onOpenChange: setOpenId }} />{opened && <DetailPanel item={fromSourceResult(opened)} onClose={()=>setOpenId(null)}/>}</>;
}

describe("find result tiles", () => {
  it("renders 1 to 3 tiles in a 3-column grid and badges only the best match", () => {
    for (const count of [1, 2, 3]) {
      const { unmount } = render(<Harness t={turn({ sourceResults: Array.from({ length: count }, (_, i) => tile(i + 1)) })} />);
      const region = screen.getByRole("region", { name: "Matching sources" });
      expect(within(region).getAllByRole("button", { name: /Acme pricing review/ })).toHaveLength(count);
      expect(within(region).getAllByText("best match")).toHaveLength(1);
      expect(region.querySelector(".grid-cols-3")).not.toBeNull();
      unmount();
    }
  });

  it("shows kind, title, outcome clamped to two lines, the mono date and duration, people and ocean", () => {
    render(<Harness t={turn({ sourceResults: [tile(1)] })} />);
    const t = screen.getByRole("button", { name: /Acme pricing review 1/ });
    expect(within(t).getByText("meeting")).toBeInTheDocument();
    expect(within(t).getByText(/Three tiers proposed/)).toHaveClass("line-clamp-2");
    expect(within(t).getByText("thu 15 oct · 42 min")).toHaveClass("font-mono");
    expect(within(t).getByText("Go-to-market")).toBeInTheDocument();
    expect(t).toHaveClass("border-ink-4"); // best match border
  });

  it("degrades: no outcome, no duration, no people", () => {
    render(<Harness t={turn({ sourceResults: [tile(2, { outcome: null, duration_ms: null, people: [], people_count: 0, kind: "slack", kind_label: "slack thread" })] })} />);
    const t = screen.getByRole("button", { name: /Acme pricing review 2/ });
    expect(within(t).queryByText(/Three tiers/)).toBeNull();
    expect(within(t).getByText("thu 15 oct")).toBeInTheDocument();
    expect(within(t).queryByText(/min/)).toBeNull();
    expect(t.querySelectorAll("span.rounded-pill.font-semibold")).toHaveLength(0); // no avatars
    expect(within(t).queryByText("best match")).toBeNull();
    expect(within(t).getByText("slack thread")).toBeInTheDocument();
  });

  it("opens the DetailPanel from the tile data, marks the tile open, and Esc closes it", async () => {
    render(<Harness t={turn({ sourceResults: [tile(1), tile(2)] })} />);
    const first = screen.getByRole("button", { name: /Acme pricing review 1/ });
    fireEvent.click(first);
    const panel = screen.getByRole("dialog", { name: /Acme pricing review 1/ });
    expect(within(panel).getByText("Propose three tiers to Acme")).toBeInTheDocument();
    expect(within(panel).getByText("Acme renewal")).toBeInTheDocument();
    expect(first).toHaveClass("border-ink", "shadow-pop");
    fireEvent.keyDown(window, { key: "Escape" });
    await waitFor(() => expect(screen.queryByRole("dialog")).toBeNull());
    await waitFor(()=>expect(first).not.toHaveClass("shadow-pop"));
  });

  it("shows no tiles and keeps the evidence strip when the field is absent or empty", () => {
    for (const sourceResults of [undefined, [] as SourceResult[]]) {
      const { unmount } = render(<Harness t={turn({ sourceResults })} />);
      expect(screen.queryByRole("region", { name: "Matching sources" })).toBeNull();
      expect(screen.queryByText(/Not the one\?/)).toBeNull();
      unmount();
    }
  });

  it("removes an open Find detail when the current answer has authoritative empty cards", () => {
    const { rerender } = render(<Harness t={turn({ sourceResults: [tile(1)] })} />);
    fireEvent.click(screen.getByRole("button", { name: /Acme pricing review 1/ }));
    expect(screen.getByRole("dialog", { name: /Acme pricing review 1/ })).toBeInTheDocument();
    rerender(<Harness t={turn({ sourceResults: [] })} />);
    expect(screen.queryByRole("region", { name: "Matching sources" })).toBeNull();
    expect(screen.queryByRole("dialog")).toBeNull();
    expect(screen.queryByText("Acme pricing review 1")).toBeNull();
  });

  it("shows no tiles while the answer is still pending", () => {
    render(<Harness t={turn({ pending: true, sourceResults: [tile(1)] })} />);
    expect(screen.queryByRole("region", { name: "Matching sources" })).toBeNull();
  });

  it("the fallback line opens the Cmd-K palette", () => {
    const opened = vi.fn();
    window.addEventListener(OPEN_COMMAND_PALETTE_EVENT, opened);
    render(<Harness t={turn({ sourceResults: [tile(1)] })} />);
    expect(screen.getByText(/Not the one\? Ask with a person or a date, or press/)).toBeInTheDocument();
    const key = screen.getByRole("button", { name: /Search titles/ });
    expect(key).toHaveClass("font-mono");
    expect(key.textContent).toBe("⌘K");
    fireEvent.click(key);
    expect(opened).toHaveBeenCalledTimes(1);
    window.removeEventListener(OPEN_COMMAND_PALETTE_EVENT, opened);
  });
});

// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ThreadTurn } from "./AnswerTurn";
import { CitationPanel } from "./CitationPanel";
vi.mock("next/navigation", () => ({ useRouter: () => ({ push: vi.fn() }) }));

import { emptyAssistant, applyCitation } from "./types";
import { SourceDetail } from "@/components/library/SourceDetail";

const first = "11111111-1111-4111-8111-111111111111";
const second = "22222222-2222-4222-8222-222222222222";
const graph = "33333333-3333-4333-8333-333333333333";
const noop = () => {};
const answer = {
  ...emptyAssistant("answer", "What did we decide?"),
  pending: false,
  text: "Use the trial plan [1]. Review it Friday [2].",
  citations: [first, second],
  citationDetails: [
    { atom_id: first, graph_id: graph, title: "Trial approved", content: "We agreed to the trial.", source_type: "transcript" },
    { atom_id: second, graph_id: "other-workspace", title: "Friday review", source_type: "email" },
  ],
};

afterEach(cleanup);

describe("evidence navigation", () => {
  it("keeps native citation URLs and shared card selection scoped to their answer", () => {
    const onCite = vi.fn();
    render(<ThreadTurn turn={answer} onCite={onCite} onRegenerate={noop} onAsk={noop} />);
    const inline = screen.getByRole("link", { name: "Open source 1" });
    expect(inline).toHaveAttribute("href", `/coral?id=${first}&graph_id=${graph}`);
    // Native links preserve keyboard focus, copy URL, and modifier-click behavior.
    inline.focus();
    expect(inline).toHaveFocus();
    fireEvent.click(screen.getByRole("button", { name: "Open source 1: Trial approved" }));
    expect(onCite).toHaveBeenCalledWith(1);
    expect(screen.getByRole("link", { name: "Open source 2" })).toHaveAttribute("href", `/coral?id=${second}&graph_id=other-workspace`);
  });

  it("keeps each answer's ordinal local to that answer", () => {
    render(<>
      <ThreadTurn turn={answer} onCite={noop} onRegenerate={noop} onAsk={noop} />
      <ThreadTurn turn={{ ...answer, key: "next", text: "Different evidence [1].", citationDetails: [answer.citationDetails[1]], citations: [second] }} onCite={noop} onRegenerate={noop} onAsk={noop} />
    </>);
    expect(screen.getAllByRole("link", { name: "Open source 1" }).map((a) => a.getAttribute("href"))).toEqual([
      `/coral?id=${first}&graph_id=${graph}`, `/coral?id=${second}&graph_id=other-workspace`,
    ]);
  });

  it("opens the same evidence from drawer and collapsed rail", () => {
    render(<><CitationPanel turn={answer} n={1} /><ThreadTurn turn={answer} onCite={noop} onRegenerate={noop} onAsk={noop} /></>);
    expect(screen.getByRole("link", { name: "View captured source" })).toHaveAttribute("href", `/coral?id=${first}&graph_id=${graph}`);
    expect(screen.getByRole("link", { name: "Open source 2" })).toHaveAttribute("href", `/coral?id=${second}&graph_id=other-workspace`);
  });

  it("does not turn a missing streamed slot into the next citation", () => {
    const streamed = applyCitation({ ...emptyAssistant("stream", ""), text: "First [1], second [2]." }, { n: 2, ...answer.citationDetails[1] });
    render(<ThreadTurn turn={streamed} onCite={noop} onRegenerate={noop} onAsk={noop} />);
    expect(screen.queryByRole("link", { name: "Open source 1" })).not.toBeInTheDocument();
    expect(screen.getByRole("link", { name: "Open source 2" })).toHaveAttribute("href", `/coral?id=${second}&graph_id=other-workspace`);
  });

  it("opens retrieval-only and legacy evidence without inventing scope", () => {
    const retrieved = { ...emptyAssistant("retrieval", ""), pending: false, mode: "retrieval_only" as const, sources: [{ atom_id: first, ocean_id: graph, title: "Retrieved passage", content: "Trial", source_type: "transcript", timestamp: null }] };
    const { rerender } = render(<><ThreadTurn turn={retrieved} onCite={noop} onRegenerate={noop} onAsk={noop} /><CitationPanel turn={retrieved} n={1} /></>);
    expect(screen.getByRole("link", { name: "View captured source" })).toHaveAttribute("href", `/coral?id=${first}&graph_id=${graph}`);
    rerender(<ThreadTurn turn={{ ...emptyAssistant("legacy", ""), pending: false, text: "Legacy [1].", citations: [first] }} onCite={noop} onRegenerate={noop} onAsk={noop} />);
    expect(screen.getByRole("link", { name: "Open source 1" })).toHaveAttribute("href", `/coral?id=${first}`);
  });

  it("keeps contradiction citations scoped, focusable, and drawer-aware without intercepting modified navigation", () => {
    const onCite = vi.fn<(n: number) => void | boolean>();
    const prevented: boolean[] = [];
    const turn = { ...answer, text: "Conflicting rollout dates.", contradictions: [{ topic: "Rollout", positions: [{ ocean_id: "other-workspace", ocean_name: "Delivery", claim: "Review the plan Friday.", citation_index: 2 }] }] };
    // Record the control's handling, then cancel at the test boundary so jsdom
    // does not attempt real page navigation for modifier/fallback clicks.
    render(<div onClick={(event) => { prevented.push(event.defaultPrevented); event.preventDefault(); }}><ThreadTurn turn={turn} onCite={onCite} onRegenerate={noop} onAsk={noop} /></div>);
    const cite = screen.getByRole("link", { name: "Open source 2" });
    expect(cite).toHaveAttribute("href", `/coral?id=${second}&graph_id=other-workspace`);
    cite.focus();
    expect(cite).toHaveFocus();
    expect(cite.tabIndex).toBe(0);
    fireEvent.click(cite);
    fireEvent.click(cite, { detail: 0 }); // Native keyboard activation emits a detail-zero click.
    expect(onCite.mock.calls).toEqual([[2], [2]]);
    expect(prevented).toEqual([true, true]);
    for (const modifier of ["ctrlKey", "metaKey", "shiftKey", "altKey"]) fireEvent.click(cite, { [modifier]: true });
    expect(onCite).toHaveBeenCalledTimes(2);
    expect(prevented.slice(2)).toEqual([false, false, false, false]);
    onCite.mockReturnValue(false);
    fireEvent.click(cite);
    expect(prevented.at(-1)).toBe(false);
    expect(onCite).toHaveBeenLastCalledWith(2);
  });

  it("keeps contradiction callback fallback keyboard-operable before a source URL is supplied", () => {
    const onCite = vi.fn();
    const turn = { ...emptyAssistant("pending-contradiction", ""), pending: false, contradictions: [{ topic: "Rollout", positions: [{ ocean_id: "other-workspace", ocean_name: "Delivery", claim: "Review Friday.", citation_index: 2 }] }] };
    render(<ThreadTurn turn={turn} onCite={onCite} onRegenerate={noop} onAsk={noop} />);
    const cite = screen.getByRole("button", { name: "2" });
    cite.focus();
    expect(cite).toHaveFocus();
    fireEvent.keyDown(cite, { key: "Enter" });
    expect(fireEvent.keyDown(cite, { key: " " })).toBe(false);
    expect(onCite.mock.calls).toEqual([[2], [2]]);
  });

  it("opens source-group passages by atom ID rather than provider source ID", () => {
    render(<QueryClientProvider client={new QueryClient()}><SourceDetail getAuthToken={() => "token"} graphId={graph} canPromote={false} onPromote={noop} detail={{
      channel_key: "meeting:trial", channel_label: "Trial meeting", source_type: "transcript", coral_count: 1, reef_count: 0, instance_count: 1, first_ts: null, last_ts: null,
      source_ids: ["meeting:trial:chunk:0"], reefs: [], corals: [{ atom_id: first, source_id: "meeting:trial:chunk:0", title: "Trial approved", timestamp: null }], page: 1, page_size: 20, has_more: false,
    }} /></QueryClientProvider>);
    expect(screen.getByRole("link", { name: /Trial approved/ })).toHaveAttribute("href", `/coral?id=${first}&graph_id=${graph}`);
  });
});

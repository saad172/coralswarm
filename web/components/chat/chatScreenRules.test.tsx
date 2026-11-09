// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import { cleanup, fireEvent, render, screen, within } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { LibraryCitation } from "@/lib/libraryMemoryApi";
import type { LegacyEvidence } from "@/lib/scopedChatApi";
import type { ConversationSummary } from "@/lib/api";
import { HUES, workspaceHue } from "@/components/ui";
import { ThreadTurn } from "./AnswerTurn";
import { ChatComposer } from "./ChatComposer";
import { ChatHeader } from "./ChatHeader";

import { emptyAssistant, type AssistantTurn } from "./types";

const deps = vi.hoisted(() => ({ remove: vi.fn() }));
vi.mock("@/lib/useEvidenceAdmission", () => ({ useEvidenceAdmission: () => ({ key: "user-a", ready: true, getToken: () => "token", graphs: [], currentGraphId: null }) }));
vi.mock("@/lib/scopedChatApi", async (actual) => ({ ...(await actual<typeof import("@/lib/scopedChatApi")>()), deleteScopedConversation: (...args: unknown[]) => deps.remove(...args) }));
afterEach(() => { cleanup(); vi.clearAllMocks(); });

const graph = "11111111-1111-4111-8111-111111111111";
const itemA = "22222222-2222-4222-8222-222222222222";
const itemB = "33333333-3333-4333-8333-333333333333";
const version = "44444444-4444-4444-8444-444444444444";
let atomSeq = 0;
function passage(item: string, title: string): LibraryCitation {
  const atom = `55555555-5555-4555-8555-${String(++atomSeq).padStart(12, "0")}`;
  return { kind: "source_passage", graph_id: graph, source_item_id: item, source_version_id: version, passage_id: atom, semantic_atom_id: atom,
    start_char: 0, end_char: 5, quote: `Quote ${atomSeq}`, quote_truncated: false, representation: "exact_passage", occurred_at: null,
    captured_at: "2026-10-01T00:00:00Z", title, jev_verdict: "verified" };
}
const legacy: LegacyEvidence = { kind: "legacy_chunk", graph_id: graph, atom_id: "66666666-6666-4666-8666-666666666666", representation: "unmapped_legacy_chunk",
  content: "Old chunk", content_truncated: false, occurred_at: null, captured_at: null };
/** Three passages from two source items, plus one raw chunk: three sources. */
function scopedTurn(extra: Partial<AssistantTurn> = {}): AssistantTurn {
  return { ...emptyAssistant("a", "Q"), pending: false, mode: "synthesized", supportStatus: "complete", text: "A [1] B [2] C [3] D [4].",
    retainedCitations: [passage(itemA, "Roadmap doc"), passage(itemA, "Roadmap doc"), passage(itemB, "Standup notes")], legacyEvidence: [legacy], scopedOutcomes: [], ...extra };
}

const actions = { onCite: vi.fn(), onRegenerate: vi.fn(), onAsk: vi.fn() };

describe("answer stats line", () => {
  it("counts distinct sources, not passages, and says nothing else", () => {
    render(<ThreadTurn turn={scopedTurn()} {...actions} />);
    expect(screen.getByText("answered from 3 sources")).toBeInTheDocument();
    expect(screen.queryByText(/passages|raw chunks|source items|checked by Jev/)).not.toBeInTheDocument();
  });
  it("offers no broaden action on an empty scoped answer", () => {
    render(<ThreadTurn turn={scopedTurn({ retainedCitations: [], legacyEvidence: [], text: "", empty: true, libraryScope: { graph_ids: [graph], source_item_id: itemA } })} {...actions} />);
    expect(screen.getByText("No matching readable evidence was returned in this scope.")).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: /Broaden|Ask across/ })).not.toBeInTheDocument();
  });
});

describe("workspace colours", () => {
  const ids = ["aaaaaaaa-0000-4000-8000-000000000001", "bbbbbbbb-0000-4000-8000-000000000002", "cccccccc-0000-4000-8000-000000000003"];
  it("is a pure function of the workspace id", () => {
    for (const id of ids) {
      expect(HUES).toContain(workspaceHue(id));
      expect(workspaceHue(id)).toBe(workspaceHue(id));
    }
  });
});

describe("chat header", () => {
  const conv = { id: "c1", graph_id: graph, title: "Roadmap", mine: true, pinned: false, shared: false, created_at: "2026-10-01", updated_at: "2026-10-01",
    oceans: [{ ocean_id: graph, ocean_name: "Platform" }] } as ConversationSummary;
  it("shows only the title and actions: no save, share, or workspace chips", () => {
    render(<ChatHeader title="Roadmap" conv={conv} sourceCount={3} onOpenEvidence={() => {}} />);
    expect(screen.getByText("Roadmap")).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: /Evidence/ })).not.toBeInTheDocument();
    expect(screen.queryByRole("button", { name: /Save|Share/ })).not.toBeInTheDocument();
    expect(screen.queryByText("Platform")).not.toBeInTheDocument();
    expect(screen.queryByRole("button", { name: /Delete chat/ })).not.toBeInTheDocument();
  });
  it("deletes from the … menu only after confirming", async () => {
    deps.remove.mockResolvedValue(undefined);
    render(<ChatHeader title="Roadmap" conv={conv} sourceCount={0} onOpenEvidence={() => {}} />);
    fireEvent.click(screen.getByRole("button", { name: "Chat actions" }));
    fireEvent.click(screen.getByRole("button", { name: "Delete chat" }));
    expect(deps.remove).not.toHaveBeenCalled();
    const dialog = screen.getByRole("dialog");
    expect(within(dialog).getByText("Delete this chat?")).toBeInTheDocument();
    fireEvent.click(within(dialog).getByRole("button", { name: "Delete" }));
    expect(deps.remove).toHaveBeenCalledWith("token", "c1", expect.anything());
  });
  it("has no … menu on a chat shared into this account", () => {
    render(<ChatHeader title="Roadmap" conv={{ ...conv, mine: false }} sourceCount={0} onOpenEvidence={() => {}} />);
    expect(screen.queryByRole("button", { name: "Chat actions" })).not.toBeInTheDocument();
  });
});

describe("composer", () => {
  it("has no workspace picker: only answer settings, then send", () => {
    render(<ChatComposer model="smart" setModel={() => {}} effort="medium" setEffort={() => {}}
      style="detailed" setStyle={() => {}} busy={false} onSend={() => {}} onStop={() => {}} scoped />);
    expect(screen.getAllByRole("button").map((b) => b.textContent?.replace("▾", "").trim() || b.getAttribute("aria-label"))).toEqual(["Answer settings", "Send"]);
    expect(screen.queryByText(/workspace/i)).not.toBeInTheDocument();
  });
  it("explains Sources only inside the style menu, with no footnote under the composer", () => {
    render(<ChatComposer model="smart" setModel={() => {}} effort="medium" setEffort={() => {}}
      style="detailed" setStyle={() => {}} busy={false} onSend={() => {}} onStop={() => {}} scoped />);
    expect(screen.queryByText(/does not generate an answer|without writing an answer/)).not.toBeInTheDocument();
    fireEvent.click(screen.getByRole("button", { name: "Answer settings" }));
    expect(screen.getByRole("tab", { name: "Sources only" })).toBeInTheDocument();
  });
});

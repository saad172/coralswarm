// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import { cleanup, fireEvent, render, screen, within } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { LibraryCitation } from "@/lib/libraryMemoryApi";
import type { LegacyEvidence, ScopedOutcome } from "@/lib/scopedChatApi";
import { ThreadTurn } from "./AnswerTurn";
import { CitationPanel } from "./CitationPanel";
vi.mock("next/navigation", () => ({ useRouter: () => ({ push: vi.fn() }) }));
import { citationVerdict, evidenceSources } from "./evidence";
import { emptyAssistant, type AssistantTurn } from "./types";

const graph = "11111111-1111-4111-8111-111111111111";
const item = "22222222-2222-4222-8222-222222222222";
const version = "33333333-3333-4333-8333-333333333333";
const atomCurrent = "44444444-4444-4444-8444-444444444444";
const atomOld = "55555555-5555-4555-8555-555555555555";
const legacyAtom = "66666666-6666-4666-8666-666666666666";
const actions = { onCite: vi.fn(), onRegenerate: vi.fn(), onAsk: vi.fn() };
afterEach(() => { cleanup(); vi.clearAllMocks(); });

function citation(atom: string, quote: string, extra: Partial<LibraryCitation> = {}): LibraryCitation {
  return { kind: "source_passage", graph_id: graph, source_item_id: item, source_version_id: version, passage_id: atom, semantic_atom_id: atom,
    start_char: 0, end_char: Array.from(quote).length, quote, quote_truncated: false, representation: "exact_passage", occurred_at: null,
    captured_at: "2026-10-01T00:00:00Z", title: `Passage ${quote.slice(0, 5)}`, ...extra };
}
function outcome(atom: string, statement: string, extra: Partial<ScopedOutcome> = {}): ScopedOutcome {
  return { graph_id: graph, semantic_atom_id: atom, kind: "fact", statement, statement_truncated: false, source_item_id: item, source_version_id: version,
    source_version_is_current: true, occurred_at: null, captured_at: "2026-10-01T00:00:00Z", representation: "exact_passage", confidence: null, synthetic_demo: false, citations: [], ...extra };
}
const legacy: LegacyEvidence = { kind: "legacy_chunk", graph_id: graph, atom_id: legacyAtom, representation: "unmapped_legacy_chunk", content: "Old chunk", content_truncated: false, occurred_at: null, captured_at: null, jev_verdict: "contradicted" };

function scopedTurn(extra: Partial<AssistantTurn> = {}): AssistantTurn {
  return {
    ...emptyAssistant("a", "Who owns Atlas?"), pending: false, mode: "synthesized", supportStatus: "complete",
    text: "Dana owns Atlas [1]. It shipped in Q3 [2]. Lee owned it before [3].",
    retainedCitations: [citation(atomCurrent, "Dana owns Atlas", { jev_verdict: "verified" }), citation(atomOld, "Atlas shipped Q3", { jev_verdict: "unsupported" })],
    legacyEvidence: [legacy],
    scopedOutcomes: [
      outcome(atomOld, "Lee owns Atlas.", { supersession: { status: "superseded", superseded_by: [{ semantic_atom_id: atomCurrent, state_key: "owner", kind: "fact", rule: "functional" }] } }),
      outcome(atomCurrent, "Dana owns Atlas."),
    ],
    ...extra,
  };
}

describe("Jev citation verdicts in scoped answers", () => {
  it("maps verdicts onto [n] in the prose's numbering, retained passages then legacy chunks", () => {
    const turn = scopedTurn();
    expect([1, 2, 3, 4].map((n) => citationVerdict(turn, n))).toEqual(["verified", "unsupported", "contradicted", null]);
    expect(evidenceSources(turn).map((s) => s.verdict)).toEqual(["verified", "unsupported", "contradicted"]);
  });

  it("speaks every verdict on its chip and flags only the undermining ones visibly", () => {
    render(<ThreadTurn turn={scopedTurn()} {...actions} />);
    const verified = screen.getByRole("link", { name: "Open source 1, Jev: verified" });
    expect(verified.textContent).toBe("1");
    const unsupported = screen.getByRole("link", { name: "Open source 2, Jev: not supported" });
    expect(unsupported.textContent).toBe("2?");
    expect(screen.getByRole("link", { name: "Open source 3, Jev: contradicted" }).textContent).toBe("3≠");
  });

  it("badges each source card with a word, not colour alone, and keeps Jev tallies off the meta line", () => {
    render(<ThreadTurn turn={scopedTurn()} {...actions} />);
    const list = screen.getByRole("region", { name: "Answer sources" });
    expect(within(list).getByText("Jev: verified")).toBeInTheDocument();
    expect(screen.getByRole("link", {name:/Open source 2, Jev: not supported/})).toBeInTheDocument();
    expect(within(list).getByText("Jev: contradicted")).toBeInTheDocument();

    expect(screen.queryByText(/checked by Jev/)).not.toBeInTheDocument();
  });

  it("explains the verdict in the evidence drawer detail", () => {
    render(<CitationPanel turn={scopedTurn()} n={2} />);
    expect(screen.getByText("Jev checked this source and it does not support the claim it is cited for.")).toBeInTheDocument();
  });

  it("renders older answers (no verdicts) exactly as before", () => {
    const turn = scopedTurn({ retainedCitations: [citation(atomCurrent, "Dana owns Atlas"), citation(atomOld, "Atlas shipped Q3")], legacyEvidence: [{ ...legacy, jev_verdict: undefined }] });
    render(<ThreadTurn turn={turn} {...actions} />);
    expect(screen.getByRole("link", { name: "Open source 1" }).textContent).toBe("1");
    expect(screen.queryByText(/Jev/)).not.toBeInTheDocument();
  });

  it("ignores an unknown verdict string rather than rejecting the answer", () => {
    const turn = scopedTurn({ retainedCitations: [citation(atomCurrent, "Dana owns Atlas", { jev_verdict: "maybe" })], legacyEvidence: [] });
    expect(citationVerdict(turn, 1)).toBeNull();
  });

});

describe("fact history in scoped answers", () => {
  it("lists current facts first and superseded ones as named history", () => {
    render(<ThreadTurn turn={scopedTurn()} {...actions} />);
    fireEvent.click(screen.getByText(/Inspect sourced outcomes · 2 · 1 superseded/));
    const history = screen.getByRole("region", { name: "Fact history" });
    expect(within(history).getByText("Lee owns Atlas.")).toBeInTheDocument();
    expect(within(history).getByText("Superseded by a later fact: “Dana owns Atlas.”")).toBeInTheDocument();
    expect(within(history).getByText("superseded")).toBeInTheDocument();
    expect(within(history).queryByText("Dana owns Atlas.")).not.toBeInTheDocument();
  });

  it("has no history section when no outcome carries supersession", () => {
    render(<ThreadTurn turn={scopedTurn({ scopedOutcomes: [outcome(atomCurrent, "Dana owns Atlas.")] })} {...actions} />);
    fireEvent.click(screen.getByText("Inspect sourced outcomes · 1"));
    expect(screen.queryByRole("region", { name: "Fact history" })).not.toBeInTheDocument();
  });
});

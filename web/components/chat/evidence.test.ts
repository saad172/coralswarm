import { describe, expect, it, vi } from "vitest";
import { evidenceSources, groupBySource, sourceCount } from "./evidence";
import { fromCitation } from "../detail/detailModel";
import { scopedMessageTurn } from "./useChatThread";
import { emptyAssistant, type AssistantTurn } from "./types";
import type { LibraryCitation } from "@/lib/libraryMemoryApi";
import type { ScopedMessage } from "@/lib/scopedChatApi";

vi.mock("@/lib/useEvidenceAdmission", () => ({ useEvidenceAdmission: () => ({ key: "user-a", ready: true, getToken: () => "frozen-a", graphs: [], currentGraphId: null }) }));

const graph = "11111111-1111-4111-8111-111111111111";
const source = "22222222-2222-4222-8222-222222222222";
const version = "33333333-3333-4333-8333-333333333333";
const passage = "44444444-4444-4444-8444-444444444444";
const atom = "55555555-5555-4555-8555-555555555555";
const scope = { graph_ids: [graph], source_item_id: source, source_version_id: version, outcome_versions: "all_retained" as const };
const quote: LibraryCitation = { kind: "source_passage", graph_id: graph, source_item_id: source, source_version_id: version, passage_id: passage, semantic_atom_id: atom,
  start_char: 0, end_char: 12, quote: "Exact phrase", quote_truncated: false, representation: "exported_text", occurred_at: null, captured_at: "2026-10-11T12:00:00Z" };
const message: ScopedMessage = { id: atom, role: "assistant", text: "Saved private answer [1]", mode: "synthesized", support_status: "complete", library_scope: scope,
  retained_citations: [quote], outcomes: [], legacy_evidence: [], contains_synthetic_demo_evidence: false };

function turn(citation: LibraryCitation, graphNames?: Record<string, string>): AssistantTurn {
  return { ...emptyAssistant(atom, ""), pending: false, supportStatus: "complete", retainedCitations: [citation], legacyEvidence: [], graphNames };
}

describe("retained evidence cards", () => {
  it("labels a Slack passage by its source type, workspace name and hue", () => {
    const [card] = evidenceSources(turn({ ...quote, source_type: "slack", title: null }, { [graph]: "My Workspace" }));
    expect(card.title).toBe("Slack");
    expect(card.ocean).toBe("My Workspace");
    expect(card.hue).toBe("amber");
    expect(card.type).toBe("Exact saved passage");
  });
  it("prefers the retained source title over the source type", () => {
    const [card] = evidenceSources(turn({ ...quote, source_type: "github", title: "PR #1000 notice" }, { [graph]: "My Workspace" }));
    expect(card.title).toBe("PR #1000 notice");
  });
  it("keeps the numbered fallback for a message stored before citations carried provenance", () => {
    const [card] = evidenceSources(turn(quote));
    expect(card.title).toBe("Retained passage 1");
    expect(card.ocean).toBeNull();
    expect(card.hue).toBe("teal");
  });
  it("shows the workspace name in the shared provenance rows instead of Not supplied", () => {
    const [card] = evidenceSources(turn({ ...quote, source_type: "slack", title: null }, { [graph]: "My Workspace" }));
    expect(fromCitation(card, turn(quote)).oceanName).toBe("My Workspace");
  });
  it("carries the admitted workspace names onto a restored scoped turn", () => {
    const restored = scopedMessageTurn(message, scope, [{ id: graph, name: "My Workspace" }]) as AssistantTurn;
    expect(restored.graphNames?.[graph]).toBe("My Workspace");
  });
});

describe("sources versus passages", () => {
  const other = "77777777-7777-4777-8777-777777777777";
  const second: LibraryCitation = { ...quote, passage_id: other, semantic_atom_id: other, quote: "Another phrase" };
  const elsewhere: LibraryCitation = { ...quote, source_item_id: other, passage_id: atom, quote: "Different item" };
  const many = { ...turn(quote), retainedCitations: [quote, second, elsewhere] };
  it("counts each source item once however many of its passages are cited", () => {
    expect(evidenceSources(many)).toHaveLength(3);
    expect(sourceCount(evidenceSources(many))).toBe(2);
  });
  it("counts every unmapped raw chunk as its own source", () => {
    const chunk = { kind: "legacy_chunk" as const, graph_id: graph, atom_id: other, representation: "unmapped_legacy_chunk" as const, content: "x", content_truncated: false, occurred_at: null, captured_at: null };
    expect(sourceCount(evidenceSources({ ...many, legacyEvidence: [chunk, { ...chunk, atom_id: atom }] }))).toBe(4);
  });
  it("groups passages under their source in first-citation order, keeping citation numbers", () => {
    expect(groupBySource(evidenceSources(many)).map((g) => g.passages.map((p) => p.n))).toEqual([[1, 2], [3]]);
  });
});

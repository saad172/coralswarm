// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import { cleanup, fireEvent, screen, within } from "@testing-library/react";
import { render } from "@/lib/testing/queryRender";
import { afterEach, beforeEach, expect, it, vi } from "vitest";
import { TopicDisagreements } from "./TopicDisagreements";
import { ApiError } from "@/lib/api";
import { readLibraryDisagreements, type LibraryDisagreementPage, type LibraryDisagreementSide } from "@/lib/libraryHistoryApi";
vi.mock("@/lib/libraryHistoryApi", () => ({ readLibraryDisagreements: vi.fn() }));
const graph = "11111111-1111-4111-8111-111111111111", node = "22222222-2222-4222-8222-222222222222", item = "33333333-3333-4333-8333-333333333333", version = "44444444-4444-4444-8444-444444444444", other = "55555555-5555-4555-8555-555555555555", passage = "66666666-6666-4666-8666-666666666666";
const onPage = vi.fn(), onNode = vi.fn(), onEvidence = vi.fn();
const props = { identity: "account", getToken: () => "fixed", graphId: graph, nodeId: node, onPage, onNode, onEvidence };
function side(id: string, title: string | null, statement: string): LibraryDisagreementSide {
  return { node_id: id, type: "decision", title, evidence_truncated: false, evidence: [{ knowledge_time: null, effective_time: [], effective_time_truncated: false, outcome: { graph_id: graph, semantic_atom_id: id, source_item_id: item, source_version_id: version, kind: "decision", statement, origin: null, occurred_at: null, captured_at: "2026-02-01T00:00:00Z", nodes: [], nodes_truncated: false, citations_truncated: false,
    citations: [{ kind: "source_passage", graph_id: graph, source_item_id: item, source_version_id: version, semantic_atom_id: id, passage_id: passage, start_char: 0, end_char: 10, quote: statement, quote_truncated: false, representation: "text", occurred_at: null, captured_at: "2026-02-01T00:00:00Z" }] } }] };
}
function page(): LibraryDisagreementPage {
  const theirs = side(other, null, "Atlas slips to May.");
  theirs.evidence[0].knowledge_time = "2026-03-04T12:00:00Z";
  theirs.evidence[0].effective_time = [{ state_key: "release.month", operation: "assert", value: "may", valid_from: "2026-05-01T00:00:00Z", valid_until: null, recorded_at: "2026-03-05T00:00:00Z", passage_id: passage }];
  return { graph_id: graph, node_id: node, anchor: side(node, "Atlas", "Atlas ships in March."), items: [{ recorded_at: "2026-09-01T00:00:00Z", other: theirs }], withheld: { count: 1, complete: true }, next_cursor: "pair-next", truncated: true };
}
beforeEach(() => { vi.mocked(readLibraryDisagreements).mockResolvedValue(page()); });
afterEach(() => { cleanup(); vi.resetAllMocks(); });

it("shows both sides with their statements, evidence times and the withheld count only", async () => {
  render(<TopicDisagreements {...props} />);
  expect(await screen.findByRole("heading", { name: "This node: Atlas" })).toBeInTheDocument();
  expect(screen.getByRole("heading", { name: "Disagrees with: Atlas slips to May." })).toBeInTheDocument();
  expect(readLibraryDisagreements).toHaveBeenCalledWith("fixed", { graph_id: graph, node_id: node, page_size: 5 }, expect.any(AbortSignal));
  const [anchorTimes, otherTimes] = screen.getAllByText("Effective time").map((dt) => dt.closest("dl") as HTMLElement);
  expect(within(anchorTimes).getByText("Not recorded. Capture and source dates do not establish it.")).toBeInTheDocument();
  expect(within(anchorTimes).getByText("Not recorded for this retained version.")).toBeInTheDocument();
  expect(within(otherTimes).getByText(/^release\.month: may, from .*2026.*, end not recorded$/)).toBeInTheDocument();
  expect(within(otherTimes).getByText(/^First recorded by CoralSwarm .*2026/)).toBeInTheDocument();
  expect(screen.getByText(/^Disagreement recorded .*2026/)).toBeInTheDocument();
  expect(screen.getByText("1 unresolved disagreement involves a side you cannot read. Its statement and evidence are withheld.")).toBeInTheDocument();
});
it("names a withdrawal or correction instead of reading it as an ordinary interval", async () => {
  const value = page(); const base = value.items[0].other.evidence[0].effective_time[0];
  value.items[0].other.evidence[0].effective_time = [{ ...base, operation: "withdraw", value: null }, { ...base, operation: "correct", value: "june", recorded_at: "2026-03-06T00:00:00Z" }];
  vi.mocked(readLibraryDisagreements).mockResolvedValue(value);
  render(<TopicDisagreements {...props} />);
  expect(await screen.findByText(/^release\.month: withdrawn, from .*2026/)).toBeInTheDocument();
  expect(screen.getByText(/^release\.month \(correct\): june, from .*2026/)).toBeInTheDocument();
});
it("says at least when the withheld probe is incomplete", async () => {
  vi.mocked(readLibraryDisagreements).mockResolvedValue({ ...page(), withheld: { count: 200, complete: false } });
  render(<TopicDisagreements {...props} />);
  expect(await screen.findByText("At least 200 unresolved disagreements involve sides you cannot read. Their statements and evidence are withheld.")).toBeInTheDocument();
});
it("falls back to unavailable statement text instead of inventing one", async () => {
  const value = page(); value.items[0].other.evidence = [];
  vi.mocked(readLibraryDisagreements).mockResolvedValue(value);
  render(<TopicDisagreements {...props} />);
  expect(await screen.findByRole("heading", { name: "Disagrees with: Statement text unavailable" })).toBeInTheDocument();
  expect(screen.getByText("No retained sourced statement is readable for this side.")).toBeInTheDocument();
});
it("opens the exact evidence passage, the other node and the next page", async () => {
  render(<TopicDisagreements {...props} />);
  await screen.findByRole("heading", { name: "This node: Atlas" });
  fireEvent.click(screen.getAllByRole("button", { name: "Read surrounding captured text" })[1]);
  expect(onEvidence).toHaveBeenCalledWith(item, graph, version, other, passage);
  fireEvent.click(screen.getByRole("button", { name: "Atlas slips to May." }));
  expect(onNode).toHaveBeenCalledWith(other, graph);
  fireEvent.click(screen.getByRole("button", { name: "Next disagreements" }));
  expect(onPage).toHaveBeenCalledWith("pair-next");
});
it("invalidates the parent inspector when the node read is refused", async () => {
  const unavailable = vi.fn();
  vi.mocked(readLibraryDisagreements).mockRejectedValue(new ApiError("Node access revoked", 404));
  render(<TopicDisagreements {...props} onUnavailable={unavailable} />);
  expect(await screen.findByRole("alert")).toHaveTextContent("Node access revoked");
  expect(unavailable).toHaveBeenCalledWith("Node access revoked");
  expect(screen.queryByText("Atlas ships in March.")).not.toBeInTheDocument();
});

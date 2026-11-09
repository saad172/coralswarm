// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import { act, cleanup, fireEvent, screen } from "@testing-library/react";
import { render, returnToTab } from "@/lib/testing/queryRender";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { SourceEvidence } from "./SourceEvidence";
import { SourceInventory } from "./SourceInventory";
import { NodeMeetingAtoms } from "@/components/meetings/NodeMeetingAtoms";
import SourcePage from "@/app/(app)/source/page";
import { ApiError, type SourceEvidencePage, type SourceItem, type MeetingEvidenceVersion } from "@/lib/api";
import { readP6Evidence, retryP6Evidence, readP6NodeAtoms, readP6SourceItems } from "@/lib/p6EvidenceApi";

const navigation = vi.hoisted(() => ({ params: new URLSearchParams(), replace: vi.fn() }));
vi.mock("next/navigation", () => ({ useSearchParams: () => navigation.params, useRouter: () => ({ replace: navigation.replace }) }));
vi.mock("@/lib/providers", () => ({ useApp: () => ({ token: "token", currentGraph: { id: "ambient-unrelated" } }) }));
vi.mock("@/lib/useEvidenceAdmission", () => ({ useEvidenceAdmission: () => ({ key: "account:scope", ready: true, getToken: () => "token", currentGraphId: "ambient-unrelated", graphs: [], queryScope: "account:scope" }) }));
vi.mock("@/lib/p6EvidenceApi", async (actual) => ({ ...(await actual<typeof import("@/lib/p6EvidenceApi")>()), readP6Evidence: vi.fn(), retryP6Evidence: vi.fn(), readP6NodeAtoms: vi.fn(), readP6SourceItems: vi.fn(), readP6LegacyCapture: vi.fn() }));
const wire = vi.hoisted(() => ({ read: vi.fn() }));
vi.mock("@/lib/api", async (actual) => ({ ...(await actual<typeof import("@/lib/api")>()), apiRequest: vi.fn((path: string) => {
  if (!path.startsWith("/v1/load/captures?")) throw new Error(`Unexpected mount read ${path}`);
  return wire.read(path);
}) }));
const evidence = vi.mocked(readP6Evidence);
const items = vi.mocked(readP6SourceItems);
const auth = vi.fn().mockResolvedValue("token");
const identity = { source_item_id: "22222222-2222-4222-8222-222222222222", source_type: "email", provider_namespace: "mailbox:two", source_id: "native:+?" };
const passage = { passage_id: "44444444-4444-4444-8444-444444444444", start_char: 0, end_char: 11, quote: "Ship Friday", start_ms: null, end_ms: null, speaker: null };
function page(): SourceEvidencePage & { selected_version: MeetingEvidenceVersion } {
  return {
    graph_id: "11111111-1111-4111-8111-111111111111", source_item_id: identity.source_item_id, source: identity,
    next_cursor: null, versions: [{ version_id: "33333333-3333-4333-8333-333333333333", captured_at: "2026-10-10T12:00:00Z", is_current: false, is_legacy_baseline: false }],
    selected_version: { ...identity, version_id: "33333333-3333-4333-8333-333333333333", captured_at: "2026-10-10T12:00:00Z", is_legacy_baseline: false, representation: "generated_summary", source_metadata: { title: "Launch checkpoint" }, participants: ["Ada"], occurred_at: "2026-10-09T12:00:00Z", raw_text: "Ship Friday", text_status: "available", extraction_status: "done", extraction_error: null, can_retry: false, atoms_next_cursor: null, atoms: [{ atom_id: "55555555-5555-4555-8555-555555555555", kind: "decision", statement: "The launch is Friday.", passages: [passage], nodes: [{ node_id: "66666666-6666-4666-8666-666666666666", graph_id: "11111111-1111-4111-8111-111111111111", title: "Launch", type: "entity" }] }] },
  };
}
function capture(evidence: SourceEvidencePage | null = page()) {
  return { model: "capture_detail", version: 2, computed_at: "2026-10-31T00:00:00Z", stale: false, refresh_eta: null, payload: { summary: null, evidence } };
}
function item(id = "22222222-2222-4222-8222-222222222222"): SourceItem {
  return { ...identity, source_item_id: id, graph_id: "11111111-1111-4111-8111-111111111111", updated_at: "2026-10-11T12:00:00Z", current_version_id: "33333333-3333-4333-8333-333333333334", mapped_version_id: "33333333-3333-4333-8333-333333333333", captured_at: "2026-10-10T12:00:00Z", occurred_at: null, representation: "normalized_text", source_metadata: { title: `Email ${id}` }, extraction_status: "done" };
}
function deferred<T>() { let resolve!: (value: T) => void; const promise = new Promise<T>((r) => { resolve = r; }); return { resolve, promise }; }
beforeEach(() => { navigation.params = new URLSearchParams(); wire.read.mockResolvedValue(capture()); auth.mockResolvedValue("token"); });
afterEach(() => { cleanup(); vi.resetAllMocks(); vi.restoreAllMocks(); vi.useRealTimers(); });
function renderEvidence() { return <SourceEvidence token="token" graphId="11111111-1111-4111-8111-111111111111" sourceItemId="22222222-2222-4222-8222-222222222222" />; }

describe("source-neutral preserved evidence", () => {
  it.each(["Older versions", "More atoms"])("keeps %s pending instead of declaring evidence empty", async (button) => {
    const initial = page();
    if (button === "Older versions") initial.next_cursor = "next-version";
    else initial.selected_version.atoms_next_cursor = "next-atom";
    wire.read.mockResolvedValueOnce(capture(initial));
    const pending = deferred<SourceEvidencePage>();
    evidence.mockReturnValueOnce(pending.promise);
    render(renderEvidence());
    fireEvent.click(await screen.findByRole("button", { name: button }));
    expect(screen.getByText(/loading preserved evidence/i)).toBeInTheDocument();
    expect(screen.queryByText("No preserved version yet")).not.toBeInTheDocument();
    await act(async () => pending.resolve({ ...page(), selected_version: null }));
    expect(await screen.findByText("No preserved version yet")).toBeInTheDocument();
    expect(screen.queryByText(/loading preserved evidence/i)).not.toBeInTheDocument();
  });
  it("uses exact source-item/version/passage identity and labels saved summaries", async () => {
    navigation.params = new URLSearchParams({ version_id: "33333333-3333-4333-8333-333333333333", semantic_atom_id: "55555555-5555-4555-8555-555555555555", passage_id: "44444444-4444-4444-8444-444444444444" });
    const view = render(renderEvidence());
    const link = await screen.findByRole("link", { name: "Show exact passage" });
    const url = new URL((link.getAttribute("href") || ""), "https://example.test");
    expect(url.pathname).toBe("/source");
    expect(Object.fromEntries(url.searchParams)).toEqual({ graph_id: "11111111-1111-4111-8111-111111111111", source_item_id: "22222222-2222-4222-8222-222222222222", version_id: "33333333-3333-4333-8333-333333333333", semantic_atom_id: "55555555-5555-4555-8555-555555555555", passage_id: "44444444-4444-4444-8444-444444444444" });
    expect(view.container.querySelector("mark")).toHaveTextContent("Ship Friday");
    expect(screen.getByText("Generated summary")).toBeInTheDocument();
    expect(screen.getByText(/complete original transcript or document was not captured/)).toBeInTheDocument();
    expect(screen.getByText("mailbox:two")).toBeInTheDocument();
    const request = new URL(wire.read.mock.calls[0][0], "https://wire.test");
    expect(Object.fromEntries(request.searchParams)).toEqual({ graph: "11111111-1111-4111-8111-111111111111", source: "22222222-2222-4222-8222-222222222222", kind: "source", version_id: "33333333-3333-4333-8333-333333333333", atom_id: "55555555-5555-4555-8555-555555555555" });
    expect(evidence).not.toHaveBeenCalled();
    navigation.params.set("passage_id", "another-passage"); view.rerender(renderEvidence());
    expect(screen.getByText("The linked passage is not available in this atom.")).toBeInTheDocument();
    expect(wire.read).toHaveBeenCalledTimes(1);

  });
  it("retries and paginates through the fixed admitted API while clearing failed evidence", async () => {
    const initial = page(); initial.next_cursor = "older/+?"; initial.selected_version.extraction_status = "failed"; initial.selected_version.can_retry = true;
    wire.read.mockResolvedValueOnce(capture(initial));
    evidence.mockResolvedValueOnce({ ...initial, next_cursor: null, versions: [{ version_id: "33333333-3333-4333-8333-333333333335", captured_at: "2026-10-01T12:00:00Z", is_current: false, is_legacy_baseline: true }] });
    vi.mocked(retryP6Evidence).mockRejectedValueOnce(new Error("Retry unavailable"));
    render(renderEvidence());
    fireEvent.click(await screen.findByRole("button", { name: "Older versions" }));
    await screen.findByRole("option", { name: /Preserved/ });
    expect(screen.getByRole("combobox")).toHaveValue("33333333-3333-4333-8333-333333333333");
    expect(evidence).toHaveBeenLastCalledWith("token", { graphId: "11111111-1111-4111-8111-111111111111", sourceItemId: "22222222-2222-4222-8222-222222222222", versionId: "33333333-3333-4333-8333-333333333333", before: "older/+?" }, expect.any(AbortSignal));
    fireEvent.click(screen.getByRole("button", { name: "Retry extraction" }));
    expect(await screen.findByRole("alert")).toHaveTextContent("Retry unavailable");
    expect(screen.queryByLabelText("Preserved source text")).not.toBeInTheDocument();
    expect(retryP6Evidence).toHaveBeenCalledWith("token", { graphId: "11111111-1111-4111-8111-111111111111", sourceItemId: "22222222-2222-4222-8222-222222222222", versionId: "33333333-3333-4333-8333-333333333333" }, expect.any(AbortSignal));
  });
  it("clears retained content on refusal and cannot revive it after a transient retry", async () => {
    const running = page(); running.selected_version.extraction_status = "running";
    wire.read.mockResolvedValueOnce(capture(running)).mockRejectedValueOnce(new ApiError("Access denied", 403)).mockRejectedValueOnce(new ApiError("Unavailable", 503));
    render(renderEvidence()); await screen.findByText("The launch is Friday.");
    act(() => returnToTab());
    expect(await screen.findByRole("alert")).toHaveTextContent("Access denied");
    expect(screen.queryByText("The launch is Friday.")).not.toBeInTheDocument();
    fireEvent.click(screen.getByRole("button", { name: "Refresh evidence" }));
    expect(await screen.findByRole("alert")).toHaveTextContent("Unavailable");
    expect(screen.queryByText("The launch is Friday.")).not.toBeInTheDocument();
    expect(evidence).not.toHaveBeenCalled();
  });
  it("requires source scope in the URL and never borrows the ambient ocean", () => {
    navigation.params = new URLSearchParams({ source_item_id: "22222222-2222-4222-8222-222222222222" }); render(<SourcePage />);
    expect(screen.getByText("No source selected")).toBeInTheDocument(); expect(evidence).not.toHaveBeenCalled();
  });
  it("links generic node support to its exact source and labels historical baselines", async () => {
    vi.mocked(readP6NodeAtoms).mockResolvedValue({ graph_id: "11111111-1111-4111-8111-111111111111", node_id: "66666666-6666-4666-8666-666666666666", next_cursor: null, items: [{ ...identity, representation: "normalized_text", atom_id: "55555555-5555-4555-8555-555555555555", kind: "decision", statement: "The launch is Friday.", source_version_id: "33333333-3333-4333-8333-333333333333", captured_at: "2026-10-10T12:00:00Z", is_legacy_baseline: true, passages: [passage] }] });
    render(<NodeMeetingAtoms graphId="11111111-1111-4111-8111-111111111111" nodeId="66666666-6666-4666-8666-666666666666" getAuthToken={auth} />);
    const link = await screen.findByRole("link", { name: "Open supporting passage" });
    const url = new URL((link.getAttribute("href") || ""), "https://example.test");
    expect(url.pathname).toBe("/source"); expect(url.searchParams.get("source_item_id")).toBe("22222222-2222-4222-8222-222222222222"); expect(url.searchParams.get("version_id")).toBe("33333333-3333-4333-8333-333333333333");
    expect(screen.getByText(/^Preserved \d/)).toBeInTheDocument(); expect(screen.getByRole("heading", { name: "Source atoms" })).toBeInTheDocument();
  });
  it.each(["", "missing", "unknown kind", "nonmeeting", "invalid"])("shows availability instead of guessing a legacy link with %s address", async (kind) => {
    const atom = { ...identity, source_item_id: undefined, source_type: "meeting", source_id: "", atom_id: "55555555-5555-4555-8555-555555555555", kind: "decision" as const, statement: "The launch is Friday.", source_version_id: "33333333-3333-4333-8333-333333333333", captured_at: "2026-10-10T12:00:00Z", is_legacy_baseline: true, passages: [passage] };
    if (kind === "missing") Object.assign(atom, { source_id: undefined });
    if (kind === "unknown kind") Object.assign(atom, { source_type: undefined, source_id: "legacy-meeting" });
    if (kind === "nonmeeting") Object.assign(atom, { source_type: "email", source_id: "legacy-email" });
    if (kind === "invalid") atom.source_id = "legacy\u0000meeting";
    vi.mocked(readP6NodeAtoms).mockResolvedValue({ graph_id: "11111111-1111-4111-8111-111111111111", node_id: "66666666-6666-4666-8666-666666666666", next_cursor: null, items: [atom] });
    render(<NodeMeetingAtoms graphId="11111111-1111-4111-8111-111111111111" nodeId="66666666-6666-4666-8666-666666666666" getAuthToken={auth} />);
    expect(await screen.findByText("Exact retained source address unavailable.")).toBeInTheDocument();
    expect(screen.getByText("The launch is Friday.")).toBeInTheDocument();
    expect(screen.queryByRole("link", { name: "Open supporting passage" })).not.toBeInTheDocument();
  });
  it("preserves a verified explicit legacy meeting address and exact version", async () => {
    vi.mocked(readP6NodeAtoms).mockResolvedValue({ graph_id: "11111111-1111-4111-8111-111111111111", node_id: "66666666-6666-4666-8666-666666666666", next_cursor: null, items: [{ ...identity, source_type: "meeting", source_item_id: undefined, source_id: "legacy/+?", atom_id: "55555555-5555-4555-8555-555555555555", kind: "decision", statement: "The launch is Friday.", source_version_id: "33333333-3333-4333-8333-333333333333", captured_at: "2026-10-10T12:00:00Z", is_legacy_baseline: true, passages: [passage] }] });
    render(<NodeMeetingAtoms graphId="11111111-1111-4111-8111-111111111111" nodeId="66666666-6666-4666-8666-666666666666" getAuthToken={auth} />);
    const link = await screen.findByRole("link", { name: "Open supporting passage" });
    const url = new URL(link.getAttribute("href") || "", "https://example.test");
    expect(url.pathname).toBe("/meeting");
    expect(url.searchParams.get("source_id")).toBe("legacy/+?");
    expect(url.searchParams.get("version_id")).toBe("33333333-3333-4333-8333-333333333333");
  });
});
describe("preserved source inventory", () => {
  it("keeps unknown baseline provenance and missing clocks explicit", async () => {
    items.mockResolvedValue({ graph_id: "11111111-1111-4111-8111-111111111111", items: [{ ...item(), captured_at: null }], next_cursor: null });
    render(<SourceInventory graphId="11111111-1111-4111-8111-111111111111" getAuthToken={auth} />);
    expect(await screen.findByText(/Retained time unknown/)).toBeInTheDocument();
    expect(screen.queryByText(/Captured/)).not.toBeInTheDocument();
  });
  it("loads bounded pages, deduplicates items, and clears loaded rows on failure", async () => {
    items.mockResolvedValueOnce({ graph_id: "11111111-1111-4111-8111-111111111111", items: [item("one")], next_cursor: "page-two" }).mockResolvedValueOnce({ graph_id: "11111111-1111-4111-8111-111111111111", items: [item("one"), item("two")], next_cursor: "page-three" }).mockRejectedValueOnce(new Error("Connection interrupted"));
    render(<SourceInventory graphId="11111111-1111-4111-8111-111111111111" getAuthToken={auth} />);
    fireEvent.click(await screen.findByRole("button", { name: "More sources" })); await screen.findByText("Email two");
    expect(screen.getAllByText("Email one")).toHaveLength(1); expect(items).toHaveBeenNthCalledWith(2, "token", { graphId: "11111111-1111-4111-8111-111111111111", before: "page-two" }, expect.any(AbortSignal));
    fireEvent.click(screen.getByRole("button", { name: "More sources" }));
    expect(await screen.findByRole("alert")).toHaveTextContent("Connection interrupted"); expect(screen.queryByText("Email one")).not.toBeInTheDocument();
  });
  it("opens only verified chunk membership and never falls back to a newer version", async () => {
    items.mockResolvedValue({ graph_id: "11111111-1111-4111-8111-111111111111", items: [item(), { ...item("unmapped"), mapped_version_id: null }], next_cursor: null });
    render(<SourceInventory graphId="11111111-1111-4111-8111-111111111111" atomId="chunk/+?" getAuthToken={auth} />);
    const link = await screen.findByRole("link", { name: /Open verified source version/ }); const url = new URL((link.getAttribute("href") || ""), "https://example.test");
    expect(url.searchParams.get("version_id")).toBe("33333333-3333-4333-8333-333333333333"); expect(url.searchParams.get("source_item_id")).toBe("22222222-2222-4222-8222-222222222222"); expect(screen.getAllByRole("link")).toHaveLength(1);
    expect(screen.queryByText("Email 22222222-2222-4222-8222-222222222222")).not.toBeInTheDocument(); expect(items).toHaveBeenCalledWith("token", { graphId: "11111111-1111-4111-8111-111111111111", atomId: "chunk/+?" }, expect.any(AbortSignal));
  });
  it("does not invent evidence links for legacy chunks without a mapping", async () => {
    items.mockResolvedValue({ graph_id: "11111111-1111-4111-8111-111111111111", items: [], next_cursor: null }); render(<SourceInventory graphId="11111111-1111-4111-8111-111111111111" atomId="chunk" getAuthToken={auth} />);
    expect(await screen.findByText("No verified preserved version")).toBeInTheDocument(); expect(screen.queryByRole("link")).not.toBeInTheDocument();
  });
  it("discards a slow page from a previous workspace", async () => {
    const old = deferred<{ graph_id: string; items: SourceItem[]; next_cursor: null }>();
    items.mockReturnValueOnce(old.promise).mockResolvedValueOnce({ graph_id: "next-ocean", items: [item("next")], next_cursor: null });
    const view = render(<SourceInventory graphId="11111111-1111-4111-8111-111111111111" getAuthToken={auth} />); await act(async () => {});
    view.rerender(<SourceInventory graphId="next-ocean" getAuthToken={auth} />); await screen.findByText("Email next");
    await act(async () => old.resolve({ graph_id: "11111111-1111-4111-8111-111111111111", items: [item("stale")], next_cursor: null })); expect(screen.queryByText("Email stale")).not.toBeInTheDocument();
  });
});

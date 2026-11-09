// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import { act, cleanup, fireEvent, screen, waitFor } from "@testing-library/react";
import { render, returnToTab } from "@/lib/testing/queryRender";
import { TransportError } from "@/lib/transport";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { MeetingEvidence, PreservedTranscript, exactPassageParts } from "./MeetingEvidence";
import { NodeMeetingAtoms } from "./NodeMeetingAtoms";
import MeetingSummaryPage from "@/app/(app)/meeting/page";
import { type SourceEvidencePage, type MeetingEvidencePassage } from "@/lib/api";

import { readP6Evidence, retryP6Evidence, readP6NodeAtoms, readP6LegacyCapture } from "@/lib/p6EvidenceApi";

const navigation = vi.hoisted(() => ({ params: new URLSearchParams(), replace: vi.fn() }));
vi.mock("next/navigation", () => ({ useSearchParams: () => navigation.params, useRouter: () => ({ replace: navigation.replace }) }));
vi.mock("@/lib/providers", () => ({ useApp: () => ({ token: "token", currentGraph: { id: "unrelated-ambient-ocean" } }) }));
vi.mock("@/lib/useEvidenceAdmission", () => ({ useEvidenceAdmission: () => ({ key: "account:scope", ready: true, getToken: () => "token", currentGraphId: "ambient-unrelated", graphs: [], queryScope: "account:scope" }) }));
vi.mock("@/lib/p6EvidenceApi", async (actual) => ({ ...(await actual<typeof import("@/lib/p6EvidenceApi")>()), readP6Evidence: vi.fn(), retryP6Evidence: vi.fn(), readP6NodeAtoms: vi.fn(), readP6SourceItems: vi.fn(), readP6LegacyCapture: vi.fn() }));
const wire = vi.hoisted(() => ({ read: vi.fn() }));
vi.mock("@/lib/api", async (actual) => ({ ...(await actual<typeof import("@/lib/api")>()), apiRequest: vi.fn((path: string) => {
  if (!path.startsWith("/v1/load/captures?")) throw new Error(`Unexpected mount read ${path}`);
  return wire.read(path);
}) }));
const evidence = vi.mocked(readP6Evidence);
const retry = vi.mocked(retryP6Evidence);
const passage: MeetingEvidencePassage = { passage_id: "44444444-4444-4444-8444-444444444444", quote: "Ship Friday", start_char: 14, end_char: 25, start_ms: null, end_ms: null, speaker: null };
const raw = "🪸 Ship Friday\nShip Friday";

function page(version = "33333333-3333-4333-8333-333333333331", overrides: Partial<NonNullable<SourceEvidencePage["selected_version"]>> = {}): SourceEvidencePage {
  return {
    graph_id: "11111111-1111-4111-8111-111111111111", source_item_id: "22222222-2222-4222-8222-222222222222", source: { source_item_id: "22222222-2222-4222-8222-222222222222", source_id: "meeting:+?", source_type: "meeting", provider_namespace: "teams" }, next_cursor: null,
    versions: [
      { version_id: "33333333-3333-4333-8333-333333333331", captured_at: "2026-10-11T10:00:00Z", is_legacy_baseline: false, is_current: true },
      { version_id: "33333333-3333-4333-8333-333333333330", captured_at: "2026-10-10T10:00:00Z", is_legacy_baseline: false, is_current: false },
    ],
    selected_version: {
      version_id: version, captured_at: "2026-10-11T10:00:00Z", is_legacy_baseline: false, raw_text: raw, text_status: "available", extraction_status: "done", extraction_error: null, can_retry: false, atoms_next_cursor: null,
      atoms: [{ atom_id: "55555555-5555-4555-8555-555555555555", kind: "decision", statement: `${version}: The team will ship Friday.`, passages: [passage], nodes: [{ node_id: "66666666-6666-4666-8666-666666666666", graph_id: "11111111-1111-4111-8111-111111111111", title: "Launch plan", type: "entity" }] }], ...overrides,
    },
  };
}
function capture(evidence: SourceEvidencePage | null = page(), owed = false) {
  return { model: "capture_detail", version: 2, computed_at: "2026-10-31T00:00:00Z", stale: owed, refresh_eta: owed ? "2026-10-31T00:00:02Z" : null,
    payload: { summary: { source_id: "meeting:+?", projection_status: "unverified_lineage", occurred_at: null, source_items: [{ graph_id: "11111111-1111-4111-8111-111111111111", source_item_id: "22222222-2222-4222-8222-222222222222", source_version_id: "33333333-3333-4333-8333-333333333331" }] }, evidence } };
}
const renderEvidence = () => <MeetingEvidence token="token" graphId="11111111-1111-4111-8111-111111111111" sourceId="meeting:+?" />;
function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((r) => { resolve = r; });
  return { promise, resolve };
}

beforeEach(() => {
  navigation.params = new URLSearchParams();
  wire.read.mockResolvedValue(capture());
});
afterEach(() => { cleanup(); vi.resetAllMocks(); vi.restoreAllMocks(); vi.useRealTimers(); });

describe("exact preserved meeting evidence", () => {
  it("counts Unicode scalars and highlights only the recorded repeated occurrence", () => {
    expect(exactPassageParts(raw, passage)).toEqual({ before: "🪸 Ship Friday\n", quote: "Ship Friday", after: "" });
    const { container } = render(<PreservedTranscript text={raw} passage={passage} />);
    expect(container.querySelectorAll("mark")).toHaveLength(1);
    expect(container.querySelector("mark")?.previousSibling?.textContent).toBe("🪸 Ship Friday\n");
    expect(screen.getByLabelText("Preserved transcript").textContent).toBe(raw);
  });

  it("does not fall back to another occurrence for invalid offsets or a mismatched quote", () => {
    for (const invalid of [{ start_char: 13 }, { end_char: 100 }, { start_char: -1 }, { end_char: 14 }, { start_char: 1.5 }, { quote: "Friday" }]) {
      expect(exactPassageParts(raw, { ...passage, ...invalid })).toBeNull();
    }
    const { container } = render(<PreservedTranscript text={raw} passage={{ ...passage, start_char: 13 }} />);
    expect(container.querySelector("mark")).toBeNull();
    expect(screen.getByRole("status")).toHaveTextContent("without a highlight");
  });

  it("keeps version, semantic atom, exact passage, and workspace in source links", async () => {
    render(renderEvidence());
    const link = await screen.findByRole("link", { name: "Show exact passage" });
    const url = new URL(link.getAttribute("href") ?? "", "https://example.test");
    expect(url.pathname).toBe("/meeting");
    expect(Object.fromEntries(url.searchParams)).toEqual({ source_id: "meeting:+?", graph_id: "11111111-1111-4111-8111-111111111111", version_id: "33333333-3333-4333-8333-333333333331", semantic_atom_id: "55555555-5555-4555-8555-555555555555", passage_id: "44444444-4444-4444-8444-444444444444" });
    const node = new URL(screen.getByRole("link", { name: "Launch plan" }).getAttribute("href") ?? "", "https://example.test");
    expect(Object.fromEntries(node.searchParams)).toEqual({ id: "66666666-6666-4666-8666-666666666666", graph_id: "11111111-1111-4111-8111-111111111111" });
    expect(node.pathname).toBe("/topics");
  });

  it("switches versions through a scoped URL and clears the prior atom selection", async () => {
    navigation.params = new URLSearchParams({ semantic_atom_id: "55555555-5555-4555-8555-555555555555", passage_id: "44444444-4444-4444-8444-444444444444" });
    render(renderEvidence());
    fireEvent.change(await screen.findByRole("combobox"), { target: { value: "33333333-3333-4333-8333-333333333330" } });
    const url = new URL(navigation.replace.mock.calls[0][0], "https://example.test");
    expect(Object.fromEntries(url.searchParams)).toEqual({ source_id: "meeting:+?", graph_id: "11111111-1111-4111-8111-111111111111", version_id: "33333333-3333-4333-8333-333333333330" });
  });

  it("drops a slow response from the previously selected version", async () => {
    const old = deferred<ReturnType<typeof capture>>();
    wire.read.mockReturnValueOnce(old.promise).mockResolvedValueOnce(capture(page("33333333-3333-4333-8333-333333333330")));
    const view = render(renderEvidence());
    navigation.params = new URLSearchParams({ version_id: "33333333-3333-4333-8333-333333333330" });
    view.rerender(renderEvidence());
    await screen.findByText("33333333-3333-4333-8333-333333333330: The team will ship Friday.");
    await act(async () => old.resolve(capture(page("33333333-3333-4333-8333-333333333331"))));
    expect(screen.queryByText("33333333-3333-4333-8333-333333333331: The team will ship Friday.")).not.toBeInTheDocument();
    expect(screen.getByRole("combobox")).toHaveValue("33333333-3333-4333-8333-333333333330");
  });

  it("clears loaded transcript and atoms when retry fails", async () => {
    wire.read.mockResolvedValue(capture(page("33333333-3333-4333-8333-333333333331", { extraction_status: "failed", can_retry: true })));
    retry.mockRejectedValue(new Error("Extraction is temporarily unavailable."));
    render(renderEvidence());
    fireEvent.click(await screen.findByRole("button", { name: "Retry extraction" }));
    expect(await screen.findByRole("alert")).toHaveTextContent("Extraction is temporarily unavailable.");
    expect(screen.queryByText("33333333-3333-4333-8333-333333333331: The team will ship Friday.")).not.toBeInTheDocument();
    expect(screen.queryByLabelText("Preserved transcript")).not.toBeInTheDocument();
    expect(retry).toHaveBeenCalledWith("token", { graphId: "11111111-1111-4111-8111-111111111111", sourceId: "meeting:+?", versionId: "33333333-3333-4333-8333-333333333331" }, expect.any(AbortSignal));
  });

  it("reads older version choices with the exact selected version pinned", async () => {
    wire.read.mockResolvedValueOnce(capture({ ...page(), next_cursor: "cursor/+?" }));
    evidence.mockResolvedValueOnce({ ...page("33333333-3333-4333-8333-333333333331"), versions: [{ version_id: "33333333-3333-4333-8333-333333333339", captured_at: "2026-10-01T10:00:00Z", is_legacy_baseline: false, is_current: false }] });
    render(renderEvidence());
    fireEvent.click(await screen.findByRole("button", { name: "Older versions" }));
    await waitFor(() => expect(screen.getAllByRole("option")).toHaveLength(2));
    expect(screen.getByRole("combobox")).toHaveValue("33333333-3333-4333-8333-333333333331");
    expect(screen.getByText("33333333-3333-4333-8333-333333333331: The team will ship Friday.")).toBeInTheDocument();
    expect(evidence).toHaveBeenLastCalledWith("token", { graphId: "11111111-1111-4111-8111-111111111111", sourceId: "meeting:+?", versionId: "33333333-3333-4333-8333-333333333331", before: "cursor/+?" }, expect.any(AbortSignal));
  });

  it("polls a pending extraction to completion without replacing source scope", async () => {
    vi.useFakeTimers();
    wire.read.mockResolvedValueOnce(capture(page("33333333-3333-4333-8333-333333333331", { extraction_status: "pending", atoms: [] }), true)).mockResolvedValueOnce(capture());
    render(renderEvidence());
    await act(async () => { await vi.advanceTimersByTimeAsync(20); }); // TanStack delivers results on a timer
    expect(screen.getByText(/Atom extraction is queued/)).toBeInTheDocument();
    await act(async () => { await vi.advanceTimersByTimeAsync(30_000); });
    await act(async () => { await vi.advanceTimersByTimeAsync(20); }); // TanStack delivers results on a timer
    expect(screen.getByText("33333333-3333-4333-8333-333333333331: The team will ship Friday.")).toBeInTheDocument();
    await act(async () => { await vi.advanceTimersByTimeAsync(8000); });
    expect(wire.read).toHaveBeenCalledTimes(2);
  });

  it("keeps preserved evidence with a note after a transient refresh failure", async () => {
    vi.useFakeTimers();
    wire.read.mockResolvedValueOnce(capture(page("33333333-3333-4333-8333-333333333331", { extraction_status: "running" }), true)).mockRejectedValueOnce(new TransportError("network", "/v1/p6/evidence", "Failed to fetch"));
    render(renderEvidence());
    await act(async () => { await vi.advanceTimersByTimeAsync(20); }); // TanStack delivers results on a timer
    await act(async () => { await vi.advanceTimersByTimeAsync(30_000); });
    await act(async () => { await vi.advanceTimersByTimeAsync(20); }); // TanStack delivers results on a timer
    expect(screen.getByRole("alert")).toHaveTextContent("Library could not be reached.");
    expect(screen.getByText("33333333-3333-4333-8333-333333333331: The team will ship Friday.")).toBeInTheDocument();
  });

  it("clears preserved evidence when a polling refresh fails verification", async () => {
    vi.useFakeTimers();
    wire.read.mockResolvedValueOnce(capture(page("33333333-3333-4333-8333-333333333331", { extraction_status: "running" }), true)).mockRejectedValueOnce(new SyntaxError("Evidence could not be verified"));
    render(renderEvidence());
    await act(async () => { await vi.advanceTimersByTimeAsync(20); }); // TanStack delivers results on a timer
    await act(async () => { await vi.advanceTimersByTimeAsync(30_000); });
    await act(async () => { await vi.advanceTimersByTimeAsync(20); }); // TanStack delivers results on a timer
    expect(screen.getByRole("alert")).toHaveTextContent("Evidence could not be verified");
    expect(screen.queryByText("33333333-3333-4333-8333-333333333331: The team will ship Friday.")).not.toBeInTheDocument();
  });

  it("distinguishes absent versions from an unavailable transcript", async () => {
    wire.read.mockResolvedValueOnce(capture({ ...page(), selected_version: null, versions: [] }));
    const view = render(renderEvidence());
    await screen.findByText("No preserved version yet");
    wire.read.mockResolvedValueOnce(capture(page("33333333-3333-4333-8333-333333333330", { raw_text: null, text_status: "unavailable" })));
    navigation.params = new URLSearchParams({ version_id: "33333333-3333-4333-8333-333333333330" });
    view.rerender(renderEvidence());
    await screen.findByText(/preserved transcript text is temporarily unavailable/);
    expect(screen.queryByText("No preserved version yet")).not.toBeInTheDocument();
  });

  it("shares one Stored read while the unattributed summary remains unavailable", async () => {
    navigation.params = new URLSearchParams({ graph_id: "11111111-1111-4111-8111-111111111111", source_id: "meeting:+?", version_id: "33333333-3333-4333-8333-333333333331" });
    vi.mocked(readP6LegacyCapture).mockRejectedValue(new Error("Current summary could not load."));
    // The summary's address is frozen against `window.location`; jsdom boots
    // at `/`, so the page must be rendered at the address it was linked to.
    window.history.replaceState(null, "", `/meeting?${navigation.params}`);
    render(<MeetingSummaryPage />);
    await screen.findByText(/The unattributed stored summary and correction overlay/);
    expect(await screen.findByText("33333333-3333-4333-8333-333333333331: The team will ship Friday.")).toBeInTheDocument();
    expect(screen.getByRole("heading", { name: "Meeting retained evidence" })).toBeInTheDocument();
    expect(wire.read).toHaveBeenCalledTimes(1);
    expect(evidence).not.toHaveBeenCalled();
    expect(readP6LegacyCapture).not.toHaveBeenCalled();
  });

  it("traverses from a node to the exact preserved meeting version and passage", async () => {
    vi.mocked(readP6NodeAtoms).mockResolvedValue({ graph_id: "node-ocean", node_id: "node-1", next_cursor: null, items: [{ atom_id: "atom-1", kind: "decision", statement: "Launch Friday", source_type: "meeting", source_id: "meeting:+?", source_version_id: "older-version", captured_at: "2026-10-10T10:00:00Z", passages: [passage] }] });
    const getAuthToken = vi.fn().mockResolvedValue("fresh-token");
    render(<NodeMeetingAtoms graphId="node-ocean" nodeId="node-1" getAuthToken={getAuthToken} />);
    const link = await screen.findByRole("link", { name: "Open supporting passage" });
    const url = new URL(link.getAttribute("href") ?? "", "https://example.test");
    expect(Object.fromEntries(url.searchParams)).toEqual({ graph_id: "node-ocean", source_id: "meeting:+?", version_id: "older-version", semantic_atom_id: "atom-1", passage_id: "44444444-4444-4444-8444-444444444444" });
    expect(readP6NodeAtoms).toHaveBeenCalledWith("token", { graphId: "node-ocean", nodeId: "node-1" }, expect.any(AbortSignal));
  });

  it("loads an atom beyond the first page directly and reads the next admitted atom page without replaying prior atoms", async () => {
    navigation.params = new URLSearchParams({ version_id: "33333333-3333-4333-8333-333333333331", semantic_atom_id: "55555555-5555-4555-8555-555555555555", passage_id: "44444444-4444-4444-8444-444444444444" });
    const first = page("33333333-3333-4333-8333-333333333331", { atoms_next_cursor: "after-100" });
    const later = page();
    if (later.selected_version) later.selected_version.atoms[0] = { ...later.selected_version.atoms[0], atom_id: "55555555-5555-4555-8555-555555555102", statement: "Another supported atom." };
    wire.read.mockResolvedValueOnce(capture(first));
    evidence.mockResolvedValueOnce(later);
    render(renderEvidence());
    fireEvent.click(await screen.findByRole("button", { name: "More atoms" }));
    await screen.findByText("Another supported atom.");
    expect(screen.queryByText("33333333-3333-4333-8333-333333333331: The team will ship Friday.")).not.toBeInTheDocument();
    expect(Object.fromEntries(new URL(wire.read.mock.calls[0][0], "https://wire.test").searchParams)).toMatchObject({ version_id: "33333333-3333-4333-8333-333333333331", atom_id: "55555555-5555-4555-8555-555555555555" });
    expect(evidence.mock.calls[0][1]).toMatchObject({ versionId: "33333333-3333-4333-8333-333333333331", atomsAfter: "after-100" });
    expect(screen.queryByRole("button", { name: "More atoms" })).not.toBeInTheDocument();
    expect(screen.getByRole("link", { name: "Show from the beginning" })).toHaveAttribute("href", expect.not.stringContaining("semantic_atom_id"));
  });

  it("recovers a failed live refresh only through a new server read", async () => {
    wire.read.mockResolvedValueOnce(capture()).mockRejectedValueOnce(new TransportError("network", "/v1/p6/evidence", "Failed to fetch")).mockResolvedValueOnce(capture(page("33333333-3333-4333-8333-333333333330")));
    render(renderEvidence()); await screen.findByText("33333333-3333-4333-8333-333333333331: The team will ship Friday.");
    act(() => returnToTab());
    await screen.findByRole("alert");
    expect(screen.getByText("33333333-3333-4333-8333-333333333331: The team will ship Friday.")).toBeInTheDocument();
    fireEvent.click(screen.getByRole("button", { name: "Refresh evidence" }));
    await screen.findByText("33333333-3333-4333-8333-333333333330: The team will ship Friday.");
    await waitFor(() => expect(screen.queryByRole("alert")).not.toBeInTheDocument());
  });
});

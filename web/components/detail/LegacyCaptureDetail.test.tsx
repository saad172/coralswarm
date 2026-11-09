// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import { act, cleanup, fireEvent, screen, waitFor } from "@testing-library/react";
import { render, returnToTab } from "@/lib/testing/queryRender";
import { ApiError } from "@/lib/api";
import { TransportError } from "@/lib/transport";
import { afterEach, beforeEach, expect, it, vi } from "vitest";
import { LegacyCaptureDetail } from "./LegacyCaptureDetail";
import { readP6LegacyCapture } from "@/lib/p6EvidenceApi";
import MeetingPage from "@/app/(app)/meeting/page";
const state = vi.hoisted(() => ({ key: "identity-one", ready: true, tokenAvailable: true, meeting: vi.fn(), params: new URLSearchParams("source_id=meeting&graph_id=11111111-1111-4111-8111-111111111111") }));
vi.mock("next/navigation", () => ({ useSearchParams: () => state.params }));
vi.mock("@/lib/useEvidenceAdmission", () => ({ useEvidenceAdmission: () => ({ key: state.key, ready: state.ready, getToken: () => { if (!state.tokenAvailable) throw new Error("Actor changed"); return "token"; }, currentGraphId: "ambient", graphs: [], queryScope: state.key }) }));
vi.mock("@/components/meetings/MeetingEvidence", () => ({ MeetingEvidence: (props: unknown) => { state.meeting(props); return null; } }));
vi.mock("@/lib/p6EvidenceApi", async (actual) => ({ ...(await actual<typeof import("@/lib/p6EvidenceApi")>()), readP6LegacyCapture: vi.fn() }));
const wire = vi.hoisted(() => vi.fn());
vi.mock("@/lib/api", async (actual) => ({ ...(await actual<typeof import("@/lib/api")>()), apiRequest: vi.fn((path: string) => {
 if (!path.startsWith("/v1/load/captures?")) throw new Error(`Unexpected mount read ${path}`);
 return wire(path);
}) }));
const projection = () => ({ source_id: "meeting", projection_status: "unverified_lineage" as const, source_items: [{ graph_id: "11111111-1111-4111-8111-111111111111", source_item_id: "22222222-2222-4222-8222-222222222222", source_version_id: "33333333-3333-4333-8333-333333333333" }], occurred_at: null });
const captured = (summary = projection()) => ({ model: "capture_detail", version: 2, computed_at: "2026-10-31T00:00:00Z", stale: false, refresh_eta: null, payload: { summary, evidence: null } });
beforeEach(() => { state.key = "identity-one"; state.ready = true; state.tokenAvailable = true; state.params = new URLSearchParams("source_id=meeting&graph_id=11111111-1111-4111-8111-111111111111"); state.meeting.mockClear(); wire.mockResolvedValue(captured()); window.history.replaceState(null, "", `/meeting?${state.params}`); });
afterEach(() => { cleanup(); vi.resetAllMocks(); vi.restoreAllMocks(); });
it("links only exact proven retained versions", async () => {
  render(<LegacyCaptureDetail />);
  const link = await screen.findByRole("link", { name: "Open retained source 1" });
  expect(Object.fromEntries(new URL(link.getAttribute("href") || "", "https://example.test").searchParams)).toEqual({ graph_id: "11111111-1111-4111-8111-111111111111", source_item_id: "22222222-2222-4222-8222-222222222222", version_id: "33333333-3333-4333-8333-333333333333" });
  expect(screen.getByText("Source event time").nextSibling).toHaveTextContent("Unknown");
  expect(screen.getByText(/summary and correction overlay/)).toHaveTextContent("unavailable");
  expect(screen.queryByRole("button", { name: /Promote|Retract/ })).not.toBeInTheDocument();
});
it.each([
  ["403", new ApiError("403", 403)],
  ["404", new ApiError("404", 404)],
  ["failed verification", new SyntaxError("failed verification")],
])("clears source links when revalidation is denied: %s", async (message, failure) => {
  render(<LegacyCaptureDetail />); await screen.findByRole("link", { name: "Open retained source 1" });
  wire.mockRejectedValueOnce(failure);
  act(() => returnToTab());
  expect(await screen.findByRole("alert")).toHaveTextContent(message);
  expect(screen.queryByRole("link", { name: "Open retained source 1" })).not.toBeInTheDocument();
});
it.each([
  ["503", new ApiError("503 unavailable", 503)],
  ["offline", new TransportError("network", "/v1/p6/legacy", "Failed to fetch")],
])("keeps verified source links with a note when revalidation fails transiently: %s", async (_kind, failure) => {
  render(<LegacyCaptureDetail />); await screen.findByRole("link", { name: "Open retained source 1" });
  wire.mockRejectedValueOnce(failure);
  act(() => returnToTab());
  expect(await screen.findByRole("alert")).toBeInTheDocument();
  expect(screen.getByRole("link", { name: "Open retained source 1" })).toBeInTheDocument();
});
it("rejects a slow source mapping from a previous identity and does not call it empty history", async () => {
  let resolve!: (value: ReturnType<typeof captured>) => void;
  wire.mockReturnValueOnce(new Promise((r) => { resolve = r; })).mockResolvedValueOnce(captured({ ...projection(), source_items: [] }));
  const view = render(<LegacyCaptureDetail />);
  state.key = "identity-two"; view.rerender(<LegacyCaptureDetail />);
  await screen.findByText("No verified source mapping");
  await act(async () => resolve(captured()));
  expect(screen.queryByRole("link", { name: "Open retained source 1" })).not.toBeInTheDocument();
  expect(screen.getByText(/does not establish an empty history/)).toBeInTheDocument();
});

it("links immutable context/outcomes without fetching a guessed aggregate session history", async () => {
  render(<LegacyCaptureDetail />);
  const link = await screen.findByRole("link", { name: "Inspect exact captured context and outcomes 1" });
  const params = new URL(link.getAttribute("href") || "", "https://example.test").searchParams;
  expect(Object.fromEntries(params)).toEqual({ g: "11111111-1111-4111-8111-111111111111", item: "22222222-2222-4222-8222-222222222222", version: "33333333-3333-4333-8333-333333333333", history: "1", view: "outcomes" });
  expect(params.has("session_id")).toBe(false);
});

it("withholds a delayed source mapping after navigation changes without rerender", async () => {
  let resolve!: (value: ReturnType<typeof captured>) => void;
  wire.mockReturnValueOnce(new Promise((r) => { resolve = r; }));
  render(<LegacyCaptureDetail />);
  window.history.replaceState(null, "", "/meeting?source_id=other&graph_id=11111111-1111-4111-8111-111111111111");
  await act(async () => resolve(captured()));
  expect(await screen.findByRole("alert")).toHaveTextContent("address changed");
  expect(screen.queryByRole("link", { name: "Open retained source 1" })).not.toBeInTheDocument();
});
it.each(["empty graph", "repeated id", "unknown selector"])("never substitutes an ambient/container read for an invalid %s meeting route", (kind) => {
  if (kind === "empty graph") state.params.set("graph_id", "");
  if (kind === "repeated id") state.params.append("source_id", "other");
  if (kind === "unknown selector") state.params.set("id", "unqualified");
  window.history.replaceState(null, "", `/meeting?${state.params}`);
  render(<LegacyCaptureDetail />);
  expect(screen.getByRole("alert")).toBeInTheDocument(); expect(readP6LegacyCapture).not.toHaveBeenCalled();
});
it.each(["actor", "address"])("refuses a stale retained-history link after its %s changes", async (axis) => {
  render(<LegacyCaptureDetail />);
  const link = await screen.findByRole("link", { name: "Inspect exact captured context and outcomes 1" });
  if (axis === "actor") state.tokenAvailable = false;
  else window.history.replaceState(null, "", "/meeting?source_id=other&graph_id=11111111-1111-4111-8111-111111111111");
  expect(fireEvent.click(link)).toBe(false);
  // The refused click re-checks; the re-check fails closed and drops the links.
  await waitFor(() => expect(screen.queryByRole("link", { name: "Inspect exact captured context and outcomes 1" })).not.toBeInTheDocument());
});
it("rejects invalid meeting selectors before either the container or exact-evidence sibling starts", () => {
  state.params = new URLSearchParams("source_id=meeting&graph_id=");
  window.history.replaceState(null, "", `/meeting?${state.params}`);
  render(<MeetingPage />);
  expect(screen.getByRole("alert")).toBeInTheDocument(); expect(readP6LegacyCapture).not.toHaveBeenCalled(); expect(state.meeting).not.toHaveBeenCalled();
});

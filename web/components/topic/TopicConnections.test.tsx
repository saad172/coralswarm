// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import { act, cleanup, fireEvent, screen, waitFor, within } from "@testing-library/react";
import { render, returnToTab } from "@/lib/testing/queryRender";
import { afterEach, beforeEach, expect, it, vi } from "vitest";
import { TopicConnections } from "./TopicConnections";
import { ApiError } from "@/lib/api";
import { TransportError } from "@/lib/transport";
import { readLibraryNeighborhood, type LibraryNeighborhood } from "@/lib/libraryHistoryApi";
vi.mock("@/lib/libraryHistoryApi", () => ({ readLibraryNeighborhood: vi.fn() }));
const graph = "11111111-1111-4111-8111-111111111111", node = "22222222-2222-4222-8222-222222222222", item = "33333333-3333-4333-8333-333333333333", version = "44444444-4444-4444-8444-444444444444";
const scope = { graph_ids: [graph], node_id: node, outcome_versions: "current" as const };
const onPage = vi.fn(), onNode = vi.fn();
const props = { identity: "account", getToken: () => "fixed", scope, nodeId: node, onPage, onNode };
function page(): LibraryNeighborhood { return { scope, node_id: node, items: [{ edge_id: version, predicate: "depends_on", origin: "inferred", status: "confirmed", confidence: 0.8, reviewed_by: "reviewer", src: { node_id: node, title: null, type: "entity" }, dst: { node_id: item, title: "Shipping", type: "entity" }, support: [{ source_item_id: item, source_version_id: version, source_version_is_current: false }, { kind: "legacy_chunk", graph_id: graph, atom_id: item }], support_truncated: true }], next_cursor: "bound-next", truncated: true }; }
beforeEach(() => { vi.mocked(readLibraryNeighborhood).mockResolvedValue(page()); });
afterEach(() => { cleanup(); vi.resetAllMocks(); });
it("renders named directed relationships, attributable support and bounded truncation without inferring validity", async () => {
  render(<TopicConnections {...props} />);
  expect(await screen.findByText("depends on")).toBeInTheDocument();
  // Grouped per predicate: the heading names the relationship once, rows name only the other reef.
  expect(screen.getByRole("region", { name: /This topic depends on, 1 topic/ })).toBeInTheDocument();
  expect(screen.queryByRole("button", { name: "Unnamed node" })).not.toBeInTheDocument();
  fireEvent.click(screen.getByText(/Inspect relationship support/));
  const link = screen.getByRole("link", { name: /Retained version support 1 · earlier capture/ });
  expect(Object.fromEntries(new URL(link.getAttribute("href") || "", "https://example.test").searchParams)).toEqual({ graph_id: graph, source_item_id: item, version_id: version });
  expect(screen.getByRole("link", { name: /Legacy chunk support/ })).toHaveAttribute("href", expect.stringContaining(`/coral?id=${item}&graph_id=${graph}`));
  expect(screen.getByText(/Reviewer: reviewer/)).toBeInTheDocument();
  expect(screen.getByText(/Additional permitted support references/)).toBeInTheDocument();
  expect(screen.getByText(/Additional permitted edges/)).toBeInTheDocument();
  expect(screen.getByText(/does not establish that a relationship is valid now/)).toBeInTheDocument();
  fireEvent.click(screen.getByRole("button", { name: "Shipping" })); expect(onNode).toHaveBeenCalledWith(item, graph);
  fireEvent.click(screen.getByRole("button", { name: "Next connections" })); expect(onPage).toHaveBeenCalledWith("bound-next");
});
it("distinguishes an unknown legacy pointer flag from an earlier retained capture", async () => {
  const value = page(); value.items[0].support = [{ source_item_id: item, source_version_id: version }];
  vi.mocked(readLibraryNeighborhood).mockResolvedValue(value);
  render(<TopicConnections {...props} />); await screen.findByText("depends on");
  fireEvent.click(screen.getByText(/Inspect relationship support/));
  expect(screen.getByRole("link", { name: /capture pointer status unknown/ })).toBeInTheDocument();
  expect(screen.queryByRole("link", { name: /earlier capture/ })).not.toBeInTheDocument();
});
it("clears relationship payloads and invalidates the parent inspector when the scope is refused", async () => {
  const unavailable = vi.fn(); render(<TopicConnections {...props} onUnavailable={unavailable} />);
  await screen.findByText("depends on"); vi.mocked(readLibraryNeighborhood).mockRejectedValueOnce(new ApiError("Node access revoked", 403));
  act(() => returnToTab());
  expect(await screen.findByRole("alert")).toHaveTextContent("Node access revoked");
  expect(screen.queryByText("depends on")).not.toBeInTheDocument(); expect(unavailable).toHaveBeenCalledWith("Node access revoked");
});
it("invalidates the parent inspector when the account changes during the read", async () => {
  let token: string | null = "fixed"; const unavailable = vi.fn();
  vi.mocked(readLibraryNeighborhood).mockImplementationOnce(async () => { token = null; return page(); });
  render(<TopicConnections {...props} getToken={() => token} onUnavailable={unavailable} />);
  await waitFor(() => expect(unavailable).toHaveBeenCalledWith("The Library account changed. Retry connections."));
  expect(screen.queryByText("depends on")).not.toBeInTheDocument();
});
it.each([
  ["503", new ApiError("Library is unavailable right now. Retry when the connection is restored.", 503), "Library is unavailable right now."],
  ["timeout", new TransportError("timeout", "/v1/library/neighborhood", "request exceeded 15000ms"), "Library did not respond in time. Retry in a moment."],
  ["offline", new TransportError("network", "/v1/library/neighborhood", "Failed to fetch"), "Library could not be reached. Check the connection and retry."],
])("keeps this panel's verified rows with a note on %s and recovers on retry", async (_kind, failure, shown) => {
  const unavailable = vi.fn(); render(<TopicConnections {...props} onUnavailable={unavailable} />);
  await screen.findByText("depends on"); vi.mocked(readLibraryNeighborhood).mockRejectedValueOnce(failure);
  act(() => returnToTab());
  expect(await screen.findByRole("alert")).toHaveTextContent(shown);
  expect(screen.getByText("depends on")).toBeInTheDocument();
  expect(unavailable).not.toHaveBeenCalled();
  fireEvent.click(screen.getByRole("button", { name: "Retry connections" }));
  await waitFor(() => expect(screen.queryByRole("alert")).not.toBeInTheDocument());
  expect(screen.getByText("depends on")).toBeInTheDocument();
});
it("discards a late previous-scope relationship page", async () => {
  let resolve!: (value: LibraryNeighborhood) => void;
  vi.mocked(readLibraryNeighborhood).mockReturnValueOnce(new Promise((r) => { resolve = r; })).mockResolvedValueOnce({ ...page(), items: [], next_cursor: null, truncated: false });
  const view = render(<TopicConnections {...props} />);
  view.rerender(<TopicConnections {...props} identity="other-account" after="new-page" />);
  await screen.findByText("No supported typed connections returned");
  await act(async () => resolve(page())); expect(screen.queryByText("depends on")).not.toBeInTheDocument();
});

it("groups edges by predicate and direction, shows stated validity, and keeps ended edges as history", async () => {
  const a = "55555555-5555-4555-8555-555555555555", b = "66666666-6666-4666-8666-666666666666", c = "77777777-7777-4777-8777-777777777777";
  const base = page().items[0];
  vi.mocked(readLibraryNeighborhood).mockResolvedValue({ ...page(), next_cursor: null, truncated: false, items: [
    { ...base, edge_id: a, predicate: "works_on", src: { node_id: node, title: "Casey", type: "entity" }, dst: { node_id: item, title: "Atlas", type: "entity" }, valid_from: "2026-01-05T00:00:00Z", valid_until: "2026-03-01T00:00:00Z" },
    { ...base, edge_id: b, predicate: "works_on", src: { node_id: node, title: "Casey", type: "entity" }, dst: { node_id: a, title: "Beacon", type: "entity" }, valid_from: "2026-03-02T00:00:00Z", valid_until: null },
    { ...base, edge_id: c, predicate: "manages", src: { node_id: b, title: null, type: "entity" }, dst: { node_id: node, title: "Casey", type: "entity" } },
  ] });
  render(<TopicConnections {...props} />);
  const works = await screen.findByRole("region", { name: /This topic works on, 2 topics/ });
  const rows = works.querySelectorAll("li > div:first-child button");
  // Current (open-ended) first, ended validity after it.
  expect(Array.from(rows).map((r) => r.textContent)).toEqual(["Beacon", "Atlas"]);
  expect(screen.getByText(/since/)).toBeInTheDocument();
  expect(screen.getByText("ended")).toBeInTheDocument();
  const managed = screen.getByRole("region", { name: /manages this topic, from 1 topic/ });
  fireEvent.click(within(managed).getByRole("button", { name: "Unnamed node" }));
  expect(onNode).toHaveBeenCalledWith(b, graph);
});

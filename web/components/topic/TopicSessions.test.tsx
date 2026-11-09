// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import { cleanup, fireEvent, screen, waitFor } from "@testing-library/react";
import { render } from "@/lib/testing/queryRender";
import { afterEach, beforeEach, expect, it, vi } from "vitest";
import { TopicSessions } from "./TopicSessions";
import { ApiError } from "@/lib/api";
import { readLibraryNodeSessions, type LibraryNodeSessions as Page } from "@/lib/libraryHistoryApi";
vi.mock("@/lib/libraryHistoryApi", () => ({ readLibraryNodeSessions: vi.fn() }));
const graph = "11111111-1111-4111-8111-111111111111", node = "22222222-2222-4222-8222-222222222222", version = "44444444-4444-4444-8444-444444444444";
const scope = { graph_ids: [graph], node_id: node, outcome_versions: "current" as const };
const onPage = vi.fn(), onSettled = vi.fn();
const props = { identity: "account", getToken: () => "fixed", scope, nodeId: node, onPage, onSettled };
function page(): Page { return { scope, node_id: node, total: 2, items: [{ graph_id: graph, provider_namespace: "history:A", session_id: "fixture%_:session", title: null, version_count: 2, last_captured_at: "2026-10-11T12:00:00Z", latest_version_id: version }], next_cursor: "sessions-next", truncated: true }; }
beforeEach(() => { vi.mocked(readLibraryNodeSessions).mockResolvedValue(page()); });
afterEach(() => { cleanup(); vi.resetAllMocks(); });
it("lists supporting sessions as plain rows (no session page) with UUID-only focus ids and pages them", async () => {
  render(<TopicSessions {...props} after="sessions-page" />);
  const name = await screen.findByText("fixture%_:session");
  expect(name).toHaveAttribute("data-library-focus", `node-session:${version}`);
  expect(Array.from(document.querySelectorAll<HTMLElement>("[data-library-focus]")).every((e) => /^[a-z0-9:_-]+$/i.test(e.dataset.libraryFocus || "") && !e.dataset.libraryFocus?.includes("fixture"))).toBe(true);
  expect(screen.getByText(/2 supporting versions/)).toBeInTheDocument();
  expect(vi.mocked(readLibraryNodeSessions).mock.calls[0][1]).toMatchObject({ scope, node_id: node, page_size: 20, after: "sessions-page" });
  fireEvent.click(screen.getByRole("button", { name: "Next sessions" })); expect(onPage).toHaveBeenCalledWith("sessions-next");
  fireEvent.click(screen.getByRole("button", { name: "First sessions" })); expect(onPage).toHaveBeenCalledWith(null);
  expect(onSettled).toHaveBeenCalled();
});
it("shows the empty state and settles", async () => {
  vi.mocked(readLibraryNodeSessions).mockResolvedValue({ ...page(), total: 0, items: [], next_cursor: null, truncated: false });
  render(<TopicSessions {...props} />);
  expect(await screen.findByText("No captured session supports this node here")).toBeInTheDocument();
  expect(onSettled).toHaveBeenCalled();
});
it("settles on a refused read so Back restoration never waits on it", async () => {
  const unavailable = vi.fn();
  vi.mocked(readLibraryNodeSessions).mockRejectedValue(new ApiError("Node access revoked", 403));
  render(<TopicSessions {...props} onUnavailable={unavailable} />);
  expect(await screen.findByRole("alert")).toHaveTextContent("Node access revoked");
  await waitFor(() => expect(onSettled).toHaveBeenCalled());
  expect(unavailable).toHaveBeenCalledWith("Node access revoked");
});

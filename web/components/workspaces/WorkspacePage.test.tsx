// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import { act, cleanup, fireEvent, screen, waitFor } from "@testing-library/react";
import { render } from "@/lib/testing/queryRender";
import { afterEach, beforeEach, expect, it, vi } from "vitest";
import WorkspacesPage from "@/app/(app)/workspaces/page";
import { QueryClientProvider } from "@tanstack/react-query";
import { makeQueryClient } from "@/lib/queryClient";
import { api, ApiError, type Graph, type Reef } from "@/lib/api";

const wire = vi.hoisted(() => ({ read: vi.fn() }));
const state = vi.hoisted(() => ({ token: "token-a", accountId: "account-a", userId: "user-a", authReady: true, queryScope: "user-a::account-a",
  getAuthToken: () => state.token, graphs: [] as Graph[], graphId: "first" }));
vi.mock("@/lib/providers", () => ({ useApp: () => state }));
vi.mock("next/navigation", () => ({ useSearchParams: () => new URLSearchParams({ id: state.graphId }) }));
// Partial mock: the Home API clients extend the real `ApiError` at import time.
vi.mock("@/lib/api", async (importOriginal) => ({ ...(await importOriginal<typeof import("@/lib/api")>()), apiRequest: wire.read, api: { reefs: { list: vi.fn() } } }));
vi.mock("@/components/workspaces/TopicCard", () => ({ TopicCard: ({ reef }: { reef: Reef }) => <p>{reef.title}</p> }));
vi.mock("@/components/workspaces/WorkspaceIndex", () => ({ WorkspaceIndex: () => null }));
vi.mock("@/components/workspaces/members/WorkspaceMembersSummary", () => ({ WorkspaceMembersSummary: () => null }));
vi.mock("@/components/PromoteModal", () => ({ PromoteModal: () => null }));
const reefs = (title: string) => ({ model: "reef_page", version: 1, computed_at: "2026-10-31T00:00:00Z", stale: false, refresh_eta: null, payload: { nodes: [{ node_id: title, title, updated_at: "2026-10-16", type: "entity" }] as Reef[], has_more: false } });
beforeEach(() => {
  state.token = "token-a"; state.accountId = "account-a"; state.graphId = "first"; state.queryScope = "user-a::account-a";
  state.graphs = [{ id: "first", name: "First", kind: "personal" }, { id: "second", name: "Second", kind: "personal" }] as Graph[];
});
afterEach(() => { cleanup(); vi.resetAllMocks(); });
it("shows a retryable error rather than claiming a failed workspace is empty", async () => {
  wire.read.mockRejectedValue(new Error("timeout"));
  render(<WorkspacesPage />);
  expect(await screen.findByText("Couldn't load this workspace's topics.")).toBeInTheDocument();
  expect(screen.queryByText("Nothing here yet")).not.toBeInTheDocument();
  expect(screen.getByText("Topics unavailable")).toBeInTheDocument();
  wire.read.mockResolvedValue({ ...reefs(""), payload: { nodes: [], has_more: false } });
  fireEvent.click(screen.getByRole("button", { name: "Retry topics" }));
  expect(await screen.findByText("Nothing here yet")).toBeInTheDocument();
});
it("does not discard loaded topics on bearer rotation", async () => {
  wire.read.mockResolvedValue(reefs("Visible topic"));
  const view = render(<WorkspacesPage />);
  expect(await screen.findByText("Visible topic")).toBeInTheDocument();
  state.token = "token-b";
  view.rerender(<WorkspacesPage />);
  expect(wire.read).toHaveBeenCalledTimes(1);
  expect(api.reefs.list).not.toHaveBeenCalled();
  expect(screen.getByText("Visible topic")).toBeInTheDocument();
});
it("ignores late topics from a workspace after navigation", async () => {
  let resolve!: (value: ReturnType<typeof reefs>) => void;
  wire.read.mockReturnValueOnce(new Promise((r) => { resolve = r; }));
  const view = render(<WorkspacesPage />);
  await waitFor(() => expect(wire.read).toHaveBeenCalledTimes(1));
  state.graphId = "second";
  wire.read.mockResolvedValue(reefs("New topic"));
  view.rerender(<WorkspacesPage />);
  expect(wire.read.mock.calls[0][4]?.signal.aborted).toBe(true);
  expect(await screen.findByText("New topic")).toBeInTheDocument();
  await act(async () => resolve(reefs("Old private topic")));
  expect(screen.queryByText("Old private topic")).not.toBeInTheDocument();
});

it("does not resurrect paged private topics after refusal, failure and a newly certified page", async () => {
  const qc = makeQueryClient();
  const first = reefs("First topic"); first.payload.has_more = true;
  wire.read.mockResolvedValue(first);
  vi.mocked(api.reefs.list).mockResolvedValue({ data: [{ node_id: "paged", title: "Paged private topic", updated_at: "2026-10-16", type: "entity" } as Reef], hasMore: false });
  render(<QueryClientProvider client={qc}><WorkspacesPage /></QueryClientProvider>);
  fireEvent.click(await screen.findByRole("button", { name: /^show .* more/ }));
  await screen.findByText("Paged private topic");
  wire.read.mockRejectedValue(new ApiError("refused", 403));
  await act(async () => { await qc.invalidateQueries(); });
  await waitFor(() => expect(screen.queryByText("Paged private topic")).not.toBeInTheDocument());
  wire.read.mockRejectedValue(new ApiError("offline", 503));
  await act(async () => { await qc.invalidateQueries(); });
  expect(wire.read).toHaveBeenCalledTimes(3);
  expect(screen.queryByText("Paged private topic")).not.toBeInTheDocument();
  expect(screen.queryByText("Nothing here yet")).not.toBeInTheDocument();
  wire.read.mockResolvedValue({ ...reefs("New permitted topic"), computed_at: "2026-10-31T00:00:01Z" });
  await act(async () => { await qc.invalidateQueries(); });
  await screen.findByText("New permitted topic");
  expect(screen.queryByText("Paged private topic")).not.toBeInTheDocument();
});

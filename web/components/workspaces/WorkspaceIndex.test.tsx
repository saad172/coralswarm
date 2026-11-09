// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import { act, cleanup, screen, waitFor } from "@testing-library/react";
import { render } from "@/lib/testing/queryRender";
import { afterEach, beforeEach, expect, it, vi } from "vitest";
import { WorkspaceIndex } from "./WorkspaceIndex";
import type { Graph, OceanStats } from "@/lib/api";
import type { OceanComponent, OceansIndex } from "@/lib/oceansIndex";

const wire = vi.hoisted(() => ({ read: vi.fn() }));
const state = vi.hoisted(() => ({ token: "token-a", accountId: "account-a", authReady: true, queryScope: "user-a::account-a",
  getAuthToken: () => state.token, graphs: [] as Graph[], refreshGraphs: vi.fn() }));
vi.mock("@/lib/providers", () => ({ useApp: () => state }));
vi.mock("@/lib/api", async (original) => ({ ...await original<typeof import("@/lib/api")>(), apiRequest: wire.read }));
vi.mock("./NewWorkspace", () => ({ NewWorkspace: () => null }));
vi.mock("./ArchivedWorkspaces", () => ({ ArchivedWorkspaces: () => null }));
vi.mock("./WorkspaceCard", () => ({ WorkspaceCard: ({ graph, stats, loading, statsError, onRetryStats }: {
  graph: Graph; stats?: OceanStats; loading: boolean; statsError?: boolean; onRetryStats?: () => void;
}) => <section aria-label={graph.name}>{loading ? "loading" : statsError ? "unavailable" : stats?.atom_count}
  {statsError && <button type="button" onClick={onRetryStats}>Retry {graph.name}</button>}</section> }));
function deferred<T>() { let resolve!: (value: T) => void; const promise = new Promise<T>((r) => { resolve = r; }); return { promise, resolve }; }
const graph = (n: number) => ({ id: `11111111-1111-4111-8111-${String(n).padStart(12, "0")}`, name: n === 1 ? "Fast" : n === 2 ? "Slow" : `Foreign ${n}`, kind: "personal" } as Graph);
function component(n: number, count: number | null, owed = false): OceanComponent {
  return { account_id: `22222222-2222-4222-8222-${String(n).padStart(12, "0")}`, anchor_graph_id: graph(n).id,
    refresh_path: `/v1/load/oceans/component/refresh?graph=${graph(n).id}`, stored: {
      model: "oceans_index", version: 1, computed_at: count === null ? null : "2026-10-31T00:00:00Z", stale: owed || count === null,
      refresh_eta: owed || count === null ? "2026-10-31T00:00:02Z" : null,
      payload: { oceans: count === null ? [] : [{ graph: graph(n), counts: { atom_count: count, reef_total: 1, source_count: 2, reef_counts: {} } as OceanStats }], members: [], team_members: {}, archived: null },
    } };
}
beforeEach(() => { state.token = "token-a"; state.accountId = "account-a"; state.authReady = true; state.queryScope = "user-a::account-a"; state.graphs = [graph(1), graph(2)]; });
afterEach(() => { cleanup(); vi.resetAllMocks(); });
it("publishes a computed component while another certificate is pending", async () => {
  wire.read.mockResolvedValue({ components: [component(1, 27), component(2, null)] });
  render(<WorkspaceIndex />);
  expect(await screen.findByText("27")).toBeInTheDocument();
  expect(screen.queryByRole("region", { name: "Slow" })).toBeNull();
  expect(screen.queryByText("No shared workspaces")).toBeNull();
  expect(screen.getByText(/reading remaining workspaces/)).toBeInTheDocument();
  expect(wire.read).toHaveBeenCalledTimes(1);
});
it("reads more than six foreign accounts in one request and does not restart on bearer rotation", async () => {
  wire.read.mockResolvedValue({ components: Array.from({ length: 9 }, (_, n) => component(n + 1, n + 20)) });
  const view = render(<WorkspaceIndex />);
  expect(await screen.findAllByRole("region")).toHaveLength(9);
  state.token = "token-b"; view.rerender(<WorkspaceIndex />);
  expect(wire.read).toHaveBeenCalledTimes(1);
  expect(wire.read.mock.calls[0][0]).toBe("/v1/load/oceans");
});
it("keeps read failures distinct from computed zero", async () => {
  wire.read.mockRejectedValue(new Error("timeout"));
  render(<WorkspaceIndex />);
  expect(await screen.findByText("timeout")).toBeInTheDocument();
  expect(screen.queryByText("0")).toBeNull();
});
it("ignores an earlier account's answer even when graph ids match", async () => {
  const old = deferred<OceansIndex>(); wire.read.mockReturnValue(old.promise);
  const view = render(<WorkspaceIndex />);
  state.accountId = "account-b"; state.queryScope = "user-a::account-b";
  wire.read.mockResolvedValue({ components: [component(1, 9), component(2, 9)] }); view.rerender(<WorkspaceIndex />);
  await waitFor(() => expect(screen.getAllByText("9")).toHaveLength(2));
  await act(async () => { old.resolve({ components: [component(1, 77), component(2, 77)] }); });
  expect(screen.queryByText("77")).not.toBeInTheDocument();
});
it("keeps safe stale data and polls until every component resolves, removing revoked shares", async () => {
  wire.read.mockResolvedValueOnce({ components: [component(1, 3, true), component(2, null)] })
    .mockResolvedValue({ components: [component(1, 4)] });
  render(<WorkspaceIndex />);
  expect(await screen.findByText("3")).toBeInTheDocument();
  expect(screen.queryByRole("region", { name: "Slow" })).toBeNull();
  expect(screen.queryByText("No shared workspaces")).toBeNull();
  expect(screen.getByText(/reading remaining workspaces/)).toBeInTheDocument();
  expect(await screen.findByText("4", {}, { timeout: 3000 })).toBeInTheDocument();
  expect(screen.queryByRole("region", { name: "Slow" })).toBeNull();
  expect(wire.read).toHaveBeenCalledTimes(2);
});

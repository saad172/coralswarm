// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import { act, cleanup, fireEvent, screen, waitFor } from "@testing-library/react";
import { render } from "@/lib/testing/queryRender";
import { afterEach, beforeEach, expect, it, vi } from "vitest";
import { WorkspaceMembers } from "./WorkspaceMembers";
import { ArchivedWorkspaces } from "../ArchivedWorkspaces";
import { api, type ArchivedTeam, type Graph } from "@/lib/api";

/* Archiving a workspace sends the admin to /workspaces?archived=…, whose
   Archived section (the only place Restore lives) is a cached read. Within
   the staleness window it must still show the workspace just archived. */

const state = vi.hoisted(() => ({
  token: "token-a", authReady: true, queryScope: "user-a::personal", internalUserId: "me",
  getAuthToken: () => state.token, refreshGraphs: vi.fn(async () => {}), isPersonalOrg: false,
}));
vi.mock("@/lib/providers", () => ({ useApp: () => state }));
vi.mock("next/navigation", () => ({ useRouter: () => ({ push: vi.fn(), replace: vi.fn() }) }));
vi.mock("@/components/org/useOrgAdmin", () => ({ useOrgAdmin: () => ({ loaded: true, hasOrg: false, isAdmin: false }) }));
vi.mock("@/lib/api", async (original) => {
  const actual = await original<typeof import("@/lib/api")>();
  return {
    ...actual,
    apiRequest: (path: string, token: string) => {
      const url = new URL(path, "http://wire.test");
      if (url.pathname === "/v1/teams/archived") return api.teams.listArchived(token);
      if (url.pathname === "/v1/teams/t-1/members") return api.teams.roster(token, "t-1");
      if (url.pathname === "/v1/teams/t-1/events") return api.teams.events(token, "t-1", Number(url.searchParams.get("page") ?? 1), Number(url.searchParams.get("page_size") ?? 20));
      throw new Error(`Unexpected workspace read ${path}`);
    },
    api: {
      teams: {
        roster: vi.fn(),
        events: vi.fn(),
        archive: vi.fn(),
        listArchived: vi.fn(),
      },
    },
  };
});

const graph = { id: "g-team", name: "Design", kind: "team", team_id: "t-1", my_team_role: "admin" } as Graph;
const archivedRow: ArchivedTeam = {
  team_id: "t-1", graph_id: "g-team", name: "Design",
  archived_at: new Date().toISOString(), expires_at: new Date(Date.now() + 100 * 86_400_000).toISOString(), restorable: true,
};

beforeEach(() => {
  vi.mocked(api.teams.roster).mockResolvedValue([{ user_id: "me", name: "Me", email: "me@x", role: "admin", joined_at: "2026-10-01" }]);
  vi.mocked(api.teams.events).mockResolvedValue([]);
  vi.mocked(api.teams.archive).mockResolvedValue(undefined);
});
afterEach(() => { cleanup(); vi.resetAllMocks(); });

function Screen({ at }: { at: "index" | "members" }) {
  return at === "index" ? <ArchivedWorkspaces /> : <WorkspaceMembers graph={graph} autoIntent="archive" />;
}

it("shows the just-archived workspace in the Archived section on return to the index", async () => {
  // The index has already been viewed (or hover-prefetched): nothing archived.
  let archived: ArchivedTeam[] = [];
  vi.mocked(api.teams.listArchived).mockImplementation(async () => archived);
  const view = render(<Screen at="index" />);
  await waitFor(() => expect(screen.getByRole("button", { name: /archived/ })).toBeInTheDocument());

  // Archive from the workspace's Manage page.
  view.rerender(<Screen at="members" />);
  const confirm = await screen.findByRole("button", { name: "Archive workspace" });
  archived = [archivedRow];
  await act(async () => { fireEvent.click(confirm); });
  await waitFor(() => expect(api.teams.archive).toHaveBeenCalledWith("token-a", "t-1"));

  // Back on the index, well inside STALE.READ.
  view.rerender(<Screen at="index" />);
  fireEvent.click(await screen.findByRole("button", { name: /archived/ }));
  expect(await screen.findByText("Design")).toBeInTheDocument();
  expect(screen.getByRole("button", { name: "Restore" })).toBeInTheDocument();
});

import { describe, expect, it } from "vitest";
import type { Graph } from "./api";
import { kebabMenuItems, resolveManageAccess } from "./workspaceManage";

function graph(overrides: Partial<Graph> = {}): Graph {
  return {
    id: "ocean-11111111-1111-1111-1111-111111111111",
    name: "Engineering",
    kind: "team",
    is_primary: false,
    created_at: "2026-01-01T00:00:00Z",
    team_id: "team-22222222-2222-2222-2222-222222222222",
    my_team_role: null,
    ...overrides,
  };
}

const notOrgAdmin = { hasOrg: false, isAdmin: false };
const orgAdmin = { hasOrg: true, isAdmin: true };
const orgNonAdmin = { hasOrg: true, isAdmin: false };

describe("kebabMenuItems (workspace-card role matrix)", () => {
  it("team admin sees Manage members + Rename + Archive", () => {
    expect(kebabMenuItems(graph({ my_team_role: "admin" }), notOrgAdmin)).toEqual([
      "manage",
      "rename",
      "archive",
    ]);
  });

  it("team member sees Manage members + Leave — no Rename, no Archive", () => {
    expect(kebabMenuItems(graph({ my_team_role: "member" }), notOrgAdmin)).toEqual(["manage", "leave"]);
  });

  it("org admin sees Manage members + Rename + Archive even without an active membership row", () => {
    expect(kebabMenuItems(graph({ my_team_role: null }), orgAdmin)).toEqual(["manage", "rename", "archive"]);
  });

  it("org admin's own role still wins over a mere member role (admin outranks member)", () => {
    expect(kebabMenuItems(graph({ my_team_role: "member" }), orgAdmin)).toEqual([
      "manage",
      "rename",
      "archive",
    ]);
  });

  // Archive sits on the SAME branch as Rename (workspace-manage design doc:
  // team admin, owner, and org admin can all archive) even though a plain
  // team admin can neither see the archived list nor restore what they just
  // archived — that narrower gate lives server-side on `GET
  // /v1/teams/archived` (account-level authority), not here. This menu has no
  // way to express that asymmetry, which is exactly why the archive
  // confirmation dialog is required to state it before the user confirms
  // (see ArchiveWorkspaceModal).
  it("a plain team admin (not account owner/org admin) still gets Archive in this menu", () => {
    const items = kebabMenuItems(graph({ my_team_role: "admin" }), notOrgAdmin);
    expect(items).toContain("archive");
  });

  it("a non-admin org member with no team role at all gets no kebab", () => {
    expect(kebabMenuItems(graph({ my_team_role: null }), orgNonAdmin)).toBeNull();
  });

  it("neither a team member nor an org admin gets no kebab", () => {
    expect(kebabMenuItems(graph({ my_team_role: null }), notOrgAdmin)).toBeNull();
  });

  // The gate is `kind`, not "is shared" — WorkspaceIndex's "shared" section
  // also contains `kind === "org"`, which has no `teams` row and nothing to
  // manage here (see the module doc comment).
  it("a kind !== 'team' ocean never gets a kebab, even with team_id/my_team_role set as if it were one", () => {
    expect(kebabMenuItems(graph({ kind: "org", my_team_role: "admin" }), orgAdmin)).toBeNull();
    expect(kebabMenuItems(graph({ kind: "personal", my_team_role: "admin" }), orgAdmin)).toBeNull();
  });
});

describe("resolveManageAccess (the manage page's own guard)", () => {
  it("no graph (missing or unmatched ?id=) is 'not-found'", () => {
    expect(resolveManageAccess(null, notOrgAdmin)).toEqual({ status: "not-found" });
  });

  it("a real ocean that isn't kind 'team' is 'not-shared', never rendered as broken", () => {
    expect(resolveManageAccess(graph({ kind: "org" }), orgAdmin)).toEqual({ status: "not-shared" });
    expect(resolveManageAccess(graph({ kind: "personal", my_team_role: null }), notOrgAdmin)).toEqual({
      status: "not-shared",
    });
  });

  it("a team ocean missing its team_id is a distinct error state, not silently treated as unauthorized", () => {
    expect(resolveManageAccess(graph({ team_id: null, my_team_role: "admin" }), notOrgAdmin)).toEqual({
      status: "missing-team-id",
    });
  });

  it("team admin gets 'admin'", () => {
    expect(resolveManageAccess(graph({ my_team_role: "admin" }), notOrgAdmin)).toEqual({ status: "admin" });
  });

  it("org admin gets 'admin' even without an active membership row", () => {
    expect(resolveManageAccess(graph({ my_team_role: null }), orgAdmin)).toEqual({ status: "admin" });
  });

  it("team member (non-admin, non-org-admin) gets 'member'", () => {
    expect(resolveManageAccess(graph({ my_team_role: "member" }), notOrgAdmin)).toEqual({ status: "member" });
  });

  it("neither a member nor an org admin gets 'unauthorized', never the roster surface", () => {
    expect(resolveManageAccess(graph({ my_team_role: null }), notOrgAdmin)).toEqual({ status: "unauthorized" });
    expect(resolveManageAccess(graph({ my_team_role: null }), orgNonAdmin)).toEqual({ status: "unauthorized" });
  });

  it("missing-team-id is checked before role, so a broken team ocean never reads as merely unauthorized", () => {
    // my_team_role is null (would otherwise be "unauthorized") AND team_id is
    // null — the missing-team-id branch must win so the page never quietly
    // tells an admin-eligible viewer "you don't have access" for a data bug.
    expect(resolveManageAccess(graph({ team_id: null, my_team_role: null }), orgAdmin)).toEqual({
      status: "missing-team-id",
    });
  });
});

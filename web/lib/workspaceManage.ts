// Pure decision logic shared by the workspace-manage page
// (app/(app)/workspaces/manage/page.tsx) and the workspace-card kebab
// (components/workspaces/WorkspaceCard.tsx) — same rationale as
// lib/teamPanel.ts/lib/askScope.ts: this project's vitest setup runs a Node
// environment with no DOM/testing-library (see vitest.config.ts's own
// comment), so anything worth unit-testing hard has to be a plain function of
// plain data, not a mounted component. Both call sites answer the SAME
// underlying question — "is this viewer a team admin or an org admin for
// this ocean?" — off the SAME two inputs (`Graph.my_team_role`,
// `useOrgAdmin()`), so it lives here once rather than being re-derived
// (and risking drift) in each component.
//
// `my_team_role` is the CALLER's role straight off `GET /v1/graphs` (see its
// doc comment in lib/api.ts) — reading it costs nothing per card/page, unlike
// the roster fetch `WorkspaceMembers.tsx` still does once it actually renders
// (that fetch re-derives the same admin/member split from the freshly loaded
// roster as a belt-and-suspenders check; the two must always agree since both
// ultimately come from the same `team_members` table, but they are
// independent reads and neither should be deleted in favor of the other).
import type { Graph } from "./api";

/** The two fields of `useOrgAdmin()`'s return value this module needs — kept
 *  minimal so callers can pass the hook's result straight through. */
export interface OrgAdminInput {
  hasOrg: boolean;
  isAdmin: boolean;
}

/** True when the viewer is either this ocean's own team admin, or an org
 *  admin (Clerk org role, only meaningful with an active org) — exactly the
 *  "team admin or org admin" rule every mutating backend route in
 *  `backend/src/routes/teams.rs` enforces. An org admin qualifies even when
 *  `my_team_role` is `null` (not an active member of this particular team) —
 *  the design doc's "Owner / org admin (even if not a member)" row. */
export function isManagingViewer(
  graph: Pick<Graph, "my_team_role">,
  orgAdmin: OrgAdminInput,
): boolean {
  return graph.my_team_role === "admin" || (orgAdmin.hasOrg && orgAdmin.isAdmin);
}

// ── Kebab menu (workspace-manage design doc, "Entry point") ─────────────────

export type KebabAction = "manage" | "rename" | "archive" | "leave";

/** What the workspace-card kebab should show for `graph`, or `null` for no
 *  kebab at all.
 *
 *  Gate is `kind === "team"` — NOT "is shared" (`WorkspaceIndex` splits
 *  private/shared, and "shared" also contains `kind === "org"`, which has no
 *  `teams` row and nothing here to manage).
 *
 *  Archive is back (workspace-manage design doc, "Archive and restore"): it
 *  was pulled from this matrix for one pass while restore was unreachable
 *  from the product (offering it would have been a dead end), and returns now
 *  that `GET /v1/teams/archived` + restore both ship alongside it. It sits on
 *  the SAME branch as Rename — `isManagingViewer` (team admin OR org admin)
 *  is exactly who the backend's `archive_team` handler authorizes — even
 *  though that same viewer, if they are "only" a team admin, can neither SEE
 *  the archived list nor restore what they just archived (a narrower,
 *  ACCOUNT-level gate — see `ArchivedTeam`'s doc comment in lib/api.ts). That
 *  asymmetry is deliberate, not a bug in this matrix: the archive
 *  confirmation (`ArchiveWorkspaceModal`) is what tells a team admin so,
 *  since this menu has no way to.
 *
 *  Every branch here must correspond to an item the viewer's role can
 *  actually use without 403ing — this is the single source of truth both the
 *  kebab AND its role-matrix tests read, so a branch that would 403 can't
 *  silently reappear in one without the other catching it. */
export function kebabMenuItems(
  graph: Pick<Graph, "kind" | "my_team_role">,
  orgAdmin: OrgAdminInput,
): KebabAction[] | null {
  if (graph.kind !== "team") return null;
  if (isManagingViewer(graph, orgAdmin)) return ["manage", "rename", "archive"];
  if (graph.my_team_role === "member") return ["manage", "leave"];
  // Neither an active member nor an org admin — same "no kebab" case
  // `resolveManageAccess` calls "unauthorized" for the page itself.
  return null;
}

// ── The manage page's own access guard ───────────────────────────────────────

export type ManageAccess =
  /** No `?id=`, or an id that matches nothing reachable (stale link, lost
   *  access). Distinct from `/workspaces` (no id): THIS route requires one. */
  | { status: "not-found" }
  /** A real, reachable ocean, but not `kind === "team"` — nothing to manage. */
  | { status: "not-shared" }
  /** `kind === "team"` but `team_id` is `null` — real, if unexpected (see
   *  `Graph.team_id`'s doc comment in lib/api.ts). Never fire a
   *  `/v1/teams/{team_id}/...` request in this state. */
  | { status: "missing-team-id" }
  /** Reachable team ocean, but the viewer is neither an active member nor an
   *  org admin. The roster/events endpoints would 403 them — refuse before
   *  ever asking, rather than rendering a page that immediately errors. */
  | { status: "unauthorized" }
  /** Active team member, not an admin — read-only roster + Leave, no history. */
  | { status: "member" }
  /** Team admin, or org admin (even if not an active member) — full controls. */
  | { status: "admin" };

/** Decide what `/workspaces/manage?id=` should render for `graph` (already
 *  resolved from `graphs.find(g => g.id === idParam)`, or `null` for a
 *  missing/unmatched id) and the viewer's org-admin state. Ordered exactly as
 *  the guards in the workspace-manage design brief: not-found, then
 *  not-shared, then missing-team-id, then role. */
export function resolveManageAccess(graph: Graph | null, orgAdmin: OrgAdminInput): ManageAccess {
  if (!graph) return { status: "not-found" };
  if (graph.kind !== "team") return { status: "not-shared" };
  if (!graph.team_id) return { status: "missing-team-id" };
  if (isManagingViewer(graph, orgAdmin)) return { status: "admin" };
  if (graph.my_team_role === "member") return { status: "member" };
  return { status: "unauthorized" };
}

// Single chokepoint that resolves the id a `/v1/teams/*` route (`api.teams.*`)
// actually expects from an ocean (`Graph`) — DISTINCT from `graph.id`
// (`graphs.id`, what every OTHER route calls the ocean id). `teams.id` and
// `graphs.id` are two separate primary keys
// (`backend/migrations_control/0022_teams.sql:35-42`); before `Graph.team_id`
// existed, every workspace-members call site read `graph.id` because that
// was the only id it had, and every `/v1/teams/{team_id}/...` route 404'd
// "team not found" for a workspace that plainly existed — see `Graph.team_id`'s
// doc comment in `lib/api.ts` for the full bug writeup.
//
// Pulled out as its own pure function (same reasoning as `lib/teamEvents.ts`/
// `lib/teamPanel.ts`/`lib/askScope.ts`: this project's vitest setup runs a
// Node environment with no DOM, so anything worth unit-testing hard has to be
// a plain function of plain data, not a mounted component) so every
// workspace-members call site (`WorkspaceMembers.tsx`, `MemberRow.tsx`,
// `AddTeamMemberModal.tsx`, `ArchiveWorkspaceModal.tsx`) goes through ONE
// tested chokepoint instead of independently reading `graph.team_id` (or,
// worse, `graph.id`) inline six different times.
import type { Graph } from "./api";

/** Resolve the `teams.id` a `/v1/teams/{team_id}/...` call needs from an
 *  ocean. Returns `graph.team_id`, NEVER `graph.id` — the two are different
 *  primary keys, and returning the wrong one is exactly the bug this
 *  function exists to make impossible to reintroduce by accident.
 *
 *  `null` is a REAL, if unexpected, answer (not an error thrown here): every
 *  ocean whose `kind !== "team"` has no team id at all, and — defensively —
 *  a `kind === "team"` ocean's `teams` row could in principle be missing.
 *  Callers must treat `null` as an error state and never splice it into a
 *  `/v1/teams/{team_id}/...` URL (`api.teams.*` would happily stringify
 *  `null` into the literal path segment `"null"`, which is worse than the
 *  bug this whole fix closes — a silently WRONG request instead of a loudly
 *  missing one). */
export function resolveTeamId(graph: Pick<Graph, "id" | "team_id">): string | null {
  return graph.team_id;
}

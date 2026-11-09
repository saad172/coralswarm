// Regression guard for the "workspace Members panel shows 'team not found'
// for every shared workspace" production bug: every workspace-members call
// site used to send `graph.id` (the OCEAN id, `graphs.id`) to
// `/v1/teams/{team_id}/...` routes that are keyed on `teams.id` — a
// different primary key (`backend/migrations_control/0022_teams.sql:
// 35-42`). This file proves `resolveTeamId` — the one chokepoint every
// workspace-members call site (`WorkspaceMembers.tsx`, `MemberRow.tsx`,
// `AddTeamMemberModal.tsx`, `ArchiveWorkspaceModal.tsx`) now goes through
// before calling `api.teams.*` — always resolves the TEAM id, never falls
// back to the ocean id, and never lets `null` through disguised as a string.
import { describe, expect, it } from "vitest";
import type { Graph } from "./api";
import { resolveTeamId } from "./teamId";

/** Deliberately gives `id` and `team_id` DIFFERENT, easy-to-tell-apart
 *  values (rather than reusing the same uuid-shaped string for both, which
 *  would let a regression that swaps them back to `graph.id` slip past
 *  unnoticed) — this is the fixture shape that actually catches the bug:
 *  any call site (or this function) that reads `graph.id` instead of
 *  `graph.team_id` fails these assertions immediately. */
function teamGraph(overrides: Partial<Graph> = {}): Graph {
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

describe("resolveTeamId", () => {
  it("returns the ocean's team_id, not its id — the exact bug this closes", () => {
    const graph = teamGraph();
    const resolved = resolveTeamId(graph);
    expect(resolved).toBe(graph.team_id);
    expect(resolved).not.toBe(graph.id);
    // Belt-and-braces against a future fixture edit accidentally making the
    // two equal again and silently defeating this test's whole point.
    expect(graph.id).not.toBe(graph.team_id);
  });

  it("returns null for a non-team ocean (never falls back to graph.id)", () => {
    const personal = teamGraph({ kind: "personal", team_id: null });
    expect(resolveTeamId(personal)).toBeNull();
    expect(resolveTeamId(personal)).not.toBe(personal.id);
  });

  it("returns null for a kind='team' ocean whose teams row is missing (defensive)", () => {
    // Real, if unexpected — see `Graph.team_id`'s doc comment in `lib/api.ts`.
    // Callers must treat this exactly like the non-team case above: refuse
    // to call `/v1/teams/{team_id}/...`, never coerce `null` into a string.
    const orphaned = teamGraph({ team_id: null });
    expect(resolveTeamId(orphaned)).toBeNull();
  });
});

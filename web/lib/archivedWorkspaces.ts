// Pure presentation logic for the archived-workspaces section
// (`ArchivedWorkspaces.tsx`, on `/workspaces`, `GET /v1/teams/archived`) —
// same rationale as lib/workspaceManage.ts/lib/teamId.ts et al.: this
// project's vitest setup runs a Node environment with no DOM (see
// vitest.config.ts's own comment), so anything worth unit-testing hard has to
// be a plain function of plain data, not a mounted component.
//
// THE ONE RULE THIS FILE EXISTS TO ENFORCE: the 120-day restore window is a
// SINGLE constant, owned entirely by the backend
// (`TEAM_OCEAN_RESTORE_WINDOW_DAYS` / `team_restore_window`,
// backend/src/routes/teams.rs) and computed fresh on every request into
// `expires_at`/`restorable`. Nothing here may reintroduce that constant or
// otherwise decide restorability itself — a client-side copy of "120" is
// exactly the kind of drift that would let a countdown here and the server's
// own 409 refusal disagree. `restorable` is always read verbatim from the
// server; the only arithmetic in this module turns the server's own
// `expires_at` into a day count for display, which is a rendering concern,
// not a second copy of the window.
import type { ArchivedTeam } from "./api";

export type ArchivedRowStatus =
  | { status: "restorable"; daysLeft: number }
  | { status: "expired" };

/** Classify one archived-team row for display. `now` is a parameter (not
 *  `new Date()` inline) purely so tests can pin it — production call sites
 *  never pass it. */
export function archivedRowStatus(
  row: Pick<ArchivedTeam, "restorable" | "expires_at">,
  now: Date = new Date(),
): ArchivedRowStatus {
  // The server's own verdict wins outright, checked FIRST and unconditionally
  // — never recomputed from `expires_at` here, so a row the server has
  // already marked expired can never read as restorable even if `expires_at`
  // itself is stale, malformed, or (clock skew) still in the future.
  if (!row.restorable) return { status: "expired" };
  const msLeft = new Date(row.expires_at).getTime() - now.getTime();
  // Ceiling, not floor/round: with (say) 0.4 days genuinely left, "0 days
  // left" reads as already gone, which would contradict the server's own
  // `restorable: true` for this row. Clamped at 0 so client/server clock
  // skew can never show a negative count for a row the server still calls
  // restorable.
  const daysLeft = Math.max(0, Math.ceil(msLeft / 86_400_000));
  return { status: "restorable", daysLeft };
}

/** "3 days left" / "1 day left" — singular only at exactly 1. */
export function daysLeftLabel(daysLeft: number): string {
  return `${daysLeft} day${daysLeft === 1 ? "" : "s"} left`;
}

// Pure humanizing logic for a team ocean's Members surface — "recent
// changes" strip (`GET /v1/teams/{team_id}/events`) and the roster's identity
// labels. Kept separate from the rendering component (`components/workspaces/
// members/TeamActivityStrip.tsx`) for the same reason `lib/teamPanel.ts`/
// `lib/askScope.ts` split their decision logic out: this project's vitest
// setup runs a Node environment with no DOM (see `lib/teamPanel.ts`'s own
// comment), so anything worth unit-testing has to be a plain function of
// plain data, not a mounted component.

import type { MembershipEvent, TeamMemberRow, TeamRole } from "./api";

/** Display label for one user id inside a team's roster/event history.
 *
 *  - The caller's own id renders "You" — matches the roster row's own
 *    "you" marker, so the same identity reads identically in both places.
 *  - An id still on the active roster renders `name ?? email`.
 *  - An id that has since departed (a `removed`/`cascade_removed` target from
 *    before this fetch, or an actor who has left since) is not in the active
 *    roster at all — the roster endpoint only returns ACTIVE members — so it
 *    falls back to "a former member", the same phrase other org surfaces
 *    use for the identical "resolvable id, but the person is gone" case, so
 *    the surfaces agree on vocabulary. */
export function personLabel(userId: string, roster: TeamMemberRow[], selfId: string | null): string {
  if (selfId && userId === selfId) return "You";
  const row = roster.find((m) => m.user_id === userId);
  return row?.name || row?.email || "a former member";
}

const ROLE_WORD: Record<TeamRole, string> = { admin: "admin", member: "member" };

/** Word for a role's value inside a sentence — indexes `ROLE_WORD` but never
 *  produces the literal string "undefined": a role value outside the two
 *  this frontend's `TeamRole` type knows about (a role the backend added
 *  that this build hasn't caught up to yet, or simply malformed data) falls
 *  back to the raw value itself. An audit trail is exactly the place a
 *  fabricated-looking "undefined" is least acceptable — the raw value, while
 *  unfamiliar, is still an honest answer to "what role". */
function roleWord(role: TeamRole): string {
  return ROLE_WORD[role] ?? role;
}

/** One line of human-readable history for a `membership_events` row, e.g.
 *  "Alex promoted Jo to admin" or "You left". Never throws on an
 *  incomplete/unexpected shape — a membership audit trail is exactly the
 *  place a rendering crash is least acceptable, so every branch below has a
 *  safe fallback.
 *
 *  `actor_user_id === null` is the automated offboard cascade (an org member
 *  revoked elsewhere costing this team a member as a side effect) — rendered
 *  as an explicit system action, never as a blank actor or a crash. */
export function describeMembershipEvent(e: MembershipEvent, roster: TeamMemberRow[], selfId: string | null): string {
  const target = personLabel(e.target_user_id, roster, selfId);

  if (e.actor_user_id === null) {
    return `${target} was automatically removed — offboarded from the organization`;
  }

  const actor = personLabel(e.actor_user_id, roster, selfId);
  const selfRemoval = e.actor_user_id === e.target_user_id;

  switch (e.action) {
    case "created":
      return `${actor} created this workspace`;
    case "added":
      return e.new_role ? `${actor} added ${target} as ${roleWord(e.new_role)}` : `${actor} added ${target}`;
    case "removed":
      return selfRemoval ? `${actor} left` : `${actor} removed ${target}`;
    case "role_changed":
      if (e.old_role === "member" && e.new_role === "admin") return `${actor} promoted ${target} to admin`;
      if (e.old_role === "admin" && e.new_role === "member") return `${actor} demoted ${target} to member`;
      return e.new_role
        ? `${actor} changed ${target}'s role to ${roleWord(e.new_role)}`
        : `${actor} changed ${target}'s role`;
    case "cascade_removed":
      // The cascade's own actor is always null in practice (see above) — this
      // arm only exists so an unexpected non-null actor still reads sensibly
      // rather than falling through to a `default` this switch doesn't have.
      return selfRemoval ? `${actor} left` : `${actor} removed ${target}`;
    case "left":
      // Distinct from `removed` on purpose (see `MembershipEventAction`): a
      // self-departure and an admin's removal are different facts, and the
      // audit trail keeps them apart.
      return `${actor} left`;
    // ── Workspace-level, not person-level ────────────────────────────────
    // These three carry `target_user_id === actor_user_id` because there is
    // no second party, so they must NOT be phrased as "X did something to Y".
    // Falling through to the default below would have produced the actively
    // wrong "X changed X's membership" for an archive.
    case "archived":
      return `${actor} archived this workspace`;
    case "restored":
      return `${actor} restored this workspace`;
    case "renamed":
      return `${actor} renamed this workspace`;
    default:
      return `${actor} changed ${target}'s membership`;
  }
}

/** Body copy for `MemberRow`'s "Leave this workspace?" dialog — split out
 *  here for the same reason `describeMembershipEvent` is (a plain function of
 *  plain data this project's Node-env vitest setup can actually exercise; see
 *  this file's top-of-file comment). The dialog's confirm button also
 *  disables itself for a sole admin (`MemberRow`'s `isSelfSoleAdmin`) — that
 *  part is plain JSX, not worth extracting, but reads next to this string so
 *  the two never drift apart.
 *
 *  A sole admin's OWN leave attempt is guaranteed to fail server-side (the
 *  last-admin guard, 409 "cannot remove the last admin — promote another
 *  member first") — the client already knows this from the same
 *  `isSoleAdmin` the roster's "only admin" badge renders from, so the dialog
 *  says so up front, rather than letting the click land on a guaranteed
 *  error. Every other leave (not the sole admin) keeps the original copy
 *  unchanged. */
export function leaveDialogDescription(isSoleAdmin: boolean): string {
  return isSoleAdmin
    ? "You're the only admin. Promote another member to admin before you can leave."
    : "You'll lose access to its topics, items, and sources right away. Another admin can add you back later.";
}

/** Compact relative time ("2h ago") for the activity strip — provenance is
 *  still exact underneath (render the ISO timestamp as a `title` tooltip
 *  alongside this), so nothing here trades away the auditable timestamp,
 *  only compresses its headline display. `now` is injectable for tests,
 *  mirroring `lib/journal.ts`'s `now: Date = new Date()` convention. */
export function timeAgo(iso: string, now: Date = new Date()): string {
  const then = new Date(iso).getTime();
  if (Number.isNaN(then)) return "";
  const diffSec = Math.max(0, Math.round((now.getTime() - then) / 1000));
  if (diffSec < 60) return "just now";
  const min = Math.round(diffSec / 60);
  if (min < 60) return `${min}m ago`;
  const hr = Math.round(min / 60);
  if (hr < 24) return `${hr}h ago`;
  const day = Math.round(hr / 24);
  if (day < 30) return `${day}d ago`;
  const mo = Math.round(day / 30);
  if (mo < 12) return `${mo}mo ago`;
  const yr = Math.round(mo / 12);
  return `${yr}y ago`;
}

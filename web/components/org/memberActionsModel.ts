import { ApiError, type OrgDirectoryMember } from "@/lib/api";

/* Pure rules and copy for the per-member Remove / Change role controls on
   Organization -> Members. The backend (settings 2.2, `org_members.rs`) is the
   real guard; this decides what to SHOW. */

export function isAdminRole(role: string): boolean {
  return /admin|owner/i.test(role);
}

/** The roster marks the owner with `is_owner` on admin-shaped rows (the real
 *  record is `accounts.owner_user_id`); a literal `owner` role counts too. The
 *  backend still refuses with `cannot_remove_owner` /
 *  `cannot_change_owner_role` if a row slips through. */
export function isOwnerRow(m: Pick<OrgDirectoryMember, "role" | "is_owner">): boolean {
  return m.is_owner === true || /owner/i.test(m.role);
}

/** Is the signed-in person an admin? Read from their own roster row, which
 *  carries the live database role the server checks on every request. */
export function callerIsAdmin(roster: OrgDirectoryMember[] | null, selfId: string | null): boolean {
  if (!roster || !selfId) return false;
  const me = roster.find((m) => m.user_id === selfId);
  return !!me && isAdminRole(me.role);
}

/** Admins get controls on other people's rows, never their own or the owner's. */
export function canManageMember(
  m: OrgDirectoryMember,
  opts: { callerIsAdmin: boolean; selfId: string | null },
): boolean {
  return opts.callerIsAdmin && m.user_id !== opts.selfId && !isOwnerRow(m);
}

const COPY: Record<string, string> = {
  cannot_remove_self: "You can't remove yourself. Ask another admin to do it.",
  cannot_change_own_role: "You can't change your own role. Ask another admin to do it.",
  cannot_remove_owner: "The organization owner can't be removed.",
  cannot_change_owner_role: "The organization owner's role can't be changed.",
  invalid_role: "That isn't a role you can pick. Choose admin or member.",
  member_not_found: "That person is no longer a member. The list has been refreshed.",
  last_admin: "An organization needs at least one admin. Make someone else an admin first.",
  clerk_error: "The sign-in service is briefly unavailable. Try again in a moment.",
  clerk_misconfigured: "The sign-in service is briefly unavailable. Try again in a moment.",
  local_cleanup_failed: "Something went wrong finishing up. Nothing is lost, and it's safe to try again.",
};

export function mapMemberActionError(e: unknown, verb: "remove" | "change"): string {
  if (e instanceof ApiError) {
    const known = COPY[e.message];
    if (known) return known;
    if (e.status === 403) return "Only an admin can do this.";
    if (e.serverMessage) return e.serverMessage;
  }
  return verb === "remove" ? "Couldn't remove that member. Try again." : "Couldn't change that role. Try again.";
}

/** Error codes after which the roster is stale and worth refetching. */
export function staleAfter(e: unknown): boolean {
  return e instanceof ApiError && (e.message === "member_not_found" || e.message === "local_cleanup_failed");
}

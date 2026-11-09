"use client";

import { useEffect, useRef, useState } from "react";
import { useApp } from "@/lib/providers";
import { api, ApiError, type TeamMemberRow, type TeamRole } from "@/lib/api";
import { leaveDialogDescription } from "@/lib/teamEvents";
import { Avatar, Badge, Button, ErrorNote, hueForName, Modal, Select } from "@/components/ui";

/** One roster row: identity, role control, and remove/leave — the mutating
 * unit of the Members surface. Owns its own network calls (role change,
 * remove/leave) so a slow or failing mutation on one row never blocks the
 * rest of the roster; `onChanged` re-fetches the parent's roster (and, for a
 * role change, the events strip) after a successful write. */
export function MemberRow({
  member,
  teamId,
  isSelf,
  canManage,
  isSoleAdmin,
  isCreator,
  autoOpenConfirm,
  onChanged,
}: {
  member: TeamMemberRow;
  /** `teams.id` for the workspace this row belongs to — DISTINCT from the
   *  ocean id (`graphs.id`), which is what `graphId` used to (wrongly) hold
   *  here. Every `/v1/teams/{team_id}/...` mutation below needs THIS id, not
   *  the ocean's — see `Graph.team_id`'s doc comment in `lib/api.ts` for the
   *  bug this naming fix closes. `null` only in the real-but-unexpected case
   *  the parent's own doc comment describes (a `kind === "team"` ocean
   *  missing its `teams` row) — every handler below refuses to fire a
   *  request rather than splice `null` into a URL. */
  teamId: string | null;
  isSelf: boolean;
  /** Team admin or org admin — see `WorkspaceMembers`'s gating. Non-admins get
   *  a read-only role badge and no remove control on anyone else's row. */
  canManage: boolean;
  /** This member is the team's only admin. Informational (the "only admin"
   *  badge) on every row; on the caller's OWN row (`isSelf`) it additionally
   *  disables the leave confirm — see `isSelfSoleAdmin` below — since the
   *  backend's last-admin guard (409) would otherwise fire on every attempt.
   *  For anyone else's row it stays purely informational: only an admin can
   *  remove another member, and removing a non-viewer admin never trips the
   *  same guard the way the viewer's own departure would. */
  isSoleAdmin: boolean;
  /** Derived from the audit trail's `created` event, when the caller could
   *  load it (admins only) — omitted entirely rather than guessed when it
   *  isn't derivable. */
  isCreator: boolean;
  /** Carries the workspace-card kebab's `intent=leave` deep link (see
   *  `WorkspaceMembers`'s doc comment) — opens THIS row's leave confirmation
   *  once on mount. The parent only ever sets this `true` for the caller's
   *  own row; still re-checked against `isSelf` here so a copy-pasted call
   *  site can never open a REMOVE confirmation for somebody else by mistake. */
  autoOpenConfirm?: boolean;
  /** `selfLeft: true` distinguishes the caller leaving this workspace
   *  themselves from any other roster mutation — the parent must reload the
   *  workspace list (the caller no longer has access to this ocean's roster
   *  at all) rather than re-fetch a roster it can no longer read. */
  onChanged: (info?: { selfLeft?: boolean }) => void;
}) {
  const { getAuthToken, authReady } = useApp();

  // Local mirror of the server role, for the optimistic update — reset
  // whenever the parent hands down a fresh roster row (a real change from
  // elsewhere, or this row's own successful mutation coming back around).
  const [role, setRole] = useState<TeamRole>(member.role);
  useEffect(() => setRole(member.role), [member.role]);
  const [roleBusy, setRoleBusy] = useState(false);
  const [roleErr, setRoleErr] = useState<string | null>(null);

  const [confirmOpen, setConfirmOpen] = useState(false);
  const [removeBusy, setRemoveBusy] = useState(false);
  const [removeErr, setRemoveErr] = useState<string | null>(null);

  // `autoOpenConfirm` (intent=leave deep link) fires at most ONCE — without
  // the guard, this row's own re-renders (a role change elsewhere in the
  // roster, a parent poll) would re-open the confirmation right after the
  // caller dismissed it.
  const firedAutoOpen = useRef(false);
  useEffect(() => {
    if (!autoOpenConfirm || !isSelf || firedAutoOpen.current) return;
    firedAutoOpen.current = true;
    setConfirmOpen(true);
  }, [autoOpenConfirm, isSelf]);

  const displayName = member.name || member.email;

  async function changeRole(next: TeamRole) {
    const token = authReady ? getAuthToken() : null;
    if (!token || next === role || roleBusy) return;
    if (!teamId) {
      setRoleErr("This workspace is missing its team id — couldn't change that role.");
      return;
    }
    const prev = role;
    setRole(next); // optimistic
    setRoleBusy(true);
    setRoleErr(null);
    try {
      await api.teams.updateMemberRole(token, teamId, member.user_id, next);
      onChanged();
    } catch (e) {
      // Roll back visibly and explain — a 409 last-admin guard reads as real
      // guidance ("assign another admin first"), not a raw error blob.
      setRole(prev);
      setRoleErr(e instanceof ApiError ? e.message : "Couldn't change that role.");
    } finally {
      setRoleBusy(false);
    }
  }

  // Self-leave, specifically, is blocked client-side when the viewer is the
  // team's only admin — see the Modal's `footer` below and its doc comment.
  // A non-self remove is never blocked this way: `isSoleAdmin` there
  // describes the row being acted on (which may or may not be an admin at
  // all), not the viewer, and only a same-workspace admin can remove someone
  // else in the first place.
  const isSelfSoleAdmin = isSelf && isSoleAdmin;

  async function confirmRemove() {
    const token = authReady ? getAuthToken() : null;
    if (!token || removeBusy || isSelfSoleAdmin) return;
    if (!teamId) {
      setRemoveErr(
        `This workspace is missing its team id — couldn't ${isSelf ? "leave it" : "remove that member"}.`,
      );
      return;
    }
    setRemoveBusy(true);
    setRemoveErr(null);
    try {
      if (isSelf) await api.teams.leave(token, teamId, member.user_id);
      else await api.teams.removeMember(token, teamId, member.user_id);
      setConfirmOpen(false);
      onChanged({ selfLeft: isSelf });
    } catch (e) {
      setRemoveErr(
        e instanceof ApiError
          ? e.message
          : isSelf
            ? "Couldn't leave this workspace."
            : "Couldn't remove this member.",
      );
    } finally {
      setRemoveBusy(false);
    }
  }

  const canRemoveOthers = canManage && !isSelf;
  // Leaving is never admin-gated — any active member can attempt it; the
  // backend's own 409 is the only thing that ever blocks it (sole admin).
  const canLeave = isSelf;

  return (
    <div className="flex flex-wrap items-center gap-x-[10px] gap-y-[8px] py-[10px]">
      <Avatar name={displayName} hue={hueForName(displayName)} size={28} />

      <div className="min-w-0 flex-1">
        <div className="flex flex-wrap items-center gap-[6px]">
          <span className="truncate text-[13px] font-medium text-ink">{displayName}</span>
          {isSelf && (
            <Badge tone="ink" outline>
              you
            </Badge>
          )}
          {isCreator && (
            <span className="font-mono text-[9.5px] uppercase tracking-[0.1em] text-ink-3">
              created this workspace
            </span>
          )}
          {isSoleAdmin && (
            <span className="font-mono text-[9.5px] uppercase tracking-[0.1em] text-warn">only admin</span>
          )}
        </div>
        <div className="mt-[2px] font-mono text-[10px] text-ink-3">
          {member.name ? `${member.email} · ` : ""}joined{" "}
          {new Date(member.joined_at).toLocaleDateString(undefined, { month: "short", day: "numeric", year: "numeric" })}
        </div>
        {roleErr && <p className="mt-[4px] max-w-[46ch] text-[11.5px] leading-[1.5] text-bad">{roleErr}</p>}
      </div>

      {canManage ? (
        <div className="w-[112px] flex-none">
          <Select
            aria-label={`${displayName}'s role`}
            value={role}
            disabled={roleBusy}
            onChange={(e) => void changeRole(e.target.value as TeamRole)}
          >
            <option value="admin">Admin</option>
            <option value="member">Member</option>
          </Select>
        </div>
      ) : (
        <Badge tone={role === "admin" ? "brand" : "ink"} outline>
          {role}
        </Badge>
      )}

      {(canRemoveOthers || canLeave) && (
        <Button size="sm" variant="ghost" className="flex-none text-bad hover:bg-bad/10" onClick={() => setConfirmOpen(true)}>
          {isSelf ? "Leave" : "Remove"}
        </Button>
      )}

      <Modal
        open={confirmOpen}
        onClose={() => !removeBusy && setConfirmOpen(false)}
        title={isSelf ? "Leave this workspace?" : `Remove ${displayName}?`}
        description={
          isSelf
            ? leaveDialogDescription(isSelfSoleAdmin)
            : `${displayName} will lose access to its topics, items, and sources right away. They can be added back later.`
        }
        footer={
          <>
            <Button size="sm" onClick={() => setConfirmOpen(false)} disabled={removeBusy}>
              Cancel
            </Button>
            {/* Sole-admin self-leave: the confirm button is disabled rather
             * than removed, so its own disabled state is the visible signal
             * that leaving isn't possible right now. Clicking it would
             * otherwise always hit the backend's last-admin guard (409
             * "cannot remove the last admin — promote another member
             * first") — the client already knows this from the same
             * `isSoleAdmin` the roster's "only admin" badge renders from,
             * so don't let the user click into a guaranteed error. */}
            <Button
              size="sm"
              variant="danger"
              onClick={() => void confirmRemove()}
              disabled={removeBusy || isSelfSoleAdmin}
            >
              {removeBusy ? "…" : isSelf ? "Leave workspace" : "Remove"}
            </Button>
          </>
        }
      >
        {removeErr && <ErrorNote>{removeErr}</ErrorNote>}
      </Modal>
    </div>
  );
}

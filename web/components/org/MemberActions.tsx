"use client";

import { useState } from "react";
import { ConfirmDialog } from "@/components/ConfirmDialog";
import { Button } from "@/components/ui";
import { useApp } from "@/lib/providers";
import { api, type OrgDirectoryMember } from "@/lib/api";
import { isAdminRole, mapMemberActionError, staleAfter } from "./memberActionsModel";

type Pending = "remove" | "role" | null;

/** Remove and Change role for one other member. Render only when
 *  `canManageMember` says so; each action asks first and the dialog stays
 *  open, showing the reason, if the server refuses. */
export function MemberActions({
  member,
  onChanged,
}: {
  member: OrgDirectoryMember;
  /** Called after a success, or when the list is known to be stale. */
  onChanged: () => void;
}) {
  const { getAuthToken } = useApp();
  const [pending, setPending] = useState<Pending>(null);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const who = member.name ?? member.email;
  const makeAdmin = !isAdminRole(member.role);
  const nextRole: "admin" | "member" = makeAdmin ? "admin" : "member";

  function close() {
    setPending(null);
    setError(null);
  }

  async function run(kind: "remove" | "role") {
    const token = getAuthToken();
    if (!token) return;
    setBusy(true);
    setError(null);
    try {
      if (kind === "remove") await api.orgs.member.remove(token, member.user_id);
      else await api.orgs.member.setRole(token, member.user_id, nextRole);
      setPending(null);
      onChanged();
    } catch (e) {
      setError(mapMemberActionError(e, kind === "remove" ? "remove" : "change"));
      if (staleAfter(e)) onChanged();
    } finally {
      setBusy(false);
    }
  }

  return (
    <>
      <span className="flex flex-none items-center gap-[8px]">
        <Button variant="secondary" size="sm" onClick={() => setPending("role")} aria-label={`${makeAdmin ? "Make admin" : "Make member"}: ${who}`}>
          {makeAdmin ? "Make admin" : "Make member"}
        </Button>
        <Button variant="danger" size="sm" onClick={() => setPending("remove")} aria-label={`Remove ${who}`}>
          Remove
        </Button>
      </span>
      <ConfirmDialog
        open={pending === "remove"}
        title={`Remove ${who}?`}
        description={`${who} loses access to this organization right away. What they captured here is archived, not deleted. Their slot goes back to your pool, so you can give it to someone else. You can add them again later.`}
        confirmLabel="Remove member"
        busyLabel="Removing…"
        busy={busy}
        error={pending === "remove" ? error : null}
        onCancel={close}
        onConfirm={() => void run("remove")}
      />
      <ConfirmDialog
        open={pending === "role"}
        title={makeAdmin ? `Make ${who} an admin?` : `Make ${who} a regular member?`}
        description={
          makeAdmin
            ? `${who} will be able to manage members, slots, and settings for the whole organization.`
            : `${who} will no longer be able to manage members, slots, or organization settings.`
        }
        confirmLabel={makeAdmin ? "Make admin" : "Make member"}
        busyLabel="Saving…"
        busy={busy}
        error={pending === "role" ? error : null}
        onCancel={close}
        onConfirm={() => void run("role")}
      />
    </>
  );
}

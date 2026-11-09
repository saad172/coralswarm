"use client";

import { useEffect, useState } from "react";
import { useApp } from "@/lib/providers";
import { api, ApiError, type Graph } from "@/lib/api";
import { resolveTeamId } from "@/lib/teamId";
import { Button, ErrorNote, Modal } from "@/components/ui";

/* Archive confirmation. Rendered for anyone `kebabMenuItems`/`canManage`
 * would show Archive to — team admin, account owner, or org admin (see
 * `lib/workspaceManage.ts`'s doc comment on why Archive sits on that same
 * branch).
 *
 * WORD CHOICE IS LOAD-BEARING: this is never called "delete" anywhere in this
 * component. The backend route (`DELETE /v1/teams/{team_id}`) is explicitly
 * documented as an ARCHIVE — reversible, content preserved — not a content
 * delete. Calling it "delete" here would describe a destructive action that
 * isn't the one actually happening.
 *
 * THE BODY COPY BELOW IS ALSO LOAD-BEARING, not decoration — see the
 * workspace-manage design doc's "A deliberate asymmetry, accepted with eyes
 * open": a team admin can archive this workspace but can neither see it on
 * the archived list nor restore it themselves (`GET /v1/teams/archived` and
 * `POST /v1/teams/{team_id}/restore` both require ACCOUNT-level authority —
 * owner on a personal account, org admin on an org one — a strictly narrower
 * gate than the team-admin-or-org-admin check that lets them archive in the
 * first place). This dialog is the ONLY moment such a viewer will ever learn
 * that, so every fact a person needs before confirming — it disappears for
 * everyone, nothing is deleted, it's restorable for 120 days, and restoring
 * it isn't something everyone here can do — is stated in full sentences
 * before the confirm button, not buried under a warning icon. */
export function ArchiveWorkspaceModal({
  open,
  onClose,
  graph,
  onArchived,
}: {
  open: boolean;
  onClose: () => void;
  graph: Graph;
  onArchived: () => void;
}) {
  const { getAuthToken, authReady } = useApp();
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState<string | null>(null);

  useEffect(() => {
    if (open) return;
    setBusy(false);
    setErr(null);
  }, [open]);

  async function confirm() {
    const token = authReady ? getAuthToken() : null;
    if (!token || busy) return;
    const teamId = resolveTeamId(graph);
    if (!teamId) {
      // Real, if unexpected (see `Graph.team_id`'s doc comment in
      // `lib/api.ts`) — never fire `DELETE /v1/teams/{team_id}` with a
      // missing team id.
      setErr("This workspace is missing its team id — couldn't archive it.");
      return;
    }
    setBusy(true);
    setErr(null);
    try {
      await api.teams.archive(token, teamId);
      onArchived();
    } catch (e) {
      setErr(e instanceof ApiError ? e.message : "Couldn't archive this workspace.");
      setBusy(false);
    }
  }

  return (
    <Modal
      open={open}
      onClose={() => !busy && onClose()}
      title={`Archive ${graph.name}?`}
      description="Reversible — but restoring it later isn't something everyone here can do."
      footer={
        <>
          <Button size="sm" onClick={onClose} disabled={busy}>
            Cancel
          </Button>
          <Button size="sm" variant="danger" onClick={() => void confirm()} disabled={busy}>
            {busy ? "Archiving…" : "Archive workspace"}
          </Button>
        </>
      }
    >
      <div className="flex flex-col gap-[12px]">
        <p className="text-[13px] leading-[1.6] text-ink-2">
          Archiving <b className="font-medium text-ink">{graph.name}</b> takes it off the workspace list for
          everyone in it — nobody, including you, will be able to open it again until someone restores it.
        </p>

        <div className="rounded-row border border-line-2 bg-bg-2 px-[12px] py-[10px]">
          <p className="font-mono text-[9.5px] uppercase tracking-[0.13em] text-ink-3">this is not a delete</p>
          <p className="mt-[4px] text-[12.5px] leading-[1.55] text-ink-2">
            Every topic, item, and source inside stays exactly as it is.
          </p>
        </div>

        <p className="text-[13px] leading-[1.6] text-ink-2">
          It can be restored for 120 days after being archived. Past that window, restoring it is no longer
          possible.
        </p>

        <p className="text-[13px] leading-[1.6] text-ink-2">
          <b className="font-medium text-ink">Only an account owner or an organization admin can restore it</b> —
          not simply a team admin. If that isn&apos;t you, you&apos;ll need to ask whoever holds that role to
          bring it back.
        </p>

        {err && <ErrorNote>{err}</ErrorNote>}
      </div>
    </Modal>
  );
}

"use client";

import { useId, useState } from "react";
import { api, ApiError } from "@/lib/api";
import { useApp } from "@/lib/providers";
import { Button, ErrorNote, Input, Modal } from "@/components/ui";

/* Create a team workspace.
 *
 * Goes through `POST /v1/teams`, NOT `POST /v1/graphs`. The graphs route
 * rejects kind='team' outright ("Team oceans go through the teams API"); the
 * teams route creates the graph, the team, its key, the creator's key envelope
 * and the creator's membership in one transaction. That last part matters —
 * team oceans are gated on an active `team_members` row, so a team ocean
 * created without one would be invisible to the person who just made it.
 *
 * Org context is SOMETIMES allowed: the backend's `ORG_TEAMS_ENABLED` flag
 * decides it server-side. That used to be invisible to the client, which left
 * only two bad options — disable the button for every org (wrong once the flag
 * is on), or always offer it and let people discover the 403 by clicking
 * (wrong while the flag is off, which is the DEFAULT).
 *
 * So the server now publishes the capability on `GET /v1/me` and this gates on
 * it: `orgTeamsEnabled === false` means "in an org, flag off, creation WILL
 * 403" and the button explains why instead of failing. `null` (not in an org)
 * and `true` both offer it normally. This restores the intent of the original
 * code — never render a control that is guaranteed to fail — without hard-
 * coding the assumption that orgs can never have team workspaces. */

export function NewWorkspace({ onCreated }: { onCreated: () => void }) {
  const { token, orgTeamsEnabled } = useApp();
  // Strictly `=== false`: `null` means "not in an org", where this flag never
  // applied, and must NOT disable the button. Only an explicit false — in an
  // org, flag off — is a guaranteed 403.
  const unavailableInOrg = orgTeamsEnabled === false;
  const reasonId = useId();
  const [open, setOpen] = useState(false);
  const [name, setName] = useState("");
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState<string | null>(null);

  async function create() {
    if (!token || !name.trim()) return;
    setBusy(true);
    setErr(null);
    try {
      await api.teams.create(token, name.trim());
      setName("");
      setOpen(false);
      onCreated();
    } catch (e) {
      setErr(e instanceof ApiError ? e.message : "Couldn't create that workspace.");
    } finally {
      setBusy(false);
    }
  }

  return (
    <>
      {/* The reason a disabled control is unavailable has to be reachable
       *  without hovering it: browsers don't reliably show `title` on a
       *  DISABLED element (some suppress it entirely), and screen readers may
       *  skip it too. A visible caption under the button carries the same
       *  explanation `title` used to, permanently rather than on hover — the
       *  accessibility half of the deliberate decision to disable rather than
       *  let people discover the 403 by clicking (see the module doc above). */}
      <div className="flex flex-col items-end gap-[6px]">
        <Button
          variant="primary"
          size="sm"
          onClick={() => setOpen(true)}
          disabled={unavailableInOrg}
          aria-describedby={unavailableInOrg ? reasonId : undefined}
        >
          New workspace
        </Button>
        {unavailableInOrg && (
          <p id={reasonId} className="max-w-[240px] text-right text-[11.5px] leading-[1.5] text-ink-3">
            Shared workspaces aren&apos;t switched on for this organization yet.
          </p>
        )}
      </div>

      <Modal
        open={open}
        onClose={() => !busy && setOpen(false)}
        title="New workspace"
        description="A shared space others can be added to. Nothing moves into it until you promote something."
        footer={
          <>
            <Button size="sm" onClick={() => setOpen(false)} disabled={busy}>
              Cancel
            </Button>
            <Button variant="primary" size="sm" onClick={create} disabled={busy || !name.trim()}>
              {busy ? "Creating…" : "Create"}
            </Button>
          </>
        }
      >
        <div className="flex flex-col gap-[12px]">
          <Input
            label="Name"
            value={name}
            autoFocus
            placeholder="Platform, Revenue, Henderson & Co…"
            onChange={(e) => setName(e.target.value)}
            onKeyDown={(e) => {
              if (e.key === "Enter" && name.trim() && !busy) void create();
            }}
          />
          {err && <ErrorNote>{err}</ErrorNote>}
        </div>
      </Modal>
    </>
  );
}

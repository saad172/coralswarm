"use client";

import { useState } from "react";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { useApp } from "@/lib/providers";
import { archivedTeamsQueryOptions, invalidate, invalidationFor } from "@/lib/appQueries";
import { api, ApiError, type ArchivedTeam } from "@/lib/api";
import { archivedRowStatus, daysLeftLabel } from "@/lib/archivedWorkspaces";
import { Button, Eyebrow } from "@/components/ui";

/* Archived team oceans — collapsed by default, below the private/shared
 * sections on `/workspaces` (workspace-manage design doc, "Archive and
 * restore"). Fetched from `GET /v1/teams/archived`, which is gated
 * SERVER-SIDE on ACCOUNT-level authority (the account owner on a personal
 * account, the org admin on an org one) — narrower than the
 * team-admin-or-org-admin gate that lets someone ARCHIVE a workspace in the
 * first place. `ArchivedTeam`'s doc comment in lib/api.ts has the full
 * authorization story; `ArchiveWorkspaceModal`'s confirmation copy is the
 * only place a team admin without this authority ever learns about it.
 *
 * WHY THIS FETCHES UNCONDITIONALLY rather than gating on a cheap client-side
 * check first: `useOrgAdmin()` answers the ORG half of "owner or org admin"
 * for free (a Clerk context read, no network call), but the PERSONAL-account
 * OWNER half — the case that actually matters today, since org named teams
 * stay behind `ORG_TEAMS_ENABLED` and personal accounts are where team
 * workspaces work now — has no equivalent client-side signal anywhere this
 * app already loads (nothing here carries the caller's own
 * `account_members.role`). Answering that cheaply would mean adding a SECOND
 * request (a new "my account role" read, or widening `GET /v1/me`) just to
 * decide whether to make the request this section needs anyway. So this
 * fires the ONE call the design doc already asks for and reads the answer
 * off the response instead: a 403 means "no authority", rendered as nothing
 * — exactly as a 200 with zero rows reads for an owner who simply hasn't
 * archived anything yet. Net cost is one fetch either way; this is the
 * cheaper of the two shapes to reach it, not a shortcut taken under
 * protest. */
export function ArchivedWorkspaces({ supplied }: { supplied?: { rows: ArchivedTeam[] | null; onChanged: () => void } }) {
  const { getAuthToken, authReady, queryScope, refreshGraphs } = useApp();
  const qc = useQueryClient();
  const [open, setOpen] = useState(false);
  // A 403 ("must be the account owner or an org admin…") is the EXPECTED
  // answer for most viewers — a team admin, a plain member, an org member
  // who isn't the org admin — and must render as nothing, never an error
  // shout on the main workspace index. Any OTHER failure (network, 500) gets
  // the same treatment: this is a supplementary section on a page whose
  // primary content already loaded. The factory folds both into `null`.
  const options = archivedTeamsQueryOptions({ getAuthToken, scope: queryScope });
  const query = useQuery({ ...options, enabled: authReady && !supplied });
  const rows = supplied ? supplied.rows ?? [] : query.data ?? [];
  const status: "loading" | "hidden" | "ready" = supplied ? supplied.rows ? "ready" : "hidden" : query.isPending ? "loading" : query.data ? "ready" : "hidden";
  const setRows = (update: (cur: ArchivedTeam[]) => ArchivedTeam[]) =>
    qc.setQueryData<ArchivedTeam[]>(options.queryKey, (cur) => (cur ? update(cur) : cur));

  if (status !== "ready") return null;

  return (
    <section className="mt-[28px] border-t border-line-2 pt-[16px]">
      <button
        type="button"
        onClick={() => setOpen((v) => !v)}
        aria-expanded={open}
        className="group flex w-full items-center gap-[8px] text-left"
      >
        <Eyebrow className="transition-colors group-hover:text-ink">
          archived{rows.length > 0 ? ` · ${rows.length}` : ""}
        </Eyebrow>
        <span className="h-px flex-1 bg-line-2" />
        <span className="flex-none font-mono text-[10px] text-ink-3 transition-colors group-hover:text-ink">
          {open ? "hide ↑" : "show ↓"}
        </span>
      </button>

      {open && (
        <div className="mt-[12px]">
          {rows.length === 0 ? (
            <p className="text-[12.5px] leading-[1.55] text-ink-2">
              Nothing archived. A workspace you archive stays here, restorable, for 120 days.
            </p>
          ) : (
            <div className="flex flex-col divide-y divide-line-3">
              {rows.map((row) => (
                <ArchivedWorkspaceRow
                  key={row.team_id}
                  row={row}
                  onRestored={() => {
                    supplied?.onChanged();
                    setRows((cur) => cur.filter((r) => r.team_id !== row.team_id));
                    void invalidate(qc, invalidationFor.workspaceRestored());
                    void refreshGraphs();
                  }}
                />
              ))}
            </div>
          )}
        </div>
      )}
    </section>
  );
}

function ArchivedWorkspaceRow({ row, onRestored }: { row: ArchivedTeam; onRestored: () => void }) {
  const { getAuthToken, authReady } = useApp();
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState<string | null>(null);
  // The 409 refusal ("the 120-day restore window … has expired") can land
  // between this row's render and the click — a real race, not a bug: the
  // list was fetched once, and the window can close under it while it sits
  // on screen. This mirrors that outcome locally so the row updates without
  // a full re-fetch. It is NEVER set from date arithmetic here, only from the
  // server's own answer at click time — `restorable: true` at render time and
  // a 409 at click time aren't a contradiction, they're the same server fact
  // observed at two different instants.
  const [locallyExpired, setLocallyExpired] = useState(false);

  const outcome = archivedRowStatus(row);
  const expired = locallyExpired || outcome.status === "expired";

  async function restore() {
    const token = authReady ? getAuthToken() : null;
    if (!token || busy) return;
    setBusy(true);
    setErr(null);
    try {
      await api.teams.restore(token, row.team_id);
      onRestored();
    } catch (e) {
      if (e instanceof ApiError && e.status === 409) {
        setLocallyExpired(true);
        setErr("This workspace's 120-day window closed just now — it can no longer be restored.");
      } else {
        setErr(e instanceof ApiError ? e.message : "Couldn't restore this workspace.");
      }
    } finally {
      setBusy(false);
    }
  }

  const daysText =
    !expired && outcome.status === "restorable" ? daysLeftLabel(outcome.daysLeft) : "restore window closed";

  return (
    <div className="flex flex-wrap items-center gap-x-[10px] gap-y-[6px] py-[10px]">
      <div className="min-w-0 flex-1">
        <div className="truncate text-[13px] font-medium text-ink">{row.name}</div>
        <div className="mt-[2px] font-mono text-[10px] text-ink-3">
          archived{" "}
          {new Date(row.archived_at).toLocaleDateString(undefined, {
            month: "short",
            day: "numeric",
            year: "numeric",
          })}
          {" · "}
          {daysText}
        </div>
        {err && <p className="mt-[4px] max-w-[46ch] text-[11.5px] leading-[1.5] text-bad">{err}</p>}
      </div>

      {expired ? (
        <span className="flex-none font-mono text-[10px] uppercase tracking-[0.1em] text-ink-3">expired</span>
      ) : (
        <Button size="sm" onClick={() => void restore()} disabled={busy}>
          {busy ? "Restoring…" : "Restore"}
        </Button>
      )}
    </div>
  );
}

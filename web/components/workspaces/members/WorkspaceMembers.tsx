"use client";

import { useEffect, useRef, useState } from "react";
import { useRouter } from "next/navigation";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { useApp } from "@/lib/providers";
import { useOrgAdmin } from "@/components/org/useOrgAdmin";
import { api, ApiError, type Graph, type MembershipEvent } from "@/lib/api";
import { invalidate, invalidationFor, teamEventsQueryOptions, teamRosterQueryOptions } from "@/lib/appQueries";
import { resolveTeamId } from "@/lib/teamId";
import { Button, Empty, ErrorNote, Eyebrow, Input, Loading } from "@/components/ui";
import { IconAdd } from "@/components/icons";
import { MemberRow } from "./MemberRow";
import { AddTeamMemberModal } from "./AddTeamMemberModal";
import { ArchiveWorkspaceModal } from "./ArchiveWorkspaceModal";
import { TeamActivityStrip } from "./TeamActivityStrip";

const EVENTS_PAGE_SIZE = 8;

/* The Members surface for one SHARED workspace (`kind: "team"` — see
 * `web/app/(app)/workspaces/page.tsx`'s vocabulary note: a workspace here is
 * an ocean, not the org). Rendered by that page only when the resolved
 * `graph.kind === "team"` — personal, member, and org oceans never show this
 * at all (personal/member have no roster concept; org membership is the
 * whole account and is managed from Settings → Organization → Members, not
 * here).
 *
 * ADMIN GATING: "admin" means a team admin (this ocean's own roster role) OR
 * an org admin (Clerk org role, only meaningful with an active org) — exactly
 * the same "team admin or org admin" rule every mutating backend route in
 * `backend/src/routes/teams.rs` enforces. Non-admins get a fully read-only
 * roster: the role control becomes a static badge, and Add/Rename/
 * Remove never render at all (hidden, not disabled — matching
 * `useOrgAdmin.ts`'s own "gate on this and HIDE ... rather than disable"
 * convention) so nothing here can ever produce a 403 by being clicked.
 *
 * ARCHIVE: back, gated on the SAME `canManage` as Rename — team admin or org
 * admin, exactly who `kebabMenuItems` (lib/workspaceManage.ts) shows Archive
 * to. `ArchiveWorkspaceModal` carries the load-bearing confirmation copy (a
 * plain team admin can archive but can neither see the archived list nor
 * restore what they archived — that dialog is the only place they learn it).
 * On success this component's own job is done: the caller no longer has
 * anything left to manage here, so it refreshes the graph list and redirects
 * to `/workspaces` with a confirmation rather than leaving them on a page for
 * a workspace they can no longer reach (see `onArchived` below).
 *
 * `autoIntent` carries the workspace-card kebab's "Rename"/"Archive"/"Leave"
 * deep link (`/workspaces/manage?id=<gid>&intent=rename|archive|leave`) —
 * see WorkspaceCard's doc comment for why those actions deep-link here rather
 * than acting in place from the card. `"rename"` opens the inline rename
 * field and `"archive"` opens the archive confirmation, both once the roster
 * has resolved `canManage` (a non-admin who somehow reaches either URL just
 * sees the normal read-only view — neither ever renders for them regardless);
 * `"leave"` auto-opens the caller's own remove/leave confirmation once their
 * row exists. All three fire at most ONCE per mount via a ref guard, so
 * dismissing the confirmation (or finishing the rename) doesn't get reopened
 * by a later re-render. */
export function WorkspaceMembers({
  graph,
  autoIntent,
}: {
  graph: Graph;
  autoIntent?: "rename" | "archive" | "leave";
}) {
  const router = useRouter();
  const { getAuthToken, authReady, queryScope, internalUserId, refreshGraphs } = useApp();
  const { hasOrg, isAdmin: isOrgAdmin } = useOrgAdmin();

  const qc = useQueryClient();
  const ctx = { getAuthToken, scope: queryScope };
  // `teams.id`, NOT `graph.id` (the workspace-members team-id bug this
  // component was rewritten to fix): every `/v1/teams/{team_id}/...` route is
  // keyed on a DIFFERENT primary key from the ocean id — sending the ocean id
  // 404'd "team not found" for every shared workspace. See `Graph.team_id`.
  const teamId = resolveTeamId({ id: graph.id, team_id: graph.team_id });

  // ── Roster ────────────────────────────────────────────────────────────────
  // A cached query keyed on the team id. Navigating between two team oceans
  // reuses this component instance; because the key changes with the team, a
  // slow response for the PREVIOUS workspace lands in that workspace's entry
  // and can never render under this one (which would let an admin act on a
  // user who isn't even in this workspace). Same entry as the workspace
  // page's members summary.
  const rosterQuery = useQuery({ ...teamRosterQueryOptions(ctx, teamId ?? ""), enabled: authReady && !!teamId });
  const roster = rosterQuery.data ?? null;
  // Auth not settled yet is transient: keep the spinner. A missing team id is
  // real, if unexpected (this only renders for `kind === "team"`): say so
  // rather than ever firing a `/v1/teams/...` request with it spliced in.
  const loadingRoster = !!teamId && rosterQuery.isPending;
  const rosterErr = !teamId
    ? "This workspace is missing its team id — couldn't load members."
    : rosterQuery.isError && !roster
      ? (rosterQuery.error instanceof ApiError ? rosterQuery.error.message : "Couldn't load this workspace's members.")
      : null;
  // After a membership mutation: the roster (here and in the summary) and the
  // audit trail are stale.
  const membersChanged = () => invalidate(qc, invalidationFor.workspaceMembersChanged());

  const isTeamAdmin = graph.my_team_role === "admin";
  const canManage = isTeamAdmin || (hasOrg && isOrgAdmin);
  const adminCount = roster?.filter((m) => m.role === "admin").length ?? 0;

  // ── Recent changes (admin-only — see the module doc above) ──────────────
  // Page 1 is a cached query, only ever enabled once the roster has resolved
  // `canManage` — the endpoint 403s for anyone else, and there is nothing to
  // show them (see `TeamActivityStrip`), so a non-admin never fires it.
  // Later pages are appended locally and tagged with their team, so a former
  // workspace's audit rows can never be spliced into this one's strip.
  const eventsQuery = useQuery({
    ...teamEventsQueryOptions(ctx, teamId ?? "", EVENTS_PAGE_SIZE),
    enabled: authReady && !!teamId && canManage,
  });
  const [olderEvents, setOlderEvents] = useState<{ teamId: string; rows: MembershipEvent[]; page: number; hasMore: boolean } | null>(null);
  const [loadingMoreEvents, setLoadingMoreEvents] = useState(false);
  const [moreEventsErr, setMoreEventsErr] = useState<string | null>(null);
  const older = olderEvents?.teamId === teamId && eventsQuery.data ? olderEvents : null;
  const events = canManage && eventsQuery.data ? [...eventsQuery.data, ...(older?.rows ?? [])] : null;
  const loadingEvents = canManage && eventsQuery.isPending;
  const eventsErr = !canManage
    ? null
    : eventsQuery.isError && !eventsQuery.data
      ? (eventsQuery.error instanceof ApiError ? eventsQuery.error.message : "Couldn't load recent changes.")
      : moreEventsErr;
  // The endpoint has no `has_more`/total — a full page MIGHT mean there's
  // another; a heuristic, not an authoritative signal.
  const eventsHasMore = older ? older.hasMore : (eventsQuery.data?.length ?? 0) === EVENTS_PAGE_SIZE;
  const eventsPage = older?.page ?? 1;
  const moreEventsTeamIdRef = useRef<string | null>(null);
  moreEventsTeamIdRef.current = teamId;

  /** Retry: page 1 again, dropping appended pages. */
  const reloadEvents = () => {
    setOlderEvents(null);
    setMoreEventsErr(null);
    return qc.invalidateQueries({ queryKey: teamEventsQueryOptions(ctx, teamId ?? "", EVENTS_PAGE_SIZE).queryKey });
  };
  async function loadMoreEvents() {
    const token = authReady ? getAuthToken() : null;
    const requestTeamId = teamId;
    if (!token || !requestTeamId) return;
    const page = eventsPage + 1;
    setLoadingMoreEvents(true);
    setMoreEventsErr(null);
    try {
      const rows = await api.teams.events(token, requestTeamId, page, EVENTS_PAGE_SIZE);
      if (moreEventsTeamIdRef.current !== requestTeamId) return; // superseded
      setOlderEvents((cur) => ({
        teamId: requestTeamId,
        rows: [...(cur?.teamId === requestTeamId ? cur.rows : []), ...rows],
        page,
        hasMore: rows.length === EVENTS_PAGE_SIZE,
      }));
    } catch (e) {
      if (moreEventsTeamIdRef.current !== requestTeamId) return; // superseded
      setMoreEventsErr(e instanceof ApiError ? e.message : "Couldn't load recent changes.");
    } finally {
      if (moreEventsTeamIdRef.current === requestTeamId) setLoadingMoreEvents(false);
    }
  }

  // Creator marker on the roster ("created this workspace") — derived from
  // the audit trail's own `created` event when it's loadable (admins only);
  // never guessed otherwise.
  const creatorUserId = events?.find((e) => e.action === "created")?.target_user_id ?? null;

  // ── Rename (inline, admin-only) ──────────────────────────────────────────
  const [renaming, setRenaming] = useState(false);
  const [name, setName] = useState(graph.name);
  useEffect(() => setName(graph.name), [graph.name]);
  const [renameBusy, setRenameBusy] = useState(false);

  // `intent=rename` (see the component doc comment): open the field as soon
  // as `canManage` resolves. `firedRenameIntent` guards this to ONE attempt —
  // without it, `canManage` flipping from `false` (roster not loaded yet) to
  // `true` fires the effect again on every subsequent render as long as the
  // dep array is satisfied, which would silently re-open the field after the
  // admin explicitly cancelled it.
  const firedRenameIntent = useRef(false);
  useEffect(() => {
    if (autoIntent !== "rename" || firedRenameIntent.current || !canManage) return;
    firedRenameIntent.current = true;
    setRenaming(true);
  }, [autoIntent, canManage]);
  const [renameErr, setRenameErr] = useState<string | null>(null);

  function cancelRename() {
    setRenaming(false);
    setName(graph.name);
    setRenameErr(null);
  }

  async function saveRename() {
    const token = authReady ? getAuthToken() : null;
    const trimmed = name.trim();
    if (!trimmed) return;
    if (!token || trimmed === graph.name) {
      setRenaming(false);
      return;
    }
    const teamId = resolveTeamId(graph);
    if (!teamId) {
      // See the roster query's comment above — never fire a `/v1/teams/...`
      // request with a missing team id.
      setRenameErr("This workspace is missing its team id — couldn't rename it.");
      return;
    }
    setRenameBusy(true);
    setRenameErr(null);
    try {
      await api.teams.rename(token, teamId, trimmed);
      await refreshGraphs();
      setRenaming(false);
      // The backend records a `renamed` membership event for this, so
      // "Recent changes" refetches without a full page reload.
      setOlderEvents(null);
      void invalidate(qc, invalidationFor.workspaceRenamed());
    } catch (e) {
      setRenameErr(e instanceof ApiError ? e.message : "Couldn't rename this workspace.");
    } finally {
      setRenameBusy(false);
    }
  }

  // ── Add member modal ─────────────────────────────────────────────────────
  const [addOpen, setAddOpen] = useState(false);

  // ── Archive (admin-only — see the component doc comment) ────────────────
  const [archiveOpen, setArchiveOpen] = useState(false);

  // `intent=archive` (see the component doc comment): open the confirmation
  // as soon as `canManage` resolves — same one-shot ref guard as rename's,
  // for the same reason (canManage flipping false → true as the roster
  // loads must not reopen a dialog the admin already dismissed).
  const firedArchiveIntent = useRef(false);
  useEffect(() => {
    if (autoIntent !== "archive" || firedArchiveIntent.current || !canManage) return;
    firedArchiveIntent.current = true;
    setArchiveOpen(true);
  }, [autoIntent, canManage]);

  return (
    <div className="mb-[24px] flex flex-col gap-[16px]">
      <div className="rounded-card border border-line bg-bg p-[16px] shadow-card">
        <div className="mb-[12px] flex flex-wrap items-start justify-between gap-[10px]">
          <div className="min-w-0 flex-1">
            <Eyebrow className="mb-[4px]">members</Eyebrow>
            {renaming ? (
              <div className="flex flex-wrap items-center gap-[8px]">
                <Input
                  value={name}
                  autoFocus
                  aria-label="Workspace name"
                  wrapperClassName="w-full max-w-[280px]"
                  onChange={(e) => setName(e.target.value)}
                  disabled={renameBusy}
                  onKeyDown={(e) => {
                    if (e.key === "Enter") void saveRename();
                    if (e.key === "Escape") cancelRename();
                  }}
                />
                <Button size="sm" variant="primary" onClick={() => void saveRename()} disabled={renameBusy || !name.trim()}>
                  {renameBusy ? "Saving…" : "Save"}
                </Button>
                <Button size="sm" onClick={cancelRename} disabled={renameBusy}>
                  Cancel
                </Button>
              </div>
            ) : (
              <div className="flex flex-wrap items-center gap-[8px]">
                <h2 className="truncate text-[15.5px] font-medium text-ink">Who&apos;s in {graph.name}</h2>
                {canManage && (
                  <button
                    type="button"
                    onClick={() => setRenaming(true)}
                    className="flex-none font-mono text-[10px] uppercase tracking-[0.1em] text-ink-3 transition-colors hover:text-ink"
                  >
                    rename
                  </button>
                )}
              </div>
            )}
            {renameErr && <ErrorNote className="mt-[8px]">{renameErr}</ErrorNote>}
          </div>

          {canManage && (
            <div className="flex flex-none flex-wrap items-center gap-[8px]">
              <Button size="sm" variant="secondary" icon={<IconAdd width={14} height={14} />} onClick={() => setAddOpen(true)}>
                Add member
              </Button>
              <Button
                size="sm"
                variant="ghost"
                className="text-bad hover:bg-bad/10"
                onClick={() => setArchiveOpen(true)}
              >
                Archive
              </Button>
            </div>
          )}
        </div>

        {loadingRoster ? (
          <Loading what="loading members" />
        ) : rosterErr ? (
          <ErrorNote
            action={
              <Button size="sm" onClick={() => void rosterQuery.refetch()}>
                Retry
              </Button>
            }
          >
            {rosterErr}
          </ErrorNote>
        ) : !roster || roster.length === 0 ? (
          <Empty title="No members yet" hint="Add someone from your organization to share this workspace with them." />
        ) : (
          <div className="flex flex-col divide-y divide-line-3">
            {roster.map((m) => (
              <MemberRow
                key={m.user_id}
                member={m}
                teamId={resolveTeamId(graph)}
                isSelf={m.user_id === internalUserId}
                canManage={canManage}
                isSoleAdmin={m.role === "admin" && adminCount === 1}
                isCreator={creatorUserId !== null && m.user_id === creatorUserId}
                // `intent=leave` (see the component doc comment) only ever
                // applies to the CALLER's own row — every other row ignores
                // it (`MemberRow` itself re-checks `isSelf` before acting on
                // it, so passing `true` here for every row is safe).
                autoOpenConfirm={autoIntent === "leave" && m.user_id === internalUserId}
                onChanged={(info) => {
                  if (info?.selfLeft) {
                    // The caller just lost access to this ocean's roster
                    // entirely — re-fetching it would just 403. Refresh the
                    // workspace list instead so the page falls back to the
                    // index, same as an archived workspace does below.
                    void invalidate(qc, invalidationFor.workspaceMembersChanged());
                    void refreshGraphs();
                    return;
                  }
                  setOlderEvents(null);
                  void membersChanged();
                }}
              />
            ))}
          </div>
        )}
      </div>

      {canManage && (
        <TeamActivityStrip
          events={events}
          roster={roster ?? []}
          selfId={internalUserId}
          loading={loadingEvents}
          err={eventsErr}
          hasMore={eventsHasMore}
          loadingMore={loadingMoreEvents}
          onLoadMore={() => void loadMoreEvents()}
          onRetry={() => void reloadEvents()}
        />
      )}

      <AddTeamMemberModal
        open={addOpen}
        onClose={() => setAddOpen(false)}
        graph={graph}
        roster={roster ?? []}
        onAdded={() => {
          setOlderEvents(null);
          void membersChanged();
        }}
      />
      <ArchiveWorkspaceModal
        open={archiveOpen}
        onClose={() => setArchiveOpen(false)}
        graph={graph}
        onArchived={() => {
          // Same reasoning as `selfLeft` above: the caller just lost access
          // to this ocean entirely (archiving removes it from every reachable
          // read — `GET /v1/graphs` included). There is nothing left on THIS
          // page to show, so refresh the graph list (it drops out of the
          // sidebar/index immediately) and redirect to `/workspaces` with a
          // confirmation rather than leave the admin on a page for a
          // workspace they can no longer reach. The index's Archived section
          // (where Restore lives) is a cached read, so it is invalidated too;
          // otherwise a visit within `STALE.READ` shows the banner but not the
          // workspace it names.
          void invalidate(qc, invalidationFor.workspaceArchived());
          void refreshGraphs();
          router.push(`/workspaces?archived=${encodeURIComponent(graph.name)}`);
        }}
      />
    </div>
  );
}

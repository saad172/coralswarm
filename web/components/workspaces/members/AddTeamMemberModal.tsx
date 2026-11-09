"use client";

import { useEffect, useMemo, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { useApp } from "@/lib/providers";
import { orgDirectoryQueryOptions } from "@/lib/appQueries";
import { peopleQueryOptions } from "@/lib/libraryQueries";
import { useOrgAdmin } from "@/components/org/useOrgAdmin";
import {
  api,
  ApiError,
  type Graph,
  type Member,
  type OrgDirectoryMember,
  type TeamMemberRow,
  type TeamRole,
} from "@/lib/api";
import { resolveTeamId } from "@/lib/teamId";
import { Avatar, Button, cn, Empty, ErrorNote, hueForName, Loading, Modal, SearchInput, Select } from "@/components/ui";

/** One directory entry, normalized from either source below into the one
 *  shape this picker actually needs. */
interface Candidate {
  user_id: string;
  label: string;
  email: string;
}

/** Add-member picker. The subset rule is made explicit rather than left for
 * the user to discover: only people already in the caller's org/account can
 * be added at all (the backend enforces this server-side with a 422 —
 * "target user is not a member of this account/organization" — the
 * membership-subset invariant), so the picker is drawn from exactly that
 * directory, never a free-text email field, and the empty/no-match states
 * say why and point at inviting the person to the org/account first. That
 * makes a 422 here effectively unreachable in normal use — the person picked
 * came FROM the directory the backend checks against — but the raw backend
 * message is still surfaced verbatim on `err` if it ever does happen (a race
 * with someone leaving the org between load and submit, for instance), so it
 * is never an unexplained failure.
 *
 * Source directory differs by plane (mirrors `api.teams.members`'s and
 * `api.orgs.members`'s own doc comments): `GET /v1/orgs/members` in an
 * organization, `GET /v1/teams/members` (account-wide) on the personal
 * plane — there is no org-directory equivalent outside an org context. */
export function AddTeamMemberModal({
  open,
  onClose,
  graph,
  roster,
  onAdded,
}: {
  open: boolean;
  onClose: () => void;
  graph: Graph;
  roster: TeamMemberRow[];
  onAdded: () => void;
}) {
  const { getAuthToken, authReady, queryScope, accountId } = useApp();
  const { hasOrg } = useOrgAdmin();

  // Who can be added: the org directory in an org, the account roster on a
  // personal account. Cached (the account roster is the same entry Library →
  // People reads), and only read while the modal is open.
  const orgDirectory = useQuery({
    ...orgDirectoryQueryOptions({ getAuthToken, scope: queryScope }),
    enabled: open && authReady && hasOrg,
  });
  const accountRoster = useQuery(peopleQueryOptions({ getAuthToken, enabled: open && authReady && !hasOrg, scopeId: accountId }));
  const directory = hasOrg ? orgDirectory : accountRoster;
  const candidates = useMemo<Candidate[] | null>(() => {
    if (hasOrg) {
      return orgDirectory.data?.map((m: OrgDirectoryMember) => ({ user_id: m.user_id, label: m.name || m.email, email: m.email })) ?? null;
    }
    return accountRoster.data?.map((m: Member) => ({ user_id: m.user_id, label: m.email, email: m.email })) ?? null;
  }, [hasOrg, orgDirectory.data, accountRoster.data]);
  const loading = open && directory.isPending;
  const loadErr = directory.isError && !directory.data
    ? (directory.error instanceof ApiError ? directory.error.message : "Couldn't load who you can add.")
    : null;

  const [q, setQ] = useState("");
  const [picked, setPicked] = useState<string | null>(null);
  const [role, setRole] = useState<TeamRole>("member");
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState<string | null>(null);

  // Reset all per-open state on close — same shape as
  // `SlackChannelActions.tsx`'s modals, for the same reason: a re-open must
  // never show a stale pick or a leftover error from last time.
  useEffect(() => {
    if (open) return;
    setQ("");
    setPicked(null);
    setRole("member");
    setBusy(false);
    setErr(null);
  }, [open]);

  const onRoster = new Set(roster.map((m) => m.user_id));
  const pickable = (candidates ?? []).filter((c) => !onRoster.has(c.user_id));
  const needle = q.trim().toLowerCase();
  const filtered = needle ? pickable.filter((c) => `${c.label} ${c.email}`.toLowerCase().includes(needle)) : pickable;

  async function confirmAdd() {
    const token = authReady ? getAuthToken() : null;
    if (!token || !picked || busy) return;
    const teamId = resolveTeamId(graph);
    if (!teamId) {
      // Real, if unexpected (see `Graph.team_id`'s doc comment in
      // `lib/api.ts`) — never fire `/v1/teams/{team_id}/members` with a
      // missing team id.
      setErr("This workspace is missing its team id — couldn't add that person.");
      return;
    }
    setBusy(true);
    setErr(null);
    try {
      await api.teams.addMember(token, teamId, picked, role);
      onAdded();
      onClose();
    } catch (e) {
      // The backend's 422 already names exactly what to do ("target user is
      // not a member of this account/organization") — surfaced verbatim
      // rather than folded into a generic message, so it is never an
      // unexplained failure even in the rare race this picker can't prevent.
      setErr(e instanceof ApiError ? e.message : "Couldn't add that person.");
      setBusy(false);
    }
  }

  const directoryWord = hasOrg ? "organization" : "account";

  return (
    <Modal
      open={open}
      onClose={() => !busy && onClose()}
      title={`Add someone to ${graph.name}`}
      description={`Only people already in your ${directoryWord} can be added. If someone isn't listed below, invite them to the ${directoryWord} first — then come back here.`}
      footer={
        <>
          <Button size="sm" onClick={onClose} disabled={busy}>
            Cancel
          </Button>
          <Button size="sm" variant="primary" onClick={() => void confirmAdd()} disabled={busy || !picked}>
            {busy ? "Adding…" : "Add"}
          </Button>
        </>
      }
    >
      <div className="flex flex-col gap-[12px]">
        {loading ? (
          <Loading what="loading who you can add" />
        ) : loadErr ? (
          // The directory fetch failed outright — distinct from "loaded fine,
          // nothing to show" below. This is the one state that's actually bad
          // news, so it gets the error treatment, not `Empty`.
          <ErrorNote>{loadErr}</ErrorNote>
        ) : !candidates ? (
          // `candidates` is only ever `null` here because the fetch never
          // ran at all (no token yet) — not because it ran and found zero
          // rows (that's `candidates.length === 0` below). Reads the same as
          // a load failure to the person looking at it: nothing to pick from,
          // for reasons that aren't "you've added everyone".
          <ErrorNote>Couldn&apos;t load who you can add.</ErrorNote>
        ) : candidates.length === 0 ? (
          // Loaded successfully — the directory itself has no one else in
          // it. Distinct from "everyone's already a member" below: there's
          // no one to invite to THIS workspace because there's no one else
          // in the org/account at all yet.
          <Empty
            title={`Nobody else in your ${directoryWord} yet`}
            hint={`Invite someone to your ${directoryWord} first, then come back and add them to this workspace.`}
          />
        ) : pickable.length === 0 ? (
          // Loaded successfully, the directory is non-empty, but every one of
          // those people is already on this roster — the one genuinely good-
          // news case of the three.
          <Empty
            title={`Everyone in your ${directoryWord} is already here`}
            hint={`Invite a new person to the ${directoryWord} first, then come back and add them to this workspace.`}
          />
        ) : (
          <>
            <SearchInput
              value={q}
              onChange={(e) => setQ(e.target.value)}
              placeholder="Search by name or email…"
              aria-label="Search people to add"
            />
            {/* A plain group of independently-tabbable toggle buttons, NOT an
                ARIA listbox — a real listbox implies a single tab stop with
                arrow-key roving focus, which this doesn't implement, so
                claiming that role would promise keyboard behavior that isn't
                there. `aria-pressed` is the correct pattern for a button that
                toggles a picked/not-picked state, which is exactly what each
                row does. */}
            <div
              role="group"
              aria-label="People you can add"
              className="flex max-h-[240px] flex-col gap-[2px] overflow-y-auto rounded-control border border-line-2 p-[3px]"
            >
              {filtered.length === 0 ? (
                <div className="px-[12px] py-[16px] text-center text-[12.5px] text-ink-3">No matches.</div>
              ) : (
                filtered.map((c) => {
                  const on = picked === c.user_id;
                  return (
                    <button
                      key={c.user_id}
                      type="button"
                      aria-pressed={on}
                      onClick={() => setPicked(c.user_id)}
                      className={cn(
                        "flex w-full items-center gap-[9px] rounded-row px-[9px] py-[8px] text-left transition-colors",
                        "focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-brand/40",
                        on ? "bg-bg-hover" : "hover:bg-bg-subtle",
                      )}
                    >
                      <Avatar name={c.label} hue={hueForName(c.label)} size={24} />
                      <span className="min-w-0 flex-1 truncate text-[12.5px] text-ink">{c.label}</span>
                      {on && <span className="flex-none text-brand-2">✓</span>}
                    </button>
                  );
                })
              )}
            </div>
            <div className="w-[150px]">
              <Select
                label="Role"
                value={role}
                onChange={(e) => setRole(e.target.value as TeamRole)}
              >
                <option value="member">Member</option>
                <option value="admin">Admin</option>
              </Select>
            </div>
          </>
        )}
        {err && <ErrorNote>{err}</ErrorNote>}
      </div>
    </Modal>
  );
}

"use client";

// CS-441 (org-onboarding plan §3 items 11 + 40, D8/D10/D15/D21) — the admin
// approval queue on the members page. The badge (CS-438) links admins to
// `/settings/organization/members` for exactly this: before this story,
// nothing on that page listed the requests it counts.
//
// Split out of the members page (rather than inlined, like every other
// section there) so it can be tested the way every other org-onboarding
// surface already is — components/join/JoinLanding.test.tsx and
// JoinRequestBanner.test.tsx both mock `@/lib/providers`/`@/lib/api` at the
// module boundary and render the real component; the app/ tree has no test
// convention of its own to match instead.
//
// `onDecided` — approving/rejecting changes the roster and the slot pool
// too (a membership row and a purchased/reserved slot), both owned by the
// members page's own `MembersBody`. Rather than duplicate that refetch
// here, the page passes its own `refetch` down; this component still
// refetches ITS OWN list independently (`refreshGeneration` below) so the
// queue works standalone if a future caller doesn't pass one.

import { useState } from "react";
import { useQueryClient } from "@tanstack/react-query";
import {
  Avatar,
  Badge,
  Button,
  Chip,
  Empty,
  ErrorNote,
  Eyebrow,
  type Hue,
  hueForName,
  Loading,
  Mono,
  Notice,
  Select,
} from "@/components/ui";
import { SettingList } from "@/components/settings/SettingRow";
import { useApp } from "@/lib/providers";
import { api, ApiError, type JoinRequest } from "@/lib/api";
import { useMembersAdmin } from "@/lib/useMembersAdmin";
import { timeAgo } from "@/lib/teamEvents";
import { useOrgAdmin } from "./useOrgAdmin";
import { refreshJoinRequestCount } from "./useJoinRequestCount";
import { FALLBACK_TIERS, resolveTierCode, tierOptionLabel, type TierOption } from "./seatTierOptions";

/** `source` → the plain-words chip an admin reads. `self_request` gets
 *  none (D8/D19) — it needs no explaining, they just asked. */
const JOIN_SOURCE_CHIP: Partial<Record<JoinRequest["source"], { label: string; hue: Hue }>> = {
  matched_not_member: { label: "Already uses CoralSwarm", hue: "pink" },
  pool_full: { label: "Asked when no slot was free", hue: "amber" },
  tombstoned_returning: { label: "Was removed before", hue: "plum" },
  unverified_domain: { label: "Address not on a verified domain", hue: "amber" },
};

/** The first name to address a requester by, in the D8 sentence only.
 *  Falls back to the email local part, then a plain "They" if neither
 *  exists. */
function firstNameOf(user: { name: string | null; email: string | null }): string {
  const name = user.name?.trim();
  if (name) return name.split(/\s+/)[0];
  const email = user.email?.trim();
  if (email) return email.split("@")[0];
  return "They";
}

/** D8 as amended by D21 (founder, 2026-09-20 + 2026-09-21): approval grants
 *  membership; capture and backfill arrive only once THIS person connects
 *  their own integration — never a promise made at approval time. Mirrors
 *  the decision's own sentence ("Dana can join Acme; once she connects
 *  Slack she captures the channels she is in and can import up to 90 days
 *  of them"), generalised off one named platform since D21 makes every
 *  member's own connection (Slack OR Teams) the trigger, and off "she"
 *  since this queue has no gender to assume. */
export function approvalSentence(user: { name: string | null; email: string | null }, orgName: string | null): string {
  const first = firstNameOf(user);
  const org = orgName ?? "this organization";
  return (
    `${first} can join ${org}; once ${first} connects a tool, they capture the channels ` +
    `they’re in and can import up to 90 days of them.`
  );
}

/** Maps the approve/reject `ApiError` to what the row shows — the API's
 *  own message, not a bespoke calm rewrite: there is no fixed set of known
 *  failure codes for this route yet worth a dedicated map (unlike the
 *  members page's own `mapAddMemberError` and siblings). */
function mapJoinDecisionError(e: unknown): string {
  if (e instanceof ApiError) return e.message;
  return "Couldn't reach the server — try again.";
}

export function JoinRequestQueue({ onDecided }: { onDecided?: () => void }) {
  const { getAuthToken } = useApp();
  const { orgName } = useOrgAdmin();
  const qc = useQueryClient();
  const admin = useMembersAdmin();

  // One `list(t, 14)` read covers both the open queue and the
  // "recently decided" disclosure: the backend appends recently-decided
  // rows after the open ones (see `api.orgs.joinRequests.list`'s own doc
  // comment), so this splits client-side on `decision` rather than making
  // two round trips for overlapping data.
  const queueQuery = { error: admin.error };
  const joinRequests = admin.data?.join_requests ?? null;
  const joinRequestsErr = queueQuery.error
    ? (queueQuery.error instanceof ApiError ? queueQuery.error.message : "Couldn't load the join queue.")
    : null;
  // CS-442's seat-tier list. Silent on failure by design: this control must
  // still work with the `["standard"]` fallback either way.
  const tiers = admin.data?.seat_tiers;
  const tierOptions: TierOption[] = tiers && tiers.length > 0 ? tiers : FALLBACK_TIERS;
  const [rowTier, setRowTier] = useState<Record<string, string>>({});
  const [decidingId, setDecidingId] = useState<string | null>(null);
  const [confirmRejectId, setConfirmRejectId] = useState<string | null>(null);
  const [queueErr, setQueueErr] = useState<string | null>(null);
  const [queueNotice, setQueueNotice] = useState<string | null>(null);

  // A decision changes the queue and the badge count (same prefix).
  function refetch() {
    refreshJoinRequestCount(qc);
  }

  // CodeRabbit (PR #1027) — an untouched row must not submit `"standard"`
  // when a non-empty tier response doesn't actually contain it. Guard now
  // lives in `seatTierOptions.ts`'s own `resolveTierCode`, shared with
  // `AutoAddSetting.tsx` — see that module's doc comment for the second,
  // different way a stale tier code can arrive that this row's own
  // `rowTier` map could never produce.
  function tierForRow(id: string): string {
    return resolveTierCode(tierOptions, rowTier[id]);
  }

  async function approveJoinRequest(id: string) {
    const token = getAuthToken();
    if (!token) return;
    setDecidingId(id);
    setQueueErr(null);
    setQueueNotice(null);
    try {
      const decision = await api.orgs.joinRequests.approve(token, id, tierForRow(id));
      setQueueNotice(decision.slot === "purchased" ? "Approved — a new slot was added to your plan." : "Approved.");
      refetch();
      onDecided?.();
    } catch (e) {
      setQueueErr(mapJoinDecisionError(e));
    } finally {
      setDecidingId(null);
    }
  }

  async function rejectJoinRequest(id: string) {
    const token = getAuthToken();
    if (!token) return;
    setDecidingId(id);
    setQueueErr(null);
    setQueueNotice(null);
    try {
      await api.orgs.joinRequests.reject(token, id);
      setConfirmRejectId(null);
      refetch();
      onDecided?.();
    } catch (e) {
      setQueueErr(mapJoinDecisionError(e));
    } finally {
      setDecidingId(null);
    }
  }

  if (joinRequestsErr) {
    return (
      <ErrorNote
        className="mb-[16px]"
        action={
          <Button variant="secondary" size="sm" onClick={refetch}>
            Try again
          </Button>
        }
      >
        {joinRequestsErr}
      </ErrorNote>
    );
  }

  if (joinRequests === null) {
    return <Loading what="the join queue" />;
  }

  const openJoinRequests = joinRequests.filter((r) => r.decision === null);
  const decidedJoinRequests = joinRequests.filter((r) => r.decision !== null);

  return (
    <>
      <div className="mb-[9px] flex items-center gap-[9px]">
        <Eyebrow>waiting to join</Eyebrow>
        <Badge tone="brand" className="px-[6px] py-[1px] text-[9px]">
          {openJoinRequests.length}
        </Badge>
      </div>
      <SettingList className="mb-[16px]">
        {openJoinRequests.length === 0 ? (
          <div className="px-[15px] py-[16px]">
            <Empty title="No one is waiting" hint="Requests to join land here." />
          </div>
        ) : (
          openJoinRequests.map((req) => {
            const chip = JOIN_SOURCE_CHIP[req.source];
            const deciding = decidingId === req.id;
            const confirming = confirmRejectId === req.id;
            const displayName = req.user.name ?? req.user.email ?? "Someone";
            return (
              <div key={req.id} className="flex flex-col gap-[10px] px-[15px] py-[12px] sm:flex-row sm:items-start">
                <Avatar
                  name={req.user.name ?? req.user.email ?? "?"}
                  hue={hueForName(req.user.name ?? req.user.email)}
                  size={24}
                />
                <div className="min-w-0 flex-1">
                  <div className="flex flex-wrap items-center gap-[8px]">
                    <span className="text-[13.5px] font-medium text-ink">{displayName}</span>
                    {chip && <Chip hue={chip.hue}>{chip.label}</Chip>}
                    <span className="font-mono text-[10px] text-ink-4">{timeAgo(req.requested_at)}</span>
                  </div>
                  {req.user.email && (
                    <p className="mt-[3px] text-[12.5px] leading-[1.5] text-ink-3">
                      <Mono>{req.user.email}</Mono>
                    </p>
                  )}
                  <p className="mt-[6px] max-w-[52ch] text-[12.5px] leading-[1.55] text-ink-2">
                    {approvalSentence(req.user, orgName)}
                  </p>
                </div>
                <div className="flex flex-none flex-col items-stretch gap-[8px] sm:items-end">
                  <Select
                    value={tierForRow(req.id)}
                    onChange={(e) => setRowTier((prev) => ({ ...prev, [req.id]: e.target.value }))}
                    disabled={deciding}
                    wrapperClassName="w-auto"
                    className="w-auto"
                    aria-label={`Slot tier for ${displayName}`}
                  >
                    {tierOptions.map((t) => (
                      <option key={t.code} value={t.code}>
                        {tierOptionLabel(t)}
                      </option>
                    ))}
                  </Select>
                  {confirming ? (
                    <div className="flex flex-wrap items-center justify-end gap-[8px]">
                      <span className="max-w-[22ch] text-right text-[11.5px] leading-[1.4] text-ink-3">
                        They won&rsquo;t be told, and can&rsquo;t ask again.
                      </span>
                      <Button variant="danger" size="sm" disabled={deciding} onClick={() => void rejectJoinRequest(req.id)}>
                        {deciding ? "…" : "Confirm reject"}
                      </Button>
                      <Button variant="ghost" size="sm" disabled={deciding} onClick={() => setConfirmRejectId(null)}>
                        Cancel
                      </Button>
                    </div>
                  ) : (
                    <div className="flex items-center gap-[8px]">
                      <Button
                        variant="secondary"
                        size="sm"
                        disabled={deciding}
                        onClick={() => setConfirmRejectId(req.id)}
                      >
                        Reject
                      </Button>
                      <Button variant="primary" size="sm" disabled={deciding} onClick={() => void approveJoinRequest(req.id)}>
                        {deciding ? "Approving…" : "Approve"}
                      </Button>
                    </div>
                  )}
                </div>
              </div>
            );
          })
        )}
      </SettingList>
      {queueErr && (
        <div className="mb-[16px] -mt-[8px]">
          <ErrorNote>{queueErr}</ErrorNote>
        </div>
      )}
      {queueNotice && (
        <div className="mb-[16px] -mt-[8px]">
          <Notice>{queueNotice}</Notice>
        </div>
      )}

      <details className="mb-[16px] rounded-card border border-line">
        <summary className="flex cursor-pointer list-none items-center gap-[8px] px-[15px] py-[10px] marker:hidden [&::-webkit-details-marker]:hidden">
          <Eyebrow>recently decided</Eyebrow>
          <span className="font-mono text-[10px] text-ink-4">{decidedJoinRequests.length}</span>
        </summary>
        <div className="divide-y divide-line-3 border-t border-line-3">
          {decidedJoinRequests.length === 0 ? (
            <div className="px-[15px] py-[14px] text-[12.5px] text-ink-3">No decisions in the last 14 days.</div>
          ) : (
            decidedJoinRequests.map((r) => {
              const tier = typeof r.evidence?.tier === "string" && r.evidence.tier ? r.evidence.tier : null;
              return (
                <div key={r.id} className="flex items-center gap-4 px-[15px] py-[10px]">
                  <span className="min-w-0 flex-1 text-[12.5px] text-ink">{r.user.name ?? r.user.email ?? "Someone"}</span>
                  {tier && <Chip>{tier}</Chip>}
                  <Badge tone={r.decision === "approved" ? "ok" : "bad"} outline>
                    {r.decision}
                  </Badge>
                  <span className="font-mono text-[10px] text-ink-4">{r.decided_at ? timeAgo(r.decided_at) : ""}</span>
                </div>
              );
            })
          )}
        </div>
      </details>
    </>
  );
}

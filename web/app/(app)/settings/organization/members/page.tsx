"use client";

import { useEffect, useState, type Dispatch, type SetStateAction } from "react";
import Link from "next/link";
import { ConfirmDialog } from "@/components/ConfirmDialog";
import { useQueryClient } from "@tanstack/react-query";
import {
  Avatar,
  Badge,
  Button,
  Chip,
  Dot,
  Empty,
  ErrorNote,
  Eyebrow,
  hueForName,
  Input,
  Loading,
  Meter,
  Mono,
  Notice,
  Select,
} from "@/components/ui";
import { OrgSettingsShell } from "@/components/org/OrgSettingsShell";
import { DomainVerification } from "@/components/org/DomainVerification";
import { AutoAddSetting } from "@/components/org/AutoAddSetting";
import { MemberActions } from "@/components/org/MemberActions";
import { callerIsAdmin, canManageMember } from "@/components/org/memberActionsModel";
import { JoinRequestQueue } from "@/components/org/JoinRequestQueue";
import { domainsListFailure } from "@/components/org/domainVerificationModel";
import { SettingList, SettingRow } from "@/components/settings/SettingRow";
import { useApp } from "@/lib/providers";
import { api, ApiError, type OrgDomain, type SlotPoolResult } from "@/lib/api";
import { useMembersAdmin, membersAdminKey } from "@/lib/useMembersAdmin";
import { timeAgo } from "@/lib/teamEvents";
import {
  IDENTITY_SERVICE_UNAVAILABLE,
  isClerkUnavailable,
  isNoFreeSeat,
  mapAddMemberError,
  seatsAddedNote,
  seatsAfterRefusal,
  slotPlan,
  validateSingleEmail,
} from "@/lib/orgInvites";
import { useInviteSeats } from "@/components/org/useInviteSeats";

function roleLabel(role: string): string {
  return role.replace(/^org:/, "");
}

/** Short lowercase date — "aug 22" — for invite provenance rows. */
function shortDate(iso: string): string {
  return new Date(iso).toLocaleDateString("en-US", { month: "short", day: "numeric" }).toLowerCase();
}

/** Maps `POST /v1/orgs/invites/{id}/resend`'s `ApiError` to calm copy. */
function mapResendError(e: unknown): string {
  if (e instanceof ApiError) {
    if (e.message === "not_pending") return "They've already signed in — this invite is no longer pending.";
    if (isClerkUnavailable(e.message)) return IDENTITY_SERVICE_UNAVAILABLE;
  }
  return "Couldn't resend that invite — try again.";
}

/** Maps `DELETE /v1/orgs/invites/{id}`'s `ApiError` to calm copy. */
function mapRevokeError(e: unknown): string {
  if (e instanceof ApiError) {
    if (e.message === "not_pending") return "They've already signed in — this invite is no longer pending.";
    if (e.message === "cannot_revoke_self") return "You can't revoke your own invite.";
    if (e.message === "cannot_revoke_owner") return "The owner's invite can't be revoked.";
    if (isClerkUnavailable(e.message)) return IDENTITY_SERVICE_UNAVAILABLE;
    // Unmapped failure: say what the server said rather than a bare "failed".
    if (e.message) return `Couldn't revoke that invite (${e.message}). Try again.`;
  }
  return "Couldn't revoke that invite — try again.";
}

/** Org member directory now runs entirely off the custom invite API
 *  (org invites PR 1, `backend/src/routes/org_invites.rs` / PR 2 web) instead of Clerk's
 *  `useOrganization` memberships/invitations hooks — adding a member
 *  purchases a slot and creates the account server-side, which Clerk's own
 *  `inviteMembers` knows nothing about. CS-435 removed the one remaining
 *  `useOrganization` read (`maxAllowedMemberships`, the slot ceiling) —
 *  `GET /v1/orgs/slots` now exposes the real purchased/used/reserved/
 *  available pool directly, so this component no longer touches Clerk's
 *  organization object at all. "Sign-in methods" stays descriptive —
 *  there's still no UI for Clerk's domain/SSO configuration. */
function MembersBody() {
  const { authReady, getAuthToken, queryScope, internalUserId } = useApp();
  const qc = useQueryClient();
  const admin = useMembersAdmin();
  const certificate = JSON.stringify([queryScope, admin.computedAt?.toISOString(), admin.authorityRevision]);

  // The org directory — the same cached entry the add-to-workspace picker
  // reads. A failed refetch keeps the roster on screen with the error.
  const rosterQuery = { data: admin.data?.members, error: admin.error };
  const roster = rosterQuery.data ?? null;
  // 403 ("member management isn't switched on") is a dead end — nothing a
  // retry would change — so it gets no retry action; every other roster
  // failure (network blip, a 5xx) is worth retrying.
  const rosterForbidden = rosterQuery.error instanceof ApiError && rosterQuery.error.status === 403;
  const rosterErr = !rosterQuery.error
    ? null
    : rosterForbidden
      ? "Member management isn't switched on for this organization yet."
      : rosterQuery.error instanceof ApiError ? rosterQuery.error.message : "Couldn't load members.";
  // CS-449 — bumped only when auto-add's "ask once" dialog admits someone.
  // `JoinRequestQueue` owns its own refetch generation and has no external
  // "refresh now" prop (its `onDecided` only notifies outward, the same
  // shape this page's own `refetch` already is), so remounting it via `key`
  // is the smallest way to make sure an admitted request stops showing as
  // still waiting without touching that component itself.
  const [queueRemountKey, setQueueRemountKey] = useState(0);

  // Session-local patch over the roster for rows the backend hasn't caught up
  // to yet (revoke race, finding 2 below) — never persisted, never sent
  // anywhere, just a client-side "hide this until the server agrees".
  const [revokedIds, setRevokedIds] = useState<Set<string>>(new Set());

  // CS-435 — the slot pool (`purchased`/`used`/`reserved`/`available`),
  // replacing `useOrganization().maxAllowedMemberships` as the meter's
  // source (see this file's own header comment for why `useOrganization`
  // stays for nothing else). `null` = not loaded yet; a fetch failure keeps
  // it `null` and surfaces `slotPoolErr` instead of a fake number.
  const slotsQuery = { data: admin.data?.slots, error: admin.error, isLoading: admin.isPending };
  const slotPool = slotsQuery.data ?? null;
  const slotPoolErr = slotsQuery.error
    ? (slotsQuery.error instanceof ApiError ? slotsQuery.error.message : "Couldn't load the slot pool.")
    : null;

  const [newEmail, setNewEmail] = useState("");
  const [newRole, setNewRole] = useState<"member" | "admin">("member");
  const [adding, setAdding] = useState(false);
  const [addErr, setAddErr] = useState<string | null>(null);
  const [emailFailed, setEmailFailed] = useState<{ userId: string; email: string }[]>([]);

  const [resendingId, setResendingId] = useState<string | null>(null);
  const [revokingId, setRevokingId] = useState<string | null>(null);
  const [pendingErr, setPendingErr] = useState<string | null>(null);
  // The invite waiting on the "Revoke this invite?" confirmation.
  const [confirmRevoke, setConfirmRevoke] = useState<{ userId: string; email: string } | null>(null);
  const [pendingNotice, setPendingNotice] = useState<string | null>(null);

  // BLOCKER 1 fix (code review on PR #1048) — the ONE read of
  // `GET /v1/orgs/domains`, lifted here from both `DomainVerification` and
  // `AutoAddSetting`, which used to run their own independent copies of it.
  // See the CS-450 comment directly above `<DomainVerification />` below for
  // why: two fetches racing meant an admin who verified a domain in one
  // component watched the other keep reading stale until a full reload.
  const domainsQuery = { error: admin.error, isFetching: admin.isRefreshing, refetch: admin.refresh };
  const [domainPatch, setDomainPatch] = useState<{ certificate: string; value: OrgDomain[] | null } | null>(null);
  const orgDomains = admin.data ? domainPatch?.certificate === certificate ? domainPatch.value : admin.data.domains : null;
  const domainsFailure = domainsQuery.error ? domainsListFailure(domainsQuery.error) : null;
  // Hidden while a retry is in flight (item 6 below).
  const orgDomainsErr = domainsFailure && !domainsQuery.isFetching ? domainsFailure.message : null;
  const orgDomainsForbidden = domainsFailure?.forbidden ?? false;
  // DomainVerification patches the shared list in place (verify, remove);
  // the patch lands in the cache entry both components read.
  const setOrgDomains: Dispatch<SetStateAction<OrgDomain[] | null>> = (next) => {
    // Mutation receipt stays local to this certificate/authority generation.
    // Never edit the payload under an older server computed_at.
    setDomainPatch({ certificate, value: typeof next === "function" ? next(orgDomains) : next });
    void qc.invalidateQueries({ queryKey: membersAdminKey(queryScope) }, { cancelRefetch: false });
  };

  // Reconcile the session-local patches whenever a fresh roster lands.
  useEffect(() => {
    const data = rosterQuery.data;
    if (!data) return;
    // Finding 2: the backend DELETE removes the Clerk membership
    // synchronously, but the control-plane row is cleaned up later by
    // webhook — an immediate refetch right after a revoke can still return
    // that user (now with `pending: null`). Once a fresh fetch no longer
    // contains a previously-revoked id at all, the webhook has caught up and
    // the local patch can drop it too.
    const presentIds = new Set(data.map((m) => m.user_id));
    setRevokedIds((prev) => {
      const next = new Set([...prev].filter((id) => presentIds.has(id)));
      return next.size === prev.size ? prev : next;
    });
    // Finding 7: prune "email not sent" banners for any user_id no longer in
    // the PENDING set — covers both a revoke (the id drops out of the roster
    // entirely, or the race above marks it `pending: null`, either way it's
    // not `true` any more) and a sign-in (pending flips to `false`) with the
    // one rule.
    const pendingIds = new Set(data.filter((m) => m.pending === true).map((m) => m.user_id));
    setEmailFailed((prev) => prev.filter((x) => pendingIds.has(x.userId)));
  }, [rosterQuery.data]);

  // An add/revoke changes both the roster and the slot pool. The domains
  // read is deliberately NOT refetched: a member change has nothing to do
  // with this org's claimed domains.
  function refetch() {
    void qc.invalidateQueries({ queryKey: membersAdminKey(queryScope) }, { cancelRefetch: false });
  }

  // item 6, code review on PR #1048: clears the error (and its "Try again"
  // button) the instant a retry is requested, rather than leaving a stale
  // error box up with an always-enabled button an impatient admin could
  // click repeatedly, queueing racing GETs. `orgDomains === null` (true on
  // a first-load failure, the only case that reaches this button) then
  // falls straight through to `DomainVerification`'s own `Loading` state.
  function refetchOrgDomains() {
    void domainsQuery.refetch();
  }

  // Inviting past the seat count is priced and confirmed first (the same
  // quote → confirm step as Billing), never bought silently by the invite:
  // the invite is sent with `allow_purchase: false`, and a 409
  // `no_free_seat` (stale or unreadable count) reopens the quote.
  const inviteSeats = useInviteSeats();
  const invitePlan = slotPlan(1, slotPool);
  const needsSeat = invitePlan.kind === "known" && invitePlan.toBuy > 0;
  // Wait for the count before sending; a failed read sends and relies on the 409.
  const slotsLoading = slotsQuery.isLoading;

  async function addMember() {
    const email = newEmail.trim();
    if (!email || !authReady || slotsLoading) return;
    const validationErr = validateSingleEmail(email);
    if (validationErr) {
      setAddErr(validationErr);
      return;
    }
    setAddErr(null);
    inviteSeats.clearError();
    if (invitePlan.kind === "known" && invitePlan.toBuy > 0) {
      await inviteSeats.requestSeats(invitePlan.toBuy, invitePlan.free, 1, () => void sendInvite(email));
      return;
    }
    await sendInvite(email);
  }

  async function sendInvite(email: string) {
    const token = getAuthToken();
    if (!token) return;
    setAdding(true);
    setAddErr(null);
    let refused = false;
    try {
      const res = await api.orgs.invites.create(token, { email, role: newRole, allow_purchase: false });
      setNewEmail("");
      refetch();
      inviteSeats.clearAdded();
      if (!res.email_sent) {
        setEmailFailed((prev) => [...prev, { userId: res.user_id, email: res.email }]);
      }
    } catch (e) {
      if (isNoFreeSeat(e)) refused = true;
      else setAddErr(mapAddMemberError(e));
    } finally {
      setAdding(false);
    }
    if (refused) await priceRefused(email);
  }

  /** The server had no free seat for this invite (nothing was created).
   *  Re-read the pool and open the quote for the seat it needs. */
  async function priceRefused(email: string) {
    let fresh: SlotPoolResult | null = null;
    try {
      const token = getAuthToken();
      fresh = token ? await api.orgs.slots.get(token) : null;
    } catch {
      fresh = null;
    }
    const { toBuy, free } = seatsAfterRefusal(1, fresh);
    await inviteSeats.requestSeats(toBuy, free, 1, () => void sendInvite(email));
  }

  async function resendInvite(userId: string, email: string) {
    const token = getAuthToken();
    if (!token) return;
    setResendingId(userId);
    setPendingErr(null);
    setPendingNotice(null);
    try {
      const res = await api.orgs.invites.resend(token, userId);
      if (res.email_sent) {
        setEmailFailed((prev) => prev.filter((x) => x.userId !== userId));
        setPendingNotice(`Sent a new sign-in email to ${email}.`);
      } else {
        setPendingErr(`Couldn't deliver the email to ${email} — try again in a moment.`);
      }
    } catch (e) {
      if (e instanceof ApiError && e.message === "not_pending") refetch();
      setPendingErr(mapResendError(e));
    } finally {
      setResendingId(null);
    }
  }

  /** Returns true when the invite is gone, so the confirm dialog knows whether
   *  to close or stay open showing the error. */
  async function revokeInvite(userId: string): Promise<boolean> {
    const token = getAuthToken();
    if (!token) return false;
    setRevokingId(userId);
    setPendingErr(null);
    setPendingNotice(null);
    try {
      await api.orgs.invites.revoke(token, userId);
      setRevokedIds((prev) => (prev.has(userId) ? prev : new Set(prev).add(userId)));
      refetch();
      return true;
    } catch (e) {
      if (e instanceof ApiError && e.message === "not_pending") refetch();
      setPendingErr(mapRevokeError(e));
      return false;
    } finally {
      setRevokingId(null);
    }
  }

  const visibleRoster = roster?.filter((m) => !revokedIds.has(m.user_id)) ?? null;
  const active = visibleRoster?.filter((m) => m.pending !== true) ?? [];
  const isAdminCaller = callerIsAdmin(roster, internalUserId);
  const pending = visibleRoster?.filter((m) => m.pending === true) ?? [];

  // Finding 3: `pending` is best-effort — on a Clerk enrichment failure EVERY
  // row comes back `pending: null/undefined` rather than an error, so an
  // admin can't tell "nobody's pending" from "we don't know who's pending".
  // Heuristic: at least one row present, and not a single one carries an
  // actual boolean — the simplest check that catches the degraded case
  // without a dedicated backend flag.
  const pendingDegraded =
    visibleRoster !== null &&
    visibleRoster.length > 0 &&
    !visibleRoster.some((m) => m.pending === true || m.pending === false);

  return (
    <>
      <SettingList className="mb-[14px]">
        <SettingRow title="Sign-in methods" desc="Email code — no passwords. The invited address is the identity." />
      </SettingList>

      {/* CS-450 (org-onboarding plan §3 item 53) — directly ABOVE the toggle
          that depends on it: a verified domain is gate 1 of auto-add. Both
          this component and `AutoAddSetting` below receive the SAME
          `orgDomains`/`orgDomainsErr` — one fetch, owned by this page (see
          the `orgDomains` state and its effect above) — so an admin who
          claims and proves a domain here sees `AutoAddSetting`'s own gate
          copy react on the SAME render, no navigation and no reload
          required (code review on PR #1048: this comment used to assert
          that reaction as already built, when it wasn't — the two
          components read their own, independent copies of the list, and an
          admin who verified one here still saw "none is verified yet"
          below until a full reload). That component's "not armed" copy
          also links to `#domains` (this section's own id) for anyone who
          lands there first. */}
      <div className="mb-[16px]">
        <DomainVerification
          domains={orgDomains}
          loadErr={orgDomainsErr}
          loadForbidden={orgDomainsForbidden}
          onRetry={refetchOrgDomains}
          onDomainsChange={setOrgDomains}
        />
      </div>

      {/* CS-449 (org-onboarding plan §3 item 38, D11 "ask once") — directly
          above the queue it can drain in one click: flipping this on is
          exactly what can make requests already sitting below admissible. */}
      <AutoAddSetting
        domains={orgDomains}
        domainsErr={orgDomainsErr}
        onRetryDomains={refetchOrgDomains}
        onAdmitted={() => {
          refetch();
          setQueueRemountKey((k) => k + 1);
        }}
      />

      {/* CS-441 (org-onboarding plan §3 items 11 + 40) — the admin approval
          queue, ABOVE the members table: the join-request badge (CS-438)
          links here for exactly this. Approving/rejecting also changes the
          roster and slot pool below, so it shares this page's own `refetch`. */}
      <JoinRequestQueue key={queueRemountKey} onDecided={refetch} />

      {rosterErr ? (
        <ErrorNote
          className="mb-[16px]"
          action={
            // A 403 ("member management isn't switched on") is a dead end —
            // no retry changes the answer — so it gets no action, same as
            // before this fix. Every other roster failure is worth retrying.
            rosterForbidden ? undefined : (
              <Button variant="secondary" size="sm" onClick={refetch}>
                Try again
              </Button>
            )
          }
        >
          {rosterErr}
        </ErrorNote>
      ) : roster === null ? (
        <Loading what="members" />
      ) : (
        <>
          <div className="mb-[9px] flex items-center gap-[9px]">
            <Eyebrow>current members</Eyebrow>
            <Badge tone="ink" className="px-[6px] py-[1px] text-[9px]">
              {active.length}
            </Badge>
            {/* Finding 3/1: when pendingDegraded, every row's `pending` came
                back null — including invites, which land in this list
                indistinguishable from confirmed members. Carry the same
                honesty here that the pending section below already does. */}
            {pendingDegraded && (
              <span className="ml-auto text-[12.5px] text-warn">includes invites we couldn&rsquo;t confirm</span>
            )}
          </div>
          <SettingList className="mb-[16px]">
            {active.length === 0 ? (
              <div className="px-[15px] py-[16px]">
                <Empty title="No members yet" />
              </div>
            ) : (
              active.map((m) => {
                // CS-436 (org-onboarding plan §3 item 49, D21): `connected_
                // providers` is only present for an admin caller (route's
                // own admin-only gate) and is `[]`, never `null`, for a
                // member who hasn't connected — an `undefined` field (non-
                // admin caller) must render NOTHING here, not a false
                // "not connected" for every row.
                const notConnected =
                  Array.isArray(m.connected_providers) && m.connected_providers.length === 0;
                return (
                  <div key={m.user_id} className="flex items-center gap-4 px-[15px] py-[12px]">
                    <Avatar name={m.name ?? m.email} hue={hueForName(m.name ?? m.email)} size={24} />
                    <div className="min-w-0 flex-1">
                      <span className="text-[13.5px] font-medium text-ink">{m.name ?? m.email}</span>
                      <p className="mt-[3px] text-[12.5px] leading-[1.5] text-ink-3">
                        <Mono>{m.email}</Mono>
                        {notConnected && m.connect_nudged_at && (
                          <span className="ml-[8px] text-ink-4">nudged {timeAgo(m.connect_nudged_at)}</span>
                        )}
                      </p>
                    </div>
                    {notConnected && (
                      <Chip hue="plum" className="flex-none">
                        hasn&rsquo;t connected a tool yet
                      </Chip>
                    )}
                    <Badge tone={/admin|owner/i.test(m.role) ? "brand" : "ink"} outline>
                      {roleLabel(m.role)}
                    </Badge>
                    {canManageMember(m, { callerIsAdmin: isAdminCaller, selfId: internalUserId }) && (
                      <MemberActions member={m} onChanged={refetch} />
                    )}
                  </div>
                );
              })
            )}

            <div className="flex items-center gap-[8px] border-t border-line-3 bg-bg-subtle px-[15px] py-[12px]">
              <Input
                type="email"
                placeholder="name@company.com"
                value={newEmail}
                onChange={(e) => setNewEmail(e.target.value)}
                wrapperClassName="flex-1"
              />
              <Select
                value={newRole}
                onChange={(e) => setNewRole(e.target.value as "member" | "admin")}
                wrapperClassName="w-auto"
                className="w-auto"
              >
                <option value="member">Member</option>
                <option value="admin">Admin</option>
              </Select>
              <Button
                variant="brand"
                disabled={adding || inviteSeats.pricing || slotsLoading || !newEmail.trim()}
                onClick={() => void addMember()}
              >
                {adding
                  ? "Adding…"
                  : inviteSeats.pricing
                    ? "Pricing…"
                    : slotsLoading
                      ? "Checking seats…"
                      : "Add member"}
              </Button>
            </div>
            <div className="border-t border-line-3 bg-bg-subtle px-[15px] pb-[11px]">
              <Mono>
                {needsSeat
                  ? "every seat is taken · you'll see the price of one more first"
                  : "uses a free seat · sends the sign-in email"}
              </Mono>
            </div>

            {emailFailed.map((entry) => (
              <div
                key={entry.userId}
                className="border-t bg-bg-subtle-2 px-[15px] py-[11px]"
                style={{ borderTopColor: "color-mix(in srgb, var(--warn) 50%, transparent)" }}
              >
                <Eyebrow className="mb-[3px] text-warn">member added — email not sent</Eyebrow>
                <p className="text-[12.5px] leading-[1.55] text-ink-2">
                  {entry.email} holds their slot, but the invite email couldn&rsquo;t be delivered.{" "}
                  <button
                    type="button"
                    className="rounded-control text-brand-2 transition-colors hover:text-brand hover:underline focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-brand/40 disabled:cursor-not-allowed disabled:opacity-50"
                    disabled={resendingId === entry.userId}
                    onClick={() => void resendInvite(entry.userId, entry.email)}
                  >
                    {resendingId === entry.userId ? "Resending…" : "Resend it"}
                  </button>{" "}
                  or share the sign-in link yourself.
                </p>
              </div>
            ))}
          </SettingList>
          {addErr && (
            <div className="mb-[16px] -mt-[8px]">
              <ErrorNote>
                {addErr}
                {inviteSeats.added > 0 && ` ${seatsAddedNote(inviteSeats.added)}`}
              </ErrorNote>
            </div>
          )}
          {inviteSeats.error && (
            <div className="mb-[16px] -mt-[8px]">
              <ErrorNote
                action={
                  <Link href="/settings/organization/billing" className="text-[12.5px] text-brand-2 underline">
                    Open Billing
                  </Link>
                }
              >
                {inviteSeats.error}
              </ErrorNote>
            </div>
          )}
          {inviteSeats.dialog}

          <div className="mb-[9px] flex items-center gap-[9px]">
            <Eyebrow>invited · not yet signed in</Eyebrow>
            <Badge tone="brand" className="px-[5px] py-[1px] text-[9.5px]">
              {pending.length}
            </Badge>
            <span className="ml-auto text-[12.5px] text-ink-4">an invite is a standing slot until revoked</span>
          </div>
          <SettingList className="mb-[16px]">
            {pending.length === 0 ? (
              <div className="px-[15px] py-[16px]">
                {pendingDegraded ? (
                  // Finding 3: every row's `pending` came back null/undefined
                  // — a Clerk enrichment failure, not "nothing's actually
                  // pending". An affirmative "Nothing pending" here would be
                  // an outright false claim, so this stays visually distinct
                  // (warn-toned, matching the "email not sent" banner above)
                  // rather than reusing the ok-toned `Notice`.
                  <div className="flex items-start gap-3 rounded-control border border-warn/40 bg-warn/[6%] px-[12px] py-[10px] text-[12.5px] leading-[1.55] text-ink-2">
                    <span className="min-w-0 flex-1">
                      Couldn&rsquo;t reach the identity service — invite status is temporarily unavailable. Refresh
                      in a moment.
                    </span>
                  </div>
                ) : (
                  <Empty title="Nothing pending" hint="Every invite has been accepted or revoked." />
                )}
              </div>
            ) : (
              pending.map((m) => {
                const invitedBy = m.invited_by?.name ?? (m.invited_by ? "an admin" : null);
                const descParts = [
                  invitedBy ? `invited by ${invitedBy}` : "invited",
                  m.invited_at ? shortDate(m.invited_at) : null,
                ].filter(Boolean);
                return (
                  <div key={m.user_id} className="flex items-center gap-4 px-[15px] py-[12px]">
                    <Avatar name={m.email} hue="coral" size={24} />
                    <div className="min-w-0 flex-1">
                      <span className="text-[13.5px] font-medium text-ink">{m.name ?? m.email}</span>
                      <p className="mt-[3px] text-[12.5px] leading-[1.5] text-ink-3">
                        <Mono>{descParts.join(" · ")}</Mono>
                      </p>
                    </div>
                    <span className="flex flex-none items-center gap-[10px]">
                      <Badge tone={/admin|owner/i.test(m.role) ? "brand" : "ink"} outline>
                        {roleLabel(m.role)}
                      </Badge>
                      <Button
                        variant="secondary"
                        size="sm"
                        disabled={resendingId === m.user_id}
                        onClick={() => void resendInvite(m.user_id, m.email)}
                      >
                        {resendingId === m.user_id ? "…" : "Resend"}
                      </Button>
                      <Button
                        variant="danger"
                        size="sm"
                        disabled={revokingId === m.user_id}
                        onClick={() => {
                          setPendingErr(null);
                          setConfirmRevoke({ userId: m.user_id, email: m.email });
                        }}
                      >
                        {revokingId === m.user_id ? "…" : "Revoke"}
                      </Button>
                    </span>
                  </div>
                );
              })
            )}
          </SettingList>
          <ConfirmDialog
            open={confirmRevoke !== null}
            title="Revoke this invite?"
            description={
              confirmRevoke
                ? `${confirmRevoke.email} won't be able to join with this invite. Their seat is released right away. You can invite them again later.`
                : undefined
            }
            confirmLabel="Revoke invite"
            busyLabel="Revoking…"
            busy={confirmRevoke !== null && revokingId === confirmRevoke.userId}
            error={confirmRevoke ? pendingErr : null}
            onCancel={() => {
              setConfirmRevoke(null);
              setPendingErr(null);
            }}
            onConfirm={() => {
              if (!confirmRevoke) return;
              void revokeInvite(confirmRevoke.userId).then((ok) => {
                if (ok) setConfirmRevoke(null);
              });
            }}
          />
          {pendingErr && !confirmRevoke && (
            <div className="mb-[16px] -mt-[8px]">
              <ErrorNote>{pendingErr}</ErrorNote>
            </div>
          )}
          {pendingNotice && (
            <div className="mb-[16px] -mt-[8px]">
              <Notice>{pendingNotice}</Notice>
            </div>
          )}

          <div className="grid grid-cols-1 gap-[12px] sm:grid-cols-2">
            <div className="rounded-card border border-line px-[15px] py-[14px]">
              <Eyebrow className="mb-[8px]">seats</Eyebrow>
              {/* CS-435: sourced from `GET /v1/orgs/slots`
                  (`backend/src/billing/slot_pool.rs`), never
                  `useOrganization().maxAllowedMemberships` — that Clerk
                  field is an org-level membership cap, unrelated to what
                  this account actually purchased. `used` is every member
                  currently holding a purchased slot; `purchased` is the
                  pool ceiling an admin controls on the Billing page. */}
              <div className="mb-[8px] flex items-baseline gap-[8px]">
                <span className="font-mono text-[22px] font-medium text-ink">
                  {slotPool ? slotPool.used : "…"}
                </span>
                <span className="text-[12.5px] text-ink-3">
                  {!slotPool ? (
                    slotPoolErr ?? "…"
                  ) : (
                    <>
                      {`of ${slotPool.purchased} seats`}
                      {/* Finding 3: the null-pending degraded read has no
                          honest "P invited" count to show — suppress the
                          clause entirely rather than render a fabricated 0. */}
                      {!pendingDegraded &&
                        ` · ${pending.length} invited, ${pending.length === 1 ? "hasn't" : "haven't"} signed in`}
                    </>
                  )}
                </span>
              </div>
              <div className="mb-[9px]">
                {slotPool && slotPool.purchased > 0 && (
                  <Meter value={slotPool.used} max={slotPool.purchased} tone="var(--hue-plum)" />
                )}
              </div>
              <Mono>a revoked invite releases its seat immediately</Mono>

              <div className="mt-[12px] border-t border-line-3 pt-[12px] text-[12.5px] text-ink-3">
                Need more seats?{" "}
                <Link href="/settings/organization/billing" className="text-brand-2 underline">
                  Change seats in Billing
                </Link>
                .
              </div>
            </div>
          </div>
        </>
      )}
    </>
  );
}

export default function SettingsOrganizationMembersPage() {
  return (
    <OrgSettingsShell title="Getting people in" lead="How someone becomes a member, and what's still outstanding.">
      <MembersBody />
      {/* Unlisted People page: the member roster and hand-over briefs. It is
          member administration, so it is surfaced from here rather than the rail. */}
      <div className="mt-[22px] border-t border-line-2 pt-[14px]">
        <Link
          href="/people"
          className="inline-flex items-center gap-[8px] text-[13px] text-ink-2 transition-colors hover:text-ink"
        >
          <Dot hue="pink" size={6} />
          People &amp; transition briefs
          <span className="font-mono text-[10px] text-ink-4">→</span>
        </Link>
      </div>
    </OrgSettingsShell>
  );
}

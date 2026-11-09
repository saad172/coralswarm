"use client";

import { useState } from "react";
import { useOrganization, useUser } from "@clerk/nextjs";
import { useApp } from "@/lib/providers";
import { canOfferLeaveOrganization, isPersonalOrgKnownForActiveOrg } from "@/lib/orgAdmin";
import { displayName, formatMonthYear, orderedEmails, primaryEmail, providerLabel } from "@/lib/profile";
import { Avatar, Button, Mono } from "@/components/ui";
import { SettingsShell } from "@/components/settings/SettingsShell";
import { DeleteAccountZone } from "@/components/settings/DeleteAccountZone";
import { SettingList, SettingRow } from "@/components/settings/SettingRow";
import { YourUsageCard } from "@/components/settings/YourUsageCard";

export default function SettingsAccountPage() {
  const { logout, ready, isPersonalOrg, resolvedOrgId } = useApp();
  const { user, isLoaded } = useUser();
  const { organization, membership } = useOrganization();
  // The Clerk org id active THIS render, and whether `useApp()`'s
  // `isPersonalOrg` is actually known to describe it — see
  // `isPersonalOrgKnownForActiveOrg`'s doc comment (`lib/orgAdmin.ts`) for
  // why this, and not `me !== null` above, is what `canLeave` and the
  // "member of" line below must gate on.
  const activeOrgId = organization?.id ?? null;
  const personalOrgKnown = isPersonalOrgKnownForActiveOrg({ ready, resolvedOrgId, activeOrgId });

  const profile = user
    ? {
        fullName: user.fullName,
        username: user.username,
        primaryEmailAddressId: user.primaryEmailAddressId,
        emails: user.emailAddresses.map((e) => ({
          id: e.id,
          emailAddress: e.emailAddress,
          verified: e.verification?.status === "verified",
        })),
      }
    : null;

  const name = displayName(profile);
  const email = primaryEmail(profile);
  const emails = orderedEmails(profile);
  const joined = formatMonthYear(user?.createdAt ?? null);
  const connections = (user?.externalAccounts ?? []).map((a) => ({
    provider: a.provider,
    identifier: a.emailAddress || a.username || null,
  }));

  // See `lib/orgAdmin.ts`'s `canOfferLeaveOrganization` doc comment for why
  // this fails closed (`false`) while `personalOrgKnown` is `false` — not yet
  // resolved, OR resolved for a DIFFERENT org than the one active this render
  // (mid org-switch, see `isPersonalOrgKnownForActiveOrg`) — and why an "org"
  // that is actually the caller's own personal org must never offer this at
  // all.
  const canLeave = canOfferLeaveOrganization({
    hasOrganization: !!organization,
    hasMembership: !!membership,
    isPersonalOrgKnown: personalOrgKnown,
    isPersonalOrg,
  });

  const [confirmLeave, setConfirmLeave] = useState(false);
  const [leaving, setLeaving] = useState(false);
  const [leaveErr, setLeaveErr] = useState<string | null>(null);
  async function leaveOrg() {
    // Re-check eligibility here, not just at render time: this function is
    // redefined every render and closes over THIS render's `canLeave`, so a
    // stale render (e.g. a click queued just as the org switched) must not
    // reach `membership.destroy()` on the strength of an earlier render's
    // "yes". Fail closed on the same terms `canLeave` does.
    if (!membership || !canLeave) return;
    setLeaving(true);
    setLeaveErr(null);
    try {
      await membership.destroy();
      window.location.assign("/");
    } catch {
      setLeaveErr("Couldn't leave the organization. Try again, or ask an admin to remove you.");
      setLeaving(false);
    }
  }

  return (
    <SettingsShell>
      <div className="mb-[14px] flex items-center gap-[16px] rounded-card border border-line px-[16px] py-[15px]">
        {user?.hasImage && user.imageUrl ? (
          <img src={user.imageUrl} alt="" width={48} height={48} className="h-[48px] w-[48px] flex-none rounded-pill border border-line object-cover" />
        ) : (
          <Avatar name={name} size={48} />
        )}
        <div className="min-w-0 flex-1">
          <div className="truncate text-[16px] font-semibold tracking-[-0.01em] text-ink">{name}</div>
          <div className="mt-[3px] truncate text-[12.5px] text-ink-3">
            {email ?? "no email on file"}
            {/* Same `is_personal_org`/`personalOrgKnown` reasoning as "Leave"
                below (`canLeave`): a personal org is this user's own workspace,
                not an organization they are a "member of". Checked directly
                against `organization` rather than reusing `canLeave` — this
                line has no membership row to destroy, so it shouldn't depend
                on one existing. */}
            {organization &&
              personalOrgKnown &&
              !isPersonalOrg &&
              ` · member of ${organization.name} since ${joined ?? "—"}`}
          </div>
        </div>
      </div>

      <YourUsageCard />

      <div className="mb-[9px] flex items-center gap-[9px]">
        <Mono className="tracking-[0.13em]">how you sign in</Mono>
        <Mono className="text-ink-4">at least one method must stay connected</Mono>
      </div>
      <SettingList className="mb-[14px]">
        {connections.length === 0 ? (
          <SettingRow
            title={isLoaded ? "Email code" : "loading…"}
            desc={emails[0]?.emailAddress}
          />
        ) : (
          connections.map((c, i) => (
            <SettingRow key={`${c.provider}-${i}`} title={providerLabel(c.provider)} desc={c.identifier ?? "connected"} />
          ))
        )}
      </SettingList>

      {/* `canLeave` (`lib/orgAdmin.ts`): a personal org is a Clerk
          organization that stands for THIS user's own workspace, not a real
          organization — Clerk's `organization`/`membership` objects exist
          for it exactly as they would for a real org, so without this check
          every personal-org user was offered "Leave", and acting on it
          destroyed their membership in their own org with no other account
          left to fall back to (there is no `clerk_org_id IS NULL` row once a
          personal org has been adopted). */}
      {canLeave && organization && (
        <div className="flex items-center gap-[14px] rounded-card border border-bad/35 px-[15px] py-[13px]">
          <div className="min-w-0 flex-1">
            <div className="text-[13.5px] font-medium text-ink">Leave {organization.name}</div>
            <p className="mt-[3px] text-[12.5px] leading-[1.5] text-ink-3">
              Shared topics stay. Your private space is archived and retained for 60 days — during
              that window an export can be requested from us — after which it is scheduled for
              deletion.
            </p>
            {leaveErr && <p className="mt-[6px] font-mono text-[11px] text-bad">{leaveErr}</p>}
          </div>
          {confirmLeave ? (
            <span className="flex flex-none items-center gap-[8px]">
              <Button variant="danger" size="sm" disabled={leaving} onClick={() => void leaveOrg()}>
                {leaving ? "leaving…" : "Confirm"}
              </Button>
              <Button size="sm" disabled={leaving} onClick={() => setConfirmLeave(false)}>
                Cancel
              </Button>
            </span>
          ) : (
            <Button variant="danger" size="sm" className="flex-none" onClick={() => setConfirmLeave(true)}>
              Leave org
            </Button>
          )}
        </div>
      )}

      <button
        type="button"
        onClick={logout}
        className="mt-[18px] flex w-full items-center justify-center gap-[9px] rounded-pill border border-line bg-bg px-4 py-[13px] text-[13.5px] text-ink-2 hover:border-ink-3 sm:w-auto sm:px-6"
      >
        Sign out
      </button>

      <DeleteAccountZone />
    </SettingsShell>
  );
}

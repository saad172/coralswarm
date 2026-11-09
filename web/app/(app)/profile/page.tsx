"use client";

import Link from "next/link";
import { useOrganization, useUser } from "@clerk/nextjs";
import { useApp } from "@/lib/providers";
import { useQuery } from "@tanstack/react-query";
import { meQueryOptions } from "@/lib/appQueries";
import { Avatar, Badge, Chip, PageHead, Card, Eyebrow } from "@/components/ui";
import { IconLogout, IconSettings } from "@/components/icons";
import {
  contextLabel,
  displayName,
  formatDateTime,
  formatMonthYear,
  oceanKindLabel,
  orderedEmails,
  primaryEmail,
  providerLabel,
  showEmailUnderName,
  type ProfileUser,
} from "@/lib/profile";

/* Your profile — the signed-in account, read-only.
 *
 * Before this page the only account surface in the app was the mobile More
 * page's name/email strip; the console header's avatar was a raw user id whose
 * only action was an immediate sign-out. This is the view that answers "who am
 * I signed in as, in which context, with which oceans".
 *
 * Deliberately view-only (v1 scope): display name and avatar still change only
 * in Clerk's own account UI, so nothing here mutates. Preferences (models,
 * privacy, MCP) stay on /settings — this page is identity, not configuration,
 * and links across rather than duplicating.
 *
 * Sources: Clerk `useUser` (name, emails, avatar, connected accounts, joined /
 * last-active), Clerk `useOrganization` (active tenancy context — same state
 * the account switcher drives), `GET /v1/me` (internal user + account ids and role, which
 * the frontend otherwise never holds) and the app context's ocean list. Shaping
 * and every half-loaded case live in lib/profile.ts, under test.
 */

function Field({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    /* Stacks on a phone (the More page's account strip is the mobile entry
       point) and becomes a label column at ≥ sm, where there's room. */
    <div className="flex flex-col gap-[3px] border-b border-line py-[13px] last:border-0 sm:flex-row sm:items-baseline sm:gap-4">
      <div className="flex-shrink-0 text-[13.5px] text-ink-2 sm:w-[132px]">{label}</div>
      <div className="min-w-0 flex-1 text-[14px]">{children}</div>
    </div>
  );
}

/** Mono, selectable id. These are safe to show — it's the caller's own account
 *  — and they're the values support/debugging actually asks for. */
function Id({ value }: { value: string | null }) {
  if (!value) return <span className="font-mono text-[12px] text-ink-3">—</span>;
  return <span className="select-all break-all font-mono text-[12px] text-ink-2">{value}</span>;
}

export default function ProfilePage() {
  const { authReady, getAuthToken, queryScope, userId, graphs, logout } = useApp();
  const { user, isLoaded } = useUser();
  const { organization, membership } = useOrganization();
  // Seeded by boot (lib/providers.tsx), so this normally paints with no
  // request. A failure degrades to "—" rather than blanking the page.
  const me = useQuery({ ...meQueryOptions({ getAuthToken, scope: queryScope }), enabled: authReady }).data ?? null;

  // `/v1/me` is the only source for the internal ids + backend role. A failure
  // here must not blank the page: everything else comes from Clerk, so degrade
  // to "—" in the identifiers panel and leave the rest intact.

  const profile: ProfileUser | null = user
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
  const lastActive = formatDateTime(user?.lastSignInAt ?? null);
  const connections = (user?.externalAccounts ?? []).map((a) => ({
    provider: a.provider,
    identifier: a.emailAddress || a.username || null,
  }));

  return (
    <>
      <PageHead title="Your profile">
        The account you&apos;re signed in as, the context it&apos;s operating in, and the workspaces it
        can reach.
      </PageHead>

      {/* Identity */}
      <Card className="mb-[18px] p-[24px]">
        <div className="flex items-center gap-4">
          {user?.hasImage && user.imageUrl ? (
            /* Clerk-hosted avatar. Plain <img>, not next/image: nothing else
               in this app uses next/image. */
            <img
              src={user.imageUrl}
              alt=""
              width={56}
              height={56}
              className="h-[56px] w-[56px] flex-shrink-0 rounded-pill border border-line object-cover"
            />
          ) : (
            <Avatar name={name} size={56} hue="coral" />
          )}
          <div className="min-w-0 flex-1">
            <div className="truncate font-display text-[24px] leading-tight">{name}</div>
            {showEmailUnderName(profile) && (
              <div className="mt-[3px] truncate font-mono text-[12px] text-ink-3">{email}</div>
            )}
            <div className="mt-[9px] flex flex-wrap items-center gap-2">
              <Chip hue="pink">{contextLabel(organization)}</Chip>
              {membership?.role && <Chip hue="plum">{membership.role.replace(/^org:/, "")}</Chip>}
              {joined && <Chip>member since {joined}</Chip>}
            </div>
          </div>
        </div>
      </Card>

      {/* Account */}
      <Card className="mb-[18px] p-[24px]">
        <Eyebrow className="mb-[4px]">account</Eyebrow>
        <h2 className="mb-2 font-display text-[22px]">Details</h2>
        <Field label="Name">
          {profile?.fullName || <span className="text-ink-3">not set</span>}
        </Field>
        {profile?.username && <Field label="Username">{profile.username}</Field>}
        <Field label={emails.length > 1 ? "Email addresses" : "Email"}>
          {emails.length === 0 ? (
            <span className="text-ink-3">{isLoaded ? "none on file" : "loading…"}</span>
          ) : (
            <div className="space-y-[6px]">
              {emails.map((e, i) => (
                <div key={e.id} className="flex flex-wrap items-center gap-2">
                  <span className="break-all">{e.emailAddress}</span>
                  {i === 0 && emails.length > 1 && (
                    <Badge tone="ink" outline>
                      primary
                    </Badge>
                  )}
                  {!e.verified && <Badge tone="warn">unverified</Badge>}
                </div>
              ))}
            </div>
          )}
        </Field>
        {lastActive && <Field label="Last sign-in">{lastActive}</Field>}
        <Field label="Preferences">
          <Link
            href="/settings"
            className="inline-flex items-center gap-[7px] font-mono text-[12px] text-accent hover:underline"
          >
            <IconSettings width={13} height={13} /> account, connections &amp; privacy on Settings →
          </Link>
        </Field>
      </Card>

      {/* Oceans */}
      <Card className="mb-[18px] p-[24px]">
        <Eyebrow className="mb-[4px]">access</Eyebrow>
        <h2 className="mb-2 font-display text-[22px]">Your workspaces</h2>
        {graphs.length === 0 ? (
          <p className="py-2 text-[13.5px] text-ink-2">
            No workspaces in this context yet — your first one is created on demand.
          </p>
        ) : (
          graphs.map((g) => (
            <div
              key={g.id}
              className="flex items-center gap-3 border-b border-line py-[13px] last:border-0"
            >
              <Avatar name={g.name} size={28} />
              <div className="min-w-0 flex-1">
                <div className="truncate text-[14px] font-medium">{g.name}</div>
                <div className="font-mono text-[11px] tracking-[0.04em] text-ink-3">
                  {oceanKindLabel(g.kind)}
                </div>
              </div>
              {g.is_primary && (
                <Badge tone="ink" outline>
                  primary
                </Badge>
              )}
            </div>
          ))
        )}
      </Card>

      {/* Connected accounts — only when Clerk has any, so personal-only
          accounts don't get an empty panel. */}
      {connections.length > 0 && (
        <Card className="mb-[18px] p-[24px]">
          <Eyebrow className="mb-[4px]">sign-in</Eyebrow>
          <h2 className="mb-2 font-display text-[22px]">Connected accounts</h2>
          {connections.map((c, i) => (
            <Field key={`${c.provider}-${i}`} label={providerLabel(c.provider)}>
              {c.identifier ? (
                <span className="break-all font-mono text-[12.5px] text-ink-2">{c.identifier}</span>
              ) : (
                <span className="text-ink-3">connected</span>
              )}
            </Field>
          ))}
        </Card>
      )}

      {/* Identifiers */}
      <Card className="mb-[18px] p-[24px]">
        <Eyebrow className="mb-[4px]">identifiers</Eyebrow>
        <h2 className="mb-2 font-display text-[22px]">This session</h2>
        <Field label="User">
          <Id value={me?.user_id ?? userId} />
        </Field>
        <Field label="Account">
          <Id value={me?.account_id ?? null} />
        </Field>
        <Field label="Role">
          {me?.role ? (
            <span className="font-mono text-[12px] text-ink-2">{me.role}</span>
          ) : (
            <span className="font-mono text-[12px] text-ink-3">—</span>
          )}
        </Field>
        {me?.device_id && (
          <Field label="Device">
            <Id value={me.device_id} />
          </Field>
        )}
        {organization && (
          <Field label="Organization">
            <span className="font-mono text-[12px] text-ink-2">
              {organization.slug ?? organization.id}
            </span>
          </Field>
        )}
      </Card>

      <button
        type="button"
        onClick={logout}
        className="flex w-full items-center justify-center gap-[9px] rounded-pill border border-line bg-bg px-4 py-[13px] text-[13.5px] text-ink-2 hover:border-ink-3 sm:w-auto sm:px-6"
      >
        <IconLogout width={15} height={15} /> Sign out
      </button>
    </>
  );
}

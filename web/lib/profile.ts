/* Profile view derivations — the pure half of app/(app)/profile/page.tsx.
 *
 * The profile page composes three independent sources: Clerk (`useUser` /
 * `useOrganization` — display name, emails, connected accounts, org context),
 * the backend's `GET /v1/me` (internal user/account ids + role) and the app
 * context's ocean list. None of them agree on shape, and every one of them can
 * be half-loaded or null while Clerk resolves. All of that shaping lives here,
 * behind minimal structural types, so it is unit-testable without mounting
 * Clerk (web/ has no component-test harness — see lib/*.test.ts).
 *
 * View-only by design: nothing here writes. In-app editing of display name /
 * avatar is deliberately out of scope for v1 — Clerk's own account UI remains
 * the only place those change.
 */

/** Minimal shape of Clerk's `EmailAddress` that this module needs. */
export interface ProfileEmail {
  id: string;
  emailAddress: string;
  /** Clerk's `verification.status` — "verified" for a confirmed address. */
  verified: boolean;
}

/** Minimal shape of Clerk's `User` that this module needs. */
export interface ProfileUser {
  fullName: string | null;
  username: string | null;
  primaryEmailAddressId: string | null;
  emails: ProfileEmail[];
}

/** Minimal shape of one Clerk external (OAuth) account. */
export interface ProfileConnection {
  provider: string;
  /** Provider-side handle or email, when Clerk exposes one. */
  identifier: string | null;
}

export const FALLBACK_NAME = "Your account";

/** The primary email address, or the first one Clerk knows about. */
export function primaryEmail(u: ProfileUser | null): string | null {
  if (!u || u.emails.length === 0) return null;
  const primary = u.emails.find((e) => e.id === u.primaryEmailAddressId);
  return (primary ?? u.emails[0]).emailAddress;
}

/** Best available human label: full name → username → email → `fallback`.
 *  Used by app chrome (the console-header avatar) where the full email list
 *  isn't loaded and the fallback is a raw user id rather than a placeholder.
 *  Whitespace-only values count as absent. Never returns an empty string, so
 *  callers can safely index [0] for an initial. */
export function accountLabel(
  u:
    | { fullName?: string | null; username?: string | null; primaryEmail?: string | null }
    | null
    | undefined,
  fallback: string,
): string {
  const label = u?.fullName?.trim() || u?.username?.trim() || u?.primaryEmail?.trim();
  return label || fallback.trim() || FALLBACK_NAME;
}

/** `accountLabel` over a fully-loaded profile, with the generic placeholder. */
export function displayName(u: ProfileUser | null): string {
  return accountLabel(
    { fullName: u?.fullName, username: u?.username, primaryEmail: primaryEmail(u) },
    FALLBACK_NAME,
  );
}

/** Emails with the primary first, then the rest in Clerk's order. Verified
 *  state is preserved so the view can flag an unconfirmed address. */
export function orderedEmails(u: ProfileUser | null): ProfileEmail[] {
  if (!u) return [];
  const primary = u.emails.filter((e) => e.id === u.primaryEmailAddressId);
  const rest = u.emails.filter((e) => e.id !== u.primaryEmailAddressId);
  return [...primary, ...rest];
}

/** True when the email is worth showing under the name — i.e. the name isn't
 *  just the email echoed back (the fallback case in `displayName`). */
export function showEmailUnderName(u: ProfileUser | null): boolean {
  const email = primaryEmail(u);
  return !!email && displayName(u) !== email;
}

/** The active tenancy context label. Mirrors OrgSwitcher's vocabulary: no org
 *  active means the personal plane, never "no organization". */
export function contextLabel(org: { name?: string | null } | null | undefined): string {
  const name = org?.name?.trim();
  return name || "Personal";
}

/** "Member since March 2026" style — month precision is all this is for. */
export function formatMonthYear(d: Date | null | undefined): string | null {
  if (!d || Number.isNaN(d.getTime())) return null;
  return d.toLocaleDateString(undefined, { month: "long", year: "numeric" });
}

/** Short absolute date+time for last-sign-in. Absolute, not relative: a
 *  "2 hours ago" on your own session is noise, a timestamp is auditable. */
export function formatDateTime(d: Date | null | undefined): string | null {
  if (!d || Number.isNaN(d.getTime())) return null;
  return d.toLocaleString(undefined, {
    month: "short",
    day: "numeric",
    year: "numeric",
    hour: "numeric",
    minute: "2-digit",
  });
}

/** Human label for a provider slug ("oauth_google" → "Google"). Clerk prefixes
 *  strategies with `oauth_`; unknown providers degrade to a title-cased slug
 *  rather than being hidden, so a newly enabled provider still renders. */
export function providerLabel(provider: string): string {
  const slug = provider.replace(/^oauth_/, "").replace(/_/g, " ").trim();
  if (!slug) return provider;
  const known: Record<string, string> = {
    google: "Google",
    github: "GitHub",
    microsoft: "Microsoft",
    slack: "Slack",
    notion: "Notion",
    apple: "Apple",
  };
  return known[slug] ?? slug.replace(/\b\w/g, (c) => c.toUpperCase());
}

/** Ocean-kind copy for the "your oceans" list. `member` is the per-member
 *  ocean inside an org tenant (member-private-oceans) and `personal` is its
 *  consumer-plane counterpart. Both are owner-only and both read "personal":
 *  the plane they sit on is not a distinction a user has to care about, and
 *  the old "member-private" wording implied a cryptographic property the
 *  product does not have. Who can actually see it is stated on the workspace
 *  itself (WorkspaceCard's "Only you." line). */
export function oceanKindLabel(kind: string): string {
  switch (kind) {
    case "personal":
      return "personal";
    case "member":
      return "personal";
    case "team":
      return "shared team";
    case "org":
      return "org commons";
    default:
      return kind;
  }
}

// Pure decision logic behind `components/org/useOrgAdmin.ts`'s `hasOrg`/
// `isAdmin` — kept in its own framework-free module so it gets real unit
// coverage. Same rationale as `lib/workspaceManage.ts`/`lib/oceanBootstrap.ts`/
// `lib/bootGate.ts`: this project's vitest setup runs a Node environment with
// no DOM/testing-library (see `vitest.config.ts`'s own comment) and no
// `@clerk/nextjs` mock, so a hook that calls `useAuth()`/`useOrganization()`/
// `useApp()` directly cannot be exercised by a plain `.test.ts` file — this
// extraction is what makes `orgAdmin.test.ts` possible at all.
export interface OrgMembershipInputs {
  orgId: string | null | undefined;
  orgRole: string | null | undefined;
  /** `useApp()`'s `isPersonalOrg` (`GET /v1/me`'s `is_personal_org`, control
   *  migration 0208): true when the active Clerk organization is actually a
   *  PERSONAL ORG — one that stands for this user's own workspace, not a real
   *  organization. Clerk's `orgId`/`orgRole` alone cannot tell the two apart:
   *  a personal org sets both exactly as a real org would (its sole member
   *  typically carries the `org:admin` creator role Clerk assigns). */
  isPersonalOrg: boolean;
}

/** `true` when an organization is the active context AT ALL — vs. the
 *  personal plane, which a personal org counts as (see
 *  [`OrgMembershipInputs.isPersonalOrg`]'s doc comment) — and, given that,
 *  whether the caller administers it.
 *
 *  `isAdmin` is derived FROM `hasOrg`, not independently from `orgId`, and
 *  that ordering is load-bearing: computing it independently
 *  (`!!orgRole && /admin|owner/.test(orgRole)` alone) would make it true for
 *  a personal org's own creator role, which is exactly the "leave/manage
 *  this org of one" bug this function exists to close. */
export function resolveOrgAdmin({ orgId, orgRole, isPersonalOrg }: OrgMembershipInputs): {
  hasOrg: boolean;
  isAdmin: boolean;
} {
  const hasOrg = !!orgId && !isPersonalOrg;
  const isAdmin = hasOrg && !!orgRole && /admin|owner/i.test(orgRole);
  return { hasOrg, isAdmin };
}

/** Inputs to [`canOfferLeaveOrganization`] — kept as plain booleans (not the
 *  live Clerk `organization`/`membership` objects or the `GET /v1/me`
 *  response) for the same testability reason as [`OrgMembershipInputs`]
 *  above. */
export interface LeaveOrgInputs {
  /** Clerk's `organization` is non-null (an org is the active context). */
  hasOrganization: boolean;
  /** Clerk's `membership` is non-null (this user has a membership row to
   *  destroy — `membership.destroy()` is `app/(app)/settings/page.tsx`'s
   *  only call site). */
  hasMembership: boolean;
  /** `GET /v1/me` has resolved for the CURRENTLY ACTIVE Clerk organization —
   *  not merely "resolved at some point". `false` covers both "not yet known"
   *  (before the first `GET /v1/me`) and "known, but for a different org"
   *  (mid org-switch — see [`isPersonalOrgKnownForActiveOrg`], the intended
   *  way to compute this). Either way this must resolve to "cannot offer
   *  leave" — see this function's own doc comment for why that direction,
   *  not the reverse. */
  isPersonalOrgKnown: boolean;
  /** `me?.is_personal_org` — meaningless (and ignored) while
   *  `isPersonalOrgKnown` is `false`. */
  isPersonalOrg: boolean;
}

/** Whether the account page may offer "Leave this organization" at all.
 *
 *  FAILS CLOSED ON THE UNKNOWN CASE, DELIBERATELY: `isPersonalOrgKnown: false`
 *  (before `GET /v1/me` resolves) refuses the affordance, the same direction
 *  as an actual personal org — never the direction that would offer it. The
 *  two failure costs are wildly asymmetric: an over-eager `true` here can
 *  strand a personal-org user with `membership.destroy()` and no
 *  `clerk_org_id IS NULL` account left to fall back to (there is none, once a
 *  personal org has been adopted — `bin/adopt_personal_org_account.rs`'s
 *  doc comment); an over-cautious `false` costs one render's worth of a
 *  missing button that reappears the instant `GET /v1/me` answers. */
export function canOfferLeaveOrganization({
  hasOrganization,
  hasMembership,
  isPersonalOrgKnown,
  isPersonalOrg,
}: LeaveOrgInputs): boolean {
  return hasOrganization && hasMembership && isPersonalOrgKnown && !isPersonalOrg;
}

/** Inputs to [`isPersonalOrgKnownForActiveOrg`]. */
export interface PersonalOrgKnownInputs {
  /** `useApp()`'s `ready` — `isPersonalOrg`/`resolvedOrgId` are the pre-boot
   *  defaults (`false`/`null`) until this is `true`. */
  ready: boolean;
  /** `useApp()`'s `resolvedOrgId` — the Clerk org id (`null` for the personal
   *  plane) that `useApp()`'s `isPersonalOrg` was actually resolved FOR. */
  resolvedOrgId: string | null;
  /** The LIVE active Clerk org id right now — `useOrganization()`'s
   *  `organization?.id ?? null`, read directly, not through `useApp()`. */
  activeOrgId: string | null;
}

/** Is `useApp()`'s `isPersonalOrg` trustworthy for the org that is active
 *  RIGHT NOW?
 *
 *  `ready` alone is not enough: Clerk updates `useAuth()`/`useOrganization()`
 *  synchronously with the render that switches the active org, but the
 *  effect that resets `useApp()`'s `ready` to `false` and re-fetches
 *  `GET /v1/me` for the new org only runs AFTER that render commits. For
 *  that one-render window, a component reading `organization.id` directly
 *  sees the NEW org while `ready` still reads `true` and `isPersonalOrg`
 *  still describes the PREVIOUS org — see `resolvedOrgId`'s doc comment on
 *  `AppState` (`lib/providers.tsx`) for the full account. Comparing
 *  `resolvedOrgId` (pinned to the boot pass that set `isPersonalOrg`)
 *  against the live `activeOrgId` catches exactly that window, without
 *  waiting a render for `ready` to catch up.
 *
 *  Feed the result into [`canOfferLeaveOrganization`]'s `isPersonalOrgKnown`
 *  — `app/(app)/settings/page.tsx`'s `leaveOrg` is the one call site
 *  where a false positive here means `membership.destroy()` on the wrong
 *  org. */
export function isPersonalOrgKnownForActiveOrg({
  ready,
  resolvedOrgId,
  activeOrgId,
}: PersonalOrgKnownInputs): boolean {
  return ready && resolvedOrgId === activeOrgId;
}

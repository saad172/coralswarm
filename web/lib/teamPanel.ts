// Pure decision logic for `app/(app)/team/page.tsx`'s tenancy branch: the
// actual page is a `"use client"`
// component wired to Clerk's `useOrganization()` hook, which this repo's
// vitest setup can't exercise (`vitest.config.ts` runs a Node environment
// with no DOM/testing-library — see that file's own comment; this project
// deliberately has no component-test harness). Every branch that decides
// WHICH PANEL TO SHOW, though, is a pure function of Clerk's org-loading
// state, so that decision lives here, gets full unit coverage, and the page
// itself just renders whichever panel comes back.
//
// WHY THIS BRANCH EXISTS AT ALL: `POST /v1/teams/invite` deliberately 403s
// for org accounts (`reject_org_account_membership_management`,
// `backend/src/routes/teams.rs`) — org `account_members` rows are written
// ONLY by the Clerk webhook (`organizationMembership.created/.updated/.deleted`,
// see `backend/src/clerk_provision.rs`), so a direct write here would let an
// org owner re-add a Clerk-offboarded member out-of-band, bypassing
// offboarding tombstones and org revocations. Org callers get Clerk's own
// `<OrganizationProfile />` Members tab instead — its invites still flow
// back through the same webhook, so membership materialization stays
// single-writer. Personal callers keep today's email-invite box unchanged.

/** `org`: Clerk's `<OrganizationProfile />`, exactly as before #1540, for
 *  every org the server does not positively say is self-serve (concierge
 *  orgs, and any org whose signal is `null` / missing / errored).
 *  `selfserve_org`: our own read-only member list plus the link to
 *  /settings/organization/members, for a team org `POST /v1/orgs` made
 *  (#1540 SF3 / review SF-a). */
export type TeamMembersPanel = "loading" | "org" | "selfserve_org" | "personal";

/** Mirrors the two fields of `useOrganization()`'s return value this
 *  decision needs, so the page can pass the hook's return value straight
 *  through with no adapter object. */
export interface TeamMembersPanelInput {
  /** `useOrganization().isLoaded` */
  isLoaded: boolean;
  /** `useOrganization().organization` — reads `null` both while Clerk is
   *  still resolving AND once resolved with Personal active; only `isLoaded`
   *  disambiguates those two states (see below). */
  organization: unknown | null | undefined;
  /** `GET /v1/bootstrap`'s `selfserve_org` for the active org (#1540 review
   *  SF-a): `true` / `false` from the server; `null` when the server could
   *  not say (read failed or timed out, an older backend without the field,
   *  or the read errored); `undefined` while it is still being read. */
  selfserveOrg?: boolean | null;
}

/**
 * Which members panel should `/team` render this render?
 *
 * `isLoaded` gates first and unconditionally: while Clerk is still
 * resolving, `organization` reads `null` — INDISTINGUISHABLE from "Personal
 * is active". Skipping this gate would flash
 * the personal invite-box panel for every org member on first paint, then
 * swap to `<OrganizationProfile />` a beat later — exactly the "flash the
 * wrong panel while Clerk resolves" failure mode this function exists to
 * prevent.
 */
export function decideTeamMembersPanel(input: TeamMembersPanelInput): TeamMembersPanel {
  if (!input.isLoaded) return "loading";
  if (!input.organization) return "personal";
  // An org: which surface depends on the server's self-serve signal. While
  // it is still being read nothing is shown (no flash of Clerk's members UI
  // for a self-serve org). ONLY an explicit `true` swaps in our own list.
  // Anything else (`false`, `null` because the server could not say or timed
  // out, a read that errored, an older backend without the field) keeps
  // Clerk's `<OrganizationProfile />`, the behaviour before #1540: a missing
  // signal must never take a concierge org's member management away. The
  // seat cap is enforced server-side (Clerk's max_allowed_memberships), so a
  // self-serve org that briefly sees Clerk's profile cannot exceed its seats.
  if (input.selfserveOrg === undefined) return "loading";
  return input.selfserveOrg === true ? "selfserve_org" : "org";
}

/** `GET /v1/bootstrap`'s `selfserve_org` as the panel reads it: a boolean
 *  only when the server sent one; `null` for `null`, an absent field (an
 *  older backend) or anything else. Shared by boot's seed and the query. */
export function parseSelfserveOrg(raw: unknown): boolean | null {
  return typeof raw === "boolean" ? raw : null;
}

/** The page's query state as `decideTeamMembersPanel`'s `selfserveOrg`: an
 *  errored read (including the client's request timeout) is `null` ("could
 *  not say"); otherwise the data, `undefined` while still pending. */
export function selfserveOrgSignal(q: { isError: boolean; data: boolean | null | undefined }): boolean | null | undefined {
  return q.isError ? null : q.data;
}

// #1540 (review SF-a): a SELF-SERVE team org (one `POST /v1/orgs` made; the
// server says so in `GET /v1/bootstrap`'s `selfserve_org`) gets no Clerk
// <OrganizationProfile/> at all, because hiding its Members page with
// `appearance.elements` is cosmetic (`#/organization-members` stays
// reachable). It gets our own read-only member list and the link below.
// Every other org keeps the profile exactly as before, configured as follows.
//
// Self-serve signup A2 (security review S4): Clerk's own membership
// management must not be reachable from the People page for a team org.
// A Clerk-native invite skips our seat quote (#1515), and Clerk's
// verified-domain enrollment adds members on its own; both would put people
// past the seats the org paid for. Members and invites go through
// /settings/organization/members (our flow). The installed @clerk/nextjs (7.x)
// has no prop that removes a built-in OrganizationProfile page, so:
//   * the General page is mounted first (`<OrganizationProfile.Page
//     label="general" />` before `label="members"`), so it is the default
//     route instead of Members;
//   * these appearance elements hide the Members nav entry, the invite
//     button (should the Members route be reached by its hash anyway) and the
//     verified-domains section (domain auto-enrollment).
// The backend's Clerk member cap (max_allowed_memberships = purchased seats)
// is the enforcement; this only keeps the UI from offering the bypass.

/** `appearance.elements` keys hidden on a team org's OrganizationProfile. */
export const TEAM_ORG_PROFILE_HIDDEN_ELEMENTS = [
  "navbarButton__members",
  "membersPageInviteButton",
  "profileSection__organizationDomains",
] as const;

/** The `appearance.elements` object that hides them. */
export function teamOrgProfileElements(): Record<(typeof TEAM_ORG_PROFILE_HIDDEN_ELEMENTS)[number], { display: "none" }> {
  return Object.fromEntries(TEAM_ORG_PROFILE_HIDDEN_ELEMENTS.map((k) => [k, { display: "none" as const }])) as Record<
    (typeof TEAM_ORG_PROFILE_HIDDEN_ELEMENTS)[number],
    { display: "none" }
  >;
}

/** Where a team org's members and invites are managed (our seat-quoted flow). */
export const TEAM_ORG_MEMBERS_HREF = "/settings/organization/members";

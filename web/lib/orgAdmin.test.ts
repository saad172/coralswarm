import { describe, expect, it } from "vitest";
import { canOfferLeaveOrganization, isPersonalOrgKnownForActiveOrg, resolveOrgAdmin } from "./orgAdmin";

// Covers the three account shapes `backend/src/tenancy/personal_org.rs`
// distinguishes: plain personal (no org at all), a personal org (a Clerk
// organization that stands for one person's own ocean, control migration
// 0208), and a real organization. This is the central fix for "the web app
// shows a one-person org the full organization experience": every screen
// gated by `useOrgAdmin()` — OrgGate, WorkspaceMembers, AddTeamMemberModal,
// WorkspaceCard, useSettingsRole — inherits whatever this function decides.
describe("resolveOrgAdmin", () => {
  it("plain personal account: no org, not an admin of anything", () => {
    expect(resolveOrgAdmin({ orgId: null, orgRole: null, isPersonalOrg: false })).toEqual({
      hasOrg: false,
      isAdmin: false,
    });
  });

  it("a personal org: orgId is set, but isPersonalOrg says it isn't a real one", () => {
    // The exact shape Clerk hands the client for a personal org: `orgId` is
    // non-null (the founder's own personal org has a real Clerk org id), and
    // Clerk's own creator-role convention makes `orgRole` "org:admin" — the
    // SAME role a real org's sole admin would carry. Only `isPersonalOrg`
    // distinguishes the two.
    expect(
      resolveOrgAdmin({ orgId: "org_personal_abc", orgRole: "org:admin", isPersonalOrg: true }),
    ).toEqual({ hasOrg: false, isAdmin: false });
  });

  it("a personal org's admin role can never leak into isAdmin, even with 'owner'", () => {
    // The regression this function must never reintroduce: computing isAdmin
    // independently of hasOrg (e.g. `!!orgRole && /admin|owner/.test(orgRole)`
    // alone) would make this true for a personal org, which is exactly the
    // "leave/manage this org of one" bug.
    expect(
      resolveOrgAdmin({ orgId: "org_personal_xyz", orgRole: "org:owner", isPersonalOrg: true }).isAdmin,
    ).toBe(false);
  });

  it("a real organization, admin: unchanged — the regression-risk path", () => {
    expect(
      resolveOrgAdmin({ orgId: "org_real_123", orgRole: "org:admin", isPersonalOrg: false }),
    ).toEqual({ hasOrg: true, isAdmin: true });
  });

  it("a real organization, plain member: has an org, but is not its admin", () => {
    expect(
      resolveOrgAdmin({ orgId: "org_real_123", orgRole: "org:member", isPersonalOrg: false }),
    ).toEqual({ hasOrg: true, isAdmin: false });
  });

  it("owner counts as admin for a real org, same as today", () => {
    expect(
      resolveOrgAdmin({ orgId: "org_real_123", orgRole: "org:owner", isPersonalOrg: false }).isAdmin,
    ).toBe(true);
  });

  it("undefined orgId/orgRole (Clerk not yet loaded) resolve the same as null", () => {
    expect(
      resolveOrgAdmin({ orgId: undefined, orgRole: undefined, isPersonalOrg: false }),
    ).toEqual({ hasOrg: false, isAdmin: false });
  });
});

// This is the "Leave" affordance on `app/(app)/settings/account/page.tsx` —
// the single most damaging site in this whole area: an over-eager `true`
// leads to `membership.destroy()` with no `clerk_org_id IS NULL` account left
// to fall back to, once a personal org has been adopted.
describe("canOfferLeaveOrganization", () => {
  const readyRealOrg = {
    hasOrganization: true,
    hasMembership: true,
    isPersonalOrgKnown: true,
    isPersonalOrg: false,
  };

  it("a real organization, fully resolved: leave IS offered — the regression-risk path", () => {
    expect(canOfferLeaveOrganization(readyRealOrg)).toBe(true);
  });

  it("a personal org: leave is refused even with an organization and a membership row", () => {
    expect(canOfferLeaveOrganization({ ...readyRealOrg, isPersonalOrg: true })).toBe(false);
  });

  it("GET /v1/me not yet resolved: refused, not defaulted to offering it", () => {
    // `isPersonalOrg: false` here is what a naive default would look like —
    // this must still refuse, because `isPersonalOrgKnown` is false.
    expect(
      canOfferLeaveOrganization({ ...readyRealOrg, isPersonalOrgKnown: false, isPersonalOrg: false }),
    ).toBe(false);
  });

  it("no active organization: refused", () => {
    expect(canOfferLeaveOrganization({ ...readyRealOrg, hasOrganization: false })).toBe(false);
  });

  it("no membership row to destroy: refused", () => {
    expect(canOfferLeaveOrganization({ ...readyRealOrg, hasMembership: false })).toBe(false);
  });

  it("nothing true at all: refused", () => {
    expect(
      canOfferLeaveOrganization({
        hasOrganization: false,
        hasMembership: false,
        isPersonalOrgKnown: false,
        isPersonalOrg: false,
      }),
    ).toBe(false);
  });
});

// The org-switch race a code review caught on `app/(app)/settings/account/
// page.tsx`: Clerk's `useOrganization()` can show the NEW org one render
// before `useApp()`'s boot-chain effect has reacted to the switch, so `ready`
// alone can still read `true` while `resolvedOrgId` (what `isPersonalOrg` was
// actually resolved for) still names the org BEFORE the switch. Comparing
// `resolvedOrgId` against the live `activeOrgId` is what closes that window.
describe("isPersonalOrgKnownForActiveOrg", () => {
  it("settled and matching (a real org, steady state): known", () => {
    expect(
      isPersonalOrgKnownForActiveOrg({
        ready: true,
        resolvedOrgId: "org_real_123",
        activeOrgId: "org_real_123",
      }),
    ).toBe(true);
  });

  it("settled and matching on the personal plane (both null): known", () => {
    expect(
      isPersonalOrgKnownForActiveOrg({ ready: true, resolvedOrgId: null, activeOrgId: null }),
    ).toBe(true);
  });

  it("boot not yet ready: refused regardless of the ids", () => {
    expect(
      isPersonalOrgKnownForActiveOrg({
        ready: false,
        resolvedOrgId: "org_real_123",
        activeOrgId: "org_real_123",
      }),
    ).toBe(false);
  });

  it("THE RACE: ready still true, but resolvedOrgId names the org from BEFORE an org switch — refused", () => {
    // Exactly the reported shape: `me.is_personal_org` was resolved for a
    // previous REAL organization (`org_real_123`), and Clerk has already
    // handed the render the NEW personal org (`org_personal_abc`) before the
    // identity effect reset `ready`. A naive `ready && !isPersonalOrg` gate
    // would offer "Leave" here; this must refuse.
    expect(
      isPersonalOrgKnownForActiveOrg({
        ready: true,
        resolvedOrgId: "org_real_123",
        activeOrgId: "org_personal_abc",
      }),
    ).toBe(false);
  });

  it("ready and ids match again once the new identity's boot pass has settled: known", () => {
    expect(
      isPersonalOrgKnownForActiveOrg({
        ready: true,
        resolvedOrgId: "org_personal_abc",
        activeOrgId: "org_personal_abc",
      }),
    ).toBe(true);
  });

  it("stale personal-plane resolution vs. a newly active org: refused", () => {
    expect(
      isPersonalOrgKnownForActiveOrg({ ready: true, resolvedOrgId: null, activeOrgId: "org_real_123" }),
    ).toBe(false);
  });
});

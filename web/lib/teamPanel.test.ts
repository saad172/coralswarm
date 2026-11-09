import { describe, expect, it } from "vitest";
import {
  decideTeamMembersPanel,
  parseSelfserveOrg,
  selfserveOrgSignal,
  TEAM_ORG_MEMBERS_HREF,
  teamOrgProfileElements,
  type TeamMembersPanelInput,
} from "./teamPanel";

describe("decideTeamMembersPanel: self-serve vs other orgs (#1540 review SF-a)", () => {
  const org = { id: "org_1", slug: "acme" };
  it("keeps Clerk's OrganizationProfile for an org the server says is not self-serve", () => {
    expect(decideTeamMembersPanel({ isLoaded: true, organization: org, selfserveOrg: false })).toBe("org");
  });
  it("shows our own member list for a self-serve org", () => {
    expect(decideTeamMembersPanel({ isLoaded: true, organization: org, selfserveOrg: true })).toBe("selfserve_org");
  });
  it("shows nothing while the signal is still being read (no flash of Clerk's members UI)", () => {
    expect(decideTeamMembersPanel({ isLoaded: true, organization: org, selfserveOrg: undefined })).toBe("loading");
  });
  it("keeps Clerk's OrganizationProfile when the server could not say (null, or a timeout it reports as null)", () => {
    expect(decideTeamMembersPanel({ isLoaded: true, organization: org, selfserveOrg: null })).toBe("org");
  });
  it("keeps Clerk's OrganizationProfile when the read errored or timed out client-side", () => {
    for (const data of [undefined, true, false] as const) {
      const selfserveOrg = selfserveOrgSignal({ isError: true, data });
      expect(selfserveOrg).toBeNull();
      expect(decideTeamMembersPanel({ isLoaded: true, organization: org, selfserveOrg })).toBe("org");
    }
  });
  it("keeps Clerk's OrganizationProfile for an older backend without the field", () => {
    const boot: { selfserve_org?: boolean | null } = {};
    const selfserveOrg = parseSelfserveOrg(boot.selfserve_org);
    expect(selfserveOrg).toBeNull();
    expect(decideTeamMembersPanel({ isLoaded: true, organization: org, selfserveOrg })).toBe("org");
  });
  it("reads only a real boolean from the server", () => {
    expect(parseSelfserveOrg(true)).toBe(true);
    expect(parseSelfserveOrg(false)).toBe(false);
    for (const raw of [null, undefined, "true", 1, {}]) expect(parseSelfserveOrg(raw)).toBeNull();
    expect(selfserveOrgSignal({ isError: false, data: undefined })).toBeUndefined();
    expect(selfserveOrgSignal({ isError: false, data: true })).toBe(true);
  });
  it("only an explicit true swaps in our own list", () => {
    for (const v of [false, null] as const) {
      expect(decideTeamMembersPanel({ isLoaded: true, organization: org, selfserveOrg: v })).toBe("org");
    }
    expect(decideTeamMembersPanel({ isLoaded: true, organization: org, selfserveOrg: true })).toBe("selfserve_org");
  });
  it("ignores the signal for a personal caller", () => {
    expect(decideTeamMembersPanel({ isLoaded: true, organization: null, selfserveOrg: true })).toBe("personal");
    expect(decideTeamMembersPanel({ isLoaded: true, organization: null, selfserveOrg: undefined })).toBe("personal");
  });
});

describe("decideTeamMembersPanel", () => {
  it("renders neither surface while Clerk is still resolving (isLoaded=false)", () => {
    // Loading state must win regardless of what `organization` happens to
    // read mid-resolution — it's `null` in that window too, which is why
    // `isLoaded` has to gate first.
    const stillOrgShaped: TeamMembersPanelInput = { isLoaded: false, organization: null };
    const stillUndefinedShaped: TeamMembersPanelInput = { isLoaded: false, organization: undefined };
    expect(decideTeamMembersPanel(stillOrgShaped)).toBe("loading");
    expect(decideTeamMembersPanel(stillUndefinedShaped)).toBe("loading");
  });

  it("renders the Clerk org surface for a non-self-serve org caller (loaded + organization present)", () => {
    const input: TeamMembersPanelInput = {
      isLoaded: true,
      organization: { id: "org_1", slug: "acme" },
      selfserveOrg: false,
    };
    expect(decideTeamMembersPanel(input)).toBe("org");
  });

  it("renders the personal email-invite panel for a personal caller (loaded + no organization)", () => {
    const input: TeamMembersPanelInput = { isLoaded: true, organization: null };
    expect(decideTeamMembersPanel(input)).toBe("personal");
  });
});

describe("team org OrganizationProfile (A2 security review S4)", () => {
  it("hides Clerk's Members nav entry, its invite button and verified domains", () => {
    const els = teamOrgProfileElements();
    expect(Object.keys(els).sort()).toEqual(
      ["membersPageInviteButton", "navbarButton__members", "profileSection__organizationDomains"].sort(),
    );
    for (const v of Object.values(els)) expect(v).toEqual({ display: "none" });
  });

  it("sends members and invites to our own flow", () => {
    expect(TEAM_ORG_MEMBERS_HREF).toBe("/settings/organization/members");
  });
});

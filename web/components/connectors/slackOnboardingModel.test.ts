import { describe, expect, it } from "vitest";
import type {
  SlackOnboardingConnected,
  SlackOnboardingMember,
  SlackOnboardingNotConnected,
} from "@/lib/api";
import {
  completedCount,
  connectIsAdminOnly,
  deriveSteps,
  workspaceActsAreAdminOnly,
  isTaskStep,
  memberView,
  reefDensity,
} from "./slackOnboardingModel";

/* The ONE client derivation of docs/connectors/slack-onboarding.md §3's
 * payload, now with two journeys in it (§3.1). These tests pin the rules that
 * would silently produce a confident wrong answer if they drifted:
 *
 *  - an unlinked member is never told the bot is in zero of their channels;
 *  - the org-admin coverage gauge is never counted as a completed step. */

const connected: SlackOnboardingConnected = {
  connected: true,
  workspace_id: "T1",
  workspace_name: "Acme",
  plane: "personal",
  channels: { state: "ok", count: 2, names: ["#platform", "#design"], error: null },
  routed: { count: 1, fanout: 0 },
  capture: { atom_count: 42, first_at: "2026-08-01T09:00:00Z", last_at: "2026-08-03T09:00:00Z" },
  huddles: { seen: 0, ingested: 0, skipped: 0, top_skip_reasons: [] },
  identity: null,
  viewer_role: "owner",
  member: null,
};

const linkedMember: SlackOnboardingMember = {
  linked: { linked: true, slack_user_id: "U1", link_method: "verified_email" },
  my_channels: {
    state: "ok",
    count: 2,
    containers: [
      { id: "C1", name: "engineering" },
      { id: "C2", name: "design" },
    ],
  },
  my_capture: { atom_count: 9, first_at: "2026-08-02T09:00:00Z", last_at: "2026-08-03T09:00:00Z" },
};

function orgMember(member: SlackOnboardingMember): SlackOnboardingConnected {
  return { ...connected, plane: "org", viewer_role: "member", identity: null, member };
}

function orgAdmin(overrides: Partial<SlackOnboardingConnected> = {}): SlackOnboardingConnected {
  return {
    ...connected,
    plane: "org",
    viewer_role: "admin",
    identity: { identified: 8, total: 12 },
    ...overrides,
  };
}

describe("deriveSteps — the org member branch", () => {
  it("gives a member their OWN three steps, not the admin's", () => {
    const rows = deriveSteps(orgMember(linkedMember));
    expect(rows.map((r) => r.kind)).toEqual(["step", "step", "step"]);
    const steps = rows.filter(isTaskStep);
    expect(steps.map((s) => s.name)).toEqual([
      "Link your Slack identity",
      "Get the bot into your channels",
      "First capture in your workspace",
    ]);
    // The admin's own step names must be nowhere near this list — a member
    // cannot "choose where it lands" for the workspace.
    expect(steps.map((s) => s.name)).not.toContain("Choose where it lands");
  });

  it("credits an email auto-match to the matcher, not to the member", () => {
    const [step1] = deriveSteps(orgMember(linkedMember)).filter(isTaskStep);
    expect(step1.state).toBe("done");
    expect(step1.status).toBe("linked automatically");

    // A link established some other way is just "linked" — the copy claims
    // only what the payload actually says.
    const manual = orgMember({
      ...linkedMember,
      linked: { linked: true, slack_user_id: "U1", link_method: "manual" },
    });
    expect(deriveSteps(manual).filter(isTaskStep)[0].status).toBe("linked");
  });

  it("counts only the member's OWN capture toward step 3", () => {
    const nothingYet = orgMember({
      ...linkedMember,
      my_capture: { atom_count: 0, first_at: null, last_at: null },
    });
    const step3 = deriveSteps(nothingYet).filter(isTaskStep)[2];
    expect(step3.state).toBe("pending");
    expect(step3.status).toBe("nothing yet");
    // …even though the ACCOUNT-wide capture on the same payload is 42. The
    // member's ocean is the only thing this step is about, so a busy
    // workspace must not tick a step off for someone nothing has reached.
    expect(nothingYet.capture.atom_count).toBe(42);
    expect(completedCount(nothingYet)).toBe(2); // the link and their channels, not capture
  });

  it("shows a linked member in no shared channels an honest, actionable zero", () => {
    const none = orgMember({ ...linkedMember, my_channels: { state: "ok", count: 0, containers: [] } });
    const step2 = deriveSteps(none).filter(isTaskStep)[1];
    expect(step2.state).toBe("active");
    expect(step2.status).toBe("not yet");
    // Members inviting the bot into their own channels IS the coverage model.
    expect(step2.showCommand).toBe(true);
  });

  it("carries the NAMED containers on step 2, and keeps absence distinct from empty", () => {
    // The `ok` arm carries the same list the count was derived from, so the
    // sentence and the list cannot disagree.
    const named = orgMember({
      ...linkedMember,
      my_channels: {
        state: "ok",
        count: 2,
        containers: [
          { id: "C1", name: "engineering" },
          { id: "C2", name: "design" },
        ],
      },
    });
    const step2 = deriveSteps(named).filter(isTaskStep)[1];
    expect(step2.state).toBe("done");
    expect(step2.containers?.map((c) => c.name)).toEqual(["engineering", "design"]);
    expect(step2.containers).toHaveLength(named.member!.my_channels.count!);

    // "We looked and it is in none of yours" is an EMPTY LIST, an answer.
    const none = orgMember({
      ...linkedMember,
      my_channels: { state: "ok", count: 0, containers: [] },
    });
    expect(deriveSteps(none).filter(isTaskStep)[1].containers).toEqual([]);

    // "We could not look" has NO list — not an empty one. Collapsing these
    // two is the absence-rendered-as-an-answer bug the three-way state and
    // the `unavailable` copy both exist to prevent; an empty list here would
    // render as "the bot is in none of your channels" when nobody looked.
    const unavailable = orgMember({
      ...linkedMember,
      my_channels: { state: "unavailable", count: null, containers: null },
    });
    expect(deriveSteps(unavailable).filter(isTaskStep)[1].containers).toBeUndefined();

    // Same for not-linked-yet: no question was asked, so there is no answer.
    const unlinked = orgMember({
      linked: { linked: false, slack_user_id: null, link_method: null },
      my_channels: { state: "unlinked", count: null, containers: null },
      my_capture: { atom_count: 0, first_at: null, last_at: null },
    });
    expect(deriveSteps(unlinked).filter(isTaskStep)[1].containers).toBeUndefined();
  });

  it("keeps the /invite affordance when the lookup failed, but not the count", () => {
    const unavailable = orgMember({
      ...linkedMember,
      my_channels: { state: "unavailable", count: null, containers: null },
    });
    const step2 = deriveSteps(unavailable).filter(isTaskStep)[1];
    expect(step2.state).toBe("unknown");
    expect(step2.status).toBe("couldn't check");
    // The command is still correct even when the number isn't.
    expect(step2.showCommand).toBe(true);
    // "unknown" is not "done": a failed lookup must never advance the reef.
    expect(completedCount(unavailable)).toBe(2); // link + own capture, not channels
  });

  it("makes the unlinked member's checklist a linking funnel", () => {
    // NEGATIVE CONTROL ANCHOR: drop the `unlinked` branch in
    // `deriveMemberSteps` and step 2 falls through to the count arm, reporting
    // "not yet" — i.e. "the bot is in none of your channels" — for someone
    // whose channels we never looked up. That sends a member to re-invite a
    // bot that may already be sitting in every channel they're in, and buries
    // the one thing actually blocking them.
    const unlinked = orgMember({
      linked: { linked: false, slack_user_id: null, link_method: null },
      my_channels: { state: "unlinked", count: null, containers: null },
      my_capture: { atom_count: 0, first_at: null, last_at: null },
    });
    const [s1, s2, s3] = deriveSteps(unlinked).filter(isTaskStep);

    expect(s1.state).toBe("active");
    expect(s1.status).toBe("start here");

    expect(s2.state).toBe("pending");
    expect(s2.status).toBe("link first");
    // No `/invite` here: inviting the bot changes nothing until we know who
    // they are, so offering the command would be busywork.
    expect(s2.showCommand).toBe(false);

    expect(s3.state).toBe("pending");
    expect(completedCount(unlinked)).toBe(0);
  });

  it("never hands a member the coverage gauge", () => {
    // Org-wide coverage is the admin's reading. A member's own link is step 1
    // of their list; the other eleven people are not their problem.
    const withCoverage: SlackOnboardingConnected = {
      ...orgMember(linkedMember),
      identity: { identified: 8, total: 12 },
    };
    expect(deriveSteps(withCoverage).some((r) => r.kind === "gauge")).toBe(false);
  });

  it("falls back to the account view when the member SECTION is absent", () => {
    // `viewer_role` alone is not enough — an older backend, or a personal
    // account whose role string somehow said "member", must not render a
    // member journey with nothing in it.
    const roleOnly: SlackOnboardingConnected = { ...connected, viewer_role: "member", member: null };
    expect(memberView(roleOnly)).toBe(false);
    expect(deriveSteps(roleOnly).filter(isTaskStep)[0].name).toBe("Invite the bot");
  });
});

describe("the coverage gauge is information, never a step", () => {
  it("appends a fourth row for an org admin and nobody else", () => {
    const admin = deriveSteps(orgAdmin());
    expect(admin).toHaveLength(4);
    expect(admin[3]).toMatchObject({ kind: "gauge", name: "Team coverage", status: "8 of 12 linked" });

    // Personal: no directory to cover.
    expect(deriveSteps(connected)).toHaveLength(3);
    // Org, but no coverage data at all.
    expect(deriveSteps(orgAdmin({ identity: null }))).toHaveLength(3);
    // Org member: covered above, restated here so the "and nobody else" half
    // of this test's name is actually asserted.
    expect(deriveSteps(orgMember(linkedMember))).toHaveLength(3);
  });

  it("is excluded from completedCount even at full coverage", () => {
    // NEGATIVE CONTROL ANCHOR: make the gauge a `kind: "step"` row with
    // `state: "done"` (or drop `isTaskStep` from `completedCount`) and this
    // reports 4 — a completion the user never earned, on a scale that only
    // goes to 3.
    const full = orgAdmin({ identity: { identified: 12, total: 12 } });
    expect(deriveSteps(full)).toHaveLength(4);
    expect(completedCount(full)).toBe(3);
    expect(completedCount(full)).toBeLessThanOrEqual(3);
  });

  it("leaves the topic mandala's 0-3 density semantics untouched", () => {
    const full = orgAdmin({ identity: { identified: 12, total: 12 } });
    // Same density an equivalently-complete PERSONAL account gets: the extra
    // row must not make an org admin's reef read differently from anyone
    // else's at the same real progress.
    expect(reefDensity(completedCount(full))).toBe(reefDensity(completedCount(connected)));
    expect(reefDensity(completedCount(full))).toBe(1);
  });

  it("reports zero coverage as a real zero, still not a step", () => {
    const none = orgAdmin({ identity: { identified: 0, total: 5 } });
    const rows = deriveSteps(none);
    expect(rows[3]).toMatchObject({ kind: "gauge", identified: 0, total: 5, status: "0 of 5 linked" });
    expect(completedCount(none)).toBe(3);
  });
});

describe("connectIsAdminOnly — the pre-connect state", () => {
  const notConnected: SlackOnboardingNotConnected = {
    connected: false,
    workspace_id: null,
    workspace_name: null,
    plane: "org",
    channels: null,
    routed: null,
    capture: null,
    huddles: null,
    identity: null,
    viewer_role: "member",
    member: null,
  };

  it("blocks the Connect button only for an org member with nothing connected", () => {
    expect(connectIsAdminOnly(notConnected)).toBe(true);

    // An org admin/owner keeps the button — they are the ones who can do it.
    expect(connectIsAdminOnly({ ...notConnected, viewer_role: "admin" })).toBe(false);
    expect(connectIsAdminOnly({ ...notConnected, viewer_role: "owner" })).toBe(false);
    // Personal: there is no admin to ask; you ARE the admin.
    expect(connectIsAdminOnly({ ...notConnected, plane: "personal", viewer_role: "owner" })).toBe(false);
    // Already connected: the question doesn't arise, and the tile shows
    // Disconnect rather than Connect anyway.
    expect(connectIsAdminOnly(orgMember(linkedMember))).toBe(false);
  });

  it("degrades to 'not blocked' when the status could not be read at all", () => {
    // The endpoint 404'd/403'd, or hasn't resolved yet. We cannot tell an
    // admin from a member without an answer, and hiding an admin's Connect
    // button on a guess is the worse failure.
    expect(connectIsAdminOnly(null)).toBe(false);
  });

  it("workspaceActsAreAdminOnly holds across BOTH connection states", () => {
    // The Disconnect button is the case connectIsAdminOnly deliberately
    // ignores: a connected org member must not get a Disconnect that
    // bounces off #531's server gate.
    expect(workspaceActsAreAdminOnly(notConnected)).toBe(true);
    expect(workspaceActsAreAdminOnly(orgMember(linkedMember))).toBe(true);

    expect(workspaceActsAreAdminOnly({ ...notConnected, viewer_role: "admin" })).toBe(false);
    expect(workspaceActsAreAdminOnly({ ...orgMember(linkedMember), viewer_role: "admin" })).toBe(false);
    expect(workspaceActsAreAdminOnly({ ...connected })).toBe(false); // personal
    expect(workspaceActsAreAdminOnly(null)).toBe(false);
  });

  it("reads the role off the NOT-connected shape, which is the whole point", () => {
    // If `viewer_role` were only present on the connected arm, this decision
    // — the one that only exists before anything is connected — would have
    // nothing to read. Pinned as a type-level fact too: the field is
    // non-optional on `SlackOnboardingNotConnected`.
    expect(notConnected.viewer_role).toBe("member");
  });
});

import { describe, expect, it } from "vitest";
import { describeMembershipEvent, leaveDialogDescription, personLabel, timeAgo } from "./teamEvents";
import type { MembershipEvent, TeamMemberRow, TeamRole } from "./api";

const alex: TeamMemberRow = { user_id: "u-alex", name: "Alex Kim", email: "alex@co.com", role: "admin", joined_at: "2026-01-01T00:00:00Z" };
const jo: TeamMemberRow = { user_id: "u-jo", name: null, email: "jo@co.com", role: "member", joined_at: "2026-01-02T00:00:00Z" };
const roster = [alex, jo];

function ev(overrides: Partial<MembershipEvent>): MembershipEvent {
  return {
    id: "e1",
    team_id: "t1",
    actor_user_id: "u-alex",
    target_user_id: "u-jo",
    action: "added",
    old_role: null,
    new_role: "member",
    created_at: "2026-08-05T00:00:00Z",
    ...overrides,
  };
}

describe("personLabel", () => {
  it("renders the caller's own id as 'You'", () => {
    expect(personLabel("u-alex", roster, "u-alex")).toBe("You");
  });

  it("falls back to email when a roster row has no name", () => {
    expect(personLabel("u-jo", roster, null)).toBe("jo@co.com");
  });

  it("prefers name over email when both are present", () => {
    expect(personLabel("u-alex", roster, null)).toBe("Alex Kim");
  });

  it("renders 'a former member' for an id no longer on the active roster", () => {
    expect(personLabel("u-departed", roster, null)).toBe("a former member");
  });
});

describe("describeMembershipEvent", () => {
  it("describes team creation", () => {
    const e = ev({ action: "created", target_user_id: "u-alex", new_role: null });
    expect(describeMembershipEvent(e, roster, null)).toBe("Alex Kim created this workspace");
  });

  it("describes an add with the assigned role", () => {
    const e = ev({ action: "added", new_role: "member" });
    expect(describeMembershipEvent(e, roster, null)).toBe("Alex Kim added jo@co.com as member");
  });

  it("describes a promotion distinctly from a generic role change", () => {
    const e = ev({ action: "role_changed", old_role: "member", new_role: "admin" });
    expect(describeMembershipEvent(e, roster, null)).toBe("Alex Kim promoted jo@co.com to admin");
  });

  it("describes a demotion distinctly", () => {
    const e = ev({ action: "role_changed", old_role: "admin", new_role: "member" });
    expect(describeMembershipEvent(e, roster, null)).toBe("Alex Kim demoted jo@co.com to member");
  });

  it("describes someone else being removed", () => {
    const e = ev({ action: "removed", actor_user_id: "u-alex", target_user_id: "u-jo" });
    expect(describeMembershipEvent(e, roster, null)).toBe("Alex Kim removed jo@co.com");
  });

  it("describes a self-removal as leaving, not as removing yourself", () => {
    const e = ev({ action: "removed", actor_user_id: "u-jo", target_user_id: "u-jo" });
    expect(describeMembershipEvent(e, roster, null)).toBe("jo@co.com left");
  });

  it("substitutes 'You' for the caller on either side", () => {
    const e = ev({ action: "role_changed", old_role: "member", new_role: "admin", actor_user_id: "u-alex", target_user_id: "u-jo" });
    expect(describeMembershipEvent(e, roster, "u-jo")).toBe("Alex Kim promoted You to admin");
    expect(describeMembershipEvent(e, roster, "u-alex")).toBe("You promoted jo@co.com to admin");
  });

  it("renders the automated offboard cascade as a system action, never a blank actor", () => {
    const e = ev({ action: "cascade_removed", actor_user_id: null, target_user_id: "u-jo" });
    expect(describeMembershipEvent(e, roster, null)).toBe(
      "jo@co.com was automatically removed — offboarded from the organization",
    );
  });

  it("never throws for a departed actor/target not on the active roster", () => {
    const e = ev({ actor_user_id: "u-gone", target_user_id: "u-also-gone" });
    expect(() => describeMembershipEvent(e, roster, null)).not.toThrow();
    expect(describeMembershipEvent(e, roster, null)).toContain("a former member");
  });

  it("describes a self-departure via the dedicated 'left' action", () => {
    const e = ev({ action: "left", actor_user_id: "u-jo", target_user_id: "u-jo", new_role: null });
    expect(describeMembershipEvent(e, roster, null)).toBe("jo@co.com left");
  });

  it("never renders the literal 'undefined' for a role value outside the known set", () => {
    // Simulates a role the backend added that this build's `TeamRole` type
    // doesn't know about yet (or malformed data) — `ROLE_WORD` has no entry
    // for it, and the fallback must be the raw value, not `undefined`.
    const e = ev({ action: "added", new_role: "owner" as unknown as TeamRole });
    const line = describeMembershipEvent(e, roster, null);
    expect(line).not.toContain("undefined");
    expect(line).toBe("Alex Kim added jo@co.com as owner");
  });

  describe("workspace-level events (archived/restored/renamed)", () => {
    // These three carry `target_user_id === actor_user_id` on the wire
    // (mirroring `created`) because there is no second party — the humanizer
    // must render them as one actor acting on the WORKSPACE, never as one
    // actor acting on themselves-as-a-target.
    const workspaceActions = ["archived", "restored", "renamed"] as const;

    it.each(workspaceActions)("describes a workspace '%s' event", (action) => {
      const e = ev({ action, actor_user_id: "u-alex", target_user_id: "u-alex", new_role: null, old_role: null });
      expect(describeMembershipEvent(e, roster, null)).toBe(`Alex Kim ${action} this workspace`);
    });

    it.each(workspaceActions)(
      "never phrases a '%s' event as one person acting on another (property: no repeated actor-as-target)",
      (action) => {
        const e = ev({ action, actor_user_id: "u-alex", target_user_id: "u-alex", new_role: null, old_role: null });
        const line = describeMembershipEvent(e, roster, null);
        // A person-directed phrasing (the bug this guards against) would read
        // something like "Alex Kim removed Alex Kim" or "Alex Kim changed
        // Alex Kim's role" — the actor's own identity appearing a SECOND time
        // as an object. Strip the leading actor mention and assert it never
        // recurs.
        const afterActor = line.replace(/^Alex Kim\s*/, "");
        expect(afterActor).not.toContain("Alex Kim");
        expect(afterActor).not.toMatch(/'s\s/); // no possessive aimed at a "target"
      },
    );

    it("substitutes 'You' for the caller on a workspace-level event same as a person-level one", () => {
      const e = ev({ action: "archived", actor_user_id: "u-alex", target_user_id: "u-alex", new_role: null, old_role: null });
      expect(describeMembershipEvent(e, roster, "u-alex")).toBe("You archived this workspace");
    });
  });
});

describe("leaveDialogDescription", () => {
  it("warns a sole admin up front that they can't leave, instead of the generic copy", () => {
    const line = leaveDialogDescription(true);
    expect(line).toBe("You're the only admin. Promote another member to admin before you can leave.");
    // Must not contain the generic copy's "another admin can add you back"
    // promise — for a sole admin there IS no other admin to do that.
    expect(line).not.toContain("Another admin can add you back later");
  });

  it("keeps the original copy for a non-sole-admin leave", () => {
    expect(leaveDialogDescription(false)).toBe(
      "You'll lose access to its topics, items, and sources right away. Another admin can add you back later.",
    );
  });
});

describe("timeAgo", () => {
  const now = new Date("2026-08-05T12:00:00Z");

  it("renders sub-minute deltas as 'just now'", () => {
    expect(timeAgo("2026-08-05T11:59:45Z", now)).toBe("just now");
  });

  it("renders minutes", () => {
    expect(timeAgo("2026-08-05T11:55:00Z", now)).toBe("5m ago");
  });

  it("renders hours", () => {
    expect(timeAgo("2026-08-05T10:00:00Z", now)).toBe("2h ago");
  });

  it("renders days", () => {
    expect(timeAgo("2026-08-02T12:00:00Z", now)).toBe("3d ago");
  });

  it("never throws on an unparseable timestamp", () => {
    expect(timeAgo("not-a-date", now)).toBe("");
  });
});

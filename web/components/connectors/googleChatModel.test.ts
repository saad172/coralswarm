import { describe, expect, it } from "vitest";
import type { GoogleChatSpace, Graph } from "@/lib/api";
import {
  containerDestinationLabel,
  containerSortWeight,
  containerSpaceStatus,
  emptySpacesCopy,
  isMailboxOptinActive,
  isPickable,
  notConnectedCopy,
  profileSharingInstruction,
  pickFailedActionable,
  singleDestinationOceanName,
  sortContainerSpaces,
  splitByArchetype,
} from "./googleChatModel";

/* Pins the decisions `googleChatModel.ts` exists to get right — see that
 * module's own doc comment for the three named traps each block below
 * guards against. */

const ocean: Graph = {
  id: "ocean-1",
  name: "Engineering",
  kind: "team",
  is_primary: false,
  created_at: "2026-09-01T00:00:00Z",
  team_id: "team-1",
  my_team_role: "admin",
};

function space(overrides: Partial<GoogleChatSpace>): GoogleChatSpace {
  return {
    space_id: "AAA",
    display_name: "eng-platform",
    space_type: "SPACE",
    history_state: "HISTORY_ON",
    ...overrides,
  };
}

describe("containerSpaceStatus — trap #1 (a listed-but-never-added space must never read as ready)", () => {
  it("an app never added to the space is needs_app", () => {
    expect(containerSpaceStatus({ kind: "not_connected" })).toBe("needs_app");
  });

  it("missing destination entirely (e.g. a mailbox row passed by mistake) is also needs_app, not a crash", () => {
    expect(containerSpaceStatus(undefined)).toBe("needs_app");
  });

  it("app added, no pick yet, is unrouted", () => {
    expect(containerSpaceStatus({ kind: "unrouted" })).toBe("unrouted");
  });

  it("a single-ocean pick is routed_single", () => {
    expect(containerSpaceStatus({ kind: "single", graph_id: "ocean-1" })).toBe("routed_single");
  });

  it("a fanout pick is routed_fanout", () => {
    expect(containerSpaceStatus({ kind: "fanout" })).toBe("routed_fanout");
  });
});

describe("isPickable — trap #2 (needs_app) and trap #3 (already-routed no-ops)", () => {
  it("only unrouted is pickable", () => {
    expect(isPickable("unrouted")).toBe(true);
    expect(isPickable("needs_app")).toBe(false);
    expect(isPickable("routed_single")).toBe(false);
    expect(isPickable("routed_fanout")).toBe(false);
  });
});

describe("containerSortWeight / sortContainerSpaces — unrouted first, needs_app second, routed last", () => {
  it("orders by actionability, then by name", () => {
    const routed = space({ space_id: "C", display_name: "zzz-routed", destination: { kind: "single", graph_id: "ocean-1" } });
    const needsApp = space({ space_id: "A", display_name: "bbb-needs-app", destination: { kind: "not_connected" } });
    const unroutedB = space({ space_id: "B", display_name: "bbb-unrouted", destination: { kind: "unrouted" } });
    const unroutedA = space({ space_id: "D", display_name: "aaa-unrouted", destination: { kind: "unrouted" } });

    const sorted = sortContainerSpaces([routed, needsApp, unroutedB, unroutedA]);

    expect(sorted.map((s) => s.space_id)).toEqual(["D", "B", "A", "C"]);
  });

  it("weight order is unrouted < needs_app < routed", () => {
    expect(containerSortWeight("unrouted")).toBeLessThan(containerSortWeight("needs_app"));
    expect(containerSortWeight("needs_app")).toBeLessThan(containerSortWeight("routed_single"));
    expect(containerSortWeight("routed_single")).toBe(containerSortWeight("routed_fanout"));
  });
});

describe("splitByArchetype — container vs mailbox never overlap", () => {
  it("splits SPACE from GROUP_CHAT/DIRECT_MESSAGE", () => {
    const s1 = space({ space_id: "S", space_type: "SPACE" });
    const g1 = space({ space_id: "G", space_type: "GROUP_CHAT", destination: undefined, mailbox_optin: { opted_in: false } });
    const d1 = space({ space_id: "D", space_type: "DIRECT_MESSAGE", destination: undefined, mailbox_optin: { opted_in: false } });

    const { containers, mailboxes } = splitByArchetype([s1, g1, d1]);
    expect(containers.map((s) => s.space_id)).toEqual(["S"]);
    expect(mailboxes.map((s) => s.space_id)).toEqual(["G", "D"]);
  });
});

describe("singleDestinationOceanName / containerDestinationLabel — never claim more than the payload backs", () => {
  it("resolves a known graph_id to its name", () => {
    expect(singleDestinationOceanName({ kind: "single", graph_id: "ocean-1" }, [ocean])).toBe("Engineering");
  });

  it("falls back to null for an unresolvable graph_id, never a guess", () => {
    expect(singleDestinationOceanName({ kind: "single", graph_id: "gone" }, [ocean])).toBeNull();
  });

  it("returns null for any non-single destination", () => {
    expect(singleDestinationOceanName({ kind: "unrouted" }, [ocean])).toBeNull();
    expect(singleDestinationOceanName({ kind: "fanout" }, [ocean])).toBeNull();
    expect(singleDestinationOceanName(undefined, [ocean])).toBeNull();
  });

  it("labels every status distinctly", () => {
    expect(containerDestinationLabel({ kind: "not_connected" }, [ocean])).toBe("add the app first");
    expect(containerDestinationLabel({ kind: "unrouted" }, [ocean])).toBe("not routed yet");
    expect(containerDestinationLabel({ kind: "fanout" }, [ocean])).toBe("everyone's individual workspaces");
    expect(containerDestinationLabel({ kind: "single", graph_id: "ocean-1" }, [ocean])).toBe("Engineering");
  });

  it("labels an unresolvable single pick generically rather than crashing", () => {
    expect(containerDestinationLabel({ kind: "single", graph_id: "gone" }, [ocean])).toBe("a workspace");
  });
});

describe("isMailboxOptinActive — retained opt-out rows are inactive", () => {
  it("is false when mailbox_optin is missing", () => {
    expect(isMailboxOptinActive(undefined)).toBe(false);
  });

  it("is false for never-opted-in", () => {
    expect(isMailboxOptinActive({ opted_in: false })).toBe(false);
  });

  it("is false for a retained opt-out row ({ opted_in: true, active: false })", () => {
    expect(isMailboxOptinActive({ opted_in: true, active: false, graph_id: "ocean-1" })).toBe(false);
  });

  it("is true only when both opted_in and active are true", () => {
    expect(isMailboxOptinActive({ opted_in: true, active: true, graph_id: "ocean-1" })).toBe(true);
  });
});

describe("pickFailedActionable — trap #2's 404 interception", () => {
  it("is true only for 404 (covers both NotConnected and GraphNotOwned)", () => {
    expect(pickFailedActionable(404)).toBe(true);
  });

  it("is false for every other status, including null (network failure)", () => {
    expect(pickFailedActionable(500)).toBe(false);
    expect(pickFailedActionable(403)).toBe(false);
    expect(pickFailedActionable(null)).toBe(false);
  });
});

describe("empty/not-connected copy — the state most people see first", () => {
  it("teaches the connect-then-add ordering, not just 'no spaces found'", () => {
    const { hint } = emptySpacesCopy();
    expect(hint.toLowerCase()).toContain("add the coralswarm app");
  });

  it("not-connected copy points back to the connect step, distinctly from the empty-spaces copy", () => {
    const empty = emptySpacesCopy();
    const notConnected = notConnectedCopy();
    expect(notConnected.title).not.toBe(empty.title);
    expect(notConnected.hint.toLowerCase()).toContain("connect google chat");
  });
});

describe("profileSharingInstruction — plan item 35, shown in the Chat connect flow", () => {
  it("tells a Workspace admin to turn on external contact and profile sharing, and where", () => {
    const { label, body } = profileSharingInstruction();
    expect(label.toLowerCase()).toContain("workspace admin");
    expect(body.toLowerCase()).toContain("external contact and profile sharing");
    expect(body).toContain("Admin console");
    expect(body).toContain("External directory sharing");
    expect(body.toLowerCase()).toContain("contact sharing");
  });

  it("does not tell the member that reconnecting will fix a directory the admin has not shared", () => {
    const { body } = profileSharingInstruction();
    expect(body.toLowerCase()).toContain("reconnecting here will not");
    expect(body.toLowerCase()).not.toContain("add the coralswarm app");
  });
});

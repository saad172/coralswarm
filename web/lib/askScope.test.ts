// Coverage for workspace classification and the Ask model/effort options.
//
// `isOwnerOnlyOcean` is display grouping only (the Workspaces index, badges);
// it must never gate what an ask can read. Chats always asks across every
// readable workspace (`lib/scopedChatScope.ts`), so there is no workspace
// picker or scope codec left here to test.
//
// This file stays deliberately exhaustive on classification (personal-primary
// vs member-primary vs team vs org vs secondary/non-primary of each kind),
// since these pure functions are where the decision actually lives.

import { describe, expect, it } from "vitest";
import type { Graph } from "./api";
import {
  DEFAULT_EFFORT,
  effortAppliesToModel,
  effortForWire,
  EFFORT_LEVELS,
  isEffortLevel,
  isOwnerOnlyOcean,
  oceanBadge,
  type EffortLevel,
} from "./askScope";

function g(over: Partial<Graph> & Pick<Graph, "id" | "kind">): Graph {
  return {
    name: over.id,
    is_primary: false,
    created_at: "2026-01-01T00:00:00Z",
    team_id: null,
    my_team_role: null,
    ...over,
  };
}

// ── Fixtures ──────────────────────────────────────────────────────────────────
// Consumer plane: one account, a personal primary + a secondary personal.
const personalPrimary = g({ id: "p1", kind: "personal", is_primary: true, name: "Personal" });
const personalSecondary = g({ id: "p2", kind: "personal", is_primary: false });

// Org plane: org commons, a team ocean, this member's primary + a secondary
// member ocean (allowed — member-private-oceans design §5).
const orgOcean = g({ id: "org1", kind: "org", is_primary: false });
const teamOcean = g({ id: "team1", kind: "team", is_primary: false });
const memberPrimary = g({ id: "m1", kind: "member", is_primary: true });
const memberSecondary = g({ id: "m2", kind: "member", is_primary: false });

describe("isOwnerOnlyOcean", () => {
  it.each([
    ["personal primary", personalPrimary, true],
    ["personal secondary", personalSecondary, false],
    ["member primary", memberPrimary, true],
    // The asymmetry that matters: EVERY member ocean is owner-only, primary or
    // not — the backend gates them on `owner_user_id`, never on `is_primary`.
    ["member secondary", memberSecondary, true],
    ["team", teamOcean, false],
    ["org", orgOcean, false],
  ])("%s → %s", (_label, graph, expected) => {
    expect(isOwnerOnlyOcean(graph)).toBe(expected);
  });
});

describe("oceanBadge", () => {
  it.each([
    ["personal primary", personalPrimary, "personal"],
    // `is_primary` used to win here and render "primary" — internal
    // vocabulary that said nothing about who can see the ocean.
    ["personal secondary", personalSecondary, "personal"],
    ["member primary", memberPrimary, "member"],
    ["member secondary", memberSecondary, "member"],
    ["team", teamOcean, "team"],
    ["org", orgOcean, "org"],
  ])("%s → %s", (_label, graph, expected) => {
    expect(oceanBadge(graph)).toBe(expected);
  });
});

// ── Reasoning effort (two-model-world amendment, 2026-09-03) ──────────────────
// The composer only SHOWS its effort dial for Smart: Fast never reasons (no
// dial, backend always resolves no effort) and Max is pinned server-side to
// `high` (dial hidden, not adjustable). These pure functions are what that
// decision actually runs through — `useChatThread`'s persistence effects and
// `ChatComposer`'s collapse branch both call them directly, and neither is
// otherwise reachable by a test in this repo (no component-test harness — see
// the file header).

describe("effortAppliesToModel / effortForWire", () => {
  it("is true only for Smart — Fast never reasons, Max is pinned", () => {
    expect(effortAppliesToModel("fast")).toBe(false);
    expect(effortAppliesToModel("smart")).toBe(true);
    expect(effortAppliesToModel("max")).toBe(false);
  });

  it("puts every level on the wire for Smart", () => {
    for (const level of EFFORT_LEVELS.map((e) => e.key)) {
      expect(effortForWire("smart", level)).toBe(level);
    }
  });

  it("drops the level for Fast and Max instead of sending a field known to be inert", () => {
    for (const level of EFFORT_LEVELS.map((e) => e.key)) {
      expect(effortForWire("fast", level)).toBeUndefined();
      expect(effortForWire("max", level)).toBeUndefined();
    }
  });
});

describe("isEffortLevel", () => {
  it("accepts exactly the four ladder rungs", () => {
    for (const level of EFFORT_LEVELS.map((e) => e.key)) {
      expect(isEffortLevel(level)).toBe(true);
    }
    expect(EFFORT_LEVELS.map((e) => e.key)).toEqual(["off", "low", "medium", "high"]);
  });

  it("rejects anything else, including null/undefined and stale values", () => {
    expect(isEffortLevel(null)).toBe(false);
    expect(isEffortLevel(undefined)).toBe(false);
    expect(isEffortLevel("")).toBe(false);
    expect(isEffortLevel("ultra")).toBe(false);
    // Retired legacy rungs (pre-two-model-world) must not be silently
    // accepted by a stale localStorage value.
    expect(isEffortLevel("xhigh")).toBe(false);
    expect(isEffortLevel("max")).toBe(false);
    // Not a real level and never was — guards against a typo'd migration or a
    // hand-edited localStorage value silently being accepted.
    expect(isEffortLevel("HIGH")).toBe(false);
  });
});

// `useChatThread.ts`'s persistence effects are `localStorage.getItem(...)`
// gated by `isEffortLevel`, then `localStorage.setItem(...)` on change —
// exactly `MODEL_PREF_KEY`'s existing shape, with a sibling key. That can't be
// exercised through a rendered hook here (no component-test harness), so this
// drives the same `Storage`-shaped contract the hook actually calls against a
// minimal in-memory fake, proving the read-validate-fallback round trip the
// hook relies on.
class FakeStorage {
  private store = new Map<string, string>();
  getItem(key: string): string | null {
    return this.store.get(key) ?? null;
  }
  setItem(key: string, value: string): void {
    this.store.set(key, value);
  }
}

function restoreEffort(storage: FakeStorage, key: string): EffortLevel {
  const saved = storage.getItem(key);
  return isEffortLevel(saved) ? saved : DEFAULT_EFFORT;
}

describe("effort preference round trip (useChatThread's cs_ask_effort contract)", () => {
  const KEY = "cs_ask_effort";

  it("round-trips every saved level back to itself", () => {
    const storage = new FakeStorage();
    for (const level of EFFORT_LEVELS.map((e) => e.key)) {
      storage.setItem(KEY, level);
      expect(restoreEffort(storage, KEY)).toBe(level);
    }
  });

  it("falls back to the documented default (medium) when nothing was ever saved", () => {
    const storage = new FakeStorage();
    expect(restoreEffort(storage, KEY)).toBe("medium");
    expect(restoreEffort(storage, KEY)).toBe(DEFAULT_EFFORT);
  });

  it("falls back to the default rather than trusting a corrupted value", () => {
    const storage = new FakeStorage();
    storage.setItem(KEY, "ultra");
    expect(restoreEffort(storage, KEY)).toBe(DEFAULT_EFFORT);
  });
});

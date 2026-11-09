import { describe, expect, it } from "vitest";
import { ApiError, type SlotPoolResult } from "@/lib/api";
import {
  inviteStopsBatch,
  isNoFreeSeat,
  mapAddMemberError,
  seatsAddedNote,
  seatsAfterRefusal,
  slotPlan,
  validateSingleEmail,
} from "@/lib/orgInvites";

function pool(over: Partial<SlotPoolResult> & { standardAvailable?: number } = {}): SlotPoolResult {
  const { standardAvailable, ...rest } = over;
  return {
    purchased: 5,
    used: 2,
    reserved: 0,
    available: 3,
    tier_breakdown: [{ tier: "standard", seats: 5 }],
    tiers: [
      { code: "standard", purchased: 5, used: 2, reserved: 0, available: standardAvailable ?? 3 },
      { code: "premium", purchased: 4, used: 0, reserved: 0, available: 4 },
    ],
    ...rest,
  };
}

describe("slotPlan", () => {
  it("reads the standard tier, which is where every admin invite lands", () => {
    // Account-wide `available` would be 7 here; only 1 standard slot is free.
    const plan = slotPlan(3, pool({ available: 7, standardAvailable: 1 }));
    expect(plan).toEqual({ kind: "known", free: 1, fromPool: 1, toBuy: 2 });
  });

  it("buys nothing when the pool covers every invite", () => {
    expect(slotPlan(2, pool())).toEqual({ kind: "known", free: 3, fromPool: 2, toBuy: 0 });
  });

  it("buys one slot per invite when the pool is empty", () => {
    expect(slotPlan(2, pool({ standardAvailable: 0 }))).toEqual({ kind: "known", free: 0, fromPool: 0, toBuy: 2 });
  });

  it("clamps a negative availability (over-reserved pool) to zero", () => {
    expect(slotPlan(1, pool({ standardAvailable: -2 }))).toEqual({ kind: "known", free: 0, fromPool: 0, toBuy: 1 });
  });

  it("falls back to the account-wide number when no standard tier is listed", () => {
    expect(slotPlan(4, pool({ tiers: [], available: 1 }))).toEqual({ kind: "known", free: 1, fromPool: 1, toBuy: 3 });
  });

  it("says nothing is bought when the org has no paid pool at all", () => {
    // No subscription yet: `acquire_seat` assigns a tier and charges nothing.
    expect(slotPlan(2, pool({ purchased: 0, available: 0, standardAvailable: 0 }))).toEqual({ kind: "unbilled" });
  });

  it("is unknown, not zero, while the pool has not loaded", () => {
    expect(slotPlan(2, null)).toEqual({ kind: "unknown" });
  });
});

describe("validateSingleEmail", () => {
  it.each(["dana@acme.com", "  dana@acme.com  "])("accepts %s", (raw) => {
    expect(validateSingleEmail(raw)).toBeNull();
  });
  it.each(["dana", "@acme.com", "dana@", "a@b@c"])("rejects %s", (raw) => {
    expect(validateSingleEmail(raw)).toMatch(/doesn't look like an email/);
  });
  it("rejects a pasted list with the one-at-a-time copy", () => {
    expect(validateSingleEmail("a@x.com, b@x.com")).toMatch(/one address at a time/i);
  });
});

describe("inviteStopsBatch", () => {
  it("keeps going past problems with one address", () => {
    expect(inviteStopsBatch(new ApiError("already_a_member", 409))).toBe(false);
    expect(inviteStopsBatch(new ApiError("invalid email", 400))).toBe(false);
  });

  it.each([
    ["seat_purchase_failed", 402],
    ["billing_unavailable", 502],
    ["clerk_error", 502],
    ["service unavailable", 503],
    ["only an organization admin can invite", 403],
  ])("stops on %s (%i): the next invite would fail the same way", (message, status) => {
    expect(inviteStopsBatch(new ApiError(message, status))).toBe(true);
  });

  it("stops on a network failure", () => {
    expect(inviteStopsBatch(new TypeError("Failed to fetch"))).toBe(true);
  });
});

describe("mapAddMemberError", () => {
  it("names a declined card", () => {
    expect(mapAddMemberError(new ApiError("seat_purchase_failed", 402, null, "card_declined"))).toMatch(/declined/);
  });
  it("names a non-admin caller instead of a generic failure", () => {
    expect(mapAddMemberError(new ApiError("caller is not an org admin", 403))).toMatch(/only an org admin/i);
  });
  it("has copy for a 409 no_free_seat the caller could not turn into a quote", () => {
    expect(mapAddMemberError(new ApiError("no free seat", 409, "no_free_seat"))).toMatch(/every seat is taken/i);
  });
  it("does not call a Clerk 4xx about the address an outage", () => {
    expect(mapAddMemberError(new ApiError("clerk_error", 422, "form_param_format_invalid"))).toMatch(/rejected that address/);
  });
  it("still calls a Clerk 5xx an outage", () => {
    expect(mapAddMemberError(new ApiError("clerk_error", 502, "internal_clerk_error"))).toMatch(/briefly unavailable/);
  });
  it("keeps the existing copy for an existing member", () => {
    expect(mapAddMemberError(new ApiError("already_a_member", 409))).toBe("That address already holds a slot here.");
  });
  it("names the browser-session rule instead of calling an admin a non-admin", () => {
    const e = new ApiError("this action needs you signed in to CoralSwarm in a browser", 403, "interactive_session_required");
    expect(mapAddMemberError(e)).toMatch(/signed in to CoralSwarm in a browser/);
  });
  it("keeps the plain admin-only copy for every other 403", () => {
    expect(mapAddMemberError(new ApiError("admin only", 403))).toBe("Only an org admin can invite people.");
  });
});

describe("no_free_seat", () => {
  it("is recognised by status and code, not the message", () => {
    expect(isNoFreeSeat(new ApiError("no free seat", 409, "no_free_seat"))).toBe(true);
    expect(isNoFreeSeat(new ApiError("already_a_member", 409))).toBe(false);
    expect(isNoFreeSeat(new ApiError("no free seat", 400, "no_free_seat"))).toBe(false);
    expect(isNoFreeSeat(new TypeError("Failed to fetch"))).toBe(false);
  });
  it("stops a batch", () => {
    expect(inviteStopsBatch(new ApiError("no free seat", 409, "no_free_seat"))).toBe(true);
  });
});

describe("seatsAfterRefusal", () => {
  it("prices what the fresh pool says is missing", () => {
    expect(seatsAfterRefusal(3, pool({ standardAvailable: 1 }))).toEqual({ toBuy: 2, free: 1 });
  });
  it("never prices zero seats after the server refused, even if the fresh count still shows room", () => {
    expect(seatsAfterRefusal(1, pool({ standardAvailable: 2 }))).toEqual({ toBuy: 1, free: 2 });
  });
  it("prices one seat per invite when the pool can't be read", () => {
    expect(seatsAfterRefusal(2, null)).toEqual({ toBuy: 2, free: 0 });
  });
});

describe("seatsAddedNote", () => {
  it("reads naturally for one and many", () => {
    expect(seatsAddedNote(1)).toBe("1 seat was added; sending again will use it.");
    expect(seatsAddedNote(3)).toBe("3 seats were added; sending again will use them.");
  });
});

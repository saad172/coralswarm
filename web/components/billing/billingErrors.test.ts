import { describe, expect, it } from "vitest";
import { ApiError } from "@/lib/api";
import { billingErrorMessage } from "./billingErrors";

const CODES = [
  "no_billing_account", "portal_not_configured", "billing_unavailable", "no_subscription", "subscription_inactive",
  "seats_in_use", "admin_required", "plan_required", "credits_frozen", "extra_usage_off", "limit_out_of_range",
  "amount_not_whole_cents", "member_cap_not_applicable", "member_cap_out_of_range", "auto_reload_requires_extra_usage",
  "auto_reload_incomplete", "auto_reload_amount_out_of_range", "auto_reload_threshold_out_of_range",
  "auto_reload_threshold_exceeds_amount", "auto_reload_exceeds_limit", "auto_reload_requires_card", "topup_amount_out_of_range", "return_urls_required",
];

describe("billingErrorMessage", () => {
  it.each(CODES)("has plain copy for %s that never echoes the server text", (code) => {
    const msg = billingErrorMessage(new ApiError("RAW SERVER TEXT", 400, code), "fallback");
    expect(msg).not.toBe("fallback");
    expect(msg).not.toContain("RAW SERVER TEXT");
    expect(msg).not.toContain(code);
    expect(msg).not.toMatch(/ocean|reef|coral/i);
  });

  it("points a bank-account customer at Manage billing to add a card", () => {
    expect(billingErrorMessage(new ApiError("x", 409, "auto_reload_requires_card"), "f")).toMatch(/Manage billing/);
  });

  it("says how many seats are free when seats are in use", () => {
    const e = new ApiError("x", 409, "seats_in_use");
    e.available = 2;
    expect(billingErrorMessage(e, "f")).toMatch(/Only 2 seats are free/);
    e.available = 0;
    expect(billingErrorMessage(e, "f")).toMatch(/Every seat is in use/);
  });

  it("understands the older seat routes that send no code", () => {
    expect(billingErrorMessage(new ApiError("seat_purchase_failed", 402, null, "card_declined"), "f")).toMatch(/declined/);
    expect(billingErrorMessage(new ApiError("not_enough_free_slots", 409), "f")).toMatch(/in use/);
    expect(billingErrorMessage(new ApiError("quantity must be between 1 and 500", 400), "f")).toMatch(/1 and 500/);
  });

  it("falls back for unknown failures", () => {
    expect(billingErrorMessage(new Error("boom"), "fallback")).toBe("fallback");
    expect(billingErrorMessage(new ApiError("weird", 500), "fallback")).toBe("fallback");
    expect(billingErrorMessage(new ApiError("x", 403), "fallback")).toMatch(/Only an admin/);
  });
});

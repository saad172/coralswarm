import { describe, expect, it } from "vitest";
import type { BillingExtraUsage } from "@/lib/api";
import { buildExtraUsageUpdate, fieldForErrorCode, formFromExtraUsage, validateTopupAmount } from "./extraUsageModel";

const OFF: BillingExtraUsage = {
  enabled: false,
  balance_micro_usd: 0,
  monthly_limit_micro_usd: null,
  spent_this_period_micro_usd: 0,
  member_cap_seat_multiple: null,
  auto_reload: { enabled: false, threshold_micro_usd: null, amount_micro_usd: null },
};

const form = (patch = {}) => ({ ...formFromExtraUsage(OFF), enabled: true, ...patch });

describe("buildExtraUsageUpdate", () => {
  it("sends only enabled:false when turning off", () => {
    expect(buildExtraUsageUpdate(form({ enabled: false, limit: "garbage" }), { isOrg: true })).toEqual({
      ok: true,
      body: { enabled: false },
    });
  });

  it("omits blank fields so the server keeps or defaults them", () => {
    expect(buildExtraUsageUpdate(form(), { isOrg: true })).toEqual({
      ok: true,
      body: { enabled: true, auto_reload: { enabled: false } },
    });
  });

  it("converts dollars to whole-cent micro-USD and sends the cap for orgs only", () => {
    const f = form({ limit: "150.25", memberCap: "1.5" });
    expect(buildExtraUsageUpdate(f, { isOrg: true })).toEqual({
      ok: true,
      body: {
        enabled: true,
        monthly_limit_micro_usd: 150_250_000,
        member_cap_seat_multiple: 1.5,
        auto_reload: { enabled: false },
      },
    });
    const personal = buildExtraUsageUpdate(f, { isOrg: false });
    expect(personal.ok && "member_cap_seat_multiple" in personal.body).toBe(false);
  });

  it("validates the limit", () => {
    const r = buildExtraUsageUpdate(form({ limit: "0.50" }), { isOrg: false });
    expect(r.ok).toBe(false);
    expect(!r.ok && r.errors.limit).toMatch(/between \$1 and \$100,000/);
    const frac = buildExtraUsageUpdate(form({ limit: "5.555" }), { isOrg: false });
    expect(!frac.ok && frac.errors.limit).toMatch(/whole cents/);
  });

  it("validates the member cap", () => {
    const r = buildExtraUsageUpdate(form({ memberCap: "500" }), { isOrg: true });
    expect(!r.ok && r.errors.memberCap).toMatch(/between 0.1 and 100/);
  });

  it("requires both auto-reload fields", () => {
    const r = buildExtraUsageUpdate(form({ reloadEnabled: true, reloadThreshold: "5" }), { isOrg: false });
    expect(!r.ok && r.errors.reloadAmount).toMatch(/needs a reload amount/);
  });

  it("checks auto-reload bounds, ordering and the monthly limit", () => {
    const tooSmall = buildExtraUsageUpdate(form({ reloadEnabled: true, reloadThreshold: "5", reloadAmount: "2" }), { isOrg: false });
    expect(!tooSmall.ok && tooSmall.errors.reloadAmount).toMatch(/between \$5 and \$1,000/);
    const inverted = buildExtraUsageUpdate(form({ reloadEnabled: true, reloadThreshold: "50", reloadAmount: "20" }), { isOrg: false });
    expect(!inverted.ok && inverted.errors.reloadThreshold).toMatch(/can't be higher/);
    const pastLimit = buildExtraUsageUpdate(
      form({ limit: "30", reloadEnabled: true, reloadThreshold: "5", reloadAmount: "40" }),
      { isOrg: false },
    );
    expect(!pastLimit.ok && pastLimit.errors.reloadAmount).toMatch(/monthly limit/);
  });

  it("builds a complete auto-reload body", () => {
    const r = buildExtraUsageUpdate(form({ reloadEnabled: true, reloadThreshold: "5", reloadAmount: "25" }), { isOrg: false });
    expect(r).toEqual({
      ok: true,
      body: { enabled: true, auto_reload: { enabled: true, threshold_micro_usd: 5_000_000, amount_micro_usd: 25_000_000 } },
    });
  });
});

describe("top-up amount", () => {
  it("accepts $5 to $1,000 in whole cents", () => {
    expect(validateTopupAmount("5")).toEqual({ ok: true, micro: 5_000_000 });
    expect(validateTopupAmount("1,000")).toEqual({ ok: true, micro: 1_000_000_000 });
    expect(validateTopupAmount("4.99").ok).toBe(false);
    expect(validateTopupAmount("1000.01").ok).toBe(false);
    expect(validateTopupAmount("").ok).toBe(false);
  });
});

describe("fieldForErrorCode", () => {
  it("places server validation codes under their input", () => {
    expect(fieldForErrorCode("limit_out_of_range")).toBe("limit");
    expect(fieldForErrorCode("member_cap_out_of_range")).toBe("memberCap");
    expect(fieldForErrorCode("auto_reload_exceeds_limit")).toBe("reloadAmount");
    expect(fieldForErrorCode("auto_reload_threshold_exceeds_amount")).toBe("reloadThreshold");
    expect(fieldForErrorCode("admin_required")).toBeNull();
  });
});

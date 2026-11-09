import { describe, expect, it } from "vitest";
import { FALLBACK_TIERS, capitalizeTierCode, resolveTierCode, tierOptionLabel } from "./seatTierOptions";

describe("capitalizeTierCode", () => {
  it("capitalises a bare seat_tiers.code", () => {
    expect(capitalizeTierCode("standard")).toBe("Standard");
  });

  it("empty string stays empty", () => {
    expect(capitalizeTierCode("")).toBe("");
  });
});

describe("tierOptionLabel", () => {
  it("no price known — just the capitalised code", () => {
    expect(tierOptionLabel({ code: "standard", price_usd_cents: null })).toBe("Standard");
  });

  it("prints a known price", () => {
    expect(tierOptionLabel({ code: "pro", price_usd_cents: 500 })).toBe("Pro — $5.00");
  });
});

describe("resolveTierCode — the guard carried from JoinRequestQueue.tsx's own tierForRow (CodeRabbit, PR #1027)", () => {
  const tiers = [
    { code: "standard", price_usd_cents: null },
    { code: "pro", price_usd_cents: 500 },
  ];

  it("keeps a desired code that IS one of the fetched options", () => {
    expect(resolveTierCode(tiers, "pro")).toBe("pro");
  });

  it("falls back to the fetched 'standard' entry when nothing is desired yet", () => {
    expect(resolveTierCode(tiers, undefined)).toBe("standard");
  });

  /** THE bug this guard exists for (FIX 5, code review on PR #1041):
   *  `AutoAddSetting.tsx`'s `settings.tier` is a SERVER value that can name
   *  a tier the currently fetched list no longer contains (deactivated,
   *  renamed) — unlike `JoinRequestQueue.tsx`'s own `rowTier` map, which a
   *  controlled `<select>` can only ever set to an option that already
   *  existed. Passed straight to `<Select value={...}>`, that would render
   *  the browser's own first-option fallback while the component's state
   *  still held the stale code — visible selection and saved value silently
   *  disagreeing. */
  it("falls back when the desired code is NOT among the fetched options, rather than passing it through", () => {
    expect(resolveTierCode(tiers, "enterprise")).toBe("standard");
  });

  it("falls back to the first fetched option when the list has no 'standard' entry at all", () => {
    const noStandard = [{ code: "pro", price_usd_cents: 500 }];
    expect(resolveTierCode(noStandard, undefined)).toBe("pro");
    expect(resolveTierCode(noStandard, "enterprise")).toBe("pro");
  });

  it("falls back to the literal default only when tierOptions is genuinely empty", () => {
    expect(resolveTierCode([], undefined)).toBe("standard");
  });

  it("the fallback constant itself resolves cleanly", () => {
    expect(resolveTierCode(FALLBACK_TIERS, undefined)).toBe("standard");
  });
});

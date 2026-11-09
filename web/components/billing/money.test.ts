import { describe, expect, it } from "vitest";
import {
  formatCents,
  formatMicroUsd,
  microUsdToInput,
  parseDollarsToMicroUsd,
  parseMemberCap,
} from "./money";

describe("dollars <-> micro-USD", () => {
  it("converts typed dollars to whole cents in micro-USD", () => {
    expect(parseDollarsToMicroUsd("50")).toEqual({ ok: true, micro: 50_000_000 });
    expect(parseDollarsToMicroUsd("$12.34")).toEqual({ ok: true, micro: 12_340_000 });
    expect(parseDollarsToMicroUsd("1,000.5")).toEqual({ ok: true, micro: 1_000_500_000 });
    expect(parseDollarsToMicroUsd(" 0.07 ")).toEqual({ ok: true, micro: 70_000 });
  });

  it("does not use floating point for cents", () => {
    // 19.99 * 100 = 1998.9999999999998 in floats
    expect(parseDollarsToMicroUsd("19.99")).toEqual({ ok: true, micro: 19_990_000 });
    expect(parseDollarsToMicroUsd("0.29")).toEqual({ ok: true, micro: 290_000 });
  });

  it("refuses fractions of a cent instead of rounding", () => {
    expect(parseDollarsToMicroUsd("1.005")).toEqual({ ok: false, reason: "fraction" });
  });

  it("refuses empty and non-numeric input", () => {
    expect(parseDollarsToMicroUsd("")).toEqual({ ok: false, reason: "empty" });
    expect(parseDollarsToMicroUsd("abc")).toEqual({ ok: false, reason: "invalid" });
    expect(parseDollarsToMicroUsd("-5")).toEqual({ ok: false, reason: "invalid" });
    expect(parseDollarsToMicroUsd("1e3")).toEqual({ ok: false, reason: "invalid" });
  });

  it("always shows two decimals", () => {
    expect(formatMicroUsd(0)).toBe("$0.00");
    expect(formatMicroUsd(5_000_000)).toBe("$5.00");
    expect(formatMicroUsd(12_345_678)).toBe("$12.35");
    expect(formatMicroUsd(1_234_560_000)).toBe("$1,234.56");
    expect(formatCents(-1250)).toBe("-$12.50");
    expect(formatCents(9900)).toBe("$99.00");
  });

  it("prefills inputs without noise", () => {
    expect(microUsdToInput(50_000_000)).toBe("50");
    expect(microUsdToInput(12_340_000)).toBe("12.34");
    expect(microUsdToInput(null)).toBe("");
  });

  it("round-trips a typed value through micro-USD", () => {
    const p = parseDollarsToMicroUsd("37.5");
    expect(p.ok && microUsdToInput(p.micro)).toBe("37.50");
  });
});

describe("member cap multiple", () => {
  it("accepts 0.1 to 100 with up to two decimals", () => {
    expect(parseMemberCap("1")).toEqual({ ok: true, value: 1 });
    expect(parseMemberCap("0.1")).toEqual({ ok: true, value: 0.1 });
    expect(parseMemberCap("2.25x")).toEqual({ ok: true, value: 2.25 });
    expect(parseMemberCap("100")).toEqual({ ok: true, value: 100 });
  });
  it("rejects out of range and malformed values", () => {
    expect(parseMemberCap("0.05")).toEqual({ ok: false, reason: "range" });
    expect(parseMemberCap("101")).toEqual({ ok: false, reason: "range" });
    expect(parseMemberCap("1.234")).toEqual({ ok: false, reason: "invalid" });
    expect(parseMemberCap("")).toEqual({ ok: false, reason: "empty" });
  });
});

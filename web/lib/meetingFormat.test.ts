import { describe, it, expect } from "vitest";
import { durationMsBetween, fmtDuration, fmtDurationMs, fmtTimestampMs } from "./meetingFormat";

describe("meetingFormat", () => {
  it("durationMsBetween computes the wall-clock delta", () => {
    expect(durationMsBetween("2026-01-01T00:00:00Z", "2026-01-01T00:01:30Z")).toBe(90_000);
  });

  it("durationMsBetween is undefined without an end", () => {
    expect(durationMsBetween("2026-01-01T00:00:00Z", null)).toBeUndefined();
  });

  it("durationMsBetween is undefined for an inverted range", () => {
    expect(durationMsBetween("2026-01-01T00:01:00Z", "2026-01-01T00:00:00Z")).toBeUndefined();
  });

  it("fmtDurationMs renders seconds-only under a minute", () => {
    expect(fmtDurationMs(45_000)).toBe("45s");
  });

  it("fmtDurationMs renders minutes + seconds under an hour", () => {
    expect(fmtDurationMs(90_000)).toBe("1m 30s");
  });

  it("fmtDurationMs renders hours + minutes over an hour", () => {
    expect(fmtDurationMs(3_725_000)).toBe("1h 02m");
  });

  it("fmtDurationMs is null for unknown durations", () => {
    expect(fmtDurationMs(undefined)).toBeNull();
    expect(fmtDurationMs(null)).toBeNull();
  });

  it("fmtDuration composes durationMsBetween + fmtDurationMs", () => {
    expect(fmtDuration("2026-01-01T00:00:00Z", "2026-01-01T00:01:30Z")).toBe("1m 30s");
    expect(fmtDuration("2026-01-01T00:00:00Z", null)).toBeNull();
  });

  it("fmtTimestampMs renders mm:ss", () => {
    expect(fmtTimestampMs(0)).toBe("00:00");
    expect(fmtTimestampMs(75_000)).toBe("01:15");
    expect(fmtTimestampMs(3_600_000)).toBe("60:00");
  });
});

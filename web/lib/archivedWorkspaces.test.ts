import { describe, expect, it } from "vitest";
import { archivedRowStatus, daysLeftLabel } from "./archivedWorkspaces";

const NOW = new Date("2026-08-05T00:00:00Z");

describe("archivedRowStatus", () => {
  it("restorable: true renders the day count computed from the server's expires_at", () => {
    const row = { restorable: true, expires_at: "2026-08-08T00:00:00Z" }; // +3 days
    expect(archivedRowStatus(row, NOW)).toEqual({ status: "restorable", daysLeft: 3 });
  });

  it("rounds up a partial day so a still-restorable row never shows 0 days left", () => {
    const row = { restorable: true, expires_at: "2026-08-05T09:36:00Z" }; // +0.4 days
    expect(archivedRowStatus(row, NOW)).toEqual({ status: "restorable", daysLeft: 1 });
  });

  it("clamps negative day counts (clock skew) to 0 rather than showing a negative number", () => {
    const row = { restorable: true, expires_at: "2026-08-04T00:00:00Z" }; // in the past
    expect(archivedRowStatus(row, NOW)).toEqual({ status: "restorable", daysLeft: 0 });
  });

  it("restorable: false is ALWAYS expired, even when expires_at is still in the future", () => {
    // This is the case that matters most: it proves the function trusts the
    // server's verdict rather than re-deriving one from the date itself.
    const row = { restorable: false, expires_at: "2099-01-01T00:00:00Z" };
    expect(archivedRowStatus(row, NOW)).toEqual({ status: "expired" });
  });

  it("restorable: false with a past expires_at is also expired (the ordinary case)", () => {
    const row = { restorable: false, expires_at: "2026-04-01T00:00:00Z" };
    expect(archivedRowStatus(row, NOW)).toEqual({ status: "expired" });
  });

  it("the boundary instant itself (expires_at === now) is still restorable when the server says so", () => {
    const row = { restorable: true, expires_at: NOW.toISOString() };
    expect(archivedRowStatus(row, NOW)).toEqual({ status: "restorable", daysLeft: 0 });
  });
});

describe("daysLeftLabel", () => {
  it("pluralizes for anything other than exactly 1", () => {
    expect(daysLeftLabel(0)).toBe("0 days left");
    expect(daysLeftLabel(2)).toBe("2 days left");
    expect(daysLeftLabel(117)).toBe("117 days left");
  });

  it("stays singular at exactly 1", () => {
    expect(daysLeftLabel(1)).toBe("1 day left");
  });
});

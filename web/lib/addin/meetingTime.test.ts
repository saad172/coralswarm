import { describe, expect, it } from "vitest";
import { formatDeadline, formatMeetingWhen } from "./meetingTime";

const now = new Date(2026, 9, 3, 9, 0);
describe("formatMeetingWhen", () => {
  it("says Today with a local time range", () => {
    const out = formatMeetingWhen(new Date(2026, 9, 3, 14, 0).toISOString(), new Date(2026, 9, 3, 14, 30).toISOString(), now);
    expect(out).toMatch(/^Today · /);
    expect(out).toMatch(/2:00/); expect(out).toMatch(/2:30/);
  });
  it("says Tomorrow and names other days", () => {
    expect(formatMeetingWhen(new Date(2026, 9, 4, 14, 0).toISOString(), new Date(2026, 9, 4, 15, 0).toISOString(), now)).toMatch(/^Tomorrow · /);
    expect(formatMeetingWhen(new Date(2026, 9, 9, 14, 0).toISOString(), new Date(2026, 9, 9, 15, 0).toISOString(), now)).not.toMatch(/^(Today|Tomorrow)/);
  });
  it("a 24h ad-hoc window shows only the start, never a full date", () => {
    const out = formatMeetingWhen(new Date(2026, 9, 3, 17, 15).toISOString(), new Date(2026, 9, 4, 17, 15).toISOString(), now);
    expect(out).toMatch(/^Today · /); expect(out).toMatch(/5:15/);
    expect(out).not.toMatch(/–|2026|10\/4/);
  });
  it("a window crossing midnight shows only the start", () => {
    const out = formatMeetingWhen(new Date(2026, 9, 3, 23, 30).toISOString(), new Date(2026, 9, 4, 0, 15).toISOString(), now);
    expect(out).toMatch(/^Today · /); expect(out).toMatch(/11:30/); expect(out).not.toMatch(/–|12:15/);
  });
  it("a window over 8h on one day shows only the start", () => {
    const out = formatMeetingWhen(new Date(2026, 9, 3, 9, 0).toISOString(), new Date(2026, 9, 3, 19, 0).toISOString(), now);
    expect(out).not.toMatch(/–/);
  });
  it("a normal range is unchanged", () => {
    expect(formatMeetingWhen(new Date(2026, 9, 3, 14, 0).toISOString(), new Date(2026, 9, 3, 14, 30).toISOString(), now)).toMatch(/^Today · .*2:00.*–.*2:30/);
  });
  it("returns null without usable bounds", () => {
    expect(formatMeetingWhen("", "", now)).toBeNull();
    expect(formatMeetingWhen(null, undefined, now)).toBeNull();
    expect(formatMeetingWhen("nope", "2026-10-03T10:00:00Z", now)).toBeNull();
    expect(formatMeetingWhen("2026-10-03T11:00:00Z", "2026-10-03T10:00:00Z", now)).toBeNull();
  });
});

describe("formatDeadline", () => {
  const now = new Date(2030, 5, 10, 9, 0);
  const time = (d: Date) => new Intl.DateTimeFormat(undefined, { hour: "numeric", minute: "2-digit" }).format(d);
  it("says today / tomorrow in local time", () => {
    const later = new Date(2030, 5, 10, 15, 42), next = new Date(2030, 5, 11, 8, 5);
    expect(formatDeadline(later.toISOString(), now)).toBe(`today at ${time(later)}`);
    expect(formatDeadline(next.toISOString(), now)).toBe(`tomorrow at ${time(next)}`);
  });
  it("uses a weekday and date further out", () => {
    const far = new Date(2030, 5, 14, 8, 5);
    expect(formatDeadline(far.toISOString(), now)).toBe(`${new Intl.DateTimeFormat(undefined, { weekday: "short", month: "short", day: "numeric" }).format(far)} at ${time(far)}`);
  });
  it("returns null for missing, junk or past instants", () => {
    expect(formatDeadline(null, now)).toBeNull(); expect(formatDeadline("soon", now)).toBeNull();
    expect(formatDeadline(new Date(2030, 5, 10, 8, 0).toISOString(), now)).toBeNull();
  });
});

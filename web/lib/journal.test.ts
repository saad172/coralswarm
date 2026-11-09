// Today-journal PR D — the pure logic under `app/(app)/today/page.tsx`.
//
// Pinned to a real timezone (America/Chicago) rather than the runner's
// default: DST behavior is the whole point of the `hoursOfLocalDay` tests
// below, and CI/local dev don't share a TZ. Pinned via `test.env` in
// `vitest.config.ts` (applies before any static import in the worker runs),
// not an in-file `process.env.TZ` assignment — a module-scope `Date` op in an
// imported module would otherwise still see the runner's own timezone.

import { describe, expect, it } from "vitest";
import {
  aggregateQualifier,
  apiHoursForSlot,
  askAboutDayHref,
  askAboutDayPhrase,
  askAboutMonthHref,
  askAboutMonthPhrase,
  askAboutWeekHref,
  askAboutWeekPhrase,
  buildDayRows,
  buildTierRows,
  countLabel,
  dayHeaderTitle,
  dayRowLabel,
  filterEntriesByGraph,
  filterPeriodEntriesByGraph,
  formatCompiledAtLabel,
  formatDayParam,
  formatGapLabel,
  formatHourLabel,
  futureSlotMask,
  hourStartForDrilldown,
  hoursOfLocalDay,
  intensityBucket,
  intensityBucketForZoom,
  intensityForPeriods,
  intensityForSlots,
  isFutureSlot,
  isSameLocalDay,
  journalHref,
  localDayAnchor,
  localDayToUtcRange,
  localMonthBounds,
  localWeekBounds,
  mergeJournalHours,
  monthCrumbLabel,
  monthHeaderTitle,
  parseBulletLine,
  parseBulletLines,
  parseDayParam,
  parseZoomParam,
  periodGapLabel,
  periodRowStats,
  rowContainingInstant,
  rowElementId,
  splitBulletLines,
  staleBadgeLabel,
  staleEntryInfo,
  statsForDay,
  statsForPeriods,
  weekdayTickLabel,
  weekHeaderTitle,
  weekRowLabel,
  zoomRequestRange,
  type DayGapRow,
  type DayHourRow,
} from "./journal";
import type { JournalAggregate, JournalEntry, JournalHour, JournalPeriod, JournalTierEntry } from "./api";

function hour(hourStart: string, atomCount: number, opts?: { asks?: number; blocks?: JournalHour["aggregate"]["blocks"]; entries?: JournalHour["entries"] }): JournalHour {
  return {
    hour_start: hourStart,
    entries: opts?.entries ?? [],
    aggregate: { atom_count: atomCount, asks: opts?.asks ?? 0, blocks: opts?.blocks ?? [] },
  };
}

// ── localDayAnchor / isSameLocalDay ───────────────────────────────────────────

describe("localDayAnchor / isSameLocalDay", () => {
  it("strips the time-of-day, keeping only Y/M/D", () => {
    const a = localDayAnchor(new Date(2026, 7, 1, 23, 59, 59));
    expect(a.getFullYear()).toBe(2026);
    expect(a.getMonth()).toBe(7);
    expect(a.getDate()).toBe(1);
    expect(a.getHours()).toBe(0);
  });

  it("two instants on the same local day compare equal even hours apart", () => {
    expect(isSameLocalDay(new Date(2026, 7, 1, 0, 5), new Date(2026, 7, 1, 23, 55))).toBe(true);
  });

  it("11pm and 1am on either side of midnight are different local days", () => {
    expect(isSameLocalDay(new Date(2026, 7, 1, 23, 0), new Date(2026, 7, 2, 1, 0))).toBe(false);
  });
});

// ── ?d= param round trip ──────────────────────────────────────────────────────

describe("parseDayParam / formatDayParam", () => {
  it("round-trips a well-formed date", () => {
    const d = parseDayParam("2026-08-01");
    expect(formatDayParam(d)).toBe("2026-08-01");
    expect(d.getFullYear()).toBe(2026);
    expect(d.getMonth()).toBe(7);
    expect(d.getDate()).toBe(1);
  });

  it("pads single-digit month/day back out", () => {
    expect(formatDayParam(new Date(2026, 0, 5))).toBe("2026-01-05");
  });

  it("falls back to now's local day when the param is missing", () => {
    const now = new Date(2026, 6, 4, 15, 0);
    expect(formatDayParam(parseDayParam(null, now))).toBe("2026-07-04");
  });

  it("falls back to now's local day when the param is malformed", () => {
    const now = new Date(2026, 6, 4, 15, 0);
    expect(formatDayParam(parseDayParam("not-a-date", now))).toBe("2026-07-04");
    expect(formatDayParam(parseDayParam("2026/08/01", now))).toBe("2026-07-04");
  });

  it("parses Y/M/D as LOCAL, not as UTC midnight", () => {
    // The bug this guards: `new Date("2026-08-01")` is UTC midnight, which in
    // a negative-UTC-offset zone (America/Chicago, UTC-5/-6) reads back as
    // July 31 locally — the page would silently open the wrong day.
    const d = parseDayParam("2026-08-01");
    expect(d.getDate()).toBe(1);
    expect(d.getMonth()).toBe(7);
  });
});

// ── day header title ───────────────────────────────────────────────────────────

describe("dayHeaderTitle", () => {
  it("marks 'today' as the italic word only when the viewed day IS today", () => {
    const now = new Date(2026, 7, 1, 12, 0);
    const t = dayHeaderTitle(new Date(2026, 7, 1), now);
    expect(t.italicWord).toBe("today");
    expect(t.title.endsWith("· today")).toBe(true);
  });

  it("carries no italic word for a past day", () => {
    const now = new Date(2026, 7, 1, 12, 0);
    const t = dayHeaderTitle(new Date(2026, 6, 28), now);
    expect(t.italicWord).toBeUndefined();
    expect(t.title).not.toContain("today");
  });
});

// ── localDayToUtcRange ────────────────────────────────────────────────────────

describe("localDayToUtcRange", () => {
  it("spans exactly local midnight to the following local midnight", () => {
    const { from, to } = localDayToUtcRange(new Date(2026, 7, 1, 9, 30));
    expect(from.getHours()).toBe(0);
    expect(from.getDate()).toBe(1);
    expect(to.getDate()).toBe(2);
    expect(to.getHours()).toBe(0);
  });

  it("is 24h wide on an ordinary (non-DST-transition) day", () => {
    const { from, to } = localDayToUtcRange(new Date(2026, 7, 1));
    expect(to.getTime() - from.getTime()).toBe(24 * 3_600_000);
  });

  it("is 23h wide on the US spring-forward day (2026-03-08, America/Chicago)", () => {
    const { from, to } = localDayToUtcRange(new Date(2026, 2, 8));
    expect(to.getTime() - from.getTime()).toBe(23 * 3_600_000);
  });

  it("is 25h wide on the US fall-back day (2026-11-01, America/Chicago)", () => {
    const { from, to } = localDayToUtcRange(new Date(2026, 10, 1));
    expect(to.getTime() - from.getTime()).toBe(25 * 3_600_000);
  });
});

// ── hoursOfLocalDay ────────────────────────────────────────────────────────────

describe("hoursOfLocalDay", () => {
  it("returns 24 contiguous 1h slots on an ordinary day", () => {
    const slots = hoursOfLocalDay(new Date(2026, 7, 1));
    expect(slots).toHaveLength(24);
    expect(slots[0].start.getHours()).toBe(0);
    expect(slots[0].start.getDate()).toBe(1);
    expect(slots[23].end.getDate()).toBe(2);
    expect(slots[23].end.getHours()).toBe(0);
    // Contiguous: every slot's end is the next slot's start.
    for (let i = 0; i < slots.length - 1; i++) {
      expect(slots[i].end.getTime()).toBe(slots[i + 1].start.getTime());
    }
  });

  it("returns 23 slots on the spring-forward day — never a hardcoded 24", () => {
    const slots = hoursOfLocalDay(new Date(2026, 2, 8));
    expect(slots).toHaveLength(23);
    expect(slots[0].start.getHours()).toBe(0);
    expect(slots[slots.length - 1].end.getDate()).toBe(9);
  });

  it("returns 25 slots on the fall-back day", () => {
    const slots = hoursOfLocalDay(new Date(2026, 10, 1));
    expect(slots).toHaveLength(25);
    expect(slots[slots.length - 1].end.getDate()).toBe(2);
  });

  it("every slot of the day is covered end-to-end with no gap or overlap", () => {
    for (const day of [new Date(2026, 7, 1), new Date(2026, 2, 8), new Date(2026, 10, 1)]) {
      const slots = hoursOfLocalDay(day);
      const { start, end } = { start: slots[0].start, end: slots[slots.length - 1].end };
      const totalMs = slots.reduce((n, s) => n + (s.end.getTime() - s.start.getTime()), 0);
      expect(totalMs).toBe(end.getTime() - start.getTime());
    }
  });
});

describe("formatHourLabel / formatGapLabel", () => {
  it("formats HH:00 in the instant's own local hour", () => {
    expect(formatHourLabel(new Date(2026, 7, 1, 7, 0))).toBe("07:00");
    expect(formatHourLabel(new Date(2026, 7, 1, 23, 0))).toBe("23:00");
    expect(formatHourLabel(new Date(2026, 7, 1, 0, 0))).toBe("00:00");
  });

  it("builds the 'no activity · HH:00–HH:00' label", () => {
    const gap: DayGapRow = { kind: "gap", start: new Date(2026, 7, 1, 23, 0), end: new Date(2026, 7, 2, 7, 0) };
    expect(formatGapLabel(gap)).toBe("no activity · 23:00–07:00");
  });
});

// ── buildDayRows (gap collapsing) ─────────────────────────────────────────────

describe("buildDayRows", () => {
  const slots = hoursOfLocalDay(new Date(2026, 7, 1)); // 24 slots, 00:00..23:00

  it("with no data at all, the whole day collapses into one gap row", () => {
    const rows = buildDayRows(slots, []);
    expect(rows).toHaveLength(1);
    expect(rows[0].kind).toBe("gap");
    expect(rows[0].start.getTime()).toBe(slots[0].start.getTime());
    expect(rows[0].end.getTime()).toBe(slots[23].end.getTime());
  });

  it("every present hour becomes its own row; consecutive absent hours collapse into one gap", () => {
    // Present at 07:00, 08:00, then nothing until 20:00.
    const present = [new Date(2026, 7, 1, 7, 30), new Date(2026, 7, 1, 8, 10), new Date(2026, 7, 1, 20, 0)];
    const rows = buildDayRows(slots, present);
    // leading gap (00:00-07:00), 07:00 hour, 08:00 hour, gap (09:00-20:00), 20:00 hour, trailing gap (21:00-24:00)
    expect(rows.map((r) => r.kind)).toEqual(["gap", "hour", "hour", "gap", "hour", "gap"]);
    expect(formatGapLabel(rows[0] as DayGapRow)).toBe("no activity · 00:00–07:00");
    expect((rows[1] as DayHourRow).start.getHours()).toBe(7);
    expect((rows[2] as DayHourRow).start.getHours()).toBe(8);
    expect(formatGapLabel(rows[3] as DayGapRow)).toBe("no activity · 09:00–20:00");
    expect((rows[4] as DayHourRow).start.getHours()).toBe(20);
    expect(formatGapLabel(rows[5] as DayGapRow)).toBe("no activity · 21:00–00:00");
  });

  it("every slot is accounted for by exactly one row (spans sum to the full day)", () => {
    const present = [new Date(2026, 7, 1, 3, 0), new Date(2026, 7, 1, 15, 0)];
    const rows = buildDayRows(slots, present);
    const totalMs = rows.reduce((n, r) => n + (r.end.getTime() - r.start.getTime()), 0);
    expect(totalMs).toBe(slots[23].end.getTime() - slots[0].start.getTime());
  });

  it("the current hour is always its own row, marked `current`, even with zero data", () => {
    const now = new Date(2026, 7, 1, 14, 20);
    const rows = buildDayRows(slots, [new Date(2026, 7, 1, 9, 0)], now);
    const currentRow = rows.find((r) => r.kind === "hour" && r.current) as DayHourRow | undefined;
    expect(currentRow).toBeDefined();
    expect(currentRow?.start.getHours()).toBe(14);
    // It must not have been folded into a neighboring gap.
    const currentStart = currentRow?.start.getTime() ?? NaN;
    expect(rows.some((r) => r.kind === "gap" && r.start.getTime() <= currentStart && currentStart < r.end.getTime())).toBe(false);
  });

  it("a real (non-current) hour is never marked current", () => {
    const now = new Date(2026, 7, 1, 14, 20);
    const rows = buildDayRows(slots, [new Date(2026, 7, 1, 9, 0)], now);
    const nineRow = rows.find((r) => r.kind === "hour" && r.start.getHours() === 9) as DayHourRow;
    expect(nineRow.current).toBe(false);
  });

  it("viewing today, no row (hour or gap) exists for any hour after the current one", () => {
    // Bug: prod showed a false "no activity · 12:00–00:00" row at 11:10am —
    // hours that simply haven't happened yet must render nothing at all.
    const now = new Date(2026, 7, 1, 11, 10);
    const rows = buildDayRows(slots, [], now);
    for (const r of rows) {
      expect(r.start.getHours()).toBeLessThanOrEqual(11);
    }
    const last = rows[rows.length - 1];
    expect(last.kind).toBe("hour");
    expect((last as DayHourRow).current).toBe(true);
    expect(last.start.getHours()).toBe(11);
  });

  it("viewing today, past-hour gaps before the current hour still collapse normally", () => {
    // 00:00-02:00 quiet, 03:00 present, 04:00-10:00 quiet, 11:00 in progress —
    // and nothing past 11:00 (future) at all.
    const now = new Date(2026, 7, 1, 11, 10);
    const rows = buildDayRows(slots, [new Date(2026, 7, 1, 3, 0)], now);
    expect(rows.map((r) => r.kind)).toEqual(["gap", "hour", "gap", "hour"]);
    expect(formatGapLabel(rows[0] as DayGapRow)).toBe("no activity · 00:00–03:00");
    expect((rows[1] as DayHourRow).start.getHours()).toBe(3);
    expect(formatGapLabel(rows[2] as DayGapRow)).toBe("no activity · 04:00–11:00");
    expect((rows[3] as DayHourRow).start.getHours()).toBe(11);
    expect((rows[3] as DayHourRow).current).toBe(true);
  });

  it("a past (non-today) day is untouched — the full 24h accounting, `now` ignored", () => {
    // `now` not passed at all (defaults to null) — same as viewing yesterday
    // or any other non-today day from the page.
    const rows = buildDayRows(slots, [new Date(2026, 7, 1, 3, 0)]);
    expect(rows.map((r) => r.kind)).toEqual(["gap", "hour", "gap"]);
    expect(formatGapLabel(rows[2] as DayGapRow)).toBe("no activity · 04:00–00:00");
  });
});

// ── apiHoursForSlot / mergeJournalHours ───────────────────────────────────────

describe("apiHoursForSlot / mergeJournalHours", () => {
  const slots = hoursOfLocalDay(new Date(2026, 7, 1));

  it("finds the one API hour landing in a whole-hour-offset local slot", () => {
    const apiHours = [hour("2026-08-01T19:00:00Z", 3)]; // 14:00 local (UTC-5 in August, Chicago DST)
    const found = apiHoursForSlot(apiHours, slots[14]);
    expect(found).toHaveLength(1);
    expect(found[0].aggregate.atom_count).toBe(3);
  });

  it("sums atom_count/asks and concatenates entries+blocks when a slot spans two API buckets", () => {
    const merged = mergeJournalHours([
      hour("2026-08-01T19:00:00Z", 3, { asks: 1, blocks: [{ source_type: "slack", title: "x", atom_count: 3 }] }),
      hour("2026-08-01T19:30:00Z", 2, { asks: 0, blocks: [{ source_type: "note", title: "y", atom_count: 2 }] }),
    ]);
    expect(merged.aggregate.atom_count).toBe(5);
    expect(merged.aggregate.asks).toBe(1);
    expect(merged.aggregate.blocks).toHaveLength(2);
  });

  it("merging zero hours yields a truthful zero aggregate, not a crash", () => {
    const merged = mergeJournalHours([]);
    expect(merged).toEqual({ entries: [], aggregate: { atom_count: 0, asks: 0, blocks: [] } });
  });
});

// ── filterEntriesByGraph ──────────────────────────────────────────────────────

describe("filterEntriesByGraph", () => {
  const hours = [
    hour("2026-08-01T14:00:00Z", 5, {
      entries: [
        { graph_id: "g1", graph_name: "Personal", summary: "a", stats: {}, compiled_at: "2026-08-01T14:05:00Z" },
        { graph_id: "g2", graph_name: "Team", summary: "b", stats: {}, compiled_at: "2026-08-01T14:05:00Z" },
      ],
    }),
  ];

  it("passes every hour through unchanged when no graph is selected", () => {
    expect(filterEntriesByGraph(hours, null)).toEqual(hours);
  });

  it("keeps only the matching workspace's entries, leaving aggregate untouched", () => {
    const filtered = filterEntriesByGraph(hours, "g1");
    expect(filtered[0].entries).toHaveLength(1);
    expect(filtered[0].entries[0].graph_id).toBe("g1");
    // aggregate isn't per-graph — the endpoint has no per-ocean breakdown.
    expect(filtered[0].aggregate.atom_count).toBe(5);
  });

  it("can filter an hour down to zero entries without dropping the hour", () => {
    const filtered = filterEntriesByGraph(hours, "g-not-present");
    expect(filtered).toHaveLength(1);
    expect(filtered[0].entries).toHaveLength(0);
  });
});

// ── aggregateQualifier ────────────────────────────────────────────────────────

describe("aggregateQualifier", () => {
  it("is undefined with no filter active, regardless of workspace count", () => {
    expect(aggregateQualifier(null, 1)).toBeUndefined();
    expect(aggregateQualifier(null, 3)).toBeUndefined();
  });

  it("is undefined when filtered but the account has only the one reachable workspace", () => {
    // The prod bug: a single-ocean account ("Primary ocean") has nothing for
    // "all oceans" to disambiguate from.
    expect(aggregateQualifier("g1", 1)).toBeUndefined();
    expect(aggregateQualifier("g1", 0)).toBeUndefined();
  });

  it("discloses 'all workspaces' when filtered and the account has more than one reachable workspace", () => {
    expect(aggregateQualifier("g1", 2)).toBe("all workspaces");
  });
});

// ── staleEntryInfo / staleBadgeLabel (ticket 14) ──────────────────────────────

function entry(compiledAt: string, stats: Record<string, unknown> = {}): JournalEntry {
  return { graph_id: "g1", graph_name: "Personal", summary: "x", stats, compiled_at: compiledAt };
}

function aggregate(atomCount: number, asks = 0): JournalAggregate {
  return { atom_count: atomCount, asks, blocks: [] };
}

describe("staleEntryInfo", () => {
  const now = new Date("2026-08-02T15:00:00Z");

  it("is null for an untouched hour — an old compiled_at alone is never stale", () => {
    const e = entry("2026-08-01T09:00:00Z", { atom_count: 5, asks: 0 });
    expect(staleEntryInfo(e, aggregate(5, 0), now)).toBeNull();
  });

  it("is null when new activity landed but hasn't cleared the minimum lag yet", () => {
    const e = entry("2026-08-02T14:55:00Z", { atom_count: 5, asks: 0 }); // 5 minutes ago
    expect(staleEntryInfo(e, aggregate(8, 0), now)).toBeNull();
  });

  it("flags real growth in atom_count once the lag clears the threshold", () => {
    const e = entry("2026-08-02T14:30:00Z", { atom_count: 5, asks: 0 }); // 30 minutes ago
    const info = staleEntryInfo(e, aggregate(9, 0), now);
    expect(info).not.toBeNull();
    expect(info?.newerCount).toBe(4);
    expect(info?.compiledAt.toISOString()).toBe("2026-08-02T14:30:00.000Z");
  });

  it("flags real growth in asks the same way as atoms", () => {
    const e = entry("2026-08-02T14:00:00Z", { atom_count: 5, asks: 1 });
    const info = staleEntryInfo(e, aggregate(5, 3), now);
    expect(info?.newerCount).toBe(2);
  });

  it("treats a missing/non-numeric stats field as zero rather than throwing", () => {
    const e = entry("2026-08-02T14:00:00Z", {});
    const info = staleEntryInfo(e, aggregate(3, 0), now);
    expect(info?.newerCount).toBe(3);
  });

  it("never goes negative when the live aggregate is (rarely) lower than the snapshot", () => {
    const e = entry("2026-08-02T14:00:00Z", { atom_count: 10, asks: 5 });
    expect(staleEntryInfo(e, aggregate(3, 1), now)).toBeNull();
  });
});

describe("formatCompiledAtLabel / staleBadgeLabel", () => {
  it("formats the compile instant's own HH:MM, not truncated to the hour", () => {
    expect(formatCompiledAtLabel(new Date(2026, 7, 2, 14, 37))).toBe("14:37");
  });

  it("renders the approved copy, singular vs plural", () => {
    const compiledAt = new Date(2026, 7, 2, 14, 37);
    expect(staleBadgeLabel({ compiledAt, newerCount: 1 })).toBe("prose from 14:37 · 1 newer item pending");
    expect(staleBadgeLabel({ compiledAt, newerCount: 4 })).toBe("prose from 14:37 · 4 newer items pending");
  });
});

// ── intensity ──────────────────────────────────────────────────────────────────

describe("intensityBucket", () => {
  it("buckets none/light/moderate/heavy", () => {
    expect(intensityBucket(0)).toBe(0);
    expect(intensityBucket(1)).toBe(1);
    expect(intensityBucket(4)).toBe(1);
    expect(intensityBucket(5)).toBe(2);
    expect(intensityBucket(14)).toBe(2);
    expect(intensityBucket(15)).toBe(3);
    expect(intensityBucket(200)).toBe(3);
  });
});

describe("intensityForSlots", () => {
  it("returns one bucket per slot, in slot order", () => {
    const slots = hoursOfLocalDay(new Date(2026, 7, 1));
    const apiHours = [hour("2026-08-01T19:00:00Z", 20)]; // 14:00 local
    const buckets = intensityForSlots(slots, apiHours);
    expect(buckets).toHaveLength(24);
    expect(buckets[14]).toBe(3);
    expect(buckets[13]).toBe(0);
  });
});

describe("isFutureSlot / futureSlotMask", () => {
  const slots = hoursOfLocalDay(new Date(2026, 7, 1));

  it("a slot starting after `now` is future", () => {
    expect(isFutureSlot(slots[12], new Date(2026, 7, 1, 11, 10))).toBe(true);
  });

  it("the in-progress slot containing `now` is NOT future", () => {
    expect(isFutureSlot(slots[11], new Date(2026, 7, 1, 11, 10))).toBe(false);
  });

  it("an already-elapsed slot is not future", () => {
    expect(isFutureSlot(slots[3], new Date(2026, 7, 1, 11, 10))).toBe(false);
  });

  it("marks every slot after the current hour, and only those", () => {
    const mask = futureSlotMask(slots, new Date(2026, 7, 1, 11, 10));
    expect(mask).toHaveLength(24);
    expect(mask.slice(0, 12)).toEqual(Array(12).fill(false)); // 00:00..11:00 (in progress)
    expect(mask.slice(12)).toEqual(Array(12).fill(true)); // 12:00..23:00
  });

  it("an all-false mask when `now` is null (a past/non-today day)", () => {
    expect(futureSlotMask(slots, null)).toEqual(Array(24).fill(false));
  });
});

// ── statsForDay ────────────────────────────────────────────────────────────────

describe("statsForDay", () => {
  it("sums atoms/asks, counts distinct source_types and hours with real atoms", () => {
    const apiHours = [
      hour("2026-08-01T14:00:00Z", 10, { asks: 1, blocks: [{ source_type: "slack", title: "x", atom_count: 10 }] }),
      hour("2026-08-01T15:00:00Z", 3, { asks: 0, blocks: [{ source_type: "note", title: "y", atom_count: 3 }] }),
      // A compiled-but-empty hour (e.g. the in-progress hour) must not count
      // as "hours active" even though it appears in the response.
      hour("2026-08-01T16:00:00Z", 0, { asks: 1 }),
    ];
    const stats = statsForDay(apiHours);
    expect(stats).toEqual({ atoms: 13, sourcesActive: 2, asks: 2, hoursActive: 2 });
  });

  it("an empty day is all zeros, not undefined/NaN", () => {
    expect(statsForDay([])).toEqual({ atoms: 0, sourcesActive: 0, asks: 0, hoursActive: 0 });
  });
});

// ── rowContainingInstant / rowElementId ───────────────────────────────────────

describe("rowContainingInstant", () => {
  const slots = hoursOfLocalDay(new Date(2026, 7, 1));

  it("finds the real hour row containing an instant", () => {
    const rows = buildDayRows(slots, [new Date(2026, 7, 1, 9, 0)]);
    const row = rowContainingInstant(rows, new Date(2026, 7, 1, 9, 45));
    expect(row?.kind).toBe("hour");
    expect(row?.start.getHours()).toBe(9);
  });

  it("finds the collapsed gap row containing an instant inside it", () => {
    const rows = buildDayRows(slots, [new Date(2026, 7, 1, 9, 0)]);
    // Hour 3 has no data — it's inside the leading 00:00-09:00 gap.
    const row = rowContainingInstant(rows, new Date(2026, 7, 1, 3, 15));
    expect(row?.kind).toBe("gap");
    expect(row?.start.getHours()).toBe(0);
    expect(row?.end.getHours()).toBe(9);
  });

  it("returns undefined for an instant outside every row's span", () => {
    const rows = buildDayRows(slots, [new Date(2026, 7, 1, 9, 0)]);
    expect(rowContainingInstant(rows, new Date(2026, 7, 2, 9, 0))).toBeUndefined();
  });
});

describe("rowElementId", () => {
  it("is stable for the same start instant and distinct for different ones", () => {
    const a = { start: new Date(2026, 7, 1, 9, 0) };
    const b = { start: new Date(2026, 7, 1, 9, 0) };
    const c = { start: new Date(2026, 7, 1, 10, 0) };
    expect(rowElementId(a)).toBe(rowElementId(b));
    expect(rowElementId(a)).not.toBe(rowElementId(c));
  });
});

// ── ask-about-this-day phrasing ───────────────────────────────────────────────

describe("askAboutDayPhrase / askAboutDayHref", () => {
  it("phrases the VIEWED date with a month name, not the raw ISO date", () => {
    expect(askAboutDayPhrase(new Date(2026, 7, 1))).toBe("What happened on August 1?");
  });

  it("uses the viewed day even when it isn't today", () => {
    expect(askAboutDayPhrase(new Date(2026, 0, 15))).toBe("What happened on January 15?");
  });

  it("URL-encodes the phrase into a /chats?q= link", () => {
    const href = askAboutDayHref(new Date(2026, 7, 1));
    expect(href).toBe(`/chats?q=${encodeURIComponent("What happened on August 1?")}`);
    expect(href.startsWith("/chats?q=")).toBe(true);
  });
});

// ══════════════════════════════════════════════════════════════════════════
// Zoomable journal (ticket 17)
// ══════════════════════════════════════════════════════════════════════════

function tierEntry(summary: string, opts?: Partial<JournalTierEntry>): JournalTierEntry {
  return {
    graph_id: "g1",
    graph_name: "Personal",
    summary,
    stats: { atom_count: 0, asks: 0, sources: {} },
    tz: "UTC",
    compiled_at: "2026-08-02T01:00:00Z",
    recompile_count: 0,
    open: false,
    ...opts,
  };
}

function period(startIso: string, endIso: string, opts?: Partial<JournalPeriod>): JournalPeriod {
  return { period_start: startIso, period_end: endIso, open: false, entries: [], ...opts };
}

// ── countLabel ─────────────────────────────────────────────────────────────

describe("countLabel", () => {
  it("singularizes at exactly 1, pluralizes otherwise — the '1 SOURCES ACTIVE' fix", () => {
    expect(countLabel(1, "source active", "sources active")).toBe("source active");
    expect(countLabel(0, "source active", "sources active")).toBe("sources active");
    expect(countLabel(3, "source active", "sources active")).toBe("sources active");
  });

  it("defaults the plural to singular + s when omitted", () => {
    expect(countLabel(2, "ask")).toBe("asks");
    expect(countLabel(1, "ask")).toBe("ask");
  });
});

// ── `?z=` param + `journalHref` ───────────────────────────────────────────────

describe("parseZoomParam", () => {
  it("defaults to day for absent or unrecognized values", () => {
    expect(parseZoomParam(null)).toBe("day");
    expect(parseZoomParam(undefined)).toBe("day");
    expect(parseZoomParam("")).toBe("day");
    expect(parseZoomParam("year")).toBe("day");
  });

  it("accepts week and month", () => {
    expect(parseZoomParam("week")).toBe("week");
    expect(parseZoomParam("month")).toBe("month");
  });
});

describe("journalHref", () => {
  const today = new Date(2026, 7, 26);

  it("day zoom on today drops both params (the shipped live-anchor rule)", () => {
    expect(journalHref("day", today, today)).toBe("/today");
  });

  it("day zoom on a non-today day keeps ?d=", () => {
    expect(journalHref("day", new Date(2026, 7, 1), today)).toBe("/today?d=2026-08-01");
  });

  it("week/month zoom on the live period omits ?d=, keeping only &z=", () => {
    expect(journalHref("week", today, today)).toBe("/today?z=week");
    expect(journalHref("month", today, today)).toBe("/today?z=month");
  });

  it("week/month zoom on a past anchor carries both ?d= and &z=", () => {
    expect(journalHref("week", new Date(2026, 6, 20), today)).toBe("/today?d=2026-07-20&z=week");
    expect(journalHref("month", new Date(2026, 6, 20), today)).toBe("/today?d=2026-07-20&z=month");
  });

  it("appends &who=team only for Team, after d and z", () => {
    expect(journalHref("day", today, today, "team")).toBe("/today?who=team");
    expect(journalHref("day", today, today, "mine")).toBe("/today");
    expect(journalHref("week", new Date(2026, 6, 20), today, "team")).toBe("/today?d=2026-07-20&z=week&who=team");
  });
});

// ── Bullet rendering (ticket 15's bounded bullet-list contract) ──────────────

describe("splitBulletLines", () => {
  it("splits on newlines, trims, and drops blank lines", () => {
    expect(splitBulletLines("one\ntwo \n\n three \n")).toEqual(["one", "two", "three"]);
  });

  it("returns an empty array for blank input", () => {
    expect(splitBulletLines("   \n\n  ")).toEqual([]);
  });
});

describe("parseBulletLine", () => {
  it("splits a 'label — detail' line on the first em dash", () => {
    expect(parseBulletLine("Week rollups — fixed the fingerprint bug")).toEqual({
      label: "Week rollups",
      detail: "fixed the fingerprint bug",
      raw: "Week rollups — fixed the fingerprint bug",
    });
  });

  it("only splits on the FIRST separator (a detail can contain its own em dash)", () => {
    const raw = "Label — first — second";
    expect(parseBulletLine(raw)).toEqual({ label: "Label", detail: "first — second", raw });
  });

  it("has a null label when there's no separator (e.g. an overflow line)", () => {
    const raw = "+ 4 smaller captures not summarized · 31 items this hour";
    expect(parseBulletLine(raw)).toEqual({ label: null, detail: raw, raw });
  });
});

describe("parseBulletLines", () => {
  it("composes split + parse over a whole compiled summary", () => {
    const text = "Week rollups — fixed the bug\nTests — 12/12 green\n";
    expect(parseBulletLines(text)).toEqual([
      { label: "Week rollups", detail: "fixed the bug", raw: "Week rollups — fixed the bug" },
      { label: "Tests", detail: "12/12 green", raw: "Tests — 12/12 green" },
    ]);
  });
});

// ── Local week/month request-window bounds ────────────────────────────────────

describe("localWeekBounds", () => {
  it("starts on Monday regardless of which weekday the anchor is", () => {
    // 2026-08-20 is a Thursday.
    const { start, end } = localWeekBounds(new Date(2026, 7, 20));
    expect(start).toEqual(new Date(2026, 7, 17)); // Monday
    expect(end).toEqual(new Date(2026, 7, 24)); // next Monday
  });

  it("an anchor that IS a Monday starts that same day", () => {
    const { start, end } = localWeekBounds(new Date(2026, 7, 17));
    expect(start).toEqual(new Date(2026, 7, 17));
    expect(end).toEqual(new Date(2026, 7, 24));
  });

  it("an anchor on a Sunday belongs to the PRECEDING Monday's week", () => {
    const { start } = localWeekBounds(new Date(2026, 7, 23)); // Sunday
    expect(start).toEqual(new Date(2026, 7, 17));
  });
});

describe("localMonthBounds", () => {
  it("spans the local calendar month", () => {
    const { start, end } = localMonthBounds(new Date(2026, 7, 20));
    expect(start).toEqual(new Date(2026, 7, 1));
    expect(end).toEqual(new Date(2026, 8, 1));
  });

  it("rolls December into next January", () => {
    const { start, end } = localMonthBounds(new Date(2026, 11, 15));
    expect(start).toEqual(new Date(2026, 11, 1));
    expect(end).toEqual(new Date(2027, 0, 1));
  });
});

describe("zoomRequestRange", () => {
  it("day zoom matches localDayToUtcRange exactly", () => {
    const anchor = new Date(2026, 7, 20);
    expect(zoomRequestRange("day", anchor)).toEqual(localDayToUtcRange(anchor));
  });

  it("week/month zoom match their own local bounds helpers", () => {
    const anchor = new Date(2026, 7, 20);
    const week = zoomRequestRange("week", anchor);
    expect(week).toEqual({ from: localWeekBounds(anchor).start, to: localWeekBounds(anchor).end });
    const month = zoomRequestRange("month", anchor);
    expect(month).toEqual({ from: localMonthBounds(anchor).start, to: localMonthBounds(anchor).end });
  });
});

// ── Period rows + gap collapsing ──────────────────────────────────────────────

describe("buildTierRows", () => {
  it("returns one 'period' row per API period when they're contiguous", () => {
    const p1 = period("2026-08-01T00:00:00Z", "2026-08-02T00:00:00Z");
    const p2 = period("2026-08-02T00:00:00Z", "2026-08-03T00:00:00Z");
    const rows = buildTierRows([p1, p2]);
    expect(rows.map((r) => r.kind)).toEqual(["period", "period"]);
  });

  it("inserts a dashed gap row for a stretch between two periods with nothing returned", () => {
    const p1 = period("2026-08-01T00:00:00Z", "2026-08-02T00:00:00Z");
    const p2 = period("2026-08-04T00:00:00Z", "2026-08-05T00:00:00Z");
    const rows = buildTierRows([p1, p2]);
    expect(rows.map((r) => r.kind)).toEqual(["period", "gap", "period"]);
    const gap = rows[1];
    expect(gap.start).toEqual(new Date("2026-08-02T00:00:00Z"));
    expect(gap.end).toEqual(new Date("2026-08-04T00:00:00Z"));
  });

  it("never fabricates a leading or trailing gap — only BETWEEN two returned periods", () => {
    const p1 = period("2026-08-05T00:00:00Z", "2026-08-06T00:00:00Z");
    const rows = buildTierRows([p1]);
    expect(rows).toHaveLength(1);
    expect(rows[0].kind).toBe("period");
  });

  it("preserves each period's open flag and entries on the row", () => {
    const entries = [tierEntry("hello")];
    const p1 = period("2026-08-01T00:00:00Z", "2026-08-02T00:00:00Z", { open: true, entries });
    const [row] = buildTierRows([p1]);
    expect(row.kind).toBe("period");
    if (row.kind === "period") {
      expect(row.open).toBe(true);
      expect(row.entries).toBe(entries);
    }
  });
});

describe("periodGapLabel", () => {
  it("collapses a one-day gap to a single date (day grain)", () => {
    const row = { start: new Date(2026, 7, 20), end: new Date(2026, 7, 21) };
    expect(periodGapLabel(row, "day")).toBe("no activity · Aug 20");
  });

  it("renders a range for a multi-day gap (day grain), last day inclusive", () => {
    const row = { start: new Date(2026, 7, 20), end: new Date(2026, 7, 23) };
    expect(periodGapLabel(row, "day")).toBe("no activity · Aug 20–Aug 22");
  });

  it("collapses a one-week gap to a single week label (week grain)", () => {
    const row = { start: new Date(2026, 7, 3), end: new Date(2026, 7, 10) };
    expect(periodGapLabel(row, "week")).toBe("no activity · Week of Aug 3");
  });

  it("renders a range for a multi-week gap (week grain)", () => {
    const row = { start: new Date(2026, 7, 3), end: new Date(2026, 7, 17) };
    expect(periodGapLabel(row, "week")).toBe("no activity · Week of Aug 3 – Week of Aug 10");
  });

  // A DST fall-back day (US: Sun Nov 1, 2026 in America/Chicago, this suite's
  // pinned TZ) is 25 real hours wide — a raw `spanMs <= 86_400_000` check
  // alone would treat that as a multi-day span and render a duplicated
  // range ("Nov 1–Nov 1"), since `end - 1ms` still lands on the SAME
  // calendar day the span started on. The label-equality guard must collapse
  // it to the single-boundary form regardless of the raw span length.
  it("collapses a DST-stretched (25h) single-day gap instead of duplicating the date", () => {
    const start = new Date(2026, 10, 1); // Sun Nov 1, 2026 local midnight (CDT)
    const end = new Date(start.getTime() + 25 * 3_600_000); // the fall-back day is 25h wide
    expect(periodGapLabel({ start, end }, "day")).toBe("no activity · Nov 1");
  });

  it("collapses a DST-stretched (169h) single-week gap instead of duplicating the week", () => {
    const start = new Date(2026, 9, 26); // Monday Oct 26, 2026 — the week containing Nov 1's fall-back
    const end = new Date(start.getTime() + (7 * 24 + 1) * 3_600_000); // 169h, one hour longer than a normal week
    expect(periodGapLabel({ start, end }, "week")).toBe("no activity · Week of Oct 26");
  });
});

describe("filterPeriodEntriesByGraph", () => {
  it("narrows each period's entries to the given graph_id", () => {
    const entries = [tierEntry("a", { graph_id: "g1" }), tierEntry("b", { graph_id: "g2" })];
    const periods = [period("2026-08-01T00:00:00Z", "2026-08-02T00:00:00Z", { entries })];
    const filtered = filterPeriodEntriesByGraph(periods, "g2");
    expect(filtered[0].entries).toHaveLength(1);
    expect(filtered[0].entries[0].graph_id).toBe("g2");
  });

  it("passes periods through unchanged when graphId is null", () => {
    const periods = [period("2026-08-01T00:00:00Z", "2026-08-02T00:00:00Z", { entries: [tierEntry("a")] })];
    expect(filterPeriodEntriesByGraph(periods, null)).toBe(periods);
  });
});

describe("periodRowStats", () => {
  it("sums atom_count/asks across one period's entries", () => {
    const entries = [
      tierEntry("a", { stats: { atom_count: 5, asks: 1 } }),
      tierEntry("b", { stats: { atom_count: 3, asks: 2 } }),
    ];
    expect(periodRowStats(entries)).toEqual({ atoms: 8, asks: 3 });
  });

  it("is all-zero for no entries", () => {
    expect(periodRowStats([])).toEqual({ atoms: 0, asks: 0 });
  });
});

describe("statsForPeriods", () => {
  it("aggregates atoms/asks/sources across periods and counts active ones", () => {
    const periods = [
      period("2026-08-01T00:00:00Z", "2026-08-02T00:00:00Z", {
        entries: [tierEntry("a", { stats: { atom_count: 10, asks: 2, sources: { slack: 5, note: 5 } } })],
      }),
      period("2026-08-02T00:00:00Z", "2026-08-03T00:00:00Z", { entries: [] }), // no activity — not "active"
      period("2026-08-03T00:00:00Z", "2026-08-04T00:00:00Z", {
        entries: [tierEntry("b", { stats: { atom_count: 4, asks: 1, sources: { slack: 4 } } })],
      }),
    ];
    expect(statsForPeriods(periods)).toEqual({ atoms: 14, sourcesActive: 2, asks: 3, periodsActive: 2 });
  });
});

describe("intensityBucketForZoom", () => {
  it("buckets week-grain child periods on the week thresholds", () => {
    expect(intensityBucketForZoom("week", 0)).toBe(0);
    expect(intensityBucketForZoom("week", 5)).toBe(1);
    expect(intensityBucketForZoom("week", 25)).toBe(2);
    expect(intensityBucketForZoom("week", 50)).toBe(3);
  });

  it("buckets month-grain child periods on the (higher) month thresholds", () => {
    expect(intensityBucketForZoom("month", 20)).toBe(1);
    expect(intensityBucketForZoom("month", 80)).toBe(2);
    expect(intensityBucketForZoom("month", 200)).toBe(3);
  });
});

describe("intensityForPeriods", () => {
  it("returns one bucket per period, in order", () => {
    const periods = [
      period("2026-08-01T00:00:00Z", "2026-08-02T00:00:00Z", { entries: [tierEntry("a", { stats: { atom_count: 0 } })] }),
      period("2026-08-02T00:00:00Z", "2026-08-03T00:00:00Z", { entries: [tierEntry("b", { stats: { atom_count: 50 } })] }),
    ];
    expect(intensityForPeriods("week", periods)).toEqual([0, 3]);
  });
});

// ── Week/month header titles + row labels ─────────────────────────────────────

describe("weekHeaderTitle", () => {
  it("plain label when the week is closed", () => {
    expect(weekHeaderTitle(new Date(2026, 7, 17), false)).toEqual({ title: "Week of August 17" });
  });

  it("appends '· this week' (italicized) when the week is open", () => {
    expect(weekHeaderTitle(new Date(2026, 7, 17), true)).toEqual({
      title: "Week of August 17 · this week",
      italicWord: "this week",
    });
  });
});

describe("monthHeaderTitle", () => {
  it("plain label when the month is closed", () => {
    expect(monthHeaderTitle(new Date(2026, 7, 1), false)).toEqual({ title: "August 2026" });
  });

  it("appends '· this month' (italicized) when the month is open", () => {
    expect(monthHeaderTitle(new Date(2026, 7, 1), true)).toEqual({
      title: "August 2026 · this month",
      italicWord: "this month",
    });
  });
});

describe("row/crumb labels", () => {
  it("weekRowLabel uses the abbreviated month", () => {
    expect(weekRowLabel(new Date(2026, 7, 17))).toBe("Week of Aug 17");
  });

  it("dayRowLabel includes the weekday", () => {
    expect(dayRowLabel(new Date(2026, 7, 26))).toBe("Wed, Aug 26");
  });

  it("monthCrumbLabel is the long month + year", () => {
    expect(monthCrumbLabel(new Date(2026, 7, 1))).toBe("August 2026");
  });

  it("weekdayTickLabel is just the abbreviated weekday, never sliced from dayRowLabel", () => {
    // A composed label's component order is locale-dependent (e.g. ja-JP
    // puts the date first) — `weekdayTickLabel` must format the weekday
    // directly rather than slicing `dayRowLabel`'s output.
    expect(weekdayTickLabel(new Date(2026, 7, 26))).toBe("Wed");
  });
});

// ── "Ask about this week/month" ───────────────────────────────────────────────

describe("askAboutWeekPhrase / askAboutWeekHref", () => {
  it("phrases the week's Monday", () => {
    expect(askAboutWeekPhrase(new Date(2026, 7, 17))).toBe("What happened the week of August 17?");
  });

  it("URL-encodes into /chats?q=", () => {
    const href = askAboutWeekHref(new Date(2026, 7, 17));
    expect(href).toBe(`/chats?q=${encodeURIComponent("What happened the week of August 17?")}`);
  });
});

describe("askAboutMonthPhrase / askAboutMonthHref", () => {
  it("phrases the month name + year", () => {
    expect(askAboutMonthPhrase(new Date(2026, 7, 1))).toBe("What happened in August 2026?");
  });

  it("URL-encodes into /chats?q=", () => {
    const href = askAboutMonthHref(new Date(2026, 7, 1));
    expect(href).toBe(`/chats?q=${encodeURIComponent("What happened in August 2026?")}`);
  });
});

// ── Hour drill-down target resolution ─────────────────────────────────────────

describe("hourStartForDrilldown", () => {
  const row = { start: new Date("2026-08-01T10:00:00Z") };

  it("uses the single matching API hour's own hour_start", () => {
    const hours = [hour("2026-08-01T10:00:00Z", 5)];
    expect(hourStartForDrilldown(row, hours)).toBe("2026-08-01T10:00:00Z");
  });

  it("falls back to the row's own boundary when no API hour matched but it's a real UTC hour boundary", () => {
    expect(hourStartForDrilldown(row, [])).toBe(row.start.toISOString());
  });

  it("returns null when the row merges more than one API hour bucket", () => {
    const hours = [hour("2026-08-01T10:00:00Z", 1), hour("2026-08-01T10:30:00Z", 1)];
    // Both fall inside [10:00, 11:00) — the half-hour-offset edge case.
    expect(hourStartForDrilldown(row, hours)).toBe(null);
  });

  it("returns null when the row's own boundary isn't an exact UTC hour", () => {
    const offRow = { start: new Date("2026-08-01T10:30:00Z") };
    expect(hourStartForDrilldown(offRow, [])).toBe(null);
  });
});

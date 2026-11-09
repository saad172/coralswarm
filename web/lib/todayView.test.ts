import { describe, expect, it } from "vitest";
import type { Graph } from "./api";
import type { Activity } from "./journalActivitiesApi";
import {
  buildStrip,
  buildTimeline,
  dayBars,
  dedupeActivities,
  densityStorageKey,
  durationLabel,
  emptyStrip,
  filterPillLabel,
  formatDuration,
  hourBars,
  monthChartDays,
  peopleRail,
  periodMeta,
  periodSpan,
  periodStats,
  periodTitle,
  periodWindow,
  readDensity,
  selectionDays,
  selfNames,
  sortActivities,
  stepIndex,
  stripWindow,
  teamGraph,
  timeSplit,
  toggleFilter,
  topicRail,
  weekChartDays,
  writeDensity,
} from "./todayView";

const TZ = "UTC";

let n = 0;
function act(start: string, end: string, over: Partial<Activity> = {}): Activity {
  n += 1;
  return {
    key: `k${n}`,
    kind: "meeting",
    kind_label: "meeting",
    owner: { user_id: "u-me", name: "Dana Ross" },
    start: `${start}Z`,
    end: `${end}Z`,
    graph: { id: "g1", name: "Private" },
    title: `Activity ${n}`,
    outcome: null,
    people: [],
    people_count: 0,
    topics: [],
    decision_count: 0,
    source_count: 1,
    ...over,
  };
}

describe("ordering", () => {
  it("sorts by start, then ocean, then key, and does not mutate", () => {
    const late = act("2026-10-29T14:00:00", "2026-10-29T15:00:00");
    const early = act("2026-10-29T09:00:00", "2026-10-29T09:30:00");
    const input = [late, early];
    expect(sortActivities(input).map((a) => a.key)).toEqual([early.key, late.key]);
    expect(input[0]).toBe(late);
  });

  it("dedupes by ocean and key, keeping the first", () => {
    const a = act("2026-10-29T09:00:00", "2026-10-29T10:00:00");
    const twin = { ...a, title: "twin" };
    const otherOcean = { ...a, graph: { id: "g2", name: "Platform" } };
    expect(dedupeActivities([a, twin, otherOcean])).toEqual([a, otherOcean]);
  });
});

describe("durations", () => {
  it("formats minutes", () => {
    expect(formatDuration(45)).toBe("45m");
    expect(formatDuration(60)).toBe("1h");
    expect(formatDuration(70)).toBe("1h 10m");
  });

  it("labels a moment as note for a note and <1m otherwise", () => {
    const at = "2026-10-29T09:00:00Z";
    expect(durationLabel({ start: at, end: at, kind: "note" })).toBe("note");
    expect(durationLabel({ start: at, end: at, kind: "slack" })).toBe("<1m");
    expect(durationLabel({ start: at, end: "2026-10-29T09:42:00Z", kind: "meeting" })).toBe("42m");
  });
});

describe("buildTimeline, Day", () => {
  const a = act("2026-10-29T09:10:00", "2026-10-29T10:00:00");
  const b = act("2026-10-29T11:10:00", "2026-10-29T11:40:00"); // 1h10 after a ends
  const c = act("2026-10-29T11:50:00", "2026-10-29T12:30:00"); // 10m after b: no gap
  const d = act("2026-10-29T13:15:00", "2026-10-29T13:20:00"); // 45m after c: a gap (boundary)

  it("sorts, and inserts a quiet row for every gap of 45 minutes or more", () => {
    const t = buildTimeline({ activities: [d, c, a, b], zoom: "day", tz: TZ, filter: null, compact: false });
    expect(t.rows.map((r) => (r.type === "gap" ? `gap ${r.start} ${r.label}` : r.type === "item" ? r.activity.key : r.type))).toEqual([
      a.key,
      "gap 10:00 1h 10m",
      b.key,
      c.key,
      "gap 12:30 45m",
      d.key,
    ]);
    expect(t.visible.map((x) => x.key)).toEqual([a.key, b.key, c.key, d.key]);
    expect(t.rows.filter((r) => r.type === "item").map((r) => (r.type === "item" ? r.index : -1))).toEqual([0, 1, 2, 3]);
  });

  it("measures a gap from the latest end so far, not the previous item's end", () => {
    const long = act("2026-10-29T09:00:00", "2026-10-29T12:00:00");
    const inside = act("2026-10-29T09:30:00", "2026-10-29T09:40:00");
    const after = act("2026-10-29T12:20:00", "2026-10-29T12:30:00");
    const t = buildTimeline({ activities: [long, inside, after], zoom: "day", tz: TZ, filter: null, compact: false });
    expect(t.rows.some((r) => r.type === "gap")).toBe(false);
  });

  it("has no gaps in Compact mode", () => {
    const t = buildTimeline({ activities: [a, b, c, d], zoom: "day", tz: TZ, filter: null, compact: true });
    expect(t.rows.some((r) => r.type === "gap")).toBe(false);
  });

  it("filters by person and by topic, keeping the unfiltered list for stats", () => {
    const withPriya = act("2026-10-29T09:00:00", "2026-10-29T09:30:00", {
      people: ["Priya Nair"],
      topics: [{ node_id: "t1", title: "Rate limiter" }],
    });
    const other = act("2026-10-29T10:00:00", "2026-10-29T10:30:00", { people: ["Tom Becker"] });
    const person = buildTimeline({
      activities: [withPriya, other],
      zoom: "day",
      tz: TZ,
      filter: { type: "person", key: "Priya Nair", label: "Priya Nair" },
      compact: false,
    });
    expect(person.visible).toEqual([withPriya]);
    expect(person.all).toHaveLength(2);
    const topic = buildTimeline({
      activities: [withPriya, other],
      zoom: "day",
      tz: TZ,
      filter: { type: "topic", key: "t1", label: "Rate limiter" },
      compact: false,
    });
    expect(topic.visible).toEqual([withPriya]);
  });
});

describe("filter toggle", () => {
  const priya = { type: "person", key: "Priya Nair", label: "Priya Nair" } as const;
  const topic = { type: "topic", key: "t1", label: "Rate limiter" } as const;

  it("sets, switches and clears", () => {
    expect(toggleFilter(null, priya)).toEqual(priya);
    expect(toggleFilter(priya, priya)).toBeNull();
    expect(toggleFilter(priya, topic)).toEqual(topic);
    // A person and a topic that share a key are different filters.
    expect(toggleFilter({ type: "person", key: "t1", label: "x" }, topic)).toEqual(topic);
  });

  it("labels the pill", () => {
    expect(filterPillLabel(priya)).toBe("with Priya Nair");
    expect(filterPillLabel(topic)).toBe("topic: Rate limiter");
  });
});

describe("buildTimeline, Week and Month", () => {
  const thu = act("2026-10-29T14:00:00", "2026-10-29T15:00:00");
  const thuEarly = act("2026-10-29T09:00:00", "2026-10-29T09:30:00");
  const tue = act("2026-10-27T10:00:00", "2026-10-27T10:30:00");
  const prevWeek = act("2026-10-21T10:00:00", "2026-10-21T10:30:00");

  it("groups Week by day, newest day first and oldest item first, with no gap rows", () => {
    const t = buildTimeline({ activities: [tue, thu, thuEarly], zoom: "week", tz: TZ, filter: null, compact: false });
    expect(t.rows.map((r) => (r.type === "head" ? `# ${r.label}` : r.type === "item" ? r.activity.key : r.type))).toEqual([
      "# thu 29 oct",
      thuEarly.key,
      thu.key,
      "# tue 27 oct",
      tue.key,
    ]);
  });

  it("groups Month by Monday-start week, newest first", () => {
    const t = buildTimeline({ activities: [prevWeek, tue, thu], zoom: "month", tz: TZ, filter: null, compact: false });
    expect(t.rows.filter((r) => r.type === "head").map((r) => (r.type === "head" ? r.label : ""))).toEqual([
      "week of 26 oct",
      "week of 19 oct",
    ]);
    // 27 and 29 Oct are the same week (Mon 26 Oct).
    expect(t.rows.filter((r) => r.type === "item")).toHaveLength(3);
  });

  it("dedupes in Month and drops a group a filter empties", () => {
    const dup = { ...tue };
    const t = buildTimeline({
      activities: [tue, dup, prevWeek],
      zoom: "month",
      tz: TZ,
      filter: { type: "person", key: "Nobody", label: "Nobody" },
      compact: false,
    });
    expect(t.all).toHaveLength(2);
    expect(t.rows).toEqual([]);
    expect(t.visible).toEqual([]);
  });
});

describe("stepIndex", () => {
  it("steps and wraps at both ends", () => {
    expect(stepIndex(0, 3, 1)).toBe(1);
    expect(stepIndex(2, 3, 1)).toBe(0);
    expect(stepIndex(0, 3, -1)).toBe(2);
    expect(stepIndex(0, 1, 1)).toBe(0);
    expect(stepIndex(0, 0, 1)).toBe(0);
  });
});

describe("stats, span and meta", () => {
  const list = [
    act("2026-10-29T09:10:00", "2026-10-29T10:00:00", { kind: "meeting", decision_count: 2 }),
    act("2026-10-29T11:00:00", "2026-10-29T12:30:00", { kind: "agent", decision_count: 1 }),
    act("2026-10-29T13:00:00", "2026-10-29T13:20:00", { kind: "doc" }),
    act("2026-10-29T16:00:00", "2026-10-29T16:20:00", { kind: "slack" }),
  ];

  it("counts activities, meetings, sessions (agent + doc) and decisions", () => {
    expect(periodStats(list)).toEqual({ activities: 4, meetings: 1, sessions: 2, decisions: 3 });
  });

  it("spans first start to last end", () => {
    expect(periodSpan(list, TZ)).toEqual({ from: "09:10", to: "16:20" });
    expect(periodSpan([], TZ)).toBeNull();
  });

  it("writes the meta line", () => {
    const span = { from: "09:10", to: "16:20" };
    expect(periodMeta({ zoom: "day", who: "mine", count: 6, span })).toBe("6 activities · 09:10–16:20");
    expect(periodMeta({ zoom: "day", who: "team", count: 1, span })).toBe("team · 1 activity · 09:10–16:20");
    expect(periodMeta({ zoom: "week", who: "mine", count: 6, span })).toBe("6 activities");
    expect(periodMeta({ zoom: "day", who: "team", count: 0, span: null })).toBe("");
  });
});

describe("charts", () => {
  it("buckets minutes by hour and kind, clipping to hours 8 to 18", () => {
    const list = [
      act("2026-10-29T09:30:00", "2026-10-29T10:30:00", { kind: "meeting" }), // 30m in 9, 30m in 10
      act("2026-10-29T10:00:00", "2026-10-29T10:20:00", { kind: "agent" }),
      act("2026-10-29T07:00:00", "2026-10-29T07:50:00", { kind: "slack" }), // outside the window
      act("2026-10-29T18:30:00", "2026-10-29T18:30:00", { kind: "note" }), // a moment counts 5m
    ];
    const bars = hourBars(list, TZ);
    expect(bars).toHaveLength(11);
    expect(bars[0].id).toBe("h8");
    expect(bars[10].id).toBe("h18");
    const at = (h: number) => bars[h - 8];
    expect(at(9).minutes).toEqual({ meeting: 30 });
    expect(at(10).minutes).toEqual({ meeting: 30, agent: 20 });
    expect(at(18).minutes).toEqual({ note: 5 });
    expect(at(8).total).toBe(0);
    // The busiest hour is full height, the empty one a 2% stub on the line.
    expect(at(10).heightPct).toBe(100);
    expect(at(8).heightPct).toBe(2);
    expect(at(8).segs).toEqual([{ kind: null, value: 1 }]);
    // Labels on every other hour.
    expect(bars.map((b) => b.label)).toEqual(["8", "", "10", "", "12", "", "14", "", "16", "", "18"]);
  });

  it("gives a tiny non-empty bucket at least 6%", () => {
    const list = [
      act("2026-10-29T09:00:00", "2026-10-29T10:00:00", { kind: "meeting" }),
      act("2026-10-29T14:00:00", "2026-10-29T14:00:00", { kind: "note" }),
    ];
    const bars = hourBars(list, TZ);
    expect(bars[14 - 8].heightPct).toBe(Math.max(6, (5 / 60) * 100));
  });

  it("buckets Week by local day with two-letter labels", () => {
    const list = [
      act("2026-10-27T10:00:00", "2026-10-27T11:00:00", { kind: "meeting" }),
      act("2026-10-27T15:00:00", "2026-10-27T15:30:00", { kind: "doc" }),
      act("2026-10-29T10:00:00", "2026-10-29T10:00:00", { kind: "slack" }),
    ];
    const bars = dayBars(list, weekChartDays(new Date(2026, 9, 29)), "week", TZ);
    expect(bars.map((b) => b.id)).toEqual([
      "2026-10-26", "2026-10-27", "2026-10-28", "2026-10-29", "2026-10-30", "2026-10-31", "2026-11-01",
    ]);
    expect(bars.map((b) => b.label)).toEqual(["mo", "tu", "we", "th", "fr", "sa", "su"]);
    expect(bars[1].minutes).toEqual({ meeting: 60, doc: 30 });
    expect(bars[3].minutes).toEqual({ slack: 5 });
    expect(bars[1].segs.map((s) => s.kind)).toEqual(["meeting", "doc"]);
  });

  it("labels Month's 14 days on Mondays only, ending today while the month is open", () => {
    const days = monthChartDays(new Date(2026, 9, 10), new Date(2026, 9, 29, 12));
    expect(days).toHaveLength(14);
    expect(days[13]).toBe("2026-10-29");
    expect(days[0]).toBe("2026-10-16");
    const bars = dayBars([], days, "month", TZ);
    expect(bars.filter((b) => b.label).map((b) => [b.id, b.label])).toEqual([
      ["2026-10-19", "19"],
      ["2026-10-26", "26"],
    ]);
    // A closed month ends on its last day.
    expect(monthChartDays(new Date(2026, 8, 10), new Date(2026, 9, 29))[13]).toBe("2026-09-30");
  });

  it("sums the split per kind in chart order and drops empty kinds", () => {
    const list = [
      act("2026-10-29T09:00:00", "2026-10-29T10:30:00", { kind: "meeting" }),
      act("2026-10-29T11:00:00", "2026-10-29T11:20:00", { kind: "agent" }),
    ];
    const split = timeSplit(hourBars(list, TZ));
    expect(split.items.map((i) => [i.kind, i.name, i.label])).toEqual([
      ["meeting", "Meetings", "1h 30m"],
      ["agent", "Agent sessions", "20m"],
    ]);
    expect(split.totalMin).toBe(110);
  });
});

describe("rails", () => {
  const me = "Dana Ross";
  const list = [
    act("2026-10-29T09:00:00", "2026-10-29T10:00:00", {
      people: [me, "Priya Nair", "Tom Becker"],
      topics: [{ node_id: "t1", title: "Rate limiter" }, { node_id: "t2", title: "Billing" }],
    }),
    act("2026-10-29T11:00:00", "2026-10-29T12:00:00", {
      people: [me, "Priya Nair"],
      topics: [{ node_id: "t1", title: "Rate limiter" }],
    }),
  ];

  it("finds my own names from the cards I own", () => {
    expect(selfNames(list, "u-me")).toEqual(new Set([me]));
    expect(selfNames(list, null)).toEqual(new Set());
  });

  it("leaves me out of People in Mine and keeps me in Team", () => {
    const self = selfNames(list, "u-me");
    expect(peopleRail(list, { who: "mine", self }).map((c) => [c.label, c.count])).toEqual([
      ["Priya Nair", 2],
      ["Tom Becker", 1],
    ]);
    expect(peopleRail(list, { who: "team", self }).map((c) => [c.label, c.count])).toEqual([
      [me, 2],
      ["Priya Nair", 2],
      ["Tom Becker", 1],
    ]);
  });

  it("counts topics by node id and ranks by count", () => {
    expect(topicRail(list).map((c) => [c.key, c.label, c.count])).toEqual([
      ["t1", "Rate limiter", 2],
      ["t2", "Billing", 1],
    ]);
  });

  it("caps each rail at 8", () => {
    const many = [act("2026-10-29T09:00:00", "2026-10-29T10:00:00", { people: Array.from({ length: 12 }, (_, i) => `P${i}`) })];
    expect(peopleRail(many, { who: "team", self: new Set() })).toHaveLength(8);
  });
});

describe("titles and windows", () => {
  it("titles each zoom", () => {
    expect(periodTitle("day", new Date(2026, 9, 29))).toBe("Thursday 29 October");
    expect(periodTitle("week", new Date(2026, 9, 29))).toBe("Week of 26 October – 1 November");
    expect(periodTitle("week", new Date(2026, 9, 22))).toBe("Week of 19 – 25 October");
    expect(periodTitle("month", new Date(2026, 9, 29))).toBe("October 2026");
  });

  it("windows the visible period and the strip", () => {
    const day = periodWindow("day", new Date(2026, 9, 29, 15));
    expect(new Date(day.to).getTime() - new Date(day.from).getTime()).toBeLessThanOrEqual(25 * 3600_000);
    expect(stripWindow(new Date(2026, 9, 29, 15))).toEqual(day);
    const month = periodWindow("month", new Date(2026, 9, 29));
    expect(Math.round((Date.parse(month.to) - Date.parse(month.from)) / 86_400_000)).toBe(31);
  });
});

describe("date strip", () => {
  const dayActivity = [
    { day: "2026-10-27", count: 0 },
    { day: "2026-10-28", count: 1 },
    { day: "2026-10-29", count: 5 },
  ];

  it("sizes and tones the dots by count", () => {
    const cells = buildStrip(dayActivity, { zoom: "day", from: "2026-10-29", to: "2026-10-29" });
    expect(cells.map((c) => [c.dotPx, c.dotTone])).toEqual([
      [4, "none"],
      [4, "muted"],
      [6, "brand"],
    ]);
    expect(cells[2].weekday).toBe("thu");
    expect(cells[2].date).toBe(29);
  });

  it("selects one day with a shadow in Day, and a flat range in Week and Month", () => {
    const day = buildStrip(dayActivity, { zoom: "day", from: "2026-10-28", to: "2026-10-28" });
    expect(day.map((c) => [c.selected, c.shadow])).toEqual([[false, false], [true, true], [false, false]]);
    const week = buildStrip(dayActivity, { zoom: "week", from: "2026-10-26", to: "2026-11-01" });
    expect(week.map((c) => [c.selected, c.shadow])).toEqual([[true, false], [true, false], [true, false]]);
  });

  it("finds a period's first and last day, and an empty strip is 14 quiet days", () => {
    expect(selectionDays("week", new Date(2026, 9, 29))).toEqual({ from: "2026-10-26", to: "2026-11-01" });
    expect(selectionDays("month", new Date(2026, 9, 29))).toEqual({ from: "2026-10-01", to: "2026-10-31" });
    expect(selectionDays("day", new Date(2026, 9, 29))).toEqual({ from: "2026-10-29", to: "2026-10-29" });
    const empty = emptyStrip(new Date(2026, 9, 29));
    expect(empty).toHaveLength(14);
    expect(empty[13]).toEqual({ day: "2026-10-29", count: 0 });
  });
});

describe("teamGraph", () => {
  const g = (id: string, kind: Graph["kind"]) => ({ id, name: id, kind }) as Graph;
  const mine = g("mine", "personal");
  const team = g("team", "team");
  const org = g("org", "org");

  it("uses the switcher's workspace when it is a shared one", () => {
    expect(teamGraph([mine, team, org], org)).toBe(org);
  });

  it("otherwise the first readable team or org ocean", () => {
    expect(teamGraph([mine, team, org], mine)).toBe(team);
    expect(teamGraph([mine, org], null)).toBe(org);
  });

  it("is null with no shared ocean, which hides Team", () => {
    expect(teamGraph([mine, g("m", "member")], mine)).toBeNull();
    expect(teamGraph([], null)).toBeNull();
  });
});

describe("Cards / Compact preference", () => {
  const store = () => {
    const m = new Map<string, string>();
    return { getItem: (k: string) => m.get(k) ?? null, setItem: (k: string, v: string) => void m.set(k, v) };
  };

  it("defaults to Cards and round-trips Compact per user", () => {
    const s = store();
    expect(readDensity(s, densityStorageKey("a"))).toBe("cards");
    writeDensity(s, densityStorageKey("a"), "compact");
    expect(readDensity(s, densityStorageKey("a"))).toBe("compact");
    expect(readDensity(s, densityStorageKey("b"))).toBe("cards");
    writeDensity(s, densityStorageKey("a"), "cards");
    expect(readDensity(s, densityStorageKey("a"))).toBe("cards");
  });

  it("treats a broken or missing store, or a junk value, as Cards", () => {
    const broken = {
      getItem: () => {
        throw new Error("denied");
      },
      setItem: () => {
        throw new Error("denied");
      },
    };
    expect(readDensity(broken, "k")).toBe("cards");
    expect(() => writeDensity(broken, "k", "compact")).not.toThrow();
    expect(readDensity(null, "k")).toBe("cards");
    expect(readDensity({ getItem: () => "huge" }, "k")).toBe("cards");
  });
});

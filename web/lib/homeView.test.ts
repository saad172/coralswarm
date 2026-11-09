import { describe, expect, it } from "vitest";
import type { Activity } from "./journalActivitiesApi";
import type { HomeKpi, HomeMetrics, SourceGroupId } from "./pageSectionTypes/home";
import {
  FLAT_BAR_COLOR,
  agoShort,
  buildHeatmap,
  buildIngestChart,
  chartTop,
  connectorReconnectHref,
  dayLabel,
  feedCountLabel,
  formatKpiDelta,
  groupFeed,
  heatLevel,
  oceanRank,
  rangeLabel,
  sourceSplit,
  sparkBars,
} from "./homeView";

const kpi = (total: number, prior: number, daily: number[] = []): HomeKpi => ({ total, prior_total: prior, daily });

describe("KPI delta", () => {
  it("rises are ok-toned with a plus", () => {
    expect(formatKpiDelta(kpi(118, 100))).toEqual({ text: "+18%", tone: "ok" });
  });
  it("falls use a true minus and the muted tone (never red)", () => {
    expect(formatKpiDelta(kpi(96, 100))).toEqual({ text: "−4%", tone: "muted" });
  });
  it("flat is 0% and muted", () => {
    expect(formatKpiDelta(kpi(5, 5))).toEqual({ text: "0%", tone: "muted" });
  });
  it("no prior window hides the delta", () => {
    expect(formatKpiDelta(kpi(5, 0))).toBeNull();
  });
});

describe("sparkbars", () => {
  it("scales to the max in 24px, keeps a 2px stub, and marks only the last bar", () => {
    const bars = sparkBars([0, 12, 24]);
    expect(bars.map((b) => b.h)).toEqual([2, 12, 24]);
    expect(bars.map((b) => b.today)).toEqual([false, false, true]);
  });
  it("an all-zero series does not divide by zero", () => {
    expect(sparkBars([0, 0]).map((b) => b.h)).toEqual([2, 2]);
  });
});

// Mon 26 Oct .. Thu 29 Oct 2026 (29 Oct is a Thursday).
const days = ["2026-10-26", "2026-10-27", "2026-10-28", "2026-10-29"];
const split = (n: number, g: SourceGroupId = "chat") => ({ [g]: n });
const metrics = (over: Partial<HomeMetrics> = {}): HomeMetrics => ({
  window: { start: "", end: "", days: 4, tz: "UTC" },
  kpis: {
    corals_ingested: kpi(30, 10, [5, 20, 0, 5]),
    reefs_formed: kpi(0, 0),
    questions_asked: kpi(0, 0),
    agent_sessions: kpi(0, 0),
    meetings_captured: kpi(0, 0),
  },
  ingest_by_day: [
    { day: days[0], by_graph: [{ graph_id: "p", by_source_type: split(3) }, { graph_id: "o", by_source_type: split(2, "meetings") }] },
    { day: days[1], by_graph: [{ graph_id: "p", by_source_type: { chat: 10, docs_mail: 5 } }, { graph_id: "o", by_source_type: split(5) }] },
    { day: days[2], by_graph: [] },
    { day: days[3], by_graph: [{ graph_id: "o", by_source_type: split(1) }] },
  ],
  oceans: [
    { graph_id: "p", name: "Private", kind: "member", total: 18, by_source_type: { chat: 13, docs_mail: 5 } },
    { graph_id: "o", name: "Company", kind: "org", total: 8, by_source_type: { meetings: 2, chat: 6 } },
  ],
  source_type_totals: { chat: 19, meetings: 2, agent_sessions: 0, docs_mail: 5, code_tickets: 0 },
  source_type_groups: [],
  fallback_group: "chat",
  ...over,
});

describe("ingested-per-day bucketing", () => {
  const chart = buildIngestChart(metrics());
  it("sums each day across oceans and rounds the axis up to 50", () => {
    expect(chart.days.map((d) => d.total)).toEqual([5, 20, 0, 1]);
    expect(chart.top).toBe(50);
    expect(chartTop(51)).toBe(100);
    expect(chartTop(0)).toBe(50);
    expect(chart.yTicks.map((t) => t.value)).toEqual([0, 13, 25, 38, 50]);
    expect(chart.days[1].heightPct).toBeCloseTo(40);
  });
  it("labels Mondays only, with the day number", () => {
    expect(chart.days.map((d) => d.xLabel)).toEqual(["26", "", "", ""]);
  });
  it("tooltips read like the design", () => {
    expect(chart.days[3].tip).toBe("thu 29 oct · 1 coral");
    expect(chart.days[1].tip).toBe("tue 27 oct · 20 corals");
    expect(dayLabel("2026-10-29")).toBe("thu 29 oct");
  });
  it("stacks by ocean in the server's order, private pink and the other hashed", () => {
    expect(chart.stacked).toBe(true);
    expect(chart.days[0].segments.map((s) => s.graphId)).toEqual(["p", "o"]);
    expect(chart.days[0].segments[0].color).toBe("var(--hue-pink)");
    expect(chart.days[0].segments[1].color).toMatch(/^var\(--hue-(teal|amber|plum|coral)\)$/);
    expect(chart.legend.map((l) => l.name)).toEqual(["Private", "Company"]);
  });
  it("falls back to single-colour bars from the KPI series when no day has a split", () => {
    const flat = buildIngestChart(metrics({ ingest_by_day: days.map((day) => ({ day, by_graph: [] })) }));
    expect(flat.stacked).toBe(false);
    expect(flat.days.map((d) => d.total)).toEqual([5, 20, 0, 5]);
    expect(flat.days[0].segments).toEqual([{ graphId: "", name: "Corals", value: 5, color: FLAT_BAR_COLOR }]);
  });
});

describe("source-type split", () => {
  it("uses the five-group colours, drops empty groups and shows percentages", () => {
    const s = sourceSplit(metrics());
    expect(s.items.map((i) => [i.id, i.pct])).toEqual([
      ["chat", "73%"],
      ["meetings", "8%"],
      ["docs_mail", "19%"],
    ]);
    expect(s.items[0].color).toBe("var(--src-chat)");
    expect(s.items[2].color).toBe("var(--src-docs-mail)");
    expect(s.totalLabel).toBe("26 corals");
  });
});

describe("oceans by volume", () => {
  it("ranks by volume, names the dominant type and scales to the largest", () => {
    const rows = oceanRank(metrics());
    expect(rows.map((r) => r.name)).toEqual(["Private", "Company"]);
    expect(rows[0]).toMatchObject({ sub: "mostly chat", widthPct: 100, valueLabel: "18" });
    expect(rows[1].widthPct).toBeCloseTo((8 / 18) * 100);
    expect(rows[1].segments.map((s) => s.id)).toEqual(["chat", "meetings"]);
  });
  it("an ocean with no split gets one flat segment", () => {
    const rows = oceanRank(metrics({ oceans: [{ graph_id: "x", name: "X", kind: "org", total: 4, by_source_type: {} }] }));
    expect(rows[0].sub).toBe("");
    expect(rows[0].segments).toHaveLength(1);
  });
});

describe("heatmap", () => {
  it("is Monday-first with blank lead and tail, and marks today and the busiest day", () => {
    const h = buildHeatmap([
      { day: "2026-10-28", total: 1 }, // Wed
      { day: "2026-10-29", total: 4 },
    ]);
    expect(h).not.toBeNull();
    expect(h!.cells).toHaveLength(7);
    expect(h!.cells.slice(0, 2).every((c) => c.blank)).toBe(true);
    expect(h!.cells[2]).toMatchObject({ label: "28", level: 1, today: false });
    expect(h!.cells[3]).toMatchObject({ label: "29", level: 4, today: true });
    expect(h!.busiest).toBe("busiest · thu 29 oct");
  });
  it("levels: 0 for none, 1..4 across the range", () => {
    expect([0, 1, 50, 99, 100].map((v) => heatLevel(v, 100))).toEqual([0, 1, 2, 4, 4]);
  });
  it("is hidden with no days", () => {
    expect(buildHeatmap([])).toBeNull();
  });
});

it("range line", () => {
  expect(rangeLabel([{ day: "2026-09-30" }, { day: "2026-10-29" }])).toBe("last 2 days · 30 sep – 29 oct");
});

describe("feed", () => {
  const act = (key: string, start: string, graph = "p"): Activity => ({
    key,
    kind: "meeting",
    kind_label: "meeting",
    owner: { user_id: null, name: null },
    start,
    end: start,
    graph: { id: graph, name: graph },
    title: key,
    outcome: null,
    people: [],
    people_count: 0,
    topics: [],
    decision_count: 0,
    source_count: 1,
  });
  const now = new Date("2026-10-29T18:00:00Z");
  const groups = groupFeed(
    [act("old", "2026-10-27T10:00:00Z"), act("a", "2026-10-29T09:10:00Z"), act("b", "2026-10-29T14:52:00Z"), act("y", "2026-10-28T12:00:00Z")],
    now,
    { tz: "UTC", oceanKind: (g) => (g === "p" ? "member" : "org") },
  );
  it("groups by local day, newest first, labelled like the design", () => {
    expect(groups.map((g) => g.label)).toEqual(["TODAY · THU 29 OCT", "YESTERDAY · WED 28 OCT", "TUE 27 OCT"]);
    expect(groups[0].items.map((i) => [i.activity.key, i.time])).toEqual([
      ["b", "14:52"],
      ["a", "09:10"],
    ]);
    expect(groups[0].items[0].oceanHue).toBe("pink");
  });
  it("count line", () => {
    expect(feedCountLabel(groups)).toBe("4 items · 3 days");
  });
  it("caps at 12", () => {
    const many = Array.from({ length: 30 }, (_, i) => act(`k${i}`, `2026-10-29T${String(i % 24).padStart(2, "0")}:00:00Z`));
    expect(groupFeed(many, now, { tz: "UTC" }).flatMap((g) => g.items)).toHaveLength(12);
  });
});

describe("connector helpers", () => {
  it("relative sync times", () => {
    const now = new Date("2026-10-29T12:00:00Z");
    expect(agoShort("2026-10-29T11:56:00Z", now)).toBe("4m");
    expect(agoShort("2026-10-29T09:00:00Z", now)).toBe("3h");
    expect(agoShort("2026-10-27T12:00:00Z", now)).toBe("2d");
    expect(agoShort(null, now)).toBe("—");
  });
  it("reconnect goes to the connector's own page when it has one", () => {
    expect(connectorReconnectHref("slack")).toBe("/connectors/slack");
    expect(connectorReconnectHref("notion")).toBe("/connectors");
  });
});

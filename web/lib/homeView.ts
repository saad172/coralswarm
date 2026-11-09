// Pure derivations behind the Home dashboard (app/(app)/home/page.tsx and
// components/home/*). The server does the local-day bucketing and the
// source-type grouping (docs/implementation/home-metrics-api.md); everything
// here is presentation arithmetic taken from the handoff prototype
// (`Home v2.dc.html`, its logic class), kept out of the components so it is
// testable without a DOM.
import type { Hue } from "@/components/ui";
import type { Activity } from "./journalActivitiesApi";
import {
  kpiDeltaPct,
  type HomeKpi,
  type HomeMetrics,
  type HomeOcean,
  type SourceGroupId,
} from "./pageSectionTypes/home";
import { oceanHue } from "./oceanHue";

const WEEKDAYS = ["sun", "mon", "tue", "wed", "thu", "fri", "sat"] as const;
const MONTHS = ["jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"] as const;

export const fmtCount = (n: number) => n.toLocaleString("en-US");

// ── KPI cards ────────────────────────────────────────────────────────────

export interface KpiDelta {
  text: string;
  /** `ok` for a rise; `muted` for flat or down (the design never paints a
   *  fall red: a quieter month is not an error). */
  tone: "ok" | "muted";
}

/** `+18%` / `−4%` / `0%` against the prior window, or null when there is no
 *  prior to compare with (the design hides the delta then). */
export function formatKpiDelta(kpi: HomeKpi): KpiDelta | null {
  const pct = kpiDeltaPct(kpi);
  if (pct === null) return null;
  if (pct > 0) return { text: `+${pct}%`, tone: "ok" };
  if (pct < 0) return { text: `−${Math.abs(pct)}%`, tone: "muted" };
  return { text: "0%", tone: "muted" };
}

export const SPARK_HEIGHT_PX = 24;

export interface SparkBar {
  /** Pixel height, 2..24. */
  h: number;
  /** The last bar (today) is the only brand-coloured one. */
  today: boolean;
}

/** 30 bars scaled to the series maximum; a zero day keeps a 2px stub so the
 *  row reads as a row. */
export function sparkBars(daily: readonly number[]): SparkBar[] {
  const max = Math.max(...daily, 1);
  return daily.map((v, i) => ({
    h: Math.max(2, Math.round((v / max) * SPARK_HEIGHT_PX)),
    today: i === daily.length - 1,
  }));
}

export interface KpiSpec {
  key: keyof HomeMetrics["kpis"];
  label: string;
}

/** The five cards, in design order. */
export const KPI_SPECS: readonly KpiSpec[] = [
  { key: "corals_ingested", label: "corals ingested" },
  { key: "reefs_formed", label: "reefs formed" },
  { key: "questions_asked", label: "questions asked" },
  { key: "agent_sessions", label: "agent sessions" },
  { key: "meetings_captured", label: "meetings captured" },
];

// ── Source types ─────────────────────────────────────────────────────────

export interface SourceGroupSpec {
  id: SourceGroupId;
  name: string;
  /** A CSS colour from the token set (globals.css `--src-*`). */
  color: string;
}

/** The five-group colour mapping from the handoff README. */
export const SOURCE_GROUPS: readonly SourceGroupSpec[] = [
  { id: "chat", name: "Chat", color: "var(--src-chat)" },
  { id: "meetings", name: "Meetings", color: "var(--src-meetings)" },
  { id: "agent_sessions", name: "Agent sessions", color: "var(--src-agent-sessions)" },
  { id: "docs_mail", name: "Docs & mail", color: "var(--src-docs-mail)" },
  { id: "code_tickets", name: "Code & tickets", color: "var(--src-code-tickets)" },
];

/** Server labels win (it owns the grouping); the table above is the fallback
 *  and the colour source. */
function groupSpecs(metrics: Pick<HomeMetrics, "source_type_groups">): SourceGroupSpec[] {
  const labels = new Map(metrics.source_type_groups.map((g) => [g.id, g.label]));
  return SOURCE_GROUPS.map((g) => ({ ...g, name: labels.get(g.id) || g.name }));
}

const sumValues = (o: Partial<Record<string, number>>) => Object.values(o).reduce<number>((a, v) => a + (v ?? 0), 0);

export interface SplitItem {
  id: SourceGroupId;
  name: string;
  color: string;
  value: number;
  /** `42%` */
  pct: string;
}

export interface SourceSplit {
  items: SplitItem[];
  total: number;
  /** `3,107 corals` */
  totalLabel: string;
}

/** The "by source type" bar under the chart: window corals per group. Groups
 *  with no corals are left out so the legend never reads `0%`. */
export function sourceSplit(metrics: Pick<HomeMetrics, "source_type_totals" | "source_type_groups">): SourceSplit {
  const total = sumValues(metrics.source_type_totals);
  const items = groupSpecs(metrics)
    .map((g) => ({ ...g, value: metrics.source_type_totals[g.id] ?? 0 }))
    .filter((g) => g.value > 0)
    .map((g) => ({ ...g, pct: `${Math.round((g.value / total) * 100)}%` }));
  return { items, total, totalLabel: `${fmtCount(total)} ${total === 1 ? "coral" : "corals"}` };
}

// ── Ingested per day ─────────────────────────────────────────────────────

export interface ChartSegment {
  graphId: string;
  name: string;
  value: number;
  color: string;
}

export interface ChartDay {
  day: string;
  total: number;
  /** Bar height as a percentage of the plot (`total / top`). */
  heightPct: number;
  /** Bottom-up, one per ocean with corals that day. */
  segments: ChartSegment[];
  /** The day number on Mondays only. */
  xLabel: string;
  /** Native tooltip: `thu 29 oct · 212 corals`. */
  tip: string;
}

export interface YTick {
  value: number;
  /** Distance from the bottom, 0..100. */
  pct: number;
}

export interface LegendItem {
  name: string;
  color: string;
}

export interface IngestChart {
  days: ChartDay[];
  yTicks: YTick[];
  top: number;
  legend: LegendItem[];
  /** False when the per-ocean split is unavailable and the bars fall back to
   *  one colour (README "Data needed": single-colour bars). */
  stacked: boolean;
}

/** `YYYY-MM-DD` to its weekday / day / month, read as a calendar date (no
 *  timezone: the server already bucketed it in the viewer's zone). */
export function calendarDay(day: string): { weekday: number; date: number; month: number } {
  const [y, m, d] = day.split("-").map(Number);
  const dt = new Date(Date.UTC(y, m - 1, d));
  return { weekday: dt.getUTCDay(), date: dt.getUTCDate(), month: dt.getUTCMonth() };
}

/** `thu 29 oct` */
export function dayLabel(day: string): string {
  const c = calendarDay(day);
  return `${WEEKDAYS[c.weekday]} ${c.date} ${MONTHS[c.month]}`;
}

const coralsLabel = (n: number) => `${fmtCount(n)} ${n === 1 ? "coral" : "corals"}`;

/** Y-axis maximum: the busiest day rounded up to a multiple of 50. */
export function chartTop(max: number): number {
  return Math.max(50, Math.ceil(max / 50) * 50);
}

/** Single-colour fallback bar colour: teal is the DS hue for ingested content. */
export const FLAT_BAR_COLOR = "var(--hue-teal)";

/** The stacked-by-ocean series. Oceans keep the server's order (volume
 *  descending) in every column so a colour always sits at the same height
 *  band. When no day carries a split (the server omitted it), the bars use the
 *  daily KPI series in one colour. */
export function buildIngestChart(metrics: Pick<HomeMetrics, "ingest_by_day" | "oceans" | "kpis">): IngestChart {
  const oceanById = new Map(metrics.oceans.map((o) => [o.graph_id, o]));
  const colorOf = (graphId: string) => {
    const o = oceanById.get(graphId);
    return `var(--hue-${oceanHue({ graph_id: graphId, kind: o?.kind, hue: o?.hue })})`;
  };
  const order = new Map(metrics.oceans.map((o, i) => [o.graph_id, i]));
  const hasSplit = metrics.ingest_by_day.some((d) => d.by_graph.length > 0);

  const raw = hasSplit
    ? metrics.ingest_by_day.map((d) => {
        const segments: ChartSegment[] = d.by_graph
          .map((g) => ({
            graphId: g.graph_id,
            name: oceanById.get(g.graph_id)?.name ?? "Ocean",
            value: sumValues(g.by_source_type),
            color: colorOf(g.graph_id),
          }))
          .filter((s) => s.value > 0)
          .sort((a, b) => (order.get(a.graphId) ?? 1e9) - (order.get(b.graphId) ?? 1e9));
        return { day: d.day, segments };
      })
    : metrics.ingest_by_day.map((d, i) => {
        const v = metrics.kpis.corals_ingested.daily[i] ?? 0;
        return {
          day: d.day,
          segments: v > 0 ? [{ graphId: "", name: "Corals", value: v, color: FLAT_BAR_COLOR }] : [],
        };
      });

  const totals = raw.map((d) => d.segments.reduce((a, s) => a + s.value, 0));
  const top = chartTop(Math.max(0, ...totals));
  const days: ChartDay[] = raw.map((d, i) => {
    const c = calendarDay(d.day);
    return {
      day: d.day,
      total: totals[i],
      heightPct: (totals[i] / top) * 100,
      segments: d.segments,
      xLabel: c.weekday === 1 ? String(c.date) : "",
      tip: `${dayLabel(d.day)} · ${coralsLabel(totals[i])}`,
    };
  });
  const yTicks = [0, 0.25, 0.5, 0.75, 1].map((f) => ({ value: Math.round(top * f), pct: f * 100 }));
  const legend = hasSplit
    ? metrics.oceans.map((o) => ({ name: o.name, color: colorOf(o.graph_id) }))
    : [{ name: "Corals", color: FLAT_BAR_COLOR }];
  return { days, yTicks, top, legend, stacked: hasSplit };
}

// ── Oceans by volume ─────────────────────────────────────────────────────

export interface OceanRankSeg {
  id: SourceGroupId;
  color: string;
  value: number;
  name: string;
}

export interface OceanRankRow {
  graphId: string;
  name: string;
  /** `mostly chat`, or "" when the split is unknown. */
  sub: string;
  valueLabel: string;
  /** Share of the largest ocean, 0..100. */
  widthPct: number;
  /** Source-type segments of the fill; one flat segment when unknown. */
  segments: OceanRankSeg[];
}

export function oceanRank(metrics: Pick<HomeMetrics, "oceans" | "source_type_groups">): OceanRankRow[] {
  const specs = groupSpecs(metrics);
  const sorted = [...metrics.oceans].sort((a, b) => b.total - a.total);
  const max = Math.max(1, ...sorted.map((o) => o.total));
  return sorted.map((o: HomeOcean) => {
    const parts = specs
      .map((g) => ({ id: g.id, color: g.color, name: g.name, value: o.by_source_type[g.id] ?? 0 }))
      .filter((p) => p.value > 0);
    const topPart = [...parts].sort((a, b) => b.value - a.value)[0];
    return {
      graphId: o.graph_id,
      name: o.name,
      sub: topPart ? `mostly ${topPart.name.toLowerCase()}` : "",
      valueLabel: fmtCount(o.total),
      widthPct: (o.total / max) * 100,
      segments: parts.length ? parts : [{ id: "chat" as const, color: FLAT_BAR_COLOR, name: "", value: 1 }],
    };
  });
}

// ── Activity heatmap ─────────────────────────────────────────────────────

export interface HeatCell {
  /** Day number, or "" for a blank leading / trailing cell. */
  label: string;
  blank: boolean;
  /** 0 (quiet) to 4 (busiest). */
  level: 0 | 1 | 2 | 3 | 4;
  today: boolean;
  tip: string;
}

export interface Heatmap {
  cells: HeatCell[];
  /** `busiest · tue 27 oct` */
  busiest: string;
}

export const HEAT_WEEKDAYS = ["mon", "tue", "wed", "thu", "fri", "sat", "sun"] as const;

export function heatLevel(v: number, max: number): 0 | 1 | 2 | 3 | 4 {
  if (v <= 0 || max <= 0) return 0;
  return Math.min(4, 1 + Math.floor((v / max) * 3.999)) as 1 | 2 | 3 | 4;
}

/** A Monday-first 7-column calendar over the window; leading and trailing
 *  cells are blank. Null when there are no days (the card is hidden). */
export function buildHeatmap(days: readonly { day: string; total: number }[]): Heatmap | null {
  if (days.length === 0) return null;
  const max = Math.max(...days.map((d) => d.total));
  const lead = (calendarDay(days[0].day).weekday + 6) % 7;
  const blank = (): HeatCell => ({ label: "", blank: true, level: 0, today: false, tip: "" });
  const cells: HeatCell[] = Array.from({ length: lead }, blank);
  days.forEach((d, i) => {
    cells.push({
      label: String(calendarDay(d.day).date),
      blank: false,
      level: heatLevel(d.total, max),
      today: i === days.length - 1,
      tip: `${dayLabel(d.day)} · ${coralsLabel(d.total)}`,
    });
  });
  while (cells.length % 7) cells.push(blank());
  const busiestDay = days[days.findIndex((d) => d.total === max)];
  return { cells, busiest: max > 0 ? `busiest · ${dayLabel(busiestDay.day)}` : "" };
}

// ── Header range line ────────────────────────────────────────────────────

/** `last 30 days · 30 sep – 29 oct` */
export function rangeLabel(days: readonly { day: string }[]): string {
  if (days.length === 0) return "";
  const f = calendarDay(days[0].day);
  const l = calendarDay(days[days.length - 1].day);
  return `last ${days.length} days · ${f.date} ${MONTHS[f.month]} – ${l.date} ${MONTHS[l.month]}`;
}

// ── Recently ingested feed ───────────────────────────────────────────────

export const FEED_MAX_ITEMS = 12;
export const FEED_DAYS = 3;

export interface FeedItem {
  activity: Activity;
  /** `14:52`, in the viewer's zone. */
  time: string;
  oceanHue: Hue;
}

export interface FeedGroup {
  /** `TODAY · THU 29 OCT` */
  label: string;
  items: FeedItem[];
}

/** Local `YYYY-MM-DD` of an instant in `tz` (viewer's zone when omitted). */
export function localDay(iso: string | Date, tz?: string): string {
  const parts = new Intl.DateTimeFormat("en-CA", { timeZone: tz, year: "numeric", month: "2-digit", day: "2-digit" }).formatToParts(
    typeof iso === "string" ? new Date(iso) : iso,
  );
  const get = (t: string) => parts.find((p) => p.type === t)?.value ?? "";
  return `${get("year")}-${get("month")}-${get("day")}`;
}

function localTime(iso: string, tz?: string): string {
  return new Intl.DateTimeFormat("en-GB", { timeZone: tz, hour: "2-digit", minute: "2-digit", hour12: false }).format(new Date(iso));
}

/** The previous calendar date of a `YYYY-MM-DD`. */
function dayBefore(day: string): string {
  const [y, m, d] = day.split("-").map(Number);
  return new Date(Date.UTC(y, m - 1, d - 1)).toISOString().slice(0, 10);
}

/** Activities (any order) to the newest `FEED_MAX_ITEMS`, grouped by local day,
 *  newest first. `ocean` maps a graph id to its kind so the dot hue matches the
 *  charts. */
export function groupFeed(
  activities: readonly Activity[],
  now: Date,
  opts: { tz?: string; oceanKind?: (graphId: string) => string | undefined } = {},
): FeedGroup[] {
  const sorted = [...activities].sort((a, b) => Date.parse(b.start) - Date.parse(a.start)).slice(0, FEED_MAX_ITEMS);
  const today = localDay(now, opts.tz);
  const yesterday = dayBefore(today);
  const groups: FeedGroup[] = [];
  for (const a of sorted) {
    const day = localDay(a.start, opts.tz);
    const base = dayLabel(day).toUpperCase();
    const label = day === today ? `TODAY · ${base}` : day === yesterday ? `YESTERDAY · ${base}` : base;
    let g = groups[groups.length - 1];
    if (!g || g.label !== label) {
      g = { label, items: [] };
      groups.push(g);
    }
    g.items.push({
      activity: a,
      time: localTime(a.start, opts.tz),
      oceanHue: oceanHue({ graph_id: a.graph.id, kind: opts.oceanKind?.(a.graph.id) }),
    });
  }
  return groups;
}

/** `12 items · 3 days` */
export function feedCountLabel(groups: readonly FeedGroup[]): string {
  const n = groups.reduce((a, g) => a + g.items.length, 0);
  return `${n} ${n === 1 ? "item" : "items"} · ${groups.length} ${groups.length === 1 ? "day" : "days"}`;
}

/** The feed window: [local midnight FEED_DAYS-1 days ago, local midnight
 *  tomorrow), as RFC3339 instants for `GET /v1/journal/activities`. */
export function feedWindow(now: Date): { from: string; to: string } {
  const from = new Date(now.getFullYear(), now.getMonth(), now.getDate() - (FEED_DAYS - 1));
  const to = new Date(now.getFullYear(), now.getMonth(), now.getDate() + 1);
  return { from: from.toISOString(), to: to.toISOString() };
}

// ── Connectors ───────────────────────────────────────────────────────────

/** `4m`, `1h`, `2d` since an instant; an em dash when there is none. */
export function agoShort(iso: string | null, now: Date): string {
  if (!iso) return "—";
  const mins = Math.max(0, Math.floor((now.getTime() - Date.parse(iso)) / 60_000));
  if (mins < 1) return "now";
  if (mins < 60) return `${mins}m`;
  const hours = Math.floor(mins / 60);
  if (hours < 24) return `${hours}h`;
  return `${Math.floor(hours / 24)}d`;
}

/** Where a failing connector is reconnected: its own page when it has one,
 *  else the connectors index, whose tiles carry the same Reconnect action. */
export function connectorReconnectHref(provider: string): string {
  switch (provider) {
    case "slack":
      return "/connectors/slack";
    case "github":
      return "/connectors/github";
    case "linear":
      return "/connectors/linear";
    case "jira":
      return "/connectors/jira";
    case "microsoft":
    case "microsoft_teams":
      return "/connectors/microsoft-teams";
    default:
      return "/connectors";
  }
}

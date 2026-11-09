// Pure derivations behind the Today page (app/(app)/today/page.tsx and
// components/today/*). The server groups and counts
// (docs/implementation/journal-activities-api.md); everything here is the
// presentation arithmetic from the handoff prototype (`Today v2.dc.html`, its
// logic class): sorting, quiet gaps, week/month grouping, chart buckets, the
// rails, the strip. It is kept out of the components so it is testable without
// a DOM, and every function takes the IANA `tz` explicitly so tests do not
// depend on the machine zone.
import type { Graph } from "./api";
import { calendarDay, dayLabel, localDay } from "./homeView";
import {
  askAboutDayHref,
  askAboutMonthHref,
  askAboutWeekHref,
  isSameLocalDay,
  localDayBounds,
  localMonthBounds,
  localWeekBounds,
  type Zoom,
} from "./journal";
import type { Activity, ActivityKind, DayActivity } from "./journalActivitiesApi";

export type Who = "mine" | "team";
export type Density = "cards" | "compact";

/** Chart / split order, as in the prototype. */
export const KIND_ORDER: readonly ActivityKind[] = ["meeting", "slack", "agent", "doc", "note"];

export const KIND_NAMES: Record<ActivityKind, string> = {
  meeting: "Meetings",
  slack: "Slack",
  agent: "Agent sessions",
  doc: "Doc sessions",
  note: "Notes",
};

/** The chart colour of a kind: `--kind-*` in globals.css. */
export const kindColor = (k: ActivityKind): string => `var(--kind-${k})`;

/** Quiet gap threshold (Day view, Cards mode). */
export const QUIET_GAP_MIN = 45;
/** A zero-length activity still takes this much room on the charts. */
export const MIN_CHART_MIN = 5;
/** Day chart hours, inclusive: 08:00 to 18:00. */
export const CHART_HOURS = { from: 8, to: 18 } as const;
export const MONTH_CHART_DAYS = 14;
export const RAIL_MAX = 8;

const WEEKDAYS_LONG = ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"] as const;
const WEEKDAYS_SHORT = ["sun", "mon", "tue", "wed", "thu", "fri", "sat"] as const;
const MONTHS_LONG = [
  "January", "February", "March", "April", "May", "June",
  "July", "August", "September", "October", "November", "December",
] as const;
const MONTHS_SHORT = ["jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"] as const;

// ── Time ─────────────────────────────────────────────────────────────────

/** `14:52` in `tz` (the viewer's zone when omitted). */
export function hhmm(iso: string | number, tz?: string): string {
  return new Intl.DateTimeFormat("en-GB", { timeZone: tz, hour: "2-digit", minute: "2-digit", hour12: false })
    .format(new Date(iso))
    .replace(/^24:/, "00:");
}

export function minutesOfDay(iso: string | number, tz?: string): number {
  const [h, m] = hhmm(iso, tz).split(":").map(Number);
  return h * 60 + m;
}

/** Whole minutes between start and end; never negative. */
export function durationMin(a: Pick<Activity, "start" | "end">): number {
  return Math.max(0, Math.round((Date.parse(a.end) - Date.parse(a.start)) / 60_000));
}

/** `45m`, `1h`, `1h 10m`. */
export function formatDuration(min: number): string {
  const m = Math.max(0, Math.round(min));
  if (m >= 60) return `${Math.floor(m / 60)}h${m % 60 ? ` ${m % 60}m` : ""}`;
  return `${m}m`;
}

/** The duration a card shows. A moment (a single message, a note) has none to
 *  show, so a note says `note` as in the prototype and anything else `<1m`. */
export function durationLabel(a: Pick<Activity, "start" | "end" | "kind">): string {
  const m = durationMin(a);
  if (m > 0) return formatDuration(m);
  return a.kind === "note" ? "note" : "<1m";
}

/** `14:52–15:34`, or just the start for a moment. */
export function rangeLabel(a: Pick<Activity, "start" | "end">, tz?: string): string {
  const s = hhmm(a.start, tz);
  const e = hhmm(a.end, tz);
  return s === e ? s : `${s}–${e}`;
}

// ── Ordering ─────────────────────────────────────────────────────────────

/** Start, then ocean, then key: the server's order, restated so the page never
 *  depends on it. */
export function sortActivities(list: readonly Activity[]): Activity[] {
  return [...list].sort(
    (a, b) =>
      Date.parse(a.start) - Date.parse(b.start) || a.graph.id.localeCompare(b.graph.id) || a.key.localeCompare(b.key),
  );
}

/** First occurrence of each (ocean, key) wins. */
export function dedupeActivities(list: readonly Activity[]): Activity[] {
  const seen = new Set<string>();
  return list.filter((a) => {
    const id = `${a.graph.id}:${a.key}`;
    if (seen.has(id)) return false;
    seen.add(id);
    return true;
  });
}

// ── Filter ───────────────────────────────────────────────────────────────

export interface TodayFilter {
  type: "person" | "topic";
  /** Person name, or topic node id. */
  key: string;
  /** What the pill shows. */
  label: string;
}

export function passesFilter(a: Activity, f: TodayFilter | null): boolean {
  if (!f) return true;
  return f.type === "person" ? a.people.includes(f.key) : a.topics.some((t) => t.node_id === f.key);
}

/** Choosing the active chip again clears the filter. */
export function toggleFilter(cur: TodayFilter | null, next: TodayFilter): TodayFilter | null {
  return cur && cur.type === next.type && cur.key === next.key ? null : next;
}

export function filterPillLabel(f: TodayFilter): string {
  return `${f.type === "person" ? "with " : "topic: "}${f.label}`;
}

// ── Timeline ─────────────────────────────────────────────────────────────

export interface HeadRow {
  type: "head";
  id: string;
  /** `thu 29 oct` or `week of 26 oct`; shown uppercase. */
  label: string;
}
export interface GapRow {
  type: "gap";
  id: string;
  /** `10:40`: when the quiet began. */
  start: string;
  /** `1h 10m` */
  label: string;
}
export interface ItemRow {
  type: "item";
  id: string;
  activity: Activity;
  /** Position in the visible, filtered list (the panel steps through it). */
  index: number;
}
export type TimelineRow = HeadRow | GapRow | ItemRow;

export interface Timeline {
  rows: TimelineRow[];
  /** Filtered, in display order: what ↑/↓ steps through. */
  visible: Activity[];
  /** Unfiltered, deduplicated, in range: what the stats and rails count. */
  all: Activity[];
}

function weekLabel(day: string): string {
  const [y, m, d] = day.split("-").map(Number);
  const start = localWeekBounds(new Date(y, m - 1, d)).start;
  return `week of ${start.getDate()} ${MONTHS_SHORT[start.getMonth()]}`;
}

/** Groups: Day is one; Week is one per local day and Month one per Monday-start
 *  week, both newest first with each group oldest first (prototype order). */
export function buildTimeline(opts: {
  activities: readonly Activity[];
  zoom: Zoom;
  tz?: string;
  filter: TodayFilter | null;
  /** Compact mode drops quiet gaps; Month is always compact. */
  compact: boolean;
}): Timeline {
  const { zoom, tz, filter } = opts;
  const all = sortActivities(dedupeActivities(opts.activities));
  const groups = new Map<string, Activity[]>();
  for (const a of all) {
    const day = localDay(a.start, tz);
    const gk = zoom === "day" ? day : zoom === "week" ? day : weekLabel(day);
    const g = groups.get(gk);
    if (g) g.push(a);
    else groups.set(gk, [a]);
  }
  const keys = [...groups.keys()];
  const ordered = zoom === "day" ? keys : keys.reverse();

  const rows: TimelineRow[] = [];
  const visible: Activity[] = [];
  const compact = opts.compact || zoom === "month";
  for (const gk of ordered) {
    const items = (groups.get(gk) ?? []).filter((a) => passesFilter(a, filter));
    if (!items.length) continue;
    if (zoom !== "day") rows.push({ type: "head", id: `head:${gk}`, label: zoom === "week" ? dayLabel(gk) : gk });
    let lastEnd: number | null = null;
    for (const a of items) {
      const s = Date.parse(a.start);
      if (zoom === "day" && !compact && lastEnd !== null && (s - lastEnd) / 60_000 >= QUIET_GAP_MIN) {
        rows.push({
          type: "gap",
          id: `gap:${a.graph.id}:${a.key}`,
          start: hhmm(lastEnd, tz),
          label: formatDuration(Math.round((s - lastEnd) / 60_000)),
        });
      }
      lastEnd = Math.max(lastEnd ?? 0, Date.parse(a.end));
      rows.push({ type: "item", id: `${a.graph.id}:${a.key}`, activity: a, index: visible.length });
      visible.push(a);
    }
  }
  return { rows, visible, all };
}

/** ↑/↓ stepping through `n` rows, wrapping at both ends. */
export function stepIndex(i: number, n: number, dir: 1 | -1): number {
  return n <= 0 ? 0 : (i + dir + n) % n;
}

export function eventCountLabel(n: number): string {
  return `${n} ${n === 1 ? "event" : "events"}`;
}

// ── Stats ────────────────────────────────────────────────────────────────

export interface TodayStats {
  activities: number;
  meetings: number;
  /** Agent sessions plus doc sessions. */
  sessions: number;
  decisions: number;
}

export function periodStats(all: readonly Activity[]): TodayStats {
  return {
    activities: all.length,
    meetings: all.filter((a) => a.kind === "meeting").length,
    sessions: all.filter((a) => a.kind === "agent" || a.kind === "doc").length,
    decisions: all.reduce((n, a) => n + a.decision_count, 0),
  };
}

/** `09:10` to `16:40` across the activities, or null when there are none. */
export function periodSpan(all: readonly Activity[], tz?: string): { from: string; to: string } | null {
  if (!all.length) return null;
  const from = Math.min(...all.map((a) => Date.parse(a.start)));
  const to = Math.max(...all.map((a) => Date.parse(a.end)));
  return { from: hhmm(from, tz), to: hhmm(to, tz) };
}

// ── Charts ───────────────────────────────────────────────────────────────

export interface BarSeg {
  /** null for the empty-bucket baseline stub. */
  kind: ActivityKind | null;
  value: number;
}
export interface ChartBar {
  id: string;
  /** X label under the bar (may be empty). */
  label: string;
  /** Native tooltip. */
  tip: string;
  minutes: Partial<Record<ActivityKind, number>>;
  total: number;
  /** 0..100. An empty bucket is a 2% stub, any other at least 6%. */
  heightPct: number;
  segs: BarSeg[];
}

const sumKinds = (m: Partial<Record<ActivityKind, number>>) => KIND_ORDER.reduce((n, k) => n + (m[k] ?? 0), 0);

function finishBars(raw: { id: string; label: string; tip: string; minutes: Partial<Record<ActivityKind, number>> }[]): ChartBar[] {
  const max = Math.max(1, ...raw.map((b) => sumKinds(b.minutes)));
  return raw.map((b) => {
    const total = sumKinds(b.minutes);
    return {
      ...b,
      tip: `${b.tip} · ${formatDuration(total)}`,
      total,
      heightPct: Math.max(total ? 6 : 2, (total / max) * 100),
      segs: total
        ? KIND_ORDER.filter((k) => b.minutes[k]).map((k) => ({ kind: k, value: b.minutes[k] ?? 0 }))
        : [{ kind: null, value: 1 }],
    };
  });
}

/** TIME BY HOUR: the minutes of each kind overlapping each hour 8 to 18. */
export function hourBars(all: readonly Activity[], tz?: string): ChartBar[] {
  const raw = [];
  for (let h = CHART_HOURS.from; h <= CHART_HOURS.to; h++) {
    const h0 = h * 60;
    const h1 = h0 + 60;
    const minutes: Partial<Record<ActivityKind, number>> = {};
    for (const a of all) {
      const s = minutesOfDay(a.start, tz);
      const e = s + Math.max(durationMin(a), MIN_CHART_MIN);
      const overlap = Math.max(0, Math.min(e, h1) - Math.max(s, h0));
      if (overlap) minutes[a.kind] = (minutes[a.kind] ?? 0) + overlap;
    }
    raw.push({ id: `h${h}`, label: (h - CHART_HOURS.from) % 2 === 0 ? String(h) : "", tip: `${h}:00`, minutes });
  }
  return finishBars(raw);
}

/** `YYYY-MM-DD` plus `n` calendar days. */
export function addDays(day: string, n: number): string {
  const [y, m, d] = day.split("-").map(Number);
  return new Date(Date.UTC(y, m - 1, d + n)).toISOString().slice(0, 10);
}

/** The `n` days ending on `end` (inclusive), ascending. */
export function daysEndingOn(end: string, n: number): string[] {
  return Array.from({ length: n }, (_, i) => addDays(end, i - n + 1));
}

/** TIME BY DAY: Week labels each day by two letters, Month labels the date on
 *  Mondays only. */
export function dayBars(all: readonly Activity[], days: readonly string[], zoom: "week" | "month", tz?: string): ChartBar[] {
  const byDay = new Map<string, Partial<Record<ActivityKind, number>>>();
  for (const a of all) {
    const d = localDay(a.start, tz);
    const m = byDay.get(d) ?? {};
    m[a.kind] = (m[a.kind] ?? 0) + Math.max(durationMin(a), MIN_CHART_MIN);
    byDay.set(d, m);
  }
  return finishBars(
    days.map((d) => {
      const c = calendarDay(d);
      return {
        id: d,
        label: zoom === "week" ? WEEKDAYS_SHORT[c.weekday].slice(0, 2) : c.weekday === 1 ? String(c.date) : "",
        tip: dayLabel(d),
        minutes: byDay.get(d) ?? {},
      };
    }),
  );
}

export interface SplitItem {
  kind: ActivityKind;
  name: string;
  minutes: number;
  /** `2h 10m` */
  label: string;
}

/** The bars' minutes summed per kind, kinds with none left out. */
export function timeSplit(bars: readonly ChartBar[]): { items: SplitItem[]; totalMin: number } {
  const items = KIND_ORDER.map((k) => {
    const minutes = bars.reduce((n, b) => n + (b.minutes[k] ?? 0), 0);
    return { kind: k, name: KIND_NAMES[k], minutes, label: formatDuration(minutes) };
  }).filter((i) => i.minutes > 0);
  return { items, totalMin: items.reduce((n, i) => n + i.minutes, 0) };
}

// ── People and topics ────────────────────────────────────────────────────

export interface RailChip {
  key: string;
  label: string;
  count: number;
}

function rank(m: Map<string, RailChip>): RailChip[] {
  return [...m.values()].sort((a, b) => b.count - a.count || a.label.localeCompare(b.label)).slice(0, RAIL_MAX);
}

/** Names of the viewer as the activities spell them (the owner name of their
 *  own cards), so Mine can leave them out of People. */
export function selfNames(all: readonly Activity[], internalUserId: string | null | undefined): Set<string> {
  const names = new Set<string>();
  if (!internalUserId) return names;
  for (const a of all) if (a.owner.user_id === internalUserId && a.owner.name) names.add(a.owner.name);
  return names;
}

export function peopleRail(all: readonly Activity[], opts: { who: Who; self: ReadonlySet<string> }): RailChip[] {
  const m = new Map<string, RailChip>();
  for (const a of all) {
    for (const p of new Set(a.people)) {
      if (opts.who === "mine" && opts.self.has(p)) continue;
      const c = m.get(p) ?? { key: p, label: p, count: 0 };
      c.count += 1;
      m.set(p, c);
    }
  }
  return rank(m);
}

export function topicRail(all: readonly Activity[]): RailChip[] {
  const m = new Map<string, RailChip>();
  for (const a of all) {
    for (const t of new Map(a.topics.map((x) => [x.node_id, x])).values()) {
      const c = m.get(t.node_id) ?? { key: t.node_id, label: t.title, count: 0 };
      c.count += 1;
      m.set(t.node_id, c);
    }
  }
  return rank(m);
}

export const firstName = (name: string | null | undefined): string => (name ?? "").trim().split(/\s+/)[0] || "Someone";

// ── Period, window, titles ───────────────────────────────────────────────

export function periodBounds(zoom: Zoom, anchor: Date): { start: Date; end: Date } {
  return zoom === "day" ? localDayBounds(anchor) : zoom === "week" ? localWeekBounds(anchor) : localMonthBounds(anchor);
}

/** The `[from, to)` an activities read asks for. At most 31 days: well inside
 *  the server's 62. */
export function periodWindow(zoom: Zoom, anchor: Date): { from: string; to: string } {
  const { start, end } = periodBounds(zoom, anchor);
  return { from: start.toISOString(), to: end.toISOString() };
}

/** The window of the strip read: today only. `day_activity` always covers the
 *  14 days ending with the day before `to`, so asking for today gives the
 *  strip its 14 days whatever period is on screen. */
export function stripWindow(now: Date): { from: string; to: string } {
  return periodWindow("day", now);
}

const dayKey = (d: Date) =>
  `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, "0")}-${String(d.getDate()).padStart(2, "0")}`;

/** `Thursday 29 October`, `Week of 23 – 29 October`, `October 2026`. */
export function periodTitle(zoom: Zoom, anchor: Date): string {
  if (zoom === "day") return `${WEEKDAYS_LONG[anchor.getDay()]} ${anchor.getDate()} ${MONTHS_LONG[anchor.getMonth()]}`;
  if (zoom === "month") return `${MONTHS_LONG[anchor.getMonth()]} ${anchor.getFullYear()}`;
  const { start, end } = localWeekBounds(anchor);
  const last = new Date(end.getFullYear(), end.getMonth(), end.getDate() - 1);
  return start.getMonth() === last.getMonth()
    ? `Week of ${start.getDate()} – ${last.getDate()} ${MONTHS_LONG[last.getMonth()]}`
    : `Week of ${start.getDate()} ${MONTHS_LONG[start.getMonth()]} – ${last.getDate()} ${MONTHS_LONG[last.getMonth()]}`;
}

/** `team · 6 activities · 09:10–16:20`; empty when nothing was captured. */
export function periodMeta(opts: {
  zoom: Zoom;
  who: Who;
  count: number;
  span: { from: string; to: string } | null;
}): string {
  const prefix = opts.who === "team" ? "team · " : "";
  if (!opts.count) return "";
  const n = `${opts.count} ${opts.count === 1 ? "activity" : "activities"}`;
  return prefix + n + (opts.zoom === "day" && opts.span ? ` · ${opts.span.from}–${opts.span.to}` : "");
}

/** The workstream panel's eyebrow. */
export const streamLabel = (who: Who): string => (who === "team" ? "team workstream" : "your workstream");

/** `09:10 – 16:40` (Day only; the panel shows no span for Week or Month). */
export function streamSpan(zoom: Zoom, span: { from: string; to: string } | null): string {
  return zoom === "day" && span ? `${span.from} – ${span.to}` : "";
}

export function askLabel(zoom: Zoom, anchor: Date, now: Date): string {
  if (zoom === "day") return isSameLocalDay(anchor, now) ? "Ask about today" : "Ask about this day";
  return `Ask about this ${zoom}`;
}

export function askHref(zoom: Zoom, anchor: Date): string {
  if (zoom === "day") return askAboutDayHref(anchor);
  return zoom === "week" ? askAboutWeekHref(localWeekBounds(anchor).start) : askAboutMonthHref(localMonthBounds(anchor).start);
}

/** Month's TIME BY DAY: the last 14 days of the month, or up to today when the
 *  month is still open. */
export function monthChartDays(anchor: Date, now: Date): string[] {
  const { start, end } = localMonthBounds(anchor);
  const lastOfMonth = new Date(end.getFullYear(), end.getMonth(), end.getDate() - 1);
  const last = now >= start && now < end ? now : lastOfMonth;
  return daysEndingOn(dayKey(last), MONTH_CHART_DAYS);
}

export function weekChartDays(anchor: Date): string[] {
  const { start } = localWeekBounds(anchor);
  return Array.from({ length: 7 }, (_, i) => dayKey(new Date(start.getFullYear(), start.getMonth(), start.getDate() + i)));
}

// ── Date strip ───────────────────────────────────────────────────────────

export interface StripCell {
  day: string;
  /** `thu` */
  weekday: string;
  date: number;
  count: number;
  selected: boolean;
  /** The selection shadow is Day only; Week and Month highlight the range flat. */
  shadow: boolean;
  /** Dot diameter in px: 4 for none, 4 to 6 by count. */
  dotPx: number;
  dotTone: "brand" | "muted" | "none";
}

/** The 14 `day_activity` days as cells. `selection` is the visible period's
 *  first and last local day. */
export function buildStrip(
  dayActivity: readonly DayActivity[],
  selection: { zoom: Zoom; from: string; to: string },
): StripCell[] {
  return dayActivity.map(({ day, count }) => {
    const c = calendarDay(day);
    const selected = day >= selection.from && day <= selection.to;
    return {
      day,
      weekday: WEEKDAYS_SHORT[c.weekday],
      date: c.date,
      count,
      selected,
      shadow: selected && selection.zoom === "day",
      dotPx: count ? 3 + Math.min(count, 3) : 4,
      dotTone: count >= 3 ? "brand" : count > 0 ? "muted" : "none",
    };
  });
}

/** Zero-activity cells for when the counts are unavailable (activities off). */
export function emptyStrip(now: Date): DayActivity[] {
  return daysEndingOn(dayKey(now), 14).map((day) => ({ day, count: 0 }));
}

/** First and last local day (`YYYY-MM-DD`) of the visible period. */
export function selectionDays(zoom: Zoom, anchor: Date): { from: string; to: string } {
  const { start, end } = periodBounds(zoom, anchor);
  return { from: dayKey(start), to: dayKey(new Date(end.getFullYear(), end.getMonth(), end.getDate() - 1)) };
}

// ── Team ─────────────────────────────────────────────────────────────────

/** The workspace Team looks at: the ocean switcher's, when it is a shared one;
 *  otherwise the first team or org ocean the viewer can read; otherwise none
 *  (the toggle is hidden). */
export function teamGraph(graphs: readonly Graph[], current: Graph | null): Graph | null {
  const shared = (g: Graph) => g.kind === "team" || g.kind === "org";
  if (current && shared(current)) return current;
  return graphs.find(shared) ?? null;
}

// ── Cards / Compact preference ───────────────────────────────────────────

export const densityStorageKey = (userId: string | null | undefined): string => `cs.today.density:${userId ?? "anon"}`;

type ReadStorage = Pick<Storage, "getItem">;
type WriteStorage = Pick<Storage, "setItem">;

/** Cards unless the user chose Compact. A broken or missing store is Cards. */
export function readDensity(storage: ReadStorage | null | undefined, key: string): Density {
  try {
    return storage?.getItem(key) === "compact" ? "compact" : "cards";
  } catch {
    return "cards";
  }
}

export function writeDensity(storage: WriteStorage | null | undefined, key: string, d: Density): void {
  try {
    storage?.setItem(key, d);
  } catch {
    /* private mode / quota: the choice just doesn't persist */
  }
}

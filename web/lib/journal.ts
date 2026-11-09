// Today-journal PR D — pure logic for the hourly `/today` page.
//
// Kept dependency-free (no React, no `useApp`) and separate from
// `app/(app)/today/page.tsx` so the date math and gap-collapsing — "the two
// things most likely to be subtly wrong" — are unit-testable without
// spinning up a component. Mirrors the shape of `lib/queryKeys.ts`/
// `lib/askScope.ts`: a plain module the screen composes.

import type { JournalAggregate, JournalEntry, JournalHour, JournalPeriod, JournalTierEntry } from "./api";

// ── Local-day bounds ─────────────────────────────────────────────────────────

/** Normalizes to local midnight of the day `date` falls on — only the Y/M/D
 *  components matter; the time-of-day is discarded. */
export function localDayAnchor(date: Date): Date {
  return new Date(date.getFullYear(), date.getMonth(), date.getDate());
}

export function isSameLocalDay(a: Date, b: Date): boolean {
  return a.getFullYear() === b.getFullYear() && a.getMonth() === b.getMonth() && a.getDate() === b.getDate();
}

/** `[start, end)` — the local calendar day containing `date`, as absolute
 *  instants: local midnight through the following local midnight. Computed
 *  by constructing each boundary directly off local Y/M/D (never by adding
 *  86_400_000ms to `start`), so a DST transition inside the day is priced
 *  into `end - start` automatically instead of silently landing 1h off. */
export function localDayBounds(date: Date): { start: Date; end: Date } {
  const anchor = localDayAnchor(date);
  const y = anchor.getFullYear();
  const m = anchor.getMonth();
  const d = anchor.getDate();
  return { start: new Date(y, m, d), end: new Date(y, m, d + 1) };
}

/** The local day `day` converted to the `[from, to)` UTC instant range
 *  `GET /v1/journal` takes. Same instants as `localDayBounds` — this alias
 *  exists so call sites read "for the API" rather than "for the row grid". */
export function localDayToUtcRange(day: Date): { from: Date; to: Date } {
  const { start, end } = localDayBounds(day);
  return { from: start, to: end };
}

// ── `?d=` URL param ───────────────────────────────────────────────────────────

const DAY_PARAM_RE = /^(\d{4})-(\d{2})-(\d{2})$/;

/** `?d=` → the local calendar day it names. Falls back to `now`'s local day
 *  when absent or unparsable. Deliberately hand-parsed rather than
 *  `new Date("2026-08-01")`, which the spec reads as UTC midnight — in any
 *  negative-UTC-offset timezone that instant falls on the PREVIOUS local
 *  day, so the page would silently open yesterday. */
export function parseDayParam(raw: string | null | undefined, now: Date = new Date()): Date {
  const m = raw ? DAY_PARAM_RE.exec(raw.trim()) : null;
  if (m) {
    const [, y, mo, d] = m;
    const parsed = new Date(Number(y), Number(mo) - 1, Number(d));
    if (!Number.isNaN(parsed.getTime())) return parsed;
  }
  return localDayAnchor(now);
}

/** The inverse of `parseDayParam` — a local day → its `?d=` value. */
export function formatDayParam(day: Date): string {
  const y = day.getFullYear();
  const mo = String(day.getMonth() + 1).padStart(2, "0");
  const d = String(day.getDate()).padStart(2, "0");
  return `${y}-${mo}-${d}`;
}

// ── The day title ─────────────────────────────────────────────────────────────

/** `PageTitle`'s `{title, italicWord}` shape for the day header — "Friday,
 *  August 1" (plus "· today", the one italic brand-colored word) only when
 *  `day` is `now`'s local day. */
export function dayHeaderTitle(day: Date, now: Date = new Date()): { title: string; italicWord?: string } {
  const label = day.toLocaleDateString(undefined, { weekday: "long", month: "long", day: "numeric" });
  if (!isSameLocalDay(day, now)) return { title: label };
  return { title: `${label} · today`, italicWord: "today" };
}

// ── Local hours of the day ────────────────────────────────────────────────────

export interface LocalHourSlot {
  /** Start of this slot, as an absolute instant. */
  start: Date;
  /** End of this slot (exclusive), as an absolute instant. */
  end: Date;
}

/** The real local hours making up `day` — NOT always 24. Walking off real
 *  elapsed time (fixed 1h steps from local midnight, clamped to the next
 *  local midnight) rather than off wall-clock hour NUMBERS means a spring-
 *  forward day (23 real hours: 2am never happens) naturally produces 23
 *  slots — the 1am slot simply runs "01:00" through "03:00" — and a
 *  fall-back day (25 real hours: 1am happens twice) produces 25, with two
 *  adjacent slots both labeled "01:00" (once CDT, once CST — genuinely
 *  distinct instants an hour apart; `HH:00` can't tell them apart in the UI
 *  any better than a wall clock can, which is the same ambiguity every
 *  calendar app has on that day). */
export function hoursOfLocalDay(day: Date): LocalHourSlot[] {
  const { start: dayStart, end: dayEnd } = localDayBounds(day);
  const slots: LocalHourSlot[] = [];
  let cursor = dayStart;
  const HOUR_MS = 3_600_000;
  // Guard against a pathological infinite loop — no real DST shift is more
  // than a couple of hours, so a calendar day is never more than ~26h.
  for (let i = 0; i < 30 && cursor.getTime() < dayEnd.getTime(); i++) {
    const next = new Date(Math.min(cursor.getTime() + HOUR_MS, dayEnd.getTime()));
    slots.push({ start: cursor, end: next });
    cursor = next;
  }
  return slots;
}

/** `HH:00` in the slot's own local hour — the row/gap-boundary label. */
export function formatHourLabel(instant: Date): string {
  return `${String(instant.getHours()).padStart(2, "0")}:00`;
}

// ── Gap collapsing ────────────────────────────────────────────────────────────

export interface DayHourRow {
  kind: "hour";
  start: Date;
  end: Date;
  /** The local hour slot containing `now` — only meaningful while viewing
   *  today. Rendered with the "in progress" note regardless of whether it
   *  has any data yet, and never folded into a gap even when it doesn't. */
  current: boolean;
}
export interface DayGapRow {
  kind: "gap";
  start: Date;
  end: Date;
}
export type DayRow = DayHourRow | DayGapRow;

function slotIndexForInstant(slots: LocalHourSlot[], instant: Date): number {
  const t = instant.getTime();
  return slots.findIndex((s) => t >= s.start.getTime() && t < s.end.getTime());
}

/** The row list for one day: every local hour slot in `slots` becomes either
 *  a real `"hour"` row (it has at least one entry in `presentInstants`, or
 *  it's the current in-progress hour) or gets folded into the nearest
 *  `"gap"` row with its immediate empty neighbors. Every slot is accounted
 *  for by exactly one row — leading/trailing gaps included — so summing a
 *  row list's spans always reconstructs the full day.
 *
 *  `now` doubles as "am I viewing today": when it's non-null and lands
 *  inside `slots` (i.e. the viewed day IS the day containing `now`), every
 *  slot strictly after the in-progress hour is dropped entirely rather than
 *  folded into a trailing "no activity" gap — those hours are in the
 *  future, not quiet, and a gap row would assert something false. Pass
 *  `null` for a past (or otherwise non-today) day to get the full,
 *  untruncated 24(ish)-hour accounting. */
export function buildDayRows(slots: LocalHourSlot[], presentInstants: Date[], now: Date | null = null): DayRow[] {
  const present = new Set<number>();
  for (const t of presentInstants) {
    const idx = slotIndexForInstant(slots, t);
    if (idx !== -1) present.add(idx);
  }
  const currentIdx = now === null ? -1 : slotIndexForInstant(slots, now);
  const effectiveSlots = currentIdx === -1 ? slots : slots.slice(0, currentIdx + 1);

  const rows: DayRow[] = [];
  let gapStart: number | null = null;
  const flushGap = (beforeIdx: number) => {
    if (gapStart === null) return;
    rows.push({ kind: "gap", start: slots[gapStart].start, end: slots[beforeIdx - 1].end });
    gapStart = null;
  };

  effectiveSlots.forEach((slot, i) => {
    const hasRow = present.has(i) || i === currentIdx;
    if (hasRow) {
      flushGap(i);
      rows.push({ kind: "hour", start: slot.start, end: slot.end, current: i === currentIdx });
    } else if (gapStart === null) {
      gapStart = i;
    }
  });
  flushGap(effectiveSlots.length);

  return rows;
}

/** "no activity · 07:00–14:00" — the collapsed gap row's label. A gap
 *  spanning exactly one hour reads as just that hour ("no activity ·
 *  09:00") rather than a same-looking range ("09:00–10:00") — the verify
 *  ticket's observation that a one-hour range reads worse than the single
 *  boundary it collapses to. */
export function formatGapLabel(gap: DayGapRow): string {
  if (gap.end.getTime() - gap.start.getTime() <= 3_600_000) {
    return `no activity · ${formatHourLabel(gap.start)}`;
  }
  return `no activity · ${formatHourLabel(gap.start)}–${formatHourLabel(gap.end)}`;
}

/** The row (real hour or collapsed gap) covering `instant` — how the rail's
 *  24-slot intensity strip finds what to scroll to: a click on a slot inside
 *  a collapsed gap must land on that gap row, not fail to find anything. */
export function rowContainingInstant(rows: DayRow[], instant: Date): DayRow | undefined {
  const t = instant.getTime();
  return rows.find((r) => t >= r.start.getTime() && t < r.end.getTime());
}

/** Stable DOM id for a row — shared by the row's own element and the rail's
 *  scroll-to-on-click. */
export function rowElementId(row: { start: Date }): string {
  return `today-row-${row.start.getTime()}`;
}

// ── Hour drill-down target (ticket 16's endpoint) ─────────────────────────────

/** The `hour_start` to pass to `api.journal.hourItems` for one day-view hour
 *  row, or `null` when the row can't be mapped to exactly one valid UTC hour
 *  boundary — the drill-down affordance is simply omitted for that row
 *  rather than risking a 400. Two cases produce `null`:
 *   - The row merges MORE than one API hour bucket (only possible in a
 *     half-hour-offset timezone — see `apiHoursForSlot`'s own doc comment on
 *     how rare/edge-case this already is); picking one of the two would
 *     silently show a partial hour as if it were the whole one.
 *   - The row has NO matching API hour (the current in-progress hour before
 *     anything has been captured/aggregated for it yet) AND its own local
 *     boundary doesn't land on an exact UTC hour boundary (same half-hour-
 *     offset case). When it DOES land on one, `row.start` itself is a valid
 *     `hour_start` — the endpoint returns a legitimate all-zero response for
 *     an hour with nothing in it yet, not an error. */
export function hourStartForDrilldown(row: { start: Date }, apiHours: JournalHour[]): string | null {
  const matches = apiHoursForSlot(apiHours, { start: row.start, end: new Date(row.start.getTime() + 3_600_000) });
  if (matches.length === 1) return matches[0].hour_start;
  if (matches.length === 0 && row.start.getTime() % 3_600_000 === 0) return row.start.toISOString();
  return null;
}

// ── Mapping API hours onto local slots ────────────────────────────────────────

/** Every `JournalHour` from the API whose `hour_start` instant falls inside
 *  `slot`. Almost always 0 or 1 — the backend buckets on UTC hour
 *  boundaries, so a whole-hour-offset local timezone lines up 1:1 — but a
 *  half-hour-offset timezone (e.g. India, Nepal) can straddle two UTC hour
 *  buckets inside one local hour, hence a list rather than an optional. */
export function apiHoursForSlot(apiHours: JournalHour[], slot: { start: Date; end: Date }): JournalHour[] {
  return apiHours.filter((h) => {
    const t = new Date(h.hour_start).getTime();
    return t >= slot.start.getTime() && t < slot.end.getTime();
  });
}

/** Combines the (usually ≤1) `JournalHour`s landing in one local slot into a
 *  single entries/aggregate pair to render. Concatenates rather than
 *  dedupes — the multi-bucket case is rare enough (see `apiHoursForSlot`)
 *  that simple addition is both correct and the simplest honest thing to
 *  do. */
export function mergeJournalHours(hours: JournalHour[]): { entries: JournalHour["entries"]; aggregate: JournalAggregate } {
  const entries = hours.flatMap((h) => h.entries);
  const atom_count = hours.reduce((n, h) => n + h.aggregate.atom_count, 0);
  const asks = hours.reduce((n, h) => n + h.aggregate.asks, 0);
  const blocks = hours.flatMap((h) => h.aggregate.blocks);
  return { entries, aggregate: { atom_count, asks, blocks } };
}

/** Ocean-switcher filtering (client-side — `/v1/journal` has no `graph_id`
 *  param). Only `entries[]` carries a `graph_id`; `aggregate` is a whole-
 *  account rollup with no per-ocean breakdown, so it is left untouched here
 *  — the simplest honest option given the choice the endpoint's shape
 *  forces (see the page's own comment at the call site for the render-side
 *  half of this: the deterministic aggregate fallback text is suppressed
 *  in filtered mode rather than shown as if it were ocean-scoped). */
export function filterEntriesByGraph(hours: JournalHour[], graphId: string | null): JournalHour[] {
  if (!graphId) return hours;
  return hours.map((h) => ({ ...h, entries: h.entries.filter((e) => e.graph_id === graphId) }));
}

/** The small mono qualifier the day column prints next to the aggregate
 *  block chips whenever they're standing in as a fallback under an active
 *  ocean filter — "all oceans" is the honest disclosure that this rollup
 *  couldn't be broken down per-ocean (see `filterEntriesByGraph`'s doc
 *  comment). Omitted when there's nothing to disambiguate: no filter is
 *  active, or the account only has the one reachable ocean the filter is
 *  already scoped to. */
export function aggregateQualifier(filterGraphId: string | null, reachableOceanCount: number): string | undefined {
  return filterGraphId && reachableOceanCount > 1 ? "all workspaces" : undefined;
}

// ── Staleness marker (ticket 14) ──────────────────────────────────────────────
//
// The churn guard (`backend/src/workers/journal_sweep.rs`) can back a hour's
// recompile off for 24h+ once it keeps re-tripping, so a busy hour's stored
// prose can knowably predate real, later activity with no indication in the
// UI. `/v1/journal` gives no separate "latest activity" timestamp to compare
// `compiled_at` against — the only signal available is the SAME hour's live
// aggregate the page already fetches. `stats.atom_count`/`stats.asks` (the
// entry's own compile-time snapshot) and `aggregate.atom_count`/
// `aggregate.asks` (live, re-read on every page load) are computed from the
// identical underlying rows (`compute_stats` in `journal_sweep.rs` derives
// `stats` from the exact `atoms`/`query_history` inputs
// `journal_range_aggregate` counts live) — so any growth between them is
// genuinely newer activity the stored prose doesn't reflect, not merely an
// old `compiled_at` (an hour nothing has touched since compiling stays at
// zero growth forever, and is correctly never flagged).

/** Minimum lag past `compiled_at` before newer activity counts as
 *  "meaningfully" stale rather than ordinary sweep-cycle lag. The compile
 *  worker sweeps every 10 minutes under normal operation
 *  (`JOURNAL_SWEEP_INTERVAL`, `backend/src/workers/journal_sweep.rs`), so an
 *  hour still lagging known-newer activity a full cycle later than that is
 *  either churn-guard-backed-off or otherwise stuck — doubled for headroom
 *  against sweep-pass jitter, the same convention that file's own
 *  `CLOSE_GRACE_MINUTES` uses. */
export const STALE_ENTRY_LAG_MINUTES = 20;

export interface StaleEntryInfo {
  /** The stored entry's own `compiled_at`, parsed. */
  compiledAt: Date;
  /** How many more atoms/asks the live aggregate now reports than this
   *  entry's own compile-time snapshot — always >= 1 when this is non-null. */
  newerCount: number;
}

function statNumber(stats: Record<string, unknown>, key: string): number {
  const v = stats[key];
  return typeof v === "number" && Number.isFinite(v) ? v : 0;
}

/** The staleness signal for one compiled hour entry — `null` when the entry
 *  isn't (yet) meaningfully stale, either because nothing new has landed
 *  since compile or because the lag hasn't cleared `STALE_ENTRY_LAG_MINUTES`.
 *  `aggregate` should be the SAME hour's live aggregate (`mergeJournalHours`'
 *  result at that row) — this deliberately does not read `compiled_at`'s age
 *  alone. */
export function staleEntryInfo(
  entry: JournalEntry,
  aggregate: JournalAggregate,
  now: Date = new Date(),
): StaleEntryInfo | null {
  const compiledAt = new Date(entry.compiled_at);
  // An unparseable compiled_at yields NaN, and every NaN comparison is
  // false — which would flag the entry stale forever. Treat it as not-stale.
  if (Number.isNaN(compiledAt.getTime())) return null;
  if (now.getTime() - compiledAt.getTime() < STALE_ENTRY_LAG_MINUTES * 60_000) return null;

  const newerAtoms = Math.max(0, aggregate.atom_count - statNumber(entry.stats, "atom_count"));
  const newerAsks = Math.max(0, aggregate.asks - statNumber(entry.stats, "asks"));
  const newerCount = newerAtoms + newerAsks;
  if (newerCount <= 0) return null;

  return { compiledAt, newerCount };
}

/** "prose from HH:MM" — the entry's own compile instant, in its own local
 *  minute (unlike `formatHourLabel`, which always truncates to a slot's
 *  `:00` boundary; a compile can land at any minute). */
export function formatCompiledAtLabel(compiledAt: Date): string {
  return `${String(compiledAt.getHours()).padStart(2, "0")}:${String(compiledAt.getMinutes()).padStart(2, "0")}`;
}

/** "prose from HH:MM · N newer items pending" — the stale badge's copy, per
 *  the zoom-UI prototype's approved answer (ticket 05). */
export function staleBadgeLabel(info: StaleEntryInfo): string {
  const { newerCount } = info;
  return `prose from ${formatCompiledAtLabel(info.compiledAt)} · ${newerCount} newer item${newerCount === 1 ? "" : "s"} pending`;
}

// ── Intensity strip ───────────────────────────────────────────────────────────

/** 0 = none, 1 = light, 2 = moderate, 3 = heavy. Coarse on purpose — the
 *  rail's 24-slot strip is a glanceable shape, not a precise chart. */
export type Intensity = 0 | 1 | 2 | 3;

export function intensityBucket(atomCount: number): Intensity {
  if (atomCount <= 0) return 0;
  if (atomCount < 5) return 1;
  if (atomCount < 15) return 2;
  return 3;
}

/** One intensity value per local hour slot, in `slots` order — the rail's
 *  strip, always over the WHOLE day's data (see `filterEntriesByGraph`'s
 *  comment: `aggregate` can't be filtered by ocean, so neither can this). */
export function intensityForSlots(slots: LocalHourSlot[], apiHours: JournalHour[]): Intensity[] {
  return slots.map((slot) => {
    const merged = mergeJournalHours(apiHoursForSlot(apiHours, slot));
    return intensityBucket(merged.aggregate.atom_count);
  });
}

/** True when `slot` hasn't started yet relative to `now` — a future hour on
 *  a day that's still in progress. The rail's strip needs this as a signal
 *  distinct from "intensity 0": a real past/current hour with zero atoms
 *  was genuinely quiet, while a future hour simply hasn't happened, and the
 *  two shouldn't render identically (see `buildDayRows`' doc comment for the
 *  matching row-list treatment). The slot CONTAINING `now` — the
 *  in-progress hour — is not future; it may already hold partial data. */
export function isFutureSlot(slot: LocalHourSlot, now: Date): boolean {
  return slot.start.getTime() > now.getTime();
}

/** `isFutureSlot` mapped over `slots`, in order — the rail's parallel array
 *  to `intensityForSlots`. Pass `null` for `now` (a past/non-today day) to
 *  get an all-`false` mask, since "future" is only meaningful while the day
 *  being viewed is still in progress. */
export function futureSlotMask(slots: LocalHourSlot[], now: Date | null): boolean[] {
  return slots.map((slot) => (now === null ? false : isFutureSlot(slot, now)));
}

// ── Rail stats ─────────────────────────────────────────────────────────────────

export interface DayStats {
  atoms: number;
  sourcesActive: number;
  asks: number;
  hoursActive: number;
}

/** The rail's 4 stat tiles, derived from the day's `JournalHour[]` — always
 *  whole-account (see `intensityForSlots`'s comment). `hoursActive` counts
 *  hours with real captured atoms, not merely a compiled entry — an
 *  in-progress hour with zero atoms so far shouldn't count. */
export function statsForDay(apiHours: JournalHour[]): DayStats {
  let atoms = 0;
  let asks = 0;
  let hoursActive = 0;
  const sources = new Set<string>();
  for (const h of apiHours) {
    atoms += h.aggregate.atom_count;
    asks += h.aggregate.asks;
    if (h.aggregate.atom_count > 0) hoursActive += 1;
    for (const b of h.aggregate.blocks) sources.add(b.source_type);
  }
  return { atoms, sourcesActive: sources.size, asks, hoursActive };
}

// ── "Ask about this day" ──────────────────────────────────────────────────────

/** The human phrasing for the rail's ask button — always the VIEWED date
 *  (month name + day), not necessarily today. */
export function askAboutDayPhrase(day: Date): string {
  const label = day.toLocaleDateString(undefined, { month: "long", day: "numeric" });
  return `What happened on ${label}?`;
}

/** `/chats?q=…` — prefills the composer (see `ChatComposer`'s `initialText`
 *  prop); never auto-sends. */
export function askAboutDayHref(day: Date): string {
  return `/chats?q=${encodeURIComponent(askAboutDayPhrase(day))}`;
}

// ── Small pluralization helper ────────────────────────────────────────────────

/** "1 source active" / "3 sources active" — the verify ticket's "1 SOURCES
 *  ACTIVE" copy nit generalized: every rail stat label that names its own
 *  count (not just "sources active") should agree with the number next to
 *  it. `Eyebrow` renders this uppercase via CSS, so casing here doesn't
 *  matter — only the singular/plural word does. */
export function countLabel(n: number, singular: string, plural: string = `${singular}s`): string {
  return n === 1 ? singular : plural;
}

// ══════════════════════════════════════════════════════════════════════════
// Zoomable journal (ticket 17): Day/Week/Month zoom + breadcrumb URL state,
// bullet rendering for the day+hour tiers' bounded bullet-list prompt
// contract (ticket 15's Answer), and the week/month "gist row" grid built
// from `GET /v1/journal?granularity=day|week|month` (ticket 15/16). Kept in
// this same dependency-free module for the same reason the day logic above
// is: pure, unit-testable date/string math, no React.
// ══════════════════════════════════════════════════════════════════════════

// ── `?z=` URL param ───────────────────────────────────────────────────────────

export type Zoom = "day" | "week" | "month";

/** `?z=` → the zoom level it names. Day is the default for any absent or
 *  unrecognized value — same "fail toward the narrowest, always-safe view"
 *  shape `parseDayParam` already uses for `?d=`. */
export function parseZoomParam(raw: string | null | undefined): Zoom {
  return raw === "week" || raw === "month" ? raw : "day";
}

/** The `/today` URL for one (zoom, anchor day) pair — extends the shipped
 *  `?d=` param with `&z=`, back/forward-safe (every distinct period gets its
 *  own URL via `router.push`). Day zoom keeps the shipped behavior exactly:
 *  today omits `?d=` entirely (the "live anchor" mode — see the day page's
 *  own comment), any other day gets `?d=`. Week/month zoom generalizes the
 *  same live-anchor idea: when `anchorDay` is today, the URL omits `?d=` too
 *  (`?z=week` alone means "whichever week contains today", so it keeps
 *  tracking forward exactly like the bare `/today` does for the day), and
 *  only a past/future week or month gets an explicit `?d=` alongside `&z=`.
 */
export function journalHref(zoom: Zoom, anchorDay: Date, now: Date = new Date(), who: "mine" | "team" = "mine"): string {
  const isLiveAnchor = isSameLocalDay(anchorDay, now);
  const qs: string[] = [];
  if (!isLiveAnchor) qs.push(`d=${formatDayParam(anchorDay)}`);
  if (zoom !== "day") qs.push(`z=${zoom}`);
  if (who === "team") qs.push("who=team");
  return qs.length ? `/today?${qs.join("&")}` : "/today";
}

// ── Bullet rendering (ticket 15's bounded bullet-list prompt contract) ────────
//
// The day and hour tiers now compile to a bounded bullet list (one line per
// workstream, ≤8 + an honest overflow line) instead of 1-3 sentence prose;
// week/month stay prose. Per the approved prototype AND ticket 04's Answer,
// the renderer must split on newlines into list items and NEVER interpret
// the model's output as markdown/HTML — this is LLM prose compiled over
// untrusted captured content, same rule `DayJournal`'s existing prose
// rendering already follows for `whitespace-pre-line`.
//
// `parseBulletLine` additionally splits each line on its own " — " (thread
// label — detail) convention from the prompt contract, so the label can
// render bold — but this is plain STRING SLICING on a fixed delimiter, not
// markup parsing: both halves render as ordinary React text children (never
// `dangerouslySetInnerHTML`), so it carries no injection risk beyond what
// rendering the raw string already has.

export interface BulletLine {
  /** The bold "thread label" prefix, or `null` when the line has no
   *  " — " separator (rendered as one plain run instead). */
  label: string | null;
  detail: string;
  /** The original, unsplit line — always safe to fall back to. */
  raw: string;
}

/** Safe newline-split list: trims each line, drops blank lines (a trailing
 *  newline or blank line in the model's output must not become an empty
 *  `<li>`). */
export function splitBulletLines(text: string): string[] {
  return text
    .split("\n")
    .map((line) => line.trim())
    .filter((line) => line.length > 0);
}

/** One line → `{label, detail}` on the FIRST " — " (em dash, spaced) only —
 *  a line with no such separator (or a raw overflow/footnote line like "+ 4
 *  smaller captures not summarized · 31 items this hour") renders with
 *  `label: null`, i.e. as one plain run. */
export function parseBulletLine(raw: string): BulletLine {
  const sep = " — ";
  const i = raw.indexOf(sep);
  if (i === -1) return { label: null, detail: raw, raw };
  return { label: raw.slice(0, i), detail: raw.slice(i + sep.length), raw };
}

/** `splitBulletLines` + `parseBulletLine` composed — the one call site
 *  every bullet-rendering component needs. */
export function parseBulletLines(text: string): BulletLine[] {
  return splitBulletLines(text).map(parseBulletLine);
}

// ── Local week/month bounds (request-window construction ONLY) ───────────────
//
// IMPORTANT CAVEAT, recorded here rather than assumed: `GET /v1/journal`'s
// day/week/month tier read path (`routes::journal::tier_periods_response`)
// computes period boundaries with `workers::journal_sweep::journal_tz()` —
// the GLOBAL fallback zone (`JOURNAL_TZ` env, default UTC) — NOT the
// per-user timezone the compile SWEEP itself resolves
// (`resolve_user_tz`/`users.timezone`). That split is a pre-existing
// backend contract (PR #474, out of this ticket's web-only scope to
// change), which means these two functions are NOT guaranteed to compute
// the exact same instants the server will snap a period to. They exist only
// to pick a REQUEST WINDOW wide enough to cover the desired week/month
// (`periods_covering` snaps `from` down to the period actually containing
// it and walks forward by each period's own real end, so a client window
// that's off by a few hours near a tz boundary still returns the intended
// periods in the overwhelmingly common case). Gap/period-row detection
// below therefore NEVER compares a client-computed boundary against a
// server-returned `period_start` — see `buildTierRows`'s doc comment.
//
// Same "construct from local Y/M/D components, never a flat day-count
// offset" approach `localDayBounds` already uses, so a DST transition
// inside the window is priced in automatically.

/** `[start, end)` of the Monday-start local week containing `date` (ISO
 *  week, matching the backend's `local_week_bounds` week-start convention —
 *  see the module caveat above for why this is a request-window helper, not
 *  a guaranteed-exact boundary). */
export function localWeekBounds(date: Date): { start: Date; end: Date } {
  const anchor = localDayAnchor(date);
  const daysSinceMonday = (anchor.getDay() + 6) % 7; // getDay(): 0=Sun..6=Sat
  const y = anchor.getFullYear();
  const m = anchor.getMonth();
  const d = anchor.getDate();
  const start = new Date(y, m, d - daysSinceMonday);
  const end = new Date(start.getFullYear(), start.getMonth(), start.getDate() + 7);
  return { start, end };
}

/** `[start, end)` of the local calendar month containing `date` — see the
 *  module caveat above. */
export function localMonthBounds(date: Date): { start: Date; end: Date } {
  const anchor = localDayAnchor(date);
  const start = new Date(anchor.getFullYear(), anchor.getMonth(), 1);
  const end = new Date(anchor.getFullYear(), anchor.getMonth() + 1, 1);
  return { start, end };
}

/** The `[from, to)` instant range to request for `zoom`'s OWN period (the
 *  summary card) — and, doubled as the same range, for that zoom's CHILD
 *  tier's gist rows (a week's day-gist rows span exactly the week; a
 *  month's week-gist rows span exactly the month — the server's own
 *  `periods_covering` naturally includes a "boundary" child period that
 *  starts before `from` when the tier doesn't align to it, e.g. the week
 *  containing a month's 1st). Day zoom reuses `localDayToUtcRange` (the
 *  hour view's own request-range helper) for byte-for-byte consistency with
 *  the shipped day page. */
export function zoomRequestRange(zoom: Zoom, anchor: Date): { from: Date; to: Date } {
  if (zoom === "day") return localDayToUtcRange(anchor);
  const { start, end } = zoom === "week" ? localWeekBounds(anchor) : localMonthBounds(anchor);
  return { from: start, to: end };
}

// ── Period rows (week/month "gist" grid + gap collapsing) ────────────────────

export interface PeriodRow {
  kind: "period";
  start: Date;
  end: Date;
  open: boolean;
  entries: JournalTierEntry[];
}
export interface PeriodGapRow {
  kind: "gap";
  start: Date;
  end: Date;
}
export type TierRow = PeriodRow | PeriodGapRow;

/** `periods[]` (already ascending by `period_start`, straight from the API)
 *  → one row per period, with a dashed "gap" row inserted for every stretch
 *  BETWEEN two consecutive returned periods where `period_end` doesn't meet
 *  the next one's `period_start` — covers a genuinely-quiet period the sweep
 *  never compiled a row for AND a pre-epoch period the tier endpoint omits
 *  entirely (`tier_periods_response`'s doc comment: "a period with neither a
 *  stored row nor... any live children is OMITTED"), identically to how
 *  `buildDayRows` collapses missing hours.
 *
 *  Deliberately does NOT try to detect a LEADING gap (before the first
 *  returned period) or a TRAILING one (after the last, before "now" or the
 *  requested `to`) — doing so would require independently recomputing the
 *  server's own period boundaries client-side, which `zoomRequestRange`'s
 *  doc comment explains is not reliably possible while the read path uses a
 *  global rather than per-user timezone. Every gap this function DOES report
 *  is fully determined by two server-returned boundaries, so it can never be
 *  wrong — the tradeoff is an occasional missing edge gap row rather than a
 *  fabricated one. */
export function buildTierRows(periods: JournalPeriod[]): TierRow[] {
  const rows: TierRow[] = [];
  let prevEnd: Date | null = null;
  for (const p of periods) {
    const start = new Date(p.period_start);
    const end = new Date(p.period_end);
    if (prevEnd && start.getTime() > prevEnd.getTime()) {
      rows.push({ kind: "gap", start: prevEnd, end: start });
    }
    rows.push({ kind: "period", start, end, open: p.open, entries: p.entries });
    prevEnd = end;
  }
  return rows;
}

/** "no activity · Aug 20" / "no activity · Aug 20–22" (week view, day-grain
 *  gaps) or "no activity · Week of Aug 10" / "…Week of Aug 10 – Week of Aug
 *  24" (month view, week-grain gaps) — the period-row analog of
 *  `formatGapLabel`, which is hour-specific (a plain `HH:00` boundary makes
 *  no sense at day/week grain). Same single-boundary-when-it-collapses-to-
 *  one shape as the hour version's own verify-ticket fix. */
export function periodGapLabel(row: { start: Date; end: Date }, childZoom: "day" | "week"): string {
  const spanMs = row.end.getTime() - row.start.getTime();
  if (childZoom === "day") {
    const startLabel = row.start.toLocaleDateString(undefined, { month: "short", day: "numeric" });
    const lastDay = new Date(row.end.getTime() - 1);
    const endLabel = lastDay.toLocaleDateString(undefined, { month: "short", day: "numeric" });
    // Compare the LABELS, not just the raw millisecond span — a DST-stretched
    // single day (25h, when the server's configurable `journal_tz()` is a DST
    // zone) would otherwise fail the `<= 86_400_000` check and render a
    // duplicated "Aug 20–Aug 20" range instead of collapsing.
    if (spanMs <= 86_400_000 || endLabel === startLabel) return `no activity · ${startLabel}`;
    return `no activity · ${startLabel}–${endLabel}`;
  }
  const startLabel = weekRowLabel(row.start);
  const lastWeekStart = new Date(row.end.getTime() - 7 * 86_400_000);
  const endLabel = weekRowLabel(lastWeekStart);
  if (spanMs <= 7 * 86_400_000 || endLabel === startLabel) return `no activity · ${startLabel}`;
  return `no activity · ${startLabel} – ${endLabel}`;
}

/** Ocean-switcher filtering for tier periods — the `entries[]` analog of
 *  `filterEntriesByGraph`. */
export function filterPeriodEntriesByGraph(periods: JournalPeriod[], graphId: string | null): JournalPeriod[] {
  if (!graphId) return periods;
  return periods.map((p) => ({ ...p, entries: p.entries.filter((e) => e.graph_id === graphId) }));
}

// ── Tier stats (rail tiles + intensity) ───────────────────────────────────────

interface JournalTierStatsShape {
  atom_count?: number;
  asks?: number;
  sources?: Record<string, number>;
}

function tierEntryStats(entry: JournalTierEntry): JournalTierStatsShape {
  return (entry.stats ?? {}) as JournalTierStatsShape;
}

/** "41 atoms · 3 asks" — a single gist row's own meta line (a week view's
 *  day rows, a month view's week rows), summed across that ONE period's
 *  `entries[]` (almost always 1, multi-ocean rows aside — same shape as
 *  `statsForPeriods` but for one row instead of a whole rail). */
export function periodRowStats(entries: JournalTierEntry[]): { atoms: number; asks: number } {
  let atoms = 0;
  let asks = 0;
  for (const e of entries) {
    const s = tierEntryStats(e);
    atoms += s.atom_count ?? 0;
    asks += s.asks ?? 0;
  }
  return { atoms, asks };
}

export interface PeriodStats {
  atoms: number;
  sourcesActive: number;
  asks: number;
  /** Count of PERIODS (days, for a week view; weeks, for a month view) that
   *  had at least one atom — the tier analog of `DayStats.hoursActive`. */
  periodsActive: number;
}

/** The rail's 4 stat tiles at week/month zoom, aggregated over a tier's
 *  CHILD periods (a week view's day rows; a month view's week rows) —
 *  mirrors `statsForDay`'s own whole-range aggregation. */
export function statsForPeriods(periods: JournalPeriod[]): PeriodStats {
  let atoms = 0;
  let asks = 0;
  let periodsActive = 0;
  const sources = new Set<string>();
  for (const p of periods) {
    let periodAtoms = 0;
    for (const e of p.entries) {
      const s = tierEntryStats(e);
      periodAtoms += s.atom_count ?? 0;
      asks += s.asks ?? 0;
      for (const src of Object.keys(s.sources ?? {})) sources.add(src);
    }
    atoms += periodAtoms;
    if (periodAtoms > 0) periodsActive += 1;
  }
  return { atoms, sourcesActive: sources.size, asks, periodsActive };
}

/** Coarse intensity bucket, same 4-step scale `intensityBucket` uses for
 *  hours — but week/month child periods run at a very different scale (a
 *  day's atom count vs. an hour's), so each zoom gets its own thresholds
 *  rather than reusing the hour-tuned ones verbatim. Still a glanceable
 *  shape, not a precise chart, per `Intensity`'s own doc comment. */
export function intensityBucketForZoom(zoom: "week" | "month", atomCount: number): Intensity {
  if (atomCount <= 0) return 0;
  const [light, moderate] = zoom === "week" ? [10, 30] : [40, 120];
  if (atomCount < light) return 1;
  if (atomCount < moderate) return 2;
  return 3;
}

/** One intensity value per CHILD period, in order — the week/month rail's
 *  ministrip, parallel to `periods`. */
export function intensityForPeriods(zoom: "week" | "month", periods: JournalPeriod[]): Intensity[] {
  return periods.map((p) => {
    const atoms = p.entries.reduce((n, e) => n + (tierEntryStats(e).atom_count ?? 0), 0);
    return intensityBucketForZoom(zoom, atoms);
  });
}

// ── Week/month header titles ──────────────────────────────────────────────────

/** "Week of August 17" (+ "· this week" italic when `open`, mirroring
 *  `dayHeaderTitle`'s "· today"). `periodStart` is the API's own
 *  `period_start` (already the correct Monday) — never re-derived
 *  client-side, per `zoomRequestRange`'s timezone caveat. */
export function weekHeaderTitle(periodStart: Date, open: boolean): { title: string; italicWord?: string } {
  const label = `Week of ${periodStart.toLocaleDateString(undefined, { month: "long", day: "numeric" })}`;
  return open ? { title: `${label} · this week`, italicWord: "this week" } : { title: label };
}

/** "August 2026" (+ "· this month" italic when `open`). */
export function monthHeaderTitle(periodStart: Date, open: boolean): { title: string; italicWord?: string } {
  const label = periodStart.toLocaleDateString(undefined, { month: "long", year: "numeric" });
  return open ? { title: `${label} · this month`, italicWord: "this month" } : { title: label };
}

/** The breadcrumb's per-row label — "Week of Aug 17" / day-gist row date
 *  ("Mon Aug 17") — kept short (abbreviated month) since it's metadata, not
 *  the page title. */
export function weekRowLabel(periodStart: Date): string {
  return `Week of ${periodStart.toLocaleDateString(undefined, { month: "short", day: "numeric" })}`;
}
export function dayRowLabel(periodStart: Date): string {
  return periodStart.toLocaleDateString(undefined, { weekday: "short", month: "short", day: "numeric" });
}

/** Just the abbreviated weekday ("Mon") — the week view's tick column.
 *  Never slice `dayRowLabel`'s output: its component order (weekday vs.
 *  date first) is locale-dependent, so a fixed `.slice(0, 3)` can yield a
 *  truncated date fragment (e.g. ja-JP) or a trailing period (e.g. de-DE)
 *  instead of the weekday. */
export function weekdayTickLabel(periodStart: Date): string {
  return periodStart.toLocaleDateString(undefined, { weekday: "short" });
}
export function monthCrumbLabel(periodStart: Date): string {
  return periodStart.toLocaleDateString(undefined, { month: "long", year: "numeric" });
}

// ── "Ask about this week/month" ───────────────────────────────────────────────

export function askAboutWeekPhrase(periodStart: Date): string {
  const label = periodStart.toLocaleDateString(undefined, { month: "long", day: "numeric" });
  return `What happened the week of ${label}?`;
}
export function askAboutWeekHref(periodStart: Date): string {
  return `/chats?q=${encodeURIComponent(askAboutWeekPhrase(periodStart))}`;
}
export function askAboutMonthPhrase(periodStart: Date): string {
  return `What happened in ${monthCrumbLabel(periodStart)}?`;
}
export function askAboutMonthHref(periodStart: Date): string {
  return `/chats?q=${encodeURIComponent(askAboutMonthPhrase(periodStart))}`;
}

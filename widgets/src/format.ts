// Formatting helpers shared by every template. Kept dependency-free (no
// date-fns/dayjs) — the formats needed are small and fixed.

const MONTHS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];

/** "Feb 12" — artboards' provenance-tag date shape. */
export function shortDate(iso: string | null | undefined): string {
  if (!iso) return "";
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return "";
  return `${MONTHS[d.getUTCMonth()]} ${d.getUTCDate()}`;
}

/** "Feb 9, 2026" */
export function longDate(iso: string | null | undefined): string {
  if (!iso) return "";
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return "";
  return `${MONTHS[d.getUTCMonth()]} ${d.getUTCDate()}, ${d.getUTCFullYear()}`;
}

/** "14:31" (24h, UTC — the widget has no reliable local timezone signal from
 *  structuredContent alone, and the artboards' times read as plain HH:MM). */
export function timeOfDay(iso: string | null | undefined): string {
  if (!iso) return "";
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return "";
  return `${String(d.getUTCHours()).padStart(2, "0")}:${String(d.getUTCMinutes()).padStart(2, "0")}`;
}

/** "Feb 9, 2026 · 14:31" */
export function longDateTime(iso: string | null | undefined): string {
  if (!iso) return "";
  const date = longDate(iso);
  const time = timeOfDay(iso);
  return time ? `${date} · ${time}` : date;
}

/** "2d" / "5d" / "2w" — the artboards' relative-age shorthand, derived from
 *  a whole-days count (matches `age_days` field semantics exactly, no extra
 *  "now" math needed when that field is present). */
export function relativeAgeFromDays(ageDays: number | null | undefined): string {
  if (ageDays === null || ageDays === undefined) return "";
  if (ageDays <= 0) return "today";
  if (ageDays === 1) return "1d";
  if (ageDays < 14) return `${ageDays}d`;
  if (ageDays < 60) return `${Math.round(ageDays / 7)}w`;
  return `${Math.round(ageDays / 30)}mo`;
}

export function relativeAgeFromIso(iso: string | null | undefined): string {
  if (!iso) return "";
  const then = new Date(iso).getTime();
  if (Number.isNaN(then)) return "";
  const days = Math.floor((Date.now() - then) / 86_400_000);
  return relativeAgeFromDays(days);
}

/** Day-bucket label for grouping activity rows: "today" / "yesterday" / a
 *  short date — client-side grouping since `recent_activity` returns a flat
 *  paginated list, no day-bucketed aggregate (see README "Backend fields
 *  needed"). */
export function dayBucket(iso: string): string {
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return "";
  const now = new Date();
  const startOfUtcDay = (x: Date) => Date.UTC(x.getUTCFullYear(), x.getUTCMonth(), x.getUTCDate());
  const diffDays = Math.round((startOfUtcDay(now) - startOfUtcDay(d)) / 86_400_000);
  if (diffDays === 0) return "today";
  if (diffDays === 1) return "yesterday";
  return longDate(iso);
}

export function pluralize(count: number, singular: string, plural = `${singular}s`): string {
  return count === 1 ? singular : plural;
}

/** Truncate on a word boundary, mirroring the backend's
 *  `truncate_at_boundary` intent closely enough for display (the backend
 *  already truncates `content`/`snippet`; this only guards a headline/
 *  excerpt field the widget itself renders in a fixed-height row). */
export function truncateWords(s: string, maxChars: number): string {
  if (s.length <= maxChars) return s;
  const cut = s.slice(0, maxChars);
  const lastSpace = cut.lastIndexOf(" ");
  return `${lastSpace > maxChars * 0.6 ? cut.slice(0, lastSpace) : cut}…`;
}

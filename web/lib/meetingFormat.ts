// Shared meeting date/duration formatting — pure functions, no React. Used by
// the meetings list, search result rows, meeting export (Markdown), and the
// public share view, so a meeting's "when/how long" reads identically
// everywhere.

/** Human date/time, e.g. "Jul 15, 2:30 PM". Falls back to the raw string on an
 *  unparsable ISO timestamp. */
export function fmtWhen(iso: string): string {
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return iso;
  return d.toLocaleString(undefined, {
    month: "short",
    day: "numeric",
    hour: "numeric",
    minute: "2-digit",
  });
}

// Wall-clock duration from start/end, in ms. null end (unknown) or an invalid
// range → undefined. Display-only, never used for chunking.
export function durationMsBetween(startedAt: string, endedAt: string | null): number | undefined {
  if (!endedAt) return undefined;
  const start = new Date(startedAt).getTime();
  const end = new Date(endedAt).getTime();
  if (Number.isNaN(start) || Number.isNaN(end) || end < start) return undefined;
  return end - start;
}

/** Render a millisecond duration as "Xs" / "Xm YYs" / "Xh YYm". `undefined`/`null`
 *  → null (unknown duration, e.g. still in progress). */
export function fmtDurationMs(ms: number | undefined | null): string | null {
  if (ms === undefined || ms === null || Number.isNaN(ms) || ms < 0) return null;
  const secs = Math.round(ms / 1000);
  const m = Math.floor(secs / 60);
  const s = secs % 60;
  if (m === 0) return `${s}s`;
  if (m < 60) return `${m}m ${s.toString().padStart(2, "0")}s`;
  const h = Math.floor(m / 60);
  return `${h}h ${(m % 60).toString().padStart(2, "0")}m`;
}

/** Wall-clock duration from start/end. null end (unknown) → null. */
export function fmtDuration(startedAt: string, endedAt: string | null): string | null {
  return fmtDurationMs(durationMsBetween(startedAt, endedAt));
}

/** `[mm:ss]`-style timestamp from a segment's `start_ms`/`end_ms` offset. */
export function fmtTimestampMs(ms: number): string {
  const totalSec = Math.max(0, Math.round(ms / 1000));
  const m = Math.floor(totalSec / 60);
  const s = totalSec % 60;
  return `${m.toString().padStart(2, "0")}:${s.toString().padStart(2, "0")}`;
}

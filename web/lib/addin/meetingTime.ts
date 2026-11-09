/* "Today · 2:00 – 2:30 PM" for the Teams hero card, from the status snapshot's
 * occurrence bounds (ISO strings, local time). Returns null when either bound is
 * missing or unparseable so the caller omits the line. */

export function formatMeetingWhen(startIso: string | null | undefined, endIso: string | null | undefined, now: Date = new Date()): string | null {
  const start = startIso ? new Date(startIso) : null;
  const end = endIso ? new Date(endIso) : null;
  if (!start || Number.isNaN(start.getTime()) || !end || Number.isNaN(end.getTime()) || end < start) return null;
  const startOfDay = (d: Date) => new Date(d.getFullYear(), d.getMonth(), d.getDate()).getTime();
  const days = Math.round((startOfDay(start) - startOfDay(now)) / 86_400_000);
  const day = days === 0 ? "Today" : days === 1 ? "Tomorrow" : days === -1 ? "Yesterday"
    : new Intl.DateTimeFormat(undefined, { weekday: "short", month: "short", day: "numeric" }).format(start);
  const time = new Intl.DateTimeFormat(undefined, { hour: "numeric", minute: "2-digit" });
  // A window longer than ~8h or spanning midnight is a capture window (e.g. Meet now = 24h), not a
  // meeting length: show only when it started.
  const startOnly = end.getTime() - start.getTime() > 8 * 3_600_000 || startOfDay(start) !== startOfDay(end);
  if (startOnly) return `${day} · ${time.format(start)}`;
  let range: string;
  try { range = time.formatRange(start, end); } catch { range = `${time.format(start)} – ${time.format(end)}`; }
  return `${day} · ${range}`;
}

/** "today at 3:42 PM", "tomorrow at 9:00 AM", "Thu, Oct 8 at 3:42 PM" in local time, for a deadline that is still
 *  ahead. Null for a missing, unparseable or already-past instant, so the caller drops the sentence. */
export function formatDeadline(iso: string | null | undefined, now: Date = new Date()): string | null {
  const at = iso ? new Date(iso) : null;
  if (!at || Number.isNaN(at.getTime()) || at.getTime() <= now.getTime()) return null;
  const startOfDay = (d: Date) => new Date(d.getFullYear(), d.getMonth(), d.getDate()).getTime();
  const days = Math.round((startOfDay(at) - startOfDay(now)) / 86_400_000);
  const day = days === 0 ? "today" : days === 1 ? "tomorrow"
    : new Intl.DateTimeFormat(undefined, { weekday: "short", month: "short", day: "numeric" }).format(at);
  return `${day} at ${new Intl.DateTimeFormat(undefined, { hour: "numeric", minute: "2-digit" }).format(at)}`;
}

// Shared derivation helpers for rendering a `CitationDetail` /
// `SearchAtomResult` — both shapes carry the same enrichment fields
// (source_type/channel_name/author_name/timestamp/reefs), just under
// slightly different optionality (see types.ts's own comments).
//
// IMPORTANT GAP (see widgets/README.md "Backend fields needed"): neither
// shape carries a human "session/meeting/thread name" distinct from
// `channel_name` — a meeting's title, a coding session's project/branch, a
// doc's title beyond the coral's own headline. The artboards show context
// like "PLATFORM SYNC" / "API-GATEWAY · MAIN" / "PARTNER PORTAL SPEC" that
// this data does not carry today. `contextLabel` below degrades honestly:
// channel_name, then `project` (search_atoms only), then the source type's
// own display label — never invented meeting/session titles.

import { shortDate, longDateTime } from "./format";
import { sourceTypeLabel } from "./theme";

export interface CitationLike {
  source_type?: string;
  channel_name?: string | null;
  author_name?: string | null;
  project?: string | null;
  timestamp?: string | null;
}

/** "PLATFORM SYNC · FEB 12" style context (uppercased by the caller's CSS,
 *  not here) — best-effort, see the gap note above. */
export function contextLabel(c: CitationLike): string {
  const context = c.channel_name || c.project || sourceTypeLabel(c.source_type);
  const date = shortDate(c.timestamp);
  return date ? `${context} · ${date}` : context;
}

/** "MO" from "Marcus Oduya" — avatar-badge initials, capped at 2 chars. */
export function initials(name: string | null | undefined): string {
  if (!name) return "?";
  const parts = name.trim().split(/\s+/).filter(Boolean);
  const first = parts[0];
  if (!first) return "?";
  if (parts.length === 1) return first.slice(0, 2).toUpperCase();
  const last = parts[parts.length - 1];
  const firstChar = first[0];
  const lastChar = last?.[0];
  if (!firstChar || !lastChar) return first.slice(0, 2).toUpperCase();
  return (firstChar + lastChar).toUpperCase();
}

export function whoWhereWhen(c: CitationLike): { who: string; where: string; when: string } {
  return {
    who: c.author_name || "Unknown",
    where: c.channel_name || sourceTypeLabel(c.source_type),
    when: longDateTime(c.timestamp),
  };
}

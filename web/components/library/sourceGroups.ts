// Fallback label for a Library · Sources channel-group row when the backend
// hasn't resolved a real "#channel" name for it. Ported from
// archive/web-legacy-ui/screens/reefs.page.tsx's RawSourcesMode `groups`
// builder; perf/library-source-groups moved the actual grouping server-side
// (`GET /v1/graphs/{id}/source-groups` — see `Store::source_groups`), so
// this file now holds only the one piece of client-side labelling logic the
// server response still needs a caller to apply: `channel_label` is `null`
// until ingest has resolved a real name, and the row still needs SOME label
// in the meantime.

import { canonicalSourceType, sourceMeta } from "./sourceKind";
import { fmtDate } from "@/lib/coral";

// Source types that are inherently channel-less — a meeting or a pasted note
// is its own thing, not a channel whose name resolves later.
const CHANNELLESS = new Set(["transcript", "note"]);

/** Resolved "#channel" name when known, else a sensible per-type label —
 *  never the opaque channel_key (e.g. a Slack channel id). Structurally
 *  typed rather than importing `SourceGroupRow`/`SourceGroupDetail` from
 *  `lib/api` — both shapes carry these same four fields, so either can be
 *  passed as-is. */
export function channelName(g: {
  channel_label: string | null;
  source_type: string;
  first_ts: string | null;
  last_ts: string | null;
}): string {
  if (g.channel_label) return g.channel_label;
  const label = sourceMeta(g.source_type).label;
  if (CHANNELLESS.has(canonicalSourceType(g.source_type))) {
    const d = fmtDate(g.last_ts ?? g.first_ts);
    return d ? `${label} · ${d}` : label;
  }
  return `${label} (channel name pending)`;
}

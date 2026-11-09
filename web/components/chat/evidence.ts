import type { Hue } from "@/components/ui";
import { citationCardLine, fmtDate, srcName } from "@/lib/coral";
import type { AssistantTurn } from "./types";
import { parseJevVerdict, type JevVerdict } from "@/lib/graphSurfaces";

export interface EvidenceSource {
  n: number;
  /** The captured item this passage came from. Several cited passages can
   *  share one; counts and the Sources tab group by it. */
  sourceKey: string;
  title: string;
  quote: string;
  ocean: string | null;
  type: string;
  sourceType?: string | null;
  date: string;
  hue: Hue;
  warning?: string;
  notice?: string;
  representation?: string;
  occurredAt?: string | null;
  capturedAt?: string | null;
  /** Jev's check of this cite (#1353); absent when not cited, not checked, or an older answer. */
  verdict?: JevVerdict | null;
}

function sourceHue(type: string | null | undefined): Hue {
  if (type === "slack") return "amber";
  if (["meeting", "zoom", "transcript"].includes(type ?? "")) return "teal";
  if (type === "decision") return "coral";
  return "plum";
}

/** Keep wire citation numbers intact, including gaps between streamed frames.
 * Empty placeholders are not evidence and must never become clickable cards. */
export function evidenceSources(turn: AssistantTurn | null): EvidenceSource[] {
  if (!turn) return [];
  const retained = turn.retainedCitations;
  if (retained !== undefined) {
    if (turn.supportStatus !== "complete") return [];
    return [...retained.map((c, i): EvidenceSource => ({
      n: i + 1, sourceKey: `${c.graph_id}:${c.source_item_id}`, title: c.title?.trim() || (c.source_type ? srcName(c.source_type) : `Retained passage ${i + 1}`), quote: c.quote,
      ocean: turn.graphNames?.[c.graph_id] ?? null,
      sourceType: c.source_type, type: "Exact saved passage", date: c.captured_at, hue: c.source_type ? sourceHue(c.source_type) : "teal", representation: c.representation,
      occurredAt: c.occurred_at, capturedAt: c.captured_at, verdict: parseJevVerdict(c.jev_verdict),
      notice: [c.quote_truncated && "Beginning of the retained passage; preview shortened.", turn.syntheticDemo && "Contains synthetic demo evidence"].filter(Boolean).join(" ") || undefined,
    })), ...(turn.legacyEvidence || []).map((c, i): EvidenceSource => ({
      // No source-item mapping exists for a raw chunk, so each is its own source.
      n: retained.length + i + 1, sourceKey: `chunk:${c.graph_id}:${c.atom_id}`, title: "Unmapped retained chunk", quote: c.content, ocean: turn.graphNames?.[c.graph_id] ?? null,
      type: "Raw retrieval chunk", date: c.captured_at || "Unknown", hue: "plum", representation: c.representation,
      occurredAt: c.occurred_at, capturedAt: c.captured_at, verdict: parseJevVerdict(c.jev_verdict),
      notice: "No exact source-version mapping is available for this chunk." + (c.content_truncated ? " Preview shortened." : ""),
    }))];
  }
  if (turn.citationDetails.length) {
    return turn.citationDetails.flatMap((c, i) => c.atom_id ? [{
      n: i + 1,
      sourceKey: c.atom_id,
      ...citationCardLine(c),
      ocean: c.graph_name || null,
      sourceType: c.source_type, type: srcName(c.source_type),
      date: fmtDate(c.timestamp),
      hue: sourceHue(c.source_type),
      warning: c.pending_suggestion?.prompt,
    }] : []);
  }
  return turn.sources.flatMap((s, i) => s.atom_id ? [{
    n: i + 1,
    sourceKey: s.atom_id,
    ...citationCardLine(s),
    ocean: s.ocean_name || null,
    sourceType: s.source_type, type: srcName(s.source_type),
    date: fmtDate(s.timestamp),
    hue: sourceHue(s.source_type),
    warning: s.pending_suggestion?.prompt,
  }] : []);
}

/** Distinct captured sources behind these passages: 31 passages from 28
 *  items is "28 sources". */
export function sourceCount(sources: readonly EvidenceSource[]): number {
  return new Set(sources.map((s) => s.sourceKey)).size;
}

export interface SourceGroup { key: string; passages: EvidenceSource[] }

/** Passages grouped under their source, in order of first citation. */
export function groupBySource(sources: readonly EvidenceSource[]): SourceGroup[] {
  const groups = new Map<string, SourceGroup>();
  for (const s of sources) {
    const group = groups.get(s.sourceKey);
    if (group) group.passages.push(s);
    else groups.set(s.sourceKey, { key: s.sourceKey, passages: [s] });
  }
  return [...groups.values()];
}

/** Jev verdict for citation `n` of a scoped answer, in the same numbering the
 *  prose uses ([n] = retained_citations[n-1], then legacy_evidence). */
export function citationVerdict(turn: AssistantTurn | null, n: number): JevVerdict | null {
  if (!turn?.retainedCitations || turn.supportStatus !== "complete" || !Number.isSafeInteger(n) || n < 1) return null;
  const retained = turn.retainedCitations;
  const item = n <= retained.length ? retained[n - 1] : turn.legacyEvidence?.[n - retained.length - 1];
  return parseJevVerdict(item?.jev_verdict);
}

export function rankByCitations(text: string, sources: readonly EvidenceSource[]): EvidenceSource[] {
  const counts = new Map<number, number>();
  for (const match of text.matchAll(/\[(\d+)\]/g)) counts.set(+match[1], (counts.get(+match[1]) ?? 0) + 1);
  return groupBySource(sources).map(g => ({ source: [...g.passages].sort((a,b) => a.n-b.n)[0], count: g.passages.reduce((n,s) => n + (counts.get(s.n) ?? 0), 0) }))
    .sort((a,b) => b.count-a.count || a.source.n-b.source.n).map(g => g.source);
}

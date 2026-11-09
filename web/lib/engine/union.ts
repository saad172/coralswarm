// Union-RAG context assembly (ADR-0007 D9). Only the device can span the
// private ocean (decrypted locally, server-blind) and the shared oceans
// (server-fetched), so the union happens here: merge local private hits with
// server-fetched shared hits into one deduped, ranked, capped context for
// synthesis (cloud under ZDR, or local).

import { mergeRanked } from "./vector";

export interface RetrievedCoral {
  id: string;
  content: string;
  score: number;
}

export interface ContextItem extends RetrievedCoral {
  source: "private" | "shared";
}

/** Merge private + shared retrieval into one ranked context (dedup by id, keep
 *  the higher score), capped to `maxItems`. */
export function assembleContext(
  privateHits: RetrievedCoral[],
  sharedHits: RetrievedCoral[],
  maxItems = 12,
): ContextItem[] {
  type Item = { id: string; content: string; source: "private" | "shared" };
  const p = privateHits.map((h) => ({
    item: { id: h.id, content: h.content, source: "private" as const },
    score: h.score,
  }));
  const s = sharedHits.map((h) => ({
    item: { id: h.id, content: h.content, source: "shared" as const },
    score: h.score,
  }));
  return mergeRanked<Item>([p, s], (t) => t.id, maxItems).map((m) => ({
    id: m.item.id,
    content: m.item.content,
    score: m.score,
    source: m.item.source,
  }));
}

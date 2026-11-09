// Local vector-index math for on-device retrieval (ADR-0007 D9/D10). Brute-force
// cosine over the user's decrypted private corals — fine at personal-ocean scale;
// swap for an ANN index (e.g. hnswlib-wasm) later if needed.

export type Vec = Float32Array | number[];

export function cosine(a: Vec, b: Vec): number {
  const n = Math.min(a.length, b.length);
  let dot = 0;
  let na = 0;
  let nb = 0;
  for (let i = 0; i < n; i++) {
    dot += a[i] * b[i];
    na += a[i] * a[i];
    nb += b[i] * b[i];
  }
  if (na === 0 || nb === 0) return 0;
  return dot / (Math.sqrt(na) * Math.sqrt(nb));
}

export interface Indexed<T> {
  item: T;
  vector: Vec;
}

export interface Scored<T> {
  item: T;
  score: number;
}

/** Top-k by cosine similarity to `query`, highest first. */
export function topK<T>(query: Vec, items: Indexed<T>[], k: number): Scored<T>[] {
  return items
    .map((x) => ({ item: x.item, score: cosine(query, x.vector) }))
    .sort((a, b) => b.score - a.score)
    .slice(0, Math.max(0, k));
}

/** Merge several ranked lists into one (the union-RAG combine, D9), keeping the
 *  best score per item id and re-sorting. `idOf` dedupes across sources. */
export function mergeRanked<T>(lists: Scored<T>[][], idOf: (t: T) => string, k: number): Scored<T>[] {
  const best = new Map<string, Scored<T>>();
  for (const list of lists) {
    for (const s of list) {
      const id = idOf(s.item);
      const prev = best.get(id);
      if (!prev || s.score > prev.score) best.set(id, s);
    }
  }
  return [...best.values()].sort((a, b) => b.score - a.score).slice(0, Math.max(0, k));
}

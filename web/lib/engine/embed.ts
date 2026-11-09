// Text embedding for on-device retrieval (ADR-0007 D10). The pipeline depends
// only on the `Embedder` interface, so a real semantic model
// (onnxruntime-web + all-MiniLM, 384-d — the same model the backend uses, for
// vector compatibility) is a drop-in. `hashEmbedder` is a deterministic,
// model-free embedder: it keeps the engine fully testable and serves as a
// degraded offline fallback until the ONNX model is wired + browser-validated.

export interface Embedder {
  readonly dim: number;
  /** Stamp written next to stored vectors. Cloud search uses openai/text-embedding-3-small@384. */
  readonly model?: string;
  embed(texts: string[]): Promise<Float32Array[]>;
}

export function hashEmbedder(dim = 384): Embedder {
  return {
    dim,
    async embed(texts: string[]): Promise<Float32Array[]> {
      return texts.map((t) => {
        const v = new Float32Array(dim);
        for (const tok of t.toLowerCase().split(/\W+/).filter(Boolean)) {
          // FNV-1a hash → bucket; bag-of-tokens vector.
          let h = 2166136261;
          for (let i = 0; i < tok.length; i++) {
            h ^= tok.charCodeAt(i);
            h = Math.imul(h, 16777619);
          }
          v[(h >>> 0) % dim] += 1;
        }
        let norm = 0;
        for (const x of v) norm += x * x;
        norm = Math.sqrt(norm) || 1;
        for (let i = 0; i < dim; i++) v[i] /= norm;
        return v;
      });
    },
  };
}

// Cloud embedder for the browser private engine. POSTs texts to `/v1/embed`
// so the page never holds OPENROUTER_API_KEY.

import { api } from "../api";
import type { Embedder } from "./embed";

export const CLOUD_EMBED_MODEL = "openai/text-embedding-3-small@384";

export function serverEmbedder(token: string): Embedder {
  return {
    dim: 384,
    model: CLOUD_EMBED_MODEL,
    async embed(texts: string[]): Promise<Float32Array[]> {
      if (texts.length === 0) return [];
      const out: Float32Array[] = [];
      for (let i = 0; i < texts.length; i += 8) {
        const chunk = texts.slice(i, i + 8);
        const res = await api.embed(token, chunk);
        if (!res.embeddings || res.embeddings.length !== chunk.length) {
          throw new Error("embed: unexpected batch size");
        }
        for (const row of res.embeddings) {
          out.push(Float32Array.from(row));
        }
      }
      return out;
    },
  };
}

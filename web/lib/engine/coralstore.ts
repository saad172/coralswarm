// Encrypted on-device store for the private ocean's corals (ADR-0007 D5/D9).
// Content is AES-GCM-encrypted under the ocean key before persistence; embeddings
// are stored alongside for local vector search. Persisted via the pluggable KV
// (IndexedDB in the browser). The server never sees any of this.

import type { KV } from "./keyvault";
import { encryptString, decryptString, toBase64, fromBase64 } from "./crypto";
import { topK, type Scored } from "./vector";
import type { Embedder } from "./embed";

export interface CoralMeta {
  id: string;
  sourceType: string;
  sourceId: string;
}

interface Record extends CoralMeta {
  enc: string; // base64 [iv|ciphertext] of the content
  emb: number[]; // embedding vector
  embModel?: string;
}

export interface SearchHit extends Scored<CoralMeta> {
  content: string;
}

const INDEX = "cs_private_corals";

export class CoralStore {
  /** Serializes INDEX mutations so reindex cannot clobber a concurrent ingest. */
  private writeTail: Promise<unknown> = Promise.resolve();

  constructor(
    private kv: KV,
    private key: CryptoKey,
  ) {}

  private enqueueWrite<T>(op: () => Promise<T>): Promise<T> {
    const run = this.writeTail.then(op, op);
    this.writeTail = run.then(
      () => undefined,
      () => undefined,
    );
    return run;
  }

  private async all(): Promise<Record[]> {
    const raw = await this.kv.get(INDEX);
    return raw ? (JSON.parse(raw) as Record[]) : [];
  }

  async put(coral: CoralMeta & { content: string; embedding: Float32Array; embModel?: string }): Promise<void> {
    await this.enqueueWrite(async () => {
      const recs = await this.all();
      const rec: Record = {
        id: coral.id,
        sourceType: coral.sourceType,
        sourceId: coral.sourceId,
        enc: toBase64(await encryptString(this.key, coral.content)),
        emb: Array.from(coral.embedding),
        embModel: coral.embModel,
      };
      const i = recs.findIndex((r) => r.id === coral.id);
      if (i >= 0) recs[i] = rec;
      else recs.push(rec);
      await this.kv.set(INDEX, JSON.stringify(recs));
    });
  }

  async search(queryEmbedding: Float32Array, k: number): Promise<SearchHit[]> {
    const recs = await this.all();
    const ranked = topK(
      queryEmbedding,
      recs.map((r) => ({ item: r, vector: r.emb })),
      k,
    );
    const hits: SearchHit[] = [];
    for (const s of ranked) {
      const content = await decryptString(this.key, fromBase64(s.item.enc));
      hits.push({
        item: { id: s.item.id, sourceType: s.item.sourceType, sourceId: s.item.sourceId },
        score: s.score,
        content,
      });
    }
    return hits;
  }

  async count(): Promise<number> {
    return (await this.all()).length;
  }

  /** Re-embed stored corals that are missing `expectedModel` (MiniLM leftovers). */
  async reindex(embedder: Embedder, expectedModel: string): Promise<void> {
    const stale = await this.enqueueWrite(async () => {
      const recs = await this.all();
      return recs.filter((r) => r.embModel !== expectedModel);
    });
    if (stale.length === 0) return;
    const contents: string[] = [];
    for (const r of stale) {
      contents.push(await decryptString(this.key, fromBase64(r.enc)));
    }
    const embeddings = await embedder.embed(contents);
    await this.enqueueWrite(async () => {
      const recs = await this.all();
      const byId = new Map(stale.map((r, i) => [r.id, embeddings[i]]));
      for (let i = 0; i < recs.length; i++) {
        const emb = byId.get(recs[i].id);
        if (!emb) continue;
        recs[i] = { ...recs[i], emb: Array.from(emb), embModel: expectedModel };
      }
      await this.kv.set(INDEX, JSON.stringify(recs));
    });
  }
}

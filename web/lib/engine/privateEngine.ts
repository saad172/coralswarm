// On-device private retrieval engine (ADR-0007 D9/D10). Ingest embeds + encrypts
// + stores corals locally; search embeds the query and returns decrypted top-k.
// The server stays blind to private content. The ask layer unions these local
// hits with server-fetched shared (org/team) results via vector.mergeRanked.

import type { KV } from "./keyvault";
import type { Embedder } from "./embed";
import { CoralStore, type SearchHit } from "./coralstore";

export interface IngestCoral {
  id: string;
  sourceType: string;
  sourceId: string;
  content: string;
}

export class PrivateEngine {
  private store: CoralStore;

  constructor(
    kv: KV,
    key: CryptoKey,
    private embedder: Embedder,
  ) {
    this.store = new CoralStore(kv, key);
  }

  /** Embed, encrypt, and store corals in the local private index. */
  async ingest(corals: IngestCoral[]): Promise<void> {
    if (corals.length === 0) return;
    const embeddings = await this.embedder.embed(corals.map((c) => c.content));
    for (let i = 0; i < corals.length; i++) {
      await this.store.put({
        ...corals[i],
        embedding: embeddings[i],
        embModel: this.embedder.model,
      });
    }
  }

  /** Embed the query and return the decrypted top-k local corals. */
  async search(query: string, k = 8): Promise<SearchHit[]> {
    const [q] = await this.embedder.embed([query]);
    return this.store.search(q, k);
  }

  count(): Promise<number> {
    return this.store.count();
  }

  /** Recompute MiniLM vectors already in IndexedDB onto the cloud model. */
  reindexIfStale(expectedModel: string): Promise<void> {
    return this.store.reindex(this.embedder, expectedModel);
  }
}

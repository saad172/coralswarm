import { describe, it, expect } from "vitest";
import { PrivateEngine } from "./privateEngine";
import { hashEmbedder } from "./embed";
import { generateOceanKey } from "./crypto";
import type { KV } from "./keyvault";

function memKV(): KV {
  const m = new Map<string, string>();
  return {
    async get(k) {
      return m.get(k) ?? null;
    },
    async set(k, v) {
      m.set(k, v);
    },
    async del(k) {
      m.delete(k);
    },
  };
}

describe("engine/privateEngine (ingest → encrypt → store → search → decrypt)", () => {
  it("retrieves the most relevant coral, with decrypted content", async () => {
    const engine = new PrivateEngine(memKV(), await generateOceanKey(), hashEmbedder());
    await engine.ingest([
      { id: "1", sourceType: "note", sourceId: "a", content: "We adopted Clerk for authentication" },
      { id: "2", sourceType: "note", sourceId: "b", content: "Embeddings are precomputed in pgvector" },
      { id: "3", sourceType: "note", sourceId: "c", content: "The reef rename flow uses PATCH nodes" },
    ]);
    expect(await engine.count()).toBe(3);

    const hits = await engine.search("authentication clerk", 2);
    expect(hits.length).toBe(2);
    expect(hits[0].item.id).toBe("1");
    expect(hits[0].content).toContain("Clerk");
  });

  it("persists across engine instances sharing the KV + key", async () => {
    const kv = memKV();
    const key = await generateOceanKey();
    await new PrivateEngine(kv, key, hashEmbedder()).ingest([
      { id: "x", sourceType: "note", sourceId: "s", content: "private decision about pricing" },
    ]);
    const reopened = new PrivateEngine(kv, key, hashEmbedder());
    const hits = await reopened.search("pricing", 1);
    expect(hits[0]?.content).toContain("pricing");
  });

  it("upserts (re-ingesting the same id replaces, not duplicates)", async () => {
    const engine = new PrivateEngine(memKV(), await generateOceanKey(), hashEmbedder());
    await engine.ingest([{ id: "1", sourceType: "note", sourceId: "a", content: "first" }]);
    await engine.ingest([{ id: "1", sourceType: "note", sourceId: "a", content: "second" }]);
    expect(await engine.count()).toBe(1);
    const hits = await engine.search("second", 1);
    expect(hits[0].content).toBe("second");
  });

  it("reindexes stored vectors when the model stamp is stale", async () => {
    const kv = memKV();
    const key = await generateOceanKey();
    const first = new PrivateEngine(kv, key, hashEmbedder());
    await first.ingest([{ id: "1", sourceType: "note", sourceId: "a", content: "Clerk authentication" }]);
    const later = new PrivateEngine(kv, key, { ...hashEmbedder(), model: "openai/text-embedding-3-small@384" });
    await later.reindexIfStale("openai/text-embedding-3-small@384");
    const hits = await later.search("authentication clerk", 1);
    expect(hits[0]?.content).toContain("Clerk");
  });

  it("reindex does not drop a concurrent ingest", async () => {
    const kv = memKV();
    const key = await generateOceanKey();
    const inner = hashEmbedder();
    const slow = {
      dim: inner.dim,
      model: "openai/text-embedding-3-small@384",
      async embed(texts: string[]) {
        await new Promise((r) => setTimeout(r, 40));
        return inner.embed(texts);
      },
    };
    await new PrivateEngine(kv, key, hashEmbedder()).ingest([
      { id: "1", sourceType: "note", sourceId: "a", content: "Clerk authentication" },
    ]);
    const later = new PrivateEngine(kv, key, slow);
    const reindex = later.reindexIfStale("openai/text-embedding-3-small@384");
    await new PrivateEngine(kv, key, { ...hashEmbedder(), model: "openai/text-embedding-3-small@384" }).ingest([
      { id: "2", sourceType: "note", sourceId: "b", content: "pgvector embeddings" },
    ]);
    await reindex;
    expect(await later.count()).toBe(2);
  });
});

import { describe, it, expect } from "vitest";
import { cosine, topK, mergeRanked } from "./vector";

describe("engine/vector", () => {
  it("cosine of identical vectors is 1", () => {
    expect(cosine([1, 2, 3], [1, 2, 3])).toBeCloseTo(1);
  });

  it("cosine of orthogonal vectors is 0", () => {
    expect(cosine([1, 0], [0, 1])).toBeCloseTo(0);
  });

  it("topK returns the k most similar, highest first", () => {
    const items = [
      { item: "a", vector: [1, 0] },
      { item: "b", vector: [0, 1] },
      { item: "c", vector: [0.9, 0.1] },
    ];
    expect(topK([1, 0], items, 2).map((x) => x.item)).toEqual(["a", "c"]);
  });

  it("mergeRanked dedupes by id, keeping the best score", () => {
    const l1 = [
      { item: { id: "x" }, score: 0.5 },
      { item: { id: "y" }, score: 0.9 },
    ];
    const l2 = [{ item: { id: "x" }, score: 0.8 }];
    const merged = mergeRanked([l1, l2], (t) => t.id, 10);
    expect(merged[0].item.id).toBe("y");
    expect(merged.find((s) => s.item.id === "x")?.score).toBe(0.8);
  });
});

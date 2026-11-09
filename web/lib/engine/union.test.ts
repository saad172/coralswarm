import { describe, it, expect } from "vitest";
import { assembleContext } from "./union";

describe("engine/union (union RAG)", () => {
  it("merges private + shared, ranked by score, tagging source", () => {
    const ctx = assembleContext(
      [
        { id: "p1", content: "private a", score: 0.9 },
        { id: "p2", content: "private b", score: 0.4 },
      ],
      [{ id: "s1", content: "shared a", score: 0.7 }],
    );
    expect(ctx.map((c) => c.id)).toEqual(["p1", "s1", "p2"]);
    expect(ctx[0].source).toBe("private");
    expect(ctx[1].source).toBe("shared");
  });

  it("dedupes by id, keeping the higher score", () => {
    const ctx = assembleContext(
      [{ id: "x", content: "from private", score: 0.6 }],
      [{ id: "x", content: "from shared", score: 0.8 }],
    );
    expect(ctx.length).toBe(1);
    expect(ctx[0].score).toBe(0.8);
  });

  it("caps to maxItems", () => {
    const many = Array.from({ length: 20 }, (_, i) => ({ id: `p${i}`, content: "c", score: i / 20 }));
    expect(assembleContext(many, [], 5).length).toBe(5);
  });
});

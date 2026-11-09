import { describe, it, expect } from "vitest";
import { parseSnippet } from "./snippet";

const STX = String.fromCharCode(2);
const ETX = String.fromCharCode(3);

describe("snippet/parseSnippet", () => {
  it("returns a single unmatched part for plain text", () => {
    expect(parseSnippet("hello world")).toEqual([{ text: "hello world", matched: false }]);
  });

  it("splits a single matched term", () => {
    const s = `we discussed the ${STX}roadmap${ETX} yesterday`;
    expect(parseSnippet(s)).toEqual([
      { text: "we discussed the ", matched: false },
      { text: "roadmap", matched: true },
      { text: " yesterday", matched: false },
    ]);
  });

  it("handles multiple matches", () => {
    const s = `${STX}Alice${ETX} met ${STX}Bob${ETX}`;
    expect(parseSnippet(s)).toEqual([
      { text: "Alice", matched: true },
      { text: " met ", matched: false },
      { text: "Bob", matched: true },
    ]);
  });

  it("degrades gracefully on an unclosed match", () => {
    const s = `intro ${STX}dangling match`;
    expect(parseSnippet(s)).toEqual([
      { text: "intro ", matched: false },
      { text: "dangling match", matched: true },
    ]);
  });

  it("returns nothing for an empty string", () => {
    expect(parseSnippet("")).toEqual([]);
  });

  it("never needs raw HTML — content that looks like markup stays plain text", () => {
    const s = `${STX}<script>alert(1)</script>${ETX}`;
    const parts = parseSnippet(s);
    expect(parts).toEqual([{ text: "<script>alert(1)</script>", matched: true }]);
  });
});

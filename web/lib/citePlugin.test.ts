import { describe, it, expect } from "vitest";
import type { InlineCode, Link, LinkReference } from "mdast";
import type { Node } from "unist";
import { unified } from "unified";
import remarkParse from "remark-parse";
import remarkGfm from "remark-gfm";
import { visit } from "unist-util-visit";
import { splitCiteMarkers, remarkCiteRefs, type CiteRefNode } from "./citePlugin";

/** Pull just the citation numbers out of a split result, in order — the
 *  detail most call sites care about. */
function citeNumbers(nodes: ReturnType<typeof splitCiteMarkers>): number[] {
  return (nodes ?? [])
    .filter((n): n is CiteRefNode => n.type === "citeRef")
    .map((n) => n.n);
}

/** Run markdown through the ACTUAL pipeline `AnswerMarkdown` uses (minus
 *  remark-rehype/react, which don't matter for asserting on the mdast shape):
 *  `remark-parse` (CommonMark, including link syntax) → `remark-gfm` (tables,
 *  etc.) → `remarkCiteRefs`. Unlike `splitCiteMarkers`'s unit tests above,
 *  which only ever see a citation marker that's already survived as literal
 *  text, this is what exercises the case where `remark-parse`'s link syntax
 *  runs first and swallows `[1](url)` into a real `link` node before
 *  `remarkCiteRefs` ever gets a look at it. */
function parseWithCitePlugin(markdown: string): Node {
  const processor = unified().use(remarkParse).use(remarkGfm).use(remarkCiteRefs);
  return processor.runSync(processor.parse(markdown));
}

/** Collect every node of a given mdast `type` in visit order — the detail
 *  the AST-level regression tests below care about. */
function collect<T extends Node = Node>(tree: Node, type: string): T[] {
  const nodes: T[] = [];
  visit(tree, type, (node) => {
    nodes.push(node as T);
  });
  return nodes;
}

describe("citePlugin/splitCiteMarkers", () => {
  it("returns undefined for text with no citation marker", () => {
    expect(splitCiteMarkers("plain prose, nothing to cite")).toBeUndefined();
  });

  it("returns undefined for an empty string", () => {
    expect(splitCiteMarkers("")).toBeUndefined();
  });

  it("splits a single marker mid-sentence", () => {
    const out = splitCiteMarkers("the roadmap shipped [3] last week");
    expect(out).toEqual([
      { type: "text", value: "the roadmap shipped " },
      { type: "citeRef", n: 3, children: [], data: { hName: "cite-chip", hProperties: { n: 3 } } },
      { type: "text", value: " last week" },
    ]);
  });

  it("splits multiple markers in order", () => {
    expect(citeNumbers(splitCiteMarkers("agreed [1] and confirmed [2] and [12] too"))).toEqual([
      1, 2, 12,
    ]);
  });

  it("handles a marker at the very start and end with no surrounding text", () => {
    const out = splitCiteMarkers("[1]");
    expect(out).toEqual([
      { type: "citeRef", n: 1, children: [], data: { hName: "cite-chip", hProperties: { n: 1 } } },
    ]);
  });

  it("handles back-to-back markers with no text between them", () => {
    expect(citeNumbers(splitCiteMarkers("sources agree [1][2]"))).toEqual([1, 2]);
  });

  it("does not treat a bracketed non-number as a citation (no digits → undefined, unchanged)", () => {
    expect(splitCiteMarkers("see [footnote] for detail")).toBeUndefined();
  });

  it("never drops the surrounding text", () => {
    const value = "before [42] after";
    const out = splitCiteMarkers(value);
    const rebuilt = (out ?? [])
      .map((n) => (n.type === "text" ? n.value : `[${(n as CiteRefNode).n}]`))
      .join("");
    expect(rebuilt).toBe(value);
  });
});

describe("remarkCiteRefs (full remark-parse + remark-gfm pipeline)", () => {
  // These run the real parser, not just the string splitter above — the
  // splitter only ever sees a `[n]` marker that already survived as literal
  // text, which is exactly what CommonMark link syntax prevents for the
  // `[1](url)` case below.

  it("reclaims a citation immediately followed by (...) as a chip, not a link", () => {
    const tree = parseWithCitePlugin("see [1](https://x.com) for details");
    const citeRefs = collect<CiteRefNode>(tree, "citeRef");
    const links = collect(tree, "link");
    expect(citeRefs.map((n) => n.n)).toEqual([1]);
    // The accidental URL must not leak through as a link anywhere in the tree.
    expect(links).toHaveLength(0);
  });

  it("leaves a citation-shaped marker inside inline code untouched", () => {
    const tree = parseWithCitePlugin("the array is `arr[1]` here");
    const citeRefs = collect<CiteRefNode>(tree, "citeRef");
    const inlineCode = collect<InlineCode>(tree, "inlineCode");
    expect(citeRefs).toHaveLength(0);
    expect(inlineCode.map((n) => n.value)).toEqual(["arr[1]"]);
  });

  it("preserves a real non-numeric link while reclaiming an adjacent bare citation", () => {
    const tree = parseWithCitePlugin("per [1] and [see docs](https://docs.example.com)");
    const citeRefs = collect<CiteRefNode>(tree, "citeRef");
    const links = collect<Link>(tree, "link");
    expect(citeRefs.map((n) => n.n)).toEqual([1]);
    expect(links).toHaveLength(1);
    expect(links[0].url).toBe("https://docs.example.com");
  });

  it("turns back-to-back citation markers into two chips (no link-reference definitions in doc)", () => {
    const tree = parseWithCitePlugin("see [1][2] for more");
    const citeRefs = collect<CiteRefNode>(tree, "citeRef");
    const links = collect<Link>(tree, "link");
    const linkReferences = collect<LinkReference>(tree, "linkReference");
    expect(citeRefs.map((n) => n.n)).toEqual([1, 2]);
    expect(links).toHaveLength(0);
    expect(linkReferences).toHaveLength(0);
  });

  it("reclaims a citation marker inside a GFM table cell", () => {
    const table = ["| Source | Note |", "| --- | --- |", "| foo | see [1] |"].join("\n");
    const tree = parseWithCitePlugin(table);
    const citeRefs = collect<CiteRefNode>(tree, "citeRef");
    expect(citeRefs.map((n) => n.n)).toEqual([1]);
  });
});

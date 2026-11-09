// Remark plugin that turns inline `[n]` citation markers in synthesized Ask
// answers into a dedicated mdast node type (`citeRef`), so `AnswerMarkdown`
// can render them as `.cite` chips (ADR-0011 §E20) *after* the rest of the
// prose has gone through full markdown parsing (headers, lists, tables,
// bold, etc.) — the same job the old flat-text `renderAnswer()` did with a
// single top-level `String.split(/(\[\d+\])/g)`, just done at the mdast
// level so it survives being nested inside a `<p>`/`<li>`/table cell.
//
// How this reaches the DOM without a custom `remark-rehype` handler: each
// `citeRef` node carries `data.hName`/`data.hProperties`. `mdast-util-to-hast`
// (which `remark-rehype`/`react-markdown` use under the hood) falls back to
// those fields for any node type it doesn't recognize — see "Unknown nodes"
// in mdast-util-to-hast's README. That turns each marker into a
// `<cite-chip n="3">` hast element, which `AnswerMarkdown` maps to the actual
// chip component via its `components` prop.

import type { Link, Root, Text } from "mdast";
import type { Data, Node } from "unist";
import type { Plugin } from "unified";
import { visit } from "unist-util-visit";

/** The custom mdast node type `remarkCiteRefs` produces for one `[n]` marker.
 *  Carries no markdown content of its own (`children: []`) — it's a pure
 *  hand-off to hast via `data.hName`/`data.hProperties`. */
export interface CiteRefNode extends Node {
  type: "citeRef";
  n: number;
  children: [];
  data: Data & {
    hName: "cite-chip";
    hProperties: { n: number };
  };
}

function makeCiteRefNode(n: number): CiteRefNode {
  return {
    type: "citeRef",
    n,
    children: [],
    data: { hName: "cite-chip", hProperties: { n } },
  };
}

/** The one source of truth for "this is a bare citation number" — no
 *  brackets, just digits. Reused below both to build the `[n]`-marker
 *  pattern for text nodes and to test a link's visible text when reclaiming
 *  an accidentally-parsed `[n](url)` link (see `bareCitationLinkNumber`). */
const CITATION_DIGITS_RE = /^\d+$/;

/** Matches one full `[n]` marker, e.g. for pulling `n` out of a split part. */
const CITE_MARKER_RE = new RegExp(`^\\[(${CITATION_DIGITS_RE.source.slice(1, -1)})\\]$`);

/** Matches `[n]` anywhere in a string — used only to check *presence* of a
 *  marker before bothering to split. Deliberately a separate regex (not
 *  reused from `CITE_MARKER_SPLIT_RE` below) because that one carries the
 *  `g` flag: a global-flagged `RegExp` is stateful (`lastIndex` persists
 *  across calls), which would make repeated `.test()` calls on the same
 *  module-level instance silently skip matches depending on call order —
 *  exactly the kind of bug this split avoids. */
const CITE_MARKER_TEST_RE = new RegExp(`\\[${CITATION_DIGITS_RE.source.slice(1, -1)}\\]`);

/** Matches `[n]` anywhere in a string, capturing the marker itself so
 *  `String.split` keeps it as a separate part alongside the surrounding
 *  plain-text parts. */
const CITE_MARKER_SPLIT_RE = new RegExp(
  `(\\[${CITATION_DIGITS_RE.source.slice(1, -1)}\\])`,
  "g",
);

/** Split one text node's raw value on `[n]` citation markers into a run of
 *  plain `text` nodes and `citeRef` nodes. Returns `undefined` when the value
 *  has no marker at all — lets the caller skip the splice entirely (the
 *  overwhelmingly common case: most text nodes carry no citation). Exported
 *  standalone so this can be unit-tested without going through a full remark
 *  pipeline. */
export function splitCiteMarkers(value: string): (Text | CiteRefNode)[] | undefined {
  if (!CITE_MARKER_TEST_RE.test(value)) return undefined;
  const parts = value.split(CITE_MARKER_SPLIT_RE);
  const nodes: (Text | CiteRefNode)[] = [];
  for (const part of parts) {
    if (part === "") continue;
    const m = CITE_MARKER_RE.exec(part);
    nodes.push(m ? makeCiteRefNode(Number(m[1])) : { type: "text", value: part });
  }
  return nodes;
}

/** `remark-parse`'s CommonMark link parsing runs during the initial parse,
 *  *before* `remarkCiteRefs` ever sees the tree — so `[1](https://x.com)`
 *  shaped input (which is now plausible in model output now that the answer
 *  is rendered as full markdown, not just flat text) is already a real mdast
 *  `link` node by the time we visit it, and the literal `[1]` text this
 *  function normally catches never exists. This reclaims that case: if a
 *  link's *entire* visible text is nothing but digits, it's treated as an
 *  accidental citation marker rather than an intentional hyperlink, and the
 *  (accidental, usually garbage) `url` is discarded. A link with any other
 *  visible content — non-numeric text, nested formatting, an image, etc. —
 *  is left completely alone, e.g. `[see source](url)`. */
function bareCitationLinkNumber(node: Link): number | undefined {
  let text = "";
  for (const child of node.children) {
    if (child.type !== "text") return undefined;
    text += child.value;
  }
  return CITATION_DIGITS_RE.test(text) ? Number(text) : undefined;
}

/** Remark plugin: rewrite inline `[n]` citation markers into `citeRef` mdast
 *  nodes, from two sources:
 *    - `text` nodes, the common case — markers that survived parsing as
 *      literal text (this leaves markers inside code spans/blocks, a
 *      different mdast node type, untouched verbatim — matches the "don't
 *      touch code" expectation a reader would have for citation-marker
 *      rewriting).
 *    - `link` nodes whose visible text is a bare citation number, the
 *      `[1](url)` edge case `remark-parse`'s link syntax swallows before this
 *      plugin ever runs (see `bareCitationLinkNumber` above). */
export const remarkCiteRefs: Plugin<[], Root> = () => (tree) => {
  visit(tree, ["text", "link"], (node, index, parent) => {
    if (!parent || index === undefined) return;
    if (node.type === "text") {
      const replacement = splitCiteMarkers((node as Text).value);
      if (!replacement) return;
      // `parent.children` is typed per-parent (PhrasingContent[], etc.) and
      // doesn't know about our custom `citeRef` node — this cast is the
      // standard escape hatch for introducing a custom mdast node type
      // funneled through `data.hName` (see module doc above).
      (parent.children as unknown as Node[]).splice(index, 1, ...replacement);
      // Continue traversal after the spliced-in nodes rather than
      // re-visiting them (they're already fully split; `citeRef` nodes have
      // no text child to descend into anyway).
      return index + replacement.length;
    }
    const n = bareCitationLinkNumber(node as Link);
    if (n === undefined) return;
    (parent.children as unknown as Node[]).splice(index, 1, makeCiteRefNode(n));
    return index + 1;
  });
};

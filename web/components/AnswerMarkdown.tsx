"use client";

import {
  createContext,
  memo,
  useCallback,
  useContext,
  useMemo,
  useRef,
  type ComponentType,
} from "react";
import Link from "next/link";
import ReactMarkdown, { type Components } from "react-markdown";
import remarkGfm from "remark-gfm";
import { remarkCiteRefs } from "@/lib/citePlugin";
import { JEV_VERDICT_META, verdictNeedsAttention, type JevVerdict } from "@/lib/graphSurfaces";

/** Extra props a `<cite-chip n="3">` hast element (produced by
 *  `remarkCiteRefs`, see `lib/citePlugin.ts`) actually carries at render
 *  time — just the citation number. */
interface CiteChipHastProps {
  n?: number;
}

/** `remarkGfm`/`remarkCiteRefs` never change identity across renders (module
 *  singletons), so this array is hoisted out of the component instead of
 *  being a fresh `[remarkGfm, remarkCiteRefs]` literal recreated on every
 *  render — `react-markdown` re-derives its whole `unified` processor
 *  whenever `remarkPlugins` looks like a new array (see
 *  `createProcessor`/`unified().use(remarkPlugins)` in `react-markdown`'s
 *  source), so a stable reference here avoids rebuilding the processor for
 *  no reason. */
const REMARK_PLUGINS = [remarkGfm, remarkCiteRefs];

/** Currently-hovered citation number (or `null`), threaded to `CiteChip`
 *  instances via context rather than as a prop baked into the memoized
 *  `components` map (see `AnswerMarkdown` below). Context updates reach
 *  consumers even through a `React.memo` bailout higher up the tree, so a
 *  hover change re-renders *only* the `CiteChip`(s) whose highlight state
 *  actually changed, not the whole answer. */
const HotCiteContext = createContext<number | null>(null);
const CiteCallbackAvailableContext = createContext(false);
const CiteHrefContext = createContext<((n: number) => string | undefined) | undefined>(undefined);
const CiteVerdictContext = createContext<((n: number) => JevVerdict | null | undefined) | undefined>(undefined);

/** One `[n]` citation chip. Reads the live hover state from
 *  `HotCiteContext` (not a prop) so its identity/behavior never has to flow
 *  back through `AnswerMarkdown`'s memoized `components` map — see the
 *  perf note on `AnswerMarkdown` for why that separation matters. */
function CiteChip({
  n,
  setHot,
  onCite,
}: CiteChipHastProps & {
  setHot: (n: number | null) => void;
  onCite?: (n: number) => void | boolean;
}) {
  const hot = useContext(HotCiteContext);
  const citeHref = useContext(CiteHrefContext);
  const canInvokeCite = useContext(CiteCallbackAvailableContext);
  const citeVerdict = useContext(CiteVerdictContext);
  return <AnswerCitation n={n} href={n == null ? undefined : citeHref?.(n)} hot={hot} setHot={setHot} onCite={canInvokeCite ? onCite : undefined} verdict={n == null ? undefined : citeVerdict?.(n)} />;
}

/** Shared citation control for prose and structured contradiction positions. */
export function AnswerCitation({ n, href, hot, setHot, onCite, verdict }: {
  n?: number;
  href?: string;
  hot: number | null;
  setHot: (n: number | null) => void;
  onCite?: (n: number) => void | boolean;
  /** Jev's check of this cite. Flagged verdicts add a glyph after the number;
   *  every verdict is spoken in the accessible name. */
  verdict?: JevVerdict | null;
}) {
  if (n == null || !Number.isFinite(n)) return null;
  const meta = verdict ? JEV_VERDICT_META[verdict] : null;
  const spoken = meta ? `, Jev: ${meta.label}` : "";
  const flag = verdict && verdictNeedsAttention(verdict) && meta
    ? <span className={`cite-flag verdict-${meta.tone}`} aria-hidden="true">{meta.glyph}</span>
    : null;
  const title = meta ? `Jev: ${meta.label}` : undefined;
  if (href) return (
    <Link href={href} aria-label={`Open source ${n}${spoken}`} title={title} className={`cite${hot === n ? " hot" : ""}`} onMouseEnter={() => setHot(n)} onMouseLeave={() => setHot(null)} onClick={(event) => {
      if (!onCite || event.button !== 0 || event.metaKey || event.ctrlKey || event.shiftKey || event.altKey) return;
      // Keep the native URL for modifier-click, copy-link and legacy citations
      // whose answer has no excerpt to open in the evidence drawer yet.
      if (onCite(n) !== false) event.preventDefault();
    }}>{n}{flag}</Link>
  );
  if (!onCite) return <span className="cite" title={title ?? "Source details are not available yet"}>{n}{flag}</span>;
  return (
    <span
      className={`cite${hot === n ? " hot" : ""}`}
      role="button"
      tabIndex={0}
      aria-label={meta ? `Show source ${n}${spoken}` : undefined}
      title={title}
      onMouseEnter={() => setHot(n)}
      onMouseLeave={() => setHot(null)}
      onClick={() => onCite(n)}
      onKeyDown={(e) => {
        if (e.key === "Enter" || e.key === " ") {
          if (e.key === " ") e.preventDefault();
          onCite(n);
        }
      }}
    >
      {n}
      {flag}
    </span>
  );
}

/** Markdown component overrides — modest, chat-bubble-appropriate styling
 *  using only design-system tokens (no ad-hoc colors/spacing scales).
 *  Headings are deliberately capped to the same weight/size as the prompt's
 *  preferred **bold lead-ins** (a stray `#`/`##` from the model shouldn't
 *  blow out a small answer bubble into looking like a document). List
 *  markers are restored explicitly because Tailwind's preflight strips the
 *  browser default `list-style`. Tables get visible cell borders/padding —
 *  this is the "structured data" surface for now, no chart library involved.
 *
 *  Each override intentionally reads only `children` off the incoming props
 *  (dropping the hast `node` extra-prop and friends) rather than spreading
 *  the full prop bag onto the DOM element, since we fully own the className
 *  here anyway. */
const MARKDOWN_COMPONENTS: Components = {
  // Model/source Markdown must not mint automatic network requests containing
  // private URL/query content. Retained image references stay inert text.
  img: ({ alt }) => <span className="text-ink-3">[Image omitted{alt ? `: ${alt}` : ""}]</span>,
  p: ({ children }) => <p className="mb-3 last:mb-0">{children}</p>,
  strong: ({ children }) => <strong className="font-semibold text-ink">{children}</strong>,
  h1: ({ children }) => (
    <p className="mb-2 mt-4 text-[15px] font-semibold text-ink first:mt-0">{children}</p>
  ),
  h2: ({ children }) => (
    <p className="mb-2 mt-4 text-[15px] font-semibold text-ink first:mt-0">{children}</p>
  ),
  h3: ({ children }) => (
    <p className="mb-2 mt-4 text-[15px] font-semibold text-ink first:mt-0">{children}</p>
  ),
  h4: ({ children }) => (
    <p className="mb-2 mt-4 text-[15px] font-semibold text-ink first:mt-0">{children}</p>
  ),
  h5: ({ children }) => (
    <p className="mb-2 mt-4 text-[15px] font-semibold text-ink first:mt-0">{children}</p>
  ),
  h6: ({ children }) => (
    <p className="mb-2 mt-4 text-[15px] font-semibold text-ink first:mt-0">{children}</p>
  ),
  ul: ({ children }) => (
    <ul className="mb-3 ml-5 list-disc space-y-1 last:mb-0">{children}</ul>
  ),
  ol: ({ children }) => (
    <ol className="mb-3 ml-5 list-decimal space-y-1 last:mb-0">{children}</ol>
  ),
  li: ({ children }) => <li className="leading-[1.6] marker:text-ink-3">{children}</li>,
  a: ({ href, children }) => href ? (
    <a
      href={href}
      target="_blank"
      rel="noreferrer noopener"
      className="text-brand-2 underline underline-offset-2 hover:text-brand"
    >
      {children}
    </a>
  ) : <span>{children}</span>,
  blockquote: ({ children }) => (
    <blockquote className="mb-3 border-l-2 border-line pl-3 italic text-ink-2 last:mb-0">
      {children}
    </blockquote>
  ),
  hr: () => <hr className="my-3 border-t border-line" />,
  // Fenced code blocks render as <pre><code>; inline spans as bare <code>.
  // Both get the mono token; the block wrapper additionally gets the
  // bg-2/border/scroll treatment. The nested <code> inside <pre> picking up
  // the same background too is an accepted, low-stakes overlap (same color,
  // no visible seam) in exchange for not needing to detect block vs. inline.
  pre: ({ children }) => (
    <pre className="mb-3 overflow-x-auto rounded-row border border-line bg-bg-2 px-3 py-[10px] font-mono text-[12.5px] leading-[1.5] text-ink last:mb-0">
      {children}
    </pre>
  ),
  code: ({ children }) => (
    <code className="rounded-badge bg-bg-2 px-[4px] py-[1px] font-mono text-[0.9em] text-ink">
      {children}
    </code>
  ),
  table: ({ children }) => (
    <div className="mb-3 overflow-x-auto rounded-row border border-line last:mb-0">
      <table className="w-full border-collapse text-[13.5px]">{children}</table>
    </div>
  ),
  thead: ({ children }) => <thead className="bg-bg-2">{children}</thead>,
  tr: ({ children }) => <tr className="border-b border-line last:border-b-0">{children}</tr>,
  th: ({ children }) => (
    <th className="border-r border-line px-3 py-[6px] text-left font-semibold text-ink last:border-r-0">
      {children}
    </th>
  ),
  td: ({ children }) => (
    <td className="border-r border-line px-3 py-[6px] align-top text-ink-2 last:border-r-0">
      {children}
    </td>
  ),
};

/** `<ReactMarkdown>` itself does zero internal memoization — it's a plain
 *  function component that reparses `children` into mdast/hast and rebuilds
 *  the whole React tree from scratch on *every* call, regardless of whether
 *  `children`/`components`/`remarkPlugins` are referentially unchanged (see
 *  `Markdown()` in `react-markdown`'s source: no `useMemo`/`memo` in sight).
 *  So keeping `components`'s identity stable only helps if something else
 *  also stops React from *calling* it again in the first place — that's
 *  this wrapper's whole job. As long as `text` and `components` are
 *  reference-stable across a re-render (see `AnswerMarkdown` below for how),
 *  `React.memo` bails out here and the expensive remark→rehype→jsx pass
 *  never runs, even though its caller re-rendered. */
const MemoizedAnswerBody = memo(function MemoizedAnswerBody({
  text,
  components,
}: {
  text: string;
  components: Components;
}) {
  return (
    <ReactMarkdown skipHtml remarkPlugins={REMARK_PLUGINS} components={components}>
      {text}
    </ReactMarkdown>
  );
});

/** Renders a synthesized Ask answer as markdown (headers, bullets, bold,
 *  tables — whatever `synthesis_stream_v1.txt` produces), turning inline
 *  `[n]` citation markers into the same `.cite` chips `renderAnswer()` used
 *  to render, now working correctly even when a marker lands inside a list
 *  item or table cell rather than only at the top level (ADR-0011 §E20).
 *
 *  The citation-chip problem: `react-markdown` renders parsed markdown, so
 *  `[n]` markers are just prose inside whatever block react-markdown
 *  produces — there's no single flat string left to `.split()` on like the
 *  old `renderAnswer()` did. `remarkCiteRefs` (see `lib/citePlugin.ts`)
 *  solves this one level down, at the mdast (markdown AST) stage: it walks
 *  every text node *before* the tree is handed to react-markdown's renderer
 *  and rewrites `[n]` markers into a dedicated `citeRef` node carrying
 *  `data.hName: "cite-chip"`. `remark-rehype`'s default "unknown node"
 *  fallback (see `mdast-util-to-hast`) turns that into a real
 *  `<cite-chip n="3">` hast element, and the `components["cite-chip"]`
 *  override below renders it as the chip — no custom `remark-rehype` handler
 *  needed, and citations now survive markdown parsing anywhere they land.
 *
 *  Perf note (the reason for the `HotCiteContext`/`MemoizedAnswerBody`
 *  dance): `hot` changes on every `mouseenter`/`mouseleave` over *any* chip
 *  (ours or the matching source-list row's), which previously fed straight
 *  into the `components` map's `useMemo` deps — busting its identity, and
 *  with it forcing `MemoizedAnswerBody`/`ReactMarkdown` to redo a full
 *  remark→rehype→jsx pass over the *entire* answer on every hover, not just
 *  update the one hovered chip. Fixed by never letting `hot` (or the
 *  parent's non-memoized `setHot`/`onCite` closures) reach the `components`
 *  map's dependency list at all:
 *    - `hot` is threaded to `CiteChip` instances via `HotCiteContext`
 *      instead. Context updates reach consumers even through a `React.memo`
 *      bailout higher up, so a hover change re-renders only the chip(s)
 *      whose highlight actually flipped.
 *    - `setHot`/`onCite` are wrapped in refs updated on every render and
 *      exposed through stable (empty-deps) callback wrappers, so `Turn`
 *      recreating its inline `onCite` closure on every hover-driven
 *      re-render no longer busts `components`'s memoization either. */
export function AnswerMarkdown({
  text,
  hot,
  setHot,
  onCite,
  citeHref,
  citeVerdict,
}: {
  text: string;
  citeHref?: (n: number) => string | undefined;
  /** Jev verdict per citation number (scoped Ask, #1353). */
  citeVerdict?: (n: number) => JevVerdict | null | undefined;
  hot: number | null;
  setHot: (n: number | null) => void;
  onCite?: (n: number) => void | boolean;
}) {
  const setHotRef = useRef(setHot);
  setHotRef.current = setHot;
  const onCiteRef = useRef(onCite);
  onCiteRef.current = onCite;

  // Stable (never-changing) wrappers that always dispatch to whatever
  // `setHot`/`onCite` the latest render passed in, via the refs above —
  // this is what lets `components` below have a dependency array that
  // never invalidates on hover.
  const stableSetHot = useCallback((n: number | null) => setHotRef.current(n), []);
  const stableOnCite = useCallback((n: number) => onCiteRef.current?.(n), []);

  const components = useMemo<Components>(() => {
    const citeChip: ComponentType<CiteChipHastProps> = (props) => (
      <CiteChip {...props} setHot={stableSetHot} onCite={stableOnCite} />
    );
    return {
      ...MARKDOWN_COMPONENTS,
      // `cite-chip` isn't a real HTML tag, so it isn't a key `Components`
      // (typed as `{ [K in keyof JSX.IntrinsicElements]?: … }`) knows about.
      // This is the standard escape hatch for a custom hast element produced
      // via `data.hName` rather than a handler — see `citePlugin.ts`.
      ...({ "cite-chip": citeChip } as unknown as Components),
    };
  }, [stableSetHot, stableOnCite]);

  return (
    <CiteCallbackAvailableContext.Provider value={!!onCite}>
    <CiteHrefContext.Provider value={citeHref}>
    <CiteVerdictContext.Provider value={citeVerdict}>
    <HotCiteContext.Provider value={hot}>
      <MemoizedAnswerBody text={text} components={components} />
    </HotCiteContext.Provider>
    </CiteVerdictContext.Provider>
    </CiteHrefContext.Provider>
    </CiteCallbackAvailableContext.Provider>
  );
}

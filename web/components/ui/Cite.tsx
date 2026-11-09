import Link from "next/link";
import type { ReactNode } from "react";
import { cn } from "./cn";
import { Dot } from "./Badge";
import type { Hue } from "./hue";

/* Citations.

   Every answer cites its sources inline — this is the product's core claim, so
   the citation chip and the source tile are first-class kit, not page-local
   markup. The chip's visual contract (coral wash, brand-2 mono numeral, 5px
   radius, raised 2px off the baseline) is also encoded as `.cite` in
   globals.css, because lib/citePlugin.ts injects citations into rendered
   markdown where it can't mount a React component. Keep the two in step. */

export function CiteChip({
  n,
  hot = false,
  onClick,
  href,
  className,
}: {
  n: number | string;
  hot?: boolean;
  onClick?: () => void;
  href?: string;
  className?: string;
}) {
  const shared = cn(
    "inline-flex min-w-[17px] items-center justify-center rounded-[5px] px-[5px] font-mono text-[10px] transition-colors",
    hot ? "bg-brand text-white" : "bg-brand-soft text-brand-2",
    className,
  );
  const style = { height: 17, margin: "0 1px", verticalAlign: 2 } as const;
  if (href) {
    return <Link href={href} aria-label={`Open source ${n}`} className={cn(shared, "hover:bg-brand hover:text-white focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-brand/40")} style={style}>{n}</Link>;
  }
  if (!onClick) {
    return (
      <span className={shared} style={style}>
        {n}
      </span>
    );
  }
  return (
    <button
      type="button"
      onClick={onClick}
      aria-label={`Show source ${n}`}
      className={cn(shared, "hover:bg-brand hover:text-white focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-brand/40")}
      style={style}
    >
      {n}
    </button>
  );
}

/** Source tile under an answer — numbered chip, title line, quoted excerpt,
 *  hue-dotted provenance. The mocks lay four of these across a grid.
 *
 *  Title-first (founder requirement: every preview row shows a title as its
 *  primary line, always): `title` is the single-line headline; `quote` is an
 *  optional secondary excerpt clamped to 2 lines, omitted entirely when
 *  there's nothing to show rather than rendering an empty line. */
export function SourceCard({
  n,
  title,
  quote,
  meta,
  hue,
  onClick,
  href,
  className,
}: {
  n: number | string;
  title: ReactNode;
  /** Omit (or pass falsy) when there's no excerpt — the secondary line is
   *  skipped rather than rendered empty. */
  quote?: ReactNode;
  meta: ReactNode;
  hue: Hue | string;
  onClick?: () => void;
  href?: string;
  className?: string;
}) {
  const inner = (
    <>
      <CiteChip n={n} />
      <span className="min-w-0 flex-1">
        <span className="block truncate text-[12.5px] leading-[1.45] text-ink">{title}</span>
        {quote && (
          <span className="mt-[2px] block line-clamp-2 text-[11.5px] leading-[1.45] text-ink-2">{quote}</span>
        )}
        <span className="mt-[4px] flex items-center gap-[6px] font-mono text-[10px] text-ink-3">
          <Dot hue={hue} size={5} />
          <span className="truncate">{meta}</span>
        </span>
      </span>
    </>
  );
  const shared = cn(
    "flex gap-[9px] rounded-control border border-line-2 bg-bg-subtle px-[11px] py-[10px] text-left",
    className,
  );
  const interactive = cn(shared, "transition-colors hover:border-line hover:bg-bg focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-brand/40");
  if (href) return <Link href={href} className={interactive}>{inner}</Link>;
  if (!onClick) return <div className={shared}>{inner}</div>;
  return (
    <button
      type="button"
      onClick={onClick}
      className={cn(shared, "transition-colors hover:border-line hover:bg-bg focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-brand/40")}
    >
      {inner}
    </button>
  );
}

/** The mono provenance line above an answer — "answered from 4 sources · 2.3s".
 *  Pairs the mandala with the attribution so the two never drift apart. */
export function AnswerMeta({ children, mark }: { children: ReactNode; mark?: ReactNode }) {
  return (
    <div className="flex items-center gap-[8px]">
      {mark}
      <span className="font-mono text-[10.5px] uppercase tracking-[0.08em] text-ink-3">{children}</span>
    </div>
  );
}

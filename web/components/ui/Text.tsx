import type { CSSProperties, ReactNode } from "react";
import { cn } from "./cn";

/* Typography roles.
   Display = Instrument Serif, UI/body = Geist, mono = JetBrains Mono. The mono
   eyebrow (uppercase, .13em tracking, 9.5–11px) is the redesign's most
   recognisable text mark — it labels sections, provenance and counts. */

/** Mono uppercase micro-label. Section eyebrows, provenance, column heads. */
export function Eyebrow({
  children,
  className,
  as: As = "div",
  style,
}: {
  children: ReactNode;
  className?: string;
  as?: "div" | "span" | "h2" | "h3";
  style?: CSSProperties;
}) {
  return (
    <As className={cn("font-mono text-[9.5px] uppercase tracking-[0.13em] text-ink-3", className)} style={style}>
      {children}
    </As>
  );
}

/** Mono count / shortcut / id. Not uppercase — these are values, not labels. */
export function Mono({ children, className }: { children: ReactNode; className?: string }) {
  return <span className={cn("font-mono text-[10px] text-ink-4", className)}>{children}</span>;
}

/** The house accent: a word of a heading in brand italic. `PageTitle`'s
 *  `italicWord` draws it; pass it as a node when the word could also occur
 *  earlier in the title (a first name like "Go" in "Good morning, Go"). */
export function TitleAccent({ children }: { children: ReactNode }) {
  return <em className="italic text-brand">{children}</em>;
}

/** Page title — 27px display serif, per the redesign's page-title role.
 *  `italicWord` sets the first occurrence of one word of a string title in
 *  brand italic, the house accent. */
export function PageTitle({
  children,
  italicWord,
  className,
}: {
  children: ReactNode;
  italicWord?: string;
  className?: string;
}) {
  const text = typeof children === "string" ? children : null;
  const i = text !== null && italicWord ? text.indexOf(italicWord) : -1;
  return (
    <h1
      className={cn(
        "font-display text-[27px] font-normal leading-[1.25] tracking-[-0.01em] text-ink",
        className,
      )}
    >
      {text === null || i === -1 ? (
        children
      ) : (
        <>
          {text.slice(0, i)}
          <TitleAccent>{italicWord}</TitleAccent>
          {text.slice(i + italicWord!.length)}
        </>
      )}
    </h1>
  );
}

/** Page header block — title, optional lead paragraph, optional right-hand
 *  actions. The standard top of a content column. */
export function PageHead({
  title,
  italicWord,
  action,
  children,
}: {
  title: string;
  italicWord?: string;
  action?: ReactNode;
  children?: ReactNode;
}) {
  return (
    <div className="mb-[22px] flex items-start gap-4">
      <div className="min-w-0 flex-1">
        <PageTitle italicWord={italicWord}>{title}</PageTitle>
        {children && (
          <p className="mt-[6px] max-w-[660px] text-[13.5px] leading-[1.6] text-ink-2">{children}</p>
        )}
      </div>
      {action && <div className="flex flex-none items-center gap-[6px] pt-1">{action}</div>}
    </div>
  );
}

/** Section header inside a page — eyebrow above a compact serif title. */
export function SectionHead({
  eyebrow,
  title,
  action,
  children,
}: {
  eyebrow?: string;
  title: ReactNode;
  action?: ReactNode;
  children?: ReactNode;
}) {
  return (
    <div className="mb-[12px] flex items-end gap-3">
      <div className="min-w-0 flex-1">
        {eyebrow && <Eyebrow className="mb-[6px]">{eyebrow}</Eyebrow>}
        <h2 className="text-[15.5px] font-medium leading-[1.35] text-ink">{title}</h2>
        {children && <p className="mt-[4px] text-[12.5px] leading-[1.55] text-ink-2">{children}</p>}
      </div>
      {action && <div className="flex flex-none items-center gap-[6px]">{action}</div>}
    </div>
  );
}

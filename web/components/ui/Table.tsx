import Link from "next/link";
import type { ReactNode } from "react";
import { cn } from "./cn";
import { Eyebrow } from "./Text";

/* Dense list rows.
   The redesign's tables are borderless: a mono eyebrow header, then rows
   separated by the faintest rule (`line-3`), 12.5px text, hover fill. Built as
   flex rows rather than <table> because every list in the mocks mixes a dot, a
   truncating title, inline metadata and a trailing count — a grid that a real
   <table> would fight. Where the content IS tabular (the permission matrix),
   use TableGrid instead, which keeps semantic table markup. */

export function ListHead({ children, className }: { children: ReactNode; className?: string }) {
  return (
    <div className={cn("flex items-center gap-[10px] border-b border-line-2 px-[12px] pb-[7px]", className)}>
      {children}
    </div>
  );
}

export function ListHeadCell({
  children,
  width,
  className,
}: {
  children: ReactNode;
  width?: number | string;
  className?: string;
}) {
  const size = width == null ? undefined : typeof width === "number" ? `${width}px` : width;
  return (
    <Eyebrow
      className={cn(size ? "flex-none overflow-hidden whitespace-nowrap" : "min-w-0 flex-1 whitespace-nowrap", className)}
      style={size ? { width: size, flex: `0 0 ${size}` } : undefined}
    >
      {children}
    </Eyebrow>
  );
}

/** One row of a bordered list. `href` makes it a link (navigation is an
 *  anchor, so ⌘-click and middle-click work); `onClick` makes it a button.
 *  `roomy` is the launchpad density (Home's Continue and What's new): 11/14px
 *  padding and a 12px gap instead of 9/12px and 10px. */
export function ListRow({
  children,
  onClick,
  href,
  active = false,
  roomy = false,
  className,
}: {
  children: ReactNode;
  onClick?: () => void;
  href?: string;
  active?: boolean;
  roomy?: boolean;
  className?: string;
}) {
  const shared = cn(
    "flex w-full items-center border-b border-line-3 text-left text-[12.5px]",
    roomy ? "gap-[12px] px-[14px] py-[11px]" : "gap-[10px] px-[12px] py-[9px]",
    "transition-colors last:border-b-0",
    active ? "bg-bg-hover" : "hover:bg-bg-subtle",
    className,
  );
  const focus = "focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-inset focus-visible:ring-brand/40";
  if (href) {
    return (
      <Link href={href} className={cn(shared, focus)}>
        {children}
      </Link>
    );
  }
  if (!onClick) return <div className={shared}>{children}</div>;
  return (
    <button type="button" onClick={onClick} className={cn(shared, focus)}>
      {children}
    </button>
  );
}

/** Truncating primary cell — the title column of nearly every row. */
export function RowTitle({ children, meta, className }: { children: ReactNode; meta?: ReactNode; className?: string }) {
  return (
    <span className={cn("min-w-0 flex-1", className)}>
      <span className="block truncate text-[12.5px] text-ink">{children}</span>
      {meta && (
        <span className="mt-[3px] flex items-center gap-[6px] font-mono text-[10px] text-ink-3">{meta}</span>
      )}
    </span>
  );
}

/** Right-aligned mono value — counts, dates, ids. */
export function RowMeta({ children, className }: { children: ReactNode; className?: string }) {
  return <span className={cn("flex-none font-mono text-[10px] text-ink-4", className)}>{children}</span>;
}

/** Semantic table for genuinely tabular data (the 3b permission matrix).
 *  Scrolls inside its own container so a wide matrix never scrolls the page. */
export function TableGrid({
  head,
  children,
  className,
}: {
  head: ReactNode;
  children: ReactNode;
  className?: string;
}) {
  return (
    <div className={cn("overflow-x-auto", className)}>
      <table className="w-full border-collapse text-[12.5px]">
        <thead>{head}</thead>
        <tbody>{children}</tbody>
      </table>
    </div>
  );
}

export function Th({ children, className }: { children: ReactNode; className?: string }) {
  return (
    <th
      scope="col"
      className={cn(
        "border-b border-line-2 px-[12px] pb-[7px] text-left font-mono text-[9.5px] font-normal uppercase tracking-[0.13em] text-ink-3",
        className,
      )}
    >
      {children}
    </th>
  );
}

export function Td({ children, className }: { children: ReactNode; className?: string }) {
  return (
    <td className={cn("border-b border-line-3 px-[12px] py-[9px] align-middle text-ink-2", className)}>
      {children}
    </td>
  );
}

import type { HTMLAttributes, ReactNode } from "react";
import { cn } from "./cn";

/* Surfaces.
   The redesign separates with 1px borders and uses shadow only to lift. A card
   is a white surface on the grey canvas; `quiet` is the near-white variant used
   for source tiles and alternating rows. */

export interface CardProps extends HTMLAttributes<HTMLDivElement> {
  /** Near-white fill and a fainter border — quiet cards, source tiles. */
  quiet?: boolean;
  /** Drop the default padding for cards that own their internal layout
   *  (tables, lists with their own row padding). */
  flush?: boolean;
  /** Ink border instead of the line border: "this one", e.g. the connector
   *  card the onboarding flow recommends. Wins over `quiet`'s border. */
  emphasis?: boolean;
}

export function Card({ quiet = false, flush = false, emphasis = false, className, children, ...rest }: CardProps) {
  return (
    <div
      className={cn(
        "rounded-card border",
        quiet ? "bg-bg-subtle" : "bg-bg shadow-card",
        emphasis ? "border-ink" : quiet ? "border-line-2" : "border-line",
        !flush && "p-[14px]",
        className,
      )}
      {...rest}
    >
      {children}
    </div>
  );
}

/** Card header rule — the 1px divider under a card's title row. */
export function CardHead({
  title,
  action,
  className,
  children,
}: {
  title: ReactNode;
  action?: ReactNode;
  className?: string;
  children?: ReactNode;
}) {
  return (
    <div className={cn("flex items-center gap-3 border-b border-line-2 px-[14px] py-[11px]", className)}>
      <div className="min-w-0 flex-1">
        <div className="truncate text-[13.5px] font-medium text-ink">{title}</div>
        {children && <div className="mt-[3px] text-[12.5px] leading-[1.5] text-ink-2">{children}</div>}
      </div>
      {action && <div className="flex flex-none items-center gap-[6px]">{action}</div>}
    </div>
  );
}

/** Inset panel — the grey recess used for sub-sections inside a white card. */
export function Panel({ className, children, ...rest }: HTMLAttributes<HTMLDivElement>) {
  return (
    <div className={cn("rounded-control border border-line-2 bg-bg-2 p-[12px]", className)} {...rest}>
      {children}
    </div>
  );
}

/** Hairline rule. `soft` uses the faintest weight, for inside a card. */
export function Divider({ soft = false, className }: { soft?: boolean; className?: string }) {
  return <div className={cn("h-px w-full", soft ? "bg-line-3" : "bg-line-2", className)} />;
}

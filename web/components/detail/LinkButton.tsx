import Link from "next/link";
import type { ReactNode } from "react";
import { cn } from "@/components/ui";

/* The kit's `Button` (components/ui/Button.tsx) renders a `<button>`, but a
   handful of detail-page actions — "Ask about this", "Open reef to rename" —
   are genuinely navigation, not a click handler with side effects. This
   mirrors Button's `secondary`/`sm` look (rounded-row, 1px line border, brand
   hover) so a link action and a button action read identically, without
   nesting a <button> inside an <a>. */
export const LINK_BUTTON_CLASS =
  "inline-flex items-center gap-[5px] rounded-row border border-line bg-bg px-[10px] py-[5px] text-[12.5px] text-ink-2 transition-colors hover:border-brand hover:text-brand-2";

export function LinkButton({
  href,
  children,
  className,
  title,
}: {
  href: string;
  children: ReactNode;
  className?: string;
  title?: string;
}) {
  return (
    <Link href={href} title={title} className={cn(LINK_BUTTON_CLASS, className)}>
      {children}
    </Link>
  );
}

"use client";

import Link from "next/link";
import type { ReactNode } from "react";
import { cn } from "@/components/ui";

/* Shared "how did I get here" row for the standalone detail routes (coral,
   coding session, meeting summary) that sit outside the main places
   (Home, Chats, Today, Workspaces) — `?id=`/`?source_id=`-addressed pages rather than real `[id]`
   segments.
   Mirrors TopicHeader's breadcrumb + trailing-actions row
   (components/topic/TopicHeader.tsx) so every detail page reads as one
   family instead of four independent inventions — and so none of them are
   dead ends back to where they came from. */

export interface Crumb {
  label: string;
  /** Omit on the last (current-page) crumb — it renders as plain text. */
  href?: string;
}

export function DetailBreadcrumb({
  crumbs,
  actions,
  className,
}: {
  crumbs: Crumb[];
  actions?: ReactNode;
  className?: string;
}) {
  return (
    <div className={cn("mb-[18px] flex flex-wrap items-center gap-[8px]", className)}>
      {crumbs.map((c, i) => (
        <span key={i} className="flex items-center gap-[8px]">
          {i > 0 && (
            <span aria-hidden className="text-ink-4">
              /
            </span>
          )}
          {c.href ? (
            <Link href={c.href} className="text-[13px] text-ink-3 transition-colors hover:text-ink">
              {c.label}
            </Link>
          ) : (
            <span className="truncate text-[14px] font-medium text-ink">{c.label}</span>
          )}
        </span>
      ))}
      {actions && <div className="ml-auto flex flex-none items-center gap-[6px]">{actions}</div>}
    </div>
  );
}

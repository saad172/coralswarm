"use client";

import type { ReactNode } from "react";
import { cn, Wordmark } from "@/components/ui";

// StepRail moved to the kit; re-exported so onboarding imports are unchanged.
export { StepRail, type StepState } from "@/components/ui";

/* Shared shell for every turn 5 / turn 6 screen: the full-bleed reef-bg
 * canvas + the 56px marketing/onboarding header (the app's own header is
 * 52px — this is deliberately not that, per the handoff). Wordmark sits
 * fixed top-left; `right` is whatever the screen needs there (a step rail,
 * a host chip, a plain email, a status badge — every 5×/6× screen puts
 * something different there, so it's a slot, not a fixed anatomy). */
export function OnboardingChrome({
  right,
  children,
}: {
  right?: ReactNode;
  children: ReactNode;
}) {
  return (
    <div className="reef-bg relative min-h-screen bg-bg-2">
      <div className="fixed inset-x-0 top-0 z-10 flex h-[56px] items-center gap-[18px] border-b border-line bg-bg-2/[.84] px-[22px] backdrop-blur-sm">
        <Wordmark size={17} />
        {right && <div className="ml-auto flex items-center gap-[18px]">{right}</div>}
      </div>
      <div className="grid min-h-screen place-items-center px-6 pb-12 pt-[80px]">{children}</div>
    </div>
  );
}

/** Small pill used for the "in your browser" / host badges
 *  in the header's right slot — outline by default, `tone="ok"` for the
 *  green org-host / active-service reading. */
export function HeaderPill({
  children,
  icon,
  tone = "neutral",
  className,
}: {
  children: ReactNode;
  icon?: ReactNode;
  tone?: "neutral" | "ok" | "warn";
  className?: string;
}) {
  return (
    <span
      className={cn(
        "inline-flex items-center gap-[7px] rounded-pill border px-[11px] py-[4px] font-mono text-[10.5px]",
        tone === "ok" && "border-ok/40 bg-ok/[6%] text-ok",
        tone === "warn" && "border-warn/50 bg-bg-subtle-2 text-warn",
        tone === "neutral" && "border-line bg-bg text-ink-3",
        className,
      )}
    >
      {icon}
      {children}
    </span>
  );
}

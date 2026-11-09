import type { ReactNode } from "react";
import { cn, Eyebrow } from "@/components/ui";

/* The white, framed card every 5×/6× step centers in the canvas below the
 * header. `eyebrow` is the "step X of N" mono label; `title` is the display
 * serif headline. Width varies by screen (520–900px in the mocks), so it's a
 * prop rather than baked in. */
export function StepCard({
  eyebrow,
  eyebrowTrailing,
  title,
  description,
  width = 560,
  className,
  children,
}: {
  eyebrow?: ReactNode;
  eyebrowTrailing?: ReactNode;
  title: ReactNode;
  description?: ReactNode;
  width?: number;
  className?: string;
  children?: ReactNode;
}) {
  return (
    <div style={{ width, maxWidth: "100%" }} className={cn("mx-auto", className)}>
      <div className="rounded-frame border border-line bg-bg p-[26px] shadow-pop">
        {(eyebrow || eyebrowTrailing) && (
          <div className="mb-[10px] flex items-center gap-[9px]">
            {eyebrow && <Eyebrow>{eyebrow}</Eyebrow>}
            {eyebrowTrailing}
          </div>
        )}
        <h1 className="mb-2 font-display text-[29px] leading-[1.2] text-ink">{title}</h1>
        {description && (
          <p className="mb-[18px] text-[13.5px] leading-[1.6] text-ink-2">{description}</p>
        )}
        {children}
      </div>
    </div>
  );
}

/** Footer action row — primary + optional secondary + an optional trailing
 *  mono hint pushed to the far right. Shared anatomy across every step. */
export function WizardActions({
  children,
  trailing,
  className,
}: {
  children: ReactNode;
  trailing?: ReactNode;
  className?: string;
}) {
  return (
    <div className={cn("flex flex-wrap items-center gap-[10px]", className)}>
      {children}
      {trailing && <span className="ml-auto font-mono text-[10.5px] text-ink-4">{trailing}</span>}
    </div>
  );
}

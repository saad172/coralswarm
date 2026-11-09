import type { ReactNode } from "react";
import { cn } from "./cn";

/* Empty, loading and error states.

   The mocks deliberately do not draw these — the handoff says to follow the
   app's existing conventions and restyle them with the new tokens. That is
   exactly what these are: same three shapes as the cream kit, retokenised.
   Screens must use them rather than inventing per-page variants, so that a
   slow network looks the same everywhere. */

export function Empty({
  title,
  hint,
  action,
  className,
}: {
  title: string;
  hint?: ReactNode;
  action?: ReactNode;
  className?: string;
}) {
  return (
    <div
      className={cn(
        "rounded-card border border-dashed border-line bg-bg px-8 py-[56px] text-center",
        className,
      )}
    >
      <div className="font-display text-[20px] leading-[1.25] text-ink">{title}</div>
      {hint && <p className="mx-auto mt-[8px] max-w-[46ch] text-[13px] leading-[1.6] text-ink-2">{hint}</p>}
      {action && <div className="mt-[16px] flex justify-center gap-[6px]">{action}</div>}
    </div>
  );
}

export function Loading({ what = "loading", className }: { what?: string; className?: string }) {
  return (
    <div
      role="status"
      aria-live="polite"
      className={cn("py-[56px] text-center font-mono text-[11px] uppercase tracking-[0.13em] text-ink-3", className)}
    >
      {what}…
    </div>
  );
}

export function ErrorNote({
  children,
  action,
  className,
}: {
  children: ReactNode;
  action?: ReactNode;
  className?: string;
}) {
  return (
    <div
      role="alert"
      className={cn(
        "flex items-start gap-3 rounded-control border border-bad/30 bg-brand-soft px-[12px] py-[10px] text-[12.5px] leading-[1.55] text-brand-2",
        className,
      )}
    >
      <span className="min-w-0 flex-1">{children}</span>
      {action && <span className="flex-none">{action}</span>}
    </div>
  );
}

/** `ok`-toned counterpart to `ErrorNote` — a quiet, real-sentence confirmation
 *  banner for "the thing you asked for happened", as distinct from an
 *  ephemeral toast (this project has none) or the mono one-liner some screens
 *  use for a short inline status. Reach for this whenever the confirmation is
 *  a full sentence someone needs to actually read (e.g. after a redirect from
 *  a destructive-reading-but-reversible action), not just a count or a label. */
export type NoticeTone = "ok" | "warn";

/* `warn` is for a banner that reports a real, currently-blocking state the
   reader has to act on — not an error (nothing failed) and not a confirmation
   (nothing succeeded). Added for the B4 monitoring-notice gate, whose
   un-attested state is exactly that shape: capture is off, on purpose, until an
   admin acts. Kept as a PROP rather than a caller-side className because tone is
   visual and this kit's own rule is that screens compose primitives and never
   hardcode a colour. */
const NOTICE_TONES: Record<NoticeTone, string> = {
  ok: "border-ok/30 bg-ok/[8%]",
  warn: "border-warn/40 bg-warn/[10%]",
};

export function Notice({
  children,
  action,
  tone = "ok",
  className,
}: {
  children: ReactNode;
  action?: ReactNode;
  tone?: NoticeTone;
  className?: string;
}) {
  return (
    <div
      role="status"
      className={cn(
        "flex items-start gap-3 rounded-control border px-[12px] py-[10px] text-[12.5px] leading-[1.55] text-ink-2",
        NOTICE_TONES[tone],
        className,
      )}
    >
      <span className="min-w-0 flex-1">{children}</span>
      {action && <span className="flex-none">{action}</span>}
    </div>
  );
}

/** Inline skeleton bar for streaming/pending content. Pulses on the system's
 *  `cs-skeleton` keyframe and holds still under reduced motion. */
export function Skeleton({ w = "100%", h = 12, className }: { w?: number | string; h?: number; className?: string }) {
  return (
    <span
      aria-hidden
      className={cn("block animate-skeleton rounded-badge bg-bg-hover motion-reduce:animate-none", className)}
      style={{ width: w, height: h }}
    />
  );
}

/** Horizontal meter — usage, storage, slots, source mix. Coral by default.
 *
 *  `indeterminate` is for work with no known total (a history import that
 *  hasn't counted yet): a segment slides along the track, and holds still as a
 *  partial fill under reduced motion. `value` is ignored and the bar drops its
 *  `aria-valuenow`, which is how a progressbar says "unknown". */
export function Meter({
  value,
  max = 100,
  indeterminate = false,
  tone = "var(--brand)",
  label,
  ariaLabel,
  className,
}: {
  value: number;
  max?: number;
  indeterminate?: boolean;
  tone?: string;
  label?: ReactNode;
  /** Accessible name for the bar, for callers whose visible `label` isn't plain text. */
  ariaLabel?: string;
  className?: string;
}) {
  const pct = max <= 0 ? 0 : Math.max(0, Math.min(100, (value / max) * 100));
  return (
    <div className={cn("flex flex-col gap-[5px]", className)}>
      {label && <div className="flex items-baseline justify-between text-[11.5px] text-ink-2">{label}</div>}
      <div
        role="progressbar"
        aria-label={ariaLabel}
        aria-valuenow={indeterminate ? undefined : value}
        aria-valuemin={indeterminate ? undefined : 0}
        aria-valuemax={indeterminate ? undefined : max}
        className="h-[6px] w-full overflow-hidden rounded-pill bg-bg-hover"
      >
        {indeterminate ? (
          <div className="meter-indeterminate h-full rounded-pill" style={{ background: tone }} />
        ) : (
          <div className="h-full rounded-pill transition-all" style={{ width: `${pct}%`, background: tone }} />
        )}
      </div>
    </div>
  );
}

/** Stacked proportional bar — the Topic page's source mix, where several hues
 *  share one track. Segments with a zero share are dropped so they can't
 *  render as invisible slivers that still consume the rounded end cap. */
export function StackedMeter({
  segments,
  className,
}: {
  segments: { value: number; tone: string; label?: string }[];
  className?: string;
}) {
  const shown = segments.filter((s) => s.value > 0);
  const total = shown.reduce((n, s) => n + s.value, 0);
  return (
    <div className={cn("flex h-[6px] w-full overflow-hidden rounded-pill bg-bg-hover", className)}>
      {total > 0 &&
        shown.map((s, i) => (
          <div
            key={i}
            title={s.label}
            style={{ width: `${(s.value / total) * 100}%`, background: s.tone }}
            className="h-full"
          />
        ))}
    </div>
  );
}

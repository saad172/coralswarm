import type { CSSProperties, ReactNode } from "react";
import { cn } from "./cn";
import { hueTint, hueVar, type Hue } from "./hue";

/* Badges, chips, dots and counts — the small metadata marks. */

export type BadgeTone = "brand" | "accent" | "ok" | "warn" | "bad" | "ink" | "plum" | "amber" | "teal";

const BADGE_FILLS: Record<BadgeTone, [string, string]> = {
  brand: ["var(--brand)", "#fff"],
  accent: ["var(--accent)", "#fff"],
  ok: ["var(--ok)", "#fff"],
  warn: ["var(--warn)", "#fff"],
  bad: ["var(--bad)", "#fff"],
  ink: ["var(--ink)", "#fff"],
  plum: ["var(--hue-plum)", "#fff"],
  // Amber is too light to carry white text — it takes ink instead.
  amber: ["var(--hue-amber)", "var(--ink)"],
  teal: ["var(--hue-teal)", "#fff"],
};

/** Solid (or outline) status badge.
 *
 *  `quiet` is the outline that carries no tone: a `line` border and `ink-3`
 *  text, for a tag that labels rather than alerts (the connector grid's Beta).
 *  It wins over `tone` and `outline`. */
export function Badge({
  children,
  tone = "brand",
  outline = false,
  quiet = false,
  className,
  style,
}: {
  children: ReactNode;
  tone?: BadgeTone;
  outline?: boolean;
  quiet?: boolean;
  className?: string;
  style?: CSSProperties;
}) {
  const [bg, fg] = BADGE_FILLS[tone] ?? BADGE_FILLS.brand;
  return (
    <span
      className={cn(
        "inline-flex items-center rounded-pill px-[9px] py-[3px] text-[11px] font-medium tracking-[0.02em]",
        quiet && "border border-line font-mono text-ink-3",
        className,
      )}
      style={
        quiet
          ? style
          : outline
          ? { border: `1px solid ${bg}`, color: bg, fontFamily: "var(--mono)", ...style }
          : { background: bg, color: fg, ...style }
      }
    >
      {children}
    </span>
  );
}

/** Tinted mono chip — scope, freshness, source metadata. 18% hue tint, or the
 *  coral wash when no hue is given. */
export function Chip({
  children,
  hue,
  className,
  style,
}: {
  children: ReactNode;
  hue?: Hue;
  className?: string;
  style?: CSSProperties;
}) {
  return (
    <span
      className={cn(
        "inline-flex items-center gap-[4px] rounded-pill px-[8px] py-[2px] font-mono text-[10.5px] tracking-[0.02em]",
        className,
      )}
      style={{
        background: hue ? hueTint(hue) : "var(--brand-soft)",
        color: hue ? hueVar(hue) : "var(--brand-2)",
        ...style,
      }}
    >
      {children}
    </span>
  );
}

/** Mono count in a coral pill — the sidebar's Review badge and its kin.
 *  9.5px in `brand-soft` with `brand-2` text, per the redesign.
 *
 *  `ariaLabel`: optional accessible name for a pill whose visible content is
 *  a bare number with no adjacent label text making its meaning clear on
 *  its own (e.g. the join-request badge — "3" alone says nothing; "3
 *  people waiting to join" does). Also sets `title` for a hover tooltip,
 *  same value. Omitted for pills whose surrounding row/label already gives
 *  the number context (the default, every pre-existing caller). */
export function CountPill({
  children,
  className,
  ariaLabel,
}: {
  children: ReactNode;
  className?: string;
  ariaLabel?: string;
}) {
  return (
    <span
      className={cn(
        "flex-none rounded-pill bg-brand-soft px-[5px] py-[1px] font-mono text-[9.5px] text-brand-2",
        className,
      )}
      aria-label={ariaLabel}
      title={ariaLabel}
    >
      {children}
    </span>
  );
}

/** Bare hue dot. 7px in sidebar sub-lists, 5px inline in metadata rows. */
export function Dot({ hue, size = 7, className }: { hue: Hue | string; size?: number; className?: string }) {
  const fill = typeof hue === "string" && hue.startsWith("#") ? hue : hueVar(hue as Hue);
  return (
    <span
      className={cn("inline-block flex-none rounded-pill", className)}
      style={{ width: size, height: size, background: fill }}
    />
  );
}

/** Outlined scope pill with a leading hue dot — "● Platform" in the ask header
 *  and composer. */
export function ScopePill({
  hue,
  children,
  className,
}: {
  hue: Hue;
  children: ReactNode;
  className?: string;
}) {
  return (
    <span
      className={cn(
        "inline-flex items-center gap-[6px] rounded-pill border border-line py-[3px] pl-[7px] pr-[9px] text-[11.5px] text-ink-2",
        className,
      )}
    >
      <Dot hue={hue} size={5} />
      {children}
    </span>
  );
}

/** First-letter avatar. The redesign's default is the coral wash with brand-2
 *  text (the sidebar user row); pass a hue for the multi-colour people case. */
export function Avatar({
  name,
  size = 24,
  hue,
  className,
}: {
  name: string;
  size?: number;
  hue?: Hue;
  className?: string;
}) {
  const initials = name
    .split(/\s+/)
    .filter(Boolean)
    .slice(0, 2)
    .map((w) => w[0]?.toUpperCase() ?? "")
    .join("");
  return (
    <span
      className={cn("grid flex-none place-items-center rounded-pill font-semibold", className)}
      style={{
        width: size,
        height: size,
        fontSize: Math.round(size * 0.42),
        background: hue ? `color-mix(in srgb, ${hueVar(hue)} 22%, var(--bg))` : "var(--brand-soft)",
        color: hue ? hueVar(hue) : "var(--brand-2)",
      }}
    >
      {initials || "?"}
    </span>
  );
}

export type VerdictTone = "ok" | "warn" | "bad" | "muted";

/* Literal class names, NOT `verdict-${tone}`: Tailwind finds classes by
   scanning source for whole strings, so an interpolated name is purged from
   the build and the tone silently never applies (the pill renders untinted). */
const VERDICT_TONE: Record<VerdictTone, string> = {
  ok: "verdict-ok",
  warn: "verdict-warn",
  bad: "verdict-bad",
  muted: "verdict-muted",
};

/** Status word + glyph on an 18% status tint (`.verdict` in globals.css).
 *  Meaning is carried by the glyph and the word, never by the tone alone;
 *  `title` adds the longer explanation for pointer users and the accessible
 *  description for screen readers. Jev citation verdicts and fact history. */
export function Verdict({
  tone,
  glyph,
  children,
  title,
  className,
}: {
  tone: VerdictTone;
  glyph: string;
  children: ReactNode;
  title?: string;
  className?: string;
}) {
  return (
    <span className={cn("verdict", VERDICT_TONE[tone], className)} title={title}>
      <span className="verdict-glyph" aria-hidden="true">{glyph}</span>
      <span>{children}</span>
      {title && <span className="sr-only">: {title}</span>}
    </span>
  );
}

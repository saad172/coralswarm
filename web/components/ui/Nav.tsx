"use client";

import { useRef, type ReactNode } from "react";
import { cn } from "./cn";
import { CountPill } from "./Badge";

/* Selection controls: segmented switches, tabs and facet rows.

   All three share the redesign's active-state anatomy — white fill, 1px line
   border, `shadow-row`, weight 500 — while inactive items keep a TRANSPARENT
   1px border so nothing shifts by a pixel when selection moves. That
   no-layout-shift rule is the reason these are components and not ad-hoc
   markup in each screen. */

export const activeRow = (on: boolean) =>
  cn(
    "border transition-colors",
    on
      ? "border-line bg-bg font-medium text-ink shadow-row"
      : "border-transparent font-normal text-ink-2 hover:bg-bg-hover",
  );

/** Pill segmented control. */
export function Seg<K extends string>({
  options,
  value,
  onChange,
  className,
}: {
  options: { key: K; label: string; count?: number }[];
  value: K;
  onChange: (k: K) => void;
  className?: string;
}) {
  return (
    <div
      role="tablist"
      className={cn("inline-flex flex-none items-center rounded-pill border border-line bg-bg-2 p-[3px]", className)}
    >
      {options.map((o) => {
        const on = o.key === value;
        return (
          <button
            type="button"
            key={o.key}
            role="tab"
            aria-selected={on}
            onClick={() => onChange(o.key)}
            className={cn(
              "whitespace-nowrap rounded-pill px-[14px] py-[5px] text-[12.5px] transition-colors",
              "focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-brand/40",
              on ? "bg-bg font-medium text-ink shadow-row" : "text-ink-2 hover:text-ink",
            )}
          >
            {o.label}
            {o.count !== undefined && (
              <span className="ml-[6px] font-mono text-[10.5px] text-ink-3">{o.count}</span>
            )}
          </button>
        );
      })}
    </div>
  );
}

/** Ordered discrete-level control ("slider") for a small, fixed ladder of
 *  named levels — the ask composer's reasoning-effort dial. A native
 *  `<input type="range">` can't be restyled into this system's flat 1px-line
 *  language (its track/thumb are UA pseudo-elements, not expressible as our
 *  utilities), so this reimplements the WAI-ARIA "Radio Group" pattern
 *  instead: one `role="radiogroup"`, one `role="radio"` per level with roving
 *  tabindex, and Left/Right (or Up/Down, Home/End) moving BOTH focus and the
 *  selection in one step — not just Tab-then-Enter through a row of buttons.
 *  Each level's own accessible name plus `aria-checked` is what a screen
 *  reader announces as the current level, exactly as it would for any other
 *  radio group. */
export function LevelDial<K extends string>({
  options,
  value,
  onChange,
  ariaLabel,
  className,
}: {
  options: { key: K; label: string }[];
  value: K;
  onChange: (k: K) => void;
  ariaLabel: string;
  className?: string;
}) {
  const refs = useRef<(HTMLButtonElement | null)[]>([]);
  const idx = Math.max(
    0,
    options.findIndex((o) => o.key === value),
  );

  const select = (i: number, moveFocus: boolean) => {
    const clamped = Math.min(options.length - 1, Math.max(0, i));
    onChange(options[clamped].key);
    if (moveFocus) refs.current[clamped]?.focus();
  };

  return (
    <div
      role="radiogroup"
      aria-label={ariaLabel}
      className={cn(
        "inline-flex items-center gap-[2px] rounded-pill border border-line bg-bg-2 p-[3px]",
        className,
      )}
    >
      {options.map((o, i) => {
        const on = i === idx;
        return (
          <button
            key={o.key}
            ref={(el) => {
              refs.current[i] = el;
            }}
            type="button"
            role="radio"
            aria-checked={on}
            tabIndex={on ? 0 : -1}
            onClick={() => select(i, false)}
            onKeyDown={(e) => {
              switch (e.key) {
                case "ArrowRight":
                case "ArrowDown":
                  e.preventDefault();
                  select(i + 1, true);
                  break;
                case "ArrowLeft":
                case "ArrowUp":
                  e.preventDefault();
                  select(i - 1, true);
                  break;
                case "Home":
                  e.preventDefault();
                  select(0, true);
                  break;
                case "End":
                  e.preventDefault();
                  select(options.length - 1, true);
                  break;
                default:
                  break;
              }
            }}
            className={cn(
              "whitespace-nowrap rounded-pill px-[10px] py-[5px] text-[11.5px] transition-colors",
              "focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-brand/40",
              on ? "bg-bg font-medium text-ink shadow-row" : "text-ink-2 hover:text-ink",
            )}
          >
            {o.label}
          </button>
        );
      })}
    </div>
  );
}

/** Underlined tab bar — page-level sections (Settings, Topic detail). */
export function Tabs<K extends string>({
  options,
  value,
  onChange,
  className,
}: {
  options: { key: K; label: string; count?: number }[];
  value: K;
  onChange: (k: K) => void;
  className?: string;
}) {
  return (
    <div role="tablist" className={cn("flex items-center gap-[18px] border-b border-line-2", className)}>
      {options.map((o) => {
        const on = o.key === value;
        return (
          <button
            type="button"
            key={o.key}
            role="tab"
            aria-selected={on}
            onClick={() => onChange(o.key)}
            className={cn(
              "-mb-px flex items-center gap-[6px] whitespace-nowrap border-b-2 pb-[9px] text-[13px] transition-colors",
              "focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-brand/40",
              on ? "border-brand font-medium text-ink" : "border-transparent text-ink-2 hover:text-ink",
            )}
          >
            {o.label}
            {o.count !== undefined && (
              <span className="font-mono text-[10px] text-ink-4">{o.count}</span>
            )}
          </button>
        );
      })}
    </div>
  );
}

/** Facet row — Library's type filters. A dot-keyed, count-bearing list that
 *  reads as one selection, unlike Tabs which navigates. */
export function FacetRow<K extends string>({
  options,
  value,
  onChange,
  className,
}: {
  options: { key: K; label: string; count?: number; dot?: ReactNode }[];
  value: K;
  onChange: (k: K) => void;
  className?: string;
}) {
  return (
    <div className={cn("flex flex-wrap items-center gap-[6px]", className)}>
      {options.map((o) => {
        const on = o.key === value;
        return (
          <button
            type="button"
            key={o.key}
            aria-pressed={on}
            onClick={() => onChange(o.key)}
            className={cn(
              "inline-flex items-center gap-[6px] rounded-pill px-[11px] py-[5px] text-[12.5px]",
              "focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-brand/40",
              activeRow(on),
            )}
          >
            {o.dot}
            {o.label}
            {o.count !== undefined && (
              <span className="font-mono text-[10px] text-ink-4">{o.count}</span>
            )}
          </button>
        );
      })}
    </div>
  );
}

/** Sidebar nav row. `count` renders the coral review-style badge. */
export function NavRow({
  icon,
  label,
  active = false,
  count,
  trailing,
  onClick,
  className,
}: {
  icon?: ReactNode;
  label: ReactNode;
  active?: boolean;
  count?: ReactNode;
  trailing?: ReactNode;
  onClick?: () => void;
  className?: string;
}) {
  return (
    <button
      type="button"
      onClick={onClick}
      className={cn(
        "flex w-full items-center gap-[9px] rounded-row px-[10px] py-[6px] text-left text-[13px]",
        "focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-brand/40",
        activeRow(active),
        className,
      )}
    >
      {icon && <span className="grid w-[15px] flex-none place-items-center">{icon}</span>}
      <span className="min-w-0 flex-1 truncate">{label}</span>
      {count !== undefined && count !== null && <CountPill>{count}</CountPill>}
      {trailing}
    </button>
  );
}

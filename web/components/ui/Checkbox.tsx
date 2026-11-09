import type { ReactNode } from "react";
import { cn } from "./cn";

/* Checkbox — 15px, ink fill with a ✓ when on.

   The kit had none (MenuItem's `selectable` paints its own glyph for a menu
   row). This is the real form control: a visually hidden native input carries
   the semantics, focus and label association, and the box is its sibling, so a
   row that wraps the whole thing in a click target behaves like any checkbox.
   No indeterminate state: the scope picker's "select all visible" is a ghost
   Button, not a tri-state box. */
export function Checkbox({
  checked,
  onChange,
  label,
  ariaLabel,
  disabled,
  className,
}: {
  checked: boolean;
  onChange: (next: boolean) => void;
  /** Visible text beside the box. Omit for a bare box inside a row that already
   *  names it, and pass `ariaLabel` instead. */
  label?: ReactNode;
  ariaLabel?: string;
  disabled?: boolean;
  className?: string;
}) {
  return (
    <label
      className={cn(
        "inline-flex items-center gap-[10px] text-[13px] text-ink",
        disabled ? "cursor-not-allowed opacity-50" : "cursor-pointer",
        className,
      )}
    >
      <input
        type="checkbox"
        checked={checked}
        disabled={disabled}
        aria-label={ariaLabel}
        onChange={(e) => onChange(e.target.checked)}
        className="peer sr-only"
      />
      <span
        aria-hidden="true"
        className={cn(
          "grid h-[15px] w-[15px] flex-none place-items-center rounded-badge border transition-colors",
          "peer-focus-visible:ring-2 peer-focus-visible:ring-brand/40",
          checked ? "border-ink bg-ink text-white" : "border-line bg-bg text-transparent",
        )}
      >
        <svg width="9" height="9" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth={3.2} strokeLinecap="round" strokeLinejoin="round">
          <path d="M5 12.5 10 17l9-10" />
        </svg>
      </span>
      {label}
    </label>
  );
}

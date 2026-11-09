import type { ButtonHTMLAttributes, ReactNode } from "react";
import { cn } from "./cn";

/* Buttons.
   The redesign's control anatomy: 8px radius, 12.5–13px label, a 1px line
   border on quiet variants, and one solid dark button for the primary action.
   Note the primary is INK, not coral — coral is an accent for dots, active
   marks and meters, and is reserved for `brand` (used sparingly, e.g. the
   new-chat plus). */

export type ButtonVariant = "primary" | "brand" | "secondary" | "ghost" | "danger";
export type ButtonSize = "sm" | "md" | "lg";

const VARIANTS: Record<ButtonVariant, string> = {
  primary: "bg-ink text-white border-ink hover:bg-ink-2 hover:border-ink-2",
  brand: "bg-brand text-white border-brand hover:bg-brand-2 hover:border-brand-2",
  secondary: "bg-bg text-ink-2 border-line hover:bg-bg-hover hover:text-ink",
  ghost: "bg-transparent text-ink-2 border-transparent hover:bg-bg-hover hover:text-ink",
  danger: "bg-transparent text-bad border-bad/40 hover:bg-bad hover:text-white hover:border-bad",
};

const SIZES: Record<ButtonSize, string> = {
  sm: "gap-[5px] rounded-row px-[10px] py-[5px] text-[12.5px]",
  md: "gap-[6px] rounded-row px-[12px] py-[6px] text-[13px]",
  lg: "gap-[7px] rounded-field px-[16px] py-[9px] text-[13.5px]",
};

export interface ButtonProps extends ButtonHTMLAttributes<HTMLButtonElement> {
  variant?: ButtonVariant;
  size?: ButtonSize;
  /** Leading glyph — a 14–15px icon from components/icons. */
  icon?: ReactNode;
  /** Trailing slot, e.g. a ⌘K hint or a count. */
  trailing?: ReactNode;
  full?: boolean;
}

/** The button's classes, for a link that must look like one (navigation is an
 *  anchor, not a `<button>`; nesting one in the other is invalid). */
export function buttonClasses({
  variant = "secondary",
  size = "md",
  full = false,
  className,
}: {
  variant?: ButtonVariant;
  size?: ButtonSize;
  full?: boolean;
  className?: string;
} = {}): string {
  return cn(
    "inline-flex items-center justify-center whitespace-nowrap border font-medium transition-colors",
    "disabled:cursor-not-allowed disabled:opacity-50",
    "focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-brand/40",
    VARIANTS[variant],
    SIZES[size],
    full && "w-full",
    className,
  );
}

export function Button({
  variant = "secondary",
  size = "md",
  icon,
  trailing,
  full = false,
  className,
  children,
  type = "button",
  ...rest
}: ButtonProps) {
  return (
    <button type={type} className={buttonClasses({ variant, size, full, className })} {...rest}>
      {icon && <span className="flex-none">{icon}</span>}
      {children}
      {trailing && <span className="ml-auto flex-none pl-2">{trailing}</span>}
    </button>
  );
}

/** Square icon-only control — toolbar actions, the composer's attach button. */
export function IconButton({
  label,
  size = 30,
  className,
  children,
  type = "button",
  ...rest
}: ButtonHTMLAttributes<HTMLButtonElement> & { label: string; size?: number }) {
  return (
    <button
      type={type}
      aria-label={label}
      title={label}
      style={{ width: size, height: size }}
      className={cn(
        "grid flex-none place-items-center rounded-field border border-line text-ink-2 transition-colors",
        "hover:bg-bg-hover hover:text-ink disabled:cursor-not-allowed disabled:opacity-50",
        "focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-brand/40",
        className,
      )}
      {...rest}
    >
      {children}
    </button>
  );
}

/** Pill-shaped suggestion / follow-up prompt (the chips under a streamed
 *  answer). Reads as a button, not a chip — it performs an action. */
export function SuggestionPill({
  className,
  children,
  type = "button",
  ...rest
}: ButtonHTMLAttributes<HTMLButtonElement>) {
  return (
    <button
      type={type}
      className={cn(
        "rounded-pill border border-line px-[13px] py-[6px] text-[12.5px] text-ink-2 transition-colors",
        "hover:border-ink-3 hover:bg-bg-hover hover:text-ink",
        "focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-brand/40",
        className,
      )}
      {...rest}
    >
      {children}
    </button>
  );
}

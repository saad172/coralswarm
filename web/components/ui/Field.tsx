"use client";

// Marked client-side because FieldShell calls useId (and Toggle takes an event
// handler). Every app page is already a client component, but declaring it here
// means a server component importing a field fails at build rather than at
// runtime on the first render.

import type { InputHTMLAttributes, ReactNode, SelectHTMLAttributes, TextareaHTMLAttributes } from "react";
import { useId } from "react";
import { cn } from "./cn";

/* Form controls. 9px radius, 1px line border, brand focus ring. */

const BASE =
  "w-full rounded-field border border-line bg-bg px-[10px] py-[7px] text-[13px] text-ink " +
  "placeholder:text-ink-4 transition-colors " +
  "focus:border-brand focus:outline-none focus:ring-[3px] focus:ring-brand/20 " +
  "disabled:cursor-not-allowed disabled:bg-bg-2 disabled:text-ink-4";

/** Label + control + hint/error wrapper. Wire `htmlFor` automatically so every
 *  field in the app is labelled without each screen remembering to. */
export function FieldShell({
  label,
  hint,
  error,
  required,
  children,
  className,
}: {
  label?: ReactNode;
  hint?: ReactNode;
  error?: ReactNode;
  required?: boolean;
  children: (id: string, describedBy: string | undefined) => ReactNode;
  className?: string;
}) {
  const id = useId();
  const hintId = hint || error ? `${id}-hint` : undefined;
  return (
    <div className={cn("flex flex-col gap-[6px]", className)}>
      {label && (
        <label htmlFor={id} className="text-[12.5px] font-medium text-ink">
          {label}
          {required && <span className="ml-[3px] text-brand-2">*</span>}
        </label>
      )}
      {children(id, hintId)}
      {(error || hint) && (
        <p id={hintId} className={cn("text-[11.5px] leading-[1.5]", error ? "text-bad" : "text-ink-3")}>
          {error || hint}
        </p>
      )}
    </div>
  );
}

export function Input({
  label,
  hint,
  error,
  className,
  wrapperClassName,
  ...rest
}: InputHTMLAttributes<HTMLInputElement> & {
  label?: ReactNode;
  hint?: ReactNode;
  error?: ReactNode;
  wrapperClassName?: string;
}) {
  return (
    <FieldShell label={label} hint={hint} error={error} required={rest.required} className={wrapperClassName}>
      {(id, describedBy) => (
        <input
          id={id}
          aria-describedby={describedBy}
          aria-invalid={error ? true : undefined}
          className={cn(BASE, error && "border-bad focus:border-bad focus:ring-bad/20", className)}
          {...rest}
        />
      )}
    </FieldShell>
  );
}

export function Textarea({
  label,
  hint,
  error,
  className,
  wrapperClassName,
  ...rest
}: TextareaHTMLAttributes<HTMLTextAreaElement> & {
  label?: ReactNode;
  hint?: ReactNode;
  error?: ReactNode;
  wrapperClassName?: string;
}) {
  return (
    <FieldShell label={label} hint={hint} error={error} required={rest.required} className={wrapperClassName}>
      {(id, describedBy) => (
        <textarea
          id={id}
          aria-describedby={describedBy}
          aria-invalid={error ? true : undefined}
          className={cn(BASE, "resize-y leading-[1.55]", error && "border-bad focus:border-bad", className)}
          {...rest}
        />
      )}
    </FieldShell>
  );
}

export function Select({
  label,
  hint,
  error,
  className,
  wrapperClassName,
  children,
  ...rest
}: SelectHTMLAttributes<HTMLSelectElement> & {
  label?: ReactNode;
  hint?: ReactNode;
  error?: ReactNode;
  wrapperClassName?: string;
}) {
  return (
    <FieldShell label={label} hint={hint} error={error} required={rest.required} className={wrapperClassName}>
      {(id, describedBy) => (
        <select id={id} aria-describedby={describedBy} className={cn(BASE, "pr-8", className)} {...rest}>
          {children}
        </select>
      )}
    </FieldShell>
  );
}

/** Search input with a leading glyph — the library/meeting filter bars. */
export function SearchInput({
  icon,
  className,
  ...rest
}: InputHTMLAttributes<HTMLInputElement> & { icon?: ReactNode }) {
  return (
    <div className="relative flex-1">
      {icon && (
        <span className="pointer-events-none absolute left-[10px] top-1/2 -translate-y-1/2 text-ink-3">
          {icon}
        </span>
      )}
      <input
        type="search"
        className={cn(BASE, icon && "pl-[32px]", "bg-bg-2 focus:bg-bg", className)}
        {...rest}
      />
    </div>
  );
}

/** Switch. Coral when on — one of the few places the accent carries state. */
export function Toggle({
  checked,
  onChange,
  label,
  hint,
  disabled,
  className,
  ariaLabel,
}: {
  checked: boolean;
  onChange: (next: boolean) => void;
  label?: ReactNode;
  hint?: ReactNode;
  disabled?: boolean;
  className?: string;
  /** Accessible name for the switch itself. Wrapping the button in a
   *  `<label>` (below) does NOT give it one — that implicit-label
   *  association only applies to native form controls (input/textarea/
   *  select), not a custom `role="switch"` button — so any caller whose
   *  visible title lives outside `label`/`hint` (e.g. in a sibling
   *  `SettingRow` title, as most callers here do) must pass this
   *  explicitly or the switch reaches assistive tech with no name at all. */
  ariaLabel?: string;
}) {
  return (
    <label className={cn("flex items-start gap-[10px]", disabled ? "opacity-50" : "cursor-pointer", className)}>
      <button
        type="button"
        role="switch"
        aria-checked={checked}
        aria-label={ariaLabel}
        disabled={disabled}
        onClick={() => !disabled && onChange(!checked)}
        className={cn(
          "relative mt-[1px] h-[18px] w-[30px] flex-none rounded-pill border transition-colors",
          "focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-brand/40",
          checked ? "border-brand bg-brand" : "border-line bg-bg-2",
          !disabled && "cursor-pointer",
        )}
      >
        <span
          className={cn(
            "absolute top-[2px] h-[12px] w-[12px] rounded-pill bg-white shadow-card transition-all",
            checked ? "left-[15px]" : "left-[2px]",
          )}
        />
      </button>
      {(label || hint) && (
        <span className="min-w-0 flex-1">
          {label && <span className="block text-[13px] text-ink">{label}</span>}
          {hint && <span className="mt-[2px] block text-[11.5px] leading-[1.5] text-ink-3">{hint}</span>}
        </span>
      )}
    </label>
  );
}

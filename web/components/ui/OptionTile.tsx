import type { ReactNode } from "react";
import { cn } from "./cn";

/* OptionTile — one choice in a pick-one group (the wizard's import-history
   step). Selected = `brand-soft` fill + `brand` border, the system's
   "selected tile" rule.

   A visually hidden native radio carries the semantics, so tiles sharing a
   `name` get the arrow-key movement, single tab stop and form behaviour of a
   real radio group for free. Wrap them in an element with `role="radiogroup"`
   and an accessible name. */
export function OptionTile({
  name,
  value,
  checked,
  onChange,
  title,
  hint,
  disabled,
  className,
}: {
  name: string;
  value: string;
  checked: boolean;
  onChange: (value: string) => void;
  title: ReactNode;
  hint?: ReactNode;
  disabled?: boolean;
  className?: string;
}) {
  return (
    <label
      className={cn(
        "flex flex-col gap-[3px] rounded-control border px-[14px] py-[12px] text-left transition-colors",
        "has-[:focus-visible]:ring-2 has-[:focus-visible]:ring-brand/40",
        checked ? "border-brand bg-brand-soft" : "border-line bg-bg hover:bg-bg-subtle",
        disabled ? "cursor-not-allowed opacity-50" : "cursor-pointer",
        className,
      )}
    >
      <input
        type="radio"
        name={name}
        value={value}
        checked={checked}
        disabled={disabled}
        onChange={() => onChange(value)}
        className="sr-only"
      />
      <span className="text-[13px] font-medium text-ink">{title}</span>
      {hint && <span className="text-[12px] leading-[1.5] text-ink-2">{hint}</span>}
    </label>
  );
}

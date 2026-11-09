"use client";

import { useEffect, useRef, useState, type ReactNode } from "react";
import { cn } from "./cn";

/* CopyPill — a value (an MCP address) that copies on click.

   Chip shape, but the WHOLE pill is the button, not a small icon inside it.
   The trailing label goes "copy" → "copied ✓" for 1.8s, as CopyAdminRequest
   does. The accessible name stays "Copy <value>" throughout; the confirmation
   is announced through a separate live region so it isn't read twice.
   `onCopied` runs once per successful copy (never when the clipboard refuses),
   for a caller that records that the value was taken (Home's setup step 2). */

const COPIED_MS = 1800;

export function CopyPill({
  value,
  children,
  ariaLabel,
  className,
  onCopied,
}: {
  /** The text written to the clipboard. */
  value: string;
  /** What the pill displays. Defaults to `value`. */
  children?: ReactNode;
  ariaLabel?: string;
  className?: string;
  /** Called after the value reached the clipboard. */
  onCopied?: () => void;
}) {
  const [copied, setCopied] = useState(false);
  const timer = useRef<ReturnType<typeof setTimeout> | undefined>(undefined);
  useEffect(() => () => clearTimeout(timer.current), []);

  const copy = async () => {
    try {
      await navigator.clipboard.writeText(value);
    } catch {
      // Clipboard can be denied (insecure context, permissions); the address
      // is still on screen to select by hand, so don't claim success.
      return;
    }
    setCopied(true);
    clearTimeout(timer.current);
    timer.current = setTimeout(() => setCopied(false), COPIED_MS);
    onCopied?.();
  };

  return (
    <button
      type="button"
      onClick={copy}
      aria-label={ariaLabel ?? `Copy ${value}`}
      className={cn(
        "inline-flex max-w-full items-center justify-between gap-[10px] rounded-pill border border-line bg-bg-2 py-[7px] pl-[14px] pr-[8px]",
        "font-mono text-[12.5px] text-ink transition-colors hover:border-ink-3 hover:bg-bg-hover",
        "focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-brand/40",
        className,
      )}
    >
      <span className="truncate">{children ?? value}</span>
      <span
        aria-hidden="true"
        className="inline-flex flex-none items-center gap-[5px] rounded-pill border border-line bg-bg px-[8px] py-[2px] text-[10.5px] text-ink-2"
      >
        {!copied && (
          <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round">
            <rect x="9" y="9" width="11" height="11" rx="2" />
            <path d="M5 15V6a2 2 0 0 1 2-2h9" />
          </svg>
        )}
        {copied ? "copied ✓" : "copy"}
      </span>
      <span role="status" className="sr-only">
        {copied ? "Copied" : ""}
      </span>
    </button>
  );
}

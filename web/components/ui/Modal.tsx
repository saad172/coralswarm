"use client";

import { useEffect, useRef, useId, type ReactNode } from "react";
import { cn } from "./cn";
import { IconButton } from "./Button";

/* Modal / dialog.

   Used for the Share-and-promote flow (1d), which the handoff is emphatic
   about: promoting something private into a shared workspace is an explicit,
   deliberate act with a visible target — never implicit. So this is a real
   modal with a focus trap and an escape hatch, not a popover.

   Escape closes and focus returns to the opener; background scroll is locked
   while open.

   Below `sm` it is a bottom sheet: pinned to the bottom edge, full width, top
   corners at the frame radius. `header` is a strip above the title row (the
   connect wizard's StepRail); `display` sets the title in the display voice. */

/** Tabbable elements, for both the trap and the move-focus-in call. Kept as
 *  parts because a descendant combinator binds only to the FIRST item of a
 *  selector list — `[x] a, button` means "a inside x" OR "any button". Every
 *  scoped variant must therefore prefix each part individually. */
const FOCUSABLE_PARTS = [
  "a[href]",
  "button:not([disabled])",
  "input:not([disabled])",
  "select:not([disabled])",
  "textarea:not([disabled])",
  '[tabindex]:not([tabindex="-1"])',
];
const FOCUSABLE = FOCUSABLE_PARTS.join(", ");
const FOCUSABLE_IN_BODY = FOCUSABLE_PARTS.map((s) => `[data-modal-body] ${s}`).join(", ");

export function Modal({
  open,
  onClose,
  title,
  description,
  header,
  display = false,
  footer,
  children,
  width = 520,
}: {
  open: boolean;
  onClose: () => void;
  title: ReactNode;
  description?: ReactNode;
  /** Strip above the title row, e.g. a StepRail. */
  header?: ReactNode;
  /** Title in `font-display` (the human voice) instead of the sans label. */
  display?: boolean;
  footer?: ReactNode;
  children?: ReactNode;
  width?: number;
}) {
  const titleId = useId();
  const panelRef = useRef<HTMLDivElement>(null);
  const openerRef = useRef<Element | null>(null);

  /* `onClose` is held in a ref so it is NOT an effect dependency.
     Every caller passes an inline arrow, so its identity changes on each
     parent render — and this effect owns the scroll lock, the keydown
     listener and the move-focus-in call. Depending on it meant that typing
     one character into a field inside the dialog re-ran the whole effect:
     cleanup pulled focus back to the opener, then the effect moved it to the
     header Close button, so the field could never receive a second keystroke.
     The dialog is only mounted while `open`, so keying on `open` alone is
     the correct lifetime. */
  const onCloseRef = useRef(onClose);
  useEffect(() => {
    onCloseRef.current = onClose;
  });

  useEffect(() => {
    if (!open) return;
    openerRef.current = document.activeElement;
    const prevOverflow = document.body.style.overflow;
    document.body.style.overflow = "hidden";

    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") {
        e.stopPropagation();
        onCloseRef.current();
        return;
      }
      if (e.key !== "Tab") return;
      // Minimal focus trap: cycle within the panel so Tab can't reach the page
      // behind the scrim.
      const focusables = panelRef.current?.querySelectorAll<HTMLElement>(FOCUSABLE);
      if (!focusables?.length) return;
      const first = focusables[0];
      const last = focusables[focusables.length - 1];
      if (!panelRef.current?.contains(document.activeElement)) {
        e.preventDefault();
        (e.shiftKey ? last : first).focus();
      } else if (e.shiftKey && document.activeElement === first) {
        e.preventDefault();
        last.focus();
      } else if (!e.shiftKey && document.activeElement === last) {
        e.preventDefault();
        first.focus();
      }
    };

    document.addEventListener("keydown", onKey, true);
    /* Move focus in on open, so a keyboard user isn't left behind the scrim.
       Skip the header Close button: a selector list matches in DOCUMENT order,
       and Close is rendered before `children`, so an unqualified query always
       returned it — landing the caret on "dismiss" even when the dialog opens
       onto a form field. */
    const panel = panelRef.current;
    (
      panel?.querySelector<HTMLElement>(FOCUSABLE_IN_BODY) ??
      panel?.querySelector<HTMLElement>(FOCUSABLE) ??
      panel
    )?.focus();

    return () => {
      document.removeEventListener("keydown", onKey, true);
      document.body.style.overflow = prevOverflow;
      (openerRef.current as HTMLElement | null)?.focus?.();
    };
  }, [open]);

  if (!open) return null;

  return (
    <div
      className="fixed inset-0 z-50 flex items-end justify-center sm:items-center sm:p-4"
      style={{ background: "rgba(20,22,26,.28)" }}
      onMouseDown={(e) => {
        if (e.target === e.currentTarget) onClose();
      }}
    >
      <div
        ref={panelRef}
        role="dialog"
        aria-modal="true"
        aria-labelledby={titleId}
        // Lets the panel itself hold focus when it contains nothing tabbable,
        // so focus never falls back to the page behind the scrim.
        tabIndex={-1}
        style={{ width, maxWidth: "100%" }}
        className="max-h-[88vh] overflow-hidden rounded-t-frame border border-b-0 border-line bg-bg pb-[env(safe-area-inset-bottom)] shadow-frame sm:rounded-frame sm:border-b sm:pb-0"
      >
        {header && <div className="border-b border-line-2 px-[18px] py-[12px]">{header}</div>}
        <div className="flex items-start gap-3 border-b border-line-2 px-[18px] py-[14px]">
          <div className="min-w-0 flex-1">
            <h2
              id={titleId}
              className={cn(
                "text-ink",
                display ? "font-display text-[22px] font-normal leading-[1.2]" : "text-[15px] font-medium leading-[1.35]",
              )}
            >
              {title}
            </h2>
            {description && (
              <p className="mt-[4px] text-[12.5px] leading-[1.55] text-ink-2">{description}</p>
            )}
          </div>
          <IconButton label="Close" size={26} className="border-transparent" onClick={onClose}>
            <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round">
              <path d="M6 6l12 12M18 6L6 18" />
            </svg>
          </IconButton>
        </div>

        {children && (
          <div data-modal-body="" className="max-h-[62vh] overflow-y-auto px-[18px] py-[14px]">
            {children}
          </div>
        )}

        {footer && (
          <div className="flex items-center justify-end gap-[8px] border-t border-line-2 bg-bg-2 px-[18px] py-[12px]">
            {footer}
          </div>
        )}
      </div>
    </div>
  );
}

/** Anchored popover — menus and switchers. Closes on outside click and Escape.
 *  Uses `shadow-pop`, the interpolated level for in-app floats. */
export function Popover({
  open,
  onClose,
  children,
  align = "left",
  side = "bottom",
  className,
  triggerRef,
}: {
  open: boolean;
  onClose: () => void;
  children: ReactNode;
  /** The element that opens this popover. Pass it and clicking that element
   *  while the menu is open CLOSES it, instead of closing and immediately
   *  reopening: the trigger is a sibling of the panel, not a descendant, so
   *  without this it counts as "outside" — dismissal fires on mousedown, then
   *  the trigger's own click toggles it straight back open.
   *
   *  Optional so existing call sites keep working; anything whose trigger is a
   *  toggle wants it. */
  triggerRef?: { current: HTMLElement | null };
  align?: "left" | "right";
  /** Which way the menu opens. Use "top" for triggers near the bottom of the
   *  viewport — the chat composer is pinned there, and a bottom-opening menu
   *  is simply clipped off-screen with no way to reach its items. */
  side?: "bottom" | "top";
  className?: string;
}) {
  const ref = useRef<HTMLDivElement>(null);

  /* Same reason as Modal above: callers pass an inline arrow, so depending on
     `onClose` re-ran this effect on every parent render. That also re-armed the
     deferred listener below — a parent that re-renders within the same tick
     (chat streaming patches per SSE delta) could clear the pending timeout
     before it ever fired, leaving the popover with no outside-click handler. */
  const onCloseRef = useRef(onClose);
  useEffect(() => {
    onCloseRef.current = onClose;
  });

  useEffect(() => {
    if (!open) return;
    const onDown = (e: MouseEvent) => {
      const target = e.target as Node;
      if (ref.current?.contains(target)) return;
      // Let the trigger's own click handler do the toggling — see `triggerRef`.
      if (triggerRef?.current?.contains(target)) return;
      onCloseRef.current();
    };
    const onKey = (e: KeyboardEvent) => e.key === "Escape" && onCloseRef.current();
    // `mousedown` on the document, deferred a tick so the click that OPENED
    // the popover doesn't immediately close it again.
    const t = setTimeout(() => document.addEventListener("mousedown", onDown), 0);
    document.addEventListener("keydown", onKey);
    return () => {
      clearTimeout(t);
      document.removeEventListener("mousedown", onDown);
      document.removeEventListener("keydown", onKey);
    };
    // `triggerRef` is deliberately not a dependency. It is a ref object, so its
    // identity is stable and `.current` is read at event time anyway — while
    // depending on it would let a caller passing an inline object re-run this
    // effect every render, which is the exact bug the ref indirection above
    // exists to prevent.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [open]);

  if (!open) return null;
  return (
    <div
      ref={ref}
      className={cn(
        "absolute z-40 min-w-[200px] overflow-hidden rounded-control border border-line bg-bg py-[4px] shadow-pop",
        // Cap the height and scroll: a long workspace list must not run off
        // the viewport in either direction.
        "max-h-[min(320px,60vh)] overflow-y-auto",
        align === "right" ? "right-0" : "left-0",
        side === "top" ? "bottom-full mb-[6px]" : "top-full mt-[6px]",
        className,
      )}
    >
      {children}
    </div>
  );
}

/** One row inside a Popover.
 *
 *  `selectable` is the opt-in multi-select variant (the ask composer's ocean
 *  picker): it paints a checkbox instead of relying on `active`'s background
 *  tint alone to say "more than one of these can be on at once" — a tint by
 *  itself reads as "the current choice" (a single-select affordance) even
 *  when the underlying state already supports several rows being on
 *  together. Every existing call omits `selectable` and renders byte-for-byte
 *  what it always has.
 *
 *  `disabled` renders the row inert with a stated reason (a `title` tooltip
 *  plus `aria-disabled`, not the native `disabled` attribute — the row stays
 *  focusable so the reason is discoverable by keyboard, not just on mouse
 *  hover) instead of a click that's silently swallowed by the caller. */
export function MenuItem({
  children,
  onClick,
  active = false,
  danger = false,
  selectable = false,
  disabled = false,
  disabledReason,
}: {
  children: ReactNode;
  onClick?: () => void;
  active?: boolean;
  danger?: boolean;
  selectable?: boolean;
  disabled?: boolean;
  disabledReason?: string;
}) {
  return (
    <button
      type="button"
      role={selectable ? "checkbox" : undefined}
      aria-checked={selectable ? active : undefined}
      aria-disabled={disabled || undefined}
      title={disabled ? disabledReason : undefined}
      onClick={disabled ? undefined : onClick}
      className={cn(
        "flex w-full items-center gap-[8px] px-[12px] py-[7px] text-left text-[12.5px] transition-colors",
        disabled
          ? "cursor-not-allowed text-ink-4"
          : danger
            ? "text-bad hover:bg-brand-soft"
            : "text-ink-2 hover:bg-bg-hover hover:text-ink",
        !disabled && active && "bg-bg-hover font-medium text-ink",
      )}
    >
      {selectable && <MenuItemCheckbox checked={active} disabled={disabled} />}
      {children}
    </button>
  );
}

/** The checkbox glyph for a `selectable` {@link MenuItem}. `aria-hidden`
 *  because the row's own `role="checkbox"`/`aria-checked` already carry the
 *  state — this is purely the visual reinforcement of it. */
function MenuItemCheckbox({ checked, disabled }: { checked: boolean; disabled: boolean }) {
  const tone =
    disabled && !checked
      ? "border-line-2 bg-bg-2 text-transparent"
      : checked
        ? "border-ink bg-ink text-white"
        : "border-line bg-bg text-transparent";
  return (
    <span
      aria-hidden="true"
      className={cn(
        "grid h-[15px] w-[15px] flex-none place-items-center rounded-badge border transition-colors",
        tone,
      )}
    >
      <svg width="9" height="9" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth={3.2} strokeLinecap="round" strokeLinejoin="round">
        <path d="M5 12.5 10 17l9-10" />
      </svg>
    </span>
  );
}

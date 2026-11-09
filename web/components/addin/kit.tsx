"use client";

/* The add-in UI kit shared by the Teams side panel and the Outlook panes.
 *
 * It is the Gmail add-on's card language (extensions/gmail-addin/Card.gs)
 * laid out natively for an iframe instead of CardService: cool-neutral surface
 * (`bg-bg-2` = #F5F6F8), coral (`brand` = #F77764) for the one filled action,
 * stroke icons tinted by type (the same hosted PNGs Gmail uses), a muted "+"
 * row, a fixed footer carrying the primary button. Colours are the project's
 * Tailwind tokens, never literals (CLAUDE.md, Design System). */

import { createContext, useCallback, useContext, useEffect, useLayoutEffect, useRef, useState, type KeyboardEvent as ReactKeyboardEvent, type ReactNode } from "react";
import { createPortal } from "react-dom";

export type Hue = "coral" | "teal" | "plum" | "pink" | "amber" | "ink" | "ok" | "muted";

/** Bump when a PNG under public/addin/icons changes (host image proxies cache by URL). */
const ICON_VERSION = "2";

export function Icon({ name, hue = "ink", size = 20 }: { name: string; hue?: Hue; size?: number }) {
  // eslint-disable-next-line @next/next/no-img-element -- fixed-size hosted glyphs, same PNGs the Gmail add-on uses
  return <img src={`/addin/icons/${name}--${hue}.png?v=${ICON_VERSION}`} alt="" width={size} height={size} className="mt-0.5 shrink-0" aria-hidden="true" />;
}

const FooterSlot = createContext<HTMLElement | null>(null);

/** Page chrome: neutral surface, scrolling body, and a fixed footer SLOT. Any
 *  component below can contribute footer buttons with <Footer>, so a screen
 *  that owns state (the meeting pane) and the shell that owns the account row
 *  compose without passing nodes upward. The slot hides itself when empty. */
export function Surface({ children, label }: { children: ReactNode; label: string }) {
  const [slot, setSlot] = useState<HTMLElement | null>(null);
  return (
    <FooterSlot.Provider value={slot}>
      <main aria-label={label} className="flex min-h-screen flex-col bg-bg-2 font-sans text-pane text-ink">
        <div className="flex-1 space-y-3 overflow-y-auto p-3">{children}</div>
        <div ref={setSlot} className="sticky bottom-0 flex items-center justify-between gap-2 border-t border-line bg-bg px-3 py-3 empty:hidden" />
      </main>
    </FooterSlot.Provider>
  );
}

export function Footer({ children }: { children: ReactNode }) {
  const slot = useContext(FooterSlot);
  return slot ? createPortal(children, slot) : null;
}

export function Section({ header, children, className = "" }: { header?: string; children: ReactNode; className?: string }) {
  return (
    <section className={`overflow-hidden rounded-card border border-line bg-bg ${className}`}>
      {header ? <h2 className="px-3 pb-1 pt-3 font-mono text-pane-label uppercase tracking-pane-label text-ink-3">{header}</h2> : null}
      <div className="divide-y divide-line-3">{children}</div>
    </section>
  );
}

export interface RowProps {
  icon?: string;
  hue?: Hue;
  top?: string;
  text: ReactNode;
  bottom?: ReactNode;
  trailing?: ReactNode;
  onClick?: () => void;
  href?: string;
}

/** Gmail's DecoratedText: start icon, optional top/bottom labels, optional end control. */
export function Row({ icon, hue, top, text, bottom, trailing, onClick, href }: RowProps) {
  const body = (
    <>
      {icon ? <Icon name={icon} hue={hue} /> : null}
      <span className="min-w-0 flex-1 break-words">
        {top ? <span className="block text-pane-2xs text-ink-3">{top}</span> : null}
        <span className="block text-ink">{text}</span>
        {bottom ? <span className="block text-pane-xs text-ink-3">{bottom}</span> : null}
      </span>
      {trailing}
    </>
  );
  const base = "flex w-full items-start gap-3 px-3 py-2.5 text-left";
  if (href) return <a href={href} target="_blank" rel="noopener noreferrer" className={`${base} hover:bg-bg-hover`}>{body}</a>;
  if (onClick) return <button type="button" onClick={onClick} className={`${base} hover:bg-bg-hover`}>{body}</button>;
  return <div className={base}>{body}</div>;
}

/** Keep's "Take a note" row: a coral "+" and the action name. `muted` has no handler. */
export function PlusRow({ text, onClick, muted = false, disabled = false }: { text: string; onClick?: () => void; muted?: boolean; disabled?: boolean }) {
  const icon = <Icon name="plus" hue={muted ? "muted" : "coral"} />;
  if (muted || !onClick) return <div className="flex items-center gap-3 px-3 py-2.5 text-ink-3">{icon}<span>{text}</span></div>;
  return (
    <button type="button" onClick={onClick} disabled={disabled} className="flex w-full items-center gap-3 px-3 py-2.5 text-left hover:bg-bg-hover disabled:opacity-60">
      {icon}<span className="text-ink">{text}</span>
    </button>
  );
}

/** The empty-state hero. The hosted PNG paints its circle AND an inbox-specific
 *  title, so only its circle is shown (cropped) and the words are real text per
 *  surface. */
export function Illustration({ image, title, body }: { image: "empty-start" | "empty-home"; title: string; body: string }) {
  return (
    <div className="px-4 pb-4 pt-5 text-center">
      <div role="img" aria-label="" className="mx-auto mb-3 h-[132px] w-[132px] rounded-full bg-brand-soft bg-no-repeat"
        style={{ backgroundImage: `url(/addin/${image}.png)`, backgroundSize: "auto 238px", backgroundPosition: "-45px -14px" }} />
      <p className="font-display text-pane-title text-ink">{title}</p>
      <p className="mx-auto mt-1 max-w-[260px] text-pane text-ink-2">{body}</p>
    </div>
  );
}

export function FilledButton({ children, onClick, disabled, full = false, type = "button" }: { children: ReactNode; onClick?: () => void; disabled?: boolean; full?: boolean; type?: "button" | "submit" }) {
  return (
    <button type={type === "submit" ? "submit" : "button"} onClick={onClick} disabled={disabled}
      className={`${full ? "w-full" : "flex-1"} rounded-control bg-brand px-4 py-2.5 text-pane font-medium text-on-brand transition hover:bg-brand-2 disabled:cursor-not-allowed disabled:opacity-50`}>
      {children}
    </button>
  );
}

export function OutlineButton({ children, onClick, disabled }: { children: ReactNode; onClick?: () => void; disabled?: boolean }) {
  return (
    <button type="button" onClick={onClick} disabled={disabled}
      className="flex-1 rounded-control border border-line bg-bg px-4 py-2.5 text-pane font-medium text-ink transition hover:bg-bg-hover disabled:cursor-not-allowed disabled:opacity-50">
      {children}
    </button>
  );
}

export function TextButton({ children, onClick, disabled }: { children: ReactNode; onClick?: () => void; disabled?: boolean }) {
  return (
    <button type="button" onClick={onClick} disabled={disabled}
      className="rounded-control px-3 py-2.5 text-pane font-medium text-ink-2 hover:bg-bg-hover disabled:opacity-50">
      {children}
    </button>
  );
}

export function Switch({ checked, onChange, disabled, label }: { checked: boolean; onChange: (next: boolean) => void; disabled?: boolean; label: string }) {
  return (
    <button type="button" role="switch" aria-checked={checked} aria-label={label} disabled={disabled} onClick={() => onChange(!checked)}
      className={`relative mt-0.5 h-5 w-9 shrink-0 rounded-pill transition-colors disabled:opacity-50 ${checked ? "bg-brand" : "bg-bg-3"}`}>
      <span className={`absolute top-0.5 h-4 w-4 rounded-full bg-bg shadow-row transition-all ${checked ? "left-[18px]" : "left-0.5"}`} />
    </button>
  );
}

export function RadioRows<T extends string>({ name, value, options, onChange, disabled }: { name: string; value: T; options: { value: T; label: string }[]; onChange: (next: T) => void; disabled?: boolean }) {
  return (
    <div role="radiogroup" className="px-3 py-2">
      {options.map((option) => (
        <label key={option.value} className="flex cursor-pointer items-center gap-3 py-1.5">
          <input type="radio" name={name} value={option.value} checked={value === option.value} disabled={disabled} onChange={() => onChange(option.value)} className="h-4 w-4 accent-brand" />
          <span className="text-ink">{option.label}</span>
        </label>
      ))}
    </div>
  );
}

/** Gmail's notification: a short transient line, announced politely. */
export function ToastRegion({ message }: { message: string | null }) {
  return (
    <div role="status" aria-live="polite" className="pointer-events-none fixed inset-x-0 bottom-20 z-20 flex justify-center px-3">
      {message ? <p className="max-w-[320px] rounded-control bg-ink px-3.5 py-2.5 text-center text-pane-sm text-on-brand shadow-row">{message}</p> : null}
    </div>
  );
}

/** One toast at a time; a new message replaces the old and restarts the timer. */
export function useToast(durationMs = 4500): [string | null, (message: string) => void] {
  const [message, setMessage] = useState<string | null>(null);
  const timer = useRef<ReturnType<typeof setTimeout> | null>(null);
  useEffect(() => () => { if (timer.current) clearTimeout(timer.current); }, []);
  const show = (next: string) => {
    if (timer.current) clearTimeout(timer.current);
    setMessage(next);
    timer.current = setTimeout(() => setMessage(null), durationMs);
  };
  return [message, show];
}

export interface MenuItem { label: string; onSelect: () => void; destructive?: boolean }

const MENU_WIDTH = 240;
const MENU_MARGIN = 8;

/** Card ⋮ menu. A header line names the account; items follow.
 *
 *  The menu is portalled to document.body and positioned with fixed
 *  coordinates from the button's rect. It used to be `absolute` inside the
 *  account card, and `Section` is `overflow-hidden` (and the pane body
 *  `overflow-y-auto`), so the menu was clipped and never visible. */
export function OverflowMenu({ label, header, subheader, items }: { label: string; header?: string; subheader?: string; items: MenuItem[] }) {
  const [open, setOpen] = useState(false);
  const [pos, setPos] = useState<{ top: number; left: number; width: number; maxHeight: number } | null>(null);
  const button = useRef<HTMLButtonElement>(null);
  const menu = useRef<HTMLDivElement>(null);

  const place = useCallback(() => {
    const trigger = button.current;
    const panel = menu.current;
    if (!trigger || !panel) return;
    const rect = trigger.getBoundingClientRect();
    const vw = window.innerWidth;
    const vh = window.innerHeight;
    const width = Math.min(MENU_WIDTH, Math.max(0, vw - MENU_MARGIN * 2));
    const maxHeight = Math.max(0, vh - MENU_MARGIN * 2);
    // Natural height (scrollHeight + borders), capped to the viewport; the panel scrolls beyond that.
    const height = Math.min(panel.scrollHeight + panel.offsetHeight - panel.clientHeight, maxHeight);
    const left = Math.max(MENU_MARGIN, Math.min(rect.right - width, vw - width - MENU_MARGIN));
    const below = vh - rect.bottom - MENU_MARGIN;
    const above = rect.top - MENU_MARGIN;
    // Down when it fits, else up when it fits, else whichever side has more room.
    const down = height <= below || (height > above && below >= above);
    const raw = down ? rect.bottom + 4 : rect.top - height - 4;
    const top = Math.max(MENU_MARGIN, Math.min(raw, vh - height - MENU_MARGIN));
    setPos((prev) => (prev && prev.top === top && prev.left === left && prev.width === width && prev.maxHeight === maxHeight ? prev : { top, left, width, maxHeight }));
  }, []);

  useLayoutEffect(() => {
    if (!open) { setPos(null); return; }
    place();
  }, [open, place]);

  useEffect(() => {
    if (!open) return;
    const onMouseDown = (event: MouseEvent) => {
      const target = event.target as Node;
      if (menu.current?.contains(target) || button.current?.contains(target)) return;
      setOpen(false);
    };
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key === "Escape") { setOpen(false); button.current?.focus(); }
    };
    // Scrolling the pane would strand a fixed menu away from its button: close.
    const onScroll = (event: Event) => { if (!(event.target instanceof Node && menu.current?.contains(event.target))) setOpen(false); };
    document.addEventListener("mousedown", onMouseDown);
    document.addEventListener("keydown", onKeyDown);
    window.addEventListener("scroll", onScroll, true);
    window.addEventListener("resize", place);
    return () => {
      document.removeEventListener("mousedown", onMouseDown);
      document.removeEventListener("keydown", onKeyDown);
      window.removeEventListener("scroll", onScroll, true);
      window.removeEventListener("resize", place);
    };
  }, [open, place]);

  // Focus the first item once the menu is mounted.
  useEffect(() => {
    if (open) menu.current?.querySelector<HTMLElement>('[role="menuitem"]')?.focus();
  }, [open]);

  const onMenuKeyDown = (event: ReactKeyboardEvent<HTMLDivElement>) => {
    const entries = Array.from(menu.current?.querySelectorAll<HTMLElement>('[role="menuitem"]') ?? []);
    if (entries.length === 0) return;
    const index = entries.indexOf(document.activeElement as HTMLElement);
    let next: number | null = null;
    if (event.key === "ArrowDown") next = (index + 1) % entries.length;
    else if (event.key === "ArrowUp") next = (index - 1 + entries.length) % entries.length;
    else if (event.key === "Home") next = 0;
    else if (event.key === "End") next = entries.length - 1;
    else if (event.key === "Tab") {
      // The menu lives under <body>, so focus must go back to the trigger first; the browser's own
      // Tab/Shift+Tab (not prevented) then moves to the control after/before it.
      button.current?.focus();
      setOpen(false);
      return;
    }
    if (next === null) return;
    event.preventDefault();
    entries[next].focus();
  };

  return (
    <div className="relative">
      <button ref={button} type="button" aria-label={label} aria-haspopup="menu" aria-expanded={open} onClick={() => setOpen((v) => !v)}
        className="flex h-8 w-8 items-center justify-center rounded-control text-ink-2 hover:bg-bg-hover">
        <svg width="16" height="16" viewBox="0 0 16 16" fill="currentColor" aria-hidden="true"><circle cx="8" cy="3" r="1.4" /><circle cx="8" cy="8" r="1.4" /><circle cx="8" cy="13" r="1.4" /></svg>
      </button>
      {open ? createPortal(
        <div ref={menu} role="menu" aria-label={label} onKeyDown={onMenuKeyDown}
          style={{ position: "fixed", top: pos?.top ?? 0, left: pos?.left ?? 0, width: pos?.width ?? MENU_WIDTH, maxHeight: pos?.maxHeight, visibility: pos ? "visible" : "hidden" }}
          className="z-50 overflow-y-auto overflow-x-hidden rounded-card border border-line bg-bg font-sans text-pane text-ink shadow-row">
          {header ? <div className="border-b border-line-3 px-3 py-2.5"><p className="break-all text-pane-sm text-ink">{header}</p>{subheader ? <p className="text-pane-xs text-ink-3">{subheader}</p> : null}</div> : null}
          {items.map((item) => (
            <button key={item.label} role="menuitem" type="button" onClick={() => { setOpen(false); item.onSelect(); }}
              className={`block w-full px-3 py-2.5 text-left text-pane hover:bg-bg-hover ${item.destructive ? "text-bad" : "text-ink"}`}>
              {item.label}
            </button>
          ))}
        </div>,
        document.body,
      ) : null}
    </div>
  );
}

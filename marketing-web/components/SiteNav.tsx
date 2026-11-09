"use client";

import { useEffect, useId, useRef, useState } from "react";
import { WordmarkPrimary } from "./logos";
import { HEADER_H, REVEAL_EASE } from "./motion";

/* Sticky header. 58px tall — the same 58 that every anchor scroll
   subtracts (see HEADER_H in motion.ts); change one and change both.

   Every link below is a distinct, real destination — no two labels
   share a target. "Security" used to alias #pricing because the v4 cut
   folded security into the pricing band with no anchor of its own;
   SectionPricing now gives that block id="security", so the label can
   point at the thing it actually names.

   Below 860px the link row and the ghost "Talk to sales" are hidden
   (.cs-nav-links / .cs-nav-ghost in globals.css) and a hamburger
   (.cs-nav-burger) takes over — see MobileDrawer below. The solid
   waitlist CTA stays visible at every width; it's the one action that
   matters most on a phone. */

const NAV_LINKS = [
  { href: "#how", label: "Product" },
  { href: "#security", label: "Security" },
  { href: "#pricing", label: "Pricing" },
  { href: "#contact", label: "Contact" },
];

// How long the drawer's close transition runs (see the inline transform/
// opacity below) — the unmount timer and the CSS transition duration must
// stay in lockstep, so both read this one constant.
const DRAWER_CLOSE_MS = 240;

function BurgerIcon({ open }: { open: boolean }) {
  // Three bars that collapse into an X — CSS transforms only, no new
  // assets. `aria-hidden` because the accessible name lives on the
  // button itself (aria-label, set by the caller).
  const bar: React.CSSProperties = {
    position: "absolute",
    left: 7,
    right: 7,
    height: 1.5,
    background: "currentColor",
    borderRadius: 1,
    transition: "transform .2s ease, opacity .2s ease, top .2s ease",
  };
  return (
    <span aria-hidden="true" style={{ position: "relative", width: 20, height: 14, display: "block" }}>
      <span style={{ ...bar, top: open ? 6 : 1, transform: open ? "rotate(45deg)" : "none" }} />
      <span style={{ ...bar, top: 6, opacity: open ? 0 : 1 }} />
      <span style={{ ...bar, top: open ? 6 : 11, transform: open ? "rotate(-45deg)" : "none" }} />
    </span>
  );
}

export function SiteNav({ reduced }: { reduced: boolean }) {
  // `mounted` — the drawer exists in the DOM at all (open or animating
  // closed). `visible` — the drawer's open/closed *visual* state. Kept
  // separate so the close transition can finish before the panel
  // unmounts, the same way SectionAsk's meta line fades rather than
  // vanishing.
  const [mounted, setMounted] = useState(false);
  const [visible, setVisible] = useState(false);
  const drawerId = useId();
  const panelRef = useRef<HTMLDivElement>(null);
  const triggerRef = useRef<HTMLButtonElement>(null);
  const closeTimer = useRef<ReturnType<typeof setTimeout> | null>(null);

  function openDrawer() {
    if (closeTimer.current) {
      clearTimeout(closeTimer.current);
      closeTimer.current = null;
    }
    setMounted(true);
  }

  function closeDrawer() {
    setVisible(false);
    if (reduced) {
      setMounted(false);
      return;
    }
    closeTimer.current = setTimeout(() => {
      setMounted(false);
      closeTimer.current = null;
    }, DRAWER_CLOSE_MS);
  }

  function toggleDrawer() {
    if (mounted) closeDrawer();
    else openDrawer();
  }

  // Flip to the open state one frame after mounting — the same
  // double-rAF entrance trick Hero.tsx uses: the closed position has to
  // paint first, or there is nothing for the transition to animate away
  // from.
  useEffect(() => {
    if (!mounted) return;
    if (reduced) {
      setVisible(true);
      return;
    }
    const raf = requestAnimationFrame(() => requestAnimationFrame(() => setVisible(true)));
    return () => cancelAnimationFrame(raf);
  }, [mounted, reduced]);

  useEffect(
    () => () => {
      if (closeTimer.current) clearTimeout(closeTimer.current);
    },
    [],
  );

  // Body scroll lock for as long as the drawer is in the DOM (open or
  // closing) — a background page that keeps scrolling under an open
  // drawer is the single most common mobile-nav bug.
  useEffect(() => {
    if (!mounted) return;
    const prev = document.body.style.overflow;
    document.body.style.overflow = "hidden";
    return () => {
      document.body.style.overflow = prev;
    };
  }, [mounted]);

  // Focus management: move focus into the panel on open, trap Tab /
  // Shift+Tab inside it, close on Escape, and hand focus back to the
  // trigger button on the way out.
  useEffect(() => {
    if (!mounted) return;
    const panel = panelRef.current;
    if (!panel) return;

    const focusable = () =>
      Array.from(panel.querySelectorAll<HTMLElement>('a[href], button:not([disabled])'));

    (focusable()[0] ?? panel).focus();

    const onKeydown = (e: KeyboardEvent) => {
      if (e.key === "Escape") {
        e.preventDefault();
        closeDrawer();
        return;
      }
      if (e.key !== "Tab") return;
      const items = focusable();
      if (items.length === 0) return;
      const first = items[0];
      const last = items[items.length - 1];
      if (e.shiftKey && document.activeElement === first) {
        e.preventDefault();
        last.focus();
      } else if (!e.shiftKey && document.activeElement === last) {
        e.preventDefault();
        first.focus();
      }
    };

    document.addEventListener("keydown", onKeydown);
    return () => {
      document.removeEventListener("keydown", onKeydown);
      triggerRef.current?.focus();
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [mounted]);

  const panelTransition = reduced
    ? "none"
    : `transform .28s ${REVEAL_EASE}`;
  const overlayTransition = reduced ? "none" : "opacity .22s ease";

  return (
    <header
      style={{
        position: "sticky",
        top: 0,
        zIndex: 80,
        background: "var(--bg)",
        borderBottom: "1px solid var(--line)",
      }}
    >
      <div
        className="cs-rail cs-rail-inner"
        style={{
          height: HEADER_H,
          padding: "0 28px",
          display: "flex",
          alignItems: "center",
          gap: 30,
        }}
      >
        <a href="#top" aria-label="CoralSwarm — back to top" style={{ color: "var(--ink)", display: "flex", alignItems: "center" }}>
          <WordmarkPrimary size={17} />
        </a>

        <nav className="cs-nav-links">
          {NAV_LINKS.map((l) => (
            <a key={l.label} href={l.href} className="cs-navlink">
              {l.label}
            </a>
          ))}
        </nav>

        <div style={{ marginLeft: "auto", display: "flex", gap: 9, alignItems: "center" }}>
          <a
            href="#contact"
            className="cs-btn-ghost cs-nav-ghost"
            style={{ borderRadius: "var(--r-row)", padding: "6px 12px", font: "500 13px/1 var(--sans)" }}
          >
            Talk to sales
          </a>
          <a
            href="#contact"
            className="cs-btn-solid"
            style={{ borderRadius: "var(--r-row)", padding: "6px 12px", font: "500 13px/1 var(--sans)", whiteSpace: "nowrap" }}
          >
            Join the waitlist
          </a>

          <button
            ref={triggerRef}
            type="button"
            className="cs-nav-burger"
            aria-expanded={mounted}
            aria-controls={drawerId}
            aria-label={mounted ? "Close menu" : "Open menu"}
            onClick={toggleDrawer}
          >
            <BurgerIcon open={mounted} />
          </button>
        </div>
      </div>

      {mounted && (
        <div
          // Scrim: covers everything below the sticky header, which
          // stays above this (z-index 80 vs 79) so the burger — now an
          // X — remains reachable to close the drawer, same as Escape,
          // a link tap, or tapping the scrim itself.
          style={{
            position: "fixed",
            top: HEADER_H,
            left: 0,
            right: 0,
            bottom: 0,
            zIndex: 79,
            display: "flex",
            justifyContent: "flex-end",
            background: "var(--overlay-scrim)",
            opacity: visible ? 1 : 0,
            transition: overlayTransition,
          }}
          onClick={closeDrawer}
        >
          <div
            id={drawerId}
            ref={panelRef}
            role="dialog"
            // `aria-modal="true"` is doing double duty: besides the
            // semantic "this is a dialog", the ARIA spec has assistive
            // tech treat everything outside the dialog subtree as
            // effectively inert for their own navigation, which is why
            // there's no matching aria-hidden applied to the rest of
            // the page here — SiteNav doesn't own that markup (Page.tsx
            // renders the sections as its own siblings), and aria-modal
            // is the sanctioned way to get the same effect without it.
            aria-modal="true"
            aria-label="Site menu"
            tabIndex={-1}
            onClick={(e) => e.stopPropagation()}
            style={{
              width: "min(320px, 86vw)",
              height: "100%",
              background: "var(--bg)",
              borderLeft: "1px solid var(--line)",
              boxShadow: "var(--shadow-overlay)",
              padding: "28px 24px",
              display: "flex",
              flexDirection: "column",
              gap: 22,
              overflowY: "auto",
              transform: visible ? "translateX(0)" : "translateX(100%)",
              transition: panelTransition,
            }}
          >
            <div className="cs-eyebrow">menu</div>

            <nav style={{ display: "flex", flexDirection: "column" }}>
              {NAV_LINKS.map((l) => (
                <a
                  key={l.label}
                  href={l.href}
                  className="cs-drawer-link"
                  onClick={closeDrawer}
                >
                  {l.label}
                </a>
              ))}
              <a href="#contact" className="cs-drawer-link" onClick={closeDrawer}>
                Talk to sales
              </a>
            </nav>

            <a
              href="#contact"
              className="cs-btn-solid"
              onClick={closeDrawer}
              style={{
                marginTop: "auto",
                borderRadius: "var(--r-field)",
                padding: "12px 18px",
                font: "500 14px/1 var(--sans)",
              }}
            >
              Join the waitlist
            </a>
          </div>
        </div>
      )}
    </header>
  );
}

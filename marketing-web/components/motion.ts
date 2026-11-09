"use client";

import { useEffect, useState } from "react";

/* Shared motion primitives for the v3 marketing page.
 *
 * The design prototype expressed all of this as one imperative class
 * that reached across the whole document. Here each concern is a hook
 * or a helper, and the per-section mocks own their own observers — but
 * the timings, easings and thresholds are the prototype's, unchanged. */

/** Smoothstep between two progress values, clamped to 0..1. */
export function ss(p: number, a: number, b: number): number {
  const t = Math.min(1, Math.max(0, (p - a) / (b - a)));
  return t * t * (3 - 2 * t);
}

/** The one reveal easing used across the page. */
export const REVEAL_EASE = "cubic-bezier(.19,1,.22,1)";

/**
 * True when the visitor has asked for reduced motion.
 *
 * Starts false so server and first client render agree (the media
 * query is not knowable during SSR/prerender); the effect corrects it
 * before paint on the client. Every consumer treats "false" as the
 * animated path, so a reduced-motion visitor sees at most one frame of
 * the animated state before it resolves.
 */
export function useReducedMotion(): boolean {
  const [reduced, setReduced] = useState(false);

  useEffect(() => {
    const mq = window.matchMedia("(prefers-reduced-motion: reduce)");
    setReduced(mq.matches);
    const onChange = (e: MediaQueryListEvent) => setReduced(e.matches);
    mq.addEventListener("change", onChange);
    return () => mq.removeEventListener("change", onChange);
  }, []);

  return reduced;
}

/** Height of the sticky header, subtracted from every anchor target. */
export const HEADER_H = 58;

/**
 * In-page anchor navigation.
 *
 * Every link on this page is a fragment link, so one delegated handler
 * covers all of them. Deliberately NOT scrollIntoView: the sticky
 * header would cover the top of each section.
 *
 * The `if (!target) return` guard matters — it lets an unresolvable
 * fragment fall through to the browser's default behaviour instead of
 * throwing inside the handler. (The v3 design shipped two links to a
 * `#sources` id that does not exist; those now point at `#how`, but the
 * guard keeps a future typo from breaking every other link on the page,
 * since one uncaught error here would kill the whole delegated
 * listener.)
 */
export function useSmoothNav(reduced: boolean): void {
  useEffect(() => {
    const onClick = (e: MouseEvent) => {
      // Let modified clicks (new tab, download, etc.) behave natively.
      if (e.defaultPrevented || e.button !== 0 || e.metaKey || e.ctrlKey || e.shiftKey || e.altKey) {
        return;
      }
      const el = e.target as Element | null;
      const a = el && el.closest ? el.closest('a[href^="#"]') : null;
      if (!a) return;

      const href = a.getAttribute("href") || "";
      const id = href.slice(1);
      if (!id) return;

      const target = id === "top" ? document.body : document.getElementById(id);
      if (!target) return;

      e.preventDefault();
      const y = id === "top" ? 0 : target.getBoundingClientRect().top + window.scrollY - HEADER_H;
      window.scrollTo({ top: Math.max(0, y), behavior: reduced ? "auto" : "smooth" });
      if (history.replaceState) history.replaceState(null, "", "#" + id);
    };

    document.addEventListener("click", onClick);
    return () => document.removeEventListener("click", onClick);
  }, [reduced]);
}

/**
 * One-shot reveal for every [data-reveal] element: fade up 16px over
 * .8s once 15% of it is visible, honouring an optional data-delay in
 * milliseconds. Unobserved after firing.
 *
 * Fail-visible: because `reduced` starts false and is only corrected
 * once the media query resolves (see useReducedMotion above), this
 * effect can run once in the animated branch — hiding elements — and
 * then rerun in the reduced branch before the reveal observer ever
 * fired. The reduced branch below explicitly clears any inline hidden
 * state it (or the previous run) may have left behind, rather than
 * just bailing out, so a reduced-motion visitor is never stuck looking
 * at permanently-invisible content.
 */
export function useReveal(reduced: boolean): void {
  useEffect(() => {
    const els = Array.from(document.querySelectorAll<HTMLElement>("[data-reveal]"));

    if (reduced) {
      els.forEach((el) => {
        el.style.transition = "";
        el.style.opacity = "";
        el.style.transform = "";
      });
      return;
    }

    els.forEach((el) => {
      el.style.opacity = "0";
      el.style.transform = "translateY(16px)";
    });

    const io = new IntersectionObserver(
      (entries) => {
        entries.forEach((entry) => {
          if (!entry.isIntersecting) return;
          const el = entry.target as HTMLElement;
          io.unobserve(el);
          const d = Number(el.getAttribute("data-delay") || 0);
          el.style.transition =
            `opacity .8s ${REVEAL_EASE} ${d}ms, transform .8s ${REVEAL_EASE} ${d}ms`;
          requestAnimationFrame(() => {
            el.style.opacity = "1";
            el.style.transform = "none";
          });
        });
      },
      { threshold: 0.15 },
    );

    els.forEach((el) => io.observe(el));
    return () => io.disconnect();
  }, [reduced]);
}

/**
 * Run `play` once, the first time `el` is at least 35% visible.
 * Every in-view mock on the page uses this.
 */
export function playOnceInView(
  el: Element,
  play: () => void,
  threshold = 0.35,
): () => void {
  let done = false;
  const io = new IntersectionObserver(
    (entries) => {
      entries.forEach((entry) => {
        if (!entry.isIntersecting || done) return;
        done = true;
        io.unobserve(entry.target);
        play();
      });
    },
    { threshold },
  );
  io.observe(el);
  return () => io.disconnect();
}

/**
 * Split an element into per-character spans so text can be typed in.
 *
 * Elements marked [data-unit] (the citation chips) are kept whole —
 * a chip appears in one step rather than being torn into characters.
 * Returns the ordered list of units; `setUnits` then reveals a prefix.
 */
export function prepUnits(el: Element | null): HTMLElement[] {
  const units: HTMLElement[] = [];
  if (!el) return units;

  const walk = (node: Node) => {
    Array.from(node.childNodes).forEach((n) => {
      if (n.nodeType === Node.TEXT_NODE) {
        const frag = document.createDocumentFragment();
        for (const ch of n.textContent || "") {
          const s = document.createElement("span");
          s.textContent = ch;
          s.style.visibility = "hidden";
          frag.appendChild(s);
          units.push(s);
        }
        node.replaceChild(frag, n);
      } else if (n.nodeType === Node.ELEMENT_NODE) {
        const e = n as HTMLElement;
        if (e.hasAttribute("data-unit")) {
          e.style.visibility = "hidden";
          units.push(e);
        } else {
          walk(e);
        }
      }
    });
  };

  walk(el);
  return units;
}

/** Reveal the first `n` units, hide the rest. */
export function setUnits(units: HTMLElement[], n: number): void {
  units.forEach((u, i) => {
    u.style.visibility = i < n ? "visible" : "hidden";
  });
}

/** The five hues, read from the live token values with literal fallbacks. */
export function readHues(): string[] {
  const cs = getComputedStyle(document.documentElement);
  const tok = (name: string, fallback: string) =>
    cs.getPropertyValue(name).trim() || fallback;
  return [
    tok("--hue-coral", "#f77764"),
    tok("--hue-pink", "#eb7290"),
    tok("--hue-amber", "#f4b860"),
    tok("--hue-teal", "#2a9d8f"),
    tok("--hue-plum", "#7c5295"),
  ];
}

/** A single token read, for the non-hue colours the canvases need. */
export function readToken(name: string, fallback: string): string {
  return getComputedStyle(document.documentElement).getPropertyValue(name).trim() || fallback;
}

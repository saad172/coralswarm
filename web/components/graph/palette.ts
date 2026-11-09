/* Design-token → plain-colour bridge for the WebGL canvas.

   Sigma paints WebGL, not CSS, so `var(--hue-coral)` means nothing to it;
   these read the tokens off `:root` once and hand Sigma concrete values.
   Kept free of any Sigma import so the page (which builds the graph model
   before the canvas has loaded) can use it without pulling WebGL code into
   the build-time prerender. */

import { DEFAULT_PALETTE, fade, type Palette } from "./toGraphology";

export interface CanvasPalette extends Palette {
  /** Concrete family list for Sigma's Canvas-2D label layer. */
  sans: string;
}

/** Resolve the tokens at call time; tokens don't change at runtime, so
 *  callers memoise the result. Safe to call during prerender (falls back). */
export function readPalette(): CanvasPalette {
  if (typeof document === "undefined") return { ...DEFAULT_PALETTE, sans: "system-ui, sans-serif" };
  const cs = getComputedStyle(document.documentElement);
  const g = (n: string, fallback: string) => cs.getPropertyValue(n).trim() || fallback;
  return {
    coral: g("--hue-coral", DEFAULT_PALETTE.coral),
    pink: g("--hue-pink", DEFAULT_PALETTE.pink),
    amber: g("--hue-amber", DEFAULT_PALETTE.amber),
    teal: g("--hue-teal", DEFAULT_PALETTE.teal),
    plum: g("--hue-plum", DEFAULT_PALETTE.plum),
    ink: g("--ink", DEFAULT_PALETTE.ink),
    ink2: g("--ink-2", DEFAULT_PALETTE.ink2),
    ink3: g("--ink-3", DEFAULT_PALETTE.ink3),
    line: g("--line", DEFAULT_PALETTE.line),
    bg: g("--bg", DEFAULT_PALETTE.bg),
    // Canvas 2D wants a concrete family list, not a `var()` chain — read the
    // next/font variable directly rather than `--sans`.
    sans: `${g("--font-sans", "system-ui")}, system-ui, sans-serif`,
  };
}

/** Same colour with its alpha multiplied by `a` — one implementation shared
 *  with the adapter (`fade`) so a halo node already at 58% dims further under
 *  hover instead of passing through unchanged. */
export const withAlpha = fade;

"use client";

import { useEffect, useRef } from "react";
import { cn } from "./cn";
import { HUES } from "./hue";

/* SwarmStream — the one motion exception in the design system.

   Particles in `lanes` drift left to right, pinch toward the middle, and are
   swallowed by a DotMandala that breathes and counter-rotates its rings; what
   leaves the far side is coral. It says "things flowing into your ocean" on
   the MCP band, and nowhere else.

   Canvas, not SVG: a hundred-odd moving dots would be a hundred-odd nodes. The
   ring geometry is DotMandala's (Mark.tsx), duplicated rather than imported
   because that component renders static SVG and this one needs per-frame
   angles. Colours are read from the `--hue-*` tokens off :root (a canvas can't
   resolve `var()`), re-read every second so a theme change lands.

   Under `prefers-reduced-motion` it draws one settled frame and schedules
   nothing. The RAF and the observer are torn down on unmount. */

const RINGS = [
  { r: 0, n: 1, s: 0.085 },
  { r: 0.14, n: 6, s: 0.06 },
  { r: 0.25, n: 10, s: 0.05 },
  { r: 0.35, n: 14, s: 0.042 },
  { r: 0.45, n: 18, s: 0.034 },
];

/** Frames simulated up front so the first paint already has a stream in it. */
const WARM_FRAMES = 160;
const MAX_PARTICLES = 150;

type Particle = { x: number; lane: number; sp: number; c: number; r: number; hit?: boolean };

export function SwarmStream({ lanes = 9, gap = 13, className }: { lanes?: number; gap?: number; className?: string }) {
  const ref = useRef<HTMLCanvasElement>(null);

  useEffect(() => {
    const cv = ref.current;
    const host = cv?.parentElement;
    const ctx = cv?.getContext("2d");
    // jsdom and locked-down browsers have no 2d context; draw nothing.
    if (!cv || !host || !ctx) return;

    const reduce = window.matchMedia?.("(prefers-reduced-motion: reduce)").matches ?? false;
    let hues: string[] = [];
    let coral = "";
    let lastRead = -10;
    let W = 0;
    let H = 0;
    let raf = 0;
    let alive = true;
    let parts: Particle[] = [];
    let last = 0;
    let pulse = 0;

    const readTokens = () => {
      const cs = getComputedStyle(document.documentElement);
      // A missing token falls back to the inherited text colour, never a literal.
      hues = HUES.map((h) => cs.getPropertyValue(`--hue-${h}`).trim() || cs.color);
      coral = hues[0];
    };
    const fit = () => {
      const r = host.getBoundingClientRect();
      const d = window.devicePixelRatio || 1;
      W = r.width;
      H = r.height;
      cv.width = W * d;
      cv.height = H * d;
      ctx.setTransform(d, 0, 0, d, 0, 0);
    };
    const mandala = (cx: number, cy: number, S: number, t: number) =>
      RINGS.forEach((ring, ri) => {
        const w = ri ? (0.22 / ri) * (ri % 2 ? -1 : 1) : 0;
        for (let i = 0; i < ring.n; i++) {
          const a = ri === 0 ? 0 : (i / ring.n) * Math.PI * 2 + ri * 0.4 + t * w;
          const br = 1 + 0.04 * Math.sin(t * 1.6 - ri * 0.7) + pulse * (ri === 0 ? 0.25 : 0.06);
          ctx.fillStyle = hues[(i + ri) % hues.length];
          ctx.beginPath();
          ctx.arc(cx + Math.cos(a) * ring.r * S * br, cy + Math.sin(a) * ring.r * S * br, ring.s * S, 0, Math.PI * 2);
          ctx.fill();
        }
      });

    const frame = (ms: number) => {
      const t = ms / 1000;
      const dt = Math.min(0.05, t - (last || t));
      last = t;
      if (Math.abs(t - lastRead) > 1) {
        readTokens();
        lastRead = t;
      }
      ctx.clearRect(0, 0, W, H);
      pulse = Math.max(0, pulse - dt * 2.5);
      const cx = W * 0.5;
      const cy = H * 0.46;
      const S = H * 0.62;
      const g = Math.min(gap, H / (lanes + 3));
      if (parts.length < MAX_PARTICLES && Math.random() < dt * 22 * Math.min(1, W / 900)) {
        const lane = Math.floor(Math.random() * lanes);
        parts.push({ x: -8, lane, sp: 70 + Math.random() * 50, c: lane % hues.length, r: 1.8 + Math.random() * 1.3 });
      }
      parts = parts.filter((p) => p.x < W + 10);
      parts.forEach((p) => {
        p.x += p.sp * dt * (Math.abs(p.x - cx) < S * 0.4 ? 0.55 : 1);
        const bump = Math.exp(-(((p.x - cx) / (W * 0.16)) ** 2));
        const y = cy + (p.lane - (lanes - 1) / 2) * g * (1 - bump);
        const hidden = Math.abs(p.x - cx) < S * 0.36;
        if (hidden && !p.hit) {
          p.hit = true;
          pulse = Math.min(1, pulse + 0.12);
        }
        if (hidden) return;
        ctx.globalAlpha = Math.max(0, Math.min(1, (p.x + 8) / 60, (W + 10 - p.x) / 60));
        ctx.fillStyle = p.x > cx ? coral : hues[p.c];
        ctx.beginPath();
        ctx.arc(p.x, y, p.x > cx ? p.r * 1.15 : p.r, 0, Math.PI * 2);
        ctx.fill();
      });
      ctx.globalAlpha = 1;
      mandala(cx, cy, S, t);
    };
    const loop = (ms: number) => {
      if (!alive) return;
      frame(ms);
      raf = requestAnimationFrame(loop);
    };

    fit();
    const ro = typeof ResizeObserver === "undefined" ? null : new ResizeObserver(() => {
      fit();
      // Resizing clears the bitmap; reduced motion has no animation loop
      // to paint it again, including the observer’s initial notification.
      if (reduce) frame(last * 1000);
    });
    ro?.observe(host);
    const now = performance.now();
    for (let i = 0; i < WARM_FRAMES; i++) frame(now - (WARM_FRAMES - i) * 33);
    if (!reduce) raf = requestAnimationFrame(loop);

    return () => {
      alive = false;
      cancelAnimationFrame(raf);
      ro?.disconnect();
    };
  }, [lanes, gap]);

  return <canvas ref={ref} aria-hidden="true" className={cn("block h-full w-full", className)} />;
}

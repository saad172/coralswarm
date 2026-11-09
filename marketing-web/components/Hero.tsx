"use client";

import { useEffect, useRef } from "react";
import { readHues, readToken, ss, REVEAL_EASE } from "./motion";

/* The hero: one screen, self-playing, no scroll coupling.
 *
 * A 7-second load-triggered timeline runs three phases, each dot lerping
 * from the previous position:
 *
 *   lanes     dots drift left-to-right along three labelled lanes
 *   clusters  t1 = ss(p, .20, .48) — they pull into five hue clusters
 *   mandala   t2 = ss(p, .56, .90) — they snap into the brand mark
 *
 * A residual ±3.2px jitter (damped to 20% by the end) keeps the mark
 * alive after it forms. The caption swaps at p = .42 and p = .72.
 *
 * One deliberate departure from the prototype: the block is sized in
 * `svh`, not `vh`. On iOS Safari a collapsing URL bar changes `vh`
 * mid-scroll, which would resize the canvas and reflow the whole
 * composition under the reader's thumb. `svh` is fixed to the
 * URL-bar-visible height, so the hero never resizes while scrolling. */

const DURATION_MS = 7000;

const CAPTIONS = [
  "raw material arrives on its own",
  "the swarm splits it into corals",
  "corals group into a reef — the reef is the logo",
];
const LANES = ["slack #eng", "meeting · 12 mar", "claude code session"];
const CLUSTERS = ["decisions", "people", "sources", "meetings", "references"];

/** [radius fraction, dot count, dot size fraction] — 52 dots in five rings. */
const RINGS: [number, number, number][] = [
  [0, 1, 0.085],
  [0.075, 6, 0.06],
  [0.135, 10, 0.05],
  [0.19, 14, 0.042],
  [0.245, 21, 0.034],
];

type Dot = {
  ri: number;
  ii: number;
  n: number;
  f: number;
  s: number;
  hue: number;
  lane: number;
  seed: number;
  j: number;
};

function buildDots(): Dot[] {
  const dots: Dot[] = [];
  const perHue = [0, 0, 0, 0, 0];
  let k = 0;
  RINGS.forEach(([f, count, s], ri) => {
    for (let i = 0; i < count; i++) {
      const hue = (i + ri) % 5;
      dots.push({
        ri,
        ii: i,
        n: count,
        f,
        s,
        hue,
        lane: k % 3,
        seed: (k * 0.618034) % 1,
        j: perHue[hue]++,
      });
      k++;
    }
  });
  return dots;
}

export function Hero({ reduced }: { reduced: boolean }) {
  const boxRef = useRef<HTMLDivElement>(null);
  const canvasRef = useRef<HTMLCanvasElement>(null);
  const captionRef = useRef<HTMLDivElement>(null);

  /* Headline entrance — the two masked lines rise in, then the coral
     accent, the sub and the CTA row fade in behind them.
     Fail-visible: `reduced` starts false (see useReducedMotion), so this
     effect can hide the lines/accent/fades in the animated branch and
     then rerun in the reduced branch before the entrance transition
     ever fires. That branch explicitly clears the inline styles rather
     than just returning, so the headline, subhead and CTAs can never be
     left permanently invisible. */
  useEffect(() => {
    const box = boxRef.current;
    if (!box) return;

    const lines = Array.from(box.querySelectorAll<HTMLElement>("[data-line]"));
    const accent = box.querySelector<HTMLElement>("[data-accent]");
    const fades = [
      box.querySelector<HTMLElement>("[data-sub]"),
      box.querySelector<HTMLElement>("[data-cta]"),
    ];

    if (reduced) {
      lines.forEach((l) => {
        l.style.transition = "";
        l.style.transform = "";
      });
      if (accent) {
        accent.style.transition = "";
        accent.style.opacity = "";
      }
      fades.forEach((e) => {
        if (e) {
          e.style.transition = "";
          e.style.opacity = "";
        }
      });
      return;
    }

    lines.forEach((l) => {
      l.style.transform = "translateY(112%)";
    });
    if (accent) accent.style.opacity = "0";
    fades.forEach((e) => {
      if (e) e.style.opacity = "0";
    });

    const raf = requestAnimationFrame(() =>
      requestAnimationFrame(() => {
        lines.forEach((l, i) => {
          l.style.transition = `transform .9s ${REVEAL_EASE} ${i * 110}ms`;
          l.style.transform = "none";
        });
        if (accent) {
          accent.style.transition = "opacity .6s ease .85s";
          accent.style.opacity = "1";
        }
        fades.forEach((e, i) => {
          if (e) {
            e.style.transition = `opacity .8s ease ${700 + i * 120}ms`;
            e.style.opacity = "1";
          }
        });
      }),
    );
    return () => cancelAnimationFrame(raf);
  }, [reduced]);

  /* The swarm canvas. */
  useEffect(() => {
    const box = boxRef.current;
    const canvas = canvasRef.current;
    if (!box || !canvas) return;
    const ctx = canvas.getContext("2d");
    if (!ctx) return;

    const hues = readHues();
    const lineC = readToken("--line", "#e4e6ea");
    const ink3 = readToken("--ink-3", "#8a9099");
    // next/font generates the family name, so read it rather than naming
    // "JetBrains Mono" — otherwise the canvas labels silently fall back
    // to the generic monospace face.
    const monoFamily = readToken("--font-mono", "ui-monospace");

    const dots = buildDots();
    const dpr = Math.min(2, window.devicePixelRatio || 1);
    const t0 = performance.now();
    let beat = -1;

    const draw = () => {
      const tm = (performance.now() - t0) / 1000;
      const w = box.clientWidth;
      const h = box.clientHeight;
      if (!w || !h) return;

      const pw = Math.round(w * dpr);
      const ph = Math.round(h * dpr);
      if (canvas.width !== pw || canvas.height !== ph) {
        canvas.width = pw;
        canvas.height = ph;
      }
      ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
      ctx.clearRect(0, 0, w, h);

      // Reduced motion renders the finished mark, once.
      const p = reduced ? 1 : Math.min(1, (performance.now() - t0) / DURATION_MS);

      // 22px dot grid — the design system's .reef-bg texture.
      ctx.globalAlpha = 0.9;
      ctx.fillStyle = lineC;
      for (let x = 11; x < w; x += 22) {
        for (let y = 11; y < h; y += 22) ctx.fillRect(x - 0.75, y - 0.75, 1.5, 1.5);
      }
      ctx.globalAlpha = 1;

      const R = Math.min(w, h) * 0.62;
      const t1 = ss(p, 0.2, 0.48);
      const t2 = ss(p, 0.56, 0.9);
      const mcx = w / 2;
      const mcy = h * 0.7;

      const CC = CLUSTERS.map((_, i) => {
        const a = -Math.PI / 2 + (i * Math.PI * 2) / 5;
        return [
          w / 2 + Math.cos(a) * Math.min(w * 0.28, 280),
          h * 0.68 + Math.sin(a) * h * 0.16,
        ] as [number, number];
      });

      for (const d of dots) {
        const laneY = h * (0.58 + d.lane * 0.12) + Math.sin(d.seed * 40 + tm * 0.9) * 10;
        let x = (((d.seed * 1.4 + tm * 0.03) % 1.4) - 0.2) * w;
        let y = laneY;

        // Cluster seat — sunflower packing inside the hue's cluster.
        const cr = 12 * Math.sqrt(d.j);
        const ca = d.j * 2.399;
        const bx = CC[d.hue][0] + Math.cos(ca) * cr;
        const by = CC[d.hue][1] + Math.sin(ca) * cr;

        // Mandala seat, slowly rotating.
        const ma = d.ri === 0 ? 0 : (d.ii / d.n) * Math.PI * 2 + d.ri * 0.4 + tm * 0.06;
        const cx2 = mcx + Math.cos(ma) * d.f * R;
        const cy2 = mcy + Math.sin(ma) * d.f * R;

        x += (bx - x) * t1;
        y += (by - y) * t1;
        x += (cx2 - x) * t2;
        y += (cy2 - y) * t2;

        const amp = 3.2 * (1 - t2 * 0.8);
        x += Math.cos(tm * 0.55 + d.seed * 39) * amp;
        y += Math.sin(tm * 0.7 + d.seed * 53) * amp;

        const r = 3.2 + (d.s * R * 0.5 - 3.2) * t2;

        ctx.fillStyle = hues[d.hue];
        ctx.beginPath();
        ctx.arc(x, y, r, 0, Math.PI * 2);
        ctx.fill();
      }

      ctx.font = `10px ${monoFamily}, monospace`;

      const laneAlpha = 1 - t1;
      if (laneAlpha > 0.01) {
        ctx.globalAlpha = laneAlpha;
        ctx.fillStyle = ink3;
        ctx.textAlign = "left";
        LANES.forEach((s, i) =>
          ctx.fillText(s.toUpperCase(), w * 0.06, h * (0.58 + i * 0.12) - 20),
        );
      }

      const clusterAlpha = t1 * (1 - t2);
      if (clusterAlpha > 0.01) {
        ctx.globalAlpha = clusterAlpha;
        ctx.textAlign = "center";
        CLUSTERS.forEach((s, i) => {
          ctx.fillStyle = hues[i];
          ctx.fillText(s.toUpperCase(), CC[i][0], CC[i][1] + 52);
        });
      }
      ctx.globalAlpha = 1;

      const b = p < 0.42 ? 0 : p < 0.72 ? 1 : 2;
      if (b !== beat) {
        beat = b;
        if (captionRef.current) captionRef.current.textContent = CAPTIONS[b];
      }
    };

    const onResize = () => draw();
    window.addEventListener("resize", onResize);
    draw();

    // The lane and cluster labels are painted with the real mono face,
    // so repaint once it has loaded.
    if (document.fonts && document.fonts.ready) {
      document.fonts.ready.then(() => draw()).catch(() => {});
    }

    if (reduced) {
      return () => window.removeEventListener("resize", onResize);
    }

    // Paint only while the hero is on screen.
    let visible = true;
    let loop = 0;
    const tick = () => {
      if (visible) draw();
      loop = requestAnimationFrame(tick);
    };
    const io = new IntersectionObserver((entries) =>
      entries.forEach((e) => {
        visible = e.isIntersecting;
      }),
    );
    io.observe(box);
    loop = requestAnimationFrame(tick);

    return () => {
      window.removeEventListener("resize", onResize);
      cancelAnimationFrame(loop);
      io.disconnect();
    };
  }, [reduced]);

  return (
    <div
      id="top"
      ref={boxRef}
      style={{
        position: "relative",
        height: "100svh",
        minHeight: 640,
        background: "var(--bg-2)",
        overflow: "hidden",
      }}
    >
      <canvas
        ref={canvasRef}
        aria-hidden="true"
        style={{ position: "absolute", inset: 0, width: "100%", height: "100%" }}
      />

      <div
        style={{
          position: "absolute",
          left: 0,
          right: 0,
          top: "14vh",
          zIndex: 2,
          display: "flex",
          flexDirection: "column",
          alignItems: "center",
          textAlign: "center",
          padding: "0 24px",
        }}
      >
        <h1
          style={{
            margin: 0,
            font: "400 clamp(44px,6.2vw,76px)/1.08 var(--display)",
            letterSpacing: "var(--tracking-title)",
            color: "var(--ink)",
            maxWidth: 960,
            textWrap: "balance",
          }}
        >
          <span style={{ display: "block", overflow: "hidden", paddingBottom: ".09em", marginBottom: "-.09em" }}>
            <span data-line style={{ display: "block" }}>Organizational memory — automated.</span>
          </span>
          <span style={{ display: "block", overflow: "hidden", paddingBottom: ".09em", marginBottom: "-.09em" }}>
            <span data-line style={{ display: "block" }}>
              For humans{" "}
              <em data-accent style={{ fontStyle: "italic", color: "var(--brand)" }}>and AI</em>.
            </span>
          </span>
        </h1>

        <p
          data-sub
          style={{
            font: "400 16px/1.6 var(--sans)",
            color: "var(--ink-2)",
            margin: "20px 0 0",
            maxWidth: 560,
            textWrap: "pretty",
          }}
        >
          It captures what your team already produces, turns it into topics you confirm, and answers
          questions — with citations — for people and agents alike.
        </p>

        <div
          data-cta
          style={{
            margin: "26px 0 0",
            display: "flex",
            alignItems: "center",
            justifyContent: "center",
            gap: 10,
            flexWrap: "wrap",
          }}
        >
          <a
            href="#contact"
            className="cs-btn-solid"
            style={{ borderRadius: "var(--r-field)", font: "500 14px/1 var(--sans)", padding: "12px 22px" }}
          >
            Join the waitlist
          </a>
          <a
            href="#how"
            className="cs-btn-quiet"
            style={{ borderRadius: "var(--r-field)", font: "500 14px/1 var(--sans)", padding: "12px 20px" }}
          >
            How it works
          </a>
        </div>
      </div>

      <div
        ref={captionRef}
        style={{
          position: "absolute",
          left: 0,
          right: 0,
          bottom: 30,
          zIndex: 2,
          textAlign: "center",
          font: "var(--type-mono-chip)",
          fontFamily: "var(--mono)",
          letterSpacing: ".06em",
          color: "var(--ink-3)",
        }}
      >
        {CAPTIONS[0]}
      </div>
    </div>
  );
}

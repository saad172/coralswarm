"use client";

import { useEffect, useRef } from "react";
import { ConnectorRing } from "./ConnectorRing";
import { playOnceInView, readHues, ss } from "./motion";

/* Block 3 — "how it works", three cards on one grey band.
 *
 * This is the compression the v4 cut is built on: v3's seven numbered
 * explainer sections become connect · corals · reefs. Each card's mock
 * is pushed to the bottom with `margin-top: auto` so all three align
 * regardless of how much copy sits above them. */

const cardStyle: React.CSSProperties = {
  background: "var(--bg)",
  border: "1px solid var(--line)",
  boxShadow: "var(--shadow-card)",
  borderRadius: "var(--r-card)",
  padding: 22,
  display: "flex",
  flexDirection: "column",
  gap: 11,
  overflow: "hidden",
};
const h3Style: React.CSSProperties = {
  font: "400 21px/1.25 var(--display)",
  letterSpacing: "var(--tracking-title)",
  color: "var(--ink)",
  margin: 0,
};
const bodyStyle: React.CSSProperties = {
  font: "400 13.5px/1.65 var(--sans)",
  color: "var(--ink-2)",
  margin: 0,
  textWrap: "pretty",
};
const quietStyle: React.CSSProperties = {
  background: "var(--bg-subtle)",
  border: "1px solid var(--line-2)",
  borderRadius: "var(--r-card)",
  padding: "14px 16px",
};

/* ---------- card 02 · corals ---------- */

const CORALS: { kind: string; hue: string; text: string; at: string }[] = [
  { kind: "decision", hue: "var(--hue-coral)", text: "AEC3 replaces DTLN in production", at: "00:41:12" },
  { kind: "decision", hue: "var(--hue-coral)", text: "Park the DTLN archive", at: "00:41:36" },
  { kind: "person", hue: "var(--hue-pink)", text: "Priya owns echo-cancel", at: "00:12:05" },
  { kind: "thread", hue: "var(--hue-teal)", text: "Echo-return follow-ups", at: "00:27:48" },
  { kind: "reference", hue: "var(--hue-plum)", text: "Far-end test, −45 dB", at: "00:09:31" },
  { kind: "reference", hue: "var(--hue-plum)", text: "ONNX drift after ~40 s", at: "00:38:54" },
];

function CoralsMock({ reduced }: { reduced: boolean }) {
  const ref = useRef<HTMLDivElement>(null);

  useEffect(() => {
    const el = ref.current;
    if (!el || reduced) return;

    const rows = Array.from(el.querySelectorAll<HTMLElement>("[data-cchip]"));
    rows.forEach((r) => {
      r.style.opacity = "0";
      r.style.transform = "translateY(-10px)";
    });

    const timers: ReturnType<typeof setTimeout>[] = [];
    const stop = playOnceInView(
      el,
      () => {
        rows.forEach((r, i) => {
          timers.push(
            setTimeout(() => {
              r.style.transition =
                "opacity .38s cubic-bezier(.25,.1,.25,1), transform .38s cubic-bezier(.25,.1,.25,1)";
              r.style.opacity = "1";
              r.style.transform = "none";
            }, 200 + i * 130),
          );
        });
      },
      0.3,
    );

    return () => {
      stop();
      timers.forEach(clearTimeout);
    };
  }, [reduced]);

  return (
    <div ref={ref} data-mock="corals" style={quietStyle}>
      <div
        style={{
          display: "flex",
          alignItems: "center",
          gap: 10,
          padding: "4px 0 12px",
          borderBottom: "1px solid var(--line-2)",
        }}
      >
        <span style={{ width: 7, height: 7, borderRadius: "var(--r-full)", background: "var(--hue-teal)", flex: "none" }} />
        <span style={{ font: "500 13px/1.4 var(--sans)", color: "var(--ink)" }}>Audio sync — weekly</span>
        <span
          style={{
            font: "var(--type-mono-meta)",
            fontFamily: "var(--mono)",
            color: "var(--ink-3)",
            marginLeft: "auto",
            flex: "none",
          }}
        >
          1 source → 6 corals
        </span>
      </div>

      <div style={{ display: "flex", flexDirection: "column", gap: 8, padding: "13px 0 2px" }}>
        {CORALS.map((c) => (
          <div key={c.text} data-cchip style={{ display: "flex", gap: 10, alignItems: "baseline" }}>
            <span
              style={{
                font: "600 9.5px/1 var(--mono)",
                letterSpacing: ".08em",
                textTransform: "uppercase",
                color: c.hue,
                flex: "none",
                minWidth: 60,
              }}
            >
              {c.kind}
            </span>
            <span style={{ font: "400 12.5px/1.5 var(--sans)", color: "var(--ink)" }}>{c.text}</span>
            <span
              style={{
                font: "var(--type-mono-meta)",
                fontFamily: "var(--mono)",
                color: "var(--ink-4)",
                marginLeft: "auto",
                flex: "none",
              }}
            >
              {c.at}
            </span>
          </div>
        ))}
      </div>

      <div
        data-cchip
        style={{
          padding: "9px 11px",
          marginTop: 12,
          borderRadius: "var(--r-row)",
          background: "color-mix(in srgb, var(--hue-amber) 10%, transparent)",
          font: "400 10.5px/1.4 var(--mono)",
          color: "color-mix(in srgb, var(--hue-amber) 55%, var(--ink))",
        }}
      >
        proposed for your review — you confirm. Nothing arrives on its own.
      </div>
    </div>
  );
}

/* ---------- card 03 · reefs ---------- */

const REEF_RINGS: [number, number, number][] = [
  [0, 1, 0.085],
  [0.14, 6, 0.06],
  [0.25, 10, 0.05],
  [0.35, 14, 0.042],
  [0.45, 18, 0.034],
];
const REEF_CYCLE = 9000;
const REEF_LABELS = ["corals…", "grouping…", "reef confirmed — this is the logo"];

function ReefMock({ reduced }: { reduced: boolean }) {
  const ref = useRef<HTMLDivElement>(null);
  const canvasRef = useRef<HTMLCanvasElement>(null);
  const statusRef = useRef<HTMLSpanElement>(null);

  useEffect(() => {
    const wrap = ref.current;
    const canvas = canvasRef.current;
    if (!wrap || !canvas) return;
    const ctx = canvas.getContext("2d");
    if (!ctx) return;

    const hues = readHues();

    const dots: { ri: number; ii: number; n: number; f: number; s: number; hue: number; seed: number }[] = [];
    let k = 0;
    REEF_RINGS.forEach(([f, count, s], ri) => {
      for (let i = 0; i < count; i++) {
        dots.push({ ri, ii: i, n: count, f, s, hue: (i + ri) % 5, seed: (k * 0.618034) % 1 });
        k++;
      }
    });

    const dpr = Math.min(2, window.devicePixelRatio || 1);
    const t0 = performance.now();
    let last = -1;

    const draw = () => {
      const w = canvas.clientWidth;
      const h = canvas.clientHeight;
      if (!w || !h) return;
      if (canvas.width !== Math.round(w * dpr)) {
        canvas.width = Math.round(w * dpr);
        canvas.height = Math.round(h * dpr);
      }
      ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
      ctx.clearRect(0, 0, w, h);

      const q = reduced ? 0.9 : ((performance.now() - t0) % REEF_CYCLE) / REEF_CYCLE;
      const t1 = ss(q, 0.18, 0.34);
      const t2 = ss(q, 0.5, 0.68);
      const out = ss(q, 0.93, 1);
      const M = Math.min(w, h) * 0.92;
      const cx = w / 2;
      const cy = h / 2;

      const CC = [0, 1, 2, 3, 4].map((i) => {
        const a = -Math.PI / 2 + (i * Math.PI * 2) / 5;
        return [cx + Math.cos(a) * w * 0.3, cy + Math.sin(a) * h * 0.28] as [number, number];
      });

      const perHue = [0, 0, 0, 0, 0];
      const seats = dots.map((d) => perHue[d.hue]++);

      dots.forEach((d, di) => {
        let x = (d.seed * 0.94 + 0.03) * w;
        let y = (((d.seed * 7.13) % 1) * 0.86 + 0.07) * h;

        const j = seats[di];
        const cr = 8 * Math.sqrt(j);
        const ca = j * 2.399;
        const bx = CC[d.hue][0] + Math.cos(ca) * cr;
        const by = CC[d.hue][1] + Math.sin(ca) * cr;

        const ma = d.ri === 0 ? 0 : (d.ii / d.n) * Math.PI * 2 + d.ri * 0.4;
        const mx = cx + Math.cos(ma) * d.f * M;
        const my = cy + Math.sin(ma) * d.f * M;

        x += (bx - x) * t1;
        y += (by - y) * t1;
        x += (mx - x) * t2;
        y += (my - y) * t2;

        const r = 3 + (d.s * M - 3) * t2;
        ctx.globalAlpha = 1 - out;
        ctx.fillStyle = hues[d.hue];
        ctx.beginPath();
        ctx.arc(x, y, r, 0, Math.PI * 2);
        ctx.fill();
      });
      ctx.globalAlpha = 1;

      const phase = q < 0.34 ? 0 : q < 0.68 ? 1 : 2;
      if (phase !== last) {
        last = phase;
        if (statusRef.current) statusRef.current.textContent = REEF_LABELS[phase];
      }
    };

    if (reduced) {
      draw();
      if (statusRef.current) statusRef.current.textContent = REEF_LABELS[2];
      return;
    }

    let visible = true;
    let loop = 0;
    const io = new IntersectionObserver((entries) =>
      entries.forEach((e) => {
        visible = e.isIntersecting;
      }),
    );
    io.observe(wrap);
    const tick = () => {
      if (visible) draw();
      loop = requestAnimationFrame(tick);
    };
    loop = requestAnimationFrame(tick);

    return () => {
      cancelAnimationFrame(loop);
      io.disconnect();
    };
  }, [reduced]);

  return (
    <div ref={ref} data-mock="reef" style={quietStyle}>
      <div
        style={{
          display: "flex",
          justifyContent: "space-between",
          alignItems: "center",
          padding: "4px 0 12px",
          borderBottom: "1px solid var(--line-2)",
        }}
      >
        <span className="cs-eyebrow">reef forming</span>
        <span
          ref={statusRef}
          style={{ font: "var(--type-mono-meta)", fontFamily: "var(--mono)", color: "var(--ink-3)" }}
        >
          corals…
        </span>
      </div>
      <canvas ref={canvasRef} aria-hidden="true" style={{ display: "block", width: "100%", height: 288 }} />
    </div>
  );
}

/* ---------- the band ---------- */

export function SectionHow({ reduced }: { reduced: boolean }) {
  return (
    <section
      id="how"
      style={{
        background: "var(--bg-2)",
        borderTop: "1px solid var(--line)",
        borderBottom: "1px solid var(--line)",
      }}
    >
      <div className="cs-band">
        <div style={{ maxWidth: 640 }}>
          <div data-reveal className="cs-eyebrow">how it works · three moves</div>
          <h2
            data-reveal
            data-delay="80"
            style={{
              font: "400 clamp(26px,3vw,32px)/1.25 var(--display)",
              letterSpacing: "var(--tracking-title)",
              color: "var(--ink)",
              margin: "14px 0 0",
            }}
          >
            Connect once. It captures, splits and groups — you confirm.
          </h2>
        </div>

        <div className="cs-grid-auto-290" style={{ marginTop: 34 }}>
          <div style={cardStyle}>
            <div className="cs-eyebrow">01 · connect</div>
            <h3 style={h3Style}>It starts with what you already have.</h3>
            <p style={bodyStyle}>
              Meetings, chat, email, code, agents. They connect once and the swarm takes it from
              there — you never file anything, and nothing is ingested that you didn&apos;t connect.
            </p>
            <div style={{ marginTop: "auto", paddingTop: 18 }}>
              <ConnectorRing reduced={reduced} />
            </div>
          </div>

          <div style={cardStyle}>
            <div className="cs-eyebrow">02 · corals</div>
            <h3 style={h3Style}>One source becomes six corals.</h3>
            <p style={bodyStyle}>
              A meeting doesn&apos;t file as one note. Extraction splits it into atoms — decisions,
              people, threads, references — each keeping its provenance to the second.
            </p>
            <div style={{ marginTop: "auto", paddingTop: 18 }}>
              <CoralsMock reduced={reduced} />
            </div>
          </div>

          <div style={cardStyle}>
            <div className="cs-eyebrow">03 · reefs</div>
            <h3 style={h3Style}>Corals group into reefs.</h3>
            <p style={bodyStyle}>
              Related corals pull together into topics — you confirm what forms. The reef and the
              logo are the same object.
            </p>
            <div style={{ marginTop: "auto", paddingTop: 18 }}>
              <ReefMock reduced={reduced} />
            </div>
          </div>
        </div>
      </div>
    </section>
  );
}

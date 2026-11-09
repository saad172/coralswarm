"use client";

import { useEffect, useRef } from "react";
import { InlineCite } from "./CiteChip";
import { playOnceInView, prepUnits, setUnits } from "./motion";

/* Block 4 — ask + cite, with the oceans story folded into the same
   section. In v3 the private/team/org boundaries owned a whole card
   with its own diagram; here it is three lines under the ask copy. */

const TYPE_MS = 17;

const OCEANS = [
  { dot: "var(--brand)", lead: "Private first.", rest: "Every capture lands in your ocean and stays there." },
  { dot: "var(--hue-teal)", lead: "You promote.", rest: "The team ocean gets exactly what you moved — nothing else." },
  { dot: "var(--hue-plum)", lead: "Then the org.", rest: "Same gesture, same guarantee: nothing moves on its own." },
];

function AskMock({ reduced }: { reduced: boolean }) {
  const ref = useRef<HTMLDivElement>(null);
  const typeRef = useRef<HTMLSpanElement>(null);
  const unitsRef = useRef<HTMLElement[] | null>(null);

  useEffect(() => {
    const el = ref.current;
    if (!el) return;

    const caret = el.querySelector<HTMLElement>("[data-ask-caret]");
    const meta = el.querySelector<HTMLElement>("[data-ask-meta]");

    if (reduced) {
      // Fail-visible: `reduced` starts false (see useReducedMotion), so
      // the animated branch below may already have split the answer
      // into hidden per-character units before this branch runs. Show
      // the whole answer with no typing animation rather than leaving
      // it mid-reveal or, in the worst case, never revealed at all.
      if (caret) caret.style.display = "none";
      if (unitsRef.current) setUnits(unitsRef.current, unitsRef.current.length);
      if (meta) {
        meta.style.transition = "";
        meta.style.opacity = "";
      }
      return;
    }

    // Guarded so React's development double-effect can't split the
    // already-split spans a second time.
    if (!unitsRef.current) unitsRef.current = prepUnits(typeRef.current);
    const units = unitsRef.current;

    setUnits(units, 0);
    if (meta) meta.style.opacity = "0";

    let timer: ReturnType<typeof setInterval> | null = null;
    const stop = playOnceInView(
      el,
      () => {
        let i = 0;
        timer = setInterval(() => {
          i += 1;
          setUnits(units, i);
          if (i >= units.length) {
            if (timer) clearInterval(timer);
            if (caret) caret.style.display = "none";
            if (meta) {
              meta.style.transition = "opacity .4s ease";
              meta.style.opacity = "1";
            }
          }
        }, TYPE_MS);
      },
      0.3,
    );

    return () => {
      stop();
      if (timer) clearInterval(timer);
    };
  }, [reduced]);

  return (
    <div
      ref={ref}
      data-reveal
      data-delay="140"
      data-mock="ask"
      style={{
        background: "var(--bg-subtle)",
        border: "1px solid var(--line-2)",
        borderRadius: "var(--r-card)",
        padding: "20px 22px",
      }}
    >
      <div style={{ display: "flex", justifyContent: "flex-end" }}>
        <div
          style={{
            background: "var(--bg-2)",
            border: "1px solid var(--line-2)",
            borderRadius: "var(--r-card)",
            padding: "8px 12px",
            font: "400 15px/1.5 var(--sans)",
            color: "var(--ink)",
          }}
        >
          What&apos;s our audio chunk size?
        </div>
      </div>

      <div style={{ marginTop: 14, font: "400 14px/1.72 var(--sans)", color: "var(--ink)" }}>
        <span ref={typeRef} data-ask-type>
          The runbook pins capture at 512 samples
          <InlineCite n={1} />, but the 4 Jun eng sync moved production to 768
          <InlineCite n={2} /> — the sources disagree. The sync is newer; the runbook hasn&apos;t been
          updated to match.
        </span>
        <span
          data-ask-caret
          style={{
            display: "inline-block",
            width: 7,
            height: 14,
            background: "var(--brand)",
            marginLeft: 2,
            verticalAlign: -2,
            animation: "cs-caret 1s steps(1) infinite",
          }}
        />
      </div>

      <div
        data-ask-meta
        style={{
          marginTop: 12,
          borderTop: "1px solid var(--line-3)",
          paddingTop: 10,
          font: "var(--type-mono-meta)",
          fontFamily: "var(--mono)",
          color: "var(--ink-3)",
        }}
      >
        answered from 2 sources · 1.8s · <span style={{ color: "var(--warn)" }}>sources disagree</span>
      </div>
    </div>
  );
}

export function SectionAsk({ reduced }: { reduced: boolean }) {
  return (
    <section id="ask" style={{ background: "var(--bg)" }}>
      <div className="cs-band">
        <div className="cs-grid-auto-340">
          <div>
            <div data-reveal className="cs-eyebrow">ask + cite</div>
            <h2
              data-reveal
              data-delay="80"
              style={{
                font: "400 clamp(26px,3vw,32px)/1.25 var(--display)",
                letterSpacing: "var(--tracking-title)",
                color: "var(--ink)",
                margin: "14px 0 0",
                maxWidth: 440,
              }}
            >
              Every answer cites its source.
            </h2>
            <p
              data-reveal
              data-delay="160"
              style={{
                font: "400 14.5px/1.7 var(--sans)",
                color: "var(--ink-2)",
                margin: "14px 0 0",
                maxWidth: 460,
                textWrap: "pretty",
              }}
            >
              Ask in plain language. The answer stands on the corals it cites — and when two sources
              disagree, it says so instead of quietly picking one.
            </p>

            <div data-reveal data-delay="220" style={{ marginTop: 24 }}>
              <div className="cs-eyebrow" style={{ paddingBottom: 10 }}>
                and it stays in its ocean
              </div>
              {OCEANS.map((o) => (
                <div
                  key={o.lead}
                  style={{
                    display: "flex",
                    alignItems: "center",
                    gap: 9,
                    padding: "10px 0",
                    borderTop: "1px solid var(--line-3)",
                  }}
                >
                  <span style={{ width: 6, height: 6, borderRadius: "var(--r-full)", background: o.dot, flex: "none" }} />
                  <span style={{ font: "400 12.5px/1.6 var(--sans)", color: "var(--ink-2)" }}>
                    <strong style={{ fontWeight: 500, color: "var(--ink)" }}>{o.lead}</strong> {o.rest}
                  </span>
                </div>
              ))}
            </div>
          </div>

          <AskMock reduced={reduced} />
        </div>
      </div>
    </section>
  );
}

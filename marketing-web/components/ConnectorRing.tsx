"use client";

import { useEffect, useRef, useState } from "react";
import { DotMandala } from "./logos";
import { playOnceInView, REVEAL_EASE } from "./motion";

/* The connector orbit in card 01: eleven provider tiles that fly in from
 * scattered offsets, then orbit the mark forever (counter-rotating so the
 * logos stay upright).
 *
 * Logos are self-hosted in /public/logos rather than hot-linked from
 * cdn.simpleicons.org and raw.githubusercontent.com as the prototype
 * does — the handoff asks for this too ("self-host these before
 * shipping"). The alt-text fallback is kept regardless. */

type Tile = { file: string; alt: string; left: number; top: number };

/* Order is load-bearing: it drives the 70ms fly-in stagger and each
   tile's scatter angle. Five sit on the inner ring (the 45° diagonals
   plus Zoom at top), six on the outer. */
const TILES: Tile[] = [
  { file: "slack", alt: "Slack", left: 69.8, top: 30.2 },
  { file: "gmail", alt: "Gmail", left: 69.8, top: 69.8 },
  { file: "googlemeet", alt: "Google Meet", left: 30.2, top: 69.8 },
  { file: "github", alt: "GitHub", left: 30.2, top: 30.2 },
  { file: "zoom", alt: "Zoom", left: 50, top: 4 },
  { file: "claude", alt: "Claude Code", left: 85.96, top: 21.32 },
  { file: "mcp", alt: "MCP", left: 94.85, top: 60.23 },
  { file: "notion", alt: "Notion", left: 69.96, top: 91.44 },
  { file: "linear", alt: "Linear", left: 30.04, top: 91.44 },
  { file: "cursor", alt: "Cursor", left: 5.15, top: 60.23 },
  { file: "microsoft", alt: "Microsoft", left: 14.04, top: 21.32 },
];

const TILE = 44;

export function ConnectorRing({ reduced }: { reduced: boolean }) {
  const ref = useRef<HTMLDivElement>(null);
  const [failed, setFailed] = useState<Record<string, boolean>>({});

  useEffect(() => {
    const el = ref.current;
    if (!el || reduced) return;

    const tiles = Array.from(el.querySelectorAll<HTMLElement>("[data-logo]"));

    // Scatter them. Golden-angle direction, distance cycling 70-130px.
    tiles.forEach((t, i) => {
      const a = i * 2.399;
      const rr = 70 + ((i * 37) % 60);
      t.style.opacity = "0";
      t.style.transform = `translate(${Math.cos(a) * rr}px,${Math.sin(a) * rr}px) scale(.7)`;
    });

    const timers: ReturnType<typeof setTimeout>[] = [];

    const play = () => {
      tiles.forEach((t, i) => {
        timers.push(
          setTimeout(() => {
            t.style.transition = `opacity 1.1s ${REVEAL_EASE}, transform 1.1s ${REVEAL_EASE}`;
            t.style.opacity = "1";
            t.style.transform = "none";
          }, 120 + i * 70),
        );
      });

      // Once everything has landed, hand over to the perpetual orbit.
      timers.push(
        setTimeout(() => {
          el.style.animation = "cs-orbit 120s linear infinite";
          tiles.forEach((t) => {
            t.style.transition = "none";
            t.style.animation = "cs-orbit-rev 120s linear infinite";
          });
        }, 120 + tiles.length * 70 + 1250),
      );
    };

    const stop = playOnceInView(el, play, 0.3);
    return () => {
      stop();
      timers.forEach(clearTimeout);
    };
  }, [reduced]);

  return (
    <div
      ref={ref}
      data-mock="logos"
      style={{ position: "relative", width: "min(100%,300px)", aspectRatio: "1", margin: "0 auto" }}
    >
      <div style={{ position: "absolute", inset: "4%", border: "1px solid var(--line)", borderRadius: "var(--r-full)" }} />
      <div style={{ position: "absolute", inset: "24%", border: "1px solid var(--line)", borderRadius: "var(--r-full)" }} />

      <div style={{ position: "absolute", left: "50%", top: "50%", transform: "translate(-50%,-50%)" }}>
        <DotMandala size={48} />
      </div>

      {TILES.map((t) => (
        <div
          key={t.file}
          data-logo
          style={{
            position: "absolute",
            left: `calc(${t.left}% - ${TILE / 2}px)`,
            top: `calc(${t.top}% - ${TILE / 2}px)`,
            width: TILE,
            height: TILE,
            display: "flex",
            alignItems: "center",
            justifyContent: "center",
            background: "var(--bg)",
            border: "1px solid var(--line)",
            borderRadius: "var(--r-full)",
            boxShadow: "var(--shadow-card)",
          }}
        >
          {failed[t.file] ? (
            <span style={{ font: "500 9px/1.2 var(--mono)", color: "var(--ink-2)", textAlign: "center" }}>
              {t.alt}
            </span>
          ) : (
            /* Plain <img>, not next/image: this is a static export with no
               loader, the SVGs are tiny and local, and next/image would
               wrap each tile in its own positioned span that fights the
               orbit transform. */
            <img
              src={`/logos/${t.file}.svg`}
              alt={t.alt}
              title={t.alt}
              style={{ height: 19, width: "auto", display: "block" }}
              onError={() => setFailed((f) => ({ ...f, [t.file]: true }))}
            />
          )}
        </div>
      ))}
    </div>
  );
}

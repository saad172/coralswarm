"use client";

import { useEffect, useRef } from "react";
import { DURATION_MS, useReducedMotion } from "@/lib/motion";
const rings = [[0, 1, .085], [.14, 5, .06], [.25, 8, .05], [.35, 11, .042], [.45, 14, .034]];
const hues = ["amber", "teal", "plum", "pink", "coral"];
const dots = rings.flatMap(([radius, count, size], ring) => Array.from({ length: count }, (_, index) => ({ radius, size, ring, index, angle: index / count * Math.PI * 2 + ring * .4 })));

export function SwarmMandala({ settled = false }: { settled?: boolean }) {
  const reduced = useReducedMotion();
  const ref = useRef<SVGSVGElement>(null);
  const positions = useRef(dots.map((d) => ({ radius: d.radius + .3, angle: d.angle + 1.2 })));
  useEffect(() => {
    if (reduced !== false) return;
    const start = performance.now();
    const from = positions.current.map((p) => ({ ...p }));
    let frame = 0;
    const draw = (now: number) => {
      const t = (now - start) / 1000;
      dots.forEach((d, i) => {
        const seed = ((i * 37 + 17) % 101) / 100;
        const x = Math.max(0, Math.min(settled ? 1 : .9, settled ? t / (DURATION_MS.settle / 1000) : (t - d.ring * .28 - seed * .55) / (1.4 + seed * .9)));
        const p = 1 - (1 - x) ** 3;
        const radius = from[i].radius + (d.radius - from[i].radius) * p + (settled ? 0 : .035 * Math.sin(2.4 * t + i));
        const angle = from[i].angle + (d.angle - from[i].angle) * p + (settled ? 0 : .22 * Math.sin(1.1 * t + i));
        positions.current[i] = { radius, angle };
        const circle = ref.current?.children[i];
        circle?.setAttribute("cx", String(.5 + Math.cos(angle) * radius));
        circle?.setAttribute("cy", String(.5 + Math.sin(angle) * radius));
        circle?.setAttribute("opacity", String(settled ? 1 : Math.min(1, 4 * p)));
      });
      if (!settled || t < DURATION_MS.settle / 1000) frame = requestAnimationFrame(draw);
    };
    frame = requestAnimationFrame(draw);
    return () => cancelAnimationFrame(frame);
  }, [settled, reduced]);
  return <svg ref={ref} aria-hidden width={settled ? 18 : 22} height={settled ? 18 : 22} viewBox="0 0 1 1" style={{ overflow: "visible", transition: reduced ? "none" : "width var(--dur-collapse) var(--ease-out), height var(--dur-collapse) var(--ease-out)" }}>
    {dots.map((d, i) => <circle key={i} cx={.5 + Math.cos(d.angle) * d.radius} cy={.5 + Math.sin(d.angle) * d.radius} r={d.size} fill={`var(--hue-${hues[(d.ring + d.index) % hues.length]})`} />)}
  </svg>;
}

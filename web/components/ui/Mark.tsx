import type { ReactNode } from "react";
import { cn } from "./cn";
import { HUES, hueVar } from "./hue";

/* The swarm mark.

   Five concentric rings of circles at radii 0 / .14 / .25 / .35 / .45 with dot
   sizes .085 / .06 / .05 / .042 / .034 in a 0–1 viewBox, filled by cycling the
   five hues. Ring counts are 1 / 6 / 10 / 14 / 18 scaled by `density` — the
   mocks draw the sidebar mark at .8 and the answer mark at .75, which is why
   density is a prop rather than baked in.

   Drawn in a 0–1 viewBox and sized by width/height so it stays crisp at any
   size without recomputing geometry. */

const RINGS = [
  { r: 0, n: 1, s: 0.085 },
  { r: 0.14, n: 6, s: 0.06 },
  { r: 0.25, n: 10, s: 0.05 },
  { r: 0.35, n: 14, s: 0.042 },
  { r: 0.45, n: 18, s: 0.034 },
];

export function DotMandala({
  size = 22,
  density = 0.8,
  mono = false,
  color = "currentColor",
  rotate = 0,
  className,
}: {
  size?: number;
  density?: number;
  mono?: boolean;
  color?: string;
  rotate?: number;
  className?: string;
}) {
  const dots: ReactNode[] = [];
  let k = 0;
  RINGS.forEach((ring, ri) => {
    const n = Math.max(1, Math.round(ring.n * density));
    for (let i = 0; i < n; i++) {
      const a = ri === 0 ? 0 : (i / n) * Math.PI * 2 + ri * 0.4 + (rotate * Math.PI) / 180;
      dots.push(
        <circle
          key={k++}
          cx={0.5 + Math.cos(a) * ring.r}
          cy={0.5 + Math.sin(a) * ring.r}
          r={ring.s}
          fill={mono ? color : hueVar(HUES[(i + ri) % HUES.length])}
        />,
      );
    }
  });
  return (
    <svg
      viewBox="0 0 1 1"
      width={size}
      height={size}
      aria-hidden
      className={cn("block flex-none", className)}
    >
      {dots}
    </svg>
  );
}

/** Wordmark — "coral" + mandala + "swarm", set tight at -.045em. */
export function Wordmark({
  size = 22,
  mono = false,
  color = "currentColor",
  className,
}: {
  size?: number;
  mono?: boolean;
  color?: string;
  className?: string;
}) {
  const dot = size * 0.78;
  return (
    <span
      className={cn("inline-flex items-center whitespace-nowrap font-sans font-semibold leading-none", className)}
      style={{ fontSize: size, letterSpacing: "-0.045em", color }}
    >
      coral
      <DotMandala
        size={dot}
        mono={mono}
        color={color}
        density={mono ? 0.6 : 0.75}
        className="mx-[0.04em]"
      />
      swarm
    </span>
  );
}

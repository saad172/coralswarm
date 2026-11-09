/* =========================================================
   CoralSwarm — Logo system (sans-led, polychrome)
   Ported from the design prototype (logos.jsx). The marketing site
   uses the primary wordmark; DotMandala backs both the mark and the
   animated hero swarm.
   ========================================================= */

/* 5-hue swarm palette — used cyclically around the cluster */
const HUES = ["coral", "pink", "amber", "teal", "plum"] as const;
const hueVar = (h: string) => `var(--hue-${h})`;

/**
 * Quantise a trig-derived coordinate to 4dp.
 *
 * The mark is server-rendered, and Math.cos/Math.sin are only required
 * to be within 1 ULP — Node and V8-in-Chrome disagree in the last
 * digit for some angles. That produced cy="33.91009190397021" on the
 * server against 33.910091903970205 on the client, and React reported
 * a hydration mismatch on every mandala on the page. 4dp is far below
 * a subpixel at every size the mark is used at (max 300px), so this
 * changes nothing visually.
 */
const q = (v: number) => Math.round(v * 1e4) / 1e4;

type DotMandalaProps = {
  size?: number;
  mono?: boolean;
  color?: string;
  density?: number;
  rotate?: number;
};

/* Dot cluster — each ring's dots cycle through the brand hues */
export function DotMandala({
  size = 80,
  mono = false,
  color = "currentColor",
  density = 1,
  rotate = 0,
}: DotMandalaProps) {
  const rings = [
    { r: 0.0, n: 1, size: 0.085 },
    { r: 0.14, n: 6, size: 0.06 },
    { r: 0.25, n: 10, size: 0.05 },
    { r: 0.35, n: 14, size: 0.042 },
    { r: 0.45, n: 18, size: 0.034 },
  ];
  const dots: React.ReactNode[] = [];
  let k = 0;
  rings.forEach((ring, ri) => {
    const n = Math.max(1, Math.round(ring.n * density));
    for (let i = 0; i < n; i++) {
      const a = ri === 0 ? 0 : (i / n) * Math.PI * 2 + ri * 0.4 + (rotate * Math.PI) / 180;
      const x = 0.5 + Math.cos(a) * ring.r;
      const y = 0.5 + Math.sin(a) * ring.r;
      const fill = mono ? color : hueVar(HUES[(i + ri) % HUES.length]);
      dots.push(
        <circle key={k++} cx={q(x * size)} cy={q(y * size)} r={q(ring.size * size)} fill={fill} />,
      );
    }
  });
  return (
    <svg viewBox={`0 0 ${size} ${size}`} width={size} height={size} style={{ display: "block" }}>
      {dots}
    </svg>
  );
}

type WordmarkProps = { size?: number; mono?: boolean; color?: string };

/* PRIMARY — sans wordmark with polychrome cluster bridge */
export function WordmarkPrimary({ size = 56, mono = false, color = "currentColor" }: WordmarkProps) {
  const dotSize = size * 0.78;
  return (
    <span
      style={{
        display: "inline-flex",
        alignItems: "center",
        fontFamily: "var(--sans)",
        fontWeight: 600,
        fontSize: size,
        lineHeight: 1,
        letterSpacing: "-0.045em",
        color,
        whiteSpace: "nowrap",
      }}
    >
      <span>coral</span>
      <span style={{ display: "inline-flex", width: dotSize, height: dotSize, margin: `0 ${size * 0.04}px` }}>
        <DotMandala size={dotSize} mono={mono} color={color} density={0.75} />
      </span>
      <span>swarm</span>
    </span>
  );
}

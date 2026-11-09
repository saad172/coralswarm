import { ImageResponse } from "next/og";

/* Share card. Generated at build time from the same dot-mandala
   geometry the site draws in code — there is no logo file, and there
   should not be one (see the v3 handoff: the mark is generated and
   must stay that way).

   Rendered with plain divs rather than an <svg>, because the image
   renderer's SVG support is narrower than its flexbox support. */

/* The site is a static export, so this route has to be rendered once
   at build time rather than per-request. */
export const dynamic = "force-static";

export const size = { width: 1200, height: 630 };
export const contentType = "image/png";
export const alt = "CoralSwarm — organizational memory, automated";

const HUES = ["#f77764", "#eb7290", "#f4b860", "#2a9d8f", "#7c5295"];

/** radius fraction, dot count, dot diameter fraction — the mark. */
const RINGS: [number, number, number][] = [
  [0, 1, 0.085],
  [0.14, 6, 0.06],
  [0.25, 10, 0.05],
  [0.35, 14, 0.042],
  [0.45, 18, 0.034],
];

const MARK = 300;

function markDots() {
  const dots: React.ReactNode[] = [];
  let k = 0;
  RINGS.forEach(([r, count, s], ri) => {
    for (let i = 0; i < count; i++) {
      const a = ri === 0 ? 0 : (i / count) * Math.PI * 2 + ri * 0.4;
      const d = s * MARK;
      const cx = 0.5 + Math.cos(a) * r;
      const cy = 0.5 + Math.sin(a) * r;
      dots.push(
        <div
          key={k}
          style={{
            position: "absolute",
            left: cx * MARK - d / 2,
            top: cy * MARK - d / 2,
            width: d,
            height: d,
            borderRadius: d,
            background: HUES[(i + ri) % HUES.length],
          }}
        />,
      );
      k++;
    }
  });
  return dots;
}

export default function OpengraphImage() {
  return new ImageResponse(
    (
      <div
        style={{
          width: "100%",
          height: "100%",
          display: "flex",
          flexDirection: "column",
          alignItems: "center",
          justifyContent: "center",
          background: "#ffffff",
          padding: 72,
        }}
      >
        <div style={{ position: "relative", width: MARK, height: MARK, display: "flex" }}>
          {markDots()}
        </div>

        <div
          style={{
            marginTop: 40,
            fontSize: 64,
            fontWeight: 600,
            letterSpacing: "-0.045em",
            color: "#14161a",
          }}
        >
          coralswarm
        </div>

        <div style={{ marginTop: 18, fontSize: 28, color: "#4b5058", textAlign: "center" }}>
          Organizational memory — automated. For humans and AI.
        </div>
      </div>
    ),
    size,
  );
}

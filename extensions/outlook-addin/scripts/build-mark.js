#!/usr/bin/env node
/* Generates the CoralSwarm DotMandala mark as an SVG, geometry-identical to
   <DotMandala> in web/components/ui.tsx (the brand source of truth): five
   concentric rings of hue-cycled dots, no container shape. Emits brand-mark.svg
   and, when run with --icons, rasterizes the ribbon/store PNGs via `sips`. */

const fs = require("fs");
const path = require("path");
const { execFileSync } = require("child_process");

// Hues in taxonomy order: coral → pink → amber → teal → plum.
const HUES = ["#F77764", "#EB7290", "#F4B860", "#2A9D8F", "#7C5295"];
const RINGS = [
  { r: 0.0, n: 1, size: 0.085 },
  { r: 0.14, n: 6, size: 0.06 },
  { r: 0.25, n: 10, size: 0.05 },
  { r: 0.35, n: 14, size: 0.042 },
  { r: 0.45, n: 18, size: 0.034 },
];

function mandalaSvg({ size = 100, mono = false, color = "#F77764", pad = 0.1 } = {}) {
  const dots = [];
  RINGS.forEach((ring, ri) => {
    for (let i = 0; i < ring.n; i++) {
      const a = ri === 0 ? 0 : (i / ring.n) * Math.PI * 2 + ri * 0.4;
      const x = 0.5 + Math.cos(a) * ring.r;
      const y = 0.5 + Math.sin(a) * ring.r;
      const fill = mono ? color : HUES[(i + ri) % HUES.length];
      dots.push(
        `<circle cx="${(x * size).toFixed(2)}" cy="${(y * size).toFixed(2)}" r="${(ring.size * size).toFixed(2)}" fill="${fill}"/>`
      );
    }
  });
  const m = size * pad;
  const vb = `${-m} ${-m} ${size + m * 2} ${size + m * 2}`;
  return `<svg xmlns="http://www.w3.org/2000/svg" viewBox="${vb}">\n  ${dots.join("\n  ")}\n</svg>\n`;
}

const assetsDir = path.join(__dirname, "..", "assets");
const svg = mandalaSvg({ size: 100, pad: 0.1 });
fs.writeFileSync(path.join(assetsDir, "brand-mark.svg"), svg);
console.log("wrote assets/brand-mark.svg");

if (process.argv.includes("--icons")) {
  const tmp = path.join(assetsDir, ".brand-mark.tmp.svg");
  fs.writeFileSync(tmp, svg);
  for (const s of [16, 32, 64, 80, 128]) {
    const out = path.join(assetsDir, `icon-${s}.png`);
    execFileSync("sips", ["-s", "format", "png", "-z", String(s), String(s), tmp, "--out", out], {
      stdio: "ignore",
    });
    console.log(`wrote assets/icon-${s}.png`);
  }
  fs.unlinkSync(tmp);
}

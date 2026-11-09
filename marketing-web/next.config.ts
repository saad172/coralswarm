import type { NextConfig } from "next";

// CoralSwarm marketing site — a fully static, isolated brand surface.
// Static export by default so it can ship to any static host (Vercel,
// Cloudflare Pages, S3). Set NEXT_OUTPUT_MODE=standalone for a Node deploy.
const nextConfig: NextConfig = {
  output: process.env.NEXT_OUTPUT_MODE === "standalone" ? "standalone" : "export",
};

export default nextConfig;

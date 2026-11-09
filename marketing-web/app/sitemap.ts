import type { MetadataRoute } from "next";

// See robots.ts for why this is a literal rather than an import from
// app/layout.tsx.
const SITE_URL = "https://coralswarm.example";

// Required under `output: "export"` — see robots.ts.
export const dynamic = "force-static";

// /privacy and /terms are provided by app/privacy and app/terms
// (added alongside this nav/SEO polish in the same PR).
//
// No `lastModified` here on purpose: `output: "export"` prerenders this
// route at build time, so `new Date()` would stamp every deploy with
// the build timestamp rather than the page's actual last change — a
// sitemap.xml that churns on every build gives crawlers a false
// "everything changed" signal instead of a useful one. Omit the field
// rather than fake it; add real per-page timestamps back if this ever
// tracks true content-modification dates.
export default function sitemap(): MetadataRoute.Sitemap {
  return [
    { url: `${SITE_URL}/`, changeFrequency: "weekly", priority: 1 },
    { url: `${SITE_URL}/support`, changeFrequency: "monthly", priority: 0.5 },
    { url: `${SITE_URL}/privacy`, changeFrequency: "yearly", priority: 0.3 },
    { url: `${SITE_URL}/terms`, changeFrequency: "yearly", priority: 0.3 },
  ];
}

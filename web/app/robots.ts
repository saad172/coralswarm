import type { MetadataRoute } from "next";

// The product app is Clerk-gated on every host it's served from
// (coralswarm.example, app./www., and coralswarm-web.example) — nothing here,
// including share links, should ever be indexed. No sitemap: there's nothing
// here worth crawling.
export const dynamic = "force-static";

export default function robots(): MetadataRoute.Robots {
  return {
    rules: {
      userAgent: "*",
      disallow: "/",
    },
  };
}

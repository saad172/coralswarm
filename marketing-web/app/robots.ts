import type { MetadataRoute } from "next";

// Same literal as app/layout.tsx's SITE_URL / metadataBase — kept as a
// separate constant rather than imported from layout.tsx because these
// metadata route files are prerendered independently under
// `output: "export"`, and layout.tsx pulls in next/font + Script, which
// would drag unrelated client-bundle concerns into a file that only
// needs to emit nine lines of text.
const SITE_URL = "https://coralswarm.example";

// Required under `output: "export"` — without this Next refuses to
// prerender the route, since a metadata route handler could otherwise be
// dynamic (e.g. per-request headers) and static export has no server to
// run it against.
export const dynamic = "force-static";

// AI crawlers get their own explicit allow groups rather than relying
// on the `userAgent: "*"` rule below to cover them. Policy is "allow
// everything" for all of them — an AEO/AI-visibility play, same reason
// llms.txt exists — but several of these bots (GPTBot, ClaudeBot,
// PerplexityBot, CCBot…) are known to sometimes ignore a wildcard rule
// and only respect a rule naming them by user-agent, so being explicit
// is the only way "allow everything" reliably reaches all of them.
const AI_CRAWLERS = [
  "GPTBot",
  "OAI-SearchBot",
  "ChatGPT-User",
  "ClaudeBot",
  "Claude-User",
  "Claude-SearchBot",
  "PerplexityBot",
  "Perplexity-User",
  "Google-Extended",
  "CCBot",
  "Applebot-Extended",
  "meta-externalagent",
];

export default function robots(): MetadataRoute.Robots {
  return {
    rules: [
      {
        userAgent: "*",
        allow: "/",
      },
      ...AI_CRAWLERS.map((userAgent) => ({ userAgent, allow: "/" })),
    ],
    sitemap: `${SITE_URL}/sitemap.xml`,
  };
}

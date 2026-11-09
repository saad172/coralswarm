import type { NextConfig } from "next";
import path from "path";
import { hostRedirectPattern } from "./lib/orgSubdomainRedirect";

const nextConfig: NextConfig = {
  // Dev-only: when `next dev` is reached through a tunnel (e.g. a Cloudflare
  // quick tunnel for mobile testing), Next treats the tunnel host as cross-origin
  // and blocks `/_next/*` chunk + HMR requests, so the page never hydrates. Allow
  // the tunnel origins. No effect on production builds.
  allowedDevOrigins: ["*.trycloudflare.com", "*.ngrok-free.app", "*.ngrok.io"],
  // The web app is a Node server on Fly (see web/Dockerfile).
  output: "standalone",
  // Pin the file-tracing root to the monorepo root so the standalone output
  // layout is deterministic: `.next/standalone/web/server.js` (what the
  // Dockerfile's `CMD node web/server.js` expects). Without this, Next infers
  // the root from lockfiles, which differed between local and CI — CI produced
  // a flat layout (server.js at the root) and the container crash-looped with
  // "Cannot find module '/app/web/server.js'".
  outputFileTracingRoot: path.join(__dirname, ".."),
  // Belt-and-braces noindex on top of the <meta robots> tag (layout.tsx) and
  // robots.ts. Every host this server answers for (app.coralswarm.example,
  // *.coralswarm.example org subdomains, coralswarm-web.example) is Clerk-gated
  // and must never be indexed.
  async headers() {
    return [
      {
        source: "/:path*",
        headers: [{ key: "X-Robots-Tag", value: "noindex, nofollow" }],
      },
      {
        source: "/teams/meeting/:path*",
        headers: [
          { key: "Content-Security-Policy", value: "frame-ancestors 'self' https://teams.microsoft.com https://*.teams.microsoft.com https://teams.cloud.microsoft https://*.teams.cloud.microsoft https://*.microsoft365.com https://*.office.com https://office.com https://microsoft365.com https://outlook.cloud.microsoft https://m365.cloud.microsoft https://outlook.live.com https://*.outlook.com;" },
          { key: "Referrer-Policy", value: "no-referrer" },
        ],
      },
      {
        // The Google Meet side panel (extensions/meet-addon): framed by meet.google.com only.
        source: "/meet/:path*",
        headers: [
          { key: "Content-Security-Policy", value: "frame-ancestors https://meet.google.com;" },
          { key: "Referrer-Policy", value: "no-referrer" },
        ],
      },
      {
        source: "/connectors/microsoft-teams/meeting-capture",
        headers: [{ key: "Referrer-Policy", value: "no-referrer" }, { key: "Content-Security-Policy", value: "frame-ancestors 'self';" }],
      },
      {
        // The add-in Clerk OAuth dialog pages (start redirects to Clerk, callback receives the code):
        // never cached, and never leak the URL (which carries the authorization code) as a referrer.
        source: "/addin/auth/:path*",
        headers: [
          { key: "Cache-Control", value: "no-store" },
          { key: "Referrer-Policy", value: "no-referrer" },
        ],
      },
      {
        source: "/outlook/:path*",
        headers: [
          { key: "Content-Security-Policy", value: "frame-ancestors 'self' https://outlook.office.com https://outlook.office365.com https://outlook.cloud.microsoft https://outlook.live.com https://*.outlook.com https://*.office.com https://*.microsoft365.com;" },
          { key: "Referrer-Policy", value: "no-referrer" },
          { key: "Cache-Control", value: "no-cache, must-revalidate" },
        ],
      },
    ];
  },
  // The invite email links to `<web base>/join` (built by backend
  // `org_invites::build_join_url`), but the page itself lives at
  // /onboarding/join — without this redirect every emailed invite link
  // 404s (caught live, org-invites PR 3 smoke test). Query params
  // (email/org/inviter) are preserved by Next automatically.
  //
  // HOST-AGNOSTIC BY CONSTRUCTION, which is why claim-based tenancy did
  // not have to touch it: `source` matches a PATH, so it fires on
  // whatever host serves the app. Backend `org_web_base_url()` still
  // builds `https://<slug>.<apex>/join` for org invites — Phase 4
  // collapses that to the apex — and both shapes hit this same rule.
  //
  // ---------------------------------------------------------------
  // Claim-based tenancy cutover, Phase 4 step 2
  // (docs/plans/claim-based-tenancy.md §Phase 4, decision 6):
  //
  //   308 `<slug>.coralswarm.example/*` -> `app.coralswarm.example/*`, before
  //   auth — purely host-based, so it sits at the edge or outermost
  //   and must never reach the gate. 308 preserves method and body.
  //
  // PR 1.3 (#794) narrowed the backend's `is_allowed_cors_origin` to
  // `HostTarget::Apex` only, so `https://acme.coralswarm.example` is no
  // longer an accepted Origin — but DNS is untouched (the wildcard
  // `*.coralswarm.example` CNAME stays; step 4 keeps it, since it is
  // load-bearing elsewhere), so a bookmarked org subdomain still
  // resolves straight into THIS Next.js server (the same Fly app,
  // `coralswarm-web`, that serves `app.coralswarm.example`) and would
  // otherwise render a page that then fails every request, silently.
  //
  // WHY HERE, evidence first (recon, not assumption):
  // - The web app is a standalone Next.js (Node) server on Fly
  //   (`web/Dockerfile`, `web/fly.toml`) — not fronted by a Cloudflare
  //   Worker. Only the BARE apex and `www.` are on a Worker
  //   (`marketing-web/wrangler.jsonc`'s `routes`, the separate
  //   marketing site); `app.coralswarm.example` and every
  //   `<slug>.coralswarm.example` reach this server directly.
  // - `next.config.ts`'s `redirects()` runs before Next's own
  //   `middleware.ts` in the router's evaluation order — and there is
  //   no `web/middleware.ts` in this app at all. Auth here is client-
  //   side Clerk (`@clerk/nextjs` client components); nothing resembling "auth"
  //   runs until the browser executes page JS. A server-side,
  //   host-only redirect answered before any HTML ships is
  //   unambiguously "before auth" and can never reach a page, a
  //   client component, or (since this app and the Rust API are
  //   separate Fly apps under separate hostnames) the backend's
  //   tenancy gate at all.
  // - The API subdomain (`<slug>.api.coralswarm.example`) is a DIFFERENT
  //   host, served by a DIFFERENT Fly app (`coralswarm-backend`), and
  //   is explicitly cutover step 3 ("keep the API subdomain
  //   redirecting for one release, then hard-error") — a later,
  //   separate release. Out of scope here by construction: it isn't
  //   this app, so this config can't touch it either way.
  //
  // The actual host-matching decision (which single label is
  // excluded, why, and the live evidence behind each of `app` / `www`
  // / `api` / the now-corrected `addin` story) lives in
  // `lib/orgSubdomainRedirect.ts`, the one place it's tested by name;
  // this entry only wires that pattern in.
  // ---------------------------------------------------------------
  async redirects() {
    return [
      {
        source: "/join",
        destination: "/onboarding/join",
        permanent: false,
      },
      {
        source: "/:path*",
        has: [{ type: "host", value: hostRedirectPattern() }],
        destination: "https://app.coralswarm.example/:path*",
        // 308: preserves method and body, per the spec above.
        permanent: true,
      },
    ];
  },
};

export default nextConfig;

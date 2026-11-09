# @coralswarm/marketing-web

The public CoralSwarm marketing site at `coralswarm.example` — an isolated Next.js 15
(App Router) app, separate from the product `web/` app and from the product's Ocean
theme.

Current cut: **v4a**, a single scrolling page (`components/Page.tsx`) composed
of eight blocks — nav, hero, how it works, ask + cite, agents/MCP, pricing,
contact, footer — every internal link an in-page anchor. It replaced an
earlier multi-section v3 design; nothing from that version (`HeroSwarm`,
`EmailForm`, `sections.tsx`/`sections2.tsx`, a CSS `@import` for fonts) is in
this tree any more.

## Brand

Cool neutral canvas, ink text, five rotating brand hues (coral / pink / amber
/ teal / plum), Instrument Serif italic for display moments, Geist for UI,
JetBrains Mono for machine text (`app/globals.css` carries the full token
set).

## Structure

```
app/
  layout.tsx        root layout, metadata, JSON-LD (Organization + SoftwareApplication)
  globals.css        design tokens + base classes
  page.tsx            renders <Page />
  robots.ts            app/robots.txt (MetadataRoute.Robots)
  sitemap.ts           app/sitemap.xml (MetadataRoute.Sitemap)
  opengraph-image.tsx   generated share card
components/
  Page.tsx            composes every section, owns the shared reduced-motion/
                       smooth-nav/reveal hooks
  logos.tsx            DotMandala + WordmarkPrimary
  motion.ts            shared motion primitives (reveal, smooth-scroll nav,
                        reduced-motion, typing/reveal helpers) — read this
                        before adding any new animation
  SiteNav.tsx           sticky header + mobile drawer (<860px)
  SiteFooter.tsx        footer nav + legal links
  Hero.tsx              self-playing canvas hero
  SectionHow.tsx         "how it works" — connect / corals / reefs
  SectionAsk.tsx          ask + cite, plus the private/team/org oceans story
  SectionAgents.tsx        agents / MCP (the one dark band)
  SectionPricing.tsx        plans + the "private by construction" security lines
  SectionContact.tsx         the one contact/waitlist form
  ConnectorRing.tsx           connector logos animation
  CiteChip.tsx                 inline citation chip used in the ask mock
  firstTouch.ts                 UTM/referrer capture for the contact form
worker/
  index.ts             the Cloudflare Worker fronting the static export
migrations/           D1 schema for the waitlist/contact tables
```

Only files with a `"use client"` directive run animation, form or drawer
logic; everything else server-renders (statically, at build time) for SEO.

## Fonts

All three faces (Instrument Serif, Geist, JetBrains Mono) are self-hosted via
`next/font/google` in `app/layout.tsx` — not a CSS `@import`. That avoids a
render-blocking stylesheet ahead of the LCP headline and stops every
visitor's IP from being sent to Google, which matters on a page whose pricing
band says "we can't read it, or recover it."

## Local dev

```bash
cd marketing-web
npm install
npm run dev          # http://localhost:3001
npm run typecheck     # app + worker/tsconfig.json
npm run lint
npm run build          # static export → ./out
```

The contact form's Turnstile widget falls back to Cloudflare's documented
always-passes test site key when `NEXT_PUBLIC_TURNSTILE_SITE_KEY` is unset, so
`npm run dev` works with no configuration. The Worker side
(`worker/index.ts`) only runs under `wrangler dev` / the deployed Worker, not
`next dev` — the API routes will 404 locally unless you also run `wrangler
dev` against `./out`.

## Configuration

| Where | Name | Required | Notes |
|---|---|---|---|
| Next build (env) | `NEXT_PUBLIC_TURNSTILE_SITE_KEY` | at build, for real bot protection | Public. Falls back to Cloudflare's always-passes test key whenever it's unset, so `npm run dev` needs no config. Production must set it as a CI secret: `deploy-marketing` injects it and fails fast when it's missing or a documented test key (guard added alongside this PR — see PR #448). |
| Next build (env) | `NEXT_PUBLIC_CF_BEACON_TOKEN` | optional | Enables Cloudflare Web Analytics (cookieless, no consent banner). Unset ⇒ no beacon script ships at all. |
| Worker secret | `TURNSTILE_SECRET_KEY` | yes | `wrangler secret put TURNSTILE_SECRET_KEY`. The Worker fails **closed** (503) if this is unset — a misconfigured deploy breaks the form loudly instead of silently disabling the bot check. |
| Worker secret | `UNSUBSCRIBE_SECRET` | yes | `wrangler secret put UNSUBSCRIBE_SECRET`. HMAC key signing one-click unsubscribe links; not a third-party credential, just a random value. |
| `wrangler.jsonc` var | `CONTACT_NOTIFY_TO` | yes | Plain (non-secret) var — where contact submissions are emailed. Must be a Google Workspace mailbox (inbound mail for coralswarm.example is on Google — the apex MX is `smtp.google.com` — not Cloudflare Email Routing). Said "Zoho" until 2026-08-25; the domain was migrated and the doc lagged. |

## Deploy

The site ships behind a small Cloudflare Worker (`worker/index.ts`) that
fronts the static export: `run_worker_first` in `wrangler.jsonc` routes only
`/api/*` and `/opengraph-image` through the Worker, and every other request
(all HTML/CSS/JS/image assets) is served straight from the `ASSETS` binding.
The Worker also owns a D1 database (`migrations/`, waitlist + contact
submissions), Cloudflare Email Sending (waitlist confirmation + founder
notification), and a per-IP rate limiter.

Production deploys run in CI, not by hand: `.github/workflows/deploy.yml`'s
`deploy-marketing` job (gated on a green CI run on `main`) does, in order:

```bash
npm install --no-audit --no-fund
npm run build                                              # → ./out
wrangler d1 migrations apply coralswarm-waitlist --remote   # migrations before deploy
wrangler deploy                                              # ships ./out + worker/index.ts
```

To deploy by hand from this directory (needs `CLOUDFLARE_API_TOKEN` /
`CLOUDFLARE_ACCOUNT_ID` and wrangler 4 — wrangler 3 silently can't parse the
assets-only Worker config):

```bash
npm run build
npx wrangler d1 migrations apply coralswarm-waitlist --remote
npx wrangler deploy
```

`next.config.ts` still supports `NEXT_OUTPUT_MODE=standalone` for a Node
server deploy, but nothing in this repo uses that path today — the shipped
target is the static export + Worker described above.

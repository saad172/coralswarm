// Where the CoralSwarm API lives.
//
//   UI       coralswarm.example / app. / www.  (Fly app `coralswarm-web`)
//   API      api.coralswarm.example            (Fly app `coralswarm-backend`)
//
// CLAIM-BASED TENANCY (docs/plans/claim-based-tenancy.md): the tenant is named
// ONLY by the Clerk JWT's `org_id` claim. It appears in no hostname and no URL
// path — `app.coralswarm.example` and `api.coralswarm.example` serve every tenant, and
// the backend's gate binds an org's pool from the claim
// (`backend/src/tenancy/gate.rs`, `HostTarget::Apex` + `TokenKind::Org`). So
// there is exactly one API base per deployment and nothing here derives a
// tenant from anything.
//
// WHAT THIS MODULE STILL DOES, AND WHY IT ISN'T A CONSTANT: one deployment is
// served from several hosts — the apex pair (`coralswarm.example`, `app.`, `www.`),
// `coralswarm-web.example`, and localhost dev. Only the first group can assume `api.<apex>`
// exists; everything else must keep using the build-time `NEXT_PUBLIC_API_BASE`
// it uses today. That is one boolean question — "is this a host of ours?" — and
// it is all the host is consulted for.
//
// OPS DEPENDENCY: setting `NEXT_PUBLIC_WEB_APEX` is a promise that `api.<apex>`
// resolves to the backend. Unset it and every call here returns
// `NEXT_PUBLIC_API_BASE` verbatim.
//
// LAZY BY REQUIREMENT: these are functions, never module-level consts. Client
// components that import the API client are prerendered on the server during
// the build (see web/Dockerfile) — a top-level `window` read crashes the
// build. Resolve at call time and treat `window === undefined` as "no host
// information", which lands on the env fallback.

/** Build-time fallback: localhost dev, `*.fly.dev`, and SSR. */
const FALLBACK_BASE = process.env.NEXT_PUBLIC_API_BASE ?? "http://localhost:8080";

/** Apex the web UI is served from, e.g. `coralswarm.example`. Empty ⇒ host
 *  derivation is disabled entirely (local dev, `*.fly.dev`-only deploys). */
const WEB_APEX = normalizeHost(process.env.NEXT_PUBLIC_WEB_APEX ?? "");

/** Lowercase, strip a trailing dot and any `:port`. Mirrors the canonicalization
 *  the backend gate applies to the `Host` header. */
function normalizeHost(raw: string): string {
  let h = raw.trim();
  const colon = h.lastIndexOf(":");
  if (colon > -1 && /^\d+$/.test(h.slice(colon + 1))) h = h.slice(0, colon);
  h = h.toLowerCase();
  if (h.endsWith(".")) h = h.slice(0, -1);
  return h;
}

/** 3–32 chars, lowercase alphanumeric with internal hyphens, and not `app`/`www`.
 *
 *  NOT a routing rule any more — no slug appears in any hostname or URL. This
 *  is a FORM VALIDATOR for `app/onboarding/org/page.tsx`, kept in lockstep with
 *  `gate.rs::is_valid_slug` and the `tenant_databases.slug` CHECK constraints
 *  (migrations 0078 + 0079) so the UI refuses exactly what the database would.
 *  The ASCII-only character class makes JS's UTF-16 `length` agree with Rust's
 *  byte length.
 *
 *  `app`/`www` are still refused because migration 0079's
 *  `tenant_databases_slug_not_reserved` CHECK still refuses them — the
 *  constraint outlives the subdomains that motivated it, so accepting them here
 *  would just move the rejection to an INSERT the user can't see. (The
 *  module-level `RESERVED_SLUGS` const they used to come from was a HOST-plane
 *  concept — "labels that mean the apex, never a tenant" — and died with the
 *  hostnames; this list is a database-lockstep list.) */
export function isValidSlug(slug: string): boolean {
  if (slug === "app" || slug === "www") return false;
  if (slug.length < 3 || slug.length > 32) return false;
  if (!/^[a-z0-9-]+$/.test(slug)) return false;
  return !slug.startsWith("-") && !slug.endsWith("-");
}

/**
 * Is the browser on a host this deployment owns — i.e. one where `api.<apex>`
 * is known to exist?
 *
 * True for `<apex>` itself and ANY host under it: `app.`, `www.`, and the
 * `<slug>.<apex>` hosts that org users still reach until the Phase 4 cutover
 * lands its 308 redirect. Deliberately no slug-shape check: a hostname no
 * longer names a tenant, so there is nothing left for a label to be valid or
 * invalid *for*, and the 43-character Clerk slug that used to fall out of this
 * classification (and produce a 403 telling the user to visit the subdomain
 * they were already on) simply routes now.
 *
 * False — meaning "use `NEXT_PUBLIC_API_BASE`" — for localhost, `*.fly.dev`,
 * lookalike domains (`evilcoralswarm.example`, matched on a label boundary rather
 * than a bare string suffix), SSR/prerender, and any build with no
 * `NEXT_PUBLIC_WEB_APEX`.
 */
function isOurHost(): boolean {
  // SSR / prerender: no host to read.
  if (typeof window === "undefined") return false;
  if (!WEB_APEX) return false;
  const host = normalizeHost(window.location.hostname);
  return host === WEB_APEX || host.endsWith(`.${WEB_APEX}`);
}

/**
 * API base for every request, on every plane.
 *
 * 1. `<apex>` and anything under it → `https://api.<apex>`;
 * 2. otherwise (localhost, `*.fly.dev`, SSR) → `NEXT_PUBLIC_API_BASE`.
 *
 * Never memoized: a value computed during prerender would poison the client.
 */
export function apiBase(): string {
  return isOurHost() ? `https://api.${WEB_APEX}` : FALLBACK_BASE;
}

/**
 * Claim-based tenancy cutover, Phase 4 step 2
 * (docs/plans/claim-based-tenancy.md §Phase 4, decision 6 — "Hard cutover.
 * 308 before auth; delete the `Subdomain` arm."): org subdomains
 * (`<slug>.coralswarm.example`) are removed, but DNS is untouched — the wildcard
 * `*.coralswarm.example` CNAME stays (decision 6, step 4: "Keep the
 * `*.coralswarm.example` CNAME — it is load-bearing elsewhere") — so anyone with
 * an org subdomain bookmarked still resolves straight into this app. Without
 * a redirect they get a page that renders and then fails every request: PR
 * 1.3 (#794) narrowed `is_allowed_cors_origin` to `HostTarget::Apex` only, so
 * an `Origin` of `https://acme.coralswarm.example` is no longer accepted.
 *
 * This module is the single source of truth for the redirect DECISION.
 * `next.config.ts`'s `redirects()` cannot call it directly at request time —
 * the `has: [{ type: "host", value }]` field it configures wants a plain
 * regex STRING, compiled once by Next's router when the server boots, not a
 * per-request callback — so `hostRedirectPattern()` produces that exact
 * string, and both `next.config.ts` (production behaviour) and
 * `orgSubdomainRedirectTarget` below (this file's own tested predicate) build
 * their answer from it, so they cannot drift apart
 * (`nextConfigRedirects.test.ts` pins the two together).
 *
 * `orgSubdomainRedirectTarget` exists so this decision has a name and a test
 * file, per this repo's convention for a web app with no component-test
 * harness (see `components/tenantIndicator.ts`, `lib/activeOcean.ts`,
 * `lib/engine/vaultNamespace.ts`): every real decision lives in a pure,
 * by-name-tested module; the `next.config.ts` wiring itself carries no logic
 * beyond this constant.
 *
 * WHAT REDIRECTS: exactly one label under the apex — `<slug>.coralswarm.example`
 * — whatever the label, whether or not it was ever a real, provisioned org
 * slug. That is deliberate: decision 6 is a HARD cutover, so this is a pure
 * host-SHAPE decision with no dependency on a live tenant list. It does not
 * need to agree with `is_valid_slug`'s shape rules (`backend/src/tenancy/
 * gate.rs`, `web/lib/apiBase.ts`) — those exist to gate NEW provisioning, a
 * different concern than "does this old bookmark still make sense" — so a
 * malformed-looking label still redirects rather than falling through to a
 * 404 the visitor can't do anything about.
 *
 * WHAT DOES NOT, on purpose — and the standard each exclusion has to clear:
 * a label is excluded ONLY when something can be shown, live, to actually
 * serve it differently. `dig`/DNS alone is not that evidence: the
 * `*.coralswarm.example` wildcard CNAMEs EVERY label to this same Fly app
 * (`coralswarm-web`) — verified live: `addin.coralswarm.example` and a made-up
 * probe label like `zzz-nonexistent.coralswarm.example` both resolve to
 * `coralswarm-web.example.` and both return this exact app's HTML. A label
 * nobody has ever registered behaves identically to one that "sounds"
 * reserved. Absent a live, positive reason, a label redirects like any
 * other — that IS decision 6's hard cutover.
 *
 * - the bare apex (`coralswarm.example`) — zero labels before it; also verified
 *   live as a DIFFERENT deployment (Cloudflare A records, not the Fly
 *   wildcard CNAME — the marketing site, `marketing-web/wrangler.jsonc`'s
 *   `routes: [{pattern: "coralswarm.example"}]`).
 * - `app.` — the ONLY label excluded for a structural reason rather than a
 *   "something else serves it" one: it is this redirect's own destination.
 *   Not excluding it would redirect `app.coralswarm.example` to itself
 *   (`ERR_TOO_MANY_REDIRECTS` in a browser) for the one host every real
 *   customer is supposed to land on. `RESERVED_SLUGS`
 *   (`backend/src/tenancy/gate.rs`) already refusing it as a tenant slug is
 *   corroboration, not the load-bearing reason.
 * - `www.` — verified LIVE as a different, already-serving host, not
 *   inferred from a config file: `curl -I https://www.coralswarm.example`
 *   answers `308` (a real redirect to the apex, already happening) and
 *   `dig www.coralswarm.example CNAME` returns nothing pointing at
 *   `coralswarm-web.example` — unlike `addin.` and the nonsense probe label
 *   above, `www.` is NOT wildcard-caught; it has its own Cloudflare custom
 *   domain (`marketing-web/wrangler.jsonc`) that already answers before this
 *   app's DNS record would ever apply. Matching `api.`'s reasoning below:
 *   even if that ever changed, redirecting `www.` here would be a safe
 *   fallback, not a break — it is excluded because it demonstrably isn't
 *   ours to redirect today, not because redirecting it would be harmful.
 * - `api.` (bare, single label) — NOT excluded: `api.coralswarm.example` is a
 *   different Fly app (`coralswarm-backend`) reached via its own DNS CNAME
 *   (verified: `dig api.coralswarm.example CNAME` → `coralswarm-backend.example.`,
 *   distinct from the wildcard target), which takes priority over
 *   `*.coralswarm.example` for that exact name, so structurally this host can
 *   never reach this app's `next.config.ts` in production. Leaving it
 *   matching the general pattern costs nothing and needs no exclusion list
 *   entry to maintain.
 * - anything with more than one label before the apex, e.g.
 *   `acme.api.coralswarm.example` — the real, customer-facing shape of the API
 *   host. It is a different host, a different Fly app, and cutover step 3
 *   (docs/plans/claim-based-tenancy.md §Phase 4: "Keep the API subdomain
 *   redirecting for one release, then hard-error naming the new base") —
 *   explicitly NOT this PR.
 * - anything that isn't under the apex at all: `evil.com`,
 *   `coralswarm.example.evil.com` (apex as a suffix trick), `evilcoralswarm.example`
 *   (apex glued on with no label boundary — the same adversarial case
 *   `web/lib/apiBase.test.ts` names "lookalike apex suffix"),
 *   `coralswarm-web.example` (the Fly default hostname — an internal/ops URL,
 *   never a user-facing one).
 *
 * CORRECTED, against an earlier version of this file: `addin.coralswarm.example`
 * used to be excluded here on the theory that it was a real, separately
 * hosted Outlook add-in (`extensions/outlook-addin/`). Verified live and
 * false on both counts: `fly apps list` shows no `coralswarm-addin` app —
 * it was never deployed — and `curl https://addin.coralswarm.example/` returns
 * this exact Next.js app's HTML (same as `app.coralswarm.example`), not the
 * add-in's static bundle. `addin` is simply a label nobody has claimed,
 * caught by the wildcard like `zzz-nonexistent.coralswarm.example` would be.
 * Excluding it left exactly the bug this PR exists to close on one host:
 * CORS-narrowed by PR 1.3, rendering a page that then fails every request,
 * silently. It now redirects like any other unclaimed label. If the add-in
 * is ever actually deployed, it gets its own Fly app and its own explicit
 * DNS record — which, like `api.`'s, takes priority over the wildcard, so
 * this rule would simply stop seeing that traffic; nothing here would need
 * to change to accommodate it.
 */

const DEFAULT_APEX = "coralswarm.example";

/** The one label excluded from the redirect — see the module doc's "WHAT
 *  DOES NOT" list. `app` is structural (it's the redirect's own
 *  destination); everything else that used to be listed here (`www`,
 *  `addin`) is either demonstrably not reachable through this app at all,
 *  or — `addin`'s corrected story — was wrongly assumed to be, and now
 *  redirects like any other unclaimed label. */
const NON_TENANT_LABELS = ["app"];

function escapeRegExp(raw: string): string {
  return raw.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

/**
 * The regex STRING (not a `RegExp`) that `next.config.ts` hands to Next's
 * `has: [{ type: "host", value }]` for the org-subdomain redirect, and that
 * `orgSubdomainRedirectTarget` below compiles into its own anchored `RegExp`.
 * Neither caller adds the exclusion list or the apex a second time.
 *
 * `[^.]+` (rather than a charset restricted to `is_valid_slug`'s
 * lowercase-alnum-hyphen rule) is deliberate — see the module doc's "WHAT
 * REDIRECTS" note. The apex is regex-escaped so its literal `.` cannot act as
 * a "match any character" wildcard (`nextConfigRedirects.test.ts` and this
 * file's own test pin the escaped, byte-exact string).
 */
export function hostRedirectPattern(apex: string = DEFAULT_APEX): string {
  const exclude = NON_TENANT_LABELS.map((label) => `${escapeRegExp(label)}\\.`).join("|");
  return `(?!${exclude})[^.]+\\.${escapeRegExp(apex)}`;
}

/** Strip a trailing `:port` and a trailing dot (FQDN form), and lowercase —
 *  mirrors the canonicalization `backend/src/tenancy/gate.rs::canonicalize`
 *  and `web/lib/apiBase.ts::normalizeHost` already apply to a raw `Host`
 *  header. Real org slugs can never need the lowercasing in practice —
 *  `is_valid_slug` and the `tenant_databases.slug` CHECK only ever allowed
 *  lowercase — but a crafted `Host` header could still arrive mixed-case, and
 *  there is no reason this predicate should answer differently for one.
 *
 *  Only this function's caller (`orgSubdomainRedirectTarget`) benefits from
 *  this normalization — `next.config.ts`'s own `has: host` match tests the
 *  raw incoming `Host` header directly, with no such preprocessing. A `:port`
 *  or trailing dot on a real HTTPS request to a custom domain essentially
 *  never happens, so the one place this diverges from production is
 *  deliberately left as a known, low-stakes gap rather than solved twice: a
 *  stray one there simply isn't redirected (served exactly as it is today,
 *  never worse). */
function normalizeHost(raw: string): string {
  let h = raw.trim();
  const colon = h.lastIndexOf(":");
  if (colon > -1 && /^\d+$/.test(h.slice(colon + 1))) h = h.slice(0, colon);
  h = h.toLowerCase();
  if (h.endsWith(".")) h = h.slice(0, -1);
  return h;
}

/**
 * `https://app.<apex>` if `rawHost` is a `<label>.<apex>` org-subdomain shape
 * that must be redirected away (see the module doc), or `null` if `rawHost`
 * should be served as-is — including every host that isn't ours at all.
 *
 * `apex` is a parameter, not a module-scope env read: this file must stay
 * directly testable, and `NEXT_PUBLIC_*` reads at module scope don't survive
 * vitest's per-test re-import the way `web/lib/apiBase.test.ts` needs them to
 * (see that file's `vi.resetModules()` dance) — passing it in sidesteps the
 * problem instead of fighting it.
 */
export function orgSubdomainRedirectTarget(rawHost: string, apex: string = DEFAULT_APEX): string | null {
  const host = normalizeHost(rawHost);
  const regex = new RegExp(`^${hostRedirectPattern(apex)}$`);
  return regex.test(host) ? `https://app.${apex}` : null;
}

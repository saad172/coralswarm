// Host → API-base matrix for lib/apiBase.ts (claim-based tenancy).
//
// The module reads `process.env.NEXT_PUBLIC_*` at
// import time (Next inlines them at build time), so every case sets the env and
// re-imports the module through `vi.resetModules()`. `window` is installed /
// removed per case: its ABSENCE is the SSR/prerender path the standalone build
// exercises, and getting that wrong crashes `next build`.
//
// WHY THE `apex` PLUMBING STAYS despite there being much less to test: under
// vitest `NEXT_PUBLIC_WEB_APEX` is unset, and `apiBase()` returns the env
// fallback for every host when it is. Assert only fallback values and the whole
// file passes with the module's logic deleted. The cases that carry weight are
// the ones expecting `https://api.coralswarm.example` — they are the only proof the
// host is read at all, and they only exist because `load()` sets the apex.

import { afterEach, describe, expect, it, vi } from "vitest";

const ENV_KEYS = ["NEXT_PUBLIC_API_BASE", "NEXT_PUBLIC_WEB_APEX"] as const;

interface Case {
  /** `undefined` ⇒ no `window` at all (SSR / prerender). */
  hostname?: string;
  apex?: string;
  apiBase?: string;
}

async function load(c: Case) {
  for (const k of ENV_KEYS) delete process.env[k];
  process.env.NEXT_PUBLIC_API_BASE = c.apiBase ?? "https://coralswarm-backend.example";
  if (c.apex !== undefined) process.env.NEXT_PUBLIC_WEB_APEX = c.apex;

  if (c.hostname === undefined) {
    delete (globalThis as { window?: unknown }).window;
  } else {
    (globalThis as { window?: unknown }).window = { location: { hostname: c.hostname } };
  }

  vi.resetModules();
  return await import("./apiBase");
}

afterEach(() => {
  for (const k of ENV_KEYS) delete process.env[k];
  delete (globalThis as { window?: unknown }).window;
  vi.restoreAllMocks();
});

const APEX = "coralswarm.example";
const API = `https://api.${APEX}`;
const FLY = "https://coralswarm-backend.example";

describe("apiBase() host matrix", () => {
  const table: Array<[string, Case, string]> = [
    // ── Env fallback: no host information, or not a host of ours ─────────────
    // SSR / prerender — no `window`. The standalone build prerenders client
    // components on the server; this must never throw.
    ["SSR (no window), apex configured", { apex: APEX }, FLY],
    ["SSR (no window), no apex", {}, FLY],
    [
      "localhost dev",
      { hostname: "localhost", apex: APEX, apiBase: "http://localhost:8080" },
      "http://localhost:8080",
    ],
    ["127.0.0.1", { hostname: "127.0.0.1", apex: APEX }, FLY],
    ["*.fly.dev (prod web today)", { hostname: "coralswarm-web.example", apex: APEX }, FLY],
    [
      "*.fly.dev (staging web)",
      {
        hostname: "coralswarm-web-staging.example",
        apex: APEX,
        apiBase: "https://coralswarm-backend-staging.example",
      },
      "https://coralswarm-backend-staging.example",
    ],
    // Derivation is off entirely with no apex, even ON the real apex.
    ["apex host but NEXT_PUBLIC_WEB_APEX unset", { hostname: APEX }, FLY],
    ["NEXT_PUBLIC_WEB_APEX empty string", { hostname: APEX, apex: "" }, FLY],
    // Suffix match is on a label boundary, not a bare string suffix — this is
    // the one case here that could hand an attacker's page our API base.
    ["lookalike apex suffix", { hostname: "evilcoralswarm.example", apex: APEX }, FLY],
    ["apex as a subdomain of someone else", { hostname: `${APEX}.evil.example.com`, apex: APEX }, FLY],
    ["unrelated domain", { hostname: "example.com", apex: APEX }, FLY],

    // ── A host of ours → the one API apex ────────────────────────────────────
    ["apex", { hostname: APEX, apex: APEX }, API],
    ["app. subdomain", { hostname: `app.${APEX}`, apex: APEX }, API],
    ["www. subdomain", { hostname: `www.${APEX}`, apex: APEX }, API],
    ["apex with trailing dot", { hostname: `${APEX}.`, apex: APEX }, API],
    ["apex with a port", { hostname: `${APEX}:3000`, apex: APEX }, API],
    ["apex configured with trailing dot / mixed case", { hostname: APEX, apex: "CoralSwarm.com." }, API],
    ["uppercase host normalized", { hostname: "APP.CoralSwarm.COM", apex: APEX }, API],

    // ── Legacy org hosts: SAME answer, no slug rules ─────────────────────────
    // Org users keep landing on `<slug>.<apex>` until Phase 4's 308 redirect
    // ships, and there is no longer anything for a label to be valid *for*.
    ["legacy org subdomain", { hostname: `acme.${APEX}`, apex: APEX }, API],
    ["legacy org subdomain, hyphenated", { hostname: `acme-team.${APEX}`, apex: APEX }, API],
    ["nested subdomain under our apex", { hostname: `a.b.${APEX}`, apex: APEX }, API],
    // The real production incident: a 43-character Clerk slug used to fall out
    // of host classification entirely, so the app talked to the apex and the
    // apex answered "use your organization's subdomain" — advice the user
    // could not follow. No label is parsed now, so it simply routes.
    [
      "43-character Clerk slug (the unroutable-host incident)",
      { hostname: `Alex-s-organization-1781542054204631874.${APEX}`, apex: APEX },
      API,
    ],
    ["two-character label", { hostname: `ab.${APEX}`, apex: APEX }, API],
    ["label with an underscore", { hostname: `acme_co.${APEX}`, apex: APEX }, API],
  ];

  it.each(table)("%s", async (_name, c, expected) => {
    const mod = await load(c);
    expect(mod.apiBase()).toBe(expected);
  });

  it("is not memoized — a second call re-reads the host", async () => {
    const mod = await load({ hostname: APEX, apex: APEX });
    expect(mod.apiBase()).toBe(API);
    (globalThis as { window?: unknown }).window = { location: { hostname: "localhost" } };
    expect(mod.apiBase()).toBe(FLY);
  });
});

// Not a routing rule any more — a FORM validator for the org-onboarding slug
// field, in lockstep with `gate.rs::is_valid_slug` and the
// `tenant_databases.slug` CHECK constraints (migrations 0078 + 0079).
describe("isValidSlug (lockstep with backend gate.rs::is_valid_slug)", () => {
  it.each([
    ["acme", true],
    ["acm", true],
    ["a1b2", true],
    ["acme-team", true],
    ["a".repeat(32), true],
    ["ab", false],
    ["a".repeat(33), false],
    ["-acme", false],
    ["acme-", false],
    ["ACME", false],
    ["acme_co", false],
    ["acme.co", false],
    // Still refused, for a database reason rather than a hostname one:
    // migration 0079's `tenant_databases_slug_not_reserved` CHECK outlived the
    // subdomains that motivated it, so accepting these here would just move
    // the rejection to an INSERT the user never sees.
    ["app", false],
    ["www", false],
    ["", false],
  ])("%s → %s", async (slug, expected) => {
    const mod = await load({ apex: APEX });
    expect(mod.isValidSlug(slug)).toBe(expected);
  });
});

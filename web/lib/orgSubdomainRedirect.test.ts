// Claim-based tenancy Phase 4 step 2 (docs/plans/claim-based-tenancy.md
// §Phase 4): the host matrix for the org-subdomain -> app.coralswarm.example
// redirect. See lib/orgSubdomainRedirect.ts's module doc for the full
// reasoning behind each exclusion; this file pins it to concrete hosts.

import { describe, expect, it } from "vitest";
import { hostRedirectPattern, orgSubdomainRedirectTarget } from "./orgSubdomainRedirect";

const APEX = "coralswarm.example";
const APP = `https://app.${APEX}`;

describe("orgSubdomainRedirectTarget()", () => {
  const table: Array<[string, string, string | null]> = [
    // ── Redirects: any org-subdomain SHAPE, real slug or not (hard cutover) ──
    ["a real org slug", "acme.coralswarm.example", APP],
    ["a slug containing 'app' as a substring, not as the whole label", "app-staging.coralswarm.example", APP],
    ["a hyphenated slug", "my-org.coralswarm.example", APP],
    ["a numeric-looking slug", "12345.coralswarm.example", APP],
    ["a short label — hard cutover doesn't validate slug length", "ab.coralswarm.example", APP],
    ["trailing dot (FQDN form) on the subdomain", "acme.coralswarm.example.", APP],
    ["a stray :port on the subdomain", "acme.coralswarm.example:8443", APP],
    ["mixed-case host — DNS is case-insensitive even though real slugs never are", "ACME.CoralSwarm.COM", APP],
    [
      "the bare API apex — 'api' isn't excluded because it is a different Fly app " +
        "(coralswarm-backend) reached via its own DNS CNAME, so this exact host " +
        "structurally can never reach next.config.ts in production",
      "api.coralswarm.example",
      APP,
    ],
    [
      "www. — NOT excluded here, matching 'api': verified live that it is served by " +
        "a different Cloudflare custom domain (its own www->apex redirect), not the " +
        "*.coralswarm.example wildcard, so this predicate's answer for it is never actually " +
        "reached in production; correct behaviour if it ever were",
      "www.coralswarm.example",
      APP,
    ],
    [
      "addin. — a wildcard-caught label with NO service behind it, not a reserved host. " +
        "Corrected from an earlier version of this file: there is no coralswarm-addin Fly " +
        "app (fly apps list has none) and addin.coralswarm.example serves this exact web app's " +
        "HTML today (curl-verified) — identical to a nonsense probe label like " +
        "zzz-nonexistent.coralswarm.example. It must redirect like any other unclaimed label; " +
        "excluding it would leave the exact CORS-then-silent-fail bug this PR exists to fix",
      "addin.coralswarm.example",
      APP,
    ],

    // ── Never redirects: apex-equivalent / known non-tenant hosts on THIS app ─
    ["the bare apex — zero labels, not a subdomain", "coralswarm.example", null],
    [
      "app. — the ONLY structurally-excluded label: this redirect's own destination " +
        "(not excluding it would redirect app.coralswarm.example to itself)",
      "app.coralswarm.example",
      null,
    ],

    // ── Never redirects: out of scope for THIS PR (cutover step 3) ────────────
    ["the real API-subdomain shape (<slug>.api.coralswarm.example) — a later, separate cutover", "acme.api.coralswarm.example", null],

    // ── Never redirects: not ours at all ───────────────────────────────────────
    ["a foreign domain", "evil.com", null],
    ["apex glued on as a suffix trick", "coralswarm.example.evil.com", null],
    ["apex glued on with no label boundary (\"lookalike apex suffix\", per apiBase.test.ts)", "evilcoralswarm.example", null],
    ["the apex's dot replaced by another char — must not match a mis-escaped regex", "acmeXcoralswarmXcom", null],
    ["the Fly default hostname", "coralswarm-web.example", null],
    ["localhost", "localhost", null],
    ["localhost with a port", "localhost:3000", null],
  ];

  it.each(table)("%s: %s -> %s", (_label, host, expected) => {
    expect(orgSubdomainRedirectTarget(host)).toBe(expected);
  });

  it("honors an explicit apex parameter instead of a module-scope env read", () => {
    expect(orgSubdomainRedirectTarget("acme.example.org", "example.org")).toBe("https://app.example.org");
    // The default apex no longer matches once a different one is passed in.
    expect(orgSubdomainRedirectTarget("acme.coralswarm.example", "example.org")).toBeNull();
  });
});

describe("hostRedirectPattern()", () => {
  it("is the exact string next.config.ts's has: [{ type: \"host\", value }] gets", () => {
    // Byte-exact on purpose: if this changes, the production redirect's real
    // regex changed, and nextConfigRedirects.test.ts (which reads this same
    // export) should be reviewed alongside it.
    expect(hostRedirectPattern()).toBe("(?!app\\.)[^.]+\\.coralswarm\\.com");
  });

  it("escapes a dotted apex so '.' cannot behave as a wildcard", () => {
    expect(hostRedirectPattern("a.b")).toBe("(?!app\\.)[^.]+\\.a\\.b");
  });
});

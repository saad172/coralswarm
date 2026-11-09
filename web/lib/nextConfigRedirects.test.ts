// Confirms next.config.ts actually WIRES the Phase-4 org-subdomain redirect
// (docs/plans/claim-based-tenancy.md §Phase 4) the way lib/orgSubdomainRedirect.ts
// promises: permanent (308), full path+query preserved (via the `:path*`
// catch-all, which Next substitutes into the destination and auto-appends
// the incoming query string to), and matched on the EXACT SAME host pattern
// orgSubdomainRedirect.ts exports — so the two files cannot drift apart.

import { describe, expect, it } from "vitest";
import type { NextConfig } from "next";
import { hostRedirectPattern } from "./orgSubdomainRedirect";

async function loadConfig(): Promise<NextConfig> {
  const mod = await import("../next.config");
  return mod.default;
}

/** Narrows `config.redirects` without a `!` (forbidden by this repo's lint
 *  config). */
async function loadRedirectRules(config: NextConfig) {
  if (!config.redirects) throw new Error("next.config.ts has no redirects()");
  return config.redirects();
}


describe("next.config.ts — org-subdomain redirect wiring", () => {
  it("registers a 308 (permanent), host-matched redirect to app.coralswarm.example preserving path", async () => {
    const config = await loadConfig();
    expect(config.redirects).toBeDefined();
    const rules = await loadRedirectRules(config);
    const rule = rules.find((r) => r.has?.some((h) => h.type === "host" && h.value === hostRedirectPattern()));
    expect(rule, "no redirect rule found matching orgSubdomainRedirect.ts's host pattern").toBeTruthy();
    expect(rule?.permanent).toBe(true); // permanent: true => 308, per Next.js's redirects() contract
    expect(rule?.source).toBe("/:path*");
    expect(rule?.destination).toBe("https://app.coralswarm.example/:path*");
  });

  it("does not touch the pre-existing /join redirect", async () => {
    const config = await loadConfig();
    const rules = await loadRedirectRules(config);
    const joinRule = rules.find((r) => r.source === "/join");
    expect(joinRule?.destination).toBe("/onboarding/join");
    expect(joinRule?.permanent).toBe(false);
  });

  it("always builds the standalone server", async () => {
    const config = await loadConfig();
    expect(config.output).toBe("standalone");
  });
});

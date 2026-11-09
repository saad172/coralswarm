import { defineConfig } from "@playwright/test";
import base from "./playwright.config";

// N-1 compatibility (README "N-1 compatibility"). Two invocations against one
// stack and one database:
//   1. `n1/seed.spec.ts` on THIS tree's e2e backend writes users, oceans,
//      sources, seeded extraction and an org through the API, and records
//      what it wrote in `.stack/n1-manifest.json`;
//   2. `n1/serve.spec.ts` on the PRODUCTION release's backend, booted on the
//      same database, reads it all back.
// The scenarios live outside `tests/`, so the PR shards never run them.
export default defineConfig({
  ...base,
  testDir: "./n1",
  // Seed then serve are separate runs; inside each, keep file order.
  fullyParallel: false,
  grep: undefined,
});

import { defineConfig, devices } from "@playwright/test";
import { runId } from "./stack/clerk-cleanup.mjs";
import { REMOTE, TARGET, WEB_BASE } from "./stack/config.mjs";

// Pin this run's id before any worker starts (they inherit E2E_RUN_ID).
runId();
// The page-load budget proves a pending read's 2 s poll spacing with the
// page's own clock (stack/build-web.mjs builds the receipts in). Node sees
// request events late on a loaded runner, so its clock alone can misread
// that poll as a second serial critical read.
process.env.E2E_CAPTURE_PAGE_READ_RECEIPTS ??= "1";

const CI = !!process.env.CI;

// One stack per runner and one worker per stack: every scenario shares the
// runner's backend, so parallelism comes from shards (`--shard=i/N`), each its
// own runner, stack and control database. Add shards, not workers.
export default defineConfig({
  testDir: "./tests",
  // The <30s rule (README): a scenario that needs longer is two scenarios,
  // or is waiting on something it should seed instead.
  timeout: 30_000,
  expect: { timeout: 10_000 },
  // Every scenario creates its own users and org, so any test can run in any
  // shard: shards split by test, not by file.
  fullyParallel: true,
  workers: 1,
  forbidOnly: CI,
  retries: CI ? 1 : 0,
  // `@quarantine` scenarios run only in their own non-blocking job
  // (`npm run test:quarantine`); the blocking run never sees them.
  grepInvert: process.env.E2E_QUARANTINE ? /@nightly/ : /@quarantine|@nightly/,
  // Against a deployed target only `@smoke` may run, whatever --grep says:
  // every other scenario leans on the local stack's fakes or seeding route.
  ...(REMOTE ? { grep: /@smoke/ } : {}),
  metadata: { target: TARGET },
  reporter: CI ? [["blob"], ["github"], ["list"]] : [["list"], ["html", { open: "never" }]],
  globalSetup: "./lib/global-setup.ts",
  globalTeardown: "./lib/global-teardown.ts",
  use: {
    baseURL: WEB_BASE,
    trace: "retain-on-failure",
    screenshot: "only-on-failure",
    video: "retain-on-failure",
    actionTimeout: 10_000,
    navigationTimeout: 15_000,
  },
  projects: [{ name: "chromium", use: { ...devices["Desktop Chrome"] } }],
});

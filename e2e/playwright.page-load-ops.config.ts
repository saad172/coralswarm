import { defineConfig } from '@playwright/test';
import base from './playwright.config';
import { REMOTE } from './stack/config.mjs';
if (REMOTE || process.env.E2E_API_ONLY !== 'true' || process.env.E2E_SKIP_CLERK_CLEANUP !== '1' || !process.env.E2E_RUN_ID) {
  throw new Error('ops validation requires an explicitly owned API-only local run and exact-prefix cleanup');
}
export default defineConfig(base, {
  testDir: './perf', testMatch: 'ops-validation.spec.ts', grep: /@nightly/, grepInvert: /@quarantine/,
  retries: 0, workers: 1, fullyParallel: false, reporter: [['list']],
});

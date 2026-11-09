import { defineConfig } from '@playwright/test';
import base from './playwright.config';
import { REMOTE } from './stack/config.mjs';
if (REMOTE) throw new Error('page-load-speed may run only against its owned local stack');
if (!process.env.E2E_RUN_ID || process.env.E2E_SKIP_CLERK_CLEANUP !== '1') {
  throw new Error('nightly requires an explicit run id and exact-prefix cleanup; disable global stale sweeps');
}
export default defineConfig(base, {
  testMatch: 'page-load-speed.spec.ts',
  grep: /@nightly/,
  grepInvert: /@quarantine/,
  fullyParallel: false,
  workers: 1,
  retries: 0,
  timeout: 240_000,
  // Full JSON ledgers retain every measured request. A single 90-minute
  // twenty-load/all-route test must not accumulate thousands of videos or a
  // multi-gigabyte browser trace; ordinary PR failure traces stay unchanged.
  use: { trace: 'off', video: 'off', screenshot: 'only-on-failure' },
  reporter: [['list'], ['json', { outputFile: 'page-load-evidence/playwright.json' }]],
});

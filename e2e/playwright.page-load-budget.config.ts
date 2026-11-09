import { defineConfig } from '@playwright/test';
import base from './playwright.config';

// The full page-load budget matrix, @nightly samples included. The merge queue
// runs only the core samples (tests/page-load-budget.spec.ts `CORE`).
export default defineConfig(base, {
  testMatch: 'page-load-budget.spec.ts',
  grepInvert: /@quarantine/,
  retries: 0,
});

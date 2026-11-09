import { defineConfig } from '@playwright/test';
import base from './playwright.config';

// Today's flag-selection cases (@nightly): run with --grep against a backend
// restarted with TODAY_ACTIVITIES_ENABLED / TODAY_GIST_ENABLED set to match.
export default defineConfig(base, {
  testMatch: 'page-load-today-selection.spec.ts',
  grepInvert: /@quarantine/,
  retries: 0,
});

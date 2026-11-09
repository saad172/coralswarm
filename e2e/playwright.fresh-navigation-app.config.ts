import { defineConfig } from '@playwright/test';
import standalone from './playwright.stored-refresh-app.config';

// Real production Next build + real Clerk DEV auth; controlled backend DTOs.
// Use E2E_API_PORT=13703 both when building and when running this config.
export default defineConfig({ ...standalone, testDir: './fresh-navigation-app',
  outputDir: './test-results/fresh-navigation-app' });

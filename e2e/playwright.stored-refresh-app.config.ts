import { defineConfig, devices } from '@playwright/test';
import { requireClerk } from './stack/config.mjs';

const { publishableKey, secretKey } = requireClerk();
process.env.CLERK_PUBLISHABLE_KEY = publishableKey;
process.env.CLERK_SECRET_KEY = secretKey;
process.env.NEXT_PUBLIC_CLERK_PUBLISHABLE_KEY = publishableKey;
const port = 13702;

// The actual standalone Next app and Clerk DEV session. Only backend DTOs are
// fixtures: this catches auth/bootstrap/timer interactions the Vite shims omit.
export default defineConfig({
  testDir: './stored-refresh-app',
  timeout: 30_000,
  expect: { timeout: 10_000 },
  workers: 1,
  retries: 0,
  reporter: [['list']],
  globalSetup: './stored-refresh-app/setup.ts',
  outputDir: './test-results/stored-refresh-app',
  use: { baseURL: `http://localhost:${port}`, trace: 'retain-on-failure', screenshot: 'only-on-failure' },
  projects: [{ name: 'chromium', use: { ...devices['Desktop Chrome'] } }],
  webServer: {
    command: 'node ../web/.next/standalone/web/server.js',
    url: `http://localhost:${port}/login`,
    env: { PORT: String(port), HOSTNAME: '127.0.0.1', NODE_ENV: 'production' },
    reuseExistingServer: false,
  },
});

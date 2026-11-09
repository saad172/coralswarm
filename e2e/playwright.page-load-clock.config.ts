import { defineConfig } from '@playwright/test';
// Isolated mocked-I/O clock contracts; deliberately no stack or Clerk setup.
export default defineConfig({ testDir: './tests', testMatch: ['page-load-clock.spec.ts', 'page-load-semantic-classifier.spec.ts', 'page-load-semantic-fanin.spec.ts', 'page-load-budget-warm-fixture.spec.ts'],
  outputDir: './test-results/page-load-clock', workers: 1, fullyParallel: false, retries: 0, reporter: [['list']] });

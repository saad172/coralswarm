import { test, expect } from '@playwright/test';
import { seedPageBudgetFixture } from '../lib/pageBudgetFixture';
import { signIn, activateOrganization } from '../lib/fixtures';
import { assertPageLoadBudget } from '../lib/pageLoadBudget';
import { deleteE2eUser } from '../lib/clerk';

// Select the matching cases with --grep after explicitly starting the owned
// backend with these real flags. A mismatch fails; there are no conditional skips.
// @nightly keeps them out of the default run, whose backend has both flags on;
// run them with -c playwright.page-load-flags.config.ts.
for (const plane of ['personal', 'org'] as const) for (const variant of ['activities-off', 'gist-off'] as const) {
  for (const period of ['day', 'week', 'month'] as const) test(`${plane}: ${variant} Today ${period} selects only the enabled indexed payloads`, { tag: '@nightly' }, async ({ page, request }, info) => {
    test.setTimeout(180_000);
    const f = await seedPageBudgetFixture(request, plane);
    try {
      const bootstrap = await f.api.json('GET', '/v1/bootstrap');
      expect(bootstrap.activities_enabled).toBe(variant === 'gist-off');
      expect(bootstrap.gist_enabled).toBe(false);
      await signIn(page, f.email); if (f.orgId) await activateOrganization(page, f.orgId);
      const rows = await assertPageLoadBudget(page, `/today?z=${period}`, { maxRequests: 6, maxDepth: 1, mode: 'owed' });
      const journal = rows.filter((row) => row.path.startsWith('/v1/journal'));
      expect(journal.every((row) => row.status === 200)).toBe(true);
      expect(journal.some((row) => row.path === '/v1/journal/gist')).toBe(false);
      const initial = journal.filter((row) => row.ledger === 'initial');
      if (variant === 'activities-off') {
        expect(journal.every((row) => row.path === '/v1/journal')).toBe(true);
        expect(initial).toHaveLength(period === 'day' ? 1 : 2);
        const spans = initial.map((row) => new URL(row.address, 'http://fixture.invalid').searchParams.get('granularity'));
        expect(spans).toEqual(expect.arrayContaining(period === 'day' ? ['hour'] : period === 'week' ? ['day', 'week'] : ['week', 'month']));
      } else {
        expect(initial.map((row) => row.model).sort()).toEqual(['journal_activities', 'journal_strip']);
      }
      await info.attach('real-feature-selection-bootstrap-and-ledger', { body: Buffer.from(JSON.stringify({ plane, variant, period, bootstrap, rows }, null, 2)), contentType: 'application/json' });
    } finally { await deleteE2eUser(f.userId); }
  });
}

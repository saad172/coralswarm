import { test, expect } from '@playwright/test';
import { setupClerkTestingToken } from '@clerk/testing/playwright';
import { pageRoutes } from '../../web/lib/pageRoutes';
import { assertPageLoadBudget } from '../lib/pageLoadBudget';

for (const entry of pageRoutes.filter((route) => route.access === 'public' && route.budget === 'page')) {
  for (const template of entry.samples) {
    test(`signed out: ${template} declared public page budget`, async ({ page }) => {
      await setupClerkTestingToken({ page });
      const path = template.replace('{shareToken}', 'unavailable-legacy-share-fixture').replace('{prefillToken}', 'unavailable-prefill-fixture');
      const expectedRefusals = 'refusals' in entry ? entry.refusals : [];
      const rows = await assertPageLoadBudget(page, path, { maxRequests: 6, maxDepth: 1, expectedRefusals });
      expect(rows.filter((row) => row.priority === 'critical').length).toBeLessThanOrEqual(6);
    });
  }
}

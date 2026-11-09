import { expect, type Page } from '@playwright/test';
import { assertPageLoadBudget } from './pageLoadBudget';

/** A new document and a real Clerk session, with only optional UI delivery
 * withheld. No auth clocks, token responses, or account checks are replaced. */
export async function bootstrapWithoutOptionalClerkUI(page: Page) {
  await page.goto('about:blank');
  let release!: () => void;
  const held = new Promise<void>(resolve => { release = resolve; });
  let uiRequested = false;
  const ui = /\/npm\/@clerk\/ui@[^/]+\/dist\/ui\.browser\.js/;
  await page.route(ui, async route => {
    uiRequested = true;
    await held;
    await route.continue();
  });
  try {
    const bootstrap = page.waitForRequest(request => new URL(request.url()).pathname === '/v1/bootstrap', { timeout: 4_000 });
    await page.goto('/home', { waitUntil: 'domcontentloaded' });
    await bootstrap;
    // Deliberately never release UI until bootstrap has started. This goes
    // red if auth initialization waits on the optional settings/menu bundle.
    expect(uiRequested, 'Clerk UI entry is bundled rather than fetched from its CDN').toBe(false);
  } finally {
    release();
    await page.unroute(ui);
  }
  await expect(page.locator('[data-page-loaded="true"][data-page-failed="false"]')).toBeVisible();
}

export async function initialReadsOverlapBoot(page: Page, path: string, required: string[]) {
  await page.goto('about:blank');
  let release!: () => void;
  const held = new Promise<void>(resolve => { release = resolve; });
  const count = new Map<string, number>();
  const record = (request: import('@playwright/test').Request) => {
    const address = new URL(request.url()).pathname;
    if (required.includes(address)) count.set(address, (count.get(address) ?? 0) + 1);
  };
  page.on('request', record);
  // Match only the two declared contracts, never unknown/duplicate modes,
  // extra parameters or subpaths. Reuse this exact matcher for teardown.
  const bootstrap = /^https?:\/\/[^/?#]+\/v1\/bootstrap(?:\?billing_banner=trial_v1)?$/;
  let entered!: () => void;
  let holdTimeout: ReturnType<typeof setTimeout> | undefined;
  const bootstrapHeld = new Promise<void>((resolve, reject) => {
    entered = () => { clearTimeout(holdTimeout); resolve(); };
    holdTimeout = setTimeout(() => reject(new Error('bootstrap did not enter the hold within 4,000 ms')), 4_000);
  });
  void bootstrapHeld.catch(() => {});
  await page.route(bootstrap, async route => { entered(); await held; await route.fallback(); });
  const waits: Promise<unknown>[] = [bootstrapHeld];
  let measured: ReturnType<typeof assertPageLoadBudget> | undefined;
  try {
    waits.push(...required.map(address => page.waitForRequest(request => new URL(request.url()).pathname === address, { timeout: 4_000 })));
    // Observe each rejection immediately, including when an earlier wait fails.
    const allReads = Promise.all(waits);
    void allReads.catch(() => {});
    measured = assertPageLoadBudget(page, path, { maxRequests: 6, maxDepth: 1, mode: 'owed' });
    void measured.catch(() => {});
    await allReads;
    for (const address of required) expect(count.get(address), `one early read for ${address}`).toBe(1);
    await expect(page.locator('[data-page-loaded="true"]')).toHaveCount(0);
    await expect(page.getByRole('main')).toHaveCount(0);
    release();
    const rows = await measured;
    await expect(page.locator('[data-page-loaded="true"][data-page-failed="false"]')).toBeVisible();
    // Cold backends owe worker recovery. The full budget recorder validates
    // every same-scope certificate, poll interval and finite recovery cycle.
    // Only proven polls may follow the single canonical initial read.
    for (const address of required) expect(rows.filter(row => row.path === address && row.method === 'GET' && row.ledger !== 'owed-refresh'),
      `one canonical initial read for ${address}`).toHaveLength(1);
  } finally {
    release();
    // Settle every request wait and the recorder before removing the route,
    // including when an absence assertion or one of the early reads fails.
    await Promise.allSettled([...waits, ...(measured ? [measured] : [])]);
    clearTimeout(holdTimeout);
    await page.unroute(bootstrap);
    page.off('request', record);
  }
}

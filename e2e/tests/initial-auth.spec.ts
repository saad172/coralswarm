import { test, expect } from '../lib/fixtures';
import { bootstrapWithoutOptionalClerkUI, initialReadsOverlapBoot } from '../lib/initialAuthBrowser';

test('bundled Clerk still renders the signed-out login UI', async ({ page }) => {
  await page.goto('/login');
  await expect(page.getByRole('textbox').first()).toBeVisible();
});

test('fresh document: real auth and required Home content do not wait for optional Clerk UI', async ({ page, signedIn }) => {
  void signedIn;
  await bootstrapWithoutOptionalClerkUI(page);
});

for (const [path, required] of [
  ['/home', ['/v1/home/continue', '/v1/home/whats-new', '/v1/home/attention', '/v1/conversations']],
  ['/connectors', ['/v1/connectors', '/v1/conversations']],
  ['/workspaces', ['/v1/load/oceans', '/v1/conversations']],
] as const) test(`fresh ${path} reads overlap bootstrap while content waits for admission`, async ({ page, signedIn }) => {
  void signedIn;
  await initialReadsOverlapBoot(page, path, [...required]);
});

// Causal control for the depth recorder: Today needs bootstrap metadata, but
// must initiate all parameterized reads even while the early sidebar cannot
// respond. This uses real auth, backend certificates and workers.
test('fresh Today initiates its reads while the independent sidebar response is withheld', async ({ page, signedIn }) => {
  void signedIn;
  await page.goto('about:blank');
  let release!: () => void;
  const held = new Promise<void>(resolve => { release = resolve; });
  await page.route('**/v1/conversations', async route => { await held; await route.fallback(); });
  try {
    const sidebar = page.waitForRequest(request => new URL(request.url()).pathname === '/v1/conversations');
    const boot = page.waitForResponse(response => new URL(response.url()).pathname === '/v1/bootstrap' && response.status() === 200);
    const reads = Promise.all(['/v1/journal/activities', '/v1/journal/strip', '/v1/journal/gist'].map(path =>
      page.waitForRequest(request => new URL(request.url()).pathname === path, { timeout: 4_000 })));
    void reads.catch(() => {});
    await page.goto('/today', { waitUntil: 'domcontentloaded' });
    await Promise.all([sidebar, boot, reads]);
  } finally {
    release();
    await page.unroute('**/v1/conversations');
  }
  await expect(page.locator('[data-page-loaded="true"][data-page-failed="false"]')).toBeVisible();
});

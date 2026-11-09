import { expect, type Page } from '@playwright/test';

// Client contract scenarios: real page/query/transport code, intercepted server
// certificates. These do not measure worker throughput or production traffic.
const connector = { key: 'slack', name: 'Slack', beta: false, status: 'not_connected', sources_on: 0,
  sources_total: 1, last_captured_at: null, importing: null, problem: null, resume_step: null, sources: [] };
async function clock(page: Page) {
  // Leave the authenticated landing document before controlling timers;
  // session cookies survive, while Home's observers/timers do not.
  await page.goto('about:blank');
  await page.clock.install();
  let start: number | undefined;
  return async (empty = false) => {
    // The real Clerk/AppProvider boot needs live timers. Freeze only once
    // its first authorized connectors read arrives, before publishing any
    // certificate or arming its recovery interval. Navigation time therefore
    // cannot consume the ETA window, and auth cannot deadlock behind it.
    if (start === undefined) {
      await page.clock.pauseAt(await page.evaluate(() => Date.now() + 1_000));
      start = await page.evaluate(() => Date.now());
    }
    return { model: 'connectors_list', version: 1, payload: { connectors: empty ? [] : [connector] },
      computed_at: new Date(start - 60_000).toISOString(), stale: true,
      refresh_eta: new Date(start + 60_000).toISOString() };
  };
}
const loaded = (page: Page) => page.locator('[data-page-loaded="true"]');
// Network delivery and TanStack's scheduled React notification are separate.
// Drain notification ticks while waiting, without letting navigation/auth
// wall time advance the controlled recovery clock.
async function settled(page: Page, ready: () => Promise<boolean>) {
  await expect.poll(async () => { await page.clock.runFor(1); return ready(); }).toBe(true);
}
async function openConnectors(page: Page) {
  const response = page.waitForResponse(r => new URL(r.url()).pathname === '/v1/connectors' && r.request().method() === 'GET');
  await page.goto('/connectors');
  await (await response).finished();
  await page.clock.runFor(1);
}
async function advanceRead(page: Page, milliseconds: number) {
  const response = page.waitForResponse(r => new URL(r.url()).pathname === '/v1/connectors' && r.request().method() === 'GET');
  await page.clock.fastForward(milliseconds);
  await (await response).finished();
  // Arm the next interval only after this read has actually published.
  await page.clock.runFor(1);
}

export async function storedAnswerSurvivesRecovery(page: Page) {
  const stored = await clock(page);
  let reads = 0;
  await page.route('**/v1/connectors', async route => { reads++; await route.fulfill({ json: await stored() }); });
  await openConnectors(page);
  await settled(page, () => page.getByRole('button', { name: 'Connect Slack', exact: true }).isVisible());
  await settled(page, async () => await loaded(page).count() === 1);
  await page.clock.fastForward(59_000); expect(reads).toBe(1);
  await advanceRead(page, 2_000); expect(reads).toBe(2);
  await advanceRead(page, 30_000); expect(reads).toBe(3);
  await advanceRead(page, 20_000); expect(reads).toBe(4);
  await page.clock.fastForward(120_000); expect(reads).toBe(4);
  await expect(page.getByRole('button', { name: 'Connect Slack', exact: true })).toBeVisible();
  await expect(page.getByText('Failed to load connectors', { exact: true })).toHaveCount(0);
  await settled(page, async () => await loaded(page).count() === 1);
}

export async function emptyAnswerPollsAndResolves(page: Page) {
  const stored = await clock(page);
  let reads = 0;
  await page.route('**/v1/connectors', async route => {
    reads++;
    const answer = await stored(true);
    await route.fulfill({ json: reads < 3 ? { ...answer, computed_at: null } :
      { ...answer, stale: false, refresh_eta: null } });
  });
  await openConnectors(page);
  await settled(page, () => page.getByText('loading connectors…', { exact: true }).isVisible());
  await settled(page, async () => await loaded(page).count() === 0);
  expect(reads).toBe(1);
  await advanceRead(page, 2_000); expect(reads).toBe(2);
  await settled(page, async () => await loaded(page).count() === 0);
  await advanceRead(page, 2_000); expect(reads).toBe(3);
  await settled(page, async () => await loaded(page).count() === 1);
  await expect(page.getByText('loading connectors…', { exact: true })).toHaveCount(0);
  await expect(page.getByRole('button', { name: 'Connect Slack', exact: true })).toHaveCount(0);
  await page.clock.fastForward(120_000); expect(reads).toBe(3);
}

export async function withheldAndRefusedAnswersClearData(page: Page) {
  const stored = await clock(page);
  let phase: 'stored' | 'withheld' | 'refused' = 'stored';
  let reads = 0;
  await page.route('**/v1/connectors/refresh', async route => route.fulfill({ status: 202, json: { refresh_eta: (await stored()).refresh_eta } }));
  await page.route('**/v1/connectors', async route => {
    reads++;
    if (phase === 'refused') return route.fulfill({ status: 403, json: { error: 'Access revoked' } });
    const answer = await stored(phase !== 'stored');
    await route.fulfill({ json: phase === 'stored' ? answer : { ...answer, computed_at: null } });
  });
  await openConnectors(page);
  await settled(page, () => page.getByRole('button', { name: 'Connect Slack', exact: true }).isVisible());
  await settled(page, async () => await loaded(page).count() === 1);
  phase = 'withheld';
  await page.getByRole('button', { name: 'Refresh Connector activity' }).click();
  await settled(page, () => page.getByText('loading connectors…', { exact: true }).isVisible());
  await expect(page.getByRole('button', { name: 'Connect Slack', exact: true })).toHaveCount(0);
  await expect(page.getByText('loading connectors…', { exact: true })).toBeVisible();
  await settled(page, async () => await loaded(page).count() === 0);
  phase = 'refused';
  const before = reads;
  await advanceRead(page, 2_000); expect(reads).toBe(before + 1);
  await settled(page, () => page.getByRole('alert').filter({ hasText: 'Access revoked' }).isVisible());
  await expect(page.getByRole('button', { name: 'Connect Slack', exact: true })).toHaveCount(0);
  await page.clock.fastForward(120_000); expect(reads).toBe(before + 1);
}

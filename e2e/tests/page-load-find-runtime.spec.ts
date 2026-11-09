import { test, expect } from '@playwright/test';
import { randomUUID } from 'node:crypto';
import { spawn } from 'node:child_process';
import { seedPageBudgetFixture, fixtureSql, holdPageWorkers } from '../lib/pageBudgetFixture';
import { signIn, activateOrganization } from '../lib/fixtures';
import { assertPageLoadBudget } from '../lib/pageLoadBudget';
import { deleteE2eUser } from '../lib/clerk';
import { API_BASE, CONTAINERS } from '../stack/config.mjs';
import { refreshRecoveryMs, refreshLimits } from '../../web/lib/pageRefreshPolicy';

async function holdConversationWorker(db: string): Promise<() => Promise<void>> {
  const child = spawn('docker', ['exec', '-i', CONTAINERS.pg, 'psql', '-XAt', '-v', 'ON_ERROR_STOP=1', '-U', 'postgres', '-d', db], { stdio: ['pipe', 'pipe', 'pipe'] });
  await new Promise<void>((resolve, reject) => {
    child.stdout.on('data', (chunk) => { if (String(chunk).includes('find-worker-barrier')) resolve(); });
    child.once('error', reject);
    child.once('exit', (code) => { if (code !== 0) reject(new Error(`Find barrier exited ${code}`)); });
    child.stdin.write("BEGIN; SET LOCAL lock_timeout='3s'; LOCK TABLE answer_support_receipts IN ACCESS EXCLUSIVE MODE; SELECT 'find-worker-barrier';\n");
  });
  return async () => { const ended = new Promise<void>((resolve) => child.once('exit', () => resolve())); child.stdin.end('ROLLBACK;\n'); await ended; };
}

// Real HTTP + registered worker only: no response interception, direct compute,
// publication inserts or outer-timeout overrides. The barrier holds the worker's
// ordinary nodes read while indexed GET remains available.
for (const plane of ['personal', 'org'] as const) test(`${plane}: saved Find cards use real worker publication, serve a reduced detail stale and clear it on recompute`, async ({ page, request }, info) => {
  test.setTimeout(240_000);
  const fixture = await seedPageBudgetFixture(request, plane);
  const envelopes: { at: number; status: number; body: unknown }[] = [];
  const fixtureMutations: { kind: string; passageId: string; affected: number }[] = [];
  const actions: { at: number; path: string; status: number; body: unknown }[] = [];
  const starts: { method: string; path: string; priority: string | null; started: number; finished: number | null; status: number | null; failure: string | null }[] = [];
  const inFlight = new Map<import('@playwright/test').Request, typeof starts[number]>();
  page.on('request', (request) => {
    const url = new URL(request.url()); if (url.origin !== new URL(API_BASE).origin) return;
    const row = { method: request.method(), path: url.pathname + url.search, priority: request.headers()['x-coralswarm-priority'] ?? null, started: Date.now(), finished: null, status: null, failure: null };
    starts.push(row); inFlight.set(request, row);
  });
  page.on('requestfinished', (request) => { const row = inFlight.get(request); if (row) row.finished = Date.now(); });
  page.on('requestfailed', (request) => { const row = inFlight.get(request); if (row) { row.finished = Date.now(); row.failure = request.failure()?.errorText ?? 'failed'; } });
  page.on('response', (response) => { const row = inFlight.get(response.request()); if (row) row.status = response.status(); });
  let id = ''; let release: (() => Promise<void>) | undefined;
  const limit = refreshRecoveryMs(refreshLimits('conversation_detail'));
  const observe = async (response: import('@playwright/test').Response) => {
    if (id && new URL(response.url()).pathname === `/v1/load/conversations/${id}`)
      envelopes.push({ at: Date.now(), status: response.status(), body: await response.json().catch(() => null) });
  };
  page.on('response', observe);
  const refresh = async () => {
    const path = `/v1/load/conversations/${id}/refresh`;
    const response = await fixture.api.call('POST', path);
    actions.push({ at: Date.now(), path, status: response.status(), body: await response.json() });
    expect(response.status()).toBe(202);
  };
  try {
    const token = `l5find${randomUUID().slice(0, 8)}`;
    await fixture.api.seedSource(fixture.graph, `The ${token} pricing review chose three usage tiers. Tom Becker owns the ${token} rollout checklist.`,
      `e2e-find-${randomUUID()}`, { sourceType: 'transcript', participants: ['Tom Becker', 'Sara Kim'], durationMs: 42 * 60_000 });
    await signIn(page, fixture.email);
    if (fixture.orgId) await activateOrganization(page, fixture.orgId);
    await page.goto(`/chats?graph_id=${fixture.graph}`);
    const question = page.getByRole('textbox', { name: 'Question' });
    await expect(question).toBeEnabled();
    await question.fill(`Find the ${token} pricing meeting`); await question.press('Enter');
    const cards = page.getByRole('region', { name: 'Matching sources' });
    await expect(cards).toBeVisible({ timeout: limit });
    id = new URL(page.url()).searchParams.get('id') ?? '';
    expect(id).toMatch(/^[a-f0-9-]{36}$/);
    const saved = new URL(page.url()); const savedPath = saved.pathname + saved.search;

    // Delete only this conversation's cache. POST/GET must drive a new actual
    // claim -> compute -> outer timeout wrapper -> publication, never a test compute.
    fixtureSql(fixture.db, `DELETE FROM read_models WHERE account_id='${fixture.account}' AND model='conversation_detail' AND params->>'id'='${id}'`);
    release = await holdPageWorkers(fixture.db);
    await refresh();
    const first = page.waitForResponse((response) => new URL(response.url()).pathname === `/v1/load/conversations/${id}`);
    const measured = assertPageLoadBudget(page, savedPath, { maxRequests: 6, maxDepth: 1, mode: 'owed' });
    expect((await (await first).json()).computed_at).toBeNull();
    await expect(cards).not.toBeVisible();
    await expect(page.locator('[data-page-loaded="false"]')).toBeAttached();
    await release(); release = undefined;
    const coldLedger = await measured;
    await expect(cards).toBeVisible({ timeout: limit });
    const tile = cards.getByRole('button', { name: /42 min/ }).first();
    await tile.click(); await expect(page.getByRole('dialog', { name: /meeting:/ })).toBeVisible();
    const computed = envelopes.map((event) => event.body as { computed_at?: string | null; payload?: { messages?: { id: string; role: string; source_results?: { quotes: { passage_id?: string; quote: string }[] }[] }[] } })
      .reverse().find((body) => body.computed_at && body.payload?.messages?.some((message) => message.source_results?.length));
    expect(computed?.computed_at).toBeTruthy();
    const supported = computed?.payload?.messages?.find((message) => message.role === 'assistant' && message.source_results?.length);
    const quote = supported?.source_results?.flatMap((source) => source.quotes).find((entry) => entry.passage_id);
    expect(quote?.passage_id).toMatch(/^[a-f0-9-]{36}$/);
    if (!quote?.passage_id || !supported) throw new Error('Verified Find receipt lacks its exact passage');

    // A valid evidence reduction leaves the warm certificate behind: the detail
    // is served stale (never as current), still with its cards, and the read
    // owes the recompute that clears them. Re-check in this mounted page, so
    // clearing the open detail cannot be credited to navigation.
    release = await holdConversationWorker(fixture.db);
    const removed = fixtureSql(fixture.db, `DELETE FROM semantic_atom_passages WHERE passage_id='${quote.passage_id}' RETURNING passage_id`);
    expect(removed).toBe(quote.passage_id);
    fixtureMutations.push({ kind: 'delete-cited-passage', passageId: quote.passage_id, affected: 1 });
    const stale = page.waitForResponse(async (response) => new URL(response.url()).pathname === `/v1/load/conversations/${id}` && (await response.json()).stale === true);
    // Exercise the app's reconnect revalidation with genuine HTTP. Headless
    // tabs can both remain visible, so focus is not a reliable return signal.
    await page.evaluate(() => window.dispatchEvent(new Event('online')));
    const behind = await (await stale).json() as { computed_at: string | null; refresh_eta: string | null };
    expect(behind.computed_at).not.toBeNull();
    expect(behind.refresh_eta, 'a stale behind read owes the recompute').not.toBeNull();
    // While the worker is held, the last answer stays on screen (the accepted
    // window); the recompute, not navigation, is what clears it.
    await expect(cards).toBeVisible();
    await refresh(); await release(); release = undefined;
    await expect(cards).not.toBeVisible({ timeout: limit });
    await expect(page.getByRole('dialog', { name: /meeting:/ })).not.toBeVisible();
    await expect(page.locator('[data-page-loaded="true"]')).toBeAttached();
    const latest = envelopes.at(-1)?.body as { computed_at: string | null; payload: { messages: { id: string; source_results?: unknown[] }[] } };
    expect(latest.computed_at).not.toBeNull();
    expect(latest.payload.messages.find((message) => message.id === supported.id)?.source_results ?? []).toEqual([]);

    // Fresh allowed evidence gets a newly minted answer; removed old cards are
    // never copied into it. A different inaccessible conversation then refuses
    // without retaining that answer's open card; returning re-verifies our id.
    await fixture.api.seedSource(fixture.graph, `The ${token} recovery meeting chose tier B.`, `e2e-recovery-${randomUUID()}`,
      { sourceType: 'transcript', durationMs: 42 * 60_000 });
    await question.fill(`Find the ${token} recovery meeting`); await question.press('Enter');
    await expect(cards).toBeVisible({ timeout: limit });
    await cards.getByRole('button', { name: /42 min/ }).first().click();
    await expect(page.getByRole('dialog', { name: /meeting:/ })).toBeVisible();
    const refusedId = randomUUID();
    const refusal = page.waitForResponse((response) => new URL(response.url()).pathname === `/v1/load/conversations/${refusedId}`);
    await page.goto(`/chats?id=${refusedId}`);
    // A fresh nonexistent UUID has no authorized indexed account anchor and
    // uses the current generic scope refusal (403), not a populated foreign-chat proof.
    expect((await refusal).status()).toBe(403);
    await expect(page.getByRole('main').getByRole('alert')).toBeVisible();
    await expect(cards).not.toBeVisible();
    await expect(page.getByRole('dialog', { name: /meeting:/ })).not.toBeVisible();
    await page.goto(savedPath);
    await expect(cards).toBeVisible({ timeout: limit });
    await info.attach('find-cold-browser-ledger', { body: Buffer.from(JSON.stringify(coldLedger, null, 2)), contentType: 'application/json' });
  } finally {
    if (release) await release();
    page.off('response', observe);
    await info.attach('find-worker-envelopes-and-refresh-actions', { body: Buffer.from(JSON.stringify({ plane, id, limit, actions, fixtureMutations, envelopes, starts }, null, 2)), contentType: 'application/json' });
    await deleteE2eUser(fixture.userId);
  }
});

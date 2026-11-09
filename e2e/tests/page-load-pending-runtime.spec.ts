import { test, expect } from '@playwright/test';
import { spawn } from 'node:child_process';
import { seedPageBudgetFixture, fixtureSql } from '../lib/pageBudgetFixture';
import { signIn, activateOrganization } from '../lib/fixtures';
import { assertPageLoadBudget } from '../lib/pageLoadBudget';
import { deleteE2eUser } from '../lib/clerk';
import { CONTAINERS, CONTROL_DB } from '../stack/config.mjs';

// These tables are read by the real computes, but not indexed read admission.
// They are outside the shared dirty-trigger address lookup, so the real scope
// mutation can commit while computation remains blocked.
async function holdCompute(db: string, table: 'source_items' | 'oauth_tokens') {
  const child = spawn('docker', ['exec', '-i', CONTAINERS.pg, 'psql', '-XAt', '-v', 'ON_ERROR_STOP=1', '-U', 'postgres', '-d', db], { stdio: ['pipe', 'pipe', 'pipe'] });
  await new Promise<void>((resolve, reject) => {
    child.stdout.on('data', (chunk) => { if (String(chunk).includes('pending-worker-barrier')) resolve(); });
    child.once('error', reject);
    child.once('exit', (code) => { if (code !== 0) reject(new Error(`Pending barrier exited ${code}`)); });
    child.stdin.write(`BEGIN; SET LOCAL lock_timeout='3s'; LOCK TABLE ${table} IN ACCESS EXCLUSIVE MODE; SELECT 'pending-worker-barrier';\n`);
  });
  return async () => { const ended = new Promise<void>((resolve) => child.once('exit', () => resolve())); child.stdin.end('ROLLBACK;\n'); await ended; };
}
for (const plane of ['personal', 'org'] as const) for (const route of ['/today', '/connectors'] as const) {
  test(`${plane}: ${route} cold and changed-scope payloads remain pending until the real worker certifies them`, async ({ page, request }, info) => {
    test.setTimeout(180_000);
    const f = await seedPageBudgetFixture(request, plane);
    let release: (() => Promise<void>) | undefined;
    const actions: unknown[] = [];
    const attempts: unknown[] = [];
    const target = route === '/today' ? '/v1/journal/activities' : '/v1/connectors';
    const barrier = () => holdCompute(route === '/today' ? f.db : CONTROL_DB, route === '/today' ? 'source_items' : 'oauth_tokens');
    try {
      await signIn(page, f.email); if (f.orgId) await activateOrganization(page, f.orgId);
      fixtureSql(f.db, `DELETE FROM read_models WHERE account_id='${f.account}'`);
      for (const phase of ['cold', 'changed-scope'] as const) {
        release = await barrier();
        if (phase === 'changed-scope') {
          const started = Date.now(); const response = await f.api.call('POST', '/v1/teams', { name: 'Actual expanded read scope' });
          actions.push({ started, finished: Date.now(), method: 'POST', path: '/v1/teams', status: response.status(), body: await response.json() });
          expect(response.ok()).toBe(true);
        }
        const first = page.waitForResponse((response) => response.request().method() === 'GET' && new URL(response.url()).pathname === target);
        const measured = assertPageLoadBudget(page, route, { maxRequests: 6, maxDepth: 1, mode: 'owed' });
        const body = await (await first).json();
        expect(body.computed_at, `${phase} must not borrow the previous scope's certificate`).toBeNull();
        expect(body.stale).toBe(true); expect(body.refresh_eta).not.toBeNull();
        await expect(page.locator('[data-page-loaded="false"]')).toBeAttached();
        await release(); release = undefined;
        const rows = await measured; attempts.push({ phase, first: body, rows });
        await expect(page.locator('[data-page-loaded="true"]')).toBeAttached();
      }
    } finally {
      if (release) await release();
      await info.attach('cold-and-changed-scope-complete-ledgers', { body: Buffer.from(JSON.stringify({ plane, route, actions, attempts }, null, 2)), contentType: 'application/json' });
      await deleteE2eUser(f.userId);
    }
  });
}

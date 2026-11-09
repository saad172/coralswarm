import { test as base, expect } from '@playwright/test';
import { refreshLimits, refreshRecoveryMs } from '../../web/lib/pageRefreshPolicy';
import { pageRoutes } from '../../web/lib/pageRoutes';
import { signIn, activateOrganization } from '../lib/fixtures';
import { API_BASE, CONTROL_DB } from '../stack/config.mjs';
import { deleteE2eUser } from '../lib/clerk';
import { assertPageLoadBudget, prepareHealthyPageLoadFixture } from '../lib/pageLoadBudget';
import { seedPageBudgetFixture, representativePath, holdPageWorkers, fixtureSql, type PageBudgetFixture } from '../lib/pageBudgetFixture';

const pageEntries = pageRoutes.filter((entry) => entry.budget === 'page');
// The merge queue measures this core set on both planes. Every other sample is
// @nightly: the full matrix runs in page-load-speed.yml (README "Tags").
const CORE = new Set(['/home', '/today', '/connectors', '/connectors/google', '/connectors/jira', '/chats', '/chats?id={conversation}']);
base('every core budget sample is a declared page sample', () => {
  expect([...CORE].filter((template) => !pageEntries.some((entry) => (entry.samples as readonly string[]).includes(template)))).toEqual([]);
});
// Host-only routes are explicit manifest checks, not pretend browser passes or
// silent test skips. The route drift check still covers their real files.
for (const entry of pageRoutes.filter((route) => route.budget !== 'page')) {
  base(`${entry.route}: declared ${entry.budget} host requirement`, () => {
    expect(entry.budget).toBe(entry.route.startsWith('/dev/') ? 'dev' : 'addin');
    expect(entry.source).toMatch(/page\.tsx$/); expect(entry.samples.length).toBeGreaterThan(0);
  });
}

for (const plane of ['personal', 'org'] as const) {
  const test = base.extend<{}, { fixture: PageBudgetFixture }>({
    fixture: [async ({ playwright }, use) => {
      const request = await playwright.request.newContext();
      const fixture = await seedPageBudgetFixture(request, plane);
      try { await use(fixture); }
      finally { await deleteE2eUser(fixture.userId); await request.dispose(); }
    }, { scope: 'worker', timeout: 120_000 }],
  });
  for (const entry of pageEntries) {
    for (const template of entry.samples) {
      test(`${plane}: ${template} initial and healthy page budget`, { tag: CORE.has(template) ? [] : ['@nightly'] }, async ({ page, fixture }, info) => {
        test.setTimeout(180_000);
        await signIn(page, fixture.email);
        if (fixture.orgId) await activateOrganization(page, fixture.orgId);
        const path = representativePath(template, fixture);
        const action = "action" in entry ? entry.action : undefined;
        const expectedRefusals = "refusals" in entry ? entry.refusals : [];
        const initial = await assertPageLoadBudget(page, path, { maxRequests: 6, maxDepth: 1, mode: 'owed', expectedRefusals, action });
        await prepareHealthyPageLoadFixture(fixture, initial);
        const healthy = await assertPageLoadBudget(page, path, { maxRequests: 6, maxDepth: 1, expectedRefusals, action });
        if (entry.route === '/home') {
          const warmed = healthy.filter((row) => row.priority === 'background' && row.path.startsWith('/v1/load/conversations/'));
          expect(warmed).toHaveLength(3);
          const firstFinished = Math.min(...warmed.map((row) => row.finished ?? Infinity));
          expect(warmed.every((row) => row.started < firstFinished), 'all three real detail prefetches start before any finishes').toBe(true);
        }
        await info.attach('actual-page-load-ledgers', { body: Buffer.from(JSON.stringify({ plane, route: entry.route, path, initial, healthy }, null, 2)), contentType: 'application/json' });
      });
    }
  }
  // Home is three plain reads with no stored model (tests/home.spec.ts), so
  // the cold stored-model case is proven on Today, whose journal models are
  // computed by the same workers over `nodes`.
  test(`${plane}: a cold Today remains pending until the actual worker answers, and owed reads are separate`, async ({ page, fixture }, info) => {
    test.setTimeout(180_000);
    await signIn(page, fixture.email);
    if (fixture.orgId) await activateOrganization(page, fixture.orgId);
    // Remove only this fixture account's cache; do not prewarm the cold case.
    fixtureSql(fixture.db, `DELETE FROM read_models WHERE account_id='${fixture.account}'`);
    const release = await holdPageWorkers(fixture.db);
    let count = 0;
    const twice = page.waitForResponse((response) => new URL(response.url()).pathname === '/v1/journal/activities' && response.request().method() === 'GET' && ++count === 2);
    const measured = assertPageLoadBudget(page, '/today', { maxRequests: 6, maxDepth: 1, mode: 'owed' });
    try {
      const response = await twice;
      expect((await response.json()).computed_at).toBeNull();
      await expect(page.locator('[data-page-loaded="false"]')).toBeAttached();
    } finally { await release(); }
    const records = await measured;
    const critical = records.filter((record) => record.priority === 'critical');
    expect(critical.filter((record) => record.ledger === 'owed-refresh').length).toBeGreaterThan(0);
    expect(critical.length).toBeGreaterThan(6);
    await info.attach('complete-cold-ledgers', { body: Buffer.from(JSON.stringify(records, null, 2)), contentType: 'application/json' });
  });
  if (plane === 'personal') test('personal: eight foreign shared accounts retain independent certificates in one browser request', async ({ page, fixture }, info) => {
    test.setTimeout(180_000);
    const foreign: { account: string; graph: string }[] = [];
    for (let n = 0; n < 8; n++) {
      const owner = fixtureSql(fixture.db, `INSERT INTO users(email) VALUES('l5-foreign-${fixture.account}-${n}@test.local') RETURNING id`);
      const account = fixtureSql(fixture.db, `INSERT INTO accounts(owner_user_id) VALUES('${owner}') RETURNING id`);
      fixtureSql(fixture.db, `INSERT INTO account_members(account_id,user_id,role) VALUES('${account}','${owner}','owner'),('${account}','${fixture.actor}','member')`);
      const graph = fixtureSql(fixture.db, `INSERT INTO graphs(account_id,name,kind) VALUES('${account}','L5 shared account ${n}','org') RETURNING id`);
      foreign.push({ account, graph });
    }
    await signIn(page, fixture.email);
    const initial = await assertPageLoadBudget(page, '/workspaces', { maxRequests: 6, maxDepth: 1, mode: 'owed' });
    expect(initial.filter((row) => row.priority === 'critical' && row.path === '/v1/load/oceans' && row.ledger !== 'owed-refresh')).toHaveLength(1);
    const records = await assertPageLoadBudget(page, '/workspaces');
    const fanIn = records.filter((row) => row.priority === 'critical' && row.path === '/v1/load/oceans');
    expect(fanIn).toHaveLength(1);
    expect(fanIn[0].certificates).toHaveLength(9);
    expect(records.filter((row) => row.path === '/v1/load/oceans/component')).toHaveLength(0);
    for (const { account } of foreign) expect(fanIn[0].certificates?.find((certificate) => certificate.identity === account)?.computedAt).not.toBeNull();
    for (let n = 0; n < 8; n++) await expect(page.getByText(`L5 shared account ${n}`, { exact: true })).toBeVisible();
    await info.attach('foreign-constituent-certificates', { body: Buffer.from(JSON.stringify({ initial, healthy: records, fanIn }, null, 2)), contentType: 'application/json' });
  });

  test(`${plane}: a real successful Slack OAuth callback records its bounded action phase`, async ({ page, request, fixture }, info) => {
    test.setTimeout(180_000);
    if (fixture.orgId) fixtureSql(CONTROL_DB, `INSERT INTO org_connector_flags(clerk_org_id,provider,enabled) VALUES('${fixture.orgId}','slack',true) ON CONFLICT(clerk_org_id,provider) DO UPDATE SET enabled=true,updated_at=now()`);
    const start = await fixture.api.json('POST', '/v1/connectors/slack/connect', { return_to: 'wizard' });
    const hop = await request.get(start.authorize_url, { maxRedirects: 0 });
    expect(hop.status()).toBe(307);
    const state = new URL(hop.headers().location).searchParams.get('state');
    expect(state).toBeTruthy();
    const back = await request.get(`${API_BASE}/v1/oauth/slack/callback?code=team-l5${fixture.account.replaceAll('-', '')}&state=${encodeURIComponent(state!)}`, { maxRedirects: 0 });
    expect(back.status()).toBe(307);
    expect(back.headers().location).toContain('provider=slack&result=ok');
    await signIn(page, fixture.email);
    if (fixture.orgId) await activateOrganization(page, fixture.orgId);
    const declaration = pageRoutes.find((entry) => entry.route === '/connectors/return');
    if (!declaration || !('action' in declaration)) throw new Error('Missing callback declaration');
    const rows = await assertPageLoadBudget(page, '/connectors/return?provider=slack&result=ok', { maxRequests: 6, maxDepth: 1, mode: 'owed', action: declaration.action });
    expect(rows.some((row) => row.method === 'POST' && row.path === '/v1/connectors/slack/refresh' && row.status === 202)).toBe(true);
    expect(rows.filter((row) => row.phase === 'callback-action' && row.priority === 'critical').length).toBeGreaterThan(0);
    await expect(page).toHaveURL(/\/connectors\?setup=slack/);
    await info.attach('actual-callback-initial-action-ledgers', { body: Buffer.from(JSON.stringify(rows, null, 2)), contentType: 'application/json' });
  });

}

import { createServer } from 'node:http';
import { mkdirSync, writeFileSync } from 'node:fs';
import { setTimeout as delay } from 'node:timers/promises';
import { test, expect } from '@playwright/test';
import { clerkApi, createUser, deleteE2eUser, primaryEmail } from '../lib/clerk';
import { signIn } from '../lib/fixtures';
import { LOCAL_API_BASE } from '../stack/config.mjs';

// Synthetic DTOs and delivery delays, never a production backend baseline.
// Auth, routing, transport, query factories and rendering are the actual app.
const graph = { id: '11111111-1111-4111-8111-111111111111', name: 'Navigation fixture', kind: 'personal', is_primary: true,
  archived_at: null, created_at: '2026-10-30T00:00:00Z', my_team_role: null, owner_user_id: null, team_id: null };
const account = '22222222-2222-4222-8222-222222222222';
const stored = (model: string, payload: unknown) => ({ model, version: 1, payload, computed_at: new Date().toISOString(), stale: false, refresh_eta: null });
const server = createServer(async (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', 'http://localhost:13702');
  res.setHeader('Access-Control-Allow-Headers', req.headers['access-control-request-headers'] ?? 'authorization, content-type');
  res.setHeader('Access-Control-Allow-Methods', 'GET, PUT, POST, OPTIONS');
  res.setHeader('Cache-Control', 'no-store');
  res.setHeader('Content-Type', 'application/json');
  if (req.method === 'OPTIONS') { res.writeHead(204); res.end(); return; }
  const path = new URL(req.url!, LOCAL_API_BASE).pathname;
  const now = new Date().toISOString();
  let user = '';
  try { user = JSON.parse(Buffer.from(req.headers.authorization!.split('.')[1], 'base64url').toString()).sub; } catch { /* DTO replay server only. */ }
  const activity = { activities: [], day_activity: [], truncated: false, window: { from: now, to: now, tz: 'UTC', who: 'mine' } };
  const replies: Record<string, unknown> = {
    '/v1/bootstrap': { activities_enabled: true, gist_enabled: true, billing_banner: null, graphs: [graph], join_requests_count: null,
      my_join_requests: [], pending_orgs: [], selfserve_org: false, selfserve_signup_enabled: true,
      me: { account_id: account, active_graph_id: graph.id, device_id: 'fixture', is_personal_org: false, org_teams_enabled: null, role: 'owner', user_id: user } },
    '/v1/home/continue': { items: [], generated_at: now },
    '/v1/home/whats-new': { items: [], window_days: 7, generated_at: now },
    '/v1/home/attention': { has_content: false, has_source: false, has_mcp: false, has_asked: false, attention: null, generated_at: now },
    '/v1/conversations': stored('conversation_list', { conversations: [], shared: [] }),
    '/v1/connectors': stored('connectors_list', { connectors: [{ key: 'slack', name: 'Slack', beta: false, status: 'not_connected', sources_on: 0,
      sources_total: 1, last_captured_at: null, importing: null, problem: null, resume_step: null, sources: [] }] }),
    '/v1/load/oceans': { components: [{ account_id: account, anchor_graph_id: null, refresh_path: '/v1/load/oceans/component/refresh',
      stored: stored('oceans_index', { oceans: [{ graph, counts: { reef_total: 0, atom_count: 0, source_count: 0, reef_counts: {} } }], members: [], team_members: {}, archived: null }) }] },
    '/v1/journal/activities': stored('journal_activities', activity),
    '/v1/journal/strip': stored('journal_strip', activity),
    '/v1/journal/gist': stored('journal_gist', { period_start: now, period_end: now, open: true, who: 'mine', granularity: 'day', tz: 'UTC', gist: null, compiled_at: null, span: null }),
    '/v1/me/timezone': {}, '/v1/me/active-graph': {}, '/v1/telemetry/client': {},
  };
  if (!(path in replies)) { res.writeHead(500); res.end(JSON.stringify({ error: `Unexpected replay path ${path}` })); return; }
  await delay(path === '/v1/bootstrap' ? 280 : 150);
  res.end(JSON.stringify(replies[path]));
});
test.beforeAll(async () => {
  expect(new URL(LOCAL_API_BASE).port, 'exclusive replay API port must match the build').toBe('13703');
  await new Promise<void>((resolve, reject) => { server.once('error', reject); server.listen(13703, '127.0.0.1', resolve); });
});
test.afterAll(async () => { server.closeAllConnections(); await new Promise<void>(resolve => server.close(() => resolve())); });

for (const cache of ['kept', 'cold'] as const) for (const path of ['/today', '/workspaces', '/connectors', '/home']) {
  test(`${cache} static cache: ${path}`, async ({ page, context }, info) => {
    const user = await createUser('freshnav');
    try {
      await signIn(page, primaryEmail(user));
      await page.unrouteAll({ behavior: 'wait' });
      await context.unrouteAll({ behavior: 'wait' });
      const cdp = await context.newCDPSession(page);
      await cdp.send('Network.enable');
      // Clerk testing installs interception, which normally disables caching.
      // Restore it explicitly for kept-cache runs; transfer sizes prove it.
      await cdp.send('Network.setCacheDisabled', { cacheDisabled: false });
      await page.goto(path);
      await expect(page.locator('[data-page-loaded="true"]')).toBeVisible();
      await page.addInitScript(() => {
        const w = window as typeof window & { __navigationSample?: unknown };
        const installed = performance.now();
        const observer = new MutationObserver(() => {
          const marker = document.querySelector('[data-page-loaded="true"][data-page-failed="false"]');
          if (!marker) return;
          w.__navigationSample = { installed, ready: performance.now(), timeOrigin: performance.timeOrigin };
          observer.disconnect();
        });
        observer.observe(document, { subtree: true, childList: true, attributes: true, attributeFilter: ['data-page-loaded', 'data-page-failed'] });
      });
      const rows = [];
      for (let n = 0; n < 3; n++) {
        await page.goto('about:blank');
        if (cache === 'cold') await cdp.send('Network.clearBrowserCache');
        await page.goto(path, { waitUntil: 'domcontentloaded' });
        await expect(page.locator('[data-page-loaded="true"][data-page-failed="false"]')).toBeVisible();
        const row = await page.evaluate(() => {
          const w = window as typeof window & { __navigationSample?: Record<string, number> };
          const nav = performance.getEntriesByType('navigation')[0] as PerformanceNavigationTiming;
          return { ready: w.__navigationSample?.ready ?? NaN, installed: w.__navigationSample?.installed ?? NaN,
            timeOrigin: w.__navigationSample?.timeOrigin ?? NaN, navigation: { start: nav.startTime, ttfb: nav.responseStart, dcl: nav.domContentLoadedEventEnd },
            resources: (performance.getEntriesByType('resource') as PerformanceResourceTiming[]).filter(r => !r.name.includes('/sessions/')).map(r => ({
              host: new URL(r.name).hostname, path: new URL(r.name).pathname, start: r.startTime, end: r.responseEnd, size: r.transferSize, encoded: r.encodedBodySize })) };
        });
        expect(row.ready).toBeGreaterThan(row.installed!);
        const staticReads = row.resources.filter(r => r.path.startsWith('/_next/static/'));
        expect(staticReads.length).toBeGreaterThan(5);
        expect(staticReads.filter(r => r.size === 0 && r.encoded > 0).length).toBe(cache === 'kept' ? staticReads.length : 0);
        rows.push(row);
      }
      mkdirSync(info.outputDir, { recursive: true });
      const artifact = info.outputPath('navigation-samples.json');
      writeFileSync(artifact, JSON.stringify({ cache, path, backend: { bootstrapMs: 280, readMs: 150 }, rows }, null, 2));
      await info.attach('navigation-samples.json', { path: artifact, contentType: 'application/json' });
      console.log(JSON.stringify({ cache, path, ready: rows.map(r => Math.round(r.ready!)) }));
    } finally { await deleteE2eUser(user.id, clerkApi); }
  });
}

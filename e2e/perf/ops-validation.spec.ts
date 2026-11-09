// Independently runnable API/fixture acceptance while L5 browser work is pending.
// Does not claim the all-route browser or initial/owed recorder passed.
import { test, expect } from '@playwright/test';
import { randomUUID } from 'node:crypto';
import { execFileSync, spawnSync } from 'node:child_process';
import { mkdirSync, writeFileSync } from 'node:fs';
import path from 'node:path';
import { createReadyOrg, addMember } from '../lib/isolation';
import { fixtureSql } from '../lib/pageBudgetFixture';
import { waitForComputed } from '../lib/readModels';
import { API_BASE, CONTROL_DB, CONTAINERS, PORTS, FAKE } from '../stack/config.mjs';
import { loadCanonicalSeed } from './seed.mjs';
import { parsePageReadTiming, assertSpeed } from './timings.mjs';

test('@nightly L6 independent fixture and internal endpoint acceptance', async ({ request }) => {
  test.setTimeout(600_000);
  const dir = process.env.PAGE_LOAD_EVIDENCE_DIR!;
  mkdirSync(dir, { recursive: true });
  const save = (name: string, value: unknown) => writeFileSync(path.join(dir, name), JSON.stringify(value, null, 2) + '\n');
  const url = `http://127.0.0.1:${PORTS.internal}/internal/admin/page-read-health`;
  for (const headers of [{}, { 'X-Internal-Token': 'incorrect' }] as Record<string, string>[]) expect((await request.get(url, { headers })).status()).toBe(401);
  expect((await request.get(`${API_BASE}/internal/admin/page-read-health`)).status()).toBe(404);
  const org = await createReadyOrg(request, 'l6-ops-scale');
  const member = await addMember(request, org, 'l6-scale');
  const me = await org.adminInOrg.json('GET', '/v1/me');
  const other = await member.inOrg.json('GET', '/v1/me');
  const graph = (await org.adminInOrg.graphs()).find((g) => g.kind === 'member')!.id;
  const db = fixtureSql(CONTROL_DB, `SELECT 'coralswarm_tenant_'||replace(slug,'-','_') FROM tenant_provisioning_jobs WHERE clerk_org_id='${org.orgId}'`);
  const fixture = loadCanonicalSeed(db, { acct: me.account_id, u1: me.user_id, u2: other.user_id, g_org: org.oceanId, g_team: randomUUID(), g_mem: graph });
  save('fixture.json', fixture);
  const role = JSON.parse(execFileSync('docker', ['exec', '-i', CONTAINERS.pg, 'psql', '-XAt', '-v', 'ON_ERROR_STOP=1', '-U', 'coralswarm_tenant_app', '-d', db, '-c',
    "SELECT json_build_object('role',current_user,'superuser',rolsuper,'bypassrls',rolbypassrls,'row_security',current_setting('row_security'),'owned_relations',(SELECT count(*) FROM pg_class WHERE relowner=(SELECT oid FROM pg_roles WHERE rolname=current_user))) FROM pg_roles WHERE rolname=current_user"], { encoding: 'utf8' }));
  expect(role.superuser).toBe(false); expect(role.bypassrls).toBe(false); expect(role.row_security).toBe('on'); expect(role.owned_relations).toBe(0);
  save('serving-role.json', role);
  const reads = [];
  for (const route of ['/v1/home/metrics?days=7&tz=UTC', `/v1/graphs/${graph}/atlas`]) {
    const started = Date.now();
    const computed = await waitForComputed(org.adminInOrg, route);
    const samples = [];
    for (let i = 0; i < 20; i++) {
      const response = await org.adminInOrg.call('GET', route);
      expect(response.status()).toBe(200);
      expect(response.headers()['x-coralswarm-admission']).toBeUndefined();
      const body = await response.json();
      expect(body.computed_at).not.toBeNull();
      samples.push(parsePageReadTiming(response.headers()['server-timing'], true));
    }
    reads.push({ path: route, model: computed.model, refresh_and_twenty_reads_elapsed_ms: Date.now() - started, ...assertSpeed(samples) });
    save('actual-http.json', { boundary: 'debug e2e real HTTP/worker, no browser acceptance claim', reads });
  }
  const health = await request.get(url, { headers: { 'X-Internal-Token': FAKE.internalToken } });
  expect(health.status()).toBe(200);
  save('owned-health-response.json', await health.json());
  const verdict = spawnSync('python3', ['../scripts/check_page_read_health.py', path.join(dir, 'owned-health-response.json')], { encoding: 'utf8' });
  // Debug API timings have the nightly 500ms gate. Exercise the real hourly
  // parser without relabelling a 250ms debug breach as a production result.
  expect([0, 1], verdict.stderr).toContain(verdict.status);
  save('owned-health-verdict.json', { exit_code: verdict.status, verdict: JSON.parse(verdict.stdout), boundary: 'owned debug process, not deployed production' });
});

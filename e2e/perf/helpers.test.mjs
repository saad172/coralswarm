import { test } from 'node:test';
import assert from 'node:assert/strict';
import { parsePageReadTiming, summarize, assertSpeed } from './timings.mjs';
import { seedVariables, SEED_URL } from './seed.mjs';
import { readFileSync } from 'node:fs';

test('actual server grammar, quoting and zero duration', () => {
  assert.equal(parsePageReadTiming('total;dur=14.25, class;desc="page_read", lane;desc="page"'), 14.25);
  assert.equal(parsePageReadTiming('note;desc="commas, and; semicolons", class;desc="page_read", total;dur=0'), 0);
  assert.equal(parsePageReadTiming('total;dur=9,class;desc="other"', false), null);
});
test('missing class, total and malformed headers fail closed', () => {
  for (const value of [null, '', 'total;dur=0', 'class;desc="other",total;dur=1', 'class;desc="page_read"',
    'class;desc="page_read",total;dur=NaN', 'class;desc="page_read",total;dur=-1',
    'class;desc="page_read",total;dur="Infinity"', 'class;desc="page_read",total;dur=1;dur=2',
    'class;desc="page_read",total;dur=1,total;dur=2', 'class;desc="page_read,total;dur=1']) {
    assert.throws(() => parsePageReadTiming(value), /Server-Timing|page_read/);
  }
});
test('nearest-rank p95 and strict <500 boundary with all twenty samples', () => {
  assert.equal(summarize([...Array(19).fill(499), 501]).p95_ms, 499);
  assert.doesNotThrow(() => assertSpeed([...Array(19).fill(499), 501]));
  assert.throws(() => assertSpeed([...Array(18).fill(1), 500, 501]), /must be <500/);
  assert.throws(() => summarize(Array(19).fill(1)), /insufficient/);
  assert.throws(() => summarize([...Array(19).fill(1), NaN]), /invalid/);
});
test('canonical fixture has one source and caller identities cannot be overridden', () => {
  const sql = readFileSync(SEED_URL, 'utf8');
  assert.match(sql, /setseed\(0\.42\)/);
  assert.match(sql, /generate_series\(1, 600000\)/);
  assert.match(sql, /generate_series\(1, 60000\)/);
  assert.doesNotMatch(sql, /^\\set\s+(acct|u1|u2|g_org|g_team|g_mem)\b/m);
  const ids = Object.fromEntries(['acct','u1','u2','g_org','g_team','g_mem'].map((key, i) => [key, `11111111-1111-1111-1111-${String(i).padStart(12,'0')}`]));
  const args = seedVariables(ids);
  assert.equal(args.length, 12);
  assert.equal(args[1], `acct='${ids.acct}'`);
  assert.throws(() => seedVariables({ ...ids, acct: "'; DROP TABLE atoms" }), /invalid seed UUID/);
  assert.throws(() => seedVariables({ ...ids, u2: ids.u1 }), /distinct/);
  assert.throws(() => seedVariables({ ...ids, extra: ids.acct }), /exactly six/);
});

test('registered model errors cannot turn into empty timing-only passes', async () => {
  const { isRegisteredModelPath, storedStates } = await import('./envelopes.mjs');
  const { default: registry } = await import('../../web/lib/pageSections.generated.json', { with: { type: 'json' } });
  assert.equal(isRegisteredModelPath('/v1/home/metrics', registry), true);
  assert.equal(isRegisteredModelPath('/v1/graphs/not-an-id/stats', registry), true);
  assert.equal(isRegisteredModelPath('/v1/home/metrics/extra', registry), false);
  assert.deepEqual(storedStates({ error: 'unauthorized' }, registry, '/v1/home/metrics'), []);
  const empty = { model: 'home_metrics', version: registry.home_metrics.version, computed_at: null, stale: true, refresh_eta: null, payload: {} };
  assert.equal(storedStates(empty, registry, '/v1/home/metrics')[0].computed_at, null);
  for (const value of [{ ...empty, model: 'invented' }, { ...empty, version: -1 }, { ...empty, stale: null },
    { ...empty, computed_at: 'not-a-date' }, { ...empty, refresh_eta: false }]) assert.throws(() => storedStates(value, registry, '/v1/home/metrics'), /invalid or unregistered/);
});
test('fan-in model evidence keeps each real component identity and certificate', async () => {
  const { storedStates } = await import('./envelopes.mjs');
  const registry = { oceans_index: { version: 1, path: '/load/oceans/component' } };
  const stored = { model: 'oceans_index', version: registry.oceans_index.version, computed_at: '2026-10-31T00:00:00Z', stale: false, refresh_eta: null };
  const states = storedStates({ components: [{ account_id: 'first', stored }, { account_id: 'second', stored: { ...stored, computed_at: null, stale: true } }] }, registry, '/v1/load/oceans');
  assert.deepEqual(states.map((s) => [s.identity, s.computed_at]), [['first', stored.computed_at], ['second', null]]);
  assert.throws(() => storedStates({ components: [{ account_id: 'first', stored: {} }] }, registry, '/v1/load/oceans'), /invalid/);
});

test('certificates must belong to the requested model, including literal routes and fan-in', async () => {
  const { storedStates } = await import('./envelopes.mjs');
  const { default: registry } = await import('../../web/lib/pageSections.generated.json', { with: { type: 'json' } });
  const home = { model: 'home_metrics', version: registry.home_metrics.version, computed_at: null, stale: true, refresh_eta: null };
  const status = { ...home, model: 'connector_status', version: registry.connector_status.version };
  assert.throws(() => storedStates(status, registry, '/v1/home/metrics?range=week'), /requested model/);
  assert.throws(() => storedStates({ ...status, model: 'connector_detail' }, registry, '/v1/connectors/status'), /requested model/);
  assert.equal(storedStates(status, registry, '/v1/connectors/status')[0].model, 'connector_status');
  const fixtures = { ...registry, oceans_index: { version: 1, path: '/load/oceans/component' } };
  const ocean = { ...home, model: 'oceans_index', version: 1 };
  const components = (stored) => ({ components: [{ account_id: 'account', stored }] });
  assert.equal(storedStates(components(ocean), fixtures, '/v1/load/oceans')[0].model, 'oceans_index');
  assert.throws(() => storedStates(components(home), fixtures, '/v1/load/oceans'), /requested model/);
  assert.throws(() => storedStates(components({ ...ocean, version: 999 }), fixtures, '/v1/load/oceans'), /invalid or unregistered/);
  assert.throws(() => storedStates(home, fixtures, '/v1/load/oceans'), /component/);
});

test('only the exact application-certified normal callback picker occurrence is Other', async () => {
  const { verifiedActionRead } = await import('./read-policy.mjs');
  const row = { method: 'GET', path: '/v1/connectors/slack/sources/source/scope-items?q=x', priority: 'normal', scope: '["issuer","actor","org"]' };
  const evidence = { ...row, address: row.path, identity: `${row.scope}|${row.path}`, phase: 'callback-action', actionEvidence: { picker: true } };
  assert.equal(verifiedActionRead(row, 0, [evidence]), true);
  assert.equal(verifiedActionRead(row, 1, [evidence]), false);
  for (const changed of [{ phase: 'initial' }, { actionEvidence: {} }, { priority: 'critical' }, { identity: '["issuer","another","org"]|' + row.path }, { address: row.path + '&cursor=unproven' }]) {
    assert.equal(verifiedActionRead(row, 0, [{ ...evidence, ...changed }]), false);
  }
  assert.equal(verifiedActionRead({ ...row, priority: 'critical' }, 0, [evidence]), false);
  assert.equal(verifiedActionRead(row, 0, []), false);
});

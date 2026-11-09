import { expect, test } from '@playwright/test';
import * as budget from '../lib/pageLoadBudget';
import { StoredReadLedger } from '../../web/lib/pageLoadEvidence';
import manifest from '../../web/lib/pageSections.generated.json' with { type: 'json' };
import { refreshLimits, refreshRecoveryMs } from '../../web/lib/pageRefreshPolicy';

// Isolated harness contract tests: these responses are synthetic, never real
// backend/Clerk acceptance or a replacement for the measured browser ledger.
const scope = { account: '11111111-1111-4111-8111-111111111111', actor: 'actor', userId: 'user_fixture', orgId: null };
const computed = Date.parse('2026-10-31T17:38:14.780Z');
const stored = (at = computed, owed = false) => ({ model: 'conversation_list', version: manifest.conversation_list.version,
  payload: { conversations: [], shared: [] }, computed_at: new Date(at).toISOString(), stale: owed,
  refresh_eta: owed ? new Date(at + 65_000).toISOString() : null });
function record(address = '/v1/conversations', body: unknown = stored(), started = computed + 59_559): budget.LoadRecord {
  const row: budget.LoadRecord = { address, path: new URL(address, 'https://fixture.invalid').pathname,
    identity: `${JSON.stringify(['https://fixture.invalid', scope.userId, scope.orgId])}|${address}`,
    method: 'GET', priority: 'critical', phase: 'initial', ledger: 'initial', page: '/home', initialWave: '/home', started, finished: started + 1,
    status: 200, failure: null, excluded: null, model: null, computedAt: null, refreshEta: null };
  const ledger = new StoredReadLedger(); ledger.start(row); ledger.response(row, body); return row;
}
function api(respond: (method: string, path: string) => unknown) {
  const calls: { method: string; path: string; at: number }[] = [];
  return { calls, call: async (method: string, path: string) => {
    calls.push({ method, path, at: Date.now() });
    const body = path === '/v1/me' ? { account_id: scope.account, user_id: scope.actor } : respond(method, path);
    return { status: () => method === 'POST' ? 202 : 200, json: async () => body };
  } };
}
const acknowledgement = { refresh_eta: '2026-10-31T17:39:20Z' };

test('60s boundary: explicitly refresh a fresh-but-aging discovered answer before healthy measurement', async () => {
  const source = record();
  expect(source.started - computed).toBe(59_559);
  const before = structuredClone(source);
  const advanced = computed + 65_681;
  const client = api((method) => method === 'POST' ? acknowledgement :
    client.calls.filter((call) => call.path === source.address && call.method === 'GET').length === 1
      ? stored(computed, true) : stored(advanced));
  const receipt = await budget.prepareHealthyPageLoadFixture({ ...scope, api: client }, [source]);
  expect(source).toEqual(before); // Setup never rewrites the measured ledger.
  expect(client.calls.map(({ method, path }) => `${method} ${path}`)).toEqual([
    'GET /v1/me', 'GET /v1/conversations', 'POST /v1/conversations/refresh', 'GET /v1/conversations',
  ]);
  expect(receipt.scope).toEqual(scope);
  expect(receipt.attempts.at(-1)?.certificates?.[0]).toMatchObject({ computedAt: new Date(advanced).toISOString(), stale: false, refreshEta: null });
  // The same age crossing without setup remains genuine owed evidence.
  const ledger = new StoredReadLedger();
  const aged = record('/v1/conversations', stored(computed, true), computed + 61_026);
  ledger.start(aged); ledger.response(aged, stored(computed, true));
  const poll = { ...record(), started: aged.finished! + budget.REFRESH_POLL_MS, finished: null };
  ledger.start(poll);
  expect(poll.ledger).toBe('owed-refresh');
  expect(poll.owedFrom).toHaveLength(1);
});

test('a one-second quiet gap cannot complete setup before its real two-second owed poll', async () => {
  const source = record();
  let reads = 0;
  const client = api((method) => method === 'POST' ? acknowledgement : ++reads < 3 ? stored(computed, true) : stored(computed + 66_000));
  const receipt = await budget.prepareHealthyPageLoadFixture({ ...scope, api: client }, [source]);
  const polls = receipt.attempts.filter((attempt) => attempt.method === 'GET' && attempt.address === source.address);
  expect(polls).toHaveLength(3); // baseline, still owed, actual worker completion
  expect(polls[2].started - polls[1].finished!).toBeGreaterThanOrEqual(budget.REFRESH_POLL_MS - 50);
  expect(polls[1].certificates?.[0].refreshEta).not.toBeNull();
  expect(polls[2].certificates?.[0].refreshEta).toBeNull();
});

test('preserves canonical query parameters and deduplicates discovered starts', async () => {
  const path = '/v1/home/metrics?days=30&graph=11111111-1111-4111-8111-111111111111';
  const body = { ...stored(), model: 'home_metrics', payload: {} };
  const source = record(path, body);
  let reads = 0;
  const client = api((method) => method === 'POST' ? acknowledgement : { ...body, computed_at: new Date(computed + ++reads * 1_000).toISOString() });
  await budget.prepareHealthyPageLoadFixture({ ...scope, api: client }, [source, structuredClone(source)]);
  expect(client.calls.filter((call) => call.method === 'POST').map((call) => call.path)).toEqual([
    '/v1/home/metrics/refresh?days=30&graph=11111111-1111-4111-8111-111111111111',
  ]);
});

test('retains independently certified fan-in components and uses their real refresh paths', async () => {
  const graph = '22222222-2222-4222-8222-222222222222';
  const other = '33333333-3333-4333-8333-333333333333';
  const fanIn = (at: number) => ({ components: [scope.account, other].map((account_id, index) => ({ account_id,
    anchor_graph_id: index ? graph : null,
    refresh_path: `/v1/load/oceans/component/refresh${index ? `?graph=${graph}` : ''}`,
    stored: { ...stored(at), model: 'oceans_index', payload: { oceans: [], members: [], team_members: {}, archived: null } },
  })) });
  const source = record('/v1/load/oceans', fanIn(computed));
  let reads = 0;
  const client = api((method) => method === 'POST' ? acknowledgement : fanIn(computed + ++reads * 1_000));
  const receipt = await budget.prepareHealthyPageLoadFixture({ ...scope, api: client }, [source]);
  expect(client.calls.filter((call) => call.method === 'POST').map((call) => call.path)).toEqual(source.certificates!.map((c) => c.refreshPath));
  expect(receipt.attempts.at(-1)?.certificates?.map((c) => c.identity)).toEqual([scope.account, other]);
});

test('does no fixture work without a discovered certified Stored address', async () => {
  const client = api(() => { throw new Error('must not call API'); });
  const refused = { ...record(), status: 403 };
  await budget.prepareHealthyPageLoadFixture({ ...scope, api: client }, [record('/v1/bootstrap', {}), record(), refused]);
  expect(client.calls).toHaveLength(0);
});

test('rejects another browser credential scope before refreshing anything', async () => {
  const client = api(() => acknowledgement);
  const foreign = { ...record(), identity: 'other-user|/v1/conversations' };
  await expect(budget.prepareHealthyPageLoadFixture({ ...scope, api: client }, [foreign])).rejects.toThrow(/scope/);
  expect(client.calls).toHaveLength(0);
});

test('invalid live certificates and refresh acknowledgements fail explicitly', async () => {
  for (const invalid of ['certificate', 'acknowledgement']) {
    const client = api((method) => method === 'POST' ? {} : invalid === 'certificate' ? { ...stored(), version: 999 } : stored());
    await expect(budget.prepareHealthyPageLoadFixture({ ...scope, api: client }, [record()])).rejects.toThrow(/certificate|acknowledgement/);
    if (invalid === 'certificate') expect(client.calls.some((call) => call.method === 'POST')).toBe(false);
  }
});

test('an unchanged computed timestamp cannot satisfy the explicit refresh precondition', async () => {
  let reads = 0;
  const client = api((method) => method === 'POST' ? acknowledgement : stored(++reads < 3 ? computed : computed + 70_000));
  const receipt = await budget.prepareHealthyPageLoadFixture({ ...scope, api: client }, [record()]);
  expect(receipt.attempts.filter((attempt) => attempt.address === '/v1/conversations')).toHaveLength(3);
  expect(receipt.attempts.at(-1)?.certificates?.[0].computedAt).toBe(new Date(computed + 70_000).toISOString());
});

test('setup expires at its fixed registry recovery bound without another read', async () => {
  // Explicit fake elapsed/wall clocks; real setup uses the shared monotonic domain.
  let now = Date.now();
  const clock = { now: () => now, wallNow: () => now };
  const client = api((method) => {
    if (method === 'POST') {
      now += refreshRecoveryMs(refreshLimits('conversation_list')) + 1;
      return acknowledgement;
    }
    return stored(computed, true);
  });
  await expect(budget.prepareHealthyPageLoadFixture({ ...scope, api: client }, [record()], clock)).rejects.toThrow(/deadline expired/);
  expect(client.calls.map(({ method, path }) => `${method} ${path}`)).toEqual([
    'GET /v1/me', 'GET /v1/conversations', 'POST /v1/conversations/refresh',
  ]);
});

test('a mismatched API account fails before any worker refresh', async () => {
  const client = api(() => { throw new Error('must not refresh'); });
  const wrongScope = { ...scope, account: 'wrong-account', api: client };
  await expect(budget.prepareHealthyPageLoadFixture(wrongScope, [record()])).rejects.toThrow(/account\/actor scope/);
  expect(client.calls.map((call) => call.path)).toEqual(['/v1/me']);
});

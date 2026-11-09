import { test, expect, type Page } from '@playwright/test';
import { createBudgetRecorder } from '../lib/pageLoadBudgetHarness';
import * as budget from '../lib/pageLoadBudget';
import { criticalReadPredecessors } from '../perf/semantic-timing.mjs';
import { createPageLoadClock } from '../lib/pageLoadClock.mjs';
import { StoredReadLedger, ConnectorCallbackPhase, REFRESH_POLL_MS } from '../../web/lib/pageLoadEvidence';
import { refreshLimits, refreshRecoveryMs } from '../../web/lib/pageRefreshPolicy';
import { DEFAULT_TRANSPORT_CONFIG } from '../../web/lib/transport';

const address = '/v1/load/chats/empty?limit=6&since_days=7';
const body = { model: 'chat_empty', version: 1, computed_at: null, stale: true, refresh_eta: '2026-10-31T23:19:54.891265Z',
  payload: { workspace_activity: null, activity: [], connections: null, connections_status: 'pending', teammate_count: null, own_conversation_count: 0 } };

test('recorder harness rejects an unbound real import before an empty fixture can hide it', () => {
  expect(() => createBudgetRecorder({
    expect, test, prepareSemanticTimingCapture: () => undefined, applyConfirmedSemanticTiming: () => undefined,
    pageLoadClock: {}, StoredReadLedger, ConnectorCallbackPhase, CONNECTOR_PICKER_PATH: '',
    REFRESH_POLL_MS, refreshRecoveryMs, refreshLimits, DEFAULT_TRANSPORT_CONFIG,
  })).toThrow('Missing recorder import binding: criticalReadPredecessors');
});

for (const declaration of [
  'import { futureDependency } from "future-module";',
  'import { anotherName as futureDependency } from "future-module";',
  'import futureDependency from "future-module";',
  'import * as futureDependency from "future-module";',
]) test(`recorder harness detects future runtime imports: ${declaration}`, () => {
  const source = `${declaration}\nexport async function assertPageLoadBudget() { if (false) futureDependency(); }`;
  expect(() => createBudgetRecorder({}, source)).toThrow('Missing recorder import binding: futureDependency');
});

function time() {
  let wall = 1793488789881, elapsed = 0;
  const clock = createPageLoadClock({ elapsedNow: () => elapsed, wallNow: () => wall });
  return { clock, advance(ms: number) { wall += ms; elapsed += ms; }, jump(ms: number) { wall += ms; } };
}

// Exercise the actual recorder body, substituting only browser/DOM I/O and
// deterministic time. All numeric budget/certificate assertions remain real.
function recorder(t: ReturnType<typeof time>, quietProbe?: (predicate: () => Promise<boolean>) => Promise<void>) {
  const locator = { count: async () => 1 };
  const checks = ((value: unknown, message?: string) => value === locator ? { toBeAttached: async () => {} } : expect(value, message)) as unknown as typeof expect;
  checks.poll = ((predicate: () => Promise<boolean>) => ({ toBe: async () => {
    if (quietProbe) return quietProbe(predicate);
    expect(await predicate()).toBe(false); t.advance(1000); expect(await predicate()).toBe(true);
  } })) as unknown as typeof expect.poll;
  const run = createBudgetRecorder({
    prepareSemanticTimingCapture: async () => undefined,
    applyConfirmedSemanticTiming: () => ({ applied: [] }), criticalReadPredecessors,
    expect: checks, test, pageLoadClock: t.clock, DEFAULT_BUDGET: budget.DEFAULT_BUDGET,
    StoredReadLedger, ConnectorCallbackPhase, CONNECTOR_PICKER_PATH: 'unused-picker',
    apiOrigin: 'https://api.fixture', canonicalRequest: () => `actor-scope|${address}`,
    exclusion: () => null, isDeclaredPageRead: budget.isDeclaredPageRead,
    REFRESH_POLL_MS, refreshRecoveryMs, refreshLimits, DEFAULT_TRANSPORT_CONFIG, Date: { now: t.clock.wallNow },
  });
  const handlers = new Map<string, (value: unknown) => void>();
  const emitRead = async (value: unknown) => {
    const request = { url: () => `https://api.fixture${address}`, method: () => 'GET', headers: () => ({ 'x-coralswarm-priority': 'critical' }), failure: () => null };
    handlers.get('request')!(request);
    handlers.get('response')!({ request: () => request, status: () => 200, ok: () => true, json: async () => value });
    await Promise.resolve(); await Promise.resolve();
    handlers.get('requestfinished')!(request);
  };
  const page = { on: (event: string, fn: (value: unknown) => void) => handlers.set(event, fn), off: (event: string) => handlers.delete(event),
    url: () => 'https://app.fixture/chats', locator: () => locator, goto: async (_path: string) => {} };
  return { run: () => run(page as unknown as Page, '/chats', { maxRequests: 6, maxDepth: 1, mode: 'owed' }, t.clock), page, emitRead };
}

test('exact194ms backward wall jump: true2016ms owed poll passes strict depth and keeps raw wall evidence', async () => {
  const t = time(); const r = recorder(t);
  r.page.goto = async () => { await r.emitRead(body); t.advance(2016); t.jump(-194); await r.emitRead({ ...body, computed_at: '2026-10-31T23:19:51.190377Z', stale: false, refresh_eta: null }); };
  const rows = await r.run();
  expect(rows).toHaveLength(2); expect(rows[1].ledger).toBe('owed-refresh');
  expect(rows[1].started - rows[0].finished!).toBe(2016);
  expect(rows[1].wallStarted! - rows[0].wallFinished!).toBe(1822);
  expect(rows[0].certificates?.[0]).toMatchObject({ model: 'chat_empty', version: 1, computedAt: null });
});

test('forward wall jump cannot turn a true1949ms early read into owed or bypass depth1', async () => {
  const t = time(); const r = recorder(t);
  r.page.goto = async () => { await r.emitRead(body); t.advance(1949); t.jump(194); await r.emitRead({ ...body, computed_at: '2026-10-31T23:19:51Z', stale: false, refresh_eta: null }); };
  await expect(r.run()).rejects.toThrow(/serial critical read/);
});

test('the unchanged1950ms tolerance boundary still qualifies a genuine owed read', async () => {
  const t = time(); const r = recorder(t);
  r.page.goto = async () => { await r.emitRead(body); t.advance(1950); await r.emitRead({ ...body, computed_at: '2026-10-31T23:19:51Z', stale: false, refresh_eta: null }); };
  expect((await r.run())[1].ledger).toBe('owed-refresh');
});

test('forward wall jump cannot complete the actual one-second quiet predicate early', async () => {
  const t = time();
  const r = recorder(t, async (quiet) => {
    t.advance(100); t.jump(86_400_000); expect(await quiet()).toBe(false);
    t.advance(899); expect(await quiet()).toBe(false);
    t.advance(1); expect(await quiet()).toBe(true);
  });
  await r.run();
});

const scope = { account: '11111111-1111-4111-8111-111111111111', actor: 'actor', userId: 'user_fixture', orgId: null };
const computed = (stamp: number) => ({ ...body, computed_at: new Date(stamp).toISOString(), stale: false, refresh_eta: null });
function discovered(t: ReturnType<typeof time>) {
  const row: budget.LoadRecord = { address, path: '/v1/load/chats/empty', identity: `${JSON.stringify(['issuer', scope.userId, scope.orgId])}|${address}`, page: '/chats', initialWave: 'entry', method: 'GET', priority: 'critical',
    phase: 'initial', ledger: 'initial', started: t.clock.now(), finished: t.clock.now(), status: 200, failure: null, excluded: null, model: null, computedAt: null, refreshEta: null };
  const ledger = new StoredReadLedger(); ledger.start(row); ledger.response(row, computed(t.clock.wallNow())); return row;
}
for (const direction of ['forward', 'backward'] as const) test(`${direction} wall jump cannot shorten or extend the actual finite healthy-setup elapsed deadline`, async () => {
  const t = time(); const originalDateNow = Date.now; Date.now = t.clock.wallNow;
  const first = discovered(t); const calls: string[] = [];
  const api = { call: async (method: 'GET' | 'POST', path: string) => {
    calls.push(`${method} ${path}`);
    if (path === '/v1/me') return { status: () => 200, json: async () => ({ account_id: scope.account, user_id: scope.actor }) };
    if (method === 'POST') {
      t.advance(direction === 'forward' ? 1 : refreshRecoveryMs(refreshLimits('chat_empty')) + 1);
      t.jump(direction === 'forward' ? 86_400_000 : -86_400_000);
      return { status: () => 202, json: async () => ({ refresh_eta: new Date(t.clock.wallNow() + 5000).toISOString() }) };
    }
    if (calls.some((call) => call.startsWith('POST')) && direction === 'backward') throw new Error('unexpected HTTP beyond elapsed deadline');
    return { status: () => 200, json: async () => computed(t.clock.wallNow()) };
  } };
  try {
    if (direction === 'forward') {
      const result = await budget.prepareHealthyPageLoadFixture({ ...scope, api }, [first], t.clock);
      expect(result.failure).toBeUndefined(); expect(calls).toHaveLength(4);
      expect(result.attempts.at(-1)?.wallStarted).toBe(t.clock.wallNow());
    } else {
      await expect(budget.prepareHealthyPageLoadFixture({ ...scope, api }, [first], t.clock)).rejects.toThrow(/deadline expired/);
      expect(calls).toHaveLength(3);
    }
  } finally { Date.now = originalDateNow; }
});

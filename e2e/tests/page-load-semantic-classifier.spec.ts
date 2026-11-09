import { test, expect, type Page } from '@playwright/test';
import { createHash } from 'node:crypto';
import { createBudgetRecorder } from '../lib/pageLoadBudgetHarness';
import saved from './fixtures/google-late-node-finish.json' with { type: 'json' };
import * as budget from '../lib/pageLoadBudget';
import { createPageLoadClock } from '../lib/pageLoadClock.mjs';
import { validateSemanticTiming, applyConfirmedSemanticTiming, criticalReadPredecessors } from '../perf/semantic-timing.mjs';
import { StoredReadLedger, ConnectorCallbackPhase, REFRESH_POLL_MS } from '../../web/lib/pageLoadEvidence';
import { refreshLimits, refreshRecoveryMs } from '../../web/lib/pageRefreshPolicy';
import { DEFAULT_TRANSPORT_CONFIG } from '../../web/lib/transport';

const address = '/v1/connectors/google', scope = '["fixture","actor",null]', nonce = '11111111-1111-4111-8111-111111111111';
const digest = createHash('sha256').update(`${nonce}\0${scope}`).digest('hex');
const { identity: _identity, ...certificate } = saved.previous.certificates[0];
// Explicit mocked payload, not a recovered historical response body.
const body = { model: certificate.model, version: certificate.version, computed_at: certificate.computedAt, stale: certificate.stale,
  refresh_eta: certificate.refreshEta, payload: { key: 'google', name: 'Google', sources: [], topics: [] } };
function snapshot(elapsed: number, validationDelay = 0) {
  const confirm = 100 + validationDelay, child = confirm + elapsed;
  return { version: 1, nonce, clock: 'browser-performance', errors: [], events: [
    { kind: 'run', at: 0, run: 1, query: 1, model: certificate.model, address, parent: null },
    { kind: 'dispatch', at: 0, attempt: 1, run: 1, address, method: 'GET', priority: 'critical', scopeDigest: digest },
    { kind: 'response', at: 50, attempt: 1, status: 200 },
    { kind: 'admission', at: confirm - 10, run: 1, query: 1, attempt: 1, certificate: { ...certificate } },
    { kind: 'cache-success', at: confirm, run: 1, query: 1, admission: 4, manual: false },
    { kind: 'run', at: child, run: 2, query: 1, model: certificate.model, address, parent: 4 },
    { kind: 'dispatch', at: child, attempt: 2, run: 2, address, method: 'GET', priority: 'critical', scopeDigest: digest },
    { kind: 'response', at: child + 50, attempt: 2, status: 200 },
    { kind: 'admission', at: child + 100, run: 2, query: 1, attempt: 2, certificate: { ...certificate } },
    { kind: 'cache-success', at: child + 101, run: 2, query: 1, admission: 9, manual: false },
  ].map((event, i) => ({ ...event, seq: i + 1 })) };
}

interface Case { elapsed?: number; nodeGap?: number; validationDelay?: number; wallJump?: number; disabled?: boolean;
  edit?: (value: ReturnType<typeof snapshot>) => void; forged?: 'object' | 'callback' | 'mutated'; mutateAfter?: (rows: budget.LoadRecord[]) => void; fresh?: boolean; retry?: boolean; callback?: boolean; wrongLedger?: boolean; mutateSnapshotAfter?: (value: ReturnType<typeof snapshot>) => void; mutateAccessor?: boolean }
function recorder(options: Case = {}) {
  let elapsed = 0, wall = saved.previous.started;
  const clock = createPageLoadClock({ elapsedNow: () => elapsed, wallNow: () => wall });
  const advance = (ms: number) => { elapsed += ms; wall += ms; };
  const handlers = new Map<string, (value: unknown) => void>(), locator = { count: async () => 1 };
  const checks = ((value: unknown, message?: string) => value === locator ? { toBeAttached: async () => {} } : expect(value, message)) as unknown as typeof expect;
  checks.poll = ((predicate: () => Promise<boolean>) => ({ toBe: async () => { expect(await predicate()).toBe(false); advance(1000); expect(await predicate()).toBe(true); } })) as unknown as typeof expect.poll;
  let observed: budget.LoadRecord[] = [];
  const prepare = async () => options.disabled ? undefined : async (records: any[], rows: budget.LoadRecord[], ledger: StoredReadLedger) => {
    observed = rows;
    const s = snapshot(options.elapsed ?? 2000, options.validationDelay);
    if (options.fresh) for (const event of s.events) if ('certificate' in event && event.certificate) { event.certificate.stale = false; event.certificate.refreshEta = null as any; }
    options.edit?.(s);
    const proof = validateSemanticTiming(s, nonce, records, rows, ledger);
    options.mutateAfter?.(rows);
    options.mutateSnapshotAfter?.(s);
    if (options.mutateAccessor) ledger.pollTimingCandidate = () => { throw Error('forged accessor must never run'); };
    if (options.forged === 'mutated') proof.timings[0].confirmationToDispatchMs = 9999;
    if (options.forged === 'object') return { ...proof };
    if (options.forged === 'callback') return Object.assign(() => 2000, proof);
    return proof;
  };
  let lastCorrection: ReturnType<typeof applyConfirmedSemanticTiming> | undefined;
  const apply = (capture: unknown, records: any[], rows: budget.LoadRecord[], ledger: StoredReadLedger) => {
    lastCorrection = applyConfirmedSemanticTiming(capture, records, rows, options.wrongLedger ? { pollTimingCandidate: () => { throw Error('forged ledger must never run'); } } : ledger);
    return lastCorrection;
  };
  const run = createBudgetRecorder({
    prepareSemanticTimingCapture: prepare, applyConfirmedSemanticTiming: apply, criticalReadPredecessors,
    expect: checks, test, pageLoadClock: clock, DEFAULT_BUDGET: budget.DEFAULT_BUDGET,
    StoredReadLedger, ConnectorCallbackPhase, CONNECTOR_PICKER_PATH: 'unused-picker',
    apiOrigin: 'https://api.fixture', canonicalRequest: () => `${scope}|${address}`,
    exclusion: () => null, isDeclaredPageRead: budget.isDeclaredPageRead,
    REFRESH_POLL_MS, refreshRecoveryMs, refreshLimits, DEFAULT_TRANSPORT_CONFIG,
  });
  const emit = async (duration: number, status = 200) => {
    const request = { url: () => `https://api.fixture${address}`, method: () => 'GET', headers: () => ({ 'x-coralswarm-priority': 'critical' }), failure: () => null };
    let resolve!: (value: unknown) => void;
    handlers.get('request')!(request);
    handlers.get('response')!({ request: () => request, status: () => status, ok: () => status === 200, json: () => new Promise(r => { resolve = r; }) });
    advance(duration); handlers.get('requestfinished')!(request);
    if (status === 200) { resolve(options.fresh ? { ...body, stale: false, refresh_eta: null } : body); await Promise.resolve(); await Promise.resolve(); await Promise.resolve(); }
  };
  const page = { on: (event: string, fn: (value: unknown) => void) => handlers.set(event, fn), off: (event: string) => handlers.delete(event), url: () => 'https://app.fixture/connectors/google', locator: () => locator,
    goto: async () => { await emit(saved.previous.finished - saved.previous.started, options.retry ? 503 : 200); advance(options.nodeGap ?? saved.current.started - saved.previous.finished); wall += options.wallJump ?? 0; await emit(saved.current.finished - saved.current.started); } };
  return { run: () => run(page as unknown as Page, '/connectors/google', { maxRequests: 6, maxDepth: 1, mode: options.retry ? 'degraded' : 'owed' }, clock), rows: () => observed, correction: () => lastCorrection };
}

test('saved hosted Node1926ms gap + explicitly MOCKED verified browser2000ms proof is owed, with raw Node evidence unchanged', async () => {
  expect(saved.label).toContain('SYNTHETIC');
  const r = recorder(), rows = await r.run();
  expect(rows).toHaveLength(2); expect(rows[1].ledger).toBe('owed-refresh'); expect(rows[1].nodeLedger).toBe('initial');
  expect(rows[1].started).toBe(saved.current.started); expect(rows[0].finished).toBe(saved.previous.finished);
  expect(rows[1].started - rows[0].finished!).toBeCloseTo(1926.9695, 3);
  expect(rows[1].confirmedPollSpacing).toMatchObject({ elapsedMs: 2000, parentAdmission: 4, confirmation: 5, attempt: 2, source: 'verified-browser-cache-confirmation' });
});
for (const wallJump of [0, 3600_000, -3600_000]) test(`Node>=1950 but TRUE browser1949 after validation delay stays initial/depth FAIL, wallstep${wallJump}`, async () => {
  const r = recorder({ elapsed: 1949, validationDelay: 250, nodeGap: 2087, wallJump });
  await expect(r.run()).rejects.toThrow(/serial critical read/);
  expect(r.rows()[1].nodeLedger).toBe('owed-refresh'); expect(r.rows()[1].ledger).toBe('initial');
  expect(r.rows()[1].confirmedPollSpacing?.elapsedMs).toBe(1949);
});
for (const [name, options] of Object.entries<Case>({
  'capture off': { disabled: true }, 'lookalike result': { forged: 'object' }, 'forged callback': { forged: 'callback' },
  'after-validation public metadata mutation': { forged: 'mutated' },
  'after-validation identical row replacement': { mutateAfter: rows => { rows[0] = { ...rows[0] }; } },
  'wrong parent': { edit: s => { s.events[5].parent = 999; } },
  'wrong priority group': { edit: s => { s.events[1].priority = 'background'; } },
  'wrong scope': { edit: s => { s.events[1].scopeDigest = '0'.repeat(64); } },
  'missing confirmation': { edit: s => { s.events[4].manual = true; } },
  'after-validation certificate mutation': { mutateAfter: rows => { rows[0].certificates![0].refreshEta = null; } },
  'wrong eligibility ledger': { wrongLedger: true },
  'after-validation accessor replacement': { mutateAccessor: true },
  'after-validation snapshot mutation': { mutateSnapshotAfter: s => { s.events[6].at += 500; } },
  'after-validation address mutation': { mutateAfter: rows => { rows[0].address += '?wrong=1'; } },
  'after-validation prior index mutation': { mutateAfter: rows => { rows.reverse(); } },
  'after-validation model mutation': { mutateAfter: rows => { rows[0].certificates![0].model = 'home_metrics'; } },
  'after-validation scope mutation': { mutateAfter: rows => { rows[0].identity = rows[0].identity.replace('actor', 'other'); } },
})) test(`${name} cannot rescue the saved late-Node gap`, async () => { await expect(recorder(options).run()).rejects.toThrow(); });
test('fresh prior answers never gain owed classification from elapsed time', async () => { await expect(recorder({ fresh: true }).run()).rejects.toThrow(/serial critical read/); });

test('after-validation priority mutation invalidates proof rather than changing its saved binding', async () => {
  const r = recorder({ mutateAfter: rows => { rows[0].priority = 'background'; } });
  const rows = await r.run(); // The mutated input has only one critical row; inspect correction itself.
  expect(r.correction()?.applied).toHaveLength(0);
  expect(rows[1].ledger).toBe('initial'); expect(rows[1].confirmedPollSpacing).toBeUndefined();
});
test('registry recovery expiry cannot be extended by an otherwise valid browser interval', async () => {
  const r = recorder({ nodeGap: refreshRecoveryMs(refreshLimits('connector_detail')) + 1 });
  await expect(r.run()).rejects.toThrow(/serial critical read/);
  expect(r.correction()?.applied).toHaveLength(0);
});
test('a transport retry remains a retry, never a semantic owed read', async () => {
  const r = recorder({ retry: true }); const rows = await r.run();
  expect(rows[1].ledger).toBe('retry'); expect(rows[1].confirmedPollSpacing).toBeUndefined();
  expect(r.correction()?.applied).toHaveLength(0);
});

test('read-only candidates are bound to the exact attempt/prior objects, never lookalikes', () => {
  const ledger = new StoredReadLedger();
  const a: budget.LoadRecord = { address, path: address, identity: `${scope}|${address}`, page: '/connectors/google', initialWave: 'entry', phase: 'initial', method: 'GET', priority: 'critical', started: 0, finished: null, status: 200, failure: null, excluded: null, ledger: 'initial', model: null, computedAt: null, refreshEta: null };
  ledger.start(a); a.finished = 100; ledger.response(a, body);
  const b: budget.LoadRecord = { ...a, started: 2026, finished: null, ledger: 'initial', model: null, certificates: undefined };
  ledger.start(b);
  expect(ledger.pollTimingCandidate(b, a)?.minimumSpacingMs).toBe(1950);
  expect(ledger.pollTimingCandidate({ ...b }, a)).toBeUndefined();
  expect(ledger.pollTimingCandidate(b, { ...a })).toBeUndefined();
  expect(b.ledger).toBe('initial'); // Getter does not apply or accept timing.
});

test('terminal cache error settlement does not authorize a successful publication interval', async () => {
  const r = recorder({ edit: s => {
    const e = s.events[9] as Record<string, unknown>;
    e.kind = 'cache-error'; delete e.admission; delete e.manual;
  } });
  await expect(r.run()).rejects.toThrow(/serial critical read/);
  expect(r.correction()?.applied).toHaveLength(0);
});

test('callback waves cannot acquire a semantic timing correction even with a valid capture', () => {
  const ledger = new StoredReadLedger();
  const a: budget.LoadRecord = { address, path: address, identity: `${scope}|${address}`, page: '/connectors/google', initialWave: 'entry', phase: 'callback-action', method: 'GET', priority: 'critical', started: saved.previous.started, finished: null, status: 200, failure: null, excluded: null, ledger: 'initial', model: null, computedAt: null, refreshEta: null };
  ledger.start(a); a.finished = saved.previous.finished; ledger.response(a, body);
  const b: budget.LoadRecord = { ...a, started: saved.current.started, finished: null, ledger: 'initial', model: null, certificates: undefined };
  ledger.start(b); b.finished = saved.current.finished; ledger.response(b, body);
  const rows = [a,b], records = rows.map(row => ({ ...row, scope, path: row.address }));
  const capture = validateSemanticTiming(snapshot(2000), nonce, records, rows, ledger);
  expect(capture.ok).toBe(true);
  expect(applyConfirmedSemanticTiming(capture, records, rows, ledger).applied).toHaveLength(0);
  expect(b.ledger).toBe('initial'); expect(b.confirmedPollSpacing).toBeUndefined();
});

test('even a validated capture cannot turn a forged eligibility callback/object into an original ledger candidate', () => {
  const real = new StoredReadLedger();
  const a: budget.LoadRecord = { address, path: address, identity: `${scope}|${address}`, page: '/connectors/google', initialWave: 'entry', phase: 'initial', method: 'GET', priority: 'critical', started: saved.previous.started, finished: null, status: 200, failure: null, excluded: null, ledger: 'initial', model: null, computedAt: null, refreshEta: null };
  real.start(a); a.finished = saved.previous.finished; real.response(a, body);
  const b: budget.LoadRecord = { ...a, started: saved.current.started, finished: null, ledger: 'initial', model: null, certificates: undefined };
  real.start(b); b.finished = saved.current.finished; real.response(b, body);
  const rows = [a,b], records = rows.map(row => ({ ...row, scope, path: row.address }));
  let calls = 0;
  const forged = { pollTimingCandidate: () => { calls++; return { model: 'connector_detail', minimumSpacingMs: 0, owedFrom: [] }; } };
  const capture = validateSemanticTiming(snapshot(2000), nonce, records, rows, forged as unknown as StoredReadLedger);
  expect(capture.ok).toBe(true); // Full receipt validation is necessary but cannot forge ledger ownership.
  expect(applyConfirmedSemanticTiming(capture, records, rows, forged).applied).toHaveLength(0);
  expect(calls).toBe(0); expect(b.ledger).toBe('initial');
});


test('validated capabilities are single-use and cannot replay a previously applied edge', () => {
  const ledger = new StoredReadLedger();
  const a: budget.LoadRecord = { address, path: address, identity: `${scope}|${address}`, page: '/connectors/google', initialWave: 'entry', phase: 'initial', method: 'GET', priority: 'critical', started: saved.previous.started, finished: null, status: 200, failure: null, excluded: null, ledger: 'initial', model: null, computedAt: null, refreshEta: null };
  ledger.start(a); a.finished = saved.previous.finished; ledger.response(a, body);
  const b: budget.LoadRecord = { ...a, started: saved.current.started, finished: null, ledger: 'initial', model: null, certificates: undefined };
  ledger.start(b); b.finished = saved.current.finished; ledger.response(b, body);
  const rows = [a,b], records = rows.map(row => ({ ...row, scope, path: row.address }));
  const capture = validateSemanticTiming(snapshot(2000), nonce, records, rows, ledger);
  expect(capture.ok).toBe(true);
  expect(applyConfirmedSemanticTiming(capture, records, rows, ledger).applied).toHaveLength(1);
  expect(applyConfirmedSemanticTiming(capture, records, rows, ledger)).toEqual({ applied: [], reason: 'no-unchanged-validated-capture' });
});

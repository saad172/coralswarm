// @vitest-environment jsdom
import { createHash } from 'node:crypto';
import { validateSemanticTiming, applyConfirmedSemanticTiming } from '../../e2e/perf/semantic-timing.mjs';
import { QueryObserver } from '@tanstack/react-query';
import { afterEach, expect, it, vi } from 'vitest';
import { apiRequest } from './api';
import { makeQueryClient } from './queryClient';
import { oceansIndexQueryOptions } from './oceansIndex';
import { StoredReadLedger, type LoadAttempt } from './pageLoadEvidence';
import { beginPageReadTiming, admitPageReadTiming, createPageReadTimingCollector } from './pageReadTimingReceipt';

vi.mock('./api', async actual => ({ ...await actual<typeof import('./api')>(), apiRequest: vi.fn() }));
let activeCollector: ReturnType<typeof createPageReadTimingCollector> | undefined;
vi.mock('./pageReadTimingReceipt', async actual => {
  const mod = await actual<typeof import('./pageReadTimingReceipt')>();
  return { ...mod, beginPageReadTiming: vi.fn((...args: Parameters<typeof mod.beginPageReadTiming>) => activeCollector?.begin(...args)),
    admitPageReadTiming: vi.fn((...args: Parameters<typeof mod.admitPageReadTiming>) => activeCollector?.admission(...args)) };
});
const clients: ReturnType<typeof makeQueryClient>[] = [];
afterEach(() => { clients.splice(0).forEach(client => client.clear()); vi.useRealTimers(); vi.resetAllMocks(); vi.unstubAllEnvs(); activeCollector = undefined; });

// MOCKED delivery ordering compatible with the small route070 evidence, NOT
// a reconstruction of that document's absent application publication receipt.
it.each([1, 9])('actual fan-in observer with %i withheld constituents polls after2s with complete verified semantic receipts despite a late Node finish', async count => {
  vi.useFakeTimers({ toFake: ['Date', 'performance', 'setTimeout', 'clearTimeout', 'setInterval', 'clearInterval'] });
  vi.setSystemTime(new Date('2026-11-01T07:51:03Z'));
  vi.stubEnv('NEXT_PUBLIC_E2E_PAGE_READ_RECEIPTS', '1');
  const client = makeQueryClient(); clients.push(client);
  const collector = createPageReadTimingCollector({ now: () => performance.now(), nonce: '11111111-1111-4111-8111-111111111111', digest: async text => createHash('sha256').update(text).digest('hex') });
  collector.watch(client); activeCollector = collector;
  const ledger = new StoredReadLedger(), rows: (LoadAttempt & { priority: string; phase: string })[] = [], publications: number[] = [];
  client.getQueryCache().subscribe(event => {
    if (event.type === 'updated' && event.action.type === 'success' && !event.action.manual) publications.push(performance.now());
  });
  vi.mocked(apiRequest).mockImplementation((_path, _token, _init, _retry, options) => {
    expect(options?.pageReadTiming).toBeDefined(); // Actual factory passes its private run to the real transport boundary.
    const index = rows.length;
    const row: LoadAttempt & { priority: string; phase: string } = { address: '/v1/load/oceans', path: '/v1/load/oceans', identity: 'fixture|/v1/load/oceans', method: 'GET', priority: 'critical', phase: 'initial', started: performance.now(), finished: null, status: 200, ledger: 'initial', model: null, computedAt: null, refreshEta: null };
    ledger.start(row); rows.push(row);
    const value = { components: Array.from({ length: count }, (_, i) => ({
      account_id: `00000000-0000-4000-8000-${String(i + 1).padStart(12, '0')}`, anchor_graph_id: null,
      refresh_path: '/v1/load/oceans/component/refresh', stored: { model: 'oceans_index', version: 1,
        computed_at: index ? '2026-11-01T07:51:07Z' : null, stale: !index,
        refresh_eta: index ? null : '2026-11-01T07:51:08Z', payload: { oceans: [], members: [], team_members: {}, archived: null } },
    })) };
    const attempt = collector.dispatch(options?.pageReadTiming, '/v1/load/oceans', 'GET', 'critical', 'fixture');
    setTimeout(() => { row.finished = performance.now(); ledger.response(row, value); }, 158);
    return new Promise(resolve => setTimeout(() => {
      collector.response(attempt, new Response(null, { status: 200 })); resolve(value as never);
    }, 100));
  });
  const observer = new QueryObserver(client, oceansIndexQueryOptions({ scope: 'fixture', getAuthToken: () => 'token' }));
  const unsubscribe = observer.subscribe(() => {});
  await vi.advanceTimersByTimeAsync(100);
  expect(publications).toEqual([100]);
  await vi.advanceTimersByTimeAsync(2000);
  expect(rows).toHaveLength(2);
  expect(rows[1].started - publications[0]).toBe(2000);
  expect(rows[1].started - rows[0].finished!).toBe(1942);
  expect(rows[1].ledger).toBe('initial');
  expect(ledger.pollTimingCandidate(rows[1], rows[0])?.owedFrom).toHaveLength(count);
  await vi.advanceTimersByTimeAsync(200);
  const snapshot = await collector.snapshot();
  expect(beginPageReadTiming).toHaveBeenCalledTimes(2);
  expect(admitPageReadTiming).toHaveBeenCalled();
  expect(snapshot.errors).toEqual([]);
  expect(snapshot.events.filter(event => event.kind === 'dispatch')).toHaveLength(2);
  const admissions = snapshot.events.filter(event => event.kind === 'admission');
  expect(admissions.length).toBeGreaterThanOrEqual(2);
  expect(admissions.every(event => event.certificate === undefined && event.components?.length === count)).toBe(true);
  const records = rows.map(row => ({ ...row, scope: 'fixture' }));
  const proof = validateSemanticTiming(snapshot, snapshot.nonce, records, rows, ledger);
  expect(proof.errors).toEqual([]); expect(proof.ok).toBe(true);
  expect(applyConfirmedSemanticTiming(proof, records, rows, ledger).applied).toHaveLength(1);
  expect(rows[1].ledger).toBe('owed-refresh');
  const bad = structuredClone(snapshot);
  bad.events.find(event => event.kind === 'admission')!.components!.pop();
  expect(validateSemanticTiming(bad, bad.nonce, records, rows, ledger).ok).toBe(false);
  unsubscribe();
});

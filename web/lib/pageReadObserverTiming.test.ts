// @vitest-environment jsdom
import { QueryObserver } from '@tanstack/react-query';
import { afterEach, expect, it, vi } from 'vitest';
import { makeQueryClient } from './queryClient';
import { pageSectionQuery, type Stored } from './pageSections';
import { StoredReadLedger, type LoadAttempt } from './pageLoadEvidence';
import { apiRequest } from './api';
import { criticalReadCount } from './transport';

vi.mock('./api', async (actual) => ({ ...await actual<typeof import('./api')>(), apiRequest: vi.fn() }));
const read = vi.mocked(apiRequest);
const withheld: Stored<Record<string, unknown>> = { model: 'connector_detail', version: 1,
  computed_at: null, stale: true, refresh_eta: '2026-11-01T02:38:18.924218Z',
  payload: { key: 'google', name: 'Google', sources: [], topics: [], captures: { items: [], next_cursor: null } } };
const clients: ReturnType<typeof makeQueryClient>[] = [];
afterEach(() => { clients.splice(0).forEach(client => client.clear()); vi.useRealTimers(); vi.resetAllMocks(); });

function scenario(finishDelay: number) {
  vi.useFakeTimers({ toFake: ['Date', 'performance', 'setTimeout', 'clearTimeout', 'setInterval', 'clearInterval'] });
  vi.setSystemTime(new Date('2026-11-01T02:38:20Z'));
  const client = makeQueryClient(); clients.push(client);
  const ledger = new StoredReadLedger(), rows: LoadAttempt[] = [], publications: number[] = [];
  client.getQueryCache().subscribe(event => {
    if (event.type === 'updated' && event.action.type === 'success' && !event.action.manual)
      publications.push(performance.now());
  });
  read.mockImplementation(() => {
    const index = rows.length;
    const row: LoadAttempt = { address: '/v1/connectors/google', path: '/v1/connectors/google',
      identity: 'same-verified-actor|/v1/connectors/google', method: 'GET', started: performance.now(),
      finished: null, status: 200, ledger: 'initial', model: null, computedAt: null, refreshEta: null };
    rows.push(row); ledger.start(row);
    const value = index === 0 ? withheld : { ...withheld, computed_at: '2026-11-01T02:38:21Z', stale: false, refresh_eta: null };
    // An admissible mocked ordering, NOT claimed hosted delivery timestamps.
    setTimeout(() => { row.finished = performance.now(); }, 100 + finishDelay);
    setTimeout(() => ledger.response(row, value), 100 + finishDelay + 23);
    return new Promise(resolve => setTimeout(() => resolve(value as never), 100));
  });
  const observer = new QueryObserver(client, pageSectionQuery('connector_detail', 'same-scope', { provider: 'google' }, () => 'token'));
  const unsubscribe = observer.subscribe(() => {});
  return { observer, rows, publications, unsubscribe };
}

it('actual TanStack withheld success arms a 2s poll while delayed Node finish moves the ledger anchor forward', async () => {
  const s = scenario(74);
  await vi.advanceTimersByTimeAsync(100);
  expect(s.publications).toEqual([100]);
  await vi.advanceTimersByTimeAsync(1999);
  expect(s.rows).toHaveLength(1);
  await vi.advanceTimersByTimeAsync(1);
  expect(s.rows).toHaveLength(2);
  expect(s.rows[1].started - s.publications[0]).toBe(2000);
  const finish = s.rows[0].finished;
  if (finish === null) throw new Error('Mock Node finish was not delivered');
  expect(s.rows[1].started - finish).toBe(1926);
  expect(s.rows[0].certificates?.[0]).toMatchObject({ model: 'connector_detail', version: 1, stale: true });
  // Characterize the existing false negative without changing the rule or
  // claiming this model proves the hosted application's unseen delivery time.
  expect(s.rows[1].ledger).toBe('initial');
  await vi.advanceTimersByTimeAsync(100);
  s.unsubscribe();
  expect(criticalReadCount()).toBe(0);
});

it('a genuinely early 1949ms read remains initial despite a start-to-start gap above 2s', async () => {
  const s = scenario(0);
  await vi.advanceTimersByTimeAsync(2049);
  expect(s.publications).toEqual([100]); expect(s.rows).toHaveLength(1);
  const refetch = s.observer.refetch();
  await vi.advanceTimersByTimeAsync(0);
  expect(s.rows[1].started - s.rows[0].started).toBe(2049);
  expect(s.rows[1].started - s.publications[0]).toBe(1949);
  expect(s.rows[1].ledger).toBe('initial');
  await vi.advanceTimersByTimeAsync(100); await refetch;
  s.unsubscribe();
  expect(criticalReadCount()).toBe(0);
});

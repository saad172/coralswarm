// @vitest-environment jsdom
import { webcrypto } from 'node:crypto';
import { QueryObserver } from '@tanstack/react-query';
import { waitFor } from '@testing-library/react';
import { afterEach, expect, it, vi } from 'vitest';
import { makeQueryClient } from './queryClient';
import { oceansIndexQueryOptions } from './oceansIndex';
import { __resetTransportForTests } from './transport';
import { StoredReadLedger, type LoadAttempt } from './pageLoadEvidence';
import { validateSemanticTiming } from '../../e2e/perf/semantic-timing.mjs';
const clients: ReturnType<typeof makeQueryClient>[] = [], stops: (() => void)[] = [];
afterEach(() => { stops.splice(0).forEach(stop => stop()); clients.splice(0).forEach(client => client.clear()); __resetTransportForTests(); vi.unstubAllGlobals(); vi.unstubAllEnvs(); });
it('actual factory/client/transport opt-in binds every fan-in constituent to its native mocked GET and confirmed parent', async () => {
  vi.stubEnv('NEXT_PUBLIC_E2E_PAGE_READ_RECEIPTS','1');
  Object.defineProperty(window, '__csPageReadTimingNonce', { value: '11111111-1111-4111-8111-111111111111', configurable: false, writable: false });
  vi.stubGlobal('crypto', webcrypto);
  const scope = '["fixture","actor",null]';
  const token = `header.${btoa(JSON.stringify({ iss: 'fixture', sub: 'actor' }))}.signature`;
  const uuid = (n: number) => `00000000-0000-4000-8000-${String(n).padStart(12,'0')}`;
  const value = (fresh: boolean) => ({ components: Array.from({ length: 9 }, (_, i) => ({ account_id: uuid(i + 1), anchor_graph_id: i ? uuid(i + 100) : null,
    refresh_path: `/v1/load/oceans/component/refresh${i ? `?graph=${uuid(i + 100)}` : ''}`, stored: { model: 'oceans_index', version: 1,
      computed_at: !fresh && i === 0 ? null : '2026-11-01T07:51:03Z', stale: !fresh && i % 3 !== 2,
      refresh_eta: !fresh && i % 3 !== 2 ? '2026-11-01T07:51:08Z' : null,
      payload: { oceans: [], members: [], team_members: {}, archived: null, secret: 'do-not-capture' } },
  })) });
  const ledger = new StoredReadLedger(), rows: (LoadAttempt & { priority: string; phase: string })[] = [];
  const fetch = vi.fn().mockImplementation(async (url: string, init: RequestInit) => {
    const path = new URL(url).pathname, body = value(rows.length > 0);
    const row: typeof rows[number] = { path, address: path, identity: `${scope}|${path}`, method: init.method ?? 'GET', priority: new Headers(init.headers).get('x-coralswarm-priority')!, phase: 'initial',
      started: performance.now(), finished: null, status: 200, ledger: 'initial', model: null, computedAt: null, refreshEta: null };
    ledger.start(row); rows.push(row); row.finished = performance.now(); ledger.response(row,body);
    return new Response(JSON.stringify(body),{ status: 200 });
  });
  vi.stubGlobal('fetch', fetch);
  const client = makeQueryClient(); clients.push(client);
  const observer = new QueryObserver(client, oceansIndexQueryOptions({ scope, getAuthToken: () => token }));
  stops.push(observer.subscribe(() => {}));
  await waitFor(() => expect(observer.getCurrentResult().isSuccess).toBe(true));
  const getSnapshot = window.__csPageReadTimingSnapshot;
  if (!getSnapshot) throw Error('Missing opt-in snapshot');
  const first = await getSnapshot(), confirmation = first.events.filter(e => e.kind === 'cache-success' && !e.manual).at(-1);
  expect(confirmation).toBeDefined();
  await observer.refetch();
  const snapshot = await getSnapshot();
  expect(snapshot.errors).toEqual([]); expect(fetch).toHaveBeenCalledTimes(2);
  expect(snapshot.events.filter(e => e.kind === 'run').at(-1)?.parent).toBe(confirmation?.admission);
  expect(snapshot.events.filter(e => e.kind === 'admission').every(e => e.certificate === undefined && e.components?.length === 9)).toBe(true);
  const records = rows.map(row => ({ ...row, scope }));
  expect(validateSemanticTiming(snapshot,snapshot.nonce,records,rows,ledger).errors).toEqual([]);
  const wrong = structuredClone(snapshot);
  wrong.events.find(e => e.kind === 'admission')!.components![1].anchorGraphId = uuid(999);
  expect(validateSemanticTiming(wrong,wrong.nonce,records,rows,ledger).ok).toBe(false);
  expect(JSON.stringify(snapshot)).not.toMatch(/do-not-capture|payload|signature|fixture|actor/);
});

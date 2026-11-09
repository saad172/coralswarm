// @vitest-environment jsdom
import { QueryObserver } from '@tanstack/react-query';
import { waitFor } from '@testing-library/react';
import { afterEach, expect, it } from 'vitest';
import { ApiError } from './api';
import { makeQueryClient } from './queryClient';
import { authorizedPageRead, pageReadPublicationBoundary, type PageReadContext } from './pageReadAuthority';
import { parseOceansIndex, type OceansIndex } from './oceansIndex';
import { createPageReadTimingCollector } from './pageReadTimingReceipt';
const clients: ReturnType<typeof makeQueryClient>[] = [], stops: (() => void)[] = [];
afterEach(() => { stops.splice(0).forEach(stop => stop()); clients.splice(0).forEach(client => client.clear()); });
const uuid = (n: number) => `00000000-0000-4000-8000-${String(n).padStart(12, '0')}`;
const body = (count: number, fresh = false) => ({ components: Array.from({ length: count }, (_, i) => ({ account_id: uuid(i + 1), anchor_graph_id: i ? uuid(i + 100) : null,
  refresh_path: `/v1/load/oceans/component/refresh${i ? `?graph=${uuid(i + 100)}` : ''}`,
  stored: { model: 'oceans_index', version: 1, computed_at: !fresh && i % 3 === 0 ? null : '2026-11-01T07:49:39Z', stale: !fresh && i % 3 !== 2,
    refresh_eta: !fresh && i % 3 !== 2 ? '2026-11-01T07:51:08Z' : null,
    payload: { oceans: [], members: [], team_members: {}, archived: null, secret: 'never-copy-payload' } },
})) });
function fixture(count: number) {
  let now = 0;
  const client = makeQueryClient(); clients.push(client);
  const collector = createPageReadTimingCollector({ now: () => ++now, nonce: '11111111-1111-4111-8111-111111111111', digest: async () => 'a'.repeat(64) });
  collector.watch(client);
  const boundary = pageReadPublicationBoundary(), address = '/v1/load/oceans';
  let read: () => Promise<OceansIndex> = async () => parseOceansIndex(body(count));
  // Test-only canonical guarded factory, NOT an edit to the currently unguarded
  // product Oceans factory. Its authority integration is separately owned.
  const options = { queryKey: ['guarded-fanin', count], staleTime: 0, retry: false as const,
    meta: { pageRead: true, model: 'oceans_index', readIdentity: 'verified-fanin-fixture', priority: 'critical' },
    structuralSharing: function(this: object, previous: unknown, value: unknown) {
      const admitted = boundary.structuralSharing.call(this, previous, value);
      collector.admission(this, admitted); return admitted;
    },
    queryFn: async (context: PageReadContext) => {
      boundary.observe(context);
      const run = collector.begin(context.client, context.queryKey, address);
      const attempt = collector.dispatch(run, address, 'GET', 'critical', 'fixture-scope');
      return authorizedPageRead(context, options.meta.readIdentity, async () => {
        try { const value = await read(); collector.response(attempt, new Response(null, { status: 200 })); return value; }
        catch (e) { if (e instanceof ApiError) collector.response(attempt, new Response(null, { status: e.status })); throw e; }
      }, { publishStored: true, withheld: value => value.components.some(c => c.stored.computed_at === null) });
    },
  };
  return { client, collector, options, setRead: (fn: typeof read) => { read = fn; } };
}
for (const count of [1, 9]) it(`projects the complete guarded ${count}-constituent set and confirms the actual parent without copying payload`, async () => {
  const f = fixture(count), observer = new QueryObserver(f.client, f.options);
  stops.push(observer.subscribe(() => {}));
  await waitFor(() => expect(observer.getCurrentResult().isSuccess).toBe(true));
  const first = await f.collector.snapshot();
  expect(first.errors).toEqual([]);
  const admission = first.events.find(e => e.kind === 'admission');
  expect(admission?.certificate).toBeUndefined(); expect(admission?.components).toHaveLength(count);
  expect(admission?.components?.[0]).toMatchObject({ identity: uuid(1), anchorGraphId: null, refreshPath: '/v1/load/oceans/component/refresh', certificate: { model: 'oceans_index', computedAt: null } });
  const confirmed = first.events.find(e => e.kind === 'cache-success' && !e.manual);
  expect(confirmed).toBeDefined();
  f.setRead(async () => parseOceansIndex(body(count, true)));
  await f.client.fetchQuery(f.options);
  const second = await f.collector.snapshot(), run = second.events.filter(e => e.kind === 'run').at(-1);
  expect(run?.parent).toBe(confirmed?.admission);
  expect(second.events.filter(e => e.kind === 'admission').at(-1)?.components).toHaveLength(count);
  expect(JSON.stringify(second)).not.toMatch(/never-copy-payload|payload|fixture-scope/);
  if (!admission?.components) throw Error('Missing full projection');
  admission.components[0].certificate.version = 999;
  admission.components.pop();
  expect((await f.collector.snapshot()).events.find(e => e.kind === 'admission')?.components).toHaveLength(count);
  expect((await f.collector.snapshot()).events.find(e => e.kind === 'admission')?.components?.[0].certificate.version).toBe(1);
  const before = second.events.length;
  f.collector.admission({}, body(count));
  expect((await f.collector.snapshot()).events).toHaveLength(before); // Unbound hook cannot forge publication.
});
it('refusal and canceled reads never acquire fan-in admission/public success, and withheld recovery keeps every certificate', async () => {
  const f = fixture(9); await f.client.fetchQuery(f.options);
  f.setRead(async () => { throw new ApiError('Forbidden', 403); });
  await expect(f.client.fetchQuery(f.options)).rejects.toBeDefined();
  const refused = await f.collector.snapshot(), refusedRun = refused.events.filter(e => e.kind === 'run').at(-1)?.run;
  expect(refused.events.some(e => e.kind === 'admission' && e.run === refusedRun)).toBe(false);
  expect(f.client.getQueryData(f.options.queryKey)).toBeUndefined();
  f.setRead(async () => parseOceansIndex(body(9)));
  await f.client.fetchQuery(f.options);
  const recovered = await f.collector.snapshot(), last = recovered.events.filter(e => e.kind === 'admission').at(-1);
  expect(last?.components).toHaveLength(9); expect(last?.components?.[0].certificate.computedAt).toBeNull();
  let release!: (value: OceansIndex) => void;
  f.setRead(() => new Promise(resolve => { release = resolve; }));
  const safe = f.client.getQueryData(f.options.queryKey);
  const pending = f.client.fetchQuery(f.options), settled = expect(pending).resolves.toBe(safe);
  await waitFor(() => expect(release).toBeDefined());
  await f.client.cancelQueries({ queryKey: f.options.queryKey });
  release(parseOceansIndex(body(9, true))); await settled; // Public cancellation may return the already-guarded cache, never the late fresh value.
  const canceled = await f.collector.snapshot(), canceledRun = canceled.events.filter(e => e.kind === 'run').at(-1)?.run;
  expect(canceled.events.some(e => e.kind === 'admission' && e.run === canceledRun)).toBe(false);
  expect(canceled.events.some(e => e.kind === 'cache-success' && e.run === canceledRun)).toBe(false);
  expect(canceled.errors).toEqual([]);
});

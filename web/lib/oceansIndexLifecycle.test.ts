// @vitest-environment jsdom
import { focusManager, QueryObserver } from '@tanstack/react-query';
import { afterEach, expect, it, vi } from 'vitest';
import { apiRequest, ApiError } from './api';
import { makeQueryClient, STALE } from './queryClient';
import { oceansIndexQueryOptions, parseOceansIndex } from './oceansIndex';
import { pageReadPublicationBoundary } from './pageReadAuthority';
vi.mock('./api', async actual => ({ ...await actual<typeof import('./api')>(), apiRequest: vi.fn() }));
const clients: ReturnType<typeof makeQueryClient>[] = [], stops: (() => void)[] = [];
afterEach(() => { stops.splice(0).forEach(stop => stop()); clients.splice(0).forEach(client => client.clear()); vi.resetAllMocks(); vi.unstubAllEnvs(); focusManager.setFocused(undefined); vi.useRealTimers(); });
const uuid = (n: number) => `00000000-0000-4000-8000-${String(n).padStart(12, '0')}`;
function body(count: number, pending = false, revision = 1) {
  return { components: Array.from({ length: count }, (_, i) => ({ account_id: uuid(i + 1), anchor_graph_id: i ? uuid(i + 100) : null,
    refresh_path: `/v1/load/oceans/component/refresh${i ? `?graph=${uuid(i + 100)}` : ''}`,
    stored: { model: 'oceans_index', version: 1, computed_at: pending && i === 0 ? null : `2026-11-01T07:51:0${revision}Z`,
      stale: pending && i === 0, refresh_eta: pending && i === 0 ? '2026-11-01T07:51:08Z' : null,
      payload: pending && i === 0 ? {} : { oceans: [{ graph: { id: uuid(i + 200), name: `Ocean ${i}` }, counts: { reef_total: revision + i, atom_count: revision + i, source_count: revision + i, reef_counts: {} } }], members: [], team_members: {}, archived: null } },
  })) };
}
function fixture() {
  vi.stubEnv('NEXT_PUBLIC_E2E_PAGE_READ_RECEIPTS', undefined);
  const client = makeQueryClient(); clients.push(client);
  const options = oceansIndexQueryOptions({ scope: 'fixture-scope', getAuthToken: () => 'fixture-token' });
  return { client, options };
}
it('always uses the existing canonical lifecycle with the original key/meta/options while capture is off', () => {
  const { options } = fixture();
  expect(options.structuralSharing).toBe(pageReadPublicationBoundary().structuralSharing);
  expect(options.queryKey).toEqual(['oceansIndex', 'fixture-scope']);
  expect(options.meta).toEqual({ pageRead: true, model: 'oceans_index', scope: 'fixture-scope', readIdentity: '["fixture-scope","/v1/load/oceans"]', priority: 'critical' });
  expect(options.staleTime).toBe(STALE.READ); expect(options.retry).toBe(false);
  expect(options.refetchOnWindowFocus).toBe(false); expect(options.refetchOnReconnect).toBe(false);
  expect(window.__csPageReadTimingSnapshot).toBeUndefined();
});
for (const count of [1,9]) it(`standard fetch/ensure/prefetch methods share the full typed ${count}-component cache`, async () => {
  const { client, options } = fixture(), payload = body(count);
  vi.mocked(apiRequest).mockResolvedValue(payload);
  await expect(client.fetchQuery(options)).resolves.toEqual(parseOceansIndex(payload));
  await expect(client.ensureQueryData(options)).resolves.toEqual(payload);
  await client.prefetchQuery(options);
  expect(apiRequest).toHaveBeenCalledTimes(1);
  expect(client.getQueryData(options.queryKey)).toEqual(payload);
  expect('computed_at' in payload).toBe(false); // There is no aggregate certificate.
});
it('normal observer refetch replaces current counts and retains safe typed data on availability error', async () => {
  const { client, options } = fixture(), first = body(2), next = body(2, false, 2);
  vi.mocked(apiRequest).mockResolvedValueOnce(first).mockResolvedValueOnce(next).mockRejectedValueOnce(new ApiError('Temporary failure',503));
  const observer = new QueryObserver(client, { ...options, enabled: false }); stops.push(observer.subscribe(() => {}));
  await observer.refetch(); expect(observer.getCurrentResult().data).toEqual(first);
  await observer.refetch(); expect(observer.getCurrentResult().data).toEqual(next);
  await observer.refetch(); expect(observer.getCurrentResult().error).toBeInstanceOf(ApiError);
  expect(observer.getCurrentResult().data).toEqual(next);
});
it('a partially withheld response replaces old counts only for its pending constituent and preserves current computed neighbors', async () => {
  const { client, options } = fixture(), first = body(9), pending = body(9,true,2);
  vi.mocked(apiRequest).mockResolvedValueOnce(first).mockResolvedValueOnce(pending);
  await client.fetchQuery(options);
  const current = await client.fetchQuery({ ...options, staleTime: 0 });
  expect(current).toEqual(pending); expect(current.components).toHaveLength(9);
  expect(current.components[0].stored.computed_at).toBeNull();
  expect(current.components[0].stored.payload).toEqual({});
  expect(current.components[1].stored.payload.oceans[0].counts.atom_count).toBe(3);
  expect(client.getQueryData(options.queryKey)).toEqual(pending);
});
it('cold pending then computed refetch retains account/anchor/refresh identities and computed-empty semantics', async () => {
  const { client, options } = fixture(), pending = body(1,true), empty = body(1);
  empty.components[0].stored.payload = { oceans: [], members: [], team_members: {}, archived: null };
  vi.mocked(apiRequest).mockResolvedValueOnce(pending).mockResolvedValueOnce(empty);
  const a = await client.fetchQuery(options), b = await client.fetchQuery({ ...options, staleTime: 0 });
  expect(a.components[0].stored.computed_at).toBeNull(); expect(b.components[0].stored.computed_at).not.toBeNull();
  expect(b.components[0].stored.payload.oceans).toEqual([]);
  expect(b.components[0].account_id).toBe(a.components[0].account_id);
  expect(b.components[0].refresh_path).toBe(a.components[0].refresh_path);
});
it.each([401,403,404])('normal scope refusal%s settles through the canonical error lifecycle', async status => {
  const { client, options } = fixture(); vi.mocked(apiRequest).mockRejectedValueOnce(new ApiError('Unavailable',status));
  await expect(client.fetchQuery(options)).rejects.toBeDefined();
  expect(client.getQueryData(options.queryKey)).toBeUndefined();
  expect(client.getQueryState(options.queryKey)?.error).toBeInstanceOf(ApiError);
});

it('does not let staggered Oceans constituents restart each other, but permits explicit recovery', async () => {
  vi.useFakeTimers();
  focusManager.setFocused(true);
  vi.setSystemTime(new Date('2026-11-01T07:51:10Z'));
  const { client, options } = fixture();
  let reads = 0;
  vi.mocked(apiRequest).mockImplementation(async () => {
    const result = body(2);
    result.components[0].stored.stale = true;
    result.components[1].stored.stale = ++reads > 1;
    result.components[0].stored.refresh_eta = '2026-11-01T07:51:08Z';
    if (reads > 1) result.components[1].stored.refresh_eta = '2026-11-01T07:51:08Z';
    return result;
  });
  const observer = new QueryObserver(client, options);
  stops.push(observer.subscribe(() => {}));
  await vi.advanceTimersByTimeAsync(1);
  expect(reads).toBe(1);
  await vi.advanceTimersByTimeAsync(30_000);
  expect(reads).toBe(2);
  await vi.advanceTimersByTimeAsync(170_000);
  const stopped = reads;
  expect(observer.getCurrentResult().data?.components).toHaveLength(2);
  expect(observer.getCurrentResult().error).toBeNull();
  await vi.advanceTimersByTimeAsync(700_000);
  expect(reads).toBe(stopped);
  await observer.refetch();
  expect(reads).toBe(stopped + 1);
  await vi.advanceTimersByTimeAsync(30_000);
  expect(reads).toBe(stopped + 2);
});

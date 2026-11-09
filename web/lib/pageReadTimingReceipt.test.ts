// @vitest-environment jsdom
import { webcrypto } from 'node:crypto';
import { QueryObserver } from '@tanstack/react-query';
import { waitFor } from '@testing-library/react';
import { afterEach, expect, it, vi } from 'vitest';
import { makeQueryClient } from './queryClient';
import { pageSectionQuery } from './pageSections';
import { __resetTransportForTests, criticalReadCount } from './transport';
import { pageReadPublicationBoundary } from './pageReadAuthority';
import { admitPageReadTiming, createPageReadTimingCollector, watchPageReadTiming } from './pageReadTimingReceipt';
const clients: ReturnType<typeof makeQueryClient>[] = [], stops: (() => void)[] = [];
afterEach(() => { stops.splice(0).forEach(stop => stop()); clients.splice(0).forEach(client => client.clear()); __resetTransportForTests(); vi.unstubAllGlobals(); vi.unstubAllEnvs(); });
const nonce = '11111111-1111-4111-8111-111111111111';
const credential = `header.${btoa(JSON.stringify({ iss: 'fixture', sub: 'private-actor', org_id: 'private-org' }))}.signature`;
const stored = { model: 'connector_detail', version: 1, computed_at: '2026-11-01T02:38:17Z', stale: false, refresh_eta: null, payload: { key: 'google', sources: [], large: 'private-payload' } };

it('flag absent installs no collector/getter and leaves the authority structural-sharing function intact', () => {
  vi.stubEnv('NEXT_PUBLIC_E2E_PAGE_READ_RECEIPTS', undefined);
  const client = makeQueryClient(); clients.push(client); watchPageReadTiming(client); admitPageReadTiming({}, stored);
  expect(window.__csPageReadTimingSnapshot).toBeUndefined();
  const a = pageSectionQuery('connector_detail', 'scope', { provider: 'google' }, () => credential);
  const b = pageSectionQuery('connector_detail', 'scope', { provider: 'google' }, () => credential);
  expect(a.structuralSharing).toBe(b.structuralSharing);
});

it('captures bounded metadata without awaiting a failed digest or trusting forged run capabilities', async () => {
  const c = createPageReadTimingCollector({ nonce, now: () => 0, digest: async () => { throw Error('digest offline'); }, limit: 2 });
  expect(c.dispatch({ id: 999 }, '/v1/connectors/google', 'GET', 'critical', 'private-actor')).toBe(1);
  c.admission({}, stored); // No actual registered Query/options: cannot fabricate admission.
  c.dispatch(undefined, '/v1/connectors/google', 'GET', 'critical', 'private-actor');
  c.dispatch(undefined, '/v1/connectors/google', 'GET', 'critical', 'private-actor');
  const s = await c.snapshot();
  expect(s.events).toHaveLength(2); expect(s.events.some(e => e.kind === 'admission')).toBe(false);
  expect(s.errors).toEqual(expect.arrayContaining(['unknown-run','overflow','scope-digest-failed']));
  expect(JSON.stringify(s)).not.toMatch(/private-actor|private-payload/);
});

it('real opt-in factory/client/transport wiring binds coalesced keys, guarded admission, refusal and withheld recovery', async () => {
  vi.stubEnv('NEXT_PUBLIC_E2E_PAGE_READ_RECEIPTS', '1');
  Object.defineProperty(window, '__csPageReadTimingNonce', { value: nonce, configurable: false, writable: false });
  vi.stubGlobal('crypto', webcrypto);
  let release!: (response: Response) => void;
  const fetch = vi.fn().mockImplementationOnce(() => new Promise<Response>(resolve => { release = resolve; }));
  vi.stubGlobal('fetch', fetch);
  const client = makeQueryClient(); clients.push(client);
  const a = pageSectionQuery('connector_detail', 'scope', { provider: 'google' }, () => credential, { queryKey: ['receipt','a'] });
  const b = pageSectionQuery('connector_detail', 'scope', { provider: 'google' }, () => credential, { queryKey: ['receipt','b'] });
  const oa = new QueryObserver(client, a), ob = new QueryObserver(client, b);
  stops.push(oa.subscribe(() => {}), ob.subscribe(() => {}));
  await waitFor(() => expect(fetch).toHaveBeenCalledTimes(1));
  release(new Response(JSON.stringify(stored), { status: 200 }));
  await waitFor(() => expect([oa.getCurrentResult().isSuccess,ob.getCurrentResult().isSuccess]).toEqual([true,true]));
  const snapshot = window.__csPageReadTimingSnapshot;
  if (!snapshot) throw Error('Opt-in getter absent');
  const first = await snapshot();
  expect(first.errors).toEqual([]);
  expect(first.events.filter(e => e.kind === 'dispatch')).toHaveLength(1);
  expect(first.events.some(e => e.kind === 'join')).toBe(true);
  const confirmations = first.events.filter(e => e.kind === 'cache-success' && !e.manual);
  expect(new Set(confirmations.map(e => e.query)).size).toBe(2);
  for (const confirmation of confirmations) {
    const admission = first.events.find(e => e.seq === confirmation.admission);
    expect(admission).toMatchObject({ kind: 'admission', query: confirmation.query, run: confirmation.run, attempt: 1 });
    expect(admission?.at).toBeLessThanOrEqual(confirmation.at);
  }
  expect(Object.getOwnPropertyDescriptor(window, '__csPageReadTimingSnapshot')).toMatchObject({ configurable: false, writable: false });
  expect(Object.isFrozen(snapshot)).toBe(true);
  expect(JSON.stringify(first)).not.toMatch(/private-actor|private-org|private-payload|signature|payload/);
  const before = first.events.length;
  const originalRun = first.events.find(e => e.kind === 'run')?.run;
  first.events[0].run = 9999;
  expect((await snapshot()).events.find(e => e.kind === 'run')?.run).toBe(originalRun); // Snapshot copies cannot append/mutate the private collector.
  admitPageReadTiming({}, stored); // Unauthorized hook/options cannot manufacture a guarded publication.
  expect((await snapshot()).events).toHaveLength(before);

  fetch.mockResolvedValueOnce(new Response(JSON.stringify({ error: 'forbidden' }), { status: 403 }));
  await expect(client.fetchQuery({ ...a, staleTime: 0 })).rejects.toBeDefined();
  await waitFor(() => expect(client.getQueryData(a.queryKey)).toBeUndefined());
  const refused = await snapshot();
  const refusedRun = refused.events.filter(e => e.kind === 'run').at(-1)?.run;
  expect(refused.events.some(e => e.kind === 'admission' && e.run === refusedRun)).toBe(false);

  fetch.mockResolvedValueOnce(new Response(JSON.stringify({ ...stored, computed_at: null, stale: true, refresh_eta: '2026-11-01T02:38:23Z' }), { status: 200 }));
  await client.fetchQuery({ ...a, staleTime: 0 });
  const pending = await snapshot();
  expect(pending.events.filter(e => e.kind === 'admission').at(-1)?.certificate).toMatchObject({ computedAt: null, stale: true });
  expect(JSON.stringify(pending)).not.toMatch(/private-actor|private-org|private-payload/);
  fetch.mockResolvedValueOnce(new Response(JSON.stringify({ ...stored, version: 999 }), { status: 200 }));
  await expect(client.fetchQuery({ ...a, staleTime: 0 })).rejects.toBeDefined();
  const invalid = await snapshot(), invalidRun = invalid.events.filter(e => e.kind === 'run').at(-1)?.run;
  expect(invalid.events.some(e => e.kind === 'admission' && e.run === invalidRun)).toBe(false);
  expect(invalid.errors).toEqual([]);
  const query = client.getQueryCache().find({ queryKey: a.queryKey, exact: true });
  if (!query || typeof query.options.structuralSharing !== 'function') throw Error('Actual guarded cache options missing');
  const guard = query.options.structuralSharing;
  const eventCount = invalid.events.length;
  expect(() => guard.call(query.options, undefined, stored)).toThrow();
  expect((await snapshot()).events).toHaveLength(eventCount);
  expect(pageReadPublicationBoundary().structuralSharing).not.toBe(guard); // Wrapper calls the unchanged guard, never replaces its authority.

  let verify!: (token: string) => void, tokenCalls = 0;
  const delayed = pageSectionQuery('connector_detail', 'scope', { provider: 'google' }, () => {
    if (++tokenCalls === 1) return credential;
    return new Promise<string>(resolve => { verify = resolve; });
  }, { queryKey: ['receipt','delayed-verification'] });
  fetch.mockResolvedValueOnce(new Response(JSON.stringify(stored), { status: 200 }));
  const unfinished = client.fetchQuery(delayed);
  // Attach rejection observation before releasing the validation barrier.
  const rejected = expect(unfinished).rejects.toBeDefined();
  await waitFor(() => expect(verify).toBeDefined());
  const held = await snapshot(), heldRun = held.events.filter(e => e.kind === 'run').at(-1)?.run;
  expect(held.events.some(e => e.kind === 'admission' && e.run === heldRun)).toBe(false);
  const otherCredential = `header.${btoa(JSON.stringify({ iss: 'fixture', sub: 'another-actor', org_id: 'private-org' }))}.signature`;
  verify(otherCredential);
  await rejected;
  const changedScope = await snapshot();
  expect(changedScope.events.some(e => e.kind === 'admission' && e.run === heldRun)).toBe(false);
  expect(changedScope.errors).toEqual([]);
  expect(fetch).toHaveBeenCalledTimes(5);
  stops.splice(0).forEach(stop => stop()); client.clear();
  await waitFor(() => expect(criticalReadCount()).toBe(0));
});

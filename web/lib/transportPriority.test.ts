import { afterEach, expect, it, vi } from "vitest";
import { guardedFetch, setPageLoadGate, __resetTransportForTests, __setTransportConfigForTests, __transportStatsForTests } from "./transport";

const tick = async () => { for (let i = 0; i < 20; i++) await Promise.resolve(); };
function wire() {
  const starts: { url: string; init: RequestInit; finish: () => void }[] = [];
  vi.stubGlobal("fetch", vi.fn((url: string, init: RequestInit) => new Promise<Response>((resolve, reject) => {
    const finish = () => resolve(new Response('{}'));
    starts.push({ url, init, finish });
    init.signal?.addEventListener('abort', () => reject(new DOMException('Aborted', 'AbortError')), { once: true });
  })));
  return starts;
}
afterEach(() => { __resetTransportForTests(); vi.unstubAllGlobals(); });

it('holds the three parallel warmers until the page registration gate and critical lifecycle settle', async () => {
  const starts = wire();
  const warm = [1, 2, 3].map((id) => guardedFetch(`/v1/conversations/${id}`, {}, { priority: 'background' }));
  await tick(); expect(starts).toHaveLength(0);
  const critical = guardedFetch('/v1/bootstrap', {}, { priority: 'critical' });
  await tick(); expect(starts).toHaveLength(1);
  setPageLoadGate('home', true);
  await tick(); expect(starts).toHaveLength(1);
  starts[0].finish(); await critical; await tick();
  expect(starts).toHaveLength(4);
  for (const row of starts.slice(1)) {
    expect(new Headers(row.init.headers).get('x-coralswarm-priority')).toBe('background');
    expect(row.init.priority).toBe('low'); row.finish();
  }
  await Promise.all(warm);
  expect(__transportStatsForTests()).toMatchObject({ inFlight: 0, queued: 0 });
});

it('yields in-flight background slots to late critical registration, then resumes without retry loss', async () => {
  const starts = wire(); __setTransportConfigForTests({ maxInFlight: 1, maxPerCallerInFlight: 1, maxAttempts: 1 });
  setPageLoadGate('home', true);
  const warm = guardedFetch('/v1/conversations/1', {}, { priority: 'background' });
  await tick(); expect(starts).toHaveLength(1);
  const critical = guardedFetch('/v1/home/metrics', {}, { priority: 'critical' });
  await tick(); expect(starts[0].init.signal?.aborted).toBe(true);
  expect(starts).toHaveLength(2); expect(starts[1].url).toContain('/home/metrics');
  starts[1].finish(); await critical; await tick(); expect(starts).toHaveLength(3);
  starts[2].finish(); await warm;
  expect(__transportStatsForTests()).toMatchObject({ inFlight: 0, queued: 0 });
});

it('promotes a queued coalesced warmer when a critical consumer joins, without duplicate GETs', async () => {
  const starts = wire();
  const a = guardedFetch('/v1/conversations/1', { headers: { Authorization: 'Bearer A' } }, { priority: 'background' });
  const b = guardedFetch('/v1/conversations/1', { headers: { Authorization: 'Bearer A' } }, { priority: 'critical' });
  await tick(); expect(starts).toHaveLength(1);
  expect(new Headers(starts[0].init.headers).get('x-coralswarm-priority')).toBe('critical');
  starts[0].finish(); await Promise.all([a, b]);
});

it('keeps priority critical during retry backoff, so background cannot slip between attempts', async () => {
  const starts = wire(); __setTransportConfigForTests({ retryBaseMs: 1, retryMaxMs: 1 });
  let first = true;
  const fetcher = vi.fn((url: string, init: RequestInit) => {
    if (first) { first = false; return Promise.resolve(new Response('{}', { status: 503 })); }
    return new Promise<Response>((resolve) => starts.push({ url, init, finish: () => resolve(new Response('{}')) }));
  });
  vi.stubGlobal('fetch', fetcher); setPageLoadGate('home', true);
  const critical = guardedFetch('/v1/home/metrics', {}, { priority: 'critical' });
  const warm = guardedFetch('/v1/conversations/1', {}, { priority: 'background' });
  await vi.waitFor(() => expect(starts).toHaveLength(1));
  expect(starts[0].url).toContain('/home/metrics'); starts[0].finish(); await critical; await tick();
  expect(starts).toHaveLength(2); starts[1].finish(); await warm;
});

it('cancels a held warmer and does not leak subscribers or queue tokens', async () => {
  const starts = wire(); const a = new AbortController();
  const warm = guardedFetch('/v1/conversations/1', { signal: a.signal }, { priority: 'background' });
  const failed = expect(warm).rejects.toMatchObject({ kind: 'aborted' });
  a.abort(); await failed; await tick(); setPageLoadGate('next', true); await tick();
  expect(starts).toHaveLength(0); expect(__transportStatsForTests()).toMatchObject({ inFlight: 0, queued: 0 });
});

it('never coalesces different credential or browser-cookie modes', async () => {
  const starts = wire();
  const calls = [
    guardedFetch('/v1/me', { headers: { Authorization: 'Bearer A' } }),
    guardedFetch('/v1/me', { headers: { Authorization: 'Bearer B' } }),
    guardedFetch('/v1/me', { headers: { Authorization: 'Bearer A' }, credentials: 'omit' }),
  ];
  await tick(); expect(starts).toHaveLength(3); starts.forEach((s) => s.finish()); await Promise.all(calls);
});

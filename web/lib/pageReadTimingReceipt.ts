import type { Query, QueryClient } from '@tanstack/react-query';

/** E2E evidence only. Nothing in this module authorizes data or a budget exemption. */
export const PAGE_READ_RECEIPT_LIMIT = 4096;
export interface ReceiptCertificate { model: string; version: number; computedAt: string | null; stale: boolean; refreshEta: string | null }
export interface ReceiptComponent { identity: string; anchorGraphId: string | null; refreshPath: string; certificate: ReceiptCertificate }
export interface TimingEvent {
  seq: number; at: number; kind: 'run' | 'dispatch' | 'response' | 'join' | 'admission' | 'cache-success' | 'cache-error' | 'removed';
  run?: number; query?: number; attempt?: number; admission?: number; parent?: number | null;
  model?: string; address?: string; method?: string; priority?: string; scopeDigest?: string; status?: number;
  certificate?: ReceiptCertificate; components?: ReceiptComponent[]; manual?: boolean;
}
export interface TimingSnapshot { version: 1; nonce: string; clock: 'browser-performance'; events: TimingEvent[]; errors: string[] }
export interface PageReadTimingRun { readonly id: number }
interface Run { address: string; handle: PageReadTimingRun; query: Query; promise: unknown; attempts: number[] }
interface Candidate { id: number; run: Run; value: WeakRef<object> }
export interface ReceiptSource { now: () => number; nonce: string; digest: (text: string) => Promise<string>; limit?: number }

/** Private collector methods are module-local capabilities, never window APIs.
 * Hash only credential identity, asynchronously. No payload copying/hashing. */
export function createPageReadTimingCollector(source: ReceiptSource) {
  const events: TimingEvent[] = [], errors = new Set<string>(), pending = new Set<Promise<void>>();
  const watched = new WeakSet<QueryClient>(), queries = new Set<WeakRef<Query>>();
  const known = new WeakSet<Query>(), queryIds = new WeakMap<Query, number>(), runs = new WeakMap<object, Run>();
  const current = new WeakMap<Query, Run>(), candidates = new WeakMap<Query, Candidate>();
  const confirmed = new WeakMap<Query, Candidate>(), responses = new WeakMap<Response, number>();
  let sequence = 0, nextQuery = 0, nextRun = 0, nextAttempt = 0;
  const limit = source.limit ?? PAGE_READ_RECEIPT_LIMIT;
  const error = (code: string) => { errors.add(code); };
  const append = (event: Omit<TimingEvent, 'seq' | 'at'>, at = source.now()) => {
    if (events.length >= limit) { error('overflow'); return undefined; }
    const row: TimingEvent = { ...event, seq: ++sequence, at }; events.push(row); return row;
  };
  const track = (query: Query) => {
    if (!known.has(query)) {
      if (nextQuery >= limit) { error('overflow'); return undefined; }
      known.add(query); queries.add(new WeakRef(query)); queryIds.set(query, ++nextQuery);
    }
    return queryIds.get(query);
  };
  const certificate = (value: unknown): ReceiptCertificate | undefined => {
    if (!value || typeof value !== 'object') return undefined;
    const b = value as Record<string, unknown>;
    if (typeof b.model !== 'string' || !Number.isInteger(b.version) || typeof b.stale !== 'boolean' ||
      !(b.computed_at === null || typeof b.computed_at === 'string') || !(b.refresh_eta === null || typeof b.refresh_eta === 'string')) return undefined;
    return { model: b.model, version: b.version as number, computedAt: b.computed_at, stale: b.stale, refreshEta: b.refresh_eta };
  };
  const projection = (value: unknown, run: Run): Pick<TimingEvent, 'certificate' | 'components'> | undefined => {
    if (run.address !== '/v1/load/oceans') {
      const cert = certificate(value); return cert ? { certificate: cert } : undefined;
    }
    const body = value as { components?: unknown };
    if (run.query.meta?.model !== 'oceans_index' || !Array.isArray(body.components) || body.components.length > 501) {
      error('invalid-component-certificate'); return;
    }
    const components: ReceiptComponent[] = [], identities = new Set<string>();
    const uuid = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
    for (const item of body.components) {
      const c = item as { account_id?: unknown; anchor_graph_id?: unknown; refresh_path?: unknown; stored?: unknown } | null;
      const cert = certificate(c?.stored);
      if (!c || typeof c.account_id !== 'string' || !uuid.test(c.account_id) || identities.has(c.account_id) ||
        !(c.anchor_graph_id === null || typeof c.anchor_graph_id === 'string' && uuid.test(c.anchor_graph_id)) ||
        c.refresh_path !== `/v1/load/oceans/component/refresh${c.anchor_graph_id ? `?graph=${c.anchor_graph_id}` : ''}` || cert?.model !== 'oceans_index') {
        error('invalid-component-certificate'); return;
      }
      identities.add(c.account_id);
      components.push({ identity: c.account_id, anchorGraphId: c.anchor_graph_id, refreshPath: c.refresh_path as string, certificate: cert });
    }
    return { components }; // No outer certificate; project ALL constituents, never payloads.
  };
  const runFor = (handle?: PageReadTimingRun) => {
    const run = handle && runs.get(handle); if (handle && !run) error('unknown-run'); return run;
  };
  const watch = (client: QueryClient) => {
    if (watched.has(client)) return; watched.add(client);
    client.getQueryCache().subscribe(event => {
      try {
      const observedAt = source.now();
      const query = event.query;
      if (query.meta?.pageRead !== true) return;
      const queryId = track(query);
      if (event.type === 'removed') { current.delete(query); candidates.delete(query); confirmed.delete(query); append({ kind: 'removed', query: queryId }); }
      if (event.type !== 'updated') return;
      if (event.action.type === 'error' || event.action.type === 'setState' && query.state.status === 'error') { confirmed.delete(query); append({ kind: 'cache-error', query: queryId, run: current.get(query)?.handle.id }); }
      if (event.action.type !== 'success') return;
      const candidate = candidates.get(query); candidates.delete(query);
      if (!candidate || !Object.is(candidate.value.deref(), query.state.data) || candidate.run.promise !== query.promise) return;
      append({ kind: 'cache-success', query: queryId, run: candidate.run.handle.id, admission: candidate.id, manual: event.action.manual === true }, observedAt);
      if (!event.action.manual) confirmed.set(query, candidate);
      } catch { error('observer-capture-failed'); }
    });
  };
  const begin = (client: QueryClient | undefined, key: readonly unknown[] | undefined, address: string): PageReadTimingRun | undefined => {
    if (!client || !watched.has(client) || !key) { error('unregistered-client'); return undefined; }
    const query = client.getQueryCache().find({ queryKey: key, exact: true });
    if (!query?.promise || nextRun >= limit) { error('unbound-run'); return undefined; }
    const handle = Object.freeze({ id: ++nextRun }), run: Run = { address, handle, query, promise: query.promise, attempts: [] };
    runs.set(handle, run); current.set(query, run);
    const prior = confirmed.get(query);
    append({ kind: 'run', query: track(query), run: handle.id, address, model: typeof query.meta?.model === 'string' ? query.meta.model : undefined,
      parent: prior && Object.is(prior.value.deref(), query.state.data) ? prior.id : null });
    return handle;
  };
  const dispatch = (handle: PageReadTimingRun | undefined, address: string, method: string, priority: string, scope: string) => {
    const run = runFor(handle); if (nextAttempt >= limit) { error('overflow'); return undefined; }
    const attempt = ++nextAttempt;
    const row = append({ kind: 'dispatch', attempt, run: run?.handle.id, address, method, priority });
    if (run) run.attempts.push(attempt);
    if (row) {
      const task = source.digest(`${source.nonce}\0${scope}`).then(digest => { row.scopeDigest = digest; }, () => error('scope-digest-failed'));
      pending.add(task); void task.finally(() => pending.delete(task));
    }
    return attempt;
  };
  const response = (attempt: number | undefined, value: Response) => {
    if (attempt === undefined) return; responses.set(value, attempt); append({ kind: 'response', attempt, status: value.status });
  };
  const join = (handle: PageReadTimingRun | undefined, value: Response) => {
    const run = runFor(handle); if (!run) return;
    const attempt = responses.get(value); if (attempt === undefined) { error('unbound-response'); return; }
    if (!run.attempts.includes(attempt)) { run.attempts.push(attempt); append({ kind: 'join', run: run.handle.id, attempt }); }
  };
  const admission = (options: object, value: unknown, admittedAt = source.now()) => {
    for (const ref of queries) {
      const query = ref.deref(); if (!query) { queries.delete(ref); continue; }
      if (query.options !== options) continue; // Observer selection is not cache admission.
      const run = current.get(query);
      if (!run || run.promise !== query.promise || !run.attempts.length || !value || typeof value !== 'object') return;
      const projected = projection(value, run); if (!projected) return;
      const row = append({ kind: 'admission', run: run.handle.id, query: queryIds.get(query), attempt: run.attempts.at(-1), ...projected }, admittedAt);
      if (row) candidates.set(query, { id: row.seq, run, value: new WeakRef(value) });
      return;
    }
  };
  return { watch, begin, dispatch, response, join, admission,
    clone(original: Response, copy: Response) { const attempt = responses.get(original); if (attempt !== undefined) responses.set(copy, attempt); },
    async snapshot(): Promise<TimingSnapshot> {
      const through = events.length;
      await Promise.all([...pending]);
      return { version: 1, nonce: source.nonce, clock: 'browser-performance', events: events.slice(0, through).map(row => ({ ...row, ...(row.certificate ? { certificate: { ...row.certificate } } : {}), ...(row.components ? { components: row.components.map(c => ({ ...c, certificate: { ...c.certificate } })) } : {}) })), errors: [...errors] };
    },
  };
}

type Collector = ReturnType<typeof createPageReadTimingCollector>;
declare global {
  interface Window {
    readonly __csPageReadTimingNonce?: string;
    readonly __csPageReadTimingSnapshot?: () => Promise<TimingSnapshot>;
  }
}
let collector: Collector | undefined;
function active(): Collector | undefined {
  if (process.env.NEXT_PUBLIC_E2E_PAGE_READ_RECEIPTS !== '1' || typeof window === 'undefined') return;
  const nonce = window.__csPageReadTimingNonce;
  const descriptor = Object.getOwnPropertyDescriptor(window, '__csPageReadTimingNonce');
  if (!descriptor || descriptor.configurable || descriptor.writable || !nonce || !/^[0-9a-f-]{36}$/i.test(nonce)) return;
  if (!collector) {
    collector = createPageReadTimingCollector({ nonce, now: () => performance.now(), digest: async text => {
      const bytes = await crypto.subtle.digest('SHA-256', new TextEncoder().encode(text));
      return Array.from(new Uint8Array(bytes), b => b.toString(16).padStart(2, '0')).join('');
    } });
    const owned = collector;
    Object.defineProperty(window, '__csPageReadTimingSnapshot', { value: Object.freeze(() => owned.snapshot()), configurable: false, writable: false });
  }
  return collector;
}
// Diagnostics must never throw into application authority/network/publication.
function safely<T>(read: () => T): T | undefined { try { return read(); } catch { return undefined; } }
export const watchPageReadTiming = (client: QueryClient) => safely(() => active()?.watch(client));
export const beginPageReadTiming = (client: QueryClient | undefined, key: readonly unknown[] | undefined, address: string) => safely(() => active()?.begin(client, key, address));
export const admitPageReadTiming = (options: object, value: unknown, admittedAt?: number) => safely(() => active()?.admission(options, value, admittedAt));
export const responsePageReadTiming = (attempt: number | undefined, value: Response) => safely(() => active()?.response(attempt, value));
export const joinPageReadTiming = (run: PageReadTimingRun | undefined, value: Response) => safely(() => active()?.join(run, value));
export const clonePageReadTiming = (original: Response, copy: Response) => safely(() => active()?.clone(original, copy));
export function dispatchPageReadTiming(run: PageReadTimingRun | undefined, url: string, method: string, priority: string, auth: string | null) {
  return safely(() => {
    const c = active(); if (!c) return;
    let scope = 'public';
    if (auth) {
      const encoded = auth.replace(/^Bearer /i, '').split('.')[1].replace(/-/g, '+').replace(/_/g, '/');
      const claims = JSON.parse(new TextDecoder().decode(Uint8Array.from(atob(encoded), ch => ch.charCodeAt(0))));
      if (typeof claims.iss !== 'string' || typeof claims.sub !== 'string') return;
      scope = JSON.stringify([claims.iss, claims.sub, claims.org_id ?? claims.o?.id ?? null]);
    }
    const path = new URL(url); path.searchParams.sort();
    return c.dispatch(run, `${path.pathname}${path.search}`, method, priority, scope);
  });
}

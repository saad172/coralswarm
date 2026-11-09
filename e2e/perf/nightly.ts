import { test, expect, type APIRequestContext, type Page, type Request, type Response } from '@playwright/test';
import { performance } from 'node:perf_hooks';
import { prepareSemanticTimingCapture } from './semantic-timing.mjs';
import { pageLoadClock } from '../lib/pageLoadClock.mjs';
import manifest from '../../web/lib/pageSections.generated.json' with { type: 'json' };
import { API_BASE } from '../stack/config.mjs';
import { isDeclaredPageRead, assertPageLoadBudget, type LoadRecord, type LoadBudget } from '../lib/pageLoadBudget';
import { createDocumentEvidence, type DocumentExpectation } from './auth-evidence.mjs';
import { pageRedirectDestination } from '../../web/lib/pageRedirectDestination';
import { verifiedActionRead } from './read-policy.mjs';
import { refreshPath } from '../lib/readModels';
import { parsePageReadTiming, assertSpeed } from './timings.mjs';
import { storedStates as validateStates, isRegisteredModelPath, assertStatesForPath } from './envelopes.mjs';

export interface ModelState { identity: string; model: string; version: number; computed_at: string | null; stale: boolean; refresh_eta: string | null }
export interface NetworkRecord {
  method: string; path: string; priority: string | null; scope: string;
  started: number; finished: number | null; wallStarted?: number; wallFinished?: number | null; status: number | null; failure: string | null;
  server_timing: string | null; total_ms: number | null; models: ModelState[];
  body_bytes: number | null; body_read_ms: number | null; node_json_parse_ms: number | null;
  browser_network_timing: ReturnType<Request['timing']> | null;
}
export function storedStates(value: unknown, path: string): ModelState[] { return validateStates(value, manifest, path); }

function credentialScope(request: Request): string {
  const token = request.headers().authorization?.replace(/^Bearer /i, '');
  if (!token) return 'public';
  const claims = JSON.parse(Buffer.from(token.split('.')[1], 'base64url').toString());
  if (!claims.sub || !claims.iss) throw new Error('missing verified browser credential identity');
  return JSON.stringify([claims.iss, claims.sub, claims.org_id ?? claims.o?.id ?? null]);
}
export type PageLoadDeclaration = Pick<LoadBudget, 'action' | 'expectedRefusals'> & { document?: DocumentExpectation };
export async function observeLoad(page: Page, path: string, discovery = false, declaration: PageLoadDeclaration = {}) {
  const captureUnavailable = (code: string) => ({ ok: false, errors: [code], matches: [], timings: [], trust: 'Capture annotation only; no budget exemption.' });
  let collectSemanticTiming: Awaited<ReturnType<typeof prepareSemanticTimingCapture>>;
  try { collectSemanticTiming = await prepareSemanticTimingCapture(page); }
  catch { collectSemanticTiming = async () => captureUnavailable('capture-setup-unavailable'); }
  const { document: expectedDocument, ...budgetDeclaration } = declaration;
  const documentObserver = createDocumentEvidence(page, { requested: path, expected: expectedDocument, resolveAlias: pageRedirectDestination, now: pageLoadClock.now });
  const records: NetworkRecord[] = [];
  const requests = new Map<Request, NetworkRecord>();
  const bodies: Promise<void>[] = [];
  const errors: string[] = [];
  const origin = new URL(API_BASE).origin;
  const onRequest = (request: Request) => {
    const url = new URL(request.url()); if (url.origin !== origin) return;
    url.searchParams.sort();
    let scope: string;
    try { scope = credentialScope(request); } catch { scope = 'invalid'; errors.push('actual browser API credential scope is malformed'); }
    const row: NetworkRecord = { method: request.method(), path: `${url.pathname}${url.search}`,
      priority: request.headers()['x-coralswarm-priority'] ?? null, scope,
      started: pageLoadClock.now(), finished: null, wallStarted: pageLoadClock.wallNow(), wallFinished: null, status: null, failure: null, server_timing: null,
      total_ms: null, models: [], body_bytes: null, body_read_ms: null, node_json_parse_ms: null, browser_network_timing: null };
    records.push(row); requests.set(request, row);
  };
  const onFinished = (request: Request) => {
    const row = requests.get(request); if (!row) return;
    row.finished = pageLoadClock.now(); row.wallFinished = pageLoadClock.wallNow(); row.failure = request.failure()?.errorText ?? null;
    row.browser_network_timing = request.timing();
  };
  const onResponse = (response: Response) => {
    const row = requests.get(response.request()); if (!row) return;
    row.status = response.status(); row.server_timing = response.headers()['server-timing'] ?? null;
    if (row.status >= 500 || response.headers()['x-coralswarm-admission']) {
      errors.push(`${row.method} ${row.path}: HTTP ${row.status}, admission=${response.headers()['x-coralswarm-admission'] ?? 'none'}`);
    }
    if (row.method !== 'GET') return;
    bodies.push((async () => {
      const expected = isDeclaredPageRead(new URL(response.url()).pathname);
      // Every actual mount GET must be declared; an absent class or duration
      // on such a response is a failure even when it would yield zero samples.
      if (expected) row.total_ms = parsePageReadTiming(row.server_timing, true);
      const start = performance.now(); const buffer = await response.body();
      row.body_read_ms = performance.now() - start; row.body_bytes = buffer.length;
      const parse = performance.now();
      const value = JSON.parse(buffer.toString('utf8'));
      row.node_json_parse_ms = performance.now() - parse;
      row.models = storedStates(value, row.path);
      if (isRegisteredModelPath(new URL(response.url()).pathname, manifest) && (row.status !== 200 || row.models.length === 0)) {
        throw new Error('registered model request did not return its real successful Stored envelope');
      }
    })().catch((error) => { errors.push(`${row.path}: ${(error as Error).message}`); }));
  };
  page.on('request', onRequest); page.on('requestfinished', onFinished); page.on('requestfailed', onFinished); page.on('response', onResponse);
  let ledger: LoadRecord[] = [];
  const attachmentStart = test.info().attachments.length;
  let budgetError: string | null = null;
  try { ledger = await assertPageLoadBudget(page, path, { ...budgetDeclaration, maxRequests: 6, maxDepth: 1, mode: 'owed' }); }
  catch (error) { budgetError = (error as Error).message; }
  finally {
    await Promise.all(bodies);
    page.off('request', onRequest); page.off('requestfinished', onFinished); page.off('requestfailed', onFinished); page.off('response', onResponse);
  }
  if (!ledger.length) {
    const complete = test.info().attachments.slice(attachmentStart).find((a) => a.name.startsWith('complete-attempts-') && a.body);
    if (complete?.body) ledger = JSON.parse(complete.body.toString('utf8'));
  }
  for (const [index, row] of records.entries()) {
    if (row.method !== 'GET' || isDeclaredPageRead(new URL(row.path, API_BASE).pathname)) continue;
    const occurrence = records.slice(0, index).filter((prior) => prior.method === row.method && prior.path === row.path && prior.scope === row.scope).length;
    if (!verifiedActionRead(row, occurrence, ledger)) errors.push(`undeclared or uncertified browser GET ${row.path}`);
  }
  // Browser/transport retries are real starts. They cannot be hidden behind a
  // later 200, either during discovery, refresh, or the twenty measured loads.
  // Background work has the same transport evidence requirement. Without an
  // explicit scheduler-cancellation receipt, an abort is also a failed attempt.
  const failed = records.filter((row) => row.failure);
  errors.push(...failed.map((row) => `${row.method} ${row.path}: ${row.failure}`));
  if (budgetError) errors.push(budgetError);
  const document = await documentObserver.complete({ records, ledger, errors });
  if (!document.ok) errors.push(...document.reasons.map((reason) => `${document.kind}: ${reason}`));
  let semanticTiming: Awaited<ReturnType<NonNullable<typeof collectSemanticTiming>>> | undefined;
  if (collectSemanticTiming) {
    try { semanticTiming = await collectSemanticTiming(records, ledger); }
    catch { semanticTiming = captureUnavailable('capture-read-unavailable'); }
    try { await test.info().attach('semantic-timing-capture', { body: Buffer.from(JSON.stringify(semanticTiming, null, 2)), contentType: 'application/json' }); }
    catch { semanticTiming = { ...semanticTiming, ok: false, errors: [...semanticTiming.errors, 'capture-attachment-unavailable'] }; }
  }
  return { discovery, path, declaration: budgetDeclaration, document, ...(semanticTiming ? { semanticTiming } : {}), records, ledger, initial: ledger.filter((r) => r.phase === 'initial' && r.ledger !== 'owed-refresh'),
    callbackAction: ledger.filter((r) => r.phase === 'callback-action'),
    owed: ledger.filter((r) => r.ledger === 'owed-refresh'), errors,
    timing_boundary: 'Server-Timing total is handler construction; body read and Node JSON parse are separate observer costs, not browser render time' };
}

export type LoadObservation = Awaited<ReturnType<typeof observeLoad>>;
export interface DiscoveredRead { path: string; scope: string; states: ModelState[]; refreshPaths?: string[] }
export function discoveredReads(observation: LoadObservation): DiscoveredRead[] {
  const reads = new Map<string, DiscoveredRead>();
  for (const row of observation.records.filter((r) => r.method === 'GET' && r.models.length)) {
    const components = row.models.some((state) => manifest[state.model as keyof typeof manifest]?.path.endsWith('/component'));
    const certificates = (observation.ledger ?? []).filter((entry) => {
      const url = new URL(entry.address ?? entry.path, 'https://ledger.invalid'); url.searchParams.sort();
      return `${url.pathname}${url.search}` === row.path && entry.identity.slice(0, entry.identity.lastIndexOf('|')) === row.scope;
    }).at(-1)?.certificates ?? [];
    const refreshPaths = components ? [...new Set(certificates.map((certificate) => certificate.refreshPath).filter((value): value is string => !!value))] : undefined;
    reads.set(`${row.scope}|${row.path}`, { path: row.path, scope: row.scope, states: row.models, refreshPaths });
  }
  return [...reads.values()];
}
export async function refreshDiscovered(request: APIRequestContext, reads: DiscoveredRead[], token: () => Promise<string>) {
  const started = pageLoadClock.now(); const deadline = started + 120_000;
  const ledger: { method: string; path: string; status: number | null; at: number; started: number;
    finished: number | null; wallStarted?: number; wallFinished?: number | null; failure?: string; states?: ModelState[] }[] = [];
  const call = async (method: 'POST' | 'GET', path: string) => {
    const remaining = deadline - pageLoadClock.now(); if (remaining <= 0) throw new Error('registered worker refresh exceeded 120s');
    const jwt = await token();
    const scope = jwt ? (() => {
      const claims = JSON.parse(Buffer.from(jwt.split('.')[1], 'base64url').toString());
      return JSON.stringify([claims.iss, claims.sub, claims.org_id ?? claims.o?.id ?? null]);
    })() : 'public';
    if (reads.some((read) => read.scope !== scope)) throw new Error('refresh credential scope differs from the actual discovered browser scope');
    const afterToken = deadline - pageLoadClock.now(); if (afterToken <= 0) throw new Error('registered worker refresh exceeded 120s');
    const entry: (typeof ledger)[number] = { method, path, status: null, at: pageLoadClock.wallNow(), started: pageLoadClock.now(), finished: null, wallStarted: pageLoadClock.wallNow(), wallFinished: null };
    ledger.push(entry);
    try {
      const response = await request.fetch(`${API_BASE}${path}`, { method, headers: jwt ? { authorization: `Bearer ${jwt}` } : {}, timeout: Math.min(10_000, afterToken), maxRetries: 0, maxRedirects: 0 });
      entry.status = response.status(); entry.at = pageLoadClock.wallNow();
      if (response.status() >= 500 || response.headers()['x-coralswarm-admission']) throw new Error(`refresh ${method} ${path}: HTTP ${response.status()} or admission failure`);
      return { response, entry };
    } catch (error) {
      entry.failure = (error as Error).message; throw error;
    } finally { entry.finished = pageLoadClock.now(); entry.wallFinished = pageLoadClock.wallNow(); }
  };
  try {
    for (const read of reads) {
      const paths = read.refreshPaths ?? [refreshPath(read.path)];
      if (!paths.length) throw new Error(`missing validated component refresh addresses: ${read.path}`);
      for (const path of paths) {
        const url = new URL(path, API_BASE);
        if (url.origin !== new URL(API_BASE).origin || !url.pathname.endsWith('/refresh')) throw new Error('invalid registered refresh address');
        assertStatesForPath(read.states, manifest, `${url.pathname.slice(0, -'/refresh'.length)}${url.search}`);
        expect((await call('POST', path)).response.status(), `refresh ${path}`).toBe(202);
      }
    }
    const pending = new Map(reads.map((read) => [read.path, read]));
    while (pending.size) {
      if (pageLoadClock.now() >= deadline) throw new Error('registered worker refresh exceeded 120s');
      for (const [path, read] of pending) {
        const { response, entry } = await call('GET', path);
        expect(response.status(), path).toBe(200);
        parsePageReadTiming(response.headers()['server-timing'], true);
        const states = storedStates(await response.json(), path);
        Object.assign(entry, { states });
        if (states.length !== read.states.length || states.some((state) => !read.states.some((old) => old.identity === state.identity && old.model === state.model))) {
          throw new Error(`refresh changed the discovered authorized component set: ${path}`);
        }
        if (states.every((state) => state.computed_at !== null && !state.stale && state.refresh_eta === null
          && state.computed_at !== read.states.find((old) => old.identity === state.identity && old.model === state.model)!.computed_at)) pending.delete(path);
      }
      if (pending.size) await new Promise((resolve) => setTimeout(resolve, Math.min(2_000, Math.max(0, deadline - pageLoadClock.now()))));
    }
    return { started, elapsed_ms: pageLoadClock.now() - started, ledger, errors: [] as string[] };
  } catch (error) { return { started, elapsed_ms: pageLoadClock.now() - started, ledger, errors: [(error as Error).message] }; }
}
export function routeStatistics(loads: LoadObservation[], discovery: LoadObservation) {
  expect(loads).toHaveLength(20);
  expect(discovery.errors, `${discovery.path}: discovery failed`).toEqual([]);
  const expected = new Set(discovery.records.filter((r) => r.method === 'GET' && r.priority === 'critical').map((r) => r.path));
  const expectedModels = discoveredReads(discovery);
  const byRead: Record<string, number[]> = {}; const byModel: Record<string, number[]> = {};
  const all: number[] = [];
  for (const load of [discovery, ...loads]) {
    for (const row of load.records) assertStatesForPath(row.models, manifest, row.path);
  }
  for (const load of loads) {
    expect(load.errors, load.path).toEqual([]);
    for (const path of expected) {
      expect(load.records.some((r) => r.path === path && r.total_ms !== null), `missing expected class=page_read timing sample for ${path}`).toBe(true);
    }
    for (const read of expectedModels) {
      for (const state of read.states) {
        expect(load.records.some((row) => row.path === read.path && row.scope === read.scope && row.total_ms !== null
          && row.models.some((actual) => actual.model === state.model && actual.identity === state.identity)),
        `missing expected scoped model sample ${state.model} at ${read.path}`).toBe(true);
        const final = load.records.filter((row) => row.path === read.path && row.scope === read.scope)
          .flatMap((row) => row.models).filter((actual) => actual.model === state.model && actual.identity === state.identity).at(-1);
        expect(final && final.computed_at !== null && !final.stale && final.refresh_eta === null,
          `required model never reached authoritative settled completion: ${state.model} at ${read.path}`).toBe(true);
      }
    }
    for (const row of load.records) {
      if (row.total_ms === null) continue;
      all.push(row.total_ms); (byRead[row.path] ??= []).push(row.total_ms);
      for (const model of new Set(row.models.map((state) => state.model))) (byModel[model] ??= []).push(row.total_ms);
    }
  }
  // Legitimate public/static pages can make no API request; report this
  // explicitly. They receive no invented zero latency or percentile pass.
  if (!expected.size && !all.length) return { status: 'no-page-read-samples', count: 0, per_read: {}, per_model: {} };
  return { status: 'sampled', ...assertSpeed(all),
    per_read: Object.fromEntries(Object.entries(byRead).map(([key, values]) => [key, assertSpeed(values)])),
    per_model: Object.fromEntries(Object.entries(byModel).map(([key, values]) => [key, assertSpeed(values)])) };
}

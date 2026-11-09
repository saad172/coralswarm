import { expect, test, type Page, type Request } from "@playwright/test";
import { prepareSemanticTimingCapture, applyConfirmedSemanticTiming, criticalReadPredecessors } from "../perf/semantic-timing.mjs";
import { pageLoadClock } from "./pageLoadClock.mjs";
import manifest from "../../web/lib/pageSections.generated.json" with { type: "json" };
import { SHELL_LOAD_PATHS } from "../../web/lib/pageShellPaths";
import { refreshLimits, refreshRecoveryMs } from "../../web/lib/pageRefreshPolicy";
import { StoredReadLedger, ConnectorCallbackPhase, REFRESH_POLL_MS, type CertificateEvidence, type OwedEvidence } from "../../web/lib/pageLoadEvidence";
export { REFRESH_POLL_MS } from "../../web/lib/pageLoadEvidence";
import { API_BASE } from "../stack/config.mjs";
import { storedTimestamp } from "../../web/lib/pageSectionWire";

import { CONNECTOR_PICKER_PATH } from "../../web/lib/connectorActionPolicy";
import { DEFAULT_TRANSPORT_CONFIG } from "../../web/lib/transport";

export interface LoadBudget { maxRequests: number; maxDepth: number; mode?: "healthy" | "owed" | "degraded"; action?: { kind: "connector-callback"; reason: string; destinations: readonly string[]; picker?: { path: string; reason: string } }; expectedRefusals?: readonly { path: string; status: number; reason: string }[] }
export const DEFAULT_BUDGET: LoadBudget = { maxRequests: 6, maxDepth: 1 };
export interface LoadRecord {
  address: string; page: string; initialWave: string;
  phase: "initial" | "callback-action"; actionEvidence?: { acknowledgedBy: string; deadline: number; picker?: boolean; source?: string };
  certificates?: CertificateEvidence[]; owedFrom?: OwedEvidence[];
  method: string; path: string; priority: string | null;
  /** Epoch-anchored monotonic times; wall fields are UTC forensics only. */
  started: number; finished: number | null; wallStarted?: number; wallFinished?: number | null; status: number | null; failure: string | null;
  ledger: "initial" | "owed-refresh" | "retry";
  nodeLedger?: "initial" | "owed-refresh" | "retry";
  confirmedPollSpacing?: { source: string; nonce: string; run: number; query: number; attempt: number; parentAdmission: number; confirmation: number; networkIndex: number; previousNetworkIndex: number; elapsedMs: number; originalNodeLedger: "initial" | "owed-refresh" | "retry" };
  identity: string; model: keyof typeof manifest | null; computedAt: string | null; refreshEta: string | null;
  excluded: "preflight" | "telemetry" | "timezone" | "active-graph" | null;
}
const apiOrigin = new URL(API_BASE).origin;
function canonicalRequest(request: Request): string {
  const url = new URL(request.url()); url.searchParams.sort();
  const token = request.headers().authorization?.replace(/^Bearer /i, '');
  let identity = 'public';
  if (token) {
    const c = JSON.parse(Buffer.from(token.split('.')[1], 'base64url').toString()) as { sub: string; iss: string; org_id?: string; o?: { id?: string } };
    identity = JSON.stringify([c.iss, c.sub, c.org_id ?? c.o?.id ?? null]);
  }
  return `${identity}|${url.pathname}${url.search}`;
}

function matches(path: string, template: string): boolean {
  const pattern = template.split(/(\{[^}]+\})/).map((part) => part.startsWith('{') ? '[^/]+' : part.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')).join('');
  return new RegExp(`^${pattern}$`).test(path);
}
export function isDeclaredPageRead(path: string): boolean {
  return [...Object.values(manifest).map((entry) => `/v1${entry.path}`), ...SHELL_LOAD_PATHS].some((template) => matches(path, template));
}
function exclusion(request: Request, path: string): LoadRecord['excluded'] {
  const method = request.method();
  if (method === 'OPTIONS' && request.headers()['access-control-request-method']) return 'preflight';
  if (method === 'POST' && path === '/v1/telemetry/client') return 'telemetry';
  if (method === 'PUT' && path === '/v1/me/timezone') return 'timezone';
  if (method === 'PUT' && path === '/v1/me/active-graph') return 'active-graph';
  return null;
}

export interface HealthyFixtureScope { account: string; actor: string; userId: string; orgId: string | null }
interface HealthyFixtureApi {
  call(method: 'GET' | 'POST', path: string): Promise<{ status(): number; json(): Promise<unknown> }>;
}
export interface HealthyFixtureSetup {
  scope: HealthyFixtureScope;
  sources: { identity: string; address: string; started: number; certificates: CertificateEvidence[] }[];
  attempts: { method: 'GET' | 'POST'; address: string; source: string | null; started: number; finished: number | null; wallStarted?: number; wallFinished?: number | null;
    status: number | null; certificates?: CertificateEvidence[]; refreshEta?: string; failure?: string }[];
  completedAt?: number; wallCompletedAt?: number;
  failure?: string;
}

/** Explicit setup ONLY between the ordinary initial/healthy route measurements.
 * Never call in cold DELETE/barrier or withheld scenarios. Refreshes go through
 * the fixture actor's real registered workers; setup has its own receipt and
 * never changes either browser ledger, priorities, or budget assertions. */
export async function prepareHealthyPageLoadFixture(
  fixture: HealthyFixtureScope & { api: HealthyFixtureApi }, initial: readonly LoadRecord[], clock = pageLoadClock,
): Promise<HealthyFixtureSetup> {
  const receipt: HealthyFixtureSetup = { scope: { account: fixture.account, actor: fixture.actor,
    userId: fixture.userId, orgId: fixture.orgId }, sources: [], attempts: [] };
  const canonical = (address: string) => {
    const url = new URL(address, 'https://fixture.invalid');
    if (!address.startsWith('/') || url.origin !== 'https://fixture.invalid') throw new Error('Healthy setup requires a local registered address');
    url.searchParams.sort(); return `${url.pathname}${url.search}`;
  };
  const request = async (method: 'GET' | 'POST', address: string, source: string | null) => {
    const attempt: HealthyFixtureSetup['attempts'][number] = { method, address, source, started: clock.now(), finished: null, wallStarted: clock.wallNow(), wallFinished: null, status: null };
    receipt.attempts.push(attempt);
    try {
      const response = await fixture.api.call(method, address);
      attempt.status = response.status();
      if (attempt.status !== (method === 'POST' ? 202 : 200)) throw new Error(`Healthy setup: ${method} ${address} refused with ${attempt.status}`);
      const body = await response.json();
      return { body, attempt };
    } catch (error) {
      attempt.failure = error instanceof Error ? error.message : String(error); throw error;
    } finally { attempt.finished = clock.now(); attempt.wallFinished = clock.wallNow(); }
  };
  // Await every request even on failure so the attached setup receipt is complete.
  const batch = async <T>(tasks: Promise<T>[]): Promise<T[]> => {
    const results = await Promise.allSettled(tasks);
    const failure = results.find((r) => r.status === 'rejected');
    if (failure?.status === 'rejected') throw failure.reason;
    return results.map((r) => (r as PromiseFulfilledResult<T>).value);
  };
  try {
    const latest = new Map<string, LoadRecord>();
    for (const row of initial) if (!row.excluded && row.method === 'GET') latest.set(row.identity, row);
    const sources = [...latest.values()].filter((row) => row.status === 200 && row.finished !== null && !row.failure && row.certificates?.length);
    for (const row of sources) {
      let credential: unknown;
      try { credential = JSON.parse(row.identity.slice(0, row.identity.lastIndexOf('|'))); } catch { /* fail closed below */ }
      if (!Array.isArray(credential) || credential[1] !== fixture.userId || credential[2] !== fixture.orgId)
        throw new Error('Healthy setup: browser credential scope differs from fixture actor');
      receipt.sources.push({ identity: row.identity, address: canonical(row.address), started: row.started,
        certificates: structuredClone(row.certificates!) });
    }
    if (!sources.length) { receipt.completedAt = clock.now(); receipt.wallCompletedAt = clock.wallNow(); return receipt; }
    const { body: me } = await request('GET', '/v1/me', null);
    if (!me || typeof me !== 'object' || !('account_id' in me) || !('user_id' in me) ||
        me.account_id !== fixture.account || me.user_id !== fixture.actor)
      throw new Error('Healthy setup: API account/actor scope differs from owned fixture');
    const read = async (source: HealthyFixtureSetup['sources'][number]) => {
      const { body, attempt } = await request('GET', source.address, source.identity);
      // Reuse exactly the browser ledger's route-selected certificate validator,
      // including real fan-in identities/refresh paths and payload checks.
      const row: LoadRecord = { ...sources.find((r) => r.identity === source.identity)!, address: source.address,
        started: attempt.started, finished: attempt.finished, status: attempt.status,
        certificates: undefined, owedFrom: undefined, model: null, computedAt: null, refreshEta: null };
      const ledger = new StoredReadLedger(); ledger.start(row); ledger.response(row, body);
      attempt.certificates = row.certificates;
      const sameConstituents = (row.certificates ?? []).length === source.certificates.length &&
        source.certificates.every((prior) => row.certificates?.some((c) => c.identity === prior.identity && c.model === prior.model && c.version === prior.version));
      if (!sameConstituents) throw new Error(`Healthy setup: invalid certificate or changed constituent scope at ${source.address}`);
      return row.certificates!;
    };
    const baseline = await batch(receipt.sources.map(read));
    const refreshes = new Map<string, string>();
    for (const [index, certificates] of baseline.entries()) {
      const source = receipt.sources[index];
      for (const certificate of certificates) {
        const url = new URL(certificate.refreshPath ?? source.address, 'https://fixture.invalid');
        if (!certificate.refreshPath) url.pathname += '/refresh';
        if (!matches(url.pathname, `/v1${manifest[certificate.model].path}/refresh`))
          throw new Error(`Healthy setup: refresh path does not match registered ${certificate.model}`);
        refreshes.set(canonical(`${url.pathname}${url.search}`), source.identity);
      }
    }
    const deadline = clock.now() + Math.max(...baseline.flat().map((c) => refreshRecoveryMs(refreshLimits(c.model))));
    await batch([...refreshes].map(async ([address, source]) => {
      const { body, attempt } = await request('POST', address, source);
      if (!body || typeof body !== 'object' || !('refresh_eta' in body) || typeof body.refresh_eta !== 'string' || !storedTimestamp(body.refresh_eta))
        throw new Error(`Healthy setup: invalid refresh acknowledgement at ${address}`);
      attempt.refreshEta = body.refresh_eta;
    }));
    await expect.poll(async () => {
      if (clock.now() >= deadline) throw new Error('Healthy setup: registered worker recovery deadline expired');
      const current = await batch(receipt.sources.map(read));
      return current.every((certificates, index) => certificates.every((certificate) => {
        const previous = baseline[index].find((c) => c.identity === certificate.identity)!;
        return certificate.computedAt !== null && !certificate.stale && certificate.refreshEta === null &&
          (previous.computedAt === null || Date.parse(certificate.computedAt) > Date.parse(previous.computedAt));
      }));
    }, { timeout: Math.max(1, deadline - clock.now()), intervals: [REFRESH_POLL_MS],
      message: 'Healthy fixture must have a new verified worker answer for every discovered Stored constituent' }).toBe(true);
    receipt.completedAt = clock.now(); receipt.wallCompletedAt = clock.wallNow();
    return receipt;
  } catch (error) {
    receipt.failure = error instanceof Error ? error.message : String(error);
    throw error;
  } finally {
    await test.info().attach('healthy-fixture-setup', { body: Buffer.from(JSON.stringify(receipt, null, 2)), contentType: 'application/json' });
  }
}

/** Starts/retries are recorded at the actual browser boundary. No response
 * interception, guessed completion delay or broad URL exclusions. The marker
 * must represent resolved Stored answers (or real errors), followed by 1s of
 * quiet over all API requests, including background and progress reads. */
export async function assertPageLoadBudget(page: Page, path: string, budget = DEFAULT_BUDGET, clock = pageLoadClock): Promise<LoadRecord[]> {
  const records: LoadRecord[] = [];
  // Optional E2E capture cannot suppress original recorder/auth failures.
  let semanticCapture: Awaited<ReturnType<NonNullable<Awaited<ReturnType<typeof prepareSemanticTimingCapture>>>>> | undefined;
  let collectSemanticTiming: Awaited<ReturnType<typeof prepareSemanticTimingCapture>>;
  try { collectSemanticTiming = await prepareSemanticTimingCapture(page); } catch { /* Original Node predicate remains authoritative. */ }
  const pending = new Map<Request, LoadRecord>();
  let lastActivity = clock.now();
  const ledger = new StoredReadLedger();
  const callback = new ConnectorCallbackPhase(path, budget.action?.kind === "connector-callback" && budget.action.reason.length > 0, budget.action?.picker?.path === CONNECTOR_PICKER_PATH && budget.action.picker.reason.length > 0);
  const bodies = new Set<Promise<void>>();
  const start = (request: Request) => {
    const url = new URL(request.url());
    if (url.origin !== apiOrigin) return; // Clerk is a separate origin.
    const currentPage = new URL(page.url());
    const row: LoadRecord = { page: `${currentPage.pathname}${currentPage.search}`, initialWave: "entry", method: request.method(), path: url.pathname, address: `${url.pathname}${url.search}`,
      priority: request.headers()['x-coralswarm-priority'] ?? null,
      started: clock.now(), finished: null, wallStarted: clock.wallNow(), wallFinished: null, status: null, failure: null, excluded: exclusion(request, url.pathname),
      phase: 'initial', ledger: 'initial', identity: canonicalRequest(request), model: null, computedAt: null, refreshEta: null };
    ledger.start(row); callback.start(row);
    records.push(row); pending.set(request, row); if (!row.excluded) lastActivity = clock.now();
  };
  const finish = (request: Request) => {
    const row = pending.get(request); if (!row) return;
    row.finished = clock.now(); row.wallFinished = clock.wallNow(); row.failure = request.failure()?.errorText ?? null; pending.delete(request); if (!row.excluded) lastActivity = clock.now();
  };
  const response = (res: import('@playwright/test').Response) => {
    const row = pending.get(res.request()); if (!row) return;
    row.status = res.status();
    if (row.excluded) return;
    if (!res.ok() || !['GET', 'POST'].includes(row.method)) { ledger.response(row, null); return; }
    const body = (async () => {
      const value: unknown = await res.json().catch(() => null);
      if (row.method === 'POST') callback.acknowledge(row, value);
      else { ledger.response(row, value); callback.observe(row, value); }
    })();
    bodies.add(body); void body.finally(() => bodies.delete(body));
  };
  page.on('request', start); page.on('requestfinished', finish); page.on('requestfailed', finish); page.on('response', response);
  try {
    await page.goto(path);
    await expect(page.locator('[data-page-settled="true"]')).toBeAttached({ timeout: 120_000 });
    await expect.poll(async () => {
      const loaded = await page.locator('[data-page-settled="true"]').count();
      return loaded > 0 && [...pending.values()].every((row) => row.excluded !== null) && bodies.size === 0 && clock.now() - lastActivity >= 1_000;
    }, { timeout: 120_000, intervals: [50, 100] }).toBe(true);
    for (const row of records) {
      const destination = callback.destination(row, row.page, budget.action?.destinations ?? []);
      if (destination) row.initialWave = `callback-destination:${destination}`;
      const action = callback.classify(row);
      if (action) { row.phase = 'callback-action'; row.actionEvidence = action; }
    }
    await test.info().attach(`node-load-ledger-${path.replace(/[^a-z0-9]/gi, "_")}`, { body: Buffer.from(JSON.stringify(records, null, 2)), contentType: "application/json" });
    if (collectSemanticTiming) {
      // Both projections originate at this recorder's actual Node boundary.
      // The full validator reuses its already-verified real body certificates.
      const timingRecords = records.map(row => ({ ...row, path: row.address,
        scope: row.identity.slice(0, row.identity.lastIndexOf('|')) }));
      try {
        const capture = await collectSemanticTiming(timingRecords, records, ledger);
        semanticCapture = capture;
        const correction = applyConfirmedSemanticTiming(capture, timingRecords, records, ledger);
        await test.info().attach('semantic-poll-classification', { body: Buffer.from(JSON.stringify({ ok: capture.ok, errors: capture.errors, ...correction }, null, 2)), contentType: 'application/json' });
      } catch { /* Capture failures supply no proof; any applied proof remains explicit on the rows. */ }
    }
    await test.info().attach(`load-ledger-${path.replace(/[^a-z0-9]/gi, "_")}`, { body: Buffer.from(JSON.stringify(records, null, 2)), contentType: "application/json" });
    const expectedRefusal = records.some((row) => budget.expectedRefusals?.some((rule) => matches(row.path, rule.path) && row.status === rule.status));
    if ((budget.mode ?? "healthy") !== "degraded" && !expectedRefusal) await expect(page.locator('[data-page-loaded="true"]')).toBeAttached();
    if (expectedRefusal) await expect(page.locator('[data-page-failed="true"][data-page-loaded="false"]')).toBeAttached();
    for (const excluded of ["timezone", "active-graph"] as const) {
      expect(records.filter((row) => row.excluded === excluded).length, `${path}: repeated excluded ${excluded} PUT`).toBeLessThanOrEqual(1);
    }
    const actual = records.filter((row) => row.excluded === null);
    for (const row of actual) {
      if (row.status !== null && row.status >= 400 && budget.mode !== "degraded") {
        expect(budget.expectedRefusals?.some((rule) => rule.reason.length > 0 && matches(row.path, rule.path) && row.status === rule.status),
          `${path}: unexpected API refusal ${row.status} ${row.path}`).toBe(true);
      }
      expect(row.priority, `${path}: request has no declared priority: ${row.method} ${row.path}`).toMatch(/^(critical|normal|background)$/);
      if (row.method === 'GET') {
        if (row.actionEvidence?.picker) expect(row.priority).toBe('normal');
        else expect(row.priority, `${path}: mount read was relabelled: ${row.path}`).not.toBe('normal');
        expect(row.actionEvidence?.picker === true || isDeclaredPageRead(row.path), `${path}: undeclared load read ${row.path}`).toBe(true);
      }
      if (row.priority === 'critical') expect(isDeclaredPageRead(row.path), `${path}: undeclared critical ${row.path}`).toBe(true);
    }
    const critical = actual.filter((row) => row.priority === 'critical');
    const actionReads = critical.filter((row) => row.phase === 'callback-action');
    const initial = critical.filter((row) => row.phase === 'initial' && row.ledger !== 'owed-refresh');
    const owed = critical.filter((row) => row.ledger === 'owed-refresh');
    const mode = budget.mode ?? 'healthy';
    if (mode === 'healthy') expect(owed.filter((row) => row.phase === 'initial'), `${path}: healthy fixture unexpectedly owed refresh`).toHaveLength(0);
    const counted = mode === 'degraded' ? initial.filter((row) => row.ledger !== 'retry') : initial;
    expect(counted.length, `${path}: critical budget\n${JSON.stringify(records, null, 2)}`).toBeLessThanOrEqual(budget.maxRequests);
    for (const row of counted) {
      const nonBootstrap: LoadRecord[] = criticalReadPredecessors(semanticCapture, row, counted);
      const depth = row.path === '/v1/bootstrap' ? 0 : nonBootstrap.length ? 2 : 1;
      expect(depth, `${path}: serial critical read ${row.path} after ${nonBootstrap.map((r) => r.path).join(', ')}`).toBeLessThanOrEqual(budget.maxDepth);
    }
    for (const row of owed) {
      expect(row.priority).toBe('critical');
      const previous = records.slice(0, records.indexOf(row)).filter((other) => other.identity === row.identity).at(-1);
      expect(previous, `${path}: owed read lacks a real prior response`).toBeTruthy();
      expect(previous?.model).toBe(row.model);
      expect(previous?.computedAt === null || previous?.refreshEta !== null).toBe(true);
      expect(previous?.finished).not.toBeNull();
      expect(row.confirmedPollSpacing?.elapsedMs ?? row.started - previous!.finished!).toBeGreaterThanOrEqual(REFRESH_POLL_MS - 50);
      expect(row.owedFrom?.length, `${path}: missing constituent refresh evidence`).toBeGreaterThan(0);
      for (const evidence of row.owedFrom ?? []) {
        expect(row.started, `${path}: exceeded finite recovery policy`).toBeLessThan(evidence.deadline);
        const peers = owed.filter((other) => other.owedFrom?.some((candidate) => candidate.cycle === evidence.cycle));
        const limit = Math.ceil(refreshRecoveryMs(refreshLimits(evidence.model)) / REFRESH_POLL_MS);
        expect(peers.length, `${path}: unbounded owed-refresh cycle ${evidence.identity}`).toBeLessThanOrEqual(limit);
      }
    }
    for (const row of actionReads) {
      expect(row.started, `${path}: callback exceeded finite recovery`).toBeLessThan(row.actionEvidence!.deadline);
    }
    if (actionReads.length) {
      const limit = (1 + Math.ceil(refreshRecoveryMs(refreshLimits('connector_detail')) / REFRESH_POLL_MS)) * DEFAULT_TRANSPORT_CONFIG.maxAttempts;
      expect(actionReads.length, `${path}: callback action attempts are unbounded`).toBeLessThanOrEqual(limit);
    }
    if (mode === 'degraded') {
      for (const row of initial.filter((item) => item.ledger === 'retry')) {
        expect(initial.filter((item) => item.identity === row.identity).length, `${path}: retries exceeded transport maxAttempts`).toBeLessThanOrEqual(DEFAULT_TRANSPORT_CONFIG.maxAttempts);
      }
    }
    const background = actual.filter((row) => row.priority === 'background');
    for (const row of background) {
      expect(row.method).toBe('GET');
      // Late critical registration must cancel/requeue warmers. Any warmer
      // still running across a later critical request is a scheduling failure.
      const earlier = critical.filter((other) => other.started <= row.started);
      expect(earlier.every((other) => other.finished !== null && other.finished <= row.started), `${path}: background started before critical settled`).toBe(true);
      const later = critical.find((other) => other.started > row.started && (row.finished === null || row.finished > other.started));
      if (later) expect(row.failure, `${path}: background did not yield to new critical read`).toMatch(/abort|cancel/i);
    }
    return records;
  } finally {
    await test.info().attach(`complete-attempts-${path.replace(/[^a-z0-9]/gi, "_")}`, { body: Buffer.from(JSON.stringify(records, null, 2)), contentType: "application/json" });
    page.off('request', start); page.off('requestfinished', finish); page.off('requestfailed', finish); page.off('response', response);
  }
}

import { recordPollTimingCandidate, readPollTimingCandidate } from "./pageLoadPollCandidates.mjs";
import { parseOceansIndex } from "./pageSectionTypes/oceans";
import { FAN_IN_LOAD_PATHS } from "./pageShellPaths";
import manifest from "./pageSections.generated.json" with { type: "json" };
import { refreshLimits, refreshRecoveryMs } from "./pageRefreshPolicy";
import { verifiedConnectorChoices, verifiedScopeItems } from "./connectorChoiceProof";
import { SCOPE_PAGES_PER_ACTION } from "./connectorActionPolicy";
import { DEFAULT_TRANSPORT_CONFIG } from "./transport";
import { isConnectorKey } from "./connectorKeys";
import { storedTimestamp, typedStored, type PageModel, type Stored } from "./pageSectionWire";

function pathParams(path: string, template: string): Record<string, string> | null {
  const names: string[] = [];
  const pattern = template.split(/(\{[^}]+\})/).map((part) => {
    if (part.startsWith("{")) { names.push(part.slice(1, -1)); return "([^/]+)"; }
    return part.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  }).join("");
  const match = new RegExp(`^${pattern}$`).exec(path);
  if (!match) return null;
  try { return Object.fromEntries(names.map((name, index) => [name, decodeURIComponent(match[index + 1])])); }
  catch { return null; }
}

/** Budget evidence uses the application's exact pure certificate validator.
 * The route selects the expected model; a response cannot nominate another
 * registered model and thereby acquire an owed-refresh exemption. */
export function storedReadEvidence(address: string, body: unknown): Stored<unknown>[] | null {
  const url = new URL(address, "https://page-read.invalid");
  const path = url.pathname;
  if (path === FAN_IN_LOAD_PATHS.oceansIndex) {
    try { return parseOceansIndex(body).components.map((c) => c.stored); } catch { return null; }
  }
  // Match literal routes before their parameterized siblings, just like the
  // router: /connectors/status is not ConnectorDetail(provider=status).
  const models = (Object.keys(manifest) as PageModel[]).sort((a, b) =>
    manifest[b].path.replace(/\{[^}]+\}/g, "").length - manifest[a].path.replace(/\{[^}]+\}/g, "").length);
  for (const model of models) {
    const params = pathParams(path, `/v1${manifest[model].path}`);
    if (!params) continue;
    try { return [typedStored(body, model, { ...Object.fromEntries(url.searchParams), ...params })]; } catch { return null; }
  }
  return null;
}

export interface CertificateEvidence {
  identity: string; model: PageModel; version: number; computedAt: string | null;
  stale: boolean; refreshEta: string | null; refreshPath?: string;
}
export interface OwedEvidence {
  identity: string; cycle: string; deadline: number; model: PageModel;
  computedAt: string | null; refreshEta: string | null;
}
function address(value: LoadAttempt): string {
  const url = new URL(value.address ?? value.path, "https://page-read.invalid");
  url.searchParams.sort();
  return `${url.pathname}${url.search}`;
}
export interface LoadAttempt {
  address?: string;
  certificates?: CertificateEvidence[]; owedFrom?: OwedEvidence[];
  identity: string; path: string; method: string; started: number; finished: number | null; status: number | null;
  ledger: "initial" | "owed-refresh" | "retry";
  model: PageModel | null; computedAt: string | null; refreshEta: string | null;
}
export const REFRESH_POLL_MS = 2_000;

/** Keeps every start. Only a verified response on the same credential scope,
 * canonical address and immediately preceding completed attempt earns polling
 * classification. Failed attempts and overlapping requests never qualify. */
export class StoredReadLedger {
  private last = new Map<string, LoadAttempt>();
  private evidence = new Map<string, { attempt: LoadAttempt; stored: Stored<unknown>[] }>();
  private cycles = new Map<string, { computedAt: string | null; deadline: number; cycle: string }>();
  start(attempt: LoadAttempt) {
    const previous = this.last.get(attempt.identity);
    const observed = this.evidence.get(attempt.identity);
    if (previous && (previous.status === null || previous.status === 429 || previous.status >= 500)) attempt.ledger = "retry";
    else if (attempt.method === "GET" && previous?.finished !== null && previous && observed?.attempt === previous &&
      address(previous) === address(attempt) &&
      observed.stored.some((component) => component.computed_at === null || component.refresh_eta !== null)) {
      const owed = (previous.certificates ?? []).filter((certificate) => certificate.computedAt === null || certificate.refreshEta !== null)
        .flatMap((certificate) => {
          const cycle = this.cycles.get(`${attempt.identity}|${certificate.identity}`);
          return cycle && attempt.started < cycle.deadline ? [{ identity: certificate.identity, ...cycle, model: certificate.model,
            computedAt: certificate.computedAt, refreshEta: certificate.refreshEta }] : [];
        });
      if (owed.length) {
        const model = observed.stored[0].model;
        recordPollTimingCandidate(this, attempt, previous, model, owed, REFRESH_POLL_MS - 50);
        if (attempt.started - previous.finished >= REFRESH_POLL_MS - 50) {
          attempt.ledger = "owed-refresh"; attempt.model = model; attempt.owedFrom = owed;
        }
      }
    }
    this.last.set(attempt.identity, attempt);
  }
  pollTimingCandidate(attempt: LoadAttempt, previous: LoadAttempt) {
    return readPollTimingCandidate(this, attempt, previous);
  }
  response(attempt: LoadAttempt, body: unknown) {
    if (this.last.get(attempt.identity) !== attempt) return;
    this.evidence.delete(attempt.identity);
    const clearCycles = () => { for (const key of this.cycles.keys()) if (key.startsWith(`${attempt.identity}|`)) this.cycles.delete(key); };
    if (attempt.method !== "GET" || !attempt.status || attempt.status < 200 || attempt.status >= 300) return;
    const stored = storedReadEvidence(attempt.address ?? attempt.path, body);
    if (!stored) { clearCycles(); return; }
    const url = new URL(attempt.address ?? attempt.path, "https://page-read.invalid");
    const components = url.pathname === FAN_IN_LOAD_PATHS.oceansIndex ? parseOceansIndex(body).components : null;
    attempt.certificates = stored.map((certificate, index) => ({ identity: components?.[index].account_id ?? address(attempt),
      model: certificate.model, version: certificate.version, computedAt: certificate.computed_at, stale: certificate.stale,
      refreshEta: certificate.refresh_eta, ...(components ? { refreshPath: components[index].refresh_path } : {}) }));
    const retained = new Set<string>();
    for (const certificate of attempt.certificates) {
      const key = `${attempt.identity}|${certificate.identity}`; retained.add(key);
      if (certificate.computedAt !== null && certificate.refreshEta === null) { this.cycles.delete(key); continue; }
      const previous = this.cycles.get(key);
      if (!previous || certificate.computedAt !== null && (previous.computedAt === null || Date.parse(certificate.computedAt) > Date.parse(previous.computedAt))) {
        const now = attempt.finished ?? attempt.started;
        this.cycles.set(key, { computedAt: certificate.computedAt, deadline: now + refreshRecoveryMs(refreshLimits(certificate.model)),
          cycle: `${key}|${now}|${certificate.computedAt}` });
      }
    }
    for (const key of this.cycles.keys()) if (key.startsWith(`${attempt.identity}|`) && !retained.has(key)) this.cycles.delete(key);
    attempt.model = stored[0]?.model ?? null;
    // Aggregate ledger flags express only whether some constituent owes a
    // refresh; each complete real certificate remains in evidence below.
    attempt.computedAt = stored.some((c) => c.computed_at === null) ? null : stored[0]?.computed_at ?? null;
    attempt.refreshEta = stored.find((c) => c.refresh_eta !== null)?.refresh_eta ?? null;
    this.evidence.set(attempt.identity, { attempt, stored });
  }
}


/** The one declared OAuth completion phase. A URL or label alone cannot
 * qualify a read: its same-scope refresh POST must really succeed first. */
export interface CallbackEvidence { acknowledgedBy: string; deadline: number; picker?: boolean; source?: string }
export class ConnectorCallbackPhase {
  private acknowledgement: LoadAttempt | null = null;
  private attempts: LoadAttempt[] = [];
  private details = new Map<LoadAttempt, Set<string>>();
  private choices = new Map<LoadAttempt, { next_cursor: string | null }>();
  private qualified = new Map<LoadAttempt, CallbackEvidence | null>();
  readonly readPath: string | null;
  readonly provider: string | null;
  constructor(pageAddress: string, declared: boolean, private readonly pickerDeclared = false) {
    const url = new URL(pageAddress, "https://page-read.invalid");
    const provider = url.searchParams.get("provider");
    this.provider = provider;
    this.readPath = declared && url.pathname === "/connectors/return" && url.searchParams.get("result") === "ok" &&
      provider && isConnectorKey(provider) ? `/v1${manifest.connector_detail.path.replace("{provider}", encodeURIComponent(provider))}` : null;
  }
  start(attempt: LoadAttempt) { this.attempts.push(attempt); }
  acknowledge(attempt: LoadAttempt, body: unknown) {
    if (this.readPath && !this.acknowledgement && attempt.method === "POST" && address(attempt) === `${this.readPath}/refresh` &&
      attempt.status === 202 && body && typeof body === "object" && "refresh_eta" in body && typeof body.refresh_eta === "string" && storedTimestamp(body.refresh_eta))
      this.acknowledgement = attempt;
  }
  observe(attempt: LoadAttempt, body: unknown) {
    if (!this.readPath || !this.provider || attempt.status !== 200 || attempt.method !== "GET") return;
    try {
      if (address(attempt) === this.readPath) this.details.set(attempt, verifiedConnectorChoices(body, this.provider));
      else if (attempt.path.startsWith(`${this.readPath}/sources/`) && attempt.path.endsWith('/scope-items')) this.choices.set(attempt, verifiedScopeItems(body));
    } catch { /* An invalid response cannot authorize another request. */ }
  }
  private context(attempt: LoadAttempt): CallbackEvidence | null {
    const ack = this.acknowledgement;
    if (!ack || ack.finished === null || attempt.started < ack.finished) return null;
    const credential = (value: LoadAttempt) => value.identity.slice(0, value.identity.lastIndexOf("|"));
    const deadline = ack.finished + refreshRecoveryMs(refreshLimits("connector_detail"));
    if (credential(ack) !== credential(attempt) || attempt.started >= deadline || this.attempts.some((row) => row !== ack && row.method === 'POST' &&
      address(row) === `${this.readPath}/refresh` && credential(row) === credential(attempt) && row.finished !== null && row.finished <= attempt.started &&
      row.status !== null && [401, 403, 404].includes(row.status))) return null;
    return { acknowledgedBy: ack.identity, deadline };
  }
  destination(attempt: LoadAttempt, pageAddress: string, declared: readonly string[]): string | null {
    if (!this.context(attempt)) return null;
    const page = new URL(pageAddress, "https://page-read.invalid");
    return declared.includes(page.pathname) && page.searchParams.get("setup") === this.provider ? `${page.pathname}${page.search}` : null;
  }
  classify(attempt: LoadAttempt): CallbackEvidence | null {
    if (attempt.method !== "GET") return null;
    const evidence = this.context(attempt);
    if (!evidence) return null;
    const credential = (value: LoadAttempt) => value.identity.slice(0, value.identity.lastIndexOf("|"));
    if (address(attempt) === this.readPath) return evidence;
    if (this.qualified.has(attempt)) return this.qualified.get(attempt) ?? null;
    const result = this.picker(attempt, credential, evidence);
    this.qualified.set(attempt, result);
    return result;
  }
  private picker(attempt: LoadAttempt, credential: (value: LoadAttempt) => string, evidence: CallbackEvidence): CallbackEvidence | null {
    if (!this.pickerDeclared) return null;
    const prefix = `${this.readPath}/sources/`;
    if (!attempt.path.startsWith(prefix) || !attempt.path.endsWith('/scope-items')) return null;
    let source: string;
    try { source = decodeURIComponent(attempt.path.slice(prefix.length, -'/scope-items'.length)); } catch { return null; }
    if (!source || attempt.path !== `${prefix}${encodeURIComponent(source)}/scope-items`) return null;
    const latest = this.attempts.filter((read) => read.method === 'GET' && address(read) === this.readPath && read.finished !== null &&
      read.finished <= attempt.started && credential(read) === credential(attempt)).at(-1);
    if (!latest || this.classify(latest) === null || !this.details.get(latest)?.has(source)) return null;
    const url = new URL(attempt.address ?? attempt.path, "https://page-read.invalid");
    if ([...url.searchParams.keys()].some((key) => key !== 'q' && key !== 'cursor')) return null;
    const index = this.attempts.indexOf(attempt);
    if (index < 0) return null;
    const prior = this.attempts.slice(0, index).filter((row) => row.method === 'GET' && row.path === attempt.path &&
      credential(row) === credential(attempt) && new URL(row.address ?? row.path, "https://page-read.invalid").searchParams.get('q') === url.searchParams.get('q'));
    const phaseReads = this.attempts.slice(0, index + 1).filter((row) => row.path.startsWith(prefix) && row.path.endsWith('/scope-items') && credential(row) === credential(attempt));
    if (phaseReads.length > SCOPE_PAGES_PER_ACTION * DEFAULT_TRANSPORT_CONFIG.maxAttempts ||
      phaseReads.slice(0, -1).filter((row) => row.status === 200 && this.choices.has(row)).length >= SCOPE_PAGES_PER_ACTION) return null;
    const previous = prior.at(-1);
    const cursor = url.searchParams.get('cursor');
    if (!previous) return cursor === null ? { ...evidence, picker: true, source } : null;
    if (previous.finished === null || previous.finished > attempt.started || !this.classify(previous)?.picker) return null;
    const page = this.choices.get(previous);
    if (previous.status === 200) {
      if (!page || page.next_cursor === null || page.next_cursor !== cursor || prior.some((row) => new URL(row.address ?? row.path, "https://page-read.invalid").searchParams.get('cursor') === cursor)) return null;
    } else if (!(previous.status === 429 || previous.status !== null && previous.status >= 500) || address(previous) !== address(attempt) ||
      prior.filter((row) => address(row) === address(attempt)).length >= DEFAULT_TRANSPORT_CONFIG.maxAttempts) return null;
    return { ...evidence, picker: true, source };
  }
}

import { pageLoadClock } from '../lib/pageLoadClock.mjs';
import { readFileSync } from 'node:fs';
import { createHash } from 'node:crypto';

// Narrow test workload contract, bound to the real pair_info implementation.
// No change to route/model declarations, limiter state or caller identity.
export function pairInfoRateContract(root) {
  const definitions = readFileSync(new URL('backend/src/routes/addin.rs', root), 'utf8');
  const handler = readFileSync(new URL('backend/src/routes/addin_microsoft.rs', root), 'utf8');
  const router = readFileSync(new URL('backend/src/lib.rs', root), 'utf8');
  if (!/routes::page_read::plain\(\s*"\/addin\/pair\/info",\s*routes::addin_microsoft::pair_info,/.test(router)) throw new Error('pair-info route binding changed');
  const body = handler.slice(handler.indexOf('pub async fn pair_info('), handler.indexOf('pub async fn pair_info(') + 3000);
  if (!/let code = q\.code\.trim\(\);/.test(body)
    || !/check_and_increment_key\(\s*&format!\("addin_info:\{code\}"\),\s*gmail_addin::PAIR_CODE_RATE_MAX_PER_WINDOW,\s*gmail_addin::PAIR_CODE_RATE_WINDOW_SECS,/.test(body)) {
    throw new Error('pair-info rate contract changed; refuse unqualified pacing');
  }
  const constant = (name) => {
    const matches = [...definitions.matchAll(new RegExp(`^pub\\(super\\) const ${name}: u(?:32|64) = (\\d+);$`, 'gm'))];
    const value = matches.length === 1 ? Number(matches[0][1]) : NaN;
    if (!Number.isSafeInteger(value) || value <= 0) throw new Error('missing or ambiguous backend rate contract');
    return value;
  };
  const windowMs = 1000 * constant('PAIR_CODE_RATE_WINDOW_SECS');
  if (!Number.isSafeInteger(windowMs)) throw new Error('invalid backend rate window');
  return { endpoint: '/v1/addin/pair/info', parameter: 'code', prefix: 'addin_info:',
    max: constant('PAIR_CODE_RATE_MAX_PER_WINDOW'), window_ms: windowMs,
    source: ['backend/src/routes/addin.rs', 'backend/src/routes/addin_microsoft.rs', 'backend/src/lib.rs'],
    source_sha256: [definitions, handler, router].map((text) => createHash('sha256').update(text).digest('hex')) };
}

export class RateCadence {
  constructor(contract, maxAttempts, clock = pageLoadClock) {
    if (!Number.isInteger(maxAttempts) || maxAttempts < 1 || maxAttempts > contract.max) throw new Error('invalid existing transport bound');
    this.clock = clock; this.contract = contract; this.maxAttempts = maxAttempts; this.starts = []; this.gates = [];
  }
  key(record) {
    const url = new URL(record.path, 'https://rate.invalid');
    if (record.method !== 'GET' || url.pathname !== this.contract.endpoint) return null;
    const code = url.searchParams.get(this.contract.parameter)?.trim();
    return code ? `${this.contract.prefix}${code}` : null;
  }
  observe(records, phase) {
    for (const record of records) {
      const key = this.key(record); if (!key) continue;
      if (!Number.isFinite(record.started)) throw new Error('rate evidence missing actual start');
      // Admission happened before a completed response. Its finish is a
      // conservative upper bound, avoiding a client/server transit-time race
      // at the exact60s boundary. Unknown completion uses observation time.
      const windowAnchor = Number.isFinite(record.finished) ? Math.max(record.started, record.finished) : Math.max(record.started, this.clock.now());
      this.starts.push({ key, started: record.started, finished: record.finished ?? null, window_anchor: windowAnchor, status: record.status, failure: record.failure, phase, wallStarted: record.wallStarted ?? null, wallFinished: record.wallFinished ?? null, wallObservedAt: this.clock.wallNow() });
    }
  }
  plan(records, now) {
    const keys = [...new Set(records.map((row) => this.key(row)).filter(Boolean))];
    const budgets = keys.map((key) => {
      // Reserve all allowed transport attempts before starting a page. Every
      // actual retry remains recorded and consumes this same real key budget.
      const reserved = records.filter((row) => this.key(row) === key).length * this.maxAttempts;
      if (reserved > this.contract.max) throw new Error('single page exceeds real admission budget');
      const recent = this.starts.filter((row) => row.key === key && row.window_anchor + this.contract.window_ms >= now).sort((a,b) => a.window_anchor-b.window_anchor);
      const overflow = recent.length + reserved - this.contract.max;
      const release = overflow > 0 ? recent[overflow - 1].window_anchor + this.contract.window_ms + 1 : now;
      return { key, actual_starts_in_window: recent.length, reserved_attempts: reserved, release_at: release };
    });
    return { requested_at: now, release_at: Math.max(now, ...budgets.map((b) => b.release_at)), budgets };
  }
  async before(records, clock = this.clock.now, wait = (ms) => new Promise((resolve) => setTimeout(resolve, ms))) {
    const gate = { ...this.plan(records, clock()), wallRequestedAt: this.clock.wallNow() }; this.gates.push(gate);
    while (clock() < gate.release_at) await wait(Math.min(50_000, gate.release_at - clock()));
    gate.applied_at = clock(); gate.wallAppliedAt = this.clock.wallNow(); return gate;
  }
  receipt() { return { contract: this.contract, max_transport_attempts: this.maxAttempts, starts: this.starts, gates: this.gates,
    admission_bound: 'Completed response finish bounds server admission from above; unknown completion is conservatively anchored at observation. Every actual start, including retries, is retained.',
    timing_boundary: 'Cadence waits occur before observeLoad/page navigation; no Server-Timing sample is delayed, removed or retried.' }; }
}

import { readPollTimingCandidate } from '../../web/lib/pageLoadPollCandidates.mjs';
import { createHash } from 'node:crypto';
import manifest from '../../web/lib/pageSections.generated.json' with { type: 'json' };
import { assertStatesForPath } from './envelopes.mjs';

const installed = new WeakSet();
// Runtime provenance: a lookalike result, function or copied callback is never
// accepted. Only this module's complete successful validation can mint proof.
const validatedCaptures = new WeakMap();
const hash = value => createHash('sha256').update(JSON.stringify(value)).digest('hex');
const binding = (records, ledger) => hash([
  records.map(r => [r.method,r.path,r.priority,r.scope,r.started,r.finished,r.wallStarted,r.wallFinished,r.status,r.failure ?? null]),
  ledger.map(r => [r.identity,r.method,r.address ?? r.path,r.priority,r.phase,r.initialWave,r.excluded,r.started,r.finished,r.wallStarted,r.wallFinished,r.status,r.failure ?? null,r.nodeLedger ?? r.ledger,r.certificates]),
]);
const resultBinding = value => hash([value.ok,value.errors,value.matches,value.timings,value.trust]);
const canonical = address => { const u = new URL(address, 'https://receipt.invalid'); u.searchParams.sort(); return `${u.pathname}${u.search}`; };
const fingerprint = (nonce, scope) => createHash('sha256').update(`${nonce}\0${scope}`).digest('hex');
const trust = 'Source-reviewed sealed E2E capture; no external attestation. Poll timing requires complete runtime-bound validation.';

const uuid = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const fanIn = run => run?.address === '/v1/load/oceans' && run.model === 'oceans_index';
const certificateProjection = c => ({ model: c.model, version: c.version, computedAt: c.computedAt, stale: c.stale, refreshEta: c.refreshEta });
const certificateShape = (c, model) => c && Object.keys(c).every(k => ['model','version','computedAt','stale','refreshEta'].includes(k)) &&
  c.model === model && c.version === manifest[model]?.version && typeof c.stale === 'boolean' &&
  (c.computedAt === null || typeof c.computedAt === 'string') && (c.refreshEta === null || typeof c.refreshEta === 'string');
const receiptCertificates = row => Array.isArray(row?.components) ? row.components.map(c => c.certificate) : row?.certificate ? [row.certificate] : [];
function projectionShape(row, run) {
  if (!fanIn(run)) return !('components' in row) && certificateShape(row.certificate, run?.model);
  if ('certificate' in row || !Array.isArray(row.components) || row.components.length > 501) return false;
  const ids = new Set();
  return row.components.every(c => {
    if (!c || Object.keys(c).some(k => !['identity','anchorGraphId','refreshPath','certificate'].includes(k)) ||
      typeof c.identity !== 'string' || !uuid.test(c.identity) || ids.has(c.identity) ||
      !(c.anchorGraphId === null || typeof c.anchorGraphId === 'string' && uuid.test(c.anchorGraphId)) ||
      c.refreshPath !== `/v1/load/oceans/component/refresh${c.anchorGraphId ? `?graph=${c.anchorGraphId}` : ''}` || !certificateShape(c.certificate, run.model)) return false;
    ids.add(c.identity); return true;
  });
}
function nodeComponents(certificates) {
  return certificates.map(c => {
    // The original ledger parsed the full typed body. Its strict refresh path
    // already encodes the body's verified anchor; no payload/history is replayed.
    const anchor = new URL(c.refreshPath, 'https://receipt.invalid').searchParams.get('graph');
    return { identity: c.identity, anchorGraphId: anchor, refreshPath: c.refreshPath, certificate: certificateProjection(c) };
  });
}

/** Capture schema/correlation only. Never modifies the ledger or its policy. */
export function validateSemanticTiming(snapshot, nonce, records, ledger, eligibilityLedger = /** @type {import('../../web/lib/pageLoadEvidence').StoredReadLedger | undefined} */ (undefined)) {
  const errors = [], matches = [], timings = [];
  const fail = code => errors.push(code);
  if (!snapshot || Object.keys(snapshot).some(k => !['version','nonce','clock','events','errors'].includes(k)) || snapshot.version !== 1 || snapshot.clock !== 'browser-performance' || snapshot.nonce !== nonce || !/^[0-9a-f-]{36}$/i.test(nonce ?? '') ||
      !Array.isArray(snapshot.events) || snapshot.events.length > 4096 || !Array.isArray(snapshot.errors) || snapshot.errors.length) {
    return { ok: false, errors: ['invalid-or-incomplete-snapshot'], matches, timings, trust };
  }
  const events = structuredClone(snapshot.events), bySeq = new Map(), runs = new Map(), attempts = new Map(), owned = new Map(), confirmations = new Map();
  const allowed = new Set(['seq','at','kind','run','query','attempt','admission','parent','address','method','priority','scopeDigest','status','certificate','components','manual','model']);
  const fields = { run: ['run','query','address','model','parent'], dispatch: ['attempt','run','address','method','priority','scopeDigest'],
    response: ['attempt','status'], join: ['run','attempt'], admission: ['run','query','attempt','certificate','components'],
    'cache-success': ['run','query','admission','manual'], 'cache-error': ['query','run'], removed: ['query'] };
  const kinds = new Set(Object.keys(fields));
  let last = -1;
  for (const [index, row] of events.entries()) {
    if (!row || Object.keys(row).some(k => !allowed.has(k)) || row.seq !== index + 1 || !Number.isFinite(row.at) || row.at < 0 || row.at < last || !kinds.has(row.kind)) { fail('malformed-event'); continue; }
    if (Object.keys(row).some(k => !['seq','at','kind',...fields[row.kind]].includes(k))) { fail('wrong-event-shape'); continue; }
    last = row.at; bySeq.set(row.seq, row);
    if (row.kind === 'run') {
      if (!Number.isInteger(row.run) || runs.has(row.run) || !Number.isInteger(row.query) || typeof row.address !== 'string' || typeof row.model !== 'string' || !manifest[row.model]) { fail('invalid-run'); continue; }
      if (row.parent !== null && confirmations.get(row.query)?.admission !== row.parent) fail('unconfirmed-parent');
      runs.set(row.run, row); owned.set(row.run, new Set());
    } else if (row.kind === 'dispatch') {
      if (!Number.isInteger(row.attempt) || attempts.has(row.attempt) || !['GET','POST','PUT','PATCH','DELETE','HEAD'].includes(row.method) || typeof row.address !== 'string' ||
        canonical(row.address) !== row.address || !/^[a-f0-9]{64}$/.test(row.scopeDigest ?? '') || !['critical','normal','background'].includes(row.priority)) { fail('invalid-dispatch'); continue; }
      if (row.run !== undefined && !runs.has(row.run)) fail('unknown-dispatch-run');
      attempts.set(row.attempt, row); if (row.run !== undefined) owned.get(row.run)?.add(row.attempt);
    } else if (row.kind === 'response') {
      if (!attempts.has(row.attempt) || !Number.isInteger(row.status) || row.status < 100 || row.status > 599 || attempts.get(row.attempt).response) fail('invalid-response');
      else attempts.get(row.attempt).response = row; // Working copy below, never app data.
    } else if (row.kind === 'join') {
      if (!runs.has(row.run) || !attempts.get(row.attempt)?.response) fail('invalid-join'); else owned.get(row.run).add(row.attempt);
    } else if (row.kind === 'admission') {
      const run = runs.get(row.run), attempt = attempts.get(row.attempt);
      if (!run || run.query !== row.query || !owned.get(row.run)?.has(row.attempt) || attempt?.response?.status !== 200 || attempt.method !== 'GET' ||
        !projectionShape(row, run) || attempt.address !== canonical(run.address)) fail('unbound-admission');
    } else if (row.kind === 'cache-success') {
      const admission = bySeq.get(row.admission);
      if (typeof row.manual !== 'boolean' || admission?.kind !== 'admission' || row.run !== admission.run || row.query !== admission.query) fail('unbound-confirmation');
      else if (!row.manual) confirmations.set(row.query, row);
    } else if (row.kind === 'cache-error' || row.kind === 'removed') confirmations.delete(row.query);
  }
  const original = records.map((row, index) => ({ row, index })).filter(({ row }) => row.method !== 'OPTIONS' && !(row.method === 'POST' && new URL(row.path, 'https://r.invalid').pathname === '/v1/telemetry/client'));
  const groups = new Map();
  const key = row => JSON.stringify([row.method, canonical(row.path ?? row.address), row.priority, row.scopeDigest]);
  for (const { row, index } of original) {
    const scopeDigest = fingerprint(nonce, row.scope), k = key({ ...row, scopeDigest });
    if (!groups.has(k)) groups.set(k, []); groups.get(k).push({ row, index });
  }
  const cursors = new Map();
  for (const attempt of attempts.values()) {
    const k = key(attempt), ordinal = cursors.get(k) ?? 0, peers = groups.get(k) ?? [], match = peers[ordinal]; cursors.set(k, ordinal + 1);
    if (!match) { fail('unmatched-browser-attempt'); continue; }
    if (ordinal > 0 && (peers[ordinal - 1].row.finished === null || peers[ordinal - 1].row.finished > match.row.started)) fail('ambiguous-overlapping-occurrences');
    if (attempt.response && attempt.response.status !== match.row.status) fail('response-status-mismatch');
    const ledgerPeers = ledger.filter(entry => entry.method === match.row.method && entry.priority === match.row.priority && canonical(entry.address ?? entry.path) === canonical(match.row.path) && entry.identity.slice(0, entry.identity.lastIndexOf('|')) === match.row.scope);
    attempt.node = match; attempt.ledgerEntry = ledgerPeers[ordinal]; attempt.certificate = attempt.ledgerEntry?.certificates;
    matches.push({ attempt: attempt.attempt, networkIndex: match.index, occurrence: ordinal });
  }
  for (const [k, peers] of groups) if ((cursors.get(k) ?? 0) !== peers.length) fail('unmatched-original-network-start');
  for (const row of events.filter(row => row?.kind === 'admission')) {
    const attempt = attempts.get(row.attempt), run = runs.get(row.run);
    if (!run || !projectionShape(row, run) || !attempt?.node || !Array.isArray(attempt.certificate)) { fail('actual-certificate-mismatch'); continue; }
    try {
      const matchesActual = fanIn(run)
        ? JSON.stringify(row.components) === JSON.stringify(nodeComponents(attempt.certificate))
        : attempt.certificate.some(value => JSON.stringify(certificateProjection(value)) === JSON.stringify(row.certificate));
      if (!matchesActual) { fail('actual-certificate-mismatch'); continue; }
      assertStatesForPath(receiptCertificates(row).map(c => ({ identity: 'single', model: c.model, version: c.version, computed_at: c.computedAt, stale: c.stale, refresh_eta: c.refreshEta })), manifest, attempt.address);
    } catch { fail('invalid-route-certificate'); }
  }
  for (const run of runs.values()) {
    const completed = events.some(e => e.kind === 'cache-success' && !e.manual && e.run === run.run);
    const failed = events.some(e => e.kind === 'cache-error' && e.run === run.run || e.kind === 'removed' && e.query === run.query);
    if (!completed && !failed) fail('unsettled-query-run');
    const admission = bySeq.get(run.parent), confirmation = events.find(e => e.kind === 'cache-success' && !e.manual && e.admission === run.parent);
    if (!admission || !confirmation) continue;
    for (const attempt of attempts.values()) if (owned.get(run.run)?.has(attempt.attempt)) timings.push({ run: run.run, attempt: attempt.attempt, parent: run.parent,
      admissionToDispatchMs: attempt.at - admission.at, confirmationToDispatchMs: attempt.at - confirmation.at,
      admissionToConfirmationMs: confirmation.at - admission.at, clock: 'browser-performance', ordering: 'admission precedes success dispatch; public cache confirmation follows observer notification, not timer arm' });
  }
  const result = { ok: errors.length === 0, errors: [...new Set(errors)], matches, timings, trust };
  if (result.ok) {
    const edges = [], bootOverlaps = [];
    // A parameter-free sidebar query starts while bootstrap is in flight.
    // Its HTTP completion alone does not establish a page dependency.
    // Require actual same-scope browser dispatch/response, registered query
    // initiation and guarded admission/cache success, all correlated above.
    // The page must dispatch BEFORE sidebar admission. Query initiation
    // alone cannot exclude a dependency in async credential/transport work.
    for (const boot of attempts.values()) {
      // Exact negotiated contracts only; arbitrary bootstrap queries cannot
      // supply this proof. Every provenance/admission condition below remains.
      if (!['/v1/bootstrap', '/v1/bootstrap?billing_banner=trial_v1'].includes(boot.address) ||
        boot.method !== 'GET' || boot.response?.status !== 200) continue;
      for (const sidebar of attempts.values()) {
        const sidebarRun = runs.get(sidebar.run);
        const sidebarSuccess = events.find(e => e.kind === 'cache-success' && !e.manual && e.run === sidebar.run);
        if (sidebar.address !== '/v1/conversations' || sidebar.method !== 'GET' || sidebar.scopeDigest !== boot.scopeDigest ||
          sidebar.response?.status !== 200 || sidebar.at >= boot.response.at ||
          !sidebarRun || sidebarRun.parent !== null || sidebarRun.at >= boot.response.at ||
          !sidebarSuccess || !sidebar.ledgerEntry) continue;
        const sidebarAdmission = bySeq.get(sidebarSuccess.admission);
        for (const next of attempts.values()) {
          const nextRun = runs.get(next.run);
          const pageSuccess = events.find(e => e.kind === 'cache-success' && !e.manual && e.run === next.run);
          if (!nextRun || nextRun.parent !== null || next.scopeDigest !== boot.scopeDigest || nextRun.at < boot.response.at ||
            next.at < boot.response.at || nextRun.at >= sidebarAdmission.at || next.seq >= sidebarAdmission.seq ||
            next.response?.status !== 200 || !next.ledgerEntry ||
            !pageSuccess) continue;
          bootOverlaps.push({ current: next.ledgerEntry, previous: sidebar.ledgerEntry, proof: {
            source: 'verified-bootstrap-overlap', bootstrapAttempt: boot.attempt, bootstrapResponseAt: boot.response.at,
            sidebarRun: sidebar.run, sidebarAttempt: sidebar.attempt, sidebarResponseAt: sidebar.response.at,
            sidebarAdmissionAt: sidebarAdmission.at, sidebarAdmissionSeq: sidebarAdmission.seq, sidebarConfirmedAt: sidebarSuccess.at,
            pageRun: next.run, pageInitiatedAt: nextRun.at, pageAttempt: next.attempt, pageDispatchedAt: next.at, pageDispatchSeq: next.seq,
            pageAdmissionAt: bySeq.get(pageSuccess.admission).at, pageConfirmedAt: pageSuccess.at,
          } });
        }
      }
    }
    for (const run of runs.values()) {
      const parent = bySeq.get(run.parent), confirmation = events.find(e => e.kind === 'cache-success' && !e.manual && e.admission === run.parent);
      const parentAttempt = parent && attempts.get(parent.attempt);
      if (!parent || !confirmation || parent.query !== run.query || !receiptCertificates(parent).some(c => c.computedAt === null || typeof c.refreshEta === 'string')) continue;
      // A successful current publication is required as well as the prior one;
      // error/removed settlement alone cannot authorize a timing correction.
      if (!events.some(e => e.kind === 'cache-success' && !e.manual && e.run === run.run)) continue;
      for (const attempt of attempts.values()) {
        if (attempt.run !== run.run || !owned.get(run.run)?.has(attempt.attempt) || confirmation.seq >= attempt.seq || attempt.response?.status !== 200 || !attempt.ledgerEntry || !parentAttempt?.ledgerEntry) continue;
        const current = attempt.ledgerEntry, previous = parentAttempt.ledgerEntry;
        if (current === previous || current.priority !== 'critical' || previous.priority !== 'critical' || current.phase !== 'initial' || previous.phase !== 'initial' ||
          current.method !== 'GET' || previous.method !== 'GET' || current.status !== 200 || previous.status !== 200 || current.failure || previous.failure ||
          current.identity !== previous.identity || canonical(current.address ?? current.path) !== canonical(previous.address ?? previous.path) ||
          (current.nodeLedger ?? current.ledger) === 'retry' || !previous.certificates?.length || !Array.isArray(current.certificates) ||
          (!fanIn(run) && (previous.certificates.length !== 1 || current.certificates.length !== 1)) ||
          previous.certificates.some(c => c.model !== run.model) || current.certificates.some(c => c.model !== run.model)) continue;
        edges.push(Object.freeze({ current, previous, proof: Object.freeze({
          source: 'verified-browser-cache-confirmation', nonce, run: run.run, query: run.query, attempt: attempt.attempt,
          parentAdmission: parent.seq, confirmation: confirmation.seq, networkIndex: attempt.node.index, previousNetworkIndex: parentAttempt.node.index,
          elapsedMs: attempt.at - confirmation.at,
        }) }));
      }
    }
    validatedCaptures.set(result, { records, ledger, recordRows: [...records], ledgerRows: [...ledger], binding: binding(records, ledger), snapshot, snapshotHash: hash(snapshot), resultHash: resultBinding(result), edges, bootOverlaps, eligibilityLedger, candidateAccessor: eligibilityLedger?.pollTimingCandidate, used: false });
  }
  return result;
}

/** Sole E2E correction boundary. No arbitrary callback/raw metadata is trusted.
 * The pure ledger supplies read-only candidates from its original full checks;
 * all deadlines/cycles continue to use their original Node timestamps. */
export function applyConfirmedSemanticTiming(capture, records, ledgerRows, ledger) {
  const validated = capture && typeof capture === 'object' && validatedCaptures.get(capture);
  if (!validated || capture.ok !== true || validated.used || validated.records !== records || validated.ledger !== ledgerRows || validated.eligibilityLedger !== ledger || typeof validated.candidateAccessor !== 'function' || ledger.pollTimingCandidate !== validated.candidateAccessor ||
      validated.recordRows.some((row, index) => row !== records[index]) || validated.ledgerRows.some((row, index) => row !== ledgerRows[index]) ||
      validated.binding !== binding(records, ledgerRows) || validated.snapshotHash !== hash(validated.snapshot) ||
      (capture.snapshot !== undefined && validated.snapshotHash !== hash(capture.snapshot)) || validated.resultHash !== resultBinding(capture))
    return { applied: [], reason: 'no-unchanged-validated-capture' };
  validated.used = true;
  const applied = [];
  for (const { current, previous, proof } of validated.edges) {
    const candidate = readPollTimingCandidate(ledger, current, previous);
    if (!candidate || candidate.model !== previous.certificates[0].model || !Number.isFinite(proof.elapsedMs) || proof.elapsedMs < 0) continue;
    const original = current.nodeLedger ?? current.ledger;
    current.nodeLedger = original;
    current.confirmedPollSpacing = { ...proof, originalNodeLedger: original };
    if (proof.elapsedMs >= candidate.minimumSpacingMs) {
      current.ledger = 'owed-refresh'; current.model = candidate.model; current.owedFrom = candidate.owedFrom;
    } else {
      current.ledger = 'initial'; delete current.owedFrom;
    }
    applied.push({ ...current.confirmedPollSpacing, effectiveLedger: current.ledger, changed: original !== current.ledger });
  }
  return { applied, bootstrapOverlaps: validated.bootOverlaps.map(edge => edge.proof), reason: 'fully-validated-same-browser-confirmation-interval' };
}

/** No SDK/network use. Only active for an explicit coordinated capture run. */
export async function prepareSemanticTimingCapture(page) {
  if (process.env.E2E_CAPTURE_PAGE_READ_RECEIPTS !== '1') return undefined;
  if (!installed.has(page)) {
    await page.addInitScript(() => { Object.defineProperty(window, '__csPageReadTimingNonce', { value: crypto.randomUUID(), configurable: false, writable: false }); });
    installed.add(page);
  }
  return async (records, ledger, eligibilityLedger = /** @type {import('../../web/lib/pageLoadEvidence').StoredReadLedger | undefined} */ (undefined)) => {
    try {
      const captured = await page.evaluate(async () => {
        const nonce = window.__csPageReadTimingNonce, nonceDescriptor = Object.getOwnPropertyDescriptor(window, '__csPageReadTimingNonce'), descriptor = Object.getOwnPropertyDescriptor(window, '__csPageReadTimingSnapshot');
        if (!nonce || !nonceDescriptor || nonceDescriptor.configurable || nonceDescriptor.writable || !descriptor || descriptor.configurable || descriptor.writable || typeof descriptor.value !== 'function' || !Object.isFrozen(descriptor.value)) return { nonce, snapshot: null };
        return { nonce, snapshot: await descriptor.value() };
      });
      const snapshot = captured.snapshot;
      // Validation annotates private working records only, not exported events.
      const result = validateSemanticTiming(structuredClone(snapshot), captured.nonce, records, ledger, eligibilityLedger);
      const capture = { ...result, ...(result.ok ? { snapshot } : { rejectedEventCount: Array.isArray(snapshot?.events) ? snapshot.events.length : null }) };
      const proof = validatedCaptures.get(result); if (proof) validatedCaptures.set(capture, proof);
      return capture;
    } catch { return { ok: false, errors: ['capture-unavailable'], matches: [], timings: [], trust }; }
  };
}

/** Temporal candidates remain conservative unless a validated capture proves
 * an early sidebar result was not yet admitted when the page dispatched. */
export function criticalReadPredecessors(capture, row, counted) {
  const validated = capture && validatedCaptures.get(capture);
  const trusted = validated && capture.ok === true && validated.snapshotHash === hash(validated.snapshot) &&
    (capture.snapshot === undefined || validated.snapshotHash === hash(capture.snapshot)) &&
    validated.resultHash === resultBinding(capture) && validated.binding === binding(validated.records, validated.ledger);
  return counted.filter(other => other !== row && other.finished !== null && other.finished < row.started &&
    other.path !== '/v1/bootstrap' && other.initialWave === row.initialWave &&
    !(trusted && validated.bootOverlaps.some(edge => edge.current === row && edge.previous === other)));
}

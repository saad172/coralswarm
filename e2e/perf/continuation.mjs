import { createHash } from 'node:crypto';
import { isDeepStrictEqual as same } from 'node:util';

const CASES = 96;
const LOADS = 20;
const hashPattern = /^[a-f0-9]{64}$/;
const require = (condition, message) => { if (!condition) throw new Error(message); };
const nonempty = value => typeof value === 'string' && value.length > 0;
const object = value => value !== null && typeof value === 'object' && !Array.isArray(value);
const empty = value => Array.isArray(value) && value.length === 0;
const copy = value => JSON.parse(JSON.stringify(value));
const freeze = value => {
  if (value && typeof value === 'object') {
    Object.values(value).forEach(freeze);
    Object.freeze(value);
  }
  return value;
};

export const caseIdentity = ({ route, path }) => JSON.stringify([route, path]);
export const sha256 = text => createHash('sha256').update(text).digest('hex');

function inventoryMap(inventory) {
  require(Array.isArray(inventory) && inventory.length === CASES, 'inventory must contain exactly 96 cases');
  const map = new Map();
  for (const entry of inventory) {
    require(object(entry) && nonempty(entry.route) && nonempty(entry.path), 'missing inventory route/path');
    const id = caseIdentity(entry);
    require(!map.has(id), 'duplicate inventory case');
    map.set(id, entry);
  }
  return map;
}

function sourceValid(source) {
  return object(source) && nonempty(source.revision) && hashPattern.test(source.sha256);
}

function verifyBlob(blob, readPriorBlob, kind) {
  require(object(blob) && nonempty(blob.id) && hashPattern.test(blob.sha256), 'receipt hash mismatch or missing receipt identity');
  // Whitelist reference fields: never clone, serialize or retain inline bodies.
  const reference = { id: blob.id, sha256: blob.sha256 };
  if (blob.path !== undefined) { require(nonempty(blob.path), 'invalid receipt path'); reference.path = blob.path; }
  if (blob.locator !== undefined) reference.locator = copy(blob.locator);
  freeze(reference);
  let json = blob.json;
  if (json === undefined) {
    require(typeof readPriorBlob === 'function', 'readPriorBlob is required for references');
    require(reference.path !== undefined || reference.locator !== undefined, 'receipt reference requires path or locator');
    json = readPriorBlob(reference, freeze({ kind }));
  }
  require(typeof json === 'string', 'prior blob must be a synchronous JSON string');
  require(sha256(json) === reference.sha256, 'receipt hash mismatch');
  return { value: freeze(JSON.parse(json)), reference };
}

function validateBindings(bindings, now) {
  require(object(bindings), 'missing bindings');
  for (const key of ['accountId', 'actorId', 'orgId', 'runId', 'stackId']) {
    require(nonempty(bindings[key]), `missing binding: ${key}`);
  }
  for (const key of ['fixtureSha256', 'seedSha256', 'backendSha256', 'migratorSha256']) {
    require(hashPattern.test(bindings[key]), `missing binding: ${key}`);
  }
  require(object(bindings.web) && Object.keys(bindings.web).length > 0, 'missing web provenance binding');
  const owner = bindings.ownerIdentity;
  require(owner?.namespace === 'nightly' && owner.owner === bindings.runId
    && nonempty(owner.lease?.id) && Number.isSafeInteger(owner.lease.expiresAt)
    && owner.lease.expiresAt > now, 'exact live nightly owner lease required');
}

function validatePriorBindings(current, prior, now, renewal, validateLeaseRenewal, context) {
  if (same(current, prior)) { validateBindings(prior, now); return null; }
  require(object(renewal) && typeof validateLeaseRenewal === 'function', 'binding/lease mismatch requires verified renewal proof');
  require(Number.isSafeInteger(renewal.verifiedAt) && renewal.verifiedAt >= 0
    && renewal.verifiedAt <= now, 'invalid renewal verification time');
  // Validate the OLD identity at actual server verification, never at a
  // fabricated later deadline. Only the expiry may differ anywhere in bindings.
  validateBindings(prior, renewal.verifiedAt);
  require(same(renewal.from, prior.ownerIdentity) && same(renewal.to, current.ownerIdentity)
    && current.ownerIdentity.lease.expiresAt > prior.ownerIdentity.lease.expiresAt, 'renewal identity/deadline mismatch');
  const advanced = copy(prior);
  advanced.ownerIdentity.lease.expiresAt = current.ownerIdentity.lease.expiresAt;
  require(same(advanced, current), 'renewal cannot change any other binding');
  require(nonempty(renewal.proof?.id) && hashPattern.test(renewal.proof.sha256), 'hashed renewal proof reference required');
  const lineage = freeze({ ...copy(renewal), ...context });
  let verified = false;
  try {
    verified = validateLeaseRenewal(freeze({ lineage, priorBindings: copy(prior), bindings: copy(current), now })) === true;
  } catch { /* Unknown, failed or partial actual verification fails closed. */ }
  require(verified, 'actual all-participant lease renewal verification rejected');
  return lineage;
}

function eligibility(receipt) {
  const counts = receipt.document_inventory;
  if (receipt.status !== 'passed') return 'prior-failed-or-partial';
  if (counts?.discovery_attempted !== true || counts.discovery_returned !== true
    || counts.measured_attempted !== LOADS || counts.measured_returned !== LOADS
    || counts.measured_not_run !== 0 || receipt.loads?.length !== LOADS) return 'incomplete-counts';
  if (!empty(receipt.validation_errors) || (receipt.errors !== undefined && !empty(receipt.errors))
    || receipt.error || receipt.fatal_outcome) return 'prior-errors';
  if (![receipt.discovery, ...receipt.loads].every(load => object(load) && load.path === receipt.path
    && empty(load.errors) && load.document?.ok === true && load.document.fatal === false
    && empty(load.document.reasons))) return 'document-evidence-failure';
  // These are the actual existing refresh/per_load_setup receipt fields.
  const cleanSetup = setup => object(setup) && empty(setup.errors) && !setup.error && !setup.failure
    && Array.isArray(setup.ledger ?? setup.attempts)
    && (setup.ledger ?? setup.attempts).every(attempt => !attempt.failure && !attempt.error
      && ((attempt.method === 'GET' && attempt.status === 200) || (attempt.method === 'POST' && attempt.status === 202)))
    && (setup.refreshed !== true || object(setup.refresh))
    && (!setup.refresh || cleanSetup(setup.refresh));
  if (!cleanSetup(receipt.refresh) || !Array.isArray(receipt.per_load_setup)
    || receipt.per_load_setup.length !== LOADS
    || !receipt.per_load_setup.every((setup, i) => setup.load === i + 1 && cleanSetup(setup))) return 'setup-failure';
  return null;
}

/** Pure, synchronous planning only. All artifact reads and provenance/live lease
 * checks belong to the caller. No route/model registry or execution is here.
 *
 * priorBundles: [{ id, sha256, path | locator }]. Parsed compact bundle:
 * { inventory, bindings, sourceFingerprint, cases: [caseReference] }.
 * readPriorBlob(reference, {kind: 'bundle' | 'case'}) MUST synchronously return
 * exact UTF-8 JSON text. Each hash is checked BEFORE parsing/validation. The
 * caller owns I/O and must not cache case bodies or retain callback receipts.
 * Small inline {id, sha256, json} blobs remain supported. Output keeps only
 * references, never raw receipts. Do not construct aggregate inline bundles
 * for large evidence; one manifest and one parsed case are held at a time.
 * bindings are caller-verified safe projections from actual fixture/pin/web/
 * owner artifacts, never inferred from receipt success flags.
 * sourceQualifications: explicit { prior, current, approvalId,
 * qualificationSha256 } records; even identical sources require approval.
 * validatePriorCase MUST recompute actual strict routeStatistics and document
 * auth/model outcomes; return { ok: true, statistics } only on full success.
 * acceptedPins can carry an earlier plan's acceptedCompleted to prevent rebinding.
 * Optional ONE leaseRenewal: { proof: {id, sha256}, verifiedAt, from, to }.
 * from/to are exact ownerIdentity objects. REQUIRED synchronous
 * validateLeaseRenewal({lineage, priorBindings, bindings, now}) must return true
 * only after caller verifies the hashed actual server receipt: ROOT-authorized
 * pre-expiry update and readback of ALL same owned participants, exact identities,
 * all metadata unchanged except expiry, and qualified operation source. No SDK
 * or receipt-reference authenticity is asserted here. The approved harness
 * sourceQualification and original bundle hash accompany each output lineage.
 */
export function planContinuation(options) {
  require(typeof options?.validatePriorCase === 'function', 'validatePriorCase is required');
  const { validatePriorCase, validateLeaseRenewal, readPriorBlob } = options;
  // Copy only small headers. Serializing options/priorBundles would duplicate
  // every inline receipt and can exceed Node's single-string limit.
  const { inventory, bindings, currentSource, sourceQualifications, priorBundles, now,
    acceptedPins = [], leaseRenewal } = {
    ...copy({ inventory: options.inventory, bindings: options.bindings, currentSource: options.currentSource,
      sourceQualifications: options.sourceQualifications, now: options.now,
      acceptedPins: options.acceptedPins, leaseRenewal: options.leaseRenewal }),
    priorBundles: Array.isArray(options.priorBundles) ? options.priorBundles.slice() : options.priorBundles,
  };
  require(leaseRenewal === undefined || (object(leaseRenewal) && typeof validateLeaseRenewal === 'function'),
    'validateLeaseRenewal is required with a renewal proof');
  require(Number.isSafeInteger(now) && now >= 0, 'explicit wall-clock now required');
  validateBindings(bindings, now);
  require(sourceValid(currentSource), 'missing current source fingerprint');
  require(Array.isArray(sourceQualifications), 'caller-approved source qualifications required');
  require(Array.isArray(priorBundles) && Array.isArray(acceptedPins), 'receipt bundles/pins must be arrays');
  const full = inventoryMap(inventory);
  const accepted = new Map(); const history = []; const bundles = []; const leaseLineage = [];
  const bundleIds = new Set(); const receiptIds = new Set();
  for (const blob of priorBundles) {
    const { value: bundle, reference: bundleReference } = verifyBlob(blob, readPriorBlob, 'bundle');
    require(!bundleIds.has(bundleReference.id), 'duplicate receipt bundle'); bundleIds.add(bundleReference.id);
    const priorInventory = inventoryMap(bundle.inventory);
    require([...priorInventory.keys()].every(id => full.has(id)), 'unknown or missing prior inventory case');
    require(sourceValid(bundle.sourceFingerprint), 'missing prior source fingerprint');
    const approvals = sourceQualifications.filter(approval => same(approval.prior, bundle.sourceFingerprint)
      && same(approval.current, currentSource) && nonempty(approval.approvalId)
      && hashPattern.test(approval.qualificationSha256));
    require(approvals.length === 1, 'missing or ambiguous caller-approved source qualification');
    const qualification = approvals[0];
    const lineage = validatePriorBindings(bindings, bundle.bindings, now, leaseRenewal, validateLeaseRenewal,
      { bundleId: bundleReference.id, bundleSha256: bundleReference.sha256, sourceQualification: copy(qualification) });
    if (lineage) leaseLineage.push(lineage);
    require(Array.isArray(bundle.cases), 'missing prior cases');
    const seen = new Set(); const caseIds = [];
    for (const prior of bundle.cases) {
      require(!receiptIds.has(prior.id), 'duplicate receipt identity'); receiptIds.add(prior.id);
      const { value: receipt, reference: receiptReference } = verifyBlob(prior, readPriorBlob, 'case');
      const caseId = caseIdentity(receipt);
      require(full.has(caseId), 'unknown prior case');
      require(!seen.has(caseId), 'duplicate prior case in bundle'); seen.add(caseId); caseIds.push(caseId);
      const receiptSource = receipt.document_inventory?.harness_source;
      // An interrupted write can lack its footer. Its approved, hashed bundle
      // preserves provenance for history only; it can never receive pass credit.
      require(receiptSource === undefined
        ? receipt.status !== 'passed'
        : receiptSource === bundle.sourceFingerprint.revision, 'receipt harness source mismatch');
      let reason = eligibility(receipt); let statistics = null;
      if (!reason) {
        try {
          const verdict = validatePriorCase(freeze({ receipt, inventoryCase: copy(full.get(caseId)),
            bindings: copy(bindings), sourceQualification: copy(qualification) }));
          if (verdict?.ok !== true || !object(verdict.statistics) || typeof verdict.then === 'function') reason = 'strict-validation-rejected';
          else statistics = copy(verdict.statistics);
        } catch { reason = 'strict-validation-rejected'; }
      }
      const record = { caseId, route: receipt.route, path: receipt.path, receiptId: prior.id, receiptSha256: prior.sha256, receiptReference,
        bundleId: bundleReference.id, bundleSha256: bundleReference.sha256, sourceQualification: copy(qualification) };
      history.push({ ...record, credited: reason === null, reason,
        leaseQualification: { ownerIdentity: copy(bundle.bindings.ownerIdentity), renewal: lineage } });
      if (!reason) {
        require(!accepted.has(caseId), 'multiple credited receipts for the same case');
        accepted.set(caseId, { ...record, measuredLoads: LOADS, statistics });
      }
    }
    bundles.push({ id: bundleReference.id, sha256: bundleReference.sha256, reference: bundleReference, caseIds, sourceQualification: copy(qualification) });
  }
  const pinned = new Set();
  for (const pin of acceptedPins) {
    require(full.has(pin.caseId) && !pinned.has(pin.caseId), 'unknown or duplicate accepted pin'); pinned.add(pin.caseId);
    const actual = accepted.get(pin.caseId);
    require(actual && ['receiptId', 'receiptSha256', 'bundleId', 'bundleSha256', 'sourceQualification']
      .every(key => same(actual[key], pin[key]))
      && (pin.receiptReference === undefined || same(actual.receiptReference, pin.receiptReference)),
    'accepted case hash/source cannot change or lose credit');
  }
  const acceptedCompleted = [...full.keys()].filter(id => accepted.has(id)).map(id => accepted.get(id));
  const remaining = inventory.filter(entry => !accepted.has(caseIdentity(entry)));
  // Filtering the complete actual inventory makes both subsets disjoint and exhaustive.
  require(acceptedCompleted.length + remaining.length === CASES, 'continuation union must be exactly 96');
  return freeze({ inventory, bindings, currentSource, plannedAt: now, bundles, acceptedCompleted, remaining,
    history, leaseLineage, measuredLoadsEach: LOADS, acceptedMeasuredLoads: acceptedCompleted.length * LOADS,
    remainingMeasuredLoads: remaining.length * LOADS, requiredMeasuredLoads: CASES * LOADS });
}

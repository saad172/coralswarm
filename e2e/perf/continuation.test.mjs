import { test } from 'node:test';
import assert from 'node:assert/strict';
import { caseIdentity, planContinuation, sha256 } from './continuation.mjs';

// All data is synthetic and in memory. The validator below is a contract fake,
// NOT runtime coverage of routeStatistics or browser/auth/model validation.
const digest = label => sha256(label);
const now = 1_000_000;
const inventory = Array.from({ length: 96 }, (_, i) => ({
  route: `/fixture/[id]/view-${i}`,
  path: `/fixture/00000000-0000-4000-8000-000000000001/view-${i}`,
}));
const bindings = {
  accountId: '00000000-0000-4000-8000-000000000002',
  actorId: '00000000-0000-4000-8000-000000000003', orgId: 'org_synthetic',
  runId: 'syntheticrun', stackId: 'pageloadl6synthetic',
  ownerIdentity: { namespace: 'nightly', owner: 'syntheticrun', lease: { id: 'synthetic-lease', expiresAt: now + 120 * 60_000 } },
  fixtureSha256: digest('safe fixture identity/count/fingerprint projection'), seedSha256: digest('seed'),
  backendSha256: digest('backend pin'), migratorSha256: digest('migrator pin'),
  web: { buildSha256: digest('web build'), source: 'synthetic-web-revision' },
};
const priorSource = { revision: 'ef1', sha256: digest('prior qualified harness source') };
const currentSource = { revision: '1463249a', sha256: digest('continuation qualified harness source') };
const qualification = { prior: priorSource, current: currentSource,
  approvalId: 'synthetic-root-approval', qualificationSha256: digest('caller source review') };
const clone = value => structuredClone(value);
const blob = (id, value) => {
  const json = JSON.stringify(value);
  return { id, json, sha256: sha256(json) };
};
const observation = path => ({ path, errors: [], document: { ok: true, fatal: false, reasons: [] }, records: [], ledger: [] });
function receipt(index, overrides = {}) {
  const entry = inventory[index];
  return { ...entry, status: 'passed', discovery: observation(entry.path),
    loads: Array.from({ length: 20 }, () => observation(entry.path)),
    validation_errors: [], refresh: { errors: [], ledger: [] },
    per_load_setup: Array.from({ length: 20 }, (_, i) => ({ load: i + 1, errors: [], attempts: [] })),
    document_inventory: { discovery_attempted: true, discovery_returned: true,
      measured_attempted: 20, measured_returned: 20, measured_not_run: 0, harness_source: priorSource.revision },
    ...overrides };
}
const bundle = (cases, overrides = {}, id = 'prior-bundle') => blob(id, {
  inventory, bindings, sourceFingerprint: priorSource,
  cases: cases.map((value, i) => blob(`${id}-route-${i}`, value)), ...overrides,
});
const options = (priorBundles = []) => ({ inventory, bindings, currentSource,
  sourceQualifications: [qualification], priorBundles, now,
  validatePriorCase: () => ({ ok: true, statistics: { fakeRecomputed: true } }) });
const plan = cases => planContinuation(options([bundle(cases)]));
const assertHistoryReference = (history, value, id) => {
  assert.deepEqual(history.receiptReference, { id, sha256: sha256(JSON.stringify(value)) });
  assert.equal(Object.hasOwn(history, 'receipt'), false);
};

test('remaining selection preserves partial proof and every unvisited identity in actual order', () => {
  const partial = receipt(22, { status: 'failed', error: 'synthetic interrupted observation',
    loads: Array.from({ length: 7 }, () => observation(inventory[22].path)),
    document_inventory: { discovery_attempted: true, discovery_returned: true,
      measured_attempted: 8, measured_returned: 7, measured_not_run: 12, harness_source: priorSource.revision } });
  const completed = Array.from({ length: 22 }, (_, i) => receipt(i));
  const prior = bundle([...completed, partial]);
  const result = planContinuation(options([prior]));
  assert.deepEqual(result.inventory, inventory);
  assert.deepEqual(result.acceptedCompleted.map(item => item.caseId), inventory.slice(0, 22).map(caseIdentity));
  assert.deepEqual(result.remaining, inventory.slice(22));
  assert.equal(result.acceptedMeasuredLoads, 440);
  assert.equal(result.remainingMeasuredLoads, 74 * 20);
  assert.equal(result.requiredMeasuredLoads, 1920);
  assert.equal(result.acceptedMeasuredLoads + result.remainingMeasuredLoads, 1920);
  assert.equal(result.history.length, 23);
  assertHistoryReference(result.history[22], partial, 'prior-bundle-route-22');
  assert.equal(prior.json, bundle([...completed, partial]).json);
  assert.equal(result.history[22].credited, false);
  assert.equal(result.history[22].reason, 'prior-failed-or-partial');
  assert.equal(result.bundles[0].sha256, prior.sha256);
  assert.deepEqual(result.bundles[0].caseIds, inventory.slice(0, 23).map(caseIdentity));
  assert.deepEqual(result.acceptedCompleted[0].sourceQualification, qualification);
});

test('passed twenty is not credited when the required strict validator rejects or throws', () => {
  for (const validator of [() => ({ ok: false }), () => true, () => undefined,
    () => ({ ok: true }), () => { throw new Error('strict route/model/auth rejection'); }]) {
    const input = options([bundle([receipt(0)])]); input.validatePriorCase = validator;
    const result = planContinuation(input);
    assert.equal(result.acceptedCompleted.length, 0);
    assert.deepEqual(result.remaining, inventory);
    assert.equal(result.history[0].reason, 'strict-validation-rejected');
  }
});

test('missing validator fails closed even with no prior receipts', () => {
  const input = options(); delete input.validatePriorCase;
  assert.throws(() => planContinuation(input), /validatePriorCase is required/);
});

test('foreign fixture/account/actor/org/run/stack/seed/backend/migrator/web binding rejects', () => {
  for (const key of ['fixtureSha256', 'accountId', 'actorId', 'orgId', 'runId', 'stackId',
    'seedSha256', 'backendSha256', 'migratorSha256', 'web']) {
    const foreign = clone(bindings);
    foreign[key] = key === 'web' ? { source: 'foreign' } : key.endsWith('Sha256') ? digest('foreign') : 'foreign';
    assert.throws(() => planContinuation(options([bundle([receipt(0)], { bindings: foreign })])), /binding|lease/);
  }
});

test('dead current/prior lease and changed exact lease id/deadline/owner/namespace reject', () => {
  for (const change of [owner => { owner.lease.expiresAt = now; },
    owner => { owner.lease.expiresAt += 1; }, owner => { owner.lease.id = 'another'; },
    owner => { owner.owner = 'another'; }, owner => { owner.namespace = 'e2e'; }]) {
    const foreign = clone(bindings); change(foreign.ownerIdentity);
    assert.throws(() => planContinuation(options([bundle([receipt(0)], { bindings: foreign })])), /lease/);
  }
  const input = options(); input.now = bindings.ownerIdentity.lease.expiresAt;
  assert.throws(() => planContinuation(input), /live nightly owner lease/);
  delete input.now;
  assert.throws(() => planContinuation(input), /explicit wall-clock/);
});

test('source changes need explicit qualified caller mapping, including identical source', () => {
  for (const mutate of [input => { input.sourceQualifications = []; },
    input => { input.currentSource = { ...currentSource, sha256: digest('unapproved current') }; },
    input => { input.sourceQualifications = [{ ...qualification, qualificationSha256: '' }]; },
    input => { input.sourceQualifications = [qualification, qualification]; },
    input => { input.currentSource = priorSource; input.sourceQualifications = []; }]) {
    const input = options([bundle([receipt(0)])]); mutate(input);
    assert.throws(() => planContinuation(input), /source qualification/);
  }
  assert.throws(() => planContinuation(options([bundle([receipt(0)], {
    sourceFingerprint: { ...priorSource, sha256: digest('arbitrary prior') },
  })])), /source qualification/);
  const wrong = receipt(0); wrong.document_inventory.harness_source = 'another-revision';
  assert.throws(() => plan([wrong]), /harness source mismatch/);
  assert.equal(plan([receipt(0)]).acceptedCompleted.length, 1);
});

test('missing/duplicate inventory and unknown prior inventory/case reject', () => {
  for (const bad of [inventory.slice(1), [...inventory.slice(1), inventory[1]],
    inventory.map((entry, i) => i === 0 ? { route: entry.route } : entry)]) {
    const input = options(); input.inventory = bad;
    assert.throws(() => planContinuation(input), /inventory/);
  }
  const unknownInventory = inventory.map((entry, i) => i === 0 ? { ...entry, path: '/unknown' } : entry);
  assert.throws(() => planContinuation(options([bundle([], { inventory: unknownInventory })])), /unknown or missing prior inventory/);
  assert.throws(() => planContinuation(options([bundle([], { inventory: inventory.slice(1) })])), /inventory/);
  assert.throws(() => planContinuation(options([bundle([], { inventory: [...inventory.slice(1), inventory[1]] })])), /duplicate inventory/);
  assert.throws(() => plan([receipt(0, { path: '/unknown' })]), /unknown prior case/);
  assert.throws(() => plan([receipt(0), receipt(0)]), /duplicate prior case/);
});

test('old failed attempts remain in history after a later fully validated case', () => {
  const failed = receipt(0, { status: 'failed', error: 'old synthetic failure' });
  const result = planContinuation(options([bundle([failed], {}, 'failed-run'), bundle([receipt(0)], {}, 'later-run')]));
  assert.equal(result.acceptedCompleted.length, 1);
  assert.equal(result.history.length, 2);
  assertHistoryReference(result.history[0], failed, 'failed-run-route-0');
  assert.equal(result.history[0].credited, false);
  assert.equal(result.history[1].credited, true);
  assert.equal(result.remaining.length, 95);
});

test('multiple credited receipts for one case reject; duplicate bundle/receipt IDs reject', () => {
  assert.throws(() => planContinuation(options([bundle([receipt(0)], {}, 'a'), bundle([receipt(0)], {}, 'b')])), /multiple credited receipts/);
  const prior = bundle([receipt(0)]);
  assert.throws(() => planContinuation(options([prior, prior])), /duplicate receipt bundle/);
  const a = blob('same-receipt-id', receipt(0)); const b = blob('same-receipt-id', receipt(1));
  assert.throws(() => planContinuation(options([bundle([], { cases: [a, b] })])), /duplicate receipt identity/);
});

test('count/document/error/setup defects never grant credit or reduce the remaining case load count', () => {
  const changes = [value => { value.document_inventory.measured_attempted = 21; },
    value => { value.document_inventory.measured_returned = 19; },
    value => { value.document_inventory.measured_not_run = -1; },
    value => { value.document_inventory.discovery_returned = false; },
    value => { value.loads.pop(); }, value => { value.discovery.document.ok = false; },
    value => { value.loads[19].document.fatal = true; },
    value => { delete value.loads[0].document.fatal; },
    value => { value.loads[1].errors.push('synthetic request failure'); },
    value => { value.loads[2].document.reasons.push('synthetic auth rejection'); },
    value => { value.validation_errors.push('synthetic validation failure'); },
    value => { value.errors = ['synthetic outer error']; },
    value => { value.refresh.errors.push('synthetic setup failure'); },
    value => { value.per_load_setup[0].attempts.push({ failure: 'synthetic setup abort' }); },
    value => { value.per_load_setup[0].attempts.push({ method: 'GET', status: 503, failure: null }); },
    value => { value.refresh.ledger.push({ method: 'POST', status: 500, failure: null }); },
    value => { value.per_load_setup[0].refreshed = true; },
    value => { value.per_load_setup[0].refresh = { errors: ['synthetic refresh failure'], ledger: [] }; },
    value => { value.per_load_setup.pop(); }];
  for (const change of changes) {
    const value = receipt(0); change(value);
    const input = options([bundle([value])]);
    input.validatePriorCase = () => { assert.fail('ineligible proof must not reach the validator'); };
    const result = planContinuation(input);
    assert.equal(result.acceptedCompleted.length, 0);
    assert.deepEqual(result.remaining, inventory);
    assert.equal(result.remainingMeasuredLoads, 1920);
    assert.notEqual(result.history[0].reason, 'strict-validation-rejected');
  }
});

test('successful real-shaped setup/refresh attempt statuses can be credited', () => {
  const value = receipt(0);
  value.refresh.ledger.push({ method: 'POST', status: 202, failure: null }, { method: 'GET', status: 200 });
  value.per_load_setup[0].attempts.push({ method: 'GET', status: 200, failure: null });
  value.per_load_setup[0].refreshed = true;
  value.per_load_setup[0].refresh = { errors: [], ledger: [{ method: 'GET', status: 200 }] };
  assert.equal(plan([value]).acceptedCompleted.length, 1);
});

test('accepted hashes cannot change: verify exact bundle and original receipt bytes', () => {
  const prior = bundle([receipt(0)]);
  assert.throws(() => planContinuation(options([{ ...prior, json: prior.json + ' ' }])), /hash mismatch/);
  const changed = JSON.parse(prior.json);
  changed.cases[0].json += ' ';
  assert.throws(() => planContinuation(options([blob(prior.id, changed)])), /hash mismatch/);
  const accepted = planContinuation(options([prior]));
  const input = options([prior]); input.acceptedPins = accepted.acceptedCompleted;
  assert.deepEqual(planContinuation(input).acceptedCompleted, accepted.acceptedCompleted);
  changed.cases[0] = blob(changed.cases[0].id, receipt(0, { changed: 'rehashed mutation' }));
  input.priorBundles = [blob(prior.id, changed)];
  assert.throws(() => planContinuation(input), /accepted case hash\/source cannot change/);
  input.priorBundles = [];
  assert.throws(() => planContinuation(input), /cannot change or lose credit/);
  assert.throws(() => { accepted.acceptedCompleted[0].receiptSha256 = digest('changed'); }, TypeError);
});

test('validator receives immutable exact proofs; planner leaves caller inputs untouched', () => {
  const input = options([bundle([receipt(0)])]);
  const original = clone({ ...input, validatePriorCase: undefined }); let calls = 0;
  input.validatePriorCase = ({ receipt: proof, inventoryCase, bindings: actual, sourceQualification }) => {
    calls++;
    assert.equal(Object.isFrozen(proof.loads[0].document), true);
    assert.deepEqual(inventoryCase, inventory[0]); assert.deepEqual(actual, bindings);
    assert.deepEqual(sourceQualification, qualification);
    assert.throws(() => { proof.loads[0].document.ok = false; }, TypeError);
    return { ok: true, statistics: { fakeRecomputed: 'strict callback result' } };
  };
  const result = planContinuation(input);
  assert.equal(calls, 1);
  assert.deepEqual(result.acceptedCompleted[0].statistics, { fakeRecomputed: 'strict callback result' });
  assert.deepEqual({ ...input, validatePriorCase: undefined }, original);
});

test('zero prior proofs and fully complete proofs keep exact 96 by 20 accounting', () => {
  const none = planContinuation(options());
  assert.equal(none.acceptedMeasuredLoads, 0); assert.deepEqual(none.remaining, inventory);
  const full = plan(inventory.map((_, i) => receipt(i)));
  assert.equal(full.acceptedCompleted.length, 96); assert.deepEqual(full.remaining, []);
  assert.equal(full.acceptedMeasuredLoads, 1920); assert.equal(full.remainingMeasuredLoads, 0);
  assert.equal(new Set(full.acceptedCompleted.map(item => item.caseId)).size, 96);
});

test('hashed interrupted raw history without a footer is preserved without credit', () => {
  const partial = receipt(50, { loads: Array.from({ length: 17 }, () => observation(inventory[50].path)) });
  delete partial.status; delete partial.document_inventory;
  const result = plan([receipt(0), partial]);
  assert.equal(result.acceptedCompleted.length, 1);
  assert.equal(result.history[1].credited, false);
  assertHistoryReference(result.history[1], partial, 'prior-bundle-route-1');
  assert.ok(result.remaining.some(item => caseIdentity(item) === caseIdentity(partial)));
  const unqualified = options([bundle([partial])]); unqualified.sourceQualifications = [];
  assert.throws(() => planContinuation(unqualified), /qualification/);
});

test('passed receipts still require explicit matching per-receipt source proof', () => {
  const passed = receipt(0); delete passed.document_inventory;
  assert.throws(() => plan([passed]), /source/);
  const partial = receipt(0, { status: 'failed' });
  partial.document_inventory.harness_source = 'foreign';
  assert.throws(() => plan([partial]), /source/);
});

// Synthetic proof references and callbacks ONLY. No actual renewal receipt or
// approval is available here; ROOT must supply and verify those independently.
function renewalOptions(cases = [receipt(0)]) {
  const input = options([bundle(cases)]);
  input.bindings = clone(bindings);
  input.bindings.ownerIdentity.lease.expiresAt += 120 * 60_000;
  input.now = bindings.ownerIdentity.lease.expiresAt + 1;
  input.leaseRenewal = { proof: { id: 'fake-renewal-receipt', sha256: digest('FAKE in-memory renewal proof') },
    verifiedAt: bindings.ownerIdentity.lease.expiresAt - 1,
    from: clone(bindings.ownerIdentity), to: clone(input.bindings.ownerIdentity) };
  input.validateLeaseRenewal = () => true;
  return input;
}

test('changed lease rejects without proof or a required renewal validator; default behavior remains exact', () => {
  const input = renewalOptions(); delete input.leaseRenewal;
  assert.throws(() => planContinuation(input), /binding\/lease mismatch/);
  input.leaseRenewal = renewalOptions().leaseRenewal; delete input.validateLeaseRenewal;
  assert.throws(() => planContinuation(input), /validateLeaseRenewal is required/);
  const unchanged = plan([receipt(0)]);
  assert.deepEqual(unchanged.leaseLineage, []);
});

test('verified pre-expiry renewal accepts old history with current live lease and records exact proof/source hashes', () => {
  const partial = receipt(1); delete partial.status; delete partial.document_inventory;
  const input = renewalOptions([receipt(0), partial]);
  const before = JSON.stringify(input.priorBundles); let calls = 0;
  input.validateLeaseRenewal = ({ lineage, priorBindings, bindings: current, now: at }) => {
    calls++;
    assert.deepEqual(priorBindings, bindings); assert.deepEqual(current, input.bindings);
    assert.deepEqual(lineage.from, bindings.ownerIdentity);
    assert.deepEqual(lineage.to, input.bindings.ownerIdentity);
    assert.deepEqual(lineage.proof, input.leaseRenewal.proof);
    assert.deepEqual(lineage.sourceQualification, qualification);
    assert.equal(lineage.bundleSha256, input.priorBundles[0].sha256);
    assert.ok(lineage.verifiedAt < lineage.from.lease.expiresAt && lineage.verifiedAt <= at);
    assert.equal(Object.isFrozen(lineage.from.lease), true);
    return true; // Fake stands for caller verification of ALL actual participants.
  };
  const result = planContinuation(input);
  assert.equal(calls, 1); assert.equal(result.leaseLineage.length, 1);
  assert.equal(result.acceptedCompleted.length, 1);
  assert.deepEqual(result.bindings, input.bindings);
  assertHistoryReference(result.history[1], partial, 'prior-bundle-route-1');
  assert.equal(result.history[1].credited, false);
  assert.ok(result.remaining.some(entry => caseIdentity(entry) === caseIdentity(partial)));
  assert.equal(JSON.stringify(input.priorBundles), before);
  assert.throws(() => { result.leaseLineage[0].proof.sha256 = digest('mutation'); }, TypeError);
  input.validatePriorCase = () => ({ ok: false });
  assert.equal(planContinuation(input).acceptedCompleted.length, 0);
});

test('renewal callback failures, unknown or partial participant updates reject instead of granting credit', () => {
  for (const validator of [() => false, () => undefined, () => ({ allParticipantsVerified: true }),
    () => ({ updated: 1, expected: 2 }), () => { throw new Error('unknown server readback'); }]) {
    const input = renewalOptions(); input.validateLeaseRenewal = validator;
    assert.throws(() => planContinuation(input), /all-participant lease renewal verification rejected/);
  }
});

test('renewal must be verified before OLD expiry and by now; new expiry must advance and remain live', () => {
  const oldExpiry = bindings.ownerIdentity.lease.expiresAt;
  for (const verifiedAt of [oldExpiry, oldExpiry + 1, -1, renewalOptions().now + 1, 1.5]) {
    const input = renewalOptions(); input.leaseRenewal.verifiedAt = verifiedAt;
    assert.throws(() => planContinuation(input), /lease|renewal verification time/);
  }
  const shortened = renewalOptions(); shortened.now = oldExpiry - 2;
  shortened.leaseRenewal.verifiedAt = oldExpiry - 3;
  shortened.bindings.ownerIdentity.lease.expiresAt = oldExpiry - 1;
  shortened.leaseRenewal.to = clone(shortened.bindings.ownerIdentity);
  assert.throws(() => planContinuation(shortened), /identity\/deadline mismatch/);
  const expired = renewalOptions(); expired.now = expired.bindings.ownerIdentity.lease.expiresAt;
  assert.throws(() => planContinuation(expired), /live nightly owner lease/);
});

test('renewal never permits owner/namespace/lease ID or other binding changes, even with a true callback', () => {
  const changes = [input => { input.bindings.ownerIdentity.owner = 'another'; input.bindings.runId = 'another'; },
    input => { input.bindings.ownerIdentity.namespace = 'e2e'; },
    input => { input.bindings.ownerIdentity.lease.id = 'another-lease'; },
    ...['accountId', 'actorId', 'orgId', 'runId', 'stackId'].map(key => input => { input.bindings[key] = 'another'; }),
    ...['fixtureSha256', 'seedSha256', 'backendSha256', 'migratorSha256'].map(key => input => { input.bindings[key] = digest('another'); }),
    input => { input.bindings.web.source = 'another'; },
    input => { input.bindings.ownerIdentity.lease.extra = 'unexpected'; }];
  for (const change of changes) {
    const input = renewalOptions(); change(input);
    input.leaseRenewal.to = clone(input.bindings.ownerIdentity);
    assert.throws(() => planContinuation(input), /binding|lease/);
  }
  const wrongFrom = renewalOptions(); wrongFrom.leaseRenewal.from.lease.expiresAt -= 1;
  assert.throws(() => planContinuation(wrongFrom), /identity\/deadline mismatch/);
  const wrongTo = renewalOptions(); wrongTo.leaseRenewal.to.lease.expiresAt += 1;
  assert.throws(() => planContinuation(wrongTo), /identity\/deadline mismatch/);
});

test('hashed proof reference and existing source qualification remain mandatory across renewal', () => {
  for (const proof of [undefined, { id: 'fake', sha256: '' }, { id: '', sha256: digest('fake') }]) {
    const input = renewalOptions(); input.leaseRenewal.proof = proof;
    assert.throws(() => planContinuation(input), /hashed renewal proof reference/);
  }
  const fork = renewalOptions(); fork.currentSource = { ...currentSource, sha256: digest('unqualified fork') };
  assert.throws(() => planContinuation(fork), /source qualification/);
});

test('renewal preserves earlier accepted pins without changing raw history or accepting rehashed rebinding', () => {
  const accepted = plan([receipt(0)]);
  const input = renewalOptions(); input.acceptedPins = accepted.acceptedCompleted;
  const renewed = planContinuation(input);
  assert.deepEqual(renewed.acceptedCompleted, accepted.acceptedCompleted);
  const withoutLease = history => history.map(({ leaseQualification, ...entry }) => entry);
  assert.deepEqual(withoutLease(renewed.history), withoutLease(accepted.history));
  assert.deepEqual(renewed.history[0].leaseQualification.ownerIdentity, bindings.ownerIdentity);
  assert.deepEqual(renewed.history[0].leaseQualification.renewal, renewed.leaseLineage[0]);
  const changed = JSON.parse(input.priorBundles[0].json);
  changed.cases[0] = blob(changed.cases[0].id, receipt(0, { tampered: true }));
  input.priorBundles = [blob(input.priorBundles[0].id, changed)];
  assert.throws(() => planContinuation(input), /accepted case hash\/source cannot change/);
});

function lazyEvidence(cases) {
  const storage = new Map();
  const references = cases.map((value, i) => {
    const path = `/fake/route-${String(i).padStart(3, '0')}.json`;
    const json = JSON.stringify(value); storage.set(path, json);
    return { id: `lazy-case-${i}`, sha256: sha256(json), path };
  });
  const manifest = blob('lazy-manifest', { inventory, bindings, sourceFingerprint: priorSource, cases: references });
  storage.set('/fake/manifest.json', manifest.json);
  const reference = { id: manifest.id, sha256: manifest.sha256, path: '/fake/manifest.json' };
  const input = options([reference]);
  input.readPriorBlob = ref => storage.get(ref.path ?? ref.locator);
  return { input, storage, references };
}

test('lazy loader verifies manifest first and validates exactly one case before loading the next', () => {
  const { input, references } = lazyEvidence([receipt(0), receipt(1)]);
  const read = input.readPriorBlob; const events = [];
  input.readPriorBlob = (ref, { kind }) => {
    assert.equal(Object.isFrozen(ref), true);
    events.push(`load:${kind}:${ref.id}`); return read(ref);
  };
  input.validatePriorCase = ({ receipt: value }) => {
    events.push(`validate:${caseIdentity(value)}`);
    return { ok: true, statistics: { fakeRecomputed: true } };
  };
  const result = planContinuation(input);
  assert.deepEqual(events, ['load:bundle:lazy-manifest', 'load:case:lazy-case-0', `validate:${caseIdentity(inventory[0])}`,
    'load:case:lazy-case-1', `validate:${caseIdentity(inventory[1])}`]);
  assert.deepEqual(result.history.map(entry => entry.receiptReference), references);
  assert.deepEqual(result.bundles[0].reference, input.priorBundles[0]);
  assert.throws(() => { result.history[0].receiptReference.path = '/changed'; }, TypeError);
});

test('loader is required for absent inline JSON, supports opaque locators, and rejects missing locators', () => {
  const { input } = lazyEvidence([receipt(0)]);
  delete input.readPriorBlob;
  assert.throws(() => planContinuation(input), /readPriorBlob is required/);
  const caseOnly = options([bundle([], { cases: [{ id: 'case-ref', sha256: digest('fake'), path: '/fake/case' }] })]);
  assert.throws(() => planContinuation(caseOnly), /readPriorBlob is required/);
  const lazy = lazyEvidence([receipt(0)]);
  lazy.input.priorBundles[0].locator = lazy.input.priorBundles[0].path;
  delete lazy.input.priorBundles[0].path;
  assert.equal(planContinuation(lazy.input).acceptedCompleted.length, 1);
  delete lazy.input.priorBundles[0].locator;
  assert.throws(() => planContinuation(lazy.input), /requires path or locator/);
});

test('bundle and case loaders reject Promise and wrong return types without strict credit', () => {
  for (const kind of ['bundle', 'case']) {
    for (const wrong of [undefined, null, 1, {}, Buffer.from('{}'), Promise.resolve('{}')]) {
      const { input } = lazyEvidence([receipt(0)]); const read = input.readPriorBlob;
      let validations = 0;
      input.readPriorBlob = (ref, context) => context.kind === kind ? wrong : read(ref);
      input.validatePriorCase = () => { validations++; return { ok: true, statistics: {} }; };
      assert.throws(() => planContinuation(input), /synchronous JSON string/);
      assert.equal(validations, 0);
    }
  }
});

test('exact manifest/case hash mismatch rejects before parsing, later loads or strict validation', () => {
  for (const path of ['/fake/manifest.json', '/fake/route-000.json']) {
    const { input, storage } = lazyEvidence([receipt(0), receipt(1)]);
    storage.set(path, 'not JSON: exact bytes changed');
    const read = input.readPriorBlob; const loaded = []; let validations = 0;
    input.readPriorBlob = ref => { loaded.push(ref.path); return read(ref); };
    input.validatePriorCase = () => { validations++; return { ok: true, statistics: {} }; };
    assert.throws(() => planContinuation(input), /receipt hash mismatch/);
    assert.deepEqual(loaded, path.endsWith('manifest.json') ? ['/fake/manifest.json'] : ['/fake/manifest.json', path]);
    assert.equal(validations, 0);
  }
});

test('51 lazy case references retain all history, including partial 050, with exactly 48 accepted and no output bodies', () => {
  const marker = 'SYNTHETIC-RAW-BODY-MUST-NOT-ENTER-PLAN';
  const cases = Array.from({ length: 51 }, (_, i) => receipt(i, { rawBody: marker }));
  cases[48].status = 'failed'; cases[49].status = 'failed';
  delete cases[50].status; delete cases[50].document_inventory; cases[50].loads.length = 17;
  const { input, storage, references } = lazyEvidence(cases);
  const originals = [...storage];
  const result = planContinuation(input);
  assert.equal(result.history.length, 51);
  assert.equal(result.acceptedCompleted.length, 48); assert.equal(result.remaining.length, 48);
  assert.deepEqual(result.remaining, inventory.slice(48));
  assert.equal(result.acceptedMeasuredLoads, 960); assert.equal(result.remainingMeasuredLoads, 960);
  assert.equal(result.requiredMeasuredLoads, 1920);
  assert.equal(result.history[50].caseId, caseIdentity(inventory[50]));
  assert.equal(result.history[50].credited, false);
  assert.deepEqual(result.history.map(entry => entry.receiptReference), references);
  const serialized = JSON.stringify(result);
  assert.equal(serialized.includes(marker), false);
  for (const entry of result.history) {
    assert.equal(Object.hasOwn(entry, 'receipt'), false);
    assert.equal(Object.hasOwn(entry.receiptReference, 'json'), false);
  }
  assert.deepEqual([...storage], originals);
  input.acceptedPins = result.acceptedCompleted;
  assert.deepEqual(planContinuation(input).acceptedCompleted, result.acceptedCompleted);
  input.acceptedPins = clone(result.acceptedCompleted);
  input.acceptedPins[0].receiptReference.path = '/rebound';
  assert.throws(() => planContinuation(input), /accepted case hash\/source cannot change/);
});

test('never JSON-copies whole options, priorBundles or inline blob wrappers, even for one-case bundles', () => {
  const input = options([bundle([receipt(0)], {}, 'one'), bundle([receipt(1)], {}, 'two')]);
  const forbidden = new Set([input, input.priorBundles, ...input.priorBundles]);
  for (const value of forbidden) value.toJSON = () => { throw new Error('aggregate JSON copy forbidden'); };
  const stringify = JSON.stringify;
  JSON.stringify = function(value, ...args) {
    assert.equal(forbidden.has(value), false, 'aggregate was passed to JSON.stringify');
    return stringify(value, ...args);
  };
  let result;
  try { result = planContinuation(input); }
  finally { JSON.stringify = stringify; }
  assert.equal(result.acceptedCompleted.length, 2);
  assert.equal(result.history.length, 2);
});

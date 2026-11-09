import { isDeepStrictEqual as same } from 'node:util';
import { planContinuation, caseIdentity, sha256 } from './continuation.mjs';

const require = (condition, message) => { if (!condition) throw new Error(message); };
const canonical = value => {
  if (Array.isArray(value)) return value.map(canonical);
  if (value && typeof value === 'object') return Object.fromEntries(Object.keys(value).sort().map(key => [key, canonical(value[key])]));
  return value;
};
export const bindingHash = value => sha256(JSON.stringify(canonical(value)));
export function checkedBlob(blob, readPriorBlob, kind = 'case') {
  const json = blob?.json ?? (typeof readPriorBlob === 'function' ? readPriorBlob(blob, { kind }) : undefined);
  require(blob && typeof json === 'string' && sha256(json) === blob.sha256, 'missing or changed proof blob');
  return JSON.parse(json);
}
export function continuationBindings({ fixture, canonicalSeed, identity, stackId, backendSha256, migrators, web }) {
  const safeFixture = Object.fromEntries(['userId','orgId','db','account','actor','graph','sharedGraph','node','atom','source','sourceItem','conversations'].map(key => [key, fixture[key]]));
  return { accountId: fixture.account, actorId: fixture.actor, clerkUserId: fixture.userId, orgId: fixture.orgId,
    runId: identity.owner, stackId, ownerIdentity: identity,
    fixtureSha256: bindingHash({ fixture: safeFixture, representativeValuesSha256: bindingHash(fixture.values), canonicalSeed }),
    seedSha256: canonicalSeed.sql_sha256, backendSha256, migratorSha256: bindingHash(migrators), web };
}

/** Verify the actual all-participant SDK receipt, never a constant true callback. */
export function verifyRenewal({ lineage, priorBindings, bindings, now }, input) {
  const receipt = checkedBlob(input.renewalProof); const approval = checkedBlob(input.renewalApprovalProof);
  require(approval.approved === true && approval.approvalId && approval.receiptSha256 === input.renewalProof.sha256
    && approval.operationCodeSha256 === receipt.operationCodeSha256 && approval.authorizationSha256 === receipt.authorizationSha256
    && approval.participantSetProofSha256 === receipt.participantSetProofSha256, 'missing actual root-approved renewal receipt/source');
  require(lineage.proof.id === input.renewalProof.id && lineage.proof.sha256 === input.renewalProof.sha256
    && same(receipt.from, lineage.from) && same(receipt.to, lineage.to) && same(receipt.from, priorBindings.ownerIdentity)
    && same(receipt.to, bindings.ownerIdentity) && receipt.verifiedAt === lineage.verifiedAt, 'renewal lineage differs from server proof');
  require(receipt.status === 'verified-all-participants-before-old-expiry' && receipt.participantCount === 2
    && receipt.updatedCount === 2 && receipt.verifiedCount === 2 && receipt.onlyExpiryChanged === true
    && receipt.noSessionOrTokenCalls === true && receipt.organizationOrRoleCalls === 0, 'partial or unknown renewal rejected');
  require(receipt.beforeReadAtUnixMs <= receipt.renewedAtUnixMs && receipt.renewedAtUnixMs <= receipt.verifiedAt
    && receipt.verifiedAt < receipt.from.lease.expiresAt && receipt.verifiedAt <= now
    && receipt.to.lease.expiresAt === receipt.renewedAtUnixMs + 120 * 60_000, 'renewal timestamps not actual pre-expiry verification');
  require(bindingHash(receipt.authorization) === receipt.authorizationSha256 && bindingHash(receipt.before) === receipt.beforeSha256
    && bindingHash(receipt.after) === receipt.afterSha256, 'renewal canonical metadata hashes changed');
  const ids = [...receipt.participantIds].sort();
  require(ids.length === 2 && new Set(ids).size === 2 && same(ids, [...approval.participantIds].sort())
    && same(ids, receipt.participants.map(p => p.userId).sort()), 'renewal participant set differs');
  for (const p of receipt.participants) {
    require(['sameUser','sameNamespaceOwnerLeaseId','newExpiryExact','allEmailsMatch','allPrivateMetadataPreservedExceptExpiry','profilePublicEmailNameUnchanged'].every(k => p.checks[k] === true), 'renewal participant verification incomplete');
    require(same(p.before, receipt.before.find(u => u.userId === p.userId)) && same(p.after, receipt.after.find(u => u.userId === p.userId))
      && p.before.allEmailsMatch === true && p.after.allEmailsMatch === true && p.before.owner === lineage.from.owner
      && p.after.owner === lineage.to.owner && p.before.namespace === 'nightly' && p.after.namespace === 'nightly'
      && same(p.before.lease, lineage.from.lease) && same(p.after.lease, lineage.to.lease)
      && p.before.profileSha256 === p.after.profileSha256 && p.before.publicMetadataSha256 === p.after.publicMetadataSha256, 'renewal metadata/identity changed beyond deadline');
  }
  require(receipt.calls.length === 6, 'unexpected SDK renewal request count');
  for (const [stage, method] of [['before-get','GET'],['update','PATCH'],['after-get','GET']]) {
    const calls = receipt.calls.filter(c => c.stage === stage);
    require(calls.length === 2 && ids.every(id => calls.some(c => c.path === `/v1/users/${id}${method === 'PATCH' ? '/metadata' : ''}`))
      && calls.every(c => c.method === method && c.status === 200 && c.started <= c.finished && c.finished <= receipt.verifiedAt), 'renewal SDK server receipt mismatch');
  }
  for (const hash of [input.renewalProof.sha256, receipt.operationCodeSha256, receipt.participantSetProofSha256]) require(input.artifacts.some(a => a.sha256 === hash), 'renewal source/proof artifact not pinned');
  return true;
}

/** The caller supplies only the actual app resolver/classifier/statistics.
 * Original principal metadata comes from HASHED original trace extraction,
 * approved with this source qualification, never the continuation login. */
export function strictReceipt(receipt, { principal, declaration, bindings, classifyDocument, safeDocumentUrl, resolveAlias, routeStatistics, guardWitness, validateSetupBody }) {
  require(principal?.kind === 'original-clerk-principal' && principal.sessionId && principal.userId === bindings.clerkUserId
    && principal.orgId === bindings.orgId && principal.scope === JSON.stringify([principal.issuer, principal.userId, principal.orgId]), 'missing original authoritative principal proof');
  require(guardWitness?.checkedBeforeRedaction === true && /^[a-f0-9]{64}$/.test(guardWitness.sourceSha256), 'missing approved before-redaction destination witness');
  require(receipt.status === 'passed' && receipt.loads?.length === 20 && receipt.validation_errors?.length === 0, 'prior case is not complete successful twenty');
  const counts = receipt.document_inventory;
  require(counts?.discovery_attempted === true && counts.discovery_returned === true && counts.measured_attempted === 20 && counts.measured_returned === 20 && counts.measured_not_run === 0, 'missing exact successful document counts');
  require(typeof validateSetupBody === 'function' && receipt.per_load_setup?.length === 20, 'missing actual setup validator/proof');
  for (const [i, setup] of receipt.per_load_setup.entries()) {
    require(setup.load === i + 1 && setup.errors?.length === 0 && !setup.error && Array.isArray(setup.attempts), 'failed original healthy setup');
    for (const attempt of setup.attempts) { require(!attempt.failure && attempt.finished != null && ((attempt.method === 'GET' && attempt.status === 200) || (attempt.method === 'POST' && attempt.status === 202)), 'failed original setup request');
      if (attempt.method === 'GET') require(validateSetupBody(attempt.path, attempt.body) === true, 'original Stored setup certificate/payload invalid');
    }
  }
  const reads = receipt.discovered_reads;
  require(Array.isArray(reads), 'missing actual discovery model mapping');
  for (const [index, load] of [receipt.discovery, ...receipt.loads].entries()) {
    require(load && load.path === receipt.path && Array.isArray(load.errors) && load.errors.length === 0, 'missing original document observation');
    const document = load.document;
    require(document?.ok === true && document.fatal === false && document.reasons?.length === 0
      && document.kind === (declaration.access === 'public' ? 'public-document' : 'authenticated-document'), 'original source guard did not certify document');
    // Both strings are actual ORIGINAL redacted receipts. No unredacted final
    // URL is invented. Root's hashed source qualification witnesses the old
    // guard's strict equality check BEFORE these fields were redacted.
    const requested = safeDocumentUrl(load.path, declaration.webBase);
    require(typeof requested === 'string' && typeof document.final_url === 'string', 'missing original top-level destination proof');
    const expected = { ...declaration, sessionId: principal.sessionId, userId: principal.userId,
      orgId: principal.orgId, scope: principal.scope, expectedReads: index ? reads : [] };
    const result = classifyDocument({ requested, finalUrl: document.final_url, expected, browser: document.browser,
      records: load.records, ledger: load.ledger, errors: load.errors, frames: document.frames, resolveAlias });
    require(result.ok === true && result.fatal === false, 'strict original document revalidation failed');
    require(document.canonical_destination === result.canonical_destination, 'original canonical destination proof differs');
    if (declaration.access !== 'public') require(document.api_scope_matches_expected === true, 'missing original browser API scope proof');
  }
  return { ok: true, statistics: routeStatistics(receipt.loads, receipt.discovery) };
}

/** Pure assembly. I/O reads/hashes and actual runtime binding construction are
 * explicit caller duties; every supplied root approval and original blob is
 * hashed. There is no authorization inferred from a passed status. */
export function assembleContinuation({ inputText, inputSha256, inventory, bindings, currentSource, now,
  declarationFor, classifyDocument, safeDocumentUrl, resolveAlias, routeStatistics, verifyArtifact, validateSetupBody, readPriorBlob }) {
  require(sha256(inputText) === inputSha256, 'continuation input hash mismatch');
  const input = JSON.parse(inputText);
  require(input.version === 1 && same(input.inventory, inventory), 'continuation requires full actual inventory');
  require(same(input.bindings, bindings) && same(input.currentSource, currentSource), 'continuation runtime/source binding mismatch');
  require(typeof verifyArtifact === 'function' && Array.isArray(input.artifacts) && input.artifacts.length > 0, 'pinned original artifacts required');
  for (const artifact of input.artifacts) require(verifyArtifact(artifact) === true, 'original artifact changed or missing');
  require(Array.isArray(input.priors) && input.priors.length > 0, 'prior proof bundle required');
  const qualifications = []; const contexts = new Map(); const fallback = new Map(); let activeContext;
  for (const prior of input.priors) {
    const bundle = checkedBlob(prior.bundle, readPriorBlob, 'bundle'); const principal = checkedBlob(prior.principalProof);
    const approval = checkedBlob(prior.qualificationProof);
    require(approval.approved === true && approval.approvalId && same(approval.prior, bundle.sourceFingerprint) && same(approval.current, currentSource)
      && approval.bindingsSha256 === bindingHash(bindings) && approval.inventorySha256 === bindingHash(inventory)
      && approval.principalProofSha256 === prior.principalProof.sha256
      && approval.bundleSha256 === prior.bundle.sha256 && approval.priorBindingsSha256 === bindingHash(bundle.bindings)
      && (!input.leaseRenewal || approval.leaseRenewalProofSha256 === input.renewalProof?.sha256), 'missing root-approved exact source/proof qualification');
    require(input.artifacts.some(a => a.sha256 === principal.traceSha256), 'original principal trace is not pinned');
    require(input.artifacts.some(a => a.sha256 === approval.destinationGuard?.sourceSha256), 'original source guard is not pinned');
    require(same(principal.sourceFingerprint, bundle.sourceFingerprint), 'original principal source mismatch');
    const qualification = { prior: approval.prior, current: approval.current, approvalId: approval.approvalId,
      qualificationSha256: prior.qualificationProof.sha256 };
    if (!qualifications.some(q => same(q, qualification))) qualifications.push(qualification);
    const context = { principal, guardWitness: approval.destinationGuard, qualificationSha256: qualification.qualificationSha256 };
    if (fallback.has(context.qualificationSha256)) require(same(fallback.get(context.qualificationSha256).principal, principal), 'inline ambiguous original principal proof');
    fallback.set(context.qualificationSha256, context);
    for (const ref of bundle.cases) contexts.set(ref.id, { ...context, reference: ref });
  }
  const plan = planContinuation({ inventory, bindings, currentSource, now,
    priorBundles: input.priors.map(prior => prior.bundle), sourceQualifications: qualifications, acceptedPins: input.acceptedPins ?? [], leaseRenewal: input.leaseRenewal,
    validateLeaseRenewal: context => verifyRenewal(context, input),
    readPriorBlob: (ref, kind) => {
      require(typeof readPriorBlob === 'function', 'synchronous original artifact reader required');
      if (kind.kind === 'case') {
        activeContext = contexts.get(ref.id);
        require(activeContext && activeContext.reference.sha256 === ref.sha256, 'unknown original case reference');
      }
      return readPriorBlob(ref, kind);
    },
    validatePriorCase: ({ receipt, inventoryCase, sourceQualification }) => strictReceipt(receipt, {
      ...(activeContext?.qualificationSha256 === sourceQualification.qualificationSha256 ? activeContext : fallback.get(sourceQualification.qualificationSha256)),
      declaration: declarationFor(inventoryCase), bindings, classifyDocument, safeDocumentUrl, resolveAlias, routeStatistics, validateSetupBody,
    }) });
  return { inputSha256, plan };
}

/** Fresh selected cases keep the unchanged twenty-load checks. The accepted
 * prior partition is never navigated, dropped, or replaced by current login. */
export function finalContinuationUnion(plan, currentReceipts, validateCurrent, readCurrentBlob) {
  require(currentReceipts.length === plan.remaining.length, 'selected continuation cases incomplete');
  const remaining = new Set(plan.remaining.map(caseIdentity)); const seen = new Set(); const current = [];
  for (const blob of currentReceipts) {
    const receipt = checkedBlob(blob, readCurrentBlob, 'case');
    const id = caseIdentity(receipt); require(remaining.has(id) && !seen.has(id), 'unexpected or duplicate measured continuation identity'); seen.add(id);
    require(receipt.document_inventory?.harness_source === plan.currentSource.revision, 'new receipt source mismatch');
    const verdict = validateCurrent(receipt); require(verdict?.ok === true, 'new continuation proof failed');
    current.push({ caseId: id, measuredLoads: 20, receiptId: blob.id, receiptSha256: blob.sha256, sourceFingerprint: plan.currentSource, statistics: verdict.statistics });
  }
  require(plan.acceptedCompleted.length + current.length === 96 && plan.acceptedMeasuredLoads + current.length * 20 === 1920, 'final union is not all96 times20');
  return { inventory: plan.inventory, acceptedPrior: plan.acceptedCompleted, acceptedCurrent: current,
    leaseLineage: plan.leaseLineage, sourceQualifications: plan.bundles.map(bundle => bundle.sourceQualification), history: plan.history,
    acceptedRepresentatives: 96, acceptedMeasuredLoads: 1920 };
}

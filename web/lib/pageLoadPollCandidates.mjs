// Internal eligibility registry shared by the pure ledger and Node adapter.
// No callback/interval resolver is accepted; a lookalike ledger cannot supply
// candidate data. Only StoredReadLedger.start records after its original checks.
const candidates = new WeakMap();
export function recordPollTimingCandidate(owner, attempt, previous, model, owedFrom, minimumSpacingMs) {
  let group = candidates.get(owner);
  if (!group) candidates.set(owner, group = new WeakMap());
  group.set(attempt, { previous, model, owedFrom: owedFrom.map(value => ({ ...value })), minimumSpacingMs });
}
export function readPollTimingCandidate(owner, attempt, previous) {
  const value = candidates.get(owner)?.get(attempt);
  if (!value || value.previous !== previous) return undefined;
  return { model: value.model, minimumSpacingMs: value.minimumSpacingMs, owedFrom: value.owedFrom.map(evidence => ({ ...evidence })) };
}

import { test, expect } from '@playwright/test';
import { createHash } from 'node:crypto';
import { StoredReadLedger, type LoadAttempt } from '../../web/lib/pageLoadEvidence';
import { validateSemanticTiming, applyConfirmedSemanticTiming } from '../perf/semantic-timing.mjs';
const address = '/v1/load/oceans', scope = '["fixture","actor",null]', nonce = '11111111-1111-4111-8111-111111111111';
const digest = createHash('sha256').update(`${nonce}\0${scope}`).digest('hex');
const uuid = (n: number) => `00000000-0000-4000-8000-${String(n).padStart(12, '0')}`;
function fixture(count = 9, interval = 2000, nodeGap = 1942) {
  const body = (fresh: boolean) => ({ components: Array.from({ length: count }, (_, i) => ({ account_id: uuid(i + 1), anchor_graph_id: i ? uuid(i + 100) : null,
    refresh_path: `/v1/load/oceans/component/refresh${i ? `?graph=${uuid(i + 100)}` : ''}`,
    stored: { model: 'oceans_index', version: 1, computed_at: !fresh && i % 3 === 0 ? null : fresh ? '2026-11-01T07:51:07Z' : '2026-11-01T07:49:39Z',
      stale: !fresh && i % 3 !== 2, refresh_eta: !fresh && i % 3 !== 2 ? '2026-11-01T07:51:08Z' : null,
      payload: { oceans: [], members: [], team_members: {}, archived: null } },
  })) });
  const prior = body(false), next = body(true), ledger = new StoredReadLedger();
  const a = { address, path: address, identity: `${scope}|${address}`, method: 'GET', started: 0, finished: null, status: 200, ledger: 'initial', model: null, computedAt: null, refreshEta: null, priority: 'critical', phase: 'initial' } as LoadAttempt & { priority: string; phase: string };
  ledger.start(a); a.finished = 158; ledger.response(a, prior);
  const b = { ...a, started: 158 + nodeGap, finished: null, certificates: undefined, ledger: 'initial', model: null } as typeof a;
  ledger.start(b); b.finished = b.started + 40; ledger.response(b, next);
  const rows = [a,b], records = rows.map(r => ({ ...r, scope }));
  const projection = (value: ReturnType<typeof body>) => value.components.map(c => ({ identity: c.account_id, anchorGraphId: c.anchor_graph_id, refreshPath: c.refresh_path,
    certificate: { model: c.stored.model, version: c.stored.version, computedAt: c.stored.computed_at, stale: c.stored.stale, refreshEta: c.stored.refresh_eta } }));
  const child = 100 + interval;
  const events = [
    { kind: 'run', at: 0, run: 1, query: 1, model: 'oceans_index', address, parent: null },
    { kind: 'dispatch', at: 0, attempt: 1, run: 1, address, method: 'GET', priority: 'critical', scopeDigest: digest },
    { kind: 'response', at: 50, attempt: 1, status: 200 },
    { kind: 'admission', at: 99, run: 1, query: 1, attempt: 1, components: projection(prior) },
    { kind: 'cache-success', at: 100, run: 1, query: 1, admission: 4, manual: false },
    { kind: 'run', at: child, run: 2, query: 1, model: 'oceans_index', address, parent: 4 },
    { kind: 'dispatch', at: child, attempt: 2, run: 2, address, method: 'GET', priority: 'critical', scopeDigest: digest },
    { kind: 'response', at: child + 10, attempt: 2, status: 200 },
    { kind: 'admission', at: child + 20, run: 2, query: 1, attempt: 2, components: projection(next) },
    { kind: 'cache-success', at: child + 21, run: 2, query: 1, admission: 9, manual: false },
  ].map((event, i) => ({ ...event, seq: i + 1 }));
  return { rows, records, ledger, snapshot: { version: 1, nonce, clock: 'browser-performance', errors: [], events } };
}
for (const count of [1,9]) test(`full ${count}-constituent mixed certificates authorize only original owed subset and preserve raw Node evidence`, () => {
  const f = fixture(count), candidate = f.ledger.pollTimingCandidate(f.rows[1], f.rows[0]);
  expect(candidate?.owedFrom).toHaveLength(Array.from({ length: count }, (_, i) => i).filter(i => i % 3 !== 2).length);
  const proof = validateSemanticTiming(f.snapshot, nonce, f.records, f.rows, f.ledger);
  expect(proof.errors).toEqual([]); expect(proof.ok).toBe(true);
  expect(applyConfirmedSemanticTiming(proof, f.records, f.rows, f.ledger).applied).toHaveLength(1);
  expect(f.rows[1].ledger).toBe('owed-refresh'); expect(f.rows[1].owedFrom).toEqual(candidate?.owedFrom);
  expect(f.rows[1].started - f.rows[0].finished!).toBe(1942);
});
test('true browser1949 downgrades an otherwise eligible fan-in Node2000 interval', () => {
  const f = fixture(9,1949,2000), proof = validateSemanticTiming(f.snapshot,nonce,f.records,f.rows,f.ledger);
  expect(proof.ok).toBe(true); expect(f.rows[1].ledger).toBe('owed-refresh');
  expect(applyConfirmedSemanticTiming(proof,f.records,f.rows,f.ledger).applied).toHaveLength(1);
  expect(f.rows[1].ledger).toBe('initial');
});
for (const [name, edit] of Object.entries<(f: ReturnType<typeof fixture>) => void>({
  model: f => { f.snapshot.events[3].components![0].certificate.model = 'home_metrics'; },
  stale: f => { f.snapshot.events[3].components![0].certificate.stale = false; },
  reordered: f => { f.snapshot.events[3].components!.reverse(); },
  unknownField: f => { Object.assign(f.snapshot.events[3].components![0], { payload: {} }); },
  emptyOmitted: f => { f.snapshot.events[3].components = []; },
  bound: f => { f.snapshot.events[3].components = Array.from({ length: 502 }, (_, i) => ({ ...f.snapshot.events[3].components![0], identity: uuid(i + 1) })); },
  anchorAndPath: f => { const c = f.snapshot.events[3].components![1]; c.anchorGraphId = uuid(999); c.refreshPath = `/v1/load/oceans/component/refresh?graph=${c.anchorGraphId}`; },
  omitted: f => { f.snapshot.events[3].components!.pop(); },
  added: f => { f.snapshot.events[3].components!.push({ ...f.snapshot.events[3].components![0], identity: uuid(99) }); },
  duplicate: f => { f.snapshot.events[3].components![1] = { ...f.snapshot.events[3].components![0] }; },
  crossAccount: f => { f.snapshot.events[3].components![0].identity = uuid(99); },
  anchor: f => { f.snapshot.events[3].components![1].anchorGraphId = uuid(999); },
  refreshPath: f => { f.snapshot.events[3].components![1].refreshPath += '&wrong=1'; },
  version: f => { f.snapshot.events[3].components![0].certificate.version = 999; },
  certificate: f => { f.snapshot.events[3].components![0].certificate.refreshEta = null; },
  childOmitted: f => { f.snapshot.events[8].components!.pop(); },
  outerOnly: f => { const row = f.snapshot.events[3]; Object.assign(row, { certificate: row.components![0].certificate }); delete row.components; },
  childVersion: f => { f.snapshot.events[8].components![0].certificate.version = 999; },
  omittedChildCertificate: f => { delete (f.snapshot.events[8].components![0] as { certificate?: unknown }).certificate; },
  outerCertificate: f => { Object.assign(f.snapshot.events[3], { certificate: f.snapshot.events[3].components![0].certificate }); },
  wrongScope: f => { f.snapshot.events[1].scopeDigest = '0'.repeat(64); },
  wrongParent: f => { f.snapshot.events[5].query = 2; },
  responseOrdering: f => { f.snapshot.events[3].attempt = 2; },
  manualOnly: f => { f.snapshot.events[4].manual = true; },
})) test(`fan-in ${name} fails closed without a partial-set rescue`, () => {
  const f = fixture(); edit(f); const proof = validateSemanticTiming(f.snapshot,nonce,f.records,f.rows,f.ledger);
  expect(proof.ok).toBe(false); expect(applyConfirmedSemanticTiming(proof,f.records,f.rows,f.ledger).applied).toHaveLength(0);
  expect(f.rows[1].ledger).toBe('initial');
});
test('post-validation constituent mutation invalidates runtime capability', () => {
  const f = fixture(), proof = validateSemanticTiming(f.snapshot,nonce,f.records,f.rows,f.ledger);
  expect(proof.ok).toBe(true); f.rows[0].certificates![0].refreshPath += '?changed=1';
  expect(applyConfirmedSemanticTiming(proof,f.records,f.rows,f.ledger).applied).toHaveLength(0);
});

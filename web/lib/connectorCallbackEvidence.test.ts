import { expect, it } from "vitest";
import { ConnectorCallbackPhase, type LoadAttempt } from "./pageLoadEvidence";
import { refreshLimits, refreshRecoveryMs } from "./pageRefreshPolicy";
const source = { key: 'channels', label: 'Channels', enabled: true, paused: false, status: 'active', scope_shape: 'items', scope: { mode: 'pick', count: 0, noun: 'channels' }, history: { supported: true, max_days: 90 }, history_days: null, history_requested_at: null, setup_completed_at: null, last_captured_at: null, week: [0, 0, 0, 0, 0, 0, 0], backfill: null };
const detail = { model: 'connector_detail', version: 1, computed_at: '2026-10-31T00:00:00Z', stale: false, refresh_eta: null,
  payload: { key: 'slack', name: 'Slack', beta: false, status: 'active', sources_on: 1, sources_total: 1, last_captured_at: null, importing: null, problem: null, resume_step: 'scope', sources: [source], account_label: null, connected_at: null, topics: [], captures: { items: [], next_cursor: null }, watched: { threads: [], next_cursor: null }, addin_tokens: { tokens: [] } } };
const row = (path: string, started: number, identity = 'actor') => ({ identity: `${identity}|${path}`, path: path.split('?')[0], address: path, method: 'GET', started, finished: started + 1, status: 200, ledger: 'initial', model: null, computedAt: null, refreshEta: null }) satisfies LoadAttempt;
const page = (next_cursor: string | null) => ({ items: [], next_cursor, total: 200 });
function ready(body: unknown = detail, ack: unknown = { refresh_eta: '2026-10-31T00:00:02Z' }) {
  const phase = new ConnectorCallbackPhase('/connectors/return?provider=slack&result=ok', true, true);
  const post = { ...row('/v1/connectors/slack/refresh', 0), method: 'POST', status: 202 };
  phase.start(post); phase.acknowledge(post, ack);
  const read = row('/v1/connectors/slack', 2); phase.start(read); phase.observe(read, body);
  return phase;
}
it('qualifies only the verified source and actual preceding cursor, with normal requests retained as action attempts', () => {
  const phase = ready(); const one = row('/v1/connectors/slack/sources/channels/scope-items', 4);
  phase.start(one); phase.observe(one, page('50'));
  expect(phase.classify(one)).toMatchObject({ picker: true, source: 'channels' });
  const next = row('/v1/connectors/slack/sources/channels/scope-items?cursor=50', 6);
  phase.start(next); phase.observe(next, page(null)); expect(phase.classify(next)?.picker).toBe(true);
});
it.each([
  ['pending', { ...detail, computed_at: null, stale: true, refresh_eta: '2026-10-31T00:00:02Z' }],
  ['withheld', { ...detail, computed_at: null, stale: true, refresh_eta: '2026-10-31T00:00:02Z', payload: {} }],
  ['invalid version', { ...detail, version: 999 }],
  ['invalid payload', { ...detail, payload: { key: 'slack', sources: [source] } }],
  ['wrong provider', { ...detail, payload: { ...detail.payload, key: 'google' } }],
  ['invalid source', { ...detail, payload: { ...detail.payload, sources: [{ ...source, enabled: 'yes' }] } }],
])('rejects picker authority from %s detail', (_kind, body) => {
  const phase = ready(body); const request = row('/v1/connectors/slack/sources/channels/scope-items', 4); phase.start(request);
  expect(phase.classify(request)).toBeNull();
});
it('refuses missing/failed acknowledgement, unrelated source/account/provider, or an expired phase', () => {
  for (const ack of [{}, { refresh_eta: 'invalid' }]) {
    const phase = ready(detail, ack); const request = row('/v1/connectors/slack/sources/channels/scope-items', 4); phase.start(request); expect(phase.classify(request)).toBeNull();
  }
  for (const request of [row('/v1/connectors/slack/sources/other/scope-items', 4), row('/v1/connectors/google/sources/channels/scope-items', 4), row('/v1/connectors/slack/sources/channels/scope-items', 4, 'other'), row('/v1/connectors/slack/sources/channels/scope-items', 2 + refreshRecoveryMs(refreshLimits('connector_detail')))]) {
    const phase = ready(); phase.start(request); expect(phase.classify(request)).toBeNull();
  }
});
it('does not grant paging from a forged cursor, repeated cursor, invalid response or concurrent request', () => {
  for (const condition of ['forged', 'repeat', 'invalid', 'concurrent']) {
    const phase = ready(); const first = row('/v1/connectors/slack/sources/channels/scope-items', 4); phase.start(first);
    phase.observe(first, condition === 'invalid' ? {} : page('50'));
    const next = row(`/v1/connectors/slack/sources/channels/scope-items?cursor=${condition === 'forged' ? '999' : '50'}`, condition === 'concurrent' ? 4 : 6);
    if (condition === 'repeat') { phase.start(next); phase.observe(next, page('50')); expect(phase.classify(next)?.picker).toBe(true); const repeated = row(next.address, 8); phase.start(repeated); expect(phase.classify(repeated)).toBeNull(); }
    else { phase.start(next); expect(phase.classify(next)).toBeNull(); }
  }
});

it('never authorizes a picker after an actual failed refresh acknowledgement', () => {
  const phase = new ConnectorCallbackPhase('/connectors/return?provider=slack&result=ok', true, true);
  const post = { ...row('/v1/connectors/slack/refresh', 0), method: 'POST', status: 403 };
  phase.start(post); phase.acknowledge(post, { refresh_eta: '2026-10-31T00:00:02Z' });
  const read = row('/v1/connectors/slack', 2); phase.start(read); phase.observe(read, detail);
  const picker = row('/v1/connectors/slack/sources/channels/scope-items', 4); phase.start(picker);
  expect(phase.classify(picker)).toBeNull();
});

it.each([null, { ...detail, computed_at: null, stale: true, refresh_eta: '2026-10-31T00:00:02Z' }, { ...detail, version: 999 }])('does not reuse an older detail certificate after a later refused/withheld/invalid response', (body) => {
  const phase = ready();
  const later = { ...row('/v1/connectors/slack', 4), status: body === null ? 403 : 200 };
  phase.start(later); phase.observe(later, body);
  const picker = row('/v1/connectors/slack/sources/channels/scope-items', 6); phase.start(picker);
  expect(phase.classify(picker)).toBeNull();
});

it('keeps the declared callback destination as a separate real navigation wave without authorizing unrelated navigation', () => {
  const phase = ready(); const read = row('/v1/connectors', 4);
  expect(phase.destination(read, '/connectors?setup=slack', ['/connectors', '/onboarding/connectors'])).toBe('/connectors?setup=slack');
  expect(phase.destination(read, '/home?setup=slack', ['/connectors', '/onboarding/connectors'])).toBeNull();
  expect(phase.destination(read, '/connectors?setup=google', ['/connectors'])).toBeNull();
  expect(phase.destination({ ...read, identity: 'other|/v1/connectors' }, '/connectors?setup=slack', ['/connectors'])).toBeNull();
});

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import ts from 'typescript';
import { classifyDocument, createDocumentEvidence, safeDocumentUrl } from './auth-evidence.mjs';
import registry from '../../web/lib/pageSections.generated.json' with { type: 'json' };

// Execute the ACTUAL route declarations, toolkit parser and canonical resolver;
// only imports are injected. No browser, SDK, network or parallel alias registry.
function actualModule(relative, imports = {}) {
  const source = readFileSync(new URL(relative, import.meta.url), 'utf8').replace(/^import .*;\n/gm, '');
  const js = ts.transpileModule(source, { compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.CommonJS } }).outputText;
  return new Function('exports', ...Object.keys(imports), `${js}\nreturn exports;`)({}, ...Object.values(imports));
}
const { pageRoutes } = actualModule('../../web/lib/pageRoutes.ts');
const { parseToolkit } = actualModule('../../web/components/connectors/composioModel.ts');
const { pageRedirectDestination: resolveAlias } = actualModule('../../web/lib/pageRedirectDestination.ts', { pageRoutes, parseToolkit });
const scope = '["https://issuer.fixture","user_fixture","org_fixture"]';
const browser = { loaded: true, signed_in: true, session_active: true, session_matches: true, user_matches: true, organization_matches: true, session_organization_matches: true };
const expected = { access: 'authenticated', webBase: 'https://web.fixture', sessionId: 'session_fixture', userId: 'user_fixture', orgId: 'org_fixture', scope };
const model = { model: 'connector_detail', version: registry.connector_detail.version, identity: 'single', computed_at: '2026-10-31T00:00:00Z', stale: false, refresh_eta: null };
const record = { method: 'GET', path: '/v1/connectors/github', priority: 'critical', scope, total_ms: 12, models: [model] };
const observation = (overrides = {}) => ({ requested: '/connectors/github/setup?installation_id=123&discard=1', finalUrl: 'https://web.fixture/connectors/github?installation_id=123', expected, browser, records: [record], resolveAlias, ...overrides });

test('real alias/query policy + actual browser and matching model evidence passes', () => {
  const input = observation({ expected: { ...expected, expectedReads: [{ path: record.path, scope, states: [model] }] } });
  assert.equal(classifyDocument(input).kind, 'authenticated-document');
  assert.equal(classifyDocument(input).ok, true);
  assert.equal(classifyDocument(observation({ requested: '/connectors/composio?toolkit=jira', finalUrl: 'https://web.fixture/connectors/jira', records: [{ ...record, path: '/v1/connectors/jira' }] })).ok, true);
  assert.equal(classifyDocument(observation({ requested: '/chats?id=fixture', finalUrl: 'https://web.fixture/chats?id=other' })).kind, 'destination-fatal');
});

test('signed-out settled boundary fails even with a usable backend setup bearer', () => {
  const result = classifyDocument(observation({ browser: { loaded: true, signed_in: false }, records: [], backendSetup: { status: 200, scope }, dataPageLoaded: true }));
  assert.equal(result.kind, 'auth-fatal'); assert.equal(result.fatal, true);
});

for (const key of ['loaded', 'session_active', 'session_matches', 'user_matches', 'organization_matches', 'session_organization_matches']) test(`actual browser ${key} refusal parks`, () => {
  assert.equal(classifyDocument(observation({ browser: { ...browser, [key]: false } })).kind, 'auth-fatal');
});

test('top-level login refuses despite a correct subframe URL; query/origin must be canonical', () => {
  assert.equal(classifyDocument(observation({ finalUrl: 'https://web.fixture/login', frames: [{ top_level: false, url: 'https://web.fixture/connectors/github?installation_id=123' }] })).kind, 'destination-fatal');
  for (const finalUrl of ['https://foreign.fixture/connectors/github?installation_id=123', 'https://web.fixture/connectors/github?installation_id=456']) assert.equal(classifyDocument(observation({ finalUrl })).kind, 'destination-fatal');
});

test('missing session/user/org/issuer configuration and unknown destination fail closed', () => {
  for (const key of ['sessionId', 'userId', 'orgId', 'scope', 'webBase', 'access']) {
    const incomplete = { ...expected }; delete incomplete[key];
    assert.equal(classifyDocument(observation({ expected: incomplete })).kind, 'unknown-fatal', key);
  }
  assert.equal(classifyDocument(observation({ expected: { ...expected, scope: '[null,"user_fixture","org_fixture"]' } })).kind, 'unknown-fatal');
  for (const finalUrl of [null, undefined, '', 'javascript:alert(1)']) assert.equal(classifyDocument(observation({ finalUrl })).kind, 'unknown-fatal');
  assert.equal(classifyDocument(observation({ resolveAlias: () => '/connectors/github/setup' })).kind, 'unknown-fatal');
});

test('correct auth and URL cannot credit absent timing/model or wrong model/version', () => {
  for (const records of [[], [{ ...record, models: [] }], [{ ...record, models: [{ ...model, model: 'home_metrics', version: registry.home_metrics.version }] }], [{ ...record, total_ms: null }], [{ ...record, models: [{ ...model, version: -1 }] }]]) {
    const result = classifyDocument(observation({ records }));
    assert.equal(result.kind, 'content-evidence-failure'); assert.equal(result.ok, false);
  }
  const expectedReads = [{ path: '/v1/connectors/jira', scope, states: [model] }];
  assert.equal(classifyDocument(observation({ expected: { ...expected, expectedReads } })).kind, 'content-evidence-failure');
});

test('every actual API issuer/user/org scope must match, except existing ledger-proven exclusions', () => {
  for (const wrong of ['public', '["other","user_fixture","org_fixture"]', '["https://issuer.fixture","other","org_fixture"]', '["https://issuer.fixture","user_fixture","other"]']) {
    assert.equal(classifyDocument(observation({ records: [{ ...record, scope: wrong }] })).kind, 'auth-fatal');
  }
  const telemetry = { method: 'POST', path: '/v1/telemetry/client', scope: 'public', models: [], total_ms: null };
  assert.equal(classifyDocument(observation({ records: [record, telemetry] })).kind, 'auth-fatal');
  assert.equal(classifyDocument(observation({ records: [record, telemetry], ledger: [{ ...telemetry, excluded: 'telemetry' }] })).ok, true);
  const timezone = { ...telemetry, method: 'PUT', path: '/v1/me/timezone' };
  assert.equal(classifyDocument(observation({ records: [record, timezone], ledger: [{ ...timezone, excluded: 'timezone' }] })).kind, 'auth-fatal');
});

test('callback alternate requires the existing declared action AND acknowledged same-scope destination wave', () => {
  const action = pageRoutes.find(row => row.route === '/connectors/return').action;
  const input = observation({ requested: '/connectors/return?provider=slack', finalUrl: 'https://web.fixture/connectors?setup=slack', expected: { ...expected, action } });
  assert.equal(classifyDocument(input).kind, 'destination-fatal');
  const wave = { identity: `${scope}|/v1/connectors`, initialWave: 'callback-destination:/connectors?setup=slack' };
  assert.equal(classifyDocument({ ...input, ledger: [wave] }).ok, true);
  assert.equal(classifyDocument({ ...input, ledger: [{ ...wave, identity: 'public|/v1/connectors' }] }).kind, 'destination-fatal');
});

test('public document stays public; its recorded failures still fail content validation', () => {
  const input = { requested: '/signup', finalUrl: 'https://web.fixture/signup', expected: { access: 'public', webBase: expected.webBase }, records: [], resolveAlias };
  assert.equal(classifyDocument(input).kind, 'public-document');
  assert.equal(classifyDocument({ ...input, errors: ['network reset'] }).kind, 'content-evidence-failure');
});

test('anonymous root follows the declared app auth gate: original route092 discovery', () => {
  const input = { requested: '/', finalUrl: 'https://web.fixture/login',
    expected: { access: 'public', audience: 'anonymous', webBase: expected.webBase }, browser: null,
    records: [], errors: [], frames: ['/home', '/home', '/login'].map(url => ({ top_level: true, url: `https://web.fixture${url}` })), resolveAlias };
  const result = classifyDocument(input);
  assert.equal(result.kind, 'public-document'); assert.equal(result.ok, true);
  assert.equal(result.canonical_destination, 'https://web.fixture/login');
  for (const finalUrl of ['https://web.fixture/home', 'https://web.fixture/signup', 'https://web.fixture/login?unexpected=1', 'https://foreign.fixture/login'])
    assert.equal(classifyDocument({ ...input, finalUrl }).kind, 'destination-fatal', finalUrl);
  assert.equal(classifyDocument({ ...input, expected: { ...input.expected, audience: undefined } }).kind, 'destination-fatal');
  assert.equal(classifyDocument({ ...input, expected: { ...input.expected, audience: 'authenticated' } }).kind, 'unknown-fatal');
});

test('authenticated root still requires Home plus actual auth, model and timing evidence', () => {
  const state = { ...model, model: 'home_metrics', version: registry.home_metrics.version };
  const home = { ...record, path: '/v1/home/metrics', models: [state] };
  const input = { requested: '/', finalUrl: 'https://web.fixture/home', expected: { ...expected, audience: 'authenticated' }, browser, records: [home], resolveAlias };
  assert.equal(classifyDocument(input).kind, 'authenticated-document');
  assert.equal(classifyDocument({ ...input, finalUrl: 'https://web.fixture/login' }).kind, 'destination-fatal');
  assert.equal(classifyDocument({ ...input, requested: '/home', finalUrl: 'https://web.fixture/login' }).kind, 'destination-fatal');
  assert.equal(classifyDocument({ ...input, browser: { ...browser, signed_in: false } }).kind, 'auth-fatal');
  for (const records of [[], [{ ...home, total_ms: null }], [{ ...home, models: [] }]])
    assert.equal(classifyDocument({ ...input, records }).kind, 'content-evidence-failure');
  for (const requested of ['/shared?t=fixture', '/signup', '/signup?pt=fixture']) {
    const publicInput = { requested, finalUrl: `https://web.fixture${requested}`, expected: { access: 'public', audience: 'anonymous', webBase: expected.webBase }, records: [], resolveAlias };
    assert.equal(classifyDocument(publicInput).kind, 'public-document');
    assert.equal(classifyDocument({ ...publicInput, finalUrl: 'https://web.fixture/login' }).kind, 'destination-fatal');
  }
});

test('read-only collector snapshots AFTER navigation; subframes diagnostic only; never calls getToken', async () => {
  const handlers = new Map(); let navigated = false; let tokenCalls = 0; const main = {};
  const page = { on: (event, fn) => handlers.set(event, fn), off: event => handlers.delete(event), mainFrame: () => main,
    url: () => { assert.equal(navigated, true); return 'https://web.fixture/connectors/github?installation_id=123'; },
    evaluate: async (fn, arg) => {
      assert.equal(navigated, true);
      const previous = globalThis.window;
      globalThis.window = { Clerk: { loaded: true, user: { id: expected.userId }, organization: { id: expected.orgId }, session: { id: expected.sessionId, status: 'active', lastActiveOrganizationId: expected.orgId, getToken() { tokenCalls++; throw new Error('forbidden'); } } } };
      try { return fn(arg); } finally { globalThis.window = previous; }
    } };
  const collector = createDocumentEvidence(page, { requested: observation().requested, expected, resolveAlias, now: () => 42 });
  navigated = true;
  handlers.get('framenavigated')({ url: () => 'https://unrelated.fixture/frame' });
  const result = await collector.complete({ records: [record], ledger: [], errors: [] });
  assert.equal(result.ok, true); assert.equal(result.frames[0].top_level, false); assert.equal(tokenCalls, 0); assert.equal(handlers.size, 0);
  assert.ok(Object.values(result.browser).every(value => typeof value === 'boolean'));
  assert.ok(!JSON.stringify(result).includes('session_fixture'));
});

test('auth receipts redact consecutive JWT-shaped query values and secrets', () => {
  const jwt = 'aaaaaaaa.bbbbbbbb.cccccccc';
  const safe = safeDocumentUrl(`https://web.fixture/chats?a=${jwt}&b=${jwt}&ticket=private&email=private#private`);
  assert.ok(!safe.includes(jwt)); assert.ok(!safe.includes('private'));
  const result = classifyDocument(observation({ browser: { ...browser, private_body: 'secret' } }));
  assert.ok(!JSON.stringify(result).includes('private_body'));
});

test('actual measured/discovery fatal branches preserve the complete observation then terminate the batch', async () => {
  const source = readFileSync(new URL('../tests/page-load-speed.spec.ts', import.meta.url), 'utf8');
  for (const discovery of [false, true]) {
    const begin = discovery ? source.indexOf('const discovery = await observeLoad(') : source.indexOf('const load = await observeLoad(');
    const end = source.indexOf(discovery ? 'validationErrors.push(...discovery.errors' : 'validationErrors.push(...load.errors', begin);
    const branch = source.slice(begin, end);
    const body = ts.transpileModule(`async function attempt() { ${branch} }`, { compilerOptions: { target: ts.ScriptTarget.ES2022 } }).outputText;
    const complete = { document: classifyDocument(observation({ browser: { loaded: true, signed_in: false }, records: [] })), records: [], errors: ['auth-fatal'], ledger: [{ complete: true }] };
    const receipt = {}, loads = [], saved = []; let calls = 0;
    const run = new Function('observeLoad', 'discoveryPage', 'page', 'routePath', 'declaration', 'receipt', 'cadence', 'save', 'filename', 'loads', 'iteration', `let discoveryReturned = false; let parkError = null; ${body}; return attempt;`)
      (async () => { calls++; return complete; }, {}, {}, '/connectors/github', {}, receipt, { observe() {}, receipt: () => ({}) }, (_name, value) => saved.push(structuredClone(value)), 'route.json', loads, 0);
    await assert.rejects(async () => { for (let i = 0; i < 20; i++) await run(); }, /auth-fatal/);
    assert.equal(calls, 1);
    assert.deepEqual(discovery ? saved[0].discovery : saved[0].loads[0], complete);
    assert.equal(receipt.fatal_outcome, 'auth-fatal');
  }
});

test('actual observer returns complete fatal evidence without serializing expected principal configuration', async () => {
  const source = readFileSync(new URL('./nightly.ts', import.meta.url), 'utf8');
  const pure = source.slice(source.indexOf('export async function observeLoad('), source.indexOf('export type LoadObservation'));
  const js = ts.transpileModule(pure, { compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.CommonJS } }).outputText;
  for (const capture of ['disabled', 'invalid', 'setup-throw', 'read-throw', 'attach-throw']) {
    const handlers = new Map();
    const page = { on: (name, handler) => handlers.set(name, handler), off: name => handlers.delete(name), url: () => 'https://web.fixture/login', mainFrame: () => null, evaluate: async () => ({ loaded: true, signed_in: false }) };
    const prepareCapture = async () => {
      if (capture === 'disabled') return undefined;
      if (capture === 'setup-throw') throw Error('unavailable capture setup');
      return async () => {
        if (capture === 'read-throw') throw Error('unavailable capture read');
        return { ok: false, errors: ['capture-invalid'], matches: [], timings: [], trust: 'annotation only' };
      };
    };
    let budgetReads = 0;
    const observer = new Function('exports', 'prepareSemanticTimingCapture', 'pageLoadClock', 'API_BASE', 'credentialScope', 'assertPageLoadBudget', 'test', 'isDeclaredPageRead', 'verifiedActionRead', 'createDocumentEvidence', 'pageRedirectDestination', `${js}\nreturn exports.observeLoad;`)
      ({}, prepareCapture, { now: () => 1 }, 'https://api.fixture', () => scope, async () => { budgetReads++; return []; },
        { info: () => ({ attachments: [], attach: async () => { if (capture === 'attach-throw') throw Error('capture attachment unavailable'); } }) }, () => true, () => false, createDocumentEvidence, resolveAlias);
    const complete = await observer(page, '/connectors/github', false, { document: expected });
    assert.equal(complete.document.kind, 'auth-fatal'); assert.equal(complete.document.fatal, true);
    assert.deepEqual(complete.records, []); assert.deepEqual(complete.ledger, []);
    assert.equal(handlers.size, 0); assert.equal(budgetReads, 1);
    assert.ok(!JSON.stringify(complete).includes('session_fixture'));
    assert.equal(complete.declaration.document, undefined);
    if (capture === 'disabled') assert.equal(complete.semanticTiming, undefined);
    else { assert.equal(complete.semanticTiming.ok, false); assert.ok(complete.semanticTiming.errors.length > 0); }
  }
});


test('duplicate query keys cannot skip later secret or JWT entries', () => {
  const jwt = 'aaaaaaaa.bbbbbbbb.cccccccc';
  for (const query of [`x=ordinary&x=${jwt}&ticket=LEAK`, `x=ordinary&x=${jwt}&y=${jwt}`, `ticket=LEAK&ticket=LEAK&x=${jwt}`]) {
    const safe = safeDocumentUrl(`https://web.fixture/callback?${query}`);
    assert.ok(!safe.includes(jwt), safe); assert.ok(!safe.includes('LEAK'), safe);
  }
});

test('actual initial refresh refuses a lease that expired during discovery', async () => {
  const source = readFileSync(new URL('../tests/page-load-speed.spec.ts', import.meta.url), 'utf8');
  const begin = source.indexOf('const reads = discoveredReads(discovery);');
  const end = source.indexOf('receipt.refresh = refresh;', begin);
  const js = ts.transpileModule(`async function attempt() { ${source.slice(begin, end)} }`, { compilerOptions: { target: ts.ScriptTarget.ES2022 } }).outputText;
  for (const expired of [false, true]) {
    let refreshes = 0;
    const run = new Function('discoveredReads', 'discovery', 'receipt', 'declaration', 'discoveryPage', 'entry', 'browserToken', 'request', 'refreshDiscovered', 'liveIdentity', `${js}; return attempt;`)
      (() => [], {}, {}, { document: {} }, { close: async () => {} }, { access: 'authenticated' }, async () => '', {}, async () => { refreshes++; return { errors: [] }; }, () => { if (expired) throw new Error('owner lease expired during discovery'); });
    if (expired) await assert.rejects(run(), /lease expired/); else await run();
    assert.equal(refreshes, expired ? 0 : 1);
  }
});

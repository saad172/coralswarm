import { createDocumentEvidence } from './auth-evidence.mjs';
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { pageLoadClock, createPageLoadClock } from '../lib/pageLoadClock.mjs';
import { parsePageReadTiming, summarize, assertSpeed } from './timings.mjs';
import { seedVariables, SEED_URL } from './seed.mjs';
import { readFileSync } from 'node:fs';

test('actual route statistics reject twenty fast observations certified for the wrong endpoint', async () => {
  const { assertStatesForPath } = await import('./envelopes.mjs');
  const { default: registry } = await import('../../web/lib/pageSections.generated.json', { with: { type: 'json' } });
  const { expect } = await import('@playwright/test');
  const ts = await import('typescript');
  // Execute the actual pure functions without loading their browser/stack I/O imports.
  const source = readFileSync(new URL('./nightly.ts', import.meta.url), 'utf8');
  const pure = source.slice(source.indexOf('export function discoveredReads('), source.indexOf('export async function refreshDiscovered('))
    + source.slice(source.indexOf('export function routeStatistics('));
  const js = ts.transpileModule(pure, { compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.CommonJS } }).outputText;
  const statistics = new Function('exports', 'expect', 'assertStatesForPath', 'manifest', 'assertSpeed', `${js}\nreturn exports.routeStatistics;`)
    ({}, expect, assertStatesForPath, registry, assertSpeed);
  const observation = (model) => ({ path: '/home', errors: [], records: [{ method: 'GET', path: '/v1/home/metrics',
    scope: 'viewer', priority: 'critical', total_ms: 1, models: [{ identity: 'single', model, version: registry[model].version,
      computed_at: '2026-10-31T00:00:00Z', stale: false, refresh_eta: null }] }] });
  const home = observation('home_metrics'); const wrong = observation('connector_status');
  assert.equal(statistics(Array(20).fill(home), home).p95_ms, 1);
  assert.throws(() => statistics(Array(20).fill(wrong), wrong), /requested model/);
  assert.throws(() => statistics(Array(20).fill(wrong), home), /requested model/);
  assert.throws(() => statistics(Array(20).fill(home), { ...home, errors: ['background connection reset'] }));
});

test('actual observer rejects a background transport failure before any response', async () => {
  const ts = await import('typescript');
  const source = readFileSync(new URL('./nightly.ts', import.meta.url), 'utf8');
  const pure = source.slice(source.indexOf('export async function observeLoad('), source.indexOf('export type LoadObservation'));
  const js = ts.transpileModule(pure, { compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.CommonJS } }).outputText;
  const handlers = new Map();
  const page = { url: () => 'https://web.fixture/home', mainFrame: () => null, on: (event, listener) => handlers.set(event, listener), off: (event) => handlers.delete(event) };
  const request = { url: () => 'https://api.fixture/v1/load/conversations/fixture', method: () => 'GET',
    headers: () => ({ 'x-coralswarm-priority': 'background' }), timing: () => ({}), failure: () => ({ errorText: 'net::ERR_CONNECTION_RESET' }) };
  const budget = async () => { handlers.get('request')(request); handlers.get('requestfailed')(request); return []; };
  const observer = new Function('exports', 'prepareSemanticTimingCapture', 'pageLoadClock', 'API_BASE', 'credentialScope', 'assertPageLoadBudget', 'test', 'isDeclaredPageRead', 'verifiedActionRead', 'createDocumentEvidence', 'pageRedirectDestination',
    `${js}\nreturn exports.observeLoad;`)({}, async () => undefined, pageLoadClock, 'https://api.fixture', () => 'public', budget,
      { info: () => ({ attachments: [] }) }, () => true, () => false, createDocumentEvidence, () => null);
  const result = await observer(page, '/home', true, { document: { access: 'public', webBase: 'https://web.fixture' } });
  assert.equal(result.records[0].failure, 'net::ERR_CONNECTION_RESET');
  assert.ok(result.errors.some((error) => error.includes('ERR_CONNECTION_RESET')));
});

test('actual refresh setup records network starts and failures before throwing', async () => {
  const ts = await import('typescript');
  const { expect } = await import('@playwright/test');
  const { assertStatesForPath } = await import('./envelopes.mjs');
  const { default: registry } = await import('../../web/lib/pageSections.generated.json', { with: { type: 'json' } });
  const source = readFileSync(new URL('./nightly.ts', import.meta.url), 'utf8');
  const pure = source.slice(source.indexOf('export async function refreshDiscovered('), source.indexOf('export function routeStatistics('));
  const js = ts.transpileModule(pure, { compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.CommonJS } }).outputText;
  const refresh = new Function('exports', 'pageLoadClock', 'API_BASE', 'refreshPath', 'assertStatesForPath', 'manifest', 'expect', `${js}\nreturn exports.refreshDiscovered;`)
    ({}, pageLoadClock, 'https://api.fixture', (path) => `${path}/refresh`, assertStatesForPath, registry, expect);
  const reads = [{ path: '/v1/home/metrics', scope: 'public', states: [{ identity: 'single', model: 'home_metrics', version: registry.home_metrics.version }] }];
  const result = await refresh({ fetch: async () => { throw new Error('setup connection reset'); } }, reads, async () => '');
  assert.deepEqual(result.errors, ['setup connection reset']);
  assert.equal(result.ledger.length, 1);
  const attempt = result.ledger[0];
  assert.equal(attempt.path, '/v1/home/metrics/refresh');
  assert.equal(attempt.status, null);
  assert.equal(attempt.failure, 'setup connection reset');
  assert.ok(attempt.finished >= attempt.started);
});


test('actual discovery preserves each certified fan-in refresh address and refuses absent ones', async () => {
  const ts = await import('typescript');
  const { default: registry } = await import('../../web/lib/pageSections.generated.json', { with: { type: 'json' } });
  const source = readFileSync(new URL('./nightly.ts', import.meta.url), 'utf8');
  const pure = source.slice(source.indexOf('export function discoveredReads('), source.indexOf('export async function refreshDiscovered('));
  const js = ts.transpileModule(pure, { compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.CommonJS } }).outputText;
  const discovery = new Function('exports', 'manifest', `${js}\nreturn exports.discoveredReads;`)({}, registry);
  const scope = '["issuer","actor",null]';
  const records = [{ method: 'GET', path: '/v1/load/oceans', scope, models: [{ model: 'oceans_index', identity: 'account-a' }, { model: 'oceans_index', identity: 'account-b' }] }];
  const ledger = [{ method: 'GET', address: '/v1/load/oceans', identity: `${scope}|/v1/load/oceans`, certificates: [{ refreshPath: '/v1/load/oceans/component/refresh?graph=a' }, { refreshPath: '/v1/load/oceans/component/refresh?graph=b' }] }];
  assert.deepEqual(discovery({ records, ledger })[0].refreshPaths, ledger[0].certificates.map((c) => c.refreshPath));
  assert.deepEqual(discovery({ records, ledger: [] })[0].refreshPaths, []);
});

for (const jump of [-194,86_400_000]) test(`actual nightly observer and callback ledger share elapsed epoch across wall step ${jump}`,async()=>{
 const ts=await import('typescript');
 const {verifiedActionRead}=await import('./read-policy.mjs');
 const source=readFileSync(new URL('./nightly.ts',import.meta.url),'utf8');
 const pure=source.slice(source.indexOf('export async function observeLoad('),source.indexOf('export type LoadObservation'));
 const js=ts.transpileModule(pure,{compilerOptions:{target:ts.ScriptTarget.ES2022,module:ts.ModuleKind.CommonJS}}).outputText;
 let elapsed=0,wall=1_000_000;
 const clock=createPageLoadClock({elapsedNow:()=>elapsed,wallNow:()=>wall});
 const handlers=new Map(),scope='["issuer","actor",null]',path='/v1/connectors/slack/sources/source/scope-items';
 const page={url:()=> 'https://web.fixture/connectors/return',mainFrame:()=>null,on:(event,fn)=>handlers.set(event,fn),off:event=>handlers.delete(event)};
 const request={url:()=>`https://api.fixture${path}`,method:()=> 'GET',headers:()=>({'x-coralswarm-priority':'normal'}),failure:()=>null,timing:()=>({})};
 let entry;
 const budget=async()=>{
  elapsed+=100;wall+=100+jump;
  handlers.get('request')(request);
  entry={method:'GET',address:path,identity:`${scope}|${path}`,priority:'normal',phase:'callback-action',actionEvidence:{picker:true},started:clock.now(),finished:null};
  elapsed+=200;wall+=200;entry.finished=clock.now();handlers.get('requestfinished')(request);return[entry];
 };
 const observer=new Function('exports','prepareSemanticTimingCapture','pageLoadClock','API_BASE','credentialScope','assertPageLoadBudget','test','isDeclaredPageRead','verifiedActionRead','createDocumentEvidence','pageRedirectDestination',`${js}\nreturn exports.observeLoad;`)
 ({},async()=>undefined,clock,'https://api.fixture',()=>scope,budget,{info:()=>({attachments:[]})},()=>false,verifiedActionRead,createDocumentEvidence,()=>null);
 const originalDateNow=Date.now;Date.now=()=>wall;
 try {
  const result=await observer(page,'/connectors/return',true,{document:{access:'public',webBase:'https://web.fixture'}});
  assert.equal(result.records[0].started,entry.started);assert.equal(result.records[0].finished,entry.finished);
  assert.equal(result.records[0].finished-result.records[0].started,200);
  assert.equal(result.records[0].wallStarted,1_000_100+jump);assert.deepEqual(result.errors,[]);
 }finally{Date.now=originalDateNow;}
});

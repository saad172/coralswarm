import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync, mkdirSync, writeFileSync, mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { pathToFileURL } from 'node:url';
import path from 'node:path';
import { createPageLoadClock } from '../lib/pageLoadClock.mjs';
import { pairInfoRateContract, RateCadence } from './rate-cadence.mjs';
const root = new URL('../../', import.meta.url);
const contract = pairInfoRateContract(root);
const record = (started, status = 404) => ({ method: 'GET', path: '/v1/addin/pair/info?code=%20same-code%20', started, finished: started + 8, status, failure: null });
test('bind to exact actual Rust contract, endpoint and trimmed code key', () => {
 assert.equal(contract.max,20); assert.equal(contract.window_ms,60_000);
 const cadence = new RateCadence(contract,3); assert.equal(cadence.key(record(1)), 'addin_info:same-code');
 assert.equal(cadence.key({ ...record(1), path:'/v1/other?code=same-code' }),null);
});
test('discovery plus20loads cannot create a21st start within the admitted window', () => {
 const cadence=new RateCadence(contract,1); cadence.observe([record(1000)],'discovery');
 for(let n=0;n<19;n++){ assert.equal(cadence.plan([record(0)],2000+n*1000).release_at,2000+n*1000);cadence.observe([record(2000+n*1000)],`load${n+1}`); }
 assert.equal(cadence.starts.length,20);assert.equal(cadence.plan([record(0)],30_000).release_at,61_009);
});
test('every actual retry counts and full existing transport allowance is reserved', () => {
 const cadence=new RateCadence(contract,3);cadence.observe(Array.from({length:18},(_,i)=>record(1000+i, i>15?429:404)),'includes actual retry starts');
 assert.equal(cadence.plan([record(0)],2000).release_at,61_009);
 assert.equal(cadence.plan([record(0)],2000).budgets[0].actual_starts_in_window,18);
});
test('unlimited routes incur no wait, expiry is strict, and pacing precedes measurement', async () => {
 const cadence=new RateCadence(contract,3);cadence.observe(Array.from({length:20},()=>record(1000)),'allstarts');
 let now=61_008; const waits=[];const gate=await cadence.before([record(0)],()=>now,async(ms)=>{waits.push(ms);now+=ms;});
 assert.deepEqual(waits,[1]);assert.equal(gate.applied_at,61_009);
 assert.equal(cadence.plan([{...record(0),path:'/v1/home/metrics'}],now).release_at,now);
 assert.equal(cadence.receipt().starts.length,20);
});

test('changed backend limiter key or invalid constants fail closed before pacing', () => {
 const temp = mkdtempSync(path.join(tmpdir(), 'l6-rate-contract-'));
 const files = contract.source;
 try {
  for (const file of files) { mkdirSync(path.dirname(path.join(temp,file)), {recursive:true});writeFileSync(path.join(temp,file),readFileSync(new URL(file,root))); }
  const handler=files[1];const original=readFileSync(new URL(handler,root),'utf8');
  writeFileSync(path.join(temp,handler),original.replace('format!("addin_info:{code}")','format!("changed:{code}")'));
  assert.throws(()=>pairInfoRateContract(pathToFileURL(`${temp}/`)),/contract changed/);
  writeFileSync(path.join(temp,handler),original);
  const definitions=readFileSync(new URL(files[0],root),'utf8');
  writeFileSync(path.join(temp,files[0]),definitions.replace(/PAIR_CODE_RATE_MAX_PER_WINDOW: u32 = \d+;/,'PAIR_CODE_RATE_MAX_PER_WINDOW: u32 = 0;'));
  assert.throws(()=>pairInfoRateContract(pathToFileURL(`${temp}/`)),/rate contract/);
 } finally { rmSync(temp,{recursive:true}); }
});

for (const jump of [-194, 86_400_000]) test(`wall step ${jump} cannot change unknown-completion anchor or the real rate window`, async () => {
 let wall=1_000_000, elapsed=0;
 const clock=createPageLoadClock({elapsedNow:()=>elapsed,wallNow:()=>wall});
 const originalDateNow=Date.now;Date.now=()=>wall;
 try {
  const cadence=new RateCadence(contract,1,clock);
  const started=clock.now();elapsed+=100;wall+=100+jump;
  cadence.observe(Array.from({length:20},()=>({...record(started),finished:null,wallStarted:1_000_000,wallFinished:null})),'unknown completions');
  assert.ok(cadence.starts.every(row=>row.window_anchor===1_000_100));
  assert.equal(cadence.starts[0].wallObservedAt,wall);
  const waits=[];
  const gate=await cadence.before([record(0)],undefined,async(ms)=>{waits.push(ms);elapsed+=ms;wall+=ms;});
  assert.deepEqual(waits,[50_000,10_001]);assert.equal(elapsed,60_101);
  assert.equal(gate.applied_at,1_060_101);assert.equal(gate.wallAppliedAt,wall);
 } finally {Date.now=originalDateNow;}
});

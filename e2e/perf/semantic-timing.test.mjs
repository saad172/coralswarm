import test from 'node:test';
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { validateSemanticTiming, prepareSemanticTimingCapture, criticalReadPredecessors } from './semantic-timing.mjs';
const nonce='11111111-1111-4111-8111-111111111111',scope='["fixture","actor",null]',address='/v1/connectors/google';
const digest=createHash('sha256').update(`${nonce}\0${scope}`).digest('hex');
const cert={model:'connector_detail',version:1,computedAt:'2026-11-01T02:38:17.077276Z',stale:true,refreshEta:'2026-11-01T02:38:18.924218Z'};
function fixture(delta=2000,validationDelay=0){
 const admission=100+validationDelay,next=admission+delta,fresh={...cert,stale:false,refreshEta:null};
 const events=[
  {kind:'run',at:0,run:1,query:1,model:cert.model,address,parent:null},
  {kind:'dispatch',at:0,attempt:1,run:1,address,method:'GET',priority:'critical',scopeDigest:digest},
  {kind:'response',at:50,attempt:1,status:200},
  {kind:'admission',at:admission,run:1,query:1,attempt:1,certificate:cert},
  {kind:'cache-success',at:admission+1,run:1,query:1,admission:4,manual:false},
  {kind:'run',at:next,run:2,query:1,model:cert.model,address,parent:4},
  {kind:'dispatch',at:next,attempt:2,run:2,address,method:'GET',priority:'critical',scopeDigest:digest},
  {kind:'response',at:next+50,attempt:2,status:200},
  {kind:'admission',at:next+100,run:2,query:1,attempt:2,certificate:fresh},
  {kind:'cache-success',at:next+101,run:2,query:1,admission:9,manual:false},
 ].map((event,i)=>({...event,seq:i+1}));
 const records=[{method:'GET',path:address,scope,priority:'critical',started:0,finished:admission+74,status:200},{method:'GET',path:address,scope,priority:'critical',started:next,finished:next+100,status:200}];
 const ledger=[cert,fresh].map(c=>({method:'GET',address,priority:'critical',identity:`${scope}|${address}`,certificates:[c],ledger:'initial'}));
 return {snapshot:{version:1,nonce,clock:'browser-performance',events,errors:[]},records,ledger};
}
test('late Node finish does not rewrite captured browser admission/confirmation deltas or classify anything',()=>{
 const f=fixture();const original=structuredClone(f);const r=validateSemanticTiming(f.snapshot,nonce,f.records,f.ledger);
 assert.equal(r.ok,true);assert.equal(r.timings[0].admissionToDispatchMs,2000);assert.equal(r.timings[0].confirmationToDispatchMs,1999);
 assert.equal(f.records[1].started-f.records[0].finished,1926);assert.deepEqual(f,original);
 assert.ok(f.ledger.every(row=>row.ledger==='initial'));assert.equal('eligible' in r,false);assert.ok(!JSON.stringify(r).includes('actor'));
});
test('actual1949 remains visible after validation delay and late observer finish; JSON arrival is never the anchor',()=>{
 const f=fixture(1949,250);const r=validateSemanticTiming(f.snapshot,nonce,f.records,f.ledger);
 assert.equal(r.ok,true);assert.equal(r.timings[0].admissionToDispatchMs,1949);assert.equal(r.timings[0].confirmationToDispatchMs,1948);
 assert.equal(f.records[1].started,2299);assert.equal(f.ledger[1].ledger,'initial');
});
test('an outstanding request without terminal evidence cannot settle a captured query run',()=>{
 for(const status of [null,503]){
  const f=fixture();f.snapshot.events=f.snapshot.events.slice(0,2);
  f.records=[{...f.records[0],finished:null,status}];f.ledger=[];
  const result=validateSemanticTiming(f.snapshot,nonce,f.records,f.ledger);
  assert.equal(result.ok,false);assert.ok(result.errors.includes('unsettled-query-run'));
 }
});
test('a terminal cache error settles a failed run without inventing a response',()=>{
 const f=fixture();f.snapshot.events=f.snapshot.events.slice(0,2);
 f.records=[{...f.records[0],finished:20,status:null,failure:'net::ERR_CONNECTION_RESET'}];f.ledger=[];
 f.snapshot.events.push({kind:'cache-error',at:21,seq:3,run:1,query:1});
 assert.equal(validateSemanticTiming(f.snapshot,nonce,f.records,f.ledger).ok,true);
});
test('a failed earlier attempt cannot conceal a successful retry awaiting cache publication',()=>{
 const f=fixture();f.snapshot.events=[...f.snapshot.events.slice(0,3),{...f.snapshot.events[6],run:1,at:100},{...f.snapshot.events[7],at:150}].map((event,i)=>({...event,seq:i+1}));
 f.snapshot.events[2].status=503;
 f.records=[{...f.records[0],status:503,finished:50},{...f.records[1],started:100,finished:150}];f.ledger=[];
 const result=validateSemanticTiming(f.snapshot,nonce,f.records,f.ledger);
 assert.equal(result.ok,false);assert.ok(result.errors.includes('unsettled-query-run'));
});
test('certificate occurrence matching keeps sequential background and critical reads separate',()=>{
 const f=fixture();f.snapshot.events[1].priority='background';
 f.records[0].priority='background';f.ledger[0].priority='background';
 const result=validateSemanticTiming(f.snapshot,nonce,f.records,f.ledger);
 assert.equal(result.ok,true);assert.deepEqual(result.matches.map(row=>row.occurrence),[0,0]);
 f.snapshot.events[8].certificate=cert;
 assert.equal(validateSemanticTiming(f.snapshot,nonce,f.records,f.ledger).ok,false);
});
for(const [name,mutate] of [
 ['replayed nonce',f=>f.snapshot.nonce='22222222-2222-4222-8222-222222222222'],
 ['clock mismatch',f=>f.snapshot.clock='node-performance'],
 ['unknown data field',f=>f.snapshot.events[3].payload={private:true}],
 ['overflow',f=>f.snapshot.errors=['overflow']],
 ['scope mismatch',f=>f.snapshot.events[1].scopeDigest='0'.repeat(64)],
 ['address mismatch',f=>f.snapshot.events[1].address='/v1/connectors/slack'],
 ['certificate version',f=>f.snapshot.events[3].certificate={...cert,version:999}],
 ['certificate model',f=>f.snapshot.events[3].certificate={...cert,model:'home_metrics'}],
 ['missing final confirmation',f=>f.snapshot.events[9].kind='removed'],
 ['missing dispatch',f=>f.snapshot.events[1].kind='removed'],
 ['forged attempt',f=>f.snapshot.events[3].attempt=999],
 ['cross-key parent',f=>f.snapshot.events[5].query=2],
 ['ambiguous identical overlap',f=>f.records[0].finished=f.records[1].started+1],
 ['refusal cannot confirm',f=>f.snapshot.events[2].status=403],
 ['extra real start',f=>f.records.push({...f.records[1]})],
 ['manual confirmation cannot parent',f=>f.snapshot.events[4].manual=true],
 ['wrong original response',f=>f.ledger[0].certificates=[{...cert,refreshEta:null}]],
 ])test(`${name} fails closed for capture correlation`,()=>{const f=fixture();mutate(f);assert.equal(validateSemanticTiming(f.snapshot,nonce,f.records,f.ledger).ok,false);});
test('absent metadata fails closed',()=>assert.equal(validateSemanticTiming(null,nonce,[],[]).ok,false));
test('reader is dormant by default and never installs a browser seam without explicit capture opt-in',async()=>{
 const previous=process.env.E2E_CAPTURE_PAGE_READ_RECEIPTS;delete process.env.E2E_CAPTURE_PAGE_READ_RECEIPTS;
 try{assert.equal(await prepareSemanticTimingCapture({addInitScript:()=>{throw Error('unexpected browser operation');}}),undefined);}finally{if(previous!==undefined)process.env.E2E_CAPTURE_PAGE_READ_RECEIPTS=previous;}
});
for(const forged of [false,true])test(`actual reader captures only an immutable sealed getter (forged descriptor=${forged})`,async()=>{
 const previousEnv=process.env.E2E_CAPTURE_PAGE_READ_RECEIPTS,previousWindow=globalThis.window;
 process.env.E2E_CAPTURE_PAGE_READ_RECEIPTS='1';globalThis.window={};
 try{
  const f=fixture();const page={addInitScript:async fn=>fn(),evaluate:async fn=>fn()};
  const collect=await prepareSemanticTimingCapture(page),actualNonce=window.__csPageReadTimingNonce;
  f.snapshot.nonce=actualNonce;
  const scopeDigest=createHash('sha256').update(`${actualNonce}\0${scope}`).digest('hex');
  f.snapshot.events.filter(e=>e.kind==='dispatch').forEach(e=>e.scopeDigest=scopeDigest);
  Object.defineProperty(window,'__csPageReadTimingSnapshot',{value:Object.freeze(async()=>f.snapshot),writable:forged,configurable:forged});
  const result=await collect(f.records,f.ledger);
  assert.equal(result.ok,!forged);
  assert.equal(Object.getOwnPropertyDescriptor(window,'__csPageReadTimingNonce').writable,false);
  if(forged)assert.equal(result.snapshot,undefined);
  else assert.equal(result.snapshot.events.length,10);
 }finally{globalThis.window=previousWindow;if(previousEnv===undefined)delete process.env.E2E_CAPTURE_PAGE_READ_RECEIPTS;else process.env.E2E_CAPTURE_PAGE_READ_RECEIPTS=previousEnv;}
});

function bootstrapOverlapFixture() {
 const f=fixture();
 const sidebar='/v1/conversations', today='/v1/journal/activities?from=2026-11-01T00%3A00%3A00.000Z&to=2026-11-02T00%3A00%3A00.000Z&tz=UTC&who=mine';
 const fresh=model=>({model,version:1,computedAt:cert.computedAt,stale:false,refreshEta:null});
 f.snapshot.events=[
  {kind:'run',at:0,run:1,query:1,model:'conversation_list',address:sidebar,parent:null},
  {kind:'dispatch',at:1,attempt:1,run:1,address:sidebar,method:'GET',priority:'critical',scopeDigest:digest},
  {kind:'dispatch',at:2,attempt:2,address:'/v1/bootstrap',method:'GET',priority:'critical',scopeDigest:digest},
  {kind:'response',at:40,attempt:1,status:200},
  {kind:'response',at:100,attempt:2,status:200},
  {kind:'run',at:110,run:2,query:2,model:'journal_activities',address:today,parent:null},
  {kind:'dispatch',at:111,attempt:3,run:2,address:today,method:'GET',priority:'critical',scopeDigest:digest},
  {kind:'admission',at:112,run:1,query:1,attempt:1,certificate:fresh('conversation_list')},
  {kind:'cache-success',at:113,run:1,query:1,admission:8,manual:false},
  {kind:'response',at:150,attempt:3,status:200},
  {kind:'admission',at:151,run:2,query:2,attempt:3,certificate:fresh('journal_activities')},
  {kind:'cache-success',at:152,run:2,query:2,admission:11,manual:false},
 ].map((e,i)=>({...e,seq:i+1}));
 f.records=[{method:'GET',path:sidebar,scope,priority:'critical',started:1,finished:41,status:200},
  {method:'GET',path:'/v1/bootstrap',scope,priority:'critical',started:2,finished:101,status:200},
  {method:'GET',path:today,scope,priority:'critical',started:111,finished:151,status:200}];
 f.ledger=f.records.map(r=>({...r,address:r.path,identity:`${scope}|${r.path}`,phase:'initial',initialWave:'entry',excluded:null,ledger:'initial',certificates:r.path==='/v1/bootstrap'?undefined:[fresh(r.path===sidebar?'conversation_list':'journal_activities')]}));
 return f;
}
test('bootstrap-overlapping sidebar completion adds no dependent depth to bootstrap-parameterized Today',()=>{
 const f=bootstrapOverlapFixture(), capture=validateSemanticTiming(f.snapshot,nonce,f.records,f.ledger);
 assert.equal(capture.ok,true,JSON.stringify(capture.errors));
 assert.deepEqual(criticalReadPredecessors(capture,f.ledger[2],f.ledger),[]);
});
function trialBootstrapFixture() {
 const f=bootstrapOverlapFixture(), address='/v1/bootstrap?billing_banner=trial_v1';
 f.snapshot.events[2].address=address;
 f.records[1].path=address;
 f.ledger[1].address=address;
 f.ledger[1].identity=`${scope}|${address}`;
 return f;
}
test('the exact trial-only bootstrap contract retains the same verified overlap proof',()=>{
 const f=trialBootstrapFixture(), c=validateSemanticTiming(f.snapshot,nonce,f.records,f.ledger);
 assert.equal(c.ok,true,JSON.stringify(c.errors));
 assert.deepEqual(criticalReadPredecessors(c,f.ledger[2],f.ledger),[]);
});
for (const address of ['/v1/bootstrap?billing_banner=unknown','/v1/bootstrap?billing_banner=trial_v1&extra=1','/v1/bootstrap/extra']) {
 test(`unrecognized bootstrap address cannot supply overlap proof: ${address}`,()=>{
  const f=trialBootstrapFixture();
  f.snapshot.events[2].address=f.records[1].path=f.ledger[1].address=address;
  f.ledger[1].identity=`${scope}|${address}`;
  const c=validateSemanticTiming(f.snapshot,nonce,f.records,f.ledger);
  assert.deepEqual(criticalReadPredecessors(c,f.ledger[2],f.ledger),[f.ledger[0]]);
 });
}
test('late sidebar HTTP still adds no causal depth before admission, while genuine page-to-page waterfalls retain depth two',()=>{
 const f=bootstrapOverlapFixture();
 f.snapshot.events[3].at=105;f.snapshot.events[4].at=106;
 // Sidebar response now follows the bootstrap response: reorder their occurrences.
 [f.snapshot.events[3],f.snapshot.events[4]]=[{...f.snapshot.events[4],at:100},{...f.snapshot.events[3],at:105}];
 f.snapshot.events.forEach((e,i)=>e.seq=i+1);
 f.records[0].finished=f.ledger[0].finished=106;
 const capture=validateSemanticTiming(f.snapshot,nonce,f.records,f.ledger);
 assert.equal(capture.ok,true,JSON.stringify(capture.errors));
 assert.deepEqual(criticalReadPredecessors(capture,f.ledger[2],f.ledger),[]);
 const serial={...f.ledger[2],started:200};
 assert.deepEqual(criticalReadPredecessors(capture,serial,[...f.ledger,serial]),[f.ledger[0],f.ledger[2]]);
});
test('missing, copied or changed capture cannot exempt any temporal predecessor',()=>{
 const f=bootstrapOverlapFixture(), c=validateSemanticTiming(f.snapshot,nonce,f.records,f.ledger);
 for(const untrusted of [undefined,{...c}])assert.deepEqual(criticalReadPredecessors(untrusted,f.ledger[2],f.ledger),[f.ledger[0]]);
 f.snapshot.events[1].at=3;
 assert.deepEqual(criticalReadPredecessors(c,f.ledger[2],f.ledger),[f.ledger[0]]);
});
for (const [name, mutate] of [
 ['failed bootstrap',f=>{f.snapshot.events[4].status=503;f.records[1].status=f.ledger[1].status=503;}],
 ['cross-scope bootstrap',f=>{f.snapshot.events[2].scopeDigest='0'.repeat(64);}],
 ['manual sidebar publication',f=>{f.snapshot.events[8].manual=true;}],
 ['unconfirmed sidebar',f=>{f.snapshot.events[8]={kind:'cache-error',at:113,seq:9,query:1,run:1};}],
]) for (const make of [bootstrapOverlapFixture,trialBootstrapFixture]) test(`${name} cannot erase a temporal waterfall (${make.name})`,()=>{
 const f=make();mutate(f);
 const c=validateSemanticTiming(f.snapshot,nonce,f.records,f.ledger);
 assert.deepEqual(criticalReadPredecessors(c,f.ledger[2],f.ledger),[f.ledger[0]]);
});
test('a page that waits for sidebar admission is still a genuine waterfall even if HTTP finished inside bootstrap',()=>{
 const f=bootstrapOverlapFixture(), e=f.snapshot.events;
 f.snapshot.events=[...e.slice(0,5),e[7],{...e[8],admission:6},{...e[5],at:114},{...e[6],at:115},...e.slice(9)]
  .map((event,i)=>({...event,seq:i+1}));
 f.records[2].started=f.ledger[2].started=115;
 const c=validateSemanticTiming(f.snapshot,nonce,f.records,f.ledger);
 assert.equal(c.ok,true,JSON.stringify(c.errors));
 assert.deepEqual(criticalReadPredecessors(c,f.ledger[2],f.ledger),[f.ledger[0]]);
});

test('a page initiated early but dispatched after sidebar publication retains its predecessor',()=>{
 const f=bootstrapOverlapFixture(), e=f.snapshot.events;
 // Async credential acquisition or transport can wait after query initiation.
 // Initiation at 110 cannot exempt dispatch at 115 after publication at 113.
 f.snapshot.events=[...e.slice(0,6),e[7],{...e[8],admission:7},{...e[6],at:115},...e.slice(9)]
  .map((event,i)=>({...event,seq:i+1}));
 f.records[2].started=f.ledger[2].started=115;
 const c=validateSemanticTiming(f.snapshot,nonce,f.records,f.ledger);
 assert.equal(c.ok,true,JSON.stringify(c.errors));
 assert.deepEqual(criticalReadPredecessors(c,f.ledger[2],f.ledger),[f.ledger[0]]);
});

test('a sidebar started after bootstrap retains the conservative temporal verdict',()=>{
 const f=bootstrapOverlapFixture(), e=f.snapshot.events;
 f.snapshot.events=[{...e[2],at:0},{...e[4],at:100},{...e[0],at:101},{...e[1],at:102},{...e[3],at:105},...e.slice(5)]
  .map((event,i)=>({...event,seq:i+1}));
 f.records[0].started=f.ledger[0].started=102;
 f.records[0].finished=f.ledger[0].finished=106;
 f.records[1].started=f.ledger[1].started=0;
 const c=validateSemanticTiming(f.snapshot,nonce,f.records,f.ledger);
 assert.equal(c.ok,true,JSON.stringify(c.errors));
 assert.deepEqual(criticalReadPredecessors(c,f.ledger[2],f.ledger),[f.ledger[0]]);
});

import test from 'node:test';
import assert from 'node:assert/strict';
import { createPageLoadClock } from '../lib/pageLoadClock.mjs';
import manifest from '../../web/lib/pageSections.generated.json' with { type:'json' };
import { prepareHealthyLoad, cachedScopedToken } from './healthy-setup.mjs';
const scope=JSON.stringify(['issuer','actor','org']);
const jwt=(now,who='actor')=>`head.${Buffer.from(JSON.stringify({iss:'issuer',sub:who,org_id:'org',exp:Math.floor(now/1000)+3600})).toString('base64url')}.signature`;
function harness(){
 let now=1_000_000,computed=now;const calls=[],responses=[];
 const body=()=>({model:'home_metrics',version:manifest.home_metrics.version,computed_at:new Date(computed).toISOString(),stale:false,refresh_eta:null,payload:{valid:true}});
 const response=(value,status=200)=>{const r={disposed:0,status:()=>status,headers:()=>({'server-timing':'class;desc="page_read",total;dur=1'}),text:async()=>JSON.stringify(value),json:async()=>value,dispose:async()=>{r.disposed++;}};responses.push(r);return r;};
 const request={fetch:async(url,options)=>{calls.push({url,method:options.method});if(options.method==='POST'){computed=now;return response({refresh_eta:new Date(now+5000).toISOString()},202);}return response(body());}};
 const reads=[{path:'/v1/home/metrics',scope,states:[{identity:'single',model:'home_metrics',version:manifest.home_metrics.version,computed_at:new Date(computed).toISOString(),stale:false,refresh_eta:null}]}];
 const refresh=async(recording,current,token)=>{try{await recording.fetch('http://local/v1/home/metrics/refresh',{method:'POST',headers:{authorization:`Bearer ${await token()}`}});await recording.fetch('http://local/v1/home/metrics',{method:'GET',headers:{authorization:`Bearer ${await token()}`}});return{errors:[]};}catch(e){return{errors:[e.message]};}};
 const options={request,reads,token:cachedScopedToken(async()=>jwt(now),scope,()=>now),apiBase:'http://local',refresh,validateBody:(_p,b)=>b.payload.valid,now:()=>now,wallNow:()=>now};
 return {options,calls,responses,body,response,setNow:n=>now=n,setComputed:n=>computed=n,getNow:()=>now};
}
test('fresh headroom performs read-only exact-path setup and retains full evidence',async()=>{
 const h=harness();h.setNow(1_005_000);const r=await prepareHealthyLoad(h.options);
 assert.equal(r.refreshed,false);assert.deepEqual(h.calls.map(c=>c.method),['GET']);assert.equal(r.attempts[0].body.payload.valid,true);assert.equal(r.attempts[0].states[0].model,'home_metrics');assert.equal(r.attempts[0].status,200);assert.ok(r.attempts[0].finished!==null);assert.equal(r.errors.length,0);
});
test('twenty ordinary loads crossing compiled max-age headroom require actual registered POST and verified GET',async()=>{
 const h=harness();const receipts=[];for(let i=0;i<20;i++){h.setNow(1_000_000+i*3000);receipts.push(await prepareHealthyLoad(h.options));}
 assert.equal(receipts.filter(r=>r.refreshed).length,1);const r=receipts.find(r=>r.refreshed);assert.deepEqual(r.attempts.map(a=>a.method),['GET','POST','GET']);assert.equal(r.headroom[0].max_age_ms,manifest.home_metrics.max_delay_seconds*1000);assert.equal(r.headroom[0].reserved_headroom_ms,r.headroom[0].max_age_ms/2);
});
test('a lawful cadence jump past the age bound refreshes before any measured navigation',async()=>{
 const h=harness();h.setNow(1_000_000+manifest.home_metrics.max_delay_seconds*1000+1);const r=await prepareHealthyLoad(h.options);assert.equal(r.refreshed,true);assert.equal(r.attempts.at(-1).states[0].computed_at,new Date(h.getNow()).toISOString());
});
test('stale ETA or missing computation force refresh even for a young certificate',async()=>{
 for(const changed of [{stale:true},{refresh_eta:new Date(1_001_000).toISOString()},{computed_at:null}]){const h=harness();const original=h.options.request.fetch;h.options.request.fetch=async(url,options)=>options.method==='GET'&&h.calls.length===0?(h.calls.push({method:'GET'}),h.response({...h.body(),...changed})):original(url,options);assert.equal((await prepareHealthyLoad(h.options)).refreshed,true);}
});
test('cached credentials reuse the real scope and refuse a different actor before HTTP',async()=>{
 const h=harness();let minted=0;const token=cachedScopedToken(async()=>{minted++;return jwt(h.getNow());},scope,()=>h.getNow());await token();await token();assert.equal(minted,1);
 h.options.token=cachedScopedToken(async()=>jwt(h.getNow(),'wrong'),scope,()=>h.getNow());await assert.rejects(prepareHealthyLoad(h.options),e=>e.receipt.errors.length===1&&/scope/.test(e.message));assert.equal(h.calls.length,0);
});
test('wrong model, invalid application certificate and changed constituent fail closed with body retained',async()=>{
 for(const type of ['model','validator','constituent']){const h=harness();if(type==='model')h.options.request.fetch=async()=>h.response({...h.body(),model:'connector_status',version:manifest.connector_status.version});if(type==='validator')h.options.validateBody=()=>false;if(type==='constituent')h.options.reads[0].states[0].identity='foreign';await assert.rejects(prepareHealthyLoad(h.options),e=>e.receipt.attempts[0].body!==null&&e.receipt.attempts[0].failure!==null&&e.receipt.finished!==null);}
});
test('network setup failures retain started/status-null/finished/error and never refresh',async()=>{
 const h=harness();h.options.request.fetch=async()=>{throw new Error('ERR_CONNECTION_RESET');};await assert.rejects(prepareHealthyLoad(h.options),e=>{const a=e.receipt.attempts[0];return a.dispatched&&a.status===null&&a.started!==null&&a.finished!==null&&a.failure==='ERR_CONNECTION_RESET'&&!e.receipt.refreshed;});
});
test('registered refresh failure retains its real body/status and fails before measurement',async()=>{
 const h=harness();h.setNow(1_040_000);const original=h.options.request.fetch;h.options.request.fetch=async(url,options)=>options.method==='POST'?h.response({error:'failed'},503):original(url,options);await assert.rejects(prepareHealthyLoad(h.options),e=>{const a=e.receipt.attempts.find(a=>a.method==='POST');return e.receipt.refreshed&&a.status===503&&a.body.error==='failed'&&a.finished!==null&&e.receipt.errors.length===1;});
});

test('response buffers are released after successful refresh consumers finish',async()=>{
 const h=harness();h.setNow(1_040_000);const original=h.options.refresh;
 h.options.refresh=async(...args)=>{const r=await original(...args);assert.ok(h.responses.every(response=>response.disposed===0));await h.responses.at(-1).json();return r;};
 await prepareHealthyLoad(h.options);assert.equal(h.responses.length,3);assert.ok(h.responses.every(response=>response.disposed===1));
});
test('certificate rejection and failed refresh release all acquired response buffers',async()=>{
 for(const kind of ['certificate','refresh']){const h=harness();if(kind==='certificate')h.options.validateBody=()=>false;else {h.setNow(1_040_000);const original=h.options.request.fetch;h.options.request.fetch=async(url,options)=>options.method==='POST'?h.response({error:'failed'},503):original(url,options);}
 await assert.rejects(prepareHealthyLoad(h.options));assert.ok(h.responses.length>0);assert.ok(h.responses.every(response=>response.disposed===1));}
});
test('disposal failure still attempts every response and fails closed',async()=>{
 const h=harness();h.setNow(1_040_000);const original=h.options.refresh;h.options.refresh=async(...args)=>{const r=await original(...args);h.responses[0].dispose=async()=>{h.responses[0].disposed++;throw new Error('cleanup failed');};return r;};
 await assert.rejects(prepareHealthyLoad(h.options),e=>e.receipt.errors.some(error=>error.includes('disposal')));assert.ok(h.responses.every(response=>response.disposed===1));
});

for (const jump of [-60_000,60_000]) test(`certificate age remains wall time after ${jump}ms wall step`,async()=>{
 const h=harness();let elapsed=0,wall=1_000_000;
 const clock=createPageLoadClock({elapsedNow:()=>elapsed,wallNow:()=>wall});
 elapsed+=10_000;wall+=10_000+jump;h.setNow(wall);
 h.options.now=clock.now;h.options.wallNow=clock.wallNow;
 const receipt=await prepareHealthyLoad(h.options);
 assert.equal(receipt.headroom[0].age_ms,10_000+jump);
 assert.equal(receipt.refreshed,jump>0);
 assert.equal(receipt.started,1_010_000);assert.equal(receipt.wallStarted,wall);
 assert.ok(h.responses.every(response=>response.disposed===1));
});
for (const direction of ['forward','backward']) test(`${direction} wall step cannot shorten or extend setup's finite elapsed deadline`,async()=>{
 const h=harness();let elapsed=0,wall=1_040_000;
 const clock=createPageLoadClock({elapsedNow:()=>elapsed,wallNow:()=>wall});h.setNow(wall);
 h.options.now=clock.now;h.options.wallNow=clock.wallNow;
 const originalDateNow=Date.now;Date.now=()=>wall;
 h.options.refresh=async()=>{elapsed+=direction==='forward'?1:120_001;wall+=direction==='forward'?86_400_000:-86_400_000;return {errors:[]};};
 try {
  if(direction==='forward') {const r=await prepareHealthyLoad(h.options);assert.equal(r.finished-r.started,1);assert.equal(r.wallFinished-r.wallStarted,86_400_000);}
  else await assert.rejects(prepareHealthyLoad(h.options),e=>/exceeded120s/.test(e.message)&&e.receipt.finished-e.receipt.started===120_001);
  assert.ok(h.responses.every(response=>response.disposed===1));
 } finally {Date.now=originalDateNow;}
});
test('cached token expiration follows raw wall time rather than the anchored elapsed epoch',async()=>{
 let wall=1_000_000,elapsed=0,minted=0;
 const clock=createPageLoadClock({elapsedNow:()=>elapsed,wallNow:()=>wall});
 const token=cachedScopedToken(async()=>{minted++;return jwt(wall);},scope,clock.wallNow);
 await token();wall+=3_600_000;await token();assert.equal(minted,2);assert.equal(clock.now(),1_000_000);
});

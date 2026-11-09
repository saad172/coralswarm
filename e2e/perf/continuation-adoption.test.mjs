import {test} from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import ts from 'typescript';
import {expect} from '@playwright/test';
import {assembleContinuation,bindingHash,finalContinuationUnion,strictReceipt} from './continuation-adoption.mjs';
import {sha256,caseIdentity} from './continuation.mjs';
import {classifyDocument,safeDocumentUrl} from './auth-evidence.mjs';
import {assertStatesForPath} from './envelopes.mjs';
import {assertSpeed} from './timings.mjs';
import manifest from '../../web/lib/pageSections.generated.json' with {type:'json'};
const source=readFileSync(new URL('./nightly.ts',import.meta.url),'utf8');
const pure=source.slice(source.indexOf('export function discoveredReads('),source.indexOf('export async function refreshDiscovered('))+source.slice(source.indexOf('export function routeStatistics('));
const js=ts.transpileModule(pure,{compilerOptions:{target:ts.ScriptTarget.ES2022,module:ts.ModuleKind.CommonJS}}).outputText;
const routeStatistics=new Function('exports','expect','assertStatesForPath','manifest','assertSpeed',`${js}\nreturn exports.routeStatistics;`)({},expect,assertStatesForPath,manifest,assertSpeed);
const now=1000,scope='["issuer","user_fixture","org_fixture"]';
const inventory=Array.from({length:96},(_,i)=>({route:`/fixture/${i}`,path:`/fixture/${i}`}));
const priorSource={revision:'prior',sha256:sha256('old-source')},currentSource={revision:'current',sha256:sha256('current-source')};
const traceHash=sha256('private-original-trace'),guardHash=sha256('reviewed-original-guard');
const binding={accountId:'account',actorId:'actor',clerkUserId:'user_fixture',orgId:'org_fixture',runId:'owned',stackId:'stack',ownerIdentity:{namespace:'nightly',owner:'owned',lease:{id:'lease',expiresAt:100000}},fixtureSha256:sha256('fixture'),seedSha256:sha256('seed'),backendSha256:sha256('backend'),migratorSha256:sha256('migrators'),web:{build:'original'}};
const principal={kind:'original-clerk-principal',sessionId:'original_PRIVATE_SID',userId:'user_fixture',orgId:'org_fixture',issuer:'issuer',scope,traceSha256:traceHash,sourceFingerprint:priorSource};
const browser={loaded:true,signed_in:true,session_active:true,session_matches:true,user_matches:true,organization_matches:true,session_organization_matches:true};
const state={identity:'single',model:'home_metrics',version:manifest.home_metrics.version,computed_at:'2026-11-01T00:00:00Z',stale:false,refresh_eta:null};
const record={method:'GET',path:'/v1/home/metrics',scope,priority:'critical',total_ms:1,models:[state]};
const declarationFor=()=>({access:'authenticated',webBase:'https://web.fixture'}),resolveAlias=()=>null;
function observation(path){return {path,errors:[],records:[structuredClone(record)],ledger:[],document:{ok:true,fatal:false,kind:'authenticated-document',reasons:[],browser,final_url:`https://web.fixture${path}`,canonical_destination:`https://web.fixture${path}`,api_scope_matches_expected:true,frames:[]}};}
function receipt(i,revision='prior'){const item=inventory[i];return {...item,status:'passed',validation_errors:[],discovery:observation(item.path),loads:Array.from({length:20},()=>observation(item.path)),discovered_reads:[{path:record.path,scope,states:[state]}],refresh:{errors:[],ledger:[]},per_load_setup:Array.from({length:20},(_,load)=>({load:load+1,errors:[],attempts:[]})),document_inventory:{discovery_attempted:true,discovery_returned:true,measured_attempted:20,measured_returned:20,measured_not_run:0,harness_source:revision}};}
const blob=(id,value)=>{const json=JSON.stringify(value,null,2)+'\n';return {id,json,sha256:sha256(json)};};
function fixture(cases=[receipt(0)]){
 const bundle=blob('bundle',{inventory,bindings:binding,sourceFingerprint:priorSource,cases:cases.map((r,i)=>blob(`receipt-${i}`,r))});
 const principalProof=blob('original-principal',principal);
 const qualificationProof=blob('review',{approved:true,approvalId:'EXPLICIT-ROOT-APPROVAL',prior:priorSource,current:currentSource,bindingsSha256:bindingHash(binding),inventorySha256:bindingHash(inventory),principalProofSha256:principalProof.sha256,bundleSha256:bundle.sha256,priorBindingsSha256:bindingHash(binding),destinationGuard:{checkedBeforeRedaction:true,sourceSha256:guardHash}});
 return {version:1,inventory,bindings:binding,currentSource,artifacts:[{path:'private-original-trace',sha256:traceHash},{path:'old-source',sha256:guardHash}],priors:[{bundle,principalProof,qualificationProof}]};
}
function assemble(input=fixture(),overrides={}){const inputText=JSON.stringify(input);return assembleContinuation({inputText,inputSha256:sha256(inputText),inventory,bindings:binding,currentSource,now,declarationFor,classifyDocument,safeDocumentUrl,resolveAlias,routeStatistics,validateSetupBody:()=>true,verifyArtifact:()=>true,...overrides});}

test('actual classifier/statistics credit known twenty; failed/footerless partial remain selected and never repaired',()=>{
 const failed={...receipt(1),status:'failed',error:'original failure'};const partial={...receipt(2),status:undefined,loads:receipt(2).loads.slice(0,7)};delete partial.document_inventory;
 const {plan}=assemble(fixture([receipt(0),failed,partial]));
 assert.equal(plan.acceptedCompleted.length,1);assert.equal(plan.remaining.length,95);assert.equal(plan.acceptedCompleted[0].statistics.p95_ms,1);
 const navigated=plan.remaining.map(caseIdentity);assert.ok(!navigated.includes(caseIdentity(inventory[0])));assert.ok(navigated.includes(caseIdentity(inventory[1]))&&navigated.includes(caseIdentity(inventory[2])));
 assert.deepEqual(plan.history.map(r=>r.credited),[true,false,false]);assert.equal(plan.history[2].receiptReference.id,'receipt-2');assert.equal(partial.loads.length,7);
});

test('input hash, artifact absence, source and exact backend/lease/fixture bindings fail closed',()=>{
 assert.throws(()=>assemble(fixture(),{inputSha256:sha256('changed')}),/hash/);
 assert.throws(()=>assemble(fixture(),{verifyArtifact:()=>false}),/artifact/);
 for(const field of ['backendSha256','fixtureSha256','seedSha256','migratorSha256'])assert.throws(()=>assemble(fixture(),{bindings:{...binding,[field]:sha256('foreign')}}),/binding/);
 assert.throws(()=>assemble(fixture(),{currentSource:{...currentSource,revision:'unreviewed'}}),/source/);
 assert.throws(()=>assemble(fixture(),{now:100000}),/live/);
 const input=fixture();input.bindings=structuredClone(binding);input.bindings.ownerIdentity.lease.id='new-lease';assert.throws(()=>assemble(input),/binding/);
});

test('missing original principal, pinned trace/source or explicit root approval cannot be replaced by new login',()=>{
 for(const change of [x=>{delete x.priors[0].principalProof;},x=>{x.artifacts=[];},x=>{x.priors[0].qualificationProof=blob('review',{});},x=>{x.priors[0].principalProof=blob('replacement',{...principal,sessionId:'NEW_LOGIN'});}]){const input=fixture();change(input);assert.throws(()=>assemble(input),/proof|artifacts|qualification/);}
});

test('correct auth URL with missing model, wrong scope or signedout browser loses prior credit',()=>{
 for(const change of [r=>{r.loads[3].records=[];},r=>{r.loads[3].records[0].scope='public';},r=>{r.loads[3].document.browser={...browser,session_matches:false};}]){const r=receipt(0);change(r);assert.equal(assemble(fixture([r])).plan.acceptedCompleted.length,0);}
});

test('redacted URL uses hashed original before-redaction guard witness without inventing an unredacted actual URL',()=>{
 const r=receipt(0);r.path='/fixture/0?code=private-known';for(const load of [r.discovery,...r.loads]){load.path=r.path;load.document.final_url=safeDocumentUrl(r.path,'https://web.fixture');load.document.canonical_destination=load.document.final_url;}
 const verdict=strictReceipt(r,{principal,declaration:declarationFor(),bindings:binding,classifyDocument,safeDocumentUrl,resolveAlias,routeStatistics,validateSetupBody:()=>true,guardWitness:{checkedBeforeRedaction:true,sourceSha256:guardHash}});assert.equal(verdict.ok,true);
 assert.throws(()=>strictReceipt(r,{principal,declaration:declarationFor(),bindings:binding,classifyDocument,safeDocumentUrl,resolveAlias,routeStatistics,validateSetupBody:()=>true,guardWitness:{}}),/witness/);
 r.loads[0].document.final_url='https://web.fixture/login';assert.throws(()=>strictReceipt(r,{principal,declaration:declarationFor(),bindings:binding,classifyDocument,safeDocumentUrl,resolveAlias,routeStatistics,validateSetupBody:()=>true,guardWitness:{checkedBeforeRedaction:true,sourceSha256:guardHash}}),/revalidation/);
});

test('final acceptance is exact prior plus all selected freshly validated twenty, not selected-only success',()=>{
 const {plan}=assemble();const current=plan.remaining.map(item=>blob(`new-${item.route}`,receipt(Number(item.route.split('/').at(-1)),'current')));
 const validate=r=>strictReceipt(r,{principal,declaration:declarationFor(),bindings:binding,classifyDocument,safeDocumentUrl,resolveAlias,routeStatistics,validateSetupBody:()=>true,guardWitness:{checkedBeforeRedaction:true,sourceSha256:guardHash}});
 const combined=finalContinuationUnion(plan,current,validate);assert.equal(combined.acceptedRepresentatives,96);assert.equal(combined.acceptedMeasuredLoads,1920);
 assert.throws(()=>finalContinuationUnion(plan,current.slice(1),validate),/incomplete/);
 assert.throws(()=>finalContinuationUnion(plan,[blob('repeat',receipt(0,'current')),...current.slice(1)],validate),/unexpected/);
 const broken=structuredClone(current);const r=JSON.parse(broken[0].json);r.loads.pop();broken[0]=blob('short',r);assert.throws(()=>finalContinuationUnion(plan,broken,validate),/twenty/);
});

function renewalFixture(){
 const ids=['user_fixture','user_member'],from=structuredClone(binding.ownerIdentity),to={...from,lease:{...from.lease,expiresAt:7200500}};
 const safe=(id,identity)=>({userId:id,namespace:identity.namespace,owner:identity.owner,lease:identity.lease,allEmailsMatch:true,profileSha256:sha256('same-profile'),privateMetadataSha256:sha256('metadata'+identity.lease.expiresAt),publicMetadataSha256:sha256('same-public')});
 const before=ids.map(id=>safe(id,from)),after=ids.map(id=>safe(id,to));
 const checks={sameUser:true,sameNamespaceOwnerLeaseId:true,newExpiryExact:true,allEmailsMatch:true,allPrivateMetadataPreservedExceptExpiry:true,profilePublicEmailNameUnchanged:true};
 const receipt={authorization:{reference:'EXPLICIT SYNTHETIC ROOT TEST'},from,to,renewedAtUnixMs:500,beforeReadAtUnixMs:400,verifiedAt:900,status:'verified-all-participants-before-old-expiry',participantCount:2,updatedCount:2,verifiedCount:2,onlyExpiryChanged:true,noSessionOrTokenCalls:true,organizationOrRoleCalls:0,participantIds:ids,before,after,participants:ids.map((id,i)=>({userId:id,before:before[i],after:after[i],checks})),operationCodeSha256:sha256('operation'),participantSetProofSha256:sha256('participants'),calls:['before-get','update','after-get'].flatMap((stage,i)=>ids.map(id=>({stage,method:i===1?'PATCH':'GET',path:`/v1/users/${id}${i===1?'/metadata':''}`,status:200,started:100+i*250,finished:200+i*250})))};
 receipt.authorizationSha256=bindingHash(receipt.authorization);receipt.beforeSha256=bindingHash(before);receipt.afterSha256=bindingHash(after);
 return {receipt,to,makeInput(r=receipt){const renewalProof=blob('server-renewal',r);return {renewalProof,renewalApprovalProof:blob('root-renewal',{approved:true,approvalId:'EXPLICIT-SYNTHETIC-ROOT',receiptSha256:renewalProof.sha256,operationCodeSha256:r.operationCodeSha256,authorizationSha256:r.authorizationSha256,participantSetProofSha256:r.participantSetProofSha256,participantIds:ids}),artifacts:[{sha256:renewalProof.sha256},{sha256:r.operationCodeSha256},{sha256:r.participantSetProofSha256}]};}};
}

test('real renewal verifier checks all participant receipt hashes, operation, HTTP phases and historical before-expiry times',async()=>{
 const {verifyRenewal}=await import('./continuation-adoption.mjs');const f=renewalFixture(),input=f.makeInput();
 const context={lineage:{proof:{id:input.renewalProof.id,sha256:input.renewalProof.sha256},from:f.receipt.from,to:f.to,verifiedAt:900},priorBindings:binding,bindings:{...binding,ownerIdentity:f.to},now:100001};
 assert.equal(verifyRenewal(context,input),true);
 assert.equal(bindingHash({b:1,a:2}),bindingHash({a:2,b:1}));assert.notEqual(bindingHash([1,2]),bindingHash([2,1]));
 for(const change of [r=>{r.verifiedCount=1;},r=>{r.participants[0].checks.profilePublicEmailNameUnchanged=false;},r=>{r.calls[2].status=500;},r=>{r.beforeSha256=sha256('wrong');},r=>{r.verifiedAt=r.from.lease.expiresAt;}]){const r=structuredClone(f.receipt);change(r);const bad=f.makeInput(r);assert.throws(()=>verifyRenewal({...context,lineage:{...context.lineage,proof:{id:bad.renewalProof.id,sha256:bad.renewalProof.sha256},verifiedAt:r.verifiedAt}},bad));}
});

test('assembler accepts only actual verified renewal lineage and still preserves old raw proof/source pins',()=>{
 const f=renewalFixture(),renewal=f.makeInput(),input=fixture();input.bindings={...binding,ownerIdentity:f.to};input.renewalProof=renewal.renewalProof;input.renewalApprovalProof=renewal.renewalApprovalProof;input.artifacts.push(...renewal.artifacts);input.leaseRenewal={proof:{id:renewal.renewalProof.id,sha256:renewal.renewalProof.sha256},from:f.receipt.from,to:f.to,verifiedAt:900};
 const q=JSON.parse(input.priors[0].qualificationProof.json);q.bindingsSha256=bindingHash(input.bindings);q.leaseRenewalProofSha256=renewal.renewalProof.sha256;input.priors[0].qualificationProof=blob('review',q);
 const {plan}=assemble(input,{bindings:input.bindings,now:100001});assert.equal(plan.acceptedCompleted.length,1);assert.equal(plan.leaseLineage.length,1);assert.equal(plan.history[0].sourceQualification.prior.revision,'prior');assert.equal(plan.history[0].receiptReference.id,'receipt-0');
 const missing=structuredClone(input);delete missing.renewalApprovalProof;assert.throws(()=>assemble(missing,{bindings:input.bindings,now:100001}),/renewal/);
});

test('compact manifest lazily reads exact original cases; output retains only history references',()=>{
 const partial={...receipt(2),status:undefined,loads:receipt(2).loads.slice(0,7)};delete partial.document_inventory;
 const input=fixture([receipt(0),{...receipt(1),status:'failed'},partial]);const bundle=JSON.parse(input.priors[0].bundle.json),files=new Map(),reads=[];
 bundle.cases=bundle.cases.map((b,i)=>{const path=`case-${i}`;files.set(path,b.json);return{id:b.id,sha256:b.sha256,path};});
 const compact=blob('bundle',bundle);files.set('bundle-file',compact.json);input.priors[0].bundle={id:compact.id,sha256:compact.sha256,path:'bundle-file'};
 const approval=JSON.parse(input.priors[0].qualificationProof.json);approval.bundleSha256=compact.sha256;input.priors[0].qualificationProof=blob('review',approval);
 const readPriorBlob=(ref,{kind})=>{reads.push([ref.path,kind]);return files.get(ref.path);};
 const {plan}=assemble(input,{readPriorBlob});assert.equal(plan.acceptedCompleted.length,1);assert.equal(plan.remaining.length,95);
 assert.equal(reads.filter(r=>r[1]==='case').length,3);assert.ok(plan.history.every(row=>row.receipt===undefined&&row.receiptReference.sha256));
 files.set('case-0',files.get('case-0')+' ');assert.throws(()=>assemble(input,{readPriorBlob}),/hash mismatch/);
});

import fs from 'node:fs';
import path from 'node:path';
import { test, expect } from '../lib/fixtures';
import { addMember, createReadyOrg } from '../lib/isolation';
import { googlePushToken } from "../lib/api";
import { selectRows } from "../lib/stack-db";
import { API_BASE, PORTS, FAKE, GOOGLE_PUSH_AUDIENCE, STATE_DIR } from '../stack/config.mjs';
import { freshPayload } from '../lib/readModels';

const fake = `http://127.0.0.1:${PORTS.fakes}`;

test('Google browser consent persists multiple sources and captures readable Calendar content', async ({page,signedIn,request,makeUser}) => {
  test.setTimeout(180_000);
  const id = `g${Date.now().toString(36)}`;
  const start = new Date(Date.now()-3600000).toISOString();
  const end = new Date(Date.now()+3600000).toISOString();
  const historical=(days:number)=>({id:`historical-${days}-${id}`,summary:`Historical ${days} day meeting`,description:'Historical calendar acceptance evidence.',start:{dateTime:new Date(Date.now()-days*86400000).toISOString()},end:{dateTime:new Date(Date.now()-days*86400000+3600000).toISOString()},status:'confirmed'});
  const register = await request.post(`${fake}/__google/native/user`, {data:{id,email:`${id}@example.test`,routes:{
    'POST www.googleapis.com/calendar/v3/calendars/primary/events/watch':{dynamicCalendarWatch:true},
    'GET www.googleapis.com/calendar/v3/calendars/primary/events': {pages:{'':{body:{items:[{id:`event-${id}`,summary:'Native Google acceptance meeting',description:'The acceptance meeting approved the calendar capture design.',start:{dateTime:start},end:{dateTime:end},status:'confirmed',updated:start},historical(400)],nextPageToken:'history'}},history:{body:{items:[historical(120),historical(400)],nextSyncToken:'sync-1'}}}},
    'POST gmail.googleapis.com/gmail/v1/users/me/watch':{body:{historyId:'1',expiration:String(Date.now()+86400000)}},
    'GET gmail.googleapis.com/gmail/v1/users/me/profile':{body:{emailAddress:`${id}@example.test`,historyId:'1'}},
    'POST workspaceevents.googleapis.com/v1/subscriptions':{body:{name:`operations/${id}`,done:true,response:{name:`subscriptions/${id}`,expireTime:new Date(Date.now()+86400000).toISOString()}}},
    'GET chat.googleapis.com/v1/spaces':{body:{spaces:[]}},
    'GET meet.googleapis.com/v2/conferenceRecords':{body:{conferenceRecords:[]}},
    'GET meet.googleapis.com/v2/spaces/abc-defg-hij':{body:{name:'spaces/accepted',meetingCode:'abc-defg-hij'}},
  }}});
  expect(register.ok()).toBeTruthy();
  await page.goto('/connectors');
  await page.getByRole('button',{name:'Connect Google',exact:true}).click();
  await page.getByRole('button',{name:'Sign in to Google',exact:true}).click();
  await page.getByLabel('Google fixture identity').fill(id);
  await page.getByRole('button',{name:'Allow',exact:true}).click();
  await expect(page.getByRole('checkbox',{name:'Calendar',exact:true})).toBeVisible();
  for(const source of ['Gmail','Calendar','Meet']) await page.getByText(source,{exact:true}).last().click();
  await page.getByRole('button',{name:'Continue',exact:true}).click();
  for(const source of ['gmail','calendar','meet']) {
    // A click resolves before its async save. Reload only after the actual
    // backend acknowledgement; otherwise navigation aborts the final Meet PUT.
    const saved=page.waitForResponse(r=>r.request().method()==='PUT' && new URL(r.url()).pathname===`/v1/connectors/google/sources/${source}/scope`);
    await page.getByRole('button',{name:'Continue',exact:true}).click();
    expect((await saved).status()).toBe(200);
  }
  await expect(page.getByRole('link',{name:'View Google',exact:true})).toBeVisible();
  await page.reload();
  const api=signedIn.api;
  const detail=await freshPayload(api, '/v1/connectors/google');
  expect(detail.sources.filter((s:any)=>s.enabled).map((s:any)=>s.key)).toEqual(['gmail','calendar','meet']);
  expect(detail.sources.find((s:any)=>s.key==='calendar')).toMatchObject({enabled:true,scope:{mode:'everything'}});
  for(const source of detail.sources.filter((s:any)=>s.enabled)) {
    expect(source.setup_completed_at).toBeTruthy();
    expect(source.history_requested_at).toBeNull();
  }
  const completion=detail.sources.map((s:any)=>s.setup_completed_at);
  await page.reload();
  expect((await freshPayload(api, '/v1/connectors/google')).sources.map((s:any)=>s.setup_completed_at)).toEqual(completion);
  expect((await freshPayload(api, '/v1/connectors/google')).status).toBe('active');
  const watchLog=await (await request.get(`${fake}/__google/native/requests?user=${id}`)).json();
  const watch=watchLog.find((r:any)=>r.route.endsWith('/events/watch'));
  expect(watch).toBeTruthy();
  expect((await request.post(`${API_BASE}/v1/webhooks/google/calendar`,{headers:{'x-goog-channel-id':watch.body.id,'x-goog-channel-token':watch.body.token,'x-goog-resource-state':'exists'}})).ok()).toBeTruthy();
  const graph=await api.personalGraph();
  let captured:any;
  await expect.poll(async()=>{
    const feed=await api.json('GET',`/v1/sources/items?graph_id=${graph.id}&source_type=google_calendar`);
    captured=feed.items?.find((item:any)=>item.source_id===`event-${id}`);
    return !!captured;
  },{timeout:90_000}).toBe(true);
  // The worker persists events sequentially. Seeing the first event in the
  // source feed does not mean the historical event has finished ingesting.
  let captureFeed:any;
  await expect.poll(async()=>{
    captureFeed=await api.json('GET','/v1/connectors/google/captures?source=calendar');
    return captureFeed.items.map((item:any)=>item.title).sort();
  },{timeout:90_000}).toEqual(['Historical 120 day meeting','Native Google acceptance meeting']);
  expect.soft(captureFeed.items).toHaveLength(2);
  expect(captureFeed.items.map((i:any)=>i.title)).toContain('Historical 120 day meeting');
  expect.soft(captureFeed.items.map((i:any)=>i.title)).not.toContain('Historical 400 day meeting');
  captured=captureFeed.items.find((i:any)=>i.title==='Native Google acceptance meeting')??captureFeed.items[0];
  expect(captured?.atom_id).toBeTruthy();
  let content='';
  await expect.poll(async()=>{
    const read=await api.call('GET',`/v1/atoms/${captured.atom_id}`);
    if(read.ok()) content=(await read.json()).content;
    return content;
  }).toContain('acceptance');
  const journal=await (await request.get(`${fake}/__google/native/requests?user=${id}`)).json();
  expect(journal.some((r:any)=>r.route==='oauth2.googleapis.com/token')).toBe(true);
  const initial=journal.find((r:any)=>r.route.endsWith('/events'));
  expect(initial).toBeTruthy();
  // Google sync-token ingestion enumerates complete event resources; the
  // documented 365-day history limit is asserted at the capture feed above.
  expect(initial.query).toMatchObject({singleEvents:'false',showDeleted:'true',maxResults:'250'});
  expect(initial.query.timeMin).toBeUndefined();
  expect(initial.query.timeMax).toBeUndefined();
  expect(initial.query.syncToken).toBeUndefined();
  const continuation=journal.find((r:any)=>r.route.endsWith('/events')&&r.query.pageToken==='history');
  expect(continuation).toBeTruthy();
  expect(continuation.query.timeMin).toBeUndefined();
  expect(continuation.query.timeMax).toBeUndefined();
  const historicalFeed=await api.json('GET',`/v1/sources/items?graph_id=${graph.id}&source_type=google_calendar`);
  expect(historicalFeed.items.some((i:any)=>i.source_id===`historical-120-${id}`)).toBe(true);
  expect(historicalFeed.items.some((i:any)=>i.source_id===`historical-400-${id}`)).toBe(false);
  const other=await makeUser('google-other');
  await other.api.graphs();
  expect((await other.api.call('GET',`/v1/atoms/${captured.atom_id}`)).status()).toBe(404);
  await api.json('PATCH','/v1/connectors/google/sources/calendar',{paused:true});
  await request.post(`${fake}/__google/native/user`,{data:{id,routes:{
    'POST www.googleapis.com/calendar/v3/calendars/primary/events/watch':{dynamicCalendarWatch:true},
    'GET www.googleapis.com/calendar/v3/calendars/primary/events':{body:{items:[{id:`resumed-${id}`,summary:'Resumed calendar acceptance',description:'Resume acceptance captured a second meeting.',start:{dateTime:start},end:{dateTime:end},status:'confirmed',updated:start},historical(400),{...historical(400),id:`event-${id}`,description:'An aged update must not replace the retained capture.'}],nextSyncToken:'sync-2'}}
  }}});
  const logFile=path.join(STATE_DIR,'backend.log');
  const before=fs.readFileSync(logFile,'utf8').length;
  const notify=()=>request.post(`${API_BASE}/v1/webhooks/google/calendar`,{headers:{'x-goog-channel-id':watch.body.id,'x-goog-channel-token':watch.body.token,'x-goog-resource-state':'exists'}});
  expect((await notify()).ok()).toBeTruthy();
  await expect.poll(()=>fs.readFileSync(logFile,'utf8').slice(before).includes('google calendar push: pass skipped')).toBe(true);
  expect((await(await request.get(`${fake}/__google/native/requests?user=${id}`)).json()).filter((r:any)=>r.route.endsWith('/events'))).toHaveLength(0);
  expect.soft((await api.json('GET','/v1/connectors/google/captures?source=calendar')).items).toHaveLength(2);
  await api.json('PATCH','/v1/connectors/google/sources/calendar',{paused:false});
  expect((await notify()).ok()).toBeTruthy();
  await expect.poll(async()=>(await api.json('GET',`/v1/sources/items?graph_id=${graph.id}&source_type=google_calendar`)).items.some((i:any)=>i.source_id===`resumed-${id}`)).toBe(true);
  const resumedFeed=await api.json('GET','/v1/connectors/google/captures?source=calendar');
  expect(resumedFeed.items).toHaveLength(3);
  expect(resumedFeed.items.map((i:any)=>i.title)).not.toContain('Historical 400 day meeting');
  const incremental=(await(await request.get(`${fake}/__google/native/requests?user=${id}`)).json()).find((r:any)=>r.route.endsWith('/events'));
  expect(incremental.query.syncToken).toBe('sync-1');
  expect(incremental.query.timeMin).toBeUndefined();
  expect(incremental.query.timeMax).toBeUndefined();
  // Keep completion assertion without preventing independent retention assertions.
  expect.soft((await freshPayload(api, '/v1/connectors/google')).status).toBe('active');
  expect((await api.call('DELETE','/v1/connectors/google')).status()).toBe(204);
  expect((await freshPayload(api, '/v1/connectors/google')).status).toBe('not_connected');
  await expect.poll(async()=>{const r=await api.call('GET',`/v1/atoms/${captured.atom_id}`);return r.ok()?(await r.json()).content:`HTTP ${r.status()}`;}).toBe(content);
});

test('Google native union grant persists independent scopes and rejects unsupported histories', async ({signedIn,request})=>{
  test.setTimeout(90_000);
  const api=signedIn.api;
  const id=`g${Date.now().toString(36)}`;
  expect((await request.post(`${fake}/__google/native/user`,{data:{id,routes:{
    'POST www.googleapis.com/calendar/v3/calendars/primary/events/watch':{dynamicCalendarWatch:true},
    'GET chat.googleapis.com/v1/spaces':{body:{spaces:[]}},
    'GET meet.googleapis.com/v2/conferenceRecords':{body:{conferenceRecords:[]}},
    'GET meet.googleapis.com/v2/spaces/abc-defg-hij':{body:{name:'spaces/accepted',meetingCode:'abc-defg-hij'}},
    'GET www.googleapis.com/calendar/v3/calendars/primary/events':{body:{items:[],nextSyncToken:'empty'}},
    'GET gmail.googleapis.com/gmail/v1/users/me/profile':{body:{emailAddress:`${id}@example.test`,historyId:'1'}},
    'POST gmail.googleapis.com/gmail/v1/users/me/watch':{body:{historyId:'1',expiration:String(Date.now()+86400000)}},
  }}})).ok()).toBeTruthy();
  const connect=await api.json('POST','/v1/connectors/google/connect',{return_to:'wizard'});
  const authorize=await request.get(connect.authorize_url,{maxRedirects:0});
  const consent=new URL(authorize.headers().location);consent.searchParams.set('login_hint',id);
  const granted=await request.get(consent.href,{maxRedirects:0});expect(granted.status()).toBe(302);
  const callback=await request.get(granted.headers().location,{maxRedirects:0});expect(callback.status()).toBe(307);
  for(const source of ['gmail','calendar','chat','meet']){
    const base=`/v1/connectors/google/sources/${source}`;
    await api.json('PATCH',base,{enabled:true});
    await api.json('PUT',`${base}/scope`,source==='calendar'?{mode:'everything'}:{mode:'pick',item_ids:[]});
    const detail=await freshPayload(api, '/v1/connectors/google');
    const saved=detail.sources.find((s:any)=>s.key===source);
    expect(saved.enabled).toBe(true);
    if(source!=='chat') {
      expect(saved.history.supported).toBe(false);
      expect(saved.history.reason).toBe(({gmail:'followed_threads_only',calendar:'automatic_window',meet:'per_meeting'} as Record<string,string>)[source]);
      const history=await api.call('POST',`${base}/backfill`,{days:7});
      expect(history.status()).toBe(409);
      expect((await history.json()).code).toBe('history_unsupported');
    }
    if(source==='meet'){
      expect(saved.scope_shape).toBe('pick_only');
      expect((await api.json('GET',`${base}/scope-items`)).items).toHaveLength(0);
      const allowed=await api.json('POST','/v1/connectors/google/meet/captures',{meeting_code:'abc-defg-hij'});
      expect(allowed.capture_state).toBe('allowed');
      const options=await api.json('GET',`${base}/scope-items`);
      expect(options.items.map((i:any)=>i.id)).toContain(allowed.capture.id);
      await api.json('PUT',`${base}/scope`,{mode:'pick',item_ids:[allowed.capture.id]});
      await api.json('PUT',`${base}/scope`,{mode:'pick',item_ids:[]});
      expect((await api.json('GET',`${base}/scope-items`)).items).toHaveLength(0);
      expect((await api.call('PUT',`${base}/scope`,{mode:'everything'})).status()).toBe(422);
    }
    await api.json('PATCH',base,{paused:true});
    expect((await freshPayload(api, '/v1/connectors/google')).sources.find((s:any)=>s.key===source).paused).toBe(true);
    await api.json('PATCH',base,{paused:false});
  }
});

for(const sameAccount of [false,true]) test(`Gmail native followed-thread capture is readable and retained after disconnect${sameAccount?' same-account':''}`,async({signedIn,request})=>{
  test.setTimeout(90_000);
  const id=`g${Date.now().toString(36)}`;
  const org=sameAccount?await createReadyOrg(request,'gnag'):null;
  const other=org?await addMember(request,org,'gnagb'):null;
  const api=other?.inOrg??signedIn.api;
  const message={id:'m1',threadId:'t1',internalDate:String(Date.now()),labelIds:['INBOX'],payload:{mimeType:'text/plain',headers:[{name:'From',value:'Writer <writer@example.test>'},{name:'To',value:`${id}@example.test`},{name:'Subject',value:'Gmail native acceptance'}],body:{data:Buffer.from('Gmail acceptance evidence confirms the delivery milestone.').toString('base64url')}}};
  expect((await request.post(`${fake}/__google/native/user`,{data:{id,routes:{
    'POST www.googleapis.com/calendar/v3/calendars/primary/events/watch':{dynamicCalendarWatch:true},
    'GET gmail.googleapis.com/gmail/v1/users/me/profile':{body:{emailAddress:`${id}@example.test`,historyId:'1'}},
    'POST gmail.googleapis.com/gmail/v1/users/me/watch':{body:{historyId:'1',expiration:String(Date.now()+86400000)}},
    'GET gmail.googleapis.com/gmail/v1/users/me/messages':{body:{messages:[{id:'m1'},{id:'m2'},{id:'m3'}]}},
    'GET gmail.googleapis.com/gmail/v1/users/me/messages/m1':{body:message},
    'GET gmail.googleapis.com/gmail/v1/users/me/messages/m2':{body:{...message,id:'m2'}},
    'GET gmail.googleapis.com/gmail/v1/users/me/messages/m3':{body:{...message,id:'m3',threadId:'foreign'}},
    'GET gmail.googleapis.com/gmail/v1/users/me/history':{body:{historyId:'2',history:[{messagesAdded:[{message:{id:'m2',threadId:'t1',labelIds:['INBOX']}},{message:{id:'m3',threadId:'foreign',labelIds:['INBOX']}}]}]}},
    'GET gmail.googleapis.com/gmail/v1/users/me/threads/t1':{body:{id:'t1',messages:[message]}},
  }}})).ok()).toBeTruthy();
  const start=await api.json('POST','/v1/connectors/google/connect',{return_to:'wizard'});
  const hop=await request.get(start.authorize_url,{maxRedirects:0});
  const consent=new URL(hop.headers().location);consent.searchParams.set('login_hint',id);
  const allowed=await request.get(consent.href,{maxRedirects:0});
  const callback=await request.get(allowed.headers().location,{maxRedirects:0});expect(callback.status()).toBe(307);
  const base='/v1/connectors/google/sources/gmail';
  await api.json('PATCH',base,{enabled:true});
  await api.json('PUT',`${base}/scope`,{mode:'pick',item_ids:[]});
  expect((await api.json('GET',`${base}/scope-items`)).items).toHaveLength(0);
  if(!sameAccount) {
  const pairing=await(await request.post(`${API_BASE}/v1/addin/pair/start`)).json();
  await api.json('POST','/v1/addin/pair/approve',{code:pairing.code});
  const paired=await(await request.post(`${API_BASE}/v1/addin/pair/poll`,{data:{code:pairing.code,poll_secret:pairing.poll_secret}})).json();
  expect(paired.status).toBe('approved');
  const tokenList=await api.json('GET','/v1/addin/tokens');expect(tokenList.tokens).toHaveLength(1);
  const addonSave=await request.post(`${API_BASE}/v1/connectors/google/ingest`,{headers:{authorization:`Bearer ${paired.token}`},data:{item_ids:['m1'],scope:'thread',follow:true}});
  expect(addonSave.ok()).toBeTruthy();
  expect((await api.call('DELETE',`/v1/addin/tokens/${tokenList.tokens[0].id}`)).status()).toBe(204);
  expect((await request.post(`${API_BASE}/v1/connectors/google/ingest`,{headers:{authorization:`Bearer ${paired.token}`},data:{item_ids:['m1']}})).status()).toBe(401);
  } else await api.json('POST','/v1/connectors/google/ingest',{item_ids:['m1'],scope:'thread',follow:true});
  expect((await api.json('GET',`${base}/scope-items`)).items).toHaveLength(1);
  let atom:any;
  await expect.poll(async()=>{const feed=await api.json('GET','/v1/connectors/google/captures?source=gmail');atom=feed.items[0];return !!atom;}).toBe(true);
  let content='';
  await expect.poll(async()=>{const r=await api.call('GET',`/v1/atoms/${atom.atom_id}`);if(r.ok())content=(await r.json()).content;return content;}).toContain('delivery milestone');
  if(other){
    // Give the same-account observer their own connection: an unconnected
    // member correctly receives not_connected rather than a picker listing.
    const observerId=`${id}b`;
    expect((await request.post(`${fake}/__google/native/user`,{data:{id:observerId,routes:{
      'POST www.googleapis.com/calendar/v3/calendars/primary/events/watch':{dynamicCalendarWatch:true},
      'GET gmail.googleapis.com/gmail/v1/users/me/profile':{body:{emailAddress:`${observerId}@example.test`,historyId:'1'}},
      'POST gmail.googleapis.com/gmail/v1/users/me/watch':{body:{historyId:'1',expiration:String(Date.now()+86400000)}},
    }}})).ok()).toBeTruthy();
    const observerStart=await org!.adminInOrg.json('POST','/v1/connectors/google/connect',{return_to:'wizard'});
    const observerHop=await request.get(observerStart.authorize_url,{maxRedirects:0});
    const observerConsent=new URL(observerHop.headers().location);observerConsent.searchParams.set('login_hint',observerId);
    const observerGrant=await request.get(observerConsent.href,{maxRedirects:0});
    expect((await request.get(observerGrant.headers().location,{maxRedirects:0})).status()).toBe(307);
    await org!.adminInOrg.json('PATCH',base,{enabled:true});
    const ownPicks=await api.json('GET',`${base}/scope-items`);
    expect(ownPicks.items.map((i:any)=>i.id)).toEqual(['t1']);
    await api.json('PUT',`${base}/scope`,{mode:'pick',item_ids:['t1']});
    expect((await org!.adminInOrg.json('GET',`${base}/scope-items`)).items).toEqual([]);
    await org!.adminInOrg.json('PUT',`${base}/scope`,{mode:'pick',item_ids:[]});
    expect((await api.json('GET',`${base}/scope-items`)).items.map((i:any)=>i.id)).toEqual(['t1']);
    await api.json('PUT',`${base}/scope`,{mode:'pick',item_ids:[]});
    expect((await api.json('GET',`${base}/scope-items`)).items).toEqual([]);
    expect((await org!.adminInOrg.json('GET','/v1/connectors/google/captures?source=gmail')).items).toEqual([]);expect((await org!.adminInOrg.call('GET',`/v1/atoms/${atom.atom_id}`)).status()).toBe(404);expect((await api.call('DELETE','/v1/connectors/google')).status()).toBe(204);await expect.poll(async()=>{const r=await api.call('GET',`/v1/atoms/${atom.atom_id}`);return r.ok()?(await r.json()).content:'';}).toBe(content);return;}
  const push=async(historyId='2')=>{
    const now=Math.floor(Date.now()/1000);
    const token=await googlePushToken(request,{iss:'https://accounts.google.com',aud:GOOGLE_PUSH_AUDIENCE,email:FAKE.googlePushServiceAccount,email_verified:true,iat:now,exp:now+600});
    const [[mark]]=selectRows('select clock_timestamp()');
    const body={subscription:'projects/e2e/subscriptions/gmail-push-sub',message:{data:Buffer.from(JSON.stringify({emailAddress:`${id}@example.test`,historyId})).toString('base64'),messageId:`${id}-${Date.now()}`,publishTime:new Date().toISOString()}};
    expect((await request.post(`${API_BASE}/v1/webhooks/google`,{data:body,headers:{authorization:`Bearer ${token}`}})).status()).toBe(200);
    await expect.poll(()=>selectRows(`select status::text from jobs where kind='IngestReceive' and payload->>'provider'='gmail_push' and created_at>'${mark}'`).map(r=>r[0]),{timeout:20_000}).toEqual(['done']);
  };
  await api.json('PATCH',base,{paused:true});
  await push();
  expect((await api.json('GET','/v1/connectors/google/captures?source=gmail')).items).toHaveLength(1);
  await api.json('PATCH',base,{paused:false});
  await push();
  await expect.poll(async()=>(await api.json('GET','/v1/connectors/google/captures?source=gmail')).items.length).toBe(2);
  await api.json('PUT',`${base}/scope`,{mode:'everything'});
  expect((await freshPayload(api, '/v1/connectors/google')).sources.find((s:any)=>s.key==='gmail').scope.mode).toBe('everything');
  const {restartGoogleWorker}=await import('../lib/google-worker');
  await restartGoogleWorker();
  await expect.poll(async()=>(await api.json('GET','/v1/connectors/google/captures?source=gmail')).items.length,{timeout:60_000}).toBe(3);
  expect((await request.post(`${fake}/__google/native/user`,{data:{id,routes:{
    'POST www.googleapis.com/calendar/v3/calendars/primary/events/watch':{dynamicCalendarWatch:true},
    'GET gmail.googleapis.com/gmail/v1/users/me/history':{body:{historyId:'3',history:[{messagesAdded:[{message:{id:'m4',threadId:'t1',labelIds:['INBOX']}}]}]}},
    'GET gmail.googleapis.com/gmail/v1/users/me/messages/m4':{body:{...message,id:'m4'}}
  }}})).ok()).toBeTruthy();
  await api.json('PATCH',base,{paused:true});
  await push('3');
  expect((await api.json('GET','/v1/connectors/google/captures?source=gmail')).items).toHaveLength(3);
  expect((await(await request.get(`${fake}/__google/native/requests?user=${id}`)).json()).filter((r:any)=>r.route.endsWith('/history'))).toHaveLength(0);
  await api.json('PATCH',base,{paused:false});
  await push('3');
  await expect.poll(async()=>(await api.json('GET','/v1/connectors/google/captures?source=gmail')).items.length).toBe(4);

  await api.json('PUT',`${base}/scope`,{mode:'pick',item_ids:[]});
  expect((await api.json('GET',`${base}/scope-items`)).items).toHaveLength(0);
  expect((await api.call('DELETE','/v1/connectors/google')).status()).toBe(204);
  await expect.poll(async()=>{const r=await api.call('GET',`/v1/atoms/${atom.atom_id}`);return r.ok()?(await r.json()).content:`HTTP ${r.status()}`;}).toBe(content);
});

test('Chat selected member space captures paged bounded history through the real startup sweep',async({request})=>{
  test.setTimeout(120_000);
  const {restartGoogleWorker}=await import('../lib/google-worker');
  const org=await createReadyOrg(request,'gnac');
  const other=await addMember(request,org,'gnacb');
  const api=org.adminInOrg; const id=`g${Date.now().toString(36)}`;
  const space={name:'spaces/dm1',spaceType:'DIRECT_MESSAGE',displayName:'Private acceptance chat',historyState:'HISTORY_ON'};
  const message=(n:number)=>({name:`spaces/dm1/messages/m${n}`,text:`Chat acceptance evidence ${n} confirms the delivery milestone.`,createTime:new Date(Date.now()-86400000*n).toISOString(),sender:{name:`users/${id}`,type:'HUMAN'}});
  expect((await request.post(`${fake}/__google/native/user`,{data:{id,routes:{
    'POST www.googleapis.com/calendar/v3/calendars/primary/events/watch':{dynamicCalendarWatch:true},
    'GET chat.googleapis.com/v1/spaces':{body:{spaces:[space]}},
    'GET chat.googleapis.com/v1/spaces/dm1':{body:space},
    'GET chat.googleapis.com/v1/spaces/dm1/messages':{pages:{'':{body:{messages:Array.from({length:2000},(_,n)=>({...message(35),name:`spaces/dm1/messages/old${n}`})),nextPageToken:"second"}},second:{body:{messages:[message(1),message(2)]}}}},
    'POST workspaceevents.googleapis.com/v1/subscriptions':{body:{name:`operations/${id}`,done:true,response:{name:`subscriptions/${id}`,expireTime:new Date(Date.now()+86400000).toISOString()}}},
  }}})).ok()).toBeTruthy();
  const start=await api.json('POST','/v1/connectors/google/connect',{return_to:'wizard'});
  const hop=await request.get(start.authorize_url,{maxRedirects:0});const consent=new URL(hop.headers().location);consent.searchParams.set('login_hint',id);
  const allowed=await request.get(consent.href,{maxRedirects:0});expect((await request.get(allowed.headers().location,{maxRedirects:0})).status()).toBe(307);
  const base='/v1/connectors/google/sources/chat';
  await api.json('PATCH',base,{enabled:true});
  const items=await api.json('GET',`${base}/scope-items`);expect(items.items.map((i:any)=>i.id)).toEqual(['dm1']);
  await api.json('PUT',`${base}/scope`,{mode:'pick',item_ids:['dm1']});
  expect((await api.call('PUT',`${base}/scope`,{mode:'pick',item_ids:['foreign-space']})).status()).toBe(422);
  expect((await api.json('GET',`${base}/scope-items`)).items[0].selected).toBe(true);
  await api.json('POST',`${base}/backfill`,{days:90});
  const graph=await api.personalGraph();
  await api.json('PATCH',base,{paused:true});
  await restartGoogleWorker();
  // Bounded negative observation of the real startup sweep, followed by a
  // positive resume on identical fixtures. No provider data is pre-seeded.
  await new Promise(resolve=>setTimeout(resolve,2000));
  const pausedRequests=await(await request.get(`${fake}/__google/native/requests?user=${id}`)).json();
  expect(pausedRequests.filter((r:any)=>r.route.endsWith('/messages'))).toHaveLength(0);
  expect((await api.json('GET','/v1/connectors/google/captures?source=chat')).items).toHaveLength(0);
  await api.json('PATCH',base,{paused:false});
  await restartGoogleWorker();
  await expect.poll(async()=>{const log=await(await request.get(`${fake}/__google/native/requests?user=${id}`)).json();return log.filter((r:any)=>r.route.endsWith('/messages')).length;}).toBe(1);
  expect((await api.json('GET','/v1/connectors/google/captures?source=chat')).items).toHaveLength(0);
  await restartGoogleWorker();
  let captures:any;
  await expect.poll(async()=>{captures=await api.json('GET','/v1/connectors/google/captures?source=chat');return captures.items.length;},{timeout:60_000}).toBe(2);
  const journal=await(await request.get(`${fake}/__google/native/requests?user=${id}`)).json();
  const pages=journal.filter((r:any)=>r.route.endsWith('/messages'));
  expect(pages.map((r:any)=>r.query.pageToken??'')).toEqual(['','second']);
  for(const p of pages){const dates=[...p.query.filter.matchAll(/"([^"]+)"/g)].map((m:any)=>Date.parse(m[1]));expect(dates).toHaveLength(2);expect(dates[1]-dates[0]).toBeLessThanOrEqual(28*86400000);}
  const atom=captures.items[0].atom_id;
  let content='';await expect.poll(async()=>{const r=await api.call('GET',`/v1/atoms/${atom}`);if(r.ok())content=(await r.json()).content;return content;}).toContain('delivery milestone');
  expect((await other.inOrg.json('GET','/v1/connectors/google/captures?source=chat')).items).toEqual([]);
  expect((await other.inOrg.call('GET',`/v1/atoms/${atom}`)).status()).toBe(404);
  const discovered={...space,name:'spaces/dm2',displayName:'New member conversation'};
  expect((await request.post(`${fake}/__google/native/user`,{data:{id,routes:{
    'POST www.googleapis.com/calendar/v3/calendars/primary/events/watch':{dynamicCalendarWatch:true},
    'GET chat.googleapis.com/v1/spaces':{body:{spaces:[space,discovered]}},
    'GET chat.googleapis.com/v1/spaces/dm1':{body:space},
    'GET chat.googleapis.com/v1/spaces/dm2':{body:discovered}
  }}})).ok()).toBeTruthy();
  await api.json('PUT',`${base}/scope`,{mode:'everything'});
  expect((await freshPayload(api, '/v1/connectors/google')).sources.find((s:any)=>s.key==='chat').scope.mode).toBe('everything');
  expect((await api.json('GET',`${base}/scope-items`)).items.filter((i:any)=>i.selected).map((i:any)=>i.id).sort()).toEqual(['dm1','dm2']);
  expect((await api.call('DELETE','/v1/connectors/google')).status()).toBe(204);
  await expect.poll(async()=>{const r=await api.call('GET',`/v1/atoms/${atom}`);return r.ok()?(await r.json()).content:`HTTP ${r.status()}`;}).toBe(content);
});

test('Meet native transcript capture respects pause and retains the readable copy after disconnect',async({request})=>{
  test.setTimeout(90_000);
  const org=await createReadyOrg(request,'gnam');
  const other=await addMember(request,org,'gnamb');
  const api=org.adminInOrg;const id=`g${Date.now().toString(36)}`;
  const record={name:'conferenceRecords/selected',space:'spaces/selected',startTime:new Date(Date.now()-3600000).toISOString(),endTime:new Date(Date.now()-60000).toISOString()};
  const transcript='conferenceRecords/selected/transcripts/t1';
  expect((await request.post(`${fake}/__google/native/user`,{data:{id,routes:{
    'POST www.googleapis.com/calendar/v3/calendars/primary/events/watch':{dynamicCalendarWatch:true},
    'GET meet.googleapis.com/v2/conferenceRecords/selected':{body:record},
    'GET meet.googleapis.com/v2/conferenceRecords/unselected':{body:{...record,name:'conferenceRecords/unselected',space:'spaces/unselected'}},
    'GET meet.googleapis.com/v2/conferenceRecords/unselected/transcripts':{body:{transcripts:[{name:'conferenceRecords/unselected/transcripts/new',state:'FILE_GENERATED'}]}},
    'GET meet.googleapis.com/v2/conferenceRecords/unselected/transcripts/new/entries':{body:{transcriptEntries:[{participant:'conferenceRecords/unselected/participants/p1',text:'Revoked meeting must not capture this new transcript.'}]}},
    'GET meet.googleapis.com/v2/conferenceRecords/unselected/participants':{body:{participants:[{name:'conferenceRecords/unselected/participants/p1',signedinUser:{user:`users/${id}`,displayName:'Native attendee'}}]}},
    'GET meet.googleapis.com/v2/spaces/unselected':{body:{name:'spaces/unselected',meetingCode:'xyz-abcd-efg'}},
    'GET meet.googleapis.com/v2/conferenceRecords':{body:{conferenceRecords:[record,{...record,name:'conferenceRecords/unselected',space:'spaces/unselected'}]}},
    'GET meet.googleapis.com/v2/spaces/selected':{body:{name:'spaces/selected',meetingCode:'abc-defg-hij'}},
    'GET meet.googleapis.com/v2/conferenceRecords/selected/transcripts':{body:{transcripts:[{name:transcript,state:'FILE_GENERATED'}]}},
    'GET meet.googleapis.com/v2/conferenceRecords/selected/transcripts/t1/entries':{body:{transcriptEntries:[{participant:'conferenceRecords/selected/participants/p1',text:'Meet acceptance evidence confirms the delivery milestone.'}]}},
    'GET meet.googleapis.com/v2/conferenceRecords/selected/participants':{body:{participants:[{name:'conferenceRecords/selected/participants/p1',signedinUser:{user:`users/${id}`,displayName:'Native attendee'}}]}},
    'POST workspaceevents.googleapis.com/v1/subscriptions':{body:{name:`operations/${id}`,done:true,response:{name:`subscriptions/${id}`,expireTime:new Date(Date.now()+86400000).toISOString()}}},
  }}})).ok()).toBeTruthy();
  const start=await api.json('POST','/v1/connectors/google/connect',{return_to:'wizard'});
  const hop=await request.get(start.authorize_url,{maxRedirects:0});const consent=new URL(hop.headers().location);consent.searchParams.set('login_hint',id);
  const grant=await request.get(consent.href,{maxRedirects:0});expect((await request.get(grant.headers().location,{maxRedirects:0})).status()).toBe(307);
  const base='/v1/connectors/google/sources/meet';
  await api.json('PATCH',base,{enabled:true,paused:true});
  await api.json('PUT',`${base}/scope`,{mode:'pick',item_ids:[]});
  const revoked='/v1/connectors/google/meet/meetings/unselected/allow';
  expect((await api.json('POST',revoked)).capture_state).toBe('allowed');
  expect((await api.json('DELETE',revoked)).revoked).toBe(true);
  const {restartGoogleWorker}=await import('../lib/google-worker');
  await api.json('PATCH',base,{paused:false});
  await restartGoogleWorker();
  await expect.poll(async()=>{const log=await(await request.get(`${fake}/__google/native/requests?user=${id}`)).json();return log.some((r:any)=>r.route==='meet.googleapis.com/v2/conferenceRecords');}).toBe(true);
  expect((await api.json('GET','/v1/connectors/google/captures?source=meet')).items).toHaveLength(0);
  await api.json('PATCH',base,{paused:true});
  const allow='/v1/connectors/google/meet/meetings/selected/allow';
  const paused=await api.json('POST',allow);expect(paused.capture_state).toBe('allowed');
  expect((await api.json('GET','/v1/connectors/google/captures?source=meet')).items).toHaveLength(0);
  await api.json('PATCH',base,{paused:false});
  const resumed=await api.json('POST',allow);expect(resumed.ingest).toBe('done');
  const captures=await api.json('GET','/v1/connectors/google/captures?source=meet');expect(captures.items).toHaveLength(1);
  const atom=captures.items[0].atom_id;
  let content='';await expect.poll(async()=>{const r=await api.call('GET',`/v1/atoms/${atom}`);if(r.ok())content=(await r.json()).content;return content;}).toContain('delivery milestone');
  expect((await other.inOrg.json('GET','/v1/connectors/google/captures?source=meet')).items).toEqual([]);
  expect((await other.inOrg.call('GET',`/v1/atoms/${atom}`)).status()).toBe(404);
  expect((await api.call('DELETE','/v1/connectors/google')).status()).toBe(204);
  await expect.poll(async()=>{const r=await api.call('GET',`/v1/atoms/${atom}`);return r.ok()?(await r.json()).content:`HTTP ${r.status()}`;}).toBe(content);
});

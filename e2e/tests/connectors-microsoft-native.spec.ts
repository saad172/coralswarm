import crypto from 'node:crypto';
import { personalUser } from '../lib/isolation';
import { selectRows } from '../lib/stack-db';
import { test, expect } from '../lib/fixtures';
import { API_BASE, PORTS } from '../stack/config.mjs';
import { freshPayload } from '../lib/readModels';

const root = '/v1/connectors/microsoft';
test('Microsoft successful browser consent persists independent sources and captures calendar through the worker', async ({page, signedIn, request}) => {
  test.setTimeout(120_000);
  const {api} = signedIn;
  const cancelled = await api.json('POST', `${root}/connect`, {return_to:'wizard'});
  const cancelledState=new URL(cancelled.authorize_url).searchParams.get('state');
  await page.goto(`${API_BASE}/v1/oauth/microsoft/callback?error=access_denied&state=${encodeURIComponent(cancelledState!)}`);
  expect((await freshPayload(api, root)).status).toBe('not_connected');
  const start = await api.json('POST', `${root}/connect`, {return_to:'wizard'});
  await page.goto(start.authorize_url);
  await page.waitForURL(/provider=microsoft.*result=ok|connectors/);
  const detail = await freshPayload(api, root);
  expect(detail.sources).toHaveLength(5);
  expect(detail.status).not.toBe('not_connected');
  for (const key of ['calendar','mail']) {
    await api.json('PATCH',`${root}/sources/${key}`,{enabled:true});
    await api.json('PUT',`${root}/sources/${key}/scope`,{mode:key==='calendar'?'everything':'pick',item_ids:[]});
  }
  await page.reload();
  const durable = await freshPayload(api, root);
  expect(durable.sources.filter((s:any)=>s.enabled).map((s:any)=>s.key).sort()).toEqual(['calendar','mail']);
  expect(durable.sources.find((s:any)=>s.key==='calendar').scope.mode).toBe('everything');
  const saved = await api.call('POST',`${root}/ingest`,{item_ids:['acceptance-mail'],scope:'message',follow:true});
  expect(saved.ok(),await saved.text()).toBe(true);
  const followed = await api.json('GET',`${root}/sources/mail/scope-items`);
  expect(followed.items.length).toBeGreaterThan(0);
  await page.goto('/connectors/microsoft');
  const mailCard=page.getByRole('heading',{name:'Outlook mail',exact:true}).locator('../..');
  await mailCard.getByRole('button',{name:'Edit',exact:true}).click();
  await expect(mailCard.getByText('Follow threads from the CoralSwarm add-in in Gmail or Outlook.',{exact:true})).toBeVisible();
  await expect(page.getByRole('region',{name:'Outlook followed threads'})).toBeVisible();
  await expect(page.getByRole('button',{name:/Stop following/}).first()).toBeVisible();
  await mailCard.getByRole('button',{name:'Close',exact:true}).click();
  const subscriptions = await (await request.get(`http://127.0.0.1:${PORTS.fakes}/__microsoft/subscriptions`)).json();
  const subscription = subscriptions.slice().reverse().find((s:any)=>s.resource?.includes('messages') && !s.resource?.includes('chats') && !s.resource?.includes('teams'));
  expect(subscription).toBeTruthy();
  const graphForMail=await api.personalGraph();
  const mailIds=async()=>JSON.stringify(await api.json('GET',`/v1/sources/items?graph_id=${graphForMail.id}&source_type=outlook`));
  const push=async(id:string)=>{
    id=`${signedIn.user.id}-${id}`;
    const response=await request.post(`${API_BASE}/v1/webhooks/graph`,{data:{value:[{subscriptionId:subscription.id,clientState:subscription.clientState,changeType:'created',resource:subscription.resource,resourceData:{id}}]}});
    expect(response.status()).toBe(202);
    await expect.poll(()=>selectRows(`SELECT status::text FROM jobs WHERE kind='IngestReceive' AND payload->'notification'->'resourceData'->>'id'='${id}'`).map(r=>r[0]),{timeout:30000}).toEqual(['done']);
  };
  await push('ms-native-live');
  await expect.poll(mailIds).toContain('ms-native-live');
  await api.json('PATCH',`${root}/sources/mail`,{paused:true});
  await push('ms-native-paused');
  expect(await mailIds()).not.toContain('ms-native-paused');
  await api.json('PATCH',`${root}/sources/mail`,{paused:false});
  await push('ms-native-resumed');
  await expect.poll(mailIds).toContain('ms-native-resumed');
  await api.json('PUT',`${root}/sources/mail/scope`,{mode:'everything',item_ids:[]});
  expect((await freshPayload(api, root)).sources.find((s:any)=>s.key==='mail').scope.mode).toBe('everything');
  await expect.poll(mailIds,{timeout:30000}).toContain(`${subscription.user}-message-0`);
  const sync = await api.call('POST',`${root}/sync`,{});
  expect(sync.ok(),await sync.text()).toBe(true);
  const graphRequests=await (await request.get(`http://127.0.0.1:${PORTS.fakes}/__microsoft/requests?user=${subscription.user}`)).json();
  const calendarRequest=graphRequests.find((r:any)=>r.path.endsWith('/calendarView/delta') && r.query.startDateTime);
  expect(calendarRequest).toBeTruthy();
  const lower=new Date(calendarRequest.query.startDateTime).getTime();
  const upper=new Date(calendarRequest.query.endDateTime).getTime();
  expect(Math.abs((Date.now()-lower)/86400000-365)).toBeLessThan(0.01);
  expect(Math.abs((upper-Date.now())/86400000-90)).toBeLessThan(0.01);
  expect((await api.call('PUT',`${root}/sources/calendar/scope`,{mode:'pick',item_ids:[]})).status()).toBe(422);
  const unsupported=await api.call('POST',`${root}/sources/calendar/backfill`,{days:7});
  expect(unsupported.status()).toBe(409);
  const graphs = await api.graphs(); const graph = graphs.find((g:any)=>g.kind==='personal') ?? graphs[0];
  await expect.poll(async()=>{
    const list = await api.json('GET',`/v1/sources/items?graph_id=${graph.id}&source_type=microsoft_calendar`);
    return list.items?.length ?? 0;
  },{timeout:30000}).toBeGreaterThan(0);
  const captures = await api.json('GET',`${root}/captures?source=calendar`);
  expect(captures.items.length).toBeGreaterThan(0);
  let atom = captures.items[0].atom_id;
  let body: any;
  await expect.poll(async()=>{const r=await api.call('GET',`/v1/atoms/${atom}`);if(r.ok())body=await r.json();return r.status();}).toBe(200);
  expect(body.content).toContain('Durable calendar capture acceptance body');
  await api.json('PATCH',`${root}/sources/calendar`,{paused:true});
  expect((await freshPayload(api, root)).sources.find((s:any)=>s.key==='calendar').paused).toBe(true);
  const requestsBefore = await (await request.get(`http://127.0.0.1:${PORTS.fakes}/__requests`)).json();
  await api.call('POST',`${root}/sync`,{});
  const requestsAfter = await (await request.get(`http://127.0.0.1:${PORTS.fakes}/__requests`)).json();
  expect(requestsAfter.filter((r:any)=>r.path.includes('calendarView')).length).toBe(requestsBefore.filter((r:any)=>r.path.includes('calendarView')).length);
  await api.json('PATCH',`${root}/sources/calendar`,{paused:false});
  const resumed = await api.call('POST',`${root}/sync`,{});
  expect(resumed.ok(),await resumed.text()).toBe(true);
  // A resumed provider sync may replace an edited event's prior atom. Retain
  // the current readable version at disconnect, not a superseded version.
  await expect.poll(async()=>{
    const latest=await api.json('GET',`${root}/captures?source=calendar`);
    if(!latest.items.length)return 404;
    const current=latest.items[0].atom_id;
    const r=await api.call('GET',`/v1/atoms/${current}`);
    if(r.ok()){atom=current;body=await r.json();}
    return r.status();
  }).toBe(200);
  expect(body.content).toContain('Durable calendar capture acceptance body');
  const retainedBefore = await api.json('GET',`${root}/captures`);
  expect(retainedBefore.items.length).toBeGreaterThan(0);
  // Do not weaken this assertion: unsupported-history completion is owned by
  // the integration fix and must be delivered before full acceptance can pass.
  const completedStatus = (await freshPayload(api, root)).status;
  expect((await api.call('DELETE',root)).ok()).toBe(true);
  const disconnectedId=`${signedIn.user.id}-ms-native-disconnected`;
  const rejectedPush=await request.post(`${API_BASE}/v1/webhooks/graph`,{data:{value:[{subscriptionId:subscription.id,clientState:subscription.clientState,changeType:'created',resource:subscription.resource,resourceData:{id:disconnectedId}}]}});
  expect(rejectedPush.status()).toBe(202);
  expect(selectRows(`SELECT count(*)::text FROM jobs WHERE kind='IngestReceive' AND payload->'notification'->'resourceData'->>'id'='${disconnectedId}'`)[0][0]).toBe('0');
  expect(await mailIds()).not.toContain(disconnectedId);
  const retained = await api.json('GET',`/v1/sources/items?graph_id=${graph.id}&source_type=microsoft_calendar`);
  expect(retained.items.length).toBeGreaterThan(0);
  expect((await api.json('GET',`/v1/atoms/${atom}`)).content).toBe(body.content);
  const reconnect=await api.json('POST',`${root}/connect`,{return_to:'wizard'});
  await page.goto(reconnect.authorize_url);
  await page.waitForURL(/provider=microsoft.*result=ok|connectors/);
  expect((await freshPayload(api, root)).status).not.toBe('not_connected');
  expect((await api.json('GET',`/v1/atoms/${atom}`)).content).toBe(body.content);
  expect((await api.call('DELETE',root)).ok()).toBe(true);
  expect(completedStatus).toBe('active');
});

test('Microsoft Teams validates independent picks before replacement and retains meeting controls', async ({page,signedIn,request})=>{
  test.setTimeout(180_000);
  const {api}=signedIn;
  const start=await api.json('POST',`${root}/connect`,{return_to:'wizard'});
  await page.goto(start.authorize_url);
  await page.waitForURL(/provider=microsoft.*result=ok|connectors/);
  for(const key of ['teams_chats','teams_channels']) {
    const source=`${root}/sources/${key}`;
    await api.json('PATCH',source,{enabled:true});
    const choices=await api.json('GET',`${source}/scope-items`);
    expect(choices.items.length).toBeGreaterThan(0);
    const id=choices.items[0].id;
    await api.json('PUT',`${source}/scope`,{mode:'pick',item_ids:[id]});
    const bad=await api.call('PUT',`${source}/scope`,{mode:'pick',item_ids:['chat:other-user-secret']});
    expect(bad.status()).toBe(422);
    expect((await api.json('GET',`${source}/scope-items`)).items.find((i:any)=>i.id===id).selected).toBe(true);
    await api.json('PUT',`${source}/scope`,{mode:'everything',item_ids:[]});
    await page.reload();
    expect((await freshPayload(api, root)).sources.find((s:any)=>s.key===key).scope.mode).toBe('everything');
  }
  const initialChat=(await api.json('GET',`${root}/sources/teams_chats/scope-items`)).items[0].id;
  const identity=initialChat.slice('chat:'.length,-'-chat'.length);
  await request.post(`http://127.0.0.1:${PORTS.fakes}/__microsoft/config`,{data:{user:identity,containers:2}});
  for(const key of ['teams_chats','teams_channels']) {
    await expect.poll(async()=>(await api.json('GET',`${root}/sources/${key}/scope-items`)).items.filter((i:any)=>i.selected).length,{timeout:75000,intervals:[1000]}).toBe(2);
  }
  const subscriptions=await (await request.get(`http://127.0.0.1:${PORTS.fakes}/__microsoft/subscriptions`)).json();
  const graph=await api.personalGraph();
  for(const [key,type,marker] of [['teams_chats','teams_chat','chats/'],['teams_channels','teams_channel','/channels/']]) {
    const subscription=subscriptions.find((s:any)=>s.user===identity && s.resource?.includes(marker));
    expect(subscription).toBeTruthy();
    const seen=async()=>JSON.stringify(await api.json('GET',`/v1/sources/items?graph_id=${graph.id}&source_type=${type}`));
    const push=async(phase:string)=>{
      const id=`${signedIn.user.id}-${key}-${phase}`;
      const response=await request.post(`${API_BASE}/v1/webhooks/graph`,{data:{value:[{subscriptionId:subscription.id,clientState:subscription.clientState,changeType:'created',resource:`${subscription.resource}/${id}`,resourceData:{id}}]}});
      expect(response.status()).toBe(202);
      await expect.poll(()=>selectRows(`SELECT status::text FROM jobs WHERE kind='IngestReceive' AND payload->'notification'->'resourceData'->>'id'='${id}'`).map(r=>r[0]),{timeout:30000}).toEqual(['done']);
      return id;
    };
    const live=await push('live');
    await expect.poll(seen).toContain(live);
    await api.json('PATCH',`${root}/sources/${key}`,{paused:true});
    const blocked=await push('paused');
    expect(await seen()).not.toContain(blocked);
    await api.json('PATCH',`${root}/sources/${key}`,{paused:false});
    const resumed=await push('resumed');
    await expect.poll(seen).toContain(resumed);
  }
  const captures=await api.json('GET',`${root}/captures`);
  expect(captures.items.length).toBeGreaterThan(0);
  const bob=await personalUser(request,'ms-other');
  await bob.api.graphs();
  const bstart=await bob.api.json('POST',`${root}/connect`,{return_to:'wizard'});
  await request.get(bstart.authorize_url);
  expect((await bob.api.json('GET',`${root}/captures`)).items).toEqual([]);
  expect((await bob.api.call('GET',`/v1/atoms/${captures.items[0].atom_id}`)).status()).toBe(404);
  const foreign=(await api.json('GET',`${root}/sources/teams_chats/scope-items`)).items[0].id;
  const rejectedForeign=await bob.api.call('PUT',`${root}/sources/teams_chats/scope`,{mode:'pick',item_ids:[foreign]});
  expect(rejectedForeign.status()).toBe(422);
  const meetings=(await freshPayload(api, root)).sources.find((s:any)=>s.key==='teams_meetings');
  expect(meetings.history).toEqual({supported:false,reason:'per_meeting'});
  const rejected=await api.call('PUT',`${root}/sources/teams_meetings/scope`,{mode:'everything',item_ids:[]});
  expect(rejected.ok()).toBe(false);
  await page.goto('/connectors/microsoft-teams/meeting-capture');
  expect(page.url()).toContain('/connectors/microsoft-teams/meeting-capture');
  await expect(page.getByRole('heading',{level:1})).toBeVisible();
});

test('Microsoft history bounds survive the real Teams continuation worker',async({page,signedIn,request})=>{
  test.setTimeout(120_000); // E2E builds shorten cadence; the real worker performs both passes.
  const {api}=signedIn;
  const start=await api.json('POST',`${root}/connect`,{return_to:'wizard'});
  await page.goto(start.authorize_url);
  await page.waitForURL(/provider=microsoft.*result=ok|connectors/);
  const source=`${root}/sources/teams_chats`;
  await api.json('PATCH',source,{enabled:true});
  const choices=await api.json('GET',`${source}/scope-items`);
  const id=choices.items[0].id;
  await api.json('PUT',`${source}/scope`,{mode:'pick',item_ids:[id]});
  const user=id.slice('chat:'.length,-'-chat'.length);
  // Scope selection starts an automatic initial capture. Await its commit
  // before requesting a separate explicit history run (race fixes live upstream).
  await expect.poll(()=>selectRows(`SELECT backfill_complete::text FROM teams_watched_resources WHERE chat_id='${user}-chat'`)[0]?.[0],{timeout:30000}).toBe('true');
  const run=`history-${Date.now()}`;
  await request.post(`http://127.0.0.1:${PORTS.fakes}/__microsoft/config`,{data:{user,historyRun:run}});
  await api.json('POST',`${source}/backfill`,{days:7});
  const bounds=()=>selectRows(`SELECT backfill_since::text,backfill_until::text,coalesce(backfill_cursor,''),backfill_complete::text FROM teams_watched_resources WHERE chat_id='${user}-chat'`)[0];
  const initial=bounds();
  expect(new Date(initial[1]).getTime()-new Date(initial[0]).getTime()).toBe(7*86400000);
  await expect.poll(()=>bounds()[2],{timeout:45000,intervals:[1000]}).toContain(`run=${run}`);
  expect(bounds().slice(0,2)).toEqual(initial.slice(0,2));
  await expect.poll(()=>bounds()[3],{timeout:45000,intervals:[1000]}).toBe('true');
  expect(bounds().slice(0,2)).toEqual(initial.slice(0,2));
  const graph=await api.personalGraph();
  const items=await api.json('GET',`/v1/sources/items?graph_id=${graph.id}&source_type=teams_chat`);
  const ids=JSON.stringify(items);
  expect(ids).toContain(`${run}-inside`);
  for(const suffix of ['below','above','first-above']) expect(ids).not.toContain(`${run}-${suffix}`);
  const logs=await (await request.get(`http://127.0.0.1:${PORTS.fakes}/__microsoft/requests?user=${user}`)).json();
  const continuation=logs.find((r:any)=>r.query.run===run);
  expect(continuation.query.$skiptoken).toBe('1');
  expect(continuation.served.map((i:any)=>i.id)).toEqual([`${run}-inside`,`${run}-below`,`${run}-above`]);
  expect(new Date(continuation.served[1].createdDateTime).getTime()).toBeLessThan(new Date(initial[0]).getTime());
  expect(new Date(continuation.served[2].createdDateTime).getTime()).toBeGreaterThan(new Date(initial[1]).getTime());
});

test('Microsoft selected meeting controls capture, pause, revoke and retain its transcript',async({page,signedIn})=>{
  test.setTimeout(120_000);
  const {api}=signedIn;
  const start=await api.json('POST',`${root}/connect`,{return_to:'wizard'});
  await page.goto(start.authorize_url);
  await page.waitForURL(/provider=microsoft.*result=ok|connectors/);
  const source=`${root}/sources/teams_meetings`;
  await api.json('PATCH',source,{enabled:true});
  await api.json('PUT',`${source}/scope`,{mode:'pick',item_ids:[]});
  const prefix=`${root}/teams/transcripts`;
  const link='https://teams.microsoft.com/meet/123456789012';
  expect((await api.json('GET',`${source}/scope-items`)).items).toEqual([]);
  expect((await api.json('GET',`${root}/captures?source=teams_meetings`)).items).toEqual([]);
  await page.goto('/connectors/microsoft-teams/meeting-capture');
  await page.getByLabel('Meeting join link').fill(link);
  await page.getByRole('button',{name:'Enable capture for this meeting',exact:true}).click();
  await expect(page.getByRole('button',{name:'Sync this meeting',exact:true})).toBeVisible();
  const choices=await api.json('GET',`${source}/scope-items`);
  expect(choices.items).toHaveLength(1);
  await page.getByRole('button',{name:'Sync this meeting',exact:true}).click();
  await expect.poll(async()=>(await api.json('GET',`${root}/captures?source=teams_meetings`)).items.length).toBeGreaterThan(0);
  const captures=await api.json('GET',`${root}/captures?source=teams_meetings`);
  const atom=captures.items[0].atom_id;
  let body:any;
  await expect.poll(async()=>{const r=await api.call('GET',`/v1/atoms/${atom}`);if(r.ok())body=await r.json();return r.status();}).toBe(200);
  expect(body.content).toContain('Selected meeting durable transcript acceptance');
  await api.json('PATCH',source,{paused:true});
  const paused=await api.call('POST',`${prefix}/sync-meeting`,{join_url:link});
  expect(paused.status()).toBe(409);
  expect((await paused.json()).code).toBe('source_paused');
  await api.json('PATCH',source,{paused:false});
  await page.getByRole('button',{name:'Stop capture',exact:true}).click();
  await expect(page.getByRole('button',{name:'Sync this meeting',exact:true})).toHaveCount(0);
  expect((await api.json('GET',`${source}/scope-items`)).items).toEqual([]);
  expect((await api.json('GET',`/v1/atoms/${atom}`)).content).toBe(body.content);
});

test('Microsoft revoked tenant consent reports approval required and reconnect restores the grant',async({page,signedIn,request})=>{
  test.setTimeout(120_000);
  const {api}=signedIn;
  const identity=crypto.randomUUID();
  const configure=(refreshError:boolean,expiresIn:number)=>request.post(`http://127.0.0.1:${PORTS.fakes}/__microsoft/config`,{data:{user:identity,refreshError,expiresIn}});
  await configure(false,0);
  const connect=async()=>{
    await request.post(`http://127.0.0.1:${PORTS.fakes}/__microsoft/config`,{data:{user:identity,nextConsent:true}});
    const start=await api.json('POST',`${root}/connect`,{return_to:'wizard'});
    await page.goto(start.authorize_url);
    await page.waitForURL(/provider=microsoft.*result=ok|connectors/);
  };
  await connect();
  await api.json('PATCH',`${root}/sources/calendar`,{enabled:true});
  await api.json('PUT',`${root}/sources/calendar/scope`,{mode:'everything',item_ids:[]});
  await configure(true,0);
  expect((await api.call('POST',`${root}/sync`,{})).ok()).toBe(false);
  await freshPayload(api, root, (c:any)=>c.status==='needs_attention');
  expect((await freshPayload(api, root)).problem.code).toBe('tenant_consent_required');
  await configure(false,3600);
  await connect();
  expect((await freshPayload(api, root)).status).not.toBe('needs_attention');
  expect((await api.call('POST',`${root}/sync`,{})).ok()).toBe(true);
});

import {test,expect} from '@playwright/test';
const source:any={key:'channels',label:'Channels',enabled:true,paused:false,status:'active',scope_shape:'items',scope:null,history:{supported:true,max_days:90},last_captured_at:null,week:[1,2,3,4,3,2,1],history_days:null,history_requested_at:null,setup_completed_at:null,backfill:null};
const keys=['slack','microsoft','google','github','jira','linear','confluence','hubspot','zendesk','dart'];
const statuses=['setting_up','active','importing','needs_attention','reconnect_needed','not_connected'];
test.beforeEach(async({page})=>{
 await page.route('**/v1/**',async route=>{const path=new URL(route.request().url()).pathname;const c={key:'slack',name:'Slack',beta:false,status:'setting_up',sources_on:1,sources_total:1,last_captured_at:null,importing:null,problem:null,resume_step:'sources',sources:[source],topics:[],account_label:'Fixture workspace',connected_at:null};let body:any=c;
 if(path==='/v1/connectors')body={connectors:keys.map((key,i)=>({...c,key,name:key==='slack'?'Slack':key,beta:i>3,status:statuses[i%6]}))};
 if(path.endsWith('scope-items'))body={items:[{id:'general',label:'General',selected:false,meta:null}],next_cursor:null,total:1};
 if(path.endsWith('/scope'))body={...source,scope:{mode:'pick',count:1,noun:'channels'}};
 if(path.endsWith('/channels'))body=source;
 if(path.endsWith('/backfill'))body={...source,history_days:7,history_requested_at:new Date().toISOString()};
 if(path.includes('meeting-captures'))body={captures:[]};
 if(path.endsWith('/captures'))body={items:[],next_cursor:null};
 await route.fulfill({json:body});});
});
test('catalog, copy timeout, navigation and full wizard keyboard flow',async({page},info)=>{
 await page.context().grantPermissions(['clipboard-read','clipboard-write']);
 await page.goto('/connectors');
 await expect(page.getByText('Beta',{exact:true})).toHaveCount(6);
 expect(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth)).toBe(true);
 await page.screenshot({path:info.outputPath('catalog.png'),fullPage:true});
 const nav=page.getByRole('navigation',{name:'Primary'}); if(info.project.name==='mobile')await expect(nav).toBeVisible();else await expect(nav).toBeHidden();
 const copy=page.getByRole('button',{name:/Copy the MCP/});await copy.click();await expect(copy).toContainText('copied');await expect(copy).not.toContainText('copied',{timeout:3000});
 const opener=page.getByRole('button',{name:'Continue setup Slack'});await opener.click();
 const dialog=page.getByRole('dialog',{name:'Choose scope: Channels'});await expect(dialog).toBeVisible();
 await page.keyboard.press('Escape');await expect(opener).toBeFocused();await page.keyboard.press('Enter');await expect(dialog).toBeVisible();await expect(page.getByText('General',{exact:true})).toBeVisible();
 for(let i=0;i<12;i++){await page.keyboard.press('Tab');expect(await page.getByRole('dialog').evaluate(el=>el.contains(document.activeElement))).toBe(true);}
 await page.screenshot({path:info.outputPath('sources.png')});
 if(info.project.name==='mobile'){const box=await page.getByRole('dialog').boundingBox();expect(Math.abs(box!.y+box!.height-844)).toBeLessThan(2);}
 await page.getByText('General',{exact:true}).click();await page.screenshot({path:info.outputPath('scope.png')});
 await page.getByRole('button',{name:'Continue',exact:true}).click();await page.getByText('Last 7 days',{exact:true}).click();await page.screenshot({path:info.outputPath('history.png')});await page.getByRole('button',{name:'Finish',exact:true}).click();
 await expect(page.getByRole('dialog',{name:'Done'})).toBeVisible();await expect(page.getByRole('link',{name:'View Slack'})).toHaveAttribute('href','/connectors/slack');await page.screenshot({path:info.outputPath('done.png')});
});
test('management rendering',async({page},info)=>{await page.goto('/connectors/slack');await expect(page.getByText('No captures yet.')).toBeVisible();await page.getByRole('button',{name:'Disconnect',exact:true}).click();await expect(page.getByRole('dialog',{name:'Stop capturing from Slack?'})).toBeVisible();await page.screenshot({path:info.outputPath('disconnect.png')});await page.keyboard.press('Escape');await expect(page.getByRole('button',{name:'Disconnect',exact:true})).toBeFocused();});
test('expired hosted sign-in can restart (UI fixture only)',async({page},info)=>{
 await page.addInitScript(()=>{sessionStorage.setItem('connector-pending:linear','fixture');sessionStorage.setItem('connector-url:linear','https://example.com');sessionStorage.setItem('connector-expiry:linear','2020-01-01T00:00:00Z');});
 await page.goto('/connectors');await page.getByRole('button',{name:'Connect linear',exact:true}).click();await page.getByRole('button',{name:'I’ve finished signing in'}).click();await expect(page.getByRole('alert')).toContainText('Sign-in expired');await page.screenshot({path:info.outputPath('expired.png')});await page.getByRole('button',{name:'Start a new sign-in'}).click();await expect(page.getByRole('button',{name:'Sign in to linear'})).toBeVisible();await expect(page.getByRole('alert')).toHaveCount(0);
});
test('reduced motion settles canvas',async({page})=>{await page.emulateMedia({reducedMotion:'reduce'});await page.goto('/connectors');await expect(page.getByRole('button',{name:'Continue setup Slack'})).toBeVisible();const canvas=page.locator('canvas').first();expect(await canvas.evaluate((c:HTMLCanvasElement)=>{const pixels=c.getContext('2d')!.getImageData(0,0,c.width,c.height).data;return pixels.some((value,index)=>index%4===3&&value>0);})).toBe(true);const before=await canvas.evaluate((c:HTMLCanvasElement)=>c.toDataURL());await page.waitForTimeout(150);expect(await canvas.evaluate((c:HTMLCanvasElement)=>c.toDataURL())).toBe(before);});
for(const status of ['paused','stopped','off','importing','needs_attention','active'])test(`source status ${status}`,async({page},info)=>{
 await page.route('**/v1/connectors/slack',route=>route.fulfill({json:{key:'slack',name:'Slack',status:'active',sources:[{...source,status}],topics:[],account_label:'Fixture',problem:null}}));await page.goto('/connectors/slack');await expect(page.getByRole('heading',{name:'Channels'})).toBeVisible();const labels:any={paused:'Paused',stopped:'Stopped',off:'Off',importing:'Importing history',needs_attention:'Needs attention',active:'Active'};await expect(page.getByText(labels[status],{exact:true}).last()).toBeVisible();await page.screenshot({path:info.outputPath(`${status}.png`),fullPage:true});
});

test('retained setup and meeting page components',async({page},info)=>{
 await page.goto('/connectors/github/setup?installation_id=123&setup_action=install');await expect(page.getByRole('link',{name:'Continue to GitHub repositories →'})).toHaveAttribute('href','/connectors/github?installation_id=123');await page.screenshot({path:info.outputPath('github-setup.png')});
 await page.goto('/connectors/microsoft-teams/meeting-capture');await expect(page.getByRole('heading',{name:'Meeting transcript capture'})).toBeVisible();await page.screenshot({path:info.outputPath('meeting-capture.png')});
});

test('selected detail: actionable notice, source segmentation and expanded topics',async({page},info)=>{
 const sources=[source,{...source,key:'chat',label:'Chat'}];
 await page.route('**/v1/connectors/slack',route=>route.fulfill({json:{key:'slack',name:'Slack',status:'needs_attention',sources,topics:[],account_label:'Design acceptance',problem:{code:'missing_scopes',source:null,params:{},since:'2026-10-30'},resume_step:'sources'}}));
 await page.route('**/v1/connectors/slack/captures*',route=>{const selected=new URL(route.request().url()).searchParams.get('source');return route.fulfill({json:{items:[{atom_id:selected||'all',source:selected||'channels',title:selected?'Filtered capture':'Design capture',excerpt:'Expanded capture body',author:'Fixture author',captured_at:'2026-10-30T12:00:00Z',topics:[{id:'topic',label:'Acceptance topic'}],meta:{}}],next_cursor:null}});});
 await page.goto('/connectors/slack');
 await expect(page.locator('header svg').first()).toBeVisible();
 await page.getByText('Design capture',{exact:false}).click();
 await expect(page.getByLabel('Capture topics')).toContainText('Acceptance topic');
 await page.screenshot({path:info.outputPath('selected-detail-expanded.png'),fullPage:true});
 await page.getByRole('tab',{name:'Chat',exact:true}).click();
 await expect(page.getByText('Filtered capture',{exact:false})).toBeVisible();
 await expect(page.getByText('Design capture',{exact:false})).toHaveCount(0);
 await page.getByRole('button',{name:'Reconnect',exact:true}).click();
 await expect(page.getByRole('button',{name:'Sign in to Slack'})).toBeVisible();
 await page.screenshot({path:info.outputPath('selected-reconnect-wizard.png'),fullPage:true});
 expect(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth)).toBe(true);
});

test('fresh meeting source controls remain local',async({page})=>{
 for(const provider of ['google','microsoft']) {
 await page.route(`**/v1/connectors/${provider}`,route=>route.fulfill({json:{key:provider,name:provider,status:'active',sources:[{...source,key:'meet',label:'Meetings'}],topics:[],problem:null}}));
 await page.goto(`/connectors/${provider}`);
 await page.getByRole('button',{name:'Edit',exact:true}).click();
 if(provider==='google') {
 await page.getByLabel('Meet link or meeting code').fill('abc-defg-hij');
 const request=page.waitForRequest(r=>r.url().endsWith('/google/meet/captures')&&r.method()==='POST');
 await page.getByRole('button',{name:'Allow meeting capture'}).click();
 expect((await request).postDataJSON()).toEqual({meeting_code:'abc-defg-hij'});
 await expect(page.getByRole('status')).toContainText('Meeting capture allowed');
 } else await expect(page.getByRole('link',{name:'Choose a meeting to capture'})).toHaveAttribute('href','/connectors/microsoft-teams/meeting-capture');
 }
});
test('recovery checks report unresolved probes honestly',async({page})=>{
 const c={key:'jira',name:'Jira',status:'needs_attention',sources:[source],topics:[],problem:{code:'capture_unavailable',params:{}}};
 await page.route('**/v1/connectors/jira',r=>r.fulfill({json:c}));
 await page.route('**/v1/connectors/jira/recover',async r=>{expect(r.request().postDataJSON()).toEqual({problem:'capture_unavailable'});await r.fulfill({json:c});});
 await page.goto('/connectors/jira');await page.getByRole('button',{name:'Retry capture check'}).click();
 await expect(page.getByRole('status').filter({hasText:'did not confirm a repair'})).toBeVisible();
});

test('fresh mail Edit explains following and isolates Gmail credentials; retry and revoke',async({page},info)=>{
 let tokenReads=0, revokeAttempts=0;
 await page.route('**/v1/oauth/connections',r=>r.fulfill({json:[]}));
 await page.route('**/v1/addin/tokens',async r=>{tokenReads++;await r.fulfill({json:{tokens:tokenReads===1?[]:[{id:'paired-key',name:'My Gmail',created_at:'2026-10-30T00:00:00Z',last_used_at:null}]}});});
 await page.route('**/v1/addin/tokens/paired-key',async r=>{expect(r.request().method()).toBe('DELETE');expect(r.request().headers().authorization).toContain('Bearer ey');revokeAttempts++;await r.fulfill(revokeAttempts===1?{status:500,json:{error:'fixture failure'}}:{json:{status:'revoked'}});});
 for(const provider of ['google','microsoft','google']) {
 await page.route(`**/v1/connectors/${provider}`,r=>r.fulfill({json:{key:provider,name:provider,status:'active',sources:[{...source,key:'mail',label:'Mail'}],topics:[],problem:null}}));
 await page.route(`**/v1/connectors/${provider}/watched*`,async r=>{expect(r.request().headers().authorization).toContain('Bearer ey');expect(new URL(r.request().url()).searchParams.get('limit')).toBe('50');await r.fulfill({json:{threads:[],next_cursor:null}});});
 await page.goto(`/connectors/${provider}`);
 await expect(page.getByText('Gmail Add-on',{exact:true})).toHaveCount(0);
 await page.getByRole('button',{name:'Edit',exact:true}).click();
 await expect(page.getByRole('region',{name:`${provider==='google'?'Gmail':'Outlook'} followed threads`})).toContainText(provider==='google'?'Gmail add-on':'Outlook add-in');
 if(provider==='microsoft') {await expect(page.getByText('Gmail Add-on',{exact:true})).toHaveCount(0);expect(tokenReads).toBe(1);}
 else if(tokenReads===1) {await expect(page.getByText(/No add-on is paired yet/)).toBeVisible();await page.screenshot({path:info.outputPath('fresh-gmail.png'),fullPage:true});}
 else {
 await page.getByRole('button',{name:'Revoke',exact:true}).click();await page.getByRole('button',{name:'Confirm',exact:true}).click();
 await expect(page.getByRole('alert')).toBeVisible();await expect(page.getByRole('button',{name:'Revoke',exact:true})).toBeVisible();
 await page.getByRole('button',{name:'Revoke',exact:true}).click();await page.getByRole('button',{name:'Confirm',exact:true}).click();await expect(page.getByText(/No add-on is paired yet/)).toBeVisible();expect(revokeAttempts).toBe(2);
 }
 }
});

test('multi-source Back retains choices and never enables the off source',async({page})=>{
 const writes:{path:string,body:any}[]=[];
 await page.route('**/v1/connectors/slack/**',async r=>{
 const path=new URL(r.request().url()).pathname;
 if(r.request().method()!=='GET')writes.push({path,body:r.request().postDataJSON()});
 const key=path.split('/')[5];
 await r.fulfill({json:path.endsWith('scope-items')?{items:[{id:key,label:`${key} choice`,selected:false,meta:null}],next_cursor:null,total:1}:path.endsWith('captures')?{items:[],next_cursor:null}:{...source,key,scope:{mode:'pick',count:1,noun:'items'}}});
 });
 await page.route('**/v1/connectors',r=>r.fulfill({json:{connectors:[{key:'slack',name:'Slack',status:'setting_up',resume_step:'scope',sources_total:3,sources_on:2,sources:[source,{...source,key:'chat',label:'Chat'},{...source,key:'mail',label:'Mail',enabled:false}]}]}}));
 await page.goto('/connectors');await page.getByRole('button',{name:'Continue setup Slack'}).click();
 await page.getByText('channels choice',{exact:true}).click();await page.getByRole('button',{name:'Continue',exact:true}).click();
 await page.getByText('chat choice',{exact:true}).click();await page.getByRole('button',{name:'Back',exact:true}).click();
 await expect(page.getByRole('checkbox',{name:'channels choice'})).toBeChecked();await page.getByRole('button',{name:'Continue',exact:true}).click();
 await expect(page.getByRole('checkbox',{name:'chat choice'})).toBeChecked();await page.getByRole('button',{name:'Continue',exact:true}).click();
 await expect(page.getByRole('dialog',{name:'Import history'})).toBeVisible();
 expect(writes).toEqual([{path:'/v1/connectors/slack/sources/channels/scope',body:{mode:'pick',item_ids:['channels']}},{path:'/v1/connectors/slack/sources/channels/scope',body:{mode:'pick',item_ids:['channels']}},{path:'/v1/connectors/slack/sources/chat/scope',body:{mode:'pick',item_ids:['chat']}}]);
});

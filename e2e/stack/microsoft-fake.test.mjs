import test from 'node:test';
import assert from 'node:assert/strict';
import {microsoftRoute} from './microsoft-fake.mjs';
function call(path, {token,body='',method='GET'}={}) {
 let status, headers, text;
 const res={writeHead(s,h){status=s;headers=h;},end(t){text=t;}};
 assert.equal(microsoftRoute({method,headers:{authorization:token?`Bearer ${token}`:''}},res,new URL(path,'http://fake'),body),true);
 return {status,headers,body:text?JSON.parse(text):undefined};
}
function login(user) {
 const auth=call(`/microsoft/common/oauth2/v2.0/authorize?login_hint=${user}&redirect_uri=http://localhost/callback&state=opaque`);
 const target=new URL(auth.headers.location);assert.equal(target.searchParams.get('state'),'opaque');
 const body=new URLSearchParams({code:target.searchParams.get('code'),scope:'openid Mail.Read Calendars.Read'}).toString();
 const token=call('/microsoft/common/oauth2/v2.0/token',{method:'POST',body});
 assert.equal(token.status,200); assert.equal(call('/microsoft/common/oauth2/v2.0/token',{method:'POST',body}).status,400);
 return token.body.access_token;
}
test('Microsoft fake consumes codes once and isolates identities and request logs',()=>{
 const a=login('a'),b=login('b');
 assert.equal(call('/microsoft/graph/v1.0/me',{token:a}).body.id,'a');
 assert.equal(call('/microsoft/graph/v1.0/me',{token:b}).body.id,'b');
 assert.equal(call('/microsoft/graph/v1.0/me',{token:'unknown'}).status,401);
 assert.equal(call('/__microsoft/requests?user=a').body.length,1);
 assert.equal(call('/__microsoft/requests?user=b').body.length,1);
});
test('Microsoft fake pages retain Graph origin and bounded request query evidence',()=>{
 const token=login('pages');
 const first=call('/microsoft/graph/v1.0/chats/pages-chat/messages?$top=50&$filter=createdDateTime%20gt%202026-01-01',{token});
 assert.equal(new URL(first.body['@odata.nextLink']).origin,'https://graph.microsoft.com');
 const next=new URL(first.body['@odata.nextLink']);
 const second=call(`/microsoft/graph${next.pathname}${next.search}`,{token});
 assert.equal(second.body['@odata.nextLink'],undefined);
 assert.notEqual(first.body.value[0].id,second.body.value[0].id);
 assert.equal(call('/__microsoft/requests?user=pages').body[1].query.$filter,'createdDateTime gt 2026-01-01');
});
test('history fixture forces a continuation and serves both sides of the window',()=>{
 const token=login('bounds');
 call('/__microsoft/config',{method:'POST',body:JSON.stringify({user:'bounds',historyRun:'bounded'})});
 const first=call('/microsoft/graph/v1.0/chats/bounds-chat/messages',{token});
 assert.equal(first.body.value.length,500);
 const next=new URL(first.body['@odata.nextLink']);
 const last=call(`/microsoft/graph${next.pathname}${next.search}`,{token});
 assert.deepEqual(last.body.value.map(v=>v.id),['bounded-inside','bounded-below','bounded-above']);
 assert.equal(last.body['@odata.nextLink'],undefined);
 assert.equal(call('/microsoft/graph/v1.0/chats/another-user-chat/messages',{token}).status,403);
});
test('calendar snapshots stay stable across sync and unsupported calls fail closed',()=>{
 const token=login('calendar');
 const a=call('/microsoft/graph/v1.0/me/calendarView/delta',{token});
 const b=call('/microsoft/graph/v1.0/me/calendarView/delta',{token});
 assert.deepEqual(a.body,b.body);
 assert.equal(call('/microsoft/graph/v1.0/not-implemented',{token}).status,404);
});
test('consent selection and expired-grant errors are scoped to one identity',()=>{
 call('/__microsoft/config',{method:'POST',body:JSON.stringify({user:'revoked',nextConsent:true,expiresIn:0})});
 const auth=call('/microsoft/common/oauth2/v2.0/authorize?redirect_uri=http://localhost/callback&state=opaque');
 const code=new URL(auth.headers.location).searchParams.get('code');
 const grant=call('/microsoft/common/oauth2/v2.0/token',{method:'POST',body:new URLSearchParams({code,grant_type:'authorization_code'}).toString()});
 assert.equal(grant.body.expires_in,0);
 call('/__microsoft/config',{method:'POST',body:JSON.stringify({user:'revoked',refreshError:true})});
 const revoked=call('/microsoft/common/oauth2/v2.0/token',{method:'POST',body:new URLSearchParams({refresh_token:grant.body.refresh_token,grant_type:'refresh_token'}).toString()});
 assert.equal(revoked.status,400); assert.deepEqual(revoked.body.error_codes,[65001]);
 const healthy=login('healthy');
 assert.equal(call('/microsoft/graph/v1.0/me',{token:healthy}).status,200);
});

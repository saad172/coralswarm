import {test} from 'node:test';
import assert from 'node:assert/strict';
import {googleNative} from './google-native.mjs';
function fixture() {
 const handle=googleNative();
 return (path,{method='GET',body='',token}={}) => {
  let status, headers, output;
  const res={writeHead(s,h){status=s;headers=h;},end(v){output=v;}};
  const handled=handle({method,headers:{authorization:token ? `Bearer ${token}` : ''}},res,new URL(path,'http://fixture'),body);
  return {handled,status,headers,output,json:()=>JSON.parse(output)};
 };
}
test('consent preserves backend state, exchanges one-use code and journals per user',()=>{
 const call=fixture();
 for(const id of ['alice','bob']) call('/__google/native/user',{method:'POST',body:JSON.stringify({id})});
 const consent=call('/google-native/accounts.google.com/o/oauth2/v2/auth?login_hint=alice&state=backend-state&scope=openid%20email&redirect_uri=http%3A%2F%2Flocalhost%2Fcallback');
 assert.equal(consent.status,302);
 const target=new URL(consent.headers.location);assert.equal(target.searchParams.get('state'),'backend-state');
 const body=new URLSearchParams({grant_type:'authorization_code',code:target.searchParams.get('code')}).toString();
 const token=call('/google-native/oauth2.googleapis.com/token',{method:'POST',body});assert.equal(token.status,200);assert.equal(token.json().access_token,'access-alice');
 assert.equal(call('/google-native/oauth2.googleapis.com/token',{method:'POST',body}).status,401);
 assert.equal(call('/__google/native/requests?user=alice').json().length,1);
 assert.deepEqual(call('/__google/native/requests?user=bob').json(),[]);
});
test('capture pagination is identity scoped and unknown requests fail closed',()=>{
 const call=fixture();
 call('/__google/native/user',{method:'POST',body:JSON.stringify({id:'alice',routes:{'GET chat.googleapis.com/v1/spaces/a/messages':{pages:{'':{body:{messages:[{text:'Alice private evidence'}],nextPageToken:'next'}},next:{body:{messages:[]}}}}}})});
 const path='/google-native/chat.googleapis.com/v1/spaces/a/messages';
 assert.equal(call(path).status,401);
 assert.equal(call(path,{token:'access-bob'}).status,401);
 assert.equal(call(path,{token:'access-alice'}).json().nextPageToken,'next');
 assert.deepEqual(call(path+'?pageToken=next',{token:'access-alice'}).json(),{messages:[]});
 assert.equal(call(path+'?pageToken=invalid',{token:'access-alice'}).status,400);
 assert.equal(call('/google-native/meet.googleapis.com/v2/spaces/a',{token:'access-alice'}).status,404);
 assert.equal(call('/__google/native/requests?user=alice').json().length,4);
});
test('external consent HTML escapes backend parameters and leaves other fakes alone',()=>{
 const call=fixture();assert.equal(call('/slack/api/auth.test').handled,false);
 const page=call('/google-native/accounts.google.com/o/oauth2/v2/auth?state=%22%3E%3Cscript%3E');
 assert.equal(page.status,200);assert.ok(!page.output.includes('<script>'));assert.ok(page.output.includes('&quot;&gt;&lt;script&gt;'));
});

test('redeemed codes cannot be reissued or overwrite pending grants',()=>{
 const call=fixture();
 call('/__google/native/user',{method:'POST',body:JSON.stringify({id:'alice'})});
 const authorize=()=>new URL(call('/google-native/accounts.google.com/o/oauth2/v2/auth?login_hint=alice&redirect_uri=http%3A%2F%2Flocalhost%2Fcallback').headers.location).searchParams.get('code');
 const exchange=code=>call('/google-native/oauth2.googleapis.com/token',{method:'POST',body:new URLSearchParams({grant_type:'authorization_code',code}).toString()}).status;
 const first=authorize(),pending=authorize();
 assert.equal(exchange(first),200);
 const next=authorize();
 assert.equal(new Set([first,pending,next]).size,3);
 assert.equal(exchange(first),401);
 assert.equal(exchange(pending),200);
 assert.equal(exchange(next),200);
});
test('Calendar watch requires exact configured route and preserves failures',()=>{
 const call=fixture();
 const route='www.googleapis.com/calendar/v3/calendars/primary/events/watch';
 const invoke=path=>call('/google-native/'+path,{method:'POST',token:'access-alice',body:JSON.stringify({id:'channel'})});
 const register=routes=>call('/__google/native/user',{method:'POST',body:JSON.stringify({id:'alice',routes})});
 register({});
 assert.equal(invoke(route).status,404);
 assert.equal(invoke('meet.googleapis.com/incorrect/events/watch').status,404);
 register({['POST '+route]:{dynamicCalendarWatch:true}});
 assert.equal(invoke(route).json().id,'channel');
 assert.equal(invoke('www.googleapis.com/calendar/v3/calendars/other/events/watch').status,404);
 register({['POST '+route]:{dynamicCalendarWatch:true,status:403,body:{error:'denied'}}});
 assert.equal(invoke(route).status,403);
 assert.deepEqual(invoke(route).json(),{error:'denied'});
});

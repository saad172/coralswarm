import { test } from 'node:test';
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import { githubFake } from './github-fake.mjs';
const keys = crypto.generateKeyPairSync('rsa', { modulusLength: 2048 });
function harness() {
  const route = githubFake({ publicKey: keys.publicKey, callback: 'http://localhost:24004/v1/oauth/github/callback' });
  return (path, { method = 'GET', token = '', body = '' } = {}) => {
    let status, headers = {}, data;
    assert.equal(route({ method, url: path, headers: { host: 'fake', authorization: `Bearer ${token}` } }, { setHeader(k,v) { headers[k]=v; }, writeHead(s,h) { status=s; Object.assign(headers,h); }, end(b) { data=b; } }, new URL(path, 'http://fake'), body), true);
    return { status, headers, data: data ? JSON.parse(data) : null };
  };
}
function jwt(key = keys.privateKey) {
  const h = Buffer.from(JSON.stringify({ alg:'RS256' })).toString('base64url');
  const p = Buffer.from(JSON.stringify({ iss:'12345', iat:Math.floor(Date.now()/1000)-60, exp:Math.floor(Date.now()/1000)+540 })).toString('base64url');
  return `${h}.${p}.${crypto.sign('RSA-SHA256',Buffer.from(`${h}.${p}`),key).toString('base64url')}`;
}
function setup(call) { for (const [user, installationId] of [['alice',1],['bob',2]]) assert.equal(call('/__github/accounts',{method:'POST',body:JSON.stringify({user,installationId})}).status,201); }
test('signed App grants only configured installations and rejects forged signatures', () => {
 const call=harness(); setup(call);
 assert.equal(call('/github/api/app/installations/1/access_tokens',{method:'POST',token:jwt()}).status,201);
 assert.equal(call('/github/api/app/installations/3',{token:jwt()}).status,404);
 assert.equal(call('/github/api/app/installations/1',{token:jwt(crypto.generateKeyPairSync('rsa',{modulusLength:2048}).privateKey)}).status,401);
});
test('consent preserves state and installation; grant is single use', () => {
 const call=harness(); setup(call);
 const redirect=call('/github/apps/e2e/installations/new?state=signed-state&user=alice');
 const url=new URL(redirect.headers.location);
 assert.equal(url.searchParams.get('state'),'signed-state'); assert.equal(url.searchParams.get('installation_id'),'1');
 const body=new URLSearchParams({code:url.searchParams.get('code'),client_id:'e2e-github',client_secret:'e2e-github-secret'}).toString();
 assert.equal(call('/github/login/oauth/access_token',{method:'POST',body}).status,200);
 assert.equal(call('/github/login/oauth/access_token',{method:'POST',body}).status,401);
});
test('repository credentials and logs are isolated and uninstall revokes reads', () => {
 const call=harness(); setup(call);
 assert.equal(call('/github/api/installation/repositories',{token:'user-alice'}).status,403);
 assert.equal(call('/github/api/repositories/21',{token:'install-1'}).status,404);
 assert.equal(call('/github/api/repositories/11',{token:'install-1'}).status,200);
 assert.deepEqual(call('/__github/requests?user=bob').data,[]);
 assert.equal(call('/github/api/app/installations/1',{method:'DELETE',token:jwt()}).status,204);
 assert.equal(call('/github/api/repositories/11',{token:'install-1'}).status,401);
});

test('continuation and future-grant controls affect only their fixture identity', () => {
 const call=harness(); setup(call);
 assert.equal(call('/__github/config',{method:'POST',body:JSON.stringify({user:'alice',continuation:true,holdPageTwo:true,repositories:3})}).status,200);
 const page1=call('/github/api/repos/alice/repo1/issues?per_page=100',{token:'install-1'});
 assert.match(page1.headers.link,/page=2/); assert.equal(page1.data[0].number,101);
 assert.equal(call('/github/api/repos/alice/repo1/issues?page=2',{token:'install-1'}).status,429);
 assert.equal(call('/github/api/installation/repositories',{token:'install-1'}).data.total_count,3);
 assert.equal(call('/github/api/installation/repositories',{token:'install-2'}).data.total_count,2);
 assert.equal(call('/github/api/repos/bob/repo1/issues',{token:'install-2'}).data.length,4);
 call('/__github/config',{method:'POST',body:JSON.stringify({user:'alice',holdPageTwo:false})});
 assert.deepEqual(call('/github/api/repos/alice/repo1/issues?page=2',{token:'install-1'}).data.map(i=>i.number),[102,103]);
});

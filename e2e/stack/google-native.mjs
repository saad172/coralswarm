import {randomUUID} from 'node:crypto';
// External-provider fixture only. No catalog responses or application DB writes.
// Every grant has its own fixtures and request journal; unknown calls fail closed.
export function googleNative() {
  const users = new Map();
  const codes = new Map();
  const json = (res, status, body) => { res.writeHead(status, { 'content-type': 'application/json' }); res.end(JSON.stringify(body)); };
  return (req, res, url, raw) => {
    const path = url.pathname;
    if (!path.startsWith('/__google/native') && !path.startsWith('/google-native/')) return false;
    if (path === '/__google/native/user' && req.method === 'POST') {
      const config = JSON.parse(raw);
      if (!/^[a-z0-9-]{1,40}$/.test(config.id)) { json(res, 400, {error:'invalid id'}); return true; }
      users.set(config.id, { ...config, requests: [] });
      json(res, 200, { id: config.id }); return true;
    }
    if (path === '/__google/native/requests') {
      json(res, 200, users.get(url.searchParams.get('user'))?.requests ?? []); return true;
    }
    const route = path.slice('/google-native/'.length);
    if (route === 'accounts.google.com/o/oauth2/v2/auth') {
      // Browser selects a fixture identity explicitly; state/redirect come from backend.
      const id = url.searchParams.get('login_hint');
      if (!users.has(id)) {
        res.writeHead(200, {'content-type':'text/html'});
        res.end(`<form method="GET"><label>Google fixture identity <input name="login_hint"></label>${[...url.searchParams].filter(([k])=>k!=='login_hint').map(([k,v])=>`<input type="hidden" name="${escape(k)}" value="${escape(v)}">`).join('')}<button>Allow</button></form>`); return true;
      }
      const code = `code-${id}-${randomUUID()}`;
      codes.set(code, {id, scope:url.searchParams.get('scope')});
      const target = new URL(url.searchParams.get('redirect_uri'));
      target.searchParams.set('code',code); target.searchParams.set('state',url.searchParams.get('state'));
      res.writeHead(302,{location:target.href});res.end();return true;
    }
    if (route === 'oauth2.googleapis.com/token') {
      const form = new URLSearchParams(raw);
      const grant = form.get('grant_type') === 'refresh_token' ? {id:form.get('refresh_token')?.replace(/^refresh-/, '')} : codes.get(form.get('code'));
      if (!grant || !users.has(grant.id)) { json(res,401,{error:'invalid_grant'}); return true; }
      if (form.get('code')) codes.delete(form.get('code'));
      const user=users.get(grant.id);
      user.requests.push({method:req.method,route,grant:form.get('grant_type')});
      const claims=Buffer.from(JSON.stringify({sub:grant.id,email:user.email ?? `${grant.id}@example.test`})).toString('base64url');
      json(res,200,{access_token:`access-${grant.id}`,refresh_token:`refresh-${grant.id}`,expires_in:3600,token_type:'Bearer',scope:grant.scope ?? user.scope,id_token:`fixture.${claims}.fixture`});return true;
    }
    const id = (req.headers.authorization ?? '').replace(/^Bearer access-/, '');
    const user = users.get(id);
    if (!user) {json(res,401,{error:'unauthorized'});return true;}
    const query=Object.fromEntries(url.searchParams);
    user.requests.push({method:req.method,route,query,body:raw ? JSON.parse(raw) : null});
    const key=`${req.method} ${route}`;
    const fixture=user.routes?.[key];
    if (!fixture) {json(res,404,{error:'unconfigured Google route',key});return true;}
    const page=query.pageToken ?? '';
    const response=fixture.pages ? fixture.pages[page] : fixture;
    if (!response) {json(res,400,{error:'invalid page token'});return true;}
    if (response.dynamicCalendarWatch === true && (response.status ?? 200) === 200
        && req.method === 'POST' && /^www\.googleapis\.com\/calendar\/v3\/calendars\/[^/]+\/events\/watch$/.test(route)) {
      const body=JSON.parse(raw);
      json(res,200,{id:body.id,resourceId:`calendar-${id}`,expiration:String(Date.now()+86400000)});return true;
    }
    json(res,response.status ?? 200,response.body ?? response);return true;
  };
}
function escape(value) { return String(value).replaceAll('&','&amp;').replaceAll('"','&quot;').replaceAll('<','&lt;').replaceAll('>','&gt;'); }

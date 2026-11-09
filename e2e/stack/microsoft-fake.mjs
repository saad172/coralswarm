// External Microsoft transport only; captures must enter through the backend.
import crypto from 'node:crypto';
const codes = new Map();
const tokens = new Map();
const logs = new Map();
const subscriptions = new Map();
const scenarios = new Map();
const clocks = new Map();
const consentUsers = [];
const reply = (res, status, body) => { res.writeHead(status, {'content-type':'application/json'}); res.end(JSON.stringify(body)); return true; };
export function microsoftRoute(req, res, url, raw) {
  if (!url.pathname.startsWith('/microsoft/') && !url.pathname.startsWith('/__microsoft/')) return false;
  if (url.pathname === '/__microsoft/config' && req.method === 'POST') { const config=JSON.parse(raw); scenarios.set(config.user,{...scenarios.get(config.user),...config}); if(config.nextConsent) consentUsers.push(config.user); return reply(res,200,{ok:true}); }
  if (url.pathname === '/__microsoft/subscriptions') return reply(res,200,[...subscriptions.values()]);
  if (url.pathname === '/__microsoft/requests') return reply(res,200,logs.get(url.searchParams.get('user')) ?? []);
  if (url.pathname.endsWith('/authorize')) {
    const user = url.searchParams.get('login_hint') || consentUsers.shift() || crypto.randomUUID();
    const code = crypto.randomUUID(); codes.set(code,user);
    const target = new URL(url.searchParams.get('redirect_uri'));
    target.searchParams.set('code',code); target.searchParams.set('state',url.searchParams.get('state'));
    res.writeHead(302,{location:target.href}); res.end(); return true;
  }
  if (url.pathname.endsWith('/token')) {
    const form = new URLSearchParams(raw);
    const user = codes.get(form.get('code')) || tokens.get(form.get('refresh_token'));
    if (!user) return reply(res,400,{error:'invalid_grant'});
    if(form.get('grant_type')==='refresh_token' && scenarios.get(user)?.refreshError) return reply(res,400,{error:'invalid_grant',error_codes:[65001],error_description:'AADSTS65001: consent required'});
    codes.delete(form.get('code'));
    const token = crypto.randomUUID(); tokens.set(token,user);
    if (!clocks.has(user)) clocks.set(user,Date.now());
    const claims = Buffer.from(JSON.stringify({tid:'41111111-1111-4111-8111-111111111111',oid:user,name:'Microsoft E2E',preferred_username:`${user}@example.test`})).toString('base64url');
    return reply(res,200,{access_token:token,refresh_token:token,expires_in:scenarios.get(user)?.expiresIn ?? 3600,scope:form.get('scope'),id_token:`e30.${claims}.fake`});
  }
  const user = tokens.get((req.headers.authorization ?? '').replace(/^Bearer /,''));
  if (!user) return reply(res,401,{error:{code:'InvalidAuthenticationToken'}});
  const entries = logs.get(user) ?? []; logs.set(user,entries);
  entries.push({method:req.method,path:url.pathname,query:Object.fromEntries(url.searchParams),body:raw ? JSON.parse(raw):null});
  const now=clocks.get(user);
  const path = url.pathname.replace('/microsoft/graph/v1.0','');
  if (path === '/me') return reply(res,200,{id:user,mail:`${user}@example.test`});
  if (path.startsWith('/subscriptions')) {
    const id=path.split('/')[2] || crypto.randomUUID();
    if(req.method==='DELETE') subscriptions.delete(id);
    else subscriptions.set(id,{id,user,...(raw?JSON.parse(raw):{})});
    return reply(res,200,{id,expirationDateTime:new Date(now+86400000).toISOString()});
  }
  if (path.startsWith('/me/mailFolders') && !path.endsWith('/messages')) return reply(res,200,{id:`${user}-folder`,totalItemCount:0});
  if ((path.startsWith('/chats/') && !path.startsWith(`/chats/${user}-`)) || (path.startsWith('/teams/') && !path.startsWith(`/teams/${user}-`))) return reply(res,403,{error:{code:'AccessDenied'}});
  if (path.startsWith('/me/messages/')) return reply(res,200,{id:decodeURIComponent(path.split('/').pop()),conversationId:`${user}-thread`,subject:'Outlook acceptance',body:{contentType:'text',content:`Durable Outlook capture for ${user}.`},from:{emailAddress:{address:`${user}@example.test`}},toRecipients:[],receivedDateTime:new Date(Date.now()).toISOString(),sentDateTime:new Date(Date.now()).toISOString()});
  const liveMessage=path.match(/^\/(?:chats|teams)\/.+\/messages\/([^/]+)$/);
  if(liveMessage) return reply(res,200,{id:liveMessage[1],createdDateTime:new Date(Date.now()).toISOString(),lastModifiedDateTime:new Date(Date.now()).toISOString(),body:{contentType:'text',content:`Teams live acceptance ${liveMessage[1]}`},from:{user:{id:user,displayName:'Microsoft E2E'}}});
  if (path.startsWith('/me/onlineMeetings')) {
    const meeting={id:`${user}-meeting`,subject:'Microsoft selected meeting',joinWebUrl:'https://teams.microsoft.com/meet/123456789012',joinMeetingIdSettings:{joinMeetingId:'123456789012'},creationDateTime:new Date(now-3600000).toISOString(),startDateTime:new Date(now-1800000).toISOString(),endDateTime:new Date(now+1800000).toISOString()};
    if (path.endsWith('/content')) {res.writeHead(200,{'content-type':'text/vtt'});res.end('WEBVTT\n\n00:00:01.000 --> 00:00:04.000\n<v Ada>Selected meeting durable transcript acceptance.\n');return true;}
    if (path.endsWith('/transcripts')) return reply(res,200,{value:[{id:'transcript-one',createdDateTime:new Date(now).toISOString()}]});
    return reply(res,200,path==='/me/onlineMeetings'?{value:[meeting]}:meeting);
  }
  if (path === '/me/calendarView') return reply(res,200,{value:[]});
  if (path === '/me/chats') return reply(res,200,{value:Array.from({length:scenarios.get(user)?.containers ?? 1},(_,i)=>({id:`${user}-chat${i || ''}`,topic:`Acceptance chat ${i}`,chatType:'group'}))});
  if (path === '/me/joinedTeams') return reply(res,200,{value:[{id:`${user}-team`,displayName:'Acceptance team'}]});
  if (path.endsWith('/channels')) return reply(res,200,{value:Array.from({length:scenarios.get(user)?.containers ?? 1},(_,i)=>({id:`${user}-channel${i || ''}`,displayName:`Acceptance channel ${i}`,membershipType:'standard'}))});
  if (path.endsWith('/members')) return reply(res,200,{value:[{id:user,userId:user,displayName:'Microsoft E2E',email:`${user}@example.test`,roles:[]}]});
  if (path.endsWith('/messages') || path.endsWith('/replies')) {
    const page = Number(url.searchParams.get('$skiptoken') ?? 0);
    const date = new Date(now-(page ? 45:1)*86400000).toISOString();
    const item={id:`${user}-message-${page}`,conversationId:`${user}-thread`,subject:'Microsoft acceptance capture',body:{contentType:'text',content:`Microsoft durable capture for ${user}, page ${page}.`},from:{user:{id:user,displayName:'Microsoft E2E'},emailAddress:{address:`${user}@example.test`}},receivedDateTime:date,sentDateTime:date,createdDateTime:date,lastModifiedDateTime:date,toRecipients:[]};
    const next = new URL(`https://graph.microsoft.com/v1.0${path}`); next.search=url.search; next.searchParams.set('$skiptoken','1');
    const scenario=scenarios.get(user);
    let values=[item];
    if(scenario?.historyRun && path.startsWith('/chats/')) {
      const event=(suffix,days)=>({...item,id:`${scenario.historyRun}-${suffix}`,createdDateTime:new Date(now+days*86400000).toISOString()});
      values=page ? [event('inside',-1),event('below',-45),event('above',1)] : Array.from({length:500},()=>event('first-above',1));
      next.searchParams.set('run',scenario.historyRun);
    }
    entries[entries.length-1].served=values.map(v=>({id:v.id,createdDateTime:v.createdDateTime}));
    return reply(res,200,{value:values,...(!page ? {'@odata.nextLink':next.href}:{})});
  }
  if (path === '/me/calendarView/delta') return reply(res,200,{value:[{id:`${user}-event`,subject:'Microsoft acceptance calendar',body:{contentType:'text',content:'Durable calendar capture acceptance body.'},start:{dateTime:new Date(now).toISOString().replace(/Z$/,''),timeZone:'UTC'},end:{dateTime:new Date(now+3600000).toISOString().replace(/Z$/,''),timeZone:'UTC'},organizer:{emailAddress:{address:`${user}@example.test`}},isOrganizer:true,attendees:[],lastModifiedDateTime:new Date(now).toISOString()}],'@odata.deltaLink':'https://graph.microsoft.com/v1.0/me/calendarView/delta?$deltatoken=next'});
  return reply(res,404,{error:{code:'UnsupportedFakeEndpoint',message:path}});
}

// External GitHub only. No application catalog or database writes.
import crypto from 'node:crypto';
export function githubFake({ publicKey, callback }) {
  const accounts = new Map(), codes = new Map(), requests = [];
  const reply = (res, status, body) => { res.writeHead(status, { 'content-type': 'application/json' }); res.end(JSON.stringify(body)); };
  const appAuth = token => {
    try {
      const [h, p, s] = token.split('.');
      const header = JSON.parse(Buffer.from(h, 'base64url'));
      const claims = JSON.parse(Buffer.from(p, 'base64url'));
      const now = Date.now() / 1000;
      return header.alg === 'RS256' && claims.iss === '12345' && claims.exp > now && claims.iat <= now && claims.exp - claims.iat <= 600 && crypto.verify('RSA-SHA256', Buffer.from(`${h}.${p}`), publicKey, Buffer.from(s, 'base64url'));
    } catch { return false; }
  };
  return (req, res, url, raw) => {
    const path = url.pathname;
    if (!path.startsWith('/github/') && !path.startsWith('/__github/')) return false;
    const send = (status, body) => { reply(res, status, body); return true; };
    if (path === '/__github/accounts' && req.method === 'POST') {
      const a = JSON.parse(raw);
      if (!a.user || !Number.isSafeInteger(a.installationId) || accounts.has(a.user)) return send(400, { error: 'invalid or duplicate fixture' });
      accounts.set(a.user, { ...a, deleted: false });
      return send(201, a);
    }
    if (path === '/__github/config' && req.method === 'POST') {
      const control = JSON.parse(raw), account = accounts.get(control.user);
      if (!account) return send(404, { error: 'unknown fixture identity' });
      if (control.repositories !== undefined) {
        if (![2, 3].includes(control.repositories)) return send(400, {});
        account.repositories = control.repositories;
      }
      if (typeof control.continuation === 'boolean') account.continuation = control.continuation;
      if (typeof control.holdPageTwo === 'boolean') account.holdPageTwo = control.holdPageTwo;
      return send(200, {});
    }
    if (path === '/__github/requests' && req.method === 'DELETE') {
      for (let i = requests.length - 1; i >= 0; i--) if (requests[i].user === url.searchParams.get('user')) requests.splice(i, 1);
      return send(200, {});
    }
    if (path === '/__github/requests') return send(200, requests.filter(r => r.user === url.searchParams.get('user')));
    const token = (req.headers.authorization ?? '').replace(/^Bearer /i, '');
    let account = [...accounts.values()].find(a => token === `user-${a.user}` || token === `install-${a.installationId}`);
    if (path.includes('/installations/new')) {
      account = accounts.get(url.searchParams.get('user'));
      if (!account) {
        const escape = value => String(value).replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
        res.writeHead(200, { 'content-type': 'text/html' });
        res.end(`<h1>GitHub test consent</h1><form><input type="hidden" name="state" value="${escape(url.searchParams.get('state') ?? '')}">${[...accounts.keys()].map(user => `<button name="user" value="${escape(user)}">${escape(user)}</button>`).join('')}</form>`);
        return true;
      }
      const code = crypto.randomUUID(); codes.set(code, account);
      const dest = new URL(callback); dest.searchParams.set('state', url.searchParams.get('state') ?? ''); dest.searchParams.set('code', code); dest.searchParams.set('installation_id', account.installationId); dest.searchParams.set('setup_action', 'install');
      res.writeHead(302, { location: dest.href }); res.end(); return true;
    }
    if (path === '/github/login/oauth/access_token') {
      const form = new URLSearchParams(raw); account = codes.get(form.get('code'));
      if (!account || form.get('client_id') !== 'e2e-github' || form.get('client_secret') !== 'e2e-github-secret') return send(401, { error: 'bad grant' });
      codes.delete(form.get('code')); requests.push({ user: account.user, method: req.method, path });
      return send(200, { access_token: `user-${account.user}`, token_type: 'bearer', scope: '' });
    }
    const install = path.match(/^\/github\/api\/app\/installations\/(\d+)(\/access_tokens)?$/);
    if (install) {
      account = [...accounts.values()].find(a => a.installationId === Number(install[1]));
      if (!appAuth(token)) return send(401, { error: 'invalid app JWT' });
      if (!account || account.deleted) return send(404, {});
      requests.push({ user: account.user, method: req.method, path });
      if (req.method === 'DELETE') { account.deleted = true; return send(204, null); }
      if (install[2]) return send(201, { token: `install-${account.installationId}`, expires_at: new Date(Date.now() + 3600000).toISOString() });
      return send(200, { id: account.installationId, account: { login: account.user, type: account.type ?? 'User' }, suspended_at: null });
    }
    if (!account || account.deleted) return send(401, { error: 'unknown credential' });
    requests.push({ user: account.user, method: req.method, path, query: Object.fromEntries(url.searchParams) });
    if (path === '/github/api/user' && token.startsWith('user-')) return send(200, { id: account.installationId + 100, login: account.user });
    if (!token.startsWith('install-')) return send(403, { error: 'installation token required' });
    const repos = Array.from({ length: account.repositories ?? 2 }, (_, i) => i + 1).map(n => ({ id: account.installationId * 10 + n, name: `repo${n}`, full_name: `${account.user}/repo${n}`, owner: { login: account.user }, private: true, default_branch: 'main', permissions: { pull: true, push: true, admin: true } }));
    if (path === '/github/api/installation/repositories') return send(200, { total_count: repos.length, repositories: repos });
    const repo = repos.find(r => path === `/github/api/repositories/${r.id}` || path === `/github/api/repos/${r.full_name}`);
    if (repo) return send(200, repo);
    const resource = repos.find(r => path.startsWith(`/github/api/repos/${r.full_name}/`));
    if (resource) {
      const suffix = path.slice(`/github/api/repos/${resource.full_name}`.length);
      if (suffix === '/issues') {
        const page = Number(url.searchParams.get('page') ?? 1);
        if (account.continuation && page === 2 && account.holdPageTwo) {
          res.setHeader('retry-after', '1');
          return send(429, { message: 'fixture continuation held' });
        }
        const daysList = account.continuation ? (page === 1 ? [1] : [10, 45]) : [1, 10, 45, 100];
        if (account.continuation && page === 1) {
          const next = new URL(req.url, `http://${req.headers.host}`);
          next.searchParams.set('page', '2');
          res.setHeader('link', `<${next.href}>; rel="next"`);
        }
        return send(200, daysList.map((days, i) => {
          const number = account.continuation ? (page === 1 ? 101 : 102 + i) : i + 1;
          return {
            id: resource.id * 1000 + number, number, title: account.continuation ? `GitHub continuation issue ${number}` : `GitHub fixture ${days} days`,
            body: `Repository ${resource.full_name} acceptance evidence aged ${days} days.`,
            state: 'open', user: { login: account.user }, labels: [],
            html_url: `https://github.com/${resource.full_name}/issues/${number}`,
            created_at: new Date(Date.now() - days * 86400000).toISOString(),
            updated_at: new Date(Date.now() - days * 86400000).toISOString(),
          };
        }));
      }
      if (/^\/issues\/\d+\/comments$/.test(suffix) || ['/pulls', '/releases', '/commits'].includes(suffix)) return send(200, []);
    }
    return send(404, { error: 'unimplemented GitHub endpoint', path });
  };
}

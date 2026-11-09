import { pageLoadClock } from '../lib/pageLoadClock.mjs';
import manifest from '../../web/lib/pageSections.generated.json' with { type: 'json' };
import { storedStates, assertStatesForPath } from './envelopes.mjs';
import { parsePageReadTiming } from './timings.mjs';

export function tokenScope(jwt) {
  const c = JSON.parse(Buffer.from(jwt.split('.')[1], 'base64url').toString());
  if (!c.iss || !c.sub || !Number.isFinite(c.exp)) throw new Error('missing real credential scope/expiry');
  return { scope: JSON.stringify([c.iss, c.sub, c.org_id ?? c.o?.id ?? null]), expires: c.exp * 1000 };
}
export function cachedScopedToken(mint, expectedScope, wallNow = pageLoadClock.wallNow) {
  let cached;
  return async () => {
    if (!cached || cached.expires - wallNow() <= 15_000) {
      const jwt = await mint(); const claims = tokenScope(jwt);
      if (claims.scope !== expectedScope || claims.expires <= wallNow() + 15_000) throw new Error('setup credential differs from real browser scope or expires too soon');
      cached = { jwt, ...claims };
    }
    return cached.jwt;
  };
}

// Separate real API setup; no browser target mount, interception or measured
// sample mutation. validateBody is the application's exact Stored validator.
export async function prepareHealthyLoad({ request, reads, token, apiBase, refresh, validateBody, now = pageLoadClock.now, wallNow = pageLoadClock.wallNow }) {
  const receipt = { started: now(), finished: null, wallStarted: wallNow(), wallFinished: null, reads: reads.map(r => ({ path: r.path, scope: r.scope })), attempts: [], refreshed: false, errors: [] };
  const deadline = receipt.started + 120_000;
  const responses = [];
  const canonical = (address) => { const u = new URL(address, apiBase); u.searchParams.sort(); if (u.origin !== new URL(apiBase).origin) throw new Error('foreign setup address'); return `${u.pathname}${u.search}`; };
  const sameSet = (states, expected) => states.length === expected.length && states.every(s => expected.some(e => e.identity === s.identity && e.model === s.model && e.version === s.version));
  const recording = new Proxy(request, { get(target, property) {
    if (property !== 'fetch') { const value = Reflect.get(target, property); return typeof value === 'function' ? value.bind(target) : value; }
    return async (url, options) => {
      const address = canonical(url); const method = options.method ?? 'GET';
      const attempt = { method, path: address, started: now(), finished: null, wallStarted: wallNow(), wallFinished: null, status: null, dispatched: false, body: null, states: [], failure: null };
      receipt.attempts.push(attempt);
      try {
        const jwt = options.headers?.authorization?.replace(/^Bearer /i, '') ?? '';
        const scope = jwt ? tokenScope(jwt).scope : 'public'; attempt.scope = scope;
        if (reads.some(read => read.scope !== scope)) throw new Error('setup request scope differs from actual discovery');
        const remaining = deadline - now(); if (remaining <= 0) throw new Error('healthy setup exceeded120s');
        attempt.dispatched = true;
        const response = await target.fetch(url, { ...options, timeout: Math.min(10_000, remaining), maxRetries: 0, maxRedirects: 0 });
        responses.push(response);
        attempt.status = response.status(); attempt.server_timing = response.headers()['server-timing'] ?? null;
        attempt.body_text = await response.text();
        attempt.body = JSON.parse(attempt.body_text);
        if (attempt.status !== (method === 'GET' ? 200 : 202) || response.headers()['x-coralswarm-admission']) throw new Error(`healthy setup ${method} ${address}: HTTP${attempt.status} or admission failure`);
        if (method === 'GET') {
          parsePageReadTiming(attempt.server_timing, true);
          const read = reads.find(r => canonical(r.path) === address); if (!read) throw new Error('setup read was not actually discovered');
          if (typeof validateBody !== 'function' || !validateBody(address, attempt.body)) throw new Error('invalid application Stored certificate/payload');
          attempt.states = storedStates(attempt.body, manifest, address); assertStatesForPath(attempt.states, manifest, address);
          if (!sameSet(attempt.states, read.states)) throw new Error('setup changed authorized constituent/model/version set');
        } else if (typeof attempt.body?.refresh_eta !== 'string' || !Number.isFinite(Date.parse(attempt.body.refresh_eta))) throw new Error('invalid registered refresh acknowledgement');
        return response;
      } catch (error) { attempt.failure = error.message; throw error; }
      finally { attempt.finished = now(); attempt.wallFinished = wallNow(); }
    };
  }});
  try {
    if (!reads.length) return receipt;
    const current = [];
    for (const read of reads) {
      const jwt = await token();
      await recording.fetch(`${apiBase}${read.path}`, { method: 'GET', headers: jwt ? { authorization: `Bearer ${jwt}` } : {} });
      const states = receipt.attempts.at(-1).states;
      current.push({ ...read, states });
    }
    receipt.headroom = current.flatMap(read => read.states.map(state => {
      const maxAge = manifest[state.model]?.max_delay_seconds * 1000;
      if (!Number.isFinite(maxAge) || maxAge <= 0) throw new Error('missing compiled freshness bound');
      const headroom = maxAge / 2;
      return { path: read.path, model: state.model, identity: state.identity, max_age_ms: maxAge, reserved_headroom_ms: headroom,
        age_ms: state.computed_at === null ? null : wallNow() - Date.parse(state.computed_at),
        refresh_needed: state.computed_at === null || state.stale || state.refresh_eta !== null || wallNow() - Date.parse(state.computed_at) >= maxAge - headroom };
    }));
    if (receipt.headroom.some(row => row.refresh_needed)) {
      receipt.refreshed = true; receipt.refresh = await refresh(recording, current, token);
      if (receipt.refresh.errors.length) throw new Error(`healthy registered refresh failed: ${receipt.refresh.errors.join('; ')}`);
    }
    if (now() > deadline) throw new Error('healthy setup exceeded120s');
    return receipt;
  } catch (error) { receipt.errors.push(error.message); error.receipt = receipt; throw error; }
  finally {
    // Refresh consumers may read these responses again. Release their cached
    // buffers only after every consumer has finished, including failure paths.
    const cleanup = await Promise.allSettled(responses.map(response => response.dispose()));
    receipt.finished = now(); receipt.wallFinished = wallNow();
    const failures = cleanup.filter(result => result.status === 'rejected');
    if (failures.length) {
      const error = new Error(`healthy setup response disposal failed (${failures.length})`);
      receipt.errors.push(error.message); error.receipt = receipt; throw error;
    }
  }
}

import registry from '../../web/lib/pageSections.generated.json' with { type: 'json' };
import { isRegisteredModelPath, assertStatesForPath } from './envelopes.mjs';

const secretKey = /token|jwt|secret|ticket|authorization|email|__clerk|code|state|signature|key/i;
const jwtShape = /[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}/g;
export function safeDocumentUrl(value, base) {
  try {
    if (typeof value !== 'string' || !value) return null;
    const url = new URL(value, base); url.username = ''; url.password = ''; url.hash = '';
    url.pathname = url.pathname.replace(jwtShape, '[redacted]');
    const sanitized = new URLSearchParams();
    for (const [key, val] of url.searchParams) {
      sanitized.append(key, secretKey.test(key) || new RegExp(jwtShape.source).test(val) ? '[redacted]' : val);
    }
    url.search = sanitized.toString();
    return url.toString();
  } catch { return null; }
}
function canonical(value, base) { if (typeof value !== 'string' || !value) throw new Error('missing URL'); const u = new URL(value, base); if (!['http:', 'https:'].includes(u.protocol) || u.username || u.password) throw new Error('invalid document URL'); u.hash = ''; u.searchParams.sort(); return u; }
function sameAddress(a, b, base) { return canonical(a, base).href === canonical(b, base).href; }
const finish = (kind, receipt, reasons = []) => ({ ...receipt, kind, ok: reasons.length === 0,
  fatal: ['auth-fatal', 'destination-fatal', 'unknown-fatal'].includes(kind), reasons });

/** No source of identity other than this document's read-only browser snapshot
 * and its actual API requests. Backend setup credentials are never accepted. */
export function classifyDocument({ requested, finalUrl, expected, browser, records = [], ledger = [], errors = [], frames = [], resolveAlias }) {
  const receipt = { final_url: safeDocumentUrl(finalUrl, expected?.webBase), frames,
    browser: browser ? Object.fromEntries(['read_error', 'loaded', 'signed_in', 'session_active', 'session_matches', 'user_matches', 'organization_matches', 'session_organization_matches'].map(key => [key, browser[key] === true])) : null, token_probe: 'not requested; no token renewal or injection', canonical_destination: null,
    api_scope_matches_expected: null, compared_api_records: 0 };
  if (!expected || !['public', 'authenticated', 'admin'].includes(expected.access) || !expected.webBase || typeof resolveAlias !== 'function')
    return finish('unknown-fatal', receipt, ['missing-document-expectation-or-canonical-resolver']);
  const authenticated = expected.access !== 'public';
  if (expected.audience !== undefined && expected.audience !== (authenticated ? 'authenticated' : 'anonymous'))
    return finish('unknown-fatal', receipt, ['inconsistent-document-auth-audience']);
  if (authenticated) {
    let scope; try { scope = JSON.parse(expected.scope); } catch { /* fail closed below */ }
    if (typeof expected.sessionId !== 'string' || !expected.sessionId || typeof expected.userId !== 'string' || !expected.userId || !Object.hasOwn(expected, 'orgId') ||
      !(expected.orgId === null || typeof expected.orgId === 'string' && expected.orgId.length > 0) ||
      !Array.isArray(scope) || scope.length !== 3 || typeof scope[0] !== 'string' || !scope[0] || scope[1] !== expected.userId || scope[2] !== expected.orgId)
      return finish('unknown-fatal', receipt, ['missing-or-inconsistent-session-user-org-scope-expectation']);
    if (!browser || browser.read_error || browser.loaded !== true || browser.signed_in !== true || browser.session_active !== true ||
      browser.session_matches !== true || browser.user_matches !== true || browser.organization_matches !== true || browser.session_organization_matches !== true)
      return finish('auth-fatal', receipt, ['actual-browser-principal-does-not-match-expected-session-user-org']);
  }
  try {
    const target = canonical(requested, expected.webBase); const actual = canonical(finalUrl, expected.webBase);
    if (actual.origin !== target.origin || actual.protocol !== target.protocol) return finish('destination-fatal', receipt, ['unexpected-top-level-origin']);
    const seen = new Set(); let destination = target;
    while (true) {
      if (seen.has(destination.href)) throw new Error('canonical redirect cycle'); seen.add(destination.href);
      const next = resolveAlias(destination.pathname, destination.search, { audience: expected.audience }); if (!next) break;
      destination = canonical(next, expected.webBase);
      if (destination.origin !== target.origin) throw new Error('foreign canonical redirect');
    }
    receipt.canonical_destination = safeDocumentUrl(destination.href, expected.webBase);
    let matches = sameAddress(actual.href, destination.href, expected.webBase);
    if (!matches && expected.action) {
      // These waves come only from ConnectorCallbackPhase.destination's real
      // same-scope acknowledged action, not an independently invented allowlist.
      matches = ledger.some(row => row.initialWave?.startsWith('callback-destination:') &&
        row.identity?.slice(0, row.identity.lastIndexOf('|')) === expected.scope &&
        expected.action.destinations.includes(actual.pathname) &&
        sameAddress(row.initialWave.slice('callback-destination:'.length), actual.href, expected.webBase));
    }
    if (!matches) return finish('destination-fatal', receipt, ['unexpected-canonical-top-level-destination-or-query']);
  } catch { return finish('unknown-fatal', receipt, ['unresolvable-top-level-destination']); }
  if (authenticated) {
    const compared = records.filter(row => !ledger.some(entry => ['telemetry', 'preflight'].includes(entry.excluded) &&
      entry.method === row.method && sameAddress(entry.address ?? entry.path, row.path, expected.webBase)));
    receipt.compared_api_records = compared.length;
    receipt.api_scope_matches_expected = compared.every(row => row.scope === expected.scope);
    if (!receipt.api_scope_matches_expected) return finish('auth-fatal', receipt, ['observed-api-credential-scope-does-not-match-browser-expectation']);
  }
  const contentErrors = [...errors];
  for (const row of records) { try { assertStatesForPath(row.models ?? [], registry, row.path); } catch { contentErrors.push('invalid-scoped-model-proof'); } }
  if (authenticated && !records.some(row => row.method === 'GET' && row.priority === 'critical' && Number.isFinite(row.total_ms))) contentErrors.push('missing-actual-critical-page-read-timing');
  if (records.some(row => row.method === 'GET' && isRegisteredModelPath(row.path, registry) && !row.models?.length)) contentErrors.push('registered-read-missing-model-proof');
  for (const read of expected.expectedReads ?? []) for (const state of read.states) {
    if (!records.some(row => row.path === read.path && row.scope === read.scope && Number.isFinite(row.total_ms) && row.models?.some(model =>
      model.identity === state.identity && model.model === state.model && model.version === state.version))) contentErrors.push('missing-discovered-scoped-model-proof');
  }
  if (contentErrors.length) return finish('content-evidence-failure', receipt, contentErrors.map((_error, i) => `recorded-content-check-failed:${i + 1}`));
  return finish(authenticated ? 'authenticated-document' : 'public-document', receipt);
}

/** Install before navigation, inspect only AFTER the existing load recorder.
 * Subframes are diagnostics; only page.url() selects the actual destination. */
export function createDocumentEvidence(page, { requested, expected, resolveAlias, now }) {
  const frames = [];
  const onFrame = frame => { try { frames.push({ at: now(), top_level: frame === page.mainFrame(), url: safeDocumentUrl(frame.url(), expected?.webBase) }); } catch { frames.push({ at: now(), unreadable: true }); } };
  page.on('framenavigated', onFrame);
  return { async complete(observation) {
    let browser = null; let finalUrl;
    try { finalUrl = page.url(); } catch { finalUrl = null; }
    try {
      if (expected?.access !== 'public') browser = await page.evaluate(({ sessionId, userId, orgId }) => {
        const clerk = window.Clerk; const session = clerk?.session;
        return { loaded: clerk?.loaded === true, signed_in: !!clerk?.user && !!session,
          session_active: session?.status === 'active', session_matches: !!session && session.id === sessionId,
          user_matches: !!clerk?.user && clerk.user.id === userId,
          organization_matches: (clerk?.organization?.id ?? null) === orgId,
          session_organization_matches: (session?.lastActiveOrganizationId ?? null) === orgId };
      }, { sessionId: expected?.sessionId, userId: expected?.userId, orgId: expected?.orgId });
    } catch { browser = { read_error: true }; }
    finally { page.off('framenavigated', onFrame); }
    return classifyDocument({ ...observation, requested, finalUrl, expected, browser, frames, resolveAlias });
  }};
}

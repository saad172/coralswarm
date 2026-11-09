/**
 * Sign-in: Clerk OAuth 2.0 authorization code + PKCE, as a PUBLIC client,
 * through Apps Script's built-in third-party-auth pattern (the apps-script-
 * oauth2 library + `CardService.newAuthorizationAction` / an
 * AuthorizationException + the `/usercallback` redirect). This replaces the
 * old device-pairing handshake (the retired Pairing.gs and its `csat_` token).
 *
 * The flow, as live-verified in Gmail web:
 *   1. The user taps "Sign in" (the sign-in card, Card.gs `buildSignInCard_`)
 *      and Clerk opens in a popup: sign in with Google, then the consent
 *      screen with the workspace (organization) picker.
 *   2. Clerk redirects to
 *      https://script.google.com/macros/d/{SCRIPT_ID}/usercallback; Apps
 *      Script routes that, by state token, to `authCallback` below, which
 *      exchanges the code (with the PKCE verifier) for tokens.
 *   3. The popup closes itself and Gmail reloads the add-on.
 *
 * The backend accepts the resulting Clerk access token (a JWT carrying the
 * add-on's client_id and the picked workspace's org_id) as a Bearer on exactly
 * the routes the legacy `csat_` token reached. `currentCredential_()` is the
 * ONE place that decides which credential every backend call sends.
 */

/**
 * Builds the OAuth2 service. This runs on every execution: the library keeps
 * no state in memory between executions, and tokens live in User Properties.
 *
 * PKCE: `generateCodeVerifier()` sets a random 32-byte verifier with method
 * S256. `getAuthorizationUrl()` sends `code_challenge` +
 * `code_challenge_method=S256` and carries the verifier inside the Apps Script
 * state token; `handleCallback()` reads it back and sends `code_verifier`. A
 * fresh verifier per execution is therefore correct.
 */
function getClerkService_() {
  var fapi = clerkFapiBaseUrl_();
  var service = OAuth2.createService(OAUTH_SERVICE_NAME)
    .setAuthorizationBaseUrl(fapi + '/oauth/authorize')
    .setTokenUrl(fapi + '/oauth/token')
    .setClientId(clerkClientId_())
    // Placeholder only, so v43's refresh() validation passes. It is stripped
    // before any request is sent; see CLIENT_SECRET_PLACEHOLDER in Config.gs.
    .setClientSecret(CLIENT_SECRET_PLACEHOLDER)
    .setTokenPayloadHandler(stripClientSecret_)
    .setScope(CLERK_SCOPES)
    // Always show Clerk's consent screen (and so its workspace picker). This
    // is also what makes "Change workspace" work.
    .setParam('prompt', 'consent')
    .setCallbackFunction('authCallback')
    .setPropertyStore(PropertiesService.getUserProperties())
    .setCache(CacheService.getUserCache())
    // Refresh tokens ROTATE, so two executions refreshing at once (homepage
    // and contextual trigger firing together) would replay the same refresh
    // token and the loser would get invalid_grant. The per-user lock
    // serializes refresh; hasAccess() re-reads the token once it holds it.
    .setLock(LockService.getUserLock());

  if (typeof service.generateCodeVerifier !== 'function') {
    throw new Error('OAuth2 library version has no generateCodeVerifier() (PKCE, added in 1.42.0). ' +
      'Bump the OAuth2 library version in appsscript.json.');
  }
  service.generateCodeVerifier();
  return service;
}

/**
 * Token payload handler: runs on BOTH the authorization_code exchange and the
 * refresh_token grant, just before the request is sent. The library always
 * puts `client_secret` in the payload; deleting it makes this a public-client
 * request (client_id + code_verifier, or client_id + refresh_token).
 */
function stripClientSecret_(payload) {
  delete payload.client_secret;
  return payload;
}

/**
 * The credential every backend call sends as its Bearer, or `null` when the
 * user is signed out. Returns `{ kind, token }`:
 *
 * * `'clerk'` — a valid Clerk access token (refreshed transparently by
 *   `hasAccess()` when expired, saving the rotated refresh token). PREFERRED:
 *   if a Clerk token is usable it wins even when a legacy token is still
 *   stored.
 * * `'legacy'` — the `csat_` token from the retired pairing flow. This is the
 *   migration path: an already-paired mailbox keeps working until its owner
 *   signs in with Clerk, at which point `authCallback` removes it.
 *
 * hasAccess() swallows refresh failures (e.g. `invalid_grant`) into
 * `lastError`; we keep that for the sign-in card and fall through to the
 * legacy token, or to signed-out.
 */
function currentCredential_() {
  if (isClientIdConfigured_()) {
    var service = getClerkService_();
    if (service.hasAccess()) {
      var access = service.getAccessToken();
      if (access) return { kind: 'clerk', token: access };
    }
    var err = service.getLastError();
    if (err) {
      console.warn('Clerk hasAccess() false; last error: ' + err);
      PropertiesService.getUserProperties().setProperty(PROP_LAST_AUTH_ERROR,
        new Date().toISOString() + ' ' + String(err));
    }
  }
  var legacy = PropertiesService.getUserProperties().getProperty(PROP_ADDIN_TOKEN);
  if (legacy) return { kind: 'legacy', token: legacy };
  return null;
}

/** Shorthand: just the bearer string (or `null`). */
function bearerToken_() {
  var cred = currentCredential_();
  return cred ? cred.token : null;
}

/**
 * Entry-point guard. Signed in → returns the credential. Signed out → throws
 * the AuthorizationException (Gmail renders our custom sign-in card via
 * `createSignInUi`, the path live-verified in Gmail web). Client id not yet
 * configured → returns `null` and the caller shows the setup card instead.
 */
function requireCredential_() {
  var cred = currentCredential_();
  if (cred) return cred;
  if (!isClientIdConfigured_()) return null;
  throwSignIn_();
}

function throwSignIn_() {
  CardService.newAuthorizationException()
    .setAuthorizationUrl(getClerkService_().getAuthorizationUrl())
    .setResourceDisplayName(RESOURCE_DISPLAY_NAME)
    .setCustomUiCallback('createSignInUi')
    .throwException();
}

/** The custom UI callback named in `setCustomUiCallback`. Must be public (no
 *  trailing underscore) and return Card[]. */
function createSignInUi() {
  return [buildSignInCard_()];
}

/**
 * Callback named in `setCallbackFunction`. Apps Script invokes it when Clerk
 * redirects to .../usercallback?code=...&state=... It must return HtmlOutput;
 * closing the popup is what tells Gmail to reload the card.
 */
function authCallback(request) {
  var p = (request && request.parameter) || {};
  var service = getClerkService_();
  var authorized;
  try {
    authorized = service.handleCallback(request);
  } catch (err) {
    console.error('handleCallback failed: ' + err);
    return callbackPage_('Sign-in failed',
      String(err) + (p.error_description ? ' (' + p.error_description + ')' : ''), false);
  }

  if (authorized) {
    PropertiesService.getUserProperties().deleteProperty(PROP_LAST_AUTH_ERROR);
    // Migration: the Clerk token now carries the mailbox, so the legacy
    // pairing token is retired — revoked server-side (best effort) and
    // removed locally.
    var outcome = retireLegacyToken_(false);
    if (outcome === 'failed') {
      // The Clerk token already works (it is preferred over the legacy one),
      // but the old csat_ credential is still live on the server. It is kept
      // locally so Sign out can retry the revoke.
      return callbackPage_('Signed in',
        'You are signed in to CoralSwarm. Your old pairing credential could not be revoked ' +
        'right now: use Sign out in Gmail to retry, or revoke it in CoralSwarm under ' +
        'Connectors → Gmail Add-on. You can close this tab.', true);
    }
    return callbackPage_('Signed in', 'You are signed in to CoralSwarm. You can close this tab.', true);
  }
  return callbackPage_('Denied', 'Access was denied. You can close this tab.', false);
}

function callbackPage_(title, message, autoClose) {
  var html = '<!doctype html><html><body style="font-family:sans-serif;padding:24px">' +
    '<h2>' + escapeHtml_(title) + '</h2><p>' + escapeHtml_(message) + '</p>' +
    (autoClose ? '<script>setTimeout(function(){ top.window.close(); }, 1000);</script>' : '') +
    '</body></html>';
  return HtmlService.createHtmlOutput(html).setTitle('CoralSwarm');
}

/**
 * Removes the legacy `csat_` credential. When it can name itself
 * (`PROP_ADDIN_TOKEN_ID`) it is revoked server-side first, using itself as the
 * bearer (`DELETE /v1/addin/tokens/{id}` accepts a `csat_` PAT). Returns the
 * revoke outcome: `'revoked'`, `'failed'`, `'not_revoked'` (no id stored) or
 * `'none'` (there was no legacy token).
 *
 * The revoke needs the `csat_` token ITSELF as the bearer (the backend does not
 * let a Clerk token call `DELETE /v1/addin/tokens/{id}`), so when it FAILS both
 * the token and its id are kept, so a later call (Sign out) can retry. Pass
 * `force` to delete locally regardless (the backend already rejected the token
 * with a 401, so there is nothing left to retry).
 */
function retireLegacyToken_(force) {
  var props = PropertiesService.getUserProperties();
  var token = props.getProperty(PROP_ADDIN_TOKEN);
  var tokenId = props.getProperty(PROP_ADDIN_TOKEN_ID);
  var outcome = 'none';
  if (token) {
    outcome = 'not_revoked';
    if (tokenId) {
      var resp = callBackend_(
        'delete',
        backendBaseUrl_() + '/v1/addin/tokens/' + encodeURIComponent(tokenId),
        null,
        token,
      );
      outcome = resp.status === 204 || resp.status === 404 ? 'revoked' : 'failed';
    }
  }
  if (outcome === 'failed' && !force) return outcome;
  props.deleteProperty(PROP_ADDIN_TOKEN);
  props.deleteProperty(PROP_ADDIN_TOKEN_ID);
  props.deleteProperty(PROP_ADDIN_ACCOUNT_EMAIL);
  return outcome;
}

/**
 * The backend refused the credential (401): drop exactly that credential and
 * show the sign-in card. Used by the action handlers (Ingest.gs, Settings.gs).
 */
function rejectedCredentialResponse_(cred) {
  if (cred && cred.kind === 'clerk') {
    getClerkService_().reset();
  } else {
    retireLegacyToken_(true);
  }
  return signedOutResponse_('CoralSwarm needs to be reconnected.');
}

/** Action response: sign-in card + a toast. */
function signedOutResponse_(text) {
  var builder = CardService.newActionResponseBuilder()
    .setNavigation(CardService.newNavigation().updateCard(buildSignInCard_()));
  if (text) builder.setNotification(CardService.newNotification().setText(text));
  return builder.build();
}

// ── Card actions ────────────────────────────────────────────────────────

/**
 * "Sign out": local only for Clerk (the tokens are removed; the grant is NOT
 * revoked at Clerk, so the old refresh token stays valid server-side until it
 * expires). A legacy `csat_` token IS revoked server-side first, as the old
 * Disconnect did.
 */
function signOut(e) {
  var outcome = retireLegacyToken_(false);
  getClerkService_().reset();
  PropertiesService.getUserProperties().deleteProperty(PROP_LAST_AUTH_ERROR);
  if (outcome === 'failed') {
    // Legacy credential kept so this can be retried; stay on the current card.
    return CardService.newActionResponseBuilder()
      .setNotification(CardService.newNotification().setText(
        'Could not revoke the old CoralSwarm credential. Try Sign out again, or revoke it in ' +
        'CoralSwarm under Connectors → Gmail Add-on.'))
      .build();
  }
  var message = 'Signed out.';
  if (outcome === 'revoked') {
    message = 'Disconnected — the CoralSwarm credential was revoked.';
  } else if (outcome === 'not_revoked') {
    message = 'Disconnected here. The server credential was not revoked — contact support.';
  } else if (outcome === 'failed') {
    message = 'Disconnected here, but revoking the server credential failed. Try again, or ' +
      'contact support if it keeps happening.';
  }
  return signedOutResponse_(message);
}

/**
 * "Change workspace": drop the stored Clerk tokens and send the user through
 * authorization again. `prompt=consent` is always set, so Clerk shows the
 * workspace picker again; the new pick becomes the token's `org_id`. This does
 * not revoke the old grant at Clerk.
 */
function changeWorkspace(e) {
  getClerkService_().reset();
  PropertiesService.getUserProperties().deleteProperty(PROP_LAST_AUTH_ERROR);
  // Throws the AuthorizationException, the same path as a cold start. (If the
  // client id is somehow unset, fall through to the sign-in card.)
  if (isClientIdConfigured_()) throwSignIn_();
  return signedOutResponse_(null);
}

/** Universal action (the card's ⋮ menu): the escape hatch when a card is
 *  wedged. */
function universalSignOut(e) {
  retireLegacyToken_(false);
  getClerkService_().reset();
  PropertiesService.getUserProperties().deleteProperty(PROP_LAST_AUTH_ERROR);
  return CardService.newUniversalActionResponseBuilder()
    .displayAddOnCards([isClientIdConfigured_() ? buildSignInCard_() : buildSetupCard_()])
    .build();
}

// ── Workspace label ─────────────────────────────────────────────────────

/** Decodes a JWT's payload for DISPLAY only (no signature check — never use it
 *  for authorization). Returns null for a non-JWT/opaque token. */
function decodeJwtPayload_(jwt) {
  if (!jwt || typeof jwt !== 'string') return null;
  var parts = jwt.split('.');
  if (parts.length !== 3) return null;
  try {
    var b64 = parts[1];
    while (b64.length % 4 !== 0) b64 += '=';
    var bytes = Utilities.base64DecodeWebSafe(b64);
    return JSON.parse(Utilities.newBlob(bytes).getDataAsString());
  } catch (err) {
    console.warn('JWT payload decode failed: ' + err);
    return null;
  }
}

/** The org id the token was issued for (`org_id`, or the compact `o.id`), or
 *  null for a personal-workspace token. Display/branching only. */
function tokenOrgId_(accessToken) {
  var claims = decodeJwtPayload_(accessToken) || {};
  return claims.org_id || (claims.o && claims.o.id) || null;
}

/**
 * Whether the watched-thread routes (`GET`/`PUT /v1/connectors/google/watched/
 * {id}`) can work for the current credential. The backend rejects them with
 * 403 for any organization context ("personal workspace only"), so the follow
 * controls are hidden for an org-workspace Clerk token. A legacy `csat_` token
 * is always personal.
 */
function watchedAvailable_() {
  var cred = currentCredential_();
  if (!cred) return false;
  return cred.kind !== 'clerk' || !tokenOrgId_(cred.token);
}

/**
 * What the signed-in card calls the workspace: the org's name if the token
 * carries one (`org_name`, then `org_slug`), else the raw `org_id`, else
 * "Personal" (no org picked). Purely presentational.
 */
function workspaceLabel_(accessToken) {
  var claims = decodeJwtPayload_(accessToken) || {};
  var o = claims.o || {};
  return claims.org_name || claims.org_slug || o.slg || claims.org_id || o.id || 'Personal';
}

/** How long a fetched workspace name is reused, in seconds. Short: a rename in
 *  Clerk shows up within minutes, and "Change workspace" never needs a flush
 *  because the cache key carries the token's org id. */
var WORKSPACE_NAME_CACHE_SECONDS = 300;

/**
 * The name the signed-in card shows for the workspace. The Clerk access token
 * only carries `org_id`, so the name comes from `GET /v1/addin/workspace`
 * (`{kind, name, account_id, org_id}`), cached per (user, org) for
 * WORKSPACE_NAME_CACHE_SECONDS in the user cache. Any failure (no token,
 * network, non-2xx, malformed body, cache unavailable) falls back to
 * `workspaceLabel_` — the card never breaks on this lookup. Legacy `csat_`
 * credentials make no call.
 */
function workspaceName_(cred) {
  if (!cred || cred.kind !== 'clerk') return workspaceLabel_(cred && cred.token);
  var fallback = workspaceLabel_(cred.token);
  var key = 'cs_ws_' + (tokenOrgId_(cred.token) || 'personal');
  var cache = null;
  try {
    cache = CacheService.getUserCache();
    var hit = cache.get(key);
    if (hit) return hit;
  } catch (ignored) {
    cache = null;
  }
  var resp = callBackend_('get', backendBaseUrl_() + '/v1/addin/workspace', null, cred.token);
  if (!resp.ok || !resp.body || typeof resp.body.name !== 'string' || !resp.body.name) {
    return fallback;
  }
  if (cache) {
    try {
      cache.put(key, resp.body.name, WORKSPACE_NAME_CACHE_SECONDS);
    } catch (ignored) {
      // Cache is an optimisation only.
    }
  }
  return resp.body.name;
}

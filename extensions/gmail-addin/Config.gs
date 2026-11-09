/**
 * CoralSwarm Gmail Add-on — configuration.
 *
 * Apps Script has no build step, so there is nothing here templated at
 * build time the way the web-hosted Outlook/Teams panes' env config is — these
 * are plain constants. Override either without editing this file via the Apps
 * Script editor: Project Settings → Script properties → add
 * `BACKEND_BASE_URL` / `WEB_BASE_URL` (e.g. for a staging backend); see
 * this add-on's README, "Local dev / testing".
 *
 * Sign-in is Clerk OAuth (authorization code + PKCE, public client) — see
 * Auth.gs and the README's "Sign-in (Clerk OAuth)". The Clerk-side constants
 * live in the block below.
 */

/** Production CoralSwarm API host. All backend calls (`callBackend_` in
 *  Http.gs) go here unless overridden. */
var DEFAULT_BACKEND_BASE_URL = 'https://api.coralswarm.example';

/** Production CoralSwarm web app host — the hosted card images
 *  (`/addin/*.png`) and the "In your ocean" deep link. The card links here,
 *  it never calls it. */
var DEFAULT_WEB_BASE_URL = 'https://app.coralswarm.example';

/**
 * The Clerk OAuth application's client_id — the ONE value to fill in once the
 * production OAuth app exists. The app must be a PUBLIC client (PKCE, no
 * secret) whose redirect URI is the one `logRedirectUri()` prints for THIS
 * Apps Script project (see the README, "Sign-in (Clerk OAuth)"). It must also
 * be the value of the backend's `GMAIL_ADDIN_CLERK_OAUTH_CLIENT_ID`.
 *
 * Production app "CoralSwarm for Gmail". If it is ever reset to 'REPLACE_ME' nobody can sign in with Clerk;
 * users who already hold a legacy `csat_` pairing token keep working (see
 * `currentCredential_`, Auth.gs). Override without editing this file via the
 * Script property `CLERK_CLIENT_ID`.
 */
var CLERK_CLIENT_ID = 'example_15';

/** Production Clerk Frontend API host (the instance behind app.coralswarm.example).
 *  Override with the Script property `CLERK_FAPI_BASE_URL` — any host you
 *  point it at must ALSO be in the manifest's `urlFetchWhitelist` and
 *  `openLinkUrlPrefixes`, or the token exchange / sign-in link is blocked. */
var DEFAULT_CLERK_FAPI_BASE_URL = 'https://clerk.coralswarm.example';

/** `user:org:read` is what makes Clerk's consent screen show the workspace
 *  (organization) picker; the pick becomes the access token's `org_id`.
 *  `offline_access` is what gets a refresh token issued. */
var CLERK_SCOPES = 'openid profile email offline_access user:org:read';

/** OAuth2 library service name; tokens live in User Properties under
 *  `oauth2.clerk`. */
var OAUTH_SERVICE_NAME = 'clerk';

/**
 * NOT a secret, and NEVER sent. The published OAuth2 library (v43 = 1.43.0)
 * has `refresh()` throw "Client Secret is required" unless some secret is
 * set. `stripClientSecret_` (Auth.gs) deletes `client_secret` from every
 * token-endpoint payload before it is sent, so Clerk sees a pure public-client
 * PKCE request.
 */
var CLIENT_SECRET_PLACEHOLDER = '__public_client_never_sent__';

/** Shown in Gmail's stock authorization prompt and the sign-in card. */
var RESOURCE_DISPLAY_NAME = 'CoralSwarm';

/**
 * `PropertiesService.getUserProperties()` keys. User Properties, not Script
 * Properties or a document/session-scoped store: they are the ONE durable,
 * per-installer key-value store Apps Script offers (see this add-on's
 * README, "Why Apps Script needs its own credential" — Gmail Add-ons have
 * no DOM/iframe to keep a live Clerk session in the way the Outlook add-in
 * does), and they are scoped to (this script, this Google account) — never
 * shared across the installer's other Google Workspace add-ons or other
 * users of the same mailbox.
 */
var PROP_ADDIN_TOKEN = 'cs_addin_token';
/* The three PROP_ADDIN_* keys below are the LEGACY device-pairing credential.
 * Nothing writes them any more; they are only read (to keep an already-paired
 * mailbox working until it signs in with Clerk) and deleted. */
/** The minted token's OWN id (`pair/poll`'s `token_id` field), stored
 *  alongside the token itself — Major 5's real-revocation fix (CodeRabbit
 *  review on #820). Without this, `disconnect()` had no way to name the
 *  server-side row to revoke: `pair/poll`'s response used to carry only
 *  `{status, token}`. */
var PROP_ADDIN_TOKEN_ID = 'cs_addin_token_id';
/** The CoralSwarm account this installation actually paired with
 *  (`pair/poll`'s `account_email` field — the `users.email` of whoever
 *  clicked Approve). Rendered on the connected card as "Connected as
 *  <email>" so a pairing bound to the WRONG account is visible rather than
 *  silent; see `backend/src/routes/addin.rs`'s module header, "The approve
 *  race", for the trade-off this makes noticeable. Optional everywhere it
 *  is read: a backend that predates the field simply leaves it unset and
 *  the card falls back to a plain "Connected". */
var PROP_ADDIN_ACCOUNT_EMAIL = 'cs_addin_account_email';
/** Why the last silent refresh failed (e.g. `invalid_grant`), shown on the
 *  sign-in card so "why am I being asked again" has an answer. */
var PROP_LAST_AUTH_ERROR = 'cs_last_auth_error';

function backendBaseUrl_() {
  var override = PropertiesService.getScriptProperties().getProperty('BACKEND_BASE_URL');
  return override || DEFAULT_BACKEND_BASE_URL;
}

function webBaseUrl_() {
  var override = PropertiesService.getScriptProperties().getProperty('WEB_BASE_URL');
  return override || DEFAULT_WEB_BASE_URL;
}

function clerkClientId_() {
  var override = PropertiesService.getScriptProperties().getProperty('CLERK_CLIENT_ID');
  return override || CLERK_CLIENT_ID;
}

function clerkFapiBaseUrl_() {
  var override = PropertiesService.getScriptProperties().getProperty('CLERK_FAPI_BASE_URL');
  return (override || DEFAULT_CLERK_FAPI_BASE_URL).replace(/\/+$/, '');
}

function isClientIdConfigured_() {
  var id = clerkClientId_();
  return !!id && id !== 'REPLACE_ME';
}

/** The fixed Apps Script OAuth callback URL to register with Clerk:
 *  https://script.google.com/macros/d/{SCRIPT_ID}/usercallback. It is keyed on
 *  the script ID, so a NEW Apps Script project needs a NEW redirect URI. */
function redirectUri_() {
  return 'https://script.google.com/macros/d/' + encodeURIComponent(ScriptApp.getScriptId()) +
    '/usercallback';
}

/** Run this from the Apps Script editor (select it, then Run) and copy the
 *  logged URL into the Clerk OAuth app's redirect URIs. */
function logRedirectUri() {
  var uri = redirectUri_();
  console.log('Script ID:    ' + ScriptApp.getScriptId());
  console.log('Redirect URI: ' + uri);
  return uri;
}

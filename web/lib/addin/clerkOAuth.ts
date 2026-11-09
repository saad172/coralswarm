/* Clerk OAuth (authorization code + PKCE, public client) for the Outlook and
 * Teams add-ins and the Google Meet side panel. The same sign-in the MCP connectors use, run in a host dialog
 * (lib/addin/authDialog.ts) instead of the pairing code.
 *
 * Framework-free and injection-friendly, like microsoftSso.ts:
 *   - env is read LAZILY, inside functions. vitest has no NEXT_PUBLIC_* env, so
 *     a module-scope read would pass vacuously; Next inlines the literal
 *     `process.env.NEXT_PUBLIC_X` accesses below at build time.
 *   - tokens live in the `createClerkSession` closure unless the caller hands
 *     it a TokenStore. Outlook and Teams pass none (memory only; silent
 *     Microsoft SSO brings them back). The Meet panel has no silent path, so
 *     it persists the TokenSet in its own partitioned localStorage.
 *
 * ROLLOUT GATE: the whole flow is active only when
 * NEXT_PUBLIC_ADDIN_CLERK_OAUTH_CLIENT_ID is set (and the Clerk frontend API
 * host can be derived). Otherwise the panes behave exactly as before.
 *
 * PER-HOST CLIENT: the Meet side panel has its own Clerk OAuth app
 * (NEXT_PUBLIC_MEET_ADDIN_CLERK_OAUTH_CLIENT_ID), so the backend can scope its
 * tokens to the Meet routes. The start page picks the client from the host,
 * which the state's first character carries (o / t / g).
 *
 * Facts this relies on (verified live, see the add-in OAuth brief):
 *   - the org picker appears only when `user:org:read` is requested; the picked
 *     org becomes the access token's `org_id` claim;
 *   - refresh tokens ROTATE on every use and the old one then returns
 *     invalid_grant, so exactly one refresh may be in flight (single-flight);
 *   - /oauth/token allows CORS from the app origin, so the pane exchanges and
 *     refreshes directly. */

import type { ConnectedSession } from "./microsoftSso";

export const ADDIN_OAUTH_SCOPE = "openid profile email offline_access user:org:read";
export const AUTH_START_PATH = "/addin/auth/start";
export const AUTH_CALLBACK_PATH = "/addin/auth/callback";

export type AddinHostKind = "outlook" | "teams" | "meet";

export interface ClerkOAuthConfig {
  clientId: string;
  /** Clerk frontend API origin, e.g. https://clerk.coralswarm.example (no trailing slash). */
  fapi: string;
}

// ── Env ──────────────────────────────────────────────────────────────────────

/** A Clerk publishable key is `pk_(test|live)_` + base64("<frontend api host>$"). */
export function fapiFromPublishableKey(publishableKey: string | undefined): string | null {
  const match = /^pk_(?:test|live)_([A-Za-z0-9+/_-]+={0,2})$/.exec((publishableKey ?? "").trim());
  if (!match) return null;
  let decoded: string;
  try { decoded = atob(match[1].replace(/-/g, "+").replace(/_/g, "/")); } catch { return null; }
  if (!decoded.endsWith("$")) return null;
  const host = decoded.slice(0, -1);
  return /^[a-z0-9]([a-z0-9.-]*[a-z0-9])?$/i.test(host) && host.includes(".") ? `https://${host.toLowerCase()}` : null;
}

export interface OAuthEnv { clientId?: string; publishableKey?: string }

/** The literal accesses are what Next.js inlines at build time. Called per use, never at module scope.
 *  Outlook and Teams share one client; Meet has its own. */
function processEnv(host: AddinHostKind = "outlook"): OAuthEnv {
  return {
    clientId: host === "meet" ? process.env.NEXT_PUBLIC_MEET_ADDIN_CLERK_OAUTH_CLIENT_ID : process.env.NEXT_PUBLIC_ADDIN_CLERK_OAUTH_CLIENT_ID,
    publishableKey: process.env.NEXT_PUBLIC_CLERK_PUBLISHABLE_KEY,
  };
}

/** The OAuth config, or null when the rollout gate is off (no client id) or the
 *  frontend API host cannot be derived (a misconfigured build keeps today's pairing
 *  flow instead of shipping a sign-in button that cannot work). */
export function readClerkOAuthConfig(env: OAuthEnv = processEnv()): ClerkOAuthConfig | null {
  const clientId = (env.clientId ?? "").trim();
  if (!clientId) return null;
  const fapi = fapiFromPublishableKey(env.publishableKey);
  return fapi ? { clientId, fapi } : null;
}

export function addinOAuthEnabled(env?: OAuthEnv): boolean { return readClerkOAuthConfig(env) !== null; }

/** The OAuth config for one host's own client (Meet has a separate Clerk app). */
export function readClerkOAuthConfigFor(host: AddinHostKind): ClerkOAuthConfig | null { return readClerkOAuthConfig(processEnv(host)); }

// ── PKCE, state, start-page parameters ───────────────────────────────────────

export function base64url(bytes: Uint8Array): string {
  let binary = "";
  bytes.forEach((b) => { binary += String.fromCharCode(b); });
  return btoa(binary).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
}

function randomBase64url(byteLength: number): string {
  const bytes = new Uint8Array(byteLength);
  crypto.getRandomValues(bytes);
  return base64url(bytes);
}

export interface Pkce { verifier: string; challenge: string }

/** RFC 7636 S256: a 64-character verifier and its 43-character challenge. */
export async function createPkce(): Promise<Pkce> {
  const verifier = randomBase64url(48);
  const digest = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(verifier));
  return { verifier, challenge: base64url(new Uint8Array(digest)) };
}

/** 33 characters: a one-letter host marker (the callback has no other way to know which
 *  host's channel to answer on) plus 32 random base64url characters. */
const HOST_MARKER: Record<AddinHostKind, string> = { outlook: "o", teams: "t", meet: "g" };
const MARKER_HOST: Record<string, AddinHostKind> = { o: "outlook", t: "teams", g: "meet" };

export function createState(host: AddinHostKind): string {
  return `${HOST_MARKER[host]}${randomBase64url(24)}`;
}

const CHALLENGE_RE = /^[A-Za-z0-9_-]{43}$/;
const STATE_RE = /^[otg][A-Za-z0-9_-]{32,127}$/;

export function isValidChallenge(value: string): boolean { return CHALLENGE_RE.test(value); }
export function isValidState(value: string): boolean { return STATE_RE.test(value); }
export function hostFromState(state: string): AddinHostKind | null {
  if (!isValidState(state)) return null;
  return MARKER_HOST[state[0]] ?? null;
}

export function redirectUriFor(origin: string): string { return `${origin}${AUTH_CALLBACK_PATH}`; }

export type StartParams = { cc: string; state: string; host: AddinHostKind; prompt: "consent" | null };

/** Validate the start page's query. Anything unexpected is rejected, so the page can only ever
 *  send the browser to the one authorize URL built from constants. */
export function parseStartParams(search: string): StartParams | null {
  const params = new URLSearchParams(search);
  const cc = params.get("cc") ?? "";
  const state = params.get("state") ?? "";
  const host = params.get("host");
  const prompt = params.get("prompt");
  if (!isValidChallenge(cc) || !isValidState(state)) return null;
  if (host !== "outlook" && host !== "teams" && host !== "meet") return null;
  if (hostFromState(state) !== host) return null;
  if (prompt !== null && prompt !== "consent") return null;
  return { cc, state, host, prompt: prompt === "consent" ? "consent" : null };
}

/** The pane builds this and hands it to the host dialog. */
export function buildStartUrl(origin: string, params: { cc: string; state: string; host: AddinHostKind; prompt?: "consent" }): string {
  const query = new URLSearchParams({ cc: params.cc, state: params.state, host: params.host });
  if (params.prompt) query.set("prompt", params.prompt);
  return `${origin}${AUTH_START_PATH}?${query.toString()}`;
}

export function buildAuthorizeUrl(config: ClerkOAuthConfig, origin: string, params: { cc: string; state: string; prompt?: "consent" | null }): string {
  const query = new URLSearchParams({
    response_type: "code",
    client_id: config.clientId,
    redirect_uri: redirectUriFor(origin),
    scope: ADDIN_OAUTH_SCOPE,
    code_challenge: params.cc,
    code_challenge_method: "S256",
    state: params.state,
  });
  if (params.prompt) query.set("prompt", params.prompt);
  return `${config.fapi}/oauth/authorize?${query.toString()}`;
}

// ── Tokens ───────────────────────────────────────────────────────────────────

export interface TokenSet {
  accessToken: string;
  /** Null only if the server omitted it; the previous one is then kept. */
  refreshToken: string | null;
  /** Epoch milliseconds. */
  expiresAt: number;
  idToken: string | null;
}

/** The refresh token was rejected (invalid_grant: revoked, expired or already used). */
export class SignedOutError extends Error { constructor() { super("Signed out"); this.name = "SignedOutError"; } }
/** The token endpoint failed for a reason that does not mean "signed out" (network, 5xx, 429). */
export class OAuthTransientError extends Error { constructor(message = "Token endpoint unavailable") { super(message); this.name = "OAuthTransientError"; } }
/** Any other token endpoint refusal. */
export class OAuthError extends Error {
  code: string;
  constructor(code: string) { super(code); this.name = "OAuthError"; this.code = code; }
}

export interface OAuthDeps { fetchImpl?: typeof fetch; now?: () => number }

/** Refresh this long before expiry so a request never races it. */
const EXPIRY_SKEW_MS = 60_000;
const DEFAULT_LIFETIME_S = 3600;

function isRecord(value: unknown): value is Record<string, unknown> { return typeof value === "object" && value !== null; }

function parseTokenResponse(body: unknown, now: number): TokenSet {
  if (!isRecord(body) || typeof body.access_token !== "string" || !body.access_token) throw new OAuthError("malformed_token_response");
  const expiresIn = typeof body.expires_in === "number" && Number.isFinite(body.expires_in) && body.expires_in > 0 ? body.expires_in : DEFAULT_LIFETIME_S;
  return {
    accessToken: body.access_token,
    refreshToken: typeof body.refresh_token === "string" && body.refresh_token ? body.refresh_token : null,
    expiresAt: now + expiresIn * 1000,
    idToken: typeof body.id_token === "string" && body.id_token ? body.id_token : null,
  };
}

async function postToken(config: ClerkOAuthConfig, form: Record<string, string>, deps: OAuthDeps): Promise<TokenSet> {
  const doFetch = deps.fetchImpl ?? ((input, init) => fetch(input, init));
  const now = deps.now ?? Date.now;
  let res: Response;
  try {
    res = await doFetch(`${config.fapi}/oauth/token`, {
      method: "POST",
      headers: { "Content-Type": "application/x-www-form-urlencoded" },
      body: new URLSearchParams(form).toString(),
      cache: "no-store",
    });
  } catch { throw new OAuthTransientError(); }
  let body: unknown = null;
  try { body = await res.json(); } catch { /* not JSON */ }
  if (res.ok) return parseTokenResponse(body, now());
  const code = isRecord(body) && typeof body.error === "string" ? body.error : `http_${res.status}`;
  if (code === "invalid_grant") throw new SignedOutError();
  if (res.status >= 500 || res.status === 429 || res.status === 408) throw new OAuthTransientError(code);
  throw new OAuthError(code);
}

export function exchangeCode(config: ClerkOAuthConfig, input: { code: string; verifier: string; redirectUri: string }, deps: OAuthDeps = {}): Promise<TokenSet> {
  return postToken(config, {
    grant_type: "authorization_code", code: input.code, redirect_uri: input.redirectUri, client_id: config.clientId, code_verifier: input.verifier,
  }, deps);
}

export function refreshTokens(config: ClerkOAuthConfig, refreshToken: string, deps: OAuthDeps = {}): Promise<TokenSet> {
  return postToken(config, { grant_type: "refresh_token", refresh_token: refreshToken, client_id: config.clientId }, deps);
}

/** Decode (NOT verify) a JWT payload: for display only. The backend is the verifier. */
export function jwtClaims(token: string | null): Record<string, unknown> | null {
  if (!token) return null;
  const parts = token.split(".");
  if (parts.length !== 3) return null;
  try {
    const json = decodeURIComponent(Array.from(atob(parts[1].replace(/-/g, "+").replace(/_/g, "/")), (c) => `%${c.charCodeAt(0).toString(16).padStart(2, "0")}`).join(""));
    const claims: unknown = JSON.parse(json);
    return isRecord(claims) ? claims : null;
  } catch { return null; }
}

/** What a Clerk-direct pane shows. The access token carries `org_id` but no workspace name, so the
 *  name comes from id-token claims when Clerk sends them and is generic otherwise. */
export function sessionFromTokens(tokens: TokenSet): ConnectedSession {
  const access = jwtClaims(tokens.accessToken);
  const id = jwtClaims(tokens.idToken);
  const str = (v: unknown) => (typeof v === "string" && v ? v : null);
  const orgId = str(access?.org_id) ?? str(id?.org_id);
  const email = str(id?.email) ?? str(access?.email) ?? "";
  const orgName = str(id?.org_name) ?? str(access?.org_name) ?? str(id?.org_slug) ?? str(access?.org_slug);
  return {
    token: tokens.accessToken,
    expiresAt: tokens.expiresAt,
    account: { email },
    workspace: orgId ? { account_id: orgId, name: orgName ?? "Your organization", kind: "org" } : { account_id: "personal", name: "Personal", kind: "personal" },
    autoLinked: false,
  };
}

// ── Persistence ──────────────────────────────────────────────────────────────

/** Where a session keeps its TokenSet between page loads. `save(null)` removes it. */
export interface TokenStore { load(): TokenSet | null; save(tokens: TokenSet | null): void }

function parseStoredTokens(value: unknown): TokenSet | null {
  if (!isRecord(value)) return null;
  const { accessToken, refreshToken, expiresAt, idToken } = value;
  if (typeof accessToken !== "string" || !accessToken) return null;
  if (refreshToken !== null && typeof refreshToken !== "string") return null;
  if (typeof expiresAt !== "number" || !Number.isFinite(expiresAt)) return null;
  if (idToken !== null && typeof idToken !== "string") return null;
  return { accessToken, refreshToken, expiresAt, idToken };
}

/** A TokenStore over one localStorage key, or null when the storage is missing or throws on
 *  access (third-party storage blocked, private window): the session then stays memory-only. */
export function localStorageTokenStore(key: string, storage: Storage | null | undefined): TokenStore | null {
  if (!storage) return null;
  try { storage.getItem(key); } catch { return null; }
  return {
    load() {
      let raw: string | null;
      try { raw = storage.getItem(key); } catch { return null; }
      if (!raw) return null;
      try { return parseStoredTokens(JSON.parse(raw)); } catch { return null; }
    },
    save(tokens) {
      try { if (tokens) storage.setItem(key, JSON.stringify(tokens)); else storage.removeItem(key); } catch { /* memory-only from here */ }
    },
  };
}

// ── Session with single-flight refresh ───────────────────────────────────────

type Refreshable = TokenSet & { refreshToken: string };
function refreshable(tokens: TokenSet | null): tokens is Refreshable { return !!tokens?.refreshToken; }

export function createClerkSession(config: ClerkOAuthConfig, deps: OAuthDeps = {}, store?: TokenStore | null) {
  const clock = deps.now ?? Date.now;
  let tokens: TokenSet | null = store?.load() ?? null;
  let inFlight: Promise<TokenSet> | null = null;
  /** Bumped by set()/clear(): a refresh that started before cannot write its result back. */
  let generation = 0;

  function write(next: TokenSet | null) { tokens = next; store?.save(next); }

  /** Another panel instance over the same store (a second Meet tab) may have rotated the
   *  refresh token; the one it stored is the live one and ours is dead. */
  function storedRotation(current: TokenSet | null): Refreshable | null {
    const stored = store?.load() ?? null;
    return refreshable(stored) && stored.refreshToken !== current?.refreshToken ? stored : null;
  }

  async function rotate(current: Refreshable, adoptStored: boolean): Promise<TokenSet> {
    try {
      const next = await refreshTokens(config, current.refreshToken, deps);
      // Rotation: the refresh token just used is dead; keep the new one (or, if the server sent none, the old).
      return { ...next, refreshToken: next.refreshToken ?? current.refreshToken, idToken: next.idToken ?? current.idToken };
    } catch (cause) {
      const stored = cause instanceof SignedOutError && adoptStored ? storedRotation(current) : null;
      if (stored) return rotate(stored, false);
      throw cause;
    }
  }

  function refresh(): Promise<TokenSet> {
    if (inFlight) return inFlight;
    const started = generation;
    const current = storedRotation(tokens) ?? tokens;
    if (!refreshable(current)) { write(null); return Promise.reject(new SignedOutError()); }
    const run: Promise<TokenSet> = rotate(current, true).then(
      (merged) => { if (started === generation) write(merged); return merged; },
      (cause) => {
        if (cause instanceof SignedOutError && started === generation) write(null);
        throw cause;
      },
    ).finally(() => { if (inFlight === run) inFlight = null; });
    inFlight = run;
    return run;
  }

  return {
    set(next: TokenSet) { generation++; inFlight = null; write(next); },
    clear() { generation++; inFlight = null; write(null); },
    hasTokens: () => tokens !== null,
    snapshot: () => tokens,
    /** A token good for at least the skew. `force` refreshes regardless (after a 401). */
    async accessToken(options: { force?: boolean } = {}): Promise<string> {
      if (!tokens) throw new SignedOutError();
      if (!options.force && tokens.expiresAt - EXPIRY_SKEW_MS > clock()) return tokens.accessToken;
      return (await refresh()).accessToken;
    },
  };
}

export type ClerkSession = ReturnType<typeof createClerkSession>;

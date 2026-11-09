/* Microsoft SSO client for the Teams and Outlook add-ins (one system).
 *
 * WHY THIS EXISTS. Clerk's session cookies are third-party inside the Teams and
 * Outlook iframes and get blocked, so a Clerk-in-the-pane sign-in was repeated
 * for every new meeting. The host already knows who the user is: Teams hands
 * the pane a Microsoft token (`authentication.getAuthToken()`), Outlook does the
 * same through nested app auth. This module trades that token for a CoralSwarm
 * credential and keeps the credential IN MEMORY ONLY — no cookie, no
 * localStorage, no sessionStorage. When it expires (or the API answers 401) the
 * pane silently trades a fresh Microsoft token again.
 *
 * It is framework-free and takes every environmental dependency (host token
 * source, API base, fetch, clock) by injection so it can be tested without a
 * DOM and so the module has no module-scope env reads (vitest has no
 * NEXT_PUBLIC_* env; a module-scope read would pass vacuously).
 *
 * Backend contract (PR 1): POST /v1/addin/microsoft/session, POST
 * /v1/addin/pair/poll, GET|DELETE /v1/addin/microsoft/link,
 * PUT /v1/addin/microsoft/link/workspace. Existing connector routes accept the
 * returned token as a Bearer. */

import { mark } from "./perf";

export type WorkspaceKind = "personal" | "org";
export interface Workspace { account_id: string; name: string; kind: WorkspaceKind }

export interface ConnectedSession {
  token: string;
  /** Epoch milliseconds. */
  expiresAt: number;
  account: { email: string };
  workspace: Workspace;
  autoLinked: boolean;
}

export interface Pairing {
  code: string;
  pollSecret: string;
  approveUrl: string;
  /** Epoch milliseconds. */
  expiresAt: number;
}

export type SessionResult =
  | ({ kind: "connected" } & ConnectedSession)
  | { kind: "link_required"; pairing: Pairing };

export type PollStatus = "pending" | "approved" | "expired";

export interface LinkInfo { account: { email: string }; workspace: Workspace; workspaces: Workspace[] }

/** What a host (Teams, Outlook) contributes: a Microsoft token and a way to open the browser. */
export interface AddinHost {
  name: "teams" | "outlook";
  /** Rejects with SsoUnavailableError when the host cannot issue a token. */
  getMicrosoftToken(): Promise<string>;
  openLink(url: string): Promise<void>;
}

/** The backend says Microsoft SSO is not switched on (404 `microsoft_sso_disabled`). */
export class SsoDisabledError extends Error { constructor() { super("Microsoft SSO is disabled"); this.name = "SsoDisabledError"; } }
/** The host cannot produce a Microsoft token (NAA/SSO unsupported, consent needed, ...). */
export class SsoUnavailableError extends Error { constructor(message = "Microsoft sign-in is unavailable in this host") { super(message); this.name = "SsoUnavailableError"; } }
/** The backend rejected the Microsoft token (401 `invalid_microsoft_token`). */
export class InvalidMicrosoftTokenError extends Error { constructor() { super("Microsoft token rejected"); this.name = "InvalidMicrosoftTokenError"; } }
/** A call failed with a non-auth HTTP status. */
export class AddinHttpError extends Error {
  status: number;
  code: string | null;
  constructor(status: number, code: string | null = null) { super(code ?? `HTTP ${status}`); this.name = "AddinHttpError"; this.status = status; this.code = code; }
}
/** The link is gone (revoked, disconnected elsewhere). The pane must go back to the start card. */
export class ReconnectRequiredError extends Error { constructor() { super("CoralSwarm needs to be reconnected"); this.name = "ReconnectRequiredError"; } }

export interface AddinClientOptions {
  host: Pick<AddinHost, "getMicrosoftToken">;
  apiBase: () => string;
  fetchImpl?: typeof fetch;
  now?: () => number;
}

/** Refresh this long before the credential's own expiry so a request never races it. */
const EXPIRY_SKEW_MS = 30_000;
/** A re-trade for a fresh credential (not the first sign-in, which may legitimately wait on the user) that has
 *  not finished by now fails as a transient error instead of leaving the caller waiting forever. */
export const TOKEN_TIMEOUT_MS = 20_000;

function withDeadline<T>(promise: Promise<T>, ms: number, onTimeout: () => void): Promise<T> {
  let timer: ReturnType<typeof setTimeout>;
  const deadline = new Promise<never>((_, reject) => { timer = setTimeout(() => { onTimeout(); reject(new AddinHttpError(408, "token_timeout")); }, ms); });
  return Promise.race([promise, deadline]).finally(() => clearTimeout(timer));
}

/** Used when the backend sends no parseable expiry: re-session soon rather than trust it forever. */
const DEFAULT_LIFETIME_MS = 5 * 60_000;

/** Bounded exponential backoff for session exchanges that fail for a transient reason
 *  (5xx, network): 1s, 2s, 4s ... capped at 30s. `attempt` is the number of failures so far, from 0. */
export const BACKOFF_BASE_MS = 1000;
export const BACKOFF_CAP_MS = 30_000;
export function backoffDelay(attempt: number): number {
  return Math.min(BACKOFF_CAP_MS, BACKOFF_BASE_MS * 2 ** Math.max(0, attempt));
}

/** A failure worth retrying later: network errors, 408, 429 and 5xx. Other 4xx (400, 403, ...)
 *  and the explained terminal outcomes (SSO off/unavailable, rejected token, reconnect) are not. */
export function isTransientSessionFailure(cause: unknown): boolean {
  if (cause instanceof SsoDisabledError || cause instanceof SsoUnavailableError || cause instanceof InvalidMicrosoftTokenError || cause instanceof ReconnectRequiredError) return false;
  if (cause instanceof AddinHttpError) return cause.status >= 500 || cause.status === 429 || cause.status === 408;
  return true;
}

function parseExpiry(value: unknown, now: number, fallback: number): number {
  if (typeof value === "number" && Number.isFinite(value)) return value < 1e12 ? value * 1000 : value;
  if (typeof value === "string") { const parsed = Date.parse(value); if (!Number.isNaN(parsed)) return parsed; }
  return now + fallback;
}

function isRecord(value: unknown): value is Record<string, unknown> { return typeof value === "object" && value !== null; }

function parseWorkspace(value: unknown): Workspace {
  if (!isRecord(value) || typeof value.account_id !== "string" || typeof value.name !== "string" || (value.kind !== "personal" && value.kind !== "org"))
    throw new AddinHttpError(502, "malformed_workspace");
  return { account_id: value.account_id, name: value.name, kind: value.kind };
}

function parseConnected(body: unknown, now: number): ConnectedSession {
  if (!isRecord(body) || typeof body.token !== "string" || !body.token || !isRecord(body.account) || typeof body.account.email !== "string")
    throw new AddinHttpError(502, "malformed_session");
  return {
    token: body.token,
    expiresAt: parseExpiry(body.expires_at, now, DEFAULT_LIFETIME_MS),
    account: { email: body.account.email },
    workspace: parseWorkspace(body.workspace),
    autoLinked: body.auto_linked === true,
  };
}

async function readBody(res: Response): Promise<unknown> {
  try { return await res.json(); } catch { return null; }
}

function errorCode(body: unknown): string | null {
  return isRecord(body) && typeof body.error === "string" ? body.error : null;
}

function isUnauthorized(cause: unknown): boolean {
  return isRecord(cause) && cause.status === 401;
}

export function createAddinClient(options: AddinClientOptions) {
  const doFetch: typeof fetch = options.fetchImpl ?? ((input, init) => fetch(input, init));
  const clock = options.now ?? (() => Date.now());
  /** The CoralSwarm credential. Memory only, by design. */
  let current: ConnectedSession | null = null;
  let inFlight: Promise<SessionResult> | null = null;
  /** Bumped by disconnect()/forget(): an exchange that started before cannot write the credential back. */
  let generation = 0;
  /** Backoff gate: after a transient exchange failure no new exchange hits the API until
   *  `retryAt`; callers in the window get the last failure back immediately. This bounds
   *  every loop above this client (polling, focus handlers, retries) at the HTTP level. */
  let failures = 0;
  let retryAt = 0;
  let lastFailure: unknown = null;

  const url = (path: string) => `${options.apiBase().replace(/\/$/, "")}${path}`;

  async function exchange(): Promise<SessionResult> {
    const started = generation;
    const msToken = await options.host.getMicrosoftToken();
    mark("exchange-start");
    const res = await doFetch(url("/v1/addin/microsoft/session"), {
      method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ ms_token: msToken }),
    });
    mark("exchange-done");
    const body = await readBody(res);
    if (res.status === 404 && errorCode(body) === "microsoft_sso_disabled") throw new SsoDisabledError();
    if (res.status === 401) { if (started === generation) current = null; throw new InvalidMicrosoftTokenError(); }
    if (!res.ok || !isRecord(body)) throw new AddinHttpError(res.status, errorCode(body));
    const now = clock();
    if (body.status === "connected") { const session = parseConnected(body, now); if (started === generation) current = session; return { kind: "connected", ...session }; }
    if (body.status === "link_required" && isRecord(body.pairing)) {
      const p = body.pairing;
      if (typeof p.code !== "string" || typeof p.poll_secret !== "string" || typeof p.approve_url !== "string") throw new AddinHttpError(502, "malformed_pairing");
      if (started === generation) current = null;
      return { kind: "link_required", pairing: { code: p.code, pollSecret: p.poll_secret, approveUrl: p.approve_url, expiresAt: parseExpiry(p.expires_at, now, 10 * 60_000) } };
    }
    throw new AddinHttpError(502, "malformed_session");
  }

  /** Trade a Microsoft token for a CoralSwarm session. Concurrent callers share one exchange. */
  function session(options: { force?: boolean } = {}): Promise<SessionResult> {
    if (inFlight) return inFlight;
    if (!options.force && lastFailure !== null && clock() < retryAt) return Promise.reject(lastFailure);
    // An exchange that outlives a forget()/disconnect() must not write backoff state back.
    const started = generation;
    const run: Promise<SessionResult> = exchange().then(
      (result) => { if (started === generation) { failures = 0; lastFailure = null; } return result; },
      (cause) => {
        if (started === generation) {
          if (isTransientSessionFailure(cause)) { lastFailure = cause; retryAt = clock() + backoffDelay(failures); failures++; }
          else { failures = 0; lastFailure = null; }
        }
        throw cause;
      },
    ).finally(() => { if (inFlight === run) inFlight = null; });
    inFlight = run;
    return run;
  }

  async function poll(pairing: Pick<Pairing, "code" | "pollSecret">): Promise<PollStatus> {
    try {
      const res = await doFetch(url("/v1/addin/pair/poll"), {
        method: "POST", headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ code: pairing.code, poll_secret: pairing.pollSecret }),
      });
      const body = await readBody(res);
      if (res.ok && isRecord(body) && (body.status === "approved" || body.status === "expired" || body.status === "pending")) return body.status;
    } catch { /* transient: keep waiting */ }
    return "pending";
  }

  /** A re-trade whose Microsoft token the backend rejects means this pane can no longer
   *  vouch for the user: same recovery as a lost link (back to the start card). */
  async function sessionOrReconnect(): Promise<SessionResult> {
    const attempt = session();
    try {
      return await withDeadline(attempt, TOKEN_TIMEOUT_MS, () => {
        // A timeout ENDS this logical exchange: the next caller starts a fresh one instead of waiting on the
        // stuck promise, and whatever the stuck one eventually returns is stale (generation moved on) and
        // cannot touch the credential or the backoff state.
        if (inFlight === attempt) { inFlight = null; generation++; }
      });
    }
    catch (cause) {
      if (cause instanceof InvalidMicrosoftTokenError) throw new ReconnectRequiredError();
      throw cause;
    }
  }

  /** A credential good for at least the skew, re-trading when needed. */
  async function ensureToken(): Promise<string> {
    if (current && current.expiresAt - EXPIRY_SKEW_MS > clock()) return current.token;
    const result = await sessionOrReconnect();
    if (result.kind !== "connected") throw new ReconnectRequiredError();
    return result.token;
  }

  /** Run `fn` with a live token; on a 401 from `fn` trade again and retry exactly once. */
  async function withToken<T>(fn: (token: string) => Promise<T>): Promise<T> {
    const first = await ensureToken();
    try { return await fn(first); }
    catch (cause) {
      if (!isUnauthorized(cause)) throw cause;
      current = null;
      const result = await sessionOrReconnect();
      if (result.kind !== "connected") throw new ReconnectRequiredError();
      return fn(result.token);
    }
  }

  /** Authenticated JSON call against the CoralSwarm API. Non-2xx throws AddinHttpError. */
  function call<T>(path: string, init: { method?: string; body?: unknown; signal?: AbortSignal } = {}): Promise<T> {
    return withToken(async (token) => {
      const headers: Record<string, string> = { Authorization: `Bearer ${token}` };
      if (init.body !== undefined) headers["Content-Type"] = "application/json";
      const res = await doFetch(url(path), { method: init.method ?? "GET", headers, signal: init.signal, ...(init.body !== undefined ? { body: JSON.stringify(init.body) } : {}) });
      if (res.status === 204) return undefined as T;
      const body = await readBody(res);
      if (!res.ok) throw new AddinHttpError(res.status, errorCode(body));
      return body as T;
    });
  }

  /** Add-in Clerk OAuth sign-in (rollout-gated; see clerkOAuth.ts): trade the host's Microsoft token for a
   *  CoralSwarm session, authorised by the user's Clerk add-in access token instead of a pairing. The
   *  backend links this Microsoft identity to the Clerk user's workspace and answers with the same
   *  `connected` body as /session, which becomes this client's credential exactly like a normal exchange. */
  async function linkWithClerk(clerkAccessToken: string): Promise<ConnectedSession> {
    const started = generation;
    const msToken = await options.host.getMicrosoftToken();
    const res = await doFetch(url("/v1/addin/microsoft/link"), {
      method: "POST", headers: { "Content-Type": "application/json", Authorization: `Bearer ${clerkAccessToken}` }, body: JSON.stringify({ ms_token: msToken }),
    });
    const body = await readBody(res);
    const code = errorCode(body);
    if (res.status === 404 && code === "microsoft_sso_disabled") throw new SsoDisabledError();
    if (res.status === 401 && code === "invalid_microsoft_token") throw new InvalidMicrosoftTokenError();
    if (!res.ok) throw new AddinHttpError(res.status, code);
    const session = parseConnected(body, clock());
    if (started === generation) current = session;
    return session;
  }

  async function link(): Promise<LinkInfo> {
    const body = await call<unknown>("/v1/addin/microsoft/link");
    if (!isRecord(body) || !isRecord(body.account) || typeof body.account.email !== "string" || !Array.isArray(body.workspaces)) throw new AddinHttpError(502, "malformed_link");
    return { account: { email: body.account.email }, workspace: parseWorkspace(body.workspace), workspaces: body.workspaces.map(parseWorkspace) };
  }

  async function changeWorkspace(accountId: string): Promise<ConnectedSession> {
    const body = await call<unknown>("/v1/addin/microsoft/link/workspace", { method: "PUT", body: { account_id: accountId } });
    const next = parseConnected(body, clock());
    current = next;
    return next;
  }

  /** Revoke the link server-side (affects Teams and Outlook alike). Local state is dropped either way. */
  async function disconnect(): Promise<{ revoked: boolean }> {
    failures = 0; lastFailure = null;
    generation++; // from here on, no exchange that began earlier may store a credential
    let revoked = false;
    try { await call<void>("/v1/addin/microsoft/link", { method: "DELETE" }); revoked = true; } catch { revoked = false; }
    current = null;
    return { revoked };
  }

  return { session, poll, withToken, call, link, linkWithClerk, changeWorkspace, disconnect, current: () => current, forget: () => { generation++; current = null; failures = 0; lastFailure = null; } };
}

export type AddinClient = ReturnType<typeof createAddinClient>;

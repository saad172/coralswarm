/* The pane's AddinClient when the Clerk OAuth rollout gate is on.
 *
 * `createPaneClient` is the one place the panes build their client. With the
 * gate off it returns `createAddinClient(...)` untouched, so behaviour is exactly
 * what shipped before. With the gate on it returns a client with the SAME
 * interface (so MailContent, MeetingPane, the compose pane, ... do not change)
 * that works in one of two modes:
 *
 *   "ms"    a Microsoft token is available. After the Clerk sign-in the pane
 *           calls POST /v1/addin/microsoft/link {ms_token} (Clerk access token
 *           as Bearer) and carries on with the returned csms_ session exactly as
 *           before, including silent re-trades on reload via /session.
 *   "clerk" no Microsoft token (SSO unavailable or off). Every call sends the
 *           Clerk access token, refreshed single-flight with rotation.
 *
 * Controls for the sign-in itself (`signIn`) are reached through
 * `oauthControlsFor(client)`; a client with no controls is a plain pairing client. */

import {
  AddinHttpError, ReconnectRequiredError, SsoDisabledError, SsoUnavailableError,
  createAddinClient, type AddinClient, type AddinHost, type ConnectedSession,
} from "./microsoftSso";
import { DialogCancelledError, runAuthDialog, type DialogTarget } from "./authDialog";
import {
  SignedOutError, createClerkSession, exchangeCode, readClerkOAuthConfig, sessionFromTokens,
  type ClerkOAuthConfig, type OAuthDeps,
} from "./clerkOAuth";

/** The pane has no usable credential and needs the user to sign in (the "Sign in to CoralSwarm" state). */
export class SignInRequiredError extends Error { constructor() { super("Sign-in required"); this.name = "SignInRequiredError"; } }

/** The workspace lookup is optional, so it must never hold the sign-in hostage. */
export const WORKSPACE_LOOKUP_TIMEOUT_MS = 8000;

export interface OAuthControls {
  /** Open the dialog, exchange the code, and establish a session. `prompt: "consent"` re-shows the org picker. */
  signIn(options?: { prompt?: "consent" }): Promise<ConnectedSession>;
  /** Whether a Clerk credential is held in memory (false once disconnect has begun). */
  hasClerkTokens(): boolean;
}

const controls = new WeakMap<AddinClient, OAuthControls>();
export function oauthControlsFor(client: AddinClient): OAuthControls | null { return controls.get(client) ?? null; }

export interface PaneClientOptions {
  host: AddinHost;
  apiBase: () => string;
  /** How this host opens its sign-in dialog. */
  dialog: DialogTarget;
  /** Test seams. */
  config?: ClerkOAuthConfig | null;
  fetchImpl?: typeof fetch;
  now?: () => number;
  runDialog?: typeof runAuthDialog;
}

export function createPaneClient(options: PaneClientOptions): AddinClient {
  const ms = createAddinClient({ host: options.host, apiBase: options.apiBase, fetchImpl: options.fetchImpl, now: options.now });
  const config = options.config === undefined ? readClerkOAuthConfig() : options.config;
  if (!config) return ms;
  return createOAuthClient(ms, config, options);
}

function isRecord(value: unknown): value is Record<string, unknown> { return typeof value === "object" && value !== null; }

export function createOAuthClient(ms: AddinClient, config: ClerkOAuthConfig, options: PaneClientOptions): AddinClient {
  const deps: OAuthDeps = { fetchImpl: options.fetchImpl, now: options.now };
  const doFetch: typeof fetch = options.fetchImpl ?? ((input, init) => fetch(input, init));
  const clerk = createClerkSession(config, deps);
  const dialog = options.runDialog ?? runAuthDialog;
  const url = (path: string) => `${options.apiBase().replace(/\/$/, "")}${path}`;

  let mode: "ms" | "clerk" | null = null;
  let direct: ConnectedSession | null = null;
  let signingIn: Promise<ConnectedSession> | null = null;
  /** Bumped when the credential is dropped (disconnect/forget), so an in-flight sign-in cannot restore it. */
  let epoch = 0;

  /** Run `fn` with a live Clerk access token; on a 401 refresh once (rotating) and retry once. */
  async function withClerkToken<T>(fn: (token: string) => Promise<T>): Promise<T> {
    try {
      const first = await clerk.accessToken();
      try { return await fn(first); }
      catch (cause) {
        if (!(cause instanceof AddinHttpError) || cause.status !== 401) throw cause;
        return await fn(await clerk.accessToken({ force: true }));
      }
    } catch (cause) {
      // A lost Clerk credential (refresh rejected, nothing stored) is a lost sign-in.
      if (cause instanceof SignedOutError) { mode = null; direct = null; throw new ReconnectRequiredError(); }
      throw cause;
    }
  }

  /** Tokens in hand: link the Microsoft identity (mode "ms") or, with no Microsoft token, go direct. */
  async function establish(): Promise<ConnectedSession> {
    try {
      const session = await withClerkToken((token) => ms.linkWithClerk(token));
      mode = "ms"; direct = null;
      return session;
    } catch (cause) {
      if (!(cause instanceof SsoUnavailableError || cause instanceof SsoDisabledError)) {
        if (cause instanceof ReconnectRequiredError) throw new SignInRequiredError();
        throw cause;
      }
    }
    const tokens = clerk.snapshot();
    if (!tokens) throw new SignInRequiredError();
    const epochAtStart = epoch;
    const workspace = await fetchWorkspace();
    // disconnect()/forget() ran while the lookup was pending: do not resurrect the credential.
    if (epoch !== epochAtStart) throw new SignInRequiredError();
    // A 401 retry inside the lookup may have refreshed the tokens: build from the current ones.
    const fromTokens = sessionFromTokens(clerk.snapshot() ?? tokens);
    mode = "clerk";
    direct = workspace ? { ...fromTokens, workspace } : fromTokens;
    return direct;
  }

  /** GET /v1/addin/workspace with the Clerk bearer: the real workspace name and account id. Any failure
   *  (404 while the backend is not deployed, 5xx, network, a malformed body) returns null so the caller
   *  keeps the id-token-derived workspace. A lost sign-in is the one thing that still surfaces. */
  async function fetchWorkspace(): Promise<ConnectedSession["workspace"] | null> {
    const abort = new AbortController();
    let timer: ReturnType<typeof setTimeout> | undefined;
    const timeout = new Promise<null>((resolve) => { timer = setTimeout(() => { abort.abort(); resolve(null); }, WORKSPACE_LOOKUP_TIMEOUT_MS); });
    try {
      const body = await Promise.race([directCall<unknown>("/v1/addin/workspace", { signal: abort.signal }), timeout]);
      if (!isRecord(body)) return null;
      const { name, account_id: accountId, kind } = body;
      if (typeof name !== "string" || !name.trim() || typeof accountId !== "string" || !accountId) return null;
      if (kind !== "personal" && kind !== "org") return null;
      return { account_id: accountId, name, kind };
    } catch (cause) {
      if (cause instanceof ReconnectRequiredError) throw new SignInRequiredError();
      return null;
    } finally { clearTimeout(timer); }
  }

  function signIn(signInOptions: { prompt?: "consent" } = {}): Promise<ConnectedSession> {
    if (signingIn) return signingIn;
    const run = (async () => {
      const result = await dialog({ target: options.dialog, prompt: signInOptions.prompt });
      const tokens = await exchangeCode(config, { code: result.code, verifier: result.verifier, redirectUri: result.redirectUri }, deps);
      clerk.set(tokens);
      return establish();
    })().finally(() => { if (signingIn === run) signingIn = null; });
    signingIn = run;
    return run;
  }

  async function directCall<T>(path: string, init: { method?: string; body?: unknown; signal?: AbortSignal }): Promise<T> {
    return withClerkToken(async (token) => {
      const headers: Record<string, string> = { Authorization: `Bearer ${token}` };
      if (init.body !== undefined) headers["Content-Type"] = "application/json";
      const res = await doFetch(url(path), { method: init.method ?? "GET", headers, signal: init.signal, ...(init.body !== undefined ? { body: JSON.stringify(init.body) } : {}) });
      if (res.status === 204) return undefined as T;
      let body: unknown = null;
      try { body = await res.json(); } catch { /* not JSON */ }
      if (!res.ok) throw new AddinHttpError(res.status, isRecord(body) && typeof body.error === "string" ? body.error : null);
      return body as T;
    });
  }

  const client: AddinClient = {
    ...ms,
    /** Boot: the silent path for returning users (Microsoft SSO + an existing link). No link, or no SSO,
     *  means the user has to sign in; never a pairing. */
    async session(sessionOptions) {
      if (clerk.hasTokens() && mode === null) return { kind: "connected", ...(await establish()) };
      try {
        const result = await ms.session(sessionOptions);
        if (result.kind === "connected") { mode = "ms"; return result; }
      } catch (cause) {
        if (!(cause instanceof SsoUnavailableError || cause instanceof SsoDisabledError)) throw cause;
      }
      throw new SignInRequiredError();
    },
    withToken: (fn) => (mode === "clerk" ? withClerkToken(fn) : ms.withToken(fn)),
    call: <T,>(path: string, init: { method?: string; body?: unknown; signal?: AbortSignal } = {}) => (mode === "clerk" ? directCall<T>(path, init) : ms.call<T>(path, init)),
    link: () => (mode === "clerk" ? Promise.reject(new AddinHttpError(404, "link_unavailable")) : ms.link()),
    changeWorkspace: (accountId) => (mode === "clerk" ? Promise.reject(new AddinHttpError(404, "link_unavailable")) : ms.changeWorkspace(accountId)),
    async disconnect() {
      const wasDirect = mode === "clerk";
      // Drop the Clerk credential first: nothing may use it while the (possibly slow) revoke is pending.
      clerk.clear(); epoch++; mode = null; direct = null;
      return wasDirect ? { revoked: false } : await ms.disconnect();
    },
    current: () => (mode === "clerk" ? direct : ms.current()),
    forget: () => { ms.forget(); clerk.clear(); epoch++; mode = null; direct = null; },
  };
  controls.set(client, { signIn, hasClerkTokens: () => clerk.hasTokens() });
  return client;
}

/** The user cancelled (closed the dialog or declined consent). */
export function isSignInCancelled(cause: unknown): boolean { return cause instanceof DialogCancelledError; }

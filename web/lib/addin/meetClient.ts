/* The Google Meet side panel's API client (web/app/meet/sidepanel).
 *
 * Clerk OAuth only, with the Meet panel's OWN client
 * (NEXT_PUBLIC_MEET_ADDIN_CLERK_OAUTH_CLIENT_ID), so the backend classifies the
 * token as the Meet add-in and allows exactly its routes (shared workspace label
 * + the three Google Meet `captures` routes; routes::addin_microsoft::meet_scope_allows).
 * No Microsoft link, no pairing: unlike oauthClient.ts there is nothing to trade.
 *
 * Every meeting loads the panel as a fresh iframe document, so the TokenSet is
 * persisted in the panel origin's localStorage (Chrome partitions it by
 * (app.coralswarm.example, meet.google.com), so it lives across meetings on Meet and
 * nowhere else), keyed by the Meet client id so dev and prod never read each
 * other's record. The member signs in once per browser, not once per meeting;
 * sign out clears it. When storage is blocked the client is memory-only. */

import { DialogCancelledError, runAuthDialog, type PopupHandle } from "./authDialog";
import {
  SignedOutError, createClerkSession, exchangeCode, localStorageTokenStore, readClerkOAuthConfigFor, sessionFromTokens,
  type ClerkOAuthConfig, type OAuthDeps, type TokenSet,
} from "./clerkOAuth";

/** The workspace lookup is optional; it must never hold the sign-in hostage. */
const WORKSPACE_LOOKUP_TIMEOUT_MS = 8000;

export interface MeetWorkspace { account_id: string; name: string; kind: "personal" | "org" }
export interface MeetAccount { email: string; workspace: MeetWorkspace }

/** A non-2xx answer. `code` is the backend's `error` (or the scope layer's `code`). */
export class MeetApiError extends Error {
  status: number;
  code: string | null;
  constructor(status: number, code: string | null) { super(code ?? `HTTP ${status}`); this.name = "MeetApiError"; this.status = status; this.code = code; }
}

/** The Clerk credential is gone (refresh rejected, or never signed in): show the sign-in card. */
export class MeetSignedOutError extends Error { constructor() { super("Signed out"); this.name = "MeetSignedOutError"; } }

/** One `google_meet_captures` row as the API serializes it. */
export interface MeetCapture {
  id: string;
  conference_record_id: string | null;
  space_name: string | null;
  meeting_code: string | null;
  status: "allowed" | "ingested" | "revoked" | string;
  created_at: string;
  expires_at: string;
}

export interface MeetClientOptions {
  apiBase: () => string;
  /** Test seams. */
  config?: ClerkOAuthConfig | null;
  fetchImpl?: typeof fetch;
  now?: () => number;
  runDialog?: typeof runAuthDialog;
  /** Where the session persists; defaults to this origin's localStorage, null for memory-only. */
  storage?: Storage | null;
}

function isRecord(value: unknown): value is Record<string, unknown> { return typeof value === "object" && value !== null; }

function defaultStorage(): Storage | null {
  try { return globalThis.localStorage ?? null; } catch { return null; }
}

export function createMeetClient(options: MeetClientOptions) {
  const config = options.config === undefined ? readClerkOAuthConfigFor("meet") : options.config;
  const deps: OAuthDeps = { fetchImpl: options.fetchImpl, now: options.now };
  const doFetch: typeof fetch = options.fetchImpl ?? ((input, init) => fetch(input, init));
  const dialog = options.runDialog ?? runAuthDialog;
  const store = config ? localStorageTokenStore(`coralswarm.meet.session.${config.clientId}`, options.storage === undefined ? defaultStorage() : options.storage) : null;
  const clerk = config ? createClerkSession(config, deps, store) : null;
  const url = (path: string) => `${options.apiBase().replace(/\/$/, "")}${path}`;

  async function token(force = false): Promise<string> {
    if (!clerk) throw new MeetSignedOutError();
    try { return await clerk.accessToken({ force }); }
    catch (cause) { if (cause instanceof SignedOutError) throw new MeetSignedOutError(); throw cause; }
  }

  async function once<T>(bearer: string, path: string, init: { method?: string; body?: unknown; signal?: AbortSignal }): Promise<T> {
    const headers: Record<string, string> = { Authorization: `Bearer ${bearer}` };
    if (init.body !== undefined) headers["Content-Type"] = "application/json";
    const res = await doFetch(url(path), { method: init.method ?? "GET", headers, signal: init.signal, ...(init.body !== undefined ? { body: JSON.stringify(init.body) } : {}) });
    if (res.status === 204) return undefined as T;
    let body: unknown = null;
    try { body = await res.json(); } catch { /* not JSON */ }
    if (!res.ok) {
      const code = isRecord(body) && typeof body.error === "string" ? body.error : isRecord(body) && typeof body.code === "string" ? body.code : null;
      throw new MeetApiError(res.status, code);
    }
    return body as T;
  }

  /** One API call with the Clerk bearer; on a 401, refresh once (rotating) and retry once. */
  async function call<T>(path: string, init: { method?: string; body?: unknown; signal?: AbortSignal } = {}): Promise<T> {
    try { return await once<T>(await token(), path, init); }
    catch (cause) {
      if (!(cause instanceof MeetApiError) || cause.status !== 401) throw cause;
      try { return await once<T>(await token(true), path, init); }
      catch (retry) {
        if (retry instanceof MeetApiError && retry.status === 401) { clerk?.clear(); throw new MeetSignedOutError(); }
        throw retry;
      }
    }
  }

  /** GET /v1/addin/workspace; null on any failure except a lost sign-in. */
  async function fetchWorkspace(): Promise<MeetWorkspace | null> {
    const abort = new AbortController();
    let timer: ReturnType<typeof setTimeout> | undefined;
    const timeout = new Promise<null>((resolve) => { timer = setTimeout(() => { abort.abort(); resolve(null); }, WORKSPACE_LOOKUP_TIMEOUT_MS); });
    try {
      const body = await Promise.race([call<unknown>("/v1/addin/workspace", { signal: abort.signal }), timeout]);
      if (!isRecord(body)) return null;
      const { name, account_id: accountId, kind } = body;
      if (typeof name !== "string" || !name.trim() || typeof accountId !== "string" || !accountId) return null;
      if (kind !== "personal" && kind !== "org") return null;
      return { account_id: accountId, name, kind };
    } catch (cause) {
      if (cause instanceof MeetSignedOutError) throw cause;
      return null;
    } finally { clearTimeout(timer); }
  }

  /** The account the panel shows: email from the id token, the workspace from the API when it answers. */
  async function accountFor(tokens: TokenSet): Promise<MeetAccount> {
    const fromTokens = sessionFromTokens(tokens);
    const workspace = await fetchWorkspace();
    return { email: fromTokens.account.email, workspace: workspace ?? fromTokens.workspace as MeetWorkspace };
  }

  let signingIn: Promise<MeetAccount> | null = null;

  return {
    /** False when the build has no Meet client id (or the Clerk host cannot be derived). */
    enabled: config !== null,
    signedIn: () => clerk?.hasTokens() ?? false,

    /** The sign-in persisted from an earlier meeting, or null (never signed in, signed out, or the
     *  stored refresh token was rejected, which also signs the client out). */
    async resume(): Promise<MeetAccount | null> {
      const snapshot = clerk?.snapshot();
      if (!snapshot) return null;
      try { return await accountFor(snapshot); }
      catch (cause) { if (cause instanceof MeetSignedOutError) return null; throw cause; }
    },

    /** Run the popup sign-in. `popup` must be opened synchronously in the click (openSignInPopup). */
    signIn(popup: PopupHandle | null, signInOptions: { prompt?: "consent" } = {}): Promise<MeetAccount> {
      if (!config || !clerk) { try { popup?.close(); } catch { /* ignore */ } return Promise.reject(new MeetSignedOutError()); }
      if (signingIn) { try { popup?.close(); } catch { /* ignore */ } return signingIn; }
      const run = (async () => {
        const result = await dialog({ target: { kind: "meet", popup }, prompt: signInOptions.prompt });
        const tokens = await exchangeCode(config, { code: result.code, verifier: result.verifier, redirectUri: result.redirectUri }, deps);
        clerk.set(tokens);
        return accountFor(tokens);
      })().finally(() => { if (signingIn === run) signingIn = null; });
      signingIn = run;
      return run;
    },

    signOut() { clerk?.clear(); },

    listCaptures: (signal?: AbortSignal) => call<{ captures: MeetCapture[] }>("/v1/connectors/google/meet/captures", { signal }),
    addCapture: (space: string) => call<{ capture: MeetCapture; capture_state: string; created: boolean }>("/v1/connectors/google/meet/captures", { method: "POST", body: { space } }),
    removeCapture: (id: string) => call<unknown>(`/v1/connectors/google/meet/captures/${encodeURIComponent(id)}`, { method: "DELETE" }),
  };
}

export type MeetClient = ReturnType<typeof createMeetClient>;

export function isSignInCancelled(cause: unknown): boolean { return cause instanceof DialogCancelledError; }

/** The open capture (if any) that covers this meeting: an `allowed` row for this space or meeting code.
 *  Ingested rows are past occurrences of the same space and do not cover this one. */
export function captureForMeeting(captures: MeetCapture[], meeting: { meetingId: string; meetingCode?: string | null }): MeetCapture | null {
  const code = meeting.meetingCode?.trim().toLowerCase() || null;
  return captures.find((c) => c.status === "allowed" && (c.space_name === meeting.meetingId || (code !== null && c.meeting_code === code))) ?? null;
}

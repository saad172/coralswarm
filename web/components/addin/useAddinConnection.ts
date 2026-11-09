"use client";

/* The connection state machine shared by every add-in pane:
 *
 *   booting ──session()──▶ connected                (linked, or auto-linked)
 *      │                └▶ start ──Connect──▶ pairing ──approved──▶ connected
 *      ├─ 404 microsoft_sso_disabled / host SSO unavailable ──▶ fallback (Clerk)
 *      └─ 401 / network ──▶ error ──Try again──▶ booting
 *
 * With the Clerk OAuth rollout gate on (the client carries oauth controls, see
 * lib/addin/oauthClient.ts) the pairing leg is replaced: booting ──▶ connected
 * (returning user, silent) or ──▶ signin ──Sign in──▶ connected, and there is no
 * `fallback`/`pairing`. With the gate off none of that code runs.
 *
 * Mirrors the Gmail add-on (Card.gs currentPairingState_, Pairing.gs): a start
 * card, a pairing screen that polls on its own AND answers the "I've approved —
 * continue" button, then the connected surface. Credentials live in the client
 * (memory only); nothing here persists anything. */

import { useCallback, useEffect, useRef, useState } from "react";
import {
  InvalidMicrosoftTokenError, ReconnectRequiredError, backoffDelay, isTransientSessionFailure, SsoDisabledError, SsoUnavailableError,
  type AddinClient, type AddinHost, type ConnectedSession, type Pairing, type SessionResult,
} from "@/lib/addin/microsoftSso";
import { copy } from "@/lib/addin/copy";
import { SignInRequiredError, isSignInCancelled, oauthControlsFor } from "@/lib/addin/oauthClient";
import { mark } from "@/lib/addin/perf";

export type Connection =
  | { status: "booting" }
  | { status: "fallback"; reason: "disabled" | "unavailable" }
  | { status: "error"; message: string }
  | { status: "start"; pairing: Pairing | null; busy: boolean }
  | { status: "pairing"; pairing: Pairing; busy: boolean }
  /** Clerk OAuth gate on: the "Sign in to CoralSwarm" state. `notice` is why the last attempt did not finish. */
  | { status: "signin"; busy: boolean; notice: "cancelled" | "failed" | null }
  | { status: "connected"; session: ConnectedSession };

export const POLL_INTERVAL_MS = 3000;
/** Automatic boot retries after a transient failure (waits 1s, 2s, 4s), then the Retry button. */
export const MAX_BOOT_RETRIES = 3;

const PRODUCTION_APP_ORIGIN = "https://app.coralswarm.example";
/** Documented local-development origins; trusted only when they are this page's own origin. */
const DEV_ORIGINS = new Set(["http://localhost:3000", "https://localhost:3001"]);

/** The approval link comes from the API but is opened in the user's browser, and a
 *  fake approval page could harvest the pairing code (and support an approve race),
 *  so its ORIGIN is pinned: the production app, or this page's own origin when that
 *  is a local-dev origin or another `app.` host of ours (staging). Plus: no
 *  credentials, and the approve path. */
export function isSafeApprovalUrl(raw: string, pageOrigin: string): boolean {
  try {
    const url = new URL(raw);
    if (url.username || url.password) return false;
    if (url.pathname !== "/addin/approve") return false;
    if (url.origin === PRODUCTION_APP_ORIGIN) return true;
    if (url.origin !== pageOrigin) return false;
    return DEV_ORIGINS.has(url.origin) || (url.protocol === "https:" && url.hostname.startsWith("app."));
  } catch { return false; }
}

export function useAddinConnection({ client, host, toast, now = Date.now }: {
  client: AddinClient;
  host: Pick<AddinHost, "openLink">;
  toast: (message: string) => void;
  now?: () => number;
}) {
  const [state, setState] = useState<Connection>({ status: "booting" });
  const oauth = oauthControlsFor(client);
  const alive = useRef(true);
  const polling = useRef(false);
  const toastRef = useRef(toast); toastRef.current = toast;
  const stateRef = useRef(state); stateRef.current = state;
  const set = useCallback((next: Connection) => { if (alive.current) setState(next); }, []);

  const failure = useCallback((cause: unknown, fromBoot: boolean) => {
    if (cause instanceof SsoDisabledError) return set({ status: "fallback", reason: "disabled" });
    if (cause instanceof SsoUnavailableError) return set({ status: "fallback", reason: "unavailable" });
    if (fromBoot) return set({ status: "error", message: cause instanceof InvalidMicrosoftTokenError ? copy.invalidMicrosoft : copy.bootFailed });
    toastRef.current(copy.pairing.couldNotStart);
    const current = stateRef.current;
    if (current.status === "start" || current.status === "pairing") set({ status: "start", pairing: current.pairing, busy: false });
  }, [set]);

  const apply = useCallback((result: SessionResult, origin: "boot" | "connect" | "approved") => {
    if (result.kind === "connected") {
      const { kind: _kind, ...session } = result; void _kind;
      mark("connected");
      set({ status: "connected", session });
      if (origin !== "boot") toastRef.current(copy.connectedToast(session.account.email));
      return;
    }
    set(origin === "boot" ? { status: "start", pairing: result.pairing, busy: false } : { status: "pairing", pairing: result.pairing, busy: false });
  }, [set]);

  const bootRetries = useRef(0);
  const bootTimer = useRef<ReturnType<typeof setTimeout> | null>(null);
  const boot = useCallback(async (retry = false) => {
    if (bootTimer.current) { clearTimeout(bootTimer.current); bootTimer.current = null; }
    if (!retry) bootRetries.current = 0;
    set({ status: "booting" });
    try { apply(await client.session({ force: true }), "boot"); }
    catch (cause) {
      if (!alive.current) return;
      if (cause instanceof SignInRequiredError) { set({ status: "signin", busy: false, notice: null }); return; }
      if (isTransientSessionFailure(cause) && !(cause instanceof InvalidMicrosoftTokenError) && bootRetries.current < MAX_BOOT_RETRIES) {
        const wait = backoffDelay(bootRetries.current++);
        bootTimer.current = setTimeout(() => { bootTimer.current = null; void boot(true); }, wait);
        return;
      }
      failure(cause, true);
    }
  }, [client, apply, failure, set]);

  useEffect(() => {
    alive.current = true;
    void boot();
    return () => { alive.current = false; if (bootTimer.current) { clearTimeout(bootTimer.current); bootTimer.current = null; } };
  }, [boot]);

  /** Start card → pairing (or straight to connected when the backend auto-links). */
  const connect = useCallback(async () => {
    const current = stateRef.current;
    if (current.status !== "start" || current.busy) return;
    const stash = current.pairing;
    if (stash && stash.expiresAt > now()) { set({ status: "pairing", pairing: stash, busy: false }); return; }
    set({ status: "start", pairing: null, busy: true });
    try { apply(await client.session({ force: true }), "connect"); }
    catch (cause) { failure(cause, false); }
  }, [client, apply, failure, now, set]);

  const finishApproval = useCallback(async () => {
    try { apply(await client.session(), "approved"); }
    catch (cause) { failure(cause, false); }
  }, [client, apply, failure]);

  const checkOnce = useCallback(async (manual: boolean) => {
    const current = stateRef.current;
    if (current.status !== "pairing" || polling.current) return;
    polling.current = true;
    const pairing = current.pairing;
    try {
      const expiredLocally = pairing.expiresAt <= now();
      const outcome = expiredLocally ? "expired" : await client.poll(pairing);
      // The screen may have moved on while the poll was in flight.
      const latest = stateRef.current;
      if (!alive.current || latest.status !== "pairing" || latest.pairing.code !== pairing.code) return;
      if (outcome === "approved") await finishApproval();
      else if (outcome === "expired") { set({ status: "start", pairing: null, busy: false }); toastRef.current(copy.pairing.expired); }
      else if (manual) toastRef.current(copy.pairing.notApproved);
    } finally { polling.current = false; }
  }, [client, finishApproval, now, set]);

  const pairingCode = state.status === "pairing" ? state.pairing.code : null;
  useEffect(() => {
    if (!pairingCode) return;
    const timer = setInterval(() => { void checkOnce(false); }, POLL_INTERVAL_MS);
    return () => clearInterval(timer);
  }, [pairingCode, checkOnce]);

  /** "Sign in to CoralSwarm": the host dialog, then a session. Runs from the button's click. */
  const signIn = useCallback(async () => {
    const current = stateRef.current;
    if (!oauth || current.status !== "signin" || current.busy) return;
    set({ status: "signin", busy: true, notice: null });
    try {
      const session = await oauth.signIn();
      apply({ kind: "connected", ...session }, "connect");
    } catch (cause) {
      set({ status: "signin", busy: false, notice: isSignInCancelled(cause) ? "cancelled" : "failed" });
    }
  }, [oauth, apply, set]);

  /** Change workspace with the gate on: the dialog again, with the consent screen (and its org picker). */
  const reselectWorkspace = useCallback(async () => {
    if (!oauth) return false;
    toastRef.current(copy.signIn.changeHint);
    try {
      const session = await oauth.signIn({ prompt: "consent" });
      set({ status: "connected", session });
      toastRef.current(copy.workspace.changed(session.workspace.name));
      return true;
    } catch (cause) {
      toastRef.current(isSignInCancelled(cause) ? copy.signIn.cancelled : copy.workspace.changeFailed);
      return false;
    }
  }, [oauth, set]);

  const continuePairing = useCallback(() => checkOnce(true), [checkOnce]);
  const restart = useCallback(() => set({ status: "start", pairing: null, busy: false }), [set]);

  const openApproval = useCallback(async () => {
    const current = stateRef.current;
    if (current.status !== "pairing") return;
    const origin = typeof window === "undefined" ? "" : window.location.origin;
    if (!isSafeApprovalUrl(current.pairing.approveUrl, origin)) { toastRef.current(copy.pairing.couldNotStart); return; }
    try { await host.openLink(current.pairing.approveUrl); } catch { toastRef.current(copy.pairing.couldNotStart); }
  }, [host]);

  const disconnect = useCallback(async () => {
    const { revoked } = await client.disconnect();
    if (oauth) { set({ status: "signin", busy: false, notice: null }); toastRef.current(copy.signIn.signedOut); return; }
    set({ status: "start", pairing: null, busy: false });
    toastRef.current(revoked ? copy.disconnect.revoked : copy.disconnect.notRevoked);
  }, [client, oauth, set]);

  /** Any child call that learns the link is gone sends the pane back to the start card. */
  const reconnectRequired = useCallback(() => {
    client.forget();
    set(oauth ? { status: "signin", busy: false, notice: null } : { status: "start", pairing: null, busy: false });
    toastRef.current(copy.reconnect);
  }, [client, oauth, set]);

  const changeWorkspace = useCallback(async (accountId: string) => {
    try {
      const session = await client.changeWorkspace(accountId);
      set({ status: "connected", session });
      toastRef.current(copy.workspace.changed(session.workspace.name));
      return true;
    } catch (cause) {
      if (cause instanceof ReconnectRequiredError) reconnectRequired();
      else toastRef.current(copy.workspace.changeFailed);
      return false;
    }
  }, [client, reconnectRequired, set]);

  return { state, oauthEnabled: oauth !== null, signIn, reselectWorkspace, connect, continuePairing, restart, openApproval, disconnect, changeWorkspace, reconnectRequired, retry: () => boot() };
}

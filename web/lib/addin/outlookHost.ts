/* Outlook host adapter: the Microsoft token comes from nested app auth (MSAL
 * `createNestablePublicClientApplication`, scope .../access_as_user), with
 * `Office.auth.getAccessToken` as the fallback when NAA is not available in the
 * client. If neither yields a token the host reports SSO unavailable and the
 * pane falls back to the existing Clerk flow.
 *
 * Requires (Azure) the SPA redirect URI `brk-multihub://app.coralswarm.example` on
 * the app registration for NAA, and the manifest's WebApplicationInfo for the
 * Office.auth fallback. */

import { MICROSOFT_APP_ID, MICROSOFT_AUTHORITY, MICROSOFT_SSO_SCOPE } from "./config";
import { loadOffice, openInBrowser, type OfficeApi } from "./office";
import { SsoUnavailableError, type AddinHost } from "./microsoftSso";
import { mark } from "./perf";

/** The two MSAL calls we use, so tests can stand in for the library. */
export interface NestedClient {
  acquireTokenSilent(request: { scopes: string[] }): Promise<{ accessToken: string }>;
  acquireTokenPopup(request: { scopes: string[] }): Promise<{ accessToken: string }>;
}

export interface OutlookHostDeps {
  office?: () => Promise<OfficeApi>;
  createNested?: () => Promise<NestedClient>;
  /** Fetches the MSAL chunk. Separate from `createNested` so the download can overlap the office.js load. */
  loadMsal?: () => Promise<unknown>;
}

type MsalModule = typeof import("@azure/msal-browser");
let msalLoad: Promise<MsalModule> | null = null;
/** Imported lazily: MSAL is only paid for by panes that actually run in Outlook. One shared import. */
function importMsal(): Promise<MsalModule> {
  msalLoad ??= import("@azure/msal-browser");
  msalLoad.catch(() => { msalLoad = null; });
  return msalLoad;
}

async function defaultCreateNested(): Promise<NestedClient> {
  const { createNestablePublicClientApplication } = await importMsal();
  return createNestablePublicClientApplication({ auth: { clientId: MICROSOFT_APP_ID, authority: MICROSOFT_AUTHORITY } });
}

function isInteractionRequired(cause: unknown): boolean {
  const code = typeof cause === "object" && cause !== null ? (cause as { errorCode?: unknown }).errorCode : undefined;
  return code === "interaction_required" || code === "consent_required" || code === "login_required" || code === "no_account_error";
}

export function createOutlookHost(deps: OutlookHostDeps = {}): AddinHost & { office(): Promise<OfficeApi>; warm(): Promise<void> } {
  const getOffice = deps.office ?? loadOffice;
  const createNested = deps.createNested ?? defaultCreateNested;
  // An injected `createNested` replaces MSAL entirely, so there is nothing to download.
  const loadMsal = deps.loadMsal ?? (deps.createNested ? async () => undefined : () => importMsal().then((m) => { mark("msal-loaded"); return m; }));
  let nested: Promise<NestedClient> | null = null;
  const scopes = [MICROSOFT_SSO_SCOPE];

  function startNested(): Promise<NestedClient> {
    nested ??= createNested().then((client) => { mark("nested-ready"); return client; });
    return nested;
  }

  /** Start everything the token needs, concurrently, without waiting for it: the office.js load, the MSAL
   *  chunk download and (as soon as office.js says NAA is available) the MSAL client's own startup. The
   *  panes call this the moment they render, so by the time the connection state machine asks for a
   *  token the slow, independent steps have been running in parallel instead of one after the other.
   *  Idempotent, never rejects: a failure surfaces from getMicrosoftToken as before. */
  let warming: Promise<void> | null = null;
  function warm(): Promise<void> {
    warming ??= (async () => {
      const officeReady = getOffice();
      const msalReady = loadMsal();
      msalReady.catch(() => {});
      const [office] = await Promise.allSettled([officeReady, msalReady]);
      if (office.status !== "fulfilled" || !office.value.context.requirements.isSetSupported("NestedAppAuth", "1.1")) return;
      startNested().catch(() => {});
    })().catch(() => {});
    return warming;
  }

  async function viaNaa(office: OfficeApi): Promise<string | null> {
    if (!office.context.requirements.isSetSupported("NestedAppAuth", "1.1")) return null;
    try {
      const client = await startNested();
      try { const token = (await client.acquireTokenSilent({ scopes })).accessToken || null; if (token) mark("token-silent"); return token; }
      catch (cause) {
        if (!isInteractionRequired(cause)) throw cause;
        return (await client.acquireTokenPopup({ scopes })).accessToken || null;
      }
    } catch { nested = null; return null; }
  }

  async function viaOfficeAuth(office: OfficeApi): Promise<string | null> {
    if (!office.auth?.getAccessToken) return null;
    try {
      const token = (await office.auth.getAccessToken({ allowSignInPrompt: true, allowConsentPrompt: true, forMSGraphAccess: false })) || null;
      if (token) mark("token-fallback");
      return token;
    }
    catch { return null; }
  }

  return {
    name: "outlook",
    office: getOffice,
    warm,
    async getMicrosoftToken() {
      void warm();
      let office: OfficeApi;
      try { office = await getOffice(); } catch (cause) { throw new SsoUnavailableError(cause instanceof Error ? cause.message : undefined); }
      const token = (await viaNaa(office)) ?? (await viaOfficeAuth(office));
      if (!token) throw new SsoUnavailableError("No Microsoft token from nested app auth or Office SSO");
      return token;
    },
    async openLink(url) { await openInBrowser(await getOffice().catch(() => null), url); },
  };
}

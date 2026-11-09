"use client";

/* The page the Clerk authorize redirect lands on, inside the host's sign-in
 * dialog. Its only job is to hand `{code, state}` (or an error) back to the pane
 * that opened the dialog, then say it is done. It never navigates, never stores
 * anything, never shows the code, and never acts on a URL parameter beyond
 * answering the opener:
 *   - Outlook: Office.context.ui.messageParent, targetOrigin = this origin;
 *   - Teams:   authentication.notifySuccess / notifyFailure;
 *   - Meet:    window.opener.postMessage, targetOrigin = this origin, then
 *              window.close() (the side panel opened this popup itself).
 * The host is read from the state's first character (clerkOAuth.createState) because
 * that is the only value Clerk round-trips for us. */

import { useEffect, useState } from "react";
import { hostFromState, isValidState } from "@/lib/addin/clerkOAuth";
import { loadOffice, type OfficeApi } from "@/lib/addin/office";

export interface TeamsSdk {
  app: { initialize(): Promise<void> };
  authentication: { notifySuccess(result?: string): void; notifyFailure(reason?: string): void };
}

export interface CallbackDeps {
  search: string;
  origin: string;
  /** Drop the query from the address bar. May throw: Office.js removes history.replaceState. */
  scrub: () => void;
  loadOffice: () => Promise<OfficeApi>;
  loadTeams: () => Promise<TeamsSdk>;
  /** The window that opened this popup (the Meet side panel), or null. */
  opener?: () => Pick<Window, "postMessage"> | null;
  /** Close this popup once the opener has its answer. */
  closeSelf?: () => void;
}

export type CallbackOutcome = "sent" | "invalid" | "failed";

export async function deliverAuthResult(deps: CallbackDeps): Promise<CallbackOutcome> {
  const params = new URLSearchParams(deps.search);
  const code = params.get("code");
  const state = params.get("state") ?? "";
  const error = params.get("error");
  // Scrub first: the code must not linger in the address bar or history. Office.js nulls
  // replaceState/pushState in add-in pages, so a throw here is expected and harmless.
  try { deps.scrub(); } catch { /* unavailable in Office add-in pages */ }

  const host = hostFromState(state);
  if (!host || !isValidState(state) || (!code && !error)) return "invalid";
  const payload = JSON.stringify(error ? { error, state } : { code, state });

  try {
    if (host === "meet") {
      const opener = deps.opener?.() ?? null;
      if (!opener) return "failed";
      opener.postMessage(error ? { error, state } : { code, state }, deps.origin);
      try { deps.closeSelf?.(); } catch { /* the browser may refuse; the page then says it is done */ }
      return "sent";
    }
    if (host === "outlook") {
      const office = await deps.loadOffice();
      const ui = office.context.ui;
      if (!ui?.messageParent) return "failed";
      try { ui.messageParent(payload, { targetOrigin: deps.origin }); }
      // Older Office clients reject the options argument. The code is bound to the pane's PKCE verifier, so it is useless without it.
      catch { ui.messageParent(payload); }
      return "sent";
    }
    const { app, authentication } = await deps.loadTeams();
    await app.initialize();
    if (error) authentication.notifyFailure(error);
    else authentication.notifySuccess(payload);
    return "sent";
  } catch { return "failed"; }
}

export function AuthCallback() {
  const [outcome, setOutcome] = useState<CallbackOutcome | "working">("working");
  useEffect(() => {
    let live = true;
    void deliverAuthResult({
      search: window.location.search,
      origin: window.location.origin,
      scrub: () => window.history.replaceState(null, "", window.location.pathname),
      loadOffice,
      loadTeams: async () => import("@microsoft/teams-js"),
      opener: () => (window.opener as Window | null) ?? null,
      closeSelf: () => window.close(),
    }).then((result) => { if (live) setOutcome(result); });
    return () => { live = false; };
  }, []);
  return (
    <main className="mx-auto max-w-sm p-6 text-center">
      <p role={outcome === "working" || outcome === "sent" ? "status" : "alert"}>
        {outcome === "working" ? "Finishing sign-in…" : outcome === "sent" ? "Signed in. You can close this window." : "Sign-in could not finish. Close this window and try again."}
      </p>
    </main>
  );
}

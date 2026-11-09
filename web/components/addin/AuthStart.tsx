"use client";

/* The page the host dialog opens first. It validates its query and sends the
 * browser to Clerk's authorize endpoint, built from constants (client id,
 * redirect URI, scopes) plus the pane's PKCE challenge and state. It cannot be
 * made to redirect anywhere else: unexpected input shows an error instead.
 * The client id is the host's own (parseStartParams checks host == state marker). */

import { useEffect, useState } from "react";
import { buildAuthorizeUrl, parseStartParams, readClerkOAuthConfigFor } from "@/lib/addin/clerkOAuth";

export function AuthStart() {
  const [failed, setFailed] = useState(false);
  useEffect(() => {
    const params = parseStartParams(window.location.search);
    // Each host signs in with its own Clerk client (Meet has its own app); the host is the state's marker.
    const config = params ? readClerkOAuthConfigFor(params.host) : null;
    if (!config || !params) { setFailed(true); return; }
    window.location.replace(buildAuthorizeUrl(config, window.location.origin, { cc: params.cc, state: params.state, prompt: params.prompt }));
  }, []);
  return (
    <main className="mx-auto max-w-sm p-6 text-center">
      {failed ? <p role="alert">Sign-in could not start. Close this window and try again.</p> : <p role="status">Opening sign-in…</p>}
    </main>
  );
}

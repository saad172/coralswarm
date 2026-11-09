// Preserve stateful deep links through the app layout → login redirect chain.
// This includes Gmail add-on pairing codes and signed Teams onboarding tickets.
// The app layout saves the complete local path/query before redirecting; login
// consumes it once after Clerk sign-in.
const KEY = "cs_post_signin_redirect";

/** Remember where to go after signing in. Call this BEFORE navigating away
 *  from a page whose own URL carries state worth returning to. */
export function setPostSignInRedirect(path: string): void {
  try {
    window.sessionStorage.setItem(KEY, path);
  } catch {
    /* sessionStorage unavailable (private mode, SSR) — the flow still works,
       it just lands on the default post-sign-in page instead. */
  }
}

/** Read and CLEAR the stored redirect — one-shot, so a later ordinary sign-in
 *  never resurrects a stale destination. `null` is the normal case for every
 *  sign-in that didn't come through `setPostSignInRedirect`. */
export function consumePostSignInRedirect(): string | null {
  try {
    const value = window.sessionStorage.getItem(KEY);
    if (value) window.sessionStorage.removeItem(KEY);
    return value;
  } catch {
    return null;
  }
}

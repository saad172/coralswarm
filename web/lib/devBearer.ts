/** Loopback-only bearer for local extract/JWKS verification.

   Off unless the page is on localhost/127.0.0.1 AND NEXT_PUBLIC_API_BASE
   points at a loopback host. Production builds (api.coralswarm.example) never
   read the key. Set `localStorage.cs_dev_token` then reload. */

const LOOPBACK = /^(127\.0\.0\.1|localhost)$/i;

function apiIsLoopback(apiBase: string): boolean {
  if (!apiBase) return false;
  try {
    const u = new URL(apiBase, "http://127.0.0.1");
    return LOOPBACK.test(u.hostname);
  } catch {
    return false;
  }
}

/** Page host and API base are both loopback. Use before showing the
 *  paste form or writing `cs_dev_token`. */
export function devBearerAllowed(): boolean {
  if (typeof window === "undefined") return false;
  return LOOPBACK.test(window.location.hostname) && apiIsLoopback(process.env.NEXT_PUBLIC_API_BASE ?? "");
}

export function readDevBearer(): string | null {
  if (!devBearerAllowed()) return null;
  try {
    const t = window.localStorage.getItem("cs_dev_token");
    return t && t.trim() ? t.trim() : null;
  } catch {
    return null;
  }
}

export function writeDevBearer(token: string | null): void {
  if (!devBearerAllowed()) return;
  if (token && token.trim()) window.localStorage.setItem("cs_dev_token", token.trim());
  else window.localStorage.removeItem("cs_dev_token");
}

/* Transports for the Outlook email pane. Both resolve `{status, ok, data}` for
 * every HTTP status and only reject on a network-level failure, so the pane
 * branches on status codes rather than exceptions.
 *
 *  - SSO: the add-in session (client.call re-trades silently on 401; a lost link
 *    surfaces as `unauthenticated`).
 *  - Clerk fallback: a fresh Clerk JWT minted per request (Clerk tokens live ~60 s). */

import { AddinHttpError, ReconnectRequiredError, type AddinClient } from "./microsoftSso";

export interface ApiResult { status: number; ok: boolean; data: unknown; unauthenticated?: boolean }
export type EmailApi = (method: "GET" | "POST" | "PUT", path: string, body?: unknown, timeoutMs?: number) => Promise<ApiResult>;

function withTimeout(timeoutMs: number | undefined): { signal?: AbortSignal; done: () => void } {
  if (!timeoutMs) return { done: () => {} };
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  return { signal: controller.signal, done: () => clearTimeout(timer) };
}

export function ssoEmailApi(client: AddinClient): EmailApi {
  return async (method, path, body, timeoutMs) => {
    const guard = withTimeout(timeoutMs);
    try {
      const data = await client.call<unknown>(path, { method, body, signal: guard.signal });
      return { status: 200, ok: true, data };
    } catch (cause) {
      if (cause instanceof ReconnectRequiredError) return { status: 0, ok: false, data: null, unauthenticated: true };
      if (cause instanceof AddinHttpError) return { status: cause.status, ok: false, data: null };
      throw cause;
    } finally { guard.done(); }
  };
}

export function bearerEmailApi(getToken: () => Promise<string | null>, apiBase: () => string, fetchImpl: typeof fetch = (input, init) => fetch(input, init)): EmailApi {
  return async (method, path, body, timeoutMs) => {
    const guard = withTimeout(timeoutMs);
    try {
      const token = await getToken();
      if (!token) return { status: 0, ok: false, data: null, unauthenticated: true };
      const headers: Record<string, string> = { Authorization: `Bearer ${token}` };
      if (body !== undefined) headers["Content-Type"] = "application/json";
      const res = await fetchImpl(`${apiBase().replace(/\/$/, "")}${path}`, { method, headers, signal: guard.signal, ...(body !== undefined ? { body: JSON.stringify(body) } : {}) });
      let data: unknown = null;
      try { data = await res.json(); } catch { data = null; }
      return { status: res.status, ok: res.ok, data };
    } finally { guard.done(); }
  };
}

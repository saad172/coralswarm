/** A Composio toolkit slug with a backend capture recipe (e.g. "linear"). */
export type ComposioToolkit = string;
export type CaptureState = "active" | "retry" | "needs_attention" | "reconnect_needed" | "disconnected";
export type ConnectionStatus = "pending" | "active" | "needs_attention" | "reconnect_needed" | "disconnected";
/** One connection's workspace-wide capture in the member's own ocean. */
export type ComposioCapture = {
  id: string; graph_id: string; connection_id: string; service: ComposioToolkit; provider_namespace: string;
  state: CaptureState; captured_items: number; watermark: string; pass_started_at: string | null;
  next_poll_at: string; last_change_at: string | null; last_capture_at: string | null;
  retry_at: string | null; error_code: string | null; created_at: string;
};
export type ComposioConnection = {
  id: string; status: ConnectionStatus; provider_namespace: string | null;
  self_check: "pending" | "passed" | "failed"; created_at: string;
  /** Plain-language explanation for every non-active status, from the backend. */
  message: string | null;
  capture: ComposioCapture | null;
};
export type ComposioStatus = {
  service: ComposioToolkit; label: string; toolkit_version: string; kinds: string[];
  configured: boolean; polling_enabled: boolean; beta: boolean;
  connections: ComposioConnection[]; limits: Record<string, string>;
};
export type ComposioToolkitSummary = { toolkit: ComposioToolkit; label: string; kinds: string[]; beta: boolean };
/** Connectors-index entry; the generic page serves every registered toolkit. */
export function toolkitLink(t: ComposioToolkitSummary): { href: string; text: string } {
  return {
    href: `/connectors/composio?toolkit=${encodeURIComponent(t.toolkit)}`,
    text: `${t.label} capture${t.beta ? " · Beta" : ""}`,
  };
}
/** Connections still waiting for the member to finish signing in. */
export function pendingConnections(status: ComposioStatus): ComposioConnection[] {
  return status.connections.filter(c => c.status === "pending");
}
/** A sign-in that expired or was abandoned before it reached a workspace. */
export function abandonedSignIn(c: ComposioConnection): boolean {
  return c.status === "reconnect_needed" && c.provider_namespace === null;
}
/** Connections worth listing; abandoned sign-ins are noise, not history. */
export function listedConnections(status: ComposioStatus): ComposioConnection[] {
  return status.connections.filter(c => !abandonedSignIn(c));
}
/** The member's most recent attempt ended without signing in. */
export function lastSignInExpired(status: ComposioStatus): ComposioConnection | null {
  const last = status.connections[status.connections.length - 1];
  return last && abandonedSignIn(last) ? last : null;
}
/** Whether a failed verify means "keep waiting". Only Composio still waiting
 *  for sign-in (409 `sign_in_pending`) or a transient failure (network, 429,
 *  5xx) does; an expired session, missing or terminal connection, or a
 *  rejected grant never finishes by asking again. */
export function keepWaiting(error: unknown): boolean {
  const { status, code } = (error ?? {}) as { status?: unknown; code?: unknown };
  if (typeof status !== "number") return true;
  if (status === 409) return code === "sign_in_pending";
  return status === 0 || status === 429 || status >= 500;
}
/** Connections the member can still act on; disconnected ones are history. */
export function liveConnections(status: ComposioStatus): ComposioConnection[] {
  return status.connections.filter(c => c.status !== "disconnected");
}
export function canDisconnect(c: ComposioConnection): boolean {
  return c.status !== "disconnected";
}
export function needsReconnect(c: ComposioConnection): boolean {
  return c.status === "reconnect_needed";
}
/** One line describing what capture is doing for an active connection. */
export function captureSummary(c: ComposioConnection, now = Date.now()): string {
  if (c.message) return c.message;
  const capture = c.capture;
  if (!capture) return "Starting capture…";
  if (capture.state === "retry") return "The last check hit a temporary problem; CoralSwarm will try again shortly.";
  const since = `Capturing everything that changes since ${new Date(capture.created_at).toLocaleDateString()}`;
  const count = capture.captured_items === 1 ? "1 item captured" : `${capture.captured_items} items captured`;
  const next = Date.parse(capture.next_poll_at) - now;
  const when = next <= 60_000 ? "Checking for changes now" : `Next check in ${Math.round(next / 60_000)} min`;
  return `${since} · ${count} · ${when}`;
}
/** Placeholder label until the status (and its recipe label) has loaded. */
export function toolkitFallbackLabel(toolkit: ComposioToolkit): string {
  return toolkit.split(/[_-]/).filter(Boolean).map(w => w[0].toUpperCase() + w.slice(1)).join(" ");
}
/** Recipe content kinds are singular slugs ("issue"); copy uses their plural. */
export function kindPlural(kind: string): string {
  const noun = kind.replace(/_/g, " ");
  return /(s|x|ch|sh)$/.test(noun) ? `${noun}es` : /[^aeiou]y$/.test(noun) ? `${noun.slice(0, -1)}ies` : `${noun}s`;
}
/** "issues, comments and projects". */
export function kindList(kinds: string[]): string {
  const plural = kinds.map(kindPlural);
  return plural.length <= 1 ? (plural[0] ?? "items") : `${plural.slice(0, -1).join(", ")} and ${plural[plural.length - 1]}`;
}
/** Toolkit slug from `?toolkit=`; only recipe-shaped slugs reach the API path. */
export function parseToolkit(raw: string | null): ComposioToolkit | null {
  return raw && /^[a-z0-9_]{1,64}$/.test(raw) ? raw : null;
}
export function limitEntries(limits: ComposioStatus["limits"]): [string, string][] {
  return Object.entries(limits).filter((e): e is [string, string] => typeof e[1] === "string").map(([k, v]) => [k.replace(/_/g, " "), v]);
}

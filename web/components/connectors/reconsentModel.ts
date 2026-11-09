import type { OAuthConnection } from "@/lib/api";

/* Re-consent prompt derivation — CS-431 (org-onboarding-from-the-invite.md
 * §3 item 34).
 *
 * Two grants can go silently stale forever: a Slack workspace connected
 * before `users:read.email` shipped (every member reads as `Unavailable`,
 * with nothing telling anyone why), and a Google Chat connection made
 * before `directory.readonly` shipped (the directory read is refused, same
 * silent `Unavailable`). The backend already refuses to guess —
 * `missing_scopes`/`scopes_unknown` on `OAuthConnection` are computed only
 * against a stored scope string the backend can actually trust (see
 * `routes::oauth::scope_reconsent_status`, `backend/src/routes/oauth.rs`).
 * This module is the ONE place the two cards (`ProviderTile` for Slack,
 * `GoogleChatConnector` for Chat) turn that payload into what to show —
 * same "shared derivation, not two that could drift" discipline
 * `slackOnboardingModel.ts`'s own doc comment describes. */

/** What each currently-required scope unlocks, in one sentence — keyed by
 *  the exact scope string the backend names (Slack's bare `users:read.email`,
 *  Google's fully-qualified scope URL). Any OTHER scope this codebase starts
 *  requiring later still renders (see `scopeDescription`'s fallback) rather
 *  than being silently dropped from the prompt; naming it here properly is
 *  the intended fix, not a requirement to ship one. */
/** Google's Workspace-directory scope — D16's rung 1, and the one scope
 *  two different surfaces need to name. Exported (and used as a key below,
 *  so the two can never disagree) for `connectorHealthModel`'s CS-448
 *  attribution, which describes the same scope from the org's side rather
 *  than from one member's. */
export const DIRECTORY_READONLY_SCOPE = "https://www.googleapis.com/auth/directory.readonly";

const SCOPE_COPY: Record<string, string> = {
  "users:read.email": "needed to match colleagues by email and invite the unmatched",
  [DIRECTORY_READONLY_SCOPE]:
    "needed to look up your Workspace directory so Chat can match colleagues by email",
};

/** One sentence naming what a missing scope unlocks. Never guesses AT the
 *  scope's purpose for one this module doesn't recognise — it says plainly
 *  that reconnecting is what restores full functionality, rather than
 *  inventing a specific claim it cannot back. */
export function scopeDescription(scope: string): string {
  return SCOPE_COPY[scope] ?? "needed for this connection to work fully";
}

/** Human-readable label for one scope. Slack's scopes are already short,
 *  bare identifiers (`users:read.email`) and render as-is; Google's are a
 *  full scope URL, so only the last path segment is shown
 *  (`directory.readonly`, not the whole `https://www.googleapis.com/auth/…`
 *  string) — the same segment [`SCOPE_COPY`]'s Google key ends with, so the
 *  two always agree on what a reader sees named. */
export function scopeLabel(scope: string): string {
  if (scope.startsWith("https://") || scope.startsWith("http://")) {
    const segments = scope.split("/").filter(Boolean);
    return segments[segments.length - 1] ?? scope;
  }
  return scope;
}

export interface ReconsentNotice {
  /** Every scope currently missing from the stored grant, in the order the
   *  backend returned them. Never empty — see [`reconsentNotice`]. */
  scopes: string[];
}

/** Whether — and what — a re-consent notice should say for one connection.
 *
 *  `null` means render nothing, for any of three reasons, and the caller
 *  does not need to (and must not try to) tell them apart:
 *    - not connected (`connection` is `null`) — nothing to reconnect;
 *    - `scopes_unknown` — the backend could not trust the stored grant
 *      enough to compute a diff at all (never guess);
 *    - `missing_scopes` is empty — the stored grant already covers
 *      everything this codebase currently requests.
 *
 *  A non-null result's `scopes` is always non-empty — a card can render
 *  "if truthy" without a separate length check. */
export function reconsentNotice(connection: OAuthConnection | null): ReconsentNotice | null {
  if (!connection) return null;
  if (connection.scopes_unknown) return null;
  if (connection.missing_scopes.length === 0) return null;
  return { scopes: connection.missing_scopes };
}

/** Whether a sync pass recorded this connection's credential as revoked
 *  (`credential_invalid_at`). Takes precedence over a missing-scope notice:
 *  a dead credential is the whole problem, and the same Reconnect fixes
 *  both. */
export function credentialRevoked(connection: OAuthConnection | null): boolean {
  return Boolean(connection?.credential_invalid_at);
}

/** The connection a tile shows (status, reconnect notice) for `provider`:
 *  the FIRST listed row. `GET /v1/oauth/connections` lists each provider's
 *  rows in the order a sync loads a token — the caller's own row before an
 *  account-wide one, newest first — so the first row is the credential
 *  actually in use. A legacy account-wide row beside the caller's own, or a
 *  revoked row a reconnect under a different account left behind, must not
 *  decide the notice. Every tile goes through this one helper so the
 *  "first row" contract lives in one place. */
export function connectionForProvider(
  connections: readonly OAuthConnection[],
  provider: string,
): OAuthConnection | null {
  return connections.find((c) => c.provider === provider) ?? null;
}

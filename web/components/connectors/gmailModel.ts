import type { OAuthConnection } from "@/lib/api";

/* Pure decisions behind the "Gmail" tile on /connectors.
 *
 * Gmail's OAuth row is stored under the DISTINCT literal `"google_gmail"`
 * (`Surface::Gmail.token_provider()`, backend `connectors/google.rs`) — not
 * `"google"`, which is Calendar's own row, and not `"google_chat"`. The
 * OAuth callback nevertheless redirects to `/connectors?connected=google`
 * for every Google surface, so that param is only a hint to refetch. */

export const GMAIL_PROVIDER = "google_gmail";
export const GMAIL_SURFACE = "gmail";
export const GMAIL_READONLY_SCOPE = "gmail.readonly";

/** The member's Gmail connection, or null. Requires the `gmail.readonly`
 *  scope so a stale/partial grant never renders as connected. Matches on the
 *  scope token's suffix, so both the bare and the full
 *  `https://www.googleapis.com/auth/gmail.readonly` forms qualify. */
export function gmailConnectionFrom(connections: OAuthConnection[]): OAuthConnection | null {
  return (
    connections.find(
      (c) =>
        c.provider === GMAIL_PROVIDER &&
        (c.scope ?? "")
          .split(/[,\s]+/)
          .filter(Boolean)
          .some((s) => s === GMAIL_READONLY_SCOPE || s.endsWith(`/${GMAIL_READONLY_SCOPE}`)),
    ) ?? null
  );
}

/** Friendly text for a failed connect. The backend refuses an org workspace
 *  whose Gmail flag is off with a 403 BEFORE any redirect — at `mint_state`
 *  with the generic "personal workspace only" body, or at `authorize` with
 *  "the gmail surface is not enabled for this workspace yet". */
export function gmailConnectError(status: number | null, message: string | null): string {
  if (status === 403) {
    return "Gmail isn't enabled for this workspace yet. Ask your workspace admin to turn it on, or switch to your personal workspace to connect Gmail.";
  }
  return message || "Couldn't start the Gmail connection.";
}

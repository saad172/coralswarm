import type { OAuthConnection } from "@/lib/api";
import type { Hue } from "@/components/ui";

/* Pure decisions behind the "Google Meet" tile on /connectors.
 *
 * Meet's OAuth row is stored under the DISTINCT literal `"google_meet"`
 * (`Surface::Meet.token_provider()`, backend `connectors/google.rs`) - not
 * `"google"` (Calendar) and not `"google_gmail"` / `"google_chat"`. The
 * OAuth callback redirects to `/connectors?connected=google` for every Google
 * surface, so that param is only a hint to refetch.
 *
 * TODO: fold into a generic GoogleSurfaceConnector once the Gmail tile
 * (#1134) has merged; the two are intentionally parallel until then. */

export const MEET_PROVIDER = "google_meet";
export const MEET_SURFACE = "meet";
export const MEET_READONLY_SCOPE = "meetings.space.readonly";

/** Tile metadata, local here (not in PROVIDER_META) so this PR does not
 *  collide with the Gmail tile's edits to that table. */
export const MEET_META: { label: string; description: string; hue: Hue; letter: string } = {
  label: "Google Meet",
  description:
    "Saves transcripts only from meetings you approve — in the CoralSwarm card on a Google Calendar event, or in Google Meet. Needs Meet transcripts turned on.",
  hue: "teal",
  letter: "M",
};

/** The member's Meet connection, or null. Requires the
 *  `meetings.space.readonly` scope so a stale/partial grant never renders as
 *  connected. Matches on the scope token's suffix so both the bare and the
 *  full `https://www.googleapis.com/auth/meetings.space.readonly` forms
 *  qualify. */
export function meetConnectionFrom(
  connections: OAuthConnection[],
  connectionsAccountId: string | null = null,
  accountId: string | null = null,
): OAuthConnection | null {
  // After an account switch the list can still hold the outgoing account's
  // rows for a render; never surface them. A null tag (first load) is trusted,
  // matching `connectionFor` / FollowedThreads on the page.
  if (connectionsAccountId !== null && connectionsAccountId !== accountId) return null;
  return (
    connections.find(
      (c) =>
        c.provider === MEET_PROVIDER &&
        (c.scope ?? "")
          .split(/[,\s]+/)
          .filter(Boolean)
          .some((s) => s === MEET_READONLY_SCOPE || s.endsWith(`/${MEET_READONLY_SCOPE}`)),
    ) ?? null
  );
}

/** Friendly text for a failed connect. The backend refuses an org workspace
 *  whose Meet flag is off (today: all org workspaces) with a 403 BEFORE any
 *  redirect - at `mint_state` ("personal workspace only") or at `authorize`
 *  via `refuse_doomed_org_authorize_redirect`. */
export function meetConnectError(status: number | null, message: string | null): string {
  if (status === 403) {
    return "Google Meet isn't enabled for this workspace yet. Ask your workspace admin to turn it on, or switch to your personal workspace to connect Google Meet.";
  }
  return message || "Couldn't start the Google Meet connection.";
}

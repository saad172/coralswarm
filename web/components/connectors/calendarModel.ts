import type { OAuthConnection } from "@/lib/api";
import type { Hue } from "@/components/ui";

/* Pure decisions behind the "Google Calendar" tile on /connectors.
 *
 * Calendar's OAuth row is stored under the ORIGINAL literal `"google"`
 * (`Surface::Calendar.token_provider()`, backend `connectors/google.rs`) —
 * not `"google_gmail"` / `"google_chat"` / `"google_meet"`. In a PERSONAL
 * workspace it is the account-wide row; in an ORG workspace it is the
 * caller's OWN per-member row (Google Calendar org lift: each member syncs
 * their own calendar into their private space). `GET /v1/oauth/connections`
 * already returns only the caller's own rows, so the same lookup serves both.
 *
 * The connect flow passes `surface=calendar` on BOTH `mintState` and
 * `authorizeUrl`. The personal plane treats a surface-less mint identically,
 * but under an org the backend gates a `surface=calendar` mint on the org's
 * Calendar flag specifically (a surface-less mint is gated on an unrelated
 * provider's flag and fails for an org that only enabled Calendar). */

export const CALENDAR_PROVIDER = "google";
export const CALENDAR_SURFACE = "calendar";
export const CALENDAR_READONLY_SCOPE = "calendar.readonly";

/** Tile metadata, local here (not in PROVIDER_META) like the Gmail/Meet
 *  tiles, so the shared provider table stays untouched. */
export const CALENDAR_META: { label: string; description: string; hue: Hue; letter: string } = {
  label: "Google Calendar",
  description:
    "Your own calendar, read-only: meeting titles, attendees, and pre-meeting context. In a team workspace each member connects their own and it lands in their private space only.",
  hue: "teal",
  letter: "C",
};

/** The member's Calendar connection, or null. Matches the `"google"` row and
 *  requires the `calendar.readonly` scope, so a stale/partial grant never
 *  renders as connected. Matches on the scope token's suffix, so both the
 *  bare and the full `https://www.googleapis.com/auth/calendar.readonly`
 *  forms qualify. Account guard as in `meetConnectionFrom`: after an account
 *  switch the list can briefly hold the outgoing account's rows. */
export function calendarConnectionFrom(
  connections: OAuthConnection[],
  connectionsAccountId: string | null = null,
  accountId: string | null = null,
): OAuthConnection | null {
  if (connectionsAccountId !== null && connectionsAccountId !== accountId) return null;
  return (
    connections.find(
      (c) =>
        c.provider === CALENDAR_PROVIDER &&
        (c.scope ?? "")
          .split(/[,\s]+/)
          .filter(Boolean)
          .some(
            (s) => s === CALENDAR_READONLY_SCOPE || s.endsWith(`/${CALENDAR_READONLY_SCOPE}`),
          ),
    ) ?? null
  );
}

/** Friendly text for a failed connect. The backend refuses an org workspace
 *  whose Calendar flag is off with a 403 BEFORE any redirect — at
 *  `mint_state` ("personal workspace only") or at `authorize` via
 *  `refuse_doomed_org_authorize_redirect`. */
export function calendarConnectError(status: number | null, message: string | null): string {
  if (status === 403) {
    return "Google Calendar isn't enabled for this workspace yet. Ask your workspace admin to turn it on, or switch to your personal workspace to connect Google Calendar.";
  }
  return message || "Couldn't start the Google Calendar connection.";
}

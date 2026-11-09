import { WORKSPACE_SURFACE } from "./googleWorkspaceModel";

/* How the redesigned grid's Connect / Continue setup / Reconnect still reach
   the OLD connect paths, until the wizard (W2) owns them. Pure: it names the
   target; the page performs it.

   - slack, microsoft, github, google: the same state-mint + authorize redirect
     the old tiles use (`api.oauth.mintState` then `authorizeUrl`). Slack and
     Google carry the surface the old cards pass.
   - every Beta toolkit: the generic toolkit connect page the old page links to.

   Delete this file with the old connect paths. */

/** Slack's member surface, as `SlackConnector` passes it. */
export const SLACK_SURFACE = "slack_member";

export type LegacyConnectTarget =
  | { kind: "oauth"; provider: string; surface?: string }
  | { kind: "href"; href: string };

const OAUTH: Record<string, { provider: string; surface?: string }> = {
  slack: { provider: "slack", surface: SLACK_SURFACE },
  microsoft: { provider: "microsoft" },
  github: { provider: "github" },
  google: { provider: "google", surface: WORKSPACE_SURFACE },
};

export function legacyConnectTarget(key: string): LegacyConnectTarget {
  const o = OAUTH[key];
  if (o) return { kind: "oauth", ...o };
  return { kind: "href", href: `/connectors/composio?toolkit=${encodeURIComponent(key)}` };
}

/* Teams host adapter: the Microsoft token comes from Teams SSO
 * (`authentication.getAuthToken()`, audience = the manifest's
 * `webApplicationInfo.resource`). Any failure to obtain one (not inside Teams,
 * consent not yet granted, tenant policy, SSO not configured) means SSO is
 * unavailable here, and the pane falls back to the existing Clerk flow. */

import { app, authentication } from "@microsoft/teams-js";
import { SsoUnavailableError, type AddinHost } from "./microsoftSso";

export function createTeamsHost(): AddinHost {
  return {
    name: "teams",
    async getMicrosoftToken() {
      try {
        await app.initialize();
        const token = await authentication.getAuthToken();
        if (!token) throw new Error("empty token");
        return token;
      } catch (cause) {
        throw new SsoUnavailableError(cause instanceof Error ? cause.message : undefined);
      }
    },
    openLink: (url) => app.openLink(url),
  };
}

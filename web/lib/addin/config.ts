/* Public identity of the CoralSwarm Microsoft app registration used for add-in
 * SSO. These are public identifiers (they also appear in both manifests), kept
 * as constants rather than env so they cannot be silently empty under vitest or
 * a misconfigured build. */

export const MICROSOFT_APP_ID = "00000000-0000-0000-0000-000000000000";
export const MICROSOFT_SSO_SCOPE = `api://app.coralswarm.example/${MICROSOFT_APP_ID}/access_as_user`;
export const MICROSOFT_AUTHORITY = "https://login.microsoftonline.com/common";

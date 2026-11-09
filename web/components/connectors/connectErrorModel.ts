/* Maps the `?error=` param the OAuth callback redirects to `/connectors` with
   into calm user-facing copy. Allow-listed: an unknown value shows nothing, so
   a crafted link cannot put arbitrary text on the page. */
const CONNECT_ERROR_MESSAGES: Record<string, string> = {
  expired: "That connect link expired — try again.",
};

export function connectErrorMessage(raw: string | null | undefined): string | null {
  if (!raw || !Object.prototype.hasOwnProperty.call(CONNECT_ERROR_MESSAGES, raw)) return null;
  return CONNECT_ERROR_MESSAGES[raw];
}

import { ApiError } from "@/lib/api";

/** Tooltip/label for a write control the server says this caller can't use
 *  (`ReefDetail.can_edit === false`). */
export const NO_EDIT_TITLE = "Only admins can change shared topics";

/** Shown when a write comes back 404. The backend answers a refused write
 *  (not allowed to change this topic) with the route's ordinary not-found
 *  response, so a 404 from a write means "you can't change this" as much as
 *  "it's gone". List endpoints don't return a per-item `can_edit`, so this is
 *  how those screens learn it. */
export const NO_EDIT_MESSAGE = "You can't change this. Only admins can change shared topics.";

/** Error text for a failed write: a friendly message for a 404, otherwise the
 *  error's own message, otherwise `fallback`. */
export function writeErrorMessage(e: unknown, fallback: string): string {
  if (e instanceof ApiError && e.status === 404) return NO_EDIT_MESSAGE;
  return e instanceof Error ? e.message : fallback;
}

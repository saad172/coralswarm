import { ApiError } from "@/lib/api";

/* Copy and small rules for in-app org deletion (backend: control 0387,
   routes/org_deletion.rs). Kept apart from the page so it is unit-tested. */

export const ORG_DELETION_PATH = "/org/deletion";

/** Same rule as the server: trimmed, case-insensitive, never empty. */
export function nameMatches(typed: string, name: string | null | undefined): boolean {
  const want = (name ?? "").trim().toLowerCase();
  return want.length > 0 && typed.trim().toLowerCase() === want;
}

export function formatDeletionDate(iso: string | null | undefined): string {
  const d = iso ? new Date(iso) : null;
  if (!d || Number.isNaN(d.getTime())) return "the end of the 7-day grace period";
  return d.toLocaleDateString(undefined, { year: "numeric", month: "long", day: "numeric" });
}

/** What the owner is told before confirming. Plain words, in order of surprise. */
export const DELETE_ORG_FACTS: string[] = [
  "Access ends now for everyone in the organization, including you, except for this page.",
  "The subscription is cancelled now. The current billing period is not refunded, and unused trial credits are lost.",
  "For 7 days you can download an export of the organization's data, or undo the deletion.",
  "Undoing brings access back but not the subscription: you will need to subscribe again from Billing.",
  "After 7 days the organization, its workspaces and its data are permanently deleted. This can't be undone.",
  "Every member's private notes in this organization will be permanently deleted with it. Members won't get a copy; we email each of them now to say so.",
  "People's own personal workspaces outside this organization are not affected.",
];

export const UNDONE_COPY =
  "The deletion is undone and everyone has access again. The subscription stays cancelled: subscribe again from Billing.";

/** Plain-words copy for a failed request, undo or export. */
export function orgDeletionErrorCopy(e: unknown): string {
  if (e instanceof ApiError) {
    switch (e.code) {
      case "confirmation_mismatch":
        return "That name doesn't match the organization. Check it and try again.";
      case "session_required":
      case "interactive_session_required":
        return "For your security, sign out and sign back in, then try again.";
      case "reverification_required":
        return "We couldn't confirm it's you. Verify again and retry.";
      case "not_owner":
        return "Only the organization's owner can do this.";
      case "not_self_serve":
        return "This organization was set up by CoralSwarm. Contact support to delete it.";
      case "not_yet":
        return "The subscription is still being cancelled. Try again in a few seconds.";
      case "too_late":
        return "It's too late to undo. The deletion has already started.";
      case "export_closed":
        return "The export is no longer available here. Contact support for the retained copy.";
      case "export_too_large":
        return "This organization's data is too large to download here. Contact support and we will send you an export.";
      case "export_in_progress":
        return "An export is already being prepared. Try again in a minute.";
      case "no_pending_deletion":
        return "There's no deletion to undo.";
    }
  }
  if (e && typeof e === "object" && (e as { code?: unknown }).code === "reverification_cancelled")
    return "Verification was cancelled, so nothing was deleted.";
  return "Something went wrong. Try again.";
}

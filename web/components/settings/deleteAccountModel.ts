import { ApiError, type DeleteAccountSharedMember } from "@/lib/api";

export const DELETED_PATH = "/account-deleted";
export const PENDING_PATH = "/account/delete/pending";

/** Case-insensitive, trimmed — the rule the confirm button enables on. */
export function emailMatches(typed: string, email: string): boolean {
  const want = email.trim().toLowerCase();
  return want.length > 0 && typed.trim().toLowerCase() === want;
}

export function formatPurgeDate(iso: string | null | undefined): string {
  const d = iso ? new Date(iso) : null;
  if (!d || Number.isNaN(d.getTime())) return "the end of the waiting period";
  return d.toLocaleDateString(undefined, { year: "numeric", month: "long", day: "numeric" });
}

export function memberLabel(m: DeleteAccountSharedMember): string {
  return m.name || m.email || m.user_id;
}

/** Plain-words copy for a failed `POST /v1/me/delete-account`. */
export function startErrorCopy(e: unknown): string {
  if (e instanceof ApiError) {
    if (e.code === "confirmation_mismatch") return "That email doesn't match your account. Check it and try again.";
    if (e.code === "session_required") return "For your security, sign out and sign back in, then try again.";
    if (e.code === "reverification_required") return "We couldn't confirm it's you. Verify again and retry.";
    if (e.code === "team_org_membership")
      return e.body?.delete_org_first
        ? "You own an organization. Delete it first (see below), then try again once its deletion is complete."
        : "You are still a member of an organization. Leave it first, then try again.";
    if (e.code === "shared_personal_account") {
      const members = Array.isArray(e.body?.members) ? (e.body.members as DeleteAccountSharedMember[]) : [];
      const who = members.map(memberLabel).join(", ");
      return `Other people use this account${who ? `: ${who}` : ""}. Remove them first, then try again.`;
    }
  }
  if (e && typeof e === "object" && (e as { code?: unknown }).code === "reverification_cancelled")
    return "Verification was cancelled, so nothing was deleted.";
  return "Couldn't schedule the deletion. Try again.";
}

/** Plain-words copy for a failed cancel (signed-in or by email link). */
export function cancelErrorCopy(e: unknown): string {
  if (e instanceof ApiError) {
    if (e.code === "invalid_token")
      return "This link isn't valid or has already been used. If your account is still scheduled for deletion, sign in to cancel it.";
    if (e.code === "too_late")
      return "It's too late to cancel. The deletion has already started and can't be stopped.";
    if (e.code === "no_pending_deletion") return "There's no deletion to cancel. Your account is not scheduled for deletion.";
  }
  return "Couldn't cancel the deletion. Try again.";
}

export const RESTORED_COPY =
  "Your account is restored. Your subscription stays cancelled — resubscribe from Billing.";

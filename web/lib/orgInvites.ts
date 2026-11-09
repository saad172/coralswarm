import { ApiError, type SlotPoolResult } from "@/lib/api";

/* Shared by the members page and the onboarding invite step, so the same
   `POST /v1/orgs/invites` failure reads the same on both. Keyed on the exact
   backend error strings in `backend/src/routes/org_invites.rs`. */

/** "The identity service" (Clerk) is briefly unreachable. The backend's
 *  `map_clerk_error` can surface any of these strings from create, resend
 *  and revoke alike. */
export const IDENTITY_SERVICE_UNAVAILABLE = "The identity service is briefly unavailable. Try again in a moment.";

export function isClerkUnavailable(message: string): boolean {
  return message === "clerk_error" || message === "clerk_misconfigured" || message === "service unavailable";
}

/** `middleware::auth::interactive_session_required`'s `code`. */
export const INTERACTIVE_SESSION_REQUIRED = "interactive_session_required";

/** `create_invite`'s 409 `code` when the invite was sent with
 *  `allow_purchase: false` and the org has no free seat. Nothing was bought
 *  and nothing was created; the caller prices the missing seats instead. */
export const NO_FREE_SEAT = "no_free_seat";

export function isNoFreeSeat(e: unknown): boolean {
  return e instanceof ApiError && e.status === 409 && e.code === NO_FREE_SEAT;
}

/** Seats to price after a 409 `no_free_seat`, from a freshly refetched pool
 *  (`null` when the refetch failed). The server just said nothing is free,
 *  so this never answers "buy nothing": an unknown or unbilled pool prices
 *  one seat per remaining invite, and a known one at least one seat. */
export function seatsAfterRefusal(remaining: number, pool: SlotPoolResult | null): { toBuy: number; free: number } {
  const plan = slotPlan(remaining, pool);
  if (plan.kind !== "known") return { toBuy: remaining, free: 0 };
  return { toBuy: Math.max(1, plan.toBuy), free: plan.free };
}

/** The line shown when seats were bought for a batch and then an invite in
 *  it failed or was skipped: the seats stay, and a re-send uses them. */
export function seatsAddedNote(added: number): string {
  return `${added} ${added === 1 ? "seat was" : "seats were"} added; sending again will use ${added === 1 ? "it" : "them"}.`;
}

export function mapAddMemberError(e: unknown): string {
  if (e instanceof ApiError) {
    // `require_org_admin` also refuses an admin whose credential is not an
    // interactive browser session (`middleware::auth::authority_allowed`).
    if (e.status === 403 && e.code === INTERACTIVE_SESSION_REQUIRED) {
      return "Inviting needs you signed in to CoralSwarm in a browser. Sign in again, then try again.";
    }
    if (e.status === 403) return "Only an org admin can invite people.";
    if (e.message === "invalid email") return "That doesn't look like an email address.";
    if (e.message === "already_a_member") return "That address already holds a slot here.";
    if (e.message === "seat_purchase_failed") {
      // `detail` is Stripe's decline reason (`classify_stripe_error`).
      if (e.detail === "card_declined") {
        return "The card on the billing page was declined — update it, then try again.";
      }
      return "The slot purchase didn't go through — check the card on the billing page, then try again.";
    }
    if (e.message === "billing_unavailable") return "Billing is briefly unavailable. Try again in a moment.";
    // Callers normally catch this first and open the seat quote; this is the
    // copy when they can't (e.g. the quote itself failed).
    if (isNoFreeSeat(e)) return "Every seat is taken. Add a seat, then send again.";
    // `map_clerk_error` passes Clerk's own 4xx through with Clerk's code;
    // that is about this address, not an outage. (Clerk's
    // `already_a_member_in_organization` never arrives here: the backend
    // turns it into 409 `already_a_member`.)
    if (e.message === "clerk_error" && e.status >= 400 && e.status < 500 && e.status !== 429) {
      return "The identity service rejected that address — check it and try again.";
    }
    if (isClerkUnavailable(e.message)) return IDENTITY_SERVICE_UNAVAILABLE;
  }
  return "Couldn't add that member — try again.";
}

/** Client-side "is this obviously not one email address" check before the
 *  invite POST. Every invite takes its own seat, so a pasted list is
 *  worth catching before the round trip. The backend's `valid_email` stays
 *  the source of truth. Returns the copy to show, or `null`. */
export function validateSingleEmail(raw: string): string | null {
  const trimmed = raw.trim();
  if (/[\s,]/.test(trimmed)) {
    return "One address at a time — each invite takes its own seat.";
  }
  const parts = trimmed.split("@");
  if (parts.length !== 2 || parts.some((p) => p.length === 0)) {
    return "That doesn't look like an email address.";
  }
  return null;
}

/** Whether a failed invite means the NEXT one in a batch would fail the same
 *  way. Only a problem with one address (malformed, already a member, a
 *  Clerk 4xx about that identifier) lets the batch continue; payment,
 *  billing, permission, identity-service and network failures stop it, so a
 *  declined card is not retried once per address. */
export function inviteStopsBatch(e: unknown): boolean {
  if (!(e instanceof ApiError)) return true;
  if (e.message === "invalid email" || e.message === "already_a_member") return false;
  if (e.message === "clerk_error" && e.status >= 400 && e.status < 500 && e.status !== 429) return false;
  return true;
}

export type SlotPlan =
  | { kind: "unknown" }
  /** Nothing has ever been bought. With no live subscription `acquire_seat`
   *  assigns a tier and makes no Stripe call, so sending charges nothing. */
  | { kind: "unbilled" }
  /** `fromPool` invites take an already-bought slot; `toBuy` more seats must
   *  be bought (through the seat quote) before the rest can be sent. The
   *  invite itself never buys: the web sends `allow_purchase: false`. */
  | { kind: "known"; free: number; fromPool: number; toBuy: number };

/** Admin invites always land on the `standard` tier (`create_invite` passes
 *  no tier), and `acquire_seat` consumes from that tier's own pool, so the
 *  account-wide `available` would over-promise when other tiers have room. */
export function slotPlan(count: number, pool: SlotPoolResult | null): SlotPlan {
  if (!pool) return { kind: "unknown" };
  if (pool.purchased === 0) return { kind: "unbilled" };
  const standard = pool.tiers?.find((t) => t.code === "standard");
  const free = Math.max(0, standard ? standard.available : pool.available);
  const fromPool = Math.min(count, free);
  return { kind: "known", free, fromPool, toBuy: count - fromPool };
}

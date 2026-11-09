import { ApiError } from "@/lib/api";
import { TRIAL_EXTRA_USAGE_COPY } from "@/lib/orgSelfserve";

/* Plain-words copy for every error the billing endpoints return. Keyed on the
   machine `code` (`{"error": msg, "code": ...}`); the older seat purchase
   routes only send a message string, so those are matched on it. The server's
   own sentence is never shown: it is written for logs, not for people. */

const BY_CODE: Record<string, string> = {
  // Portal
  no_billing_account: "There's no billing account yet. Subscribe first, then you can manage billing.",
  portal_not_configured: "The billing portal isn't available right now. Try again later or contact support.",
  billing_unavailable: "Billing is briefly unavailable. Try again in a moment.",
  // Seat quote
  no_subscription: "There's no active subscription yet. Subscribe first, then you can change seats.",
  subscription_inactive: "The subscription isn't active. Update billing, then change seats.",
  seats_in_use: "Those seats are in use. Remove members first, then give seats back.",
  // Extra usage and top-up
  admin_required: "Only an admin can change billing.",
  plan_required: "Extra usage is available on paid plans. Subscribe to turn it on.",
  credits_frozen: "Your extra-usage credits are on hold because the subscription ended. Resubscribe to use or add to them.",
  extra_usage_off: "Extra usage is off. Turn it on before adding funds.",
  // #1521: a self-serve team's free trial runs on its free credits only.
  trial_active: TRIAL_EXTRA_USAGE_COPY,
  limit_out_of_range: "The monthly limit must be between $1 and $100,000.",
  amount_not_whole_cents: "Amounts must be in whole cents, like $12.34.",
  member_cap_not_applicable: "Per-member limits only apply to organizations.",
  member_cap_out_of_range: "The per-member limit must be between 0.1 and 100 times a seat's monthly credits.",
  auto_reload_requires_extra_usage: "Turn on extra usage before turning on auto-reload.",
  auto_reload_incomplete: "Auto-reload needs both a trigger balance and a reload amount.",
  auto_reload_amount_out_of_range: "The reload amount must be between $5 and $1,000.",
  auto_reload_threshold_out_of_range: "The trigger balance must be at least $1.",
  auto_reload_threshold_exceeds_amount: "The trigger balance can't be higher than the reload amount.",
  auto_reload_requires_card:
    "Auto-reload can only charge a card, and your default payment method isn't one. Add a card from Manage billing, then turn auto-reload on.",
  auto_reload_exceeds_limit: "Auto-reload can't go past the monthly limit. Lower the amount or raise the limit.",
  topup_amount_out_of_range: "Add between $5 and $1,000 at a time.",
  return_urls_required: "Couldn't start checkout. Reload the page and try again.",
};

/** Message strings from `POST /v1/orgs/slots[/reduce]`, which send no `code`. */
function seatChangeByMessage(e: ApiError): string | null {
  const m = e.message;
  if (e.status === 403) return BY_CODE.admin_required;
  if (m.includes("quantity")) return "Pick a number of seats between 1 and 500.";
  if (m.includes("unknown or inactive seat tier")) return "That seat type isn't available.";
  if (m.includes("no active team subscription")) return BY_CODE.no_subscription;
  if (m.includes("frozen")) return "Billing is paused. Open Manage billing, then try again.";
  if (m === "not_enough_free_slots") return BY_CODE.seats_in_use;
  if (m === "seat_purchase_failed") {
    return e.detail === "card_declined"
      ? "The card was declined. Update it in Manage billing, then try again."
      : "The seat change didn't go through. Check the card in Manage billing, then try again.";
  }
  if (m === "billing_unavailable") return BY_CODE.billing_unavailable;
  return null;
}

/** Plain copy for a failed billing call. `fallback` names what was attempted. */
export function billingErrorMessage(e: unknown, fallback: string): string {
  if (!(e instanceof ApiError)) return fallback;
  if (e.code && BY_CODE[e.code]) {
    if (e.code === "seats_in_use" && typeof e.available === "number") {
      return e.available === 0
        ? "Every seat is in use. Remove a member first, then give a seat back."
        : `Only ${e.available} ${e.available === 1 ? "seat is" : "seats are"} free to give back. Remove members first to free more.`;
    }
    return BY_CODE[e.code];
  }
  const bySeat = seatChangeByMessage(e);
  if (bySeat) return bySeat;
  if (e.status === 403) return BY_CODE.admin_required;
  if (e.status === 502 || e.status === 503) return BY_CODE.billing_unavailable;
  return fallback;
}

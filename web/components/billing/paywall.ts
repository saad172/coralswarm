/* Per-reason copy for the "out of credits" refusal (HTTP 402 /
   `payment_required`). The backend sends `refusal_reason`; an older backend
   sends none, and then the original generic sentence stays. What the reader
   can do depends on who they are: an admin (or someone on their own account)
   can fix it on Billing, a member can only ask an admin. */

export type RefusalReason =
  | "credits_frozen"
  | "free_plan"
  | "extra_usage_off"
  | "member_cap_reached"
  | "monthly_limit_reached"
  | "extra_balance_exhausted";

export type PaywallAudience = "personal" | "admin" | "member";

export interface PaywallCopy {
  message: string;
  /** What to do about it. `null` for members, who can only ask an admin. */
  action: { label: string; href: string } | null;
}

export const PERSONAL_BILLING_HREF = "/settings/billing";
export const ORG_BILLING_HREF = "/settings/organization/billing";

/** The sentence used before `refusal_reason` existed. Kept word for word. */
export const GENERIC_OUT_OF_CREDITS = "You're out of credits for this billing period.";

const REASONS: Record<RefusalReason, { message: string; actor: string; label: string }> = {
  credits_frozen: {
    message: "Your subscription has ended, so your credits are on hold. They come back when you resubscribe.",
    actor: "resubscribe",
    label: "Resubscribe",
  },
  free_plan: {
    message: "You've used this month's credits on the Free plan.",
    actor: "upgrade the plan",
    label: "Upgrade",
  },
  extra_usage_off: {
    message: "You've used all of this month's plan credits, and extra usage is off.",
    actor: "turn on extra usage",
    label: "Turn on extra usage",
  },
  member_cap_reached: {
    message: "You've reached your personal share of extra usage for this month. It resets on the 1st.",
    actor: "raise the per-member limit",
    label: "Raise the member limit",
  },
  monthly_limit_reached: {
    message: "The monthly extra-usage limit has been reached. It resets on the 1st.",
    actor: "raise the monthly limit",
    label: "Raise the limit",
  },
  extra_balance_exhausted: {
    message: "The extra-usage balance has run out.",
    actor: "add funds",
    label: "Add funds",
  },
};

export function isRefusalReason(reason: unknown): reason is RefusalReason {
  return typeof reason === "string" && Object.prototype.hasOwnProperty.call(REASONS, reason);
}

/** Copy for a refusal. Returns `null` when the reason is absent or unknown,
 *  so the caller keeps its original wording. */
export function paywallCopy(reason: string | null | undefined, audience: PaywallAudience): PaywallCopy | null {
  if (!isRefusalReason(reason)) return null;
  const r = REASONS[reason];
  if (audience === "member") {
    return { message: `${r.message} Ask an admin to ${r.actor}.`, action: null };
  }
  const href = audience === "personal" ? PERSONAL_BILLING_HREF : ORG_BILLING_HREF;
  return { message: r.message, action: { label: r.label, href } };
}

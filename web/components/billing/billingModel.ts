import type { BillingPaymentMethod, BillingSummary } from "@/lib/api";
import { formatCents } from "./money";

/* What the billing summary means, decided once. */

/** Statuses that still have a plan running. */
const LIVE_STATUSES = ["active", "trialing", "past_due"];

export function isSubscribed(summary: BillingSummary): boolean {
  const status = summary.subscription.status;
  return status != null && LIVE_STATUSES.includes(status);
}

export function isFreePlan(summary: BillingSummary): boolean {
  return summary.plan == null || summary.plan.code === "free";
}

/** A cancellation (deliberate or lapsed) holds purchased extra-usage credits
 *  until the account resubscribes. */
export function hasFrozenCredits(summary: BillingSummary): boolean {
  return !isSubscribed(summary) && summary.extra_usage.balance_micro_usd > 0;
}

/** Extra usage can be configured and topped up only on a running paid plan. */
export function canUseExtraUsage(summary: BillingSummary): boolean {
  return isSubscribed(summary) && !isFreePlan(summary);
}

/** The checkout tier: Pro for a personal account, one standard seat for an org. */
export function subscribeTier(summary: BillingSummary): string {
  return summary.account_kind === "org" ? "standard" : "pro";
}

export function formatDate(iso: string | null | undefined): string | null {
  if (!iso) return null;
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return null;
  return d.toLocaleDateString("en-US", { year: "numeric", month: "long", day: "numeric", timeZone: "UTC" });
}

export function formatCredits(n: number): string {
  return Math.round(n).toLocaleString("en-US");
}

/** Price of one extra-usage credit, shown next to the balance (22,000 micro-USD). */
export const EXTRA_USAGE_PRICE_PER_CREDIT = "$0.022";

function titleCase(s: string): string {
  return s ? s.charAt(0).toUpperCase() + s.slice(1) : s;
}

/** How the default payment method reads on the billing page: a card as
 *  "Visa ending 4242" with its expiry, a bank account as "Example Bank •••• 4242",
 *  Link as "Link" with its email. `null` when nothing is on file. */
export function paymentMethodLine(pm: BillingPaymentMethod | null | undefined): { label: string; detail: string | null } | null {
  if (!pm) return null;
  const last4 = pm.last4 ? `•••• ${pm.last4}` : null;
  switch (pm.type) {
    case "card": {
      const expiry =
        pm.exp_month != null && pm.exp_year != null ? `expires ${String(pm.exp_month).padStart(2, "0")}/${pm.exp_year}` : null;
      return { label: `${pm.brand ? titleCase(pm.brand) : "Card"}${pm.last4 ? ` ending ${pm.last4}` : ""}`, detail: expiry };
    }
    case "us_bank_account":
      return { label: [pm.bank_name || "Bank account", last4].filter(Boolean).join(" "), detail: null };
    case "link":
      return { label: "Link", detail: pm.email ?? null };
    default:
      return { label: [titleCase(pm.type.replace(/_/g, " ")), last4].filter(Boolean).join(" "), detail: null };
  }
}

/** What Billing says while a team is on its usage-bound free trial. */
export interface TrialBillingView {
  /** Credits used, total and left, as plain numbers for the meter. */
  used: number;
  total: number;
  left: number;
  /** The plan line: free trial, and the rule that ends it. */
  planText: string;
  /** The credits line under the meter. */
  creditsText: string;
  /** What is charged once it converts, or `null` when the price is unknown. */
  priceText: string | null;
}

/** The trial facts for the Billing page, or `null` when the org is not in a
 *  live trial. The subscription mirror reads `active` for a trialing
 *  subscription (Stripe's `trialing` is stored as `active`), so the trial is
 *  told apart by `summary.trial`, never by `subscription.status`. */
export function trialBillingView(summary: BillingSummary): TrialBillingView | null {
  const t = summary.trial;
  if (!t || t.state !== "trialing") return null;
  const total = Math.max(0, t.credits_total);
  const used = Math.min(total, Math.max(0, t.credits_used));
  const left = Math.max(0, total - used);
  const end = formatDate(t.ends_at);
  // Seats bought during the trial raise the Stripe quantity, not `org_trials.seats`.
  const seatCount = Math.max(t.seats, summary.seats?.total ?? 0);
  const seats = `${seatCount} seat${seatCount === 1 ? "" : "s"}`;
  const cents = t.seat_price_usd_cents;
  const priceText =
    cents != null
      ? `After the trial: ${seats} at ${formatCents(cents)} per seat per month, ${formatCents(seatCount * cents)} a month.` +
        (t.seat_monthly_credits ? ` Each seat then gets ${formatCredits(t.seat_monthly_credits)} credits a month.` : "")
      : null;
  return {
    used,
    total,
    left,
    planText:
      `You're on a free trial. It converts to a paid subscription when the trial credits run out` +
      `${end ? `, or on ${end}` : ""}, whichever comes first.`,
    creditsText: `${formatCredits(left)} trial credits left.`,
    priceText,
  };
}

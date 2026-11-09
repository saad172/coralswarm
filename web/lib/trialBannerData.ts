import type { BillingSummary, BillingTrial, TrialBannerData } from "./api";

const object = (value: unknown): value is Record<string, unknown> => value !== null && typeof value === "object" && !Array.isArray(value);
const number = (value: unknown): value is number => typeof value === "number" && Number.isFinite(value);
const nullableNumber = (value: unknown) => value === null || number(value);
const nullableString = (value: unknown) => value === null || typeof value === "string";

export function isBillingTrial(value: unknown): value is BillingTrial {
  return object(value) && typeof value.state === "string" &&
    number(value.seats) && number(value.credits_total) && number(value.credits_used) &&
    nullableString(value.ends_at) && typeof value.payment_failed === "boolean" &&
    (value.seat_price_usd_cents === undefined || nullableNumber(value.seat_price_usd_cents)) &&
    (value.seat_monthly_credits === undefined || nullableNumber(value.seat_monthly_credits));
}

export function parseTrialBanner(value: unknown): TrialBannerData | null {
  if (!object(value) || value.kind !== "trial_banner_v1" || !(value.trial === null || isBillingTrial(value.trial))) return null;
  return { kind: "trial_banner_v1", trial: value.trial };
}

/** Rolling new web / old server: seed full billing only with its complete
 * legacy shape, including an explicit payment_method (null is valid data).
 * A trial-only DTO can never pass this check. */
export function isBillingSummary(value: unknown): value is BillingSummary {
  if (!object(value)) return false;
  const { plan, seats, credits, extra_usage: extra, subscription: sub, payment_method: pm } = value;
  return (value.account_kind === "org" || value.account_kind === "personal") &&
    (plan === null || object(plan) && typeof plan.code === "string" && typeof plan.name === "string") &&
    object(seats) && number(seats.used) && number(seats.total) &&
    object(credits) && number(credits.allowance_used) && number(credits.allowance_total) && nullableString(credits.resets_at) &&
    object(extra) && typeof extra.enabled === "boolean" && number(extra.balance_micro_usd) &&
    nullableNumber(extra.monthly_limit_micro_usd) && number(extra.spent_this_period_micro_usd) &&
    nullableNumber(extra.member_cap_seat_multiple) && object(extra.auto_reload) && typeof extra.auto_reload.enabled === "boolean" &&
    nullableNumber(extra.auto_reload.threshold_micro_usd) && nullableNumber(extra.auto_reload.amount_micro_usd) &&
    object(sub) && nullableString(sub.status) && typeof sub.has_billing_account === "boolean" &&
    nullableString(sub.renews_at) && typeof sub.cancel_at_period_end === "boolean" && nullableString(sub.cancel_at) &&
    (pm === null || object(pm) && typeof pm.type === "string" &&
      ["brand", "last4", "bank_name", "email"].every((key) => pm[key] === undefined || nullableString(pm[key])) &&
      ["exp_month", "exp_year"].every((key) => pm[key] === undefined || nullableNumber(pm[key]))) &&
    (value.trial === undefined || value.trial === null || isBillingTrial(value.trial));
}

export function bannerFromSummary(value: BillingSummary): TrialBannerData {
  return { kind: "trial_banner_v1", trial: value.trial ?? null };
}

import type { BillingExtraUsage, ExtraUsageUpdate } from "@/lib/api";
import {
  LIMIT_MAX_MICRO_USD,
  LIMIT_MIN_MICRO_USD,
  MEMBER_CAP_MAX,
  MEMBER_CAP_MIN,
  RELOAD_THRESHOLD_MIN_MICRO_USD,
  TOPUP_MAX_MICRO_USD,
  TOPUP_MIN_MICRO_USD,
  microUsdToInput,
  parseDollarsToMicroUsd,
  parseMemberCap,
  type DollarsParse,
} from "./money";

/* The extra-usage form: stored values -> text fields -> one PUT body. Pure so
   the validation can be tested without rendering. */

export interface ExtraUsageFormState {
  enabled: boolean;
  limit: string;
  memberCap: string;
  reloadEnabled: boolean;
  reloadThreshold: string;
  reloadAmount: string;
}

export type ExtraUsageFieldErrors = Partial<Record<"limit" | "memberCap" | "reloadThreshold" | "reloadAmount", string>>;

export function formFromExtraUsage(extra: BillingExtraUsage): ExtraUsageFormState {
  return {
    enabled: extra.enabled,
    limit: microUsdToInput(extra.monthly_limit_micro_usd),
    memberCap: extra.member_cap_seat_multiple == null ? "" : String(extra.member_cap_seat_multiple),
    reloadEnabled: extra.auto_reload.enabled,
    reloadThreshold: microUsdToInput(extra.auto_reload.threshold_micro_usd),
    reloadAmount: microUsdToInput(extra.auto_reload.amount_micro_usd),
  };
}

function dollarsError(p: Extract<DollarsParse, { ok: false }>, what: string): string {
  if (p.reason === "fraction") return `${what} must be in whole cents, like $12.34.`;
  if (p.reason === "empty") return `Enter ${what.toLowerCase()}.`;
  return `${what} must be a dollar amount, like 50 or 12.34.`;
}

const usd = (micro: number) => `$${(micro / 1_000_000).toLocaleString("en-US")}`;

export function buildExtraUsageUpdate(
  form: ExtraUsageFormState,
  opts: { isOrg: boolean },
): { ok: true; body: ExtraUsageUpdate } | { ok: false; errors: ExtraUsageFieldErrors } {
  // Turning it off keeps every stored setting; nothing else needs checking.
  if (!form.enabled) return { ok: true, body: { enabled: false } };

  const errors: ExtraUsageFieldErrors = {};
  const body: ExtraUsageUpdate = { enabled: true };

  // Blank means "keep what's stored" (or the server default on first enable).
  let limitMicro: number | null = null;
  if (form.limit.trim() !== "") {
    const p = parseDollarsToMicroUsd(form.limit);
    if (!p.ok) errors.limit = dollarsError(p, "The monthly limit");
    else if (p.micro < LIMIT_MIN_MICRO_USD || p.micro > LIMIT_MAX_MICRO_USD) {
      errors.limit = `The monthly limit must be between ${usd(LIMIT_MIN_MICRO_USD)} and ${usd(LIMIT_MAX_MICRO_USD)}.`;
    } else {
      limitMicro = p.micro;
      body.monthly_limit_micro_usd = p.micro;
    }
  }

  if (opts.isOrg && form.memberCap.trim() !== "") {
    const p = parseMemberCap(form.memberCap);
    if (!p.ok) {
      errors.memberCap =
        p.reason === "range"
          ? `The per-member limit must be between ${MEMBER_CAP_MIN} and ${MEMBER_CAP_MAX} times a seat's monthly credits.`
          : "Use a number like 1 or 2.5, with up to two decimals.";
    } else body.member_cap_seat_multiple = p.value;
  }

  if (!form.reloadEnabled) {
    body.auto_reload = { enabled: false };
  } else {
    const t = parseDollarsToMicroUsd(form.reloadThreshold);
    const a = parseDollarsToMicroUsd(form.reloadAmount);
    if (!t.ok) {
      errors.reloadThreshold =
        t.reason === "empty" ? "Auto-reload needs a trigger balance." : dollarsError(t, "The trigger balance");
    } else if (t.micro < RELOAD_THRESHOLD_MIN_MICRO_USD) {
      errors.reloadThreshold = `The trigger balance must be at least ${usd(RELOAD_THRESHOLD_MIN_MICRO_USD)}.`;
    }
    if (!a.ok) {
      errors.reloadAmount = a.reason === "empty" ? "Auto-reload needs a reload amount." : dollarsError(a, "The reload amount");
    } else if (a.micro < TOPUP_MIN_MICRO_USD || a.micro > TOPUP_MAX_MICRO_USD) {
      errors.reloadAmount = `The reload amount must be between ${usd(TOPUP_MIN_MICRO_USD)} and ${usd(TOPUP_MAX_MICRO_USD)}.`;
    }
    if (t.ok && a.ok && !errors.reloadThreshold && !errors.reloadAmount) {
      if (t.micro > a.micro) errors.reloadThreshold = "The trigger balance can't be higher than the reload amount.";
      else if (limitMicro != null && a.micro > limitMicro) {
        errors.reloadAmount = "Auto-reload can't go past the monthly limit. Lower the amount or raise the limit.";
      } else {
        body.auto_reload = { enabled: true, threshold_micro_usd: t.micro, amount_micro_usd: a.micro };
      }
    }
  }

  return Object.keys(errors).length > 0 ? { ok: false, errors } : { ok: true, body };
}

export function validateTopupAmount(text: string): { ok: true; micro: number } | { ok: false; error: string } {
  const p = parseDollarsToMicroUsd(text);
  if (!p.ok) return { ok: false, error: dollarsError(p, "The amount") };
  if (p.micro < TOPUP_MIN_MICRO_USD || p.micro > TOPUP_MAX_MICRO_USD) {
    return { ok: false, error: `Add between ${usd(TOPUP_MIN_MICRO_USD)} and ${usd(TOPUP_MAX_MICRO_USD)} at a time.` };
  }
  return { ok: true, micro: p.micro };
}

/** Which form field a server validation code belongs under, so the message
 *  lands next to the input instead of in a banner. */
export function fieldForErrorCode(code: string | null | undefined): keyof ExtraUsageFieldErrors | null {
  switch (code) {
    case "limit_out_of_range":
      return "limit";
    case "member_cap_out_of_range":
    case "member_cap_not_applicable":
      return "memberCap";
    case "auto_reload_threshold_out_of_range":
    case "auto_reload_threshold_exceeds_amount":
      return "reloadThreshold";
    case "auto_reload_amount_out_of_range":
    case "auto_reload_exceeds_limit":
      return "reloadAmount";
    default:
      return null;
  }
}

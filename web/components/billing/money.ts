/* Money on the billing pages.

   The API speaks integer micro-USD (1 USD = 1_000_000) everywhere except the
   seat quote, which is in cents. People type dollars. Typed amounts must be
   whole cents, so a typed value goes dollars -> whole cents -> micro-USD by
   string arithmetic, never through floating point. */

export const MICRO_PER_USD = 1_000_000;
export const MICRO_PER_CENT = 10_000;

/** Bounds mirrored from the backend (`billing::extra_usage`). The server
 *  stays the authority; these only catch typos before a round trip. */
export const TOPUP_MIN_MICRO_USD = 5 * MICRO_PER_USD;
export const TOPUP_MAX_MICRO_USD = 1_000 * MICRO_PER_USD;
export const LIMIT_MIN_MICRO_USD = 1 * MICRO_PER_USD;
export const LIMIT_MAX_MICRO_USD = 100_000 * MICRO_PER_USD;
export const RELOAD_THRESHOLD_MIN_MICRO_USD = 1 * MICRO_PER_USD;
export const MEMBER_CAP_MIN = 0.1;
export const MEMBER_CAP_MAX = 100;

const USD = new Intl.NumberFormat("en-US", { style: "currency", currency: "USD", minimumFractionDigits: 2, maximumFractionDigits: 2 });

/** `12_340_000` -> `$12.34`. Always two decimals. */
export function formatMicroUsd(micro: number): string {
  return USD.format(Math.round(micro / MICRO_PER_CENT) / 100);
}

/** `-1250` -> `-$12.50`. For the seat quote, which is in cents. */
export function formatCents(cents: number): string {
  return USD.format(cents / 100);
}

export type DollarsParse =
  | { ok: true; micro: number }
  | { ok: false; reason: "empty" | "invalid" | "fraction" };

/** Parses what someone typed into a dollar field: `50`, `$50`, `1,000.5`,
 *  `12.34`. More than two decimals is refused (`fraction`), never rounded. */
export function parseDollarsToMicroUsd(text: string): DollarsParse {
  const cleaned = text.trim().replace(/^\$/, "").replace(/,/g, "").trim();
  if (cleaned === "") return { ok: false, reason: "empty" };
  const m = /^(\d+)(?:\.(\d*))?$/.exec(cleaned);
  if (!m) return { ok: false, reason: "invalid" };
  const frac = m[2] ?? "";
  if (frac.length > 2) return { ok: false, reason: "fraction" };
  const cents = Number(m[1]) * 100 + Number(frac.padEnd(2, "0"));
  if (!Number.isSafeInteger(cents)) return { ok: false, reason: "invalid" };
  return { ok: true, micro: cents * MICRO_PER_CENT };
}

/** The value to prefill a dollar input with: `50_000_000` -> `50`. */
export function microUsdToInput(micro: number | null | undefined): string {
  if (micro == null) return "";
  const cents = Math.round(micro / MICRO_PER_CENT);
  return cents % 100 === 0 ? String(cents / 100) : (cents / 100).toFixed(2);
}

export type MultipleParse = { ok: true; value: number } | { ok: false; reason: "empty" | "invalid" | "range" };

/** The org member cap, a multiple of one seat's allowance: 0.1 to 100, two decimals. */
export function parseMemberCap(text: string): MultipleParse {
  const cleaned = text.trim().replace(/x$/i, "").trim();
  if (cleaned === "") return { ok: false, reason: "empty" };
  if (!/^\d+(?:\.\d{1,2})?$/.test(cleaned)) return { ok: false, reason: "invalid" };
  const value = Number(cleaned);
  if (value < MEMBER_CAP_MIN || value > MEMBER_CAP_MAX) return { ok: false, reason: "range" };
  return { ok: true, value };
}

/* Shared seat-tier option shape, fallback list, and label/selection rules —
 * CS-449 fix pass (code review on PR #1041). Extracted from three copies:
 * `JoinRequestQueue.tsx`'s own `TierOption`/`FALLBACK_TIERS`/
 * `tierOptionLabel` (CS-441/CS-442, item 40), pasted verbatim into
 * `AutoAddSetting.tsx` (CS-449, item 38) rather than imported, plus
 * `autoAddModel.ts`'s own `tierLabel` doing the same bare capitalisation a
 * third way for the "ask once" dialog's copy. One rule, one place, so a
 * fourth call site can't drift from it either.
 *
 * `resolveTierCode` carries forward the guard CodeRabbit asked for on PR
 * #1027 (`JoinRequestQueue.tsx`'s own `tierForRow`) — but generalised.
 * That review only ever protected ONE shape of staleness: an untouched
 * row's default falling back to the literal string `"standard"` when a
 * non-empty `seatTiers` response didn't actually contain it. It could
 * only ever produce that one shape of bug, because a controlled `<select>`
 * can never set `rowTier[id]` to a value that wasn't one of its own
 * rendered `<option>`s. `AutoAddSetting.tsx`'s `settings.tier` has no such
 * protection: it comes from the SERVER (a previously saved setting), so it
 * can name a tier that has since been deactivated or renamed — a value
 * `tierOptions` genuinely does not contain — with no guard at all. Passing
 * that straight to `<Select value={settings.tier}>` renders the browser's
 * own "no matching option" fallback (the first `<option>`) while the
 * component's own state still holds the stale code, so the visible
 * selection and the value a save would submit silently disagree. */

export interface TierOption {
  code: string;
  price_usd_cents: number | null;
}

export const DEFAULT_TIER_CODE = "standard";

/** `["standard"]` — the one-click default every seat-tier control needs
 *  before (or instead of) a real `api.billing.seatTiers` read succeeds.
 *  Both call sites treat that read as silent-on-failure by design: this
 *  fallback has to work standing alone. */
export const FALLBACK_TIERS: TierOption[] = [{ code: DEFAULT_TIER_CODE, price_usd_cents: null }];

/** `"standard"` -> `"Standard"` — the one capitalisation rule every tier
 *  label in this codebase shares, with or without a price alongside it. */
export function capitalizeTierCode(code: string): string {
  return code ? code[0].toUpperCase() + code.slice(1) : code;
}

/** A `<select>` option's label — capitalised code, plus its price when one
 *  is known. The fallback entry's `price_usd_cents: null` is how this knows
 *  not to print a price it doesn't actually have. */
export function tierOptionLabel(t: TierOption): string {
  const label = capitalizeTierCode(t.code);
  if (t.price_usd_cents == null) return label;
  return `${label} — $${(t.price_usd_cents / 100).toFixed(2)}`;
}

/** The code a controlled `<select>` should actually be given as its
 *  `value` — guaranteed to be one of `tierOptions`'s own codes, never a
 *  `desired` code the currently fetched list doesn't contain (see this
 *  module's own doc comment for the two different ways a caller can end up
 *  with a `desired` like that). Preference order once `desired` doesn't
 *  qualify: the fetched `"standard"` entry, then the first fetched option,
 *  then the literal default — reachable only when `tierOptions` is
 *  genuinely empty, which `FALLBACK_TIERS` guards against but a future
 *  caller passing `[]` directly would not. */
export function resolveTierCode(tierOptions: TierOption[], desired: string | undefined): string {
  if (desired !== undefined && tierOptions.some((t) => t.code === desired)) return desired;
  return tierOptions.find((t) => t.code === DEFAULT_TIER_CODE)?.code ?? tierOptions[0]?.code ?? DEFAULT_TIER_CODE;
}

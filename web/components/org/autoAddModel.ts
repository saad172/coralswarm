import type { AutoAddAdmitResult, AutoAddQueuedPasser, AutoAddQueuedPreview, OrgDomain } from "@/lib/api";
import { capitalizeTierCode } from "./seatTierOptions";

/* Auto-add settings-surface decision logic — CS-449 (org-onboarding plan §3
 * item 38, D11 "ask once"), building on CS-447's toggle (item 9) and CS-443's
 * domain verification (item 37). Same split `connectorHealthModel.ts` makes:
 * every "should we ask? what do we say?" question lives here, pure and
 * tested directly, so `AutoAddSetting.tsx` has no decision logic of its own
 * to drift from a second renderer.
 *
 * Three questions, three groups of functions below:
 *   1. Can the toggle fire at all? — `autoAddDomainState` / `autoAddGateCopy`
 *   2. Should flipping it on ask once, and what does the dialog say? —
 *      `planAskOnceDialog` / `askOnceCopy`
 *   3. What actually happened after the one click? —
 *      `summarizeAdmitOutcome` / `fallReasonLabel` */

// ── 1. The domain gate (D11: auto-add is meaningless without one) ─────────

export interface AutoAddDomainState {
  /** True when at least one claimed domain is verified BY DNS — the only
   *  proof that arms auto-add (see `OrgDomain`'s own doc comment; an
   *  email-verified domain arms the D2 guest rule only, never this). */
  armed: boolean;
  /** DNS-verified domain names, for the row's own description once armed. */
  armingDomains: string[];
  /** Every OTHER claimed domain (unverified, or verified by email only) —
   *  named in the gate copy so an admin who already claimed one isn't told
   *  "verify a domain" as if they'd done nothing. */
  claimedNotArming: string[];
}

/** `true` exactly when a claimed domain's OWN proof arms auto-add — D11's
 *  amendment, founder ruling 2026-09-22: DNS verification arms it, email
 *  verification (which still proves the D2 guest rule) never does. This is
 *  the ONE place that check is made (see `OrgDomain.method`'s own doc
 *  comment in `lib/api.ts`) — `autoAddDomainState` below and
 *  `components/org/domainVerificationModel.ts`'s `domainStatus` (CS-450,
 *  the claim/verify admin surface) both call this rather than each keeping
 *  their own copy of the condition, so the two surfaces can never disagree
 *  about which domains are armed. */
export function domainArmsAutoAdd(d: OrgDomain): boolean {
  return d.verified && d.method === "dns_txt";
}

/** `null` domains ⇒ not loaded (or the read failed) ⇒ fails closed, same as
 *  the backend's own `assertion_ladder::domain_signal` posture: a domain
 *  this surface cannot confirm must never be treated as armed. */
export function autoAddDomainState(domains: OrgDomain[] | null): AutoAddDomainState {
  if (!domains) return { armed: false, armingDomains: [], claimedNotArming: [] };
  const arming: string[] = [];
  const notArming: string[] = [];
  for (const d of domains) {
    if (domainArmsAutoAdd(d)) arming.push(d.domain);
    else notArming.push(d.domain);
  }
  return { armed: arming.length > 0, armingDomains: arming, claimedNotArming: notArming };
}

/** Oxford-comma-joined list — `"acme.com"`, `"acme.com or beta.com"`,
 *  `"acme.com, beta.com, or gamma.com"`. No caller passes an empty list. */
function naturalList(items: string[]): string {
  if (items.length === 1) return items[0];
  if (items.length === 2) return `${items[0]} or ${items[1]}`;
  return `${items.slice(0, -1).join(", ")}, or ${items[items.length - 1]}`;
}

/** The row's own description — D11's own sentence, *"automatically add
 *  colleagues with an @acme.com address"*, when armed; otherwise WHY the
 *  toggle isn't offered, so an admin who has never verified a domain is
 *  told what to do next rather than shown a control that can never fire. */
export function autoAddGateCopy(state: AutoAddDomainState): string {
  if (state.armed) {
    const addresses = naturalList(state.armingDomains.map((d) => `@${d}`));
    return `Automatically add colleagues with an ${addresses} address.`;
  }
  if (state.claimedNotArming.length === 0) {
    return "Auto-add needs a domain this organization has verified by DNS. None is verified yet.";
  }
  const names = naturalList(state.claimedNotArming);
  const verb = state.claimedNotArming.length === 1 ? "isn't" : "aren't";
  return `Auto-add needs a domain verified by DNS. ${names} ${verb} verified that way yet.`;
}

// ── 2. The D11 "ask once" dialog ───────────────────────────────────────────

export type AskOnceDialogPlan =
  | { kind: "hidden" }
  | {
      /** Advisory pool covers every queued passer — the one-click case,
       *  D11's own build note: "there's no shortfall to explain". */
      kind: "all-fit";
      passers: AutoAddQueuedPasser[];
      tier: string;
    }
  | {
      /** Advisory pool covers SOME of the queued passers — "admit what
       *  fits, oldest first" (founder, 2026-09-22). The button's own words
       *  ("Admit the {wouldAdmit} oldest") stay literally true through
       *  `askOnceMaxAdmit`, which caps the backend at exactly this many —
       *  NOT through trimming `askOnceRequestIds` to match, which is the
       *  bug this shape had before (code review on PR #1041). `drain_org`
       *  only ever collects a candidate whose `source` has already been
       *  patched to `pool_full`, and that patch only happens for an id
       *  `admit-queued` actually saw — an id this surface never submitted
       *  keeps its original source forever and the drain never touches it.
       *  So `askOnceRequestIds` now sends EVERY passer shown, and the
       *  backend both admits the oldest `wouldAdmit` of them for real AND
       *  patches the rest to `pool_full` in the same call, making the
       *  footnote below ("added as slots free up") actually true. */
      kind: "partial";
      passers: AutoAddQueuedPasser[];
      wouldAdmit: number;
      freeSlots: number;
      tier: string;
    }
  | {
      /** Advisory pool covers NONE of them right now — `askOnceMaxAdmit`
       *  reports `0`. Still lists who is queued (D11: "show the admin the
       *  queued requests"), and — corrected (code review on PR #1041) — now
       *  offers a real action rather than a dead end: the click admits
       *  nobody and spends no slot, but still submits every passer shown so
       *  the backend patches all of them to `pool_full`, the same state
       *  `admit-queued` gives an unfit "partial" remainder. Without that
       *  submission these requests keep whatever `source` they already had
       *  and `drain_org` never collects them — the promise "added as slots
       *  free up" was false for this branch until this fix. */
      kind: "none-fit";
      passers: AutoAddQueuedPasser[];
      tier: string;
    };

/** Turns one `queuedPreview` read into what the dialog shows — or doesn't.
 *  `null`/empty passers ⇒ `"hidden"`: flipping auto-add on with nothing
 *  queued just turns it on, no dialog, per D11's own scope note ("turning
 *  the toggle on changes what happens to the NEXT person; item 38 is the
 *  ask for everyone ALREADY queued" — nothing queued, nothing to ask). */
export function planAskOnceDialog(preview: AutoAddQueuedPreview | null): AskOnceDialogPlan {
  if (!preview || preview.passers.length === 0) return { kind: "hidden" };
  // Defensive clamp — `would_admit`/`free_slots` are documented as
  // non-negative integers already, but a plan built from a malformed/future
  // payload must not tell an admin a button will admit more people than
  // exist. `Number.isFinite` has to run BEFORE the clamp, not instead of
  // it (fixed — code review on PR #1041): `would_admit` missing entirely
  // deserialises as `undefined`, and `Math.min(undefined, n)` /
  // `Math.max(0, NaN)` are both `NaN` — which fails every comparison below,
  // including `NaN === 0`, so the old code fell all the way through to
  // "partial" with `wouldAdmit: NaN` baked into the plan (a button reading
  // "Admit the NaN oldest", a footnote reading "stay NaN in the queue", and
  // `passers.slice(0, NaN)` silently sending an empty `request_ids`).
  const rawWouldAdmit = Number.isFinite(preview.would_admit) ? preview.would_admit : 0;
  const rawFreeSlots = Number.isFinite(preview.free_slots) ? preview.free_slots : 0;
  const wouldAdmit = Math.max(0, Math.min(rawWouldAdmit, preview.passers.length));
  if (wouldAdmit >= preview.passers.length) {
    return { kind: "all-fit", passers: preview.passers, tier: preview.tier };
  }
  if (wouldAdmit === 0) {
    return { kind: "none-fit", passers: preview.passers, tier: preview.tier };
  }
  return {
    kind: "partial",
    passers: preview.passers,
    wouldAdmit,
    freeSlots: Math.max(0, rawFreeSlots),
    tier: preview.tier,
  };
}

/** `"standard"` → `"Standard"` — `seatTierOptions.ts`'s own
 *  `capitalizeTierCode`, the one capitalisation rule `JoinRequestQueue.tsx`'s
 *  `tierOptionLabel` and this dialog's copy both apply to a `seat_tiers.code`,
 *  so the two surfaces never describe the same tier two ways. (This used to
 *  be a third, separate copy of that rule — code review on PR #1041.) */
export function tierLabel(tier: string): string {
  return capitalizeTierCode(tier);
}

function personCount(n: number): string {
  return `${n} queued ${n === 1 ? "person" : "people"}`;
}

export interface AskOnceCopy {
  headline: string;
  /** Second line — omitted (`null`) when there's nothing more to say, per
   *  D11's own "no shortfall to explain" rule for the all-fit case. */
  subline: string | null;
  /** Below the people list — the drain promise, only when something will
   *  stay queued after the click. */
  footnote: string | null;
  /** `null` only for a `"hidden"` plan, which renders no dialog at all.
   *  Every other kind — including `"none-fit"`, corrected (code review on
   *  PR #1041) to offer a real "queue them for the drain" action instead of
   *  a dead end — has a primary action. */
  admitLabel: string | null;
  dismissLabel: string;
}

/** The dialog's copy, keyed on {@link AskOnceDialogPlan}'s `kind` — the one
 *  place these sentences are written, so the component only renders them. */
export function askOnceCopy(plan: AskOnceDialogPlan): AskOnceCopy | null {
  if (plan.kind === "hidden") return null;
  if (plan.kind === "all-fit") {
    const n = plan.passers.length;
    return {
      headline: `${personCount(n)} would be added.`,
      subline: null,
      footnote: null,
      admitLabel: n === 1 ? "Admit them" : `Admit all ${n}`,
      dismissLabel: "Not now",
    };
  }
  if (plan.kind === "partial") {
    const remainder = plan.passers.length - plan.wouldAdmit;
    const tier = tierLabel(plan.tier);
    return {
      headline: `${personCount(plan.passers.length)} would be added.`,
      subline: `${plan.freeSlots} ${tier} slot${plan.freeSlots === 1 ? "" : "s"} free`,
      footnote:
        `The other ${remainder} ${remainder === 1 ? "stays" : "stay"} in the queue and ` +
        `${remainder === 1 ? "is" : "are"} added as slots free up.`,
      admitLabel: `Admit the ${plan.wouldAdmit} oldest`,
      dismissLabel: "Not now",
    };
  }
  // "none-fit" — corrected (code review on PR #1041): this used to be a
  // dead end (no button, just a promise the UI never acted on). It now
  // offers the same real action `askOnceMaxAdmit`/`askOnceRequestIds`
  // describe: submit every passer shown at `max_admit: 0`, so nobody is
  // admitted and no slot is spent, but the backend still patches all of
  // them to `pool_full` so the drain genuinely collects them later.
  const n = plan.passers.length;
  const tier = tierLabel(plan.tier);
  return {
    headline: `${personCount(n)} would be added — once a ${tier} slot frees up.`,
    subline: `No ${tier} slot is free right now.`,
    footnote: "Nobody is added and no slot is spent yet — this just makes sure they're added automatically the moment one frees.",
    admitLabel: n === 1 ? "Queue for the next slot" : `Queue all ${n} for the next slot`,
    dismissLabel: "Not now",
  };
}

/** The `request_ids` body for `admitQueued` — EVERY passer the dialog
 *  shows, oldest first, for every actionable plan. `null` only for
 *  `"hidden"`, which never calls it.
 *
 *  Corrected (code review on PR #1041) after an earlier round sent only the
 *  oldest `wouldAdmit` of these instead, reasoning that the button's own
 *  words ("Admit the N oldest") had to stay literally true and that a wider
 *  send could let a lucky race admit more than the admin clicked. Both
 *  premises were real, but the fix broke something bigger: `drain_org`
 *  (`routes::org_join_requests`) only ever collects a candidate whose
 *  `source` has already been patched to `pool_full` — and that patch only
 *  happens for an id `admit-queued` actually saw. An id this call never
 *  submitted keeps its ORIGINAL source forever, so the 15-of-20 left out by
 *  the narrower send were never drained; the footnote promising "added as
 *  slots free up" was false. The button's own words now stay true through
 *  {@link askOnceMaxAdmit} instead: the backend admits at most that many,
 *  oldest first, from the full set sent here, and patches everyone past
 *  the cap to `pool_full` in the same call — so the remainder genuinely
 *  drains, and a click can still never admit more than it promised. */
export function askOnceRequestIds(plan: AskOnceDialogPlan): string[] | null {
  if (plan.kind === "hidden") return null;
  // Passers are already oldest-first (the backend's own contract) —
  // preserved as-is; the ordering is the server's consent to admit "the
  // oldest N", not a promise about array order it re-derives regardless.
  return plan.passers.map((p) => p.request_id);
}

/** The `max_admit` body field alongside {@link askOnceRequestIds} — the
 *  ceiling the button's own words promise. `"all-fit"` sends every passer's
 *  own count (nothing is held back); `"partial"` sends `wouldAdmit`
 *  (exactly "Admit the N oldest"); `"none-fit"` sends `0`, valid and
 *  deliberate — that click's entire purpose is admitting nobody while still
 *  queuing everyone shown for the drain. `null` only for `"hidden"`. */
export function askOnceMaxAdmit(plan: AskOnceDialogPlan): number | null {
  if (plan.kind === "hidden") return null;
  if (plan.kind === "all-fit") return plan.passers.length;
  if (plan.kind === "partial") return plan.wouldAdmit;
  return 0; // "none-fit"
}

// ── 3. What actually happened ───────────────────────────────────────────

/** Every label `not_admitted[].reason` can actually carry, turned into a
 *  clause that reads naturally after an em dash — "— the pool ran out", not
 *  "— pool_full". Confirmed 2026-09-22 by reading `FallReason::label()` in
 *  `backend/src/services/org_auto_add.rs` directly (an earlier paraphrase
 *  had `already_member`, which is not a real label — see `fallReasonLabel`'s
 *  own note on why that class of mistake matters). Eight are `FallReason`
 *  values; `pool_full`, `admit_cap_reached`, and `unavailable` are not —
 *  `admit-queued` emits them itself: `pool_full` for a real reservation
 *  attempt that found the pool empty (gate 2, spent for real rather than
 *  merely read), `admit_cap_reached` for a candidate that passed every gate
 *  but `max_admit` was already spent by an earlier id in the same call — so
 *  NO reservation was even attempted, distinct from `pool_full` on purpose
 *  (confirmed 2026-09-22 against `ADMIT_CAP_REACHED_REASON` in
 *  `backend/src/routes/org_join_requests.rs` and `NotAdmitted::reason`'s own
 *  doc comment in `backend/src/routes/org_auto_add.rs`, not guessed — an
 *  earlier round of this fix deliberately left this key out until it could
 *  be confirmed this way) — and `unavailable` for an infra failure. Adding a
 *  newly-shipped label is a one-liner here; anything this map doesn't
 *  recognise still renders, via {@link fallReasonLabel}'s fallback, rather
 *  than leaking the raw enum at an admin. */
const FALL_REASON_LABELS: Record<string, string> = {
  // Gate 1 — the D16 domain ladder.
  domain_does_not_arm_auto_add: "their address isn't on a domain this organization has verified",
  // Evidence matched, but too weakly to auto-admit — NOT a rejection, they
  // stay queued for a human to decide.
  ladder_approval_only: "the evidence wasn't strong enough to add them automatically",
  // Gate 2 — the pool. `pool_full` is admit-queued's own real-reservation
  // outcome; `no_seat_tier_configured` is dead code today (`decide()` never
  // returns it) but, if it ever fires, must read as an infrastructure gap,
  // never as anything decided about the person.
  pool_full: "the pool ran out",
  // This click's OWN cap was already spent by an earlier, older id in the
  // same request — distinct from `pool_full` so an admin is never told the
  // pool was empty when it was actually their own "admit the N oldest"
  // limit doing exactly what it promised.
  admit_cap_reached: "the click's own limit was already spent by an earlier request",
  no_seat_tier_configured: "there was no slot tier to admit into",
  // Gate 3 — the tombstone.
  tombstoned: "they'd been removed before",
  // Other decision-time reasons.
  already_a_member: "they were already a member",
  previously_rejected: "they'd been rejected before",
  classification_too_old: "the evidence had gone stale — it needs a fresh look",
  auto_add_off: "auto-add wasn't on for them at the time",
  // Not a decision about the person at all.
  unavailable: "something on our side failed — try again",
};

/** `reason` → the clause above, or a readable fallback for a label this
 *  build doesn't recognise yet (a snake_case string turned into plain
 *  words) — never the raw enum.
 *
 *  The fallback only catches a label that's GENUINELY new — it cannot catch
 *  a key in {@link FALL_REASON_LABELS} that's simply spelled wrong. A wrong
 *  key never throws and never looks broken: it just quietly falls through
 *  to the same fallback as a real unknown label, reading as plausible copy
 *  either way. That's exactly the shape the map's own `already_member` bug
 *  took (the real label is `already_a_member`) before it was caught by
 *  checking the backend source directly rather than trusting a paraphrase. */
export function fallReasonLabel(reason: string): string {
  return FALL_REASON_LABELS[reason] ?? reason.replace(/_/g, " ").toLowerCase();
}

export interface AdmitOutcomeSummary {
  requested: number;
  admitted: number;
  shortfall: number;
  /** The one-line result — "Added all 5.", "3 of the 5 were added.", etc.
   *  Measured against `requested` (the PROMISE this click made — see this
   *  function's own doc comment) rather than any earlier estimate, so it
   *  never claims a number the response didn't actually report. */
  headline: string;
  /** Grouped `not_admitted` reasons, most common first — `[]` when nothing
   *  fell short. Ties keep the order the response reported them in.
   *  Excludes the "none-fit" case (`requested === 0`) entirely — see this
   *  function's own doc comment. */
  reasons: { label: string; count: number }[];
}

/** Turns one `admitQueued` response into the honest, refreshed line the UI
 *  shows after the click — "report honestly what actually happened … and
 *  refresh rather than leaving a stale count on screen" (this story's own
 *  brief).
 *
 *  `requestedCount` is `askOnceMaxAdmit`'s own number — the promise THIS
 *  click made ("Admit the N oldest") — NOT `request_ids.length` (corrected,
 *  code review on PR #1041: `request_ids` now always carries every passer
 *  shown, per `askOnceRequestIds`'s own doc comment, so its length no
 *  longer means "how many this click promised to admit"). `admitted` can
 *  never exceed the promise — the backend never admits more than
 *  `max_admit` — but CAN still fall short of it: the pool is reserved for
 *  real only at admit time, so even this capped, already-advised request
 *  can hit a pool that shrank further in the meantime.
 *
 *  `requestedCount === 0` is the "none-fit" click's own promise (admit
 *  nobody, queue everyone for the drain) — its own headline, since
 *  "Added all 0."/`shortfall` math built for a real admit attempt reads as
 *  a failure for a click that succeeded exactly as intended. `not_admitted`
 *  is real HERE too (every id sent lands in it, however the backend labels
 *  each one), but none of those labels are a shortfall against a promise
 *  that was never "admit some of you" — so `reasons` is deliberately empty
 *  rather than reporting them as if something fell short. */
export function summarizeAdmitOutcome(requestedCount: number, result: AutoAddAdmitResult): AdmitOutcomeSummary {
  const admitted = result.admitted.length;
  if (requestedCount === 0) {
    return {
      requested: 0,
      admitted,
      shortfall: 0,
      headline: "Nobody was added — they're queued to be added automatically as slots free up.",
      reasons: [],
    };
  }
  const shortfall = Math.max(0, requestedCount - admitted);
  const headline =
    shortfall === 0
      ? admitted === 1
        ? "Added."
        : `Added all ${admitted}.`
      : admitted === 0
        ? "None were added."
        : `${admitted} of the ${requestedCount} were added.`;

  const counts = new Map<string, number>();
  for (const item of result.not_admitted) {
    const label = fallReasonLabel(item.reason);
    counts.set(label, (counts.get(label) ?? 0) + 1);
  }
  return {
    requested: requestedCount,
    admitted,
    shortfall,
    headline,
    reasons: [...counts.entries()].map(([label, count]) => ({ label, count })),
  };
}

/** The `reasons` groups above, joined into one clause for after an em dash
 *  — `"the pool ran out"`, or `"the pool ran out (3), they were already a
 *  member (1)"` when more than one reason is in play. `""` when there's
 *  nothing to explain (no shortfall).
 *
 *  Fixed (code review on PR #1041) — the dangling "— ." bug: `admit_queued`
 *  drops an id from BOTH `admitted` and `not_admitted`, silently, when it no
 *  longer names an OPEN request for this account (approved by hand since
 *  the preview, or simply unknown) — see `AutoAddAdmitResult`'s own doc
 *  comment in `lib/api.ts`. That leaves `shortfall` positive with `reasons`
 *  empty: e.g. 5 sent, 2 approved by another admin in the meantime, comes
 *  back `{admitted: [a,b,c], not_admitted: []}` — 3 of 5 admitted, and NO
 *  reason on file for the other 2. That is a real, nameable thing (those 2
 *  stopped being open requests) rather than a gap to hide, so any shortfall
 *  the reported reasons don't cover is named here explicitly instead of
 *  silently producing an empty clause. */
export function admitOutcomeReasonClause(summary: AdmitOutcomeSummary): string {
  const parts = summary.reasons.map((r) => ({ label: r.label, count: r.count }));
  const explained = parts.reduce((n, r) => n + r.count, 0);
  const unexplained = Math.max(0, summary.shortfall - explained);
  if (unexplained > 0) {
    parts.push({ label: "they were no longer open requests by the time this ran", count: unexplained });
  }
  if (parts.length === 0) return "";
  if (parts.length === 1) return parts[0].label;
  return parts.map((r) => `${r.label} (${r.count})`).join(", ");
}

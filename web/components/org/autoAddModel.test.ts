import { describe, expect, it } from "vitest";
import type { AutoAddAdmitResult, AutoAddQueuedPasser, AutoAddQueuedPreview, OrgDomain } from "@/lib/api";
import {
  admitOutcomeReasonClause,
  askOnceCopy,
  askOnceMaxAdmit,
  askOnceRequestIds,
  autoAddDomainState,
  autoAddGateCopy,
  domainArmsAutoAdd,
  fallReasonLabel,
  planAskOnceDialog,
  summarizeAdmitOutcome,
  tierLabel,
} from "./autoAddModel";

function domain(overrides: Partial<OrgDomain> = {}): OrgDomain {
  return {
    id: "dom_1",
    domain: "acme.example",
    verified: true,
    method: "dns_txt",
    verified_at: "2026-09-20T10:00:00Z",
    claimed_at: "2026-09-18T10:00:00Z",
    dns_record_name: "_coralswarm.acme.example",
    dns_record_value: "cs-verify-abc123",
    confirmation_email: null,
    confirmation_sent_at: null,
    ...overrides,
  };
}

function passer(overrides: Partial<AutoAddQueuedPasser> = {}): AutoAddQueuedPasser {
  return {
    request_id: "req_1",
    email: "person@example.com",
    name: "Ada",
    requested_at: "2026-09-20T10:00:00Z",
    ...overrides,
  };
}

describe("domainArmsAutoAdd — the ONE place this check is made (CS-450 shares it)", () => {
  it("arms on a DNS-verified domain", () => {
    expect(domainArmsAutoAdd(domain({ verified: true, method: "dns_txt" }))).toBe(true);
  });

  it("does not arm on an email-verified domain", () => {
    expect(domainArmsAutoAdd(domain({ verified: true, method: "email" }))).toBe(false);
  });

  it("does not arm on an unverified domain", () => {
    expect(domainArmsAutoAdd(domain({ verified: false, method: null }))).toBe(false);
  });
});

describe("autoAddDomainState — D11: auto-add is meaningless without a verified domain", () => {
  it("fails closed while domains haven't loaded", () => {
    expect(autoAddDomainState(null)).toEqual({ armed: false, armingDomains: [], claimedNotArming: [] });
  });

  it("arms only on a DNS-verified domain, never an email-verified one", () => {
    const state = autoAddDomainState([domain({ domain: "acme.example", method: "dns_txt", verified: true })]);
    expect(state.armed).toBe(true);
    expect(state.armingDomains).toEqual(["acme.example"]);
  });

  it("does not arm on an email-verified domain — it proves the guest rule only", () => {
    const state = autoAddDomainState([domain({ domain: "acme.example", method: "email", verified: true })]);
    expect(state.armed).toBe(false);
    expect(state.claimedNotArming).toEqual(["acme.example"]);
  });

  it("does not arm on an unverified (claimed but unproven) domain", () => {
    const state = autoAddDomainState([domain({ domain: "acme.example", method: null, verified: false })]);
    expect(state.armed).toBe(false);
    expect(state.claimedNotArming).toEqual(["acme.example"]);
  });

  it("arms on any one DNS-verified domain even alongside others that don't qualify", () => {
    const state = autoAddDomainState([
      domain({ domain: "acme.example", method: "email", verified: true }),
      domain({ domain: "acme.dev", method: "dns_txt", verified: true }),
    ]);
    expect(state.armed).toBe(true);
    expect(state.armingDomains).toEqual(["acme.dev"]);
    expect(state.claimedNotArming).toEqual(["acme.example"]);
  });
});

describe("autoAddGateCopy", () => {
  it("says D11's own sentence for one armed domain", () => {
    const copy = autoAddGateCopy(autoAddDomainState([domain({ domain: "acme.com" })]));
    expect(copy).toBe("Automatically add colleagues with an @acme.com address.");
  });

  it("lists every armed domain naturally", () => {
    const two = autoAddGateCopy(
      autoAddDomainState([domain({ domain: "acme.com" }), domain({ domain: "beta.com" })]),
    );
    expect(two).toBe("Automatically add colleagues with an @acme.com or @beta.com address.");

    const three = autoAddGateCopy(
      autoAddDomainState([domain({ domain: "acme.com" }), domain({ domain: "beta.com" }), domain({ domain: "gamma.com" })]),
    );
    expect(three).toBe("Automatically add colleagues with an @acme.com, @beta.com, or @gamma.com address.");
  });

  it("explains the gate, generically, when nothing has been claimed", () => {
    expect(autoAddGateCopy(autoAddDomainState(null))).toMatch(/none is verified yet/i);
  });

  it("names a claimed-but-not-arming domain rather than pretending none exists", () => {
    const copy = autoAddGateCopy(autoAddDomainState([domain({ domain: "acme.com", method: "email" })]));
    expect(copy).toContain("acme.com");
    expect(copy).toMatch(/isn't verified that way yet/i);
  });
});

function preview(overrides: Partial<AutoAddQueuedPreview> = {}): AutoAddQueuedPreview {
  return { tier: "standard", free_slots: 5, would_admit: 5, passers: [passer()], ...overrides };
}

describe("planAskOnceDialog — D11 'ask once'", () => {
  it("stays hidden with nothing queued — flipping the toggle just turns it on", () => {
    expect(planAskOnceDialog(null)).toEqual({ kind: "hidden" });
    expect(planAskOnceDialog(preview({ passers: [], would_admit: 0 }))).toEqual({ kind: "hidden" });
  });

  it("is 'all-fit' when the advisory pool covers every passer", () => {
    const p = preview({ passers: [passer(), passer({ request_id: "req_2" })], free_slots: 5, would_admit: 2 });
    expect(planAskOnceDialog(p)).toEqual({ kind: "all-fit", passers: p.passers, tier: "standard" });
  });

  it("is 'partial' — founder ruling: admit what fits, oldest first — when the pool covers some", () => {
    const passers = Array.from({ length: 20 }, (_, i) => passer({ request_id: `req_${i}` }));
    const p = preview({ passers, free_slots: 5, would_admit: 5 });
    const plan = planAskOnceDialog(p);
    expect(plan).toMatchObject({ kind: "partial", wouldAdmit: 5, freeSlots: 5, tier: "standard" });
    if (plan.kind === "partial") expect(plan.passers).toHaveLength(20);
  });

  it("is 'none-fit' when the advisory pool covers nobody, but still lists who is queued", () => {
    const p = preview({ passers: [passer(), passer({ request_id: "req_2" })], free_slots: 0, would_admit: 0 });
    const plan = planAskOnceDialog(p);
    expect(plan.kind).toBe("none-fit");
    if (plan.kind === "none-fit") expect(plan.passers).toHaveLength(2);
  });

  it("clamps a malformed would_admit rather than promising more admits than exist", () => {
    const p = preview({ passers: [passer()], free_slots: 99, would_admit: 99 });
    const plan = planAskOnceDialog(p);
    expect(plan.kind).toBe("all-fit");
  });

  /** THE fix: a non-finite `would_admit` (e.g. absent from a malformed
   *  payload, deserialising as `undefined`) used to slip past `Math.min`/
   *  `Math.max` as `NaN`, which fails EVERY comparison including
   *  `NaN === 0` — so the plan fell through to "partial" with `NaN` baked
   *  into `wouldAdmit`, rendering "Admit the NaN oldest" and silently
   *  posting an empty `request_ids` (`passers.slice(0, NaN)` is `[]`). A
   *  non-finite value must read as "admits nobody", not "partial". */
  it("treats a non-finite would_admit as zero rather than producing a NaN plan", () => {
    const malformed = { tier: "standard", free_slots: 5, passers: [passer()] } as unknown as AutoAddQueuedPreview;
    const plan = planAskOnceDialog(malformed);
    expect(plan.kind).toBe("none-fit");
    expect(Number.isNaN((plan as { wouldAdmit?: number }).wouldAdmit)).toBe(false);
  });

  it("treats a non-finite free_slots the same way, clamping the partial branch's own subline to zero", () => {
    const passers = [passer({ request_id: "a" }), passer({ request_id: "b" })];
    const p = { tier: "standard", would_admit: 1, passers } as unknown as AutoAddQueuedPreview;
    const plan = planAskOnceDialog(p);
    expect(plan).toMatchObject({ kind: "partial", wouldAdmit: 1, freeSlots: 0 });
  });
});

describe("askOnceCopy", () => {
  it("all-fit: no shortfall to explain — no slots line, no 'stays queued' footnote", () => {
    const copy = askOnceCopy({ kind: "all-fit", passers: [passer(), passer({ request_id: "req_2" })], tier: "standard" });
    expect(copy?.headline).toBe("2 queued people would be added.");
    expect(copy?.subline).toBeNull();
    expect(copy?.footnote).toBeNull();
    expect(copy?.admitLabel).toBe("Admit all 2");
  });

  it("all-fit, singular", () => {
    const copy = askOnceCopy({ kind: "all-fit", passers: [passer()], tier: "standard" });
    expect(copy?.headline).toBe("1 queued person would be added.");
    expect(copy?.admitLabel).toBe("Admit them");
  });

  /** THE shape from this story's own brief: 20 queued, 5 free, "Admit the 5
   *  oldest", and the drain promise about the other 15. */
  it("partial: matches the brief's own mock exactly", () => {
    const passers = Array.from({ length: 20 }, (_, i) => passer({ request_id: `req_${i}` }));
    const copy = askOnceCopy({ kind: "partial", passers, wouldAdmit: 5, freeSlots: 5, tier: "standard" });
    expect(copy?.headline).toBe("20 queued people would be added.");
    expect(copy?.subline).toBe("5 Standard slots free");
    expect(copy?.admitLabel).toBe("Admit the 5 oldest");
    expect(copy?.footnote).toBe("The other 15 stay in the queue and are added as slots free up.");
  });

  it("partial, remainder of one — singular verbs", () => {
    const passers = Array.from({ length: 6 }, (_, i) => passer({ request_id: `req_${i}` }));
    const copy = askOnceCopy({ kind: "partial", passers, wouldAdmit: 5, freeSlots: 5, tier: "standard" });
    expect(copy?.footnote).toBe("The other 1 stays in the queue and is added as slots free up.");
  });

  /** Corrected (code review on PR #1041): "none-fit" used to offer no
   *  button at all — a dead end, since nothing else ever submits these
   *  requests for the drain to find. It now offers a real "queue them"
   *  action that promises zero admits (`max_admit: 0`) honestly. */
  it("none-fit: offers a real action that promises zero admits, honestly", () => {
    const copy = askOnceCopy({ kind: "none-fit", passers: [passer(), passer({ request_id: "req_2" })], tier: "standard" });
    expect(copy?.admitLabel).toBe("Queue all 2 for the next slot");
    expect(copy?.dismissLabel).toBe("Not now");
    expect(copy?.headline).toMatch(/once a Standard slot frees up/);
    expect(copy?.subline).toMatch(/No Standard slot is free right now/);
    expect(copy?.footnote).toMatch(/[Nn]obody is added and no slot is spent/);
  });

  it("none-fit, singular", () => {
    const copy = askOnceCopy({ kind: "none-fit", passers: [passer()], tier: "standard" });
    expect(copy?.admitLabel).toBe("Queue for the next slot");
  });

  it("hidden plans render no copy at all", () => {
    expect(askOnceCopy({ kind: "hidden" })).toBeNull();
  });
});

describe("askOnceRequestIds — every passer shown, for every actionable plan", () => {
  /** THE fix (code review on PR #1041): an earlier round sent only the
   *  oldest `wouldAdmit` ids, which meant everyone left out never got
   *  submitted at all — and `drain_org` only ever collects a candidate
   *  whose `source` has already been patched to `pool_full` by a
   *  submission. Every actionable plan now sends every passer it shows;
   *  `askOnceMaxAdmit` (tested below) is what keeps the button's own words
   *  literally true instead. */
  it("hidden never submits anything", () => {
    expect(askOnceRequestIds({ kind: "hidden" })).toBeNull();
  });

  it("all-fit sends every passer", () => {
    const passers = [passer({ request_id: "a" }), passer({ request_id: "b" }), passer({ request_id: "c" })];
    expect(askOnceRequestIds({ kind: "all-fit", passers, tier: "standard" })).toEqual(["a", "b", "c"]);
  });

  it("none-fit ALSO sends every passer shown — its own click needs them patched to pool_full for the drain", () => {
    const passers = [passer({ request_id: "a" }), passer({ request_id: "b" })];
    expect(askOnceRequestIds({ kind: "none-fit", passers, tier: "standard" })).toEqual(["a", "b"]);
  });

  it("partial sends every passer shown, not just the oldest wouldAdmit", () => {
    const passers = [
      passer({ request_id: "a" }),
      passer({ request_id: "b" }),
      passer({ request_id: "c" }),
      passer({ request_id: "d" }),
    ];
    expect(askOnceRequestIds({ kind: "partial", passers, wouldAdmit: 2, freeSlots: 2, tier: "standard" })).toEqual([
      "a",
      "b",
      "c",
      "d",
    ]);
  });

  it("matches the brief's own mock: 20 shown, 5 free — sends all 20, not just the 5 oldest", () => {
    const passers = Array.from({ length: 20 }, (_, i) => passer({ request_id: `req_${i}` }));
    const ids = askOnceRequestIds({ kind: "partial", passers, wouldAdmit: 5, freeSlots: 5, tier: "standard" });
    expect(ids).toEqual(passers.map((p) => p.request_id));
  });
});

describe("askOnceMaxAdmit — the cap that keeps the button's own words true now that every passer is sent", () => {
  it("hidden has no cap", () => {
    expect(askOnceMaxAdmit({ kind: "hidden" })).toBeNull();
  });

  it("all-fit caps at every passer — nothing is held back", () => {
    const passers = [passer({ request_id: "a" }), passer({ request_id: "b" })];
    expect(askOnceMaxAdmit({ kind: "all-fit", passers, tier: "standard" })).toBe(2);
  });

  it("partial caps at exactly wouldAdmit", () => {
    const passers = Array.from({ length: 20 }, (_, i) => passer({ request_id: `req_${i}` }));
    expect(askOnceMaxAdmit({ kind: "partial", passers, wouldAdmit: 5, freeSlots: 5, tier: "standard" })).toBe(5);
  });

  it("none-fit caps at zero — its own point, not an oversight", () => {
    const passers = [passer({ request_id: "a" }), passer({ request_id: "b" })];
    expect(askOnceMaxAdmit({ kind: "none-fit", passers, tier: "standard" })).toBe(0);
  });
});

describe("tierLabel", () => {
  it("capitalises a bare seat_tiers.code", () => {
    expect(tierLabel("standard")).toBe("Standard");
    expect(tierLabel("")).toBe("");
  });
});

describe("fallReasonLabel — never leaks the raw backend enum", () => {
  /** All ten labels `not_admitted[].reason` can actually carry, confirmed
   *  2026-09-22 by reading `FallReason::label()` in
   *  `backend/src/services/org_auto_add.rs` directly (not the backend
   *  agent's paraphrase — which had `already_member`, a key that never
   *  matches the real `already_a_member` and would have silently lost to
   *  the fallback forever, exactly the failure `fallReasonLabel`'s fallback
   *  exists to catch when a label is genuinely unknown, and exactly the
   *  failure it CANNOT catch when a key is merely spelled wrong). */
  it("knows every confirmed FallReason label, spelled exactly as the backend returns it", () => {
    expect(fallReasonLabel("pool_full")).toBe("the pool ran out");
    expect(fallReasonLabel("already_a_member")).toContain("already a member");
    expect(fallReasonLabel("tombstoned")).toContain("removed");
    expect(fallReasonLabel("previously_rejected")).toContain("rejected");
    expect(fallReasonLabel("domain_does_not_arm_auto_add")).toMatch(/verified/);
    expect(fallReasonLabel("classification_too_old")).toMatch(/stale/);
    expect(fallReasonLabel("auto_add_off")).toMatch(/auto-add wasn't on/);
    expect(fallReasonLabel("no_seat_tier_configured")).toMatch(/slot tier/);
    expect(fallReasonLabel("unavailable")).toMatch(/our side failed/);
  });

  /** `admit_cap_reached` (code review on PR #1041, FIX 1): the eleventh
   *  label, added once every passer started being sent alongside
   *  `max_admit`. Confirmed against `ADMIT_CAP_REACHED_REASON` in
   *  `backend/src/routes/org_join_requests.rs` and `NotAdmitted::reason`'s
   *  own doc comment in `backend/src/routes/org_auto_add.rs` directly, not
   *  guessed — a candidate that passed every gate but whose reservation was
   *  never even attempted because this click's own `max_admit` was already
   *  spent by an earlier, older id. Must read distinctly from `pool_full`:
   *  the pool was not actually empty, the click's own limit was. */
  it("admit_cap_reached reads distinctly from pool_full — the click's own limit, not an empty pool", () => {
    const label = fallReasonLabel("admit_cap_reached");
    expect(label).not.toBe(fallReasonLabel("pool_full"));
    expect(label).toMatch(/limit/);
  });

  /** `ladder_approval_only` is evidence that matched but wasn't strong
   *  enough to auto-admit — explicitly NOT a rejection, they stay queued
   *  for a human. The copy must not read as one. */
  it("does not describe ladder_approval_only as a rejection", () => {
    const label = fallReasonLabel("ladder_approval_only");
    expect(label).not.toMatch(/reject/i);
    expect(label).toMatch(/evidence/);
  });

  /** The dead key this exact bug shipped with. Asserted explicitly so a
   *  future edit can't silently reintroduce `already_member` (it would
   *  still pass every other test above, since the fallback renders SOME
   *  plausible-looking text for it and never throws). */
  it("already_member (the wrong, paraphrased spelling) is not a key — it degrades to the fallback", () => {
    expect(fallReasonLabel("already_member")).toBe("already member");
  });

  it("degrades an unrecognised label to readable words instead of the raw enum", () => {
    expect(fallReasonLabel("stale_token_expired")).toBe("stale token expired");
  });
});

function admitResult(overrides: Partial<AutoAddAdmitResult> = {}): AutoAddAdmitResult {
  return { admitted: [], not_admitted: [], ...overrides };
}

describe("summarizeAdmitOutcome — honest about what THIS click actually did", () => {
  it("everyone requested got admitted", () => {
    const summary = summarizeAdmitOutcome(5, admitResult({ admitted: ["a", "b", "c", "d", "e"] }));
    expect(summary.headline).toBe("Added all 5.");
    expect(summary.shortfall).toBe(0);
    expect(summary.reasons).toEqual([]);
  });

  it("singular", () => {
    const summary = summarizeAdmitOutcome(1, admitResult({ admitted: ["a"] }));
    expect(summary.headline).toBe("Added.");
  });

  /** THE sentence this story's own brief specifies: "3 of the 5 were added
   *  — the pool ran out". */
  it("a partial admit reports the real count against what was actually requested, not the advisory estimate", () => {
    const summary = summarizeAdmitOutcome(
      5,
      admitResult({
        admitted: ["a", "b", "c"],
        not_admitted: [
          { request_id: "d", reason: "pool_full" },
          { request_id: "e", reason: "pool_full" },
        ],
      }),
    );
    expect(summary.headline).toBe("3 of the 5 were added.");
    expect(summary.shortfall).toBe(2);
    expect(admitOutcomeReasonClause(summary)).toBe("the pool ran out");
  });

  it("none admitted", () => {
    const summary = summarizeAdmitOutcome(3, admitResult({ not_admitted: [{ request_id: "a", reason: "pool_full" }] }));
    expect(summary.headline).toBe("None were added.");
  });

  /** `requestedCount` is `askOnceMaxAdmit`'s own number, not
   *  `request_ids.length` (corrected — code review on PR #1041, since
   *  `request_ids` now always carries every passer shown), so `admitted`
   *  can never exceed it — the backend never admits more than `max_admit`.
   *  It CAN still fall short of it — the pool is read for real only at
   *  admit time, so even this capped, already-advised request can hit a
   *  pool that shrank further between the preview and the click. */
  it("even the capped, promise-sized request can still come up short — honestly reported", () => {
    const summary = summarizeAdmitOutcome(
      5,
      admitResult({
        admitted: ["a", "b", "c"],
        not_admitted: [
          { request_id: "d", reason: "pool_full" },
          { request_id: "e", reason: "already_a_member" },
        ],
      }),
    );
    expect(summary.admitted).toBe(3);
    expect(summary.headline).toBe("3 of the 5 were added.");
  });

  it("groups multiple distinct reasons, most common first, into one readable clause", () => {
    const summary = summarizeAdmitOutcome(
      4,
      admitResult({
        admitted: ["a"],
        not_admitted: [
          { request_id: "b", reason: "pool_full" },
          { request_id: "c", reason: "pool_full" },
          { request_id: "d", reason: "already_a_member" },
        ],
      }),
    );
    expect(admitOutcomeReasonClause(summary)).toBe("the pool ran out (2), they were already a member (1)");
  });

  it("an unrecognised reason still renders inside the clause, not silently dropped", () => {
    const summary = summarizeAdmitOutcome(
      2,
      admitResult({ admitted: ["a"], not_admitted: [{ request_id: "b", reason: "brand_new_classification" }] }),
    );
    expect(admitOutcomeReasonClause(summary)).toBe("brand new classification");
  });

  it("no reason clause when nothing fell short", () => {
    const summary = summarizeAdmitOutcome(2, admitResult({ admitted: ["a", "b"] }));
    expect(admitOutcomeReasonClause(summary)).toBe("");
  });

  /** THE FIX 4 bug: 5 sent, 2 approved by hand by another admin between the
   *  preview and the click. `admit_queued`'s own contract drops an id from
   *  BOTH `admitted` and `not_admitted`, silently, once it no longer names
   *  an open request — so this comes back `{admitted: [a,b,c],
   *  not_admitted: []}`. The old code computed `shortfall = 5 - 3 = 2` from
   *  `requestedCount` alone, independent of `not_admitted`, while `reasons`
   *  stayed `[]` — rendering "3 of the 5 were added. — .": a shortfall with
   *  an empty reason clause after the em dash. */
  it("a shortfall the response gives no reason for is named, not left blank", () => {
    const summary = summarizeAdmitOutcome(5, admitResult({ admitted: ["a", "b", "c"], not_admitted: [] }));
    expect(summary.headline).toBe("3 of the 5 were added.");
    expect(summary.shortfall).toBe(2);
    expect(summary.reasons).toEqual([]);
    expect(admitOutcomeReasonClause(summary)).toBe("they were no longer open requests by the time this ran");
  });

  it("mixes a reported reason with an unexplained remainder, both counted", () => {
    const summary = summarizeAdmitOutcome(
      5,
      admitResult({ admitted: ["a", "b"], not_admitted: [{ request_id: "c", reason: "pool_full" }] }),
    );
    // 5 requested, 2 admitted, 1 reported not_admitted — 2 more are
    // unexplained (dropped silently, per `admit_queued`'s own contract).
    expect(summary.shortfall).toBe(3);
    expect(admitOutcomeReasonClause(summary)).toBe(
      "the pool ran out (1), they were no longer open requests by the time this ran (2)",
    );
  });

  /** The "none-fit" click's own promise: admit nobody, queue everyone for
   *  the drain. `requestedCount: 0` must not read as "Added all 0." — a
   *  literal, honest description of a click that succeeded exactly as
   *  intended, not a shortfall against a promise that was never "admit
   *  some of you". */
  it("requestedCount 0 (the none-fit click) reports success, never a shortfall", () => {
    const summary = summarizeAdmitOutcome(
      0,
      admitResult({ admitted: [], not_admitted: [{ request_id: "a", reason: "pool_full" }] }),
    );
    expect(summary.headline).toBe("Nobody was added — they're queued to be added automatically as slots free up.");
    expect(summary.shortfall).toBe(0);
    expect(summary.reasons).toEqual([]);
    expect(admitOutcomeReasonClause(summary)).toBe("");
  });
});

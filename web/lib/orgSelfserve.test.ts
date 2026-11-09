import { describe, expect, it } from "vitest";
import { ApiError, type BillingTrial, type OrgSetupStatus } from "./api";
import {
  CARD_ALREADY_TRIALED,
  NAME_ERROR,
  NAME_HIDDEN_CHARS_ERROR,
  NAME_PERSONAL_ERROR,
  PAYMENT_CONFIRM_GRACE_MS,
  POLL_MAX_MS,
  POLL_SLOW_MAX_MS,
  SEATS_ERROR,
  TRIAL_EXTRA_USAGE_COPY,
  MAX_STORED_ATTEMPTS,
  attemptByKey,
  attemptByOrg,
  attemptFor,
  attemptFromQuery,
  attemptQuery,
  createErrorView,
  dropAttempt,
  dropAttemptForOrg,
  formatSeatPrice,
  formatWait,
  isProvisioningGate,
  nextPollDelay,
  orgFromQuery,
  pendingTeamTarget,
  readAttempt,
  readAttempts,
  recordAttemptOrg,
  rememberAttempt,
  repeatTrialCopy,
  resumeErrorView,
  resumeSeats,
  retryErrorView,
  setupView,
  statusQuery,
  stepLabel,
  trialBannerView,
  trialCopy,
  trialRefusesExtraUsage,
  validateCreateForm,
  type AttemptStore,
  type KV,
  type SetupInput,
} from "./orgSelfserve";

function memKV(): KV & { data: Map<string, string> } {
  const data = new Map<string, string>();
  return {
    data,
    getItem: (k) => data.get(k) ?? null,
    setItem: (k, v) => void data.set(k, v),
    removeItem: (k) => void data.delete(k),
  };
}

function memStore(user = "user_1", kv = memKV()): AttemptStore & { kv: ReturnType<typeof memKV> } {
  return { kv, user };
}

function apiErr(status: number, code?: string, body: Record<string, unknown> = {}): ApiError {
  return new ApiError(code ?? `HTTP ${status}`, status, code ?? null, null, null, { ...(code ? { code } : {}), ...body });
}

const NOW = Date.parse("2026-10-27T12:00:00Z");

function status(p: Partial<OrgSetupStatus>): OrgSetupStatus {
  return {
    org_id: "org_1",
    status: "provisioning",
    step: null,
    waiting_for: null,
    retryable: false,
    resume_payment_until: null,
    message: "",
    ...p,
  };
}

function input(s: OrgSetupStatus | null, extra: Partial<SetupInput> = {}): SetupInput {
  return { status: s, page: "return", now: NOW, elapsedMs: 0, ...extra };
}

describe("validateCreateForm", () => {
  it("accepts a name and the default 2 seats, normalising spaces", () => {
    expect(validateCreateForm({ name: "  Acme   Corp ", seats: "2" })).toEqual({ ok: true, name: "Acme Corp", seats: 2 });
  });

  it("refuses fewer than 2 seats, fractions, junk and more than 1,000", () => {
    for (const seats of ["1", "0", "", "2.5", "-3", "abc", "1001"]) {
      const r = validateCreateForm({ name: "Acme", seats });
      expect(r.ok, seats).toBe(false);
      if (!r.ok) expect(r.errors.seats).toBe(SEATS_ERROR);
    }
    expect(validateCreateForm({ name: "Acme", seats: "1000" }).ok).toBe(true);
  });

  it("mirrors A2's name rules", () => {
    const bad = ["", " ", "A", "x".repeat(121), "!!", "--"];
    for (const name of bad) {
      const r = validateCreateForm({ name, seats: "2" });
      expect(r.ok, JSON.stringify(name)).toBe(false);
      if (!r.ok) expect(r.errors.name).toBe(NAME_ERROR);
    }
    expect(validateCreateForm({ name: "Ab", seats: "2" }).ok).toBe(true);
    expect(validateCreateForm({ name: "x".repeat(120), seats: "2" }).ok).toBe(true);
    const personal = validateCreateForm({ name: "Sam's Organization", seats: "2" });
    expect(personal.ok).toBe(false);
    if (!personal.ok) expect(personal.errors.name).toBe(NAME_PERSONAL_ERROR);
  });

  it("names hidden characters (format and control) instead of the length rule", () => {
    for (const name of ["Ac​me", "‮Acme", "Acme⁦", "Ac\u0007me"]) {
      const r = validateCreateForm({ name, seats: "2" });
      expect(r.ok, JSON.stringify(name)).toBe(false);
      if (!r.ok) expect(r.errors.name).toBe(NAME_HIDDEN_CHARS_ERROR);
    }
    // NFKC, as A2 does: a full-width name is fine.
    expect(validateCreateForm({ name: "Ａｃｍｅ", seats: "2" })).toMatchObject({ ok: true, name: "Acme" });
  });

  it("reports both fields at once", () => {
    const r = validateCreateForm({ name: "", seats: "1" });
    expect(r).toEqual({ ok: false, errors: { name: NAME_ERROR, seats: SEATS_ERROR } });
  });
});

describe("the sign-up attempt", () => {
  it("reuses the key for the same name and seats, and mints a new one when either changes", () => {
    const s = memStore();
    let n = 0;
    const mint = () => `key-000${++n}`;
    const a = attemptFor(s, "Acme", 3, mint);
    expect(attemptFor(s, " Acme ", 3, mint).key).toBe(a.key);
    const b = attemptFor(s, "Acme", 4, mint);
    expect(b.key).not.toBe(a.key);
    const c = attemptFor(s, "Acme Two", 4, mint);
    expect(c.key).not.toBe(b.key);
  });

  it("records the org on the matching attempt only, and drops it", () => {
    const s = memStore();
    const a = attemptFor(s, "Acme", 3, () => "key-aaaa1");
    recordAttemptOrg(s, "some-other-key", "org_x");
    expect(readAttempt(s)?.orgId).toBeUndefined();
    recordAttemptOrg(s, a.key, "org_1");
    expect(readAttempt(s)).toEqual({ key: "key-aaaa1", name: "Acme", seats: 3, orgId: "org_1" });
    dropAttempt(s, a.key);
    expect(readAttempt(s)).toBeNull();
  });

  it("is per user: another person on the same browser sees none of it", () => {
    const kv = memKV();
    const alice = memStore("user_alice", kv);
    const bob = memStore("user_bob", kv);
    const a = attemptFor(alice, "Acme", 3, () => "key-alice1");
    recordAttemptOrg(alice, a.key, "org_alice1");
    expect(readAttempts(bob)).toEqual([]);
    expect(attemptByOrg(bob, "org_alice1")).toBeNull();
    // Bob's own sign-up, and his clearing it, leave Alice's alone.
    const b = attemptFor(bob, "Acme", 3, () => "key-bob001");
    expect(b.key).toBe("key-bob001");
    dropAttempt(bob, b.key);
    dropAttemptForOrg(bob, "org_alice1");
    expect(attemptByOrg(alice, "org_alice1")?.key).toBe("key-alice1");
  });

  it("keeps several attempts: a second team doesn't clobber the first", () => {
    const s = memStore();
    const first = attemptFor(s, "Acme", 3, () => "key-first1");
    recordAttemptOrg(s, first.key, "org_first1");
    const second = attemptFor(s, "Beta", 2, () => "key-second");
    recordAttemptOrg(s, second.key, "org_second");
    expect(readAttempt(s)?.key).toBe("key-second");
    expect(attemptByOrg(s, "org_first1")?.key).toBe("key-first1");
    // Finishing one forgets only that one.
    dropAttemptForOrg(s, "org_second");
    expect(readAttempts(s).map((a) => a.key)).toEqual(["key-first1"]);
    // Going back to the first name reuses its key.
    expect(attemptFor(s, "Acme", 3, () => "never").key).toBe("key-first1");
  });

  it("caps how many it keeps, newest first", () => {
    const s = memStore();
    for (let i = 0; i < MAX_STORED_ATTEMPTS + 3; i++) attemptFor(s, `Team ${i}`, 2, () => `key-cap-${i}`);
    const keys = readAttempts(s).map((a) => a.key);
    expect(keys).toHaveLength(MAX_STORED_ATTEMPTS);
    expect(keys[0]).toBe(`key-cap-${MAX_STORED_ATTEMPTS + 2}`);
  });

  it("remembers a confirmed attempt at the front, and finds it by key", () => {
    const s = memStore();
    attemptFor(s, "Acme", 3, () => "key-aaaa1");
    rememberAttempt(s, { key: "key-url001", name: "From URL", seats: 4, orgId: "org_url1" });
    expect(readAttempt(s)?.key).toBe("key-url001");
    expect(attemptByKey(s, "key-aaaa1")?.name).toBe("Acme");
  });

  it("ignores junk in storage", () => {
    const s = memStore();
    s.kv.setItem("cs_team_signup_attempts:user_1", "{not json");
    expect(readAttempts(s)).toEqual([]);
    s.kv.setItem("cs_team_signup_attempts:user_1", JSON.stringify([{ key: 1 }, { key: "key-ok0001", name: "Ok", seats: 2 }]));
    expect(readAttempts(s).map((a) => a.key)).toEqual(["key-ok0001"]);
  });

  it("works with no storage at all", () => {
    expect(attemptFor(null, "Acme", 2, () => "key-bbbb1").key).toBe("key-bbbb1");
    expect(readAttempt(null)).toBeNull();
  });

  it("round-trips through the return URL query, and rejects a malformed one", () => {
    const q = new URLSearchParams(attemptQuery({ key: "key-cccc1", name: "Acme & Co", seats: 5 }));
    expect(attemptFromQuery(q)).toEqual({ key: "key-cccc1", name: "Acme & Co", seats: 5 });
    expect(attemptFromQuery(new URLSearchParams("k=short&n=Acme&s=2"))).toBeNull();
    expect(attemptFromQuery(new URLSearchParams("k=key-cccc1&n=Acme&s=1"))).toBeNull();
    expect(attemptFromQuery(new URLSearchParams("k=key-cccc1&s=2"))).toBeNull();
  });
});

describe("createErrorView: every A2 code has human copy", () => {
  const codes: Array<[number, string]> = [
    [400, "invalid_name"],
    [400, "invalid_seats"],
    [400, "return_urls_required"],
    [400, "idempotency_key_required"],
    [403, "email_unverified"],
    [403, "identity_required"],
    [403, "interactive_session_required"],
    [409, "idempotency_key_reused"],
    [409, "create_in_progress"],
    [409, "signup_complete"],
    [409, "checkout_complete"],
    [410, "checkout_expired"],
    [422, "org_create_refused"],
    [429, "too_many_pending_orgs"],
    [429, "org_create_rate_limited"],
    [503, "identity_unavailable"],
    [503, "org_create_unavailable"],
  ];
  it.each(codes)("%i %s", (st, code) => {
    const v = createErrorView(apiErr(st, code));
    expect(v.message).toBeTruthy();
    expect(v.message).not.toMatch(/HTTP|_/);
  });

  it("a Clerk configuration refusal is a generic retry-later error, never 'try a different name'", () => {
    const v = createErrorView(apiErr(503, "org_create_unavailable"));
    expect(v.field).toBeUndefined();
    expect(v.message).not.toMatch(/different name|that name/i);
    expect(v.message).toMatch(/Try again later/);
    expect(v.keepAttempt).toBe(true);
  });

  it("puts field errors under their field", () => {
    expect(createErrorView(apiErr(400, "invalid_name")).field).toBe("name");
    expect(createErrorView(apiErr(400, "invalid_seats")).field).toBe("seats");
    expect(createErrorView(apiErr(422, "org_create_refused")).field).toBe("name");
  });

  it("honours retry_after_s on both 429s", () => {
    const pending = createErrorView(apiErr(429, "too_many_pending_orgs", { retry_after_s: 3600 }));
    expect(pending.retryAfterS).toBe(3600);
    expect(pending.message).toContain("about 1 hour");
    const rate = createErrorView(apiErr(429, "org_create_rate_limited", { retry_after_s: 90 }));
    expect(rate.retryAfterS).toBe(90);
    expect(rate.message).toContain("about 2 minutes");
    expect(createErrorView(apiErr(429, "org_create_rate_limited")).retryAfterS).toBeUndefined();
  });

  it("keeps the key only when the server may have made the team", () => {
    expect(createErrorView(new TypeError("fetch failed")).keepAttempt).toBe(true);
    expect(createErrorView(apiErr(503, "identity_unavailable")).keepAttempt).toBe(true);
    expect(createErrorView(apiErr(500)).keepAttempt).toBe(true);
    expect(createErrorView(apiErr(409, "create_in_progress")).keepAttempt).toBe(true);
    expect(createErrorView(apiErr(400, "invalid_name")).keepAttempt).toBe(false);
    expect(createErrorView(apiErr(409, "idempotency_key_reused")).keepAttempt).toBe(false);
    expect(createErrorView(apiErr(410, "checkout_expired")).keepAttempt).toBe(false);
  });

  it("resubmits create_in_progress by itself, and offers a fresh start on any 410 or unknown 409", () => {
    expect(createErrorView(apiErr(409, "create_in_progress")).autoRetryMs).toBeGreaterThan(0);
    expect(createErrorView(apiErr(410, "checkout_expired")).startOver).toBe(true);
    expect(createErrorView(apiErr(410, "some_new_gone_code")).startOver).toBe(true);
    expect(createErrorView(apiErr(409, "some_new_conflict")).startOver).toBe(true);
  });

  it("maps flag-off, signed-out and the bare return-URL 400", () => {
    expect(createErrorView(apiErr(404)).message).toMatch(/isn't open yet/);
    expect(createErrorView(apiErr(401)).message).toMatch(/signed out/);
    expect(createErrorView(apiErr(400)).message).toMatch(/checkout/);
  });

  it("S3: a code-less 400 keeps the key (the return-URL check runs after the team exists)", () => {
    expect(createErrorView(apiErr(400)).keepAttempt).toBe(true);
    // A coded 400 is still a refusal of this attempt.
    expect(createErrorView(apiErr(400, "invalid_seats")).keepAttempt).toBe(false);
  });

  it("checkout_complete links to the team's setup", () => {
    expect(createErrorView(apiErr(409, "checkout_complete")).statusLink).toBe(true);
    expect(resumeErrorView(apiErr(409, "checkout_complete")).statusLink).toBe(true);
  });
});

describe("resume and retry errors", () => {
  it("no_pending_checkout and not_a_team_org have copy, and neither reads as paid", () => {
    const none = resumeErrorView(apiErr(409, "no_pending_checkout"));
    expect(none.message).toMatch(/no sign-up waiting for payment/);
    expect(none.alreadyPaid).toBeUndefined();
    expect(none.message).not.toMatch(/went through/);
    const personal = resumeErrorView(apiErr(400, "not_a_team_org"));
    expect(personal.message).toMatch(/personal account/);
    expect(personal.message).not.toMatch(/checkout/i);
  });

  it("resume: expired → start over, complete → already paid", () => {
    expect(resumeErrorView(apiErr(410, "checkout_expired")).startOver).toBe(true);
    expect(resumeErrorView(apiErr(409, "checkout_complete")).alreadyPaid).toBe(true);
    expect(resumeErrorView(apiErr(404, "org_not_found")).message).toMatch(/created this team/);
  });

  it.each(["nothing_to_retry", "not_retryable", "already_set_up", "retry_rate_limited", "retry_limit_reached"])(
    "retry: %s has copy",
    (code) => {
      const v = retryErrorView(apiErr(code.startsWith("retry_") ? 429 : 409, code));
      expect(v.message).not.toMatch(/HTTP|_/);
    },
  );

  it("retry: honours retry_after_s", () => {
    expect(retryErrorView(apiErr(429, "retry_rate_limited", { retry_after_s: 540 })).retryAfterS).toBe(540);
  });
});

describe("setupView: status → screen", () => {
  it("checking before any answer, and keeps polling", () => {
    const v = setupView(input(null));
    expect(v).toMatchObject({ kind: "checking", poll: true });
  });

  it("provisioning shows the step and polls", () => {
    const v = setupView(input(status({ status: "provisioning", step: "migrate" })));
    expect(v).toMatchObject({ kind: "provisioning", stage: 1, poll: true, canRetry: false });
    expect(v.body).toContain(stepLabel("migrate"));
    expect(stepLabel("queued")).toMatch(/Getting ready/);
    expect(stepLabel("some_future_step")).toBe("Setting up your team");
  });

  it("waiting_for=update says we're updating", () => {
    const v = setupView(input(status({ status: "provisioning", step: "migrate", waiting_for: "update" })));
    expect(v.kind).toBe("updating");
    expect(v.body).toMatch(/We're updating, this takes a few minutes/);
    expect(v.poll).toBe(true);
  });

  it("waiting_for=payment, in either status, is a settling payment", () => {
    for (const st of ["provisioning", "awaiting_payment"] as const) {
      const v = setupView(input(status({ status: st, waiting_for: "payment" }), { elapsedMs: 10 * 60_000 }));
      expect(v).toMatchObject({ kind: "payment_settling", stage: 0, poll: true, canResume: false });
    }
  });

  it("ready is terminal at the last stage", () => {
    expect(setupView(input(status({ status: "ready" })))).toMatchObject({ kind: "ready", stage: 3, poll: false });
  });

  it("failed offers Retry only when retryable, and never polls", () => {
    expect(setupView(input(status({ status: "failed", retryable: true })))).toMatchObject({
      kind: "failed_retryable",
      canRetry: true,
      poll: false,
    });
    const hard = setupView(input(status({ status: "failed", retryable: false })));
    expect(hard).toMatchObject({ kind: "failed", canRetry: false, poll: false });
    expect(hard.body).toContain("person@example.com");
  });

  it("canceled and payment_abandoned are terminal with Start again", () => {
    expect(setupView(input(status({ status: "canceled" })))).toMatchObject({ kind: "canceled", canStartOver: true, poll: false });
    expect(setupView(input(status({ status: "payment_abandoned" })))).toMatchObject({
      kind: "abandoned",
      canStartOver: true,
      canResume: false,
      poll: false,
    });
  });

  it("awaiting_payment right after Stripe: confirm first, then offer Resume", () => {
    const until = new Date(NOW + 20 * 3600_000).toISOString();
    const s = status({ status: "awaiting_payment", resume_payment_until: until });
    expect(setupView(input(s, { elapsedMs: 0 }))).toMatchObject({ kind: "confirming_payment", canResume: false, poll: true });
    const later = setupView(input(s, { elapsedMs: PAYMENT_CONFIRM_GRACE_MS }));
    expect(later).toMatchObject({ kind: "resume", canResume: true, poll: true });
  });

  it("awaiting_payment on the cancel page offers Resume at once", () => {
    const until = new Date(NOW + 3600_000).toISOString();
    const v = setupView(input(status({ status: "awaiting_payment", resume_payment_until: until }), { page: "canceled" }));
    expect(v).toMatchObject({ kind: "resume", canResume: true });
    expect(v.title).toBe("Payment wasn't finished");
  });

  it("awaiting_payment past resume_payment_until is abandoned", () => {
    const v = setupView(
      input(status({ status: "awaiting_payment", resume_payment_until: new Date(NOW - 1).toISOString() }), { page: "canceled" }),
    );
    expect(v).toMatchObject({ kind: "abandoned", canResume: false, canStartOver: true });
  });

  it("awaiting_payment with no Checkout yet (no deadline) can still resume", () => {
    const v = setupView(input(status({ status: "awaiting_payment" }), { page: "canceled" }));
    expect(v.canResume).toBe(true);
  });

  it("S2: awaiting_payment on a team this browser didn't start never offers payment", () => {
    const s = status({ status: "awaiting_payment", resume_payment_until: new Date(NOW + 3600_000).toISOString() });
    for (const page of ["return", "canceled"] as const) {
      const v = setupView(input(s, { page, owned: false, elapsedMs: 0 }));
      expect(v.kind).toBe("not_ours");
      expect(v.canResume).toBe(false);
      expect(v.canOpen).toBe(true);
      expect(v.title).not.toMatch(/payment yet/);
    }
    // An older team with no sign-up record at all reads the same way.
    expect(setupView(input(status({ status: "awaiting_payment" }), { owned: false })).canResume).toBe(false);
  });

  it("S2: ready on a team this browser didn't start asks before opening", () => {
    const mine = setupView(input(status({ status: "ready" })));
    expect(mine.canOpen).toBe(false);
    const other = setupView(input(status({ status: "ready" }), { owned: false }));
    expect(other.kind).toBe("ready");
    expect(other.canOpen).toBe(true);
  });

  it("a 404 is not_found, terminal", () => {
    expect(setupView(input(null, { notFound: true }))).toMatchObject({ kind: "not_found", poll: false });
  });

  it("an unknown status from a newer backend keeps checking instead of claiming anything", () => {
    const v = setupView(input(status({ status: "something_new" as OrgSetupStatus["status"] })));
    expect(v).toMatchObject({ kind: "checking", poll: true });
  });
});

describe("nextPollDelay", () => {
  it("backs off from 1.5 s and caps at 10 s", () => {
    const v = { poll: true, kind: "provisioning" as const };
    expect(nextPollDelay(0, v)).toBe(1500);
    expect(nextPollDelay(1, v)).toBe(2250);
    expect(nextPollDelay(50, v)).toBe(POLL_MAX_MS);
  });

  it("slows further while waiting on a person or a deploy, and stops when terminal", () => {
    expect(nextPollDelay(50, { poll: true, kind: "updating" })).toBe(POLL_SLOW_MAX_MS);
    expect(nextPollDelay(50, { poll: true, kind: "resume" })).toBe(POLL_SLOW_MAX_MS);
    expect(nextPollDelay(0, { poll: false, kind: "ready" })).toBe(false);
  });
});

describe("Checkout copy (#1521)", () => {
  it("states $0 today, 3,000 credits, auto-convert, emails, the 90-day backstop, with N seats and the price", () => {
    const text = trialCopy(3, 2000).join(" ");
    expect(text).toContain("Your team gets 3,000 free credits to share, and you won't be charged today.");
    expect(text).toContain("or after 90 days, whichever comes first, your subscription starts automatically.");
    expect(text).toContain("We charge the card you add for 3 seats at $20 per seat per month");
    expect(text).toContain("We'll email you when 80% of the free credits are used, when they run out, and 7 days before the 90 days are up.");
    expect(text).toContain("One free trial per person and per card.");
    expect(text).toContain("Extra usage turns on after your first payment.");
  });

  it("repeat-trial copy", () => {
    expect(repeatTrialCopy(4, 1950)).toBe(
      "You've already used a free trial, so your subscription starts today: we'll charge 4 seats × $19.50 now.",
    );
    expect(repeatTrialCopy(4, null)).toMatch(/charge for 4 seats now/);
  });

  it("formats prices and waits", () => {
    expect(formatSeatPrice(2000)).toBe("$20");
    expect(formatSeatPrice(1999)).toBe("$19.99");
    expect(formatSeatPrice(null)).toBeNull();
    expect(formatWait(45)).toBe("45 seconds");
    expect(formatWait(61)).toBe("about 2 minutes");
  });
});

describe("trial banner", () => {
  const trial = (p: Partial<BillingTrial>): BillingTrial => ({
    state: "trialing",
    seats: 3,
    credits_total: 3000,
    credits_used: 120,
    ends_at: "2027-01-25T00:00:00Z",
    payment_failed: false,
    ...p,
  });

  it("trialing: credits left and the auto-convert note", () => {
    const v = trialBannerView(trial({}));
    expect(v?.text).toContain("Free trial: 2,880 of 3,000 credits left.");
    expect(v?.text).toContain("or on January 25, 2027");
    expect(v?.text).toContain("charged for 3 seats");
    expect(v).toMatchObject({ tone: "ok", dismissible: true });
  });

  it("warns, undismissibly, at 80% used", () => {
    expect(trialBannerView(trial({ credits_used: 2400 }))).toMatchObject({ tone: "warn", dismissible: false });
  });

  it("converting: declined card warns; a repeat card right after sign-up is explained", () => {
    expect(trialBannerView(trial({ state: "converting", payment_failed: true }))).toMatchObject({ tone: "warn" });
    expect(trialBannerView(trial({ state: "converting" }), true)?.text).toBe(CARD_ALREADY_TRIALED);
    expect(trialBannerView(trial({ state: "converted" }), true)?.text).toBe(CARD_ALREADY_TRIALED);
    expect(trialBannerView(trial({ state: "converted" }))).toBeNull();
  });

  it("no banner without a trial", () => {
    expect(trialBannerView(null)).toBeNull();
    expect(trialBannerView(trial({ state: "canceled" }))).toBeNull();
  });

  it("extra usage is refused while the trial is live", () => {
    for (const s of ["checkout_open", "trialing", "converting"]) expect(trialRefusesExtraUsage(trial({ state: s }))).toBe(true);
    for (const s of ["converted", "canceled"]) expect(trialRefusesExtraUsage(trial({ state: s }))).toBe(false);
    expect(trialRefusesExtraUsage(null)).toBe(false);
    expect(TRIAL_EXTRA_USAGE_COPY).toMatch(/after your free trial ends/);
  });
});

describe("A2's final error list (cac74c5c9)", () => {
  it("create_in_progress resends after the server's retry_after_s", () => {
    expect(createErrorView(apiErr(409, "create_in_progress", { retry_after_s: 7 })).autoRetryMs).toBe(7000);
    expect(createErrorView(apiErr(409, "create_in_progress")).autoRetryMs).toBe(2000);
    // Bounded, whatever the server says.
    expect(createErrorView(apiErr(409, "create_in_progress", { retry_after_s: 900 })).autoRetryMs).toBe(30_000);
  });

  it("signup_complete carries the org to follow, keeping the key", () => {
    const v = createErrorView(apiErr(409, "signup_complete", { org_id: "org_2abcDEF123" }));
    expect(v).toMatchObject({ statusOrgId: "org_2abcDEF123", keepAttempt: true });
    expect(v.startOver).toBeUndefined();
    expect(createErrorView(apiErr(409, "signup_complete")).statusOrgId).toBeUndefined();
  });

  it("checkout_complete is not an error to start over from", () => {
    const v = createErrorView(apiErr(409, "checkout_complete"));
    expect(v).toMatchObject({ keepAttempt: true });
    expect(v.startOver).toBeUndefined();
  });

  it("500 and the code-less 503 keep the key (the team may exist)", () => {
    expect(createErrorView(new ApiError("internal error", 500, null, null, null, { error: "internal error" })).keepAttempt).toBe(true);
    expect(
      createErrorView(new ApiError("service unavailable", 503, null, null, null, { error: "service unavailable" })),
    ).toMatchObject({ keepAttempt: true });
  });
});

describe("the switcher's pending teams", () => {
  const stored = [{ key: "key-aaaa1", name: "Acme", seats: 3, orgId: "org_pending1" }];
  it("S4: sends a pending team to its setup page only while sign-up is on", () => {
    expect(pendingTeamTarget("org_pending1", { selfserve: true, stored })).toContain("org=org_pending1");
    expect(pendingTeamTarget("org_pending1", { selfserve: false, stored })).toBeNull();
    expect(pendingTeamTarget("org_other01", { selfserve: true, stored })).toBeNull();
  });

  it("uses bootstrap's pending_orgs when present", () => {
    expect(pendingTeamTarget("org_server1", { selfserve: true, stored: [], serverPending: ["org_server1"] })).toBe("org=org_server1");
    expect(pendingTeamTarget("org_server1", { selfserve: true, stored: [], serverPending: undefined })).toBeNull();
  });
});

describe("return-page targets and resume seats", () => {
  it("?org= names a known team; junk is ignored", () => {
    expect(orgFromQuery(new URLSearchParams("org=org_2abcDEF123"))).toBe("org_2abcDEF123");
    expect(orgFromQuery(new URLSearchParams("org=../../x"))).toBeNull();
    expect(orgFromQuery(new URLSearchParams(""))).toBeNull();
    const q = new URLSearchParams(statusQuery("org_2abcDEF123", { key: "key-dddd1", name: "Acme", seats: 4 }));
    expect(q.get("org")).toBe("org_2abcDEF123");
    expect(attemptFromQuery(q)).toEqual({ key: "key-dddd1", name: "Acme", seats: 4 });
    expect(statusQuery("org_2abcDEF123")).toBe("org=org_2abcDEF123");
  });

  it("resume seats come from the status body first", () => {
    expect(resumeSeats({ seats: 5 }, { seats: 3 })).toBe(5);
    expect(resumeSeats({ seats: null }, { seats: 3 })).toBe(3);
    expect(resumeSeats({}, null)).toBeNull();
    expect(resumeSeats(null, null)).toBeNull();
  });
});

describe("the provisioning gate", () => {
  it("recognises the gate's provisioning 503 and not an outage", () => {
    expect(isProvisioningGate(new ApiError("service unavailable", 503, null, null, null, { status: "provisioning" }))).toBe(true);
    expect(isProvisioningGate(new ApiError("service unavailable", 503, null, null, null, {}))).toBe(false);
  });
});

import { describe, expect, it } from "vitest";
import {
  bootFailureActions,
  bootFailureOutcome,
  bootRecordForFailure,
  isGatingAttempt,
  shouldAttemptBoot,
  type BootRecord,
} from "./bootGate";
import { ApiError } from "./api";
import { UnexpectedEmptyOrgError } from "./oceanBootstrap";

const PERSONAL = "__personal__";
const ORG_A = "org_a";
const ORG_B = "org_b";

describe("shouldAttemptBoot — the loop fix", () => {
  it("fires on the very first identity ever seen (last === null)", () => {
    expect(shouldAttemptBoot(null, PERSONAL)).toBe(true);
    expect(shouldAttemptBoot(null, ORG_A)).toBe(true);
  });

  it("fires for a genuinely NEW identity (org switch / different user), regardless of the prior outcome", () => {
    const succeededA: BootRecord = { identityKey: ORG_A, outcome: "success" };
    const failedA: BootRecord = { identityKey: ORG_A, outcome: "error" };
    expect(shouldAttemptBoot(succeededA, ORG_B)).toBe(true);
    expect(shouldAttemptBoot(failedA, ORG_B)).toBe(true);
    expect(shouldAttemptBoot(succeededA, PERSONAL)).toBe(true);
  });

  it("does NOT re-fire for the SAME identity that already succeeded (background Clerk revalidation is a no-op here)", () => {
    const settled: BootRecord = { identityKey: ORG_A, outcome: "success" };
    expect(shouldAttemptBoot(settled, ORG_A)).toBe(false);
  });

  // THE ACTUAL BUG: before this predicate existed, only success was ever
  // recorded, so a permanently-failing identity looked identical to "never
  // attempted" on every subsequent effect re-fire — an unbounded re-arm that
  // manifested in production as a "Maximum update depth exceeded" crash on an
  // org host whose bootstrap request the backend rejects. This is the
  // regression test for that: an identity settled as an ERROR must ALSO be
  // refused a re-fire from this predicate — that's what makes each identity
  // bounded to exactly one automatic attempt.
  it("does NOT re-fire for the SAME identity that already settled as an error (the loop fix)", () => {
    const settled: BootRecord = { identityKey: ORG_A, outcome: "error" };
    expect(shouldAttemptBoot(settled, ORG_A)).toBe(false);
  });

  it("stays false across arbitrarily many repeated calls for a failed identity — bounded, not just one-shot-then-forgotten", () => {
    const settled: BootRecord = { identityKey: ORG_A, outcome: "error" };
    for (let i = 0; i < 1000; i++) {
      expect(shouldAttemptBoot(settled, ORG_A)).toBe(false);
    }
  });
});

describe("isGatingAttempt", () => {
  it("gates (returns true) when nothing has settled yet", () => {
    expect(isGatingAttempt(null, PERSONAL)).toBe(true);
  });

  it("gates for a different identity than the one on record", () => {
    const settled: BootRecord = { identityKey: ORG_A, outcome: "success" };
    expect(isGatingAttempt(settled, ORG_B)).toBe(true);
    expect(isGatingAttempt(settled, PERSONAL)).toBe(true);
  });

  it("does NOT gate the SAME identity that already succeeded (quiet background retry against a good UI)", () => {
    const settled: BootRecord = { identityKey: ORG_A, outcome: "success" };
    expect(isGatingAttempt(settled, ORG_A)).toBe(false);
  });

  it("DOES gate the SAME identity that previously errored (nothing good is on screen for it)", () => {
    const settled: BootRecord = { identityKey: ORG_A, outcome: "error" };
    expect(isGatingAttempt(settled, ORG_A)).toBe(true);
  });
});

describe("shouldAttemptBoot / isGatingAttempt together — the terminal-state contract", () => {
  it("a failed identity reaches a stable terminal state: no auto re-fire, but a bypassing retry still gates", () => {
    let last: BootRecord | null = null;
    // First automatic attempt (identity effect): fires, and gates.
    expect(shouldAttemptBoot(last, ORG_A)).toBe(true);
    expect(isGatingAttempt(last, ORG_A)).toBe(true);
    // ... attempt fails, settles as error:
    last = { identityKey: ORG_A, outcome: "error" };
    // Any number of further automatic (identity-effect) re-fires for the
    // SAME identity must be no-ops — this is "never re-arms".
    expect(shouldAttemptBoot(last, ORG_A)).toBe(false);
    expect(shouldAttemptBoot(last, ORG_A)).toBe(false);
    // An explicit retry path (focus self-heal / retry button) bypasses
    // `shouldAttemptBoot` entirely (it calls `attemptBoot` directly per
    // providers.tsx) but still consults `isGatingAttempt` to decide whether
    // to show the full-screen gate — which stays true for a known-bad identity.
    expect(isGatingAttempt(last, ORG_A)).toBe(true);
  });

  it("a succeeded identity also reaches a stable terminal state, but non-gating", () => {
    const last: BootRecord = { identityKey: PERSONAL, outcome: "success" };
    expect(shouldAttemptBoot(last, PERSONAL)).toBe(false);
    expect(isGatingAttempt(last, PERSONAL)).toBe(false);
  });
});

// THE SECOND TRAP, SAME INCIDENT CLASS: `attemptBoot`'s `catch` used to call
// `setBootError` and NOTHING else — never `setBooted(true)`. `ready` in
// `providers.tsx` is `isLoaded && booted`, and `app/(app)/layout.tsx`'s only
// redirect to `/login` is `if (ready && !token) router.replace("/login")`.
// So for a Clerk session the SDK still considers signed-in, but whose bearer
// the backend flatly rejects (401, and it survives `lib/api.ts`'s one
// refresh-and-retry — see `req()`), `ready` stayed false forever: the user
// sat on the boot-error screen, whose only control ("retry") just re-ran the
// identical, permanently-failing boot. No path back to /login. Observed in
// production 2026-09-09. `bootFailureOutcome` is the pure decision of
// whether a thrown boot-chain error is that terminal signed-out verdict
// (`ready` must be allowed to become true so the redirect can fire) or an
// ordinary retryable failure (today's behavior: error screen + retry stays
// available, `ready` stays false).
describe("bootFailureOutcome — the signed-out-with-no-way-back trap", () => {
  it("classifies a 401 that survived api.ts's refresh-and-retry as signed-out", () => {
    expect(bootFailureOutcome(new ApiError("missing token", 401))).toEqual({
      type: "signed-out",
    });
    expect(bootFailureOutcome(new ApiError("HTTP 401", 401))).toEqual({ type: "signed-out" });
  });

  it("keeps a 403 (tenancy refusal) retryable — never signs the user out of a working session", () => {
    expect(bootFailureOutcome(new ApiError("organization is not active", 403))).toEqual({
      type: "retryable-error",
    });
  });

  it("keeps other HTTP statuses retryable", () => {
    for (const status of [400, 404, 500, 503]) {
      expect(bootFailureOutcome(new ApiError("something else", status))).toEqual({
        type: "retryable-error",
      });
    }
  });

  it("keeps a network/timeout Error retryable (no status to classify)", () => {
    expect(bootFailureOutcome(new Error("boot timed out after 15000ms"))).toEqual({
      type: "retryable-error",
    });
  });

  it("keeps UnexpectedEmptyOrgError retryable (an org-tenancy condition, not an auth one)", () => {
    expect(bootFailureOutcome(new UnexpectedEmptyOrgError())).toEqual({
      type: "retryable-error",
    });
  });

  it("keeps a non-Error thrown value retryable", () => {
    expect(bootFailureOutcome("not an error at all")).toEqual({ type: "retryable-error" });
    expect(bootFailureOutcome(undefined)).toEqual({ type: "retryable-error" });
  });
});

// The loop-fix contract (see the module doc atop `bootGate.ts`) depends on
// EVERY settled boot attempt — success or failure, and now failure has two
// flavors — writing a `BootRecord` into `bootLastSettledRef`, or the identity
// effect has no memory that this identity was ever tried and re-arms it.
// `bootRecordForFailure` is the one place that turns a `bootFailureOutcome`
// into the record `providers.tsx` writes, so a signed-out verdict cannot
// accidentally be wired up WITHOUT also feeding the loop-fix ref — there is
// no second call site to forget.
describe("bootRecordForFailure — every failure outcome still settles the identity", () => {
  it("records a signed-out verdict as a settled (error-flavored) record for the identity", () => {
    expect(bootRecordForFailure(ORG_A, { type: "signed-out" })).toEqual({
      identityKey: ORG_A,
      outcome: "error",
    });
  });

  it("records an ordinary retryable failure the same way (both are 'not currently bootable')", () => {
    expect(bootRecordForFailure(ORG_A, { type: "retryable-error" })).toEqual({
      identityKey: ORG_A,
      outcome: "error",
    });
  });

  it("once recorded, the loop fix refuses to re-fire for that identity regardless of which failure flavor settled it", () => {
    const signedOutRecord = bootRecordForFailure(PERSONAL, { type: "signed-out" });
    expect(shouldAttemptBoot(signedOutRecord, PERSONAL)).toBe(false);
    // and still gates a future different identity, same as any other error record
    expect(isGatingAttempt(signedOutRecord, PERSONAL)).toBe(true);
  });
});

// THE THIRD TRAP, FOUND BEFORE SHIPPING: the seemingly-obvious fix for
// "signed out" — have `attemptBoot`'s catch call `setToken(null);
// setBooted(true)` itself, mirroring the identity effect's real
// `!isSignedIn` branch — is an infinite redirect loop. `app/login/page.tsx`
// redirects `/login` -> `/ask` whenever Clerk's `isSignedIn` is true, and a
// 401-survived-retry means the BACKEND disagrees with Clerk, not that Clerk
// itself is signed out: `isSignedIn` stays true. Faking the signed-out UI
// state while `isSignedIn` stays true sends `/login` -> `/ask` -> (layout's
// `ready && !token` redirect) -> `/login` -> ... forever. The only exit is
// ending the REAL Clerk session (`signOut()`) so `isSignedIn` itself flips
// false. `bootFailureActions` is the assertable version of "this catch may
// never set `booted` itself" — a test, not just a comment, so flipping
// `markBooted` to `true` for `"signed-out"` fails CI instead of shipping.
describe("bootFailureActions — the catch may never fake the signed-out UI state itself", () => {
  it("a signed-out verdict signs out of Clerk for real, and never marks booted itself", () => {
    expect(bootFailureActions({ type: "signed-out" })).toEqual({
      signOutClerk: true,
      markBooted: false,
    });
  });

  it("a retryable error never signs out of Clerk and never marks booted itself", () => {
    expect(bootFailureActions({ type: "retryable-error" })).toEqual({
      signOutClerk: false,
      markBooted: false,
    });
  });

  it("markBooted is false for every outcome — the invariant the redirect loop depends on", () => {
    for (const outcome of [{ type: "signed-out" }, { type: "retryable-error" }] as const) {
      expect(bootFailureActions(outcome).markBooted).toBe(false);
    }
  });
});

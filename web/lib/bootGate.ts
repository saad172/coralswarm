import { ApiError } from "./api";

// Pure boot-attempt gating for `lib/providers.tsx`'s `ClerkAuthProvider` —
// decides whether the identity effect should (re-)invoke `attemptBoot` for a
// given Clerk identity, and whether that invocation should be "gating" (drop
// the UI to the surfacing/error screen) or run quietly in the background.
// Extracted so this decision — previously living only in effect-dependency
// wiring — gets full unit coverage; see `lib/oceanBootstrap.ts` for the
// companion plane-decision half of the same incident.
//
// THE ACTUAL LOOP THIS FIXES ("Maximum update depth exceeded"):
//
// Before this module existed, the identity effect computed whether to (re-)
// attempt a boot from ONE ref, `bootedIdentityRef`, which was written ONLY on
// SUCCESS:
//
//   const gating = bootedIdentityRef.current === null
//     || bootedIdentityRef.current !== identityKey;
//   if (gating) { setBooted(false); setBootError(null); }
//   void attemptBoot(identityKey, gating);
//
// and the effect re-ran on `[isLoaded, isSignedIn, orgId, orgSlug,
// attemptBoot, resetOcean]`. For an identity that boots successfully, one
// pass sets `bootedIdentityRef` and every later re-fire (Clerk revalidating
// the session on focus, `attemptBoot`'s own identity churning because it
// depends on `getToken`, etc.) computes `gating: false` and is a quiet no-op
// — fine.
//
// For an identity that NEVER succeeds — exactly an org host whose bootstrap
// request the backend rejects (`cannot create a personal ocean in an
// organization`, ADR-0007 D2) — `bootedIdentityRef` never gets written, so
// EVERY re-fire of that effect, for ANY reason, still computes `gating:
// true`, resets `setBooted(false)`/`setBootError(null)` (state updates), and
// fires a brand new `attemptBoot`, whose `catch` unconditionally calls
// `setBootError(...)` again (another state update) once the attempt fails.
// Two state-setting passes per re-fire, and nothing in that path ever marks
// the identity as "already tried" — only "already succeeded". Any dependency
// churn on that effect (worst case: `attemptBoot`'s own identity, which rides
// transitively on Clerk's `getToken` reference) re-arms the doomed attempt
// from scratch. That update-triggers-effect-triggers-update cycle, repeating
// with no memory of the prior failure, is exactly what React's nested-update
// counter flags as "Maximum update depth exceeded" once it fires enough times
// inside one flush — a single rejected boot request took the whole page down
// instead of landing on a terminal, retryable error.
//
// THE FIX: track the last SETTLED outcome per identity — success OR error,
// not success alone — and refuse to let the identity effect re-invoke
// `attemptBoot` for an identity that has already settled, for ANY reason
// short of it becoming a genuinely different identity. Reaching a settled
// outcome for identity X is a one-way door for the effect: only an explicit,
// human-triggered retry (the "retry" button, or the focus/visibility
// self-heal — both call `attemptBoot` directly, bypassing this gate on
// purpose) can attempt X again. That makes every automatic path through the
// identity effect bounded at exactly one attempt per identity, structurally,
// regardless of how many times the effect itself re-runs.

/** One identity's last known SETTLED result. `null` means "nothing has
 *  settled yet for any identity" (fresh sign-in / fresh mount). */
export interface BootRecord {
  identityKey: string;
  outcome: "success" | "error";
}

/**
 * Should the identity effect invoke a brand-new `attemptBoot` for
 * `identityKey` right now, given the last settled record (or `null`)?
 *
 * True only when `identityKey` has never been seen before — i.e. `last` is
 * `null`, or belongs to a DIFFERENT identity (a genuine org switch / sign-in
 * as someone else). False once `identityKey` itself has a settled record,
 * REGARDLESS of whether that record is `"success"` or `"error"` — a repeat
 * effect fire for an identity that already succeeded is Clerk's focus-driven
 * session revalidation (handled by the token-refresh interval effect
 * instead, see `providers.tsx`); a repeat fire for an identity that already
 * failed is the exact loop described above, and must be a no-op here.
 *
 * This function has no notion of "retry" — deliberately. Retrying a settled
 * identity is a decision the CALLER makes explicitly (button click, focus
 * self-heal), never something this predicate grants on its own, so a caller
 * can't accidentally wire it back into an effect dependency array and
 * reintroduce the loop.
 */
export function shouldAttemptBoot(last: BootRecord | null, identityKey: string): boolean {
  return last === null || last.identityKey !== identityKey;
}

/**
 * Should THIS attempt for `identityKey` be "gating" — i.e. drop the UI into
 * the "surfacing…" / error screen — or run quietly against the
 * already-rendering app?
 *
 * False only when `identityKey` already has a SUCCESSFUL settled record
 * (the already-booted UI is good; a background revalidation or a self-heal
 * retry after a later transient hiccup should not flash the gate). True in
 * every other case: never settled, settled as an error (nothing good is
 * currently on screen for this identity), or a different identity than the
 * one on record (a real identity change always gates).
 */
export function isGatingAttempt(last: BootRecord | null, identityKey: string): boolean {
  return !(last !== null && last.identityKey === identityKey && last.outcome === "success");
}

// ── THE SECOND TRAP, SAME INCIDENT CLASS ────────────────────────────────────
//
// `attemptBoot`'s `catch` (`providers.tsx`) used to call `setBootError(...)`
// and NEVER `setBooted(true)`, for any failure. `ready` is `isLoaded &&
// booted`, and `app/(app)/layout.tsx`'s only redirect to `/login` is
// `if (ready && !token) router.replace("/login")` — gated on `ready`. A
// signed-out user normally never hits `attemptBoot` at all (the identity
// effect's `!isSignedIn` branch handles that separately and DOES set
// `booted`), but when Clerk still believes the session is valid and the
// BACKEND rejects the bearer outright — a 401 that survives `lib/api.ts`'s
// one refresh-and-retry (`req()`'s
// `if (res.status === 401 && !retried && token && tokenRefresher)`) — `ready`
// stayed false forever. The user sat on the boot-error screen, whose only
// control ("retry") just re-ran the identical, permanently-failing boot: no
// path back to /login. Observed in production 2026-09-09.
//
// A 401 that survives that retry already ruled out the transient case (stale
// token), so it is a genuine, terminal auth failure — settle it as SIGNED
// OUT, not as a retryable error. Every other failure (403 tenancy refusals,
// network errors, the boot watchdog's timeout, `UnexpectedEmptyOrgError`,
// any other status) keeps today's behavior exactly: a retryable error,
// `ready` stays false, the retry button re-runs `attemptBoot`.
//
// A THIRD TRAP, FOUND ON REVIEW BEFORE SHIPPING: the obvious-looking fix for
// "signed out" — have this catch call `setToken(null); setBooted(true)`
// itself, exactly like the identity effect's real `!isSignedIn` branch does
// — creates an infinite redirect loop, and is *worse* than the stuck error
// screen it replaces. `app/login/page.tsx`'s `WebLogin` redirects
// `/login` → `/home` whenever Clerk's `isSignedIn` is true. A 401-survived-
// retry means the BACKEND disagrees with Clerk, not that Clerk itself is
// signed out — `isSignedIn` stays true throughout. So faking the signed-out
// UI state while `isSignedIn` stays true sends `/login`'s effect straight
// back to `/home`, whose layout re-checks `ready && !token`, redirects to
// `/login` again, forever. The only way to close the loop is to actually END
// the Clerk session (`signOut()`, already available via `useAuth()` in
// `providers.tsx`) so `isSignedIn` itself flips false — only then does
// `/login` render `<SignIn>` instead of bouncing away from itself, and only
// then should this catch's `(ready, token)` state change at all. That is why
// `BootFailureActions` below never grants `markBooted: true`: NOTHING in
// this catch is allowed to synthesize the signed-out UI state directly: the
// only lever it pulls is ending the real session and waiting for the
// identity effect's `!isSignedIn` branch (which already does the right
// thing) to react to that.

/** What `attemptBoot`'s `catch` should DO to `(ready, token)` for a thrown
 *  boot-chain error — distinct from `bootErrorCopy.ts::bootErrorMessage`,
 *  which only decides what COPY to show and never influences state. */
export type BootFailureOutcome = { type: "signed-out" } | { type: "retryable-error" };

/**
 * Classify a thrown boot-chain error. Only a 401 `ApiError` — which can only
 * reach here after `lib/api.ts`'s single refresh-and-retry already failed —
 * is `"signed-out"`; everything else (403, other statuses, network errors,
 * the watchdog timeout, `UnexpectedEmptyOrgError`, non-`Error` throws) is
 * `"retryable-error"`.
 */
export function bootFailureOutcome(err: unknown): BootFailureOutcome {
  if (err instanceof ApiError && err.status === 401) return { type: "signed-out" };
  return { type: "retryable-error" };
}

/**
 * The `BootRecord` `attemptBoot`'s catch must write into `bootLastSettledRef`
 * for a given failure, regardless of which `BootFailureOutcome` fired — the
 * loop fix (this file's module doc) cares only THAT an identity settled, not
 * how. Routing both flavors through one function means a future third
 * `BootFailureOutcome` variant can't be added to `attemptBoot` without also
 * deciding how it feeds the loop-fix ref; there is no second call site to
 * forget it at.
 *
 * Both flavors record `outcome: "error"` — a signed-out verdict leaves
 * nothing bootable on screen for this identity any more than an ordinary
 * retryable failure does, which is exactly what `"error"` means to
 * `isGatingAttempt`. Only the success path in `attemptBoot`'s `try` block
 * ever writes `"success"`.
 */
export function bootRecordForFailure(identityKey: string, _outcome: BootFailureOutcome): BootRecord {
  return { identityKey, outcome: "error" };
}

/** What `attemptBoot`'s `catch` should DO, beyond recording the settled
 *  outcome, for a given `BootFailureOutcome` — see the "THIRD TRAP" note
 *  above for why this exists as its own testable decision rather than being
 *  left as an unchecked comment at the call site. */
export interface BootFailureActions {
  /** End the real Clerk session (`void signOut().catch(() => {})`). The only
   *  lever a "signed-out" verdict is allowed to pull: ending the session is
   *  what flips Clerk's `isSignedIn`, which is what lets the identity
   *  effect's own `!isSignedIn` branch set `token`/`booted` afterward. */
  signOutClerk: boolean;
  /** Should this catch call `setBooted(true)` itself? ALWAYS `false` — for
   *  BOTH outcomes. Kept as an explicit field (not just "this function never
   *  returns that key") so a future edit that flips it to `true` for
   *  `"signed-out"` — reintroducing the redirect loop described above —
   *  fails a unit test instead of silently passing review. */
  markBooted: boolean;
}

export function bootFailureActions(outcome: BootFailureOutcome): BootFailureActions {
  if (outcome.type === "signed-out") return { signOutClerk: true, markBooted: false };
  return { signOutClerk: false, markBooted: false };
}

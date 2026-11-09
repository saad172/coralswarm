// Whether the private engine may act at all, and with WHICH bearer — as a pure
// function of (who Clerk says is signed in now, which bearer we are holding).
// No I/O and no React, same reason `vaultNamespace.ts` is shaped this way:
// `web/` has no component-test harness (vitest runs in `node` and its glob is
// `.test.ts`), so anything that must not regress lives in a plain module and
// `provider.tsx` just executes the answer.
//
// ── WHAT WAS WRONG: THE SESSION'S STORE AND ITS TOKEN CAME FROM DIFFERENT
//    CLOCKS ─────────────────────────────────────────────────────────────────
//
// PR 2.4 named the on-device store per (user, org) and keyed `EngineSession` on
// that name, so an org switch tears the session down and builds a fresh one.
// That fixed WHICH DATABASE the session opens. It did not fix WHICH TOKEN the
// session acts with, and those two values are produced by different mechanisms
// that do not change at the same instant:
//
//   * the database name comes from `useAuth()`'s `orgId` — Clerk's own live
//     session state, which flips SYNCHRONOUSLY the moment `setActive` lands;
//   * the token comes from `useApp()`, i.e. `ClerkAuthProvider`'s `token`
//     REACT STATE, which is only ever written after `await getToken()` inside
//     `attemptBoot` (or the 40s refresh effect). Both are async, and for a
//     newly-active org `getToken()` cannot be served from Clerk's cache — the
//     cached JWT carries the PREVIOUS `org_id` claim — so it is a real network
//     round trip to Clerk.
//
// So on an A → B switch there is a committed render where `orgId` is already B
// and `token` is still A's. React then mounts a BRAND-NEW `EngineSession`
// (the `key` changed) whose `dbName`/`latchKey` name org B and whose `token`
// authorizes org A, and that session's mount effect runs immediately —
// child effects flush before the ancestor `ClerkAuthProvider` effect that would
// drop the UI to "surfacing…". The window is not a microtask; it is however
// long Clerk takes to mint B's token.
//
// WHAT THAT COST, CONCRETELY. Tenancy rides the token's `org_id` claim alone
// (docs/plans/claim-based-tenancy.md), so every request that session makes in
// that window is executed against the WRONG ACCOUNT:
//
//   * `escrowEnroll` is the severe one. It reads the ORG recovery public key
//     (`GET /v1/escrow/recovery-pubkey`, `WHERE account_id = <token's org>`),
//     wraps the key held in THIS session's store to it, and stores the blob
//     (`PUT /v1/escrow/wrapped-key`, `(account_id, user_id)`). With a stale
//     token that wraps org B's on-device key to org A's recovery public key
//     and files it under org A — i.e. org A's admins, at their M-of-N
//     threshold, reconstruct A's recovery private key and unwrap a key that
//     belongs to a different organization's private ocean.
//   * The escrow latch is set for org B (`sessionStorage`) BEFORE that call
//     and is never cleared, so the enrollment that should have gone to org B
//     never happens — and `sessionStorage` survives `window.location.reload()`,
//     so the switch's own reload does not heal it. That is exactly the failure
//     `escrowCheckKey` was introduced to remove ("an admin recovery that
//     everyone believed was configured would come up empty"), re-entered
//     through a different door.
//   * `api.privateKey.get` is fetched for org B's store using org A's bearer.
//     That endpoint happens to be keyed by the CONTROL-PLANE user id today
//     (`user_private_keys.user_id`, one row per Clerk user), so the response is
//     the same either way at this instant — which is an accident of a
//     server-side table shape, not a property this component is entitled to
//     rely on. `escrow_wrapped_keys` next door is already `(account_id,
//     user_id)`; the day the private-key blob follows it, this same line
//     becomes a cross-tenant read with no change here.
//
// ── THE FIX: A SESSION'S STORE AND ITS TOKEN MUST NAME THE SAME IDENTITY ────
//
// `engineSessionFor` refuses to describe a session at all unless the bearer's
// own claims name the same (user, org) the store is named for. `provider.tsx`
// renders no `EngineSession` when it returns null, so during the switch window
// there is simply no session — not a session holding a foreign token — and the
// engine mounts the instant B's token lands. `EngineSession`'s `token` prop is
// consequently NON-NULLABLE: the type is what stops a future edit from
// threading the raw `useApp()` token back in.
//
// The comparison goes through `vaultIdentityKey`, the same function that builds
// the database name, rather than comparing `orgId` strings directly. That is
// deliberate: two sides that encode identity independently can drift apart, and
// a drift here fails OPEN (a token accepted for the wrong store). Sharing the
// encoder makes "the store's identity" and "the token's identity" literally the
// same string function, so they cannot disagree.
//
// ── READING A CLAIM OUT OF AN UNVERIFIED JWT IS SOUND *HERE* ────────────────
//
// This module base64url-decodes the token payload without verifying the
// signature, which would be indefensible if the claim were used to GRANT
// anything. It is only ever used to REFUSE: a claim that matches buys the
// holder nothing the backend does not independently re-verify against Clerk's
// JWKS, and a forged claim only lets an attacker use a token they already had.
// The direction of the tradeoff is what makes it safe — every parse failure,
// every shape we do not recognize, returns null and the engine stays unmounted.
// "The private engine does not mount" is the app's shipping configuration
// anyway (`NEXT_PUBLIC_ENGINE` is set nowhere), so a false refusal costs
// nothing and a false acceptance costs a cross-tenant key.

import {
  escrowCheckKey,
  vaultDatabaseName,
  vaultIdentityKey,
  type VaultIdentity,
} from "./vaultNamespace";

/** Everything one `EngineSession` needs, and the proof that the three agree:
 *  `dbName` and `latchKey` are named for an identity, and `token` is a bearer
 *  whose own claims name that SAME identity. Only `engineSessionFor` may
 *  construct one. */
export interface EngineSessionSpec {
  /** IndexedDB database for this identity's private-ocean store. */
  dbName: string;
  /** `sessionStorage` key latching this identity's org-escrow re-check. */
  latchKey: string;
  /** A bearer bound to that identity. Never null: a session without a bound
   *  token is not a session, it is the null context. */
  token: string;
}

/** The subset of a Clerk session JWT this module reads. Everything is
 *  `unknown` because the payload is attacker-shaped until proven otherwise —
 *  a token is just a string until each field has been checked. */
interface RawClaims {
  sub?: unknown;
  org_id?: unknown;
  /** Clerk v2's nested organization claim, `{ id, slg, rol }`. */
  o?: unknown;
}

/** A non-empty string, or null. Every claim read goes through this so an
 *  empty string is never mistaken for a present value (Clerk ids are never
 *  empty, and `vaultIdentityKey` uses the empty org segment as its "personal
 *  instance" sentinel — an empty org id would land on it). */
function nonEmptyString(v: unknown): string | null {
  return typeof v === "string" && v !== "" ? v : null;
}

/** Decode one base64url JWT segment as JSON, or null if it is not that. */
function decodeSegment(segment: string): RawClaims | null {
  try {
    const b64 = segment.replace(/-/g, "+").replace(/_/g, "/");
    const padded = b64 + "=".repeat((4 - (b64.length % 4)) % 4);
    const binary = atob(padded);
    const bytes = new Uint8Array(binary.length);
    for (let i = 0; i < binary.length; i++) bytes[i] = binary.charCodeAt(i);
    const parsed: unknown = JSON.parse(new TextDecoder().decode(bytes));
    if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) return null;
    return parsed as RawClaims;
  } catch {
    // Malformed base64, malformed UTF-8, malformed JSON — all the same answer.
    return null;
  }
}

/** Sentinel for "there IS an organization claim here but it is not a shape we
 *  can read". Distinct from `null` (= no organization, i.e. the personal
 *  instance) because conflating the two is precisely how this fails open. */
const UNREADABLE = Symbol("unreadable-org-claim");

/**
 * The organization a token is scoped to: a non-empty id, `null` for the
 * personal instance, or `UNREADABLE`.
 *
 * MIRRORS THE BACKEND'S PRECEDENCE EXACTLY (`backend/src/clerk.rs`'s
 * `normalize_v2_org_claim`): the v1 top-level `org_id` wins as a group, and
 * the v2 nested `o` claim is consulted ONLY when `org_id` is absent. A client
 * that disagreed with the server about which claim carries tenancy would be
 * deciding "whose store is this?" from a different field than the one the
 * request is actually executed under.
 *
 * THE FAIL-OPEN THIS EXISTS TO PREVENT. An org claim we fail to recognize
 * reads as "no org", and "no org" is a real identity — the user's personal
 * instance — so an unrecognized org token would be ACCEPTED for the personal
 * session. That is not hypothetical: Clerk shipped exactly this shape change
 * once already, and the backend's own v1-only decoder silently served every v2
 * org token as a personal-plane request until it was caught (see the
 * `ClerkClaims` doc comment). So a `o` claim that is present but yields no
 * usable id is `UNREADABLE`, never personal. A future v3 under a third key
 * name cannot be enumerated in advance, which is why the doc on
 * `tokenIdentityKey` says to extend this function the same day Clerk's claim
 * shape moves.
 */
function orgFromClaims(claims: RawClaims): string | null | typeof UNREADABLE {
  // v1: `org_id` present and usable.
  const v1 = nonEmptyString(claims.org_id);
  if (v1 !== null) return v1;
  // `org_id` present but unusable (empty string, number, object). Absent or
  // explicitly null falls through to `o`, which is what Clerk actually emits.
  if (claims.org_id !== undefined && claims.org_id !== null) return UNREADABLE;

  // v2: the nested `o` claim.
  if (claims.o === undefined || claims.o === null) return null; // genuinely personal
  if (typeof claims.o !== "object" || Array.isArray(claims.o)) return UNREADABLE;
  const id = nonEmptyString((claims.o as { id?: unknown }).id);
  return id ?? UNREADABLE;
}

/**
 * The identity a bearer actually authorizes, in `vaultIdentityKey`'s encoding
 * so it is directly comparable to the identity a store is named for — or
 * `null` when that cannot be established, which callers must read as "do not
 * use this token", never as "use it anyway".
 *
 * Null covers every one of: no token, not three dot-separated segments, a
 * payload that is not base64url JSON, no usable `sub`, and an organization
 * claim in a shape this module does not recognize. All of them are the same
 * decision because all of them mean the same thing — we cannot say whose token
 * this is, so nothing may be done with it.
 *
 * IF CLERK'S CLAIM SHAPE MOVES AGAIN, `orgFromClaims` is the function to
 * extend, and the test file's "an organization token is never accepted for the
 * personal instance" case is the one that must be extended alongside it.
 */
export function tokenIdentityKey(token: string | null | undefined): string | null {
  if (typeof token !== "string") return null;
  const segments = token.split(".");
  if (segments.length !== 3) return null;
  const claims = decodeSegment(segments[1]);
  if (claims === null) return null;
  const sub = nonEmptyString(claims.sub);
  if (sub === null) return null;
  const org = orgFromClaims(claims);
  if (org === UNREADABLE) return null;
  // Routed through the SAME encoder the database name uses, so the two strings
  // are comparable by construction rather than by convention. `isLoaded: true`
  // is not an assumption about Clerk — a decoded token is, by definition, a
  // resolved identity.
  return vaultIdentityKey({ isLoaded: true, userId: sub, orgId: org });
}

/**
 * The engine session for this (identity, bearer) pair, or `null` when there
 * must not be one.
 *
 * Null in three cases, which the caller treats identically (render the null
 * context — `useEngine()` returns null, `EngineGate` passes children through,
 * and the app behaves exactly as it does with the engine switched off):
 *
 *   1. no identity yet (Clerk unloaded / signed out) — there is no store to
 *      name, and naming a default one is the bug `vaultNamespace` exists to
 *      prevent;
 *   2. no usable bearer;
 *   3. THE ONE THIS MODULE WAS ADDED FOR — a bearer that is perfectly valid
 *      but authorizes a DIFFERENT identity than the store is named for. That
 *      is the steady state for the whole of an org switch, because Clerk's
 *      `orgId` moves synchronously and the app's token does not.
 *
 * Case 3 is a REFUSAL, not a fallback: there is deliberately no "use it
 * anyway", no "close enough if the user matches", and no path that keeps the
 * previous session alive under the new identity. A session that cannot prove
 * its bearer and its store name the same identity does not exist.
 */
export function engineSessionFor(
  identity: VaultIdentity,
  token: string | null | undefined,
): EngineSessionSpec | null {
  const dbName = vaultDatabaseName(identity);
  const latchKey = escrowCheckKey(identity);
  const wanted = vaultIdentityKey(identity);
  if (dbName === null || latchKey === null || wanted === null) return null;
  const held = tokenIdentityKey(token);
  if (held === null || held !== wanted) return null;
  // `token` is a non-empty string here: `tokenIdentityKey` returned non-null,
  // which it only does for a three-segment string with a decodable payload.
  return { dbName, latchKey, token: token as string };
}

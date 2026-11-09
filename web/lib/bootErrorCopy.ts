// What the boot chain's failure screen actually says. Pure, so the mapping
// gets unit coverage; `lib/providers.tsx` only calls it.
//
// THE BUG THIS FIXES: every boot failure except one rendered
// "Couldn't reach CoralSwarm. Check your connection and retry." — including
// the tenancy 403s from `backend/src/tenancy/gate.rs`, which have nothing to
// do with the user's connection. That copy sent a founder looking at their
// network while the real answer was a tenancy refusal.
//
// The gate's messages are written to be read by a human and already say the
// specific thing that is wrong ("token does not match this organization",
// "organization is not provisioned for service", "organization is not
// active"), so the right move for a 403 is to SHOW the backend's own reason
// rather than invent a frontend paraphrase that drifts from it. Anything
// unrecognized keeps the connection copy, which is genuinely the likeliest
// cause of an unlabeled failure (a timeout from the boot watchdog has no
// status at all).
//
// THE BRANCH THAT USED TO COME FIRST IS GONE (claim-based tenancy PR 2.1). A
// `hostReason` parameter, defaulted from `apiBase.ts::unroutableHostReason()`,
// pre-empted every other case to explain an org whose Clerk slug could not be
// routed as a hostname — the real 43-character-slug incident. No hostname
// names a tenant any more, so that condition cannot occur and there is nothing
// left for the branch to say.

import { ApiError } from "./api";
import { UnexpectedEmptyOrgError } from "./oceanBootstrap";
import { isProvisioningGate } from "./orgSelfserve";

/** Shown when we have nothing more specific — a network error, or the boot
 *  watchdog's timeout, neither of which carries a status. */
export const GENERIC_BOOT_ERROR = "Couldn't reach CoralSwarm. Check your connection and retry.";

/** Shown for a 401. Signing out is an ordinary thing a person does, and it is
 *  the single likeliest reason boot fails on a machine that was working an
 *  hour ago — so it gets copy that names the cause and the remedy. */
export const SIGNED_OUT_ERROR = "You're signed out. Sign in to continue.";

/** Shown for the binding gate's 503 `status: "provisioning"`: the team exists
 *  and its database is still being built. */
export const PROVISIONING_BOOT_ERROR = "This team is still being set up. Retry in a minute; it opens as soon as it's ready.";

/**
 * Map a thrown boot-chain error to the message the failure screen shows.
 *
 * Deliberately narrow: only errors we can say something TRUER about than the
 * generic copy get their own branch. Growing this into a status-code switch
 * would just relocate the original bug — inventing frontend prose for
 * conditions the backend already describes.
 */
export function bootErrorMessage(err: unknown): string {
  // The org plane returned zero oceans — its own already-specific, already-
  // user-facing message (see `lib/oceanBootstrap.ts`).
  if (err instanceof UnexpectedEmptyOrgError) return err.message;

  // The binding gate's 503 for a team whose database is still being built
  // (A2: `{"status":"provisioning"}`). Same status as an outage, but the
  // connection copy would send someone whose team is mid-setup to check
  // their network.
  if (isProvisioningGate(err)) return PROVISIONING_BOOT_ERROR;

  // A tenancy/authorization refusal. `lib/api.ts` has already unwrapped the
  // response body's `error` field into `message`, so this IS the gate's
  // reason string — unless the body was empty or not JSON, in which case that
  // same code leaves behind its bare `HTTP <status>` placeholder. Showing a
  // user "HTTP 403" would be a different flavor of the bug this module fixes
  // (technically accurate, humanly useless), so treat the placeholder as "no
  // reason available" and keep the generic copy.
  if (err instanceof ApiError && err.status === 403) {
    const reason = err.message.trim();
    if (reason.length === 0 || reason === `HTTP ${err.status}`) return GENERIC_BOOT_ERROR;
    return reason;
  }

  // A 401 is NOT a connection problem, and calling it one is the exact bug
  // this module's header describes — just at a status nobody included when
  // the 403 branch was written. Observed 2026-09-09: a signed-out client
  // told its user to check their network while `/v1/graphs` was answering
  // `401 {"error":"missing token"}` from a healthy backend.
  //
  // Unlike the 403 branch, this one deliberately does NOT pass the backend's
  // reason through. "missing token" is written for whoever is holding the API,
  // not for the person reading the screen — and this module's whole rule is to
  // show the backend's words only where they were written for a human.
  if (err instanceof ApiError && err.status === 401) return SIGNED_OUT_ERROR;

  return GENERIC_BOOT_ERROR;
}

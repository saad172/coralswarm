import { ApiError, type OrgDomain } from "@/lib/api";
import { domainArmsAutoAdd } from "./autoAddModel";

/* Domain-verification admin surface (CS-450, org-onboarding plan §3 item 53)
 * — the "what state is this domain in, and what do we say about it" logic,
 * split out of `DomainVerification.tsx` the same way `autoAddModel.ts` and
 * `connectorHealthModel.ts` keep every such decision pure and tested rather
 * than inline in JSX.
 *
 * Four things this story's own brief calls out as load-bearing, and this
 * file is where each one is decided exactly once:
 *
 *   1. A `verify` `409` and a `502` are DIFFERENT facts and must never
 *      render the same copy (`dnsCheckFailure`) — see
 *      `backend/src/routes/org_domains.rs`'s own header comment, "'Could
 *      not ask' is not 'not published'".
 *   2. DNS and email verification are not interchangeable — only
 *      `method === "dns_txt"` arms auto-add (D11 amendment). Every row says
 *      so, using the SAME predicate the auto-add toggle itself gates on
 *      (`domainStatus`, built on `autoAddModel.ts`'s `domainArmsAutoAdd` —
 *      not a second copy of that condition). An email-verified domain can
 *      still be UPGRADED to DNS (`verify_dns`'s own short-circuit only
 *      skips a row already `dns_txt`-verified) — this file never hides that
 *      path behind `d.verified` alone.
 *   3. A `confirmation` `503` has TWO distinct causes that share the status
 *      code, and only one of them is a deployment fact. The backend's own
 *      `error` field discriminates them (code review, CS-450 fix round —
 *      the original brief's "any 503 here is a deployment fact" was wrong,
 *      founder's own correction): `"email_confirmation_not_configured"` is
 *      permanent (`DOMAIN_VERIFICATION_SECRET`/`RESEND_API_KEY` unset on
 *      THIS deployment — DNS is unaffected and stays the working route);
 *      anything else (the plain `"service unavailable"` every other route
 *      in this module shares for `state.control_db()` being `None`) is a
 *      transient blip and MUST keep a retry, exactly like `dnsCheckFailure`'s
 *      own 503 branch already does (`confirmationFailure`).
 *   4. A raw backend error body must never reach an audience it wasn't
 *      written for (a 403 "admin only"/"personal accounts have no org
 *      domains" shown verbatim to a non-admin or personal-account caller),
 *      and a response that means "the client's list is stale" (a 404, or a
 *      409 "already verified") is worth telling the caller to refetch over,
 *      not just showing calmer copy for (`domainsListFailure`).
 */

// ── 1. What a domain row says about itself ─────────────────────────────────

export type DomainStatusKind = "arms_auto_add" | "verified_other_proof" | "unverified";

export interface DomainStatus {
  kind: DomainStatusKind;
  /** Short tag for a badge — "DNS" / "Email" / "Unverified". */
  tag: string;
  /** The one sentence every row shows under the domain name. */
  label: string;
}

/** `domainArmsAutoAdd(d)` — imported, not re-derived — is the only reading
 *  that arms auto-add. A domain `verified` by anything else (today, only
 *  `"email"`) still proves control (D2's guest rule), so it reads as
 *  verified, but its own sentence says plainly that auto-add stays off.
 *  Unrecognised-but-verified rows (a `method` this build doesn't know about)
 *  fall into the same "verified, doesn't arm" bucket rather than crashing or
 *  silently reading as armed — the safer of the two wrong guesses. */
export function domainStatus(d: OrgDomain): DomainStatus {
  if (domainArmsAutoAdd(d)) {
    return { kind: "arms_auto_add", tag: "DNS", label: "Verified by DNS — arms auto-add." };
  }
  if (d.verified) {
    return {
      kind: "verified_other_proof",
      tag: "Email",
      label:
        "Verified by email — proves control, but only a DNS-verified domain arms auto-add. Publish the DNS record below to arm it.",
    };
  }
  return { kind: "unverified", tag: "Unverified", label: "Not verified yet." };
}

// ── 2. The DNS "check now" outcome — 409 vs 502 must never blur ────────────

export type DnsCheckOutcome =
  /** The resolver answered — either with no matching record yet, or with a
   *  match that a DIFFERENT org already verified first (D16/CS-447: one
   *  domain verifies for exactly one org, globally). Both are real,
   *  actionable facts the backend's own message already names precisely —
   *  read straight through rather than re-derived here, so this surface
   *  cannot write a second, possibly wrong, version of that sentence. */
  | { kind: "not_published"; message: string }
  /** We could not reach a resolver AT ALL. This says NOTHING about whether
   *  the record is published — conflating it with `not_published` would
   *  send an admin to "fix" a DNS record that was already correct. Its copy
   *  is written here (not passed through) because the backend's own 502
   *  message is process-internal in tone; this is the one calm sentence an
   *  admin sees for it. */
  | { kind: "resolver_unreachable"; message: string }
  | { kind: "other"; message: string };

/** Classifies a failed `POST /v1/orgs/domains/{id}/verify` call. The split
 *  between `not_published` (409) and `resolver_unreachable` (502) is the
 *  entire point of this function — see this file's own header comment,
 *  point 1, and never collapse the two into one code path. */
export function dnsCheckFailure(e: unknown): DnsCheckOutcome {
  if (e instanceof ApiError) {
    if (e.status === 409) return { kind: "not_published", message: e.message };
    if (e.status === 502) {
      return {
        kind: "resolver_unreachable",
        message: "We couldn't reach a DNS resolver just now — that says nothing about your record. Try again shortly.",
      };
    }
    if (e.status === 404) return { kind: "other", message: "That domain isn't in the list any more — refresh." };
    if (e.status === 403) return { kind: "other", message: "Only an admin can check this." };
    if (e.status === 503) return { kind: "other", message: "Briefly unavailable — try again." };
  }
  return { kind: "other", message: "Couldn't check DNS — try again." };
}

// ── 3. The email-confirmation outcome — a 503 has TWO causes ──────────────

export type ConfirmationOutcome =
  | { kind: "sent" }
  /** `400` — the address is not at the claimed domain. The backend's own
   *  message names the domain and why, so it is shown verbatim. */
  | { kind: "domain_mismatch"; message: string }
  /** `409` — either "already verified" or "sent very recently, try again
   *  shortly"; the backend's own message says which. */
  | { kind: "blocked"; message: string }
  /** `502` — the email provider refused the send. Nothing was consumed;
   *  worth retrying. */
  | { kind: "send_failed"; message: string }
  /** `503` with `error: "email_confirmation_not_configured"` — the ONE
   *  genuinely permanent 503 (`backend/src/routes/org_domains.rs::
   *  confirmation_not_configured`: `DOMAIN_VERIFICATION_SECRET` or
   *  `RESEND_API_KEY` unset on THIS deployment). Not a retry — DNS is the
   *  route that still works. See this file's own header comment, point 3. */
  | { kind: "not_configured"; message: string }
  /** `503` with any OTHER body — today, only the plain
   *  `error: "service unavailable"` every route in this module shares for
   *  `state.control_db()` being `None`, a transient control-plane blip.
   *  MUST keep a retry: this is the exact case the story's original brief
   *  got wrong by reading the endpoint's `utoipa` annotation instead of the
   *  handler (founder's own correction, code review on PR #1048) — collapsing
   *  it into `not_configured` would tell an admin email is permanently off
   *  when the database merely hiccuped. */
  | { kind: "transient"; message: string }
  | { kind: "other"; message: string };

/** The one exact string `confirmation_not_configured()` sends
 *  (`backend/src/routes/org_domains.rs`) — read from that source, not
 *  guessed. Any other 503 body (including the module-wide plain
 *  `"service unavailable"`) is the transient case. */
const CONFIRMATION_NOT_CONFIGURED = "email_confirmation_not_configured";

export function confirmationFailure(e: unknown): ConfirmationOutcome {
  if (e instanceof ApiError) {
    if (e.status === 400) return { kind: "domain_mismatch", message: e.message };
    if (e.status === 409) return { kind: "blocked", message: e.message };
    if (e.status === 502) return { kind: "send_failed", message: e.message };
    if (e.status === 503) {
      if (e.message === CONFIRMATION_NOT_CONFIGURED) {
        return {
          kind: "not_configured",
          message: "Email confirmation isn't set up on this deployment — verify by DNS instead.",
        };
      }
      // Reconciled with `dnsCheckFailure`'s own 503 branch below — the same
      // status code from the same `admin_ctx` preamble must read the same
      // way in both mappers.
      return { kind: "transient", message: "Briefly unavailable — try again." };
    }
    if (e.status === 404) return { kind: "other", message: "That domain isn't in the list any more — refresh." };
    if (e.status === 403) return { kind: "other", message: "Only an admin can do this." };
  }
  return { kind: "other", message: "Couldn't send that — try again." };
}

// ── 4. Claim / remove ───────────────────────────────────────────────────

export type ClaimOutcome =
  /** `400` — `services::org_domains::DomainRejection::message`, written to
   *  show an admin verbatim (not a code this surface re-maps): "that is not
   *  a domain…", "a public email provider…", etc. */
  | { kind: "rejected"; message: string }
  /** `409` — this org's own 25-domain cap; the message names the number. */
  | { kind: "limit_reached"; message: string }
  | { kind: "other"; message: string };

export function claimFailure(e: unknown): ClaimOutcome {
  if (e instanceof ApiError) {
    if (e.status === 400) return { kind: "rejected", message: e.message };
    if (e.status === 409) return { kind: "limit_reached", message: e.message };
    if (e.status === 403) return { kind: "other", message: "Only an admin can claim a domain." };
    if (e.status === 503) return { kind: "other", message: "Briefly unavailable — try again." };
  }
  return { kind: "other", message: "Couldn't claim that domain — try again." };
}

export function removeFailure(e: unknown): string {
  if (e instanceof ApiError) {
    if (e.status === 404) return "That domain is already gone.";
    if (e.status === 403) return "Only an admin can remove a domain.";
    if (e.status === 503) return "Briefly unavailable — try again.";
  }
  return "Couldn't remove that domain — try again.";
}

// ── 5. The list read — a raw backend body is not for every audience ───────

export interface DomainsListFailure {
  message: string;
  /** `true` for a 403 (not an org admin, or a personal account —
   *  `admin_ctx`'s two rejections both land here). A dead end, same
   *  precedent this page's own roster read holds ("A 403 … is a dead end —
   *  no retry changes the answer") — `DomainVerification.tsx` withholds the
   *  retry action when this is `true`, instead of offering a click that can
   *  never succeed. */
  forbidden: boolean;
}

/** Classifies a failed `GET /v1/orgs/domains`. Every OTHER verb in this
 *  file maps its own 403 to calm copy already (`claimFailure`,
 *  `dnsCheckFailure`, `confirmationFailure`, `removeFailure`) — this one
 *  existed only as a bare `e.message` passthrough before the CS-450 fix
 *  round, which is how "admin only" (`require_org_admin`) or "personal
 *  accounts have no org domains" (`refuse_personal_org`) reached the screen
 *  verbatim, inside a `role="alert"` note, with a retry that could never
 *  succeed. */
export function domainsListFailure(e: unknown): DomainsListFailure {
  if (e instanceof ApiError && e.status === 403) {
    return { message: "Only an admin can manage this organization's domains.", forbidden: true };
  }
  return {
    message: e instanceof ApiError ? e.message : "Couldn't load this organization's domains.",
    forbidden: false,
  };
}

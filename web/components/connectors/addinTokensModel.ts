import type { AddinToken } from "@/lib/api";
import { resolveOrgAdmin } from "@/lib/orgAdmin";

/* Every DECISION behind the "Gmail Add-on" section on Connectors
 * (`GmailAddonTokens.tsx`), kept in a framework-free module so it gets real
 * unit coverage — this project's vitest setup runs a Node environment with no
 * DOM and no testing-library (see `vitest.config.ts`'s own comment), so a
 * component that calls `useApp()`/`useOrgAdmin()` cannot be exercised by a
 * plain `.test.ts` file at all. Same rationale (and the same shape) as
 * `slackOnboardingModel.ts` beside it and `lib/orgAdmin.ts`.
 *
 * The four things that would silently produce a confident WRONG answer if
 * they drifted, and are therefore pinned by name in `addinTokensModel.test.ts`:
 *
 *  1. WHO SEES THIS SECTION. Add-on tokens are personal-plane only
 *     (`addin_tokens.owner_user_id` is NOT NULL; `pair_approve` and the list
 *     endpoint both 400 outright in an org context). But "am I in an org" is
 *     NOT `orgId != null` here: a PERSONAL ORG (control migration 0208) sets
 *     `orgId` exactly like a real organization does, and the backend's own
 *     gate resolves it to `GateOutcome::Personal`
 *     (`backend/src/tenancy/gate.rs`: `TokenKind::PersonalOrg(_) =>
 *     GateOutcome::Personal`), so `tenant.org_context()` is `None` and the
 *     endpoint answers normally. Hiding the section from a personal org would
 *     strand its owner with no way to revoke a token they can still mint.
 *     This is why the predicate below derives its own answer from the raw
 *     Clerk inputs via `resolveOrgAdmin` instead of taking a `hasOrg: boolean`
 *     a caller could compute wrongly.
 *  2. "NEVER USED" IS A REAL STATE, not a missing field — a token that was
 *     approved and then never presented has `last_used_at: null`. It renders
 *     as the word "never", never as a blank or a fallback to `created_at`.
 *  3. A 404 FROM THE LIST ENDPOINT IS EXPECTED FOR NOW. `GET
 *     /v1/addin/tokens` ships in a SIBLING backend PR; until that deploys,
 *     this section must degrade to an honest error line rather than throwing.
 *  4. "NO ROWS" IS NOT AN ANSWER — it is three different situations (still
 *     loading, load failed, genuinely none), and only one of them may be
 *     reported as "no paired add-ons". See `addinSectionState` below, which
 *     exists because the header got this wrong.
 */

// ── 1. Who sees the section ──────────────────────────────────────────────────

export interface AddinTokensAudienceInputs {
  /** Clerk auth + organization have both resolved (`useOrgAdmin()`'s
   *  `loaded`). While false, nothing is known yet and the answer is `"wait"`
   *  — NOT `"show"`: `orgId` is `undefined` before Clerk resolves, which is
   *  indistinguishable from the personal plane, and optimistically showing
   *  would fire a request that 400s for every org member on every page load. */
  loaded: boolean;
  orgId: string | null | undefined;
  orgRole: string | null | undefined;
  /** `useApp()`'s `isPersonalOrg` (`GET /v1/me`'s `is_personal_org`). See the
   *  module doc's point 1 — this is what keeps a one-person org on the SHOW
   *  side, matching what the backend will actually answer. */
  isPersonalOrg: boolean;
}

/** `"show"` — render the section and fetch the list; `"org"` — an actual
 *  organization is active, so render nothing and never call the endpoint;
 *  `"wait"` — Clerk hasn't resolved yet, render nothing YET and don't call.
 *
 *  Three states rather than a boolean deliberately: "not yet known" and
 *  "known to be an org" produce the same empty render but must not produce
 *  the same FETCH decision, and a boolean collapses exactly that difference. */
export type AddinTokensAudience = "show" | "org" | "wait";

export function addinTokensAudience({
  loaded,
  orgId,
  orgRole,
  isPersonalOrg,
}: AddinTokensAudienceInputs): AddinTokensAudience {
  if (!loaded) return "wait";
  const { hasOrg } = resolveOrgAdmin({ orgId, orgRole, isPersonalOrg });
  return hasOrg ? "org" : "show";
}

// ── 2. The rows ──────────────────────────────────────────────────────────────

/** One rendered row. Every field is a finished STRING: the component does no
 *  formatting of its own, so what a test asserts here is exactly what ships. */
export interface AddinTokenRow {
  id: string;
  name: string;
  /** e.g. `"9/3/2026"`. */
  createdLabel: string;
  /** e.g. `"9/10/2026"`, or the literal `"never"` — see the module doc's
   *  point 2. */
  lastUsedLabel: string;
}

/** A local day label, or `"unknown"` for anything that isn't a parseable
 *  timestamp. `new Date("nonsense").toLocaleDateString()` is the string
 *  `"Invalid Date"`, which reads like a bug report in the middle of a
 *  credential list; "unknown" is the same admission without the noise. */
export function formatDay(iso: string): string {
  const ms = Date.parse(iso);
  if (Number.isNaN(ms)) return "unknown";
  return new Date(ms).toLocaleDateString();
}

/** NEWEST FIRST. The list is a revocation surface for the "I lost my phone"
 *  case, and the token someone is looking for is overwhelmingly the one they
 *  just made — server order is not relied on for that (the sibling endpoint
 *  documents a shape, not an ordering). An unparseable `created_at` sorts
 *  last rather than poisoning the comparator with `NaN`. */
export function toTokenRows(tokens: AddinToken[]): AddinTokenRow[] {
  const byNewest = [...tokens].sort((a, b) => {
    const at = Date.parse(a.created_at);
    const bt = Date.parse(b.created_at);
    if (Number.isNaN(at) && Number.isNaN(bt)) return 0;
    if (Number.isNaN(at)) return 1;
    if (Number.isNaN(bt)) return -1;
    return bt - at;
  });
  return byNewest.map((t) => ({
    id: t.id,
    name: t.name,
    createdLabel: formatDay(t.created_at),
    lastUsedLabel: t.last_used_at === null ? "never" : formatDay(t.last_used_at),
  }));
}

/** What the list becomes after a successful `DELETE /v1/addin/tokens/{id}`.
 *  Applied locally instead of re-fetching: the backend's 204 is authoritative
 *  about that one row, and a refetch would make "Revoke all" issue N deletes
 *  and N list calls, each able to land out of order and resurrect a row that
 *  is already gone. */
export function withoutToken(tokens: AddinToken[], id: string): AddinToken[] {
  return tokens.filter((t) => t.id !== id);
}

// ── 3. What the section is currently SAYING ──────────────────────────────────

/** The four states the section can be in, as ONE value.
 *
 *  This exists because of a real defect (CodeRabbit round 1 on #906): the
 *  header read the row count directly, and `rows.length === 0` is true in
 *  THREE different situations — still loading, load failed, and genuinely no
 *  tokens — so the card announced "no paired add-ons" while it was still
 *  asking, and again directly above "couldn't load your tokens". On the
 *  expected-404 path (the list endpoint ships in a sibling backend PR) that
 *  is the only thing most people would ever see: a confident, wrong,
 *  self-contradicting answer about whether anything is paired.
 *
 *  An absent list rendering as "nothing is there" is the general shape — the
 *  failure mode is that missing data looks like a successful empty result —
 *  and a row count alone cannot distinguish them. So the count stops being
 *  the input: every consumer (the header line, the empty-state copy, the
 *  Revoke-all button) reads THIS, and they cannot disagree. */
export type AddinSectionState = "loading" | "error" | "empty" | "loaded";

export interface AddinSectionStateInputs {
  /** A list request is in flight — including a RETRY after a failure, which
   *  is why `GmailAddonTokens` sets this at the top of `load()` rather than
   *  only on first mount. Without that, a retry would drop straight back to
   *  `empty` and re-announce "no paired add-ons" while the request was still
   *  running: the same defect, one click later. */
  loading: boolean;
  /** The load error copy currently held, or `null`. */
  loadError: string | null;
  /** Rows that would actually render — meaningful ONLY in the `loaded`
   *  state, which is the whole point of routing through this function. */
  rowCount: number;
}

/** Order matters: in flight beats a stale error beats a count. An error left
 *  over from the PREVIOUS attempt must not outrank a request that is
 *  currently running, and a count of 0 must never outrank either. */
export function addinSectionState({
  loading,
  loadError,
  rowCount,
}: AddinSectionStateInputs): AddinSectionState {
  if (loading) return "loading";
  if (loadError !== null) return "error";
  return rowCount === 0 ? "empty" : "loaded";
}

/** The mono line under the "Gmail Add-on" title, or `null` for "say nothing".
 *
 *  `null` on `error` is deliberate and is the fix's actual content: the
 *  honest answer there is that we do not know what is paired, and the
 *  `ErrorNote` immediately below already says why. A status line that
 *  guessed — in either direction — would contradict it. */
export function addinHeaderStatus(state: AddinSectionState, rowCount: number): string | null {
  switch (state) {
    case "loading":
      return "checking…";
    case "error":
      return null;
    case "empty":
      return "no paired add-ons";
    case "loaded":
      return `${rowCount} paired`;
  }
}

// ── 4. Failure copy ──────────────────────────────────────────────────────────

/** Copy for a failed `GET /v1/addin/tokens`, keyed on the HTTP status.
 *
 *  `404` is called out by name because it is, RIGHT NOW, the expected answer
 *  from production: the list endpoint ships in a sibling backend PR that has
 *  not merged. "Not available on this backend yet" is the true statement in
 *  that window, and it keeps the section from reading as "you have no tokens"
 *  (which would be a lie that talks someone out of revoking a live one).
 *
 *  `null` status = not an `ApiError` at all (a network/transport failure). */
export function addinListErrorCopy(status: number | null): string {
  if (status === 404) {
    return "Add-on token management isn't available on this backend yet. Your add-on keeps working; come back once this deploys.";
  }
  if (status === 400) {
    return "Add-on tokens live in your personal workspace. Switch out of the organization workspace to manage them.";
  }
  if (status === 401 || status === 403) {
    return "Your session isn't authorized to list add-on tokens — reload and try again.";
  }
  return "Couldn't load your Gmail Add-on tokens.";
}

/** Copy for a failed `DELETE`. A 404 here is NOT the missing-endpoint case
 *  the list's 404 is: `revoke_token` answers 404 for "no such id", "already
 *  revoked" and "belongs to someone else" alike, and from this screen the
 *  overwhelmingly likely one is "already revoked" — in another tab, or by the
 *  add-on's own Disconnect. Saying so is more useful than "not found", and
 *  the row is dropped locally either way (there is nothing left to revoke). */
export function addinRevokeErrorCopy(status: number | null): string {
  if (status === 404) return "That token was already revoked.";
  return "Couldn't revoke that token — try again.";
}

/** Whether a failed revoke should still drop the row from the list. True for
 *  404 only: the token is provably gone (revoked, or never the caller's), so
 *  leaving it rendered invites an endless retry against a row that can never
 *  succeed. Any other status (503, a transport error) means the token may
 *  well still be live and MUST stay visible and revocable. */
export function revokeFailureClearsRow(status: number | null): boolean {
  return status === 404;
}

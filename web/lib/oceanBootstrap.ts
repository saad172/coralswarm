// Pure decision logic for `lib/providers.tsx`'s `bootstrap()` — what to do
// with the ocean list the backend returns on first boot (org-tenancy
// follow-up; see `lib/bootGate.ts` for the retry/termination half of the same
// incident).
//
// THE BUG THIS FIXES: `bootstrap()` used to run
//
//   let list = await api.graphs.list(tok);
//   if (list.length === 0) {
//     const personal = await api.graphs.create(tok, "Personal");   // kind defaults to "personal"
//     list = [personal];
//   }
//
// under the comment "the backend auto-provisions a private Primary ocean on
// first Clerk auth, so the list is normally non-empty; the create fallback
// covers legacy first-run." That is true ONLY on the personal
// plane. It is FALSE on an org plane: a freshly onboarded org tenant has zero
// oceans (org onboarding does not — today — auto-provision one), so `list` is
// empty there too, and the fallback fires and requests `kind: "personal"`
// regardless of which plane it's running on. The backend correctly refuses
// this (`backend/src/routes/graphs.rs::create_graph`, ADR-0007 D2: a personal
// ocean must never be created under an org account, since it would be
// returned to every org member by the reachable-ocean union) with
// `{"error":"cannot create a personal ocean in an organization; switch to
// your personal account"}`. So in any org context, first login previously
// always threw here.
//
// PLANE SOURCE: this module takes `Plane` as a plain "personal" | "org" value
// derived from CLERK'S ACTIVE ORG (`orgId !== null` in `providers.tsx`), which
// is now the only source there is — claim-based tenancy (PR 2.1) deleted the
// host-derived alternative (`activeOrgSlugFromHost`/`isApexHost`) that this
// comment used to argue against choosing. The argument is worth keeping as the
// reason the surviving choice is right: `providers.tsx` already computes
// `identityKey = orgId ?? "__personal__"` from the exact same signal to decide
// which Clerk identity is booting, so the plane decision and the identity
// decision cannot disagree — there is no second source of truth to keep in
// lockstep.
//
// ONE CORRECTION, ADDED WITHOUT A REVISION MARK BECAUSE IT IS SMALL: `orgId`
// alone is necessary but not sufficient. A PERSONAL ORG — a Clerk organization
// that stands for one person's own ocean (control migration 0208) — sets
// `orgId` exactly like a real organization does, but the backend serves it on
// the shared CONSUMER plane (`tenancy::personal_org`'s module doc: "a personal
// org IS the consumer plane, by design"). `providers.tsx` overrides `plane` to
// `"personal"` whenever `GET /v1/me`'s `is_personal_org` says so, BEFORE
// calling `decideOceanBootstrap` — so by the time `plane` reaches this module
// it already agrees with the backend, and everything below still holds
// exactly as written for the caller who supplies it correctly.
//
// DECISION ON EMPTY ORG PLANE: do NOT synthesize a `kind: "org"` ocean here,
// and do NOT render a silent empty/first-run screen either. The org-onboarding
// workstream (see project memory: org onboarding DB-per-org rollout) is making
// an org ocean always exist for a real org tenant, so an org plane reporting
// zero oceans is becoming an UNEXPECTED condition, not a routine first-run
// state — treat it that way: surface a visible, retryable error instead of
// papering over it (or silently creating an ocean this module has no
// authority to name/configure correctly — team-ocean creation, membership
// wiring, etc. belong to that separate design workstream, out of scope here).
export type Plane = "personal" | "org";

export type OceanBootstrapDecision =
  /** The list already has entries — plane is irrelevant, just use it. */
  | { type: "use" }
  /** Empty list on the personal plane: the legacy first-run fallback —
   *  create the account's private Primary ocean. */
  | { type: "create-personal" }
  /** Empty list on an org plane. Never valid to paper over with a personal
   *  ocean (the backend rejects it outright) or a silently-created org ocean
   *  (out of scope, and about to become backend-guaranteed anyway) — the
   *  caller should surface this as a visible, retryable error. */
  | { type: "unexpected-empty-org" };

/**
 * Decide what `bootstrap()` should do with `oceanCount` reachable oceans,
 * given `plane`. Pure and total — no I/O, so this is where the actual "what
 * should happen" logic gets full unit coverage; `providers.tsx` just executes
 * whichever branch comes back.
 */
export function decideOceanBootstrap(plane: Plane, oceanCount: number): OceanBootstrapDecision {
  if (oceanCount > 0) return { type: "use" };
  if (plane === "org") return { type: "unexpected-empty-org" };
  return { type: "create-personal" };
}

/**
 * The plane actually served, given the plane Clerk's `orgId` implies and the
 * backend's own verdict on whether the active org is a PERSONAL org — one
 * that stands for one person's own ocean (control migration 0208) — from
 * `GET /v1/me`'s `is_personal_org`.
 *
 * ONE-DIRECTIONAL, DELIBERATELY: `isPersonalOrg` can only pull `"org"` down to
 * `"personal"`, never the reverse. The backend's own design makes a personal
 * org "the consumer plane, by design" (`tenancy::personal_org`'s module doc),
 * so `orgId`'s "org" guess is simply wrong there and this corrects it. There
 * is no symmetric case: `isPersonalOrg` is always `false` when `orgId` is
 * null (no org, nothing in `personal_orgs` to name), so it can never be asked
 * to push `"personal"` up to `"org"`.
 *
 * Pure and total, same rationale as [`decideOceanBootstrap`] above:
 * `providers.tsx` computes `plane` from Clerk's `orgId` before `GET /v1/me`
 * resolves (it has to — that's the identity the boot chain is fetching a
 * token FOR), then calls this the moment `is_personal_org` is known, BEFORE
 * `decideOceanBootstrap` ever sees the result. Skipping this step is exactly
 * the bug: a personal-org caller with zero oceans yet would hit
 * `decideOceanBootstrap("org", 0)` and throw `UnexpectedEmptyOrgError` for an
 * account the backend itself serves on the personal plane.
 */
export function effectivePlane(plane: Plane, isPersonalOrg: boolean): Plane {
  return isPersonalOrg ? "personal" : plane;
}

/** Thrown by `providers.tsx`'s `bootstrap()` for the `"unexpected-empty-org"`
 *  branch so the boot-error surface (`lib/providers.tsx`'s `attemptBoot`
 *  catch) can show this specific, actionable message instead of the generic
 *  "Couldn't reach CoralSwarm" fallback used for network/timeout failures. A
 *  distinct class (not a plain `Error`) so that `catch` can `instanceof`-check
 *  it without string-matching a message. */
export class UnexpectedEmptyOrgError extends Error {
  constructor() {
    super(
      "This organization doesn't have a workspace yet, which shouldn't happen — please retry, " +
        "or contact support if this keeps happening.",
    );
    this.name = "UnexpectedEmptyOrgError";
  }
}

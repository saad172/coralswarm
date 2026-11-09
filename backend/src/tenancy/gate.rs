//! The binding gate's **pure decision core** (org-tenancy PR 1.2).
//!
//! This module holds the Host classification and the gate *policy* as pure,
//! side-effect-free functions so the security-critical logic can be exhaustively
//! unit-tested without a database, a network, or Clerk. The async wiring in
//! `middleware::auth` classifies the request Host, resolves the routing row
//! (cached), determines the token kind, calls [`decide`], and acts on the
//! [`GateOutcome`] — but it makes no policy decisions of its own.
//!
//! The policy is evaluated in **two phases**, because the plan requires an
//! unknown subdomain to fail closed *before* the request is authenticated:
//! [`precheck`] is the token-independent half (run before JWT verification and
//! before any provisioning write) and [`decide`] is the whole policy (run once
//! the claims are verified). Whenever [`precheck`] answers, its answer is
//! identical to [`decide`]'s for every token — asserted exhaustively by
//! `precheck_matches_decide_for_every_token` — so the early reject can never
//! introduce a status the full policy would not have produced.
//!
//! Auth is **Clerk-only**: the legacy opaque session-token path is removed, so a
//! token is either an org token or a personal (no-org) token — both verified
//! Clerk tokens. (Part 2 removes the session-table fallback in `require_auth`.)
//!
//! Policy (behind `MULTITENANT_ENABLED`; when the flag is off the gate is a
//! pass-through and behavior is byte-identical to today):
//!
//! * A Host that is not our apex or a `<slug>.<apex>` subdomain → 404 (fail
//!   closed; never a default tenant).
//! * On the **apex**: a personal (no-org) token proceeds on the consumer
//!   plane. An **org** token binds that org's tenant, decided entirely by the
//!   org's OWN routing row — looked up by `clerk_org_id`, never by a slug (see
//!   [`TenantRuntime::resolve_binding`]):
//!     - no routing row at all (a genuinely un-onboarded org, OR a
//!       "shared-DB" org that predates the tenancy split and has not yet been
//!       migrated into a tenant DB) → 403 "not provisioned for service";
//!     - `provisioning` → 503 (retryable — mid-onboarding);
//!     - `suspended`/`offboarded` → 403 "not active" (a write-freeze or a
//!       torn-down tenant is a real rejection);
//!     - `active` → bind the tenant, provided the row's org equals the claim.
//! * On an **org subdomain**: unchanged — the routing row must exist (else
//!   404), be `active` (else 503/403), and the caller must present a Clerk
//!   **org** token whose `org_id` equals the subdomain's org (else 403). This
//!   arm is on its way out: claim-based tenancy names the tenant only by the
//!   claim, and the cutover deletes `Subdomain` together with the 308 redirect
//!   that replaces it. It stays live until then so that shipped clients which
//!   build `<slug>.api.<apex>` themselves keep working.
//! * Every one of the above is fail-closed and NEVER falls back to serving
//!   the shared consumer plane for org content — see [`GateOutcome`]'s doc.
//!
//! **Claim-based tenancy (`docs/plans/claim-based-tenancy.md`) removed the
//! Host from the apex decision.** Two independent signals used to have to
//! agree; now the claim alone names the tenant, so the arms above are the
//! whole boundary. The cross-product invariants in this module's tests are
//! the guard, and they were written before this change so they were already
//! green and already watching when it landed.
//!
//! [`TenantRuntime::resolve_binding`]: super::runtime::TenantRuntime::resolve_binding

/// How a canonicalized Host maps to the tenancy planes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HostTarget {
    /// The apex or an app/www host → personal/consumer plane.
    Apex,
    /// `<slug>.<apex>` → a candidate org subdomain (slug validated).
    Subdomain(String),
    /// A Host that is not ours at all → fail closed.
    Foreign,
}

/// What the auth layer determined the (verified, Clerk) bearer token to be.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TokenKind {
    /// A verified Clerk token carrying an active organization (`org_id`) that
    /// is a REAL organization — one with its own tenant database behind a
    /// routing row.
    Org(String),
    /// A verified Clerk token with no active organization.
    Personal,
    /// A verified Clerk token whose `org_id` names a **personal
    /// organization**: a Clerk org that stands for one person's own ocean
    /// (control migration `0208_personal_orgs.sql`), recorded in
    /// `personal_orgs` and confirmed to belong to THIS caller by
    /// `middleware::auth::classify_personal_org_claim`.
    ///
    /// These deliberately have **no tenant database and no routing row** —
    /// they live on the shared consumer plane alongside ordinary personal
    /// accounts, and the org id names which ACCOUNT on that plane, not which
    /// database. That is why this is a distinct variant rather than an
    /// [`Self::Org`] whose routing row happens to be missing: the difference
    /// is not "an org whose row is absent", it is "an org that is never
    /// supposed to have one".
    ///
    /// **This variant is the single carve-out in
    /// [`tests::a_real_org_token_is_never_served_the_consumer_plane`].** Read
    /// that test before widening it.
    PersonalOrg(String),
}

/// The routing row state for a subdomain's slug, as resolved from
/// `tenant_databases` (control plane).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RoutingState {
    /// Active tenant; carries the bound `clerk_org_id`.
    Active(String),
    Provisioning,
    Suspended,
    Offboarded,
    /// No routing row for this slug.
    Missing,
}

/// The gate's verdict. The async layer maps each to an HTTP response (or
/// proceeds), and only [`GateOutcome::Org`]/[`GateOutcome::Personal`] continue.
/// (A missing/invalid token is a 401 handled *before* `decide` is reached, so
/// the gate policy itself never emits 401.)
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GateOutcome {
    /// Proceed on the consumer/personal plane.
    Personal,
    /// Proceed on the tenant plane for this `clerk_org_id`.
    Org(String),
    /// 404 — unknown subdomain / foreign host (fail closed).
    NotFound,
    /// 503 `{"error":"service unavailable","status":"provisioning"}`: an org
    /// on the apex whose routing row exists and is being built (self-serve
    /// signup PR A2), so a client can tell "your workspace is being set up"
    /// from an outage without parsing prose. Apex arm only: the legacy
    /// subdomain arms exist only to serve the redirect, and a self-serve org
    /// never reaches them.
    Provisioning,
    /// 503: the legacy subdomain arms' mid-provisioning answer.
    Unavailable,
    /// 403 with a reason.
    Forbidden(&'static str),
}

/// Subdomain labels the gate routes to the apex plane — never valid tenant slugs.
/// A tenant provisioned with one of these would be classified as [`Apex`] on its
/// own subdomain and become unreachable, so they are forbidden here AND by a
/// `tenant_databases` CHECK (migration 0079).
///
/// [`Apex`]: HostTarget::Apex
pub const RESERVED_SLUGS: &[&str] = &["app", "www"];

/// A slug is 3–32 chars, lowercase alphanumeric with internal hyphens, no
/// leading/trailing hyphen, and not a reserved apex label — kept in lockstep
/// with the `tenant_databases.slug` CHECK constraints (migrations 0078 + 0079).
pub fn is_valid_slug(s: &str) -> bool {
    if RESERVED_SLUGS.contains(&s) {
        return false;
    }
    let len = s.len();
    if !(3..=32).contains(&len) {
        return false;
    }
    let bytes = s.as_bytes();
    let ok_char = |b: u8| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-';
    if !bytes.iter().all(|&b| ok_char(b)) {
        return false;
    }
    bytes[0] != b'-' && bytes[len - 1] != b'-'
}

/// Canonicalize a raw `Host` header and classify it against the configured apex.
///
/// Canonicalization (defends against the spoofing variants the review flagged):
/// trims whitespace, strips a `:port` suffix, lowercases (ASCII), and removes a
/// single trailing dot. Only an exact apex / `app.`/`www.` host is [`Apex`]; a
/// single-label `<slug>.<apex>` with a valid slug is [`Subdomain`]; everything
/// else (including multi-label subdomains and non-matching domains) is
/// [`Foreign`].
///
/// [`Apex`]: HostTarget::Apex
/// [`Subdomain`]: HostTarget::Subdomain
/// [`Foreign`]: HostTarget::Foreign
pub fn classify_host(raw: &str, apex: &str) -> HostTarget {
    let (h, apex) = canonicalize(raw, apex);
    if h.is_empty() || apex.is_empty() {
        return HostTarget::Foreign;
    }
    if h == apex || h == format!("app.{apex}") || h == format!("www.{apex}") {
        return HostTarget::Apex;
    }
    if let Some(sub) = h.strip_suffix(&format!(".{apex}")) {
        // A single label only — no nested subdomains — and a valid slug.
        if !sub.is_empty() && !sub.contains('.') && is_valid_slug(sub) {
            return HostTarget::Subdomain(sub.to_string());
        }
    }
    HostTarget::Foreign
}

/// Canonicalize a raw `Host` header and the configured apex the same way for
/// both [`classify_host`] and [`unroutable_label_reason`]. Extracted so the two
/// can never disagree about what host they are talking about — a diagnostic
/// that canonicalizes differently from the decision it explains is worse than
/// no diagnostic.
fn canonicalize(raw: &str, apex: &str) -> (String, String) {
    let mut h = raw.trim();
    // Strip a trailing `:port` (domain hosts only — a ':' here is always a port).
    if let Some((host, port)) = h.rsplit_once(':') {
        if !port.is_empty() && port.bytes().all(|b| b.is_ascii_digit()) {
            h = host;
        }
    }
    let mut h = h.to_ascii_lowercase();
    if h.ends_with('.') {
        h.pop();
    }
    (h, apex.trim().trim_end_matches('.').to_ascii_lowercase())
}

/// Why a host that IS under our apex can nonetheless never name a tenant.
///
/// [`classify_host`] answers this `Foreign`, and [`decide`] turns that into a
/// bare 404 — correct, but indistinguishable from "some stranger's domain
/// pointed at us". That cost real debugging time: an organization whose Clerk
/// slug is auto-generated (`Alex-s-organization-1781542054204631874`, 43
/// chars) is simply unroutable, because 32 is the cap in
/// [`is_valid_slug`], in `web/lib/apiBase.ts::isValidSlug`, and in the
/// `tenant_databases.slug` CHECK (migration 0078) — so it can never even be
/// provisioned. All three agree; nothing told anyone.
///
/// Returns `Some(reason)` ONLY for a single-label host under our own apex that
/// fails the slug rules, so it never describes a genuinely foreign domain (which
/// would leak what we do and don't host). `None` for every host the gate handles
/// normally, including the apex itself and valid subdomains.
///
/// Purely a diagnostic: this changes no routing decision, and the caller
/// attaches it to the SAME 404 the gate already returned.
pub fn unroutable_label_reason(raw: &str, apex: &str) -> Option<&'static str> {
    let (h, apex) = canonicalize(raw, apex);
    if h.is_empty() || apex.is_empty() {
        return None;
    }
    let sub = h.strip_suffix(&format!(".{apex}"))?;
    // Multi-label hosts and the reserved apex labels are deliberate routing
    // outcomes, not misconfigured organizations — say nothing about them.
    if sub.is_empty() || sub.contains('.') || RESERVED_SLUGS.contains(&sub) {
        return None;
    }
    if is_valid_slug(sub) {
        return None;
    }
    let len = sub.len();
    if len > 32 {
        // The case actually seen in production.
        Some("this organization's address is too long to route: it must be 32 characters or fewer. Rename the organization's slug in Clerk.")
    } else if len < 3 {
        Some("this organization's address is too short to route: it must be at least 3 characters.")
    } else {
        Some("this organization's address contains characters that cannot be routed: use lowercase letters, digits and hyphens only, not starting or ending with a hyphen.")
    }
}

/// The gate policy. Pure: no I/O, total over its inputs. `routing` is the
/// routing row relevant to THIS decision — for [`HostTarget::Subdomain`] the
/// caller resolves it by the request's subdomain slug; for
/// [`HostTarget::Apex`] with an org token (org-tenancy PR 4.3), the caller
/// resolves it by the verified token's `clerk_org_id` instead (there is no
/// slug on the apex) — see [`TenantRuntime::resolve_binding`]. It is ignored
/// for `Apex` + a personal token and for `Foreign` hosts (no lookup needed).
///
/// [`TenantRuntime::resolve_binding`]: super::runtime::TenantRuntime::resolve_binding
pub fn decide(host: &HostTarget, token: &TokenKind, routing: &RoutingState) -> GateOutcome {
    match host {
        HostTarget::Foreign => GateOutcome::NotFound,
        HostTarget::Apex => match token {
            TokenKind::Personal => GateOutcome::Personal,
            // A personal org IS the consumer plane, by design. It has no
            // routing row on purpose, so `routing` is deliberately NOT
            // consulted here: consulting it would make the answer depend on a
            // row this org is never supposed to have, and every non-`Active`
            // state would refuse a caller who is exactly where they belong.
            //
            // The caller's `account_id` has already been resolved from this
            // same claim by `resolve_clerk_context` — the org's own `accounts`
            // row, provisioned by Clerk's `organization.created` webhook — so
            // `Personal` here means "the shared plane, scoped to that
            // account", not "fall back to whatever account you had".
            TokenKind::PersonalOrg(_) => GateOutcome::Personal,
            // Claim-based tenancy (`docs/plans/claim-based-tenancy.md` PR
            // 1.2): the apex is now every tenant's door, and the org's OWN
            // routing row — resolved by `clerk_org_id`, never by a slug (see
            // this fn's doc) — decides whether it opens. Exactly one state
            // opens it. Every other state, and every lookup failure upstream,
            // is a refusal; NONE of them may fall through to the consumer
            // plane, because reachability there has no account filter at all.
            TokenKind::Org(claim_org) => match routing {
                // No tenant DB for this org at all: either a brand-new org
                // that was never onboarded, or a "shared-DB" org that
                // predates the tenancy split and has not yet been migrated
                // (org-tenancy PR 4.3's `migrate_shared_db_org.sh`). This is
                // the apex arm's own verdict and is deliberately DIFFERENT from
                // the `Subdomain` arm's `Missing` answer below (404, which does
                // not confirm whether that tenant exists): here the caller has
                // already proven which org they mean, so a support-actionable
                // 403 leaks nothing extra.
                //
                // THREE independently-proven ways `org_claim` reaches this
                // arm — none open to enumeration by an unproven caller:
                //   1. A verified JWT `org_id` claim (REST's apex traffic,
                //      claim-based tenancy PR 1.2 — this is the arm's
                //      original, and until PR 3.2 below, only caller).
                //   2. As of claim-based tenancy §Phase 3 PR 3.2, an MCP
                //      caller's STORED tenant selection
                //      (`mcp::tools::tenancy::select_tenant`). This is NOT a
                //      claim — it is re-confirmed as a CURRENT live membership
                //      on THIS SAME request
                //      (`tenant_selection::resolve_selected_tenant`) before
                //      `mcp::auth::resolve_personal_tenancy` ever hands
                //      `org_claim` to `finish_binding`/this arm, so the
                //      caller is exactly as proven as case 1, just by a fresh
                //      membership re-check instead of a signed claim.
                //   3. As of #825, an MCP caller's own verified `org_id`
                //      claim on the personal/apex resource — the
                //      organization they picked on Clerk's consent screen.
                //      This one is BOTH: a signed claim (case 1's proof) and
                //      a fresh `live_org_membership` re-check on this same
                //      request, twice over — once at the door under
                //      `OrgMembershipPolicy::RequireExisting`, once inside
                //      `tenant_selection::resolve_request_tenancy` (case 2's
                //      proof).
                //
                // Case 3 is what `mcp::auth::claims_for_resource` used to
                // prevent by STRIPPING a claim-sourced `org_id` on that
                // resource. That strip is deleted: it was written when
                // `/mcp` did not advertise `user:org:read` and any claim
                // there was therefore an artifact, and #825 made such a
                // claim a deliberate user choice instead.
                //
                // `/mcp`'s (retired) ORG resource never reaches this Apex arm
                // at all: it always routed through `Subdomain` below, and a
                // subdomain Host is 410 Gone before `/mcp` even runs.
                RoutingState::Missing => {
                    GateOutcome::Forbidden("organization is not provisioned for service")
                }
                // Mid-onboarding: retryable, not a permanent no.
                RoutingState::Provisioning => GateOutcome::Provisioning,
                // Write-frozen (offboarding) or torn down: a real rejection.
                RoutingState::Suspended | RoutingState::Offboarded => {
                    GateOutcome::Forbidden("organization is not active")
                }
                // Fully onboarded and active: bind this tenant. The ONLY arm
                // that changed in PR 1.2 — it used to answer "use your
                // organization's subdomain".
                //
                // The equality check is deliberate even though the caller
                // resolved this row BY `claim_org` (`resolve_routing_by_org`),
                // which makes a mismatch mean the control plane disagrees with
                // itself. It is kept for two reasons. First, nothing else in
                // the codebase compares the claim to the row any more: the
                // `Subdomain` arm below holds the only other copy, and the
                // cutover deletes it. Second, `finish_binding` builds the pool
                // from the ROW's `account_id`/`db_secret_ref`, and the
                // registry's `tenant_meta` fingerprint assert then confirms the
                // DSN matches THAT row's org — so on a mismatch it would
                // confirm the wrong tenant's identity and hand back its pool,
                // while `live_org_membership` had authorized the other org
                // entirely. This one comparison is what keeps
                // `a_bound_tenant_is_always_the_one_the_token_named` true.
                RoutingState::Active(bound_org) => {
                    if claim_org == bound_org {
                        GateOutcome::Org(bound_org.clone())
                    } else {
                        GateOutcome::Forbidden("token does not match this organization")
                    }
                }
            },
        },
        HostTarget::Subdomain(_) => match routing {
            RoutingState::Missing => GateOutcome::NotFound,
            RoutingState::Provisioning => GateOutcome::Unavailable,
            RoutingState::Suspended | RoutingState::Offboarded => {
                GateOutcome::Forbidden("organization is not active")
            }
            RoutingState::Active(bound_org) => match token {
                TokenKind::Personal => GateOutcome::Forbidden("organization token required"),
                // A personal org has no tenant database, so it can never be
                // the org a subdomain is bound to. This arm is dead in
                // production — claim-based tenancy retired every org host —
                // but the gate must still answer, and refusing is the only
                // answer that cannot be wrong.
                TokenKind::PersonalOrg(_) => {
                    GateOutcome::Forbidden("a personal organization has no tenant database")
                }
                TokenKind::Org(claim_org) => {
                    if claim_org == bound_org {
                        GateOutcome::Org(bound_org.clone())
                    } else {
                        GateOutcome::Forbidden("token does not match this organization")
                    }
                }
            },
        },
    }
}

/// The TOKEN-INDEPENDENT half of [`decide`], so the async layer can reject a
/// request whose Host alone already condemns it **before** it verifies a JWT
/// or writes anything (org-tenancy plan line 50: "unknown subdomain → 404
/// fail-closed *before auth*").
///
/// `Some(outcome)` means the verdict is settled by (host, routing) alone and
/// is byte-identical to what [`decide`] would return for EITHER token kind —
/// `precheck_matches_decide_for_every_token` proves that exhaustively, so this
/// can never drift into a second, subtly different policy. `None` means the
/// decision genuinely needs the verified token and must wait for it:
///
/// * [`HostTarget::Apex`] — a personal token proceeds and an org token is
///   refused by a rejection that depends on the org's OWN routing row (looked
///   up by `clerk_org_id`, which only exists once the token is verified), so
///   nothing can be settled early. That is fine: the apex is the public
///   consumer plane, so there is no tenant to enumerate and no early reject to
///   make.
/// * [`HostTarget::Subdomain`] + [`RoutingState::Active`] — the routed tenant
///   exists and is servable; whether THIS caller may use it is exactly the
///   org-match check, which needs verified claims.
///
/// Note what is NOT deferred: `Foreign` (404), and a subdomain whose routing
/// row is missing (404) / provisioning (503) / suspended or offboarded (403).
/// Those are the enumeration-relevant answers, and they are now given without
/// verifying a token or touching a provisioning write path.
pub fn precheck(host: &HostTarget, routing: &RoutingState) -> Option<GateOutcome> {
    match host {
        HostTarget::Foreign => Some(GateOutcome::NotFound),
        // Needs the token (and, for an org token, a routing lookup keyed by the
        // token's own org id) — see this fn's doc.
        HostTarget::Apex => None,
        HostTarget::Subdomain(_) => match routing {
            RoutingState::Missing => Some(GateOutcome::NotFound),
            RoutingState::Provisioning => Some(GateOutcome::Unavailable),
            RoutingState::Suspended | RoutingState::Offboarded => {
                Some(GateOutcome::Forbidden("organization is not active"))
            }
            // Servable — the remaining question is whose token this is.
            RoutingState::Active(_) => None,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const APEX: &str = "coralswarm.example";

    /// The org a routing row is bound to in the matrices below, and the org
    /// whose token therefore *matches* it.
    const BOUND_ORG: &str = "org_a";
    /// A different org — the token that must never be served `BOUND_ORG`.
    const OTHER_ORG: &str = "org_b";
    /// A Clerk org that stands for a person's own ocean. Deliberately NOT
    /// equal to `BOUND_ORG`: a personal org must be served the consumer plane
    /// because of WHAT IT IS, never because it happens to match a routing row.
    const PERSONAL_ORG: &str = "org_personal_a";

    // ---------------------------------------------------------------------
    // The gate's input domain, enumerated ONCE.
    //
    // Every matrix test below draws its axes from these three functions
    // rather than repeating a literal list. Two reasons, both about the
    // failure mode this module actually has:
    //
    //  * One list means a state cannot be covered by one matrix and missed by
    //    another.
    //  * Each carries an exhaustiveness anchor — a `match` with no wildcard —
    //    so adding a variant to `HostTarget`/`TokenKind`/`RoutingState` fails
    //    to COMPILE here. Whoever adds it must come and say what the
    //    invariants claim about it. Silently shrinking every matrix's coverage
    //    is not one of the available outcomes.
    //
    // The sizes below are written as LITERALS on purpose. A matrix that
    // checks its own coverage against `all_routing_states().len()` is
    // checking a list against itself: drop a state and both sides shrink
    // together and the assertion still passes. Measured — that is not a
    // hypothetical. The literal is the independent second opinion, and
    // `the_input_domain_is_the_size_it_claims_to_be` is where the two meet.
    // ---------------------------------------------------------------------

    /// `HostTarget` variants covered — see the note above on why this is a
    /// literal.
    const HOST_COUNT: usize = 3;
    /// `RoutingState` variants covered.
    const ROUTING_STATE_COUNT: usize = 5;
    /// `TokenKind` cases covered (personal, matching org, non-matching org,
    /// personal org).
    const TOKEN_COUNT: usize = 4;
    /// Of those, the ones naming a REAL organization — one with a tenant
    /// database behind a routing row. Deliberately EXCLUDES
    /// [`TokenKind::PersonalOrg`], which has no tenant database and whose
    /// whole purpose is to be served the consumer plane.
    const ORG_TOKEN_COUNT: usize = 2;
    /// Of those, the ones naming a personal organization.
    const PERSONAL_ORG_TOKEN_COUNT: usize = 1;
    /// `(host, routing)` pairs [`precheck`] settles **before** the JWT is
    /// verified. A floor (`>=`) would let pre-auth rejects quietly move
    /// post-auth — re-introducing the enumeration signal the two-phase split
    /// exists to remove — so this is exact, and a change to it has to be
    /// deliberate.
    const PRECHECK_ANSWER_COUNT: usize = 9;

    /// Every `HostTarget`.
    ///
    /// Claim-based tenancy deletes `Subdomain` (Plan 1 PR 1.2). That will
    /// break this function to compile, which is the intent: the host axis
    /// disappearing is a change the matrices should be forced to acknowledge,
    /// not absorb.
    fn all_hosts() -> Vec<HostTarget> {
        let all = vec![
            HostTarget::Foreign,
            HostTarget::Apex,
            HostTarget::Subdomain("acme".into()),
        ];
        for h in &all {
            match h {
                HostTarget::Foreign | HostTarget::Apex | HostTarget::Subdomain(_) => {}
            }
        }
        all
    }

    /// Every `TokenKind` the gate can be handed — including both an org token
    /// that matches the bound routing row and one that does not, since "wrong
    /// org" is a distinct policy path from "an org".
    fn all_tokens() -> Vec<TokenKind> {
        let all = vec![
            TokenKind::Personal,
            TokenKind::Org(BOUND_ORG.into()),
            TokenKind::Org(OTHER_ORG.into()),
            TokenKind::PersonalOrg(PERSONAL_ORG.into()),
        ];
        for t in &all {
            match t {
                TokenKind::Personal | TokenKind::Org(_) | TokenKind::PersonalOrg(_) => {}
            }
        }
        all
    }

    /// Only the personal-organization tokens — the carve-out from
    /// [`org_tokens`].
    fn personal_org_tokens() -> Vec<TokenKind> {
        let all = vec![TokenKind::PersonalOrg(PERSONAL_ORG.into())];
        assert_eq!(
            all.len(),
            PERSONAL_ORG_TOKEN_COUNT,
            "personal-org token list and its literal count disagree"
        );
        all
    }

    /// Every `RoutingState`.
    fn all_routing_states() -> Vec<RoutingState> {
        let all = vec![
            RoutingState::Missing,
            RoutingState::Provisioning,
            RoutingState::Suspended,
            RoutingState::Offboarded,
            RoutingState::Active(BOUND_ORG.into()),
        ];
        for r in &all {
            match r {
                RoutingState::Missing
                | RoutingState::Provisioning
                | RoutingState::Suspended
                | RoutingState::Offboarded
                | RoutingState::Active(_) => {}
            }
        }
        all
    }

    /// Whether an outcome *declines* to serve the request.
    ///
    /// The two serving outcomes are named explicitly rather than caught by a
    /// `_` arm, so a future third way to serve a request cannot be silently
    /// classified as a refusal by every invariant that calls this.
    fn is_refusal(outcome: &GateOutcome) -> bool {
        match outcome {
            GateOutcome::Personal | GateOutcome::Org(_) => false,
            GateOutcome::NotFound
            | GateOutcome::Provisioning
            | GateOutcome::Unavailable
            | GateOutcome::Forbidden(_) => true,
        }
    }

    #[test]
    fn slug_charset_matches_migration_check() {
        assert!(is_valid_slug("acme"));
        assert!(is_valid_slug("acme-corp"));
        assert!(is_valid_slug("a1b"));
        assert!(!is_valid_slug("ab")); // too short (<3)
        assert!(!is_valid_slug("-acme")); // leading hyphen
        assert!(!is_valid_slug("acme-")); // trailing hyphen
        assert!(!is_valid_slug("Acme")); // uppercase
        assert!(!is_valid_slug("acme.corp")); // dot
        assert!(!is_valid_slug(&"a".repeat(33))); // too long (>32)
        // reserved apex labels are not valid tenant slugs
        assert!(!is_valid_slug("app"));
        assert!(!is_valid_slug("www"));
    }

    /// The production case, end to end through the pure layer: an org whose
    /// Clerk slug was auto-generated at 43 characters is unroutable, and the
    /// gate must now be able to SAY so instead of only answering 404.
    #[test]
    fn an_over_long_clerk_slug_is_diagnosed_not_just_404d() {
        let host = "Alex-s-organization-1781542054204631874.coralswarm.example";
        // The routing decision is unchanged — this is the bug's symptom, and
        // the diagnostic must not alter it.
        assert_eq!(classify_host(host, APEX), HostTarget::Foreign);
        assert_eq!(
            decide(&HostTarget::Foreign, &TokenKind::Personal, &RoutingState::Missing),
            GateOutcome::NotFound
        );
        // ...but the reason is now available to attach to that 404.
        let reason = unroutable_label_reason(host, APEX).expect("a 43-char label must be diagnosed");
        assert!(reason.contains("too long"), "reason should name the length problem: {reason}");
        assert!(reason.contains("32"), "reason should state the actual limit: {reason}");
    }

    #[test]
    fn unroutable_reason_covers_each_way_a_label_can_fail() {
        assert!(unroutable_label_reason(&format!("{}.{APEX}", "a".repeat(33)), APEX)
            .is_some_and(|r| r.contains("too long")));
        assert!(unroutable_label_reason(&format!("ab.{APEX}"), APEX).is_some_and(|r| r.contains("too short")));
        // Uppercase canonicalizes to lowercase and is therefore VALID, so the
        // charset case needs a character that survives canonicalization.
        assert!(unroutable_label_reason(&format!("acme_corp.{APEX}"), APEX)
            .is_some_and(|r| r.contains("lowercase letters")));
        assert!(unroutable_label_reason(&format!("-acme.{APEX}"), APEX).is_some_and(|r| r.contains("hyphen")));
    }

    /// The diagnostic must stay silent for everything that is NOT a
    /// misconfigured organization — above all for genuinely foreign domains,
    /// where naming our slug rules would disclose what we do and don't host.
    #[test]
    fn unroutable_reason_says_nothing_about_hosts_that_are_not_ours() {
        assert_eq!(unroutable_label_reason("evil.example.com", APEX), None);
        assert_eq!(unroutable_label_reason("coralswarm.example.evil.example.com", APEX), None);
        // Our apex, our reserved labels, and a perfectly good subdomain: all
        // deliberate routing outcomes, none of them a misconfiguration.
        assert_eq!(unroutable_label_reason(APEX, APEX), None);
        assert_eq!(unroutable_label_reason("app.coralswarm.example", APEX), None);
        assert_eq!(unroutable_label_reason("www.coralswarm.example", APEX), None);
        assert_eq!(unroutable_label_reason("acme.coralswarm.example", APEX), None);
        // A nested subdomain is a routing decision, not a bad org slug.
        assert_eq!(unroutable_label_reason("a.b.coralswarm.example", APEX), None);
        assert_eq!(unroutable_label_reason("", APEX), None);
    }

    /// The diagnostic canonicalizes exactly like the decision it explains —
    /// a port, a trailing dot or mixed case must not make it disagree with
    /// `classify_host` about which host is under discussion.
    #[test]
    fn unroutable_reason_canonicalizes_like_classify_host() {
        let long = "a".repeat(40);
        for raw in [
            format!("{long}.{APEX}"),
            format!("{long}.{APEX}:443"),
            format!("{long}.{APEX}."),
            format!("  {}.{APEX}  ", long.to_uppercase()),
        ] {
            assert_eq!(classify_host(&raw, APEX), HostTarget::Foreign, "host: {raw}");
            assert!(unroutable_label_reason(&raw, APEX).is_some(), "host: {raw}");
        }
        // And a valid slug stays undiagnosed through the same variations.
        for raw in ["acme.coralswarm.example:443", "ACME.coralswarm.example", "acme.coralswarm.example."] {
            assert!(matches!(classify_host(raw, APEX), HostTarget::Subdomain(_)), "host: {raw}");
            assert_eq!(unroutable_label_reason(raw, APEX), None, "host: {raw}");
        }
    }

    #[test]
    fn reserved_labels_stay_apex_never_a_tenant() {
        // app./www. always classify as the apex plane, and can never be produced
        // as a Subdomain (is_valid_slug rejects them → Foreign if ever forced).
        assert_eq!(classify_host("app.coralswarm.example", APEX), HostTarget::Apex);
        assert_eq!(classify_host("www.coralswarm.example", APEX), HostTarget::Apex);
    }

    #[test]
    fn classify_apex_and_app_hosts() {
        assert_eq!(classify_host("coralswarm.example", APEX), HostTarget::Apex);
        assert_eq!(classify_host("app.coralswarm.example", APEX), HostTarget::Apex);
        assert_eq!(classify_host("www.coralswarm.example", APEX), HostTarget::Apex);
    }

    #[test]
    fn classify_canonicalizes_spoofing_variants() {
        // case, trailing dot, and port are all normalized to the same subdomain
        for h in [
            "acme.coralswarm.example",
            "ACME.coralswarm.example",
            "acme.coralswarm.example.",
            "acme.coralswarm.example:443",
            "  acme.coralswarm.example  ",
        ] {
            assert_eq!(
                classify_host(h, APEX),
                HostTarget::Subdomain("acme".into()),
                "host {h:?}"
            );
        }
    }

    #[test]
    fn classify_rejects_foreign_and_nested_and_bad_slug() {
        assert_eq!(classify_host("evil.com", APEX), HostTarget::Foreign);
        assert_eq!(
            classify_host("acme.coralswarm.example.evil.com", APEX),
            HostTarget::Foreign
        );
        // nested subdomain is not a single-label tenant
        assert_eq!(
            classify_host("a.b.coralswarm.example", APEX),
            HostTarget::Foreign
        );
        // a subdomain that fails the slug charset is not a tenant
        assert_eq!(classify_host("-bad.coralswarm.example", APEX), HostTarget::Foreign);
        assert_eq!(classify_host("", APEX), HostTarget::Foreign);
    }

    #[test]
    fn apex_allows_personal_refuses_org() {
        assert_eq!(
            decide(&HostTarget::Apex, &TokenKind::Personal, &RoutingState::Missing),
            GateOutcome::Personal
        );
        assert!(matches!(
            decide(&HostTarget::Apex, &TokenKind::Org("org_a".into()), &RoutingState::Missing),
            GateOutcome::Forbidden(_)
        ));
    }

    /// The apex arm, state by state — the one test claim-based tenancy PR 1.2
    /// legitimately had to change.
    ///
    /// It used to assert that an org token on the apex is ALWAYS refused,
    /// because org content lived only on a subdomain. Now the apex is the
    /// tenant's door: **only** the `Active` expectation inverted, from
    /// "use your organization's subdomain" to a bound tenant. Every other
    /// state kept its exact status and message, and that is the property to
    /// hold on to — the plan names those four as hard requirements.
    ///
    /// If you are reading this because you are changing the gate again: this
    /// test pins today's behaviour and is *meant* to be edited. The
    /// cross-product invariants further down are not. Do not carry a change
    /// here across to them.
    ///
    /// Personal tokens on the apex are unaffected by `routing` in every branch
    /// (a control case, not asserted per-branch here — see
    /// `apex_allows_personal_refuses_org`).
    ///
    /// `/mcp` used to diverge here — its personal resource STRIPPED an
    /// `org_id` claim instead of honoring it, because Clerk's consent screen
    /// offered no "personal" option and a refusal would have left an org
    /// member with no way to reach their own ocean. **Both halves of that are
    /// gone:** personal organizations (migration 0208) gave the consent
    /// screen its personal row, #825 restored `user:org:read`, and the strip
    /// was deleted — `/mcp` now runs an org claim through this very arm, via
    /// `mcp::auth::resolve_personal_tenancy`. The two paths agree.
    #[test]
    fn apex_org_token_matrix_binds_only_on_an_active_row() {
        let org = TokenKind::Org(BOUND_ORG.into());

        // ── unchanged by PR 1.2 ──────────────────────────────────────────
        // No routing row at all — brand-new OR a pre-split "shared-DB" org
        // that has not been migrated yet: fail-closed 403, the SAME message
        // `/mcp` gives this case.
        assert_eq!(
            decide(&HostTarget::Apex, &org, &RoutingState::Missing),
            GateOutcome::Forbidden("organization is not provisioned for service")
        );
        // Mid-onboarding: retryable 503, not a permanent no, and readable as
        // "provisioning" (self-serve signup PR A2).
        assert_eq!(
            decide(&HostTarget::Apex, &org, &RoutingState::Provisioning),
            GateOutcome::Provisioning
        );
        // Write-frozen / torn down: a real rejection.
        assert_eq!(
            decide(&HostTarget::Apex, &org, &RoutingState::Suspended),
            GateOutcome::Forbidden("organization is not active")
        );
        assert_eq!(
            decide(&HostTarget::Apex, &org, &RoutingState::Offboarded),
            GateOutcome::Forbidden("organization is not active")
        );

        // ── the one arm PR 1.2 inverted ──────────────────────────────────
        // Fully onboarded and active: the apex now binds this tenant.
        assert_eq!(
            decide(&HostTarget::Apex, &org, &RoutingState::Active(BOUND_ORG.into())),
            GateOutcome::Org(BOUND_ORG.into())
        );
        // ...but only for the org the token actually claimed. A routing row
        // bound to a different org than the claim means the control plane
        // disagrees with itself; serving it would hand this caller another
        // tenant's database.
        assert_eq!(
            decide(
                &HostTarget::Apex,
                &TokenKind::Org(OTHER_ORG.into()),
                &RoutingState::Active(BOUND_ORG.into())
            ),
            GateOutcome::Forbidden("token does not match this organization")
        );
    }

    #[test]
    fn foreign_host_always_404() {
        assert_eq!(
            decide(
                &HostTarget::Foreign,
                &TokenKind::Org("org_a".into()),
                &RoutingState::Active("org_a".into())
            ),
            GateOutcome::NotFound
        );
    }

    #[test]
    fn org_subdomain_matrix() {
        let sub = HostTarget::Subdomain("acme".into());
        let org = |o: &str| TokenKind::Org(o.into());

        // unknown subdomain → 404 (fail closed)
        assert_eq!(decide(&sub, &org("org_a"), &RoutingState::Missing), GateOutcome::NotFound);
        // provisioning → 503
        assert_eq!(decide(&sub, &org("org_a"), &RoutingState::Provisioning), GateOutcome::Unavailable);
        // suspended / offboarded → 403
        assert!(matches!(decide(&sub, &org("org_a"), &RoutingState::Suspended), GateOutcome::Forbidden(_)));
        assert!(matches!(decide(&sub, &org("org_a"), &RoutingState::Offboarded), GateOutcome::Forbidden(_)));
        // active + matching org → proceed
        assert_eq!(
            decide(&sub, &org("org_a"), &RoutingState::Active("org_a".into())),
            GateOutcome::Org("org_a".into())
        );
        // active + WRONG org (org A token on org B subdomain) → 403
        assert!(matches!(
            decide(&sub, &org("org_b"), &RoutingState::Active("org_a".into())),
            GateOutcome::Forbidden(_)
        ));
        // active + personal token → 403
        assert!(matches!(
            decide(&sub, &TokenKind::Personal, &RoutingState::Active("org_a".into())),
            GateOutcome::Forbidden(_)
        ));
    }

    /// The load-bearing invariant of the early-reject split: whenever
    /// [`precheck`] answers, its answer is EXACTLY [`decide`]'s — for every
    /// token kind, over the full (host × routing × token) cross-product.
    ///
    /// This is what makes moving Host classification ahead of JWT verification
    /// safe: an early reject can only ever return the same status the old
    /// post-auth ordering returned, never a new or different one. If someone
    /// later edits one function's policy without the other, this fails.
    #[test]
    fn precheck_matches_decide_for_every_token() {
        // The axes are the shared enumerations, so this matrix and the
        // claim-based-tenancy invariants below can never cover different sets.
        let hosts = all_hosts();
        let routings = all_routing_states();
        let tokens = all_tokens();
        let mut compared = 0usize;

        for host in &hosts {
            for routing in &routings {
                let early = precheck(host, routing);
                for token in &tokens {
                    let full = decide(host, token, routing);
                    match &early {
                        Some(outcome) => {
                            assert_eq!(
                                *outcome, full,
                                "precheck({host:?}, {routing:?}) = {outcome:?} but \
                                 decide(.., {token:?}, ..) = {full:?}"
                            );
                            compared += 1;
                        }
                        // Deferred: only ever because the verdict really does
                        // depend on the token — proceed outcomes, or a
                        // rejection whose choice needs the claims.
                        None => {}
                    }
                }
            }
        }

        assert_eq!(
            compared,
            PRECHECK_ANSWER_COUNT * TOKEN_COUNT,
            "only {compared} (precheck, decide) pairs were actually compared — an \
             equality matrix that compares nothing passes for free"
        );
    }

    /// The deferral set is exactly the two token-dependent cases, and nothing
    /// else: a subdomain that is unknown / provisioning / suspended /
    /// offboarded must ALWAYS be answered early (that is the whole point —
    /// no JWT verification, no provisioning write, no enumeration signal).
    #[test]
    fn precheck_defers_only_where_the_token_actually_matters() {
        let sub = HostTarget::Subdomain("acme".into());

        assert!(precheck(&HostTarget::Foreign, &RoutingState::Missing).is_some());
        for routing in [
            RoutingState::Missing,
            RoutingState::Provisioning,
            RoutingState::Suspended,
            RoutingState::Offboarded,
        ] {
            assert!(
                precheck(&sub, &routing).is_some(),
                "a {routing:?} subdomain must be rejected before auth"
            );
        }
        assert_eq!(
            precheck(&sub, &RoutingState::Missing),
            Some(GateOutcome::NotFound),
            "an unknown subdomain is a fail-closed 404, pre-auth"
        );

        // Token-dependent: the org-match check and the apex plane.
        assert!(precheck(&sub, &RoutingState::Active("org_a".into())).is_none());
        for routing in [
            RoutingState::Missing,
            RoutingState::Provisioning,
            RoutingState::Suspended,
            RoutingState::Offboarded,
            RoutingState::Active("org_a".into()),
        ] {
            assert!(
                precheck(&HostTarget::Apex, &routing).is_none(),
                "the apex plane is decided by the token, never pre-auth"
            );
        }
    }

    // =====================================================================
    // Claim-based tenancy — Plan 1 Phase 1, PR 1.1.
    // `docs/plans/claim-based-tenancy.md`
    //
    // The invariants below are written BEFORE the behaviour they guard
    // changes, and against today's gate, where every one of them passes
    // trivially. That is the point.
    //
    // Claim-based tenancy removes the Host from the tenancy decision, which
    // removes the cross-check between two independent signals. What is left
    // is this arm. The `Active` case will be rewritten (PR 1.2) to bind a
    // tenant instead of refusing, and the tests that pin today's refusal will
    // legitimately have to change with it — including
    // `apex_org_token_matrix_refuses_every_routing_state` above.
    //
    // The realistic failure mode is not someone deleting a check. It is
    // someone correctly updating the one expectation that must invert, then
    // applying the same reasoning to its neighbours. So these are matrices
    // over the whole input domain, not named cases: there is no single arm to
    // edit, and they hold identically before and after the rewrite. If a
    // change makes one of them red, the change is wrong.
    // =====================================================================

    /// Where the enumerations and the literal sizes meet.
    ///
    /// Every matrix below measures its own coverage against the literals, and
    /// this is the one place that says the literals describe the real lists.
    /// Deleting an axis entry now fails **here**, loudly, instead of shrinking
    /// a matrix and its expectation together — which is how a coverage
    /// assertion silently becomes decoration.
    ///
    /// If a variant is genuinely added or removed, the honest fix is to change
    /// the literal deliberately, having decided what the invariants claim
    /// about the new domain.
    #[test]
    fn the_input_domain_is_the_size_it_claims_to_be() {
        let hosts = all_hosts();
        let routings = all_routing_states();
        let tokens = all_tokens();

        assert_eq!(hosts.len(), HOST_COUNT, "host axis");
        assert_eq!(routings.len(), ROUTING_STATE_COUNT, "routing axis");
        assert_eq!(tokens.len(), TOKEN_COUNT, "token axis");
        assert_eq!(org_tokens().len(), ORG_TOKEN_COUNT, "org-token axis");

        // Length is not coverage, and the anchors do not close this.
        //
        // Each `all_*` anchor is a `match` over the *type*, so it proves every
        // variant is HANDLED, not that any is PRESENT. `vec![Foreign, Foreign]`
        // compiles, keeps `HOST_COUNT` at 3, satisfies every
        // `assert_eq!(checked, HOST_COUNT * ...)` exactly — and removes the
        // apex from every matrix in this file at once. Measured, not guessed.
        //
        // So name what must actually appear. Deletion is caught by the counts;
        // this catches replacement.
        assert!(
            hosts.iter().any(|h| matches!(h, HostTarget::Foreign)),
            "host axis lost Foreign"
        );
        assert!(
            hosts.iter().any(|h| matches!(h, HostTarget::Apex)),
            "host axis lost Apex — the arm the whole plan turns on"
        );
        assert!(
            hosts.iter().any(|h| matches!(h, HostTarget::Subdomain(_))),
            "host axis lost Subdomain"
        );

        assert!(
            routings.iter().any(|r| matches!(r, RoutingState::Missing)),
            "routing axis lost Missing"
        );
        assert!(
            routings.iter().any(|r| matches!(r, RoutingState::Provisioning)),
            "routing axis lost Provisioning"
        );
        assert!(
            routings.iter().any(|r| matches!(r, RoutingState::Suspended)),
            "routing axis lost Suspended"
        );
        assert!(
            routings.iter().any(|r| matches!(r, RoutingState::Offboarded)),
            "routing axis lost Offboarded"
        );
        assert_eq!(
            routings.iter().filter(|r| matches!(r, RoutingState::Active(_))).count(),
            1,
            "exactly one Active row — `an_org_token_is_servable_only_on_an_active_routing_row` \
             subtracts exactly one from the axis"
        );

        // The token axis needs more than presence. Both org tokens must name
        // DIFFERENT orgs, with exactly one matching the bound routing row.
        // Replacing the non-matching one with a second copy of the matching
        // one keeps both counts and silently deletes the wrong-org case — the
        // single case `a_bound_tenant_is_always_the_one_the_token_named`
        // exists to catch.
        assert_ne!(BOUND_ORG, OTHER_ORG, "the two org fixtures must be different orgs");
        assert!(
            tokens.iter().any(|t| matches!(t, TokenKind::Personal)),
            "token axis lost Personal"
        );
        assert!(
            tokens.iter().any(|t| matches!(t, TokenKind::Org(o) if o == BOUND_ORG)),
            "token axis lost the org token that MATCHES the bound routing row"
        );
        assert!(
            tokens.iter().any(|t| matches!(t, TokenKind::Org(o) if o != BOUND_ORG)),
            "token axis lost the org token that does NOT match the bound routing row"
        );
        assert!(
            routings.iter().any(|r| matches!(r, RoutingState::Active(o) if o == BOUND_ORG)),
            "the Active row must be bound to BOUND_ORG, or \"matching org\" means nothing"
        );
    }

    /// The org tokens from [`all_tokens`] — asserted non-empty, so a matrix
    /// that filters for them can never quietly iterate over nothing.
    fn org_tokens() -> Vec<TokenKind> {
        let orgs: Vec<TokenKind> = all_tokens()
            .into_iter()
            .filter(|t| matches!(t, TokenKind::Org(_)))
            .collect();
        assert!(!orgs.is_empty(), "the org-token axis must not be empty");
        orgs
    }

    /// **[SEC — HIGH] A REAL org token is never served the consumer plane.**
    ///
    /// Not on any host, not in any routing state — not even the one the gate
    /// is about to start serving.
    ///
    /// ## The carve-out, and why it does not weaken this
    ///
    /// This used to read "an org token", full stop. It now excludes
    /// [`TokenKind::PersonalOrg`], and that narrowing is the whole point of
    /// personal organizations: they have no tenant database, they live on the
    /// consumer plane by design, and the org id names an ACCOUNT there rather
    /// than a database.
    ///
    /// The danger this test guards is a caller with a tenant database of their
    /// own being handed the shared plane instead. A personal org has no tenant
    /// database to be diverted from, so it cannot suffer that failure. What it
    /// COULD suffer is the mirror image — being bound to somebody's tenant —
    /// and [`a_personal_org_is_never_bound_to_a_tenant`] is the guard for
    /// that. The two together still cover every token.
    ///
    /// **The narrowing is only sound because
    /// `middleware::auth::classify_personal_org_claim` has already proved the
    /// org is in `personal_orgs` AND belongs to this caller's own `sub`.** A
    /// `PersonalOrg` this gate cannot trust would make this carve-out a hole.
    /// See `tenancy::personal_org` for that proof.
    ///
    /// This is the whole safety story once the Host stops corroborating the
    /// claim. Landing an org caller on the consumer plane is not a partial
    /// failure: reachability there has **no account filter at all**
    /// (`migrations_control/0084_bootstrap_scope_functions.sql:190-199`
    /// filters by `user_id` only), so the caller is handed the union of every
    /// account they belong to.
    ///
    /// Note this is deliberately stronger than "never `Personal` except when
    /// `Active`" — `Active` must yield [`GateOutcome::Org`], so `Personal` is
    /// wrong there too, and there is no reason to carve out the one state
    /// where a mistake would be most expensive.
    #[test]
    fn a_real_org_token_is_never_served_the_consumer_plane() {
        let mut checked = 0usize;
        for host in all_hosts() {
            for routing in all_routing_states() {
                for token in org_tokens() {
                    let outcome = decide(&host, &token, &routing);
                    assert_ne!(
                        outcome,
                        GateOutcome::Personal,
                        "a REAL org token was served the consumer plane: \
                         decide({host:?}, {token:?}, {routing:?}) — this is the \
                         wrong-plane failure the whole design fears; a personal \
                         org is the ONLY sanctioned exception and it is a \
                         different variant"
                    );
                    checked += 1;
                }
            }
        }
        assert_eq!(
            checked,
            HOST_COUNT * ROUTING_STATE_COUNT * ORG_TOKEN_COUNT,
            "the matrix did not cover the full input domain — a green result here \
             would mean nothing"
        );
    }

    /// **[SEC — HIGH] A personal org is never bound to a tenant.**
    ///
    /// The mirror image of the test above, and the half of the old
    /// "an org token is never served the consumer plane" that the carve-out
    /// gave up. A personal organization has no tenant database and no routing
    /// row; binding one to a tenant would hand a caller somebody else's
    /// database on the strength of a row that is not supposed to exist.
    ///
    /// Note this holds on EVERY host and in EVERY routing state, including
    /// `Active` — the state where a routing row exists and a careless
    /// implementation would be most tempted to use it.
    #[test]
    fn a_personal_org_is_never_bound_to_a_tenant() {
        let mut checked = 0usize;
        for host in all_hosts() {
            for routing in all_routing_states() {
                for token in personal_org_tokens() {
                    let outcome = decide(&host, &token, &routing);
                    assert!(
                        !matches!(outcome, GateOutcome::Org(_)),
                        "a personal org was bound to a tenant: \
                         decide({host:?}, {token:?}, {routing:?}) -> {outcome:?}"
                    );
                    checked += 1;
                }
            }
        }
        assert_eq!(
            checked,
            HOST_COUNT * ROUTING_STATE_COUNT * PERSONAL_ORG_TOKEN_COUNT,
            "the matrix did not cover the full input domain — a green result here \
             would mean nothing"
        );
    }

    /// **On the apex, a personal org reaches the consumer plane in EVERY
    /// routing state.**
    ///
    /// The positive form, and it is doing real work: it pins that `decide`
    /// does NOT consult `routing` for this variant. A personal org has no
    /// routing row on purpose, so every state it can be handed is meaningless
    /// for it — and an implementation that checked anyway would refuse
    /// `Missing`, which is the state every personal org is permanently in.
    ///
    /// Without this, `a_personal_org_is_never_bound_to_a_tenant` above is
    /// satisfied by a gate that refuses personal orgs outright, i.e. by the
    /// feature not working at all.
    #[test]
    fn on_the_apex_a_personal_org_reaches_the_consumer_plane_in_every_routing_state() {
        let mut checked = 0usize;
        for routing in all_routing_states() {
            for token in personal_org_tokens() {
                assert_eq!(
                    decide(&HostTarget::Apex, &token, &routing),
                    GateOutcome::Personal,
                    "a personal org was refused on the apex in routing state \
                     {routing:?} — it has no routing row BY DESIGN, so no state \
                     may change this answer"
                );
                checked += 1;
            }
        }
        assert_eq!(
            checked,
            ROUTING_STATE_COUNT * PERSONAL_ORG_TOKEN_COUNT,
            "the matrix did not cover every routing state — a green result here \
             would mean nothing"
        );
    }

    /// **Only an `Active` routing row can ever serve an org token.**
    ///
    /// `Missing`, `Provisioning`, `Suspended` and `Offboarded` are refusals
    /// today and stay refusals after the `Active` arm starts binding a tenant
    /// — neither `Personal` (the consumer-plane leak above) nor `Org` (which
    /// would serve a tenant that is un-provisioned, half-built, write-frozen
    /// or torn down).
    ///
    /// A lookup *error* is not a state on this axis: `finish_binding`
    /// (`runtime.rs`) maps a failed `resolve_routing_by_org` to
    /// `GateReject::Unavailable` without ever reaching [`decide`]. That path
    /// must keep failing closed too; it simply cannot be asserted here.
    #[test]
    fn an_org_token_is_servable_only_on_an_active_routing_row() {
        let mut checked = 0usize;
        for host in all_hosts() {
            for routing in all_routing_states() {
                if matches!(routing, RoutingState::Active(_)) {
                    continue;
                }
                for token in org_tokens() {
                    let outcome = decide(&host, &token, &routing);
                    assert!(
                        is_refusal(&outcome),
                        "an org token was served on a non-active routing row: \
                         decide({host:?}, {token:?}, {routing:?}) = {outcome:?}"
                    );
                    checked += 1;
                }
            }
        }
        assert_eq!(
            checked,
            HOST_COUNT * (ROUTING_STATE_COUNT - 1) * ORG_TOKEN_COUNT,
            "the matrix did not cover every non-active routing state"
        );
    }

    /// **[SEC — HIGH] The tenant served is the tenant the token named.**
    ///
    /// The invariants above constrain [`GateOutcome::Org`] only by saying it
    /// is not `Personal`. They say nothing about *which* org — so org B's
    /// token being handed org A's tenant satisfies every one of them. This is
    /// the other half of the boundary, and it is the half that is about to
    /// lose its only enforcement.
    ///
    /// Today the claim ↔ row equality check exists in exactly one place in the
    /// codebase: `claim_org == bound_org`, inside the `Subdomain` arm of
    /// [`decide`]. **PR 1.2 deletes that arm.** The mechanically obvious way
    /// to write the new `Active` arm — mirroring the deleted code minus its
    /// `if` — leaves no claim ↔ row comparison anywhere.
    ///
    /// Nothing downstream would catch it. `finish_binding` builds the pool
    /// from the *row's* `account_id`/`db_secret_ref`, and the registry's
    /// `tenant_meta` fingerprint assert then confirms the DSN matches **that**
    /// row's org — so it verifies the wrong tenant's identity and returns its
    /// pool, while `live_org_membership` ran earlier against the *other* org.
    /// Membership checked for B, pool handed for A, and no layer between them
    /// compares the two.
    ///
    /// It is not enough that `finish_binding` happens to look the row up by
    /// the caller's own claim. That is one call site's habit, not a property —
    /// and Phase 3's `select_tenant`, plus the deferred `id → tenant` index
    /// for link sharing, both deliberately resolve a row from something *other
    /// than* the caller's claim. This is the assertion that makes the habit a
    /// rule.
    #[test]
    fn a_bound_tenant_is_always_the_one_the_token_named() {
        let mut served = 0usize;
        for host in all_hosts() {
            for routing in all_routing_states() {
                for token in all_tokens() {
                    if let GateOutcome::Org(bound) = decide(&host, &token, &routing) {
                        assert_eq!(
                            TokenKind::Org(bound.clone()),
                            token,
                            "decide({host:?}, {token:?}, {routing:?}) bound tenant {bound:?} — \
                             the gate served a tenant the caller never named"
                        );
                        served += 1;
                    }
                }
            }
        }
        assert!(
            served > 0,
            "no input bound a tenant at all, so this asserted nothing — a gate that \
             refuses everything satisfies every invariant in this file"
        );
    }

    /// A gate that refuses **everything** satisfies every "never serves X"
    /// invariant above. Pin that both planes are still reachable, so the
    /// matrices are two-sided and a total-refusal regression is loud.
    #[test]
    fn the_gate_still_serves_both_planes() {
        let (mut personal, mut org) = (0usize, 0usize);
        for host in all_hosts() {
            for routing in all_routing_states() {
                for token in all_tokens() {
                    match decide(&host, &token, &routing) {
                        GateOutcome::Personal => personal += 1,
                        GateOutcome::Org(_) => org += 1,
                        GateOutcome::NotFound
                        | GateOutcome::Provisioning
                        | GateOutcome::Unavailable
                        | GateOutcome::Forbidden(_) => {}
                    }
                }
            }
        }
        assert!(personal > 0, "no input reaches the consumer plane");
        assert!(org > 0, "no input binds a tenant");
    }

    /// The mirror of the leak: **a personal token is never bound to a
    /// tenant.**
    ///
    /// [`TokenKind::Personal`] means *no tenant was named* — by a claim today,
    /// and by an explicit selection after Phase 3. If it can come back
    /// [`GateOutcome::Org`], the gate has invented a tenant nobody asked for.
    ///
    /// Deliberately phrased in terms of the gate's input, not the wire,
    /// because Phase 3 makes the wire-level version false: PR 3.1 drops
    /// `user:org:read` so **every MCP token arrives personal-shaped**, and PR
    /// 3.2 then binds a tenant from a stored selection. That caller has no
    /// `org_id` claim and is nonetheless an org caller.
    ///
    /// If you are about to relax this because your caller's token carries no
    /// `org_id`: don't. `finish_binding` takes `org_claim` as a plain
    /// `Option<&str>` parameter precisely so a selection can be passed there.
    /// Whatever names the tenant must reach [`decide`] as
    /// [`TokenKind::Org`] — that is what makes
    /// `a_bound_tenant_is_always_the_one_the_token_named` meaningful for a
    /// selected tenant too.
    #[test]
    fn a_personal_token_is_never_bound_to_a_tenant() {
        let mut checked = 0usize;
        for host in all_hosts() {
            for routing in all_routing_states() {
                let outcome = decide(&host, &TokenKind::Personal, &routing);
                assert!(
                    !matches!(outcome, GateOutcome::Org(_)),
                    "a personal token was bound to a tenant: \
                     decide({host:?}, Personal, {routing:?}) = {outcome:?}"
                );
                checked += 1;
            }
        }
        assert_eq!(
            checked,
            HOST_COUNT * ROUTING_STATE_COUNT,
            "the matrix did not cover the full (host × routing) domain"
        );
    }

    /// [`precheck`] runs **before** the JWT is verified, so it knows nothing
    /// about who is calling. It may therefore only ever *refuse*. If it can
    /// answer `Personal` or `Org`, an unauthenticated request is served a
    /// plane on the strength of its `Host` header alone.
    ///
    /// `precheck_matches_decide_for_every_token` already pins precheck to
    /// [`decide`], but only where precheck answers at all — it is an equality
    /// check, so both halves drifting together satisfies it. This one does
    /// not depend on `decide` being right.
    #[test]
    fn precheck_can_only_ever_refuse() {
        let mut answered = 0usize;
        for host in all_hosts() {
            for routing in all_routing_states() {
                if let Some(outcome) = precheck(&host, &routing) {
                    assert!(
                        is_refusal(&outcome),
                        "precheck admitted an unauthenticated request: \
                         precheck({host:?}, {routing:?}) = {outcome:?}"
                    );
                    answered += 1;
                }
            }
        }
        assert_eq!(
            answered, PRECHECK_ANSWER_COUNT,
            "precheck settled {answered} (host, routing) pairs pre-auth, expected \
             {PRECHECK_ANSWER_COUNT} — a reject that moves post-auth puts the \
             enumeration signal back"
        );
    }
}

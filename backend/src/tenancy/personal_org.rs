//! Personal organizations: a Clerk org that stands for one person's own ocean.
//!
//! # Why this exists
//!
//! Clerk draws the OAuth consent screen, and that screen lists **only
//! organizations**. It has no row for a person's own ocean. So somebody
//! connecting an MCP client for their personal ocean reaches a picker that
//! cannot express what they want. That is why `user:org:read` was dropped in
//! #810 and tenant selection moved to a post-connect tool.
//!
//! The fix (founder decision, 2026-09-07) is to give every user a Clerk
//! organization that **stands for** their own ocean, so the picker has a row
//! for it. `personal_orgs` (control migration 0208) records which org stands
//! for whom.
//!
//! A personal org gets **no database and no routing row**. Its org id is
//! translated back to the caller's ordinary personal path before anything
//! downstream tries to resolve a tenant. This module is the policy for that
//! translation; the lookup and the wiring live in
//! [`crate::middleware::auth`], next to `live_org_membership`.
//!
//! # Why this needs its own guard, separate from `tenancy::gate`
//!
//! [`crate::tenancy::gate::decide`] is reached *after* the claim has already
//! been turned into a [`TokenKind`]. By then the translation has happened or
//! not, and the gate cannot tell the difference: a stripped claim simply looks
//! like a personal token, which is exactly what it is meant to look like. The
//! gate's invariants stay true either way, so a broken translation would leave
//! every one of them green.
//!
//! The translation also has to run *before*
//! [`crate::middleware::auth::resolve_clerk_context`], not after. Two reasons,
//! and **neither of them is "the org has no `accounts` row"** — it has one.
//! Clerk's `organization.created` webhook provisions one for every org,
//! personal or not (`clerk_webhook::handle_org_created` ->
//! `clerk_provision::ensure_org_account`), and since 2026-09-08 adoption
//! (`bin/adopt_personal_org_account`) can even relabel the caller's OWN
//! personal account onto it — see [`ClaimAction::ServeAsPersonalOrg`]'s own
//! doc below, which resolves exactly that account.
//!
//! First, this module can decide to **refuse**, and a refusal has to land
//! before anything binds an account. `live_org_membership` cannot make that
//! call: it asks only whether the caller is a member of the claimed org, and a
//! personal org is an ordinary Clerk org, so that can be true for somebody
//! else's — see [`PersonalOrgClaim::PersonalOrgOfAnotherUser`].
//!
//! Second, the decision compares the token's own `sub` against
//! `personal_orgs.clerk_user_id`. `resolve_clerk_context` is the function that
//! maps a Clerk subject to a local `users` row, so before it runs there is no
//! internal UUID to compare against even if we wanted one. See
//! `middleware::auth::classify_personal_org_claim`.
//!
//! The real danger of running this late is what an untranslated claim becomes
//! downstream: with no `personal_org` recorded,
//! [`crate::tenancy::gate::decide`] classifies it as an ordinary
//! `TokenKind::Org`, and that gate's apex arm for `TokenKind::Org` refuses any
//! org with no `tenant_databases` routing row — which a personal org
//! deliberately never has — rather than ever falling through to the consumer
//! plane. So a late translation still refuses every personal-org token, just
//! via a code path that was never meant to evaluate one, for a reason that has
//! nothing to do with a missing account.
//!
//! [`TokenKind`]: crate::tenancy::gate::TokenKind
//!
//! # Why "keep" and "strip" are the outcomes, not "personal" and "org"
//!
//! The dangerous mistake here is not refusing too much — a refusal is loud and
//! a user complains. It is **stripping an ordinary org claim**, which would
//! serve the caller their own personal ocean while they believe they are
//! working in their organization: a wrong-tenant answer with a `200` on it.
//! Naming the outcomes after the action makes that the thing the invariants
//! below can actually assert.
//!
//! # This guard binds only if the request path routes its decision through
//! [`resolve_personal_org_plane`]
//!
//! Nothing in the type system enforces that. A second, parallel translation
//! written somewhere else would leave every test in this file green while
//! guarding nothing, and it would look identical from outside. Both call sites
//! — `middleware::auth::require_auth` (REST) and `mcp::auth::mcp_auth` (MCP) —
//! reach this function through
//! [`crate::middleware::auth::normalize_personal_org_claim`], and
//! `personal_org_wiring_pg.rs` is what proves it on the real request path.
//! See the same hazard recorded for `mcp::tenant_selection` in #804.

/// What the verified token's `org_id` claim turns out to be, once the
/// `personal_orgs` map (control migration 0208) has been consulted.
///
/// The two "personal org" variants are distinguished by comparing the map's
/// `clerk_user_id` against the **token's own `sub`** — never against anything
/// the caller can set, and never against a local row whose absence could
/// change the answer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum PersonalOrgClaim {
    /// The token carries no `org_id` at all. The overwhelmingly common case,
    /// and the one every personal caller is in today.
    NoOrgClaim,
    /// The token names an organization with no row in `personal_orgs`: an
    /// ordinary org, with a real tenant database behind it.
    OrdinaryOrg,
    /// The token names a personal org, and the map says it stands for **this**
    /// caller's own ocean.
    PersonalOrgOfCaller,
    /// The token names a personal org that stands for **somebody else's**
    /// ocean.
    ///
    /// Reachable without any compromise: Clerk lets a user be invited into
    /// another user's organization, and a personal org is an ordinary Clerk
    /// org in every respect Clerk cares about. So this is a state the product
    /// can produce on its own, not only an attack.
    PersonalOrgOfAnotherUser,
}

/// What the request path must do with the token's claims.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ClaimAction {
    /// Leave the claims exactly as the token presented them.
    KeepClaim,
    /// Serve this request on the shared consumer plane, bound to the personal
    /// organization's OWN account.
    ///
    /// The claim is deliberately **kept**, not stripped. An earlier version of
    /// this module stripped it, which sent the request to the caller's old
    /// personal account; the founder chose instead (2026-09-08) to make the
    /// organization's own account the home, still on the shared database. So
    /// the org id has to survive far enough for `resolve_clerk_context` to
    /// resolve that account and for `tenancy::gate` to be handed
    /// `TokenKind::PersonalOrg`.
    ServeAsPersonalOrg,
    /// Refuse the request outright.
    Refuse(&'static str),
}

/// The policy. Total over [`PersonalOrgClaim`], no wildcard arm: adding a
/// variant is `error[E0004]` here and in both enumerators in the test module
/// below.
pub(crate) fn resolve_personal_org_plane(claim: &PersonalOrgClaim) -> ClaimAction {
    match claim {
        // Nothing to translate. Kept rather than stripped so that "strip"
        // means one thing only — see the invariant
        // `only_the_callers_own_personal_org_is_served_as_one`.
        PersonalOrgClaim::NoOrgClaim => ClaimAction::KeepClaim,
        // An ordinary org must reach the gate with its claim intact. Stripping
        // here is the silent wrong-tenant failure this module exists to make
        // impossible.
        PersonalOrgClaim::OrdinaryOrg => ClaimAction::KeepClaim,
        // The whole point of the feature: the org's own account, on the
        // shared plane.
        PersonalOrgClaim::PersonalOrgOfCaller => ClaimAction::ServeAsPersonalOrg,
        // Refused, not quietly redirected. Serving here would hand the caller
        // their OWN ocean while they asked for someone else's — a benign
        // result reported as if the request had been honored. A refusal says
        // what happened.
        PersonalOrgClaim::PersonalOrgOfAnotherUser => {
            ClaimAction::Refuse("this organization is another user's personal ocean")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Written as LITERALS on purpose. A matrix that sizes itself from
    /// `all_claims().len()` is checking a list against itself and would stay
    /// green after a variant was dropped from that list.
    const CLAIM_COUNT: usize = 4;
    /// Of those, the ones naming a personal org.
    const PERSONAL_ORG_CLAIM_COUNT: usize = 2;
    /// Of those, the ones naming an org of any kind.
    const ORG_BEARING_CLAIM_COUNT: usize = 3;

    fn all_claims() -> Vec<PersonalOrgClaim> {
        let all = vec![
            PersonalOrgClaim::NoOrgClaim,
            PersonalOrgClaim::OrdinaryOrg,
            PersonalOrgClaim::PersonalOrgOfCaller,
            PersonalOrgClaim::PersonalOrgOfAnotherUser,
        ];
        // Exhaustiveness anchor: no wildcard, so a new variant fails to
        // compile here until it is added to `all` above.
        for c in &all {
            match c {
                PersonalOrgClaim::NoOrgClaim
                | PersonalOrgClaim::OrdinaryOrg
                | PersonalOrgClaim::PersonalOrgOfCaller
                | PersonalOrgClaim::PersonalOrgOfAnotherUser => {}
            }
        }
        all
    }

    fn personal_org_claims() -> Vec<PersonalOrgClaim> {
        let all = vec![
            PersonalOrgClaim::PersonalOrgOfCaller,
            PersonalOrgClaim::PersonalOrgOfAnotherUser,
        ];
        assert_eq!(
            all.len(),
            PERSONAL_ORG_CLAIM_COUNT,
            "personal-org claim list and its literal count disagree"
        );
        all
    }

    #[test]
    fn the_input_domain_is_the_size_it_claims_to_be() {
        assert_eq!(all_claims().len(), CLAIM_COUNT, "claim axis changed size");
        for expected in [
            PersonalOrgClaim::NoOrgClaim,
            PersonalOrgClaim::OrdinaryOrg,
            PersonalOrgClaim::PersonalOrgOfCaller,
            PersonalOrgClaim::PersonalOrgOfAnotherUser,
        ] {
            assert!(
                all_claims().contains(&expected),
                "{expected:?} is missing from the matrix input"
            );
        }
        let org_bearing = all_claims()
            .iter()
            .filter(|c| !matches!(c, PersonalOrgClaim::NoOrgClaim))
            .count();
        assert_eq!(
            org_bearing, ORG_BEARING_CLAIM_COUNT,
            "org-bearing claim count changed"
        );
    }

    /// **[SEC — HIGH] An ordinary org claim is never stripped.**
    ///
    /// This is the whole safety story for the translation. Stripping an
    /// ordinary org claim does not fail loudly: the caller is served their own
    /// personal ocean, with a `200`, while believing they are in their
    /// organization. Nothing downstream can detect it, because a stripped
    /// claim is indistinguishable from a token that never carried one — that
    /// is precisely what makes the translation safe when it is right, and
    /// undetectable when it is wrong.
    #[test]
    fn an_ordinary_org_claim_is_never_served_as_a_personal_org() {
        let mut checked = 0usize;
        for claim in all_claims() {
            if !matches!(claim, PersonalOrgClaim::OrdinaryOrg) {
                continue;
            }
            let action = resolve_personal_org_plane(&claim);
            assert_ne!(
                action,
                ClaimAction::ServeAsPersonalOrg,
                "an ordinary org claim was stripped: resolve_personal_org_plane({claim:?}) \
                 — the caller would be served their own personal ocean with a 200 on it"
            );
            checked += 1;
        }
        assert_eq!(
            checked, 1,
            "the matrix did not reach the ordinary-org claim at all — a green result \
             here would mean nothing"
        );
    }

    /// **[SEC — HIGH] A personal org is never kept as a tenant claim.**
    ///
    /// A personal org has no routing row on purpose, so keeping its claim
    /// sends it to `gate::decide`, which answers `Forbidden("organization is
    /// not provisioned for service")`. That is fail-closed, so it is not a
    /// data leak — but it is the exact stranding this feature exists to
    /// prevent, and it strands the caller on the WEB app too, where the org
    /// also appears in the organization menu.
    #[test]
    fn a_personal_org_is_never_kept_as_a_tenant_claim() {
        let mut checked = 0usize;
        for claim in personal_org_claims() {
            let action = resolve_personal_org_plane(&claim);
            assert_ne!(
                action,
                ClaimAction::KeepClaim,
                "a personal org reached the gate as a tenant claim: \
                 resolve_personal_org_plane({claim:?})"
            );
            checked += 1;
        }
        assert_eq!(
            checked, PERSONAL_ORG_CLAIM_COUNT,
            "the matrix did not cover every personal-org claim — a green result here \
             would mean nothing"
        );
    }

    /// **[SEC — HIGH] Stripping only ever happens for the caller's own
    /// personal org.**
    ///
    /// The two-sided form of the invariant above. It is not enough that
    /// ordinary orgs survive; nothing else may be stripped either, or a future
    /// variant added to the enum inherits the silent-wrong-tenant behaviour by
    /// default.
    #[test]
    fn only_the_callers_own_personal_org_is_served_as_one() {
        let mut stripped = 0usize;
        for claim in all_claims() {
            if resolve_personal_org_plane(&claim) == ClaimAction::ServeAsPersonalOrg {
                assert_eq!(
                    claim,
                    PersonalOrgClaim::PersonalOrgOfCaller,
                    "the org claim was stripped for {claim:?}, which is not the caller's \
                     own personal org — the caller is now on a plane they did not ask for"
                );
                stripped += 1;
            }
        }
        assert_eq!(
            stripped, 1,
            "exactly one input must strip the claim; {stripped} did — if this is 0 the \
             feature does nothing and every invariant above is vacuously true"
        );
    }

    /// **A personal org belonging to somebody else is always refused.**
    #[test]
    fn another_users_personal_org_is_always_refused() {
        let action = resolve_personal_org_plane(&PersonalOrgClaim::PersonalOrgOfAnotherUser);
        assert!(
            matches!(action, ClaimAction::Refuse(_)),
            "another user's personal org resolved to {action:?}, not a refusal"
        );
    }

    /// **The negative control.** Every invariant above is satisfied by a
    /// policy that refuses everything, and by a policy that keeps everything.
    /// This is what stops either from passing.
    #[test]
    fn the_policy_still_reaches_every_outcome() {
        let (mut kept, mut stripped, mut refused) = (0usize, 0usize, 0usize);
        for claim in all_claims() {
            match resolve_personal_org_plane(&claim) {
                ClaimAction::KeepClaim => kept += 1,
                ClaimAction::ServeAsPersonalOrg => stripped += 1,
                ClaimAction::Refuse(_) => refused += 1,
            }
        }
        assert!(kept > 0, "no input keeps its claim — every org is now unreachable");
        assert!(stripped > 0, "no input is served as a personal org — the feature does nothing");
        assert!(refused > 0, "no input is refused — the policy cannot say no");
        assert_eq!(
            kept + stripped + refused,
            CLAIM_COUNT,
            "the outcome tally did not account for every input"
        );
    }

    /// Today, before any personal org exists, only the first two variants are
    /// reachable in production — and both keep the claim, so this whole module
    /// is a no-op until `create_personal_orgs` writes its first row. Pins that,
    /// so a change to the no-personal-orgs-yet behaviour is deliberate.
    #[test]
    fn with_no_personal_orgs_yet_the_translation_is_a_no_op() {
        assert_eq!(
            resolve_personal_org_plane(&PersonalOrgClaim::NoOrgClaim),
            ClaimAction::KeepClaim
        );
        assert_eq!(
            resolve_personal_org_plane(&PersonalOrgClaim::OrdinaryOrg),
            ClaimAction::KeepClaim
        );
    }
}

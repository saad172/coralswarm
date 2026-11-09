//! The Phase 3 tenant-selection gate: a **pure decision core**
//! ([`resolve_selected_tenant`], written in PR 3.1 and left deliberately
//! unwired) plus the real I/O this PR (3.2) wires around it —
//! `docs/plans/claim-based-tenancy.md` §Phase 3.
//!
//! [`resolve_selected_tenant`] itself is UNCHANGED from PR 3.1: this PR does
//! not touch its logic or its exhaustive invariant tests, only supplies real
//! inputs and a real caller. The table it reads from is
//! `mcp_tenant_selection` (migration `0203_mcp_tenant_selection`, keyed
//! `(user_id, client_id)` per that migration's header). The two tools that
//! read/write it — `list_tenants`/`select_tenant` — live in
//! `mcp::tools::tenancy`; the one real call site that resolves a live
//! request's tenancy through [`resolve_selected_tenant`] is
//! [`resolve_request_tenancy`] below, called from
//! `mcp::auth::resolve_mcp_tenancy`'s personal/apex branch. See that
//! function's own doc for exactly where it sits in the request path.
//!
//! # Why this needs its own guard, separate from `tenancy::gate`
//!
//! `tenancy::gate`'s own doc trail already names the danger twice:
//!
//! * `a_bound_tenant_is_always_the_one_the_token_named`'s comment
//!   (`gate.rs:1064`) says PR 3.2's tool "deliberately
//!   resolve\[s\] a row from something *other than* the caller's claim" — the
//!   `claim_org == bound_org` check inside `decide` is not enough on its own
//!   once the org id can originate somewhere other than a claim.
//! * `a_personal_token_is_never_bound_to_a_tenant`'s comment goes further:
//!   after PR 3.1 drops `user:org:read`, *every* MCP token arrives
//!   personal-shaped, and PR 3.2 "then binds a tenant from a stored
//!   selection. That caller has no `org_id` claim and is nonetheless an org
//!   caller." It concludes that whatever names the tenant must still reach
//!   `decide` as [`crate::tenancy::gate::TokenKind::Org`] — which is true, and
//!   which is exactly what makes `a_bound_tenant_is_always_the_one_the_token_
//!   named` keep meaning something for a selected tenant.
//!
//! But `decide`'s claim-vs-row check is the SECOND of two checks a
//! claim-sourced org id gets today. The FIRST happens earlier and outside
//! `gate` entirely: `middleware::auth::resolve_clerk_context`, called with
//! [`crate::middleware::auth::OrgMembershipPolicy::RequireExisting`] for
//! `/mcp`, re-checks `live_org_membership` (`middleware/auth.rs:475`) for the
//! claim's own `org_id` on every single request, strictly BEFORE
//! `finish_binding`/`decide` ever run (`mcp::auth::resolve_mcp_tenancy`'s
//! ordering-invariant doc says so explicitly). A claim that named an org the
//! caller was removed from never reaches `decide` as `TokenKind::Org` at all
//! — it 403s one layer earlier.
//!
//! A **stored selection has no claim to trigger that check.** Nothing today
//! re-runs `live_org_membership` keyed on a selection row's org id, because
//! nothing today reads a selection row at all. If PR 3.2's tool handler reads
//! the stored org id and hands it straight to `finish_binding` as `org_claim`
//! — the mechanically obvious thing to do, since `finish_binding` already
//! takes `org_claim: Option<&str>` as a plain parameter, not something it
//! extracts from a token — it would skip the membership re-check that every
//! claim-sourced org id gets for free, and `decide`'s own claim-vs-row
//! equality check would still pass: the row IS active, and the "claim"
//! (really: the stale selection) DOES equal the bound org. Membership is
//! never asked. That is the "falls entirely outside today's enforcement" gap.
//!
//! [`resolve_selected_tenant`] is the guard for exactly that missing step. It
//! is not a replacement for `a_bound_tenant_is_always_the_one_the_token_
//! named` — it is the check that must run BEFORE a selection is ever allowed
//! to become a `TokenKind::Org` in the first place. [`resolve_request_tenancy`]
//! below is that real implementation: it computes [`LiveMembership`] by
//! reusing `middleware::auth::live_org_membership` directly (not a re-shaped
//! copy — see [`live_membership_for_selection`]), parameterized by the
//! SELECTED org id (not a claim), and `mcp::auth::resolve_mcp_tenancy`
//! refuses to call `finish_binding` at all unless this function answers
//! [`SelectionOutcome::Org`].
//!
//! # The consent screen is a second way to name a tenant (#825)
//!
//! Everything above was written when a stored selection was the ONLY thing
//! that could name a tenant over MCP, because #810 had dropped
//! `user:org:read` from [`super::MCP_SCOPES_PERSONAL`] and every MCP token
//! therefore arrived personal-shaped. **#825 put that scope back** — personal
//! organizations (control migration 0208) gave Clerk's consent screen a row
//! for "my own ocean", which is what made the picker usable again — so a
//! `/mcp` token can once more carry an `org_id` claim, and that claim is a
//! deliberate choice the user made on the consent screen.
//!
//! Between #810 and #825 landing, `mcp::auth::claims_for_resource` STRIPPED
//! that claim on arrival: the user picked an organization, the pick was
//! discarded, and the same question was asked a second time in-band. That
//! strip is gone, and [`ConsentOrg`] is the claim reaching this decision
//! instead — a second, independent way for an org id to arrive here, exactly
//! like the stored selection was for `gate::decide`.
//!
//! It gets the SAME two checks a stored selection gets, for the same reason
//! and in the same order — [`live_membership_for_the_org_in_play`] re-checks
//! `live_org_membership` for it on this very call before it can become a
//! [`SelectionOutcome::Org`], and `gate::decide`'s claim-vs-routing-row check
//! then runs on it downstream. That first check is deliberately REDUNDANT
//! with the one `middleware::auth::resolve_clerk_context` already ran for the
//! same claim under [`crate::middleware::auth::OrgMembershipPolicy::
//! RequireExisting`] (one extra indexed lookup on an org-claim request). It
//! is duplicated on purpose: this module's guarantee — "an [`SelectionOutcome::
//! Org`] answer implies a membership confirmed on THIS call" — is then true
//! of every org id this function can return, provable from this file alone,
//! rather than true of one input by local check and of the other only by a
//! distant caller's ordering.
//!
//! # Why there is no "wrong org" case here
//!
//! `tenancy::gate`'s matrices need `OTHER_ORG` — a second, non-matching org
//! fixture — because `TokenKind::Org` and `RoutingState::Active` are
//! independently-sourced strings that a bug could mismatch. [`LiveMembership`]
//! deliberately carries no org id of its own: it is defined as the answer to
//! "is the caller *currently* a member of THE org this decision is about"
//! (the same way `live_org_membership(pool, org_id, user_id)` takes the org
//! id it checks as a parameter, rather than returning some other org's
//! membership). A "selection names org A but membership confirms org B"
//! state is not merely disallowed — it has no representation to disallow.
//! That is a stronger property than a matching check, not a weaker one; see
//! `a_served_org_is_always_the_one_currently_selected_and_confirmed_live`.
//!
//! With [`ConsentOrg`] there are now two org ids in scope at once (a stored
//! one and a claimed one), so "THE org this decision is about" needs saying
//! out loud: [`live_membership_for_the_org_in_play`] computes the axis for
//! whichever of the two [`resolve_selected_or_consented_tenant`] will
//! actually consult, and `the_membership_axis_always_describes_the_org_the_
//! decision_returns` pins that the two never diverge.

use uuid::Uuid;

use crate::middleware::auth::NormalizedClaims;
use crate::tenancy::gate::TokenKind;

/// What the control-plane selection row says for one caller — one row of
/// `mcp_tenant_selection` (migration `0203_mcp_tenant_selection`), keyed
/// `(user_id, client_id)` per that migration's header.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum StoredSelection {
    /// No selection row exists — the default for every caller who has never
    /// called `select_tenant` (or who last cleared their selection). Before
    /// this PR wired a writer, this was the ONLY reachable state for every
    /// caller (see `today_the_only_reachable_state_resolves_to_personal`,
    /// which still pins that this specific input always resolves to
    /// `Personal` regardless of the membership axis — a true statement about
    /// this arm independent of whether an `Org` row exists for anyone else).
    None,
    /// A selection row names this Clerk `org_id`, and that org is a REAL
    /// organization from this caller's point of view — it is not in
    /// `personal_orgs` as standing for this caller's own ocean.
    Org(String),
    /// A selection row names an organization that `personal_orgs` (control
    /// migration 0208) says stands for **this caller's own** ocean.
    ///
    /// # Why this is a variant and not just an `Org` that later fails
    ///
    /// It is the stored-selection half of the lockout fixed here. A personal
    /// org deliberately has NO `tenant_databases` routing row, so resolving
    /// one as [`Self::Org`] hands `finish_binding` a `TokenKind::Org` whose
    /// routing row is `RoutingState::Missing`, and `gate::decide` answers
    /// 403 "organization is not provisioned for service". That 403 fires
    /// inside `mcp::auth::mcp_auth`, BEFORE tool dispatch — so the caller
    /// cannot reach `select_tenant` to undo the choice. Every subsequent
    /// request 403s identically. It is a permanent lockout per
    /// `(user_id, client_id)`, recoverable only by deleting the row by hand.
    ///
    /// Making it a distinct variant is what stops that from being a comment.
    /// [`resolve_selected_tenant`] has no wildcard arm on this axis, so this
    /// variant could not be added without the gate saying, in code, what it
    /// means — and what it means is [`SelectionOutcome::Personal`]: the
    /// caller's own home, which is the plane a personal org denotes anyway.
    ///
    /// **Another user's personal org is NOT this variant.** Being invited
    /// into somebody else's personal org is an ordinary org membership from
    /// this caller's point of view, with a real database behind it, so it
    /// stays [`Self::Org`]. That is why [`load_stored_selection`]'s
    /// classification joins `users` on the SELECTION ROW'S OWN `user_id`
    /// rather than matching the org id alone — the same `u.id = am.user_id`
    /// shape [`has_any_org_membership`] uses for the same reason.
    OwnPersonalOrg(String),
}

/// The result of re-checking, on THIS call, whether the caller is still a
/// member of the org a [`StoredSelection::Org`] names. Deliberately shaped
/// like the `Option<(Uuid, String)>` `live_org_membership` already returns
/// for a claim's org id (`Some` collapses to [`CurrentMember`], `None` to
/// [`NotAMember`] — the row/role payload is irrelevant to this decision, the
/// same way `decide` never looks at anything but the routing row's bound
/// org). [`StoredSelection`] is advisory data, never a capability, the same
/// way `user_active_graph`'s doc describes its own per-account pointer; a
/// [`LiveMembership`] re-check is what keeps consulting it safe.
///
/// [`CurrentMember`]: LiveMembership::CurrentMember
/// [`NotAMember`]: LiveMembership::NotAMember
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LiveMembership {
    /// `account_members`, joined through `accounts.clerk_org_id`, minus a
    /// wholesale `org_revocations` row, still has a live row for this
    /// (org, user) pair, checked right now.
    CurrentMember,
    /// No live row: removed, never joined, or the org is wholesale-revoked.
    NotAMember,
}

/// What Clerk's OAuth **consent screen** said about this connection, carried
/// by the VERIFIED token's own `org_id` claim (#825 — see this module's doc,
/// "The consent screen is a second way to name a tenant").
///
/// A second axis rather than a second [`StoredSelection`] variant on purpose:
/// the two are independently sourced (one is a control-plane row this server
/// wrote, the other a signed claim Clerk minted) and both can be present at
/// once, which a single enum cannot express. Precedence between them is a
/// decision [`resolve_selected_or_consented_tenant`] makes explicitly, in
/// code, over every pair.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ConsentOrg {
    /// The token names no organization at all, OR names one that
    /// `personal_orgs` says stands for this caller's OWN ocean — which
    /// denotes the consumer plane, not a tenant, and must never become a
    /// [`TokenKind::Org`].
    None,
    /// The token names a REAL organization: the user picked it from the
    /// consent screen's picker and Clerk signed that choice into the token.
    Org(String),
}

impl ConsentOrg {
    /// Derive from the caller's normalized claims — the only constructor
    /// production code uses.
    ///
    /// Takes [`NormalizedClaims`] rather than an `Option<&str>` org id (plus,
    /// inevitably, a "this one is real, honest" boolean beside it): the
    /// real-vs-personal classification is exactly what a bare org id cannot
    /// carry, and [`NormalizedClaims::token_kind`] is the ONE function in the
    /// codebase that consults `personal_orgs` to make it. Deriving the
    /// predicate here, from its subject, is what stops a call site from
    /// passing the wrong one — the same reasoning
    /// `TenantRuntime::finish_binding`'s doc gives for taking a
    /// [`TokenKind`] instead of a string.
    ///
    /// The `match` has no wildcard, so a new [`TokenKind`] variant fails to
    /// COMPILE here until this function says which side of the line it falls
    /// on.
    ///
    /// [`TenantRuntime::finish_binding`]: crate::tenancy::runtime::TenantRuntime::finish_binding
    pub(crate) fn from_claims(claims: &NormalizedClaims) -> Self {
        match claims.token_kind() {
            TokenKind::Org(org) => Self::Org(org),
            // A personal org IS the consumer plane, by design (migration
            // 0208: no `tenant_databases` row, ever). Treating one as a
            // consent-screen TENANT choice would hand `finish_binding` a
            // `TokenKind::Org` whose routing row is `RoutingState::Missing`
            // and 403 the one caller who is already exactly where they
            // belong — the same lockout shape
            // [`StoredSelection::OwnPersonalOrg`] exists to prevent on the
            // stored-selection side.
            TokenKind::PersonalOrg(_) | TokenKind::Personal => Self::None,
        }
    }
}

/// What the selection gate resolves to. Mirrors [`crate::tenancy::gate::
/// GateOutcome`]'s shape on purpose — [`Personal`]/[`Forbidden`] both mean
/// "no tenant" ([`None`] for `finish_binding`'s `org_claim` parameter);
/// [`Org`] means "bind this tenant" (`Some(id)`, which then goes through
/// `gate::decide`'s OWN claim-vs-routing-row check exactly as a claim-sourced
/// org id would — see this module's doc for why the two checks are layered,
/// not duplicated).
///
/// [`Personal`]: SelectionOutcome::Personal
/// [`Forbidden`]: SelectionOutcome::Forbidden
/// [`Org`]: SelectionOutcome::Org
/// [`None`]: Option::None
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum SelectionOutcome {
    /// Proceed on the consumer/personal plane.
    Personal,
    /// Proceed on the tenant plane for this `clerk_org_id` — but ONLY as
    /// input to `gate::decide`'s own re-check, never as a final answer on
    /// its own (see this module's doc).
    Org(String),
    /// Refuse, with a reason. The only outcome a stale/revoked selection may
    /// ever produce — never [`SelectionOutcome::Personal`] (that would
    /// silently substitute a plane nobody asked for) and never
    /// [`SelectionOutcome::Org`] (that is the leak this module exists to
    /// prevent).
    Forbidden(&'static str),
}

/// The selection gate. Pure: no I/O, total over its inputs, and — like
/// [`crate::tenancy::gate::decide`], whose shape this deliberately mirrors —
/// a `match` with no wildcard arm on either axis, so adding a third
/// [`StoredSelection`] or [`LiveMembership`] variant fails to COMPILE here
/// until this function says what it means for the new variant.
///
/// The one property this function exists to guarantee: **[`SelectionOutcome::
/// Org`] is returned if and only if `selection` names that exact org AND
/// `membership` is [`LiveMembership::CurrentMember`]** — asserted
/// exhaustively by
/// `a_served_org_is_always_the_one_currently_selected_and_confirmed_live`
/// below, over every `(selection, membership)` pair, not a hand-picked
/// subset.
pub(crate) fn resolve_selected_tenant(
    selection: &StoredSelection,
    membership: &LiveMembership,
) -> SelectionOutcome {
    match selection {
        StoredSelection::None => SelectionOutcome::Personal,
        // **Self-healing, deliberately — and this is an ASSUMPTION, not a
        // founder ruling.** A stale row naming the caller's own personal org
        // is treated as if it were absent, rather than refused.
        //
        // `Forbidden` would be the reflexively "safer" answer, and it is the
        // right answer for the `NotAMember` case below, because there a
        // refusal is loud and a user complains. Here it is not: this exact
        // state is what the lockout PUTS a user in, and the refusal fires
        // before tool dispatch, so `select_tenant` — the only in-band way to
        // clear the row — is unreachable. A user already stuck today would
        // stay stuck. Healing costs nothing in access terms: a personal org
        // denotes the caller's OWN home on the shared consumer plane, which
        // is exactly the plane `Personal` serves, so this grants no reach the
        // caller did not already have. It is not a privilege decision at all.
        //
        // The membership axis is ignored on purpose. There is no tenant
        // plane to be a member OF: a personal org has no routing row by
        // design (migration 0208's header), so "are they still a member"
        // has no bearing on serving them their own ocean. Pinned over the
        // FULL membership axis by
        // `a_personal_org_selection_heals_to_personal_whatever_membership_says`.
        StoredSelection::OwnPersonalOrg(_) => SelectionOutcome::Personal,
        StoredSelection::Org(org_id) => match membership {
            LiveMembership::CurrentMember => SelectionOutcome::Org(org_id.clone()),
            LiveMembership::NotAMember => SelectionOutcome::Forbidden(
                "selected organization no longer includes this member",
            ),
        },
    }
}

/// The FULL personal-resource gate (#825): [`resolve_selected_tenant`]'s
/// stored-selection decision, with the consent-screen claim filling in the
/// one hole a stored selection leaves — and nothing else.
///
/// # Purely additive, by construction
///
/// The only `(selection, consent)` pair this function answers differently
/// from [`resolve_selected_tenant`] is
/// ([`StoredSelection::None`], [`ConsentOrg::Org`]): a caller with no
/// selection row at all whose token names a real organization. Every other
/// pair delegates, unchanged — spelled out one arm at a time rather than
/// wildcarded, so a new variant on EITHER axis fails to compile here until
/// this function says what it means for it.
///
/// So nobody already using the in-band chooser changes behaviour: a stored
/// [`StoredSelection::Org`] still wins (including its `Forbidden` answer for
/// a revoked membership), and a stored [`StoredSelection::OwnPersonalOrg`]
/// still heals to [`SelectionOutcome::Personal`] rather than being overridden
/// by a claim — a row is an explicit, in-band choice, and the consent screen
/// does not get to overrule one.
///
/// # The membership axis, when two org ids are in play
///
/// `membership` describes THE org this function will consult — the stored
/// one in every delegating arm, the consented one in the new arm — never
/// "some org". [`live_membership_for_the_org_in_play`] is the only
/// production producer of that pairing, and
/// `the_membership_axis_always_describes_the_org_the_decision_returns` pins
/// it. Same property [`resolve_selected_tenant`] already had, restated for a
/// second possible subject.
///
/// # Why the new arm re-checks membership at all
///
/// The consent claim already passed `live_org_membership` upstream: `/mcp`
/// resolves every token under [`crate::middleware::auth::OrgMembershipPolicy::
/// RequireExisting`], which 403s a claim naming an org the caller is not a
/// live member of BEFORE `mcp::auth::resolve_mcp_tenancy` runs at all. This
/// arm checks again anyway, at the cost of one indexed lookup, so that the
/// invariant this module exists to guarantee is provable HERE: every
/// [`SelectionOutcome::Org`] this function returns names an org whose
/// membership was confirmed on THIS call, whichever axis named it. See this
/// module's doc for the longer version.
pub(crate) fn resolve_selected_or_consented_tenant(
    selection: &StoredSelection,
    membership: &LiveMembership,
    consent: &ConsentOrg,
) -> SelectionOutcome {
    match (selection, consent) {
        // ── The one new answer #825 needs ────────────────────────────────
        // No stored row, and the token names a real organization the user
        // picked on the consent screen. Before this arm existed the claim was
        // stripped at the door and this caller resolved to `Personal` — then
        // got a `tools/list` narrowed to `list_tenants`/`select_tenant` and
        // was asked to pick the very organization they had just picked.
        (StoredSelection::None, ConsentOrg::Org(org_id)) => match membership {
            LiveMembership::CurrentMember => SelectionOutcome::Org(org_id.clone()),
            // Refuse, never silently fall back to `Personal`: the caller
            // asked for an organization and must be told they cannot have
            // it, not handed a different plane with a 200 on it. (In
            // practice `RequireExisting` has already 403'd this request one
            // layer earlier — see this function's doc — so this arm is the
            // belt to that braces.)
            LiveMembership::NotAMember => SelectionOutcome::Forbidden(
                "this connection's organization no longer includes this member",
            ),
        },
        // ── Everything else: the stored-selection gate, untouched ────────
        (StoredSelection::None, ConsentOrg::None)
        | (StoredSelection::Org(_), ConsentOrg::None)
        | (StoredSelection::Org(_), ConsentOrg::Org(_))
        | (StoredSelection::OwnPersonalOrg(_), ConsentOrg::None)
        | (StoredSelection::OwnPersonalOrg(_), ConsentOrg::Org(_)) => {
            resolve_selected_tenant(selection, membership)
        }
    }
}

// ── PR 3.2: real I/O around the pure gate above ───────────────────────────────
//
// Everything below this line is new in PR 3.2. [`resolve_selected_tenant`]
// itself, above, is untouched — these functions only supply it real inputs
// (a real DB-backed [`StoredSelection`] and a real DB-backed
// [`LiveMembership`]) and give it a real caller.

/// Load the stored selection for one `(user_id, client_id)` pair from
/// `mcp_tenant_selection` (migration `0203_mcp_tenant_selection`), CLASSIFIED
/// against `personal_orgs`. No row is [`StoredSelection::None`] — the table's
/// only representation of "no selection" (see that variant's doc comment).
///
/// # Why the classification happens HERE, on the read
///
/// It would be tempting to classify only on the write (`select_tenant`), and
/// this PR does that too — but only as defence in depth, never as the load-
/// bearing check. Rows already written cannot be un-written by a validator
/// added later: the rows that cause the lockout this fixes are, by
/// definition, already in the table. A read-side classification is the only
/// one that reaches them, and it is what makes
/// [`StoredSelection::OwnPersonalOrg`] self-healing on the caller's very next
/// request instead of requiring an operator to `DELETE` the row.
///
/// One round trip, same as before: the `personal_orgs` classification rides
/// along as a correlated `EXISTS` on the same statement rather than costing a
/// second query. The join is on the SELECTION ROW'S OWN `user_id`
/// (`u.id = s.user_id`), not on the org id alone — the load-bearing half.
/// Another user's personal org is an ordinary organization from this caller's
/// point of view, with a real tenant database behind it, and must stay
/// [`StoredSelection::Org`]. Same shape, same reason, as
/// [`has_any_org_membership`]'s `u.id = am.user_id`.
async fn load_stored_selection(
    pool: &sqlx::PgPool,
    user_id: Uuid,
    client_id: &str,
) -> Result<StoredSelection, sqlx::Error> {
    let row: Option<(String, bool)> = sqlx::query_as(
        "SELECT s.clerk_org_id, \
                EXISTS ( \
                    SELECT 1 FROM personal_orgs p \
                    JOIN users u ON u.clerk_user_id = p.clerk_user_id \
                    WHERE p.clerk_org_id = s.clerk_org_id AND u.id = s.user_id \
                ) \
         FROM mcp_tenant_selection s \
         WHERE s.user_id = $1 AND s.client_id = $2",
    )
    .bind(user_id)
    .bind(client_id)
    .fetch_optional(pool)
    .await?;
    Ok(match row {
        Some((org_id, true)) => StoredSelection::OwnPersonalOrg(org_id),
        Some((org_id, false)) => StoredSelection::Org(org_id),
        None => StoredSelection::None,
    })
}

/// Whether `org_id` is the organization `personal_orgs` says stands for
/// **this** caller's own ocean.
///
/// The write-side mirror of [`load_stored_selection`]'s classification, for
/// `tools::tenancy::select_tenant`'s explicit refusal. Same predicate, same
/// `u.id = <this caller>` join, stated once per side because the two run at
/// different times against different inputs (a candidate org id here, a
/// persisted row there).
///
/// Takes the caller's internal `users.id` rather than a Clerk `sub`, which is
/// the only difference from `middleware::auth::classify_personal_org_claim` —
/// that function runs at the door, BEFORE a `users` row has been resolved, so
/// it compares against the token's `sub` instead. Both read the same table
/// and answer the same question.
pub(crate) async fn is_own_personal_org(
    pool: &sqlx::PgPool,
    org_id: &str,
    user_id: Uuid,
) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar(
        "SELECT EXISTS ( \
             SELECT 1 FROM personal_orgs p \
             JOIN users u ON u.clerk_user_id = p.clerk_user_id \
             WHERE p.clerk_org_id = $1 AND u.id = $2 \
         )",
    )
    .bind(org_id)
    .bind(user_id)
    .fetch_one(pool)
    .await
}

/// Fresh live-membership re-check for a stored selection's org id.
///
/// **Reuses `middleware::auth::live_org_membership` directly** — the exact
/// query a claim-sourced org id is re-checked against on every request
/// (`middleware::auth::resolve_clerk_context`'s `RequireExisting`/
/// `ProvisionFromClaims` policies, and `mcp::auth::mcp_auth` step 4) —
/// rather than a hand-maintained copy of its SQL. This module's own doc
/// ("Why this needs its own guard") is explicit that a stored selection
/// needs the SAME check a claim gets, not an equivalent-but-separately-
/// maintained one that could quietly drift (e.g. forgetting the
/// `org_revocations` wholesale-revocation guard in one copy but not the
/// other).
pub(crate) async fn live_membership_for_selection(
    pool: &sqlx::PgPool,
    org_id: &str,
    user_id: Uuid,
) -> Result<LiveMembership, sqlx::Error> {
    let row = crate::middleware::auth::live_org_membership(pool, org_id, user_id).await?;
    Ok(match row {
        Some(_) => LiveMembership::CurrentMember,
        None => LiveMembership::NotAMember,
    })
}

/// Persist a selection (upsert on the `(user_id, client_id)` primary key —
/// see migration `0203_mcp_tenant_selection`'s header for why a second
/// `select_tenant` call from the same client REPLACES the first rather than
/// creating a second row).
///
/// **Callers MUST have already confirmed [`LiveMembership::CurrentMember`]
/// for `org_id` before calling this** — this function itself performs no
/// membership check and will happily persist a selection for an org the
/// caller has never belonged to. `mcp::tools::tenancy::select_tenant` is the
/// only caller, and it re-checks membership via
/// [`live_membership_for_selection`] immediately before this call — never
/// blind.
/// Whether `user_id` currently has ANY live, non-revoked organization
/// membership at all — independent of which org, if any, is currently
/// SELECTED. Same `account_members` JOIN `accounts` shape (plus the
/// `org_revocations` wholesale-revocation guard) `tools::tenancy::
/// list_tenants` already queries for the same user, existence-checked rather
/// than listed.
///
/// The one real call site is `server::CoralSwarmMcp`'s Phase 3 PR 3.3
/// catalogue gate (decision 5): a caller with ≥1 org membership and no
/// active selection has something to choose between and must be steered to
/// `list_tenants`/`select_tenant` first; a caller with none has nothing to
/// choose and sees the ordinary catalogue, exactly as before this PR.
/// A caller's OWN personal organization does not count.
///
/// This answers "is there a real organization for this caller to CHOOSE
/// BETWEEN", which is what the tool-catalogue gate is asking. A personal org
/// is not a choice — it is the caller's own home, and after
/// `adopt_personal_org_account` runs it is the ONLY account they have.
/// Counting it would demand `select_tenant` from someone with nothing to
/// select, and since MCP tokens carry no `org_id` while `user:org:read` is
/// unadvertised, that lock-out is permanent rather than a nudge.
///
/// Another user's personal org still counts here — being invited into one is
/// an ordinary org membership from this caller's point of view — so the
/// exclusion is matched on `am.user_id`, never on the org id alone.
pub(crate) async fn has_any_org_membership(
    pool: &sqlx::PgPool,
    user_id: Uuid,
) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar(
        "SELECT EXISTS (\
             SELECT 1 FROM account_members am \
             JOIN accounts a ON a.id = am.account_id \
             WHERE am.user_id = $1 \
               AND a.clerk_org_id IS NOT NULL \
               AND NOT EXISTS (SELECT 1 FROM org_revocations r WHERE r.clerk_org_id = a.clerk_org_id) \
               AND COALESCE(org_deletion_state_for_org(a.clerk_org_id), '') \
                   NOT IN ('requested', 'grace', 'awaiting_approval', 'destroying') \
               AND NOT EXISTS ( \
                   SELECT 1 FROM personal_orgs p \
                   JOIN users u ON u.clerk_user_id = p.clerk_user_id \
                   WHERE p.clerk_org_id = a.clerk_org_id AND u.id = am.user_id \
               ) \
         )",
    )
    .bind(user_id)
    .fetch_one(pool)
    .await
}

pub(crate) async fn store_selection(
    pool: &sqlx::PgPool,
    user_id: Uuid,
    client_id: &str,
    org_id: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO mcp_tenant_selection (user_id, client_id, clerk_org_id, updated_at) \
         VALUES ($1, $2, $3, now()) \
         ON CONFLICT (user_id, client_id) \
         DO UPDATE SET clerk_org_id = EXCLUDED.clerk_org_id, updated_at = now()",
    )
    .bind(user_id)
    .bind(client_id)
    .bind(org_id)
    .execute(pool)
    .await?;
    Ok(())
}

/// Clear a stored selection — reverts `(user_id, client_id)` to
/// [`StoredSelection::None`] (personal), by deleting the row rather than
/// writing a sentinel value (see that variant's doc comment on why "no row"
/// IS the representation).
pub(crate) async fn clear_selection(
    pool: &sqlx::PgPool,
    user_id: Uuid,
    client_id: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query("DELETE FROM mcp_tenant_selection WHERE user_id = $1 AND client_id = $2")
        .bind(user_id)
        .bind(client_id)
        .execute(pool)
        .await?;
    Ok(())
}

/// Which org id [`resolve_selected_or_consented_tenant`] will actually
/// consult for this input pair — i.e. the SUBJECT of the [`LiveMembership`]
/// axis, and the only org id that pair can possibly resolve to. `None` when
/// nothing that could become a tenant is named at all.
///
/// Pure, and separate from the query below, so the pairing this module's doc
/// calls "THE org this decision is about" is a proposition a test can check
/// against the gate's own answers without a database —
/// `the_membership_axis_always_describes_the_org_the_decision_returns`. A
/// mismatch between this and the gate is then a red test, not a subtlety
/// buried in a `match` that also does I/O.
fn org_in_play<'a>(selection: &'a StoredSelection, consent: &'a ConsentOrg) -> Option<&'a str> {
    match (selection, consent) {
        // A stored row is what the decision turns on, whatever the claim
        // says — so the stored org is the subject.
        (StoredSelection::Org(org_id), ConsentOrg::None)
        | (StoredSelection::Org(org_id), ConsentOrg::Org(_)) => Some(org_id.as_str()),
        // No stored row + a real consent-screen organization: the claim is
        // what may become a `TokenKind::Org`, so the claim's org is the
        // subject.
        (StoredSelection::None, ConsentOrg::Org(org_id)) => Some(org_id.as_str()),
        // Nothing nameable: the gate ignores the membership axis entirely
        // here (see `today_the_only_reachable_state_resolves_to_personal`).
        (StoredSelection::None, ConsentOrg::None) => None,
        // A personal org has no tenant plane to be a member of, and
        // `live_org_membership` would answer `NotAMember` for one anyway (it
        // matches `accounts.clerk_org_id`, and after
        // `adopt_personal_org_account` an `accounts` row DOES exist — so the
        // answer it gives is incidental, not meaningful). Skipping the query
        // is both cheaper and honest: `resolve_selected_tenant`'s
        // `OwnPersonalOrg` arm ignores this axis entirely, pinned over the
        // whole axis by
        // `a_personal_org_selection_heals_to_personal_whatever_membership_says`.
        (StoredSelection::OwnPersonalOrg(_), ConsentOrg::None)
        | (StoredSelection::OwnPersonalOrg(_), ConsentOrg::Org(_)) => None,
    }
}

/// The membership axis for [`org_in_play`]'s org, freshly re-checked on this
/// call. `NotAMember` — a value the gate ignores in exactly the cases that
/// produce it — when no org is in play, which costs nothing to state
/// explicitly and avoids inventing a third, meaningless "N/A" variant.
///
/// For a consent-sourced org this query is deliberately redundant with the
/// one `resolve_clerk_context` already ran for the same claim under
/// `RequireExisting` — see [`resolve_selected_or_consented_tenant`]'s doc for
/// why the duplicate is worth one indexed lookup.
async fn live_membership_for_the_org_in_play(
    pool: &sqlx::PgPool,
    user_id: Uuid,
    selection: &StoredSelection,
    consent: &ConsentOrg,
) -> Result<LiveMembership, sqlx::Error> {
    match org_in_play(selection, consent) {
        Some(org_id) => live_membership_for_selection(pool, org_id, user_id).await,
        None => Ok(LiveMembership::NotAMember),
    }
}

/// **The one real call site that resolves a live request's tenancy through
/// [`resolve_selected_or_consented_tenant`].** Called from
/// `mcp::auth::resolve_mcp_tenancy`'s personal/apex branch, on every request
/// to that resource (never cached — same revocation-on-next-call posture as
/// `live_org_membership` itself): loads the stored selection, classifies the
/// token's own consent-screen `org_id` claim ([`ConsentOrg::from_claims`]),
/// re-checks live membership for whichever of the two the gate will consult,
/// and hands all three to the pure gate unmodified.
///
/// This function — not the pure gates directly — is what a caller outside
/// this module should use to turn `(user_id, client_id, claims)` into a
/// [`SelectionOutcome`] against CURRENT database state. Calling
/// [`resolve_selected_tenant`] with hand-rolled inputs instead of going
/// through here is exactly the "reimplements the resolution path" mistake
/// this module's guard exists to catch: the invariant it protects only means
/// something if this IS the path real requests take.
///
/// `client_id` is `None` for a token carrying neither a `client_id` nor an
/// `azp` claim. There is then no stable per-client identifier to key a
/// selection row by, so no row can exist and none can ever be written
/// (`tools::tenancy::select_tenant` refuses such a caller outright) — the
/// query is skipped and [`StoredSelection::None`], which is the answer it
/// would have returned, is used directly. Such a caller's consent-screen
/// claim is still honored: it is the only door they have.
pub(crate) async fn resolve_request_tenancy(
    pool: &sqlx::PgPool,
    user_id: Uuid,
    client_id: Option<&str>,
    claims: &NormalizedClaims,
) -> Result<SelectionOutcome, sqlx::Error> {
    let selection = match client_id {
        Some(client_id) => load_stored_selection(pool, user_id, client_id).await?,
        None => StoredSelection::None,
    };
    let consent = ConsentOrg::from_claims(claims);
    let membership =
        live_membership_for_the_org_in_play(pool, user_id, &selection, &consent).await?;
    Ok(resolve_selected_or_consented_tenant(&selection, &membership, &consent))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The org a [`StoredSelection::Org`] fixture names in every matrix
    /// below. There is only ever one selected-org fixture (unlike
    /// `tenancy::gate`'s `BOUND_ORG`/`OTHER_ORG` pair) — see this module's
    /// doc, "Why there is no wrong-org case here".
    const SELECTED_ORG: &str = "org_selected";

    /// [`StoredSelection`] variants covered — see the note on
    /// `tenancy::gate`'s own `HOST_COUNT` etc. for why this is a literal and
    /// not `all_selections().len()`: a matrix that checks its own coverage
    /// against a list it draws from is checking a list against itself.
    const SELECTION_COUNT: usize = 3;
    /// [`LiveMembership`] variants covered.
    const MEMBERSHIP_COUNT: usize = 2;

    /// The org a [`StoredSelection::OwnPersonalOrg`] fixture names. Distinct
    /// from [`SELECTED_ORG`] so an assertion cannot pass by conflating the
    /// two arms.
    const OWN_PERSONAL_ORG: &str = "org_personal_of_caller";

    /// Every [`StoredSelection`]. The trailing `match` with no wildcard is an
    /// exhaustiveness anchor exactly like `tenancy::gate::tests::all_hosts`'s:
    /// it proves every variant is HANDLED (fails to compile otherwise), which
    /// is a different property from proving one is PRESENT — see
    /// `the_input_domain_is_the_size_it_claims_to_be` for the second half.
    fn all_selections() -> Vec<StoredSelection> {
        let all = vec![
            StoredSelection::None,
            StoredSelection::Org(SELECTED_ORG.into()),
            StoredSelection::OwnPersonalOrg(OWN_PERSONAL_ORG.into()),
        ];
        for s in &all {
            match s {
                StoredSelection::None
                | StoredSelection::Org(_)
                | StoredSelection::OwnPersonalOrg(_) => {}
            }
        }
        all
    }

    /// Every [`LiveMembership`].
    fn all_memberships() -> Vec<LiveMembership> {
        let all = vec![LiveMembership::CurrentMember, LiveMembership::NotAMember];
        for m in &all {
            match m {
                LiveMembership::CurrentMember | LiveMembership::NotAMember => {}
            }
        }
        all
    }

    /// Where the enumerations and the literal sizes meet — the independent
    /// second opinion `tenancy::gate::tests::the_input_domain_is_the_size_
    /// it_claims_to_be` documents at length. Length alone is not coverage
    /// (`vec![None, None]` keeps `SELECTION_COUNT` at 2 while silently
    /// deleting the `Org` case from every matrix below), so this also names
    /// what must actually be PRESENT.
    #[test]
    fn the_input_domain_is_the_size_it_claims_to_be() {
        let selections = all_selections();
        let memberships = all_memberships();

        assert_eq!(selections.len(), SELECTION_COUNT, "selection axis");
        assert_eq!(memberships.len(), MEMBERSHIP_COUNT, "membership axis");

        assert!(
            selections.iter().any(|s| matches!(s, StoredSelection::None)),
            "selection axis lost None — the only state reachable today"
        );
        assert!(
            selections
                .iter()
                .any(|s| matches!(s, StoredSelection::Org(o) if o == SELECTED_ORG)),
            "selection axis lost Org — the arm PR 3.2 introduces"
        );
        assert!(
            selections
                .iter()
                .any(|s| matches!(s, StoredSelection::OwnPersonalOrg(o) if o == OWN_PERSONAL_ORG)),
            "selection axis lost OwnPersonalOrg — the arm whose ABSENCE was the lockout: \
             with only None|Org on this axis, a stored selection naming the caller's own \
             personal org resolved as a real tenant, found no routing row, and 403'd every \
             request before tool dispatch could reach select_tenant to undo it"
        );
        assert!(
            memberships.iter().any(|m| matches!(m, LiveMembership::CurrentMember)),
            "membership axis lost CurrentMember"
        );
        assert!(
            memberships.iter().any(|m| matches!(m, LiveMembership::NotAMember)),
            "membership axis lost NotAMember"
        );
    }

    /// **The load-bearing invariant.** No `(selection, membership)` state
    /// ever serves an org other than the one CURRENTLY selected AND
    /// confirmed live — over the full cross product, not named cases.
    ///
    /// This is the "own equivalent guard" `gate.rs`'s comment on
    /// `select_tenant` asks for: it fails the moment either half of the
    /// `Org` arm's condition is dropped, independent of `tenancy::gate`
    /// ever being touched.
    #[test]
    fn a_served_org_is_always_the_one_currently_selected_and_confirmed_live() {
        let mut served = 0usize;
        for selection in all_selections() {
            for membership in all_memberships() {
                let outcome = resolve_selected_tenant(&selection, &membership);
                if let SelectionOutcome::Org(bound) = &outcome {
                    assert_eq!(
                        selection,
                        StoredSelection::Org(bound.clone()),
                        "resolve_selected_tenant({selection:?}, {membership:?}) = {outcome:?} — \
                         served an org the stored selection never named"
                    );
                    assert_eq!(
                        membership,
                        LiveMembership::CurrentMember,
                        "resolve_selected_tenant({selection:?}, {membership:?}) = {outcome:?} — \
                         served an org without a live membership re-check confirming it"
                    );
                    served += 1;
                }
            }
        }
        assert!(
            served > 0,
            "no input served a tenant at all, so this asserted nothing — a gate that \
             refuses everything satisfies every invariant in this file"
        );
    }

    /// The other half, stated without reference to `Org`'s payload at all
    /// (mirrors `tenancy::gate::tests::an_org_token_is_servable_only_on_an_
    /// active_routing_row`): for every membership state that is NOT
    /// [`LiveMembership::CurrentMember`], no selection — including a
    /// selection that DOES name a real org — may ever resolve to
    /// [`SelectionOutcome::Org`].
    #[test]
    fn an_org_is_served_only_when_live_membership_currently_confirms_it() {
        let mut checked = 0usize;
        for selection in all_selections() {
            for membership in all_memberships() {
                if matches!(membership, LiveMembership::CurrentMember) {
                    continue;
                }
                let outcome = resolve_selected_tenant(&selection, &membership);
                assert!(
                    !matches!(outcome, SelectionOutcome::Org(_)),
                    "resolve_selected_tenant({selection:?}, {membership:?}) = {outcome:?} — \
                     served an org with no live membership confirming it"
                );
                checked += 1;
            }
        }
        assert_eq!(
            checked,
            SELECTION_COUNT * (MEMBERSHIP_COUNT - 1),
            "the matrix did not cover every non-CurrentMember membership state"
        );
    }

    /// The mirror of the leak, stated the way `tenancy::gate::tests::a_
    /// personal_token_is_never_bound_to_a_tenant` states it: [`StoredSelection
    /// ::None`] means *no tenant was selected*. If it can come back
    /// [`SelectionOutcome::Org`], the gate has invented a tenant nobody
    /// selected — regardless of what the (irrelevant) membership axis says.
    #[test]
    fn a_selection_of_none_is_never_bound_to_a_tenant() {
        let mut checked = 0usize;
        for membership in all_memberships() {
            let outcome = resolve_selected_tenant(&StoredSelection::None, &membership);
            assert!(
                !matches!(outcome, SelectionOutcome::Org(_)),
                "resolve_selected_tenant(None, {membership:?}) = {outcome:?} — a caller with \
                 no stored selection was bound to a tenant"
            );
            checked += 1;
        }
        assert_eq!(checked, MEMBERSHIP_COUNT, "the matrix did not cover every membership state");
    }

    /// **The lockout, stated as an invariant.** A selection naming the
    /// caller's OWN personal org may never become a bound tenant.
    ///
    /// This is the half that says "no leak". A personal org has no routing
    /// row by design, so binding one as a tenant cannot succeed — it can only
    /// reach `gate::decide` and come back 403 "organization is not
    /// provisioned for service", from inside `mcp_auth`, before tool
    /// dispatch. Hence the lockout: the 403 lands earlier than
    /// `select_tenant`, so the caller cannot undo their own selection.
    #[test]
    fn a_personal_org_selection_is_never_bound_to_a_tenant() {
        let mut checked = 0usize;
        for membership in all_memberships() {
            let selection = StoredSelection::OwnPersonalOrg(OWN_PERSONAL_ORG.into());
            let outcome = resolve_selected_tenant(&selection, &membership);
            assert!(
                !matches!(outcome, SelectionOutcome::Org(_)),
                "resolve_selected_tenant({selection:?}, {membership:?}) = {outcome:?} — the \
                 caller's own personal org was bound as a tenant. It has no routing row by \
                 design (migration 0208), so this 403s at gate::decide on every subsequent \
                 request, before tool dispatch, and select_tenant becomes unreachable."
            );
            checked += 1;
        }
        assert_eq!(checked, MEMBERSHIP_COUNT, "the matrix did not cover every membership state");
    }

    /// **The self-healing half, and the reason the test above is not enough
    /// on its own.** `Forbidden` would satisfy "never bound to a tenant"
    /// perfectly while leaving every already-stuck user exactly as stuck.
    /// This asserts the DECISION — `Personal`, the caller's own home — not
    /// merely the absence of the bad one.
    ///
    /// Membership is swept deliberately: a personal org has no tenant plane
    /// to be a member of, so no value on that axis may change this answer.
    #[test]
    fn a_personal_org_selection_heals_to_personal_whatever_membership_says() {
        for membership in all_memberships() {
            let selection = StoredSelection::OwnPersonalOrg(OWN_PERSONAL_ORG.into());
            assert_eq!(
                resolve_selected_tenant(&selection, &membership),
                SelectionOutcome::Personal,
                "a stale selection naming the caller's own personal org must resolve to their \
                 own home, not be refused: the refusal fires before tool dispatch, so a \
                 refused caller has no in-band way to clear the row. membership = {membership:?}"
            );
        }
    }

    /// A gate that refuses everything satisfies every "never serves X"
    /// invariant above (mirrors `tenancy::gate::tests::the_gate_still_
    /// serves_both_planes`). Pin that both outcomes are still reachable, so
    /// a total-refusal regression — e.g. collapsing the whole function to
    /// `Personal` — is loud instead of passing every invariant in this file
    /// for free.
    #[test]
    fn the_gate_still_serves_both_personal_and_org() {
        let (mut personal, mut org) = (0usize, 0usize);
        for selection in all_selections() {
            for membership in all_memberships() {
                match resolve_selected_tenant(&selection, &membership) {
                    SelectionOutcome::Personal => personal += 1,
                    SelectionOutcome::Org(_) => org += 1,
                    SelectionOutcome::Forbidden(_) => {}
                }
            }
        }
        assert!(personal > 0, "no input reaches the personal plane");
        assert!(org > 0, "no input binds a tenant");
    }

    /// **No-selection is always personal, independent of the membership
    /// axis.** Before this PR wired a writer, [`StoredSelection::None`] was
    /// the ONLY state any real caller could be in — this test predates
    /// `select_tenant` and was true unconditionally. It stays true and stays
    /// meaningful now that `StoredSelection::Org` is reachable too: it pins
    /// that a caller with NO stored row and nothing else to go on lands on
    /// the personal plane — this test says nothing about the `Org` arm at
    /// all.
    ///
    /// It is a statement about [`resolve_selected_tenant`] ALONE, which is
    /// why #825's consent-screen axis does not contradict it: a caller with
    /// no stored row and a real `org_id` claim now resolves to
    /// [`SelectionOutcome::Org`] through
    /// [`resolve_selected_or_consented_tenant`], and the pair of statements
    /// is exactly the "purely additive" property that function claims —
    /// pinned by `a_consent_org_only_ever_fills_the_no_selection_hole`.
    #[test]
    fn today_the_only_reachable_state_resolves_to_personal() {
        for membership in all_memberships() {
            assert_eq!(
                resolve_selected_tenant(&StoredSelection::None, &membership),
                SelectionOutcome::Personal,
                "membership = {membership:?}"
            );
        }
    }

    // ── #825: the consent-screen axis ────────────────────────────────────────

    /// The org a [`ConsentOrg::Org`] fixture names. Deliberately DIFFERENT
    /// from both [`SELECTED_ORG`] and [`OWN_PERSONAL_ORG`], so no assertion
    /// below can pass by conflating the axis that named an org with the org
    /// itself — the precedence tests turn entirely on telling the two apart.
    const CONSENTED_ORG: &str = "org_consented";

    /// [`ConsentOrg`] variants covered — a literal, for the same reason
    /// [`SELECTION_COUNT`] is one.
    const CONSENT_COUNT: usize = 2;

    /// Every [`ConsentOrg`]. Same exhaustiveness-anchor shape as
    /// [`all_selections`]: the trailing wildcard-free `match` fails to
    /// compile if a variant is added and not handled.
    fn all_consents() -> Vec<ConsentOrg> {
        let all = vec![ConsentOrg::None, ConsentOrg::Org(CONSENTED_ORG.into())];
        for c in &all {
            match c {
                ConsentOrg::None | ConsentOrg::Org(_) => {}
            }
        }
        all
    }

    #[test]
    fn the_consent_axis_is_the_size_it_claims_to_be() {
        let consents = all_consents();
        assert_eq!(consents.len(), CONSENT_COUNT, "consent axis");
        assert!(
            consents.iter().any(|c| matches!(c, ConsentOrg::None)),
            "consent axis lost None — every token minted between #810 and #825"
        );
        assert!(
            consents
                .iter()
                .any(|c| matches!(c, ConsentOrg::Org(o) if o == CONSENTED_ORG)),
            "consent axis lost Org — the arm #825's consent-screen picker produces, \
             and the one this whole axis exists for"
        );
    }

    /// **The load-bearing invariant, extended to the consent axis.** Over the
    /// FULL `(selection, membership, consent)` cross product, no state ever
    /// serves an org other than the one the decision's own subject
    /// ([`org_in_play`]) names AND live membership currently confirms.
    ///
    /// The `membership` assertion is the security half: it is what makes a
    /// consent-screen claim exactly as proven as a stored selection, rather
    /// than trusted for being signed.
    #[test]
    fn a_served_org_is_always_the_one_in_play_and_confirmed_live() {
        let mut served = 0usize;
        for selection in all_selections() {
            for membership in all_memberships() {
                for consent in all_consents() {
                    let outcome =
                        resolve_selected_or_consented_tenant(&selection, &membership, &consent);
                    if let SelectionOutcome::Org(bound) = &outcome {
                        assert_eq!(
                            org_in_play(&selection, &consent),
                            Some(bound.as_str()),
                            "resolve_selected_or_consented_tenant({selection:?}, \
                             {membership:?}, {consent:?}) = {outcome:?} — served an org \
                             neither the stored selection nor the consent screen put in play"
                        );
                        assert_eq!(
                            membership,
                            LiveMembership::CurrentMember,
                            "resolve_selected_or_consented_tenant({selection:?}, \
                             {membership:?}, {consent:?}) = {outcome:?} — served an org \
                             without a live membership re-check confirming it"
                        );
                        served += 1;
                    }
                }
            }
        }
        assert!(
            served > 0,
            "no input served a tenant at all, so this asserted nothing — a gate that \
             refuses everything satisfies every invariant in this file"
        );
    }

    /// The membership axis's SUBJECT never drifts from the answer: whatever
    /// [`org_in_play`] says is being checked is the only org the gate can
    /// return for that pair. Stated separately from the invariant above
    /// because it is the property that keeps "no wrong-org case" true now
    /// that two org ids can be in scope at once — a bug that checked the
    /// stored org's membership and returned the consented one would satisfy
    /// a naive "an Org outcome implies CurrentMember" check and fail here.
    #[test]
    fn the_membership_axis_always_describes_the_org_the_decision_returns() {
        for selection in all_selections() {
            for consent in all_consents() {
                let subject = org_in_play(&selection, &consent);
                for membership in all_memberships() {
                    let outcome =
                        resolve_selected_or_consented_tenant(&selection, &membership, &consent);
                    match &outcome {
                        SelectionOutcome::Org(bound) => assert_eq!(
                            subject,
                            Some(bound.as_str()),
                            "({selection:?}, {consent:?}) checks {subject:?} but served \
                             {bound} — the membership axis describes a different org than \
                             the one bound"
                        ),
                        // Nothing bound: the axis's subject is irrelevant, but
                        // a pair with NO subject must never bind at all.
                        _ => {}
                    }
                }
            }
        }
        // The pair with no subject at all can never bind, in any membership
        // state — otherwise "no org in play" would still be able to serve one.
        for membership in all_memberships() {
            let outcome = resolve_selected_or_consented_tenant(
                &StoredSelection::None,
                &membership,
                &ConsentOrg::None,
            );
            assert_eq!(
                outcome,
                SelectionOutcome::Personal,
                "no org in play must resolve to the personal plane, membership = {membership:?}"
            );
        }
    }

    /// **The fix itself: no stored row + a real consent-screen organization +
    /// live membership binds THAT organization.**
    ///
    /// Stated as its own positive assertion, and not left to the invariant
    /// tests above, because every one of those is conditioned on an `Org`
    /// outcome having already happened (`if let SelectionOutcome::Org(..)`)
    /// or on the OTHER arms' answers — all of them stay green if this arm
    /// quietly stops binding anything, which is precisely the pre-#825 bug.
    /// Confirmed the hard way: reverting the arm to delegate left all 15 of
    /// the other tests in this file passing, and this is the one that goes
    /// red.
    #[test]
    fn a_consent_org_binds_when_no_selection_exists_and_membership_is_live() {
        assert_eq!(
            resolve_selected_or_consented_tenant(
                &StoredSelection::None,
                &LiveMembership::CurrentMember,
                &ConsentOrg::Org(CONSENTED_ORG.into()),
            ),
            SelectionOutcome::Org(CONSENTED_ORG.into()),
            "the organization the user picked on the consent screen must be the one bound — \
             resolving to Personal here is the bug this arm exists to fix: the pick is \
             discarded, the caller is put back on the shared plane, and the tool catalogue \
             then narrows to list_tenants/select_tenant to ask them the same question again"
        );
    }

    /// **Purely additive.** For every input pair EXCEPT
    /// (no stored row, real consent org), the composed gate answers exactly
    /// what [`resolve_selected_tenant`] alone answered before #825 — so
    /// nobody already using the in-band chooser changes behaviour, and the
    /// consent claim fills a hole rather than taking over a decision.
    ///
    /// The new arm's own ANSWER is asserted above, not here: this test
    /// deliberately only COUNTS that arm, and a version of it that also tried
    /// to pin the answer would be doing two jobs and could pass for the wrong
    /// one.
    #[test]
    fn a_consent_org_only_ever_fills_the_no_selection_hole() {
        let mut filled = 0usize;
        let mut delegated = 0usize;
        for selection in all_selections() {
            for membership in all_memberships() {
                for consent in all_consents() {
                    let composed =
                        resolve_selected_or_consented_tenant(&selection, &membership, &consent);
                    let stored_only = resolve_selected_tenant(&selection, &membership);
                    let is_the_new_arm = matches!(selection, StoredSelection::None)
                        && matches!(consent, ConsentOrg::Org(_));
                    if is_the_new_arm {
                        filled += 1;
                    } else {
                        assert_eq!(
                            composed, stored_only,
                            "({selection:?}, {membership:?}, {consent:?}): the consent axis \
                             changed an answer it must not touch"
                        );
                        delegated += 1;
                    }
                }
            }
        }
        assert_eq!(
            filled,
            MEMBERSHIP_COUNT,
            "the new arm must be reachable in every membership state, and only there"
        );
        assert_eq!(
            delegated,
            SELECTION_COUNT * MEMBERSHIP_COUNT * CONSENT_COUNT - MEMBERSHIP_COUNT,
            "every other pair must delegate — a shrinking count means an arm stopped \
             being compared to the pre-#825 gate"
        );
    }

    /// A stored selection WINS over a consent-screen claim, always: the row
    /// is an explicit in-band choice (`select_tenant`), and the consent
    /// screen does not get to overrule one. Checked against a consent org
    /// that is DIFFERENT from every stored fixture, so "won" is observable
    /// rather than a coincidence of equal ids.
    #[test]
    fn a_stored_selection_always_wins_over_a_consent_org() {
        let consent = ConsentOrg::Org(CONSENTED_ORG.into());
        let mut checked = 0usize;
        for selection in all_selections() {
            if matches!(selection, StoredSelection::None) {
                continue;
            }
            for membership in all_memberships() {
                let outcome =
                    resolve_selected_or_consented_tenant(&selection, &membership, &consent);
                assert_ne!(
                    outcome,
                    SelectionOutcome::Org(CONSENTED_ORG.into()),
                    "({selection:?}, {membership:?}) let the consent screen override an \
                     explicit stored selection"
                );
                assert_eq!(
                    outcome,
                    resolve_selected_tenant(&selection, &membership),
                    "({selection:?}, {membership:?}) answered differently than the stored \
                     selection alone would have"
                );
                checked += 1;
            }
        }
        assert_eq!(
            checked,
            (SELECTION_COUNT - 1) * MEMBERSHIP_COUNT,
            "every stored-row state must be covered on both membership states"
        );
    }

    /// A consent org the caller is no longer a live member of is REFUSED —
    /// never silently downgraded to the personal plane. The same rule
    /// [`SelectionOutcome::Forbidden`]'s doc states for a stale selection:
    /// substituting a plane nobody asked for is the failure mode, not the
    /// safe fallback.
    #[test]
    fn a_consent_org_without_live_membership_is_refused_never_personal() {
        let outcome = resolve_selected_or_consented_tenant(
            &StoredSelection::None,
            &LiveMembership::NotAMember,
            &ConsentOrg::Org(CONSENTED_ORG.into()),
        );
        assert!(
            matches!(outcome, SelectionOutcome::Forbidden(_)),
            "a consent org with no live membership must be refused, got {outcome:?}"
        );
    }

    /// [`ConsentOrg::from_claims`] classifies from the ONE function that
    /// consults `personal_orgs`, so a personal-organization claim can never
    /// become a consent-screen TENANT choice — it denotes the caller's own
    /// ocean on the shared plane, which has no routing row by design.
    #[test]
    fn a_personal_org_claim_is_never_a_consent_tenant() {
        use crate::clerk::ClerkClaims;
        use crate::middleware::auth::NormalizedClaims;

        // Built by JSON round-trip: `ClerkClaims` only derives `Deserialize`
        // (it is never hand-built in production either) — same shape
        // `mcp::auth`'s own tests use.
        let with_org = |org: Option<&str>| -> ClerkClaims {
            serde_json::from_value(serde_json::json!({
                "sub": "user_consent",
                "exp": 4_102_444_800_usize,
                "client_id": "client_consent",
                "org_id": org,
            }))
            .unwrap()
        };

        assert_eq!(
            ConsentOrg::from_claims(&NormalizedClaims::for_test(with_org(None))),
            ConsentOrg::None,
            "a token with no org claim names no tenant"
        );
        assert_eq!(
            ConsentOrg::from_claims(&NormalizedClaims::for_test(with_org(Some(CONSENTED_ORG)))),
            ConsentOrg::Org(CONSENTED_ORG.into()),
            "a real organization claim IS the consent-screen choice #825 restored"
        );
        assert_eq!(
            ConsentOrg::from_claims(&NormalizedClaims::for_test_personal_org(with_org(Some(
                OWN_PERSONAL_ORG
            )))),
            ConsentOrg::None,
            "a personal-organization claim must never be treated as a tenant choice — it \
             has no routing row by design, so binding it would 403 the one caller who is \
             already exactly where they belong"
        );
    }
}

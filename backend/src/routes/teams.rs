//! Team endpoints — account-level membership and named team key management.
//!
//! # Account-level membership (CS-057)
//! - `GET    /v1/teams`                           — list accounts the caller belongs to
//! - `GET    /v1/teams/members`                   — list members of the caller's account
//! - `POST   /v1/account/members`                 — invite a user by email (owners only);
//!   `POST /v1/teams/invite` is kept as a deprecated compatibility alias (org-team-membership
//!   design doc decision #15 — these two routes write `account_members`, not `teams`, so the
//!   old `/teams/...` names were always a misnomer).
//! - `DELETE /v1/account/members/{user_id}`       — remove a member (owners only);
//!   `DELETE /v1/teams/members/{user_id}` is the equivalent deprecated alias.
//!
//! **Org accounts are refused by the two WRITE endpoints above** (403,
//! `reject_org_account_membership_management`): under the Clerk-authoritative
//! membership model (org-tenancy PR 2.2, migrations 0081/0082), org
//! `account_members` rows are written ONLY by Clerk webhook events — a legacy
//! `/teams` write here would bypass the offboarding tombstones/org
//! revocations (an org owner could re-add a Clerk-offboarded member
//! out-of-band). The read endpoints are unaffected (org membership rows live
//! on the control plane too). **This refusal is unconditional and does NOT
//! consult `ORG_TEAMS_ENABLED`** (org-team-membership design doc, "org gate"
//! item) — org account membership is Clerk-driven full stop, regardless of
//! whether org-context named teams are turned on. Contrast with the CS-094
//! named-team routes below, whose org gate the flag DOES control.
//!
//! # Named teams with envelope encryption (CS-094)
//! - `POST   /v1/teams`                             — create a named team (generates team key)
//! - `PATCH  /v1/teams/{team_id}`                    — rename a team (NEW, team-ocean lifecycle
//!   management — updates BOTH `teams.name` and `graphs.name`, see [`rename_team`]'s doc comment)
//! - `DELETE /v1/teams/{team_id}`                    — archive a team ocean (NEW, team-ocean
//!   lifecycle management — stamps `graphs.archived_at`, does NOT delete content, see
//!   [`archive_team`]'s doc comment)
//! - `POST   /v1/teams/{team_id}/restore`            — unarchive a team ocean (NEW, team-ocean
//!   lifecycle management — the small, additive counterpart to the archive endpoint above; refuses
//!   with 409 past the 120-day restore window, see [`restore_team`]'s doc comment)
//! - `GET    /v1/teams/archived`                     — list archived team oceans for the caller's
//!   account (NEW — the read half of the lifecycle trio; makes `POST .../restore` reachable at all,
//!   since archived teams are otherwise invisible everywhere else. Gated on account-level authority
//!   — owner/org admin, NOT team admin — see [`list_archived_teams`]'s doc comment)
//! - `GET    /v1/teams/{team_id}/members`           — list the active roster (NEW — added at
//!   integration follow-up; the design doc's API-surface table listed this without a `NEW`
//!   marker, but grepping the pre-this-PR file confirmed it had never actually been implemented)
//! - `POST   /v1/teams/{team_id}/members`           — add a member
//! - `PATCH  /v1/teams/{team_id}/members/{user_id}` — assign a member's role (NEW)
//! - `DELETE /v1/teams/{team_id}/members/{user_id}` — soft-remove a member, OR (NEW, team-ocean
//!   lifecycle management) let a member remove THEMSELVES ("leave a workspace") with no admin
//!   rights at all — see [`remove_team_member`]'s doc comment
//! - `GET    /v1/teams/{team_id}/events`            — membership audit history (NEW)
//!
//! # Org directory
//! - `GET    /v1/orgs/members` — the caller's org roster (user_id/name/email/role), the
//!   picker data `POST .../members`'s `user_id` field is chosen from (NEW, org context only).
//!
//! ## The org gate (`ORG_TEAMS_ENABLED`)
//!
//! Every CS-094 named-team route above used to hard-403 any org caller
//! (`reject_org_named_teams`, historically "no org team-key model yet").
//! That blanket refusal is now CONDITIONAL on the `ORG_TEAMS_ENABLED` env
//! flag (default OFF, same parse contract as every other tenancy flag in
//! this codebase — see [`org_teams_active_for`]'s own doc comment): flag off,
//! an org caller gets EXACTLY today's 403 with today's error body; flag on,
//! the org key model below takes over and the route proceeds. Personal /
//! flag-off requests are byte-identical to before either way.
//!
//! ## The org key model (design doc decision #3)
//!
//! Named teams were designed around ONE model — a symmetric team key (TK),
//! wrapped once per active member under THAT member's own personal-account
//! DEK (`team_member_key_envelopes`, one row per `(team_key, user_id)`).
//! That model does not extend to org accounts as-is: an org's DEK lives on
//! its own TENANT plane (`account_keys.dek_wrapped`, resolved via
//! [`crate::crypto::resolve_dek_for_context`]), not the CONTROL plane
//! `accounts.dek_wrapped` [`load_account_dek`]/[`load_user_dek`] read, and
//! org membership can add/remove people the API never even names (Clerk
//! webhooks) — a scheme that must re-wrap a key for every member on every
//! change does not fit an org's membership churn model.
//!
//! So org-context named teams use a SECOND, deliberately simpler model:
//! **one envelope, wrapped under the ORG's own account DEK**, created once
//! at `create_team` time. Adding an org member never touches
//! `team_member_key_envelopes` at all — [`add_member`]'s org branch is a
//! PURE `team_members` roster insert
//! ([`crate::store::Store::add_team_member_tx_no_envelope_scoped`]). This is
//! not a weaker trust boundary than the personal per-member scheme — see the
//! design doc's "Rejected, and why": **the server holds `MASTER_KEK` and
//! [`load_account_dek`]/[`load_user_dek`] unwrap at will**, so a per-member
//! envelope was never a cryptographic access boundary a member without one
//! could not eventually be granted by the server itself; it is purely a
//! key-DISTRIBUTION artifact. Real authorization for who may read a team
//! ocean's content is (and remains) the reachability predicate
//! (`tenancy::reachability`), untouched by any of this file's changes — see
//! design doc decision #14, "Roles are administrative only."
//!
//! [`create_team`] and [`add_member`] each branch on
//! [`crate::tenancy::extractor::Tenant::org_context`] to pick between the
//! two models; every other named-team handler (remove, role-assignment,
//! events) is model-agnostic (it only ever touches `team_members`/
//! `membership_events`, never the envelope table), so those need no org/
//! personal branch at all beyond the authorization difference described
//! next.
//!
//! ## Org-admin roster rights (design doc decision #4/#5)
//!
//! An org admin (`accounts.clerk_org_id IS NOT NULL AND account_members.role
//! IN ('owner', 'admin')` — see [`is_org_admin`]) may perform every roster
//! WRITE (`add_member`, `remove_team_member`, the role-assignment PATCH) on
//! ANY team in their org, even one they are not a member of, and may READ
//! that team's roster and audit history ([`list_team_members`],
//! [`list_team_events`]). This is deliberately narrower than a reachability
//! bypass: an org admin gets NO read access to the CONTENT of a team ocean
//! they are not a member of — nothing here touches `tenancy::reachability`
//! or any of the 41 call sites that trust its single authorization point.
//! Break-glass is an org admin adding THEMSELVES as an ordinary member,
//! which — like every other roster write — is a visible `team_members` row
//! plus a `membership_events` audit row.
//!
//! ### That right has to exist at the ROW LEVEL too, not just here
//!
//! Authorizing an org admin in this file is only half of it. On the org
//! plane every one of those reads and writes runs inside a
//! `tenancy::scope`-scoped transaction against tenant-plane policies
//! (`migrations_tenant/0009_tenant_rls`), and those policies resolve
//! `teams`/`team_members`/`membership_events` through
//! `tenant_rls_visible_graph_ids()`, which for a TEAM ocean requires an
//! ACTIVE `team_members` row. A non-member org admin therefore passed the
//! route check and then met a database that had no rows for them:
//! `list_team_events` answered `200 []` ("this team has no history"),
//! `list_team_members` answered `404 team not found` for a team that plainly
//! exists, and every roster WRITE 404'd at its `FOR UPDATE` row lock. Silent
//! on the surface that exists to be audited. It does not reproduce against a
//! superuser DSN, because `BYPASSRLS` skips policy evaluation entirely.
//!
//! The fix is a second, deliberately narrow session GUC —
//! `app.is_org_admin`, set ONLY by
//! [`crate::tenancy::scope::set_tenant_org_admin`], which cannot be called
//! without an [`crate::tenancy::scope::OrgAdminProof`] minted by the
//! control-plane check itself — plus one `OR tenant_rls_is_org_admin()` arm
//! on the three GOVERNANCE tables' policies
//! (`migrations_tenant/0032_membership_events`).
//! `tenant_rls_visible_graph_ids()` is NOT touched: it is the CONTENT anchor
//! that `atoms`, `nodes`, `conversations`, `team_keys` and ~25 other tables
//! resolve through, and widening it would hand org admins the contents of
//! every team ocean in the tenant — the exact "org-admin read bypass" the
//! design doc's "Rejected, and why" refuses. Staying out of the content path
//! is the entire value of the fix.
//!
//! Concretely: every handler that consults [`is_org_admin`] must open its
//! transaction with the three-argument `begin_scoped_tx_resp!` form so the
//! grant is actually carried. `set_tenant_org_admin` then reads the GUC back
//! through the migration's own function before returning, so a tenant
//! database that has not applied 0032 fails the request loudly instead of
//! serving an empty list.
//!
//! ## Membership ⊆ org membership (design doc decision #10)
//!
//! `add_member` requires the target to already be a LIVE `account_members`
//! row for the caller's account (checked via
//! [`crate::store::Store::is_account_member`], a control-plane read) —
//! applies to both the personal and org paths. Before this, `add_member`
//! would insert ANY `user_id` in the system into `team_members`, whether or
//! not they belonged to the account at all.
//!
//! ## The last-admin race (design doc decision #9, defect #1)
//!
//! `remove_team_member` used to read `count_team_admins` unlocked, at
//! READ COMMITTED — two concurrent removals of a team's two remaining
//! admins could both pass the "not the last admin" guard and orphan the
//! team. Every interactive membership WRITE (`add_member`,
//! `remove_team_member`, the role-assignment PATCH) now opens with
//! [`crate::store::Store::lock_team_row_scoped`] (`SELECT 1 FROM teams
//! WHERE id = $1 FOR UPDATE`) as the first statement of its scoped write
//! transaction, serializing concurrent writers on the same team row before
//! any admin-count read happens. Deliberately NOT a DB constraint: the org
//! offboard cascade (a different agent's work) must still be able to orphan
//! a team when its sole admin leaves the org, and a constraint enforcing
//! "always at least one admin" would wedge that worker forever.
//!
//! ## Audit trail (design doc decision #11)
//!
//! Every roster mutation (`create_team`'s seeded admin, `add_member`,
//! `remove_team_member`, the role PATCH) writes exactly one
//! `membership_events` row IN THE SAME TRANSACTION as the mutation itself —
//! see each `Store` method's own doc comment
//! ([`crate::store::Store::create_team_tx`],
//! [`crate::store::Store::add_team_member_tx`],
//! [`crate::store::Store::add_team_member_tx_no_envelope_scoped`],
//! [`crate::store::Store::soft_remove_team_member`],
//! [`crate::store::Store::update_team_member_role_tx`]).
//!
//! The team-ocean lifecycle trio follows the same discipline, with the
//! `action` vocabulary widened to `'archived'`/`'restored'`/`'renamed'`/
//! `'left'` (`backend/migrations_control/0125_membership_events.sql` and its
//! tenant twin `0032`, edited in place — this table is new in an unmerged PR,
//! see those migrations' headers): [`crate::store::Store::archive_team`]/
//! [`crate::store::Store::restore_team`] and
//! [`crate::store::Store::rename_team`] write them. All four events set `target_user_id` to the ACTOR
//! themselves (mirroring `'created'`) since these are team-level, not
//! member-level, changes — except `'left'`, which is a genuine member-level
//! event where actor and target already agree because the member is acting
//! on their own row (see [`remove_team_member`]'s doc comment for why
//! `'left'` is a distinct value from `'removed'` rather than reusing it).
//!
//! # Envelope encryption flow (personal accounts)
//! Each named team has a symmetric team key (TK) stored only in wrapped form.
//! Every active member has an envelope in `team_member_key_envelopes` holding the TK
//! wrapped under their account DEK.  Adding a member unwraps the caller's envelope,
//! re-wraps the raw TK for the new member, and stores it atomically.
//!
//! # Consumer-plane RLS scoping (org-tenancy OPS-4 phase B.5b)
//!
//! This file has two disjoint halves, and only one of them touches an
//! RLS-covered table:
//!
//! * **The account-level membership routes** (`list_teams`, `list_members`,
//!   `invite_member`, `remove_member`) read and write `accounts`, `users` and
//!   `account_members` through `state.control_store()`. None of those three is
//!   in migration `0083_consumer_rls`'s 36-table roster — they are
//!   CONTROL-plane tables with no policy at all — so these handlers need no
//!   scoping and are left byte-identical. (`list_team_memberships`'s name is
//!   misleading: it joins `accounts` to `account_members`, never `teams` or
//!   `team_members`.)
//! * **The CS-094 named-team routes** (`create_team`, `add_member`,
//!   `remove_team_member`, `assign_team_member_role`, `list_team_events`, and
//!   the team-ocean lifecycle trio `rename_team`/`archive_team`/
//!   `restore_team`) write/read `teams`, `team_keys`, `team_members`,
//!   `team_member_key_envelopes` and `membership_events` — all 0083-covered
//!   (except `membership_events`, whose own migration lands independently of
//!   this file) — `rename_team` additionally writes `graphs.name` and
//!   `archive_team`/`restore_team` write `graphs.archived_at`, both via
//!   `graphs`' OWN 0083 policy, not a new one. Each of these handlers runs
//!   its content work inside ONE
//!   [`crate::tenancy::scope::begin_account_scoped`]-family transaction on
//!   `user.account_id`.
//!
//! **Why the caller's account is the right scope here — and it genuinely is,
//! unlike the reachability-gated routes elsewhere in this conversion.** Named
//! teams are created inside the caller's own account (`create_team_tx` takes
//! `user.account_id` as the owning account and mints the graph there), and
//! every one of these handlers already sits behind `reject_org_named_teams`,
//! which now refuses an org `TenancyDecision` only when `ORG_TEAMS_ENABLED`
//! is off — so with the flag off this is always a Personal request against
//! the shared consumer pool, and the account-of-one that owns the team IS the
//! caller's active account; with the flag on, `user.account_id` is the ORG's
//! own account id, still the correct scope for that org's own team rows.
//! There is no cross-account reader to accommodate: unlike shares or meeting
//! summaries, nothing here is reachability-gated.
//!
//! The DEK reads (`load_account_dek`/`load_user_dek`/`load_org_team_key_dek`)
//! stay unscoped on their respective planes: `accounts.dek_wrapped` and the
//! target user's personal-account DEK are CONTROL-plane columns outside the
//! roster; the org account DEK is a TENANT-plane column resolved through the
//! request's own verified `TenantContext`, never a scoped transaction (it has
//! its own tenant-aware resolver, [`crate::crypto::resolve_dek_for_context`]).
//! In `add_member` these reads also sit between two scoped transactions, which
//! is the second reason each handler's transaction is opened only around the
//! DB work that actually needs it rather than wrapped around the whole
//! handler (PR #291's connection-holding rationale).
//!
//! A `Tenant` with no pool cannot open the scoped transaction, so these
//! handlers that open one return a 500.

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Extension, Json,
};
use chrono::{DateTime, Duration, Utc};
use coralswarm_core::kek::{load_kek, load_kek_next, unwrap_dek_with_rotation};
use coralswarm_core::team_crypto::{generate_team_key, unwrap_team_key, wrap_team_key_for_member};
use serde::Deserialize;
use serde_json::{json, Value};
use std::sync::Arc;
use utoipa::path as utoipa_path;
use uuid::Uuid;

use crate::{
    middleware::auth::UserContext, store::Store, tenancy::extractor::Tenant,
    tenancy::runtime::TenantContext, tenancy::scope::OrgAdminProof, AppState,
};

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Refuse account-level membership WRITES when the caller's active account is
/// an org account (`accounts.clerk_org_id IS NOT NULL`) — org membership is
/// managed exclusively through Clerk (webhook-authoritative; see the module
/// doc). Returns `Some(response)` when the request must be rejected.
///
/// Fail-closed on a lookup error (we cannot prove the account is personal).
/// With no `state.db` configured the request is allowed through. Uses the raw
/// pool rather than a `Store` method because this is control-plane
/// governance, not content access.
async fn reject_org_account_membership_management(
    state: &AppState,
    user: &UserContext,
) -> Option<(StatusCode, Json<Value>)> {
    // `accounts` is CONTROL-plane — read through the named control accessor
    // (org-tenancy PR 1.6; the grep-guard flags the raw `state.db` field).
    let pool = state.control_db()?;
    match sqlx::query_scalar::<_, Option<String>>(
        "SELECT clerk_org_id FROM accounts WHERE id = $1",
    )
    .bind(user.account_id)
    .fetch_optional(pool)
    .await
    {
        Ok(Some(Some(_org))) => Some((
            StatusCode::FORBIDDEN,
            Json(json!({
                "error": "organization membership is managed through your identity provider; \
                          the /teams membership API is unavailable for organization accounts"
            })),
        )),
        Ok(_) => None,
        Err(e) => {
            tracing::error!(error = %e, account_id = %user.account_id, "org-account check failed; refusing membership write");
            Some((
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({"error": "service unavailable"})),
            ))
        }
    }
}

/// Whether org-context named-team routes are ACTIVE for `clerk_org_id` — the
/// `ORG_TEAMS_ENABLED` flag (Foundation train PR 1.3: env default,
/// overridable per-org via the `org_connector_flags` registry — see
/// [`crate::connector_flags::resolve`]) AND its `MEMBER_OCEANS_ENABLED`
/// coupling requirement (org-team-membership design doc, decisions #2/#13),
/// both true. Deliberately named `_active` rather than `_enabled`: this is a
/// COMPOSED check, not the flag reader itself — see
/// [`crate::workers::member_ocean_reconciler::org_teams_enabled`] for the
/// raw `ORG_TEAMS_ENABLED` env-default parse, which supplies this function's
/// `env_default` argument to the registry rather than being re-derived
/// (there used to be a second, private copy of that same one-line parse
/// living in this file; a CodeRabbit-flagged review comment at integration
/// pointed out that two parsers for one env var is exactly how parsing
/// semantics silently drift apart, so it was deleted in favor of calling the
/// reconciler's own reader — that module owns `ORG_TEAMS_ENABLED`'s parse
/// contract, this file only composes on top of it).
///
/// **PR 1.3 promotes this from belt-and-braces to load-bearing for the
/// per-org-override case.** `backend/src/lib.rs`'s `run()` startup assert
/// only ever checked the GLOBAL env-var pair (`ORG_TEAMS_ENABLED` vs
/// `MEMBER_OCEANS_ENABLED`) — a deploy-time misconfiguration check with no
/// notion of a per-org override, because no such override existed. Now that
/// an operator can flip `org_connector_flags` to enable `"teams"` for ONE org
/// without touching the env var at all (and therefore without ever going
/// through a redeploy the startup assert would run at), the boot-time assert
/// cannot see that action. THIS re-check — which still calls
/// `member_oceans_enabled()` itself, unconditionally, regardless of where the
/// `"teams"` decision came from — is now the ONLY thing standing between a
/// per-org override and the "no removal path for a departed org member"
/// failure mode described below for that org. The admin setter
/// ([`crate::routes::admin::set_connector_flag`]) ALSO refuses to write an
/// enabling `"teams"` row while `MEMBER_OCEANS_ENABLED` is globally off, as a
/// second belt — but this function is the one every request actually goes
/// through, so it must keep failing closed on its own regardless of whether
/// that write-time guard was ever bypassed (a row written before this PR
/// shipped, a direct DB edit, etc). Do not add a THIRD copy of this coupling
/// check anywhere else — extend this one, the setter's guard, or the startup
/// assert, not any of them independently.
///
/// Checked once per request (not cached beyond the registry's own TTL cache),
/// so flipping either flag takes effect within that window with no redeploy.
/// Default OFF.
///
/// The coupling itself (design doc decision #13): the org offboard cascade
/// that tombstones `team_members.departing_at` when a member leaves the org
/// ships inside the member-ocean reconciler's archive arm (decision #7, a
/// different agent's work). Turning org teams on WITHOUT that reconciler
/// running would let a departed org member's team-ocean membership silently
/// survive their offboarding forever — there would be no removal path at
/// all for that case. Teams resolving "on" (env default OR per-org override)
/// with `MEMBER_OCEANS_ENABLED` off logs an `error!` (so log-based alerting
/// can page on a genuine misconfiguration) and behaves exactly as if teams
/// had resolved "off" for this org.
pub(crate) async fn org_teams_active_for(state: &AppState, clerk_org_id: &str) -> bool {
    let teams_flag = crate::connector_flags::resolve(
        state,
        clerk_org_id,
        "teams",
        crate::workers::member_ocean_reconciler::org_teams_enabled(),
    )
    .await;
    if !teams_flag {
        return false;
    }
    if !crate::workers::member_ocean_reconciler::member_oceans_enabled() {
        tracing::error!(
            org_id = %clerk_org_id,
            "teams resolved ON for this org but MEMBER_OCEANS_ENABLED is off fleet-wide — \
             refusing to enable org teams. The offboard cascade that revokes a departed org \
             member's team-ocean membership lives in the member-ocean reconciler's archive arm \
             (design doc decision #7); without it running, org teams would have no removal path \
             for a departed member at all. Set MEMBER_OCEANS_ENABLED, or leave teams off for \
             this org (env default and/or its org_connector_flags row)."
        );
        return false;
    }
    true
}

/// Whether `user_id` is an ORG admin (`owner` or `admin`) of `account_id` —
/// org-team-membership design doc decisions #4/#5. Deliberately narrow: this
/// grants roster management (the `team_members`/`membership_events`/`teams`
/// GOVERNANCE tables) only, never a reachability/content read bypass — see
/// the module doc's "Org-admin roster rights" section and the design doc's
/// "Rejected, and why" (an org-admin read bypass would add an OR-arm to the
/// single authorization point 41 call sites trust).
///
/// Returns an [`OrgAdminProof`] rather than a `bool` on purpose: that token is
/// what `tenancy::scope::set_tenant_org_admin` demands before it will set the
/// `app.is_org_admin` GUC the tenant-plane governance policies read
/// (migration `0032_membership_events`). Handlers still get a boolean answer —
/// `.is_some()` — but the value that unlocks the database row is one only this
/// verified check can mint, so no handler can hand the GUC a boolean it
/// computed some other way. The check itself lives in
/// [`crate::tenancy::scope::verify_org_admin`] for exactly that reason (a
/// private constructor is only private if the whole mint site is in one
/// module); this wrapper adds the HTTP mapping.
///
/// Query against the CONTROL plane (`accounts`/`account_members`), the same
/// convention [`reject_org_account_membership_management`] just above already
/// establishes for org governance checks: this is control-plane governance,
/// not tenant content access. With no `state.control_db()` configured this
/// fails closed to `Ok(None)`.
async fn is_org_admin(
    state: &AppState,
    account_id: Uuid,
    user_id: Uuid,
) -> Result<Option<OrgAdminProof>, (StatusCode, Json<Value>)> {
    let Some(pool) = state.control_db() else {
        return Ok(None);
    };
    match crate::tenancy::scope::verify_org_admin(pool, account_id, user_id).await {
        Ok(v) => Ok(v),
        Err(e) => {
            tracing::error!(error = %e, %account_id, %user_id, "is_org_admin check failed; refusing org-admin bypass");
            Err((
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({"error": "service unavailable"})),
            ))
        }
    }
}

/// The `account_members.role` values that grant ACCOUNT-level authority —
/// `'owner'` and `'admin'`. Factored out as a pure predicate purely so
/// [`has_account_authority`]'s exact decision is unit-testable without a
/// database — see the `account_role_grants_authority_*` tests below.
///
/// Deliberately does NOT consult `team_members.role` at all: a caller can be
/// a TEAM admin (`team_members.role = 'admin'`) while their OWN
/// `account_members.role` is a plain `'member'`, and that caller must still
/// be refused here — see [`has_account_authority`]'s own doc comment for why
/// that asymmetry is load-bearing, not an oversight.
fn account_role_grants_authority(role: Option<&str>) -> bool {
    matches!(role, Some("owner") | Some("admin"))
}

/// Whether `user_id` holds ACCOUNT-level authority — owner or org admin —
/// over `account_id`: `account_members.role IN ('owner', 'admin')`
/// (workspace-manage design doc, "Archive and restore"). Gates
/// [`list_archived_teams`] below.
///
/// This is deliberately a DIFFERENT (and narrower) check than [`is_org_admin`]
/// just above, and reuses that function's own `get_account_member_role`
/// STORE read rather than adding a fourth hand-rolled role query to this
/// file: `is_org_admin` answers `Ok(None)` — never `Some`, for ANYONE — on
/// every personal account, because it requires `accounts.clerk_org_id IS NOT
/// NULL` by construction (see its own doc comment). A literal "must be an
/// org admin" gate on the archived-teams list would therefore be
/// unreachable on a personal account, and personal accounts are exactly
/// where named teams (and therefore archived team oceans) work today
/// (`ORG_TEAMS_ENABLED` defaults off — see [`org_teams_active_for`]). This
/// check reads `account_members.role` directly instead, which already
/// stores `'owner'` on personal accounts and `'admin'` on org accounts, so
/// ONE rule reads correctly on both planes without a personal/org branch.
///
/// Control-plane read (`account_members`), same convention every other
/// governance check in this file already establishes. With no
/// `state.control_store()` configured this fails closed to `Ok(false)`.
pub(crate) async fn has_account_authority(
    state: &AppState,
    account_id: Uuid,
    user_id: Uuid,
) -> Result<bool, (StatusCode, Json<Value>)> {
    let Some(store) = state.control_store() else {
        return Ok(false);
    };
    match store.get_account_member_role(account_id, user_id).await {
        // Settings launch PR 2.8: never for a non-interactive credential.
        Ok(role) => Ok(account_role_grants_authority(role.as_deref())
            && crate::middleware::auth::authority_allowed()),
        Err(e) => {
            tracing::error!(error = %e, %account_id, %user_id, "account-authority check failed; refusing");
            Err((
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({"error": "service unavailable"})),
            ))
        }
    }
}

/// How long a team ocean stays restorable after being archived — the ONLY
/// place `120` appears as a literal in this file. Both window computations
/// ([`restore_team`]'s 409 refusal below, and the `expires_at`/`restorable`
/// fields [`list_archived_teams`] returns) go through
/// [`team_restore_window`], which reads this constant, so the two call sites
/// can never silently disagree the way two independently-inlined `120`s
/// could.
///
/// Same shape as the member-ocean archive → restore → purge window
/// (`workers::member_ocean_reconciler`, `interval '60 days'` — see e.g. its
/// `restore_arm`/`purge_arm` SQL and `docs/plans/member-private-oceans.md`
/// §2.6), deliberately LONGER here (120 vs. 60 days): unlike a departing
/// individual's private ocean, archiving a shared team workspace is a
/// heavier, more consequential action with more people who might need to
/// notice and ask for it back.
///
/// **No hard purge at the end of this window in this PR** — a >120-day
/// archived team ocean is left invisible, unrestorable, and undeleted, the
/// same known interim state member oceans sat in between their PR 7 and PR
/// 8. Purge belongs in its own future arm with its own flag and its own
/// retention window, mirroring `MEMBER_OCEAN_PURGE_ENABLED`
/// (`workers::member_ocean_reconciler`'s purge arm) — see [`archive_team`]'s
/// doc comment for the identical discipline already applied to content
/// deletion, which this window extends to the archived-row's visibility
/// itself. If you are reading this while wiring up that purge arm: this
/// constant is very likely NOT the right window for it — re-derive the
/// retention period from the purge design doc rather than assuming it must
/// match the restore window.
const TEAM_OCEAN_RESTORE_WINDOW_DAYS: i64 = 120;

/// `(expires_at, restorable)` for a team ocean archived at `archived_at`,
/// evaluated against `now`. `expires_at` is `archived_at +`
/// [`TEAM_OCEAN_RESTORE_WINDOW_DAYS`]; `restorable` is `now <= expires_at` —
/// INCLUSIVE, so the instant `archived_at + 120 days` itself is still
/// restorable, and only strictly AFTER that instant is refused.
///
/// Pure function (no I/O), so it is unit-testable without a database — see
/// the `restore_window_day_119_is_restorable`/
/// `restore_window_day_121_is_expired` tests below, which pin this exact
/// boundary.
pub(crate) fn team_restore_window(archived_at: DateTime<Utc>, now: DateTime<Utc>) -> (DateTime<Utc>, bool) {
    let expires_at = archived_at + Duration::days(TEAM_OCEAN_RESTORE_WINDOW_DAYS);
    (expires_at, now <= expires_at)
}

/// The CONTROL-plane store, for the account-level membership routes below
/// (org-tenancy PR 1.6): `account_members`/`users`/`accounts` are control-tree
/// tables with no counterpart on any tenant database, so those routes pin
/// `state.control_store()` — same convention as `routes::graphs`'s
/// `require_control_store!`. Variant for handlers returning
/// `(StatusCode, Json<Value>)`.
macro_rules! require_control_store {
    ($state:expr) => {
        match $state.control_store() {
            Some(s) => s,
            None => {
                return (
                    StatusCode::SERVICE_UNAVAILABLE,
                    Json(json!({"error": "service unavailable"})),
                )
            }
        }
    };
}

/// Content-plane store from the `Tenant` extractor, for the CS-094 named-team
/// routes (org-tenancy PR 1.6; was ambient `state.store`) — `teams`/
/// `team_members`/`team_keys`/`team_member_key_envelopes`/`graphs`/
/// `membership_events` are tenant-tree tables. Variant for handlers that
/// return `impl IntoResponse` / `Response`.
macro_rules! require_store_resp {
    ($tenant:expr) => {
        match &$tenant.store {
            Some(s) => s,
            None => {
                return (
                    StatusCode::SERVICE_UNAVAILABLE,
                    Json(json!({"error": "service unavailable"})),
                )
                    .into_response()
            }
        }
    };
}

/// Control-plane counterpart of [`require_store_resp!`], for the named-team
/// handlers' DEK reads ([`load_account_dek`]/[`load_user_dek`] resolve
/// `accounts.dek_wrapped`/the target user's personal-account DEK — both
/// CONTROL-plane) and for [`add_member`]'s membership-subset check
/// (`is_account_member`, also CONTROL-plane, used on both the personal and
/// org paths).
macro_rules! require_control_store_resp {
    ($state:expr) => {
        match $state.control_store() {
            Some(s) => s,
            None => {
                return (
                    StatusCode::SERVICE_UNAVAILABLE,
                    Json(json!({"error": "service unavailable"})),
                )
                    .into_response()
            }
        }
    };
}

/// Open a named-team handler's account-scoped transaction (org-tenancy OPS-4
/// phase B.5b), or early-return a 500. Always `Some`: with no pool
/// `begin_governance_scoped` errors and this returns the 500.
///
/// Always keyed on `user.account_id` — see the module doc's RLS section for
/// why that is the correct scope for this route family specifically.
///
/// The three-argument form additionally carries an
/// `Option<OrgAdminProof>` into
/// [`crate::tenancy::extractor::Tenant::begin_governance_scoped`] (defined in
/// `tenancy::scope`), which sets the
/// `app.is_org_admin` GUC the tenant-plane governance policies read. EVERY
/// handler that consults `is_org_admin` for its authorization decision must
/// use that form — see the module doc's "Org-admin roster rights" section for
/// what goes wrong when the route says yes and the row-level policy says
/// nothing. Passing `None` is exactly the two-argument form.
macro_rules! begin_scoped_tx_resp {
    ($tenant:expr, $account_id:expr) => {
        begin_scoped_tx_resp!($tenant, $account_id, None)
    };
    ($tenant:expr, $account_id:expr, $org_admin:expr) => {
        {
            match $tenant.begin_governance_scoped($account_id, $org_admin).await {
                Ok(tx) => Some(tx),
                Err(e) => {
                    tracing::error!(error = %e, "failed to open account-scoped transaction");
                    return (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(json!({"error": "internal error"})),
                    )
                        .into_response();
                }
            }
        }
    };
}

/// Unwrap an AUTHORIZATION read, failing closed AND loud.
///
/// Every one of these reads used to be `.unwrap_or(None)` / `.unwrap_or(false)`
/// / `.unwrap_or(0)`. That failed closed — correct — but it made a genuine
/// store error INDISTINGUISHABLE from a real "no, you are not an admin": the
/// caller got a 403 (or, for `count_team_admins`, a 409 "cannot remove the
/// last admin") and nothing at all was written to the log. A CodeRabbit review
/// flagged the pattern; the fix is not to open the gate on error, it is to
/// stop lying about WHY it is closed.
///
/// **500, not 403** — deliberately, and the same call
/// [`reject_org_account_membership_management`]/[`is_org_admin`] already make
/// one screen up (they answer 503 on a control-plane read failure rather than
/// silently denying). A store error is not an authorization decision: 403 is a
/// factual claim about the caller's rights that we have no evidence for, and
/// it sends a database outage to the support queue as a permissions complaint
/// while the real signal — a dead pool — never surfaces at all. 5xx is honest,
/// is what monitoring alerts on, and (critically) still refuses the request,
/// so nothing about the fail-closed property changes.
macro_rules! authz_read {
    ($check:expr, $result:expr) => {
        match $result {
            Ok(v) => v,
            Err(e) => {
                tracing::error!(
                    error = %e,
                    check = $check,
                    "authorization read failed; refusing the request (fail-closed)"
                );
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({"error": "internal error"})),
                )
                    .into_response();
            }
        }
    };
}

/// Dispatch one Store call through the scoped transaction, or the plain
/// unscoped method when `$tx` is `None`. `begin_scoped_tx_resp!` never yields `None`: with
/// no pool it returns a 500 instead.
macro_rules! scoped_store {
    ($store:expr, $tx:expr, $unscoped:ident / $scoped:ident ( $($arg:expr),* $(,)? )) => {
        match $tx.as_mut() {
            Some(tx) => $store.$scoped(tx, $($arg),*).await,
            None => $store.$unscoped($($arg),*).await,
        }
    };
}

/// Translate the `{team_id}` PATH PARAMETER to the team's real `teams.id`
/// (see [`crate::store::Store::resolve_team_id`]'s doc comment for the full
/// "id resolution compat" story — `GET /v1/graphs` exposes only `graphs.id`,
/// never `teams.id`, so every by-id call the web UI makes on a team ocean
/// arrives carrying a `graph_id`, which never matched `teams.id` and 404'd —
/// the prod incident this macro fixes).
///
/// MUST be the very first team-touching statement in a handler, evaluated
/// immediately after the scoped transaction opens and before
/// `team_exists`/`lock_team_row_or_return!`/any other `team_id`-keyed
/// read-or-write — everything after it is keyed on `teams.id`, not
/// `graph_id`, so resolving late would leave those earlier calls still
/// looking up the wrong row.
///
/// A miss (neither `id` nor `graph_id` matches anything) evaluates to the
/// ORIGINAL, unresolved value, so the very next `team_exists`/
/// `lock_team_row_or_return!` check still 404s exactly as it did before this
/// macro existed — this only ever narrows an ambiguous id to the real row,
/// it never invents an answer or changes what "not found" means.
macro_rules! resolve_team_id {
    ($store:expr, $tx:expr, $team_id:expr) => {
        match authz_read!(
            "resolve_team_id",
            scoped_store!($store, $tx, resolve_team_id / resolve_team_id_scoped($team_id))
        ) {
            Some(id) => id,
            None => $team_id,
        }
    };
}

/// Acquire [`crate::store::Store::lock_team_row_scoped`] as the first
/// statement of a scoped write transaction — org-team-membership design doc
/// decision #9/defect #1 (see the module doc's "last-admin race" section).
/// A no-op when there is no scoped transaction at all. On `Ok(false)` (team no longer
/// exists) or `Err` this returns a `Response` for the caller to propagate
/// with `return`; on success it returns `()` and the caller's transaction
/// now holds the lock for its remaining lifetime.
macro_rules! lock_team_row_or_return {
    ($store:expr, $tx:expr, $team_id:expr) => {
        if let Some(pg_tx) = $tx.as_mut() {
            match $store.lock_team_row_scoped(pg_tx, $team_id).await {
                Ok(true) => {}
                Ok(false) => {
                    return (
                        StatusCode::NOT_FOUND,
                        Json(json!({"error": "team not found"})),
                    )
                        .into_response()
                }
                Err(e) => {
                    tracing::error!(error = %e, team_id = %$team_id, "failed to lock team row");
                    return (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(json!({"error": "internal error"})),
                    )
                        .into_response();
                }
            }
        }
    };
}

/// Commit a WRITE handler's scoped transaction before responding — a commit
/// failure means nothing was persisted, so it must surface as a 500, never a
/// false success.
macro_rules! commit_write_tx {
    ($tx:expr) => {
        if let Some(tx) = $tx.take() {
            if let Err(e) = tx.commit().await {
                tracing::error!(error = %e, "scoped transaction commit failed");
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({"error": "internal error"})),
                )
                    .into_response();
            }
        }
    };
}

/// Close a READ-ONLY scoped transaction. Nothing was written, so a commit
/// failure is logged and never surfaced.
macro_rules! close_read_tx {
    ($tx:expr) => {
        if let Some(tx) = $tx.take() {
            if let Err(e) = tx.commit().await {
                tracing::warn!(error = %e, "read-only scoped transaction commit failed");
            }
        }
    };
}

/// Fail-closed gate for the CS-094 named-team routes under an org
/// `TenancyDecision` (org-tenancy PR 1.6, lifted by the org-team-membership
/// design doc). `None` (no org context, or the tenancy runtime is
/// off/absent) always passes through unchanged — Personal / flag-off
/// requests are byte-identical to before this design shipped.
///
/// Under an org `TenancyDecision`: passes through when
/// [`org_teams_active_for`] is true (the org key model + org-admin
/// authorization documented in the module doc take over from here — this
/// gate only decides PLANE eligibility, never per-request authorization);
/// otherwise refuses with EXACTLY today's 403 body (the JSON error message is
/// the API contract this preserves — only the internal log line's stated
/// reason changed, since "no org team-key model yet" stopped being true once
/// the flag exists to turn that model on).
async fn reject_org_named_teams(state: &AppState, tenant: &Tenant) -> Option<Response> {
    let ctx = tenant.org_context()?;
    if org_teams_active_for(state, &ctx.org_id).await {
        return None;
    }
    tracing::info!(
        org_id = %ctx.org_id,
        "named-team route refused under org context (ORG_TEAMS_ENABLED is off)"
    );
    Some(
        (
            StatusCode::FORBIDDEN,
            Json(json!({
                "error": "named teams are not yet available in organization workspaces"
            })),
        )
            .into_response(),
    )
}

// ── GET /v1/teams ─────────────────────────────────────────────────────────────

#[utoipa_path(
    get,
    path = "/v1/teams",
    tag = "teams",
    security(("bearer_token" = [])),
    responses(
        (status = 200, description = "List of accounts the caller belongs to", body = Value),
        (status = 503, description = "Database unavailable"),
    )
)]
pub async fn list_teams(
    State(state): State<Arc<AppState>>,
    Extension(user): Extension<UserContext>,
) -> (StatusCode, Json<Value>) {
    // Control-plane read (`account_members` join): org accounts have their
    // membership rows on the control plane too, so this serves org callers
    // unchanged (org-tenancy PR 1.6).
    let store = require_control_store!(state);

    let rows = match store.list_team_memberships(user.user_id).await {
        Ok(r) => r,
        Err(e) => {
            tracing::error!(error = %e, "list_teams DB error");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            );
        }
    };

    let teams: Vec<Value> = rows
        .into_iter()
        .map(|m| {
            json!({
                "account_id":    m.account_id,
                "owner_user_id": m.owner_user_id,
                "plan_tier":     m.plan_tier,
                "role":          m.role,
                "joined_at":     m.joined_at,
            })
        })
        .collect();

    (StatusCode::OK, Json(json!(teams)))
}

// ── GET /v1/teams/members ─────────────────────────────────────────────────────

#[utoipa_path(
    get,
    path = "/v1/teams/members",
    tag = "teams",
    security(("bearer_token" = [])),
    responses(
        (status = 200, description = "Members of the caller's account", body = Value),
        (status = 503, description = "Database unavailable"),
    )
)]
pub async fn list_members(
    State(state): State<Arc<AppState>>,
    Extension(user): Extension<UserContext>,
) -> (StatusCode, Json<Value>) {
    // Control-plane read — see `list_teams`'s comment.
    let store = require_control_store!(state);

    let rows = match store.list_account_members(user.account_id).await {
        Ok(r) => r,
        Err(e) => {
            tracing::error!(error = %e, "list_members DB error");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            );
        }
    };

    let members: Vec<Value> = rows
        .into_iter()
        .map(|m| {
            json!({
                "user_id":   m.user_id,
                "email":     m.email,
                "role":      m.role,
                "joined_at": m.joined_at,
            })
        })
        .collect();

    (StatusCode::OK, Json(json!(members)))
}

// ── POST /v1/account/members (was POST /v1/teams/invite) ─────────────────────

#[derive(Debug, Deserialize)]
pub struct InviteRequest {
    pub email: String,
    pub role: String,
}

#[utoipa_path(
    post,
    path = "/v1/account/members",
    tag = "teams",
    security(("bearer_token" = [])),
    responses(
        (status = 200, description = "User invited",            body = Value),
        (status = 400, description = "Invalid role"),
        (status = 403, description = "Caller is not an owner"),
        (status = 409, description = "User already a member"),
        (status = 503, description = "Database unavailable"),
    )
)]
pub async fn invite_member(
    State(state): State<Arc<AppState>>,
    Extension(user): Extension<UserContext>,
    Json(body): Json<InviteRequest>,
) -> (StatusCode, Json<Value>) {
    // Settings launch PR 2.8: a human, browser-session action only
    // (`middleware::auth::authority_allowed`; `require_auth` answers this
    // 403 as `interactive_session_required`).
    if !crate::middleware::auth::authority_allowed() {
        return (StatusCode::FORBIDDEN, Json(json!({"error": "interactive session required"})));
    }
    // ── Org accounts: membership is Clerk's, not this API's (PR 2.2) ─────────
    // Checked FIRST (before role validation or the owner check) so an org
    // caller always gets the same 403 regardless of payload or role.
    if let Some(reject) = reject_org_account_membership_management(&state, &user).await {
        return reject;
    }
    // ── Validate role ─────────────────────────────────────────────────────────
    if body.role != "member" && body.role != "owner" {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "role must be 'member' or 'owner'"})),
        );
    }

    // Control-plane store: every table this WRITE path touches
    // (`account_members`, `users`) is control-tree, and the org gate above
    // already refused org accounts (org-tenancy PR 1.6).
    let store = require_control_store!(state);

    // ── Authorization: only owners can invite ─────────────────────────────────
    let caller_role = match store.get_account_member_role(user.account_id, user.user_id).await {
        Ok(r) => r,
        Err(e) => {
            tracing::error!(error = %e, "invite_member role check error");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            );
        }
    };

    match caller_role.as_deref() {
        Some("owner") => {}
        _ => {
            return (
                StatusCode::FORBIDDEN,
                Json(json!({"error": "only owners can invite members"})),
            )
        }
    }

    // ── Look up user by email; create if missing ───────────────────────────────
    let invited_email = body.email.trim().to_lowercase();

    let existing_user_id = match store.find_user_by_email(&invited_email).await {
        Ok(r) => r,
        Err(e) => {
            tracing::error!(error = %e, "invite_member user lookup error");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            );
        }
    };

    let (invited_user_id, is_new_user) = match existing_user_id {
        Some(id) => (id, false),
        None => {
            // Create a stub user record (no password/session yet).
            match store.create_stub_user(&invited_email).await {
                Ok(id) => (id, true),
                Err(e) => {
                    tracing::error!(error = %e, "invite_member user insert error");
                    return (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(json!({"error": "internal error"})),
                    );
                }
            }
        }
    };

    // ── Check they are not already a member ───────────────────────────────────
    if !is_new_user {
        let already_member = match store.is_account_member(user.account_id, invited_user_id).await {
            Ok(r) => r,
            Err(e) => {
                tracing::error!(error = %e, "invite_member membership check error");
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({"error": "internal error"})),
                );
            }
        };

        if already_member {
            return (
                StatusCode::CONFLICT,
                Json(json!({"error": "user is already a member of this account"})),
            );
        }
    }

    // ── Insert membership ─────────────────────────────────────────────────────
    // `seat_tier_id: None` — this route is gated to personal accounts only
    // by `reject_org_account_membership_management` above, and a personal
    // member must never carry a seat tier (`account_members_seat_tier_iff_org`).
    if let Err(e) =
        store.insert_account_member(user.account_id, invited_user_id, &body.role, None).await
    {
        tracing::error!(error = %e, "invite_member insert membership error");
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": "internal error"})),
        );
    }

    // ── Best-effort billing seed (enforcement-completion PR review round 2,
    // finding 3) ─────────────────────────────────────────────────────────────
    // This is the join path for PERSONAL/team accounts (`clerk_org_id IS
    // NULL` — the org gate at the top of this handler already refused any
    // org account), so it is exactly the path
    // `bin/backfill_billing_enforcement` flips going forward and the one
    // `clerk_provision::ensure_org_membership`'s own call seeds on the org
    // side. Same invariant, same reasoning: with `billing_enforcement_enabled
    // = true` (now universal — enforcement-completion PR), a member with no
    // `member_allowance_periods` row falls through to the account-level pool
    // check in `enforcement::is_blocked`, and a free grant never credits that
    // pool — an unseeded member is hard-blocked instead of getting the free
    // tier the moment they can first make a request.
    //
    // Best-effort and deliberately after the membership INSERT above, never
    // before: a failure here must not fail the invite — the membership
    // itself is what matters, and `seed_member_period` is idempotent, so a
    // later membership upsert (a re-invite/role change) or an operator
    // `backfill_billing_enforcement` re-run repairs a missed seed. Uses the
    // raw pool (`state.control_db()`), same as
    // `reject_org_account_membership_management` above, since this is
    // control-plane billing state, not `Store`-mediated content access.
    if let Some(pool) = state.control_db() {
        if let Err(e) =
            crate::billing::renewal::seed_member_period(pool, user.account_id, invited_user_id)
                .await
        {
            tracing::warn!(
                error = %e, account_id = %user.account_id, user_id = %invited_user_id,
                "billing: failed to seed the invited member's allowance period — invite itself \
                 still succeeded; a later membership upsert or an operator backfill pass can \
                 repair this"
            );
        }
    } else {
        tracing::warn!(
            account_id = %user.account_id, user_id = %invited_user_id,
            "billing: control-plane pool unavailable — skipped seeding the invited member's \
             allowance period; invite itself still succeeded, and an operator \
             backfill_billing_enforcement pass will repair this"
        );
    }

    // ── Send invite email via Resend (fire-and-forget) ────────────────────────
    if let Some(resend_key) = state.resend_api_key.clone() {
        let client = state.http_client.clone();
        let email_to = invited_email.clone();
        let account_id = user.account_id;
        // Sync (no DB call) and always the PERSONAL base — this route's own
        // org gate above (`reject_org_account_membership_management`)
        // already guarantees every reachable caller here has
        // `clerk_org_id IS NULL`, so there is no org host to consider.
        // Computed before the spawn since `state` itself isn't otherwise
        // captured by this task (org-slack step 5 — replaces the previous
        // hardcoded `https://app.coralswarm.example`, which only happened to be
        // correct because `WEB_APEX_DOMAIN` is `coralswarm.example` in prod
        // today; this tracks that value instead of hardcoding it).
        let web_base = state.personal_web_base_url();
        tokio::spawn(async move {
            let payload = json!({
                "from": "person@example.com",
                "to": [email_to],
                "subject": "You've been invited to a Coralswarm account",
                "html": format!(
                    "<p>You've been invited to join a Coralswarm account ({account_id}).</p>\
                     <p>Sign in at <a href=\"{web_base}\">{web_base}</a> to accept.</p>"
                ),
            });
            match client
                .post("https://api.resend.com/emails")
                .bearer_auth(&resend_key)
                .json(&payload)
                .send()
                .await
            {
                Ok(resp) if resp.status().is_success() => {
                    tracing::info!(%email_to, "invite email sent");
                }
                Ok(resp) => {
                    tracing::warn!(status = %resp.status(), %email_to, "Resend returned non-success for invite");
                }
                Err(e) => {
                    tracing::warn!(error = %e, %email_to, "failed to send invite email via Resend");
                }
            }
        });
    }

    (
        StatusCode::OK,
        Json(json!({
            "user_id": invited_user_id,
            "email":   invited_email,
            "role":    body.role,
            "invited": true,
        })),
    )
}

/// Deprecated alias for [`invite_member`] at the old, misnamed path
/// (`POST /v1/teams/invite` writes `account_members`, not `teams` — see the
/// module doc's decision #15). Kept for one release per the design doc.
///
/// A separate function rather than a second route on the same handler
/// because `utoipa::path`'s `deprecated` flag is driven by Rust's own
/// `#[deprecated]` attribute (`ast_fn.attrs.has_deprecated()`), which would
/// emit a compiler warning at every call site of `invite_member` (including
/// this crate's own route registration) unless every one of those call sites
/// also carried `#[allow(deprecated)]` — too invasive a lint-surface change
/// to make blind in a PR whose build this agent cannot run locally. The
/// deprecation is instead communicated through the OpenAPI `description`
/// text below, which every client-facing doc viewer (Swagger UI included)
/// surfaces just as visibly.
#[utoipa_path(
    post,
    path = "/v1/teams/invite",
    tag = "teams",
    security(("bearer_token" = [])),
    description = "Deprecated — use `POST /v1/account/members` instead. Kept as a \
                    compatibility alias for one release.",
    responses(
        (status = 200, description = "User invited",            body = Value),
        (status = 400, description = "Invalid role"),
        (status = 403, description = "Caller is not an owner"),
        (status = 409, description = "User already a member"),
        (status = 503, description = "Database unavailable"),
    )
)]
pub async fn invite_member_legacy(
    State(state): State<Arc<AppState>>,
    Extension(user): Extension<UserContext>,
    Json(body): Json<InviteRequest>,
) -> (StatusCode, Json<Value>) {
    // Settings launch PR 2.8: a human, browser-session action only
    // (`middleware::auth::authority_allowed`; `require_auth` answers this
    // 403 as `interactive_session_required`).
    if !crate::middleware::auth::authority_allowed() {
        return (StatusCode::FORBIDDEN, Json(json!({"error": "interactive session required"})));
    }
    invite_member(State(state), Extension(user), Json(body)).await
}

// ── DELETE /v1/account/members/{user_id} (was DELETE /v1/teams/members/{user_id}) ─

#[utoipa_path(
    delete,
    path = "/v1/account/members/{user_id}",
    tag = "teams",
    security(("bearer_token" = [])),
    params(("user_id" = Uuid, Path, description = "User to remove")),
    responses(
        (status = 204, description = "Member removed"),
        (status = 403, description = "Caller is not an owner"),
        (status = 404, description = "Member not found"),
        (status = 409, description = "Cannot remove last owner"),
        (status = 503, description = "Database unavailable"),
    )
)]
pub async fn remove_member(
    State(state): State<Arc<AppState>>,
    Extension(user): Extension<UserContext>,
    Path(target_user_id): Path<Uuid>,
) -> (StatusCode, Json<Value>) {
    // Settings launch PR 2.8: a human, browser-session action only
    // (`middleware::auth::authority_allowed`; `require_auth` answers this
    // 403 as `interactive_session_required`).
    if !crate::middleware::auth::authority_allowed() {
        return (StatusCode::FORBIDDEN, Json(json!({"error": "interactive session required"})));
    }
    // ── Org accounts: membership is Clerk's, not this API's (PR 2.2) ─────────
    // Removals too: an out-of-band delete here would strip a member WITHOUT
    // the offboarding tombstone/grant-clear, leaving Clerk and this backend
    // disagreeing about who was offboarded and how.
    if let Some(reject) = reject_org_account_membership_management(&state, &user).await {
        return reject;
    }
    // Control-plane store — see `invite_member`'s comment.
    let store = require_control_store!(state);

    // ── Authorization: caller must be an owner ────────────────────────────────
    let caller_role = match store.get_account_member_role(user.account_id, user.user_id).await {
        Ok(r) => r,
        Err(e) => {
            tracing::error!(error = %e, "remove_member caller role check error");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            );
        }
    };

    match caller_role.as_deref() {
        Some("owner") => {}
        _ => {
            return (
                StatusCode::FORBIDDEN,
                Json(json!({"error": "only owners can remove members"})),
            )
        }
    }

    // ── Guard: cannot remove self if last owner ───────────────────────────────
    if target_user_id == user.user_id {
        let owner_count = match store.count_account_owners(user.account_id).await {
            Ok(n) => n,
            Err(e) => {
                tracing::error!(error = %e, "remove_member owner count error");
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({"error": "internal error"})),
                );
            }
        };

        if owner_count <= 1 {
            return (
                StatusCode::CONFLICT,
                Json(json!({"error": "cannot remove the last owner of an account"})),
            );
        }
    }

    // ── Delete the membership record ──────────────────────────────────────────
    let rows_affected = match store.delete_account_member(user.account_id, target_user_id).await {
        Ok(n) => n,
        Err(e) => {
            tracing::error!(error = %e, "remove_member delete error");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            );
        }
    };

    if rows_affected == 0 {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({"error": "member not found in this account"})),
        );
    }

    (StatusCode::NO_CONTENT, Json(json!(null)))
}

/// Deprecated alias for [`remove_member`] — see [`invite_member_legacy`]'s
/// doc comment for why this is a separate function rather than a second
/// route on the same handler.
#[utoipa_path(
    delete,
    path = "/v1/teams/members/{user_id}",
    tag = "teams",
    security(("bearer_token" = [])),
    params(("user_id" = Uuid, Path, description = "User to remove")),
    description = "Deprecated — use `DELETE /v1/account/members/{user_id}` instead. Kept as a \
                    compatibility alias for one release.",
    responses(
        (status = 204, description = "Member removed"),
        (status = 403, description = "Caller is not an owner"),
        (status = 404, description = "Member not found"),
        (status = 409, description = "Cannot remove last owner"),
        (status = 503, description = "Database unavailable"),
    )
)]
pub async fn remove_member_legacy(
    State(state): State<Arc<AppState>>,
    Extension(user): Extension<UserContext>,
    Path(target_user_id): Path<Uuid>,
) -> (StatusCode, Json<Value>) {
    // Settings launch PR 2.8: a human, browser-session action only
    // (`middleware::auth::authority_allowed`; `require_auth` answers this
    // 403 as `interactive_session_required`).
    if !crate::middleware::auth::authority_allowed() {
        return (StatusCode::FORBIDDEN, Json(json!({"error": "interactive session required"})));
    }
    remove_member(State(state), Extension(user), Path(target_user_id)).await
}

// ── CS-094: Named teams with envelope encryption ──────────────────────────────

#[derive(Deserialize)]
pub struct CreateTeamRequest {
    pub name: String,
}

#[derive(Deserialize)]
pub struct AddMemberRequest {
    pub user_id: Uuid,
    pub role: String,
}

#[derive(Debug, Deserialize)]
pub struct AssignRoleRequest {
    pub role: String,
}

/// Body of `PATCH /v1/teams/{team_id}` (team-ocean lifecycle management —
/// rename).
///
/// `ToSchema` + [`rename_team`]'s own `#[utoipa_path(request_body = ...)]`
/// attribute is what registers this in the generated OpenAPI spec's
/// `#/components/schemas` — the same pairing `IngestAtomRequest`/
/// `CleanupTranscriptRequest`/`CreateQueryRequest` use elsewhere in this
/// codebase (`routes::ingest`, `routes::transcript`, `routes::queries`).
/// Without both halves the handler's request body type is simply absent
/// from the spec — CodeRabbit finding on PR #537.
#[derive(Debug, Deserialize, utoipa::ToSchema)]
pub struct RenameTeamRequest {
    pub name: String,
}

/// Max length for a team's display name, enforced by [`rename_team`]. NOT
/// inherited from [`create_team`]: that handler's own [`CreateTeamRequest`]
/// validates only non-emptiness after trim, and `teams.name`/`graphs.name`
/// are both unconstrained `TEXT` columns with no DB-level cap either — so
/// this is a NEW constraint this endpoint introduces rather than one that
/// already existed and is merely being matched. 200 mirrors
/// `routes::conversations::patch_conversation`'s own title cap
/// (`title.len() > 200`), the closest existing precedent in this codebase
/// for a short display-name-shaped field; applying the same cap to
/// `create_team` would be a reasonable follow-up so the two endpoints agree,
/// but is out of scope for this change (renaming is the only thing asked
/// for here, and widening `create_team`'s validation is a separate,
/// unrelated behavior change for a handler this PR does not otherwise
/// touch).
const MAX_TEAM_NAME_LEN: usize = 200;

/// Validate + normalize a team name for [`rename_team`]: trim, reject
/// empty-after-trim, reject over [`MAX_TEAM_NAME_LEN`].
///
/// A free function rather than inlined in the handler, for the same reason
/// [`events_page_bounds`] is: `rename_team` cannot be reached at all without
/// a `Store` (`require_store_resp!` runs before this check — see
/// `rename_team_empty_name_hits_the_store_gate_first`), so leaving the logic
/// inline meant no unit test could exercise the boundary without a database.
/// A prior version of this test asserted the test's OWN string construction
/// (`"a".repeat(MAX_TEAM_NAME_LEN).chars().count() <= MAX_TEAM_NAME_LEN`)
/// rather than calling any validation code — the exact tautological-test
/// pattern review has already asked this file to stop reintroducing. Pulling
/// the check into a pure function that the handler actually calls makes it
/// possible to test the real code path without a DB.
fn validate_team_name(raw: &str) -> Result<String, &'static str> {
    let trimmed = raw.trim().to_string();
    if trimmed.is_empty() {
        return Err("name must not be empty");
    }
    if trimmed.chars().count() > MAX_TEAM_NAME_LEN {
        return Err("name is too long");
    }
    Ok(trimmed)
}

#[derive(Debug, Deserialize)]
pub struct TeamEventsQueryParams {
    #[serde(default = "default_events_page")]
    pub page: u32,
    #[serde(default = "default_events_page_size")]
    pub page_size: u32,
}

fn default_events_page() -> u32 {
    1
}

fn default_events_page_size() -> u32 {
    20
}

/// Resolve `GET /v1/teams/{id}/events`'s pagination into the `(LIMIT, OFFSET)`
/// pair the store method takes.
///
/// A free function rather than three lines inlined in [`list_team_events`] so
/// the bounds are testable without a database: the handler cannot be reached at
/// all without a `Store`, so inline arithmetic here was code no unit test could
/// observe. The clamping is what makes the endpoint safe to expose — a caller
/// asking for `page_size=100000` gets 50, and `page=0` (or a `page` that would
/// make `offset` negative) gets page 1 — and "unbounded LIMIT reachable from a
/// query string" is exactly the kind of regression that should fail a test
/// rather than a load test.
///
/// `u32` in, `i64` out: both are non-negative by construction after the clamp,
/// and the widening cast cannot overflow (`u32::MAX as i64` is exact), so the
/// product below cannot wrap the way `u32 * u32` could.
fn events_page_bounds(params: &TeamEventsQueryParams) -> (i64, i64) {
    let page_size = params.page_size.clamp(1, 50) as i64;
    let page = params.page.max(1) as i64;
    (page_size, (page - 1) * page_size)
}

/// Fetch + unwrap the account DEK for a given account_id.
///
/// Rotation-aware (org-tenancy PR 1.3 security review, P1): tries
/// `MASTER_KEK_HEX_NEXT` then `MASTER_KEK_HEX`, exactly like every other
/// unwrap site (`crypto::resolve_dek_for_tenant`) — a plain single-key unwrap
/// here would 500 every team-key operation for the whole rotation window
/// while DEKs are still wrapped under the outgoing key, contradicting the
/// zero-downtime rotation `coralswarm_core::kek`'s module doc promises.
///
/// Plane note (org-tenancy PR 1.6, resolving PR 1.3's P2): this reads via
/// `Store::get_account_dek_wrapped` on the CONTROL plane — deliberately, and
/// now explicitly (call sites pass `require_control_store_resp!(state)`).
/// **PERSONAL-ACCOUNT PATH ONLY** (org-team-membership design doc): an org
/// account's DEK lives on its own TENANT plane instead
/// (`account_keys.dek_wrapped`) — see [`load_org_team_key_dek`] for the
/// org-context sibling this function has no knowledge of and never falls
/// back to.
async fn load_account_dek(
    store: &dyn Store,
    account_id: Uuid,
) -> Result<[u8; 32], (StatusCode, Json<Value>)> {
    let kek = load_kek().map_err(|e| {
        tracing::error!(error = %e, "failed to load master KEK");
        (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": "crypto configuration error"})))
    })?;
    let next_kek = load_kek_next().map_err(|e| {
        tracing::error!(error = %e, "failed to load MASTER_KEK_HEX_NEXT");
        (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": "crypto configuration error"})))
    })?;

    // Same silent-failure class as the authorization reads (see `authz_read!`):
    // this used to be a bare `.unwrap_or(None)`, so a store error and a genuinely
    // unprovisioned account produced the identical 503 "account DEK not
    // provisioned" with nothing in the log. The response stays the same — an
    // unreadable DEK is unusable either way — but the cause is now recorded.
    let wrapped = store
        .get_account_dek_wrapped(account_id)
        .await
        .unwrap_or_else(|e| {
            tracing::error!(error = %e, %account_id, "account DEK read failed; treating as unprovisioned");
            None
        });

    let wrapped = wrapped.ok_or_else(|| (
        StatusCode::SERVICE_UNAVAILABLE,
        Json(json!({"error": "account DEK not provisioned"})),
    ))?;

    unwrap_dek_with_rotation(&kek, next_kek.as_ref(), &wrapped).map_err(|e| {
        tracing::error!(error = %e, %account_id, "DEK unwrap failed");
        (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": "DEK unwrap failed"})))
    })
}

/// Look up which account a user belongs to, then return that account's
/// unwrapped DEK. Rotation-aware for the same reason as [`load_account_dek`].
/// **PERSONAL-ACCOUNT PATH ONLY** — see [`load_account_dek`]'s doc comment.
/// Never called on the org path at all: org `add_member` doesn't wrap a
/// per-member envelope (module doc, "Org key model").
async fn load_user_dek(
    store: &dyn Store,
    user_id: Uuid,
) -> Result<[u8; 32], (StatusCode, Json<Value>)> {
    let kek = load_kek().map_err(|e| {
        tracing::error!(error = %e, "failed to load master KEK");
        (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": "crypto configuration error"})))
    })?;
    let next_kek = load_kek_next().map_err(|e| {
        tracing::error!(error = %e, "failed to load MASTER_KEK_HEX_NEXT");
        (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": "crypto configuration error"})))
    })?;

    // See [`load_account_dek`]'s twin of this read: a store error must not be
    // indistinguishable from "this user has no DEK". Here the distinction also
    // changes what the caller is told to do — 422 tells an admin the TARGET
    // needs provisioning, which is bad advice if the truth is that our database
    // is unreachable — so the log line is the only thing standing between a real
    // outage and a support ticket about the wrong user.
    let wrapped = store
        .get_user_dek_wrapped(user_id)
        .await
        .unwrap_or_else(|e| {
            tracing::error!(error = %e, %user_id, "target user DEK read failed; treating as unprovisioned");
            None
        });

    let wrapped = wrapped.ok_or_else(|| (
        StatusCode::UNPROCESSABLE_ENTITY,
        Json(json!({"error": "target user has no provisioned DEK"})),
    ))?;

    unwrap_dek_with_rotation(&kek, next_kek.as_ref(), &wrapped).map_err(|e| {
        tracing::error!(error = %e, %user_id, "DEK unwrap failed for target user");
        (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": "DEK unwrap failed for target user"})))
    })
}

/// Resolve the ORG account DEK for the team-key wrap step in an org context
/// (org-team-membership design doc decision #3 — see the module doc's "Org
/// key model" section). Reads `account_keys.dek_wrapped` on the ORG's OWN
/// tenant pool via [`crate::crypto::resolve_dek_for_context`] — the same
/// tenant-aware DEK resolver the embed worker and `routes::admin` already use
/// for org content encryption — rather than [`load_account_dek`]'s
/// CONTROL-plane `accounts.dek_wrapped` read, which org accounts never
/// populate (org DEKs were relocated to the tenant plane by org-tenancy
/// PR 1.3).
async fn load_org_team_key_dek(
    ctx: &TenantContext,
) -> Result<[u8; 32], (StatusCode, Json<Value>)> {
    match crate::crypto::resolve_dek_for_context(ctx, ctx.account_id).await {
        Ok(Some(dek)) => Ok(dek),
        Ok(None) => Err((
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error": "org account DEK not provisioned"})),
        )),
        Err(e) => {
            tracing::error!(error = %e, org_id = %ctx.org_id, "org account DEK resolution failed");
            Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "DEK unwrap failed"})),
            ))
        }
    }
}

// ── POST /v1/teams ────────────────────────────────────────────────────────────

/// Create a named team within the caller's account, generating a fresh team key.
///
/// Transaction: graph row → team row → team_keys row → creator envelope → creator membership
/// → `membership_events` `'created'` row.
///
/// Org-capable when `ORG_TEAMS_ENABLED` is on (design doc decision #12: any
/// org member may self-serve create a team ocean; the creator is seeded as
/// its first admin — no extra authorization check is needed here beyond
/// having reached this handler at all, since an org `TenancyDecision` is
/// only ever minted for a gate-verified, LIVE org member — see
/// `middleware::auth::resolve_clerk_context`). The only branch point is the
/// KEY MODEL: see the module doc.
#[utoipa_path(
    post,
    path = "/v1/teams",
    tag = "teams",
    security(("bearer_token" = [])),
    responses(
        (status = 201, description = "Team created", body = Value),
        (status = 403, description = "Org context and ORG_TEAMS_ENABLED is off"),
        (status = 422, description = "Invalid request body"),
        (status = 503, description = "Database not available"),
    )
)]
pub async fn create_team(
    State(state): State<Arc<AppState>>,
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
    Json(req): Json<CreateTeamRequest>,
) -> impl IntoResponse {
    // Settings launch PR 2.8: a human, browser-session action only
    // (`middleware::auth::authority_allowed`).
    if let Some(refusal) = crate::middleware::auth::interactive_session_refusal() {
        return refusal.into_response();
    }
    if let Some(reject) = reject_org_named_teams(&state, &tenant).await {
        return reject;
    }
    let store = require_store_resp!(tenant);

    let name = req.name.trim().to_string();
    if name.is_empty() {
        return (StatusCode::UNPROCESSABLE_ENTITY, Json(json!({"error": "name must not be empty"}))).into_response();
    }

    // ── Org key model branch point (module doc, design doc decision #3) ────
    // Personal: wrap under the CALLER'S account DEK, control-plane
    // (`load_account_dek`) — unchanged from pre-org-teams behavior.
    // Org: wrap under the ORG's account DEK, tenant-plane
    // (`load_org_team_key_dek`) — this becomes the ONE envelope the whole
    // org team shares; per-member envelopes never exist for org teams.
    let caller_dek = if let Some(ctx) = tenant.org_context() {
        // ── Fail closed if the two account ids disagree (CodeRabbit) ────────
        // This handler resolves the team key's wrapping DEK from
        // `ctx.account_id` and then writes the graph/team/team_keys rows under
        // `user.account_id`. The module doc asserts those are the same id for
        // an org request — `middleware::auth::resolve_clerk_context` mints the
        // `TenantContext` from the very account the `UserContext` carries —
        // but NOTHING enforced it, and the failure mode of a future divergence
        // is uniquely bad: the team key would be wrapped under org X's DEK
        // while every row lands in org Y. There is no write-time error to
        // notice, no constraint violated, and no repair path — the org's own
        // DEK cannot unwrap that envelope, so the team ocean is permanently
        // unopenable from the moment it is created. Silent, permanent,
        // undetectable-at-write: exactly the class that must be checked before
        // the write, not discovered after it.
        //
        // Mirrors `crypto::resolve_dek_for_context`'s own internal
        // `CryptoError::TenantMismatch` guard, which refuses the analogous
        // "resolve this account's DEK through a context minted for a different
        // account" and is likewise a caller/wiring bug that maps to 500 rather
        // than a client-triggerable 4xx.
        if ctx.account_id != user.account_id {
            tracing::error!(
                context_account_id = %ctx.account_id,
                user_account_id = %user.account_id,
                org_id = %ctx.org_id,
                user_id = %user.user_id,
                "create_team: tenant context account_id != authenticated account_id — refusing to \
                 wrap a team key under one account's DEK while writing team rows under another \
                 (the team ocean would be permanently unopenable)"
            );
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            )
                .into_response();
        }
        match load_org_team_key_dek(ctx).await {
            Ok(d) => d,
            Err((s, b)) => return (s, b).into_response(),
        }
    } else {
        let control_store = require_control_store_resp!(state);
        match load_account_dek(control_store.as_ref(), user.account_id).await {
            Ok(d) => d,
            Err((s, b)) => return (s, b).into_response(),
        }
    };

    let team_key = generate_team_key();
    // `wrap_team_key_for_member` is a generic AES-GCM wrap of any 32-byte key
    // under any 32-byte DEK — reused as-is for the org path even though its
    // name says "member": the DEK it wraps under here is the ORG account
    // DEK, not any individual member's, so this call produces the org's
    // single shared envelope, not a per-member one.
    let wrapped_for_creator = match wrap_team_key_for_member(&caller_dek, &team_key) {
        Ok(w) => w,
        Err(e) => {
            tracing::error!(error = %e, "team key wrap failed");
            return (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": "key wrap failed"}))).into_response();
        }
    };

    // RLS-scoped to the caller's own account (org-tenancy OPS-4 phase B.5b;
    // see the module doc for why the caller's account is the correct — and
    // only possible — scope here). `create_team_tx` already runs everything in
    // one transaction internally (including the `membership_events` audit
    // row); the scoped twin takes ours instead, so every insert stays atomic
    // exactly as before.
    let created = {
        // `begin_member_scoped`, NOT `begin_account_scoped`: the latter sets
        // `app.account_id` alone, which on the org tenant plane leaves
        // `tenant_rls_actor_user_id()` NULL — and every policy the create
        // sequence writes through resolves the actor from it. With the
        // account-only scope, `create_team` cannot write its own rows no
        // matter what shape those policies take. On Personal / absent-decision
        // requests this is exactly `begin_account_scoped`, so the consumer
        // plane is unchanged (see that method's doc).
        let mut tx = match tenant.begin_member_scoped(user.account_id).await {
            Ok(tx) => tx,
            Err(e) => {
                tracing::error!(error = %e, "create_team: begin_member_scoped failed");
                return (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": "internal error"}))).into_response();
            }
        };
        let created = match store
            .create_team_tx_scoped(&mut tx, user.account_id, user.user_id, &name, &wrapped_for_creator)
            .await
        {
            Ok(c) => c,
            Err(e) => {
                tracing::error!(error = %e, "create_team transaction failed");
                return (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": "internal error"}))).into_response();
            }
        };
        if let Err(e) = tx.commit().await {
            tracing::error!(error = %e, "create_team: scoped commit failed");
            return (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": "internal error"}))).into_response();
        }
        created
    };

    tracing::info!(team_id = %created.team_id, graph_id = %created.graph_id, user_id = %user.user_id, "team created");
    (StatusCode::CREATED, Json(json!({ "team_id": created.team_id, "graph_id": created.graph_id, "name": name }))).into_response()
}

// ── GET /v1/teams/{team_id}/members ───────────────────────────────────────────

/// List the ACTIVE roster of a named team. Readable by any ACTIVE member of
/// that team (they can already see everyone's contributions inside the
/// ocean — seeing who else is in it is not a privilege escalation), OR (org
/// context only) an org admin, same as every other roster endpoint.
///
/// # Identity resolution (control/tenant plane split)
///
/// `team_members` lives on the CONTENT plane — the shared consumer pool for
/// Personal, an org's own tenant pool for Org — but `users.name`/
/// `users.email` are CONTROL-plane columns, and an org tenant database has
/// no `users` table at all (confirmed by grepping `migrations_tenant/` for
/// `CREATE TABLE.*users` — no hits). So a single `team_members JOIN users`
/// is possible for Personal (same physical database) but structurally
/// IMPOSSIBLE for Org (two different Postgres databases, no cross-database
/// joins). Rather than have two different query shapes for the same
/// endpoint depending on plane, this always does two steps, uniformly:
/// [`Store::list_team_members`]/`_scoped` for the roster rows (`user_id`/
/// `role`/`joined_at`, `name`/`email` always `None` from Postgres), then a
/// SECOND, CONTROL-plane query here resolving `user_id → (name, email)` —
/// the exact same two-step pattern [`list_org_members`] already uses for the
/// identical problem (an org tenant DB can't join `account_members` to
/// `users` either). With no pool (`tenant.pool.is_none()`) this second
/// step is skipped and `name`/`email` stay `None`.
#[utoipa_path(
    get,
    path = "/v1/teams/{team_id}/members",
    tag = "teams",
    params(("team_id" = Uuid, Path, description = "Team identifier")),
    security(("bearer_token" = [])),
    responses(
        (status = 200, description = "Active team roster", body = Value),
        (status = 403, description = "Caller is not an active team member or org admin"),
        (status = 404, description = "Team not found"),
        (status = 503, description = "Database not available"),
    )
)]
pub async fn list_team_members(
    State(state): State<Arc<AppState>>,
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
    Path(team_id): Path<Uuid>,
) -> impl IntoResponse {
    if let Some(reject) = reject_org_named_teams(&state, &tenant).await {
        return reject;
    }
    let store = require_store_resp!(tenant);

    let caller_org_admin = if tenant.org_context().is_some() {
        match is_org_admin(&state, user.account_id, user.user_id).await {
            Ok(p) => p,
            Err((s, b)) => return (s, b).into_response(),
        }
    } else {
        None
    };

    // ONE read-only scoped transaction: existence + active-membership check
    // + roster fetch — same shape as `list_team_events`. Carries the
    // governance grant when the caller is a verified org admin, so a
    // non-member admin's reads below resolve rows instead of silently
    // resolving nothing (see the module doc's "Org-admin roster rights").
    let mut scoped_tx = begin_scoped_tx_resp!(tenant, user.account_id, caller_org_admin);
    let team_id = resolve_team_id!(store, scoped_tx, team_id);

    // With the grant set, `teams` is visible to an org admin for every team in
    // the org, so a `false` here is a genuine "no such team" — not the RLS
    // artefact it would be without it (which is how this endpoint used to 404
    // on teams that plainly exist). `set_tenant_org_admin` has already proven
    // the grant is actually in effect against this database, so there is no
    // third possibility left to disambiguate.
    let exists = authz_read!(
        "team_exists",
        scoped_store!(store, scoped_tx, team_exists / team_exists_scoped(team_id))
    );
    if !exists {
        close_read_tx!(scoped_tx);
        return (StatusCode::NOT_FOUND, Json(json!({"error": "team not found"}))).into_response();
    }

    let caller_is_active_member = authz_read!(
        "find_active_team_member_id",
        scoped_store!(
            store,
            scoped_tx,
            find_active_team_member_id / find_active_team_member_id_scoped(team_id, user.user_id)
        )
    )
    .is_some();
    if !caller_is_active_member && caller_org_admin.is_none() {
        close_read_tx!(scoped_tx);
        return (StatusCode::FORBIDDEN, Json(json!({"error": "must be an active team member or org admin to view the roster"}))).into_response();
    }

    let mut roster = match scoped_store!(store, scoped_tx, list_team_members / list_team_members_scoped(team_id)) {
        Ok(r) => r,
        Err(e) => {
            tracing::error!(error = %e, "list_team_members store error");
            close_read_tx!(scoped_tx);
            return (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": "internal error"}))).into_response();
        }
    };
    close_read_tx!(scoped_tx);

    // Identity resolution — see this handler's own doc comment.
    if tenant.pool.is_some() && !roster.is_empty() {
        let user_ids: Vec<Uuid> = roster.iter().map(|m| m.user_id).collect();
        let pool = match state.control_db() {
            Some(p) => p,
            None => {
                return (
                    StatusCode::SERVICE_UNAVAILABLE,
                    Json(json!({"error": "service unavailable"})),
                )
                    .into_response()
            }
        };
        let identity_rows: Result<Vec<(Uuid, Option<String>, String)>, sqlx::Error> = sqlx::query_as(
            "SELECT id, name, email FROM users WHERE id = ANY($1)",
        )
        .bind(&user_ids)
        .fetch_all(crate::tenancy::scope::request_read(pool))
        .await;
        let identity_rows = match identity_rows {
            Ok(r) => r,
            Err(e) => {
                tracing::error!(error = %e, %team_id, "list_team_members identity resolution failed");
                return (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": "internal error"}))).into_response();
            }
        };
        let identities: std::collections::HashMap<Uuid, (Option<String>, String)> = identity_rows
            .into_iter()
            .map(|(id, name, email)| (id, (name, email)))
            .collect();
        for member in roster.iter_mut() {
            if let Some((name, email)) = identities.get(&member.user_id) {
                member.name = name.clone();
                member.email = Some(email.clone());
            }
        }
    }

    let out: Vec<Value> = roster
        .into_iter()
        .map(|m| {
            json!({
                "user_id":   m.user_id,
                "name":      m.name,
                "email":     m.email,
                "role":      m.role,
                "joined_at": m.joined_at,
            })
        })
        .collect();

    (StatusCode::OK, Json(json!(out))).into_response()
}

// ── POST /v1/teams/{team_id}/members ─────────────────────────────────────────

/// Add a member to a named team. Caller must be a team admin, OR (org
/// context only) an org admin — see the module doc's "Org-admin roster
/// rights" section.
///
/// PERSONAL path: unwraps caller's envelope to get the raw TK, re-wraps it
/// for the new member (unchanged from before this design). ORG path: a pure
/// `team_members` insert, no envelope at all — see the module doc's "Org key
/// model" section.
///
/// Both paths now also enforce the membership-subset invariant (design doc
/// decision #10): the target must already be a live `account_members` row
/// for the caller's account.
#[utoipa_path(
    post,
    path = "/v1/teams/{team_id}/members",
    tag = "teams",
    params(("team_id" = Uuid, Path, description = "Team identifier")),
    security(("bearer_token" = [])),
    responses(
        (status = 201, description = "Member added", body = Value),
        (status = 403, description = "Caller is not a team admin or org admin"),
        (status = 404, description = "Team not found"),
        (status = 409, description = "User already an active member"),
        (status = 422, description = "Invalid role, or target not a member of this account"),
        (status = 503, description = "Database not available"),
    )
)]
pub async fn add_member(
    State(state): State<Arc<AppState>>,
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
    Path(team_id): Path<Uuid>,
    Json(req): Json<AddMemberRequest>,
) -> impl IntoResponse {
    // Settings launch PR 2.8: a human, browser-session action only
    // (`middleware::auth::authority_allowed`).
    if let Some(refusal) = crate::middleware::auth::interactive_session_refusal() {
        return refusal.into_response();
    }
    if let Some(reject) = reject_org_named_teams(&state, &tenant).await {
        return reject;
    }
    let store = require_store_resp!(tenant);
    let control_store = require_control_store_resp!(state);

    let role = req.role.trim().to_lowercase();
    if role != "admin" && role != "member" {
        return (StatusCode::UNPROCESSABLE_ENTITY, Json(json!({"error": "role must be 'admin' or 'member'"}))).into_response();
    }

    // ── Control-plane pre-reads, OUTSIDE any tenant-scoped transaction ──────
    // (module doc: DEK/control-plane reads never sit inside a scoped tx).
    // Resolved once, up front, then simply branched on below — same
    // rationale as `create_team`'s DEK read sandwiched between transactions.
    let caller_org_admin = match is_org_admin(&state, user.account_id, user.user_id).await {
        Ok(p) => p,
        Err((s, b)) => return (s, b).into_response(),
    };
    // Membership-subset invariant (design doc #10): team membership can
    // never exceed account/org membership. Checked for BOTH personal and org
    // callers, once, up front — applied inside each branch below.
    let target_is_account_member = match control_store.is_account_member(user.account_id, req.user_id).await {
        Ok(b) => b,
        Err(e) => {
            tracing::error!(error = %e, "add_member: target account-membership check failed");
            return (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": "internal error"}))).into_response();
        }
    };

    if tenant.org_context().is_some() {
        // ══════════════════════════ ORG PATH ═══════════════════════════════
        // No per-member envelope (module doc, design doc #3) — a pure
        // roster insert. No control-plane crypto is sandwiched in the
        // middle the way the personal path below has, so (unlike personal)
        // checks + write share ONE scoped transaction, the same shape as
        // `remove_team_member`.
        // Carries the governance grant for a verified org admin: without it
        // the `FOR UPDATE` row lock immediately below cannot even SEE a team
        // the admin is not a member of, and this whole branch — the one the
        // design's decision #4 exists for — 404s.
        let mut scoped_tx = begin_scoped_tx_resp!(tenant, user.account_id, caller_org_admin);
        let team_id = resolve_team_id!(store, scoped_tx, team_id);

        // Race fix (design doc #9/defect #1) — first statement.
        lock_team_row_or_return!(store, scoped_tx, team_id);

        let caller_is_team_admin = authz_read!(
            "find_team_admin_id",
            scoped_store!(
                store,
                scoped_tx,
                find_team_admin_id / find_team_admin_id_scoped(team_id, user.user_id)
            )
        )
        .is_some();
        if !caller_is_team_admin && caller_org_admin.is_none() {
            return (StatusCode::FORBIDDEN, Json(json!({"error": "must be a team admin or org admin to add members"}))).into_response();
        }

        let exists = authz_read!(
            "team_exists",
            scoped_store!(store, scoped_tx, team_exists / team_exists_scoped(team_id))
        );
        if !exists {
            return (StatusCode::NOT_FOUND, Json(json!({"error": "team not found"}))).into_response();
        }

        let already_member = authz_read!(
            "find_active_team_member_id",
            scoped_store!(
                store,
                scoped_tx,
                find_active_team_member_id / find_active_team_member_id_scoped(team_id, req.user_id)
            )
        );
        if already_member.is_some() {
            return (StatusCode::CONFLICT, Json(json!({"error": "user is already an active team member"}))).into_response();
        }

        if !target_is_account_member {
            return (StatusCode::UNPROCESSABLE_ENTITY, Json(json!({"error": "target user is not a member of this organization"}))).into_response();
        }

        let added = match scoped_store!(
            store,
            scoped_tx,
            add_team_member_tx_no_envelope
                / add_team_member_tx_no_envelope_scoped(team_id, req.user_id, &role, user.user_id)
        ) {
            Ok(a) => a,
            Err(e) => {
                tracing::error!(error = %e, "add_team_member_tx_no_envelope failed");
                return (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": "internal error"}))).into_response();
            }
        };
        commit_write_tx!(scoped_tx);

        tracing::info!(%team_id, member_id = %added.member_id, new_user_id = %req.user_id, added_by = %user.user_id, %role, org = true, "team member added (org, no envelope)");
        return (StatusCode::CREATED, Json(json!({ "member_id": added.member_id, "user_id": req.user_id, "role": role }))).into_response();
    }

    // ══════════════════════════ PERSONAL PATH (unchanged shape) ═══════════════
    // First of TWO scoped transactions (org-tenancy OPS-4 phase B.5b): the
    // four pre-checks. It closes before the CONTROL-plane DEK reads + key
    // crypto below, so no content-pool connection is pinned across them; the
    // write then takes its own scoped transaction (module doc).
    // No governance grant on this path: `is_org_admin` can only ever be true
    // for an account whose `clerk_org_id IS NOT NULL`, and such an account's
    // teams are served by the ORG branch above. Passing the proof here would
    // be inert anyway (`begin_governance_scoped` sets the GUC only under a
    // `TenancyDecision::Org`), but not passing it says so at the call site.
    let mut scoped_tx = begin_scoped_tx_resp!(tenant, user.account_id);
    let team_id = resolve_team_id!(store, scoped_tx, team_id);

    let is_admin = authz_read!(
        "find_team_admin_id",
        scoped_store!(store, scoped_tx, find_team_admin_id / find_team_admin_id_scoped(team_id, user.user_id))
    );
    if is_admin.is_none() {
        return (StatusCode::FORBIDDEN, Json(json!({"error": "must be a team admin to add members"}))).into_response();
    }

    let exists = authz_read!(
        "team_exists",
        scoped_store!(store, scoped_tx, team_exists / team_exists_scoped(team_id))
    );
    if !exists {
        return (StatusCode::NOT_FOUND, Json(json!({"error": "team not found"}))).into_response();
    }

    let already_member = authz_read!(
        "find_active_team_member_id",
        scoped_store!(
            store,
            scoped_tx,
            find_active_team_member_id / find_active_team_member_id_scoped(team_id, req.user_id)
        )
    );
    if already_member.is_some() {
        return (StatusCode::CONFLICT, Json(json!({"error": "user is already an active team member"}))).into_response();
    }

    if !target_is_account_member {
        return (StatusCode::UNPROCESSABLE_ENTITY, Json(json!({"error": "target user is not a member of this account"}))).into_response();
    }

    let envelope = authz_read!(
        "get_caller_key_envelope",
        scoped_store!(
            store,
            scoped_tx,
            get_caller_key_envelope / get_caller_key_envelope_scoped(team_id, user.user_id)
        )
    );
    close_read_tx!(scoped_tx);
    let envelope = match envelope {
        Some(e) => e,
        None => {
            tracing::error!(%team_id, user_id = %user.user_id, "no key envelope found for caller");
            return (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": "no team key envelope found for caller"}))).into_response();
        }
    };

    // CONTROL-plane DEK reads — see `create_team`'s comment.
    let caller_dek = match load_account_dek(control_store.as_ref(), user.account_id).await {
        Ok(d) => d,
        Err((s, b)) => return (s, b).into_response(),
    };

    let raw_team_key = match unwrap_team_key(&caller_dek, &envelope.wrapped_key) {
        Ok(k) => k,
        Err(e) => {
            tracing::error!(error = %e, %team_id, user_id = %user.user_id, "team key unwrap failed");
            return (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": "team key unwrap failed"}))).into_response();
        }
    };

    let new_member_dek = match load_user_dek(control_store.as_ref(), req.user_id).await {
        Ok(d) => d,
        Err((s, b)) => return (s, b).into_response(),
    };

    let wrapped_for_new_member = match wrap_team_key_for_member(&new_member_dek, &raw_team_key) {
        Ok(w) => w,
        Err(e) => {
            tracing::error!(error = %e, "team key re-wrap failed for new member");
            return (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": "key wrap failed for new member"}))).into_response();
        }
    };

    // Second scoped transaction: the write. `add_team_member_tx` already runs
    // its membership + envelope + audit-event inserts in one transaction
    // internally; the scoped twin takes ours, so they stay atomic exactly as
    // before.
    let mut scoped_tx = begin_scoped_tx_resp!(tenant, user.account_id);
    // Race fix (design doc #9/defect #1) — first statement of the write tx.
    lock_team_row_or_return!(store, scoped_tx, team_id);
    let added = match scoped_store!(
        store,
        scoped_tx,
        add_team_member_tx
            / add_team_member_tx_scoped(
                team_id,
                envelope.team_key_id,
                req.user_id,
                &role,
                &wrapped_for_new_member,
                user.user_id,
            )
    ) {
        Ok(a) => a,
        Err(e) => {
            tracing::error!(error = %e, "add_team_member transaction failed");
            return (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": "internal error"}))).into_response();
        }
    };
    commit_write_tx!(scoped_tx);

    tracing::info!(%team_id, member_id = %added.member_id, new_user_id = %req.user_id, added_by = %user.user_id, %role, "team member added");
    (StatusCode::CREATED, Json(json!({ "member_id": added.member_id, "user_id": req.user_id, "role": role }))).into_response()
}

// ── PATCH /v1/teams/{team_id}/members/{user_id} ──────────────────────────────

/// Assign a team member's role. Caller must be a team admin, OR (org context
/// only) an org admin. Idempotent when the role isn't actually changing —
/// succeeds without writing an audit event (design doc #3 of the new-
/// endpoint requirements).
#[utoipa_path(
    patch,
    path = "/v1/teams/{team_id}/members/{user_id}",
    tag = "teams",
    params(
        ("team_id" = Uuid, Path, description = "Team identifier"),
        ("user_id" = Uuid, Path, description = "Member whose role is changing"),
    ),
    security(("bearer_token" = [])),
    responses(
        (status = 200, description = "Role updated (or already at that role)", body = Value),
        (status = 403, description = "Caller is not a team admin or org admin"),
        (status = 404, description = "Member not found or not active"),
        (status = 409, description = "Would demote the last admin"),
        (status = 422, description = "Invalid role value"),
        (status = 503, description = "Database not available"),
    )
)]
pub async fn assign_team_member_role(
    State(state): State<Arc<AppState>>,
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
    Path((team_id, target_user_id)): Path<(Uuid, Uuid)>,
    Json(req): Json<AssignRoleRequest>,
) -> impl IntoResponse {
    // Settings launch PR 2.8: a human, browser-session action only
    // (`middleware::auth::authority_allowed`).
    if let Some(refusal) = crate::middleware::auth::interactive_session_refusal() {
        return refusal.into_response();
    }
    if let Some(reject) = reject_org_named_teams(&state, &tenant).await {
        return reject;
    }
    let store = require_store_resp!(tenant);

    let new_role = req.role.trim().to_lowercase();
    if new_role != "admin" && new_role != "member" {
        return (StatusCode::UNPROCESSABLE_ENTITY, Json(json!({"error": "role must be 'admin' or 'member'"}))).into_response();
    }

    let caller_org_admin = if tenant.org_context().is_some() {
        match is_org_admin(&state, user.account_id, user.user_id).await {
            Ok(p) => p,
            Err((s, b)) => return (s, b).into_response(),
        }
    } else {
        None
    };

    // ONE scoped transaction for the whole handler — same shape as
    // `remove_team_member`: no control-plane crypto sits between the checks
    // and the write here. Carries the governance grant when the caller is a
    // verified org admin (module doc, "Org-admin roster rights").
    let mut scoped_tx = begin_scoped_tx_resp!(tenant, user.account_id, caller_org_admin);
    let team_id = resolve_team_id!(store, scoped_tx, team_id);

    // Race fix (design doc #9/defect #1) — first statement, before the
    // admin-count read below.
    lock_team_row_or_return!(store, scoped_tx, team_id);

    let is_team_admin = authz_read!(
        "find_team_admin_id",
        scoped_store!(store, scoped_tx, find_team_admin_id / find_team_admin_id_scoped(team_id, user.user_id))
    )
    .is_some();
    if !is_team_admin && caller_org_admin.is_none() {
        return (StatusCode::FORBIDDEN, Json(json!({"error": "must be a team admin or org admin to change member roles"}))).into_response();
    }

    let target = authz_read!(
        "find_active_team_member",
        scoped_store!(
            store,
            scoped_tx,
            find_active_team_member / find_active_team_member_scoped(team_id, target_user_id)
        )
    );
    let (target_member_id, old_role) = match target {
        Some(r) => r,
        None => return (StatusCode::NOT_FOUND, Json(json!({"error": "member not found or not active"}))).into_response(),
    };

    if old_role == new_role {
        // Idempotent no-op: nothing changed, so no `membership_events` row is
        // written — writing one would fabricate a role change that never
        // happened.
        close_read_tx!(scoped_tx);
        return (
            StatusCode::OK,
            Json(json!({ "member_id": target_member_id, "user_id": target_user_id, "role": new_role, "changed": false })),
        )
            .into_response();
    }

    if old_role == "admin" && new_role == "member" {
        // `.unwrap_or(0)` here was the worst of the silent-failure set: a store
        // error became "this team has zero admins", which the guard below reads
        // as "you are demoting the last one" and answers 409 — a 200-shaped lie
        // about the team's state, with no log line at all.
        let admin_count = authz_read!(
            "count_team_admins",
            scoped_store!(store, scoped_tx, count_team_admins / count_team_admins_scoped(team_id))
        );
        if admin_count <= 1 {
            return (StatusCode::CONFLICT, Json(json!({"error": "cannot demote the last admin — assign another admin first"}))).into_response();
        }
    }

    match scoped_store!(
        store,
        scoped_tx,
        update_team_member_role_tx
            / update_team_member_role_tx_scoped(team_id, target_member_id, target_user_id, user.user_id, &old_role, &new_role)
    ) {
        // `update_team_member_role_tx` now returns rows-affected rather than
        // `()`, so the audit row is written only when a role genuinely changed
        // (CodeRabbit: it used to INSERT a `role_changed` event even for an
        // UPDATE that matched nothing, fabricating history in the one table
        // whose whole purpose is to be trustworthy). Mirrors
        // `soft_remove_team_member`'s existing arms a few dozen lines below.
        //
        // `target_member_id` was resolved by `find_active_team_member` earlier
        // in THIS transaction, and the handler holds the `teams` row lock, so
        // 0 rows means the member row was removed by a path that does not take
        // that lock. 404 matches what the removal handler answers for the same
        // situation.
        Ok(1) => {}
        Ok(_) => {
            tracing::warn!(%team_id, %target_user_id, "assign role: member row vanished mid-transaction");
            return (StatusCode::NOT_FOUND, Json(json!({"error": "member not found or already departed"}))).into_response();
        }
        Err(e) => {
            tracing::error!(error = %e, "update_team_member_role_tx failed");
            return (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": "internal error"}))).into_response();
        }
    }
    commit_write_tx!(scoped_tx);

    tracing::info!(%team_id, %target_user_id, changed_by = %user.user_id, %old_role, %new_role, "team member role changed");
    (
        StatusCode::OK,
        Json(json!({ "member_id": target_member_id, "user_id": target_user_id, "role": new_role, "changed": true })),
    )
        .into_response()
}

// ── DELETE /v1/teams/{team_id}/members/{user_id} ─────────────────────────────

/// Soft-remove a member from a named team (sets departing_at). Caller must
/// be a team admin, OR (org context only) an org admin — OR the TARGET
/// themselves: `user_id == caller` (leaving a workspace one was added to) is
/// permitted with NO admin rights at all (team-ocean lifecycle management,
/// "leave a workspace" item). Every other rule still applies to a self-leave
/// exactly as it would to an admin-initiated removal, in particular the
/// last-admin guard just below — the sole admin of a team may not leave it
/// either, and gets the same 409 telling them to promote someone first
/// (there being no OTHER admin to do the promoting is precisely why the
/// guard exists; whether the departing person is that admin or someone else
/// removing them does not change the invariant it protects). TK rotation is
/// deferred (CS-104) — unaffected by this design.
#[utoipa_path(
    delete,
    path = "/v1/teams/{team_id}/members/{user_id}",
    tag = "teams",
    params(
        ("team_id" = Uuid, Path, description = "Team identifier"),
        ("user_id" = Uuid, Path, description = "User to remove (or the caller themselves, to leave)"),
    ),
    security(("bearer_token" = [])),
    responses(
        (status = 204, description = "Member removed, or the caller left"),
        (status = 403, description = "Caller is not a team admin, org admin, or the target themselves"),
        (status = 404, description = "Member not found"),
        (status = 409, description = "Cannot remove (or leave as) the last admin"),
        (status = 503, description = "Database not available"),
    )
)]
pub async fn remove_team_member(
    State(state): State<Arc<AppState>>,
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
    Path((team_id, target_user_id)): Path<(Uuid, Uuid)>,
) -> impl IntoResponse {
    // Settings launch PR 2.8: a human, browser-session action only
    // (`middleware::auth::authority_allowed`).
    if let Some(refusal) = crate::middleware::auth::interactive_session_refusal() {
        return refusal.into_response();
    }
    if let Some(reject) = reject_org_named_teams(&state, &tenant).await {
        return reject;
    }
    let store = require_store_resp!(tenant);

    // A member removing THEMSELVES is "leaving", not a roster write, and
    // needs no admin rights at all — computed once, up front, and consulted
    // both by the authorization check below and by the audit `action` value
    // passed to the store (`'left'` vs `'removed'` — see
    // `Store::soft_remove_team_member`'s doc comment for why these are
    // deliberately distinct values rather than one reused for both).
    let is_self_removal = target_user_id == user.user_id;

    let caller_org_admin = if tenant.org_context().is_some() {
        match is_org_admin(&state, user.account_id, user.user_id).await {
            Ok(p) => p,
            Err((s, b)) => return (s, b).into_response(),
        }
    } else {
        None
    };

    // ONE scoped transaction for the whole handler (org-tenancy OPS-4 phase
    // B.5b): unlike `add_member` there is no crypto/control-plane work between
    // the checks and the write, so nothing is held across foreign I/O. Carries
    // the governance grant when the caller is a verified org admin (module
    // doc, "Org-admin roster rights").
    let mut scoped_tx = begin_scoped_tx_resp!(tenant, user.account_id, caller_org_admin);
    let team_id = resolve_team_id!(store, scoped_tx, team_id);

    // Race fix (design doc #9/defect #1): lock BEFORE the admin-count read
    // below — this is the fix for the exact race the design doc calls out
    // (two concurrent removals of a team's two remaining admins both passing
    // the "not the last admin" guard).
    lock_team_row_or_return!(store, scoped_tx, team_id);

    let is_admin = authz_read!(
        "find_team_admin_id",
        scoped_store!(store, scoped_tx, find_team_admin_id / find_team_admin_id_scoped(team_id, user.user_id))
    );
    if is_admin.is_none() && caller_org_admin.is_none() && !is_self_removal {
        return (StatusCode::FORBIDDEN, Json(json!({"error": "must be a team admin, org admin, or the member themselves to remove them"}))).into_response();
    }

    let target = authz_read!(
        "find_active_team_member",
        scoped_store!(
            store,
            scoped_tx,
            find_active_team_member / find_active_team_member_scoped(team_id, target_user_id)
        )
    );
    let (target_member_id, target_role) = match target {
        Some(r) => r,
        None => return (StatusCode::NOT_FOUND, Json(json!({"error": "member not found or already departed"}))).into_response(),
    };

    if target_role == "admin" {
        // See `assign_team_member_role`'s twin of this read for why a store
        // error must not be allowed to masquerade as "zero admins" here.
        // Applies identically whether the sole admin is being removed by
        // someone else or is leaving on their own — see this handler's own
        // doc comment.
        let admin_count = authz_read!(
            "count_team_admins",
            scoped_store!(store, scoped_tx, count_team_admins / count_team_admins_scoped(team_id))
        );
        if admin_count <= 1 {
            return (StatusCode::CONFLICT, Json(json!({"error": "cannot remove the last admin — promote another member first"}))).into_response();
        }
    }

    let action = if is_self_removal { "left" } else { "removed" };

    match scoped_store!(
        store,
        scoped_tx,
        soft_remove_team_member
            / soft_remove_team_member_scoped(target_member_id, team_id, user.user_id, target_user_id, &target_role, action)
    ) {
        Ok(1) => {}
        Ok(_) => return (StatusCode::NOT_FOUND, Json(json!({"error": "member not found"}))).into_response(),
        Err(e) => {
            tracing::error!(error = %e, "failed to soft-delete team member");
            return (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": "internal error"}))).into_response();
        }
    }
    commit_write_tx!(scoped_tx);

    pause_github_watches_for_departed_member(&tenant, user.account_id, team_id, target_user_id).await;

    tracing::info!(%team_id, removed_user_id = %target_user_id, removed_by = %user.user_id, %action, "team member soft-removed (TK rotation pending)");
    StatusCode::NO_CONTENT.into_response()
}

/// The removed member can no longer reach this team's ocean, so any GitHub
/// repo they routed INTO it must stop syncing.
///
/// Without this, nothing reacts to the roster change: a watch row is chosen
/// once at selection time and keeps its `graph_id` forever, so the backfill
/// would go on being re-enqueued and go on failing — three attempts, dead,
/// re-enqueued by the next selection save, indefinitely. The content itself
/// is never at risk (`connectors::github_backfill::destination_reachable`
/// refuses every write independently, and is what actually enforces the
/// boundary); this is what makes the failure TERMINAL and visible instead of
/// a silent retry loop.
///
/// Called AFTER the roster transaction commits, deliberately: the pause is a
/// reaction to a removal that has already happened, and folding it into that
/// transaction would let a `github_watched_repos` problem roll back a
/// successful roster write.
///
/// Best-effort throughout. A failure here never turns a successful removal
/// into a 500 — the write gate still refuses, and the backfill's own
/// reachability check pauses the row on its next tick as the backstop.
///
/// A `Tenant` with no pool has nothing to pause, hence the `tenant.pool`
/// guard rather than an unwrap.
async fn pause_github_watches_for_departed_member(
    tenant: &Tenant,
    account_id: Uuid,
    team_id: Uuid,
    departed_user_id: Uuid,
) {
    let Some(pool) = tenant.pool.as_ref() else { return };

    // `teams.graph_id` is the ocean this team owns — the destination a
    // `RequestedDestination::ExistingTeam`/`NewTeam` pick resolves to
    // (`connectors::github_repos::resolve_destination`). Read on the same
    // content pool the watch rows live on.
    let graph_id: Option<Uuid> = match crate::tenancy::scope::begin_tenant_system_scoped(pool, account_id).await {
        Ok(mut tx) => {
            let found = sqlx::query_scalar::<_, Uuid>("SELECT graph_id FROM teams WHERE id = $1 AND account_id = $2")
                .bind(team_id)
                .bind(account_id)
                .fetch_optional(&mut *tx)
                .await;
            tx.rollback().await.ok();
            match found {
                Ok(g) => g,
                Err(e) => {
                    tracing::warn!(
                        error = %e, %team_id,
                        "team member removed: could not load the team's ocean to pause GitHub watches"
                    );
                    return;
                }
            }
        }
        Err(e) => {
            tracing::warn!(
                error = %e, %team_id,
                "team member removed: could not open a scoped tx to pause GitHub watches"
            );
            return;
        }
    };
    let Some(graph_id) = graph_id else { return };

    match crate::connectors::github_disconnect::pause_for_unreachable_destination(
        pool,
        account_id,
        departed_user_id,
        graph_id,
    )
    .await
    {
        Ok(0) => {}
        Ok(paused) => tracing::info!(
            %team_id, %graph_id, departed_user_id = %departed_user_id, paused,
            "team member removed: paused their GitHub watches pointed at this team's ocean"
        ),
        Err(e) => tracing::warn!(
            error = %e, %team_id, departed_user_id = %departed_user_id,
            "team member removed: could not pause GitHub watches for the now-unreachable team ocean \
             — the write gate still refuses, and the backfill pauses the row on its next tick"
        ),
    }
}

// ── PATCH /v1/teams/{team_id} ─────────────────────────────────────────────────

/// Rename a named team (team-ocean lifecycle management). Updates BOTH
/// `teams.name` and the team's ocean's display name (`graphs.name`) — see
/// [`crate::store::Store::rename_team`]'s doc comment for why the two track
/// each other ([`create_team`] seeds both from the same argument at creation
/// time, and this is the one other place either can change). Caller must be
/// a team admin, OR (org context only) an org admin — same authorization,
/// and the same `SELECT ... FOR UPDATE` row lock, as every other lifecycle
/// write in this file (design doc #9).
///
/// Idempotent when the (trimmed) name is unchanged: the response reports
/// `"changed": false` and no `membership_events` row is written in that
/// case — the "audit only real mutations" discipline
/// [`assign_team_member_role`] already follows for roles, applied here to a
/// rename instead.
#[utoipa_path(
    patch,
    path = "/v1/teams/{team_id}",
    tag = "teams",
    params(("team_id" = Uuid, Path, description = "Team identifier")),
    request_body = RenameTeamRequest,
    security(("bearer_token" = [])),
    responses(
        (status = 200, description = "Renamed (or already at that name)", body = Value),
        (status = 403, description = "Caller is not a team admin or org admin"),
        (status = 404, description = "Team not found"),
        (status = 422, description = "Invalid name (empty after trim, or too long)"),
        (status = 503, description = "Database not available"),
    )
)]
pub async fn rename_team(
    State(state): State<Arc<AppState>>,
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
    Path(team_id): Path<Uuid>,
    Json(req): Json<RenameTeamRequest>,
) -> impl IntoResponse {
    // Settings launch PR 2.8: a human, browser-session action only
    // (`middleware::auth::authority_allowed`).
    if let Some(refusal) = crate::middleware::auth::interactive_session_refusal() {
        return refusal.into_response();
    }
    if let Some(reject) = reject_org_named_teams(&state, &tenant).await {
        return reject;
    }
    let store = require_store_resp!(tenant);

    let new_name = match validate_team_name(&req.name) {
        Ok(n) => n,
        Err(msg) => return (StatusCode::UNPROCESSABLE_ENTITY, Json(json!({"error": msg}))).into_response(),
    };

    let caller_org_admin = if tenant.org_context().is_some() {
        match is_org_admin(&state, user.account_id, user.user_id).await {
            Ok(p) => p,
            Err((s, b)) => return (s, b).into_response(),
        }
    } else {
        None
    };

    // ONE scoped transaction for the whole handler — same shape as
    // `assign_team_member_role`/`remove_team_member`: no control-plane
    // crypto sits between the checks and the write here.
    let mut scoped_tx = begin_scoped_tx_resp!(tenant, user.account_id, caller_org_admin);
    let team_id = resolve_team_id!(store, scoped_tx, team_id);

    // Race fix / consistency with the other lifecycle writes (design doc
    // #9): same row lock, same first-statement placement, which is also
    // this handler's existence check (404 on `Ok(false)`).
    lock_team_row_or_return!(store, scoped_tx, team_id);

    let is_team_admin = authz_read!(
        "find_team_admin_id",
        scoped_store!(store, scoped_tx, find_team_admin_id / find_team_admin_id_scoped(team_id, user.user_id))
    )
    .is_some();
    if !is_team_admin && caller_org_admin.is_none() {
        return (StatusCode::FORBIDDEN, Json(json!({"error": "must be a team admin or org admin to rename this workspace"}))).into_response();
    }

    let rows = match scoped_store!(
        store,
        scoped_tx,
        rename_team / rename_team_scoped(team_id, &new_name, user.user_id)
    ) {
        Ok(n) => n,
        Err(e) => {
            tracing::error!(error = %e, "rename_team store error");
            return (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": "internal error"}))).into_response();
        }
    };
    commit_write_tx!(scoped_tx);

    tracing::info!(%team_id, renamed_by = %user.user_id, changed = rows > 0, "team renamed");
    (
        StatusCode::OK,
        Json(json!({ "team_id": team_id, "name": new_name, "changed": rows > 0 })),
    )
        .into_response()
}

// ── DELETE /v1/teams/{team_id} ────────────────────────────────────────────────

/// Archive a team ocean (team-ocean lifecycle management) — **archive, NOT
/// destroy.** Stamps `graphs.archived_at = now()` on the team's underlying
/// ocean and touches nothing else.
///
/// That alone is sufficient to remove the workspace from every caller's
/// reachable set: the tenant reachability function
/// (`backend/migrations_tenant/0006_member_ocean_reachability_functions.sql`)
/// and its control-plane twin (`migrations_control/0091`) both already carry
/// `AND g.archived_at IS NULL` as a TOP-LEVEL condition applying to EVERY
/// ocean kind, team included — the single authorization point ~41 call
/// sites across this codebase already trust
/// ([`crate::tenancy::reachability`]). No predicate changed and no new
/// authorization surface was added anywhere in this PR to make archiving
/// work; it works by construction of reachability logic that already
/// existed.
///
/// **Does NOT delete atoms, nodes, reefs, team keys, team-member key
/// envelopes, seat grants, or R2 blobs — deliberately, and this is NOT a gap
/// to be "completed" by a follow-up inside this handler.** A hard purge
/// belongs in its own future purge arm, with its own flag and its own
/// retention window, mirroring how `member_ocean_reconciler`'s purge arm
/// treats archived member oceans — a separate, later, and (until it runs)
/// reversible step. If you are reading this while adding content deletion
/// here: don't; add a new, explicitly-flagged purge path instead, and read
/// the member-ocean reconciler's purge arm first for the shape that
/// decision already took once in this codebase.
///
/// Authorization: team admin OR org admin, same as every other lifecycle
/// write, with the same `SELECT ... FOR UPDATE` row lock (design doc #9) —
/// which also serves as this handler's existence check (404 on `Ok(false)`).
/// Idempotent: archiving an already-archived workspace answers 204 without
/// error and without a second audit row (see
/// [`crate::store::Store::archive_team`]'s doc comment).
///
/// See [`restore_team`] for the small, additive unarchive counterpart this
/// PR also adds.
#[utoipa_path(
    delete,
    path = "/v1/teams/{team_id}",
    tag = "teams",
    params(("team_id" = Uuid, Path, description = "Team identifier")),
    security(("bearer_token" = [])),
    responses(
        (status = 204, description = "Archived (idempotent — already-archived is not an error)"),
        (status = 403, description = "Caller is not a team admin or org admin"),
        (status = 404, description = "Team not found"),
        (status = 503, description = "Database not available"),
    )
)]
pub async fn archive_team(
    State(state): State<Arc<AppState>>,
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
    Path(team_id): Path<Uuid>,
) -> impl IntoResponse {
    // Settings launch PR 2.8: a human, browser-session action only
    // (`middleware::auth::authority_allowed`).
    if let Some(refusal) = crate::middleware::auth::interactive_session_refusal() {
        return refusal.into_response();
    }
    if let Some(reject) = reject_org_named_teams(&state, &tenant).await {
        return reject;
    }
    let store = require_store_resp!(tenant);

    let caller_org_admin = if tenant.org_context().is_some() {
        match is_org_admin(&state, user.account_id, user.user_id).await {
            Ok(p) => p,
            Err((s, b)) => return (s, b).into_response(),
        }
    } else {
        None
    };

    let mut scoped_tx = begin_scoped_tx_resp!(tenant, user.account_id, caller_org_admin);
    let team_id = resolve_team_id!(store, scoped_tx, team_id);

    // Same row lock every other lifecycle write takes, for the same reason
    // (design doc #9) — also this handler's existence check.
    lock_team_row_or_return!(store, scoped_tx, team_id);

    let is_team_admin = authz_read!(
        "find_team_admin_id",
        scoped_store!(store, scoped_tx, find_team_admin_id / find_team_admin_id_scoped(team_id, user.user_id))
    )
    .is_some();
    if !is_team_admin && caller_org_admin.is_none() {
        return (StatusCode::FORBIDDEN, Json(json!({"error": "must be a team admin or org admin to archive this workspace"}))).into_response();
    }

    if let Err(e) = scoped_store!(store, scoped_tx, archive_team / archive_team_scoped(team_id, user.user_id)) {
        tracing::error!(error = %e, "archive_team store error");
        return (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": "internal error"}))).into_response();
    }
    commit_write_tx!(scoped_tx);

    tracing::info!(%team_id, archived_by = %user.user_id, "team ocean archived (content retained — see this handler's doc comment)");
    StatusCode::NO_CONTENT.into_response()
}

// ── POST /v1/teams/{team_id}/restore ──────────────────────────────────────────

/// Restore (unarchive) a team ocean (team-ocean lifecycle management) —
/// clears `graphs.archived_at`. The small counterpart to [`archive_team`]
/// that makes archiving a genuinely reversible action rather than a one-way
/// door.
///
/// Authorization is the SAME team-admin-or-org-admin check [`archive_team`]
/// uses, re-evaluated LIVE (never cached) against the CURRENT roster — this
/// is exactly what "must refuse to restore a team whose account no longer
/// has the caller as admin" falls out of, for free: if the caller was
/// demoted, removed from the team, or had their org admin role revoked
/// between the archive and this call, [`crate::store::Store::find_team_admin_id`]
/// / [`is_org_admin`] simply no longer find them, and this answers 403
/// exactly as any other lifecycle write would. No separate "were you the one
/// who archived it" check exists or is needed — admin status is what gates
/// every other lifecycle write, and there is no reason restore should be
/// gated on archiver identity instead.
///
/// Idempotent, same shape as [`archive_team`]: restoring a workspace that is
/// not currently archived answers 200 without error and without a second
/// audit row.
///
/// # The 120-day restore window
///
/// Refuses with **409** once more than [`TEAM_OCEAN_RESTORE_WINDOW_DAYS`]
/// have passed since `archived_at` — see [`team_restore_window`]. Inside the
/// window this endpoint's behavior is unchanged from before the window
/// existed: same authorization, same idempotency, same response shape. The
/// check only runs when the ocean IS currently archived
/// ([`crate::store::Store::team_archived_at`] answers `Some`); a team that
/// is not archived at all still falls straight through to the idempotent
/// no-op restore below, exactly as it always has.
#[utoipa_path(
    post,
    path = "/v1/teams/{team_id}/restore",
    tag = "teams",
    params(("team_id" = Uuid, Path, description = "Team identifier")),
    security(("bearer_token" = [])),
    responses(
        (status = 200, description = "Restored (idempotent — already-active is not an error)", body = Value),
        (status = 403, description = "Caller is not a team admin or org admin"),
        (status = 404, description = "Team not found"),
        (status = 409, description = "The 120-day restore window has expired"),
        (status = 503, description = "Database not available"),
    )
)]
pub async fn restore_team(
    State(state): State<Arc<AppState>>,
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
    Path(team_id): Path<Uuid>,
) -> impl IntoResponse {
    // Settings launch PR 2.8: a human, browser-session action only
    // (`middleware::auth::authority_allowed`).
    if let Some(refusal) = crate::middleware::auth::interactive_session_refusal() {
        return refusal.into_response();
    }
    if let Some(reject) = reject_org_named_teams(&state, &tenant).await {
        return reject;
    }
    let store = require_store_resp!(tenant);

    let caller_org_admin = if tenant.org_context().is_some() {
        match is_org_admin(&state, user.account_id, user.user_id).await {
            Ok(p) => p,
            Err((s, b)) => return (s, b).into_response(),
        }
    } else {
        None
    };

    // RESTORE IS A HIGHER AUTHORITY THAN ARCHIVE, deliberately asymmetric.
    //
    // A team admin may archive their own workspace, but may NOT restore it —
    // restore requires ACCOUNT-level authority, the same rule that gates
    // `list_archived_teams`. Keeping the two in step is the whole point: the
    // archived list is what makes a workspace findable again, so anyone who
    // can restore must be able to see it, and anyone who cannot see it must
    // not be able to restore it. A team-admin-or-org-admin rule here would
    // let someone restore a workspace they are forbidden to list.
    //
    // `has_account_authority` reads `account_members.role IN ('owner','admin')`,
    // which resolves to the ORG ADMIN on an org account and the ACCOUNT OWNER
    // on a personal one. That distinction is load-bearing rather than
    // cosmetic: `is_org_admin` requires `accounts.clerk_org_id IS NOT NULL`
    // and therefore returns `None` for EVERY caller on a personal account —
    // so gating restore on org-admin alone would make it unreachable on
    // exactly the plane where team workspaces work today.
    //
    // Read on the CONTROL plane before the scoped transaction opens, matching
    // how `is_org_admin` is resolved above and this file's rule that
    // control-plane reads never happen inside a scoped tx.
    // Propagate a store failure as its own response rather than collapsing it
    // into `false`: a database error is not an authorization decision, and
    // rendering one as 403 tells the caller a fact about their rights we have
    // no evidence for (the `authz_read!` reasoning, applied here too).
    let has_authority = match has_account_authority(&state, user.account_id, user.user_id).await {
        Ok(v) => v,
        Err((s, b)) => return (s, b).into_response(),
    };
    if !has_authority {
        return (
            StatusCode::FORBIDDEN,
            Json(json!({
                "error": "only an account owner or organization admin can restore an archived workspace"
            })),
        )
            .into_response();
    }

    let mut scoped_tx = begin_scoped_tx_resp!(tenant, user.account_id, caller_org_admin);
    let team_id = resolve_team_id!(store, scoped_tx, team_id);

    lock_team_row_or_return!(store, scoped_tx, team_id);

    // 120-day restore window (workspace-manage design doc, "Archive and
    // restore"). Checked on the scoped Postgres path only.
    if let Some(pg_tx) = scoped_tx.as_mut() {
        let archived_at = authz_read!(
            "team_archived_at",
            store.team_archived_at_scoped(pg_tx, team_id).await
        );
        if let Some(archived_at) = archived_at {
            let (_, restorable) = team_restore_window(archived_at, Utc::now());
            if !restorable {
                // No write has happened yet (only the `FOR UPDATE` row lock
                // above) — returning here without `commit_write_tx!` drops
                // `scoped_tx`, which rolls the transaction back, same as
                // every other early-return-before-commit branch in this
                // file (e.g. the 403 case just above).
                return (
                    StatusCode::CONFLICT,
                    Json(json!({
                        "error": "the 120-day restore window for this workspace has expired"
                    })),
                )
                    .into_response();
            }
        }
    }

    let rows = match scoped_store!(store, scoped_tx, restore_team / restore_team_scoped(team_id, user.user_id)) {
        Ok(n) => n,
        Err(e) => {
            tracing::error!(error = %e, "restore_team store error");
            return (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": "internal error"}))).into_response();
        }
    };
    commit_write_tx!(scoped_tx);

    tracing::info!(%team_id, restored_by = %user.user_id, changed = rows > 0, "team ocean restored");
    (
        StatusCode::OK,
        Json(json!({ "team_id": team_id, "archived": false, "changed": rows > 0 })),
    )
        .into_response()
}

// ── GET /v1/teams/archived ──────────────────────────────────────────────────

/// List archived team oceans for the caller's account — the read half of the
/// workspace-manage design doc's "Archive and restore" section, and the
/// endpoint that makes [`restore_team`] REACHABLE at all: nothing else in
/// this API surfaces a team's id once it is archived (every reachability
/// read — `GET /v1/graphs` included — filters `archived_at IS NULL`, see
/// [`archive_team`]'s doc comment), so before this endpoint existed
/// `POST /v1/teams/{team_id}/restore` had no caller that could ever learn
/// which `{team_id}` to pass it.
///
/// Response shape, one entry per archived team ocean:
/// ```json
/// { "team_id": "...", "graph_id": "...", "name": "...",
///   "archived_at": "...", "expires_at": "...", "restorable": true }
/// ```
/// `expires_at`/`restorable` are computed here (never stored) via
/// [`team_restore_window`] — the SAME function [`restore_team`]'s own 409
/// check calls — so a UI rendering a countdown from this list and a restore
/// attempt against the same team can never disagree about whether the
/// window is still open.
///
/// # Authorization — account-level authority, NOT team admin
///
/// Gated on `account_members.role IN ('owner', 'admin')` for the CALLER's
/// own account ([`has_account_authority`]), which resolves to the ORG ADMIN
/// on an org account and the ACCOUNT OWNER on a personal one — see that
/// function's own doc comment for why this is written as ONE role check
/// rather than a literal "org admin" gate.
///
/// This is deliberately NARROWER than [`archive_team`]'s "team admin OR org
/// admin" gate: a team admin can archive a workspace but can neither see it
/// on this list nor restore it (workspace-manage design doc, "A deliberate
/// asymmetry, accepted with eyes open" — raised and kept on purpose, so team
/// admins stay in control of their own workspace's lifecycle without also
/// gaining account-wide visibility). The frontend's archive confirmation is
/// expected to say so before the user confirms, since this is the only
/// moment a team admin will learn it.
///
/// # Why this does NOT go through `tenant_rls_visible_graph_ids()`
///
/// That predicate (and its control-plane twin) is the single authorization
/// point ~41 call sites across this codebase already trust for CONTENT
/// reachability, and it filters `archived_at IS NULL` as a TOP-LEVEL
/// condition BY DESIGN — an archived ocean is invisible there for everyone,
/// which is exactly what makes archiving remove a workspace from every
/// reachable-graphs read in the first place (`archive_team`'s doc comment).
/// Reusing it here would make this endpoint permanently answer `[]`. It
/// instead calls [`crate::store::Store::list_archived_teams`], which reads
/// `teams`/`graphs` directly, scoped to the caller's own `account_id` — see
/// that method's own doc comment for the org-tenant-plane caveat this
/// implies (in short: on an org tenant database, `graphs`' own RLS policy
/// was deliberately NOT widened with an org-admin governance arm the way
/// `teams`/`team_members`/`membership_events` were in `migrations_tenant/
/// 0032`, for the same "stay out of the content path" reason described
/// there — so this can under-report archived team oceans for an org admin
/// on that plane specifically. `ORG_TEAMS_ENABLED` defaults off, and
/// personal accounts, where this predicate has no membership component at
/// all, are exactly where team workspaces work today; closing that gap for
/// the org-tenant plane is out of this endpoint's scope, in the same spirit
/// as the "orphaned-teams view for org admins" open item `0032` already
/// lists).
#[utoipa_path(
    get,
    path = "/v1/teams/archived",
    tag = "teams",
    security(("bearer_token" = [])),
    responses(
        (status = 200, description = "Archived team oceans for the caller's account", body = Value),
        (status = 403, description = "Caller does not hold account-level authority (owner or org admin)"),
        (status = 503, description = "Database not available"),
    )
)]
pub async fn list_archived_teams(
    State(state): State<Arc<AppState>>,
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
) -> impl IntoResponse {
    if let Some(reject) = reject_org_named_teams(&state, &tenant).await {
        return reject;
    }
    let store = require_store_resp!(tenant);

    let authorized = match has_account_authority(&state, user.account_id, user.user_id).await {
        Ok(a) => a,
        Err((s, b)) => return (s, b).into_response(),
    };
    if !authorized {
        return (
            StatusCode::FORBIDDEN,
            Json(json!({
                "error": "must be the account owner or an org admin to view archived workspaces"
            })),
        )
            .into_response();
    }

    let caller_org_admin = if tenant.org_context().is_some() {
        match is_org_admin(&state, user.account_id, user.user_id).await {
            Ok(p) => p,
            Err((s, b)) => return (s, b).into_response(),
        }
    } else {
        None
    };

    // Read-only scoped transaction, same convention as `list_team_events`
    // just below — carries the org-admin governance grant so `teams` rows
    // resolve for a non-member org admin too (see `migrations_tenant/0032`'s
    // `teams` arm), even though (per this handler's own doc comment) the
    // `graphs` join stays gated by the un-widened `graphs` RLS policy on
    // that plane regardless.
    let mut scoped_tx = begin_scoped_tx_resp!(tenant, user.account_id, caller_org_admin);

    let rows = match scoped_store!(
        store,
        scoped_tx,
        list_archived_teams / list_archived_teams_scoped(user.account_id)
    ) {
        Ok(r) => r,
        Err(e) => {
            tracing::error!(error = %e, "list_archived_teams store error");
            close_read_tx!(scoped_tx);
            return (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": "internal error"}))).into_response();
        }
    };
    close_read_tx!(scoped_tx);

    let now = Utc::now();
    let out: Vec<Value> = rows
        .into_iter()
        .map(|t| {
            let (expires_at, restorable) = team_restore_window(t.archived_at, now);
            json!({
                "team_id": t.team_id,
                "graph_id": t.graph_id,
                "name": t.name,
                "archived_at": t.archived_at,
                "expires_at": expires_at,
                "restorable": restorable,
            })
        })
        .collect();

    (StatusCode::OK, Json(json!(out))).into_response()
}

// ── GET /v1/teams/{team_id}/events ────────────────────────────────────────────

/// Paginated `membership_events` history for one team, newest first.
/// Readable by team admins and org admins only — same authorization as the
/// roster WRITES, not a public roster view.
///
/// An empty `200 []` from this endpoint means the team has no recorded history,
/// and nothing else: a team that does not exist (or that the caller cannot
/// resolve) is a 404, and any failure to READ is a 5xx. That distinction is the
/// point of the existence probe in the body — see its comment, and the module
/// doc's "Org-admin roster rights" section for the RLS gap that made an empty
/// list ambiguous in the first place.
#[utoipa_path(
    get,
    path = "/v1/teams/{team_id}/events",
    tag = "teams",
    params(
        ("team_id" = Uuid, Path, description = "Team identifier"),
        ("page" = Option<u32>, Query, description = "Page number (default 1)"),
        ("page_size" = Option<u32>, Query, description = "Page size (default 20, max 50)"),
    ),
    security(("bearer_token" = [])),
    responses(
        (status = 200, description = "Membership history, newest first", body = Value),
        (status = 403, description = "Caller is not a team admin or org admin"),
        (status = 404, description = "Team not found"),
        (status = 503, description = "Database not available"),
    )
)]
pub async fn list_team_events(
    State(state): State<Arc<AppState>>,
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
    Path(team_id): Path<Uuid>,
    Query(params): Query<TeamEventsQueryParams>,
) -> impl IntoResponse {
    if let Some(reject) = reject_org_named_teams(&state, &tenant).await {
        return reject;
    }
    let store = require_store_resp!(tenant);

    let caller_org_admin = if tenant.org_context().is_some() {
        match is_org_admin(&state, user.account_id, user.user_id).await {
            Ok(p) => p,
            Err((s, b)) => return (s, b).into_response(),
        }
    } else {
        None
    };

    let (page_size, offset) = events_page_bounds(&params);

    // Carries the governance grant when the caller is a verified org admin.
    // THIS is the endpoint the RLS gap was reported against: authorized at the
    // route by `is_org_admin`, then served by a `membership_events` read whose
    // policy resolves through team MEMBERSHIP, so a non-member org admin was
    // answered `200 []` — "this team has no history" — on the one surface that
    // exists to audit it. The grant closes the gap; the existence probe below
    // closes the residual "empty because there is nothing, or empty because
    // there is nothing VISIBLE?" ambiguity.
    let mut scoped_tx = begin_scoped_tx_resp!(tenant, user.account_id, caller_org_admin);
    let team_id = resolve_team_id!(store, scoped_tx, team_id);

    let is_team_admin = authz_read!(
        "find_team_admin_id",
        scoped_store!(store, scoped_tx, find_team_admin_id / find_team_admin_id_scoped(team_id, user.user_id))
    )
    .is_some();
    if !is_team_admin && caller_org_admin.is_none() {
        close_read_tx!(scoped_tx);
        return (StatusCode::FORBIDDEN, Json(json!({"error": "must be a team admin or org admin to view team history"}))).into_response();
    }

    // Existence probe, in the SAME scoped transaction as the read below (the
    // roster endpoint has always had one; this endpoint did not, which is why
    // it answered `200 []` for a team id that does not exist at all, as well
    // as for one the caller merely could not see).
    //
    // Three states used to collapse into one `200 []` response body here — no
    // such team, no visible such team, and a real team with no history — and
    // only the third is an honest empty list. They no longer can:
    //
    //   * a caller authorized as a TEAM admin necessarily holds an active
    //     `team_members` row, which makes the team ocean reachable, which makes
    //     the `teams` row visible — so `false` cannot be an RLS artefact;
    //   * a caller authorized as an ORG admin is carrying the governance
    //     grant, which `set_tenant_org_admin` has already proven this database
    //     honours (it calls `tenant_rls_is_org_admin()` back and errors out if
    //     the migration is not applied) — so `false` cannot be an RLS artefact
    //     for them either.
    //
    // Which leaves exactly one meaning — the team does not exist — and 404 is
    // the honest answer to that. An empty `200 []` past this point is now a
    // fact about the audit trail rather than a fact about our own visibility.
    let exists = authz_read!(
        "team_exists",
        scoped_store!(store, scoped_tx, team_exists / team_exists_scoped(team_id))
    );
    if !exists {
        close_read_tx!(scoped_tx);
        return (StatusCode::NOT_FOUND, Json(json!({"error": "team not found"}))).into_response();
    }

    let events = match scoped_store!(
        store,
        scoped_tx,
        list_membership_events / list_membership_events_scoped(team_id, page_size, offset)
    ) {
        Ok(e) => e,
        Err(e) => {
            tracing::error!(error = %e, "list_team_events store error");
            close_read_tx!(scoped_tx);
            return (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": "internal error"}))).into_response();
        }
    };
    close_read_tx!(scoped_tx);

    let out: Vec<Value> = events
        .into_iter()
        .map(|e| {
            json!({
                "id":              e.id,
                "team_id":         e.team_id,
                "actor_user_id":   e.actor_user_id,
                "target_user_id":  e.target_user_id,
                "action":          e.action,
                "old_role":        e.old_role,
                "new_role":        e.new_role,
                "created_at":      e.created_at,
            })
        })
        .collect();

    (StatusCode::OK, Json(json!(out))).into_response()
}

// ── GET /v1/orgs/members ──────────────────────────────────────────────────────

/// Directory backing the UI member picker (org-team-membership design doc
/// decision #10): `user_id`/`name`/`email`/`role` for every LIVE
/// `account_members` row of the caller's org. Org context only — this is the
/// source of truth for "who can I add to a team", not a general roster
/// endpoint.
///
/// Raw pool query against the CONTROL plane, the same convention
/// [`is_org_admin`] and [`reject_org_account_membership_management`] already
/// establish for org-governance reads that have no tenant-plane counterpart.
///
/// **Org-invites train PR 1 addition:** each member also carries a DERIVED
/// `pending` (Clerk `last_sign_in_at IS NULL`) plus `invited_by`/
/// `invited_at`, read from Clerk's `public_metadata` — see
/// `routes::org_invites`'s module doc for the full "no invite table" design.
/// Two Clerk Backend API calls run CONCURRENTLY via `tokio::join!`
/// (`list_org_users` for `last_sign_in_at`, `list_org_memberships` for the
/// invite metadata), joined to these DB rows by `users.clerk_user_id`, over
/// a DEDICATED short-timeout `ClerkAdminClient` (`clerk_admin::
/// clerk_http_client`) rather than `AppState.http_client` — that client is
/// sized for the LLM path's 60s (default) timeout, which this roster
/// hot-path read must never be able to borrow (org-invites train PR 1
/// review, finding 8). BEST-EFFORT: any Clerk API failure (not configured,
/// transport, non-2xx) degrades every member's `pending`/`invited_by`/
/// `invited_at` to `null` rather than failing the whole roster read — a
/// member picker that already works today must not start 500ing because an
/// unrelated enrichment call failed. Logged once at `warn!`, not per member.
///
/// **Admin-only enrichment (finding 9):** `pending`/`invited_by`/
/// `invited_at` can reveal who has and hasn't accepted an invite and who
/// invited whom — not information a plain member needs about their
/// teammates. A non-admin caller gets the roster's EXACT pre-org-invites-
/// train shape (`user_id`/`name`/`email`/`role` only, the three new keys
/// omitted entirely rather than nulled) — checked via the same
/// [`crate::routes::org_invites::is_admin`] gate the invite endpoints use.
/// The Clerk enrichment calls themselves are skipped for a non-admin caller
/// too, not just hidden from the response — no reason to pay for a lookup
/// nobody will see.
///
/// **`connected_providers`/`connect_nudged_at` (CS-436, org-onboarding plan
/// §3 item 49, D21):** same admin-only gate as the Clerk-derived fields
/// above — a non-admin caller gets the same three-key legacy shape and
/// never sees either. `connected_providers` is the DISTINCT `oauth_tokens.
/// provider` values this member owns on this account (`[]` when they have
/// not connected anything with their own grant — never `null`, so the web
/// side can test emptiness without a null check); `connect_nudged_at` is
/// the last time `workers::member_connect_nudge` sent this member the
/// connect nudge (`null` = never). This is the "approved, never connected"
/// state D21 says must never be silent, made visible right on the roster
/// the admin already reads — no separate screen.
#[utoipa_path(
    get,
    path = "/v1/orgs/members",
    tag = "teams",
    security(("bearer_token" = [])),
    responses(
        (status = 200, description = "The caller's org roster", body = Value),
        (status = 403, description = "Not an org context, or ORG_TEAMS_ENABLED is off"),
        (status = 503, description = "Database unavailable"),
    )
)]
pub async fn list_org_members(
    State(state): State<Arc<AppState>>,
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
) -> (StatusCode, Json<Value>) {
    let Some(org_ctx) = tenant.org_context() else {
        return (
            StatusCode::FORBIDDEN,
            Json(json!({"error": "this endpoint is only available in an organization workspace"})),
        );
    };
    let clerk_org_id = org_ctx.org_id.clone();
    if !org_teams_active_for(&state, &org_ctx.org_id).await {
        return (
            StatusCode::FORBIDDEN,
            Json(json!({"error": "named teams are not yet available in organization workspaces"})),
        );
    }

    let pool = match state.control_db() {
        Some(p) => p,
        None => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({"error": "service unavailable"})),
            )
        }
    };

    // `connected_providers`/`connect_nudged_at` — CS-436 (org-onboarding
    // plan §3 item 49, D21): "approved, never connected" must be visible to
    // the admin, not just guarded against by email. Both are correlated
    // subqueries inside this SAME `SELECT` (one round trip, not a per-member
    // loop — `scripts/check_no_unbounded_fanout.sh`'s target is a Rust-level
    // N+1, which this is not: Postgres plans and executes the whole
    // statement, including its subplans, as a single query).
    //
    // `connected_providers`: DISTINCT `oauth_tokens.provider` this member
    // OWNS on this account (`owner_user_id = am.user_id` — never `IS NULL OR
    // =`, see `connectors::TokenOwner`'s own doc comment for why an
    // account-wide token, Slack's today, must never read as "this member
    // connected"). `COALESCE(..., ARRAY[]::text[])` so a member with no
    // owned token gets `[]`, never `NULL` — the web side treats an empty
    // array, not a null, as "not connected".
    //
    // `connect_nudged_at`: the most recent `last_notified_at` this worker's
    // OWN provider value (`workers::member_connect_nudge`, `services::
    // unmatched_recipient_email::MEMBER_CONNECT_PROVIDER`) claimed for this
    // member's email — `NULL` means never nudged (either still inside the
    // grace window, already connected, or the worker has not reached them
    // yet).
    // `oauth_tokens` and `slack_unmatched_email_notices` (the two subqueries
    // below) carry `account_isolation` policies that cast
    // `current_setting('app.account_id')::uuid`. On the `coralswarm_app` role
    // an unscoped read hard-errors (`invalid input syntax for type uuid: ""`
    // on a recycled pooled connection), so the whole read runs in the
    // account-scoped transaction (`SET LOCAL`, never a bare `SET`) — #1500.
    let mut scoped = match crate::tenancy::scope::begin_account_scoped(pool, user.account_id).await {
        Ok(tx) => tx,
        Err(e) => {
            tracing::error!(error = %e, "list_org_members: could not open the account-scoped transaction");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            );
        }
    };
    let rows: Result<
        Vec<(
            Uuid,
            Option<String>,
            String,
            String,
            Option<String>,
            Vec<String>,
            Option<DateTime<Utc>>,
            bool,
        )>,
        sqlx::Error,
    > = sqlx::query_as(
        "SELECT u.id, u.name, u.email, am.role, u.clerk_user_id, \
                COALESCE(( \
                    SELECT array_agg(DISTINCT t.provider ORDER BY t.provider) \
                    FROM oauth_tokens t \
                    WHERE t.account_id = am.account_id AND t.owner_user_id = am.user_id \
                ), ARRAY[]::text[]) AS connected_providers, \
                ( \
                    SELECT n.last_notified_at FROM slack_unmatched_email_notices n \
                    WHERE n.provider = 'member_connect' AND n.email = u.email \
                ) AS connect_nudged_at, \
                COALESCE(a.owner_user_id = am.user_id, false) AS is_owner \
         FROM account_members am \
         JOIN users u ON u.id = am.user_id \
         JOIN accounts a ON a.id = am.account_id \
         WHERE am.account_id = $1 \
         ORDER BY am.joined_at ASC",
    )
    .bind(user.account_id)
    .fetch_all(&mut *scoped)
    .await;
    // Read-only: release the connection before the Clerk calls below.
    drop(scoped);

    let rows = match rows {
        Ok(r) => r,
        Err(e) => {
            tracing::error!(error = %e, "list_org_members DB error");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            );
        }
    };

    // Admin-only gate (finding 9) — see this function's own doc comment.
    // `UserContext.role` is the live DB role, same bar
    // `routes::org_invites::require_org_admin` uses.
    let caller_is_admin = crate::routes::org_invites::is_admin(&user.role);

    // Best-effort Clerk enrichment — see this function's own doc comment.
    // `None` for either map means "Clerk lookup unavailable/failed/skipped
    // (non-admin caller)", which every member below reads as pending=null /
    // invite fields=null, not an error for the whole endpoint. Concurrent
    // (`tokio::join!`) over a dedicated short-timeout client (finding 8).
    let (last_sign_in_by_clerk_id, invite_meta_by_clerk_id): (
        Option<std::collections::HashMap<String, Option<i64>>>,
        Option<std::collections::HashMap<String, (Value, String)>>,
    ) = if !caller_is_admin {
        (None, None)
    } else {
        match crate::clerk_admin::ClerkAdminClient::from_env() {
            Some(clerk) => {
                let (users, memberships) = tokio::join!(
                    clerk.list_org_users(&clerk_org_id),
                    clerk.list_org_memberships(&clerk_org_id),
                );
                match (users, memberships) {
                    (Ok(users), Ok(memberships)) => {
                        let last_sign_in = users
                            .into_iter()
                            .map(|u| (u.id, u.last_sign_in_at))
                            .collect();
                        let meta = memberships
                            .into_iter()
                            .map(|m| (m.public_user_data.user_id, (m.public_metadata, m.role)))
                            .collect();
                        (Some(last_sign_in), Some(meta))
                    }
                    (users, memberships) => {
                        tracing::warn!(
                            clerk_org_id = %clerk_org_id,
                            users_ok = users.is_ok(), memberships_ok = memberships.is_ok(),
                            "list_org_members: Clerk enrichment failed — serving the roster with \
                             pending/invited_by/invited_at all null"
                        );
                        (None, None)
                    }
                }
            }
            None => (None, None),
        }
    };

    let members: Vec<Value> = rows
        .into_iter()
        .map(|(user_id, name, email, role, clerk_user_id, connected_providers, connect_nudged_at, is_owner)| {
            if !caller_is_admin {
                // Exact pre-org-invites-train shape — see this function's
                // own doc comment, "Admin-only enrichment".
                return json!({
                    "user_id": user_id,
                    "name":    name,
                    "email":   email,
                    "role":    role,
                });
            }
            let pending = clerk_user_id.as_deref().and_then(|cid| {
                last_sign_in_by_clerk_id
                    .as_ref()
                    .and_then(|m| m.get(cid))
                    .map(|last_sign_in| last_sign_in.is_none())
            });
            let (invited_by, invited_at) = clerk_user_id
                .as_deref()
                .and_then(|cid| invite_meta_by_clerk_id.as_ref().and_then(|m| m.get(cid)))
                .map(|(metadata, _role)| {
                    let invited_by = match (
                        metadata.get("invited_by_user_id").and_then(|v| v.as_str()),
                        metadata.get("invited_by_name").and_then(|v| v.as_str()),
                    ) {
                        (Some(id), name) => Some(json!({"user_id": id, "name": name})),
                        (None, _) => None,
                    };
                    let invited_at = metadata
                        .get("invited_at")
                        .and_then(|v| v.as_str())
                        .map(str::to_string);
                    (invited_by, invited_at)
                })
                .unwrap_or((None, None));
            json!({
                "user_id": user_id,
                "name":    name,
                "email":   email,
                "role":    role,
                "pending": pending,
                "invited_by": invited_by,
                "invited_at": invited_at,
                "connected_providers": connected_providers,
                "connect_nudged_at": connect_nudged_at,
                // True for `accounts.owner_user_id`. The owner's `role` reads
                // `admin`, so this is the only way for a client to tell that
                // `DELETE`/`PATCH /v1/orgs/members/{user_id}` will refuse the row.
                "is_owner": is_owner,
            })
        })
        .collect();

    (StatusCode::OK, Json(json!(members)))
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        body::Body,
        http::{Request as HttpRequest, StatusCode},
        middleware,
        routing::{delete, get, patch, post},
        Router,
    };
    use tower::ServiceExt;

    fn test_state() -> Arc<AppState> {
        crate::test_app_state()
    }

    fn teams_router(state: Arc<AppState>) -> Router {
        Router::new()
            .route("/v1/teams", get(list_teams))
            .route("/v1/teams/members", get(list_members))
            // Canonical (new) paths use the real handlers; the old `/teams/...`
            // paths use the thin deprecated-alias wrappers — matches each
            // function's own `#[utoipa_path]` annotation (see `invite_member`
            // vs `invite_member_legacy`'s doc comments) and mirrors the real
            // registration in `lib.rs`.
            .route("/v1/teams/invite", post(invite_member_legacy))
            .route("/v1/teams/members/{user_id}", delete(remove_member_legacy))
            .route("/v1/account/members", post(invite_member))
            .route("/v1/account/members/{user_id}", delete(remove_member))
            .route("/v1/teams/{team_id}/members", get(list_team_members).post(add_member))
            .route(
                "/v1/teams/{team_id}/members/{user_id}",
                patch(assign_team_member_role),
            )
            .route(
                "/v1/teams/{team_id}",
                patch(rename_team).delete(archive_team),
            )
            .route("/v1/teams/{team_id}/restore", post(restore_team))
            .route("/v1/teams/archived", get(list_archived_teams))
            .route("/v1/teams/{team_id}/events", get(list_team_events))
            .route("/v1/orgs/members", get(list_org_members))
            .layer(middleware::from_fn_with_state(
                state.clone(),
                crate::middleware::auth::require_auth,
            ))
            .with_state(state)
    }

    // ── GET /v1/teams — no auth → 401 ────────────────────────────────────────

    #[tokio::test]
    async fn list_teams_requires_auth() {
        let router = teams_router(test_state());
        let req = HttpRequest::builder()
            .uri("/v1/teams")
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    // ── GET /v1/teams — Bearer + no DB → 503 ─────────────────────────────────

    #[tokio::test]
    async fn list_teams_no_db_returns_503() {
        let router = teams_router(test_state());
        let req = HttpRequest::builder()
            .uri("/v1/teams")
            .header("Authorization", "Bearer sometoken")
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    // ── GET /v1/teams/members — no auth → 401 ────────────────────────────────

    #[tokio::test]
    async fn list_members_requires_auth() {
        let router = teams_router(test_state());
        let req = HttpRequest::builder()
            .uri("/v1/teams/members")
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    // ── GET /v1/teams/members — Bearer + no DB → 503 ─────────────────────────

    #[tokio::test]
    async fn list_members_no_db_returns_503() {
        let router = teams_router(test_state());
        let req = HttpRequest::builder()
            .uri("/v1/teams/members")
            .header("Authorization", "Bearer sometoken")
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    // ── POST /v1/teams/invite — no auth → 401 ────────────────────────────────

    #[tokio::test]
    async fn invite_member_requires_auth() {
        let router = teams_router(test_state());
        let req = HttpRequest::builder()
            .method("POST")
            .uri("/v1/teams/invite")
            .header("content-type", "application/json")
            .body(Body::from(r#"{"email":"a@b.com","role":"member"}"#))
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    // ── POST /v1/teams/invite — invalid role → 400 (via fake auth) ───────────

    #[tokio::test]
    async fn invite_member_invalid_role_returns_400() {
        use axum::middleware::from_fn;

        async fn inject_user(
            mut req: axum::extract::Request,
            next: axum::middleware::Next,
        ) -> axum::response::Response {
            req.extensions_mut().insert(UserContext {
                user_id: Uuid::nil(),
                account_id: Uuid::nil(),
                role: "owner".into(),
                device_id: "test-device".into(),
            });
            next.run(req).await
        }

        let state = test_state();
        let router = Router::new()
            .route("/v1/teams/invite", post(invite_member))
            .layer(from_fn(inject_user))
            .with_state(state);

        let req = HttpRequest::builder()
            .method("POST")
            .uri("/v1/teams/invite")
            .header("content-type", "application/json")
            .body(Body::from(r#"{"email":"a@b.com","role":"superuser"}"#))
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    // ── POST /v1/teams/invite — valid shape, no DB → 503 ─────────────────────

    #[tokio::test]
    async fn invite_member_no_db_returns_503() {
        use axum::middleware::from_fn;

        async fn inject_user(
            mut req: axum::extract::Request,
            next: axum::middleware::Next,
        ) -> axum::response::Response {
            req.extensions_mut().insert(UserContext {
                user_id: Uuid::nil(),
                account_id: Uuid::nil(),
                role: "owner".into(),
                device_id: "test-device".into(),
            });
            next.run(req).await
        }

        let state = test_state();
        let router = Router::new()
            .route("/v1/teams/invite", post(invite_member))
            .layer(from_fn(inject_user))
            .with_state(state);

        let req = HttpRequest::builder()
            .method("POST")
            .uri("/v1/teams/invite")
            .header("content-type", "application/json")
            .body(Body::from(r#"{"email":"a@b.com","role":"member"}"#))
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    // ── DELETE /v1/teams/members/{id} — no auth → 401 ────────────────────────

    #[tokio::test]
    async fn remove_member_requires_auth() {
        let router = teams_router(test_state());
        let req = HttpRequest::builder()
            .method("DELETE")
            .uri("/v1/teams/members/00000000-0000-0000-0000-000000000000")
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    // ── DELETE /v1/teams/members/{id} — Bearer + no DB → 503 ─────────────────

    #[tokio::test]
    async fn remove_member_no_db_returns_503() {
        let router = teams_router(test_state());
        let req = HttpRequest::builder()
            .method("DELETE")
            .uri("/v1/teams/members/00000000-0000-0000-0000-000000000000")
            .header("Authorization", "Bearer sometoken")
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    // ── DELETE /v1/teams/members/{id} — handler, no DB → 503 ─────────────────

    #[tokio::test]
    async fn remove_member_handler_no_db_returns_503() {
        use axum::middleware::from_fn;

        async fn inject_user(
            mut req: axum::extract::Request,
            next: axum::middleware::Next,
        ) -> axum::response::Response {
            req.extensions_mut().insert(UserContext {
                user_id: Uuid::nil(),
                account_id: Uuid::nil(),
                role: "owner".into(),
                device_id: "test-device".into(),
            });
            next.run(req).await
        }

        let state = test_state();
        let router = Router::new()
            .route("/v1/teams/members/{user_id}", delete(remove_member))
            .layer(from_fn(inject_user))
            .with_state(state);

        let req = HttpRequest::builder()
            .method("DELETE")
            .uri("/v1/teams/members/00000000-0000-0000-0000-000000000001")
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    // ── POST /v1/account/members — deprecated alias, no auth → 401 ───────────

    #[tokio::test]
    async fn invite_member_account_alias_requires_auth() {
        let router = teams_router(test_state());
        let req = HttpRequest::builder()
            .method("POST")
            .uri("/v1/account/members")
            .header("content-type", "application/json")
            .body(Body::from(r#"{"email":"a@b.com","role":"member"}"#))
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    // ── DELETE /v1/account/members/{id} — alias, no auth → 401 ───────────────

    #[tokio::test]
    async fn remove_member_account_alias_requires_auth() {
        let router = teams_router(test_state());
        let req = HttpRequest::builder()
            .method("DELETE")
            .uri("/v1/account/members/00000000-0000-0000-0000-000000000000")
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    // ── PATCH /v1/teams/{id}/members/{id} — no auth → 401 ─────────────────────

    #[tokio::test]
    async fn assign_role_requires_auth() {
        let router = teams_router(test_state());
        let req = HttpRequest::builder()
            .method("PATCH")
            .uri("/v1/teams/00000000-0000-0000-0000-000000000001/members/00000000-0000-0000-0000-000000000002")
            .header("content-type", "application/json")
            .body(Body::from(r#"{"role":"admin"}"#))
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    // ── PATCH /v1/teams/{id}/members/{id} — Bearer + no DB → 503 ──────────────

    #[tokio::test]
    async fn assign_role_no_db_returns_503() {
        let router = teams_router(test_state());
        let req = HttpRequest::builder()
            .method("PATCH")
            .uri("/v1/teams/00000000-0000-0000-0000-000000000001/members/00000000-0000-0000-0000-000000000002")
            .header("Authorization", "Bearer sometoken")
            .header("content-type", "application/json")
            .body(Body::from(r#"{"role":"admin"}"#))
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    // ── GET /v1/teams/{id}/members — no auth → 401 ─────────────────────────────

    #[tokio::test]
    async fn list_team_members_requires_auth() {
        let router = teams_router(test_state());
        let req = HttpRequest::builder()
            .uri("/v1/teams/00000000-0000-0000-0000-000000000001/members")
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    // ── GET /v1/teams/{id}/members — Bearer + no DB → 503 ──────────────────────

    #[tokio::test]
    async fn list_team_members_no_db_returns_503() {
        let router = teams_router(test_state());
        let req = HttpRequest::builder()
            .uri("/v1/teams/00000000-0000-0000-0000-000000000001/members")
            .header("Authorization", "Bearer sometoken")
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    // ── GET /v1/teams/{id}/events — no auth → 401 ──────────────────────────────

    #[tokio::test]
    async fn list_team_events_requires_auth() {
        let router = teams_router(test_state());
        let req = HttpRequest::builder()
            .uri("/v1/teams/00000000-0000-0000-0000-000000000001/events")
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    // ── GET /v1/teams/{id}/events — Bearer + no DB → 503 ───────────────────────

    #[tokio::test]
    async fn list_team_events_no_db_returns_503() {
        let router = teams_router(test_state());
        let req = HttpRequest::builder()
            .uri("/v1/teams/00000000-0000-0000-0000-000000000001/events")
            .header("Authorization", "Bearer sometoken")
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    // ── PATCH /v1/teams/{id} (rename) — no auth → 401 ─────────────────────────

    #[tokio::test]
    async fn rename_team_requires_auth() {
        let router = teams_router(test_state());
        let req = HttpRequest::builder()
            .method("PATCH")
            .uri("/v1/teams/00000000-0000-0000-0000-000000000001")
            .header("content-type", "application/json")
            .body(Body::from(r#"{"name":"New Name"}"#))
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    // ── PATCH /v1/teams/{id} (rename) — Bearer + no DB → 503 ──────────────────

    #[tokio::test]
    async fn rename_team_no_db_returns_503() {
        let router = teams_router(test_state());
        let req = HttpRequest::builder()
            .method("PATCH")
            .uri("/v1/teams/00000000-0000-0000-0000-000000000001")
            .header("Authorization", "Bearer sometoken")
            .header("content-type", "application/json")
            .body(Body::from(r#"{"name":"New Name"}"#))
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    // ── DELETE /v1/teams/{id} (archive) — no auth → 401 ───────────────────────

    #[tokio::test]
    async fn archive_team_requires_auth() {
        let router = teams_router(test_state());
        let req = HttpRequest::builder()
            .method("DELETE")
            .uri("/v1/teams/00000000-0000-0000-0000-000000000001")
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    // ── DELETE /v1/teams/{id} (archive) — Bearer + no DB → 503 ────────────────

    #[tokio::test]
    async fn archive_team_no_db_returns_503() {
        let router = teams_router(test_state());
        let req = HttpRequest::builder()
            .method("DELETE")
            .uri("/v1/teams/00000000-0000-0000-0000-000000000001")
            .header("Authorization", "Bearer sometoken")
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    // ── POST /v1/teams/{id}/restore — no auth → 401 ───────────────────────────

    #[tokio::test]
    async fn restore_team_requires_auth() {
        let router = teams_router(test_state());
        let req = HttpRequest::builder()
            .method("POST")
            .uri("/v1/teams/00000000-0000-0000-0000-000000000001/restore")
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    // ── POST /v1/teams/{id}/restore — Bearer + no DB → 503 ────────────────────

    #[tokio::test]
    async fn restore_team_no_db_returns_503() {
        let router = teams_router(test_state());
        let req = HttpRequest::builder()
            .method("POST")
            .uri("/v1/teams/00000000-0000-0000-0000-000000000001/restore")
            .header("Authorization", "Bearer sometoken")
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    // ── GET /v1/teams/archived — no auth → 401 ─────────────────────────────────

    #[tokio::test]
    async fn list_archived_teams_requires_auth() {
        let router = teams_router(test_state());
        let req = HttpRequest::builder()
            .uri("/v1/teams/archived")
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    // ── GET /v1/teams/archived — Bearer + no DB → 503 ──────────────────────────

    #[tokio::test]
    async fn list_archived_teams_no_db_returns_503() {
        let router = teams_router(test_state());
        let req = HttpRequest::builder()
            .uri("/v1/teams/archived")
            .header("Authorization", "Bearer sometoken")
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    // ── GET /v1/orgs/members — no auth → 401 ───────────────────────────────────

    #[tokio::test]
    async fn list_org_members_requires_auth() {
        let router = teams_router(test_state());
        let req = HttpRequest::builder()
            .uri("/v1/orgs/members")
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    // ── GET /v1/orgs/members — Bearer + no DB → 503 (require_auth gate first) ─

    #[tokio::test]
    async fn list_org_members_no_db_returns_503() {
        let router = teams_router(test_state());
        let req = HttpRequest::builder()
            .uri("/v1/orgs/members")
            .header("Authorization", "Bearer sometoken")
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        // `require_auth` itself 503s when there's no DB pool at all, before
        // this handler's own org-context check ever runs.
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    // ── Unit: role validation logic ───────────────────────────────────────────

    #[test]
    fn valid_invite_roles() {
        for role in &["member", "owner"] {
            assert!(
                *role == "member" || *role == "owner",
                "{role} should be a valid invite role"
            );
        }
    }

    #[test]
    fn invalid_invite_role_detected() {
        let role = "admin";
        assert!(
            role != "member" && role != "owner",
            "admin should NOT be a valid invite role"
        );
    }

    #[test]
    fn invite_email_normalisation() {
        let raw = "  Test@Example.COM  ";
        let normalised = raw.trim().to_lowercase();
        assert_eq!(normalised, "test@example.com");
    }

    // ── Unit: assign-role request body ────────────────────────────────────────
    //
    // These replace three tests (`valid_assign_roles`,
    // `invalid_assign_role_detected`, `assign_role_idempotent_no_op_detection`)
    // deleted in the same change that added them. Those three asserted string
    // literals against string literals — `assert_eq!("admin", "admin")` — and
    // never called `assign_team_member_role`, `AssignRoleRequest`, or any other
    // item in this module. Changing the handler to accept "owner" left all
    // three green. They were false confidence, and the pre-existing
    // `valid_invite_roles`/`invalid_invite_role_detected` above follow the same
    // shape; do not extend that pattern here.
    //
    // What IS testable without a database: the request/query types this module
    // owns, and the pure helper the events endpoint's pagination now lives in.
    // The handlers themselves cannot be reached without a `Store` (they 503
    // first — see `assign_role_no_db_returns_503`), which is precisely why the
    // logic worth testing was moved out of them rather than restated in a test.

    #[test]
    fn assign_role_request_deserializes_role_verbatim() {
        // The handler normalises with `.trim().to_lowercase()` and only then
        // compares, so the deserializer must NOT be doing that itself —
        // otherwise the normalisation in the handler is untested dead code and
        // a future `#[serde(...)]` attribute could silently take over the
        // contract.
        let req: AssignRoleRequest = serde_json::from_str(r#"{"role":"  Admin  "}"#).unwrap();
        assert_eq!(req.role, "  Admin  ");
        assert_eq!(req.role.trim().to_lowercase(), "admin");
    }

    #[test]
    fn assign_role_request_requires_a_role_field() {
        // A missing `role` must be a deserialization failure (422 from axum's
        // `Json` extractor), never an empty string that reaches the handler's
        // validation and gets rejected there for the wrong reason.
        let err = serde_json::from_str::<AssignRoleRequest>(r#"{}"#).unwrap_err();
        assert!(
            err.to_string().contains("role"),
            "expected a missing-field error naming `role`, got: {err}"
        );
    }

    // ── Unit: events pagination bounds (`events_page_bounds`) ─────────────────

    #[test]
    fn events_query_params_default_to_first_page_of_twenty() {
        let params: TeamEventsQueryParams = serde_json::from_str("{}").unwrap();
        assert_eq!(params.page, 1);
        assert_eq!(params.page_size, 20);
        assert_eq!(events_page_bounds(&params), (20, 0));
    }

    #[test]
    fn events_page_bounds_clamps_page_size_to_fifty() {
        let params = TeamEventsQueryParams {
            page: 1,
            page_size: 100_000,
        };
        let (page_size, offset) = events_page_bounds(&params);
        assert_eq!(page_size, 50, "an unbounded LIMIT must never reach the store");
        assert_eq!(offset, 0);
    }

    #[test]
    fn events_page_bounds_never_produces_a_negative_offset() {
        // `page=0` is reachable — it is a valid `u32` and the `#[serde(default)]`
        // only fills in an ABSENT field, not a zero one — and `(0 - 1) * n` on
        // the i64 the store binds to `OFFSET` would be a hard SQL error.
        for page in [0, 1] {
            let params = TeamEventsQueryParams { page, page_size: 20 };
            assert_eq!(
                events_page_bounds(&params),
                (20, 0),
                "page={page} must resolve to the first page"
            );
        }
        let params = TeamEventsQueryParams {
            page: 3,
            page_size: 20,
        };
        assert_eq!(events_page_bounds(&params), (20, 40));
    }

    #[test]
    fn events_page_bounds_clamps_zero_page_size_up_to_one() {
        // `page_size=0` would make every page's offset 0 AND return nothing —
        // an infinite "no results" pager. One is the smallest honest answer.
        let params = TeamEventsQueryParams {
            page: 5,
            page_size: 0,
        };
        assert_eq!(events_page_bounds(&params), (1, 4));
    }

    // ── Route: reaching the named-team handlers past `require_auth` ───────────
    //
    // The `teams_router` tests above all stop at `require_auth`, which 503s on
    // a state with no pool BEFORE any handler extractor runs (see
    // `list_org_members_no_db_returns_503`'s own note). That is fine for
    // asserting the auth gate, and useless for asserting anything about a
    // handler's own extractors. These two mount the handler behind the same
    // fake-auth middleware `invite_member_invalid_role_returns_400` uses, so
    // the request genuinely reaches the handler: `Tenant` resolves to a
    // store-less/pool-less legacy tenant, and `require_store_resp!` is what
    // produces the 503. Anything that rejected EARLIER — a `Query`/`Json`
    // deserialization failure, a `Path` mismatch — would show up as a 4xx here
    // instead, which is exactly the distinction being asserted.

    /// The fake-auth layer these two share: injects a `UserContext` so
    /// `Extension<UserContext>` resolves without `require_auth` (and therefore
    /// without a database).
    async fn inject_test_user(
        mut req: axum::extract::Request,
        next: axum::middleware::Next,
    ) -> axum::response::Response {
        req.extensions_mut().insert(UserContext {
            user_id: Uuid::nil(),
            account_id: Uuid::nil(),
            role: "owner".into(),
            device_id: "test-device".into(),
        });
        next.run(req).await
    }

    #[tokio::test]
    async fn list_team_events_out_of_range_params_reach_the_handler() {
        // `page=0&page_size=100000` must be ACCEPTED by the `Query` extractor
        // and clamped by `events_page_bounds` inside the handler — not rejected
        // with a 400 at the extractor. That is what makes the bounds tests
        // above the whole story rather than half of it: if a future
        // `TeamEventsQueryParams` gains a validating deserializer, the clamping
        // stops being reachable and this goes red.
        let router = Router::new()
            .route("/v1/teams/{team_id}/events", get(list_team_events))
            .layer(axum::middleware::from_fn(inject_test_user))
            .with_state(test_state());

        let req = HttpRequest::builder()
            .uri("/v1/teams/00000000-0000-0000-0000-000000000001/events?page=0&page_size=100000")
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::SERVICE_UNAVAILABLE,
            "out-of-range pagination must reach the handler (503, no store), not be rejected \
             by the Query extractor"
        );
    }

    #[tokio::test]
    async fn assign_role_invalid_role_hits_the_store_gate_first() {
        // Pins the ORDER of `assign_team_member_role`'s early returns:
        // `require_store_resp!` runs BEFORE the role validation, so an invalid
        // role on a store-less deployment is 503, not 422. Worth pinning rather
        // than assuming — flipping those two would mean the role check runs on
        // a path that can never write, and would leak "this deployment has no
        // database" into a validation-shaped response.
        let router = Router::new()
            .route(
                "/v1/teams/{team_id}/members/{user_id}",
                patch(assign_team_member_role),
            )
            .layer(axum::middleware::from_fn(inject_test_user))
            .with_state(test_state());

        let req = HttpRequest::builder()
            .method("PATCH")
            .uri("/v1/teams/00000000-0000-0000-0000-000000000001/members/00000000-0000-0000-0000-000000000002")
            .header("content-type", "application/json")
            .body(Body::from(r#"{"role":"superuser"}"#))
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn assign_role_malformed_body_is_a_422_from_the_extractor() {
        // The counterpart of the test above, and the reason it is worth having
        // both: a body that does not DESERIALIZE is rejected by axum's `Json`
        // extractor before the handler runs at all, so it does NOT get the
        // store gate's 503. Together the two pin the boundary between "axum
        // rejected this" and "the handler rejected this", which is the thing a
        // reader of `assign_team_member_role` actually has to know.
        let router = Router::new()
            .route(
                "/v1/teams/{team_id}/members/{user_id}",
                patch(assign_team_member_role),
            )
            .layer(axum::middleware::from_fn(inject_test_user))
            .with_state(test_state());

        let req = HttpRequest::builder()
            .method("PATCH")
            .uri("/v1/teams/00000000-0000-0000-0000-000000000001/members/00000000-0000-0000-0000-000000000002")
            .header("content-type", "application/json")
            .body(Body::from(r#"{"not_role":"admin"}"#))
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::UNPROCESSABLE_ENTITY);
    }

    #[tokio::test]
    async fn rename_team_empty_name_hits_the_store_gate_first() {
        // Same ORDER pin as `assign_role_invalid_role_hits_the_store_gate_first`,
        // for `rename_team`: `require_store_resp!` runs BEFORE the
        // empty/too-long name validation, so an invalid name on a store-less
        // deployment is 503, not 422.
        let router = Router::new()
            .route("/v1/teams/{team_id}", patch(rename_team))
            .layer(axum::middleware::from_fn(inject_test_user))
            .with_state(test_state());

        let req = HttpRequest::builder()
            .method("PATCH")
            .uri("/v1/teams/00000000-0000-0000-0000-000000000001")
            .header("content-type", "application/json")
            .body(Body::from(r#"{"name":"   "}"#))
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn rename_team_malformed_body_is_a_422_from_the_extractor() {
        // Counterpart of the test above — a body missing `name` entirely does
        // not deserialize, so it is rejected by axum's `Json` extractor before
        // the handler (and its store gate) ever runs. Same boundary
        // `assign_role_malformed_body_is_a_422_from_the_extractor` pins for
        // the role-assignment PATCH.
        let router = Router::new()
            .route("/v1/teams/{team_id}", patch(rename_team))
            .layer(axum::middleware::from_fn(inject_test_user))
            .with_state(test_state());

        let req = HttpRequest::builder()
            .method("PATCH")
            .uri("/v1/teams/00000000-0000-0000-0000-000000000001")
            .header("content-type", "application/json")
            .body(Body::from(r#"{"not_name":"whatever"}"#))
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::UNPROCESSABLE_ENTITY);
    }

    // ── Unit: rename-team name validation logic ───────────────────────────────
    //
    // `rename_team` cannot be driven end to end without a `Store`
    // (`require_store_resp!` runs before name validation — see
    // `rename_team_empty_name_hits_the_store_gate_first`), so these call the
    // extracted `validate_team_name` helper directly: the SAME function the
    // handler calls, not a reimplementation of its arithmetic. This replaces
    // a prior version of this test that built `at_limit`/`over_limit` strings
    // and then asserted facts about `String::repeat` and `MAX_TEAM_NAME_LEN`
    // themselves — never calling any validation code, so it could not have
    // failed if the length rule were changed or deleted outright. That is
    // precisely the pattern the immediately preceding PR removed three other
    // tests in this file for (`assert_eq!("admin", "admin")` naming a
    // function it never called); reintroducing it here would be the same
    // defect under a different name.

    #[test]
    fn validate_team_name_boundary_is_exactly_reached_and_exceeded() {
        let at_limit = "a".repeat(MAX_TEAM_NAME_LEN);
        let over_limit = "a".repeat(MAX_TEAM_NAME_LEN + 1);
        assert_eq!(
            validate_team_name(&at_limit),
            Ok(at_limit.clone()),
            "a name of exactly MAX_TEAM_NAME_LEN chars must be ACCEPTED"
        );
        assert_eq!(
            validate_team_name(&over_limit),
            Err("name is too long"),
            "a name of MAX_TEAM_NAME_LEN + 1 chars must be REJECTED"
        );
    }

    #[test]
    fn validate_team_name_rejects_empty_after_trim() {
        assert_eq!(validate_team_name(""), Err("name must not be empty"));
        assert_eq!(validate_team_name("   "), Err("name must not be empty"));
    }

    #[test]
    fn validate_team_name_trims_before_returning() {
        assert_eq!(validate_team_name("  Ocean Crew  "), Ok("Ocean Crew".to_string()));
    }

    #[test]
    fn rename_team_request_trims_are_the_handlers_job_not_serdes() {
        // Mirrors `assign_role_request_deserializes_role_verbatim`: the
        // handler normalises with `.trim()`, so the deserializer must NOT be
        // doing that itself, or the handler's own trimming would be untested
        // dead code.
        let req: RenameTeamRequest = serde_json::from_str(r#"{"name":"  Ocean Crew  "}"#).unwrap();
        assert_eq!(req.name, "  Ocean Crew  ");
        assert_eq!(req.name.trim(), "Ocean Crew");
    }

    // ── 120-day restore window (pure — no database needed) ─────────────────────
    //
    // `team_restore_window` is the ONE function both `restore_team`'s 409
    // refusal and `list_archived_teams`'s `expires_at`/`restorable` fields
    // call — pinning its exact boundary here pins both call sites at once.

    #[test]
    fn restore_window_day_119_is_restorable() {
        let archived_at = Utc::now() - Duration::days(119);
        let (_, restorable) = team_restore_window(archived_at, Utc::now());
        assert!(
            restorable,
            "119 days after archiving must still be inside the 120-day window"
        );
    }

    #[test]
    fn restore_window_day_121_is_expired() {
        let archived_at = Utc::now() - Duration::days(121);
        let (_, restorable) = team_restore_window(archived_at, Utc::now());
        assert!(
            !restorable,
            "121 days after archiving must be past the 120-day window"
        );
    }

    #[test]
    fn restore_window_exact_boundary_instant_is_inclusive() {
        let archived_at = Utc::now() - Duration::days(200);
        let now = archived_at + Duration::days(TEAM_OCEAN_RESTORE_WINDOW_DAYS);
        let (expires_at, restorable) = team_restore_window(archived_at, now);
        assert_eq!(expires_at, now);
        assert!(
            restorable,
            "the boundary instant itself (archived_at + 120 days) must still be restorable"
        );
    }

    #[test]
    fn restore_window_one_second_past_boundary_is_expired() {
        let archived_at = Utc::now() - Duration::days(200);
        let now = archived_at + Duration::days(TEAM_OCEAN_RESTORE_WINDOW_DAYS) + Duration::seconds(1);
        let (_, restorable) = team_restore_window(archived_at, now);
        assert!(!restorable);
    }

    // ── Account-authority predicate (pure — no database needed) ────────────────
    //
    // `account_role_grants_authority` is exactly what `has_account_authority`
    // wraps with the `get_account_member_role` DB read — pinning it here pins
    // the owner/admin/member/none decision without a database. `'owner'`
    // covers the personal-account-owner case, `'admin'` covers the org-admin
    // case, `'member'` covers a team admin who holds no ACCOUNT-level role
    // beyond plain membership — see `has_account_authority`'s own doc
    // comment for why one rule has to cover both the personal and org planes.

    #[test]
    fn account_role_grants_authority_owner_and_admin_true() {
        assert!(account_role_grants_authority(Some("owner")));
        assert!(account_role_grants_authority(Some("admin")));
    }

    #[test]
    fn account_role_grants_authority_member_and_absent_false() {
        // A team admin (`team_members.role = 'admin'`) whose own
        // `account_members.role` is a plain `'member'` — the exact scenario
        // the workspace-manage design doc's "deliberate asymmetry" describes:
        // archiving does not imply seeing or restoring the archive.
        assert!(!account_role_grants_authority(Some("member")));
        // Not an account member at all.
        assert!(!account_role_grants_authority(None));
    }

    // ── Live Postgres: account authority + 120-day window, real round-trip ─────
    //
    // Gated on `RELINFER_TEST_DATABASE_URL`, same convention as
    // `workers::embed`'s live tests (see that module's own doc comment on
    // why): `has_account_authority`, `team_restore_window`, and
    // `Store::list_archived_teams`/`Store::team_archived_at` are all
    // crate-private or need a REAL `timestamptz` round-trip (not just
    // in-process `chrono` arithmetic) to mean anything, so there is nothing
    // for a separate `backend/tests/*_pg.rs` file to call — the live test
    // lives here instead.
    //
    // Deliberately does NOT go through the HTTP router / `require_auth`
    // middleware: that would need a real Clerk JWT verified against a JWKS
    // endpoint, which nothing in this crate's test harness sets up (every
    // OTHER test in this file that needs auth stops at 401/503 for exactly
    // that reason). This calls the private authorization/window helpers and
    // the `Store` methods the route handlers call, directly, against a real
    // database — the same depth of coverage, without needing a mock Clerk.
    #[tokio::test]
    async fn account_authority_and_restore_window_against_real_postgres() {
        use crate::store::{PostgresStore, Store};

        let url = match std::env::var("RELINFER_TEST_DATABASE_URL") {
            Ok(u) => u,
            Err(_) => {
                eprintln!(
                    "skipping: set RELINFER_TEST_DATABASE_URL to run the archived-teams/restore-window live test"
                );
                return;
            }
        };
        // Bound the connect attempt (CodeRabbit review, PR #190 precedent —
        // see `workers::embed`'s identical live test).
        const CONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);
        let pool = tokio::time::timeout(
            CONNECT_TIMEOUT,
            sqlx::postgres::PgPoolOptions::new().max_connections(4).connect(&url),
        )
        .await
        .unwrap_or_else(|_| panic!("connect to test Postgres timed out after {CONNECT_TIMEOUT:?}"))
        .expect("connect to test Postgres");

        // ── Seed: one account, an OWNER and a plain MEMBER — the plain
        // member stands in for "a team admin who is not owner/org-admin":
        // `has_account_authority` never looks at `team_members.role` at all,
        // only `account_members.role`, so a caller who is a team ADMIN but a
        // plain account MEMBER must still be refused. ──
        let owner_email = format!("{}@test.local", Uuid::new_v4());
        let owner_id: Uuid = sqlx::query_scalar("INSERT INTO users (email) VALUES ($1) RETURNING id")
            .bind(&owner_email)
            .fetch_one(&pool)
            .await
            .expect("insert owner user");
        let member_email = format!("{}@test.local", Uuid::new_v4());
        let member_id: Uuid = sqlx::query_scalar("INSERT INTO users (email) VALUES ($1) RETURNING id")
            .bind(&member_email)
            .fetch_one(&pool)
            .await
            .expect("insert member user");
        let account_id: Uuid =
            sqlx::query_scalar("INSERT INTO accounts (owner_user_id) VALUES ($1) RETURNING id")
                .bind(owner_id)
                .fetch_one(&pool)
                .await
                .expect("insert account");
        sqlx::query("INSERT INTO account_members (account_id, user_id, role) VALUES ($1, $2, 'owner')")
            .bind(account_id)
            .bind(owner_id)
            .execute(&pool)
            .await
            .expect("insert owner account_members row");
        sqlx::query("INSERT INTO account_members (account_id, user_id, role) VALUES ($1, $2, 'member')")
            .bind(account_id)
            .bind(member_id)
            .execute(&pool)
            .await
            .expect("insert member account_members row");

        // Built via the crate-root helper rather than assigning `state.db` /
        // `state.store` here: `scripts/check_no_ambient_pool.sh` forbids that
        // pattern anywhere under `routes/`, and it is a text grep that cannot
        // distinguish a test fixture from a handler. Allowlisting this file
        // would blind that guard to every future handler in it, so the fixture
        // is constructed outside `routes/` instead.
        let state = crate::test_app_state_owned_with_db_and_store(pool.clone(), None);

        // The `GET /v1/teams/archived` gate: an account OWNER — including on
        // a PERSONAL account, which has no org admin at all — succeeds; a
        // plain member (the team-admin-but-not-owner case) is refused. A
        // literal "must be an org admin" rule would have gotten the FIRST
        // assertion wrong on a personal account, which is exactly the bug
        // this design avoids.
        assert!(
            has_account_authority(&state, account_id, owner_id).await.unwrap(),
            "the account owner must hold account-level authority"
        );
        assert!(
            !has_account_authority(&state, account_id, member_id).await.unwrap(),
            "a plain account member must be refused, even if they are a TEAM admin elsewhere"
        );

        // ── The 120-day restore window, round-tripped through a REAL
        // `timestamptz` column rather than pure in-process arithmetic. ──
        let store = PostgresStore::new(pool.clone());
        let graph = store
            .create_graph(account_id, "archived-teams-window-e2e", "team")
            .await
            .expect("create graph");
        let team_id: Uuid = sqlx::query_scalar(
            "INSERT INTO teams (account_id, graph_id, name, created_by) \
             VALUES ($1, $2, $3, $4) RETURNING id",
        )
        .bind(account_id)
        .bind(graph.id)
        .bind("Archived Team Window E2E")
        .bind(owner_id)
        .fetch_one(&pool)
        .await
        .expect("insert team");

        // Day 119: still inside the window.
        sqlx::query("UPDATE graphs SET archived_at = now() - interval '119 days' WHERE id = $1")
            .bind(graph.id)
            .execute(&pool)
            .await
            .expect("archive at day 119");
        let archived_at = store
            .team_archived_at(team_id)
            .await
            .expect("team_archived_at (day 119)")
            .expect("must be archived");
        let (_, restorable) = team_restore_window(archived_at, Utc::now());
        assert!(restorable, "day 119 (real timestamptz round-trip) must be restorable");

        let archived = store
            .list_archived_teams(account_id)
            .await
            .expect("list_archived_teams (day 119)");
        let row = archived
            .iter()
            .find(|t| t.team_id == team_id)
            .expect("archived team must appear in the list at day 119");
        assert_eq!(row.graph_id, graph.id);
        assert_eq!(row.name, "archived-teams-window-e2e");

        // Day 121: past the window.
        sqlx::query("UPDATE graphs SET archived_at = now() - interval '121 days' WHERE id = $1")
            .bind(graph.id)
            .execute(&pool)
            .await
            .expect("archive at day 121");
        let archived_at = store
            .team_archived_at(team_id)
            .await
            .expect("team_archived_at (day 121)")
            .expect("must still be archived");
        let (_, restorable) = team_restore_window(archived_at, Utc::now());
        assert!(!restorable, "day 121 (real timestamptz round-trip) must be refused");

        // Still appears in the archived list — just no longer restorable.
        // `list_archived_teams` never filters on the window itself, it only
        // reports the raw `archived_at` for the caller to compute against
        // (see that method's own doc comment).
        let archived = store
            .list_archived_teams(account_id)
            .await
            .expect("list_archived_teams (day 121)");
        assert!(archived.iter().any(|t| t.team_id == team_id));

        // Best-effort cleanup — not asserted; a fresh random account/team
        // each run keeps re-runs independent regardless of whether this runs.
        let _ = sqlx::query("DELETE FROM teams WHERE id = $1")
            .bind(team_id)
            .execute(&pool)
            .await;
        let _ = sqlx::query("DELETE FROM graphs WHERE id = $1")
            .bind(graph.id)
            .execute(&pool)
            .await;
        let _ = sqlx::query("DELETE FROM account_members WHERE account_id = $1")
            .bind(account_id)
            .execute(&pool)
            .await;
        let _ = sqlx::query("DELETE FROM accounts WHERE id = $1")
            .bind(account_id)
            .execute(&pool)
            .await;
        let _ = sqlx::query("DELETE FROM users WHERE id = $1")
            .bind(owner_id)
            .execute(&pool)
            .await;
        let _ = sqlx::query("DELETE FROM users WHERE id = $1")
            .bind(member_id)
            .execute(&pool)
            .await;
    }

    // ── Not testable here: the org-admin RLS grant end to end ─────────────────
    //
    // "A verified org admin CAN read the roster/history of a team they are not
    // a member of, and CANNOT read that team's ocean content" is the property
    // this change exists to establish, and it is not assertable from this
    // module: it needs a real Postgres tenant database, the tenant migration
    // tree applied, and a connection as the non-`BYPASSRLS`
    // `coralswarm_tenant_app` role — none of which `test_app_state()` has (every
    // handler test above stops at 401/503 for exactly that reason). The harness
    // that does have all three is `backend/tests/tenant_rls_pg.rs`
    // (`ORGT_TEST_BASE_URL`-gated), and that is where the assertion belongs:
    // set `app.is_org_admin`, prove `team_members`/`membership_events`/`teams`
    // rows for a non-member team become visible, and prove in the same
    // transaction that `atoms`/`nodes`/`team_keys` for that team's ocean stay
    // invisible. That file is owned elsewhere in this PR; see this change's
    // notes for the exact assertions it needs (including the policy-count
    // invariant, which this migration deliberately leaves at 39 by replacing
    // policies in place rather than adding new ones).

    // Note: the ORG_TEAMS_ENABLED raw-flag parse tests that used to live here
    // (`parse_org_teams_flag`) were removed along with that function — the
    // parse contract is now owned exclusively by
    // `workers::member_ocean_reconciler::org_teams_enabled`/
    // `parse_enabled_flag`, whose own test module asserts it (see that
    // file's `flag_parsing` tests). `org_teams_active_for`'s composed
    // MEMBER_OCEANS_ENABLED coupling reads a registry lookup plus a live env
    // var end-to-end and
    // is intentionally not unit-tested here — mutating global env state from
    // a `#[test]` risks racing other tests in the same process, the same
    // reasoning that motivated hoisting `parse_enabled_flag`/
    // `parse_org_teams_flag` out as pure functions in the first place.
}

/// `GET /v1/orgs/members`'s `connected_providers`/`connect_nudged_at` shape
/// — CS-436 (org-onboarding plan §3 item 49, D21). A REAL Postgres suite
/// (`ORGT_TEST_BASE_URL`-gated), in-crate rather than under `backend/tests/`
/// because [`list_org_members`] is only `pub`, not re-exported, and this
/// module's `mod tests` above already establishes the "mutate global env
/// state, gate on ORGT_TEST_BASE_URL" pattern is fine for a `#[cfg(test)]`
/// module in THIS file specifically (unlike the flag-parsing tests just
/// above, which deliberately avoid it). Handlers are called DIRECTLY
/// (`list_org_members(State(state), tenant, Extension(user)).await`), same
/// shape `routes::org_invites::pg_tests` uses — no `require_auth`
/// middleware, no Clerk JWT needed, since `Tenant`/`Extension<UserContext>`
/// are exactly what that middleware would have produced.
#[cfg(test)]
mod pg_tests {
    use super::*;
    use sqlx::postgres::PgPoolOptions;
    use sqlx::{Connection, Executor, PgConnection, PgPool};
    use tokio::sync::Mutex as AsyncMutex;

    static ENV_LOCK: AsyncMutex<()> = AsyncMutex::const_new(());

    fn base_url() -> Option<String> {
        std::env::var("ORGT_TEST_BASE_URL").ok()
    }
    fn with_db(base: &str, db: &str) -> String {
        let (prefix, rest) = base.split_at(base.rfind('/').expect("url has a path /"));
        let params = rest.find('?').map(|i| &rest[i..]).unwrap_or("");
        format!("{prefix}/{db}{params}")
    }
    async fn admin_conn(base: &str) -> PgConnection {
        PgConnection::connect(base).await.expect("connect admin")
    }
    async fn recreate(conn: &mut PgConnection, db: &str) {
        conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .unwrap();
        conn.execute(format!("CREATE DATABASE {db}").as_str())
            .await
            .unwrap();
    }
    async fn control_pool(base: &str, db: &str) -> PgPool {
        let pool = PgPoolOptions::new().connect(&with_db(base, db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();
        pool
    }

    /// `ORG_TEAMS_ENABLED`/`MEMBER_OCEANS_ENABLED` — both required for
    /// [`org_teams_active_for`] to pass (see that function's own doc
    /// comment: independently toggleable, both must be on). Same
    /// `EnvGuard`-on-`Drop` convention every `*_pg.rs` file in this crate
    /// uses for process-global env vars.
    #[allow(dead_code)]
    struct EnvGuard(tokio::sync::MutexGuard<'static, ()>);
    impl EnvGuard {
        async fn acquire() -> Self {
            let guard = ENV_LOCK.lock().await;
            // SAFETY: serialized by `guard`, held for this struct's whole
            // lifetime (including on panic/unwind, via `Drop` below).
            unsafe {
                std::env::set_var("ORG_TEAMS_ENABLED", "1");
                std::env::set_var("MEMBER_OCEANS_ENABLED", "1");
            }
            Self(guard)
        }
    }
    impl Drop for EnvGuard {
        fn drop(&mut self) {
            // SAFETY: see `acquire` — still serialized by `self.0` here.
            unsafe {
                std::env::remove_var("ORG_TEAMS_ENABLED");
                std::env::remove_var("MEMBER_OCEANS_ENABLED");
            }
        }
    }

    fn app_state(control: PgPool) -> Arc<AppState> {
        Arc::new(crate::test_app_state_owned_with_db_and_store(control, None))
    }

    fn org_tenant(control: PgPool, org_id: &str, account_id: Uuid, user_id: Uuid) -> Tenant {
        let ctx = TenantContext {
            org_id: org_id.to_string(),
            account_id,
            user_id,
            pool: control,
            page_pool: None,
        };
        Tenant {
            store: None,
            pool: None,
            decision: Some(crate::tenancy::runtime::TenancyDecision::Org(ctx)),
            class: crate::route_class::RouteClass::Other,
        }
    }

    fn admin_ctx(account_id: Uuid, user_id: Uuid) -> UserContext {
        UserContext {
            user_id,
            account_id,
            role: "admin".to_string(),
            device_id: "teams-pg-test".to_string(),
        }
    }

    async fn mint_org(control: &PgPool, org_id: &str) -> Uuid {
        let owner: Uuid = sqlx::query_scalar("INSERT INTO users(email) VALUES ($1) RETURNING id")
            .bind(format!("{org_id}-owner@teams-pg.test"))
            .fetch_one(control)
            .await
            .unwrap();
        sqlx::query_scalar("INSERT INTO accounts(owner_user_id, clerk_org_id) VALUES ($1, $2) RETURNING id")
            .bind(owner)
            .bind(org_id)
            .fetch_one(control)
            .await
            .unwrap()
    }

    async fn add_member(control: &PgPool, account_id: Uuid, email: &str) -> Uuid {
        let user_id: Uuid = sqlx::query_scalar("INSERT INTO users(email) VALUES ($1) RETURNING id")
            .bind(email)
            .fetch_one(control)
            .await
            .unwrap();
        // `seat_tier_id` is required explicitly since PR F (seat lifecycle) —
        // `account_members_default_seat_tier()` no longer defaults it
        // silently (same pattern `routes::org_invites::pg_tests::seed_org` uses).
        sqlx::query(
            "INSERT INTO account_members(account_id, user_id, role, seat_tier_id) \
             VALUES ($1, $2, 'member', (SELECT id FROM seat_tiers WHERE code = 'standard' AND is_active = true))",
        )
        .bind(account_id)
        .bind(user_id)
        .execute(control)
        .await
        .unwrap();
        user_id
    }

    #[tokio::test]
    async fn connected_providers_and_connect_nudged_at_are_populated_correctly() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping connected_providers_and_connect_nudged_at_are_populated_correctly: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let _env = EnvGuard::acquire().await;
        let db = "orgt_teams_connect1_ctl";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let mut conn = admin_conn(&base).await;
        recreate(&mut conn, db).await;
        let control = control_pool(&base, db).await;

        let org_id = format!("org_{}", Uuid::new_v4());
        let account_id = mint_org(&control, &org_id).await;
        let admin_id = add_member(&control, account_id, "admin@teams-pg.test").await;
        let connected_id = add_member(&control, account_id, "connected@teams-pg.test").await;
        let unconnected_id = add_member(&control, account_id, "unconnected@teams-pg.test").await;

        // `connected_id` owns a Google token — must appear in
        // `connected_providers`.
        sqlx::query(
            "INSERT INTO oauth_tokens \
                 (account_id, provider, scope, access_token, owner_user_id) \
             VALUES ($1, 'google', 'scope', $2, $3)",
        )
        .bind(account_id)
        .bind(b"ciphertext".to_vec())
        .bind(connected_id)
        .execute(&control)
        .await
        .unwrap();

        // `unconnected_id` was already nudged once — must surface on
        // `connect_nudged_at`.
        sqlx::query(
            "INSERT INTO slack_unmatched_email_notices \
                 (account_id, provider, email, last_notified_at, unsubscribe_token) \
             VALUES ($1, 'member_connect', 'unconnected@teams-pg.test', now() - INTERVAL '3 days', 'tok-fixed')",
        )
        .bind(account_id)
        .execute(&control)
        .await
        .unwrap();

        let state = app_state(control.clone());
        let tenant = org_tenant(control.clone(), &org_id, account_id, admin_id);
        let user = admin_ctx(account_id, admin_id);

        let (status, Json(body)) =
            list_org_members(axum::extract::State(state), tenant, axum::extract::Extension(user)).await;
        assert_eq!(status, StatusCode::OK);
        let members = body.as_array().expect("roster is a JSON array");
        assert_eq!(members.len(), 3, "admin + connected + unconnected");

        let find = |id: Uuid| {
            members
                .iter()
                .find(|m| m["user_id"].as_str() == Some(&id.to_string()))
                .unwrap_or_else(|| panic!("member {id} missing from roster"))
        };

        let connected = find(connected_id);
        assert_eq!(
            connected["connected_providers"].as_array().unwrap(),
            &[Value::String("google".to_string())],
        );
        assert!(connected["connect_nudged_at"].is_null());

        let unconnected = find(unconnected_id);
        assert_eq!(
            unconnected["connected_providers"].as_array().unwrap().len(),
            0,
            "an unconnected member gets [], never null"
        );
        assert!(
            unconnected["connect_nudged_at"].is_string(),
            "the prior nudge's last_notified_at must surface"
        );

        // The admin themself owns no token and was never nudged.
        let admin_row = find(admin_id);
        assert_eq!(admin_row["connected_providers"].as_array().unwrap().len(), 0);
        assert!(admin_row["connect_nudged_at"].is_null());
    }

    #[tokio::test]
    async fn non_admin_caller_never_sees_either_field() {
        let Some(base) = base_url() else {
            eprintln!("skipping non_admin_caller_never_sees_either_field: set ORGT_TEST_BASE_URL");
            return;
        };
        let _env = EnvGuard::acquire().await;
        let db = "orgt_teams_connect2_ctl";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let mut conn = admin_conn(&base).await;
        recreate(&mut conn, db).await;
        let control = control_pool(&base, db).await;

        let org_id = format!("org_{}", Uuid::new_v4());
        let account_id = mint_org(&control, &org_id).await;
        let member_id = add_member(&control, account_id, "plain@teams-pg.test").await;

        let state = app_state(control.clone());
        let tenant = org_tenant(control.clone(), &org_id, account_id, member_id);
        let user = UserContext {
            user_id: member_id,
            account_id,
            role: "member".to_string(),
            device_id: "teams-pg-test".to_string(),
        };

        let (status, Json(body)) =
            list_org_members(axum::extract::State(state), tenant, axum::extract::Extension(user)).await;
        assert_eq!(status, StatusCode::OK);
        let members = body.as_array().unwrap();
        assert_eq!(members.len(), 1);
        let row = &members[0];
        assert!(row.get("connected_providers").is_none());
        assert!(row.get("connect_nudged_at").is_none());
        // Exact pre-org-invites-train shape, unchanged by this story.
        assert!(row.get("pending").is_none());
    }
}

//! Consumer-plane RLS scoping (org-tenancy PR 3.4).
//!
//! Migration `0083_consumer_rls` adds Postgres Row-Level Security policies to
//! the consumer plane's content tables, keyed by the session GUC
//! `app.account_id`. This module is the ONE place `SET LOCAL app.account_id`
//! gets issued, so that with OPS-4 having switched the app's connection role
//! to the non-`BYPASSRLS` `coralswarm_app` (run and verified in production
//! 2026-08-04), the policies actually scope real connections instead of being
//! backstop-in-name-only.
//!
//! **Why this is a function taking `&PgPool`, not a method hung off a single
//! "connection acquisition" choke point on [`crate::store::PostgresStore`]:**
//! there isn't one to hang it off. `PostgresStore` has no per-request held
//! connection — nearly every one of its ~170 query call sites (`grep -c
//! '&self.pool\|self.pool.begin' backend/src/store/mod.rs`) executes
//! directly against `&self.pool`, which sqlx checks out a connection for,
//! runs one statement/transaction on, and returns to the pool immediately.
//! There is no single method through which "the app acquires a connection for
//! this request" already flows — retrofitting every call site to route
//! through one is a mechanical migration on the scale of PRs 1.4/1.5/1.6 (the
//! route-chunk migrations), not something that belongs in a migration-landing
//! PR that must stay inert and byte-identical.
//!
//! So instead of scattering ad hoc `SET LOCAL`/`set_config` calls across
//! routes (or silently doing nothing until a much larger follow-up PR), this
//! module gives the ONE reusable primitive future call sites should use, and
//! wires it into the closest thing this codebase has to a per-request
//! "which pool do I use" choke point: [`crate::tenancy::extractor::Tenant`],
//! the axum extractor every route already asks for its content-plane
//! pool/store (see its own module doc: "the one place a handler resolves
//! which pool/store it should actually use"). [`Tenant::begin_account_scoped`]
//! landed additive — at the time no call site invoked it, so the landing PR's
//! behavior (every route still calling `Store` methods against the ambient
//! `state.db`/`ctx.pool` exactly as before) was unchanged bit-for-bit. Wiring
//! individual routes/workers to actually use it (replacing their ad hoc
//! `&self.pool`/`self.pool.begin()` calls with a scoped transaction) was
//! deliberately left as later, incremental work, gated on OPS-4 actually
//! running — there was no value in scoping connections against a role that
//! ignores RLS, and every such conversion was itself a small, reviewable
//! diff rather than one big bang. Both halves have since happened: the
//! conversions shipped (org-tenancy phases B.x; `scripts/rls_coverage.manifest`
//! is the per-file ledger) and OPS-4 has run, so these scopes now enforce.
//!
//! # Both planes, two different boundaries (tenant RLS, migration
//! # `migrations_tenant/0009_tenant_rls`)
//!
//! This module used to be consumer-plane-only, on the rationale that "tenant
//! DBs get whole-database isolation, not RLS". That is still true of the
//! CROSS-ORG boundary — a tenant database holds exactly one org, so there is no
//! second org's row for a policy to hide — but it was never true of the
//! boundary INSIDE a tenant database. Every member-private ocean in an org
//! shares that org's single `account_id`, so `app.account_id` cannot tell one
//! member's reef from another's, and migration `0009_tenant_rls` adds
//! member-ocean policies keyed on a NEW session GUC:
//!
//! * `app.account_id` — the consumer plane's account boundary, unchanged.
//! * `app.user_id`    — the tenant plane's member boundary. The acting end
//!   user's uuid. Unset (or `''`) means "no member-private ocean is visible",
//!   which is fail-closed on exactly the rows the boundary protects and leaves
//!   org-commons rows readable — see 0009's §3 for why this is `missing_ok`
//!   here while `app.account_id` is deliberately strict on the consumer plane.
//! * `app.tenant_actor` — `'system'`, and nothing else. The explicit service
//!   principal for background work that is not acting as any one member (see
//!   [`begin_tenant_system_scoped`], and 0009's §4 for the full cost/benefit).
//! * `app.is_org_admin` — `'true'`, and nothing else. The GOVERNANCE-table
//!   grant for a verified org admin (org-team-membership design doc decision
//!   #4). Deliberately NOT a content grant: see
//!   [`set_tenant_org_admin`]/[`OrgAdminProof`] below and migration
//!   `0032_membership_events`, which is the only migration whose policies read
//!   it. Unset (or `''`) means "not an org admin", which is what every
//!   non-governance request sets it to by simply never touching it.
//!
//! # The GUC contract, and which phase implements it
//!
//! Tenant RLS ships in THREE phases, and the primitives below are the fixed
//! interface between them. Full detail — including the Phase-2 call-site
//! inventory and the Phase-3 cutover — is in
//! `docs/tenant-rls-cutover-runbook.md`.
//!
//! * **Phase 1 (shipped with `0009_tenant_rls`)** — the role, the policies, and
//!   these functions. Each landed ADDITIVELY, with zero call sites anywhere in
//!   `backend/src` — exactly as [`begin_account_scoped`] itself landed alongside
//!   `0083_consumer_rls` — so Phase 1 was bit-for-bit behaviour-preserving.
//!   That "nothing calls these yet" is no longer true: Phase 2 below has
//!   shipped, and [`begin_tenant_scoped`], [`begin_tenant_system_scoped`] and
//!   `Tenant::begin_member_scoped` all have real call sites now (see the latter's
//!   own doc for the current count, and the runbook's §2.5 for the per-file
//!   inventory). [`set_tenant_user`] is still called only from inside this
//!   module — by [`begin_tenant_scoped`] and `Tenant::begin_member_scoped`,
//!   which is the intended shape.
//! * **Phase 2 (separate PR)** — convert the tenant-plane call sites to set
//!   `app.user_id`. The contract they must satisfy:
//!
//!   | GUC                | Type / value                        | Set by                                              | Unset means |
//!   |--------------------|-------------------------------------|-----------------------------------------------------|-------------|
//!   | `app.user_id`      | uuid, as text; the ACTING END USER  | [`set_tenant_user`], [`begin_tenant_scoped`], [`Tenant::begin_member_scoped`] | no member/team ocean is visible |
//!   | `app.tenant_actor` | exactly `"system"`, or absent       | [`begin_tenant_system_scoped`], [`begin_tenant_system_scoped_without_account`] | not the service principal |
//!   | `app.is_org_admin` | exactly `"true"`, or absent         | [`set_tenant_org_admin`], [`Tenant::begin_governance_scoped`] | not an org admin; governance tables gate on team membership only |
//!
//!   `app.user_id` MUST be the gate-verified
//!   [`super::runtime::TenantContext::user_id`], never a handler parameter.
//!   `app.is_org_admin` carries the SAME discipline, enforced structurally
//!   rather than by convention: [`set_tenant_org_admin`] cannot be called
//!   without an [`OrgAdminProof`], and the only way to obtain one is
//!   [`verify_org_admin`], which performs the CONTROL-plane check itself. No
//!   handler can therefore set it from a request body, a header, a path
//!   parameter, or a boolean it computed some other way.
//!   All three are transaction-local (`set_config(..., true)`), so all three
//!   REQUIRE an open transaction — a query executed directly against a pooled
//!   `&PgPool` cannot carry them at all. Any such path must either move inside
//!   a transaction or be listed as a known Phase-3 gap.
//! * **Phase 3 (ops, "OPS-4-T")** — repoint each tenant DSN at the
//!   NOBYPASSRLS role. Only valid once Phase 2 is proven.
//!
//! Both GUCs are inert until Phase 3: the app connects as an owner/`BYPASSRLS`
//! role today, and `BYPASSRLS` skips policy evaluation entirely.

use sqlx::{PgPool, Postgres, Transaction};
use uuid::Uuid;

use crate::route_class::RouteClass;
use crate::store::StoreError;

use super::runtime::TenancyDecision;

/// The GUC name `tenant_rls_is_system()` (migration `0008_tenant_rls`) reads.
/// Named as a constant rather than inlined so no `set_config` call site can
/// drift from the migration's spelling.
///
/// There is exactly ONE writer of this GUC now:
/// [`begin_tenant_system_scoped`]/[`begin_tenant_system_scoped_without_account`],
/// both `SET LOCAL`. This doc used to name a second —
/// [`super::registry::TenantRegistry::get_or_build_worker_pool`]'s
/// connection-startup `-c app.tenant_actor=system` option. PR #388 deleted
/// that option (a connection pooler rejects unknown startup parameters), so
/// no connection anywhere carries this GUC at the session level.
pub const TENANT_ACTOR_GUC: &str = "app.tenant_actor";

/// The ONLY value `app.tenant_actor` recognises. Anything else — including
/// unset and `''` — is not the system principal (`tenant_rls_is_system()` in
/// migration `0009_tenant_rls`).
pub const TENANT_ACTOR_SYSTEM: &str = "system";

/// The GUC name `tenant_rls_is_org_admin()` (migration
/// `0032_membership_events`) reads. Same constant-not-literal reasoning as
/// [`TENANT_ACTOR_GUC`]: the setter below and the migration's reader must
/// never drift apart on the spelling.
pub const ORG_ADMIN_GUC: &str = "app.is_org_admin";

/// The ONLY value `app.is_org_admin` recognises. Anything else — including
/// unset, `''`, `'t'` and `'TRUE'` — is not an org admin
/// (`tenant_rls_is_org_admin()` compares the raw text, exactly like
/// `tenant_rls_is_system()`, so an unexpected value can never raise inside a
/// policy the way a `::boolean` cast would).
const ORG_ADMIN_TRUE: &str = "true";

/// Proof that the acting user has been verified — against the CONTROL plane,
/// in this process, for this request — to be an ORG ADMIN of the account whose
/// governance tables are about to be read or written.
///
/// **This type is the whole security argument for `app.is_org_admin`.** It has
/// a private, zero-sized field and no public constructor, so the only way any
/// module outside this one can obtain a value is to call [`verify_org_admin`]
/// and get back a `Some`. [`set_tenant_org_admin`] demands one by value.
/// Together that makes "the GUC was set without the control-plane check having
/// passed" not a discipline anyone has to remember, but a state the type
/// system refuses to construct — the same structural fail-closed shape
/// [`super::reachability`]'s `require_matching_user` uses to keep a
/// caller-supplied `user_id` out of a verified `TenantContext`.
///
/// `Copy` deliberately: a handler typically tests it (`.is_some()`) for the
/// route-layer authorization decision AND hands it to the transaction opener,
/// and forcing a clone/borrow dance there would only invite someone to reach
/// for a `bool` instead.
#[derive(Debug, Clone, Copy)]
pub struct OrgAdminProof(());

/// Run the org-admin check against the CONTROL plane and mint an
/// [`OrgAdminProof`] if it passes.
///
/// The predicate is org-team-membership design doc decision #5's
/// `is_org_admin`: the account must BE an org (`accounts.clerk_org_id IS NOT
/// NULL` — a personal account can never mint a proof, however its
/// `account_members.role` reads) and the user must hold `owner` or `admin` on
/// it. `accounts`/`account_members` are control-tree tables with no tenant
/// counterpart, which is why this takes the control pool explicitly rather
/// than reaching through a `Tenant`.
///
/// Errors propagate; they are NOT collapsed into "not an admin". A caller that
/// cannot tell an outage from a denial will eventually report one as the
/// other — see `routes::teams`'s wrapper, which turns this into a 503 rather
/// than a silent 403.
pub async fn verify_org_admin(
    control_pool: &PgPool,
    account_id: Uuid,
    user_id: Uuid,
) -> Result<Option<OrgAdminProof>, StoreError> {
    let mut control = begin_request_tenant_read(control_pool, account_id, user_id).await?;
    let proof = verify_org_admin_tx(&mut control, account_id, user_id).await?;
    control.commit().await?;
    Ok(proof)
}
pub(crate) async fn verify_org_admin_tx(control:&mut sqlx::PgConnection,account_id:Uuid,user_id:Uuid)->Result<Option<OrgAdminProof>,StoreError>{
    let found: Option<bool> = sqlx::query_scalar::<_, bool>(
        "SELECT true FROM accounts a \
         JOIN account_members am ON am.account_id = a.id \
         WHERE a.id = $1 AND am.user_id = $2 \
           AND a.clerk_org_id IS NOT NULL \
           AND am.role IN ('owner', 'admin') \
         LIMIT 1",
    )
    .bind(account_id)
    .bind(user_id)
    .fetch_optional(control)
    .await?;
    // Settings launch PR 2.8: no org-admin proof for a non-interactive
    // credential (`middleware::auth::authority_allowed`). Consulted only for
    // a real admin, so a plain member's refusal keeps its own answer.
    if found.is_some() && !crate::middleware::auth::authority_allowed() {
        return Ok(None);
    }
    Ok(found.map(|_| OrgAdminProof(())))
}

/// Set `app.is_org_admin` on an already-open TENANT transaction, and prove the
/// database actually honours it before returning.
///
/// # What this grants, and what it deliberately does not
///
/// Migration `0032_membership_events` adds `OR tenant_rls_is_org_admin()` to
/// the `USING` clause of exactly three policies: `teams`, `team_members` and
/// `membership_events` — the GOVERNANCE tables the roster/audit endpoints
/// read and write. It does NOT touch `tenant_rls_visible_graph_ids()`, which
/// is the CONTENT anchor every ocean-content table (`atoms`, `nodes`,
/// `conversations`, …) resolves through. So this GUC lets a non-member org
/// admin see and manage WHO IS IN a team; it does not let them read one atom
/// of that team's ocean. That split is design doc decision #4 ("roster
/// management on any team, NO read bypass") expressed as a policy predicate,
/// and it is the reason this is a second, narrow GUC instead of an OR-arm on
/// the visibility function.
///
/// `teams`' widened arm is `USING`-only — its `WITH CHECK` stays exactly
/// 0009's membership predicate — so an org admin can SELECT (and `FOR UPDATE`
/// lock) any team row in the tenant but can never INSERT one, nor leave an
/// UPDATEd one, pointing at an ocean they cannot reach. Break-glass stays what
/// the design says it is: add yourself to the roster, which is a visible
/// `team_members` row plus a `membership_events` audit row.
///
/// # Why it re-reads the value it just set
///
/// A `set_config` on an unregistered `app.*` placeholder GUC always succeeds,
/// whether or not any policy in this database reads it. If the tenant DB is
/// behind on migrations (0032 not applied), the grant is silently inert: the
/// route would authorize the caller, the scoped read would return nothing, and
/// the handler would answer "this team has no members" — the exact silent
/// wrong answer this whole change exists to remove. Calling the reader back
/// converts that into a loud failure at the top of the request:
/// `tenant_rls_is_org_admin()` does not exist on a pre-0032 database, so the
/// statement errors (42883) and the caller gets a 500 instead of an empty
/// list. One extra round trip, on governance requests only.
///
/// Same `set_config(..., true)` / `SET LOCAL` semantics as every other GUC
/// here: transaction-local, reset at COMMIT/ROLLBACK, so a pooled connection
/// reused afterwards by an ordinary member's request never inherits it.
pub async fn set_tenant_org_admin(
    tx: &mut Transaction<'static, Postgres>,
    _proof: OrgAdminProof,
) -> Result<(), StoreError> {
    sqlx::query(&format!("SELECT set_config('{ORG_ADMIN_GUC}', $1, true)"))
        .bind(ORG_ADMIN_TRUE)
        .execute(&mut **tx)
        .await?;

    let in_effect = sqlx::query_scalar::<_, bool>("SELECT tenant_rls_is_org_admin()")
        .fetch_one(&mut **tx)
        .await
        .map_err(|e| {
            StoreError::TenantAuthzMismatch(format!(
                "tenant database does not honour the org-admin governance grant \
                 (tenant_rls_is_org_admin() unreadable — is migration \
                 0032_membership_events applied to this tenant?): {e}"
            ))
        })?;
    if !in_effect {
        return Err(StoreError::TenantAuthzMismatch(format!(
            "set_config('{ORG_ADMIN_GUC}', '{ORG_ADMIN_TRUE}') did not satisfy \
             tenant_rls_is_org_admin() — the GUC name or its recognised value has drifted \
             between backend/src/tenancy/scope.rs and migrations_tenant/0032_membership_events.sql"
        )));
    }
    Ok(())
}

/// Opens a transaction on `pool` with the Postgres session GUC
/// `app.account_id` set for the lifetime of that transaction.
///
/// Uses `SELECT set_config('app.account_id', $1, true)` rather than the
/// (syntactically invalid for bind parameters) `SET LOCAL app.account_id =
/// $1` — `set_config`'s third argument `true` gives `SET LOCAL` semantics:
/// the value is visible only within the current transaction and is reset
/// automatically at `COMMIT`/`ROLLBACK`, which is exactly what makes this
/// safe to call on a connection that will be returned to the pool and reused
/// by a wholly different request/account afterward (no cross-request leakage
/// of one account's scope onto another's connection).
///
/// **This is live enforcement, not a no-op.** It WAS a pure no-op while the
/// app connected as an owner/`BYPASSRLS` role (`BYPASSRLS` skips RLS policy
/// evaluation entirely, so `set_config('app.account_id', ...)` had precisely
/// zero effect on which rows any query returned). OPS-4 has since switched
/// the app's connection role to the non-`BYPASSRLS` `coralswarm_app`
/// (migration `0083_consumer_rls`; verified in production 2026-08-04), so
/// every content-table policy's `current_setting('app.account_id')::uuid`
/// predicate now reads the value this function sets — and an RLS-covered
/// query that runs OUTSIDE such a transaction hard-errors rather than
/// silently returning all rows.
pub async fn begin_account_scoped(
    pool: &PgPool,
    account_id: Uuid,
) -> Result<Transaction<'static, Postgres>, StoreError> {
    let account_id = account_id.to_string();
    begin_with_local_settings(
        pool,
        &[
            (SEARCH_PATH_GUC, SEARCH_PATH_DEFAULT),
            (ACCOUNT_GUC, &account_id),
        ],
    )
    .await
}

/// [`begin_account_scoped`], plus the tenant plane's member-ocean identity.
///
/// Use this for any transaction on an ORG TENANT pool that acts on behalf of a
/// specific end user. `user_id` must be the gate-verified
/// [`super::runtime::TenantContext::user_id`] — the one user the request's
/// context is valid for — never a caller-supplied id (the same invariant
/// [`super::reachability`]'s `require_matching_user` enforces structurally).
///
/// Passing the wrong user here does not widen anything: `0009_tenant_rls`'s
/// predicate can only ever hide rows, so a mismatched id yields "member ocean
/// not found", never another member's reef. It is still a bug, and still worth
/// getting right at the call site rather than relying on the policy to catch
/// it.
pub async fn begin_tenant_scoped(
    pool: &PgPool,
    account_id: Uuid,
    user_id: Uuid,
) -> Result<Transaction<'static, Postgres>, StoreError> {
    let account_id = account_id.to_string();
    let user_id = user_id.to_string();
    begin_with_local_settings(
        pool,
        &[
            (SEARCH_PATH_GUC, SEARCH_PATH_DEFAULT),
            (ACCOUNT_GUC, &account_id),
            (USER_GUC, &user_id),
        ],
    )
    .await
}

/// A transaction on an org tenant pool running as the TENANT SERVICE PRINCIPAL
/// — i.e. with `0009_tenant_rls`'s member-ocean policies satisfied for EVERY
/// ocean in the tenant, not just one member's.
///
/// **This is a deliberate, opt-in bypass of the member-ocean backstop.** It
/// exists because background work — the embed/extract/summary workers, the
/// member-ocean reconciler, purge, export — legitimately acts across every
/// ocean in a tenant and has no honest `user_id` to present. `grep -rn
/// begin_tenant_system_scoped backend/src` is intended to remain a complete and
/// short list of every place the backstop is stood down (this now includes
/// [`begin_tenant_system_scoped_without_account`] below, whose name deliberately
/// contains this same substring for exactly that reason); adding a call here
/// is a security-relevant change and should be reviewed as one.
///
/// If a call site DOES know which member's ocean it is working on — most job
/// payloads carry a `graph_id`, and a graph carries `owner_user_id` — prefer
/// [`begin_tenant_scoped`] with that owner. This function is the fallback for
/// work that is genuinely tenant-wide, not a shortcut around resolving the
/// user.
pub async fn begin_tenant_system_scoped(
    pool: &PgPool,
    account_id: Uuid,
) -> Result<Transaction<'static, Postgres>, StoreError> {
    let account_id = account_id.to_string();
    begin_with_local_settings(
        pool,
        &[
            (SEARCH_PATH_GUC, SEARCH_PATH_DEFAULT),
            (ACCOUNT_GUC, &account_id),
            (TENANT_ACTOR_GUC, TENANT_ACTOR_SYSTEM),
        ],
    )
    .await
}

/// [`begin_tenant_system_scoped`] minus the `app.account_id` half — sets ONLY
/// `app.tenant_actor = 'system'` on a fresh transaction, for a caller that has
/// no `accounts.id` to give the bundled version and no use for one.
///
/// **Moved here from `workers::mcp_task_sweep` (adversarial review round 1,
/// LOW) to restore the audit invariant `begin_tenant_system_scoped`'s own doc
/// comment states**: `grep -rn begin_tenant_system_scoped backend/src` is
/// supposed to be a complete, reviewable list of every place the member-ocean
/// backstop is stood down. A same-shaped helper living in a different module
/// under a different name would satisfy that invariant only by prose (a
/// module doc explaining the equivalence), which is exactly the kind of gap
/// that erodes an audit grep over time. This function's name deliberately
/// CONTAINS `begin_tenant_system_scoped` as a substring so the same grep
/// keeps catching it.
///
/// **Why a separate function instead of making `account_id` optional on the
/// one above:** every existing caller of `begin_tenant_system_scoped` DOES
/// have a real `account_id` and needs `app.account_id` set for OTHER tables'
/// policies in the same transaction (seat grants, Slack connector writes,
/// membership events, …) — threading an `Option<Uuid>` through all of them
/// for the sake of one caller that has neither the value nor the need would
/// only make the common case worse to read for no shared benefit. The two
/// functions share every line except the one `begin_account_scoped` call,
/// which is the entire point of the split.
///
/// The one caller today is `workers::mcp_task_sweep`'s org-tenant-plane tick:
/// `WorkerTenant` carries a `clerk_org_id` (`TenantId::Org`), never an
/// `accounts.id`, and `mcp_tasks`'s tenant-plane `member_ocean_isolation`
/// policy (migration `0047_mcp_tasks`) is anchored purely on
/// `tenant_rls_visible_graph_ids()`, which never reads `app.account_id` at
/// all (there is exactly one account's rows in a tenant database to begin
/// with — that column is informational there, not a policy anchor).
pub async fn begin_tenant_system_scoped_without_account(
    pool: &PgPool,
) -> Result<Transaction<'static, Postgres>, StoreError> {
    begin_with_local_settings(
        pool,
        &[
            (SEARCH_PATH_GUC, SEARCH_PATH_DEFAULT),
            (TENANT_ACTOR_GUC, TENANT_ACTOR_SYSTEM),
        ],
    )
    .await
}

/// `search_path`, pinned to the Postgres default (`"$user", public`) on
/// every scoped transaction.
///
/// Neon’s transaction pooler can hand out a server backend whose
/// `search_path` was emptied. Unqualified names (`atoms`, `jobs`,
/// `resolve_tenant_reachable_graphs`) then fail with "does not exist"
/// even though the objects are in `public`. Startup `-c search_path=` is
/// rejected by that pooler (same class as `app.tenant_actor` — see
/// [`set_request_statement_timeout`]). `set_config(..., true)` is SET LOCAL,
/// so it is pooler-safe and does not leak onto the next borrower.
/// Keep [`SEARCH_PATH_DEFAULT`] identical to the role/database default
/// (`ALTER ROLE … SET search_path = "$user", public`).
const SEARCH_PATH_GUC: &str = "search_path";
const SEARCH_PATH_DEFAULT: &str = "\"$user\", public";
/// `app.account_id` — see [`begin_account_scoped`].
const ACCOUNT_GUC: &str = "app.account_id";
/// `app.user_id` — see [`set_tenant_user`].
const USER_GUC: &str = "app.user_id";
/// `statement_timeout` — see [`set_request_statement_timeout`].
const STATEMENT_TIMEOUT_GUC: &str = "statement_timeout";

/// Open a transaction on `pool` and apply every transaction-local GUC it
/// needs in ONE statement — `BEGIN` plus a single
/// `SELECT set_config($1, $2, true), set_config($3, $4, true), …` — rather
/// than one round trip per setting. A scoped request transaction used to
/// cost `BEGIN` + three or four separate `set_config` statements before its
/// first real query; through Neon's pooler each of those is a full network
/// round trip, paid on every request.
///
/// Every setting is still `set_config(..., true)` (SET LOCAL): nothing here
/// changes WHICH GUCs are set or their scope, only how many statements carry
/// them. Both the GUC name and its value are bind parameters — never
/// interpolated — so no text reaches the statement unquoted. The names are
/// compile-time constants from this module anyway; binding them keeps that
/// true for any future caller too.
async fn begin_with_local_settings(
    pool: &PgPool,
    settings: &[(&str, &str)],
) -> Result<Transaction<'static, Postgres>, StoreError> {
    begin_class_with_local_settings(pool, RouteClass::Other, settings).await
}

/// [`begin_with_local_settings`] shaped by the request's route class
/// (`docs/plans/page-load-standard/design.md` §4.3): a `PageRead` opens
/// `BEGIN READ ONLY`, so a write inside a page read fails at the database
/// (SQLSTATE 25006) instead of landing from the page lane; every other class
/// opens exactly today's `BEGIN`. The statement timeout is the caller's to
/// put in `settings`, because the consumer plane sets none for `Other`.
async fn begin_class_with_local_settings(
    pool: &PgPool,
    class: RouteClass,
    settings: &[(&str, &str)],
) -> Result<Transaction<'static, Postgres>, StoreError> {
    // Server-Timing `tx_open`: connection acquire + BEGIN + the one
    // set_config statement. Measured here so every scoped opener
    // (member, account, tenant) is covered; a no-op outside a request task.
    let started = std::time::Instant::now();
    let begun = match class {
        RouteClass::PageRead => pool.begin_with("BEGIN READ ONLY").await,
        RouteClass::Stream | RouteClass::Other => pool.begin().await,
    };
    // StoreError::from records database failures once for the request ledger.
    let mut tx = begun?;
    set_local_settings(&mut tx, settings).await?;
    crate::request_timing::record_tx_open(started.elapsed());
    Ok(tx)
}

/// The one-statement body of [`begin_with_local_settings`], on an
/// already-open transaction.
async fn set_local_settings(
    tx: &mut Transaction<'_, Postgres>,
    settings: &[(&str, &str)],
) -> Result<(), StoreError> {
    use std::fmt::Write as _;
    debug_assert!(!settings.is_empty(), "a scoped transaction sets at least one GUC");
    let mut sql = String::from("SELECT ");
    for i in 0..settings.len() {
        if i > 0 {
            sql.push_str(", ");
        }
        write!(sql, "set_config(${}, ${}, true)", 2 * i + 1, 2 * i + 2)
            .expect("writing to a String cannot fail");
    }
    let mut query = sqlx::query(&sql);
    for (name, value) in settings {
        query = query.bind(*name).bind(*value);
    }
    query.execute(&mut **tx).await?;
    Ok(())
}

/// Set `app.user_id` on an already-open transaction.
///
/// Postgres `statement_timeout` for one org-tenant REQUEST transaction.
///
/// # Why `SET LOCAL` and never the startup packet
///
/// An earlier cut of W2 set this through `PgConnectOptions::options([...])`,
/// i.e. the connection STARTUP packet. That would have taken production down
/// for every org tenant on a pooled DSN.
///
/// **Neon's pooler allows exactly five startup parameters** —
/// `client_encoding`, `datestyle`, `timezone`, `standard_conforming_strings`
/// and `application_name` — and rejects anything else outright with
/// `unsupported startup parameter in options: …`. `statement_timeout` is not
/// on that list. The pool would have failed to BUILD, so no org-tenant request
/// would have been served at all.
///
/// This is not a hypothesis. It is exactly what happened to the worker pool:
/// `app.tenant_actor=system` was set the same way, Neon refused it, and every
/// org tenant's background work — embed, extract, summary — was dead in
/// production, visible only as a log line (PR #388). `SET LOCAL` inside an
/// already-open transaction is the mechanism that survives a pooler, and it is
/// the one #388 moved that GUC to.
///
/// Ten seconds, deliberately EXCEEDING this pool's own `DEFAULT_ACQUIRE_TIMEOUT`
/// (5 s): a caller gives up waiting for a connection at 5 s, but the query
/// holding it is not cancelled until 10 s. So this is a backstop against one
/// query pinning a connection indefinitely, not a promise that waiters never
/// time out. Any legitimate request-path query finishes in milliseconds;
/// anything near this bound is a bug, and failing it fast makes that bug
/// visible instead of letting it silently consume one of only five
/// connections.
pub async fn set_request_statement_timeout(
    tx: &mut Transaction<'_, Postgres>,
) -> Result<(), StoreError> {
    // `SET LOCAL` cannot take a bind parameter, and `set_config`'s value
    // argument can — same reasoning as `begin_account_scoped`'s use of
    // `set_config` over `SET LOCAL app.account_id = $1`. The value is a
    // compile-time constant here regardless, so no untrusted text reaches it.
    sqlx::query("SELECT set_config('statement_timeout', $1, true)")
        .bind(REQUEST_STATEMENT_TIMEOUT)
        .execute(&mut **tx)
        .await?;
    Ok(())
}

/// See [`set_request_statement_timeout`]. Milliseconds, as text, because
/// `set_config` takes text and a bare integer is interpreted as ms.
const REQUEST_STATEMENT_TIMEOUT: &str = "10000";

/// Same `set_config(..., true)` / `SET LOCAL` semantics as
/// [`begin_account_scoped`]: transaction-local, reset at COMMIT/ROLLBACK, so a
/// pooled connection reused by a different member's request afterwards never
/// inherits this value.
pub async fn set_tenant_user(
    tx: &mut Transaction<'static, Postgres>,
    user_id: Uuid,
) -> Result<(), StoreError> {
    sqlx::query("SELECT set_config('app.user_id', $1, true)")
        .bind(user_id.to_string())
        .execute(&mut **tx)
        .await?;
    Ok(())
}

/// Name the ONE team ocean this transaction is allowed to bootstrap, for
/// migration `0040_team_create_bootstrap_rls`'s INSERT arms.
///
/// **`grep -rn set_team_bootstrap_graph backend/src` is intended to remain a
/// complete and short list** — one call site, inside
/// [`crate::store::Store::create_team_tx_scoped`] — the same reviewability
/// property [`begin_tenant_system_scoped`] and [`set_tenant_org_admin`] ask
/// for. Unlike those two, though, this grant cannot be misused into a bypass
/// even if a future call site does add itself: 0040's resolver additionally
/// requires the named graph to have ZERO `team_members` rows, so it is NULL
/// for every established team ocean in the tenant. Naming someone else's team
/// here buys nothing.
///
/// Must be called AFTER the `INSERT INTO graphs` whose id it names — the graph
/// row has to exist for the resolver's `kind = 'team'` check to see it — and
/// the caller mints that id itself rather than using `RETURNING`, because
/// `INSERT ... RETURNING` applies the SELECT policy and a brand-new team ocean
/// is not yet selectable. See 0040's header, "the RETURNING trap".
///
/// Same `set_config(..., true)` semantics as every other setter here:
/// transaction-local, discarded at COMMIT/ROLLBACK, never inherited by the
/// next borrower of a pooled connection.
pub async fn set_team_bootstrap_graph(
    tx: &mut Transaction<'_, Postgres>,
    graph_id: Uuid,
) -> Result<(), StoreError> {
    sqlx::query("SELECT set_config('app.creating_graph_id', $1, true)")
        .bind(graph_id.to_string())
        .execute(&mut **tx)
        .await?;
    Ok(())
}

impl super::extractor::Tenant {
    /// [`begin_account_scoped`], resolved against this request's own
    /// content-plane pool (`self.pool` — the ambient consumer pool on
    /// `TenancyDecision::Personal`/absent-decision, or the tenant pool on
    /// `TenancyDecision::Org`; see the [`super::extractor`] module doc).
    ///
    /// Returns [`StoreError::Db`] if this `Tenant` has no pool at all (e.g.
    /// a misconfigured deploy — the same case existing
    /// routes already guard with their own `require_store!`-style handling).
    ///
    /// **This sets `app.account_id` only.** On an ORG request that is NOT
    /// sufficient for migration `0009_tenant_rls`'s member-ocean policies —
    /// every member ocean in an org shares one `account_id`, so the GUC this
    /// sets cannot tell member A's reef from member B's. Org-plane call sites
    /// move to [`Self::begin_member_scoped`] in Phase 2; see this module's
    /// header and `docs/tenant-rls-cutover-runbook.md`. This method is
    /// deliberately left UNCHANGED so Phase 1 is behaviorally identical to the
    /// commit before it.
    pub async fn begin_account_scoped(
        &self,
        account_id: Uuid,
    ) -> Result<Transaction<'static, Postgres>, StoreError> {
        let pool = self.pool.as_ref().ok_or_else(|| {
            StoreError::Db(Box::from(
                "begin_account_scoped: this request has no content-plane pool",
            ))
        })?;
        match self.class {
            // A page read on the consumer plane gets the same read-only
            // shape and 1 s budget as on the tenant plane; the consumer pool
            // has no lane to reserve, so this is the whole of what the class
            // changes there (design §4.3).
            RouteClass::PageRead => {
                let account_id = account_id.to_string();
                begin_class_with_local_settings(
                    pool,
                    RouteClass::PageRead,
                    &[
                        (SEARCH_PATH_GUC, SEARCH_PATH_DEFAULT),
                        (ACCOUNT_GUC, &account_id),
                        (
                            STATEMENT_TIMEOUT_GUC,
                            RouteClass::PageRead.statement_timeout_ms(),
                        ),
                    ],
                )
                .await
            }
            RouteClass::Stream | RouteClass::Other => begin_account_scoped(pool, account_id).await,
        }
    }

    /// [`Self::begin_account_scoped`] PLUS the tenant plane's member-ocean
    /// identity — the drop-in replacement Phase 2 converts org-plane call sites
    /// to.
    ///
    /// On [`TenancyDecision::Org`] this additionally sets `app.user_id` from
    /// [`super::runtime::TenantContext::user_id`]: the gate-verified user this
    /// context is the ONLY valid context for (the same id
    /// [`super::reachability`]'s `require_matching_user` refuses to serve any
    /// other user against). The identity therefore comes from the request's own
    /// verified context, never from a handler parameter a caller could get
    /// wrong.
    ///
    /// On Personal / absent-decision requests this is EXACTLY
    /// [`Self::begin_account_scoped`] — no `app.user_id` is set, because no
    /// consumer-plane policy reads it and setting it would imply a per-user
    /// boundary the consumer plane does not have. That equivalence is what
    /// makes the Phase-2 conversion a mechanical, one-token change at call
    /// sites that serve both planes.
    ///
    /// **This used to say "Nothing calls this yet."** Phase 2
    /// (security/tenant-rls-phase2, §2.5 of
    /// `docs/tenant-rls-cutover-runbook.md`) has since converted the org
    /// plane's call sites to this method: 31 of them across 17 files as of that
    /// change (`grep -rn '\.begin_member_scoped(' backend/src` reports 32/18 —
    /// it also matches the pattern printed in this very comment, so discount
    /// one). They span routes and MCP tools, including `routes/nodes.rs`'s
    /// promotion helpers, whose `Store::promote_node_scoped` call is what writes
    /// a same-pool promotion's `node_promotions` provenance row under
    /// `app.user_id` (runbook §3.6's promotion item traces that path). Further
    /// route handlers reach this method indirectly through `routes/nodes.rs`'s
    /// `begin_scoped_tx!` macro and are not in that count.
    pub async fn begin_member_scoped(
        &self,
        account_id: Uuid,
    ) -> Result<Transaction<'static, Postgres>, StoreError> {
        let Some(TenancyDecision::Org(ctx)) = self.decision.as_ref() else {
            return self.begin_account_scoped(account_id).await;
        };
        let pool = self.pool.as_ref().ok_or_else(|| {
            StoreError::Db(Box::from(
                "begin_member_scoped: this request has no content-plane pool",
            ))
        })?;
        // All four GUCs in the ONE statement `begin_with_local_settings`
        // sends after BEGIN — the same values, in the same transaction-local
        // scope, that `begin_account_scoped` + `set_tenant_user` +
        // `set_request_statement_timeout` used to set with three further
        // round trips.
        //
        // `statement_timeout` is W2 (`docs/plans/request-reliability-and-
        // scale.md` §6.2): bound how long ONE query may hold one of this
        // tenant's request connections. Set here — the org REQUEST path —
        // and nowhere else, so worker queries (embedding sweeps, backfills,
        // tombstone purges), which legitimately run for minutes, are
        // untouched. The value follows the route class: 10 s for `Other`
        // and `Stream` (`REQUEST_STATEMENT_TIMEOUT`), 1 s for `PageRead`,
        // which also opens READ ONLY on the page lane (design §4.3).
        let account_id = account_id.to_string();
        let user_id = ctx.user_id.to_string();
        begin_class_with_local_settings(
            pool,
            self.class,
            &[
                (SEARCH_PATH_GUC, SEARCH_PATH_DEFAULT),
                (ACCOUNT_GUC, &account_id),
                (USER_GUC, &user_id),
                (STATEMENT_TIMEOUT_GUC, self.class.statement_timeout_ms()),
            ],
        )
        .await
    }

    /// A separate read-only snapshot for paired Library proofs. Isolation is
    /// selected in BEGIN, before the first scoped-GUC SELECT establishes a
    /// snapshot. Consumer identity intentionally matches begin_member_scoped.
    pub async fn begin_member_repeatable_read(
        &self,
        account_id: Uuid,
    ) -> Result<Transaction<'static, Postgres>, StoreError> {
        let pool = self.pool.as_ref().ok_or_else(|| {
            StoreError::Db(Box::from("Library snapshot has no content-plane pool"))
        })?;
        let mut tx = pool.begin_with("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY").await?;
        let account = account_id.to_string();
        if let Some(TenancyDecision::Org(ctx)) = self.decision.as_ref() {
            let user = ctx.user_id.to_string();
            set_local_settings(&mut tx, &[
                (SEARCH_PATH_GUC, SEARCH_PATH_DEFAULT), (ACCOUNT_GUC, &account),
                (USER_GUC, &user), (STATEMENT_TIMEOUT_GUC, REQUEST_STATEMENT_TIMEOUT),
            ]).await?;
        } else {
            set_local_settings(&mut tx, &[
                (SEARCH_PATH_GUC, SEARCH_PATH_DEFAULT), (ACCOUNT_GUC, &account),
            ]).await?;
        }
        Ok(tx)
    }

    /// [`Self::begin_member_scoped`] PLUS, when an [`OrgAdminProof`] is
    /// presented on an ORG request, the governance-table grant
    /// [`set_tenant_org_admin`] documents.
    ///
    /// This is the transaction opener the named-team ROSTER and AUDIT routes
    /// use (`routes::teams`). It exists as a separate method rather than an
    /// argument to [`Self::begin_member_scoped`] so that `grep -rn
    /// begin_governance_scoped backend/src` stays a complete and short list of
    /// every place the governance grant can possibly be applied — the same
    /// reviewability property [`begin_tenant_system_scoped`]'s doc asks for.
    ///
    /// `org_admin: None` makes this EXACTLY [`Self::begin_member_scoped`], so
    /// a route that computes the proof once and threads it through
    /// unconditionally is correct on the personal plane too.
    ///
    /// The `TenancyDecision::Org` guard is not redundant with the proof:
    /// [`verify_org_admin`] can legitimately succeed for a request whose
    /// tenancy runtime is off (an org account served on the consumer plane).
    /// `app.is_org_admin` has NO reader on the consumer plane — migration
    /// 0083's policies are account-scoped and never consult it — so setting it
    /// there would be a GUC nothing reads, and the verification round trip
    /// inside [`set_tenant_org_admin`] would hard-error on a missing function.
    /// Restricting it to the org plane keeps the GUC exactly as wide as the
    /// policies that read it.
    pub async fn begin_governance_scoped(
        &self,
        account_id: Uuid,
        org_admin: Option<OrgAdminProof>,
    ) -> Result<Transaction<'static, Postgres>, StoreError> {
        let mut tx = self.begin_member_scoped(account_id).await?;
        if let (Some(proof), Some(TenancyDecision::Org(_))) = (org_admin, self.decision.as_ref()) {
            set_tenant_org_admin(&mut tx, proof).await?;
        }
        Ok(tx)
    }
}

/// Class-aware executor for an otherwise unscoped read (control identity,
/// SECURITY DEFINER cross-account unions, public token lookups). It adds no
/// account/user predicate: those callers already own their authorization.
/// Scoped reads must use `begin_request_account_read`/`begin_request_tenant_read`
/// or Tenant's methods to retain their RLS settings.
///
/// The class is captured from the outer request scope BEFORE auth runs. Off a
/// request task it is Other; background work and provisioning keep their
/// existing behavior. No session GUCs or extra pools: PageRead uses SET LOCAL
/// inside BEGIN READ ONLY on the very same shared/control or selected lane.
#[derive(Debug, Clone)]
pub struct RequestRead {
    pool: PgPool,
    class: RouteClass,
}

pub fn request_read(pool: &PgPool) -> RequestRead {
    RequestRead {
        pool: pool.clone(),
        class: crate::request_timing::current_class(),
    }
}

impl RequestRead {
    async fn begin(&self) -> Result<Transaction<'static, Postgres>, sqlx::Error> {
        let started = std::time::Instant::now();
        let mut tx = self.pool.begin_with("BEGIN READ ONLY").await?;
        sqlx::query(
            "SELECT set_config('statement_timeout', $1, true), set_config('search_path', $2, true)",
        )
        .bind(self.class.statement_timeout_ms())
        .bind(SEARCH_PATH_DEFAULT)
        .execute(&mut *tx)
        .await?;
        crate::request_timing::record_tx_open(started.elapsed());
        Ok(tx)
    }
}

impl<'c> sqlx::Executor<'c> for RequestRead {
    type Database = Postgres;

    fn fetch_many<'e, 'q: 'e, E>(
        self,
        query: E,
    ) -> futures::stream::BoxStream<
        'e,
        Result<sqlx::Either<sqlx::postgres::PgQueryResult, sqlx::postgres::PgRow>, sqlx::Error>,
    >
    where
        'c: 'e,
        E: 'q + sqlx::Execute<'q, Postgres>,
    {
        use futures::TryStreamExt;
        let class = self.class;
        Box::pin(
            (async_stream::try_stream! {
                if self.class == RouteClass::PageRead {
                    let mut tx = self.begin().await?;
                    {
                        let mut rows = (&mut *tx).fetch_many(query);
                        while let Some(row) = rows.try_next().await? { yield row; }
                    }
                    tx.commit().await?;
                } else {
                    let mut rows = (&self.pool).fetch_many(query);
                    while let Some(row) = rows.try_next().await? { yield row; }
                }
            })
            .map_err(move |e| crate::request_timing::record_raw_sqlx_error(class, e)),
        )
    }

    fn fetch_optional<'e, 'q: 'e, E>(
        self,
        query: E,
    ) -> futures::future::BoxFuture<'e, Result<Option<sqlx::postgres::PgRow>, sqlx::Error>>
    where
        'c: 'e,
        E: 'q + sqlx::Execute<'q, Postgres>,
    {
        use futures::TryFutureExt;
        let class = self.class;
        Box::pin(
            (async move {
                if self.class != RouteClass::PageRead {
                    return (&self.pool).fetch_optional(query).await;
                }
                let mut tx = self.begin().await?;
                let row = (&mut *tx).fetch_optional(query).await?;
                tx.commit().await?;
                Ok(row)
            })
            .map_err(move |e| crate::request_timing::record_raw_sqlx_error(class, e)),
        )
    }

    fn prepare_with<'e, 'q: 'e>(
        self,
        sql: &'q str,
        parameters: &'e [sqlx::postgres::PgTypeInfo],
    ) -> futures::future::BoxFuture<'e, Result<sqlx::postgres::PgStatement<'q>, sqlx::Error>>
    where
        'c: 'e,
    {
        use futures::TryFutureExt;
        let class = self.class;
        Box::pin(
            (async move {
                if self.class != RouteClass::PageRead {
                    return (&self.pool).prepare_with(sql, parameters).await;
                }
                let mut tx = self.begin().await?;
                let statement = (&mut *tx).prepare_with(sql, parameters).await?;
                tx.commit().await?;
                Ok(statement)
            })
            .map_err(move |e| crate::request_timing::record_raw_sqlx_error(class, e)),
        )
    }

    fn describe<'e, 'q: 'e>(
        self,
        sql: &'q str,
    ) -> futures::future::BoxFuture<'e, Result<sqlx::Describe<Postgres>, sqlx::Error>>
    where
        'c: 'e,
    {
        use futures::TryFutureExt;
        let class = self.class;
        Box::pin(
            (async move {
                if self.class != RouteClass::PageRead {
                    return (&self.pool).describe(sql).await;
                }
                let mut tx = self.begin().await?;
                let description = (&mut *tx).describe(sql).await?;
                tx.commit().await?;
                Ok(description)
            })
            .map_err(move |e| crate::request_timing::record_raw_sqlx_error(class, e)),
        )
    }
}

/// Account-scoped control read with the caller's class budget. The write
/// opener remains separate so first-login provisioning and mixed POST paths
/// keep their existing lifecycle and permissions.
pub async fn begin_request_account_read(
    pool: &PgPool,
    account_id: Uuid,
) -> Result<Transaction<'static, Postgres>, StoreError> {
    let class = crate::request_timing::current_class();
    let account = account_id.to_string();
    if class != RouteClass::PageRead {
        return begin_account_scoped(pool, account_id).await;
    }
    begin_class_with_local_settings(
        pool,
        class,
        &[
            (SEARCH_PATH_GUC, SEARCH_PATH_DEFAULT),
            (ACCOUNT_GUC, &account),
            (STATEMENT_TIMEOUT_GUC, class.statement_timeout_ms()),
        ],
    )
    .await
}

/// Member-scoped read, including when the table is on the control plane.
pub async fn begin_request_tenant_read(
    pool: &PgPool,
    account_id: Uuid,
    user_id: Uuid,
) -> Result<Transaction<'static, Postgres>, StoreError> {
    let class = crate::request_timing::current_class();
    if class != RouteClass::PageRead {
        return begin_tenant_scoped(pool, account_id, user_id).await;
    }
    let account = account_id.to_string();
    let user = user_id.to_string();
    begin_class_with_local_settings(
        pool,
        class,
        &[
            (SEARCH_PATH_GUC, SEARCH_PATH_DEFAULT),
            (ACCOUNT_GUC, &account),
            (USER_GUC, &user),
            (STATEMENT_TIMEOUT_GUC, class.statement_timeout_ms()),
        ],
    )
    .await
}

/// The watched-resource read keeps the existing explicit system actor grant;
/// only transaction access mode and budget change on PageRead.
pub async fn begin_request_system_read(
    pool: &PgPool,
    account_id: Uuid,
) -> Result<Transaction<'static, Postgres>, StoreError> {
    let class = crate::request_timing::current_class();
    if class != RouteClass::PageRead {
        return begin_tenant_system_scoped(pool, account_id).await;
    }
    let account = account_id.to_string();
    begin_class_with_local_settings(
        pool,
        class,
        &[
            (SEARCH_PATH_GUC, SEARCH_PATH_DEFAULT),
            (ACCOUNT_GUC, &account),
            (TENANT_ACTOR_GUC, TENANT_ACTOR_SYSTEM),
            (STATEMENT_TIMEOUT_GUC, class.statement_timeout_ms()),
        ],
    )
    .await
}

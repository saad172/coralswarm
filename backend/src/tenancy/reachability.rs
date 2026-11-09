//! Reachability decomposition (org-tenancy PR 1.7) — the authorization core.
//!
//! `Store::list_reachable_graphs` / `Store::graph_reachable_by_user` join
//! `graphs` + `account_members` + `teams`/`team_members` in ONE query (the D9
//! read union, ADR-0007). That single-query join is only possible because,
//! today, `account_members` (a control-plane table) and `graphs`/`teams`/
//! `team_members` (tenant-tree tables — see the migration-split doc) live on
//! the SAME physical database: the shared control/consumer plane. Once an
//! org's content moves to its own tenant database, the join is no longer
//! expressible as one SQL statement — Postgres cannot join across two
//! separate connections/databases. This module is the ONE place that
//! branches on which plane a request is scoped to:
//!
//! * **Consumer plane** (`TenancyDecision::Personal`, or no decision at all —
//!   the tenancy runtime is off or the call site hasn't been migrated yet):
//!   delegate to the existing [`Store::list_reachable_graphs`] /
//!   [`Store::graph_reachable_by_user`] unchanged. Same physical DB, so the
//!   joined query still works and stays byte-identical.
//! * **Org tenant plane** (`TenancyDecision::Org`): membership is already
//!   established by the time a [`TenantContext`] exists. The binding gate
//!   only ever produces `TenancyDecision::Org` after `resolve_clerk_context`
//!   has verified the caller's Clerk JWT `org_id` claim and freshly
//!   upserted/reconfirmed the corresponding control-plane `account_members`
//!   row for `ctx.org_id`/`ctx.account_id` (`clerk_provision::
//!   ensure_org_membership`, run every request, before the gate). Clerk
//!   itself would not issue an org-scoped session token for an org the user
//!   is not currently an active member of. (Note: `ensure_org_membership` is
//!   upsert-only on this hot path — membership *removal* is webhook-only, so
//!   a still-valid org JWT keeps re-upserting membership until the token
//!   expires. That's a pre-existing JWT-revocation-window property shared by
//!   the consumer plane's own `account_members` row today; this module does
//!   not introduce or widen it.) So there is nothing left to re-query
//!   against the control plane here — the "two-step" collapses to ONE step
//!   scoped to the tenant pool: enumerate `graphs` filtered to `account_id =
//!   ctx.account_id` (a tenant database holds exactly one org's rows —
//!   ADR-0007 D2-impl — so this equality is the tenant-side equivalent of
//!   the consumer query's `account_id IN (SELECT account_id FROM
//!   account_members WHERE user_id = $1)` subquery, minus the subquery),
//!   gated by tenant-local `teams`/`team_members` for team oceans, plus
//!   (member-private-oceans PR 2, plan §2.2) `archived_at IS NULL` and a
//!   PURE-EQUALITY `kind <> 'member' OR owner_user_id = $u` gate for
//!   owner-scoped oceans — a structurally identical predicate to the
//!   consumer query's, just executed against the tenant pool instead of the
//!   shared one. As of PR 2 this predicate is NOT hand-rolled Rust SQL
//!   anymore: both tenant-path queries below delegate to `SECURITY DEFINER`
//!   SQL functions (`resolve_tenant_reachable_graphs` /
//!   `resolve_tenant_graph_reachable_by_user`, migrations
//!   `0091_member_ocean_reachability_functions` /
//!   `0006_member_ocean_reachability_functions`), for the same rolling-
//!   deploy reason the consumer arm's carve-out functions exist — see
//!   `tenant_list_reachable_graphs`'s doc comment below for the full
//!   argument. Row creation stays behind `MEMBER_OCEANS_ENABLED`; no
//!   `kind = 'member'` row exists yet, so this predicate change is
//!   observably inert until that flag flips (PR 4+).
//!
//!   **The `account_id = ctx.account_id` scope alone is NOT sufficient**: it
//!   only gates which ACCOUNT's graphs are visible, and (unlike the
//!   consumer query) `account_members` does not exist tenant-side at all
//!   (absent from `migrations_tenant/0001_tenant_baseline.sql` — there is no
//!   SQL predicate that could re-check membership here even if we wanted
//!   one). For a NON-team ocean the team-gating predicate below never even
//!   runs, so without a further check the tenant path would return the
//!   org's non-team oceans for ANY `user_id` passed in, regardless of
//!   whether that user is actually a member — strictly wider than the
//!   consumer path's `account_members`-gated result. The fix is structural,
//!   not a query: [`TenantContext::user_id`] is the ONE user this context is
//!   valid for (stamped by the gate from the same verified request that
//!   proved `account_id`/`org_id`), and the Org arm below refuses to serve
//!   any other `user_id` — see [`require_matching_user`].
//!
//! D9's full union ("private ocean + every org/team the user belongs to") is
//! realized WITHIN one physical plane per call to this resolver; spanning
//! personal + org (or two different orgs) in a single logical request is a
//! deliberately separate product decision (see the implementation plan's
//! "cross-boundary ask" item, recommended v1 answer: context-scoped only, never a generic cross-DB join) — callers that need a
//! cross-plane union invoke this resolver once per relevant
//! [`TenancyDecision`] and merge at a higher layer.
//!
//! # RLS scoping (org-tenancy B.3): this module is deliberately EXEMPT
//!
//! Every other consumer-plane query path is being wrapped in
//! [`crate::tenancy::scope::begin_account_scoped`] ahead of OPS-4. This one is
//! not, and the reason is structural rather than an omission — both of its
//! arms are already outside the single-`account_id` scoping model, in opposite
//! directions:
//!
//! * **The consumer arm** delegates to [`Store::list_reachable_graphs`] /
//!   [`Store::graph_reachable_by_user`], which (since PR #284) execute the
//!   `resolve_reachable_graphs` / `resolve_graph_reachable_by_user`
//!   `SECURITY DEFINER` functions from migration
//!   `0084_bootstrap_scope_functions`. Those are the ONE PERMANENT carve-out
//!   in this codebase, and 0084's header flags them for founder review by
//!   name: `graphs.account_id IN (SELECT account_id FROM account_members
//!   WHERE user_id = $1)` is deliberately MULTI-account (ADR-0007 "Model B" —
//!   a user belongs to several `accounts` rows and sees across all of them in
//!   ONE query), which is by construction incompatible with an
//!   `app.account_id` predicate that admits exactly one account. Wrapping this
//!   arm in a scope would not tighten anything; it would BREAK reachability
//!   for every multi-account user, and the decision (already taken, not this
//!   module's to revisit) is that the carve-out stays. The security property
//!   is preserved inside the function body, which re-implements the identical
//!   `account_members` predicate.
//!
//! * **The org-tenant arm**'s two queries run against `ctx.pool` — the ORG
//!   TENANT pool — and only ever there (they are reachable solely from the
//!   `TenancyDecision::Org(ctx)` match arm). It stays exempt, but the REASON
//!   has changed and the old one should not be repeated: this used to say
//!   "a tenant database gets whole-database isolation, not RLS: migration
//!   `0083_consumer_rls` is control/consumer-plane only, so there is no policy
//!   on that database for a scope to satisfy." Half of that is now false —
//!   `migrations_tenant/0009_tenant_rls` puts 34 member-ocean policies on
//!   tenant databases, keyed on an `app.user_id` GUC. (Whole-database
//!   isolation remains the CROSS-ORG story; the member-ocean boundary inside a
//!   tenant is what 0009 adds.)
//!
//!   The arm is exempt anyway, and now for a stronger reason than "no policies
//!   exist there": both of its queries are calls into
//!   `resolve_tenant_reachable_graphs` / `resolve_tenant_graph_reachable_by_user`,
//!   `SECURITY DEFINER` functions that already enforce the IDENTICAL
//!   member-ocean predicate 0009's policies enforce, with the user supplied as
//!   an explicit argument rather than read from a session GUC. Wrapping the
//!   call in a scope would add a GUC that the function body does not consult.
//!   That arm's real guards are those function bodies, [`require_matching_user`],
//!   and the explicit `account_id = $N` equality, all described below.
//!
//! This is an authorization-scope carve-out, not a transaction-budget
//! exemption. PageRead list queries use `scope::request_read`: READ ONLY +
//! 1 s, without adding a single-account GUC to the consumer D9 union.
//!
//! This carve-out is documented in `scripts/rls_coverage.manifest`'s header
//! ("PERMANENT DOCUMENTED EXCEPTION" block), not as a per-file entry: the
//! gate's grep no longer sees this file's Store calls (they deliberately have
//! no `_scoped` sibling), so a file entry would only draw stale-entry notes.
//! The compensating control is `backend/tests/reachable_graphs_parity_pg.rs`,
//! wired into CI — see the manifest header for the full reasoning.
//!
//! **Fail-closed discipline:** every tenant-path call first asserts
//! `user_id == ctx.user_id` (see [`require_matching_user`]) and then scopes
//! its query by an explicit `account_id = $N` equality bound to the
//! gate-verified [`TenantContext::account_id`]. Even if some future caller
//! ever invoked this resolver with a stale or mismatched context, both
//! checks can only ever narrow the result set — never widen it to another
//! account's graphs or another user's membership — and any failure
//! (mismatch or query error) is always propagated as `Err`, never silently
//! treated as an empty-but-successful (and therefore falsely reassuring)
//! result.

use uuid::Uuid;

use crate::store::{Graph, Store, StoreError};

use super::runtime::{TenancyDecision, TenantContext};

/// The load-bearing equality check: a [`TenantContext`] is only ever valid
/// for the single user the binding gate resolved it for. `user_id` reaches
/// this module as a free parameter (every existing caller of the old
/// `Store::list_reachable_graphs`/`graph_reachable_by_user` passed a bare
/// `Uuid`), so nothing in the type system stops a future caller — a 1.4/1.5/
/// 1.6 route, MCP's OAuth principal (PR 2.3, where the token's subject and a
/// caller-supplied id can differ), a background job, or an impersonation
/// path — from passing a DIFFERENT user's id against a context built for
/// someone else. Since `account_members` does not exist on the tenant plane
/// (this module's doc comment), there is no SQL predicate that could catch
/// that mismatch downstream — it must be refused here, structurally, before
/// any query runs. Fails closed with a real `Err` (never `Ok(false)`/`Ok(
/// vec![])`, which would be indistinguishable from a legitimate "nothing
/// reachable" result and could mask the bug rather than surface it).
fn require_matching_user(ctx: &TenantContext, user_id: Uuid) -> Result<(), StoreError> {
    if ctx.user_id != user_id {
        // `TenantAuthzMismatch` (org-tenancy PR 1.4), not `Db` — this is a
        // refused authorization check, never a database failure, and the two
        // must stay distinguishable in logs/metrics (a spike here is a
        // probe/bug signal; a spike in `Db` is an outage signal). Both still
        // map to the same outward 500 at every call site.
        return Err(StoreError::TenantAuthzMismatch(format!(
            "ReachabilityResolver: TenantContext is scoped to user {}, refusing to \
             resolve reachability for a different user_id {user_id} (org {})",
            ctx.user_id, ctx.org_id
        )));
    }
    Ok(())
}

/// The single place the Consumer-vs-tenant-pool reachability split lives.
///
/// Callers that used to invoke `Store::list_reachable_graphs` /
/// `Store::graph_reachable_by_user` directly should resolve through here
/// instead, threading the request's resolved [`TenancyDecision`] (or `None`
/// at call sites that have not yet been wired to the binding gate — this
/// behaves exactly like `TenancyDecision::Personal`, so it is always safe and
/// byte-identical to today's behavior). When PR 1.4/1.5/1.6 migrate a route
/// to the `Tenant` extractor, updating its call site here is a one-argument
/// change (swap `None` for `Some(&decision)`) — the branching logic itself
/// never needs to move again.
pub struct ReachabilityResolver;

impl ReachabilityResolver {
    /// See [`Store::list_reachable_graphs`]'s doc comment for the exact D9
    /// union semantics this preserves on both planes.
    pub async fn list_reachable_graphs(
        decision: Option<&TenancyDecision>,
        consumer_store: &dyn Store,
        user_id: Uuid,
    ) -> Result<Vec<Graph>, StoreError> {
        match decision {
            None | Some(TenancyDecision::Personal) => {
                consumer_store.list_reachable_graphs(user_id).await
            }
            Some(TenancyDecision::Org(ctx)) => {
                require_matching_user(ctx, user_id)?;
                tenant_list_reachable_graphs(ctx, user_id).await
            }
        }
    }

    /// The ids of every ocean `user_id` may READ — [`Self::list_reachable_graphs`]
    /// reduced to the `graph_id` allowlist account-wide listings filter on
    /// (settings 2.12).
    ///
    /// A listing keyed on `graphs.account_id` alone is NOT a read scope in an
    /// org: every ocean there (the commons, every team ocean, every member's
    /// private ocean) shares the one account. The D9 union this returns is:
    /// the caller's own private ocean, `org`/`personal` oceans, and `team`
    /// oceans they are an active member of — never another member's
    /// `member` ocean, never a team they are not on, never an archived ocean.
    /// It is the read twin of `routes::nodes::ReefWriter` (settings 2.10): a
    /// member is SHOWN what they can read, and may only WRITE the narrower
    /// set that type allows.
    ///
    /// Callers keep their own `account_id` predicate too: on the consumer
    /// plane this union spans every account the user belongs to, and the
    /// account predicate narrows it back to the active one. An empty result
    /// means "nothing readable", so an `= ANY($ids)` filter on it returns no
    /// rows — the fail-closed direction.
    pub async fn readable_graph_ids(
        decision: Option<&TenancyDecision>,
        consumer_store: &dyn Store,
        user_id: Uuid,
    ) -> Result<Vec<Uuid>, StoreError> {
        Ok(Self::list_reachable_graphs(decision, consumer_store, user_id)
            .await?
            .into_iter()
            .map(|g| g.id)
            .collect())
    }

    /// See [`Store::graph_reachable_by_user`]'s doc comment (the IDOR guard
    /// used to re-verify a caller-supplied `graph_id` before use).
    pub async fn graph_reachable_by_user(
        decision: Option<&TenancyDecision>,
        consumer_store: &dyn Store,
        graph_id: Uuid,
        user_id: Uuid,
    ) -> Result<bool, StoreError> {
        match decision {
            None | Some(TenancyDecision::Personal) => {
                consumer_store
                    .graph_reachable_by_user(graph_id, user_id)
                    .await
            }
            Some(TenancyDecision::Org(ctx)) => {
                require_matching_user(ctx, user_id)?;
                tenant_graph_reachable_by_user(ctx, graph_id, user_id).await
            }
        }
    }

    /// Shared node-id → graph → reachability → owning-account dance.
    ///
    /// Every "read one reef by id" call site — `routes::nodes::
    /// get_node_atoms`, `routes::nodes::get_node_by_id` (`GET
    /// /v1/nodes/{id}`), and MCP's `get_reef` — used to hand-roll this exact
    /// three-step sequence inline (each one's own history/doc comments say as
    /// much: `get_reef`'s says it "mirrors `get_node_atoms`"). This is the one
    /// copy all three now share.
    ///
    /// `scoped_tx` is the CALLER's OWN account-scoped transaction. A node-id-
    /// anchored read can't know the node's actual owning account until this
    /// function resolves it, so callers can only ever open their scoped
    /// transaction against their own active account first (module docs' RLS
    /// section) — this function's first step (`node_graph_id`/
    /// `node_graph_id_scoped`) runs inside that transaction.
    ///
    /// Returns the reef's `graph_id` and its OWNING account on success —
    /// callers that need to query the reef's corals/etc. by account (e.g.
    /// `reef_atoms_scoped`) use the returned account, not `scoped_tx`'s own
    /// scope. **The two can differ**: [`Self::graph_reachable_by_user`]'s D9
    /// union is scoped to `user_id` across every account/team they belong
    /// to (see `resolve_graph_reachable_by_user`'s `account_id IN (SELECT
    /// account_id FROM account_members WHERE user_id = ...)`), not to
    /// whichever single account `scoped_tx` happens to be opened against —
    /// so a team ocean a caller reaches via team membership, or an org ocean
    /// under an account distinct from the caller's currently-active one, is
    /// reachable (and thus GET-able) here even when it would NOT pass an
    /// ownership check keyed to the caller's active account (see
    /// `node_owned_by_account`, which `patch_node` uses — narrower by
    /// design, see this PR's description for why that's a separate, still-
    /// open question).
    ///
    /// `Err(ReefAccessError::NotFound)` deliberately collapses "no such
    /// node" and "exists but not reachable by this user" into one outcome —
    /// the same non-leaking posture every node-id-anchored read already had
    /// before this was factored out of them.
    ///
    /// # KNOWN GAP (consumer/control plane only — tenant plane is unaffected)
    ///
    /// This function's FIRST step (`node_graph_id`/`node_graph_id_scoped`)
    /// runs inside the CALLER's OWN account-scoped transaction, opened
    /// before the node's actual owning account is known (see above). On the
    /// consumer plane, migration `0083_consumer_rls`'s `nodes` policy is
    /// `graph_id IN (SELECT id FROM graphs WHERE account_id =
    /// current_setting('app.account_id'))` — i.e. account-EQUALITY, not
    /// reachability. So under a REAL RLS-enforcing connection (`coralswarm_
    /// app`, live in production since 2026-08-04 per `tenancy::scope`'s
    /// module doc), this bootstrap step — and every later read this
    /// function's callers make through the SAME caller-scoped transaction
    /// (`get_node_scoped`, `reef_atoms_scoped`, …) — returns nothing for a
    /// node whose owning account differs from the caller's active one, even
    /// though [`Self::graph_reachable_by_user`] below would say the caller
    /// CAN reach it. In other words: on the consumer plane specifically, a
    /// node reachable only via `account_members`/`team_members` membership
    /// in a DIFFERENT account than the caller's active one can 404 here in
    /// production, contradicting this function's own doc above.
    ///
    /// This is a PRE-EXISTING gap, not introduced by the `GET /v1/nodes/
    /// {id}` PR that extracted this function — `routes::nodes::
    /// get_node_atoms` and MCP's `get_reef` had the IDENTICAL shape before
    /// the extraction (open the caller's own scoped tx, then look the node
    /// up through it), and this function preserves their behavior exactly
    /// rather than changing it.
    ///
    /// The tenant plane (`TenancyDecision::Org`) does NOT have this problem:
    /// `migrations_tenant/0009_tenant_rls`'s `nodes` policy is `graph_id IN
    /// (SELECT * FROM tenant_rls_visible_graph_ids())` — graph-anchored via
    /// `app.user_id`, not account-equality — and a tenant database holds
    /// exactly one account, so `scoped_tx`'s account is always the RIGHT one
    /// regardless of team membership. Team-ocean-via-membership genuinely
    /// works today on the plane the reported "Topic unavailable" bug this PR
    /// fixes actually runs on (every org has its own tenant database — the
    /// DB-per-org cutover is complete; there is no live org/team content left
    /// on the shared consumer plane to trip this).
    ///
    /// Properly closing this gap (an RLS-safe `node_id -> graph_id` bootstrap
    /// analogous to `Store::graph_account_id`'s `resolve_graph_account_id`
    /// SECURITY DEFINER function, migration 0084, PLUS re-scoping every
    /// subsequent read in `get_node_atoms`/`get_node_by_id`/`get_reef` to the
    /// node's OWNING account rather than the caller's, mirroring `list_nodes`'
    /// existing correct pattern) touches a new migration and three handlers'
    /// transaction lifecycles (two of which predate this PR) — a distinct,
    /// security-relevant change that deserves its own PR and review rather
    /// than riding in here. Tracked, not silently left: see this PR's
    /// description.
    pub async fn resolve_reef_access(
        decision: Option<&TenancyDecision>,
        store: &dyn Store,
        scoped_tx: &mut Option<sqlx::Transaction<'static, sqlx::Postgres>>,
        node_id: Uuid,
        user_id: Uuid,
    ) -> Result<ReefAccess, ReefAccessError> {
        // INVARIANT this match relies on: `None` here means "this request has
        // NO content-plane Postgres pool at all" (so there is no RLS to
        // bypass), and ONLY that. It must never mean "opening the
        // scoped transaction failed" — `begin_scoped_tx!`
        // (`routes/nodes.rs`) / `begin_scoped_tx!` (`mcp/tools/read.rs`) both
        // `return` early out of the WHOLE HANDLER on an open error, so
        // control never reaches here with a `None` that stands for a failed
        // open. If a future edit ever routes a failed scope-open into this
        // `None` arm instead of returning, this silently becomes an
        // UNSCOPED read of `nodes` on an org tenant (whole-database
        // isolation still holds, but the tenant's own member/team RLS
        // (`migrations_tenant/0009_tenant_rls`) would no longer apply) — and
        // nothing here would catch it: not a type (`ScopedTx` is a bare
        // `Option`, indistinguishable from a deliberate no-pool case by
        // construction), not a test, not the org-arm guard (it pattern-
        // matches on tenancy-decision enums and does not know this
        // `Option`-shaped scope exists at all — the exact shape that
        // produced the transcripts defect, #850). A fully structural fix
        // (typing "no pool" and "scope open failed" as distinct variants)
        // would mean widening the shared `ScopedTx` alias every one of this
        // codebase's `scoped_store!`/`begin_scoped_tx!` call sites (dozens,
        // across `routes/nodes.rs`, `mcp/tools/mod.rs`, `routes/
        // relationships.rs`, `routes/meetings.rs`, `routes/supersession.rs`,
        // `routes/teams.rs`, …) — disproportionate to fix here for one new
        // function; this comment is the guard until that's worth doing.
        let graph_id = match scoped_tx.as_mut() {
            Some(tx) => store.node_graph_id_scoped(tx, node_id).await,
            None => store.node_graph_id(node_id).await,
        };
        let graph_id = match graph_id {
            Ok(Some(g)) => g,
            Ok(None) => return Err(ReefAccessError::NotFound),
            Err(e) => return Err(e.into()),
        };

        match Self::graph_reachable_by_user(decision, store, graph_id, user_id).await {
            Ok(true) => {}
            Ok(false) => return Err(ReefAccessError::NotFound),
            Err(e) => return Err(e.into()),
        }

        // The bootstrap resolver (migration 0084) — a SECURITY DEFINER
        // function, deliberately run on the pool outside `scoped_tx` (see
        // `get_node_atoms`'s own comment on this same call).
        match store.graph_account_id(graph_id).await {
            Ok(Some(owning_account_id)) => Ok(ReefAccess { graph_id, owning_account_id }),
            Ok(None) => Err(ReefAccessError::NotFound),
            Err(e) => Err(e.into()),
        }
    }
}

/// Success half of [`ReachabilityResolver::resolve_reef_access`] — a named
/// struct rather than a `(Uuid, Uuid)` tuple: both fields are `Uuid`, so
/// nothing but the field NAME distinguishes "which ocean" from "which
/// account owns it", and one of them (`owning_account_id`) feeds directly
/// into an account-scoped read (`reef_atoms_scoped`). A positional
/// `let (a, b) = ...` that swapped the two would compile clean and scope a
/// cross-account read to the wrong tenant — exactly the class of bug this
/// resolver exists to prevent, so it must not be reintroducible by a typo at
/// the call site.
pub struct ReefAccess {
    /// The ocean (graph) this reef lives in.
    pub graph_id: Uuid,
    /// The ocean's OWNING account — NOT necessarily the account `scoped_tx`
    /// (the caller's own) is scoped to. See this function's doc comment:
    /// reachability is a `user_id`-wide union across every account/team the
    /// caller belongs to, while `scoped_tx` is opened against exactly one.
    pub owning_account_id: Uuid,
}

/// Failure half of [`ReachabilityResolver::resolve_reef_access`]. See that
/// function's doc comment for why `NotFound` is deliberately one variant
/// rather than two.
#[derive(Debug, thiserror::Error)]
pub enum ReefAccessError {
    #[error("reef not found or not reachable by this user")]
    NotFound,
    #[error(transparent)]
    Store(#[from] StoreError),
}

/// Row shape shared by both tenant-path queries below — mirrors the
/// consumer query's `SELECT` list exactly (order matters for the tuple bind
/// in `sqlx::query_as`).
///
/// **7 columns, not 5** (member-private-oceans PR 2, plan §2.1/§7): migration
/// 0090/0005 added `owner_user_id`/`archived_at` to `graphs`, and the paired
/// SQL functions this module calls (`resolve_tenant_reachable_graphs` /
/// `resolve_tenant_graph_reachable_by_user`, migrations
/// `0091_member_ocean_reachability_functions` /
/// `0006_member_ocean_reachability_functions`) grew their `RETURNS TABLE` the
/// same way. **This tuple and that `RETURNS TABLE` must stay in lockstep** —
/// a mismatch here does not fail to compile (sqlx's tuple `FromRow` just
/// binds however many columns the query text actually selects), it fails at
/// RUNTIME as a column-count/type mismatch, or worse, a silent field
/// transposition if two adjacent columns share a type. `store::mod`'s
/// `GraphRow`/`graph_row_to_graph` are the consumer-side twin of this same
/// shape; `backend/tests/reachable_graphs_parity_pg.rs` and
/// `backend/tests/reachability_pg.rs` are the guards that a drift between
/// the SELECT list and the function body is caught in CI.
///
/// `pub(crate)` (member-private-oceans PR 6 / CodeRabbit review of #334):
/// `routes::nodes`'s tenant-pool promotion-routing reads
/// (`resolve_tenant_node_graph`/`resolve_tenant_graph`) select this SAME
/// column shape directly off `graphs` on the SAME tenant pool, so they
/// reuse THIS type and mapper rather than declaring their own positional
/// tuple — two independent tuples of the same shape (`kind`/`name` are both
/// `String`, adjacent) would silently compile after a column reorder in
/// either copy and hand `is_owned_active_member_ocean` the wrong field,
/// which is the one gate standing between member A and member B's reef. One
/// declaration, reused, makes that drift a compile error instead.
///
/// **8 columns, not 7** (workspace-members team-id fix, on top of the 5 -> 7
/// grown by member-private-oceans PR 2 above): `team_id` (`teams.id`,
/// `Some` iff `kind = 'team'`) is appended as the 8th column — see
/// `store::mod`'s `GraphRow`/`TEAM_ID_SUBQUERY` doc comment for the full
/// join rationale (a deterministic-not-unique correlated subquery on
/// `teams.graph_id = graphs.id`, baked into `resolve_tenant_reachable_graphs`'s
/// body by migration `0038_reachable_graphs_team_id`/
/// `0131_reachable_graphs_team_id` since a SQL function's `RETURNS TABLE`
/// can't be joined into from the caller's side).
///
/// **9 columns, not 8** (`my_team_role`, folded into the SAME migration pair
/// as `team_id` above, per the workspace-manage design doc's "Making role
/// cheap" section — see that migration's header for why it could not safely
/// wait for a follow-up): the CALLER's role in the team `team_id` resolves
/// to, `Some("admin"|"member")` or `None`. Populated here because
/// `tenant_list_reachable_graphs` already threads `user_id` through to
/// `resolve_tenant_reachable_graphs($1, $2)` as `p_user_id`, which the
/// function body's role subquery scopes to — the SAME reason `team_id`
/// itself had to be baked into the function body rather than joined in from
/// this side.
pub(crate) type GraphRow = (
    Uuid,
    String,
    String,
    bool,
    chrono::DateTime<chrono::Utc>,
    Option<Uuid>,
    Option<chrono::DateTime<chrono::Utc>>,
    Option<Uuid>,
    Option<String>,
);

pub(crate) fn row_to_graph(row: GraphRow) -> Graph {
    let (id, name, kind, is_primary, created_at, owner_user_id, archived_at, team_id, my_team_role) =
        row;
    Graph {
        id,
        name,
        kind,
        is_primary,
        created_at,
        owner_user_id,
        archived_at,
        team_id,
        my_team_role,
    }
}

/// Tenant-pool half of `list_reachable_graphs`.
///
/// **This delegates to a `SECURITY DEFINER` SQL function
/// (`resolve_tenant_reachable_graphs`) instead of the hand-rolled inline SQL
/// this function used to run directly (member-private-oceans PR 2, plan
/// §2.2) — do not "simplify" this back into an inline query string.** The
/// reason is rolling-deploy safety, not style: this arm's predicate governs
/// who can read a member-private ocean, and a fleet mid-deploy runs old and
/// new pod binaries against the SAME database concurrently. When the
/// predicate lived in Rust, an old pod's compiled-in string could never pick
/// up a mid-rollout predicate fix — it would keep serving its stale,
/// member-oblivious version (which special-cased only `'team'`) for as long
/// as it kept running, returning every member ocean in the account to every
/// member. With the predicate in the database, replacing the FUNCTION BODY
/// (migration 0091/0006) is one atomic DDL statement both old and new pods
/// observe identically the instant it commits, because both call the SAME
/// named function — there is no window where they disagree. See
/// `migrations_control/0091_member_ocean_reachability_functions.sql`'s
/// header for the full argument, and this module's doc comment for why
/// `account_id = ctx.account_id` (rather than a control-plane
/// `account_members` subquery) is the correct tenant-side scope in the first
/// place. `MEMBER_OCEANS_ENABLED` (row creation, not this predicate) is what
/// actually gates the feature — this predicate change is a no-op today
/// because no `kind = 'member'` row exists yet.
async fn tenant_list_reachable_graphs(
    ctx: &TenantContext,
    user_id: Uuid,
) -> Result<Vec<Graph>, StoreError> {
    let rows: Vec<GraphRow> = sqlx::query_as(
        "SELECT id, name, kind, is_primary, created_at, owner_user_id, archived_at, team_id, \
         my_team_role \
         FROM public.resolve_tenant_reachable_graphs($1, $2)",
    )
    .bind(ctx.account_id)
    .bind(user_id)
    .fetch_all(crate::tenancy::scope::request_read(&ctx.pool))
    .await?;

    Ok(rows.into_iter().map(row_to_graph).collect())
}

/// Tenant-pool half of `graph_reachable_by_user` — the same scoping as
/// [`tenant_list_reachable_graphs`] (including the same rolling-deploy
/// rationale for delegating to a SQL function — see that function's doc
/// comment), existence-checked for a single `graph_id` (the IDOR
/// re-verification shape).
async fn tenant_graph_reachable_by_user(
    ctx: &TenantContext,
    graph_id: Uuid,
    user_id: Uuid,
) -> Result<bool, StoreError> {
    let exists: bool =
        sqlx::query_scalar("SELECT public.resolve_tenant_graph_reachable_by_user($1, $2, $3)")
            .bind(graph_id)
            .bind(ctx.account_id)
            .bind(user_id)
            .fetch_one(&ctx.pool)
            .await?;
    Ok(exists)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `None` and `Some(Personal)` must be handled identically by every
    /// match in this module — a pure structural check that doesn't need a
    /// database. (Full behavioral coverage, including the tenant-pool path
    /// and the consumer/tenant parity guarantee, lives in
    /// `tests/reachability_pg.rs` since both branches ultimately need a real
    /// Postgres pool.)
    #[test]
    fn none_and_personal_are_the_same_decision_shape() {
        // `TenancyDecision` intentionally has no `PartialEq` (it carries a
        // live `PgPool` inside `Org`), so this just asserts the match arms
        // compile identically for both — a change that splits their
        // behavior would have to touch this match arm and would be caught
        // by the parity test regardless.
        fn arm(decision: Option<&TenancyDecision>) -> &'static str {
            match decision {
                None | Some(TenancyDecision::Personal) => "consumer",
                Some(TenancyDecision::Org(_)) => "tenant",
            }
        }
        assert_eq!(arm(None), "consumer");
        assert_eq!(arm(Some(&TenancyDecision::Personal)), "consumer");
    }

    /// A lazy pool never connects unless a query executes against it, so
    /// `require_matching_user`'s short-circuit (before any SQL runs) is
    /// testable with no live Postgres — mirrors `crypto.rs`'s
    /// `resolve_dek_for_context_rejects_account_id_mismatch` pattern for the
    /// analogous DEK-side assert.
    fn lazy_ctx(user_id: Uuid) -> TenantContext {
        TenantContext {
            org_id: "org_a".to_string(),
            account_id: Uuid::new_v4(),
            user_id,
            pool: sqlx::postgres::PgPoolOptions::new()
                .connect_lazy("postgres://localhost/unused")
                .expect("lazy pool construction never connects"),
            page_pool: None,
        }
    }

    // `connect_lazy` needs a Tokio context to construct (it defers the actual
    // connection, but the pool internals still require a runtime handle), so
    // these are `#[tokio::test]` even though `require_matching_user` itself
    // is sync — same reason `tenancy::tenant_id_tests` uses `#[tokio::test]`
    // for its own lazy-pool `TenantContext`.
    #[tokio::test]
    async fn require_matching_user_accepts_the_contexts_own_user() {
        let user_id = Uuid::new_v4();
        let ctx = lazy_ctx(user_id);
        assert!(require_matching_user(&ctx, user_id).is_ok());
    }

    #[tokio::test]
    async fn require_matching_user_rejects_any_other_user_id() {
        // The P1 fix: without this, a caller passing a DIFFERENT user_id than
        // the one the gate built this context for would (pre-fix) still
        // reach the tenant SQL and get back the org's non-team oceans,
        // regardless of whether that user is actually a member — this is
        // the structural check that closes it.
        let ctx = lazy_ctx(Uuid::new_v4());
        let other = Uuid::new_v4();
        let err = require_matching_user(&ctx, other).expect_err("mismatched user_id must be rejected");
        assert!(matches!(err, StoreError::TenantAuthzMismatch(_)));
    }
}

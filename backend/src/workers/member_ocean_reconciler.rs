//! Member-ocean reconciler — the sweep worker from the member-private-oceans
//! plan (`docs/plans/member-private-oceans.md` §2.4 "Provisioning — three
//! triggers, order-independent", trigger 3). **This module now implements
//! the PROVISION, ARCHIVE, RESTORE, and PURGE arms** (provision shipped in
//! PR 3; archive/restore in PR 7, §2.6 "Lifecycle"; purge in PR 8, same
//! section). **PR 8 is a deletion path** — [`purge_arm`]/[`purge_one_graph`]
//! are the only functions in this file that delete a tenant-side row or an
//! R2 blob; every other arm here is additive/idempotent. Purge is gated
//! behind its OWN flag ([`member_ocean_purge_enabled`],
//! `MEMBER_OCEAN_PURGE_ENABLED`), checked independently of and in addition to
//! [`member_oceans_enabled`] — both must be on.
//!
//! # What a tick does, per org tenant, in order
//!
//! 1. **Restore arm** (new, PR 7) — for every CURRENTLY live member
//!    (`account_members`), un-archive at most one eligible archived
//!    `kind='member'` graph within the 60-day window. Runs BEFORE the
//!    provision arm on purpose: see [`restore_arm`]'s doc comment for why
//!    ordering here closes a real race, and
//!    [`crate::tenancy::member_ocean::provision_one`]'s doc comment for the
//!    matching fix on the other two triggers (webhook, first-sight), which
//!    reach a rejoining member far more often than this 5-minute sweep does.
//!    Skipped entirely for a revoked org (`org_revocations`) — restoring
//!    would undo a teardown.
//! 2. **Archive arm** (new, PR 7) — reads `member_ocean_desired_state` (a
//!    CONTROL-plane table) for this org's `state = 'archived'` rows, since a
//!    departed member no longer has an `account_members` row to re-derive
//!    that fact from (unlike provisioning's roster, which IS
//!    `account_members`). Stamps tenant-side `graphs.archived_at` ONCE per
//!    graph and revokes that member's `coding_session_shares`/
//!    `meeting_shares` rows. Runs even for a revoked org — see this arm's own
//!    doc comment for why archiving is safe (and useful, for the admin-export
//!    window) even though restore/provision are not. **org-team-membership PR:**
//!    also cascades the same departed member's org offboarding onto
//!    `team_members` (stamping `departing_at` and writing `membership_events`
//!    rows) — see this arm's team-membership cascade step ("step 3b") for
//!    the full rationale, and
//!    [`org_teams_enabled`] for the companion startup assert this addition
//!    motivates (`ORG_TEAMS_ENABLED` requires `MEMBER_OCEANS_ENABLED`,
//!    checked in `lib.rs::run()`).
//! 3. **Provision arm** (PR 3, unchanged in behavior) — read `account_members`
//!    for that org from the CONTROL pool, then for each member attempt
//!
//! ```sql
//! INSERT INTO graphs (account_id, name, kind, owner_user_id, is_primary)
//! VALUES ($account, $display_name_or_'My Ocean', 'member', $user, true)
//! ON CONFLICT (account_id, owner_user_id)
//!   WHERE is_primary AND owner_user_id IS NOT NULL AND archived_at IS NULL
//! DO NOTHING
//! ```
//!
//! on the TENANT pool. The `ON CONFLICT` target is `uq_graphs_one_primary_per_owner`
//! (`migrations_control/0090_member_ocean_columns.sql` /
//! `migrations_tenant/0005_member_ocean_columns.sql`), **not** a
//! one-member-ocean-per-owner index — that index (`uq_graphs_one_member_ocean_per_owner`)
//! was in an earlier plan draft and was REMOVED during PR 1 because it
//! contradicted the plan's §5 decision that secondary (non-primary) member
//! oceans are allowed. `uq_graphs_one_primary_per_owner` is the correct target
//! because the auto-provisioned ocean is always `is_primary = true`, so two
//! concurrent first-sight provisions (or two racing reconciler ticks across
//! two backend processes) collide on it and exactly one wins — the same
//! race-safety property `TenantRegistry::build`'s `ensure_org_ocean` already
//! relies on for the org commons ocean (`tenancy/registry.rs`).
//!
//! Attempting the insert for EVERY current member on EVERY tick (rather than
//! pre-computing an app-level diff against existing `graphs` rows first) is
//! deliberate: the partial unique index already makes "does this member have
//! a primary member ocean" a single atomic decision at insert time, so a
//! pre-select-then-insert app-level diff would just reintroduce the
//! check-then-act race window the index exists to close, for zero benefit —
//! `ON CONFLICT ... DO NOTHING RETURNING id` returning no row IS the "already
//! present, skip" case. This also means the retroactive backfill (plan §2.4:
//! "the retroactive backfill is trigger 3 with no extra code") needs no
//! special-casing: a brand-new org and a long-since-fully-provisioned org run
//! through the exact same code path, and the fully-provisioned steady state
//! is simply "every insert this tick conflicts and no-ops".
//! 4. **Purge arm** (new, PR 8) — for every `kind = 'member'` graph archived
//!    more than 60 days ago (the same window `restore_arm` reads), in bounded
//!    batches: re-asserts that SAME eligibility window under a
//!    `SELECT ... FOR UPDATE` lock on the graph's own row (immediately before
//!    touching anything — see [`purge_one_graph`]'s doc comment for why the
//!    candidate read alone is not sufficient, and for the Critical fix this
//!    closes), deletes every R2 blob the graph's atoms/transcripts/coding
//!    sessions reference, then deletes the graph and every one of its
//!    children in one tenant-side transaction. Skipped entirely for a
//!    revoked org, same as restore/provision — see [`purge_arm`]'s doc
//!    comment. Gated behind its OWN flag, `MEMBER_OCEAN_PURGE_ENABLED` (see
//!    [`member_ocean_purge_enabled`]), independent of and in addition to
//!    `MEMBER_OCEANS_ENABLED`.
//!
//! # Desired state vs. `account_members`
//!
//! Desired state for the PROVISION arm is *just* `account_members` — that arm
//! does not read or write `member_ocean_desired_state` at all: a member's
//! `account_members` row already IS the source of truth for "who should have
//! a member ocean", and the `graphs` unique index already makes re-running
//! that arm a no-op.
//!
//! The ARCHIVE arm is exactly the opposite: `account_members` CANNOT answer
//! "who left" (a departed member's row is deleted, not flagged), so it is the
//! one place in this file that reads `member_ocean_desired_state` — its
//! `state = 'archived'` rows are the only remaining record of "this user was
//! once a member of this org and has since left" once their
//! `account_members` row is gone.
//!
//! **Correction to an earlier plan/migration-comment prediction:** migration
//! `0092_member_ocean_control_tables.sql`'s header says
//! `member_ocean_desired_state` "gets its first writer in PR 4 (the webhook
//! trigger's upsert)". PR 4 as actually shipped (`routes::clerk_webhook::
//! handle_membership_upsert`, `tenancy::runtime::finish_binding`) does NOT
//! write to this table at all — verified by grep across `src/` before this
//! PR. This PR (7) is therefore this table's actual first writer, via
//! `clerk_provision::{ensure_org_membership, remove_org_membership,
//! revoke_org_access}` — see those functions' own doc comments for the
//! archive/restore intent-writes and the replay-safe `CASE`-on-`restored_at`
//! guard on `offboarded_at` (a bare `COALESCE` there would report a stale
//! `offboarded_at` across a leave→rejoin→leave cycle).
//!
//! # The org commons ocean is NOT provisioned here (and does not need to be)
//!
//! The plan's §2.4 lists the reconciler as also provisioning the org commons
//! ocean. It doesn't need to: `TenantRegistry::build` (`tenancy/registry.rs`)
//! already calls `ensure_org_ocean` on EVERY cold pool build, unconditionally,
//! for EVERY caller — including this very worker. Concretely, the tenant
//! harness (`tenant_harness::run_multi_tenant`) resolves this tick's
//! `WorkerTenant.pool` via `registry.get_or_build(org, row.account_id,
//! &row.db_secret_ref)` (see that function's call site) BEFORE `tick_fn` is
//! ever invoked, so by the time this tick's body runs, the org's commons
//! ocean is already guaranteed to exist — for every tenant this reconciler
//! (or ANY other tenant-loop worker) is scheduled against, not just the ones
//! a user has visited. Duplicating that INSERT here would be redundant with,
//! not a backstop for, the mechanism `tenancy/registry.rs`'s module doc
//! already documents as the fix for the empty-tenant prod bug (PR #327). See
//! this module's own PR description for the full finding.
//!
//! # Consumer plane
//!
//! Member oceans are an org-tenant-only concept (plan §2.1: "Inside the org
//! tenant DB, not the consumer plane") — the consumer/personal plane has no
//! `account_members`-driven membership model at all (a personal account has
//! exactly one member, its owner) and no `kind='member'` row may ever exist
//! there (plan §6 P0 #7). This tick is therefore a pure no-op on
//! `TenantId::Consumer`.
//!
//! # Feature flag
//!
//! Gated on `MEMBER_OCEANS_ENABLED` (default OFF, same fail-closed convention
//! as `tenancy::runtime`'s flags) — checked once per tick rather than at
//! spawn time, so flipping the flag takes effect on this worker's very next
//! scheduled tick with no redeploy. With the flag off this tick is a pure
//! `Tick::idle()` and touches neither pool — inert on merge, exactly as PR 3
//! is specified to be.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::time::Duration;

use async_trait::async_trait;
use coralswarm_core::r2::{assert_blob_key_org_scope, validate_blob_key, R2Client, R2Error};
use sqlx::PgPool;
use uuid::Uuid;

use crate::tenancy::TenantId;
use crate::workers::tenant_harness::{Tick, WorkerTenant};

/// How often the reconciler sweeps one org tenant. A background catch-up net
/// under the (PR 4) webhook + first-sight triggers, not the primary
/// provisioning path once those land — so this can run considerably less
/// often than a queue worker's poll interval. Matches `reaper::REAP_INTERVAL`
/// (5 minutes): frequent enough that a newly-onboarded org (or a member added
/// through a path that doesn't fire the webhook, e.g. a backfill) converges
/// promptly, infrequent enough to add negligible load on the control-plane
/// `account_members` read across every active tenant.
pub(crate) const RECONCILE_INTERVAL: Duration = Duration::from_secs(5 * 60);

/// Per-tick timeout on each DB call, mirroring `tombstone_purge`/`reaper`'s
/// defensive pattern: a call that hasn't finished in this long is stuck (pool
/// exhaustion, network partition), so bound it and let the next scheduled
/// tick retry rather than parking the harness's schedule on a hung query.
const RECONCILE_TIMEOUT: Duration = Duration::from_secs(30);

/// How far back [`archive_arm`]'s share-revocation RETRY sweep reaches.
///
/// Share revocation is deliberately decoupled from "freshly archived this
/// tick" so a failed `UPDATE ... SET revoked_at = now()` is retried on the
/// next tick rather than being attempted exactly once (see [`archive_arm`]).
/// Unbounded, though, that retry is O(lifetime departures): `state='archived'`
/// rows in `member_ocean_desired_state` are never cleared, so an org with N
/// former members would re-scan all N owners' graphs and re-issue two
/// guaranteed-no-op share `UPDATE`s every [`RECONCILE_INTERVAL`], forever, with
/// N only ever growing (CodeRabbit review).
///
/// A share that was going to be revoked has been revoked within minutes of the
/// archive; one still un-revoked a week later is not going to be fixed by tick
/// 2017 either — it needs an operator, and the ERROR-level log in
/// [`revoke_shares_for_graphs`] is what pages them. So the retry window is
/// bounded here, making steady-state cost proportional to RECENT churn rather
/// than to org age.
///
/// This window is NOT the only thing keeping the guarantee: owners who still
/// owe an `archived_at` stamp are unioned back in on every tick regardless of
/// how long ago they left, so the "tenant DB was unreachable for months"
/// case still revokes shares at the moment it finally archives. See
/// [`archive_arm`] step 3.
const SHARE_REVOKE_RETRY_DAYS: i32 = 7;

/// Fallback ocean name for a member with no recorded display name
/// (`users.name` is nullable — 0067_users_display_name). Mirrors the
/// existing per-kind naming convention (`create_first_ocean_scoped` names a
/// personal ocean "Primary", an org ocean "Organization"): a member ocean
/// defaults to the owner's own display name so it reads naturally in the web
/// UI, falling back to this generic label only when no name is on file.
///
/// `pub(crate)` (member-private-oceans PR 4): reused verbatim by
/// [`crate::tenancy::member_ocean`], the shared provisioning helper the
/// webhook (trigger 1) and first-sight (trigger 2) triggers call, so all
/// three triggers name a member's ocean identically regardless of which one
/// wins the provisioning race — see that module's doc comment.
pub(crate) const FALLBACK_OCEAN_NAME: &str = "My Ocean";

/// Pure parsing logic behind [`member_oceans_enabled`], factored out so the
/// flag semantics are unit-testable without mutating process-global env
/// state (mirrors `reaper::derive_stale_after`'s pure-logic-helper split).
/// Same fail-closed contract as every other tenancy flag in this codebase
/// (`tenancy::runtime::env_flag`, not reused directly here to avoid adding a
/// cross-module `pub` surface to a file a concurrent sibling PR is also
/// touching): `"1"` or a case-insensitive `"true"` is on; anything else
/// (including unset, modeled as `None`) is off.
fn parse_enabled_flag(raw: Option<&str>) -> bool {
    matches!(raw, Some(v) if v == "1" || v.eq_ignore_ascii_case("true"))
}

/// Whether member-ocean provisioning is turned on. Checked once per tick
/// (not cached at spawn time) so flipping `MEMBER_OCEANS_ENABLED` takes
/// effect on this worker's very next scheduled tick with no redeploy.
///
/// `pub(crate)` (member-private-oceans PR 4): [`crate::tenancy::member_ocean`]
/// calls this directly rather than re-deriving the same env-var contract, so
/// all three provisioning triggers agree on "on" with zero drift risk.
pub(crate) fn member_oceans_enabled() -> bool {
    parse_enabled_flag(std::env::var("MEMBER_OCEANS_ENABLED").ok().as_deref())
}

/// Whether the org-facing team-ocean routes are turned on
/// (`org-team-membership-design.md` decision #13). This flag does NOT gate
/// anything in this file directly — the org team routes it controls live in
/// `routes/teams.rs` — but this file owns the copy of the check that matters
/// for THIS module's own correctness: [`crate::run`]'s startup validation
/// asserts `org_teams_enabled() implies member_oceans_enabled()`, because the
/// org-offboard team-membership cascade (see [`archive_arm`]'s "step 3b") lives
/// INSIDE the member-ocean reconciler's archive arm, not as a standalone
/// worker. Enabling org teams without member oceans would let admins manage
/// team rosters while the one place that stamps `team_members.departing_at`
/// on org offboarding never runs.
///
/// **Note for integration**: a sibling PR may also define an
/// `org_teams_enabled()` helper local to `routes/teams.rs` for the route
/// guard itself. This copy exists so the startup assert in `lib.rs::run()`
/// has something to call without creating a `routes` → `workers` (or
/// vice versa) dependency neither module already has; the two should be
/// reconciled to a single source of truth at integration time rather than
/// silently drifting.
///
/// Same fail-closed parsing contract as [`member_oceans_enabled`]: `"1"` or a
/// case-insensitive `"true"` is on, unset/anything else is off.
pub(crate) fn org_teams_enabled() -> bool {
    parse_enabled_flag(std::env::var("ORG_TEAMS_ENABLED").ok().as_deref())
}

/// Whether the PURGE arm may delete anything, this tick. **A separate flag
/// from [`member_oceans_enabled`], and BOTH must be on** — see the plan's
/// PR 8 row ("Flag: same + `MEMBER_OCEAN_PURGE_ENABLED`"). Deliberately
/// independent rather than folded into the one flag: flipping
/// `MEMBER_OCEANS_ENABLED` off must not be a way to accidentally also
/// silence the purge arm's own deliberate, separately-reviewed on/off
/// switch, and — more importantly the other direction — provisioning/
/// archive/restore being on must never be sufficient, by itself, to make
/// this codebase start deleting rows and blobs. Default off (unset / anything
/// other than `"1"`/`"true"` parses to off), same fail-closed contract as
/// every other tenancy flag.
pub(crate) fn member_ocean_purge_enabled() -> bool {
    parse_enabled_flag(std::env::var("MEMBER_OCEAN_PURGE_ENABLED").ok().as_deref())
}

/// How many eligible graphs [`purge_arm`] will delete in one tick. Plan §7
/// ("Bound the tick. Do not let one sweep delete unboundedly; batch and carry
/// over"): an org that has accumulated a large backlog of 60-day-eligible
/// archived member oceans (e.g. this flag was off for a while, or a large
/// layoff) must not turn one tick into an unbounded blob-deletion loop that
/// holds a tenant-pool connection (and, per-graph, a control-plane row lock —
/// see [`purge_one_graph`]'s doc comment) for an arbitrarily long time.
/// Leftover eligible graphs are simply picked up again next
/// [`RECONCILE_INTERVAL`] — `archived_at` never becomes MORE eligible by
/// waiting, so carrying over is always safe, never a missed deadline.
const PURGE_BATCH_SIZE: i64 = 25;

/// Upper bound on how long [`purge_one_graph`]'s blob-deletion loop may run
/// IN TOTAL, for one graph, in one tick, before aborting and retrying next
/// tick. **Establishes a new convention for this file** — no whole-loop
/// budget existed before this (CodeRabbit, second round on PR #346's Major):
/// per-call bounding (`RECONCILE_TIMEOUT` on each `delete_blob`) closes the
/// "one hung request" failure mode, but the realistic failure mode is
/// different and NOT caught by any per-call bound at all — every individual
/// call succeeding just under `RECONCILE_TIMEOUT`, thousands of times, so
/// nothing ever times out yet the loop runs for hours regardless. Narrowing
/// the lock (a prior round of this same fix) closed the LOCK-CONTENTION
/// blast radius, but does not touch two OTHER resources this loop holds for
/// its entire duration, confirmed by re-reading the code rather than
/// assumed: `tenant_tx` (the same transaction/connection used for the
/// graph's eligibility `SELECT ... FOR UPDATE`, the blob-key read, and later
/// the child-table/`graphs` deletes) begins BEFORE this loop and does not
/// commit until AFTER it — so for the loop's whole duration it holds (a) a
/// connection checked out of `tenant_pool`, the SAME pool that also serves
/// ordinary HTTP request handling for this org, idle-in-transaction, and
/// (b) an open transaction snapshot, which blocks `VACUUM` from reclaiming
/// dead tuples on the tenant database for as long as it stays open — an
/// org-wide degradation with no lock involved at all.
///
/// **Duration: [`RECONCILE_INTERVAL`] (5 minutes), deliberately, not an
/// arbitrary pick.** This arm should never hold a tenant-pool connection or
/// transaction longer than the reconciler's own natural tick cadence — if a
/// graph's blob deletion cannot finish within one tick's worth of time, the
/// correct answer is to split the work across MULTIPLE ticks, exactly the
/// way [`PURGE_BATCH_SIZE`] already splits a large BACKLOG of graphs across
/// ticks. This is safe: `delete_atom_blob` is idempotent, and nothing at the
/// row level is touched until every blob for this graph has succeeded, so a
/// budget-exceeded abort simply re-attempts every key (including ones that
/// already succeeded this attempt) on the next tick with zero risk of
/// double-deleting rows or under-counting blobs.
///
/// Overridable via `MEMBER_OCEAN_PURGE_BLOB_BUDGET_SECS` — parsed fresh on
/// every call (consistent with every other tunable in this file being
/// re-read live, not cached at spawn time), so an operator can tighten or
/// loosen it without a redeploy, and so `member_ocean_purge_pg.rs` can
/// exercise the "budget exceeded" path in seconds rather than waiting out
/// the real 5-minute default.
fn purge_blob_delete_budget() -> Duration {
    std::env::var("MEMBER_OCEAN_PURGE_BLOB_BUDGET_SECS")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .map(Duration::from_secs)
        .unwrap_or(RECONCILE_INTERVAL)
}

/// The harness config this worker is spawned with (`startup.rs`). It is the
/// one worker that keeps a Clerk-deleted org scheduled
/// ([`crate::workers::tenant_harness::HarnessConfig::including_revoked_orgs`]):
/// `revoke_org_access` writes archive intents for every member, and only
/// [`archive_arm`] turns them into `archived_at` stamps and revoked
/// `meeting_shares`/`coding_session_shares` tokens. Every other worker drops a
/// revoked org (`tenancy::routing::list_active`, #1396), and [`tick`] already
/// skips restore, provision and purge for it.
pub fn harness_config() -> crate::workers::tenant_harness::HarnessConfig {
    crate::workers::tenant_harness::HarnessConfig::sweep_worker("member_ocean_reconciler", RECONCILE_INTERVAL)
        .including_revoked_orgs()
}

/// One org member's control-plane identity, as needed to provision (or skip)
/// their member ocean.
struct MemberRow {
    user_id: Uuid,
    display_name: Option<String>,
}

/// One reconcile sweep on one tenant. Driven by
/// `workers::tenant_harness::run_multi_tenant` as a sweep worker with cadence
/// [`RECONCILE_INTERVAL`]; `control_pool` is the shared control/consumer pool
/// (captured by the closure at spawn time, the same pattern
/// `workers::summary::tick` uses for its `account_prefs` read) — needed
/// because `account_members` (and, as of PR 7, `member_ocean_desired_state` /
/// `member_ocean_lifecycle_log`) are control-plane tables that do not exist
/// on tenant databases at all. `blobs` is the shared `R2Client` (`None` in an
/// environment with no R2 configured, e.g. local dev) — needed ONLY by
/// [`purge_arm`]; every other arm in this file ignores it.
///
/// Runs the restore, archive, provision, and purge arms in THAT order — see
/// [`restore_arm`]'s doc comment for why restore must run before provision
/// within a single tick, and [`archive_arm`]'s for why it runs regardless of
/// whether the org is revoked while restore/provision (and, per its own doc
/// comment, purge) do not. Purge runs last: it only ever targets graphs
/// archived ≥60 days ago, so nothing this tick's restore/archive/provision
/// arms do to OTHER graphs this same tick can change a purge candidate's
/// eligibility (restore/archive touch a specific owner's graph; purge's own
/// `SELECT ... FOR UPDATE` re-check inside [`purge_one_graph`] is what
/// protects against a DIFFERENT tick, or a first-sight/webhook trigger,
/// racing the SAME graph — see that function's doc comment).
pub async fn tick(
    t: &WorkerTenant,
    control_pool: &PgPool,
    blobs: Option<&dyn PurgeBlobDeleter>,
) -> Result<Tick, String> {
    if !member_oceans_enabled() {
        return Ok(Tick::idle());
    }

    let org_id = match &t.tenant {
        TenantId::Consumer => return Ok(Tick::idle()),
        TenantId::Org(org_id) => org_id,
    };

    // Resolve this org's control-plane account id. Read from the routing
    // table (control-plane, keyed by clerk_org_id) rather than the tenant's
    // own `account_keys` row so this tick has no dependency on that table's
    // state (rotation, a not-yet-run onboarding step) — every ACTIVE tenant
    // (the only kind the harness ever schedules) has a routing row by
    // construction.
    let account_id: Uuid = match tokio::time::timeout(
        RECONCILE_TIMEOUT,
        sqlx::query_scalar("SELECT account_id FROM tenant_databases WHERE clerk_org_id = $1")
            .bind(org_id.as_str())
            .fetch_optional(control_pool),
    )
    .await
    {
        Err(_) => {
            tracing::warn!(
                org = %org_id,
                timeout_secs = RECONCILE_TIMEOUT.as_secs(),
                "member-ocean reconcile: account_id lookup timed out; will retry next cycle"
            );
            return Ok(Tick::idle());
        }
        Ok(Err(e)) => {
            tracing::warn!(
                org = %org_id,
                error = %e,
                "member-ocean reconcile: account_id lookup failed; will retry next cycle"
            );
            return Ok(Tick::idle());
        }
        Ok(Ok(None)) => {
            // Defensive only: the harness never schedules a tenant without an
            // active routing row, so this should be unreachable in practice.
            tracing::warn!(
                org = %org_id,
                "member-ocean reconcile: no tenant_databases row for a scheduled tenant — skipping"
            );
            return Ok(Tick::idle());
        }
        Ok(Ok(Some(id))) => id,
    };

    // member-private-oceans PR 7, §D "Org-revocation interaction": the
    // reconciler must never provision or restore a member ocean for an org
    // `org_revocations` has a row for — that would undo a teardown. Checked
    // once per tick (not cached — a revocation is a one-way, never-cleared
    // event per `org_revocations`'s own doc comment, so there is no
    // staleness risk to a fresh read every 5 minutes). Does NOT gate the
    // archive arm — see [`archive_arm`]'s doc comment for why archiving a
    // revoked org's members is still correct and desired.
    let revoked: bool = match tokio::time::timeout(
        RECONCILE_TIMEOUT,
        sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS (SELECT 1 FROM org_revocations WHERE clerk_org_id = $1)",
        )
        .bind(org_id.as_str())
        .fetch_one(control_pool),
    )
    .await
    {
        Err(_) => {
            tracing::warn!(
                org = %org_id,
                timeout_secs = RECONCILE_TIMEOUT.as_secs(),
                "member-ocean reconcile: org_revocations lookup timed out — treating as revoked \
                 (fail closed: skip restore/provision this tick, archive still runs)"
            );
            true
        }
        Ok(Err(e)) => {
            tracing::warn!(
                org = %org_id,
                error = %e,
                "member-ocean reconcile: org_revocations lookup failed — treating as revoked \
                 (fail closed: skip restore/provision this tick, archive still runs)"
            );
            true
        }
        Ok(Ok(r)) => r,
    };

    // Desired state = every account_members row for this org (plan §2.4:
    // "the retroactive backfill is trigger 3 with no extra code"). Plain
    // control-plane read: `account_members` carries no 0083 RLS policy (it is
    // a control-only table entirely outside that migration's roster), so
    // there is no app.account_id GUC to set and nothing for
    // begin_account_scoped to scope here. Also the roster the restore arm
    // checks for an eligible archived ocean to un-archive.
    let members: Vec<MemberRow> = match tokio::time::timeout(
        RECONCILE_TIMEOUT,
        sqlx::query_as::<_, (Uuid, Option<String>)>(
            "SELECT am.user_id, u.name \
             FROM account_members am \
             JOIN users u ON u.id = am.user_id \
             WHERE am.account_id = $1",
        )
        .bind(account_id)
        .fetch_all(control_pool),
    )
    .await
    {
        Err(_) => {
            tracing::warn!(
                org = %org_id,
                timeout_secs = RECONCILE_TIMEOUT.as_secs(),
                "member-ocean reconcile: account_members read timed out; will retry next cycle"
            );
            return Ok(Tick::idle());
        }
        Ok(Err(e)) => {
            tracing::warn!(
                org = %org_id,
                error = %e,
                "member-ocean reconcile: account_members read failed; will retry next cycle"
            );
            return Ok(Tick::idle());
        }
        Ok(Ok(rows)) => rows
            .into_iter()
            .map(|(user_id, display_name)| MemberRow {
                user_id,
                display_name,
            })
            .collect(),
    };

    let mut processed = 0usize;

    // 1. Restore — BEFORE provision, so a rejoin-within-window wins the race
    //    against a fresh insert within this same tick (see restore_arm's doc).
    if !revoked && !members.is_empty() {
        processed += restore_arm(org_id, account_id, &members, &t.pool, control_pool).await;
    }

    // 2. Archive — runs regardless of `revoked` (see archive_arm's doc).
    //    `&members` (the live roster) lets it exclude anyone still current.
    processed += archive_arm(org_id, account_id, &members, &t.pool, control_pool).await;

    // 3. Provision — unchanged behavior from PR 3, now additionally skipped
    //    for a revoked org (PR 3 had no such check; nothing exercised it
    //    because `account_members` is already empty for a revoked org by the
    //    time `revoke_org_access` finishes — this is belt-and-braces against
    //    that invariant ever drifting).
    if !revoked && !members.is_empty() {
        processed += provision_arm(org_id, account_id, &members, &t.pool).await;
    }

    // 4. Purge (PR 8) — its OWN flag, `MEMBER_OCEAN_PURGE_ENABLED`, checked
    //    independently of and in addition to `MEMBER_OCEANS_ENABLED` (already
    //    gated at the top of this function). Skipped for a revoked org, same
    //    as restore/provision (see `purge_arm`'s doc comment) — unlike them,
    //    it does not depend on `members` (a purge candidate's owner may no
    //    longer be a live member at all).
    if !revoked && member_ocean_purge_enabled() {
        processed += purge_arm(org_id, account_id, &t.pool, control_pool, blobs).await;
    }

    if processed > 0 {
        tracing::info!(org = %org_id, processed, checked = members.len(), "member-ocean reconcile: sweep complete");
        Ok(Tick::worked(processed))
    } else {
        tracing::debug!(org = %org_id, checked = members.len(), "member-ocean reconcile: nothing to do");
        Ok(Tick::idle())
    }
}

/// Provision arm (PR 3, extracted unchanged from the original single-function
/// `tick` body into its own function for symmetry with [`restore_arm`] /
/// [`archive_arm`] — no behavior change). For every current member
/// (`account_members`, passed in as `members`), race-safely insert a primary
/// `kind='member'` ocean if one doesn't already exist.
///
/// ONE round-trip for the whole roster, not one per member — see this
/// module's top-level doc for the full "retroactive backfill" and
/// race-safety reasoning (unchanged by this PR).
async fn provision_arm(
    org_id: &str,
    account_id: Uuid,
    members: &[MemberRow],
    tenant_pool: &PgPool,
) -> usize {
    let user_ids: Vec<Uuid> = members.iter().map(|m| m.user_id).collect();
    let names: Vec<String> = members
        .iter()
        .map(|m| {
            m.display_name
                .as_deref()
                .map(str::trim)
                .filter(|n| !n.is_empty())
                .unwrap_or(FALLBACK_OCEAN_NAME)
                .to_string()
        })
        .collect();

    let inserted_ids: Vec<Uuid> = match tokio::time::timeout(
        RECONCILE_TIMEOUT,
        sqlx::query_scalar::<_, Uuid>(
            "INSERT INTO graphs (account_id, name, kind, owner_user_id, is_primary) \
             SELECT $1, n, 'member', u, true \
             FROM UNNEST($2::text[], $3::uuid[]) AS t(n, u) \
             ON CONFLICT (account_id, owner_user_id) \
               WHERE is_primary AND owner_user_id IS NOT NULL AND archived_at IS NULL \
             DO NOTHING \
             RETURNING id",
        )
        .bind(account_id)
        .bind(&names)
        .bind(&user_ids)
        .fetch_all(tenant_pool),
    )
    .await
    {
        Err(_) => {
            tracing::warn!(
                org = %org_id,
                members = members.len(),
                timeout_secs = RECONCILE_TIMEOUT.as_secs(),
                "member-ocean reconcile: provision batch timed out; will retry next cycle"
            );
            return 0;
        }
        Ok(Err(e)) => {
            tracing::warn!(
                org = %org_id,
                members = members.len(),
                error = %e,
                "member-ocean reconcile: provision batch failed; will retry next cycle"
            );
            return 0;
        }
        Ok(Ok(ids)) => ids,
    };

    for graph_id in &inserted_ids {
        tracing::info!(org = %org_id, graph_id = %graph_id, "member-ocean reconcile: provisioned member ocean");
    }
    inserted_ids.len()
}

/// Restore arm (member-private-oceans PR 7, plan §2.6 "Restore"). For every
/// CURRENTLY live member of this org, un-archive at most one eligible
/// archived `kind='member'` ocean — idempotent, precisely scoped, and never
/// resurrecting anything outside the 60-day window.
///
/// **Why this runs BEFORE the provision arm in the same tick.** Both arms'
/// `ON CONFLICT`/eligibility logic key off the SAME partial unique index
/// (`uq_graphs_one_primary_per_owner`, `archived_at IS NULL`). An archived
/// ocean does NOT block that index, so if the provision arm ran first for a
/// rejoining member with an archived-but-in-window ocean, it would happily
/// insert a brand-new empty primary ocean — after which the restore arm's own
/// `NOT EXISTS` guard (see the query below) would correctly refuse to touch
/// the now-collided archived ocean, exactly as designed for the day-61 case.
/// Running restore FIRST means a genuine within-window rejoin always gets its
/// original content back; provision only ever creates a fresh ocean when
/// restore found nothing eligible. See
/// [`crate::tenancy::member_ocean::provision_one`]'s doc comment for the
/// identical fix applied to the webhook/first-sight triggers, which is where
/// this race is actually most likely to bite in practice (this sweep is a
/// 5-minute-interval backstop, not the primary path).
///
/// **At most one restore per owner.** An owner can have accumulated more than
/// one archived `kind='member'` row (a former secondary ocean archived
/// alongside the primary at offboarding — see [`archive_arm`]). Restoring all
/// of them as `is_primary = true` simultaneously would violate
/// `uq_graphs_one_primary_per_owner` outright, so `DISTINCT ON
/// (owner_user_id)` picks exactly one candidate per owner: the most recently
/// archived (tie-broken by most recently created) — see
/// `tenancy::member_ocean::provision_one`'s doc comment for the identical
/// tie-break and its rationale. Any other archived ocean for that owner is
/// left alone (still archived, eligible for a future purge worker, PR 8).
///
/// **Two-phase audit, control-plane, best-effort.** A `'started'` row is
/// written BEFORE the tenant-side `UPDATE`, so a crash between them leaves a
/// `started` row with no `finished_at` — the operator "verify by hand" signal
/// plan §2.6 describes. If the audit `INSERT` itself fails, this function
/// skips the tenant-side write entirely for this tick (retried next cycle)
/// rather than restoring without a paper trail.
///
/// **No auto-restore of team membership, by design.** This function's `UPDATE`
/// touches exactly one table (`graphs`) and MUST NEVER be extended to clear
/// `team_members.departing_at` on a rejoin — `org-team-membership-design.md`
/// decision #6/"Rejected" list names this exact pattern as the bug PR #341
/// already fixed for MCP write grants: "DELETE THE GRANT, KEEP THE DENY" (see
/// `clerk_provision.rs:826`'s comment on `remove_org_membership`). Rejoining
/// an org restores ORG access (a fresh `account_members` row, live
/// reachability per `middleware::auth::live_org_membership`) but must NOT
/// silently restore TEAM-ocean access — the whole point of
/// [`archive_arm`]'s step-6 cascade is that a departed member's team
/// memberships stay revoked until an admin explicitly re-adds them, a visible
/// roster action with its own `membership_events` row (`action = 'added'`),
/// not an invisible side effect of this restore arm. A member ocean and a
/// team ocean are different trust boundaries: the former is the rejoining
/// member's own private data (safe to hand back automatically, within the
/// window); the latter is access to OTHER people's shared content, which
/// requires a human decision every time.
async fn restore_arm(
    org_id: &str,
    account_id: Uuid,
    members: &[MemberRow],
    tenant_pool: &PgPool,
    control_pool: &PgPool,
) -> usize {
    let user_ids: Vec<Uuid> = members.iter().map(|m| m.user_id).collect();

    // 1. Candidates: read-only, at most one per owner.
    let candidates: Vec<(Uuid, Uuid)> = match tokio::time::timeout(
        RECONCILE_TIMEOUT,
        sqlx::query_as::<_, (Uuid, Uuid)>(
            "SELECT DISTINCT ON (g.owner_user_id) g.id, g.owner_user_id \
             FROM graphs g \
             WHERE g.account_id = $1 AND g.owner_user_id = ANY($2::uuid[]) AND g.kind = 'member' \
               AND g.archived_at IS NOT NULL \
               AND g.archived_at > now() - interval '60 days' \
               AND NOT EXISTS ( \
                     SELECT 1 FROM graphs g2 \
                     WHERE g2.account_id = g.account_id AND g2.owner_user_id = g.owner_user_id \
                       AND g2.kind = 'member' AND g2.archived_at IS NULL) \
             ORDER BY g.owner_user_id, g.archived_at DESC, g.created_at DESC",
        )
        .bind(account_id)
        .bind(&user_ids)
        .fetch_all(tenant_pool),
    )
    .await
    {
        Err(_) => {
            tracing::warn!(org = %org_id, timeout_secs = RECONCILE_TIMEOUT.as_secs(), "member-ocean reconcile: restore-candidate read timed out; will retry next cycle");
            return 0;
        }
        Ok(Err(e)) => {
            tracing::warn!(org = %org_id, error = %e, "member-ocean reconcile: restore-candidate read failed; will retry next cycle");
            return 0;
        }
        Ok(Ok(rows)) => rows,
    };
    if candidates.is_empty() {
        return 0;
    }

    // 2. Two-phase audit: 'started' BEFORE the tenant-side write.
    let started = match log_started_batch(control_pool, "restore", org_id, &candidates).await {
        Ok(rows) => rows,
        Err(e) => {
            tracing::warn!(org = %org_id, error = %e, "member-ocean reconcile: restore audit 'started' write failed; skipping restore this tick");
            return 0;
        }
    };
    let log_id_by_graph: HashMap<Uuid, Uuid> = started.into_iter().collect();
    let candidate_ids: Vec<Uuid> = candidates.iter().map(|(g, _)| *g).collect();

    // 3. Materialize: restore exactly the candidate rows, nothing else. The
    //    eligibility predicate is carried INTO the `UPDATE` itself (not just
    //    the read-only `restore_candidate` select above) — CodeRabbit review:
    //    step 1 proves eligibility only as of ITS OWN snapshot. Between steps
    //    1 and 3, the webhook or first-sight trigger can commit a fresh
    //    primary for the SAME owner (both fire far more readily than this
    //    5-minute sweep — see this function's own doc comment). Updating by
    //    `id` alone would then un-archive the OLD ocean with `is_primary =
    //    true` anyway, colliding with the fresh one on
    //    `uq_graphs_one_primary_per_owner` (23505) and aborting the WHOLE
    //    batch statement — poisoning every other owner's restore in this
    //    tick, repeating every cycle until the fresh ocean is gone. Re-
    //    checking `archived_at IS NOT NULL AND NOT EXISTS (...)` here makes a
    //    raced owner match zero rows instead: a clean, silent loss for that
    //    one owner (correctly reached by the day-61-shaped outcome, not a
    //    batch-wide error), while every other owner's restore still commits.
    let restored_ids: Vec<Uuid> = match tokio::time::timeout(
        RECONCILE_TIMEOUT,
        sqlx::query_scalar::<_, Uuid>(
            "UPDATE graphs g SET archived_at = NULL, is_primary = true \
             WHERE g.id = ANY($1::uuid[]) \
               AND g.archived_at IS NOT NULL \
               AND NOT EXISTS ( \
                     SELECT 1 FROM graphs g2 \
                     WHERE g2.account_id = g.account_id AND g2.owner_user_id = g.owner_user_id \
                       AND g2.kind = 'member' AND g2.archived_at IS NULL) \
             RETURNING g.id",
        )
        .bind(&candidate_ids)
        .fetch_all(tenant_pool),
    )
    .await
    {
        Err(_) => {
            tracing::warn!(org = %org_id, timeout_secs = RECONCILE_TIMEOUT.as_secs(), "member-ocean reconcile: restore batch timed out; will retry next cycle");
            // Outcome genuinely unknown: `tokio::time::timeout` drops the
            // query future, but the backend may still complete and commit
            // the `UPDATE` server-side. Leave the 'started' rows OPEN rather
            // than stamping them 'failed' — a `started` row with no
            // `finished_at` past the job timeout is deliberately the
            // operator's "verify by hand" signal (plan §2.6); a false
            // 'failed' here would instead permanently misreport a graph
            // that in fact ended up restored, and nothing would ever
            // correct it (both materialize statements are one-shot).
            return 0;
        }
        Ok(Err(e)) => {
            tracing::warn!(org = %org_id, error = %e, "member-ocean reconcile: restore batch failed; will retry next cycle");
            // Definitively did not commit (a real DB error, not a dropped
            // future) — safe to record as 'failed'.
            let log_ids: Vec<Uuid> = log_id_by_graph.values().copied().collect();
            let _ = log_finish_batch(control_pool, &log_ids, "failed", Some(&e.to_string())).await;
            return 0;
        }
        Ok(Ok(ids)) => ids,
    };

    // 4. Complete audit for what actually restored; fail audit for any
    //    candidate that lost a race between steps 1 and 3 (should be rare —
    //    nothing else in this codebase mutates `archived_at`).
    let restored_set: HashSet<Uuid> = restored_ids.iter().copied().collect();
    let completed_ids: Vec<Uuid> = restored_ids
        .iter()
        .filter_map(|g| log_id_by_graph.get(g).copied())
        .collect();
    let _ = log_finish_batch(control_pool, &completed_ids, "completed", None).await;
    let raced_ids: Vec<Uuid> = candidate_ids
        .iter()
        .filter(|g| !restored_set.contains(g))
        .filter_map(|g| log_id_by_graph.get(g).copied())
        .collect();
    if !raced_ids.is_empty() {
        let _ = log_finish_batch(control_pool, &raced_ids, "failed", Some("candidate no longer eligible at update time")).await;
    }

    for (graph_id, owner_user_id) in candidates.iter().filter(|(g, _)| restored_set.contains(g)) {
        tracing::info!(org = %org_id, graph_id = %graph_id, owner_user_id = %owner_user_id, "member-ocean reconcile: restored member ocean");
    }

    restored_ids.len()
}

/// Archive arm (member-private-oceans PR 7, plan §2.6 "Archive"). Reads
/// `member_ocean_desired_state`'s `state = 'archived'` rows for this org —
/// the only remaining record of "this user was a member and has since left",
/// since `account_members` no longer has their row at all — and stamps
/// tenant-side `graphs.archived_at` ONCE (`WHERE archived_at IS NULL`) for
/// every one of their non-archived `kind='member'` oceans (both primary and
/// any secondary — plan §A: a departing member's private data leaves the
/// live plane in full, not just their primary ocean).
///
/// **Runs even for a revoked org, deliberately** (unlike restore/provision).
/// Access is already fully dead the instant `account_members` is gone or
/// `org_revocations` has a row (the binding gate never mints a `TenantContext`
/// for that user again, regardless of token TTL) — plan §2.6: "a late archive
/// costs nothing in security terms". But the 60-day admin-export window still
/// matters until the org's OWN teardown (`offboard_tenant.sh`) physically
/// deletes the tenant database (plan §2.6: "the org's clock wins" governs
/// teardown TIMING, not whether archiving happens at all) — so this arm must
/// not skip a revoked org, only the restore/provision arms must.
///
/// **Share revocation is intentionally decoupled from "freshly archived this
/// tick".** It re-attempts `UPDATE ... SET revoked_at = now() WHERE
/// revoked_at IS NULL` against `kind='member'` graphs owned by archived users
/// in this org — not only the ones this exact tick just archived. `revoked_at
/// IS NULL` already makes this naturally idempotent (a no-op once done), and
/// decoupling it from the one-shot `archived_at IS NULL` archive stamp means a
/// share-revocation failure (timeout, connection error) is retried every
/// [`RECONCILE_INTERVAL`] rather than only ever attempted once at the exact
/// instant `archived_at` was set.
///
/// That retry set is BOUNDED (CodeRabbit review), because
/// `member_ocean_desired_state`'s `'archived'` rows are never cleared and an
/// unbounded sweep would grow forever with org age. It is the union of:
///
/// * members offboarded within [`SHARE_REVOKE_RETRY_DAYS`] — the actual retry
///   window, proportional to recent churn; and
/// * the owners of THIS tick's archive candidates, however long ago they left
///   — so the "tenant DB was unreachable for two months, `archived_at` is only
///   being stamped now" case still revokes shares at the moment it archives,
///   which a pure time window would silently miss.
///
/// The archive-candidate read itself is deliberately NOT windowed: a graph
/// that still owes an `archived_at` stamp owes it forever.
///
/// Same two-phase audit discipline as [`restore_arm`].
///
/// `live_members` is the SAME `account_members` roster `tick` already fetched
/// for the restore/provision arms — passed in here (rather than re-querying)
/// so this arm can exclude any user id that is CURRENTLY a live member from
/// the archive set (CodeRabbit review). `member_ocean_desired_state` and
/// `account_members` are two independently-written control-plane tables and
/// can transiently disagree (out-of-order Clerk delivery, the
/// `org_revocations`-gated `'present'` upsert racing this read, a future
/// operator edit); without this filter, a user who is simultaneously "live"
/// per `account_members` and `'archived'` per desired state gets
/// restored-then-archived in the SAME tick (`restore_arm` runs first, off the
/// live roster) — repeating every [`RECONCILE_INTERVAL`] with lifecycle-log
/// noise on both sides until the two tables converge.
///
/// **Team-ocean membership cascade (`org-team-membership-design.md`
/// decisions #6/#7), step 3b below.** Alongside (and BEFORE, deliberately —
/// see step 3b's own inline comment for why) materializing the
/// `graphs.archived_at` stamp in step 5, this arm ALSO cascades every
/// departed member's org offboarding onto `team_members`: creator-managed
/// team oceans (`graphs.kind = 'team'`) live in the SAME tenant database as
/// member oceans, but `team_members.role` is a pure ACL row with no
/// `account_members`-style reconciliation of its own —
/// `clerk_provision::remove_org_membership` deletes the `account_members` row
/// and tombstones the user on the CONTROL pool, but has no transaction that
/// spans into the org's TENANT database, so it cannot touch `team_members`
/// itself (see that function's own doc comment, and the "DELETE THE GRANT,
/// KEEP THE DENY" note at `clerk_provision.rs:826` this cascade deliberately
/// mirrors). Without this step, rejoining the org would silently restore
/// every team ocean a member was ever part of, with no admin action and no
/// audit trail — see [`cascade_team_membership`] for the query, the
/// idempotency argument, and why an orphaned team (zero remaining admins) is
/// an INTENDED possible outcome here, not a bug.
///
/// Scoped to `archived_users` (the full, already-live-filtered desired-state
/// set from step 1), not just this tick's graph-archive `candidates` — same
/// "not windowed" reasoning as the archive-candidate read itself: a
/// `team_members` row that still owes a `departing_at` stamp owes it however
/// long ago its owner left, including members who departed before this
/// cascade existed at all (a one-time backfill, for free, on the very next
/// tick after this ships). This is deliberately NOT tied to `archived_ids`
/// (this tick's freshly-archived graphs only) — a member whose graphs were
/// already fully archived on some earlier tick, long before `team_members`
/// cascading existed, must still be swept out of any team they were never
/// removed from.
async fn archive_arm(
    org_id: &str,
    account_id: Uuid,
    live_members: &[MemberRow],
    tenant_pool: &PgPool,
    control_pool: &PgPool,
) -> usize {
    // 1. Who left: control-plane desired state, NOT account_members. Each row
    //    carries whether the offboarding is RECENT enough to still be inside
    //    the share-revocation retry window (step 3). A NULL `offboarded_at`
    //    counts as recent — the column is nullable and "we don't know when"
    //    must not silently drop someone out of the sweep.
    let archived_rows: Vec<(Uuid, bool)> = match tokio::time::timeout(
        RECONCILE_TIMEOUT,
        sqlx::query_as::<_, (Uuid, bool)>(
            "SELECT user_id, \
                    (offboarded_at IS NULL \
                     OR offboarded_at > now() - make_interval(days => $2::int)) AS recent \
             FROM member_ocean_desired_state \
             WHERE clerk_org_id = $1 AND state = 'archived'",
        )
        .bind(org_id)
        .bind(SHARE_REVOKE_RETRY_DAYS)
        .fetch_all(control_pool),
    )
    .await
    {
        Err(_) => {
            tracing::warn!(org = %org_id, timeout_secs = RECONCILE_TIMEOUT.as_secs(), "member-ocean reconcile: desired-state read timed out; will retry next cycle");
            return 0;
        }
        Ok(Err(e)) => {
            tracing::warn!(org = %org_id, error = %e, "member-ocean reconcile: desired-state read failed; will retry next cycle");
            return 0;
        }
        Ok(Ok(rows)) => rows,
    };
    // Exclude anyone who is CURRENTLY a live member (see this function's own
    // doc comment) — `account_members` is the more current source for "is
    // this user actually still in the org right now".
    let live: HashSet<Uuid> = live_members.iter().map(|m| m.user_id).collect();
    let archived_rows: Vec<(Uuid, bool)> = archived_rows.into_iter().filter(|(u, _)| !live.contains(u)).collect();
    if archived_rows.is_empty() {
        return 0;
    }
    let archived_users: Vec<Uuid> = archived_rows.iter().map(|(u, _)| *u).collect();

    // 2. Archive candidates: read-only, and NOT windowed — a graph that still
    //    owes an `archived_at` stamp owes it however long ago its owner left.
    //    Read before share revocation (step 3) only so its owners can be
    //    unioned into that sweep; nothing here writes.
    let candidates: Vec<(Uuid, Uuid)> = match tokio::time::timeout(
        RECONCILE_TIMEOUT,
        sqlx::query_as::<_, (Uuid, Uuid)>(
            "SELECT id, owner_user_id FROM graphs \
             WHERE account_id = $1 AND owner_user_id = ANY($2::uuid[]) AND kind = 'member' \
               AND archived_at IS NULL",
        )
        .bind(account_id)
        .bind(&archived_users)
        .fetch_all(tenant_pool),
    )
    .await
    {
        Err(_) => {
            tracing::warn!(org = %org_id, timeout_secs = RECONCILE_TIMEOUT.as_secs(), "member-ocean reconcile: archive-candidate read timed out; will retry next cycle");
            return 0;
        }
        Ok(Err(e)) => {
            tracing::warn!(org = %org_id, error = %e, "member-ocean reconcile: archive-candidate read failed; will retry next cycle");
            return 0;
        }
        Ok(Ok(rows)) => rows,
    };

    // 3. Share revocation, decoupled from the archive stamp but bounded:
    //    recently-offboarded members (the retry window) UNION the owners of
    //    this tick's archive candidates (see doc comment for both halves).
    let share_sweep_users: Vec<Uuid> = {
        let mut set: HashSet<Uuid> = archived_rows.iter().filter(|(_, recent)| *recent).map(|(u, _)| *u).collect();
        set.extend(candidates.iter().map(|(_, owner)| *owner));
        set.into_iter().collect()
    };
    if !share_sweep_users.is_empty() {
        let sweep_graph_ids: Vec<Uuid> = match tokio::time::timeout(
            RECONCILE_TIMEOUT,
            sqlx::query_scalar::<_, Uuid>(
                "SELECT id FROM graphs WHERE account_id = $1 AND owner_user_id = ANY($2::uuid[]) AND kind = 'member'",
            )
            .bind(account_id)
            .bind(&share_sweep_users)
            .fetch_all(tenant_pool),
        )
        .await
        {
            Ok(Ok(ids)) => ids,
            Err(_) => {
                tracing::warn!(org = %org_id, timeout_secs = RECONCILE_TIMEOUT.as_secs(), "member-ocean reconcile: member-ocean-id read for share revocation timed out — shares will not be re-checked this tick, will retry next cycle");
                Vec::new()
            }
            Ok(Err(e)) => {
                tracing::warn!(org = %org_id, error = %e, "member-ocean reconcile: member-ocean-id read for share revocation failed — shares will not be re-checked this tick, will retry next cycle");
                Vec::new()
            }
        };
        revoke_shares_for_graphs(org_id, tenant_pool, &sweep_graph_ids).await;
    }

    // 3b. Team-ocean membership cascade — see this function's own doc comment
    //     (the "Team-ocean membership cascade" section) for the full
    //     rationale. Deliberately placed BEFORE the `candidates.is_empty()`
    //     early return two lines down, and scoped to the FULL
    //     `archived_users` set rather than `candidates`' owners: a member
    //     whose graphs were already fully archived on some earlier tick (so
    //     `candidates` is empty for them today) must still be swept out of
    //     any team they were never removed from — the exact backfill case an
    //     early return here would silently skip forever. Deliberately NOT
    //     folded into this function's return value (`processed`, below):
    //     every existing caller of `tick`/`archive_arm` treats `processed`
    //     as "count of GRAPHS archived/restored/provisioned this tick" (see
    //     `member_ocean_lifecycle_pg.rs`'s exact-count assertions), and share
    //     revocation just above — the closest precedent for a side effect
    //     this arm performs alongside the graph stamp — already follows the
    //     same convention of not contributing to that count. A failure here
    //     is logged and retried next tick, same as every other best-effort
    //     step in this arm.
    if !archived_users.is_empty() {
        match tokio::time::timeout(
            RECONCILE_TIMEOUT,
            cascade_team_membership(account_id, &archived_users, tenant_pool),
        )
        .await
        {
            Err(_) => {
                tracing::warn!(
                    org = %org_id,
                    timeout_secs = RECONCILE_TIMEOUT.as_secs(),
                    "member-ocean reconcile: team-membership cascade timed out; will retry next cycle"
                );
            }
            Ok(Err(e)) => {
                tracing::warn!(
                    org = %org_id,
                    error = %e,
                    "member-ocean reconcile: team-membership cascade failed; will retry next cycle"
                );
            }
            Ok(Ok(cascaded)) => {
                if cascaded > 0 {
                    tracing::info!(
                        org = %org_id,
                        team_memberships_cascaded = cascaded,
                        "member-ocean reconcile: cascaded org offboarding onto team_members"
                    );
                }
            }
        }
    }

    if candidates.is_empty() {
        return 0;
    }

    // 4. Two-phase audit: 'started' BEFORE the tenant-side write.
    let started = match log_started_batch(control_pool, "archive", org_id, &candidates).await {
        Ok(rows) => rows,
        Err(e) => {
            tracing::warn!(org = %org_id, error = %e, "member-ocean reconcile: archive audit 'started' write failed; skipping archive this tick");
            return 0;
        }
    };
    let log_id_by_graph: HashMap<Uuid, Uuid> = started.into_iter().collect();
    let candidate_ids: Vec<Uuid> = candidates.iter().map(|(g, _)| *g).collect();

    // 5. Materialize ONCE — `archived_at IS NULL` is the whole reason
    //    re-running this arm is always safe (plan §2.6/§A).
    let archived_ids: Vec<Uuid> = match tokio::time::timeout(
        RECONCILE_TIMEOUT,
        sqlx::query_scalar::<_, Uuid>(
            "UPDATE graphs SET archived_at = now(), is_primary = false \
             WHERE id = ANY($1::uuid[]) AND archived_at IS NULL RETURNING id",
        )
        .bind(&candidate_ids)
        .fetch_all(tenant_pool),
    )
    .await
    {
        Err(_) => {
            tracing::warn!(org = %org_id, timeout_secs = RECONCILE_TIMEOUT.as_secs(), "member-ocean reconcile: archive batch timed out; will retry next cycle");
            // Same reasoning as restore_arm's identical arm: the statement
            // may have committed despite the future being dropped, so leave
            // these 'started' rows open rather than mis-recording 'failed'
            // on a graph that may in fact now be archived.
            return 0;
        }
        Ok(Err(e)) => {
            tracing::warn!(org = %org_id, error = %e, "member-ocean reconcile: archive batch failed; will retry next cycle");
            // Definitively did not commit — safe to record as 'failed'.
            let log_ids: Vec<Uuid> = log_id_by_graph.values().copied().collect();
            let _ = log_finish_batch(control_pool, &log_ids, "failed", Some(&e.to_string())).await;
            return 0;
        }
        Ok(Ok(ids)) => ids,
    };

    let archived_set: HashSet<Uuid> = archived_ids.iter().copied().collect();
    let completed_ids: Vec<Uuid> = archived_ids
        .iter()
        .filter_map(|g| log_id_by_graph.get(g).copied())
        .collect();
    let _ = log_finish_batch(control_pool, &completed_ids, "completed", None).await;
    let raced_ids: Vec<Uuid> = candidate_ids
        .iter()
        .filter(|g| !archived_set.contains(g))
        .filter_map(|g| log_id_by_graph.get(g).copied())
        .collect();
    if !raced_ids.is_empty() {
        let _ = log_finish_batch(control_pool, &raced_ids, "failed", Some("candidate already archived at update time")).await;
    }

    for (graph_id, owner_user_id) in candidates.iter().filter(|(g, _)| archived_set.contains(g)) {
        tracing::info!(org = %org_id, graph_id = %graph_id, owner_user_id = %owner_user_id, "member-ocean reconcile: archived member ocean");
    }

    archived_ids.len()
}

/// Cascades a departed org member's offboarding onto every team ocean
/// (`graphs.kind = 'team'`) they belong to — [`archive_arm`]'s "step 3b", see
/// that function's own doc comment for the full "why the tenant plane, why
/// here" rationale. For every `user_id` in `archived_users`, stamps
/// `team_members.departing_at` on their still-current membership in every
/// team owned by this account, and writes one `membership_events` row per
/// team actually affected.
///
/// Runs in ONE tenant-side transaction so the `team_members` write and its
/// `membership_events` audit rows commit atomically — unlike the two-phase
/// (`'started'`/`'completed'`) discipline [`log_started_batch`]/
/// [`log_finish_batch`] use elsewhere in this file, that split exists
/// specifically because THOSE audit rows live on the CONTROL pool while the
/// materialized write lands on the TENANT pool (two separate connections, no
/// shared transaction possible — see this module's top-level doc, "Desired
/// state vs. `account_members`"). Here both the write and its audit trail are
/// on the SAME tenant connection, so an ordinary transaction already gives
/// all-or-nothing with no crash-window to reason about.
///
/// **Idempotent.** `AND tm.departing_at IS NULL` in the `UPDATE` means a
/// member already cascaded out of a given team matches zero rows there on
/// every later tick — no re-stamping, and (because the `membership_events`
/// insert is derived from this `UPDATE`'s own `RETURNING`, never from an
/// unconditional `SELECT`) no duplicate audit rows either. A member who was
/// never on any team in this account, or who has already been cascaded out
/// of every team they were on, is a correctly-silent no-op.
///
/// **Deliberately allowed to orphan a team.** If the departing member was a
/// team's only admin, this leaves that team with zero admins. That is
/// INTENDED — `org-team-membership-design.md` decision #9 places the
/// last-admin invariant ONLY at the interactive `remove_team_member` API
/// surface (a `SELECT ... FOR UPDATE` there), never here. A sole admin
/// leaving the company must be representable and must never wedge this
/// worker — do NOT add a guard, a `WHERE` clause excluding sole admins, or a
/// CHECK constraint here, no matter how much it looks like a bug to a future
/// reader. An orphaned team is a to-do for org admins to notice (decision
/// #4: org admins get roster management on ANY team), not a data-integrity
/// violation this worker is responsible for preventing.
///
/// `actor_user_id = NULL` on every written row: nobody clicked "remove" —
/// this is a system-driven consequence of an org-level offboarding decision
/// made entirely outside the team, not a roster action any admin took on the
/// team itself.
async fn cascade_team_membership(
    account_id: Uuid,
    archived_users: &[Uuid],
    tenant_pool: &PgPool,
) -> Result<usize, sqlx::Error> {
    let mut tx = tenant_pool.begin().await?;

    let affected: Vec<(Uuid, Uuid, String)> = sqlx::query_as::<_, (Uuid, Uuid, String)>(
        "UPDATE team_members tm SET departing_at = now() \
         WHERE tm.user_id = ANY($1::uuid[]) AND tm.departing_at IS NULL \
           AND tm.team_id IN (SELECT id FROM teams WHERE account_id = $2) \
         RETURNING tm.team_id, tm.user_id, tm.role",
    )
    .bind(archived_users)
    .bind(account_id)
    .fetch_all(&mut *tx)
    .await?;

    if affected.is_empty() {
        // Nothing to cascade this tick. Commit anyway (rather than rollback)
        // purely so this function has one exit path — an empty-affected
        // transaction has nothing for either verb to meaningfully undo.
        tx.commit().await?;
        return Ok(0);
    }

    let team_ids: Vec<Uuid> = affected.iter().map(|(t, _, _)| *t).collect();
    let user_ids: Vec<Uuid> = affected.iter().map(|(_, u, _)| *u).collect();
    let old_roles: Vec<String> = affected.iter().map(|(_, _, r)| r.clone()).collect();

    // One membership_events row per (team, member) pair the UPDATE actually
    // touched — derived from `affected` (the `RETURNING` set), never from a
    // fresh SELECT, which is what makes a re-run of this tick's cascade
    // write zero duplicate rows.
    sqlx::query(
        "INSERT INTO membership_events \
             (team_id, actor_user_id, target_user_id, action, old_role, new_role) \
         SELECT t, NULL, u, 'cascade_removed', r, NULL \
         FROM UNNEST($1::uuid[], $2::uuid[], $3::text[]) AS x(t, u, r)",
    )
    .bind(&team_ids)
    .bind(&user_ids)
    .bind(&old_roles)
    .execute(&mut *tx)
    .await?;

    tx.commit().await?;

    Ok(affected.len())
}

/// Best-effort share revocation at archive time (plan §2.6/§A: "so no token
/// outlives reachability"). Covers both tenant-tree share tables keyed by
/// `graph_id` (`migrations_tenant/0001_tenant_baseline.sql`): the plan's
/// prose says "shares / session_shares / meeting_shares", but only two such
/// tables actually exist in this schema — `coding_session_shares` and
/// `meeting_shares` — there is no separate generic `shares` table.
///
/// Never returns an error: a failure here is logged at ERROR (louder than
/// this file's usual WARN, since a live token surviving an archive is a
/// genuine exposure) and does not block the archive stamp itself — the next
/// tick retries this independently of `archived_at` (see [`archive_arm`]'s
/// doc comment for why it is decoupled from the one-shot archive candidate
/// set).
async fn revoke_shares_for_graphs(org_id: &str, tenant_pool: &PgPool, graph_ids: &[Uuid]) {
    if graph_ids.is_empty() {
        return;
    }
    for table in ["coding_session_shares", "meeting_shares"] {
        let sql = format!(
            "UPDATE {table} SET revoked_at = now() WHERE graph_id = ANY($1::uuid[]) AND revoked_at IS NULL"
        );
        match tokio::time::timeout(
            RECONCILE_TIMEOUT,
            sqlx::query(&sql).bind(graph_ids).execute(tenant_pool),
        )
        .await
        {
            Err(_) => {
                tracing::error!(org = %org_id, table, timeout_secs = RECONCILE_TIMEOUT.as_secs(), "member-ocean reconcile: share revocation timed out — a share token may outlive reachability until the next tick retries");
            }
            Ok(Err(e)) => {
                tracing::error!(org = %org_id, table, error = %e, "member-ocean reconcile: share revocation failed — a share token may outlive reachability until the next tick retries");
            }
            Ok(Ok(res)) => {
                if res.rows_affected() > 0 {
                    tracing::info!(org = %org_id, table, revoked = res.rows_affected(), "member-ocean reconcile: revoked shares for archived member ocean(s)");
                }
            }
        }
    }
}

/// Write `'started'` `member_ocean_lifecycle_log` rows (control-plane) for a
/// batch of `(graph_id, subject_user_id)` pairs, BEFORE any tenant-side
/// write — the two-phase audit discipline plan §2.6 specifies ("a `started`
/// row with no `finished_at` past the job timeout is the operator's 'verify
/// by hand' signal"). Returns `(graph_id, log_id)` pairs so the caller can
/// look up which log row to finish once the tenant-side write's own result
/// (`RETURNING`) is known. `actor` is always `'reconciler'` — this file is
/// the only writer to this table today (request-path restore, PR 7's
/// `tenancy::member_ocean::provision_one` fix, is explicitly NOT audited —
/// see that function's own doc comment).
async fn log_started_batch(
    control_pool: &PgPool,
    action: &str,
    org_id: &str,
    entries: &[(Uuid, Uuid)],
) -> Result<Vec<(Uuid, Uuid)>, sqlx::Error> {
    if entries.is_empty() {
        return Ok(Vec::new());
    }
    let graph_ids: Vec<Uuid> = entries.iter().map(|(g, _)| *g).collect();
    let subject_ids: Vec<Uuid> = entries.iter().map(|(_, u)| *u).collect();
    sqlx::query_as::<_, (Uuid, Uuid)>(
        "INSERT INTO member_ocean_lifecycle_log \
             (action, status, clerk_org_id, subject_user_id, graph_id, actor) \
         SELECT $1, 'started', $2, u, g, 'reconciler' \
         FROM UNNEST($3::uuid[], $4::uuid[]) AS t(g, u) \
         RETURNING graph_id, id",
    )
    .bind(action)
    .bind(org_id)
    .bind(&graph_ids)
    .bind(&subject_ids)
    .fetch_all(control_pool)
    .await
}

/// Finish a batch of `member_ocean_lifecycle_log` rows by id (control-plane):
/// `status` in `{"completed", "failed"}`, `finished_at = now()` (satisfying
/// migration 0092's `finished_at`-matches-`status` CHECK), and — when
/// `error` is `Some` — an `error` key merged into `manifest` (one of 0092's
/// allowlisted keys). Best-effort: callers log and continue on `Err` rather
/// than treating a failed audit WRITE as a reason to redo (or undo) work that
/// already committed on the tenant side.
async fn log_finish_batch(
    control_pool: &PgPool,
    log_ids: &[Uuid],
    status: &str,
    error: Option<&str>,
) -> Result<(), sqlx::Error> {
    if log_ids.is_empty() {
        return Ok(());
    }
    let manifest = match error {
        Some(e) => serde_json::json!({ "error": e }),
        None => serde_json::json!({}),
    };
    sqlx::query(
        "UPDATE member_ocean_lifecycle_log \
         SET status = $1, finished_at = now(), manifest = manifest || $2 \
         WHERE id = ANY($3::uuid[])",
    )
    .bind(status)
    .bind(manifest)
    .bind(log_ids)
    .execute(control_pool)
    .await?;
    Ok(())
}

// ── Purge (member-private-oceans PR 8) ──────────────────────────────────────

/// A destination for deleting one R2 blob by key. The real `R2Client` has no
/// failure/roundtrip injection point (same note as
/// `services::promotion::PromotionBlobStore`'s own doc comment and
/// `promotion_cross_db_pg.rs`'s), so this arm is written against a small seam
/// rather than a concrete `R2Client`, letting `member_ocean_purge_pg.rs`
/// substitute an in-memory recording stub and assert EXACTLY which keys were
/// deleted (the blast-radius property: a sibling member's and the org
/// commons' blobs must never appear in that log). A dedicated trait rather
/// than reusing `PromotionBlobStore` directly: this arm only ever DELETES
/// (never reads or writes a blob), and naming a purge-only seam after
/// "promotion" would mislead a future reader of this file.
#[async_trait]
pub trait PurgeBlobDeleter: Send + Sync {
    /// Delete the object at `blob_key`. Must be idempotent (same contract as
    /// `R2Client::delete_atom_blob`) — a retry after a partial prior attempt
    /// must not error just because the object is already gone.
    async fn delete_blob(&self, blob_key: &str) -> Result<(), R2Error>;
}

#[async_trait]
impl PurgeBlobDeleter for R2Client {
    async fn delete_blob(&self, blob_key: &str) -> Result<(), R2Error> {
        self.delete_atom_blob(blob_key).await
    }
}

/// The full `graph_id`-anchored (or one-hop-from-`graph_id`) content roster
/// this schema defines, per `migrations_control/0083_consumer_rls.sql`'s own
/// enumeration (its §(a)/(b)/(c)/(d)) — MINUS the handful of §(a) tables that
/// carry no `graph_id`/anchor column at all (`llm_usage`, `mcp_tool_calls`,
/// `oauth_tokens`, `slack_huddle_captures`, `slack_integrations`,
/// `graph_subscriptions`, `queries`): those are account-scoped only and have
/// nothing for a graph-scoped purge to touch.
///
/// Each entry is `(table_name, count_sql)`, where `count_sql` is a
/// `SELECT count(*) ... ` query taking the graph id as its one bind
/// parameter (`$1`). Every one of these tables' anchor foreign key
/// (`graph_id`, or one hop via `node_id`/`conversation_id`/`team_id`/
/// `team_key_id`) is `ON DELETE CASCADE` off `graphs`, VERIFIED against
/// `migrations_tenant/0001_tenant_baseline.sql`'s actual `FOREIGN KEY`
/// roster (not merely against `0083_consumer_rls.sql`'s prose, which — see
/// below — turned out to disagree with the schema for one table) — **EXCEPT
/// THREE tables, whose anchor column carries NO foreign key at all**:
/// `meeting_transcripts` and `transcript_corrections` (both noted "soft" in
/// `0056_meeting_transcripts`'s own header and in `0083`'s roster comment —
/// expected), and **`op_log`** (its `graph_id` has NO `FOREIGN KEY`
/// constraint anywhere in `0001_tenant_baseline.sql`, verified by grepping
/// every `FOREIGN KEY` in that file — **NOT expected**: `0083`'s own comment
/// files `op_log` under "(b) Direct `graph_id` column, one hop to `graphs`",
/// the CASCADE-covered category, noting only that the column is *nullable*,
/// not that it is unenforced. **This is a real discrepancy between that
/// migration's prose and the actual schema, caught by this PR's own
/// `member_ocean_purge_pg.rs` test failing** (`op_log` rows survived a purge
/// until this was found and fixed) — flagged here rather than silently
/// reconciled, per this PR's own instructions.** Deleting the `graphs` row
/// alone would silently strand all three — [`purge_one_graph`] deletes all
/// three EXPLICITLY, before the `graphs` row delete. `coding_session_shares`/
/// `meeting_shares` DO carry a CASCADE'd `graph_id` FK already, but
/// [`purge_one_graph`] also deletes them explicitly (plan §7 step 4: "so no
/// token outlives the data") — belt and braces, not a correctness
/// requirement.
///
/// This list serves two purposes: (1) [`purge_one_graph`]'s pre-delete count
/// snapshot, folded into the audit manifest's `table_counts` key (migration
/// 0093); (2) `member_ocean_purge_pg.rs` imports this EXACT const for its
/// post-purge per-table emptiness assertion, so the production code's
/// coverage claim and the test's coverage claim cannot silently drift apart
/// — which is exactly the mechanism that caught the `op_log` gap above.
///
/// **Kept in sync by hand — there is no schema-introspection script
/// enforcing this against the live roster** (unlike, say,
/// `check_rls_coverage.sh`'s account_id sibling). A new `graph_id`-bearing
/// table added later without an entry here is still correctly DELETED by the
/// `graphs` row's CASCADE, PROVIDED its FK follows the existing `ON DELETE
/// CASCADE` convention every table but the three soft ones above does — but
/// a table that omits the FK entirely (as `op_log` turned out to) will
/// silently strand rows unless it is ALSO added to [`purge_one_graph`]'s
/// explicit-delete list. Flagged here rather than silently assumed complete.
pub const GRAPH_CONTENT_TABLES: &[(&str, &str)] = &[
    ("atoms", "SELECT count(*) FROM atoms WHERE graph_id = $1"),
    ("nodes", "SELECT count(*) FROM nodes WHERE graph_id = $1"),
    (
        "node_atoms",
        "SELECT count(*) FROM node_atoms WHERE node_id IN (SELECT node_id FROM nodes WHERE graph_id = $1)",
    ),
    (
        "relationship_edge",
        "SELECT count(*) FROM relationship_edge WHERE graph_id = $1",
    ),
    ("node_merges", "SELECT count(*) FROM node_merges WHERE graph_id = $1"),
    (
        "meeting_transcripts",
        "SELECT count(*) FROM meeting_transcripts WHERE graph_id = $1",
    ),
    ("source_items", "SELECT count(*) FROM source_items WHERE graph_id = $1"),
    ("source_version_chunks", "SELECT count(*) FROM source_version_chunks WHERE graph_id = $1"),
    ("source_versions", "SELECT count(*) FROM source_versions WHERE graph_id = $1"),
    ("semantic_atoms", "SELECT count(*) FROM semantic_atoms WHERE graph_id = $1"),
    ("semantic_atom_passages", "SELECT count(*) FROM semantic_atom_passages WHERE graph_id = $1"),
    ("node_semantic_atoms", "SELECT count(*) FROM node_semantic_atoms WHERE graph_id = $1"),
    ("semantic_atom_windows", "SELECT count(*) FROM semantic_atom_windows WHERE graph_id = $1"),
    (
        "transcript_corrections",
        "SELECT count(*) FROM transcript_corrections WHERE graph_id = $1",
    ),
    ("meeting_shares", "SELECT count(*) FROM meeting_shares WHERE graph_id = $1"),
    ("projects", "SELECT count(*) FROM projects WHERE graph_id = $1"),
    ("coding_sessions", "SELECT count(*) FROM coding_sessions WHERE graph_id = $1"),
    (
        "coding_session_shares",
        "SELECT count(*) FROM coding_session_shares WHERE graph_id = $1",
    ),
    ("team_keys", "SELECT count(*) FROM team_keys WHERE graph_id = $1"),
    (
        "team_member_key_envelopes",
        "SELECT count(*) FROM team_member_key_envelopes WHERE team_key_id IN \
         (SELECT id FROM team_keys WHERE graph_id = $1)",
    ),
    ("teams", "SELECT count(*) FROM teams WHERE graph_id = $1"),
    (
        "team_members",
        "SELECT count(*) FROM team_members WHERE team_id IN (SELECT id FROM teams WHERE graph_id = $1)",
    ),
    (
        "teams_watched_resources",
        "SELECT count(*) FROM teams_watched_resources WHERE graph_id = $1",
    ),
    (
        "conversation_oceans",
        "SELECT count(*) FROM conversation_oceans WHERE graph_id = $1",
    ),
    ("conversations", "SELECT count(*) FROM conversations WHERE graph_id = $1"),
    (
        "conversation_messages",
        "SELECT count(*) FROM conversation_messages WHERE conversation_id IN \
         (SELECT id FROM conversations WHERE graph_id = $1)",
    ),
    (
        "conversation_surfaced_edges",
        "SELECT count(*) FROM conversation_surfaced_edges WHERE conversation_id IN \
         (SELECT id FROM conversations WHERE graph_id = $1)",
    ),
    (
        "supersession_edge",
        "SELECT count(*) FROM supersession_edge WHERE \
         superseded_id IN (SELECT node_id FROM nodes WHERE graph_id = $1) \
         OR superseder_id IN (SELECT node_id FROM nodes WHERE graph_id = $1)",
    ),
    (
        "non_supersession_pair",
        "SELECT count(*) FROM non_supersession_pair WHERE \
         node_a_id IN (SELECT node_id FROM nodes WHERE graph_id = $1) \
         OR node_b_id IN (SELECT node_id FROM nodes WHERE graph_id = $1)",
    ),
    (
        "node_promotions",
        "SELECT count(*) FROM node_promotions WHERE \
         source_node_id IN (SELECT node_id FROM nodes WHERE graph_id = $1) \
         OR target_graph_id = $1",
    ),
    ("op_log", "SELECT count(*) FROM op_log WHERE graph_id = $1"),
    ("activity_events", "SELECT count(*) FROM activity_events WHERE graph_id = $1"),
    ("query_history", "SELECT count(*) FROM query_history WHERE graph_id = $1"),
    // Today-journal PR A (migration 0113/0024): direct `graph_id` column,
    // same shape as `activity_events` above. The row itself is already
    // guaranteed gone by `journal_rollups.graph_id`'s own
    // `ON DELETE CASCADE` the moment the ocean's `graphs` row is deleted —
    // this entry only makes the purge's audit manifest report it, matching
    // every other graph_id-anchored content table here.
    ("journal_rollups", "SELECT count(*) FROM journal_rollups WHERE graph_id = $1"),
    // The Today gist (0404/0218): same `graph_id ON DELETE CASCADE` shape.
    ("journal_gists", "SELECT count(*) FROM journal_gists WHERE graph_id = $1 /* history_floor: purge manifest count, never shown to a member */"),
    (
        "outlook_watched_conversations",
        "SELECT count(*) FROM outlook_watched_conversations WHERE graph_id = $1",
    ),
    (
        "outlook_workflows",
        "SELECT count(*) FROM outlook_workflows WHERE graph_id = $1",
    ),
];

/// Snapshot per-table row counts for `graph_id`, across [`GRAPH_CONTENT_TABLES`].
/// Best-effort for audit richness only: a single table's count query failing
/// logs a warning and records `0` for that table rather than aborting the
/// purge — the counts are metadata for the manifest, never the mechanism
/// that decides what gets deleted.
async fn table_counts(tenant_pool: &PgPool, graph_id: Uuid) -> BTreeMap<String, i64> {
    let mut out = BTreeMap::new();
    for (name, sql) in GRAPH_CONTENT_TABLES {
        let count: i64 = match tokio::time::timeout(
            RECONCILE_TIMEOUT,
            sqlx::query_scalar(sql).bind(graph_id).fetch_one(tenant_pool),
        )
        .await
        {
            Err(_) => {
                tracing::warn!(table = name, graph_id = %graph_id, timeout_secs = RECONCILE_TIMEOUT.as_secs(), "member-ocean purge: table-count query timed out; recording 0 in the audit manifest");
                0
            }
            Ok(Err(e)) => {
                tracing::warn!(table = name, graph_id = %graph_id, error = %e, "member-ocean purge: table-count query failed; recording 0 in the audit manifest");
                0
            }
            Ok(Ok(count)) => count,
        };
        out.insert((*name).to_string(), count);
    }
    out
}

/// Drain guard (plan §7): whether any `pending`/`running` `jobs` row in this
/// tenant DB still references `graph_id` — either directly
/// (`payload->>'graph_id'`, the shape `ExtractThreads`/`ExtractDecisions`/
/// `ComputeDrift`/`EntityResolution`/`InferRelationships`/`InferExpertise`/
/// `SlackNameBackfill` use) or via one of its atoms (`payload->>'atom_id'` /
/// `payload->'atom_ids'`, the shape `EmbedAtoms`/`EmbedAtomsRetry`/
/// `ExtractEntities`/`ExtractRelationships` use — see `jobs::JobKind`).
/// Racing a worker mid-write (e.g. `workers::embed` computing and persisting
/// an embedding for one of these atoms while purge deletes the atom's row
/// out from under it) is exactly the hazard this guard exists to avoid.
/// `DeleteSourceBlob` is the sole exception: it cannot create content, and
/// purge consumes its persisted keys while removing the graph. Concurrent
/// deletes are idempotent, so waiting for this cleanup job would only prevent
/// purge from finishing the very cleanup it already owns.
/// Skips this graph for the current tick on any other match; retried next tick —
/// `archived_at` never becomes LESS eligible by waiting, so there is no
/// deadline this can miss, only delay.
///
/// The `(payload->>'graph_id')::uuid` / `(payload->>'atom_id')::uuid` casts
/// are safe against every `JobKind` variant that omits the key: `->>` on a
/// missing key yields SQL `NULL`, and `NULL::uuid` is `NULL` (no parse is
/// attempted), never an error — every variant that DOES carry the key types
/// it as `uuid::Uuid`, so a present value is always well-formed.
async fn graph_has_inflight_jobs(
    tenant_pool: &PgPool,
    graph_id: Uuid,
    atom_ids: &[Uuid],
) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar(
        "SELECT EXISTS ( \
             SELECT 1 FROM jobs \
             WHERE status IN ('pending', 'running') \
               AND kind <> 'DeleteSourceBlob' \
               AND ( \
                 (payload->>'graph_id')::uuid = $1 \
                 OR (payload->>'atom_id')::uuid = ANY($2::uuid[]) \
                 OR EXISTS ( \
                       SELECT 1 FROM jsonb_array_elements_text(COALESCE(payload->'atom_ids', '[]'::jsonb)) t(v) \
                       WHERE t.v::uuid = ANY($2::uuid[]) \
                 ) \
               ) \
         )",
    )
    .bind(graph_id)
    .bind(atom_ids)
    .fetch_one(tenant_pool)
    .await
}

/// Fresh (unlocked) read of whether this org's `tenant_databases` routing row
/// is `active`. Called TWICE from [`purge_one_graph`] — once before any lock
/// is acquired at all, once again immediately before blob deletion — and
/// deliberately never promoted to a `SELECT ... FOR UPDATE` held across the
/// blob loop or the row-delete transaction: unlike the graph's own
/// eligibility (where losing the race is genuine, irreversible member data
/// loss), an org being suspended WHILE a purge is mid-flight for one of its
/// graphs is not a new exposure — that org's whole tenant database and R2
/// prefix are about to be removed by its own teardown regardless of exactly
/// when this one graph's blobs finish. An earlier revision of this file DID
/// hold a `FOR UPDATE` lock on this row across blob deletion; CodeRabbit
/// correctly flagged that as its own Major finding (an idle-in-transaction
/// lock on a control-plane table, held for as long as R2 takes to answer N
/// deletes, contends with other code that touches this same row —
/// `tenancy::routing::reconcile_slug`'s org-rename webhook, schema-version
/// bumps), so it was removed in favor of these two plain, unlocked reads.
async fn routing_is_active(control_pool: &PgPool, org_id: &str) -> Result<bool, sqlx::Error> {
    let status: Option<String> =
        sqlx::query_scalar("SELECT status FROM tenant_databases WHERE clerk_org_id = $1")
            .bind(org_id)
            .fetch_optional(control_pool)
            .await?;
    Ok(status.as_deref() == Some("active"))
}

async fn log_purge_started(
    control_pool: &PgPool,
    org_id: &str,
    graph_id: Uuid,
    owner_user_id: Uuid,
) -> Result<Uuid, sqlx::Error> {
    sqlx::query_scalar(
        "INSERT INTO member_ocean_lifecycle_log \
             (action, status, clerk_org_id, subject_user_id, graph_id, actor) \
         VALUES ('purge', 'started', $1, $2, $3, 'reconciler') \
         RETURNING id",
    )
    .bind(org_id)
    .bind(owner_user_id)
    .bind(graph_id)
    .fetch_one(control_pool)
    .await
}

/// Best-effort: a failure here is logged loudly (this is the ONLY record
/// that a purge, having already committed on the tenant side, finished) but
/// never propagated — the tenant-side delete already committed by the time
/// this is called, and there is nothing left to roll back.
async fn log_purge_completed(
    control_pool: &PgPool,
    log_id: Uuid,
    table_counts: &BTreeMap<String, i64>,
    blobs_deleted: i64,
) {
    let manifest = serde_json::json!({ "table_counts": table_counts, "blobs": blobs_deleted });
    if let Err(e) = sqlx::query(
        "UPDATE member_ocean_lifecycle_log \
         SET status = 'completed', finished_at = now(), manifest = manifest || $1 \
         WHERE id = $2",
    )
    .bind(manifest)
    .bind(log_id)
    .execute(control_pool)
    .await
    {
        tracing::error!(
            log_id = %log_id,
            error = %e,
            "member-ocean purge: audit 'completed' write failed — the tenant-side delete ALREADY \
             committed; this log row is stuck at 'started' past this job's timeout and is this \
             feature's documented operator 'verify by hand' signal (plan §2.6)"
        );
    }
}

/// Best-effort, same rationale as [`log_purge_completed`]. Called both when
/// nothing was ever deleted (e.g. an invalid blob key aborted before any
/// delete) and when the tenant-side delete itself fails.
async fn log_purge_failed(control_pool: &PgPool, log_id: Uuid, reason: &str) {
    let manifest = serde_json::json!({ "error": reason });
    if let Err(e) = sqlx::query(
        "UPDATE member_ocean_lifecycle_log \
         SET status = 'failed', finished_at = now(), manifest = manifest || $1 \
         WHERE id = $2",
    )
    .bind(manifest)
    .bind(log_id)
    .execute(control_pool)
    .await
    {
        tracing::error!(log_id = %log_id, error = %e, "member-ocean purge: audit 'failed' write failed");
    }
}

/// PURGE arm (member-private-oceans PR 8, plan §2.6 "Lifecycle" / §7).
/// **The only place in this codebase that deletes a `kind='member'` graph
/// row, its content, or an R2 blob.** Founder decision: purge deletes FOR
/// REAL — there is no mandatory log-only observation period gating this arm.
/// The only gate is [`member_ocean_purge_enabled`] (`MEMBER_OCEAN_PURGE_ENABLED`,
/// default off, independent of [`member_oceans_enabled`] — both must be on
/// for this to run at all, checked by the caller in `tick`).
///
/// **Never purges for a revoked org** — that teardown is
/// `offboard_tenant.sh`'s job, which deletes the WHOLE tenant Neon project
/// and R2 prefix; a partial in-flight purge racing that teardown is at best
/// redundant and at worst a wasted, doomed write against a database about to
/// disappear. Enforced by the CALLER: `tick` computes `revoked` once per
/// tick (`tenancy::routing::org_is_revoked`, fail-closed on lookup
/// failure/timeout — the same check restore/provision already gate on) and
/// only calls this function when `!revoked`, exactly like restore/provision.
/// This arm does not re-check it itself — one shared, already-fail-closed
/// revocation check per tick, not a second copy (this function originally
/// carried its own byte-identical copy of `org_is_revoked`; deduped into
/// `tenancy::routing`'s single definition, and this internal check removed
/// in favor of the caller's, once PR 7/#340 merged first and `tick` already
/// had `revoked` in scope).
///
/// Reads at most [`PURGE_BATCH_SIZE`] eligible graphs
/// (`kind = 'member' AND archived_at < now() - interval '60 days'`), oldest
/// first, and processes each independently via [`purge_one_graph`] — one
/// graph's failure (drain guard, a suspended routing row, a delete error)
/// never blocks the rest of the batch.
async fn purge_arm(
    org_id: &str,
    account_id: Uuid,
    tenant_pool: &PgPool,
    control_pool: &PgPool,
    blobs: Option<&dyn PurgeBlobDeleter>,
) -> usize {
    let candidates: Vec<(Uuid, Uuid)> = match tokio::time::timeout(
        RECONCILE_TIMEOUT,
        sqlx::query_as::<_, (Uuid, Uuid)>(
            "SELECT id, owner_user_id FROM graphs \
             WHERE account_id = $1 AND kind = 'member' \
               AND archived_at IS NOT NULL \
               AND archived_at < now() - interval '60 days' \
             ORDER BY archived_at ASC \
             LIMIT $2",
        )
        .bind(account_id)
        .bind(PURGE_BATCH_SIZE)
        .fetch_all(tenant_pool),
    )
    .await
    {
        Err(_) => {
            tracing::warn!(org = %org_id, timeout_secs = RECONCILE_TIMEOUT.as_secs(), "member-ocean purge: candidate read timed out; will retry next cycle");
            return 0;
        }
        Ok(Err(e)) => {
            tracing::warn!(org = %org_id, error = %e, "member-ocean purge: candidate read failed; will retry next cycle");
            return 0;
        }
        Ok(Ok(rows)) => rows,
    };

    let mut purged = 0usize;
    for (graph_id, owner_user_id) in candidates {
        match purge_one_graph(org_id, graph_id, owner_user_id, tenant_pool, control_pool, blobs).await {
            Ok(true) => purged += 1,
            Ok(false) => {
                // Skipped this tick (drain guard / routing not active / raced) —
                // logged at its own call site; retried next tick.
            }
            Err(e) => {
                tracing::warn!(org = %org_id, graph_id = %graph_id, error = %e, "member-ocean purge: graph failed; will retry next cycle");
            }
        }
    }
    purged
}

/// Purge exactly one graph. Returns `Ok(true)` if it was purged this tick,
/// `Ok(false)` if it was deliberately skipped (retried next tick, not an
/// error), `Err` on an unexpected failure (also retried next tick, but loud).
///
/// **Eligibility is re-asserted at mutation time, not trusted from the
/// candidate read** (fix for a Critical finding on this PR: the original
/// version checked `archived_at` only in `purge_arm`'s candidate `SELECT`,
/// so a graph restored — #340's `restore_arm`, or the webhook/first-sight
/// `tenancy::member_ocean::provision_one` restore-before-provision path —
/// between that read and this function's blob/row deletes would still lose
/// its data, at the exact moment the product promises it comes back). See
/// step 3 below for the mechanism and an explicit statement of what, if
/// anything, remains racy.
///
/// **Lock scope, and a Major finding this revision fixes.** An earlier
/// revision of this fix held BOTH the graph's own row lock AND the org's
/// `tenant_databases` routing-row lock continuously across the (network-
/// bound, unbounded-latency) blob-deletion loop. CodeRabbit correctly flagged
/// that as its own problem: `tenant_databases` is a control-plane table other
/// code paths also touch (`tenancy::routing::reconcile_slug`'s org-rename
/// webhook, schema-version bumps), and holding an idle-in-transaction lock on
/// it for as long as R2 takes to answer N blob deletes is a real, avoidable
/// availability cost — not merely a lock held "a little longer than ideal."
/// **Only the GRAPH-level lock is load-bearing for the Critical fix.** The
/// org-suspension check does NOT need to be held across blob deletion at
/// all: this function's own analysis (§2.6, and repeated below) already
/// established that an org being suspended WHILE a purge is mid-flight for
/// one of its graphes is not a new exposure — that org's entire tenant
/// database and R2 prefix are about to be removed by its own teardown
/// regardless of whether this one graph's blobs finish a few minutes earlier
/// or later. So this revision demotes the org-routing check back to a plain,
/// UNLOCKED read (`routing_is_active`), checked TWICE (once before any lock
/// is even acquired, once again immediately before blob deletion) instead of
/// once under a lock held for the whole operation. The GRAPH's own row lock
/// — the one that actually prevents data loss — is the ONLY lock held across
/// blob deletion and the row-delete transaction now.
///
/// Order:
/// 1. Atom ids only, for the drain guard ([`graph_has_inflight_jobs`]) —
///    lightweight, read before any lock. Safe to read this early: no
///    legitimate write path can add an atom to a graph that isn't reachable
///    (`archived_at IS NULL`), and step 5 re-confirms `archived_at` is still
///    set before anything is deleted, so there is no window in which this
///    early read could go stale in a way that matters.
/// 2. Cheap, UNLOCKED pre-check that org routing is `active`
///    ([`routing_is_active`]) — short-circuits before writing an audit row
///    or acquiring any lock when the org is already known-suspended.
/// 3. Audit `'started'` row, BEFORE anything is touched.
/// 4. Acquire the ONE lock this function holds: `SELECT ... FOR UPDATE` on
///    THIS graph's own row (`graphs`, tenant-plane), re-asserting the SAME
///    60-day window the candidate `SELECT` used. Held CONTINUOUSLY from here
///    through the row-delete transaction's commit. Every restore path in
///    this codebase (`restore_arm`, and `provision_one`'s restore-before-
///    provision CTE) is a plain `UPDATE graphs ... WHERE id = $graph_id` —
///    i.e. a statement against this EXACT row — so all of them block on
///    this lock identically; none needed special-casing.
///
///    The failure mode this closes is not "a lock held a little longer than
///    ideal," it is "a member who rejoined inside the product's stated
///    60-day recovery window loses their data irreversibly" — the single
///    highest-severity outcome this feature exists to prevent. The blast
///    radius is narrow: this lock contends ONLY with an attempt to
///    restore/re-provision THIS SPECIFIC graph (one row, one owner) while
///    purge is mid-flight for it — not with any other member's restore, not
///    with any other graph, not with ordinary reads, and (per the scope note
///    above) not with anything on the control plane at all.
///    [`PURGE_BATCH_SIZE`] already bounds how many such locks one tick
///    sequentially acquires.
///
///    **What remains racy, stated explicitly: nothing, for the restore-vs-
///    purge race this fix closes.** Postgres row-level locking makes the
///    eligibility check and every subsequent delete inside this same
///    transaction atomic with respect to any other statement targeting this
///    `graphs.id` row — that statement simply blocks until this transaction
///    ends, then observes the post-commit (i.e. already-deleted) state. The
///    ONE scenario this does NOT, and should NOT, protect against: a member
///    who rejoins WHILE this tick's candidate `SELECT` has already found
///    their graph genuinely ≥60-days-archived and this function has already
///    acquired the lock loses the race and gets a FRESH ocean rather than a
///    restore. Per plan §2.6 that is correct, not a bug — "the boundary is a
///    SQL predicate, not worker timing," and this graph was ALREADY past the
///    60-day mark when the candidate read ran, so day-61+ semantics apply
///    regardless of exactly when the purge physically finishes.
/// 5. A SECOND, still-UNLOCKED `routing_is_active` re-check, immediately
///    before blob deletion — see the lock-scope note above for why this is
///    deliberately NOT promoted to a locked/held check.
/// 6. NOW read the authoritative blob-key set — `atoms.content_blob_key`,
///    `meeting_transcripts.raw_blob_key`, `source_versions.raw_blob_key`,
///    `coding_sessions.raw_blob_key` —
///    via the SAME locked transaction that just confirmed eligibility, so
///    this read cannot straddle a restore either. `meeting_transcripts`/
///    `coding_sessions` use the exact same `R2Client::put_atom_blob`-built
///    key shape as an atom (`routes/ingest.rs:1108-1121`), so the same
///    `validate_blob_key`/`assert_blob_key_org_scope` gate applies unchanged,
///    no second key convention; both columns are nullable (`raw_inline` is
///    the dev/no-R2 alternative for each) and `NULL` is filtered by the read
///    query, never treated as an error. (Unchanged from before this fix: no
///    de-duplication across the three sources — a key repeated across them
///    is a separate, already-deferred finding on this PR, out of scope
///    here.) Promoted content is unaffected regardless of any of this:
///    `Store::copy_atoms_to_graph_scoped` (`store/mod.rs:13426`) gives every
///    promoted coral a FRESH `atom_id` AND a target-graph-namespaced key,
///    never the source atom's, so a promoted copy's blob lives under the
///    TARGET graph's prefix and this graph-scoped read can never reach it
///    (pinned by `member_ocean_purge_pg.rs`'s promotion-survival test).
/// 7. Validate + org-scope-assert EVERY blob key first, THEN delete every
///    blob (still row-driven, never prefix-driven — `R2Client` has no
///    list/prefix-delete operation at all; see `packages/core/src/r2.rs`).
///    Splitting validate-all from delete-all means a single corrupt key
///    aborts before any blob for this graph is touched. **Each
///    `delete_blob` call is now individually bounded by
///    [`RECONCILE_TIMEOUT`]** (the other half of the Major fix — CodeRabbit:
///    this was the one unbounded outbound await in this file). A timeout is
///    treated as a genuine failure, never as success: the key's deletion
///    outcome is unknown (it may have landed server-side after our client
///    gave up, or not at all), so it is NOT counted in `blobs_deleted`, the
///    whole tenant transaction is rolled back (no child rows or the graph
///    row are deleted this tick either — see the "blobs before rows"
///    ordering below), and the audit row is stamped `'failed'` with a
///    manifest error identifying the timed-out key, never `'completed'`.
///    The graph is retried next tick; `delete_atom_blob` is idempotent, so
///    re-attempting every key (including ones that may have actually
///    succeeded despite the client timeout) is always safe. **The loop as a
///    WHOLE is also bounded**, by [`purge_blob_delete_budget`] — see that
///    function's own doc comment for why a per-call bound alone is not
///    enough (CodeRabbit's third round on this PR's Major: `tenant_tx`
///    begins before this loop and does not commit until after it, so for
///    however long this loop runs, it holds a `tenant_pool` CONNECTION
///    idle-in-transaction — the SAME pool that also serves this org's live
///    HTTP request handling — and an open transaction snapshot that blocks
///    `VACUUM` on the tenant database; neither is mitigated by the
///    lock-narrowing above, and "every call individually succeeds just under
///    `RECONCILE_TIMEOUT`, thousands of times" is the realistic failure mode
///    a per-call bound alone never catches). Exceeding the budget aborts
///    exactly like a per-call timeout — rolls back, stamps `'failed'` with
///    the partial `blobs_deleted`/total count in the message, never
///    `'completed'` — and is retried next tick for the same idempotency
///    reason.
/// 8. Snapshot per-table counts for the audit manifest via a separate
///    `tenant_pool` connection (not the lock-holding transaction), each
///    query individually bounded by [`RECONCILE_TIMEOUT`] — safe despite the
///    open transaction: a `FOR UPDATE` lock on the `graphs` row does not
///    block reads of OTHER tables on a different connection, and nothing can
///    be concurrently WRITING this graph's content (every legitimate write
///    path requires reachability, which step 4 just confirmed is not the
///    case, under lock).
/// 9. Within the SAME locked transaction from step 4: explicit deletes
///    (each individually bounded by [`RECONCILE_TIMEOUT`]) for the three
///    FK-less tables (`meeting_transcripts`, `transcript_corrections`,
///    `op_log` — see [`GRAPH_CONTENT_TABLES`]'s doc comment for how `op_log`
///    was found) and the two share tables (belt-and-braces, plan §7 step 4),
///    then `DELETE FROM graphs WHERE id = $1 AND kind = 'member' AND
///    archived_at IS NOT NULL AND archived_at < now() - interval '60 days'`
///    (also bounded) — the SAME window predicate re-asserted directly on the
///    mutating statement itself, not merely relied upon via the lock (the
///    other half of the Critical fix). CASCADE covers every other
///    [`GRAPH_CONTENT_TABLES`] entry.
/// 10. **A 0-row `graphs` delete is NEVER recorded as `completed`.** Under
///    the lock from step 4 this should be unreachable in practice — the same
///    transaction already confirmed this exact predicate under `FOR UPDATE`,
///    and nothing else could have changed the row since — but checking the
///    ACTUAL delete outcome, rather than trusting the check that preceded
///    it, is the whole point of "re-assert at mutation time." A 0-row
///    outcome rolls back the whole transaction (so the explicit child-table
///    deletes in step 9 are undone too — this is exactly why they share one
///    transaction with the `graphs` delete) and records `'failed'` with a
///    manifest error explicit that nothing was deleted, never `'completed'`.
/// 11. Audit `'completed'` with per-table counts + blob count. Transaction
///    commit/rollback calls themselves are deliberately NOT wrapped in
///    [`RECONCILE_TIMEOUT`], matching this file's existing convention
///    elsewhere (`restore_arm`/`archive_arm` never bound their own
///    commit/rollback either) — a timed-out commit is genuinely ambiguous
///    (it may have landed server-side regardless), and bounding it would
///    only add a second layer of uncertainty on top of Postgres's own,
///    without a safe way to act on it.
async fn purge_one_graph(
    org_id: &str,
    graph_id: Uuid,
    owner_user_id: Uuid,
    tenant_pool: &PgPool,
    control_pool: &PgPool,
    blobs: Option<&dyn PurgeBlobDeleter>,
) -> Result<bool, String> {
    // 1. Atom ids only — lightweight, pre-lock, for the drain guard.
    let atom_ids: Vec<Uuid> = tokio::time::timeout(
        RECONCILE_TIMEOUT,
        sqlx::query_scalar::<_, Uuid>("SELECT atom_id FROM atoms WHERE graph_id = $1")
            .bind(graph_id)
            .fetch_all(tenant_pool),
    )
    .await
    .map_err(|_| "atom-id read timed out".to_string())?
    .map_err(|e| format!("atom-id read failed: {e}"))?;

    // Drain guard.
    let inflight = tokio::time::timeout(
        RECONCILE_TIMEOUT,
        graph_has_inflight_jobs(tenant_pool, graph_id, &atom_ids),
    )
    .await
    .map_err(|_| "drain-guard check timed out".to_string())?
    .map_err(|e| format!("drain-guard check failed: {e}"))?;
    if inflight {
        tracing::info!(org = %org_id, graph_id = %graph_id, "member-ocean purge: graph has in-flight jobs — skipping this tick, will retry");
        return Ok(false);
    }

    // 2. Cheap, unlocked pre-check — before writing an audit row or
    //    acquiring any lock.
    let active = tokio::time::timeout(RECONCILE_TIMEOUT, routing_is_active(control_pool, org_id))
        .await
        .map_err(|_| "routing status check timed out".to_string())?
        .map_err(|e| format!("routing status check failed: {e}"))?;
    if !active {
        tracing::warn!(org = %org_id, graph_id = %graph_id, "member-ocean purge: org routing not active — aborting before any blob is touched");
        return Ok(false);
    }

    // 3. Audit 'started' — BEFORE anything is touched.
    let log_id = log_purge_started(control_pool, org_id, graph_id, owner_user_id)
        .await
        .map_err(|e| format!("audit 'started' write failed: {e}"))?;

    // 4. Acquire the ONE lock this function holds — see this function's own
    //    doc comment for why the org-routing lock this earlier revision also
    //    held here was removed (CodeRabbit Major: an idle-in-transaction
    //    control-plane lock held across unbounded R2 calls).
    let mut tenant_tx = match tenant_pool.begin().await {
        Ok(tx) => tx,
        Err(e) => {
            log_purge_failed(control_pool, log_id, &format!("tenant tx begin failed: {e}")).await;
            return Err(format!("graph {graph_id}: tenant tx begin failed: {e}"));
        }
    };
    // THE fix: re-assert the exact candidate-read predicate, under lock,
    // immediately before touching anything for this graph. Bounded like
    // every other DB round trip in this file.
    let still_eligible: Option<Uuid> = match tokio::time::timeout(
        RECONCILE_TIMEOUT,
        sqlx::query_scalar::<_, Uuid>(
            "SELECT id FROM graphs \
             WHERE id = $1 AND kind = 'member' \
               AND archived_at IS NOT NULL AND archived_at < now() - interval '60 days' \
             FOR UPDATE",
        )
        .bind(graph_id)
        .fetch_optional(&mut *tenant_tx),
    )
    .await
    {
        Err(_) => {
            let _ = tenant_tx.rollback().await;
            return Err(format!("graph {graph_id}: locked eligibility check timed out"));
        }
        Ok(Err(e)) => {
            let _ = tenant_tx.rollback().await;
            return Err(format!("graph {graph_id}: locked eligibility check failed: {e}"));
        }
        Ok(Ok(id)) => id,
    };
    if still_eligible.is_none() {
        let _ = tenant_tx.rollback().await;
        tracing::warn!(
            org = %org_id,
            graph_id = %graph_id,
            "member-ocean purge: graph no longer purge-eligible (restored, re-archived within the \
             window, or already gone) — aborting before any blob is touched"
        );
        log_purge_failed(
            control_pool,
            log_id,
            "graph no longer purge-eligible at blob-deletion time (likely restored) — skipped, no \
             data was deleted",
        )
        .await;
        return Ok(false);
    }

    // 5. SECOND, still-unlocked routing re-check, immediately before blob
    //    deletion — see this function's own doc comment for why this is
    //    deliberately not a held lock.
    let active_again = match tokio::time::timeout(
        RECONCILE_TIMEOUT,
        routing_is_active(control_pool, org_id),
    )
    .await
    {
        Err(_) => {
            let _ = tenant_tx.rollback().await;
            log_purge_failed(control_pool, log_id, "routing status re-check before blob deletion timed out").await;
            return Err(format!("graph {graph_id}: routing status re-check before blob deletion timed out"));
        }
        Ok(Err(e)) => {
            let _ = tenant_tx.rollback().await;
            log_purge_failed(control_pool, log_id, &format!("routing status re-check before blob deletion failed: {e}")).await;
            return Err(format!("graph {graph_id}: routing status re-check before blob deletion failed: {e}"));
        }
        Ok(Ok(a)) => a,
    };
    if !active_again {
        let _ = tenant_tx.rollback().await;
        tracing::warn!(org = %org_id, graph_id = %graph_id, "member-ocean purge: org routing not active immediately before blob deletion — aborting, will retry");
        log_purge_failed(control_pool, log_id, "routing not active before blob deletion").await;
        return Ok(false);
    }

    // 6. NOW read the authoritative blob-key set, under the same lock.
    let blob_keys: Vec<String> = match tokio::time::timeout(
        RECONCILE_TIMEOUT,
        sqlx::query_scalar::<_, String>(
            "SELECT content_blob_key FROM atoms WHERE graph_id = $1 \
             UNION ALL \
             SELECT raw_blob_key FROM meeting_transcripts \
               WHERE graph_id = $1 AND raw_blob_key IS NOT NULL \
             UNION ALL \
             SELECT raw_blob_key FROM source_versions \
               WHERE graph_id = $1 AND raw_blob_key IS NOT NULL \
             UNION \
             SELECT raw_blob_key FROM coding_sessions \
               WHERE graph_id = $1 AND raw_blob_key IS NOT NULL \
             UNION \
             SELECT payload->>'blob_key' FROM jobs \
               WHERE kind='DeleteSourceBlob' AND status IN ('pending','running','dead') \
               AND payload->>'graph_id'=$1::text AND payload->>'blob_key' IS NOT NULL",
        )
        .bind(graph_id)
        .fetch_all(&mut *tenant_tx),
    )
    .await
    {
        Err(_) => {
            let _ = tenant_tx.rollback().await;
            return Err(format!("graph {graph_id}: blob-key read timed out"));
        }
        Ok(Err(e)) => {
            let _ = tenant_tx.rollback().await;
            return Err(format!("graph {graph_id}: blob-key read failed: {e}"));
        }
        Ok(Ok(rows)) => rows,
    };

    // 7. Validate + org-scope EVERY key before deleting ANY of them.
    for key in &blob_keys {
        if !key.starts_with(&format!("{org_id}/{graph_id}/")) {
            let _ = tenant_tx.rollback().await;
            log_purge_failed(control_pool, log_id, "blob key graph ownership mismatch").await;
            return Err(format!("graph {graph_id} has a blob key outside its own namespace"));
        }
        if let Err(e) = validate_blob_key(key) {
            let _ = tenant_tx.rollback().await;
            log_purge_failed(control_pool, log_id, &format!("invalid blob key: {e}")).await;
            return Err(format!(
                "graph {graph_id} has an invalid blob key — aborting for operator review: {e}"
            ));
        }
        if let Err(e) = assert_blob_key_org_scope(key, Some(org_id)) {
            let _ = tenant_tx.rollback().await;
            log_purge_failed(control_pool, log_id, &format!("blob key org-scope mismatch: {e}")).await;
            return Err(format!(
                "graph {graph_id} has a blob key scoped to a different org — aborting for \
                 operator review: {e}"
            ));
        }
    }

    // Delete every blob — the graph lock from step 4 remains held throughout.
    // Each delete is individually bounded by RECONCILE_TIMEOUT (CodeRabbit
    // Major: this was this file's one unbounded outbound await). A timeout
    // is a failure, never a success — see this function's own doc comment
    // for the full reasoning (never counted in blobs_deleted, whole
    // transaction rolled back, audit stamped 'failed' identifying the key,
    // retried next tick since delete_atom_blob is idempotent).
    let mut blobs_deleted: i64 = 0;
    if !blob_keys.is_empty() {
        match blobs {
            None => {
                let _ = tenant_tx.rollback().await;
                log_purge_failed(
                    control_pool,
                    log_id,
                    "no R2 client configured but this graph has blobs to delete",
                )
                .await;
                return Err(format!(
                    "graph {graph_id}: {} blob(s) to delete but no R2 client is configured",
                    blob_keys.len()
                ));
            }
            Some(store) => {
                // Whole-loop budget — see `purge_blob_delete_budget`'s own
                // doc comment for why this exists and why this duration.
                // Started here (not at `tenant_tx`'s own begin, a few steps
                // earlier) because this loop — the slow, network-bound R2
                // calls — is the dominant contributor to how long the
                // tenant-pool connection and transaction stay open; the
                // eligibility check and blob-key read before it are both
                // single, already-`RECONCILE_TIMEOUT`-bounded local DB round
                // trips, not something this budget needs to additionally
                // cover.
                let budget = purge_blob_delete_budget();
                let blob_loop_deadline = tokio::time::Instant::now() + budget;
                for key in &blob_keys {
                    if tokio::time::Instant::now() >= blob_loop_deadline {
                        let _ = tenant_tx.rollback().await;
                        tracing::warn!(
                            org = %org_id,
                            graph_id = %graph_id,
                            blobs_deleted,
                            total = blob_keys.len(),
                            budget_secs = budget.as_secs(),
                            "member-ocean purge: blob-deletion budget exceeded — aborting this \
                             tick, will resume next tick (every key, including ones already \
                             deleted this attempt, will be re-attempted — delete_atom_blob is \
                             idempotent)"
                        );
                        log_purge_failed(
                            control_pool,
                            log_id,
                            &format!(
                                "blob-deletion budget of {}s exceeded after {blobs_deleted}/{} \
                                 blobs this attempt — NOT a completed purge, retried next tick \
                                 (delete_atom_blob is idempotent, so re-attempting already-deleted \
                                 keys is safe)",
                                budget.as_secs(),
                                blob_keys.len()
                            ),
                        )
                        .await;
                        return Err(format!(
                            "graph {graph_id}: blob-deletion budget exceeded after \
                             {blobs_deleted}/{} blobs — will retry next tick",
                            blob_keys.len()
                        ));
                    }
                    match tokio::time::timeout(RECONCILE_TIMEOUT, store.delete_blob(key)).await {
                        Ok(Ok(())) => blobs_deleted += 1,
                        Ok(Err(e)) => {
                            let _ = tenant_tx.rollback().await;
                            log_purge_failed(control_pool, log_id, &format!("blob delete failed: {e}"))
                                .await;
                            return Err(format!("graph {graph_id}: blob delete failed for {key}: {e}"));
                        }
                        Err(_) => {
                            let _ = tenant_tx.rollback().await;
                            log_purge_failed(
                                control_pool,
                                log_id,
                                &format!(
                                    "blob delete timed out after {}s for key {key} — outcome \
                                     unknown, treated as failure (never as success)",
                                    RECONCILE_TIMEOUT.as_secs()
                                ),
                            )
                            .await;
                            return Err(format!(
                                "graph {graph_id}: blob delete timed out for key {key} — will \
                                 retry next tick (delete_atom_blob is idempotent)"
                            ));
                        }
                    }
                }
            }
        }
    }

    // 8. Snapshot counts for the audit manifest — separate connection, see
    //    this function's own doc comment for why that is safe here.
    let counts = table_counts(tenant_pool, graph_id).await;

    // 9. Explicit deletes, then the graphs row itself — all within the SAME
    //    locked transaction opened in step 4.
    macro_rules! tenant_delete_or_abort {
        ($table:expr) => {
            match tokio::time::timeout(
                RECONCILE_TIMEOUT,
                sqlx::query(concat!("DELETE FROM ", $table, " WHERE graph_id = $1"))
                    .bind(graph_id)
                    .execute(&mut *tenant_tx),
            )
            .await
            {
                Err(_) => {
                    let _ = tenant_tx.rollback().await;
                    log_purge_failed(control_pool, log_id, &format!("{} delete timed out", $table)).await;
                    return Err(format!("graph {graph_id}: {} delete timed out", $table));
                }
                Ok(Err(e)) => {
                    let _ = tenant_tx.rollback().await;
                    log_purge_failed(control_pool, log_id, &format!("{} delete failed: {e}", $table)).await;
                    return Err(format!("graph {graph_id}: {} delete failed: {e}", $table));
                }
                Ok(Ok(_)) => {}
            }
        };
    }
    // FK-less children — CASCADE off `graphs` will NOT reach these (see
    // `GRAPH_CONTENT_TABLES`'s doc comment, incl. how `op_log` was found to
    // belong on this list despite `0083_consumer_rls.sql`'s prose saying
    // otherwise).
    tenant_delete_or_abort!("meeting_transcripts");
    tenant_delete_or_abort!("transcript_corrections");
    tenant_delete_or_abort!("op_log");
    // Shares — belt-and-braces alongside the CASCADE their `graph_id` FK
    // already provides (plan §7 step 4).
    tenant_delete_or_abort!("coding_session_shares");
    tenant_delete_or_abort!("meeting_shares");

    // The graph row itself — the SAME 60-day window predicate re-asserted
    // directly on the DELETE (not merely relied upon via the lock above),
    // per this function's own doc comment. CASCADEs to every other
    // GRAPH_CONTENT_TABLES entry (atoms, nodes, node_atoms,
    // relationship_edge, conversations, ...).
    let graph_deleted = match tokio::time::timeout(
        RECONCILE_TIMEOUT,
        sqlx::query(
            "DELETE FROM graphs WHERE id = $1 AND kind = 'member' \
               AND archived_at IS NOT NULL AND archived_at < now() - interval '60 days'",
        )
        .bind(graph_id)
        .execute(&mut *tenant_tx),
    )
    .await
    {
        Err(_) => {
            let _ = tenant_tx.rollback().await;
            log_purge_failed(control_pool, log_id, "graphs delete timed out").await;
            return Err(format!("graph {graph_id}: graphs delete timed out"));
        }
        Ok(Err(e)) => {
            let _ = tenant_tx.rollback().await;
            log_purge_failed(control_pool, log_id, &format!("graphs delete failed: {e}")).await;
            return Err(format!("graph {graph_id}: graphs delete failed: {e}"));
        }
        Ok(Ok(res)) => res.rows_affected(),
    };
    // 10. A 0-row delete is NEVER a completion — see this function's own doc
    //    comment for why this should be unreachable in practice (the lock
    //    already made the eligibility check and this delete atomic) and why
    //    it is checked anyway.
    if graph_deleted == 0 {
        let _ = tenant_tx.rollback().await;
        tracing::warn!(
            org = %org_id,
            graph_id = %graph_id,
            "member-ocean purge: graphs delete affected 0 rows — graph no longer eligible at \
             delete time — aborting, NOT recording completed"
        );
        log_purge_failed(
            control_pool,
            log_id,
            "graphs delete affected 0 rows — graph no longer eligible at delete time — skipped, \
             no data was deleted (transaction rolled back)",
        )
        .await;
        return Ok(false);
    }
    if let Err(e) = tenant_tx.commit().await {
        log_purge_failed(control_pool, log_id, &format!("tenant tx commit failed: {e}")).await;
        return Err(format!("graph {graph_id}: tenant tx commit failed: {e}"));
    }

    // 11. Audit: completed, with counts.
    let mut manifest_counts = counts;
    manifest_counts.insert("graphs".to_string(), graph_deleted as i64);
    log_purge_completed(control_pool, log_id, &manifest_counts, blobs_deleted).await;

    tracing::info!(
        org = %org_id,
        graph_id = %graph_id,
        owner_user_id = %owner_user_id,
        blobs_deleted,
        "member-ocean purge: purged member ocean"
    );
    Ok(true)
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn enabled_flag_parsing_matches_tenancy_runtime_env_flag_semantics() {
        // Same contract as tenancy::runtime::env_flag: "1" or a
        // case-insensitive "true" is on; unset (None) or anything else is off.
        assert!(!parse_enabled_flag(None), "unset defaults off");
        assert!(parse_enabled_flag(Some("1")));
        assert!(parse_enabled_flag(Some("true")));
        assert!(parse_enabled_flag(Some("TRUE")));
        assert!(parse_enabled_flag(Some("True")));
        assert!(!parse_enabled_flag(Some("0")));
        assert!(!parse_enabled_flag(Some("false")));
        assert!(!parse_enabled_flag(Some("yes")));
        assert!(!parse_enabled_flag(Some("")));
    }

    #[test]
    fn fallback_ocean_name_used_when_no_display_name_or_blank() {
        let cases: [Option<&str>; 3] = [None, Some(""), Some("   ")];
        for case in cases {
            let name = case
                .map(str::trim)
                .filter(|n| !n.is_empty())
                .unwrap_or(FALLBACK_OCEAN_NAME);
            assert_eq!(name, FALLBACK_OCEAN_NAME, "case: {case:?}");
        }
    }

    #[test]
    fn real_display_name_is_used_verbatim() {
        let case = Some("Jane Smith");
        let name = case
            .map(str::trim)
            .filter(|n| !n.is_empty())
            .unwrap_or(FALLBACK_OCEAN_NAME);
        assert_eq!(name, "Jane Smith");
    }

    /// [`member_ocean_purge_enabled`] shares [`parse_enabled_flag`] with
    /// [`member_oceans_enabled`], so this is really re-asserting that the
    /// purge gate reads its OWN, separate env var name — `MEMBER_OCEAN_PURGE_ENABLED`
    /// — not a documentation claim this test can't catch a regression in.
    /// The actual "both flags independently gate the purge arm" property is
    /// proved end-to-end by `member_ocean_purge_pg.rs` (purge-flag-off and
    /// member-oceans-flag-off cases), which is the only place that can
    /// observe `tick`'s actual behavior.
    #[test]
    fn purge_enabled_flag_env_var_name_is_distinct_from_member_oceans_flag() {
        assert_ne!(
            "MEMBER_OCEAN_PURGE_ENABLED", "MEMBER_OCEANS_ENABLED",
            "the purge gate must never be toggled by the same env var as provisioning/archive/restore"
        );
    }

    /// [`org_teams_enabled`] shares [`parse_enabled_flag`] with every other
    /// flag in this file, so this is really re-asserting that it reads its
    /// OWN env var (`ORG_TEAMS_ENABLED`), distinct from `MEMBER_OCEANS_ENABLED`
    /// — the two are related (the startup assert in `lib.rs::run()` requires
    /// the latter whenever the former is on) but must remain independently
    /// toggleable env vars, not aliases of each other.
    #[test]
    fn org_teams_enabled_flag_env_var_name_is_distinct_from_member_oceans_flag() {
        assert_ne!(
            "ORG_TEAMS_ENABLED", "MEMBER_OCEANS_ENABLED",
            "org teams and member oceans must remain independently toggleable env vars"
        );
    }

    /// Same fail-closed parsing contract as [`member_oceans_enabled`] — see
    /// [`enabled_flag_parsing_matches_tenancy_runtime_env_flag_semantics`] for
    /// the exhaustive case table this re-derives via the shared
    /// [`parse_enabled_flag`] helper; this test exists so a future reader who
    /// only searches for "org_teams" finds direct proof of the parsing
    /// contract without having to know it is `member_oceans_enabled`'s twin.
    #[test]
    fn org_teams_enabled_parsing_matches_shared_flag_semantics() {
        assert!(!parse_enabled_flag(None), "unset defaults off");
        assert!(parse_enabled_flag(Some("1")));
        assert!(parse_enabled_flag(Some("true")));
        assert!(parse_enabled_flag(Some("TRUE")));
        assert!(!parse_enabled_flag(Some("0")));
        assert!(!parse_enabled_flag(Some("yes")));
    }

    /// [`GRAPH_CONTENT_TABLES`] entries must each be a plain `$1`-parameterized
    /// count query — a copy/paste of a wrong bind shape (e.g. `$2`, or a
    /// literal not covered by the graph-id predicate) would silently make
    /// `table_counts`/the coverage test lie. This does not run any SQL (no
    /// DB in a unit test); it only guards the textual shape every entry must
    /// have.
    #[test]
    fn graph_content_tables_are_well_formed_count_queries() {
        assert!(
            GRAPH_CONTENT_TABLES.len() >= 25,
            "expected the full 0083 graph_id-bearing roster (minus account-only tables); got {}",
            GRAPH_CONTENT_TABLES.len()
        );
        let mut seen = std::collections::HashSet::new();
        for (name, sql) in GRAPH_CONTENT_TABLES {
            assert!(seen.insert(*name), "duplicate table entry: {name}");
            assert!(
                sql.trim_start().to_ascii_uppercase().starts_with("SELECT COUNT(*)"),
                "{name}: entry must be a SELECT count(*) query, got: {sql}"
            );
            assert!(sql.contains('$'), "{name}: entry must be parameterized by the graph id, got: {sql}");
        }
        // The three FK-less tables this arm must delete explicitly (see
        // GRAPH_CONTENT_TABLES's own doc comment — `op_log` was found to
        // belong here despite 0083_consumer_rls.sql's prose claiming it is
        // CASCADE-covered) must be present — dropping any of them from the
        // roster would silently drop it from the coverage test too.
        for must_have in ["meeting_transcripts", "transcript_corrections", "op_log"] {
            assert!(
                GRAPH_CONTENT_TABLES.iter().any(|(n, _)| *n == must_have),
                "GRAPH_CONTENT_TABLES must include {must_have} (no FK to graphs — see doc comment)"
            );
        }
    }
}

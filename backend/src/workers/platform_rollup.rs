//! Nightly platform rollup worker (platform-admin-metrics PR 5 of 7,
//! migration `0095_platform_metrics`).
//!
//! Computes, once per calendar day (recomputed intraday on every tick — see
//! `0095_platform_metrics.sql`'s "PRIMARY KEY (day, tenant)" note: "today"'s
//! row is upserted repeatedly until the day is over), the cross-tenant
//! aggregate facts `platform_metrics_daily`/`platform_active_users` exist for:
//! atom/reef/session counts, `llm_usage` token sums, and PER-USER activity
//! presence rows, split by `source` (`access` | `activity` | `query` | `mcp`).
//!
//! # Which plane does what (tenant-RLS approach)
//!
//! This worker registers with `workers::tenant_harness::run_multi_tenant`
//! (`HarnessConfig::sweep_worker`) exactly like `tombstone_purge`/`reaper`, so
//! [`tick`] is invoked once per due tenant — `TenantId::Consumer` and one
//! `TenantId::Org(_)` per active row in `tenant_databases` — with its own
//! `(pool, store)` pair. The two branches use DIFFERENT scoping strategies,
//! matching the two different isolation models this codebase already commits
//! to (`tenancy::scope`'s module doc, "Both planes, two different
//! boundaries"):
//!
//! * **Org tenants** — a tenant DB holds exactly one org, so every content
//!   table is queried directly against `t.pool` with no
//!   `account_id`/`app.account_id` predicate at all. `migrations_tenant/
//!   0009_tenant_rls` nonetheless layers MEMBER-ocean policies onto that same
//!   tenant DB, and these ambient reads carry no GUC that satisfies them.
//!   This paragraph used to say the harness hands out a SEPARATE
//!   `app.tenant_actor=system` worker pool that satisfies
//!   `tenant_rls_is_system()` by construction, so no per-tick transaction was
//!   needed. That is no longer true: PR #388 deleted the startup-packet GUC
//!   because a connection pooler rejects it. What covers these reads today is
//!   the owner exemption alone (`0009` §7 does not FORCE row security),
//!   asserted at pool build by
//!   `tenancy::registry::worker_pool_rls_exemption_breach`, which refuses to
//!   build the pool for any role RLS would apply to. See
//!   `collect_tenant_stats`'s own doc for the conversion this arm still
//!   needs.
//! * **Consumer plane** — the control DB doubling as the shared
//!   personal-content plane, where every content table this worker reads
//!   (`atoms`/`graphs`/`conversations`/`llm_usage`/`activity_events`/
//!   `queries`/`mcp_tool_calls`) IS on migration `0083_consumer_rls`'s
//!   per-account policy roster. Because this worker's job is a
//!   CROSS-ACCOUNT aggregate (one `platform_metrics_daily` row for the
//!   *whole* consumer plane, keyed `tenant = '__consumer__'` — see 0095's
//!   header), it cannot run as a single query the way an ordinary
//!   single-account request would. It follows the SAME loop-per-account
//!   conversion `workers::tombstone_purge`/`workers::reembed` already use for
//!   exactly this reason (their own module docs, "Consumer-plane RLS
//!   scoping"): enumerate `accounts` (a control table, outside the RLS
//!   roster — no scope needed to read it), then run one
//!   `tenancy::scope::begin_account_scoped` transaction PER ACCOUNT, with an
//!   explicit `account_id = $1` predicate as the primary scoping mechanism
//!   (correct even pre-OPS-4, when the app still ran as the BYPASSRLS
//!   owner role) and the GUC as the backstop that OPS-4's role switch has
//!   since made live. Partial sums from every account are
//!   then combined into the ONE consumer-plane row.
//!
//! `user_access_log` (the `access` activity source) is a THIRD case: it is
//! control-only, has no tenant-tree counterpart, and — like
//! `workers::access_log_purge`'s module doc explains — was deliberately never
//! added to 0083's RLS roster at all (platform-operator data, not per-account
//! content). Reading it is therefore a single unscoped query, run once
//! (not per account), and only on the consumer-plane branch: a login has no
//! org-tenant dimension to attribute it to (see "Known simplifications"
//! below).
//!
//! # Where reads happen vs. where writes land — TWO pools, not one
//!
//! `platform_metrics_daily`/`platform_active_users` are CONTROL-ONLY tables
//! with NO tenant-tree counterpart at all — migration `0095_platform_metrics`'s
//! header is explicit that this is permanent, not a gap some future migration
//! closes. An org tenant's own database therefore PHYSICALLY CANNOT hold
//! these tables, which means [`tick`] cannot simply read-and-write against
//! the one `t.pool` `WorkerTenant` hands it the way `tombstone_purge`/`reaper`
//! do (their target tables — `op_log`, `jobs` — DO exist per-tenant). Instead
//! [`tick`] takes a SECOND pool, `control` — always the control-plane pool
//! (`state.db` in `lib.rs`) — and:
//!
//! * on the **org branch**, stats are COLLECTED from `t.pool` (that org's own
//!   tenant DB, per the scoping-free reasoning above) but the resulting row is
//!   WRITTEN to `control`;
//! * on the **consumer branch**, `t.pool` and `control` are the SAME physical
//!   pool (the control DB doubles as the consumer-content plane), so the
//!   split is a no-op there — reads and the write both go through `control`.
//!
//! This is why `lib.rs`'s spawn site closes over its OWN clone of the control
//! pool for this worker's `tick_fn` closure, in addition to the clone the
//! harness itself already owns as its `consumer_pool` argument — see that
//! spawn site's comment for the exact wiring.
//!
//! # Resilience — no tenant may abort the whole run, and a skip must be
//! # observable
//!
//! `run_multi_tenant` already gives every ORG tenant per-tenant failure
//! isolation for free (its own module doc: "A tick error or panic is caught,
//! logged, and backs off THAT tenant only; every other tenant ... keeps
//! processing"): if [`tick`] returns `Err` for one org (its DB is down, a
//! query times out, whatever), the harness logs
//! `"worker tick failed — backing off this tenant only"` with that org's
//! identity attached and retries it on its own backoff schedule, while every
//! other org and the consumer plane keep rolling up normally. This worker
//! adds nothing on top of that for the ORG branch — it inherits the harness's
//! isolation contract exactly like `tombstone_purge`/`reaper` do.
//!
//! The CONSUMER branch has a second, finer-grained failure mode the harness
//! doesn't cover on its own: a single ACCOUNT (not a whole tenant DB) failing
//! mid-loop. [`consumer_tick`] does not abort on the first account failure —
//! it logs the failure, records the account id, and keeps going so a single
//! bad account doesn't block every other account's contribution to this
//! cycle's numbers being computed... but then, before writing anything, it
//! checks whether ANY account failed. If so, it refuses to upsert
//! `platform_metrics_daily`/`platform_active_users` for `'__consumer__'`
//! at all this cycle and returns `Err` (surfaced to the harness as a
//! tick failure, backed off and retried like an org tenant). **This is the
//! deliberate failure mode this worker is written to avoid: a rollup that
//! silently under-counts because it dropped a failed account's contribution
//! but still writes a row that LOOKS like a complete day's total.**
//! (`workers::reembed`'s `BatchOutcome::all_succeeded` gates its own
//! continuation cursor on the identical principle — a partial pass must
//! never look like a complete one.) The previous cycle's row (if any) is left
//! untouched, so the failure is directly observable: its `computed_at` stays
//! stale relative to `now()`, and every logged
//! `"platform rollup: consumer pass incomplete"` line names exactly which
//! accounts failed. An operator (or PR 6's dashboard) can trivially query
//! `SELECT tenant FROM platform_metrics_daily WHERE day = current_date AND
//! computed_at < now() - interval '2 hours'` to find every tenant (consumer
//! or org) the rollup silently failed to refresh today — that query is the
//! intended "skipped tenants are observable" surface, on top of the
//! structured `tracing::warn!`/`tracing::error!` lines both this module and
//! the harness itself already emit with the tenant identity attached.
//!
//! # Known simplifications (documented, not hidden)
//!
//! * **`access` is consumer-plane-only.** `user_access_log` records a login
//!   against a `user_id` with no org/tenant column at all (a user can belong
//!   to several orgs; the table cannot say which one, if any, a given login
//!   was "for"). Attributing logins to individual org tenants would need a
//!   join against `account_members`/org membership at read time — deferred;
//!   this PR's `access` rows are always written under `'__consumer__'`, which
//!   over-counts an org-only user who never touches personal content as
//!   "active in the consumer plane" purely by virtue of logging in. Flagged
//!   here rather than silently baked in.
//! * **"Session" = `conversations`.** The schema has no single canonical
//!   "session" concept; `conversations` (one row per ask/search conversation,
//!   `account_id`/`user_id`/`created_at` on both planes) is the closest fit
//!   and is what `session_count` counts, scoped to rows created on the
//!   rollup day (a per-day activity fact, matching 0095's header: this table
//!   is NOT a cumulative growth curve).
//!
//! # Interval
//!
//! [`rollup_interval`] defaults to [`DEFAULT_ROLLUP_INTERVAL`] (24h, matching
//! the brief's "make the interval env-configurable, default daily") and is
//! overridable via `PLATFORM_ROLLUP_INTERVAL_SECS` for operators who want
//! "today"'s row refreshed more often intraday. Registered as a
//! `HarnessConfig::sweep_worker`, so — same as `tombstone_purge`/`reaper` —
//! the harness fires an immediate first tick per newly-seen tenant, then
//! re-ticks every `rollup_interval()` thereafter (org tenants additionally
//! back off while genuinely idle, per the harness's Neon scale-to-zero
//! design; a day with zero activity for a tenant returns [`Tick::idle`],
//! which is exactly the right signal for that backoff to kick in).

use std::collections::HashSet;
use std::time::Duration;

use chrono::{NaiveDate, Utc};
use sqlx::PgPool;
use uuid::Uuid;

use crate::tenancy::scope::begin_account_scoped;
use crate::tenancy::TenantId;
use crate::workers::tenant_harness::{Tick, WorkerTenant};

/// `platform_metrics_daily`/`platform_active_users.tenant` value for the
/// shared personal/consumer plane — must match the literal `0095_platform_
/// metrics.sql` documents (`'__consumer__'`) byte-for-byte.
const CONSUMER_TENANT: &str = "__consumer__";

/// Default rollup cadence: once a day, matching the brief's "make the
/// interval env-configurable, default daily" and mirroring
/// `workers::tombstone_purge::PURGE_INTERVAL` / `workers::access_log_purge::
/// PURGE_INTERVAL`'s identical daily default for the same "ample for the
/// retention/aggregation window, doesn't compete with peak traffic" reasoning.
pub const DEFAULT_ROLLUP_INTERVAL: Duration = Duration::from_secs(24 * 60 * 60);

/// Read the operator-tunable rollup cadence (`PLATFORM_ROLLUP_INTERVAL_SECS`),
/// falling back to [`DEFAULT_ROLLUP_INTERVAL`] on a missing, unparsable, or
/// zero value — mirrors `lib.rs::worker_shutdown_grace`'s / `llm_timeout_secs`'s
/// accessor shape (raw env read, `.trim().parse()`, sane fallback).
pub fn rollup_interval() -> Duration {
    std::env::var("PLATFORM_ROLLUP_INTERVAL_SECS")
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .filter(|secs| *secs > 0)
        .map(Duration::from_secs)
        .unwrap_or(DEFAULT_ROLLUP_INTERVAL)
}

/// Bound on the whole-tenant-DB collection pass (org tenants) and on the
/// control-only `user_access_log`/account-enumeration reads — mirrors
/// `workers::reembed::CANDIDATE_QUERY_TIMEOUT`'s "a stalled Postgres must not
/// block this worker indefinitely" reasoning.
const QUERY_TIMEOUT: Duration = Duration::from_secs(60);

/// Bound on ONE account's scoped collection pass inside the consumer-plane
/// loop. Deliberately shorter than [`QUERY_TIMEOUT`]: with potentially many
/// accounts to loop over per cycle, a single hung account must fail fast
/// (and be recorded as a failed account — see the module doc's resilience
/// section) rather than eating most of the cycle's budget.
const ACCOUNT_QUERY_TIMEOUT: Duration = Duration::from_secs(30);

// ── Collected stats (pure data, no I/O) ──────────────────────────────────────

/// One tenant's (or one consumer-plane account's) raw collection result for a
/// single day — before cross-account merging (see [`DayTotals::merge`]).
#[derive(Debug, Default, Clone)]
struct DayStats {
    atoms: i64,
    reefs: i64,
    sessions: i64,
    input_tokens: i64,
    output_tokens: i64,
    /// Anthropic prompt-cache tokens (migration `0073_llm_usage_cache_
    /// tokens.sql` on the source `llm_usage` table; migration
    /// `0123_platform_metrics_daily_cache_tokens.sql` on this rollup's own
    /// destination table). Always a real, computed sum here — never left
    /// absent — so `write_rollup`/`upsert_metrics_daily` can write real,
    /// non-null values on every row this worker touches (see that
    /// migration's "NULLABLE, NO DEFAULT" doc comment for why NULL vs 0
    /// matters downstream in `admin_spend`'s completeness signal).
    cache_creation_tokens: i64,
    cache_read_tokens: i64,
    activity_users: Vec<Uuid>,
    query_users: Vec<Uuid>,
    mcp_users: Vec<Uuid>,
}

/// Running totals for one `(day, tenant)` rollup row. For an org tenant this
/// is exactly one [`DayStats`]; for the consumer plane it is the SUM of every
/// account's [`DayStats`] this cycle (see the module doc's "which plane does
/// what" section) — counts add, and per-source user ids dedupe through a
/// `HashSet` so a user who (rarely, but possibly) touched two accounts in one
/// day is still counted once, matching `platform_active_users`'s PK grain
/// (`day, user_id, tenant, source`).
#[derive(Debug, Default)]
struct DayTotals {
    atoms: i64,
    reefs: i64,
    sessions: i64,
    input_tokens: i64,
    output_tokens: i64,
    cache_creation_tokens: i64,
    cache_read_tokens: i64,
    activity_users: HashSet<Uuid>,
    query_users: HashSet<Uuid>,
    mcp_users: HashSet<Uuid>,
}

impl DayTotals {
    fn merge(&mut self, stats: DayStats) {
        self.atoms += stats.atoms;
        self.reefs += stats.reefs;
        self.sessions += stats.sessions;
        self.input_tokens += stats.input_tokens;
        self.output_tokens += stats.output_tokens;
        self.cache_creation_tokens += stats.cache_creation_tokens;
        self.cache_read_tokens += stats.cache_read_tokens;
        self.activity_users.extend(stats.activity_users);
        self.query_users.extend(stats.query_users);
        self.mcp_users.extend(stats.mcp_users);
    }
}

// ── Entry point (driven by `workers::tenant_harness::run_multi_tenant`) ─────

/// One rollup pass for one tenant — see the module doc for the full
/// consumer-vs-org scoping split. `pub` (like every other tenant-loop
/// worker's `tick`) so integration tests can drive it directly against a real
/// Postgres without spinning up the harness.
///
/// `control` is ALWAYS the control-plane pool (`state.db` in `lib.rs`, the
/// same pool `run_multi_tenant`'s own `consumer_pool` argument is), passed
/// separately from `t.pool` because `platform_metrics_daily`/
/// `platform_active_users` are CONTROL-ONLY tables with NO tenant-tree
/// counterpart at all (migration `0095_platform_metrics`'s header is explicit
/// that this is permanent, not a gap to close later) — an org tenant's `t.pool`
/// physically cannot hold them. For `TenantId::Consumer`, `t.pool` and
/// `control` are the SAME physical pool (the control DB doubles as the
/// consumer-content plane), so this split is a no-op there; for
/// `TenantId::Org(_)`, stats are COLLECTED from `t.pool` (that org's own
/// tenant DB) but the rollup row is WRITTEN to `control` — the two pools are
/// genuinely different databases for that branch. Wiring this in `lib.rs`
/// means the spawn site closes over its own clone of the control pool
/// alongside the harness's own captured copy — see the spawn site's comment.
pub async fn tick(t: &WorkerTenant, control: &PgPool) -> Result<Tick, String> {
    let day = Utc::now().date_naive();
    match &t.tenant {
        TenantId::Consumer => consumer_tick(control, day).await,
        TenantId::Org(org) => org_tick(&t.pool, control, org, day).await,
    }
}

// ── Consumer plane: loop-per-account (org-tenancy RLS convention) ───────────

async fn consumer_tick(pool: &PgPool, day: NaiveDate) -> Result<Tick, String> {
    // Account roster — control-table read, needs no scope (same as
    // `workers::tombstone_purge`/`workers::reembed`'s identical enumeration).
    let account_ids: Vec<Uuid> = match tokio::time::timeout(
        QUERY_TIMEOUT,
        sqlx::query_scalar::<_, Uuid>("SELECT id FROM accounts ORDER BY id").fetch_all(pool),
    )
    .await
    {
        Err(_) => {
            return Err(format!(
                "platform rollup: consumer account enumeration timed out after {}s",
                QUERY_TIMEOUT.as_secs()
            ))
        }
        Ok(Err(e)) => return Err(format!("platform rollup: consumer account enumeration failed: {e}")),
        Ok(Ok(ids)) => ids,
    };

    let mut totals = DayTotals::default();
    let mut failed_accounts: Vec<Uuid> = Vec::new();

    for account_id in &account_ids {
        match tokio::time::timeout(ACCOUNT_QUERY_TIMEOUT, collect_account_stats(pool, *account_id, day)).await
        {
            Ok(Ok(stats)) => totals.merge(stats),
            Ok(Err(e)) => {
                tracing::warn!(
                    %account_id, error = %e,
                    "platform rollup: failed to collect stats for account this pass"
                );
                failed_accounts.push(*account_id);
            }
            Err(_) => {
                tracing::warn!(
                    %account_id, timeout_secs = ACCOUNT_QUERY_TIMEOUT.as_secs(),
                    "platform rollup: stats collection timed out for account this pass"
                );
                failed_accounts.push(*account_id);
            }
        }
    }

    if !failed_accounts.is_empty() {
        // See the module doc's resilience section: never upsert a
        // partial-looking-complete consumer row. The harness surfaces this
        // `Err` as a tick failure for `TenantId::Consumer` (logged, retried
        // on the consumer's own schedule) — the existing row for `day` (if
        // any) is left exactly as it was, so its `computed_at` staying stale
        // is itself the observable signal.
        tracing::error!(
            failed = failed_accounts.len(),
            total = account_ids.len(),
            ?failed_accounts,
            %day,
            "platform rollup: consumer pass incomplete — refusing to write a partial rollup"
        );
        return Err(format!(
            "platform rollup: {}/{} accounts failed to report stats for {day} — consumer rollup not written this pass",
            failed_accounts.len(),
            account_ids.len()
        ));
    }

    // `access` (logins): control-only, unscoped, consumer-plane-only — see
    // the module doc's "Known simplifications". A failure here does NOT
    // block the rest of the consumer rollup (unlike an account failure
    // above): login activity is a strictly additive, best-effort signal, not
    // part of the atom/reef/session/token totals this worker must never
    // under-report silently.
    let access_users: Vec<Uuid> = match tokio::time::timeout(
        QUERY_TIMEOUT,
        sqlx::query_scalar::<_, Uuid>(
            "SELECT DISTINCT user_id FROM user_access_log WHERE created_at::date = $1",
        )
        .bind(day)
        .fetch_all(pool),
    )
    .await
    {
        Err(_) => {
            tracing::warn!(
                timeout_secs = QUERY_TIMEOUT.as_secs(),
                "platform rollup: access-log active-user query timed out; treating as zero for this pass"
            );
            Vec::new()
        }
        Ok(Err(e)) => {
            tracing::warn!(
                error = %e,
                "platform rollup: access-log active-user query failed; treating as zero for this pass"
            );
            Vec::new()
        }
        Ok(Ok(rows)) => rows,
    };

    write_rollup(pool, day, CONSUMER_TENANT, &totals, &access_users).await
}

/// Collect one account's day-scoped stats inside a single
/// `begin_account_scoped` transaction (org-tenancy RLS convention — see the
/// module doc). Every statement carries the explicit `account_id = $1`
/// predicate as the primary scoping mechanism (correct pre- AND post-OPS-4);
/// the transaction-scoped `app.account_id` GUC is the forward-compatible
/// backstop once the app's connection role loses `BYPASSRLS`.
async fn collect_account_stats(pool: &PgPool, account_id: Uuid, day: NaiveDate) -> Result<DayStats, sqlx::Error> {
    let mut tx = begin_account_scoped(pool, account_id)
        .await
        .map_err(|e| sqlx::Error::Protocol(e.to_string()))?;

    let atoms: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM atoms a \
         JOIN graphs g ON g.id = a.graph_id \
         WHERE g.account_id = $1 AND a.created_at::date = $2",
    )
    .bind(account_id)
    .bind(day)
    .fetch_one(&mut *tx)
    .await?;

    let reefs: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM graphs WHERE account_id = $1 AND created_at::date = $2",
    )
    .bind(account_id)
    .bind(day)
    .fetch_one(&mut *tx)
    .await?;

    let sessions: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM conversations WHERE account_id = $1 AND created_at::date = $2",
    )
    .bind(account_id)
    .bind(day)
    .fetch_one(&mut *tx)
    .await?;

    // One query for all four sums (CodeRabbit round 1 on #511): two separate
    // `SELECT`s here could observe different `llm_usage` snapshots — this
    // transaction is `begin_account_scoped`'s default READ COMMITTED, where
    // each statement (not the whole transaction) gets its own snapshot, so a
    // row committed between two sequential queries could make plain and
    // cache totals disagree about which rows they cover for the same day.
    // `cache_creation_input_tokens`/`cache_read_input_tokens` are BIGINT
    // (migration `0073_llm_usage_cache_tokens.sql`) — `SUM(bigint)` returns
    // NUMERIC in Postgres (overflow-safety default), which sqlx won't decode
    // into `i64` without the explicit `::bigint` cast, same pitfall
    // `routes/admin.rs`'s personal-plane query already casts around.
    let (input_tokens, output_tokens, cache_creation_tokens, cache_read_tokens): (i64, i64, i64, i64) =
        sqlx::query_as(
            "SELECT COALESCE(SUM(input_tokens), 0)::bigint, COALESCE(SUM(output_tokens), 0)::bigint, \
                    COALESCE(SUM(cache_creation_input_tokens), 0)::bigint, \
                    COALESCE(SUM(cache_read_input_tokens), 0)::bigint \
             FROM llm_usage WHERE account_id = $1 AND created_at::date = $2",
        )
        .bind(account_id)
        .bind(day)
        .fetch_one(&mut *tx)
        .await?;

    let activity_users: Vec<Uuid> = sqlx::query_scalar(
        "SELECT DISTINCT user_id FROM activity_events \
         WHERE account_id = $1 AND created_at::date = $2 AND user_id IS NOT NULL",
    )
    .bind(account_id)
    .bind(day)
    .fetch_all(&mut *tx)
    .await?;

    let query_users: Vec<Uuid> = sqlx::query_scalar(
        "SELECT DISTINCT user_id FROM queries WHERE account_id = $1 AND created_at::date = $2",
    )
    .bind(account_id)
    .bind(day)
    .fetch_all(&mut *tx)
    .await?;

    let mcp_users: Vec<Uuid> = sqlx::query_scalar(
        "SELECT DISTINCT user_id FROM mcp_tool_calls \
         WHERE account_id = $1 AND created_at::date = $2 AND user_id IS NOT NULL",
    )
    .bind(account_id)
    .bind(day)
    .fetch_all(&mut *tx)
    .await?;

    tx.commit().await?;

    Ok(DayStats {
        atoms,
        reefs,
        sessions,
        input_tokens,
        output_tokens,
        cache_creation_tokens,
        cache_read_tokens,
        activity_users,
        query_users,
        mcp_users,
    })
}

// ── Org tenant: one whole-database pass, and it still needs a scope ─────────
//
// Whole-database isolation is real, and it covers the CROSS-ORG boundary
// only. It is NOT why [`collect_tenant_stats`] runs ambient — that is an open
// gap, stated in full on the function itself. Inside a tenant DB, six of the
// seven counted tables carry member-ocean policies
// (`atoms`/`graphs`/`conversations`/`llm_usage`/`activity_events`/
// `mcp_tool_calls` — `queries` is one of `migrations_tenant/0009_tenant_rls`
// §3f's deliberately-uncovered ten). With no GUC set, those six count the org
// and personal oceans and omit every member and team ocean, so the rollup
// under-reports the tenant rather than failing.

/// `tenant_pool` is the org's OWN tenant DB (stats are collected from it);
/// `control` is the control-plane pool the rollup row is WRITTEN to —
/// `platform_metrics_daily`/`platform_active_users` physically do not exist
/// on `tenant_pool` (see [`tick`]'s doc comment).
async fn org_tick(tenant_pool: &PgPool, control: &PgPool, org: &str, day: NaiveDate) -> Result<Tick, String> {
    let stats = match tokio::time::timeout(QUERY_TIMEOUT, collect_tenant_stats(tenant_pool, day)).await {
        Err(_) => {
            return Err(format!(
                "platform rollup: tenant {org} stats collection timed out after {}s",
                QUERY_TIMEOUT.as_secs()
            ))
        }
        Ok(Err(e)) => return Err(format!("platform rollup: tenant {org} stats collection failed: {e}")),
        Ok(Ok(s)) => s,
    };

    let mut totals = DayTotals::default();
    totals.merge(stats);

    // No `access` source on the org tenant plane: `user_access_log` is
    // control-only and has no tenant-tree counterpart (see the module doc's
    // "Known simplifications"). The rollup row itself is written to
    // `control`, NOT `tenant_pool` — see this function's own doc comment.
    write_rollup(control, day, org, &totals, &[]).await
}

/// Collect one org tenant's day-scoped stats directly against `pool`.
///
/// These seven counts are genuinely tenant-wide — a rollup that counted only one
/// member's ocean would be wrong by definition — so this is honest work for
/// `begin_tenant_system_scoped` rather than an owner-scoped transaction.
///
/// It currently runs ambient, with no GUC at all. That used to be justified by
/// the harness handing out a connection-scoped `app.tenant_actor=system` pool;
/// that pool's startup-packet GUC was removed because a connection pooler
/// rejects it (see `TenantRegistry::connect_and_verify`), so these queries now
/// satisfy `0009_tenant_rls` only via the owner exemption. Wrapping the seven
/// counts in one short system-scoped read transaction is the fix — safe here
/// because, unlike most tenant-loop workers, this tick makes no LLM or R2 calls
/// and is already bounded by an explicit timeout.
async fn collect_tenant_stats(pool: &PgPool, day: NaiveDate) -> Result<DayStats, sqlx::Error> {
    let atoms: i64 = sqlx::query_scalar("SELECT count(*) FROM atoms WHERE created_at::date = $1")
        .bind(day)
        .fetch_one(pool)
        .await?;

    let reefs: i64 = sqlx::query_scalar("SELECT count(*) FROM graphs WHERE created_at::date = $1")
        .bind(day)
        .fetch_one(pool)
        .await?;

    let sessions: i64 = sqlx::query_scalar("SELECT count(*) FROM conversations WHERE created_at::date = $1")
        .bind(day)
        .fetch_one(pool)
        .await?;

    // One query for all four sums (CodeRabbit round 1 on #511): this
    // collector has no transaction at all (queries `pool` directly), so two
    // sequential `SELECT`s here are even more exposed than
    // `collect_account_stats`'s READ COMMITTED transaction to a row
    // committed between them making plain and cache totals disagree about
    // which rows they cover for the same day. Same `SUM(bigint)` -> NUMERIC
    // decode pitfall as the personal-plane query above and
    // `routes/admin.rs`'s existing cast sites — explicit `::bigint` required.
    let (input_tokens, output_tokens, cache_creation_tokens, cache_read_tokens): (i64, i64, i64, i64) =
        sqlx::query_as(
            "SELECT COALESCE(SUM(input_tokens), 0)::bigint, COALESCE(SUM(output_tokens), 0)::bigint, \
                    COALESCE(SUM(cache_creation_input_tokens), 0)::bigint, \
                    COALESCE(SUM(cache_read_input_tokens), 0)::bigint \
             FROM llm_usage WHERE created_at::date = $1",
        )
        .bind(day)
        .fetch_one(pool)
        .await?;

    let activity_users: Vec<Uuid> = sqlx::query_scalar(
        "SELECT DISTINCT user_id FROM activity_events WHERE created_at::date = $1 AND user_id IS NOT NULL",
    )
    .bind(day)
    .fetch_all(pool)
    .await?;

    let query_users: Vec<Uuid> = sqlx::query_scalar("SELECT DISTINCT user_id FROM queries WHERE created_at::date = $1")
        .bind(day)
        .fetch_all(pool)
        .await?;

    let mcp_users: Vec<Uuid> = sqlx::query_scalar(
        "SELECT DISTINCT user_id FROM mcp_tool_calls WHERE created_at::date = $1 AND user_id IS NOT NULL",
    )
    .bind(day)
    .fetch_all(pool)
    .await?;

    Ok(DayStats {
        atoms,
        reefs,
        sessions,
        input_tokens,
        output_tokens,
        cache_creation_tokens,
        cache_read_tokens,
        activity_users,
        query_users,
        mcp_users,
    })
}

// ── Shared upsert path (both planes) ─────────────────────────────────────────

/// Upsert `platform_metrics_daily` plus every `platform_active_users` source
/// row for one `(day, tenant)`, and report the [`Tick`] outcome. `access` is
/// empty on the org-tenant branch (see the module doc).
async fn write_rollup(
    pool: &PgPool,
    day: NaiveDate,
    tenant: &str,
    totals: &DayTotals,
    access_users: &[Uuid],
) -> Result<Tick, String> {
    upsert_metrics_daily(pool, day, tenant, totals)
        .await
        .map_err(|e| format!("platform rollup: failed to upsert platform_metrics_daily for tenant {tenant}: {e}"))?;

    let activity_vec: Vec<Uuid> = totals.activity_users.iter().copied().collect();
    let query_vec: Vec<Uuid> = totals.query_users.iter().copied().collect();
    let mcp_vec: Vec<Uuid> = totals.mcp_users.iter().copied().collect();

    let mut active_user_rows = 0u64;
    for (source, users) in [("activity", &activity_vec), ("query", &query_vec), ("mcp", &mcp_vec)] {
        active_user_rows += upsert_active_users(pool, day, tenant, source, users)
            .await
            .map_err(|e| format!("platform rollup: failed to upsert platform_active_users({source}) for tenant {tenant}: {e}"))?;
    }
    if !access_users.is_empty() {
        active_user_rows += upsert_active_users(pool, day, tenant, "access", access_users)
            .await
            .map_err(|e| format!("platform rollup: failed to upsert platform_active_users(access) for tenant {tenant}: {e}"))?;
    }

    tracing::info!(
        %tenant, %day,
        atoms = totals.atoms, reefs = totals.reefs, sessions = totals.sessions,
        input_tokens = totals.input_tokens, output_tokens = totals.output_tokens,
        active_user_rows,
        "platform rollup: tenant rollup complete"
    );

    // A genuinely idle tenant (zero activity, zero newly-active users today)
    // reports `Tick::idle()` — the right signal for the harness's org-tenant
    // idle backoff (Neon scale-to-zero) to engage; any real activity keeps
    // it at `base_poll` (see the module doc's "Interval" section).
    if totals.atoms > 0 || totals.reefs > 0 || totals.sessions > 0 || active_user_rows > 0 {
        Ok(Tick::worked((totals.atoms + totals.reefs + totals.sessions) as usize + active_user_rows as usize))
    } else {
        Ok(Tick::idle())
    }
}

/// Writes `llm_cache_creation_tokens`/`llm_cache_read_tokens` (migration
/// `0123_platform_metrics_daily_cache_tokens.sql`) as real, non-null values
/// on EVERY row this function touches — `totals.cache_creation_tokens`/
/// `cache_read_tokens` are always genuinely computed sums (never a stand-in
/// for "unknown"), including on a same-day re-upsert. `NULL` in those two
/// columns therefore only ever persists for a `(day, tenant)` row this
/// worker has not (re)written since before this migration shipped — see that
/// migration's own doc comment for why that distinction from a real `0`
/// matters to `admin_spend`'s completeness signal.
async fn upsert_metrics_daily(pool: &PgPool, day: NaiveDate, tenant: &str, totals: &DayTotals) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO platform_metrics_daily \
           (day, tenant, atom_count, reef_count, session_count, llm_input_tokens, llm_output_tokens, \
            llm_cache_creation_tokens, llm_cache_read_tokens, computed_at) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, now()) \
         ON CONFLICT (day, tenant) DO UPDATE SET \
           atom_count = EXCLUDED.atom_count, \
           reef_count = EXCLUDED.reef_count, \
           session_count = EXCLUDED.session_count, \
           llm_input_tokens = EXCLUDED.llm_input_tokens, \
           llm_output_tokens = EXCLUDED.llm_output_tokens, \
           llm_cache_creation_tokens = EXCLUDED.llm_cache_creation_tokens, \
           llm_cache_read_tokens = EXCLUDED.llm_cache_read_tokens, \
           computed_at = EXCLUDED.computed_at",
    )
    .bind(day)
    .bind(tenant)
    .bind(totals.atoms)
    .bind(totals.reefs)
    .bind(totals.sessions)
    .bind(totals.input_tokens)
    .bind(totals.output_tokens)
    .bind(totals.cache_creation_tokens)
    .bind(totals.cache_read_tokens)
    .execute(pool)
    .await?;
    Ok(())
}

/// Bulk-upsert one source's active-user presence rows via `UNNEST` (one
/// round trip regardless of how many users this cycle found, rather than one
/// `INSERT` per user). `ON CONFLICT ... DO NOTHING` matches 0095's own doc
/// comment on `platform_active_users`'s PK: "presence has no payload to
/// update". Returns the number of NEW rows actually inserted (rows already
/// present from an earlier pass today are not re-counted).
async fn upsert_active_users(
    pool: &PgPool,
    day: NaiveDate,
    tenant: &str,
    source: &str,
    users: &[Uuid],
) -> Result<u64, sqlx::Error> {
    if users.is_empty() {
        return Ok(0);
    }
    let result = sqlx::query(
        "INSERT INTO platform_active_users (day, user_id, tenant, source) \
         SELECT $1, u, $2, $3 FROM UNNEST($4::uuid[]) AS u \
         ON CONFLICT (day, user_id, tenant, source) DO NOTHING",
    )
    .bind(day)
    .bind(tenant)
    .bind(source)
    .bind(users)
    .execute(pool)
    .await?;
    Ok(result.rows_affected())
}

// ── Tests (pure logic only — live-Postgres proofs live in
//    backend/tests/platform_rollup_pg.rs) ────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rollup_interval_defaults_and_parses_env(){
        // Can't safely mutate process env in a parallel test binary — this
        // just proves the default constant matches the brief ("default
        // daily") without touching std::env, mirroring the constant-sanity
        // tests elsewhere (e.g. `access_log_purge`'s
        // `retention_and_interval_constants_are_sane`).
        assert_eq!(DEFAULT_ROLLUP_INTERVAL, Duration::from_secs(24 * 60 * 60));
    }

    #[test]
    fn day_totals_merge_sums_counts_and_dedupes_users() {
        let mut totals = DayTotals::default();
        let u1 = Uuid::new_v4();
        let u2 = Uuid::new_v4();

        totals.merge(DayStats {
            atoms: 3,
            reefs: 1,
            sessions: 2,
            input_tokens: 100,
            output_tokens: 50,
            cache_creation_tokens: 40,
            cache_read_tokens: 20,
            activity_users: vec![u1],
            query_users: vec![u1],
            mcp_users: vec![],
        });
        totals.merge(DayStats {
            atoms: 4,
            reefs: 0,
            sessions: 1,
            input_tokens: 25,
            output_tokens: 10,
            cache_creation_tokens: 5,
            cache_read_tokens: 15,
            // Same user u1 active in a SECOND account this day: must dedupe,
            // not double count (platform_active_users's PK is (day, user_id,
            // tenant, source) — no account dimension at all).
            activity_users: vec![u1, u2],
            query_users: vec![],
            mcp_users: vec![u2],
        });

        assert_eq!(totals.atoms, 7);
        assert_eq!(totals.reefs, 1);
        assert_eq!(totals.sessions, 3);
        assert_eq!(totals.input_tokens, 125);
        assert_eq!(totals.output_tokens, 60);
        assert_eq!(totals.cache_creation_tokens, 45, "40 + 5");
        assert_eq!(totals.cache_read_tokens, 35, "20 + 15");
        assert_eq!(totals.activity_users.len(), 2, "u1 deduped across two accounts, u2 added once");
        assert!(totals.activity_users.contains(&u1));
        assert!(totals.activity_users.contains(&u2));
        assert_eq!(totals.query_users.len(), 1);
        assert_eq!(totals.mcp_users.len(), 1);
    }
}

//! Daily `llm_usage` vs `query_history` cross-check, plus deduped
//! unknown-model alerting (usage-metering PR11 of 13,
//! `.scratch/usage-metering/plan.md` §1 PR11).
//!
//! # What PR9 already does, and what this PR adds on top
//!
//! `workers::billing_derivation` (PR9, merged) already:
//!   * classifies source-id-less `llm_usage` rows into cache-hit
//!     (`*_cached` task, zero tokens) vs. an "unthreaded" upper bound
//!     (pre-epoch rows AND the retired batch-extraction worker's unmetered
//!     legacy rows, genuinely inseparable with today's schema), and alerts on the
//!     latter, aggregated, once per ~5-minute tick.
//!   * prices every `llm_usage` bucket through `billing::rates::PriceSheet::
//!     rate`, whose unmatched branch ALREADY emits a raw, non-deduped
//!     `tracing::warn!(alert = true, model = .., ...)` (PR1) — see
//!     `billing::unknown_model_alert`'s module doc for the full relationship.
//!
//! Neither of those is duplicated or contradicted here. This worker adds two
//! things PR9 does not do at all:
//!   1. A genuine `llm_usage` vs `query_history` CROSS-CHECK — did the
//!      metered choke point (PR4) actually fire for every request-path turn
//!      that ran synthesis, and does every `llm_usage` query-axis row name a
//!      real `query_history` turn — which PR9 never computes (it costs
//!      `llm_usage`, it never compares it against `query_history` at all).
//!   2. A DEDUPED, PAGED unknown-model alert
//!      (`billing::unknown_model_alert::UnknownModelAlertTracker`), which is
//!      what actually pages an operator; PR9's own per-tick warn stays
//!      exactly as noisy as it always was (unmodified, deliberately — see
//!      `billing::unknown_model_alert`'s module doc for why this PR does not
//!      touch `PriceSheet::rate` itself).
//!
//! # Shape: `platform_rollup`'s exact precedent, not `billing_derivation`'s
//!
//! Registered with `workers::tenant_harness::HarnessConfig::sweep_worker`
//! (24h default, env-overridable) exactly like `platform_rollup` and
//! `billing_derivation` — the harness enumerates tenants, this module's
//! [`tick`] runs once per due tenant. Unlike `billing_derivation`'s
//! cursor-based incremental design, this worker copies `platform_rollup`'s
//! SIMPLER shape instead: recompute the WHOLE calendar day
//! (`Utc::now().date_naive()`, `created_at::date = $day` — the exact
//! predicate `platform_rollup`'s own per-day queries already use) and
//! `ON CONFLICT (day, tenant) DO UPDATE` into `usage_reconciliation_daily`
//! on every tick, same as `platform_rollup::upsert_metrics_daily`. No
//! cursor, no overlap window: a reconciliation table has no ledger to
//! double-charge, so there is nothing a re-run needs to be idempotent
//! AGAINST beyond "the same day's row gets overwritten with a fresher
//! total", which `ON CONFLICT ... DO UPDATE` already gives for free.
//!
//! Two pools, same split as `platform_rollup`/`billing_derivation`:
//! `usage_reconciliation_daily` is CONTROL-ONLY (migration
//! `0154_usage_reconciliation_daily.sql`), so content is COLLECTED from
//! `t.pool` per tenant and the summary row is WRITTEN to `control`.
//!
//! # Per-account loop, reusing PR9's own helpers
//!
//! `llm_usage`/`query_history` are both RLS-covered
//! (`account_isolation`/`member_ocean_isolation`) on the SAME predicates PR9
//! already worked out — so this worker reuses PR9's own account-roster and
//! scope-opening helpers verbatim
//! ([`crate::workers::billing_derivation::enumerate_consumer_accounts`],
//! [`crate::workers::billing_derivation::resolve_org_account_id`],
//! [`crate::workers::billing_derivation::open_read_scope`], all made
//! `pub(crate)` in this same PR) rather than re-deriving the identical
//! Consumer-loop-per-account / Org-whole-tenant-DB split a second time. The
//! Org branch omits the `account_id` predicate on `llm_usage` for the SAME
//! reason PR9's journal axis does (worker-written rows can carry
//! `account_id = NULL` there; a whole org tenant DB belongs to exactly one
//! account, so "no predicate" and "this one account" mean the same thing).
//! `query_history` is written ONLY by request-path handlers (never a
//! background worker), so its `account_id` is always real on both planes,
//! and both the query_history cross-check and the query-axis orphan scan
//! filter on it uniformly — no Org special case needed there.
//!
//! # The cross-check queries
//!
//! * **`query_history_missing_usage`** — `query_history` rows this day WHERE
//!   `model IS NOT NULL` (synthesis actually ran — see migration
//!   `0029_query_history_model.sql`/`0120`'s header: a NULL model means
//!   retrieval-only/abstention/no-synthesis-turn, which legitimately never
//!   writes `llm_usage`, so counting those would manufacture false
//!   anomalies) AND no `llm_usage` row anywhere carries a matching
//!   `source_query_id`. A nonzero count means PR4's choke point did not fire
//!   for a real synthesis call.
//! * **`llm_usage_orphan_query_rows`** — the reverse: `llm_usage` query-axis
//!   rows this day naming a `source_query_id` with no matching
//!   `query_history.id` at all.
//!
//! # Unknown-model scan — the sentinel stays silent, genuinely unknown
//! # models don't
//!
//! [`scan_model_groups`] groups a day's `llm_usage` rows by DISTINCT
//! `(provider, model)` in SQL (`GROUP BY provider, model`) — never iterates
//! raw rows — then [`tick`] filters out
//! [`crate::billing::rates::UNATTRIBUTED_SENTINEL`] BEFORE consulting
//! [`PriceSheet::rate`], exactly matching plan §3 item 4 / §2 item 7 ("keep
//! the sentinel path; alert only on genuinely unrecognized models") — the
//! sentinel is how org-plane rows with no per-model breakdown are deliberately,
//! silently priced off the blend, and must never reach the alert tracker.
//! Every account's surviving unmatched pairs are unioned into ONE
//! `HashMap<(provider, model), row_count>` for the WHOLE tick (across every
//! account on the Consumer plane, or the one account on Org) before a SINGLE
//! pass over that map calls
//! [`crate::billing::unknown_model_alert::UnknownModelAlertTracker::record`]
//! once per distinct pair — see that module's doc for the full escalation
//! design and why a thousand rows sharing one pair still drives exactly one
//! call.
//!
//! # Staleness
//!
//! `usage_reconciliation_daily.computed_at` is set on every upsert. An
//! operator (or a future dashboard) can poll it exactly the way
//! `platform_rollup`'s own module doc documents for
//! `platform_metrics_daily`: `SELECT tenant FROM usage_reconciliation_daily
//! WHERE day = current_date AND computed_at < now() - interval '2 hours'`
//! finds every tenant this worker silently failed to refresh today.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Duration;

use chrono::{DateTime, NaiveDate, Utc};
use sqlx::{PgPool, Postgres, Transaction};
use uuid::Uuid;

use crate::billing::rates::{PriceSheet, UNATTRIBUTED_SENTINEL};
use crate::billing::unknown_model_alert::{UnknownModelAlertTracker, UnknownModelSeverity};
use crate::tenancy::scope::{begin_account_scoped, begin_tenant_system_scoped};
use crate::tenancy::TenantId;
use crate::workers::billing_derivation::{enumerate_consumer_accounts, resolve_org_account_id};
use crate::workers::tenant_harness::{Tick, WorkerTenant};

// ── Tunables ──────────────────────────────────────────────────────────────

/// Daily, matching the plan's own framing ("A daily `llm_usage` vs
/// `query_history` cross-check") and `platform_rollup::DEFAULT_ROLLUP_
/// INTERVAL`'s identical cadence. Env-overridable via
/// `USAGE_RECONCILIATION_INTERVAL_SECS`, same accessor shape as
/// `billing_derivation::derivation_interval`/`platform_rollup::rollup_interval`.
pub const DEFAULT_INTERVAL: Duration = Duration::from_secs(24 * 60 * 60);

pub fn reconciliation_interval() -> Duration {
    std::env::var("USAGE_RECONCILIATION_INTERVAL_SECS")
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .filter(|secs| *secs > 0)
        .map(Duration::from_secs)
        .unwrap_or(DEFAULT_INTERVAL)
}

/// `platform_metrics_daily`'s own `'__consumer__'` convention, copied
/// byte-for-byte so an operator joining the two tables by `(day, tenant)`
/// never has to reconcile two different spellings of the same plane.
const CONSUMER_TENANT: &str = "__consumer__";

// ── Entry point ───────────────────────────────────────────────────────────

/// One reconciliation pass for one tenant. `control` is always the
/// control-plane pool (same two-pool split as `platform_rollup`/
/// `billing_derivation`). `alert_tracker` is the ONE, process-wide,
/// long-lived [`UnknownModelAlertTracker`] instance `lib.rs`'s spawn site
/// owns for the life of this worker's background task — see that module's
/// doc for why it must be shared across every tenant and every tick, not
/// reconstructed per call.
pub async fn tick(
    t: &WorkerTenant,
    control: &PgPool,
    alert_tracker: &Mutex<UnknownModelAlertTracker>,
) -> Result<Tick, String> {
    let sheet = PriceSheet::load(control)
        .await
        .map_err(|e| format!("usage reconciliation: failed to load PriceSheet: {e}"))?;
    let as_of = Utc::now();
    let day = as_of.date_naive();
    reconcile_day(t, control, &sheet, as_of, day, alert_tracker).await
}

/// The day-taking half of [`tick`], split out so `pg_tests` can drive a
/// specific, deterministic day rather than always reconciling "today" —
/// every seeded row in this suite lands under `now()` regardless (this
/// worker has no cursor-lag window to backdate past, unlike
/// `billing_derivation`'s tests), so [`tick`]'s own `Utc::now().date_naive()`
/// is exercised directly by every live test in this file.
async fn reconcile_day(
    t: &WorkerTenant,
    control: &PgPool,
    sheet: &PriceSheet,
    as_of: DateTime<Utc>,
    day: NaiveDate,
    alert_tracker: &Mutex<UnknownModelAlertTracker>,
) -> Result<Tick, String> {
    let is_org = matches!(t.tenant, TenantId::Org(_));
    let tenant_label = match &t.tenant {
        TenantId::Consumer => CONSUMER_TENANT.to_string(),
        TenantId::Org(org) => org.clone(),
    };
    let account_ids: Vec<Uuid> = match &t.tenant {
        TenantId::Consumer => enumerate_consumer_accounts(control).await?,
        TenantId::Org(org) => vec![resolve_org_account_id(control, org).await?],
    };

    let mut totals = DayTotals::default();
    // Unioned across every account this tick, before the alert tracker is
    // ever consulted — see the module doc's "Unknown-model scan" section.
    let mut unknown_pairs: HashMap<(String, String), i64> = HashMap::new();

    for account_id in account_ids {
        let mut tx = open_read_scope(&t.pool, &t.tenant, account_id)
            .await
            .map_err(|e| format!("usage reconciliation: failed to open read scope for account {account_id}: {e}"))?;

        let (qh_rows, qh_missing) = query_history_reconciliation(&mut tx, account_id, day).await?;
        totals.query_history_rows += qh_rows;
        totals.query_history_missing_usage += qh_missing;

        totals.llm_usage_orphan_query_rows += orphan_query_axis_rows(&mut tx, account_id, day).await?;

        let (cache_hit, unthreaded) = classify_unthreaded(&mut tx, account_id, is_org, day).await?;
        totals.cache_hit_rows += cache_hit;
        totals.unthreaded_rows += unthreaded;

        for (provider, model, row_count) in scan_model_groups(&mut tx, account_id, is_org, day).await? {
            if model == UNATTRIBUTED_SENTINEL {
                // Deliberate, silent — see the module doc's "Unknown-model
                // scan" section and plan §3 item 4.
                continue;
            }
            if !sheet.rate(&provider, &model, as_of).matched {
                totals.unknown_model_rows += row_count;
                *unknown_pairs.entry((provider, model)).or_insert(0) += row_count;
            }
        }

        tx.commit()
            .await
            .map_err(|e| format!("usage reconciliation: failed to commit read scope for account {account_id}: {e}"))?;
    }

    // ── Alert phase: one record() call per DISTINCT pair for the WHOLE
    // tick, never per row, never per account. ──────────────────────────────
    if !unknown_pairs.is_empty() {
        let mut tracker = alert_tracker.lock().unwrap_or_else(|poison| poison.into_inner());
        for ((provider, model), row_count) in &unknown_pairs {
            match tracker.record(provider, model, as_of) {
                UnknownModelSeverity::Warn => tracing::warn!(
                    provider = %provider, model = %model, row_count = *row_count, tenant = %tenant_label, %day,
                    "usage reconciliation: unrecognized model seen this pass — will page if it persists"
                ),
                UnknownModelSeverity::Alert => tracing::error!(
                    alert = true,
                    credit_health = true,
                    provider = %provider, model = %model, row_count = *row_count, tenant = %tenant_label, %day,
                    "usage reconciliation: model has been unrecognized for multiple consecutive \
                     reconciliation passes — its cost is silently blending into the fallback rate; \
                     add a model_rates row"
                ),
                UnknownModelSeverity::Quiet => tracing::debug!(
                    provider = %provider, model = %model, row_count = *row_count, tenant = %tenant_label, %day,
                    "usage reconciliation: model still unrecognized (already alerted this outage)"
                ),
            }
        }
    }

    // Cross-check anomalies get their own visible signal — `credit_health =
    // true` on the genuine metering-gap direction (a real synthesis call
    // that the choke point never metered), a plain `warn!` on the orphan
    // direction (a stray id, not a missed charge). Neither is routed through
    // `alert_tracker`: that tracker's escalate/quiet contract is specific to
    // the unknown-model condition (see its own module doc); a day with a
    // metering gap is worth surfacing every day it recurs, not silenced
    // after the second occurrence.
    if totals.query_history_missing_usage > 0 {
        tracing::warn!(
            credit_health = true,
            count = totals.query_history_missing_usage, tenant = %tenant_label, %day,
            "usage reconciliation: query_history rows with a recorded synthesis model but no \
             matching llm_usage.source_query_id — the metering choke point did not fire for these"
        );
    }
    if totals.llm_usage_orphan_query_rows > 0 {
        tracing::warn!(
            count = totals.llm_usage_orphan_query_rows, tenant = %tenant_label, %day,
            "usage reconciliation: llm_usage query-axis rows naming no query_history row in this window"
        );
    }

    upsert_reconciliation_row(control, day, &tenant_label, &totals)
        .await
        .map_err(|e| format!("usage reconciliation: failed to write usage_reconciliation_daily for tenant {tenant_label}: {e}"))?;

    tracing::info!(
        tenant = %tenant_label, %day,
        query_history_rows = totals.query_history_rows,
        query_history_missing_usage = totals.query_history_missing_usage,
        llm_usage_orphan_query_rows = totals.llm_usage_orphan_query_rows,
        unknown_model_rows = totals.unknown_model_rows,
        cache_hit_rows = totals.cache_hit_rows,
        unthreaded_rows = totals.unthreaded_rows,
        "usage reconciliation: tenant pass complete"
    );

    if totals.is_all_zero() {
        Ok(Tick::idle())
    } else {
        Ok(Tick::worked(totals.activity_count()))
    }
}

/// Which scoped-transaction primitive to open on `t.pool` for reading
/// `llm_usage`/`query_history` — the identical per-plane split
/// `workers::billing_derivation::open_read_scope` already established (see
/// that function's own doc for the full "Per-account loop on BOTH branches"
/// reasoning). Deliberately a SEPARATE, LOCAL copy rather than an import of
/// that private-to-its-module twin: `scripts/check_rls_coverage.sh`'s
/// `converted` check greps each file for a LITERAL `begin_account_scoped`/
/// `begin_tenant_system_scoped` call, not a transitive one reached through
/// an imported helper, so this file needs its own literal call site to be
/// correctly recognized as scoped rather than requiring a `# converted: <why>`
/// waiver comment in `scripts/rls_coverage.manifest`.
async fn open_read_scope(
    pool: &PgPool,
    tenant: &TenantId,
    account_id: Uuid,
) -> Result<Transaction<'static, Postgres>, crate::store::StoreError> {
    match tenant {
        TenantId::Consumer => begin_account_scoped(pool, account_id).await,
        TenantId::Org(_) => begin_tenant_system_scoped(pool, account_id).await,
    }
}

// ── Per-tick totals ──────────────────────────────────────────────────────

#[derive(Debug, Default, Clone, Copy)]
struct DayTotals {
    query_history_rows: i64,
    query_history_missing_usage: i64,
    llm_usage_orphan_query_rows: i64,
    unknown_model_rows: i64,
    cache_hit_rows: i64,
    unthreaded_rows: i64,
}

impl DayTotals {
    fn is_all_zero(&self) -> bool {
        self.query_history_rows == 0
            && self.query_history_missing_usage == 0
            && self.llm_usage_orphan_query_rows == 0
            && self.unknown_model_rows == 0
            && self.cache_hit_rows == 0
            && self.unthreaded_rows == 0
    }

    /// Feeds `Tick::worked(n)` — matches `platform_rollup`'s own convention
    /// of summing whichever counts genuinely reflect "there was something
    /// here today" rather than a literal row-count of every table touched.
    fn activity_count(&self) -> usize {
        (self.query_history_rows + self.unknown_model_rows + self.cache_hit_rows + self.unthreaded_rows).max(0) as usize
    }
}

// ── Cross-check queries ──────────────────────────────────────────────────

/// `(query_history_rows, query_history_missing_usage)` — see the module
/// doc's "The cross-check queries" section for why `model IS NOT NULL`
/// gates the missing-usage half.
async fn query_history_reconciliation(
    tx: &mut Transaction<'static, Postgres>,
    account_id: Uuid,
    day: NaiveDate,
) -> Result<(i64, i64), String> {
    let row: (i64, i64) = sqlx::query_as(
        "SELECT \
             count(*) AS query_history_rows, \
             count(*) FILTER ( \
                 WHERE model IS NOT NULL \
                   AND NOT EXISTS (SELECT 1 FROM llm_usage lu WHERE lu.source_query_id = qh.id) \
             ) AS query_history_missing_usage \
         FROM query_history qh \
         WHERE qh.account_id = $1 AND qh.created_at::date = $2",
    )
    .bind(account_id)
    .bind(day)
    .fetch_one(&mut **tx)
    .await
    .map_err(|e| format!("query_history reconciliation: {e}"))?;
    Ok(row)
}

/// `llm_usage` query-axis rows this day naming a `source_query_id` with no
/// matching `query_history.id` anywhere.
async fn orphan_query_axis_rows(
    tx: &mut Transaction<'static, Postgres>,
    account_id: Uuid,
    day: NaiveDate,
) -> Result<i64, String> {
    let count: i64 = sqlx::query_scalar(
        "SELECT count(DISTINCT lu.source_query_id) \
         FROM llm_usage lu \
         WHERE lu.account_id = $1 AND lu.source_query_id IS NOT NULL AND lu.created_at::date = $2 \
           AND NOT EXISTS (SELECT 1 FROM query_history qh WHERE qh.id = lu.source_query_id)",
    )
    .bind(account_id)
    .bind(day)
    .fetch_one(&mut **tx)
    .await
    .map_err(|e| format!("orphan query-axis scan: {e}"))?;
    Ok(count)
}

/// `(cache_hit_rows, unthreaded_rows)` — the SAME predicate
/// `workers::billing_derivation::classify_unthreaded_usage` uses (task LIKE
/// `%_cached`, among rows carrying none of the three source ids), re-derived
/// here at day granularity for the historical record. Does not itself alert
/// on `unthreaded_rows` — see the module doc's "What PR9 already does"
/// section for why that alert stays PR9's alone.
async fn classify_unthreaded(
    tx: &mut Transaction<'static, Postgres>,
    account_id: Uuid,
    is_org: bool,
    day: NaiveDate,
) -> Result<(i64, i64), String> {
    let row: (i64, i64) = if is_org {
        sqlx::query_as(
            "SELECT \
                COALESCE(SUM(CASE WHEN task LIKE '%\\_cached' ESCAPE '\\' THEN 1 ELSE 0 END), 0)::bigint, \
                COALESCE(SUM(CASE WHEN task NOT LIKE '%\\_cached' ESCAPE '\\' THEN 1 ELSE 0 END), 0)::bigint \
             FROM llm_usage \
             WHERE source_job_id IS NULL AND source_query_id IS NULL \
               AND source_journal_period IS NULL AND created_at::date = $1",
        )
        .bind(day)
        .fetch_one(&mut **tx)
        .await
    } else {
        sqlx::query_as(
            "SELECT \
                COALESCE(SUM(CASE WHEN task LIKE '%\\_cached' ESCAPE '\\' THEN 1 ELSE 0 END), 0)::bigint, \
                COALESCE(SUM(CASE WHEN task NOT LIKE '%\\_cached' ESCAPE '\\' THEN 1 ELSE 0 END), 0)::bigint \
             FROM llm_usage \
             WHERE account_id = $1 AND source_job_id IS NULL AND source_query_id IS NULL \
               AND source_journal_period IS NULL AND created_at::date = $2",
        )
        .bind(account_id)
        .bind(day)
        .fetch_one(&mut **tx)
        .await
    }
    .map_err(|e| format!("classification: {e}"))?;
    Ok(row)
}

/// One `(provider, model, row_count)` triple per DISTINCT pair this
/// account/tenant wrote today — the SQL-level dedup step every caller of
/// [`crate::billing::unknown_model_alert::UnknownModelAlertTracker::record`]
/// must apply before touching the tracker (see that module's doc and
/// [`tick`]'s alert phase above).
async fn scan_model_groups(
    tx: &mut Transaction<'static, Postgres>,
    account_id: Uuid,
    is_org: bool,
    day: NaiveDate,
) -> Result<Vec<(String, String, i64)>, String> {
    #[derive(sqlx::FromRow)]
    struct ModelGroupRow {
        provider: String,
        model: String,
        row_count: i64,
    }

    let rows: Vec<ModelGroupRow> = if is_org {
        sqlx::query_as(
            "SELECT provider, model, count(*)::bigint AS row_count \
             FROM llm_usage \
             WHERE created_at::date = $1 \
             GROUP BY provider, model",
        )
        .bind(day)
        .fetch_all(&mut **tx)
        .await
    } else {
        sqlx::query_as(
            "SELECT provider, model, count(*)::bigint AS row_count \
             FROM llm_usage \
             WHERE account_id = $1 AND created_at::date = $2 \
             GROUP BY provider, model",
        )
        .bind(account_id)
        .bind(day)
        .fetch_all(&mut **tx)
        .await
    }
    .map_err(|e| format!("model scan: {e}"))?;

    Ok(rows.into_iter().map(|r| (r.provider, r.model, r.row_count)).collect())
}

// ── Write phase ───────────────────────────────────────────────────────────

/// `ON CONFLICT (day, tenant) DO UPDATE` — identical upsert shape to
/// `platform_rollup::upsert_metrics_daily`, including `computed_at =
/// EXCLUDED.computed_at` refreshing the staleness column on every call, even
/// an all-zero one, so a genuinely dormant tenant's row still proves this
/// worker reached it today.
async fn upsert_reconciliation_row(
    control: &PgPool,
    day: NaiveDate,
    tenant: &str,
    totals: &DayTotals,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO usage_reconciliation_daily \
             (day, tenant, query_history_rows, query_history_missing_usage, \
              llm_usage_orphan_query_rows, unknown_model_rows, cache_hit_rows, unthreaded_rows, computed_at) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, now()) \
         ON CONFLICT (day, tenant) DO UPDATE SET \
             query_history_rows = EXCLUDED.query_history_rows, \
             query_history_missing_usage = EXCLUDED.query_history_missing_usage, \
             llm_usage_orphan_query_rows = EXCLUDED.llm_usage_orphan_query_rows, \
             unknown_model_rows = EXCLUDED.unknown_model_rows, \
             cache_hit_rows = EXCLUDED.cache_hit_rows, \
             unthreaded_rows = EXCLUDED.unthreaded_rows, \
             computed_at = EXCLUDED.computed_at",
    )
    .bind(day)
    .bind(tenant)
    .bind(totals.query_history_rows)
    .bind(totals.query_history_missing_usage)
    .bind(totals.llm_usage_orphan_query_rows)
    .bind(totals.unknown_model_rows)
    .bind(totals.cache_hit_rows)
    .bind(totals.unthreaded_rows)
    .execute(control)
    .await?;
    Ok(())
}

// ── Live-Postgres tests ──────────────────────────────────────────────────
//
// Gated on `RELINFER_TEST_DATABASE_URL`, same convention as
// `workers::billing_derivation::pg_tests` (see that module for the
// precedent this file follows verbatim, including "run on the Consumer
// plane only" — the CI job this suite rides in applies only
// `migrations_control`, with no tenant-tree database to exercise the Org
// branch against; that branch is covered by `cargo check`'s type-checking
// and direct review, flagged honestly rather than implied covered). `cargo
// test --lib` with no env var set SKIPS these with an `eprintln!` marker.
// Fresh account/graph UUIDs and randomized unknown-model strings separate
// fixture rows and alert sightings, but the daily summary covers EVERY
// Consumer account in the shared CI database. Each test therefore recomputes
// its baseline through the same reconciliation inputs before adding its rows;
// reading a persisted summary alone can mix stale totals or price sheets.
#[cfg(test)]
mod pg_tests {
    use std::sync::Arc;

    use super::*;
    use crate::store::PostgresStore;

    async fn pg_test_pool(test_name: &str) -> Option<PgPool> {
        let url = match std::env::var("RELINFER_TEST_DATABASE_URL") {
            Ok(u) => u,
            Err(_) => {
                eprintln!("skipping {test_name}: set RELINFER_TEST_DATABASE_URL to run it");
                return None;
            }
        };
        Some(
            sqlx::postgres::PgPoolOptions::new()
                .max_connections(8)
                .connect(&url)
                .await
                .expect("connect to test Postgres"),
        )
    }

    fn consumer_tenant(pool: PgPool) -> WorkerTenant {
        WorkerTenant {
            tenant: TenantId::Consumer,
            store: Arc::new(PostgresStore::new(pool.clone())),
            pool,
        }
    }

    async fn seed_account(pool: &PgPool) -> Uuid {
        let user_id = Uuid::new_v4();
        let account_id = Uuid::new_v4();
        sqlx::query("INSERT INTO users (id, email) VALUES ($1, $2)")
            .bind(user_id)
            .bind(format!("{user_id}@usage-reconciliation-pr11.test"))
            .execute(pool)
            .await
            .expect("insert user");
        sqlx::query("INSERT INTO accounts (id, owner_user_id) VALUES ($1, $2)")
            .bind(account_id)
            .bind(user_id)
            .execute(pool)
            .await
            .expect("insert account");
        account_id
    }

    async fn seed_graph(pool: &PgPool, account_id: Uuid) -> Uuid {
        let graph_id = Uuid::new_v4();
        sqlx::query("INSERT INTO graphs (id, account_id, name) VALUES ($1, $2, 'test ocean')")
            .bind(graph_id)
            .bind(account_id)
            .execute(pool)
            .await
            .expect("insert graph");
        graph_id
    }

    #[allow(clippy::too_many_arguments)]
    async fn insert_query_history(
        pool: &PgPool,
        account_id: Uuid,
        graph_id: Uuid,
        model: Option<&str>,
    ) -> Uuid {
        let id = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO query_history (id, account_id, graph_id, query_text, model) \
             VALUES ($1, $2, $3, 'test query', $4)",
        )
        .bind(id)
        .bind(account_id)
        .bind(graph_id)
        .bind(model)
        .execute(pool)
        .await
        .expect("insert query_history");
        id
    }

    #[allow(clippy::too_many_arguments)]
    async fn insert_llm_usage(
        pool: &PgPool,
        account_id: Uuid,
        graph_id: Uuid,
        task: &str,
        model: &str,
        input_tokens: i32,
        output_tokens: i32,
        source_query_id: Option<Uuid>,
    ) {
        sqlx::query(
            "INSERT INTO llm_usage (account_id, graph_id, task, model, input_tokens, output_tokens, source_query_id) \
             VALUES ($1, $2, $3, $4, $5, $6, $7)",
        )
        .bind(account_id)
        .bind(graph_id)
        .bind(task)
        .bind(model)
        .bind(input_tokens)
        .bind(output_tokens)
        .bind(source_query_id)
        .execute(pool)
        .await
        .expect("insert llm_usage");
    }

    async fn reconciliation_row(pool: &PgPool, day: NaiveDate, tenant: &str) -> Option<DayTotalsRow> {
        sqlx::query_as(
            "SELECT query_history_rows, query_history_missing_usage, llm_usage_orphan_query_rows, \
                    unknown_model_rows, cache_hit_rows, unthreaded_rows \
             FROM usage_reconciliation_daily WHERE day = $1 AND tenant = $2",
        )
        .bind(day)
        .bind(tenant)
        .fetch_optional(pool)
        .await
        .expect("fetch reconciliation row")
    }

    #[derive(sqlx::FromRow, Debug, Clone, Copy, PartialEq, Eq)]
    struct DayTotalsRow {
        query_history_rows: i64,
        query_history_missing_usage: i64,
        llm_usage_orphan_query_rows: i64,
        unknown_model_rows: i64,
        cache_hit_rows: i64,
        unthreaded_rows: i64,
    }

    impl DayTotalsRow {
        const ZERO: DayTotalsRow = DayTotalsRow {
            query_history_rows: 0,
            query_history_missing_usage: 0,
            llm_usage_orphan_query_rows: 0,
            unknown_model_rows: 0,
            cache_hit_rows: 0,
            unthreaded_rows: 0,
        };

        /// `self - other`, field-wise. `usage_reconciliation_daily` is keyed
        /// `(day, tenant)` — a WHOLE-CONSUMER-PLANE aggregate, not a
        /// per-account one — so every test in this suite that shares one
        /// scratch database with every OTHER test in this suite (same
        /// `TenantId::Consumer` sweeps EVERY account that exists, exactly the
        /// cross-test-pollution hazard `billing_derivation::pg_tests::
        /// read_failure_refuses_to_write_anything`'s own doc comment already
        /// names) must compare a BEFORE/AFTER delta, never an absolute value,
        /// or an earlier test's still-present fixture rows silently inflate
        /// a later test's count.
        fn delta(&self, before: &DayTotalsRow) -> DayTotalsRow {
            DayTotalsRow {
                query_history_rows: self.query_history_rows - before.query_history_rows,
                query_history_missing_usage: self.query_history_missing_usage - before.query_history_missing_usage,
                llm_usage_orphan_query_rows: self.llm_usage_orphan_query_rows - before.llm_usage_orphan_query_rows,
                unknown_model_rows: self.unknown_model_rows - before.unknown_model_rows,
                cache_hit_rows: self.cache_hit_rows - before.cache_hit_rows,
                unthreaded_rows: self.unthreaded_rows - before.unthreaded_rows,
            }
        }
    }

    async fn reconciliation_row_or_zero(pool: &PgPool, day: NaiveDate, tenant: &str) -> DayTotalsRow {
        reconciliation_row(pool, day, tenant).await.unwrap_or(DayTotalsRow::ZERO)
    }

    fn new_tracker() -> Mutex<UnknownModelAlertTracker> {
        Mutex::new(UnknownModelAlertTracker::new())
    }

    async fn reconciled_baseline(
        t: &WorkerTenant,
        pool: &PgPool,
        sheet: &PriceSheet,
        as_of: DateTime<Utc>,
        day: NaiveDate,
    ) -> DayTotalsRow {
        // Refresh the whole-plane summary using the exact context used by
        // the measured pass. Keep priming sightings out of its fresh tracker.
        let baseline_tracker = new_tracker();
        reconcile_day(t, pool, sheet, as_of, day, &baseline_tracker)
            .await
            .expect("baseline reconciliation succeeds");
        reconciliation_row(pool, day, CONSUMER_TENANT)
            .await
            .expect("baseline reconciliation persists its summary")
    }

    /// **"Done + verified" — the daily cross-check runs and reports a sane
    /// reconciliation for a seeded window.** One query_history turn that DID
    /// meter correctly (a matching llm_usage row), one that DID NOT (a
    /// missing choke-point write), and one retrieval-only turn
    /// (`model IS NULL`) that legitimately has no usage row and must NOT be
    /// counted as an anomaly.
    #[tokio::test]
    async fn reconciliation_reports_a_sane_summary_for_a_seeded_window() {
        let Some(pool) = pg_test_pool("reconciliation_reports_a_sane_summary_for_a_seeded_window").await else {
            return;
        };
        let account_id = seed_account(&pool).await;
        let graph_id = seed_graph(&pool, account_id).await;
        let day = Utc::now().date_naive();
        let t = consumer_tenant(pool.clone());
        let sheet_seed = crate::billing::rates::tests::seeded_for_tests();
        let as_of = Utc::now();
        let before = reconciled_baseline(&t, &pool, &sheet_seed, as_of, day).await;

        // Metered correctly.
        let metered_query = insert_query_history(&pool, account_id, graph_id, Some("claude-sonnet-4-6")).await;
        insert_llm_usage(&pool, account_id, graph_id, "synthesis", "claude-sonnet-4-6", 100, 50, Some(metered_query)).await;

        // Synthesis ran, but the choke point never wrote llm_usage — the
        // genuine anomaly.
        insert_query_history(&pool, account_id, graph_id, Some("claude-sonnet-4-6")).await;

        // Retrieval-only turn: model NULL, no llm_usage row — NOT an anomaly.
        insert_query_history(&pool, account_id, graph_id, None).await;

        let tracker = new_tracker();
        let result = reconcile_day(&t, &pool, &sheet_seed, as_of, day, &tracker).await.expect("tick succeeds");
        assert!(result.processed > 0, "real query_history activity must not report idle");

        let after = reconciliation_row_or_zero(&pool, day, CONSUMER_TENANT).await;
        let delta = after.delta(&before);
        assert_eq!(delta.query_history_rows, 3, "all three query_history rows counted");
        assert_eq!(
            delta.query_history_missing_usage, 1,
            "exactly the one synthesis-ran-but-unmetered row, not the retrieval-only one"
        );
        assert_eq!(delta.llm_usage_orphan_query_rows, 0, "the one llm_usage row correctly names a real query_history turn");
    }

    /// **Negative control 1 — dedup.** Ten `llm_usage` rows share one
    /// genuinely unknown `(provider, model)` pair. The raw per-row count
    /// (`unknown_model_rows`) reflects all ten, but the alert tracker — the
    /// actual paging mechanism — is consulted exactly ONCE for this pass.
    #[tokio::test]
    async fn ten_rows_sharing_one_unknown_model_alert_exactly_once() {
        let Some(pool) = pg_test_pool("ten_rows_sharing_one_unknown_model_alert_exactly_once").await else {
            return;
        };
        let account_id = seed_account(&pool).await;
        let graph_id = seed_graph(&pool, account_id).await;
        let unknown_model = format!("totally-unrecognized-model-{}", Uuid::new_v4());
        let day = Utc::now().date_naive();
        let t = consumer_tenant(pool.clone());
        let sheet_seed = crate::billing::rates::tests::seeded_for_tests();
        let as_of = Utc::now();
        let before = reconciled_baseline(&t, &pool, &sheet_seed, as_of, day).await;

        for _ in 0..10 {
            insert_llm_usage(&pool, account_id, graph_id, "extract_entities", &unknown_model, 10, 5, None).await;
        }

        let tracker = new_tracker();
        reconcile_day(&t, &pool, &sheet_seed, as_of, day, &tracker).await.expect("tick succeeds");

        let after = reconciliation_row_or_zero(&pool, day, CONSUMER_TENANT).await;
        let delta = after.delta(&before);
        assert_eq!(delta.unknown_model_rows, 10, "the raw row count reflects all ten rows");

        let guard = tracker.lock().unwrap();
        assert_eq!(
            guard.sighting_count("anthropic", &unknown_model),
            1,
            "the tracker — the actual alert/dedup mechanism — was consulted exactly ONCE for \
             this pass despite ten underlying rows"
        );
    }

    /// **Negative control 2 — the sentinel.** A `__unattributed__` row must
    /// NEVER raise an unknown-model alert; a genuinely unrecognized model in
    /// the SAME pass must.
    #[tokio::test]
    async fn sentinel_is_silent_while_a_real_unknown_model_alerts() {
        let Some(pool) = pg_test_pool("sentinel_is_silent_while_a_real_unknown_model_alerts").await else {
            return;
        };
        let account_id = seed_account(&pool).await;
        let graph_id = seed_graph(&pool, account_id).await;
        let unknown_model = format!("genuinely-unrecognized-{}", Uuid::new_v4());
        let day = Utc::now().date_naive();
        let t = consumer_tenant(pool.clone());
        let sheet_seed = crate::billing::rates::tests::seeded_for_tests();
        let as_of = Utc::now();
        let before = reconciled_baseline(&t, &pool, &sheet_seed, as_of, day).await;

        insert_llm_usage(&pool, account_id, graph_id, "journal_compile", UNATTRIBUTED_SENTINEL, 1000, 0, None).await;
        insert_llm_usage(&pool, account_id, graph_id, "extract_entities", &unknown_model, 10, 5, None).await;

        let tracker = new_tracker();
        reconcile_day(&t, &pool, &sheet_seed, as_of, day, &tracker).await.expect("tick succeeds");

        let after = reconciliation_row_or_zero(&pool, day, CONSUMER_TENANT).await;
        let delta = after.delta(&before);
        // Only the genuinely unknown row counts — the sentinel row is
        // excluded entirely, never even reaching `PriceSheet::rate`'s
        // unmatched branch from this worker's perspective.
        assert_eq!(delta.unknown_model_rows, 1, "only the non-sentinel row counts as unknown");

        let guard = tracker.lock().unwrap();
        assert_eq!(
            guard.sighting_count("anthropic", UNATTRIBUTED_SENTINEL),
            0,
            "the sentinel must never reach the alert tracker"
        );
        assert_eq!(
            guard.sighting_count("anthropic", &unknown_model),
            1,
            "a real unknown model in the same pass must alert"
        );
    }

    /// **Negative control 3 — classification.** A `*_cached` zero-token row
    /// must be counted as a cache hit, never as an anomaly
    /// (`unthreaded_rows`) and never as `unknown_model_rows` (it carries the
    /// real, known extraction model).
    #[tokio::test]
    async fn cached_zero_token_row_is_not_counted_as_an_anomaly() {
        let Some(pool) = pg_test_pool("cached_zero_token_row_is_not_counted_as_an_anomaly").await else {
            return;
        };
        let account_id = seed_account(&pool).await;
        let graph_id = seed_graph(&pool, account_id).await;
        let day = Utc::now().date_naive();
        let t = consumer_tenant(pool.clone());
        let sheet_seed = crate::billing::rates::tests::seeded_for_tests();
        let as_of = Utc::now();
        let before = reconciled_baseline(&t, &pool, &sheet_seed, as_of, day).await;

        // Real extraction model — `claude-haiku-4-5` resolves via the seeded
        // PriceSheet, so this row must not be flagged unknown either.
        insert_llm_usage(&pool, account_id, graph_id, "extract_entities_cached", "claude-haiku-4-5", 0, 0, None).await;

        let tracker = new_tracker();
        reconcile_day(&t, &pool, &sheet_seed, as_of, day, &tracker).await.expect("tick succeeds");

        let after = reconciliation_row_or_zero(&pool, day, CONSUMER_TENANT).await;
        let delta = after.delta(&before);
        assert_eq!(delta.cache_hit_rows, 1, "the cache-hit row is counted in its own bucket");
        assert_eq!(delta.unthreaded_rows, 0, "a cache-hit row must NOT be folded into the anomaly bucket");
        assert_eq!(delta.unknown_model_rows, 0, "a real, known model must not be flagged unknown");
    }
}

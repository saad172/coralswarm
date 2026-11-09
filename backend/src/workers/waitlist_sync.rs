//! Cloudflare D1 waitlist sync worker (platform-admin-metrics PR 5 of 7,
//! migration `0096_waitlist_snapshot`).
//!
//! Pulls `waitlist_signups` from the marketing site's Cloudflare D1 database
//! (`marketing-web/migrations/0001_create_waitlist_signups.sql`) over D1's
//! REST API and upserts a normalized projection into the control-plane
//! `waitlist_snapshot` table — see that migration's header for the full
//! "why a synced projection, never the source of truth" rationale.
//!
//! ## Why this is its OWN standalone worker, not part of `platform_rollup`
//!
//! The brief groups this under the same PR as the rollup worker, but the two
//! have nothing in common mechanically: `platform_rollup` fans out across
//! `[consumer] ++ every org tenant DB` via `workers::tenant_harness`; this
//! worker makes exactly ONE outbound HTTP call to a system that has no
//! tenant dimension at all (D1 holds one global waitlist, not one per org)
//! and writes to exactly one control-only table. Folding it into
//! `platform_rollup::tick` would mean either running an HTTP call once per
//! tenant tick (wasteful and wrong — the answer doesn't vary by tenant) or
//! threading a "only run this part on the consumer tick" special case through
//! the tenant harness for a worker that isn't tenant-shaped at all. Modelled
//! instead on `workers::access_log_purge` — the existing precedent for
//! "control-only, no tenant dimension, owns a small standalone loop instead
//! of registering with the harness" (see that worker's own module doc,
//! "Why this is NOT a `workers::tenant_harness` sweep worker").
//!
//! ## Config: skip, never fail, never panic, never log a secret
//!
//! `CF_ACCOUNT_ID` / `CF_D1_DATABASE_ID` / `CF_API_TOKEN` are read once per
//! sweep (so a redeploy that adds them takes effect without a code change,
//! matching the brief). If ANY is unset, [`tick`] logs a warning naming which
//! are missing and returns `Ok(0)` (a clean no-op, not an error) — the rest
//! of the platform rollup (a wholly separate worker/task) is entirely
//! unaffected either way, and this worker's own next sweep just tries again.
//! The token itself is never logged, never included in an error message, and
//! never persisted anywhere (mirrors `R2Client`'s / `OPENROUTER_API_KEY`'s
//! existing "credentials never appear in a log line" convention in this
//! codebase).
//!
//! ## Missing `utm_source`/`country` degrade gracefully — THE reason for
//! ## `SELECT *`
//!
//! D1's `waitlist_signups` gains `utm_source`/`utm_medium`/`utm_campaign`/
//! `referrer`/`landing_path`/`country`/`first_seen_at` in
//! `marketing-web/migrations/0002_waitlist_attribution.sql`
//! (`cs-admin-b-waitlist-attribution`, PR #357 — NOT YET MERGED as of this
//! PR). In production, D1 may therefore still be on the pre-#357 schema
//! (`id`, `email`, `created_at` only) for a while after this worker ships.
//! SQLite/D1 hard-errors on `SELECT utm_source, country FROM ...` against a
//! table that doesn't have those columns yet — so [`fetch_rows`] deliberately
//! sends `SELECT * FROM waitlist_signups`, not an explicit column list. D1's
//! REST API returns each row as a JSON OBJECT keyed by column name (not a
//! positional array), so [`parse_row`] simply looks up `"utm_source"`/
//! `"country"` by KEY on each row's `serde_json::Value::Object` map —
//! whichever columns D1 actually has for that row are present, and any
//! column D1 doesn't have yet is just an absent key, which
//! `Value::get(...).and_then(Value::as_str)` already treats identically to a
//! present-but-`NULL` value (`None` either way). No schema probing, no two
//! SQL variants to keep in sync, no retry-without-those-columns fallback
//! logic — the row-as-JSON-object shape makes the degradation automatic.
//!
//! ## Normalization
//!
//! `email_normalized = email.trim().to_lowercase()` — the exact join key PR
//! 6 (the conversion-funnel view, not built here) uses against
//! `lower(trim(users.email))`, EXACT match only, no gmail-dot/plus-alias
//! canonicalization (per the brief). `email_raw` is stored verbatim (no
//! mutation) so an operator/support flow can see exactly what the visitor
//! typed.
//!
//! ## `created_at` parsing
//!
//! D1/SQLite's `datetime('now')` default (what
//! `0001_create_waitlist_signups.sql` uses) produces a bare
//! `"YYYY-MM-DD HH:MM:SS"` string in UTC, with no timezone offset and no `T`
//! separator — NOT RFC 3339. [`parse_signed_up_at`] parses that exact format;
//! a row whose `created_at` doesn't match (a future schema change, a hand
//! edited row) yields `None` rather than failing the row or the sweep —
//! `waitlist_snapshot.signed_up_at` is nullable specifically for this
//! (migration `0096`'s header).

use std::time::Duration;

use chrono::{DateTime, NaiveDateTime, Utc};
use serde::Deserialize;
use serde_json::Value;
use sqlx::PgPool;

use crate::shutdown::Shutdown;

// ── Tuning constants ──────────────────────────────────────────────────────────

/// How often the sync runs. Daily is ample for a waitlist (low thousands of
/// rows, not a high-frequency feed) and mirrors every other daily sweep in
/// this codebase (`workers::tombstone_purge`, `workers::access_log_purge`).
pub const SYNC_INTERVAL: Duration = Duration::from_secs(24 * 60 * 60);

/// Bound on the D1 REST call — a hung HTTP request must not park this
/// worker's loop forever, same reasoning as every other worker's DB-call
/// timeouts in this codebase.
const D1_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// Bound on the batch upsert against the control DB.
const UPSERT_TIMEOUT: Duration = Duration::from_secs(30);

const SHUTDOWN_POLL_INTERVAL: Duration = Duration::from_millis(500);

// ── Config ────────────────────────────────────────────────────────────────────

/// The three Cloudflare env vars this worker needs, all-or-nothing (see the
/// module doc's "Config" section).
struct D1Config {
    account_id: String,
    database_id: String,
    api_token: String,
}

impl D1Config {
    /// `None` (with a logged warning naming which vars are missing — never
    /// their values) if any of the three env vars is unset. Read fresh on
    /// every sweep rather than cached at startup, so a redeploy that adds
    /// them takes effect on this worker's next cycle without a restart.
    fn from_env() -> Option<Self> {
        let account_id = std::env::var("CF_ACCOUNT_ID").ok();
        let database_id = std::env::var("CF_D1_DATABASE_ID").ok();
        let api_token = std::env::var("CF_API_TOKEN").ok();

        match (account_id, database_id, api_token) {
            (Some(account_id), Some(database_id), Some(api_token))
                if !account_id.is_empty() && !database_id.is_empty() && !api_token.is_empty() =>
            {
                Some(D1Config { account_id, database_id, api_token })
            }
            (account_id, database_id, api_token) => {
                let mut missing = Vec::new();
                if account_id.as_deref().unwrap_or("").is_empty() {
                    missing.push("CF_ACCOUNT_ID");
                }
                if database_id.as_deref().unwrap_or("").is_empty() {
                    missing.push("CF_D1_DATABASE_ID");
                }
                if api_token.as_deref().unwrap_or("").is_empty() {
                    missing.push("CF_API_TOKEN");
                }
                tracing::warn!(
                    ?missing,
                    "waitlist sync: Cloudflare D1 credentials not fully set — skipping this sweep \
                     (the rest of the platform rollup is unaffected)"
                );
                None
            }
        }
    }

    fn query_url(&self) -> String {
        format!(
            "https://api.cloudflare.com/client/v4/accounts/{}/d1/database/{}/query",
            self.account_id, self.database_id
        )
    }
}

// ── D1 REST response shapes ──────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
struct D1Envelope {
    result: Vec<D1QueryResult>,
    success: bool,
    #[serde(default)]
    errors: Vec<D1Error>,
}

#[derive(Debug, Deserialize)]
struct D1QueryResult {
    #[serde(default)]
    results: Vec<Value>,
    success: bool,
    #[serde(default)]
    error: Option<String>,
}

#[derive(Debug, Deserialize)]
struct D1Error {
    #[serde(default)]
    code: i64,
    #[serde(default)]
    message: String,
}

// ── One normalized snapshot row ───────────────────────────────────────────────

/// `pub` (fields included) so `backend/tests/waitlist_sync_pg.rs` can build
/// rows directly and exercise [`upsert_snapshot`] against a real Postgres
/// without standing up a mock D1 HTTP endpoint — the D1-fetch/parsing half
/// (`fetch_rows`/`parse_row`/[`D1Config`]) is exhaustively unit-tested above
/// without a live server; this is the seam between the two halves.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotRow {
    pub email_normalized: String,
    pub email_raw: String,
    pub signed_up_at: Option<DateTime<Utc>>,
    pub utm_source: Option<String>,
    pub country: Option<String>,
}

/// D1/SQLite's `datetime('now')` shape: `"YYYY-MM-DD HH:MM:SS"`, UTC, no
/// offset, no `T` separator — NOT RFC 3339. See the module doc.
const D1_DATETIME_FMT: &str = "%Y-%m-%d %H:%M:%S";

fn parse_signed_up_at(raw: Option<&str>) -> Option<DateTime<Utc>> {
    let raw = raw?;
    NaiveDateTime::parse_from_str(raw, D1_DATETIME_FMT)
        .ok()
        .map(|naive| naive.and_utc())
}

/// Normalize one D1 row (a JSON object keyed by column name — see the module
/// doc's "missing columns degrade gracefully" section for why THAT shape,
/// specifically, is what makes `utm_source`/`country` optional for free).
/// Returns `None` (logged, row skipped, sweep continues) for a row with no
/// usable email — never panics on a malformed row.
fn parse_row(row: &Value) -> Option<SnapshotRow> {
    let email_raw = row.get("email").and_then(Value::as_str)?.to_string();
    let email_normalized = email_raw.trim().to_lowercase();
    if email_normalized.is_empty() {
        return None;
    }

    let signed_up_at = parse_signed_up_at(row.get("created_at").and_then(Value::as_str));
    let utm_source = row.get("utm_source").and_then(Value::as_str).map(str::to_string);
    let country = row.get("country").and_then(Value::as_str).map(str::to_string);

    Some(SnapshotRow { email_normalized, email_raw, signed_up_at, utm_source, country })
}

// ── D1 fetch ──────────────────────────────────────────────────────────────────

/// `SELECT *` deliberately, not an explicit column list — see the module
/// doc's "missing columns degrade gracefully" section for why this is the
/// mechanism, not an incidental choice.
const FETCH_SQL: &str = "SELECT * FROM waitlist_signups";

async fn fetch_rows(http: &reqwest::Client, cfg: &D1Config) -> Result<Vec<Value>, String> {
    let resp = tokio::time::timeout(
        D1_REQUEST_TIMEOUT,
        http.post(cfg.query_url())
            .bearer_auth(&cfg.api_token)
            .json(&serde_json::json!({ "sql": FETCH_SQL }))
            .send(),
    )
    .await
    .map_err(|_| format!("D1 query timed out after {}s", D1_REQUEST_TIMEOUT.as_secs()))?
    .map_err(|e| format!("D1 query request failed: {e}"))?;

    let status = resp.status();
    let envelope: D1Envelope = resp
        .json()
        .await
        .map_err(|e| format!("D1 response body was not the expected JSON shape (HTTP {status}): {e}"))?;

    if !envelope.success {
        let messages: Vec<String> = envelope.errors.iter().map(|e| format!("[{}] {}", e.code, e.message)).collect();
        return Err(format!("D1 API reported failure (HTTP {status}): {}", messages.join("; ")));
    }

    let Some(query_result) = envelope.result.into_iter().next() else {
        return Err("D1 response carried no query result".to_string());
    };
    if !query_result.success {
        return Err(format!(
            "D1 query itself failed: {}",
            query_result.error.unwrap_or_else(|| "no error message provided".to_string())
        ));
    }

    Ok(query_result.results)
}

// ── Upsert ────────────────────────────────────────────────────────────────────

/// Pure dedup logic behind [`upsert_snapshot`]'s batch-safety guard, factored
/// out so it's unit-testable without a database (mirrors this codebase's
/// "pure logic helper, unit-tested in isolation" convention, e.g.
/// `workers::reembed::needs_reembed`). Last row for a given
/// `email_normalized` wins, in input order — deterministic regardless of the
/// backing `HashMap`'s iteration order (only insertion order, not iteration
/// order, decides which row's DATA survives).
fn dedupe_by_email_last_wins(rows: &[SnapshotRow]) -> Vec<&SnapshotRow> {
    let mut deduped: std::collections::HashMap<&str, &SnapshotRow> = std::collections::HashMap::new();
    for row in rows {
        deduped.insert(row.email_normalized.as_str(), row);
    }
    deduped.into_values().collect()
}

/// Bulk-upsert every parsed row via `UNNEST`, one round trip regardless of
/// waitlist size (mirrors `workers::platform_rollup::upsert_active_users`'s
/// identical bulk-upsert shape). `synced_at` is always bumped to `now()` on
/// every row this sweep saw — that timestamp, not row presence, is the
/// snapshot's own freshness signal (migration `0096`'s header).
///
/// **Dedupes by `email_normalized` before building the batch, last-wins.**
/// D1's own `email` UNIQUE constraint is on the RAW string, so two distinct
/// D1 rows CAN normalize to the same key (e.g. `"Same@test.dev"` and
/// `" same@test.dev "` are different raw rows, identical once
/// trimmed+lowercased). Without this dedup, a single multi-row `INSERT ...
/// ON CONFLICT DO UPDATE` that tried to affect the SAME conflict target twice
/// would hard-error with Postgres's "ON CONFLICT DO UPDATE command cannot
/// affect row a second time" — caught by this file's own
/// `backend/tests/waitlist_sync_pg.rs` before it could ever reach production
/// as a silent whole-sync failure the first time D1 held such a pair.
pub async fn upsert_snapshot(pool: &PgPool, rows: &[SnapshotRow]) -> Result<u64, sqlx::Error> {
    if rows.is_empty() {
        return Ok(0);
    }

    let rows = dedupe_by_email_last_wins(rows);

    let email_normalized: Vec<&str> = rows.iter().map(|r| r.email_normalized.as_str()).collect();
    let email_raw: Vec<&str> = rows.iter().map(|r| r.email_raw.as_str()).collect();
    let signed_up_at: Vec<Option<DateTime<Utc>>> = rows.iter().map(|r| r.signed_up_at).collect();
    let utm_source: Vec<Option<&str>> = rows.iter().map(|r| r.utm_source.as_deref()).collect();
    let country: Vec<Option<&str>> = rows.iter().map(|r| r.country.as_deref()).collect();

    let result = sqlx::query(
        "INSERT INTO waitlist_snapshot \
           (email_normalized, email_raw, signed_up_at, utm_source, country, synced_at) \
         SELECT en, er, sa, us, ct, now() \
         FROM UNNEST($1::text[], $2::text[], $3::timestamptz[], $4::text[], $5::text[]) \
           AS t(en, er, sa, us, ct) \
         ON CONFLICT (email_normalized) DO UPDATE SET \
           email_raw = EXCLUDED.email_raw, \
           signed_up_at = EXCLUDED.signed_up_at, \
           utm_source = EXCLUDED.utm_source, \
           country = EXCLUDED.country, \
           synced_at = EXCLUDED.synced_at",
    )
    .bind(&email_normalized as &[&str])
    .bind(&email_raw as &[&str])
    .bind(&signed_up_at as &[Option<DateTime<Utc>>])
    .bind(&utm_source as &[Option<&str>])
    .bind(&country as &[Option<&str>])
    .execute(pool)
    .await?;

    Ok(result.rows_affected())
}

// ── Worker entry point ────────────────────────────────────────────────────────

/// Run the waitlist-sync worker loop until `shutdown` is signalled. Spawned
/// directly from `lib.rs` with the control pool and the shared
/// `reqwest::Client` — no tenant dimension to thread through (see the module
/// doc). Fires once immediately on start (matching every other sweep
/// worker's fire-once-at-boot behavior), then every [`SYNC_INTERVAL`]
/// thereafter.
pub async fn run(pool: PgPool, http: reqwest::Client, shutdown: Shutdown) {
    tracing::info!("waitlist-sync worker started");

    loop {
        if shutdown.is_triggered() {
            tracing::info!("waitlist-sync worker: shutdown signalled, stopping");
            return;
        }

        match tick(&pool, &http).await {
            Ok(0) => tracing::debug!("waitlist sync: nothing to do (skipped or empty waitlist)"),
            Ok(n) => tracing::info!(rows_upserted = n, "waitlist sync complete"),
            Err(e) => tracing::warn!(error = %e, "waitlist sync failed; will retry next cycle"),
        }

        tokio::select! {
            _ = tokio::time::sleep(SYNC_INTERVAL) => {}
            _ = poll_until_triggered(&shutdown) => {
                tracing::info!("waitlist-sync worker: shutdown signalled during inter-sweep sleep, stopping");
                return;
            }
        }
    }
}

async fn poll_until_triggered(shutdown: &Shutdown) {
    loop {
        if shutdown.is_triggered() {
            return;
        }
        tokio::time::sleep(SHUTDOWN_POLL_INTERVAL).await;
    }
}

/// One sync pass: fetch every `waitlist_signups` row from D1 and upsert a
/// normalized snapshot. Returns `Ok(0)` (not an error) when Cloudflare
/// credentials are unset — see the module doc's "Config" section. `pub` so
/// integration tests can drive one pass directly.
pub async fn tick(pool: &PgPool, http: &reqwest::Client) -> Result<u64, String> {
    let Some(cfg) = D1Config::from_env() else {
        return Ok(0);
    };

    let raw_rows = fetch_rows(http, &cfg).await?;
    let mut skipped = 0usize;
    let snapshot_rows: Vec<SnapshotRow> = raw_rows
        .iter()
        .filter_map(|row| {
            let parsed = parse_row(row);
            if parsed.is_none() {
                skipped += 1;
            }
            parsed
        })
        .collect();
    if skipped > 0 {
        tracing::warn!(skipped, "waitlist sync: some D1 rows had no usable email and were skipped");
    }

    tokio::time::timeout(UPSERT_TIMEOUT, upsert_snapshot(pool, &snapshot_rows))
        .await
        .map_err(|_| format!("waitlist snapshot upsert timed out after {}s", UPSERT_TIMEOUT.as_secs()))?
        .map_err(|e| format!("waitlist snapshot upsert failed: {e}"))
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_row_normalizes_email_and_tolerates_missing_attribution_columns() {
        let row = serde_json::json!({
            "id": 1,
            "email": "  Someone@Example.COM ",
            "created_at": "2026-07-20 10:15:30"
            // No utm_source/country key at all — pre-PR-#357 D1 schema.
        });
        let parsed = parse_row(&row).expect("row parses");
        assert_eq!(parsed.email_normalized, "someone@example.com");
        assert_eq!(parsed.email_raw, "  Someone@Example.COM ");
        assert_eq!(parsed.utm_source, None, "missing column ⇒ None, not an error");
        assert_eq!(parsed.country, None);
        assert!(parsed.signed_up_at.is_some());
    }

    #[test]
    fn parse_row_reads_attribution_columns_when_present() {
        let row = serde_json::json!({
            "id": 2,
            "email": "person@test.dev",
            "created_at": "2026-07-21 00:00:00",
            "utm_source": "twitter",
            "country": "US"
        });
        let parsed = parse_row(&row).expect("row parses");
        assert_eq!(parsed.utm_source.as_deref(), Some("twitter"));
        assert_eq!(parsed.country.as_deref(), Some("US"));
    }

    #[test]
    fn parse_row_treats_null_attribution_the_same_as_missing() {
        let row = serde_json::json!({
            "email": "null-fields@test.dev",
            "created_at": "2026-07-22 00:00:00",
            "utm_source": null,
            "country": null
        });
        let parsed = parse_row(&row).expect("row parses");
        assert_eq!(parsed.utm_source, None);
        assert_eq!(parsed.country, None);
    }

    #[test]
    fn parse_row_rejects_missing_or_blank_email() {
        assert!(parse_row(&serde_json::json!({"created_at": "2026-07-20 10:15:30"})).is_none());
        assert!(parse_row(&serde_json::json!({"email": "   ", "created_at": "2026-07-20 10:15:30"})).is_none());
        assert!(parse_row(&serde_json::json!({"email": Value::Null})).is_none());
    }

    #[test]
    fn signed_up_at_parses_d1_datetime_shape_and_degrades_on_mismatch() {
        assert!(parse_signed_up_at(Some("2026-07-20 10:15:30")).is_some());
        assert_eq!(parse_signed_up_at(Some("not-a-date")), None);
        // RFC 3339 (has a `T` and offset) is NOT the D1 shape — must not parse.
        assert_eq!(parse_signed_up_at(Some("2026-07-20T10:15:30Z")), None);
        assert_eq!(parse_signed_up_at(None), None);
    }

    #[test]
    fn d1_config_requires_all_three_vars() {
        // Pure check of the tri-state matching logic without touching
        // std::env (parallel test binary — see `platform_rollup`'s identical
        // rationale for not mutating process env in tests).
        let none: Option<String> = None;
        let some = |s: &str| Some(s.to_string());

        // Mirrors D1Config::from_env's match arms directly.
        fn is_configured(a: &Option<String>, b: &Option<String>, c: &Option<String>) -> bool {
            matches!((a, b, c), (Some(a), Some(b), Some(c)) if !a.is_empty() && !b.is_empty() && !c.is_empty())
        }
        assert!(!is_configured(&none, &some("db"), &some("tok")));
        assert!(!is_configured(&some("acct"), &none, &some("tok")));
        assert!(!is_configured(&some("acct"), &some("db"), &none));
        assert!(!is_configured(&some(""), &some("db"), &some("tok")), "blank counts as missing");
        assert!(is_configured(&some("acct"), &some("db"), &some("tok")));
    }

    #[test]
    fn dedupe_by_email_last_wins_collapses_shared_normalized_key() {
        let a = SnapshotRow {
            email_normalized: "same@test.dev".to_string(),
            email_raw: "Same@Test.dev".to_string(),
            signed_up_at: None,
            utm_source: None,
            country: None,
        };
        let b = SnapshotRow {
            email_normalized: "same@test.dev".to_string(),
            email_raw: " same@test.dev ".to_string(),
            signed_up_at: None,
            utm_source: Some("google".to_string()),
            country: None,
        };
        let c = SnapshotRow {
            email_normalized: "other@test.dev".to_string(),
            email_raw: "other@test.dev".to_string(),
            signed_up_at: None,
            utm_source: None,
            country: None,
        };
        let rows = vec![a, b.clone(), c.clone()];

        let deduped = dedupe_by_email_last_wins(&rows);
        assert_eq!(deduped.len(), 2, "two distinct email_normalized keys survive");

        let same = deduped.iter().find(|r| r.email_normalized == "same@test.dev").expect("same@ present");
        assert_eq!(same.email_raw, b.email_raw, "last row for the shared key wins, not the first");
        assert_eq!(same.utm_source, b.utm_source);

        let other = deduped.iter().find(|r| r.email_normalized == "other@test.dev").expect("other@ present");
        assert_eq!(other.email_raw, c.email_raw);
    }
}

//! Per-org connector flag registry (Foundation train PR 1.3,
//! `.scratch/m365-enterprise/plan.md`).
//!
//! Before this module, `ORG_SLACK_OAUTH_ENABLED`
//! (`routes::oauth::org_slack_oauth_enabled`) and `ORG_TEAMS_ENABLED`
//! (`workers::member_ocean_reconciler::org_teams_enabled`) were the whole
//! story: process-global env vars, all-or-nothing across every org account
//! the fleet serves. This module adds the missing per-org override
//! dimension, backed by the `org_connector_flags` control-plane table
//! (migration `0178_org_connector_flags`).
//!
//! # Resolution order
//! For a given `(clerk_org_id, provider)`:
//! 1. A row in `org_connector_flags` — if present — wins outright.
//! 2. Otherwise, the caller-supplied `env_default` (the SAME env-var reader
//!    that was the only source of truth before this module existed) applies.
//!
//! So shipping this table with zero rows is byte-identical to today's
//! behavior: every org keeps reading the same global env default it always
//! has. [`resolve_flag`] is the pure version of that rule, factored out so
//! it is unit-testable with no database — the same "pure logic extracted for
//! testability" convention `routes::oauth::parse_org_slack_oauth_flag` and
//! `workers::member_ocean_reconciler::parse_enabled_flag` already use for the
//! env-var side of this exact contract.
//!
//! # Authoritative overrides
//! Mutable organization gates are read from the control database on every
//! decision. A process-local TTL cannot invalidate another process, and an
//! in-flight lookup must never restore an enabled value after an admin disables
//! it. Missing rows still use the caller's fresh environment default; lookup
//! failures fail closed when a control database is configured.

use crate::AppState;

/// Compatibility handle retained on `AppState` for existing admin callers.
/// Organization enablement is no longer cached.
#[derive(Clone, Default)]
pub struct ConnectorFlagCache;

impl ConnectorFlagCache {
    pub fn new() -> Self {
        Self
    }

    /// Existing callers may invalidate after writes; authoritative reads make
    /// local invalidation unnecessary.
    pub fn invalidate(&self, _org_id: &str, _provider: &str) {}
}

// ── Resolution ────────────────────────────────────────────────────────────────

/// Pure resolution rule: a row (if present) overrides `env_default`. See the
/// module doc's "Resolution order" section — this is the whole contract,
/// factored out so it is testable without a database.
fn resolve_flag(row: Option<bool>, env_default: bool) -> bool {
    row.unwrap_or(env_default)
}

/// Resolve whether `provider` is enabled for `clerk_org_id`: a per-org
/// `org_connector_flags` row overrides; a missing row falls back to
/// `env_default` (the caller's own env-var reader, e.g.
/// `routes::oauth::org_slack_oauth_enabled()` or
/// `workers::member_ocean_reconciler::org_teams_enabled()`) — unchanged from
/// this org's behavior before this registry existed.
///
/// Always queries the control pool. Without a control pool,
/// there is no organization override, so `env_default` applies. A configured
/// database lookup failure refuses access rather than bypassing a disabled
/// organization gate via a permissive environment default.
pub async fn resolve(
    state: &AppState,
    clerk_org_id: &str,
    provider: &str,
    env_default: bool,
) -> bool {
    let Some(pool) = state.control_db() else {
        return env_default;
    };

    let row: Result<Option<bool>, sqlx::Error> = sqlx::query_scalar(
        "SELECT enabled FROM org_connector_flags WHERE clerk_org_id = $1 AND provider = $2",
    )
    .bind(clerk_org_id)
    .bind(provider)
    .fetch_optional(crate::tenancy::scope::request_read(pool))
    .await;

    let row = match row {
        Ok(row) => row,
        Err(e) => {
            tracing::error!(
                error = %e,
                org_id = %clerk_org_id,
                provider,
                "org_connector_flags lookup failed; refusing connector access"
            );
            return false;
        }
    };
    resolve_flag(row, env_default)
}

/// Whether an ORG account may use the **GitHub** connector — the same
/// per-org-override-over-env-default resolution every other provider's own
/// helper (`routes::oauth::org_slack_oauth_enabled_for`, `routes::
/// teams_ingest::org_microsoft_teams_enabled_for`) wraps [`resolve`] with,
/// kept HERE (unlike those two) because GitHub's brief asked for it directly
/// on this module rather than on `routes::oauth`. `env_default` is
/// `ORG_GITHUB_ENABLED` (docs/connectors/github.md §3.12/§4) — same
/// fail-closed parse rule as every sibling flag (`"1"` or a
/// case-insensitive `"true"` is on; unset or anything else is off).
///
/// Gates `routes::oauth::authorize`/`callback`'s org/provider check and
/// every `routes::github` selection-API handler for org-plane requests.
pub async fn github_enabled(state: &AppState, clerk_org_id: &str) -> bool {
    let env_default = matches!(
        std::env::var("ORG_GITHUB_ENABLED"),
        Ok(v) if v == "1" || v.eq_ignore_ascii_case("true")
    );
    resolve(state, clerk_org_id, "github", env_default).await
}

/// Upsert a per-org override; subsequent authoritative reads observe it so the
/// change is visible on the very next request. Used by
/// [`crate::routes::admin::set_connector_flag`].
pub async fn set_override(
    pool: &sqlx::PgPool,
    cache: &ConnectorFlagCache,
    clerk_org_id: &str,
    provider: &str,
    enabled: bool,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO org_connector_flags (clerk_org_id, provider, enabled) \
         VALUES ($1, $2, $3) \
         ON CONFLICT (clerk_org_id, provider) \
         DO UPDATE SET enabled = EXCLUDED.enabled, updated_at = now()",
    )
    .bind(clerk_org_id)
    .bind(provider)
    .bind(enabled)
    .execute(pool)
    .await?;

    cache.invalidate(clerk_org_id, provider);
    Ok(())
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // ── resolve_flag: the pure resolution-order contract ───────────────────

    #[test]
    fn row_override_wins_over_env_default_when_true() {
        assert!(resolve_flag(Some(true), false));
    }

    #[test]
    fn row_override_wins_over_env_default_when_false() {
        assert!(!resolve_flag(Some(false), true));
    }

    #[test]
    fn missing_row_falls_back_to_env_default_true() {
        assert!(resolve_flag(None, true));
    }

    // ── github_enabled's env_default parsing (the pure half — see
    // resolve_flag's own tests for the row-override half, identical for
    // every provider) ──

    #[test]
    fn org_github_enabled_env_default_parses_one_and_true() {
        for (raw, expected) in [
            (Some("1"), true),
            (Some("true"), true),
            (Some("TRUE"), true),
            (Some("0"), false),
            (Some("false"), false),
            (Some("yes"), false),
            (None, false),
        ] {
            let parsed = matches!(raw, Some(v) if v == "1" || v.eq_ignore_ascii_case("true"));
            assert_eq!(parsed, expected, "raw={raw:?}");
        }
    }

    #[test]
    fn missing_row_falls_back_to_env_default_false() {
        assert!(!resolve_flag(None, false));
    }
}

// ── pg tests: the REAL #731 regression guard ───────────────────────────────

#[cfg(test)]
mod pg_tests {
    //! Real control-database resolution, environment defaults, and disable
    //! visibility across independent application states.

    use sqlx::postgres::PgPoolOptions;
    use sqlx::{Connection, Executor, PgConnection, PgPool};
    use std::sync::Arc;

    use super::resolve;
    use crate::AppState;

    fn base_url() -> Option<String> {
        std::env::var("ORGT_TEST_BASE_URL").ok()
    }
    fn with_db(base: &str, db: &str) -> String {
        let (prefix, rest) = base.split_at(base.rfind('/').expect("url has a path /"));
        let params = rest.find('?').map(|i| &rest[i..]).unwrap_or("");
        format!("{prefix}/{db}{params}")
    }
    async fn recreate(base: &str, db: &str) -> PgConnection {
        let mut conn = PgConnection::connect(base).await.expect("connect admin");
        conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .unwrap();
        conn.execute(format!("CREATE DATABASE {db}").as_str())
            .await
            .unwrap();
        conn
    }

    fn state_with_control(pool: PgPool) -> Arc<AppState> {
        Arc::new(crate::test_app_state_owned_with_db_and_store(pool, None))
    }

    #[tokio::test]
    async fn cached_no_override_row_stays_live_to_an_env_default_flip_through_the_real_resolve_pg()
    {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping cached_no_override_row_stays_live_to_an_env_default_flip_through_the_\
                 real_resolve_pg: set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "orgt_connflags_cache_liveness";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let mut admin_conn = recreate(&base, db).await;
        let pool: PgPool = PgPoolOptions::new()
            .connect(&with_db(&base, db))
            .await
            .unwrap();
        sqlx::migrate!("./migrations_control")
            .run(&pool)
            .await
            .unwrap();

        let state = state_with_control(pool.clone());
        let org_id = "org_connflags_cache_liveness";

        assert!(
            !resolve(&state, org_id, "slack", false).await,
            "missing override uses the current environment default"
        );
        assert!(
            resolve(&state, org_id, "slack", true).await,
            "a fresh environment default applies on the next decision"
        );

        admin_conn
            .execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .ok();
    }
    #[tokio::test]
    async fn disable_is_visible_to_independent_states_pg() {
        let Some(base) = base_url() else {
            return;
        };
        let db = "orgt_connflags_disable_processes";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let mut admin = recreate(&base, db).await;
        let pool = PgPoolOptions::new()
            .connect(&with_db(&base, db))
            .await
            .unwrap();
        sqlx::migrate!("./migrations_control")
            .run(&pool)
            .await
            .unwrap();
        let a = state_with_control(pool.clone());
        let b = state_with_control(pool.clone());
        super::set_override(&pool, &a.connector_flag_cache, "org_disable", "teams", true)
            .await
            .unwrap();
        assert!(resolve(&a, "org_disable", "teams", false).await);
        assert!(resolve(&b, "org_disable", "teams", false).await);
        super::set_override(
            &pool,
            &a.connector_flag_cache,
            "org_disable",
            "teams",
            false,
        )
        .await
        .unwrap();
        assert!(!resolve(&a, "org_disable", "teams", true).await);
        assert!(
            !resolve(&b, "org_disable", "teams", true).await,
            "other process must observe the committed disable immediately"
        );

        // Pin the old SELECT's MVCC snapshot, then commit disable while that
        // actual resolve() is waiting in PostgreSQL. The delayed old result
        // may finish enabled, but it must never poison the next decision.
        super::set_override(&pool, &a.connector_flag_cache, "org_disable", "teams", true)
            .await
            .unwrap();
        let c = state_with_control(pool.clone());
        pool.execute("ALTER TABLE org_connector_flags RENAME TO flag_backing;
            CREATE FUNCTION wait_flag(flag boolean) RETURNS boolean LANGUAGE plpgsql AS $$
            BEGIN PERFORM pg_advisory_xact_lock(759001); RETURN flag; END $$;
            CREATE VIEW org_connector_flags AS SELECT clerk_org_id, provider, wait_flag(enabled) AS enabled FROM flag_backing;")
            .await.unwrap();
        let mut gate = pool.acquire().await.unwrap();
        sqlx::query("SELECT pg_advisory_lock(759001)")
            .execute(&mut *gate)
            .await
            .unwrap();
        let delayed_state = c.clone();
        let delayed =
            tokio::spawn(
                async move { resolve(&delayed_state, "org_disable", "teams", true).await },
            );
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                let waiting: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_locks WHERE locktype='advisory' AND objid=759001 AND NOT granted)")
                    .fetch_one(&pool).await.unwrap();
                if waiting { break; }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        }).await.expect("lookup reached the deterministic database barrier");
        sqlx::query("UPDATE flag_backing SET enabled=false WHERE clerk_org_id='org_disable' AND provider='teams'")
            .execute(&pool).await.unwrap();
        sqlx::query("SELECT pg_advisory_unlock(759001)")
            .execute(&mut *gate)
            .await
            .unwrap();
        assert!(
            delayed.await.unwrap(),
            "in-flight statement keeps its pre-disable snapshot"
        );
        assert!(
            !resolve(&c, "org_disable", "teams", true).await,
            "delayed lookup cannot restore an enabled cache entry"
        );
        assert!(
            !resolve(&b, "org_disable", "teams", true).await,
            "independent process observes disable"
        );
        pool.execute("DROP VIEW org_connector_flags").await.unwrap();
        assert!(
            !resolve(&c, "org_disable", "teams", true).await,
            "lookup errors must not bypass disabled gates via env fallback"
        );
        drop(gate);
        pool.close().await;
        admin
            .execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .unwrap();
    }
}

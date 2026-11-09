//! Live Postgres tests for `connector_installs` (migration
//! `0121_connector_installs`) — the control-plane routing index
//! `docs/plans/org-connectors-and-channel-ingest.md` §9c describes.
//!
//! Named `connector_installs_pg.rs` (matches `backend/tests/*_pg.rs`) and
//! wired into `.github/workflows/ci.yml`'s existing `rls-probe-harness` job
//! (added to its `--no-run` build-binaries step and given its own `run:`
//! step, right after `bootstrap_scope_pg` — no new job, same already-running
//! Postgres service, same disk/profile budget that job already pays for).
//! `scripts/check_pg_suite_coverage.sh` exists precisely so a suite like this
//! one — proving the core claims of the PR (multi-row resolution, disconnect
//! maintenance) — cannot silently sit unwired: unwired, it would still
//! compile and "pass" under a plain `cargo test` (its `setup()`-equivalent
//! env-var check just skips every test and returns `Ok(())`), a green check
//! asserting nothing. Do not rename this file to dodge that glob — an
//! earlier draft did exactly that (as `connector_installs_pg_e2e.rs`,
//! mimicking `mcp_pg_e2e.rs`'s naming) and was corrected in review:
//! `mcp_pg_e2e.rs` is precedent for a naming CONVENTION, not a licence to
//! leave a suite uncovered by the gate that exists to catch that.
//!
//! Gated on `ORGT_TEST_BASE_URL` (a Postgres with `CREATE DATABASE` rights)
//! — same scratch-DB-per-test pattern as `bootstrap_scope_pg.rs`, whose
//! multi-row-resolver coverage (`resolve_connector_install_account_ids`)
//! this file deliberately does NOT duplicate — see that file instead for the
//! "two accounts, one workspace, both returned" proof (it also gained a new
//! test for exactly this migration, so it required no separate CI wiring).
//!
//! This file covers the two things `bootstrap_scope_pg.rs` does not:
//!
//!   1. **The backfill.** `record_connector_install`/the legacy
//!      `routes/slack.rs::oauth_callback` write path are `pub(crate)` (an
//!      external test crate like this one cannot call them — see
//!      `mcp_pg_e2e.rs`'s own doc comment on the same constraint for a
//!      different module), so this drives the underlying SQL directly, the
//!      same way every other `*_pg`-style suite in this repo does. The
//!      backfill statement itself is extracted VERBATIM from the migration
//!      file at test time (between its `BACKFILL BEGIN`/`BACKFILL END`
//!      markers) and re-executed against freshly-seeded `oauth_tokens` /
//!      `slack_integrations` rows — so this test asserts the literal
//!      production SQL text, not a hand-copied approximation that could
//!      silently drift from it. (The migration's own automatic run, which
//!      happens once at `sqlx::migrate!` time against tables that are still
//!      empty then, is exercised too — it must not error over zero rows.)
//!
//!   2. **Disconnect maintenance.** `routes/oauth.rs::delete_connection` and
//!      `routes/slack.rs::delete_integration` each carry a guarded DELETE
//!      against `connector_installs` (see those two functions' own doc
//!      comments for the full asymmetry). Driving the real Axum handlers
//!      would need a full `Tenant`/`UserContext` extraction harness for a
//!      question that is really about SQL correctness, so this instead
//!      re-executes the SAME guarded DELETE statements the handlers run,
//!      hand-copied and cross-referenced by file:function — the same
//!      "pattern-copied from X" discipline `atom_title_backfill_pg.rs` and
//!      others in this suite already use for their own seed helpers. A
//!      change to either handler's DELETE shape that isn't mirrored here
//!      will not be caught by `cargo build` (only by re-reading both), so
//!      the comment at each guarded DELETE below says so explicitly.
//!
//! Run with:
//!   ORGT_TEST_BASE_URL=postgres://postgres:postgres@localhost:5432/postgres \
//!     cargo test -p coralswarm-backend --test connector_installs_pg -- --test-threads=1

use sqlx::postgres::PgPoolOptions;
use sqlx::{Connection, Executor, PgConnection, PgPool};
use uuid::Uuid;

fn base_url() -> Option<String> {
    std::env::var("ORGT_TEST_BASE_URL").ok()
}
fn with_db(base: &str, db: &str) -> String {
    let (prefix, rest) = base.split_at(base.rfind('/').expect("url has a path /"));
    let params = rest.find('?').map(|i| &rest[i..]).unwrap_or("");
    format!("{prefix}/{db}{params}")
}
async fn admin(base: &str) -> PgConnection {
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

/// Read `backend/migrations_control/0121_connector_installs.sql` and return
/// the text strictly between its `-- BACKFILL BEGIN` / `-- BACKFILL END`
/// markers — the exact statements the migration runs, verbatim.
fn extract_backfill_sql() -> String {
    let path = concat!(env!("CARGO_MANIFEST_DIR"), "/migrations_control/0121_connector_installs.sql");
    let text = std::fs::read_to_string(path)
        .unwrap_or_else(|e| panic!("read {path}: {e}"));
    let start = text
        .find("-- BACKFILL BEGIN")
        .expect("migration must contain a '-- BACKFILL BEGIN' marker");
    let end = text
        .find("-- BACKFILL END")
        .expect("migration must contain a '-- BACKFILL END' marker");
    assert!(end > start, "BACKFILL END must come after BACKFILL BEGIN");
    text[start..end].to_string()
}

async fn seed_user_and_account(pool: &PgPool, email: &str, clerk_org_id: Option<&str>) -> Uuid {
    let user_id: Uuid = sqlx::query_scalar("INSERT INTO users(email) VALUES ($1) RETURNING id")
        .bind(email)
        .fetch_one(pool)
        .await
        .expect("insert user");
    sqlx::query_scalar(
        "INSERT INTO accounts(owner_user_id, clerk_org_id) VALUES ($1, $2) RETURNING id",
    )
    .bind(user_id)
    .bind(clerk_org_id)
    .fetch_one(pool)
    .await
    .expect("insert account")
}

// ── 1. Backfill ──────────────────────────────────────────────────────────────

#[tokio::test]
async fn backfill_derives_rows_from_oauth_tokens_and_slack_integrations() {
    let Some(base) = base_url() else {
        eprintln!(
            "skipping backfill_derives_rows_from_oauth_tokens_and_slack_integrations: \
             set ORGT_TEST_BASE_URL"
        );
        return;
    };

    let db = "orgt_connector_installs_backfill";
    let _cleanup_db = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
    let mut admin_conn = admin(&base).await;
    recreate(&mut admin_conn, db).await;
    let db_url = with_db(&base, db);

    let pool: PgPool = PgPoolOptions::new().connect(&db_url).await.expect("connect");
    sqlx::migrate!("./migrations_control")
        .run(&pool)
        .await
        .expect("apply control tree incl. 0121_connector_installs — the migration's OWN backfill \
                 run (over empty oauth_tokens/slack_integrations at this point) must not error");

    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM connector_installs")
            .fetch_one(&pool)
            .await
            .unwrap(),
        0,
        "the migration's own backfill ran over empty source tables — must insert nothing"
    );

    // ── Fixture: modern path (oauth_tokens), legacy path (slack_integrations
    // only), and the multi-tenant case (two accounts, one workspace, via
    // oauth_tokens). ─────────────────────────────────────────────────────────
    let account_modern = seed_user_and_account(&pool, "modern@x.co", None).await;
    let account_legacy = seed_user_and_account(&pool, "legacy@x.co", None).await;
    let account_org = seed_user_and_account(&pool, "org-member@x.co", Some("org_abc123")).await;

    sqlx::query(
        "INSERT INTO oauth_tokens \
            (account_id, provider, scope, access_token, provider_user_id, provider_workspace_id) \
         VALUES ($1, 'slack', 'chat:write', 'ciphertext', 'U-MODERN', 'T-SHARED')",
    )
    .bind(account_modern)
    .execute(&pool)
    .await
    .expect("seed oauth_tokens row");

    // Same workspace, a DIFFERENT account — the multi-tenant case.
    sqlx::query(
        "INSERT INTO oauth_tokens \
            (account_id, provider, scope, access_token, provider_user_id, provider_workspace_id) \
         VALUES ($1, 'slack', 'chat:write', 'ciphertext', 'U-ORG', 'T-SHARED')",
    )
    .bind(account_org)
    .execute(&pool)
    .await
    .expect("seed second oauth_tokens row for the same workspace");

    // A provider_workspace_id-less oauth_tokens row (e.g. a per-user Google
    // connection with no workspace concept) must be SKIPPED, not backfilled
    // as a broken NOT-NULL-violating row.
    sqlx::query(
        "INSERT INTO oauth_tokens (account_id, provider, scope, access_token) \
         VALUES ($1, 'google', 'calendar.readonly', 'ciphertext')",
    )
    .bind(account_modern)
    .execute(&pool)
    .await
    .expect("seed workspace-less oauth_tokens row");

    // Legacy-only install: a slack_integrations row with NO oauth_tokens
    // counterpart at all — the exact gap 0086's own header documents.
    sqlx::query(
        "INSERT INTO slack_integrations (account_id, workspace_id, workspace_name, bot_token) \
         VALUES ($1, 'T-LEGACY-ONLY', 'Legacy Co', 'xoxb-legacy')",
    )
    .bind(account_legacy)
    .execute(&pool)
    .await
    .expect("seed slack_integrations row");

    // Re-execute the EXACT backfill SQL extracted from the migration file.
    let backfill_sql = extract_backfill_sql();
    pool.execute(sqlx::raw_sql(&backfill_sql))
        .await
        .expect("re-executing the extracted backfill SQL must succeed");

    // ── Assertions ───────────────────────────────────────────────────────────
    let rows: Vec<(String, String, Option<String>, Option<String>, Uuid)> = sqlx::query_as(
        "SELECT provider, external_workspace_id, external_user_id, clerk_org_id, account_id \
         FROM connector_installs ORDER BY provider, external_workspace_id, account_id",
    )
    .fetch_all(&pool)
    .await
    .unwrap();

    assert_eq!(
        rows.len(),
        3,
        "expected exactly 3 backfilled rows (2 slack/T-SHARED installers + 1 legacy-only), got \
         {rows:?}"
    );

    let shared: Vec<_> = rows
        .iter()
        .filter(|(p, w, ..)| p == "slack" && w == "T-SHARED")
        .collect();
    assert_eq!(
        shared.len(),
        2,
        "both accounts that installed T-SHARED via oauth_tokens must be backfilled — the \
         multi-tenant case, got {rows:?}"
    );
    assert!(
        shared.iter().any(|(_, _, u, _, a)| u.as_deref() == Some("U-MODERN") && *a == account_modern),
        "the modern-path install must carry its provider_user_id, got {shared:?}"
    );
    assert!(
        shared
            .iter()
            .any(|(_, _, u, org, a)| u.as_deref() == Some("U-ORG") && org.as_deref() == Some("org_abc123") && *a == account_org),
        "the org account's install must carry both provider_user_id and clerk_org_id, got {shared:?}"
    );

    let legacy_row = rows
        .iter()
        .find(|(p, w, ..)| p == "slack" && w == "T-LEGACY-ONLY")
        .expect("the slack_integrations-only workspace must be backfilled");
    assert_eq!(legacy_row.4, account_legacy);
    assert_eq!(
        legacy_row.2, None,
        "slack_integrations carries no provider_user_id — external_user_id must be NULL, not a \
         fabricated value"
    );

    assert!(
        !rows.iter().any(|(p, ..)| p == "google"),
        "the workspace-less google oauth_tokens row must never appear — got {rows:?}"
    );

    // ── Idempotency: re-running the exact same SQL a second time must not
    // error and must not create duplicates. ────────────────────────────────
    pool.execute(sqlx::raw_sql(&backfill_sql))
        .await
        .expect("re-running the backfill SQL a second time must be a no-op, not an error");
    let count_after_rerun: i64 = sqlx::query_scalar("SELECT count(*) FROM connector_installs")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count_after_rerun, 3, "re-running the backfill must not duplicate rows");

    pool.close().await;
    admin_conn
        .execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
        .await
        .ok();
}

// ── 2. Disconnect maintenance ────────────────────────────────────────────────

/// Mirrors `routes/oauth.rs::delete_connection`'s guarded DELETE
/// (SAME text — keep in sync by hand; see this file's own header). Deletes
/// `connector_installs` rows for the given (account_id, provider,
/// workspace_ids) UNLESS a `slack_integrations` row still backs the same
/// workspace.
async fn run_oauth_delete_connection_install_guard(
    pool: &PgPool,
    account_id: Uuid,
    provider: &str,
    workspace_ids: &[String],
) {
    sqlx::query(
        "DELETE FROM connector_installs ci \
         WHERE ci.account_id = $1 AND ci.provider = $2 \
           AND ci.external_workspace_id = ANY($3) \
           AND ($2 <> 'slack' OR NOT EXISTS ( \
                 SELECT 1 FROM slack_integrations si \
                 WHERE si.account_id = $1 AND si.workspace_id = ci.external_workspace_id \
               ))",
    )
    .bind(account_id)
    .bind(provider)
    .bind(workspace_ids)
    .execute(pool)
    .await
    .expect("connector_installs delete guard (oauth.rs shape) must succeed");
}

/// Mirrors `routes/slack.rs::delete_integration`'s guarded DELETE (SAME
/// text — keep in sync by hand; see this file's own header). Deletes the
/// `connector_installs` row for (account_id, 'slack', workspace_id) UNLESS
/// an `oauth_tokens` row still backs the same workspace.
async fn run_slack_delete_integration_install_guard(pool: &PgPool, account_id: Uuid, workspace_id: &str) {
    sqlx::query(
        "DELETE FROM connector_installs \
         WHERE account_id = $1 AND provider = 'slack' AND external_workspace_id = $2 \
           AND NOT EXISTS ( \
                 SELECT 1 FROM oauth_tokens ot \
                 WHERE ot.account_id = $1 AND ot.provider = 'slack' \
                   AND ot.provider_workspace_id = $2 \
               )",
    )
    .bind(account_id)
    .bind(workspace_id)
    .execute(pool)
    .await
    .expect("connector_installs delete guard (slack.rs shape) must succeed");
}

#[tokio::test]
async fn disconnect_maintenance_respects_the_other_backend() {
    let Some(base) = base_url() else {
        eprintln!("skipping disconnect_maintenance_respects_the_other_backend: set ORGT_TEST_BASE_URL");
        return;
    };

    let db = "orgt_connector_installs_disconnect";
    let _cleanup_db = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
    let mut admin_conn = admin(&base).await;
    recreate(&mut admin_conn, db).await;
    let db_url = with_db(&base, db);

    let pool: PgPool = PgPoolOptions::new().connect(&db_url).await.expect("connect");
    sqlx::migrate!("./migrations_control")
        .run(&pool)
        .await
        .expect("apply control tree");

    let account = seed_user_and_account(&pool, "disconnect@x.co", None).await;

    // ── Case A: oauth_tokens delete, no slack_integrations backing it —
    // connector_installs row for that workspace must be removed. ───────────
    sqlx::query(
        "INSERT INTO connector_installs (provider, external_workspace_id, account_id) \
         VALUES ('slack', 'T-ONLY-OAUTH', $1)",
    )
    .bind(account)
    .execute(&pool)
    .await
    .unwrap();
    run_oauth_delete_connection_install_guard(
        &pool,
        account,
        "slack",
        &["T-ONLY-OAUTH".to_string()],
    )
    .await;
    let remaining: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM connector_installs WHERE account_id = $1 AND external_workspace_id = 'T-ONLY-OAUTH'",
    )
    .bind(account)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(remaining, 0, "no slack_integrations backing this workspace — the row must be removed");

    // ── Case B: oauth_tokens delete, but slack_integrations STILL has a live
    // row for the same (account, workspace) — connector_installs row must
    // SURVIVE (the legacy path still legitimately owns it). ────────────────
    sqlx::query(
        "INSERT INTO connector_installs (provider, external_workspace_id, account_id) \
         VALUES ('slack', 'T-DUAL-BACKED', $1)",
    )
    .bind(account)
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO slack_integrations (account_id, workspace_id, workspace_name, bot_token) \
         VALUES ($1, 'T-DUAL-BACKED', 'Dual Co', 'xoxb-dual')",
    )
    .bind(account)
    .execute(&pool)
    .await
    .unwrap();
    run_oauth_delete_connection_install_guard(
        &pool,
        account,
        "slack",
        &["T-DUAL-BACKED".to_string()],
    )
    .await;
    let survives: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM connector_installs WHERE account_id = $1 AND external_workspace_id = 'T-DUAL-BACKED'",
    )
    .bind(account)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        survives, 1,
        "slack_integrations still legitimately owns this workspace — the row must survive \
         routes/oauth.rs's own disconnect (which never touches slack_integrations at all)"
    );

    // ── Case C: the mirror image — slack_integrations delete, but
    // oauth_tokens STILL has a live row for the same (account, workspace) —
    // connector_installs row must SURVIVE `routes/slack.rs`'s own disconnect. ─
    sqlx::query(
        "INSERT INTO oauth_tokens (account_id, provider, scope, access_token, provider_workspace_id) \
         VALUES ($1, 'slack', 'chat:write', 'ciphertext', 'T-DUAL-BACKED')",
    )
    .bind(account)
    .execute(&pool)
    .await
    .unwrap();
    run_slack_delete_integration_install_guard(&pool, account, "T-DUAL-BACKED").await;
    let survives_b: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM connector_installs WHERE account_id = $1 AND external_workspace_id = 'T-DUAL-BACKED'",
    )
    .bind(account)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        survives_b, 1,
        "oauth_tokens still legitimately owns this workspace — the row must survive \
         routes/slack.rs's own disconnect"
    );

    // ── Case D: with the oauth_tokens row now ALSO gone, the same
    // slack.rs-shaped delete must finally remove the row. ───────────────────
    sqlx::query("DELETE FROM oauth_tokens WHERE account_id = $1 AND provider_workspace_id = 'T-DUAL-BACKED'")
        .bind(account)
        .execute(&pool)
        .await
        .unwrap();
    run_slack_delete_integration_install_guard(&pool, account, "T-DUAL-BACKED").await;
    let gone: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM connector_installs WHERE account_id = $1 AND external_workspace_id = 'T-DUAL-BACKED'",
    )
    .bind(account)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(gone, 0, "with neither backend owning it any longer, the row must finally be removed");

    pool.close().await;
    admin_conn
        .execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
        .await
        .ok();
}

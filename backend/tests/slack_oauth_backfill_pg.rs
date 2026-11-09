//! Foundation train PR 1.5 — pg-gated correctness proof for the
//! `slack_integrations.bot_token` plaintext cleanup:
//!
//!   1. A legacy `slack_integrations` row (the shape
//!      `routes::slack::oauth_callback` wrote BEFORE this PR's dual-write —
//!      `bot_token` only, no `oauth_tokens` row at all) is discovered by
//!      `coralswarm_backend::slack_oauth_backfill::preview_slack_oauth_backfill`
//!      (dry run, writes nothing) and converted by
//!      `backfill_slack_oauth_tokens` (execute) into a real, encrypted
//!      `oauth_tokens` row — provisioning the account's wrapped DEK along
//!      the way when one is not already present.
//!   2. The backfilled row resolves through the EXACT SAME scoped/decrypt
//!      path every migrated production read site now uses
//!      (`workers::huddle::get_slack_bot_token_for_workspace`, via its
//!      `test-support` alias), against a real `coralswarm_app` login — not
//!      just the owner/BYPASSRLS pool the backfill itself writes through.
//!   3. The pass is idempotent: re-running finds nothing left to convert.
//!   4. Disconnect actually stops ingest for a backfilled connection: once
//!      the `oauth_tokens` row a real disconnect would delete
//!      (`routes::oauth::delete_connection`'s own `DELETE ... WHERE
//!      owner_user_id IS NULL` — reproduced here directly rather than
//!      driving the full Axum route) is gone, the SAME resolution path
//!      returns `Ok(None)` — the "no integration found, skip ingest" arm
//!      every migrated read site now takes — even though the legacy
//!      `slack_integrations` row's plaintext `bot_token` may still exist.
//!      This is the exact incident class (`routes/oauth.rs::delete_connection`'s
//!      own doc comment: "Disconnect didn't stop ingest") proven closed for
//!      a backfilled connection specifically.
//!
//! Uses the shared two-account [`RlsProbe`] harness purely for its scratch DB
//! + migrated control tree + real `coralswarm_app` login (`app_pool`); the
//! built-in `AccountFixture`s are not used here since this suite seeds its
//! own `slack_integrations`/`oauth_tokens` rows against `probe.a`/`probe.b`'s
//! account ids directly.
//!
//! Env-gated, like every other `*_pg.rs` here:
//!
//!   ORGT_TEST_BASE_URL=postgres://alex@localhost:5432/postgres \
//!     cargo test -p coralswarm-backend --test slack_oauth_backfill_pg

mod support;

use coralswarm_backend::slack_oauth_backfill::{
    backfill_slack_oauth_tokens, preview_slack_oauth_backfill, BACKFILLED_SCOPE_PLACEHOLDER,
};
use coralswarm_backend::workers::huddle::test_support_slack_bot_token_for_workspace;
use support::rls_probe::RlsProbe;
use uuid::Uuid;

/// A 32-byte master KEK, hex-encoded, for the whole binary — same idiom as
/// `slack_token_rls_pg.rs`'s `set_master_kek` (every test sets the identical
/// value, so a benign race across this binary's concurrently-run tests is
/// the worst case).
const TEST_KEK_HEX: &str = "cafefeedcafefeedcafefeedcafefeedcafefeedcafefeedcafefeedcafefeed";

fn set_master_kek() {
    // SAFETY: every test in this binary sets this to the identical value.
    unsafe {
        std::env::set_var("MASTER_KEK_HEX", TEST_KEK_HEX);
    }
}

/// Seed a LEGACY `slack_integrations` row for `account_id` — the exact shape
/// `routes::slack::oauth_callback` wrote before this PR's dual-write:
/// `bot_token` in plaintext, no `oauth_tokens` row alongside it. Through the
/// owner/BYPASSRLS pool, matching how a real OAuth callback commit would look
/// from any other backend connection's point of view.
async fn seed_legacy_integration(owner: &sqlx::PgPool, account_id: Uuid, workspace_id: &str, bot_token: &str) {
    sqlx::query(
        "INSERT INTO slack_integrations (account_id, workspace_id, workspace_name, bot_token) \
         VALUES ($1, $2, 'Backfill Test Workspace', $3)",
    )
    .bind(account_id)
    .bind(workspace_id)
    .bind(bot_token)
    .execute(owner)
    .await
    .expect("seed legacy slack_integrations row");
}

async fn count_oauth_tokens_rows(owner: &sqlx::PgPool, account_id: Uuid, workspace_id: &str) -> i64 {
    sqlx::query_scalar(
        "SELECT count(*) FROM oauth_tokens \
         WHERE account_id = $1 AND provider = 'slack' AND provider_workspace_id = $2 \
           AND owner_user_id IS NULL",
    )
    .bind(account_id)
    .bind(workspace_id)
    .fetch_one(owner)
    .await
    .expect("count oauth_tokens rows")
}

/// The main round-trip: dry run finds it, execute converts it, the
/// backfilled row resolves through the real scoped path with the ORIGINAL
/// plaintext token, a second execute is a no-op, and the scope column carries
/// the documented placeholder (never a fabricated real scope list).
#[tokio::test]
async fn legacy_integration_backfills_and_resolves_via_oauth_tokens_pg() {
    let Some(probe) = RlsProbe::setup("orgt_slack_oauth_backfill_convert").await else {
        eprintln!("skipping legacy_integration_backfills_and_resolves_via_oauth_tokens_pg: set ORGT_TEST_BASE_URL");
        return;
    };
    set_master_kek();

    let workspace_id = format!("T{}", Uuid::new_v4().simple());
    let original_token = format!("xoxb-legacy-{}", Uuid::new_v4().simple());
    seed_legacy_integration(&probe.owner, probe.a.account_id, &workspace_id, &original_token).await;

    // Account B gets no legacy integration at all — proves the backfill
    // narrows correctly to accounts that actually have one, and account A's
    // conversion never touches B.
    assert_eq!(count_oauth_tokens_rows(&probe.owner, probe.b.account_id, &workspace_id).await, 0);

    // ── Dry run: finds the work, writes nothing ─────────────────────────────
    let preview = preview_slack_oauth_backfill(&probe.owner, None).await;
    assert_eq!(preview.errors, 0, "{:?}", preview.messages);
    assert!(preview.workspaces_seen >= 1, "must find at least the seeded legacy workspace");
    assert_eq!(
        count_oauth_tokens_rows(&probe.owner, probe.a.account_id, &workspace_id).await,
        0,
        "dry run must not write anything"
    );

    // ── Execute: converts it ─────────────────────────────────────────────────
    let executed = backfill_slack_oauth_tokens(&probe.owner, Some(probe.a.account_id)).await;
    assert_eq!(executed.errors, 0, "{:?}", executed.messages);
    assert_eq!(executed.workspaces_seen, 1, "account A has exactly one legacy workspace");
    assert_eq!(executed.rows_written, 1);
    assert_eq!(
        count_oauth_tokens_rows(&probe.owner, probe.a.account_id, &workspace_id).await,
        1,
        "exactly one oauth_tokens row must now exist for this workspace"
    );

    // The placeholder, never a fabricated real Slack scope list (see that
    // constant's own doc comment for why).
    let scope: String = sqlx::query_scalar(
        "SELECT scope FROM oauth_tokens WHERE account_id = $1 AND provider_workspace_id = $2",
    )
    .bind(probe.a.account_id)
    .bind(&workspace_id)
    .fetch_one(&probe.owner)
    .await
    .expect("read scope");
    assert_eq!(scope, BACKFILLED_SCOPE_PLACEHOLDER);

    // ── Resolves through the REAL scoped/decrypt path, as coralswarm_app ────
    let resolved = test_support_slack_bot_token_for_workspace(&probe.app_pool, probe.a.account_id, &workspace_id)
        .await
        .expect("resolution must succeed under RLS");
    assert_eq!(
        resolved.as_deref(),
        Some(original_token.as_str()),
        "the backfilled row must decrypt back to the ORIGINAL plaintext bot_token"
    );

    // ── Idempotent: a second execute finds nothing left ─────────────────────
    let second = backfill_slack_oauth_tokens(&probe.owner, Some(probe.a.account_id)).await;
    assert_eq!(second.errors, 0, "{:?}", second.messages);
    assert_eq!(second.workspaces_seen, 0, "the workspace is already backfilled — nothing left to do");
    assert_eq!(second.rows_written, 0);

    probe.teardown().await;
}

/// An account with no `dek_wrapped` yet gets one provisioned as part of the
/// backfill pass (mirrors `clerk_provision::provision_account_dek` /
/// `oauth_dek_cutover::ensure_account_dek`'s identical behavior for the
/// wrapped-DEK cutover backfill), and the row still resolves correctly
/// afterward.
#[tokio::test]
async fn backfill_provisions_a_missing_dek_pg() {
    let Some(probe) = RlsProbe::setup("orgt_slack_oauth_backfill_provision_dek").await else {
        eprintln!("skipping backfill_provisions_a_missing_dek_pg: set ORGT_TEST_BASE_URL");
        return;
    };
    set_master_kek();

    // `RlsProbe::setup`'s own fixture accounts may already carry a
    // dek_wrapped from other suites sharing the harness pattern — force this
    // one to NULL so this test genuinely exercises the provisioning path
    // regardless.
    sqlx::query("UPDATE accounts SET dek_wrapped = NULL WHERE id = $1")
        .bind(probe.a.account_id)
        .execute(&probe.owner)
        .await
        .expect("clear dek_wrapped");

    let workspace_id = format!("T{}", Uuid::new_v4().simple());
    let original_token = format!("xoxb-nodek-{}", Uuid::new_v4().simple());
    seed_legacy_integration(&probe.owner, probe.a.account_id, &workspace_id, &original_token).await;

    let executed = backfill_slack_oauth_tokens(&probe.owner, Some(probe.a.account_id)).await;
    assert_eq!(executed.errors, 0, "{:?}", executed.messages);
    assert_eq!(executed.accounts_provisioned_dek, 1, "a DEK must be freshly provisioned");
    assert_eq!(executed.rows_written, 1);

    let resolved = test_support_slack_bot_token_for_workspace(&probe.app_pool, probe.a.account_id, &workspace_id)
        .await
        .expect("resolution must succeed under a freshly-provisioned DEK");
    assert_eq!(resolved.as_deref(), Some(original_token.as_str()));

    probe.teardown().await;
}

/// Disconnect actually stops ingest for a backfilled connection: after the
/// backfill runs, deleting the `oauth_tokens` row exactly the way
/// `routes::oauth::delete_connection` does (`DELETE ... WHERE owner_user_id
/// IS NULL`) makes the SAME resolution path every migrated read site uses
/// return `Ok(None)` — even though the legacy `slack_integrations` row (with
/// its plaintext `bot_token`) may still be present, e.g. mid-disconnect
/// before that row's own delete runs. This is the "Disconnect didn't stop
/// ingest" incident proven closed for a backfilled (formerly legacy-only)
/// connection specifically.
#[tokio::test]
async fn disconnect_stops_resolution_for_a_backfilled_connection_pg() {
    let Some(probe) = RlsProbe::setup("orgt_slack_oauth_backfill_disconnect").await else {
        eprintln!("skipping disconnect_stops_resolution_for_a_backfilled_connection_pg: set ORGT_TEST_BASE_URL");
        return;
    };
    set_master_kek();

    let workspace_id = format!("T{}", Uuid::new_v4().simple());
    let original_token = format!("xoxb-disconnect-{}", Uuid::new_v4().simple());
    seed_legacy_integration(&probe.owner, probe.a.account_id, &workspace_id, &original_token).await;

    let executed = backfill_slack_oauth_tokens(&probe.owner, Some(probe.a.account_id)).await;
    assert_eq!(executed.errors, 0, "{:?}", executed.messages);
    assert_eq!(executed.rows_written, 1);

    // Sanity: resolves before disconnect.
    let before = test_support_slack_bot_token_for_workspace(&probe.app_pool, probe.a.account_id, &workspace_id)
        .await
        .expect("must resolve before disconnect");
    assert_eq!(before.as_deref(), Some(original_token.as_str()));

    // The disconnect itself — same predicate as
    // `routes::oauth::delete_connection`'s `DELETE FROM oauth_tokens WHERE
    // account_id = $1 AND provider = $2 AND owner_user_id IS NULL`. The
    // legacy `slack_integrations` row is DELIBERATELY left in place here
    // (that route deletes it too, in the same transaction, but this test's
    // whole point is that resolution stops the moment `oauth_tokens` is
    // gone, independent of whether `slack_integrations` has been cleaned up
    // yet).
    sqlx::query("DELETE FROM oauth_tokens WHERE account_id = $1 AND provider = 'slack' AND owner_user_id IS NULL")
        .bind(probe.a.account_id)
        .execute(&probe.owner)
        .await
        .expect("simulate disconnect's oauth_tokens delete");

    let after = test_support_slack_bot_token_for_workspace(&probe.app_pool, probe.a.account_id, &workspace_id)
        .await
        .expect("a missing oauth_tokens row is Ok(None), not Err");
    assert!(
        after.is_none(),
        "resolution must stop once oauth_tokens is gone, even though slack_integrations still \
         carries the plaintext bot_token — got {after:?}"
    );

    probe.teardown().await;
}

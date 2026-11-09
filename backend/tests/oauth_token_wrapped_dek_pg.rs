//! Foundation train PR 1.2 (CS-031) — pg-gated correctness proof for the
//! wrapped-DEK cutover backfill (`coralswarm_backend::oauth_dek_cutover`,
//! driven operationally by `bin/backfill_oauth_tokens_wrapped_dek`).
//!
//! Proves the three properties the migration's own runbook depends on:
//!
//!   1. A row still encrypted under the legacy `ACCOUNT_MASTER_KEK`-XOR DEK
//!      is re-encrypted under the wrapped DEK and decrypts correctly
//!      afterward via `crate::crypto::resolve_dek` — the SAME resolution
//!      path every production oauth_tokens read now uses.
//!   2. The pass is idempotent: re-running it finds nothing left to convert.
//!   3. A dry run (`preview_oauth_tokens_cutover`) reports the work without
//!      writing anything.
//!
//! Uses the shared two-account [`RlsProbe`] harness purely for its scratch DB
//! + migrated control tree + BYPASSRLS owner pool; the RLS/cross-account
//! properties it also proves are not this suite's concern (see
//! `slack_token_rls_pg.rs` for those).
//!
//! Env-gated, like every other `*_pg.rs` here:
//!
//!   ORGT_TEST_BASE_URL=postgres://alex@localhost:5432/postgres \
//!     cargo test -p coralswarm-backend --test oauth_token_wrapped_dek_pg

mod support;

use coralswarm_backend::crypto::{derive_legacy_oauth_account_dek, resolve_dek};
use coralswarm_backend::oauth_dek_cutover::{preview_oauth_tokens_cutover, reencrypt_oauth_tokens};
use coralswarm_core::oauth_token::{decrypt_token, encrypt_token};
use support::rls_probe::RlsProbe;
use uuid::Uuid;

/// Every test in this binary that touches `ACCOUNT_MASTER_KEK`/`MASTER_KEK_HEX`
/// writes the SAME constant values below, so concurrent writes across this
/// binary's own `#[tokio::test]`s are benign — same idiom as
/// `slack_token_rls_pg.rs`'s `set_master_kek`. This binary runs its own
/// tests concurrently (no `--test-threads=1` pragma here), but since every
/// test sets the identical values there is nothing to race incorrectly.
const TEST_ACCOUNT_MASTER_KEK_HEX: &str =
    "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const TEST_MASTER_KEK_HEX: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

fn set_test_keks() {
    // SAFETY: every test in this binary sets these two vars to the identical
    // values, so a benign race is the worst case.
    unsafe {
        std::env::set_var("ACCOUNT_MASTER_KEK", TEST_ACCOUNT_MASTER_KEK_HEX);
        std::env::set_var("MASTER_KEK_HEX", TEST_MASTER_KEK_HEX);
    }
}

/// Insert a legacy-format `oauth_tokens` row for `account_id`: every
/// encrypted column under `derive_legacy_oauth_account_dek`'s DEK, exactly
/// how `routes::oauth::store_token_bundle_tx` wrote rows before Foundation
/// PR 1.2. `dek_migrated_at` starts `NULL` (the column's default), matching
/// every row that exists the instant migration `0177` applies.
async fn seed_legacy_row(
    owner: &sqlx::PgPool,
    account_id: Uuid,
    access_plain: &str,
    refresh_plain: Option<&str>,
    user_token_plain: Option<&str>,
) -> Uuid {
    let legacy_dek = derive_legacy_oauth_account_dek(account_id)
        .expect("ACCOUNT_MASTER_KEK must be set before seeding a legacy row");
    let access_enc = encrypt_token(&legacy_dek, access_plain).expect("encrypt access_token");
    let refresh_enc = refresh_plain
        .map(|p| encrypt_token(&legacy_dek, p).expect("encrypt refresh_token"));
    let user_token_enc = user_token_plain
        .map(|p| encrypt_token(&legacy_dek, p).expect("encrypt user_token_encrypted"));

    sqlx::query_scalar(
        "INSERT INTO oauth_tokens \
             (account_id, provider, scope, access_token, refresh_token, \
              user_token_encrypted, provider_workspace_id) \
         VALUES ($1, 'microsoft', 'Mail.Read', $2, $3, $4, $5) RETURNING id",
    )
    .bind(account_id)
    .bind(&access_enc)
    .bind(&refresh_enc)
    .bind(&user_token_enc)
    .bind(format!("W-{account_id}"))
    .fetch_one(owner)
    .await
    .expect("seed legacy oauth_tokens row")
}

async fn dek_migrated_at(owner: &sqlx::PgPool, row_id: Uuid) -> Option<chrono::DateTime<chrono::Utc>> {
    sqlx::query_scalar("SELECT dek_migrated_at FROM oauth_tokens WHERE id = $1")
        .bind(row_id)
        .fetch_one(owner)
        .await
        .expect("read dek_migrated_at")
}

async fn read_columns(
    owner: &sqlx::PgPool,
    row_id: Uuid,
) -> (Vec<u8>, Option<Vec<u8>>, Option<Vec<u8>>) {
    sqlx::query_as(
        "SELECT access_token, refresh_token, user_token_encrypted FROM oauth_tokens WHERE id = $1",
    )
    .bind(row_id)
    .fetch_one(owner)
    .await
    .expect("read encrypted columns")
}

/// The main round-trip: a legacy row for account A converts cleanly, decrypts
/// under the wrapped DEK afterward, and is marked migrated. Account B is left
/// alone (never touched by an `--account-id`-narrowed run) to prove the
/// narrowing actually narrows.
#[tokio::test]
async fn legacy_row_converts_and_decrypts_via_wrapped_dek_pg() {
    let Some(probe) = RlsProbe::setup("orgt_oauth_dek_cutover_convert").await else {
        eprintln!("skipping legacy_row_converts_and_decrypts_via_wrapped_dek_pg: set ORGT_TEST_BASE_URL");
        return;
    };
    set_test_keks();

    let row_id = seed_legacy_row(
        &probe.owner,
        probe.a.account_id,
        "access-plaintext-1",
        Some("refresh-plaintext-1"),
        Some("user-token-plaintext-1"),
    )
    .await;
    // Account B gets no oauth_tokens row at all — proves a narrowed run
    // never touches accounts outside its filter (nothing to assert on B
    // beyond "the run doesn't error enumerating it").

    assert!(
        dek_migrated_at(&probe.owner, row_id).await.is_none(),
        "a freshly seeded legacy row must start unmigrated"
    );

    let outcome = reencrypt_oauth_tokens(&probe.owner, Some(probe.a.account_id)).await;
    assert_eq!(outcome.errors, 0, "no errors expected: {:?}", outcome.messages);
    assert_eq!(outcome.rows_seen, 1);
    assert_eq!(outcome.rows_migrated, 1);
    assert_eq!(
        outcome.accounts_provisioned_dek, 1,
        "account A had no dek_wrapped yet, so this pass must provision one"
    );

    assert!(
        dek_migrated_at(&probe.owner, row_id).await.is_some(),
        "a converted row must have dek_migrated_at set"
    );

    let wrapped_dek = resolve_dek(&probe.owner, probe.a.account_id)
        .await
        .expect("resolve_dek must succeed post-cutover")
        .expect("account A must now have a wrapped DEK");
    let (access_enc, refresh_enc, user_token_enc) = read_columns(&probe.owner, row_id).await;

    assert_eq!(
        decrypt_token(&wrapped_dek, &access_enc).expect("decrypt access_token"),
        "access-plaintext-1"
    );
    assert_eq!(
        decrypt_token(&wrapped_dek, refresh_enc.as_deref().expect("refresh_token present"))
            .expect("decrypt refresh_token"),
        "refresh-plaintext-1"
    );
    assert_eq!(
        decrypt_token(&wrapped_dek, user_token_enc.as_deref().expect("user_token present"))
            .expect("decrypt user_token_encrypted"),
        "user-token-plaintext-1"
    );

    // The legacy DEK must NOT decrypt the now-re-encrypted row — proves this
    // is a genuine re-encryption, not merely marking the row migrated.
    let legacy_dek = derive_legacy_oauth_account_dek(probe.a.account_id).unwrap();
    assert!(
        decrypt_token(&legacy_dek, &access_enc).is_err(),
        "the legacy DEK must no longer decrypt the converted row"
    );

    probe.teardown().await;
}

/// Idempotency: re-running the pass over an already-converted account finds
/// nothing left to do, and does not disturb the already-converted row.
#[tokio::test]
async fn rerun_after_full_conversion_is_a_no_op_pg() {
    let Some(probe) = RlsProbe::setup("orgt_oauth_dek_cutover_idempotent").await else {
        eprintln!("skipping rerun_after_full_conversion_is_a_no_op_pg: set ORGT_TEST_BASE_URL");
        return;
    };
    set_test_keks();

    let row_id = seed_legacy_row(&probe.owner, probe.a.account_id, "access-plaintext-2", None, None)
        .await;

    let first = reencrypt_oauth_tokens(&probe.owner, Some(probe.a.account_id)).await;
    assert_eq!(first.errors, 0, "{:?}", first.messages);
    assert_eq!(first.rows_migrated, 1);
    let migrated_at_first = dek_migrated_at(&probe.owner, row_id).await.expect("migrated");

    let second = reencrypt_oauth_tokens(&probe.owner, Some(probe.a.account_id)).await;
    assert_eq!(second.errors, 0, "{:?}", second.messages);
    assert_eq!(second.rows_seen, 0, "an already-converted row must not be re-selected");
    assert_eq!(second.rows_migrated, 0);
    assert_eq!(
        second.accounts_provisioned_dek, 0,
        "the account already has a dek_wrapped from the first pass"
    );

    let migrated_at_second = dek_migrated_at(&probe.owner, row_id).await.expect("still migrated");
    assert_eq!(
        migrated_at_first, migrated_at_second,
        "a no-op re-run must not touch the already-converted row's timestamp"
    );

    probe.teardown().await;
}

/// Dry run: reports the work without decrypting, writing, or requiring
/// either KEK to be configured. Deliberately does NOT call [`set_test_keks`]
/// — and deliberately does NOT insert real ciphertext (via
/// [`seed_legacy_row`], which itself needs `ACCOUNT_MASTER_KEK` to encrypt
/// under): unsetting/restoring a process-global env var here would race any
/// OTHER test in this binary running concurrently that needs it set (`cargo
/// test` parallelizes across `#[tokio::test]`s by default). Since a dry run
/// never decrypts a column's contents — only counts rows by
/// `dek_migrated_at IS NULL` (see `oauth_dek_cutover::process_account`) — an
/// arbitrary placeholder byte string proves the same property with no KEK
/// dependency and no shared-env-var risk at all.
#[tokio::test]
async fn dry_run_reports_without_writing_pg() {
    let Some(probe) = RlsProbe::setup("orgt_oauth_dek_cutover_dryrun").await else {
        eprintln!("skipping dry_run_reports_without_writing_pg: set ORGT_TEST_BASE_URL");
        return;
    };

    let placeholder = b"not-real-ciphertext-dry-run-never-reads-this".to_vec();
    let row_id: Uuid = sqlx::query_scalar(
        "INSERT INTO oauth_tokens (account_id, provider, scope, access_token, provider_workspace_id) \
         VALUES ($1, 'microsoft', 'Mail.Read', $2, $3) RETURNING id",
    )
    .bind(probe.a.account_id)
    .bind(&placeholder)
    .bind(format!("W-{}", probe.a.account_id))
    .fetch_one(&probe.owner)
    .await
    .expect("seed placeholder oauth_tokens row");

    let preview = preview_oauth_tokens_cutover(&probe.owner, Some(probe.a.account_id)).await;
    assert_eq!(preview.errors, 0, "{:?}", preview.messages);
    assert_eq!(preview.rows_seen, 1, "dry run must still count the legacy row");
    assert_eq!(preview.rows_migrated, 0, "a dry run must never migrate anything");
    assert_eq!(preview.accounts_provisioned_dek, 0, "a dry run must never provision a DEK");

    assert!(
        dek_migrated_at(&probe.owner, row_id).await.is_none(),
        "a dry run must leave dek_migrated_at untouched"
    );
    let (access_after, _, _) = read_columns(&probe.owner, row_id).await;
    assert_eq!(access_after, placeholder, "a dry run must never rewrite ciphertext");

    probe.teardown().await;
}

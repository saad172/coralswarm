//! CS-453 (Phase G wave 1, D21) — pg-gated correctness proof for
//! `connectors::credential_health`'s durable set/clear/read helpers over
//! `oauth_tokens.credential_invalid_at`/`.credential_invalid_reason`
//! (control migration `0251`, tenant twin `0103`), and for the
//! `tokens_revoked` Events API `tokens.oauth` marking path
//! (`mark_slack_provider_user_revoked`, `routes::slack_events`'
//! `mark_tokens_oauth_revocations`).
//!
//! Three properties this suite pins:
//!
//!   1. `mark_credential_invalid` / `clear_credential_invalid` are
//!      OWNER-EXACT: marking/clearing a `Member(uid)` row never touches the
//!      `AccountWide` row for the SAME account+provider, and never touches
//!      account B's row for the same provider — see `TokenOwner`'s own doc
//!      comment for why an `owner_user_id IS NULL OR = x` predicate would be
//!      a real cross-credential bug, not a style nit.
//!   2. `is_credential_live` distinguishes "no row at all" (`None`) from
//!      "row exists and is dead" (`Some(false)`) from "row exists and is
//!      live" (`Some(true)`).
//!   3. `mark_slack_provider_user_revoked` — the `tokens_revoked` `tokens.
//!      oauth` matcher — finds `"slack_user"` rows by `provider_user_id` +
//!      `provider_workspace_id`, leaves the account-wide `"slack"` bot-token
//!      row untouched even when it stores the installer's id, scoped to the
//!      one account/workspace named, and is a no-op (not an error) when
//!      nothing matches.
//!
//! Uses the shared two-account [`RlsProbe`] harness purely for its scratch
//! DB + migrated control tree + BYPASSRLS owner pool — same posture as
//! `oauth_token_wrapped_dek_pg.rs`, which this file's env-gating and
//! structure otherwise mirror.
//!
//!   ORGT_TEST_BASE_URL=postgres://alex@localhost:5432/postgres \
//!     cargo test -p coralswarm-backend --features test-support --test credential_invalid_pg

mod support;

use coralswarm_backend::{
    clear_credential_invalid, is_credential_live, mark_credential_invalid,
    mark_slack_provider_user_revoked, CredentialDeathReason, TokenOwner,
};
use support::rls_probe::RlsProbe;
use uuid::Uuid;

/// Seed a bare `oauth_tokens` row (ciphertext columns are never read by this
/// suite, so a placeholder value stands in for real encrypted bytes — same
/// shortcut `slack_events.rs`'s own in-crate `seed_personal_install` test
/// helper takes).
async fn seed_oauth_token(
    owner: &sqlx::PgPool,
    account_id: Uuid,
    provider: &str,
    owner_user_id: Option<Uuid>,
    provider_user_id: &str,
    provider_workspace_id: &str,
) {
    sqlx::query(
        "INSERT INTO oauth_tokens \
             (account_id, provider, scope, access_token, owner_user_id, \
              provider_user_id, provider_workspace_id) \
         VALUES ($1, $2, 'scope', 'unused-ciphertext', $3, $4, $5)",
    )
    .bind(account_id)
    .bind(provider)
    .bind(owner_user_id)
    .bind(provider_user_id)
    .bind(provider_workspace_id)
    .execute(owner)
    .await
    .expect("seed oauth_tokens row");
}

async fn read_invalid_columns(
    owner: &sqlx::PgPool,
    account_id: Uuid,
    provider: &str,
    owner_user_id: Option<Uuid>,
) -> (Option<chrono::DateTime<chrono::Utc>>, Option<String>) {
    let row: (Option<chrono::DateTime<chrono::Utc>>, Option<String>) = match owner_user_id {
        None => sqlx::query_as(
            "SELECT credential_invalid_at, credential_invalid_reason FROM oauth_tokens \
             WHERE account_id = $1 AND provider = $2 AND owner_user_id IS NULL",
        )
        .bind(account_id)
        .bind(provider)
        .fetch_one(owner)
        .await
        .expect("read row"),
        Some(uid) => sqlx::query_as(
            "SELECT credential_invalid_at, credential_invalid_reason FROM oauth_tokens \
             WHERE account_id = $1 AND provider = $2 AND owner_user_id = $3",
        )
        .bind(account_id)
        .bind(provider)
        .bind(uid)
        .fetch_one(owner)
        .await
        .expect("read row"),
    };
    row
}

// ═══════════════════════════════════════════════════════════════════════════
// 1. mark_credential_invalid / clear_credential_invalid — owner-exactness
// ═══════════════════════════════════════════════════════════════════════════

/// Marking a `Member(uid)` row invalid must NEVER also mark the
/// `AccountWide` row for the same `(account_id, provider)` — the exact
/// cross-credential mixup `TokenOwner`'s own doc comment names as the
/// reason there is no `owner_user_id IS NULL OR = x` fallback anywhere in
/// this codebase.
#[tokio::test]
async fn marking_a_member_row_never_touches_the_account_wide_row_pg() {
    let Some(probe) = RlsProbe::setup("orgt_cred_invalid_member_vs_wide").await else {
        eprintln!("skipping marking_a_member_row_never_touches_the_account_wide_row_pg: set ORGT_TEST_BASE_URL");
        return;
    };

    seed_oauth_token(&probe.owner, probe.a.account_id, "microsoft", None, "wide-user", "workspace-1").await;
    seed_oauth_token(
        &probe.owner,
        probe.a.account_id,
        "microsoft",
        Some(probe.a.user_id),
        "member-user",
        "workspace-1",
    )
    .await;

    let rows = mark_credential_invalid(
        &probe.app_pool,
        probe.a.account_id,
        TokenOwner::Member(probe.a.user_id),
        "microsoft",
        CredentialDeathReason::MicrosoftConsentRevoked,
        None,
    )
    .await
    .expect("mark member row");
    assert_eq!(rows, 1, "exactly the member row should be updated");

    let (member_invalid_at, member_reason) =
        read_invalid_columns(&probe.owner, probe.a.account_id, "microsoft", Some(probe.a.user_id)).await;
    assert!(member_invalid_at.is_some(), "member row must be marked invalid");
    assert_eq!(member_reason.as_deref(), Some("ms_consent_revoked"));

    let (wide_invalid_at, wide_reason) =
        read_invalid_columns(&probe.owner, probe.a.account_id, "microsoft", None).await;
    assert!(wide_invalid_at.is_none(), "AccountWide row must be untouched");
    assert!(wide_reason.is_none(), "AccountWide row must be untouched");

    probe.teardown().await;
}

/// The mirror image: marking the `AccountWide` row never touches a
/// `Member(uid)` row for the same account+provider.
#[tokio::test]
async fn marking_the_account_wide_row_never_touches_a_member_row_pg() {
    let Some(probe) = RlsProbe::setup("orgt_cred_invalid_wide_vs_member").await else {
        eprintln!("skipping marking_the_account_wide_row_never_touches_a_member_row_pg: set ORGT_TEST_BASE_URL");
        return;
    };

    seed_oauth_token(&probe.owner, probe.a.account_id, "slack", None, "bot-user", "workspace-1").await;
    seed_oauth_token(
        &probe.owner,
        probe.a.account_id,
        "slack_user",
        Some(probe.a.user_id),
        "member-user",
        "workspace-1",
    )
    .await;

    let rows = mark_credential_invalid(
        &probe.app_pool,
        probe.a.account_id,
        TokenOwner::AccountWide,
        "slack",
        CredentialDeathReason::SlackTokenRevoked,
        None,
    )
    .await
    .expect("mark account-wide row");
    assert_eq!(rows, 1);

    let (wide_invalid_at, _) = read_invalid_columns(&probe.owner, probe.a.account_id, "slack", None).await;
    assert!(wide_invalid_at.is_some());

    let (member_invalid_at, _) =
        read_invalid_columns(&probe.owner, probe.a.account_id, "slack_user", Some(probe.a.user_id)).await;
    assert!(member_invalid_at.is_none(), "the slack_user member row must be untouched");

    probe.teardown().await;
}

/// Cross-ACCOUNT exactness: account A and account B can each hold a
/// `Member(uid)`-owned row under the SAME `owner_user_id` value only if
/// their `user_id`s happen to collide (they never do — every `users.id` is
/// a fresh UUID) — but the stronger, actually-load-bearing proof is that
/// `mark_credential_invalid` is `account_id`-scoped at all: marking A's row
/// must never touch B's row for the identical provider, even with the
/// SAME `TokenOwner` shape.
#[tokio::test]
async fn marking_one_account_never_touches_another_accounts_row_pg() {
    let Some(probe) = RlsProbe::setup("orgt_cred_invalid_cross_account").await else {
        eprintln!("skipping marking_one_account_never_touches_another_accounts_row_pg: set ORGT_TEST_BASE_URL");
        return;
    };

    seed_oauth_token(&probe.owner, probe.a.account_id, "microsoft", None, "a-user", "workspace-a").await;
    seed_oauth_token(&probe.owner, probe.b.account_id, "microsoft", None, "b-user", "workspace-b").await;

    let rows = mark_credential_invalid(
        &probe.app_pool,
        probe.a.account_id,
        TokenOwner::AccountWide,
        "microsoft",
        CredentialDeathReason::MicrosoftAccountDisabled,
        None,
    )
    .await
    .expect("mark account A's row");
    assert_eq!(rows, 1);

    let (a_invalid_at, _) = read_invalid_columns(&probe.owner, probe.a.account_id, "microsoft", None).await;
    assert!(a_invalid_at.is_some());
    let (b_invalid_at, _) = read_invalid_columns(&probe.owner, probe.b.account_id, "microsoft", None).await;
    assert!(b_invalid_at.is_none(), "account B's row must be untouched");

    probe.teardown().await;
}

/// `clear_credential_invalid` is the same owner-exact shape as `mark`, and
/// round-trips: mark, assert dead, clear, assert live again. This is the
/// same operation `routes::oauth::store_token_bundle_tx`'s `ON CONFLICT ...
/// DO UPDATE SET credential_invalid_at = NULL, credential_invalid_reason =
/// NULL` performs inline on every successful refresh/reconnect — proven
/// here at the SQL-semantics level since exercising that route directly
/// would require standing up the full DEK/encryption path this suite has no
/// need to depend on.
#[tokio::test]
async fn clear_round_trips_and_is_owner_exact_pg() {
    let Some(probe) = RlsProbe::setup("orgt_cred_invalid_clear_roundtrip").await else {
        eprintln!("skipping clear_round_trips_and_is_owner_exact_pg: set ORGT_TEST_BASE_URL");
        return;
    };

    seed_oauth_token(&probe.owner, probe.a.account_id, "microsoft", None, "wide-user", "workspace-1").await;
    seed_oauth_token(
        &probe.owner,
        probe.a.account_id,
        "microsoft",
        Some(probe.a.user_id),
        "member-user",
        "workspace-1",
    )
    .await;

    for owner in [TokenOwner::AccountWide, TokenOwner::Member(probe.a.user_id)] {
        mark_credential_invalid(
            &probe.app_pool,
            probe.a.account_id,
            owner,
            "microsoft",
            CredentialDeathReason::MicrosoftPasswordChangedOrAdminReset,
            None,
        )
        .await
        .expect("mark");
    }
    assert_eq!(
        is_credential_live(&probe.app_pool, probe.a.account_id, TokenOwner::AccountWide, "microsoft")
            .await
            .expect("read"),
        Some(false)
    );
    assert_eq!(
        is_credential_live(
            &probe.app_pool,
            probe.a.account_id,
            TokenOwner::Member(probe.a.user_id),
            "microsoft"
        )
        .await
        .expect("read"),
        Some(false)
    );

    // Clear ONLY the Member row.
    let cleared = clear_credential_invalid(
        &probe.app_pool,
        probe.a.account_id,
        TokenOwner::Member(probe.a.user_id),
        "microsoft",
    )
    .await
    .expect("clear member row");
    assert_eq!(cleared, 1);

    assert_eq!(
        is_credential_live(
            &probe.app_pool,
            probe.a.account_id,
            TokenOwner::Member(probe.a.user_id),
            "microsoft"
        )
        .await
        .expect("read"),
        Some(true),
        "member row must be live again"
    );
    assert_eq!(
        is_credential_live(&probe.app_pool, probe.a.account_id, TokenOwner::AccountWide, "microsoft")
            .await
            .expect("read"),
        Some(false),
        "AccountWide row must remain marked invalid — clear is owner-exact too"
    );

    probe.teardown().await;
}

/// The witness guard: a sync pass that loaded the row, then failed on the
/// OLD token while the user reconnected (the reconnect upsert rewrites the
/// row and clears any mark), must not mark the fresh row. Its witness —
/// the `updated_at` it loaded — predates the reconnect, so the guarded
/// mark is refused and the row stays live; a witness taken AFTER the
/// reconnect (a pass that genuinely failed on the new token) still marks.
#[tokio::test]
async fn a_stale_witness_never_marks_a_row_reconnected_since_pg() {
    let Some(probe) = RlsProbe::setup("orgt_cred_invalid_stale_witness").await else {
        eprintln!("skipping a_stale_witness_never_marks_a_row_reconnected_since_pg: set ORGT_TEST_BASE_URL");
        return;
    };
    let account_id = probe.a.account_id;
    let member = TokenOwner::Member(probe.a.user_id);
    seed_oauth_token(&probe.owner, account_id, "google", Some(probe.a.user_id), "sub", "a@example.com").await;
    let updated_at = |pool: sqlx::PgPool| async move {
        sqlx::query_scalar::<_, chrono::DateTime<chrono::Utc>>(
            "SELECT updated_at FROM oauth_tokens WHERE account_id = $1 AND provider = 'google'",
        )
        .bind(account_id)
        .fetch_one(&pool)
        .await
        .expect("read updated_at")
    };
    let loaded = updated_at(probe.owner.clone()).await;

    // The reconnect lands mid-pass: the same columns
    // `store_token_bundle_tx`'s upsert writes.
    sqlx::query(
        "UPDATE oauth_tokens SET access_token = 'fresh-ciphertext', updated_at = now(), \
             credential_invalid_at = NULL, credential_invalid_reason = NULL \
         WHERE account_id = $1 AND provider = 'google'",
    )
    .bind(account_id)
    .execute(&probe.owner)
    .await
    .expect("reconnect");

    let marked = mark_credential_invalid(
        &probe.app_pool,
        account_id,
        member,
        "google",
        CredentialDeathReason::GoogleInvalidGrant,
        Some(loaded),
    )
    .await
    .expect("guarded mark");
    assert_eq!(marked, 0, "the stale pass's mark is refused");
    assert_eq!(read_invalid_columns(&probe.owner, account_id, "google", Some(probe.a.user_id)).await, (None, None));

    let current = updated_at(probe.owner.clone()).await;
    let marked = mark_credential_invalid(
        &probe.app_pool,
        account_id,
        member,
        "google",
        CredentialDeathReason::GoogleInvalidGrant,
        Some(current),
    )
    .await
    .expect("guarded mark");
    assert_eq!(marked, 1, "a witness of the current row still marks it");
    assert_eq!(
        read_invalid_columns(&probe.owner, account_id, "google", Some(probe.a.user_id)).await.1.as_deref(),
        Some("google_invalid_grant")
    );

    probe.teardown().await;
}

// ═══════════════════════════════════════════════════════════════════════════
// 2. is_credential_live — None vs Some(false) vs Some(true)
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn is_credential_live_distinguishes_absent_dead_and_live_pg() {
    let Some(probe) = RlsProbe::setup("orgt_cred_invalid_is_live_tristate").await else {
        eprintln!("skipping is_credential_live_distinguishes_absent_dead_and_live_pg: set ORGT_TEST_BASE_URL");
        return;
    };

    // No row at all for "google" — None, not Some(false).
    assert_eq!(
        is_credential_live(&probe.app_pool, probe.a.account_id, TokenOwner::AccountWide, "google")
            .await
            .expect("read"),
        None
    );

    seed_oauth_token(&probe.owner, probe.a.account_id, "slack", None, "u1", "w1").await;
    // A freshly seeded row (credential_invalid_at defaults NULL) is live.
    assert_eq!(
        is_credential_live(&probe.app_pool, probe.a.account_id, TokenOwner::AccountWide, "slack")
            .await
            .expect("read"),
        Some(true)
    );

    mark_credential_invalid(
        &probe.app_pool,
        probe.a.account_id,
        TokenOwner::AccountWide,
        "slack",
        CredentialDeathReason::SlackAccountInactive,
        None,
    )
    .await
    .expect("mark");
    assert_eq!(
        is_credential_live(&probe.app_pool, probe.a.account_id, TokenOwner::AccountWide, "slack")
            .await
            .expect("read"),
        Some(false)
    );

    probe.teardown().await;
}

// ═══════════════════════════════════════════════════════════════════════════
// 3. mark_slack_provider_user_revoked — the tokens_revoked `tokens.oauth` matcher
// ═══════════════════════════════════════════════════════════════════════════

/// Matches only `"slack_user"` by `provider_user_id` +
/// `provider_workspace_id`, scoped to the one account — the exact shape
/// `routes::slack_events`' `mark_tokens_oauth_revocations` needs for a
/// `tokens_revoked` `tokens.oauth` entry. The account-wide `"slack"` row
/// stores the installing human (not the bot user) as `provider_user_id`, so
/// the matcher must leave that live bot-token row untouched even when the
/// revoked oauth id is the installer.
#[tokio::test]
async fn matches_only_slack_user_and_leaves_account_wide_slack_untouched_pg() {
    let Some(probe) = RlsProbe::setup("orgt_cred_invalid_slack_oauth_event").await else {
        eprintln!(
            "skipping matches_only_slack_user_and_leaves_account_wide_slack_untouched_pg: \
             set ORGT_TEST_BASE_URL"
        );
        return;
    };

    // Production shape: the account-wide bot-token row stores the
    // installing human as `provider_user_id`.
    seed_oauth_token(&probe.owner, probe.a.account_id, "slack", None, "U_REVOKED", "T1").await;
    // A hypothetical CS-454 per-member row for the SAME workspace.
    seed_oauth_token(
        &probe.owner,
        probe.a.account_id,
        "slack_user",
        Some(probe.a.user_id),
        "U_REVOKED",
        "T1",
    )
    .await;
    // A different workspace's member row for the SAME slack user id — must
    // NOT match (scoped to team_id).
    seed_oauth_token(
        &probe.owner,
        probe.a.account_id,
        "slack_user",
        Some(probe.b.user_id),
        "U_REVOKED",
        "T_OTHER",
    )
    .await;

    let rows = mark_slack_provider_user_revoked(&probe.app_pool, probe.a.account_id, "T1", "U_REVOKED")
        .await
        .expect("mark by provider identity");
    assert_eq!(rows, 1, "exactly the slack_user row in workspace T1 should match");

    let (member_invalid_at, member_reason) =
        read_invalid_columns(&probe.owner, probe.a.account_id, "slack_user", Some(probe.a.user_id)).await;
    assert!(member_invalid_at.is_some());
    assert_eq!(member_reason.as_deref(), Some("slack_tokens_revoked_event"));

    // The account-wide slack row still holds the installer's id and must
    // stay live — `tokens.oauth` is a user-grant revocation, not bot death.
    let (bot_invalid_at, _) = read_invalid_columns(&probe.owner, probe.a.account_id, "slack", None).await;
    assert!(bot_invalid_at.is_none(), "account-wide slack bot-token row must stay untouched");

    // The other-workspace row for the SAME slack user id is untouched.
    let (other_ws_invalid_at, _) =
        read_invalid_columns(&probe.owner, probe.a.account_id, "slack_user", Some(probe.b.user_id)).await;
    assert!(other_ws_invalid_at.is_none(), "a different workspace's row must not match");

    probe.teardown().await;
}

/// A `tokens.oauth` entry naming a Slack user id nothing matches (the
/// common case before CS-454 ships any `slack_user` rows) is a silent
/// no-op — `0` rows, `Ok`, never an error.
#[tokio::test]
async fn unmatched_slack_user_id_is_a_silent_no_op_pg() {
    let Some(probe) = RlsProbe::setup("orgt_cred_invalid_slack_oauth_no_match").await else {
        eprintln!("skipping unmatched_slack_user_id_is_a_silent_no_op_pg: set ORGT_TEST_BASE_URL");
        return;
    };

    seed_oauth_token(&probe.owner, probe.a.account_id, "slack", None, "U_BOT", "T1").await;

    let rows = mark_slack_provider_user_revoked(&probe.app_pool, probe.a.account_id, "T1", "U_NOBODY")
        .await
        .expect("no-op match");
    assert_eq!(rows, 0);

    probe.teardown().await;
}

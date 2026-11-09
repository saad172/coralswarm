//! RLS proof for the Slack bot-token lookup shared by the huddle worker and
//! the two Slack webhook routes (`workers::huddle::get_slack_bot_token_pub`),
//! driven against a real `coralswarm_app` login via the shared
//! `tests/support/rls_probe.rs` harness (PR #286).
//!
//! # Why this suite exists (finding F4)
//!
//! `get_slack_bot_token_pub` read the RLS-covered `oauth_tokens` table with
//! `.fetch_optional(pool)` on a bare `&PgPool`, documented as "deliberately
//! AMBIENT (unscoped) … its callers … own picking the scoping seam for its own
//! request context". That rationale was structurally impossible: taking a
//! `&PgPool` means checking out this function's OWN connection, and `SET LOCAL
//! app.account_id` is per-transaction on ONE connection — a scope opened by a
//! caller lives on a different connection and can never reach this query (see
//! `tenancy::scope`'s module doc). Post-OPS-4 the read simply hard-errored, and
//! BOTH callers swallow that:
//!
//!   * `routes::slack_events::handle_member_joined` — `warn!` + `return`
//!     (a spawned task; huddle capture just stops).
//!   * `routes::slack_interactivity` — `warn!` + **`200 OK` to Slack**, so
//!     Slack never even retried the user's backfill click.
//!
//! i.e. the Slack integration dies silently while every webhook keeps getting
//! `200`s. The three tests below prove the fixed lookup (a) succeeds under RLS
//! and returns the REAL decrypted token, (b) cannot resolve another account's
//! token, and (c) genuinely fails closed when run unscoped — the negative
//! control, since a lookup that returned `Ok(None)` would look identical to
//! "this workspace has no token" from the outside.
//!
//! Env-gated, like every other `*_pg.rs` here:
//!
//!   ORGT_TEST_BASE_URL=postgres://alex@localhost:5432/postgres \
//!     cargo test -p coralswarm-backend --test slack_token_rls_pg

mod support;

use coralswarm_backend::workers::huddle::test_support_slack_bot_token;
use support::rls_probe::{AccountFixture, RlsProbe};
use uuid::Uuid;

/// A 32-byte master KEK, hex-encoded (`deadbeef` × 8 = 64 hex chars), for the
/// whole binary. Test-only, never a real credential. Set once (see
/// [`set_master_kek`]); the suite runs `--test-threads=1`.
///
/// Foundation PR 1.2 (CS-031): this is now `MASTER_KEK_HEX` — the WRAPPED-DEK
/// scheme `get_slack_bot_token` resolves through
/// (`crate::crypto::resolve_dek`), not the retired `ACCOUNT_MASTER_KEK`
/// XOR derivation. See [`seed_slack_token`]'s own doc comment for how each
/// account's actual DEK is provisioned.
const TEST_KEK_HEX: &str = "deadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef";

fn set_master_kek() -> [u8; 32] {
    // SAFETY: env mutation is process-global; this binary runs with
    // `--test-threads=1` and every test in it sets the SAME value.
    unsafe {
        std::env::set_var("MASTER_KEK_HEX", TEST_KEK_HEX);
    }
    let bytes = hex::decode(TEST_KEK_HEX).expect("test KEK is valid hex");
    let mut kek = [0u8; 32];
    kek.copy_from_slice(&bytes);
    kek
}

/// Seed one account's encrypted Slack bot token exactly as
/// `routes::oauth`'s callback would post-cutover: a real per-account DEK,
/// wrapped under `kek` and written to `accounts.dek_wrapped` (mirroring
/// `clerk_provision::provision_account_dek`), then the plaintext token
/// encrypted under that SAME DEK — through the owner/BYPASSRLS pool. Returns
/// `(plaintext, dek)` — the DEK is handed back (rather than re-derived from a
/// formula, since each account's is now independently random) so callers that
/// need to prove cross-account isolation (Layer 2 in
/// `slack_bot_token_lookup_cannot_cross_accounts`) can exercise it directly.
async fn seed_slack_token(
    probe: &RlsProbe,
    f: &AccountFixture,
    kek: &[u8; 32],
    tag: &str,
) -> (String, [u8; 32]) {
    let dek = coralswarm_core::kek::generate_dek();
    let wrapped = coralswarm_core::kek::wrap_dek(kek, &dek).expect("wrap_dek must succeed");
    sqlx::query("UPDATE accounts SET dek_wrapped = $1 WHERE id = $2")
        .bind(&wrapped)
        .bind(f.account_id)
        .execute(&probe.owner)
        .await
        .expect("seed dek_wrapped");

    let plaintext = format!("xoxb-{tag}-do-not-leak");
    let encrypted = coralswarm_core::oauth_token::encrypt_token(&dek, &plaintext)
        .expect("encrypt bot token");
    sqlx::query(
        "INSERT INTO oauth_tokens \
             (account_id, provider, scope, access_token, provider_workspace_id) \
         VALUES ($1, 'slack', 'channels:history', $2, $3)",
    )
    .bind(f.account_id)
    .bind(&encrypted)
    .bind(format!("T-{tag}"))
    .execute(&probe.owner)
    .await
    .expect("seed oauth_tokens");
    (plaintext, dek)
}

/// The positive property: post-fix, the lookup runs under its own
/// `begin_account_scoped` transaction and returns the REAL decrypted token for
/// each account — the thing huddle capture and interactivity backfill need.
#[tokio::test]
async fn slack_bot_token_lookup_succeeds_under_rls() {
    let Some(probe) = RlsProbe::setup("orgt_slack_token_rls_ok").await else {
        eprintln!("skipping slack_bot_token_lookup_succeeds_under_rls: set ORGT_TEST_BASE_URL");
        return;
    };
    let kek = set_master_kek();
    let (token_a, _dek_a) = seed_slack_token(&probe, &probe.a, &kek, "a").await;
    let (token_b, _dek_b) = seed_slack_token(&probe, &probe.b, &kek, "b").await;

    // `probe.app_pool` is a real `coralswarm_app` login — the exact connection
    // path prod runs on today. Pre-fix this returned
    // `Err("error returned from database: unrecognized configuration parameter
    // \"app.account_id\"")`, which both callers turned into a shrug.
    let got_a = test_support_slack_bot_token(&probe.app_pool, probe.a.account_id)
        .await
        .expect("bot-token lookup must SUCCEED as coralswarm_app (F4: it used to hard-error)");
    assert_eq!(
        got_a.as_deref(),
        Some(token_a.as_str()),
        "the scoped lookup must return account A's REAL decrypted bot token"
    );

    let got_b = test_support_slack_bot_token(&probe.app_pool, probe.b.account_id)
        .await
        .expect("bot-token lookup must succeed for B too");
    assert_eq!(
        got_b.as_deref(),
        Some(token_b.as_str()),
        "…and B's own token under B's own scope"
    );
    assert_ne!(got_a, got_b, "the two accounts must not share a token");

    // An account with no Slack install is `Ok(None)` — a real product state
    // that must stay distinguishable from a failure (the callers' `Ok(None)`
    // arms are a deliberate, permanent "nothing to do").
    let orphan_user: Uuid =
        sqlx::query_scalar("INSERT INTO users(email) VALUES ('slack-orphan@x.co') RETURNING id")
            .fetch_one(&probe.owner)
            .await
            .expect("seed token-less user");
    let orphan: Uuid =
        sqlx::query_scalar("INSERT INTO accounts(owner_user_id) VALUES ($1) RETURNING id")
            .bind(orphan_user)
            .fetch_one(&probe.owner)
            .await
            .expect("seed a token-less account");
    let none = test_support_slack_bot_token(&probe.app_pool, orphan)
        .await
        .expect("a token-less account must be Ok(None), not Err");
    assert!(none.is_none(), "no Slack install ⇒ Ok(None), got {none:?}");

    probe.teardown().await;
}

/// The isolation property: holding another workspace's `account_id` (which a
/// webhook resolves freely — `resolve_oauth_account_id` is a SECURITY DEFINER
/// bootstrap and cross-account by design) buys nothing. The policy filters the
/// row, and the DEK is per-account anyway, so neither layer alone is load
/// bearing here — both are asserted.
#[tokio::test]
async fn slack_bot_token_lookup_cannot_cross_accounts() {
    let Some(probe) = RlsProbe::setup("orgt_slack_token_rls_cross").await else {
        eprintln!("skipping slack_bot_token_lookup_cannot_cross_accounts: set ORGT_TEST_BASE_URL");
        return;
    };
    let kek = set_master_kek();
    let (_token_a, dek_a) = seed_slack_token(&probe, &probe.a, &kek, "a").await;
    let (token_b, _dek_b) = seed_slack_token(&probe, &probe.b, &kek, "b").await;

    // Layer 1 — RLS: scoped to A, B's token row is simply not there.
    {
        let mut tx = probe.scoped_tx(probe.a.account_id).await;
        let leaked: Option<Vec<u8>> = sqlx::query_scalar(
            "SELECT access_token FROM oauth_tokens WHERE account_id = $1 AND provider = 'slack'",
        )
        .bind(probe.b.account_id)
        .fetch_optional(&mut *tx)
        .await
        .expect("cross-account oauth_tokens probe runs");
        assert!(
            leaked.is_none(),
            "scoped to A, B's oauth_tokens row must be invisible — got {} bytes",
            leaked.map(|v| v.len()).unwrap_or(0)
        );
        tx.rollback().await.ok();
    }

    // Layer 2 — the per-account DEK: even the ciphertext, handed over, does
    // not decrypt under A's key.
    let b_ciphertext: Vec<u8> = sqlx::query_scalar(
        "SELECT access_token FROM oauth_tokens WHERE account_id = $1 AND provider = 'slack'",
    )
    .bind(probe.b.account_id)
    .fetch_one(&probe.owner)
    .await
    .expect("read B's ciphertext via the BYPASSRLS owner pool");
    assert!(
        coralswarm_core::oauth_token::decrypt_token(&dek_a, &b_ciphertext).is_err(),
        "A's DEK must not decrypt B's bot token"
    );

    // End to end: the production entry point, called with B's account id from
    // the same pool, returns B's token and ONLY B's token — the resolver being
    // cross-account is not itself authorization, but it is also not a leak:
    // each id serves exactly its own row.
    let got = test_support_slack_bot_token(&probe.app_pool, probe.b.account_id)
        .await
        .expect("lookup succeeds");
    assert_eq!(got.as_deref(), Some(token_b.as_str()));

    probe.teardown().await;
}

/// The negative control. `get_slack_bot_token_pub`'s failure mode is invisible
/// from the outside (both callers answer `200`/return quietly), so this pins
/// the underlying mechanism directly: the exact statement it issues, run as
/// `coralswarm_app` WITHOUT `app.account_id` set — which is precisely what
/// `.fetch_optional(pool)` on a bare pool did — must HARD ERROR, with the
/// SQLSTATE an RLS scope failure actually raises.
///
/// Note the failure is an ERROR, not an empty result: that matters, because
/// `Ok(None)` would have been indistinguishable from "this workspace has no
/// Slack install" and the callers' `Ok(None)` arms would have been reached
/// instead — a silent, permanent, retry-free stop.
#[tokio::test]
async fn unscoped_slack_token_read_hard_errors_negative_control() {
    let Some(probe) = RlsProbe::setup("orgt_slack_token_rls_negctl").await else {
        eprintln!("skipping unscoped_slack_token_read_hard_errors_negative_control: set ORGT_TEST_BASE_URL");
        return;
    };
    let kek = set_master_kek();
    let _token_a = seed_slack_token(&probe, &probe.a, &kek, "a").await; // (plaintext, dek), both unused here

    const QUERY: &str =
        "SELECT account_id FROM oauth_tokens WHERE account_id = $1 AND provider = 'slack' LIMIT 1";

    let mut tx = probe
        .app_pool
        .begin()
        .await
        .expect("begin unscoped tx as coralswarm_app");
    let err = sqlx::query_scalar::<_, Uuid>(QUERY)
        .bind(probe.a.account_id)
        .fetch_optional(&mut *tx)
        .await
        .expect_err(
            "the UNSCOPED oauth_tokens read must hard-error as coralswarm_app. If this ever \
             passes, 0083's policy stopped failing closed and this whole suite proves nothing.",
        );
    let sqlstate = err
        .as_database_error()
        .and_then(|d| d.code())
        .map(|c| c.to_string());
    assert!(
        matches!(sqlstate.as_deref(), Some("42704") | Some("22P02")),
        "an RLS scope failure raises 42704 (unrecognized configuration parameter) or \
         22P02 (invalid uuid input syntax), got {sqlstate:?} ({err})"
    );
    tx.rollback().await.ok();

    // …and scoped, the identical statement finds the row. Same query, same
    // role, same connection pool — the ONLY difference is the scope, which is
    // exactly the delta this fix introduces.
    probe
        .assert_scoped_returns_own_rows(
            probe.a.account_id,
            QUERY,
            &[probe.a.account_id],
            &[probe.a.account_id],
        )
        .await;

    probe.teardown().await;
}

//! RLS proofs for the org-tenancy B.3b surface: the signature-authenticated
//! webhooks, the capability-token share viewers, the cross-DB promotion
//! service's consumer-plane half, and the eval harness.
//!
//! What makes this group different from B.3a is that most of these call sites
//! have **no session to scope from**. A Slack/Teams/Graph webhook proves
//! itself with a provider signature; a share viewer proves itself with an
//! opaque capability token; promotion starts from a bare `node_id`. Each must
//! DISCOVER an owning account before `begin_account_scoped` can be called at
//! all — the chicken-and-egg that migrations `0084`/`0086`'s narrow
//! `SECURITY DEFINER` resolvers exist to break.
//!
//! So this file proves two things, not one:
//!
//!   A. the resolvers behave as *bootstraps* — they answer with a bare id
//!      from an unscoped connection (which is the whole point), they return
//!      NOTHING for a token/id that shouldn't resolve, and they never hand
//!      back a content column;
//!   B. the follow-up reads are still fully scoped — holding a correctly
//!      resolved id for account B buys an attacker nothing from account A's
//!      scope (cross-account-denied), and running them unscoped hard-errors.
//!
//! Env-gated, like every `*_pg.rs` here:
//!
//!   ORGT_TEST_BASE_URL=postgres://alex@localhost:5432/postgres \
//!     cargo test -p coralswarm-backend --test webhook_share_rls_b3_pg

mod support;

use support::rls_probe::{AccountFixture, RlsProbe};
use uuid::Uuid;

struct ShareFixture {
    share_id: Uuid,
    token: String,
    revoked_token: String,
    revoked_share_id: Uuid,
    huddle_id: Uuid,
    workspace_id: String,
}

async fn seed(probe: &RlsProbe, f: &AccountFixture, tag: &str) -> ShareFixture {
    let owner = &probe.owner;

    sqlx::query(
        "INSERT INTO meeting_transcripts (graph_id, source_id, raw_inline) VALUES ($1, $2, 'hello')",
    )
    .bind(f.graph_id)
    .bind(format!("meeting-{tag}"))
    .execute(owner)
    .await
    .expect("seed meeting_transcripts");

    let token = format!("tok-active-{tag}");
    let share_id: Uuid = sqlx::query_scalar(
        "INSERT INTO meeting_shares (graph_id, source_id, token, visibility, created_by) \
         VALUES ($1, $2, $3, 'link', $4) RETURNING id",
    )
    .bind(f.graph_id)
    .bind(format!("meeting-{tag}"))
    .bind(&token)
    .bind(f.user_id)
    .fetch_one(owner)
    .await
    .expect("seed meeting_shares");

    let revoked_token = format!("tok-revoked-{tag}");
    let revoked_share_id: Uuid = sqlx::query_scalar(
        "INSERT INTO meeting_shares (graph_id, source_id, token, visibility, created_by, revoked_at) \
         VALUES ($1, $2, $3, 'link', $4, now()) RETURNING id",
    )
    .bind(f.graph_id)
    .bind(format!("meeting-{tag}"))
    .bind(&revoked_token)
    .bind(f.user_id)
    .fetch_one(owner)
    .await
    .expect("seed revoked meeting_share");

    let workspace_id = format!("T-{tag}");
    sqlx::query(
        "INSERT INTO slack_integrations (account_id, workspace_id, workspace_name, bot_token) \
         VALUES ($1, $2, 'WS', 'xoxb-secret-do-not-leak')",
    )
    .bind(f.account_id)
    .bind(&workspace_id)
    .execute(owner)
    .await
    .expect("seed slack_integrations");

    // `slack_huddle_captures` is a terminal-outcome ledger, not a queue
    // (migration `0102_slack_huddle_captures_ledger` / tenant `0014`): seed a
    // row recording a decided skip (any non-NULL `skip_reason` is a valid
    // terminal row for RLS purposes — this fixture only needs a row to exist
    // under the seeded account, not to exercise the ingest path).
    let huddle_id: Uuid = sqlx::query_scalar(
        "INSERT INTO slack_huddle_captures \
         (account_id, channel_id, huddle_thread_ts, skip_reason) \
         VALUES ($1, $2, '1.1', 'test fixture row') RETURNING id",
    )
    .bind(f.account_id)
    .bind(format!("C-{tag}"))
    .fetch_one(owner)
    .await
    .expect("seed slack_huddle_captures");

    ShareFixture { share_id, token, revoked_token, revoked_share_id, huddle_id, workspace_id }
}

/// The 0086 bootstrap resolvers used by B.3b: correct answers unscoped, the
/// right NULLs, and bare ids only.
#[tokio::test]
async fn bootstrap_resolvers_answer_unscoped_and_return_ids_only() {
    let Some(probe) = RlsProbe::setup("orgt_rls_b3b_bootstrap").await else {
        eprintln!("skipping bootstrap_resolvers_answer_unscoped_and_return_ids_only: set ORGT_TEST_BASE_URL");
        return;
    };
    let a = seed(&probe, &probe.a, "a").await;
    let b = seed(&probe, &probe.b, "b").await;
    // Unscoped app-role pool — exactly how a webhook/viewer calls these.
    let pool = &probe.app_pool;

    // resolve_meeting_share_account_id: ACTIVE shares only. A revoked token
    // and an unknown token must both resolve to NULL, so `routes/shares.rs`'s
    // generic-404 contract is preserved (no live/dead distinction leaks).
    for (token, want) in [
        (a.token.as_str(), Some(probe.a.account_id)),
        (b.token.as_str(), Some(probe.b.account_id)),
        (a.revoked_token.as_str(), None),
        ("tok-never-existed", None),
    ] {
        let got: Option<Uuid> = sqlx::query_scalar("SELECT resolve_meeting_share_account_id($1)")
            .bind(token)
            .fetch_one(pool)
            .await
            .expect("resolver must run unscoped as coralswarm_app");
        assert_eq!(got, want, "resolve_meeting_share_account_id({token:?})");
    }

    // resolve_meeting_share_graph_id: ANY share, active OR revoked — the
    // revoke path must keep working on an already-revoked share.
    for (share_id, want) in [
        (a.share_id, Some(probe.a.graph_id)),
        (a.revoked_share_id, Some(probe.a.graph_id)),
        (Uuid::new_v4(), None),
    ] {
        let got: Option<Uuid> = sqlx::query_scalar("SELECT resolve_meeting_share_graph_id($1)")
            .bind(share_id)
            .fetch_one(pool)
            .await
            .expect("resolver must run unscoped");
        assert_eq!(got, want, "resolve_meeting_share_graph_id({share_id})");
    }

    // resolve_slack_integration_account_id: keyed on workspace_id, NOT on
    // oauth_tokens — legacy Slack installs write only `slack_integrations`,
    // and resolving them through the oauth-keyed resolver would orphan them.
    let got: Option<Uuid> = sqlx::query_scalar("SELECT resolve_slack_integration_account_id($1)")
        .bind(&b.workspace_id)
        .fetch_one(pool)
        .await
        .expect("resolver must run unscoped");
    assert_eq!(got, Some(probe.b.account_id));
    let got: Option<Uuid> = sqlx::query_scalar("SELECT resolve_slack_integration_account_id($1)")
        .bind("T-unknown")
        .fetch_one(pool)
        .await
        .expect("resolver must run unscoped");
    assert_eq!(got, None);

    // resolve_node_account_id: promotion's source bootstrap.
    let got: Option<Uuid> = sqlx::query_scalar("SELECT resolve_node_account_id($1)")
        .bind(probe.a.node_id)
        .fetch_one(pool)
        .await
        .expect("resolver must run unscoped");
    assert_eq!(got, Some(probe.a.account_id));

    probe.teardown().await;
}

/// Having bootstrapped an id is NOT authorization: every follow-up read is
/// still policy-filtered, and running it with no scope at all hard-errors.
#[tokio::test]
async fn b3b_reads_stay_scoped_after_bootstrap() {
    let Some(probe) = RlsProbe::setup("orgt_rls_b3b_reads").await else {
        eprintln!("skipping b3b_reads_stay_scoped_after_bootstrap: set ORGT_TEST_BASE_URL");
        return;
    };
    let a = seed(&probe, &probe.a, "a").await;
    let b = seed(&probe, &probe.b, "b").await;

    // ── meeting_shares (routes/shares.rs viewers + revoke) ──────────────────
    probe
        .assert_scoped_returns_own_rows(
            probe.a.account_id,
            "SELECT id FROM meeting_shares WHERE id = $1",
            &[a.share_id],
            &[a.share_id],
        )
        .await;
    // The real property: A resolved B's token correctly (the resolver is
    // cross-account by design) but reading the row under A's scope yields
    // nothing — so a stolen/guessed id can never be turned into content.
    probe
        .assert_cross_account_returns_nothing(
            probe.a.account_id,
            "SELECT id FROM meeting_shares WHERE id = $1",
            &[b.share_id],
        )
        .await;
    probe.assert_unscoped_hard_errors("SELECT id FROM meeting_shares", &[]).await;

    // ── meeting_transcripts (build_shared_payload / create_share gate) ──────
    probe
        .assert_cross_account_returns_nothing(
            probe.a.account_id,
            "SELECT graph_id FROM meeting_transcripts WHERE graph_id = $1",
            &[probe.b.graph_id],
        )
        .await;
    probe
        .assert_unscoped_hard_errors("SELECT graph_id FROM meeting_transcripts", &[])
        .await;

    // ── slack_huddle_captures (routes/slack_events.rs webhook writes) ───────
    probe
        .assert_scoped_returns_own_rows(
            probe.b.account_id,
            "SELECT id FROM slack_huddle_captures WHERE account_id = $1",
            &[probe.b.account_id],
            &[b.huddle_id],
        )
        .await;
    probe
        .assert_cross_account_returns_nothing(
            probe.b.account_id,
            "SELECT id FROM slack_huddle_captures WHERE id = $1",
            &[a.huddle_id],
        )
        .await;
    probe
        .assert_unscoped_hard_errors("SELECT id FROM slack_huddle_captures", &[])
        .await;

    // ── nodes / node_atoms / atoms (services/promotion.rs source reads) ─────
    probe
        .assert_cross_account_returns_nothing(
            probe.a.account_id,
            "SELECT node_id FROM nodes WHERE node_id = $1",
            &[probe.b.node_id],
        )
        .await;
    probe
        .assert_cross_account_returns_nothing(
            probe.a.account_id,
            "SELECT atom_id FROM atoms WHERE graph_id = $1",
            &[probe.b.graph_id],
        )
        .await;

    // The bot token a Slack webhook re-reads under scope: A holding B's
    // workspace id gets nothing back.
    {
        let mut tx = probe.scoped_tx(probe.a.account_id).await;
        let leaked: Option<String> = sqlx::query_scalar(
            "SELECT bot_token FROM slack_integrations WHERE workspace_id = $1",
        )
        .bind(&b.workspace_id)
        .fetch_optional(&mut *tx)
        .await
        .expect("cross-account bot_token probe");
        assert!(
            leaked.is_none(),
            "a resolved workspace id must not yield another tenant's bot token, got {leaked:?}"
        );
        tx.rollback().await.ok();
    }

    probe.teardown().await;
}

/// Write-side denial for this group: a forged-owner INSERT is refused by the
/// policy's `WITH CHECK` with SQLSTATE 42501, and a scoped UPDATE aimed at
/// another account's share simply affects zero rows (never someone else's).
#[tokio::test]
async fn b3b_writes_cannot_cross_accounts() {
    let Some(probe) = RlsProbe::setup("orgt_rls_b3b_writes").await else {
        eprintln!("skipping b3b_writes_cannot_cross_accounts: set ORGT_TEST_BASE_URL");
        return;
    };
    let _a = seed(&probe, &probe.a, "a").await;
    let b = seed(&probe, &probe.b, "b").await;

    // Forged huddle capture for account B, from A's scope.
    let mut tx = probe.scoped_tx(probe.a.account_id).await;
    let res = sqlx::query(
        "INSERT INTO slack_huddle_captures \
         (account_id, channel_id, huddle_thread_ts, skip_reason) \
         VALUES ($1, 'C-forged', '9.9', 'forged row')",
    )
    .bind(probe.b.account_id)
    .execute(&mut *tx)
    .await;
    let err = res.expect_err("cross-account slack_huddle_captures INSERT must be denied");
    assert_eq!(
        err.as_database_error().and_then(|e| e.code()).as_deref(),
        Some("42501"),
        "expected an RLS WITH CHECK denial (42501), got {err}"
    );
    tx.rollback().await.ok();

    // Revoking B's share from A's scope: the row is invisible, so zero rows.
    let mut tx = probe.scoped_tx(probe.a.account_id).await;
    let affected = sqlx::query(
        "UPDATE meeting_shares SET revoked_at = now() WHERE id = $1 AND revoked_at IS NULL",
    )
    .bind(b.share_id)
    .execute(&mut *tx)
    .await
    .expect("scoped update runs")
    .rows_affected();
    assert_eq!(affected, 0, "account A must not be able to revoke account B's share");
    tx.commit().await.expect("commit no-op update");

    // …and B's share is genuinely still active.
    let still_active: Option<Uuid> =
        sqlx::query_scalar("SELECT resolve_meeting_share_account_id($1)")
            .bind(&b.token)
            .fetch_one(&probe.app_pool)
            .await
            .expect("resolver");
    assert_eq!(
        still_active,
        Some(probe.b.account_id),
        "B's share must survive A's attempted revoke"
    );

    probe.teardown().await;
}

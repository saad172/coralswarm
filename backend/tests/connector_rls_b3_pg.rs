//! RLS proofs for the org-tenancy B.3 connector/integration surface.
//!
//! These are the ~7 RLS-covered tables that have **no `Store` methods at
//! all** — every query against them is inline sqlx living in
//! `connectors/{microsoft,slack,state}.rs`,
//! `routes/{oauth,slack,sync,teams_ingest,workflows,connectors,calendar}.rs`,
//! `llm/usage.rs` and `mcp/audit.rs`. There is therefore no `_scoped` Store
//! sibling to lean on; the conversion binds each statement to a
//! `tenancy::scope::begin_account_scoped` transaction in place, and this file
//! proves the three properties that matters for, against the REAL
//! `coralswarm_app` role (migration `0083_consumer_rls`):
//!
//!   1. scoped to account A, the converted query shape returns exactly A's
//!      rows;
//!   2. scoped to account A, the same shape aimed at B's ids returns NOTHING
//!      (the actual security property — no cross-account read/write);
//!   3. unscoped, it HARD-ERRORS rather than quietly returning nothing (what
//!      catches a future refactor that drops the scoping after OPS-4).
//!
//! Plus: the three `SECURITY DEFINER` sweep resolvers added by migration
//! `0086_webhook_share_bootstrap_functions` really are cross-account (that is
//! their entire job) and really do return bare ids only.
//!
//! Env-gated, like every `*_pg.rs` here:
//!
//!   ORGT_TEST_BASE_URL=postgres://alex@localhost:5432/postgres \
//!     cargo test -p coralswarm-backend --test connector_rls_b3_pg

mod support;

use support::rls_probe::{AccountFixture, RlsProbe};
use uuid::Uuid;

/// Seed one row in each of this PR's tables for `f`, returning the ids the
/// probes assert on. Seeded through the OWNER pool so the fixture itself is
/// never subject to the policies under test.
struct ConnectorFixture {
    oauth_id: Uuid,
    slack_id: Uuid,
    teams_watch_id: Uuid,
    outlook_watch_id: Uuid,
    subscription_row_id: Uuid,
    subscription_id: String,
    workflow_id: Uuid,
    llm_usage_id: Uuid,
    mcp_call_id: Uuid,
}

async fn seed_connector_rows(
    probe: &RlsProbe,
    f: &AccountFixture,
    tag: &str,
) -> ConnectorFixture {
    let owner = &probe.owner;

    let oauth_id: Uuid = sqlx::query_scalar(
        "INSERT INTO oauth_tokens (account_id, provider, scope, access_token, \
         provider_workspace_id) VALUES ($1, 'microsoft', 'Mail.Read', $2, $3) RETURNING id",
    )
    .bind(f.account_id)
    .bind(b"cipher".to_vec())
    .bind(format!("ws-{tag}"))
    .fetch_one(owner)
    .await
    .expect("seed oauth_tokens");

    let slack_id: Uuid = sqlx::query_scalar(
        "INSERT INTO slack_integrations (account_id, workspace_id, workspace_name, bot_token) \
         VALUES ($1, $2, 'Probe WS', 'xoxb-probe') RETURNING id",
    )
    .bind(f.account_id)
    .bind(format!("T-{tag}"))
    .fetch_one(owner)
    .await
    .expect("seed slack_integrations");

    let teams_watch_id: Uuid = sqlx::query_scalar(
        "INSERT INTO teams_watched_resources \
         (account_id, user_id, graph_id, kind, team_id, channel_id, display_name) \
         VALUES ($1, $2, $3, 'channel', $4, $5, 'General') RETURNING id",
    )
    .bind(f.account_id)
    .bind(f.user_id)
    .bind(f.graph_id)
    .bind(format!("team-{tag}"))
    .bind(format!("chan-{tag}"))
    .fetch_one(owner)
    .await
    .expect("seed teams_watched_resources");

    let outlook_watch_id: Uuid = sqlx::query_scalar(
        "INSERT INTO outlook_watched_conversations \
         (account_id, graph_id, ingested_by, conversation_id) VALUES ($1, $2, $3, $4) RETURNING id",
    )
    .bind(f.account_id)
    .bind(f.graph_id)
    .bind(f.user_id)
    .bind(format!("conv-{tag}"))
    .fetch_one(owner)
    .await
    .expect("seed outlook_watched_conversations");

    let subscription_id = format!("sub-{tag}");
    let subscription_row_id: Uuid = sqlx::query_scalar(
        "INSERT INTO graph_subscriptions \
         (account_id, subscription_id, resource, client_state, expires_at) \
         VALUES ($1, $2, 'me/mailFolders(''Inbox'')/messages', 'secret-do-not-leak', \
                 now() + interval '1 hour') RETURNING id",
    )
    .bind(f.account_id)
    .bind(&subscription_id)
    .fetch_one(owner)
    .await
    .expect("seed graph_subscriptions");

    let workflow_id: Uuid = sqlx::query_scalar(
        "INSERT INTO outlook_workflows (account_id, user_id, graph_id, name, filter) \
         VALUES ($1, $2, $3, 'probe rule', '{}'::jsonb) RETURNING id",
    )
    .bind(f.account_id)
    .bind(f.user_id)
    .bind(f.graph_id)
    .fetch_one(owner)
    .await
    .expect("seed outlook_workflows");

    let llm_usage_id: Uuid = sqlx::query_scalar(
        "INSERT INTO llm_usage (account_id, graph_id, task, model) \
         VALUES ($1, $2, 'probe', 'claude') RETURNING id",
    )
    .bind(f.account_id)
    .bind(f.graph_id)
    .fetch_one(owner)
    .await
    .expect("seed llm_usage");

    let mcp_call_id: Uuid = sqlx::query_scalar(
        "INSERT INTO mcp_tool_calls (account_id, user_id, tool, outcome) \
         VALUES ($1, $2, 'search_atoms', 'ok') RETURNING id",
    )
    .bind(f.account_id)
    .bind(f.user_id)
    .fetch_one(owner)
    .await
    .expect("seed mcp_tool_calls");

    ConnectorFixture {
        oauth_id,
        slack_id,
        teams_watch_id,
        outlook_watch_id,
        subscription_row_id,
        subscription_id,
        workflow_id,
        llm_usage_id,
        mcp_call_id,
    }
}

/// Every converted READ shape in this PR: own-rows / cross-account-denied /
/// unscoped-hard-error, one table at a time.
#[tokio::test]
async fn b3_connector_tables_are_account_scoped() {
    let Some(probe) = RlsProbe::setup("orgt_rls_b3_connectors").await else {
        eprintln!("skipping b3_connector_tables_are_account_scoped: set ORGT_TEST_BASE_URL");
        return;
    };
    let a = seed_connector_rows(&probe, &probe.a, "a").await;
    let b = seed_connector_rows(&probe, &probe.b, "b").await;

    // ── oauth_tokens — routes/connectors.rs::{load_creds,load_creds_bg},
    // routes/oauth.rs::{list_connections,delete_connection},
    // routes/calendar.rs::connection_info, connectors/state.rs::connection_map
    probe
        .assert_scoped_returns_own_rows(
            probe.a.account_id,
            "SELECT id FROM oauth_tokens WHERE account_id = $1 AND provider = 'microsoft'",
            &[probe.a.account_id],
            &[a.oauth_id],
        )
        .await;
    // The IDOR pivot: A's session, B's account_id in the bind. RLS wins.
    probe
        .assert_cross_account_returns_nothing(
            probe.a.account_id,
            "SELECT id FROM oauth_tokens WHERE account_id = $1",
            &[probe.b.account_id],
        )
        .await;
    probe.assert_unscoped_hard_errors("SELECT id FROM oauth_tokens", &[]).await;

    // ── slack_integrations — routes/slack.rs::{list_integrations,...},
    // connectors/slack.rs::after_token_stored
    probe
        .assert_scoped_returns_own_rows(
            probe.b.account_id,
            "SELECT id FROM slack_integrations WHERE account_id = $1",
            &[probe.b.account_id],
            &[b.slack_id],
        )
        .await;
    probe
        .assert_cross_account_returns_nothing(
            probe.b.account_id,
            "SELECT id FROM slack_integrations WHERE id = $1",
            &[a.slack_id],
        )
        .await;
    probe.assert_unscoped_hard_errors("SELECT id FROM slack_integrations", &[]).await;

    // ── teams_watched_resources — routes/teams_ingest.rs (list/delete/watch),
    // connectors/microsoft.rs::{teams_watched_*,describe_containers}. Probed by
    // graph_id, the shape `describe_containers` uses with no account_id at all
    // — exactly the read that has nothing but RLS holding it in place.
    probe
        .assert_scoped_returns_own_rows(
            probe.a.account_id,
            "SELECT id FROM teams_watched_resources WHERE graph_id = $1",
            &[probe.a.graph_id],
            &[a.teams_watch_id],
        )
        .await;
    probe
        .assert_cross_account_returns_nothing(
            probe.a.account_id,
            "SELECT id FROM teams_watched_resources WHERE graph_id = $1",
            &[probe.b.graph_id],
        )
        .await;
    probe
        .assert_unscoped_hard_errors("SELECT id FROM teams_watched_resources", &[])
        .await;

    // ── outlook_watched_conversations — connectors/microsoft.rs watch helpers
    // + describe_containers (again, graph_id-only shape).
    probe
        .assert_scoped_returns_own_rows(
            probe.b.account_id,
            "SELECT id FROM outlook_watched_conversations WHERE graph_id = $1",
            &[probe.b.graph_id],
            &[b.outlook_watch_id],
        )
        .await;
    probe
        .assert_cross_account_returns_nothing(
            probe.b.account_id,
            "SELECT id FROM outlook_watched_conversations WHERE graph_id = $1",
            &[probe.a.graph_id],
        )
        .await;
    probe
        .assert_unscoped_hard_errors("SELECT id FROM outlook_watched_conversations", &[])
        .await;

    // ── graph_subscriptions — connectors/microsoft.rs::{subscription_by_id,
    // subscription_expiry, set_subscription_expiry, delete_subscription_row}.
    // The webhook shape keys on the PROVIDER's subscription id and never
    // mentions an account, so the scope is the only thing standing between an
    // attacker-chosen subscription id and another tenant's `client_state`.
    probe
        .assert_scoped_returns_own_rows(
            probe.a.account_id,
            "SELECT id FROM graph_subscriptions",
            &[],
            &[a.subscription_row_id],
        )
        .await;
    // The webhook's own shape binds a TEXT subscription id, which the
    // uuid-only helper can't express — do that one inline.
    {
        let mut tx = probe.scoped_tx(probe.a.account_id).await;
        let leaked: Option<String> = sqlx::query_scalar(
            "SELECT client_state FROM graph_subscriptions WHERE subscription_id = $1",
        )
        .bind(&b.subscription_id)
        .fetch_optional(&mut *tx)
        .await
        .expect("cross-account subscription probe");
        assert!(
            leaked.is_none(),
            "account A scoped read of account B's subscription must return nothing, \
             got client_state {leaked:?}"
        );
        tx.rollback().await.ok();
    }
    probe.assert_unscoped_hard_errors("SELECT id FROM graph_subscriptions", &[]).await;

    // ── outlook_workflows — routes/workflows.rs (CRUD + enabled_for_account)
    probe
        .assert_scoped_returns_own_rows(
            probe.a.account_id,
            "SELECT id FROM outlook_workflows WHERE account_id = $1 AND enabled",
            &[probe.a.account_id],
            &[a.workflow_id],
        )
        .await;
    probe
        .assert_cross_account_returns_nothing(
            probe.a.account_id,
            "SELECT id FROM outlook_workflows WHERE id = $1",
            &[b.workflow_id],
        )
        .await;
    probe.assert_unscoped_hard_errors("SELECT id FROM outlook_workflows", &[]).await;

    // ── llm_usage — llm/usage.rs::record
    probe
        .assert_scoped_returns_own_rows(
            probe.b.account_id,
            "SELECT id FROM llm_usage WHERE account_id = $1",
            &[probe.b.account_id],
            &[b.llm_usage_id],
        )
        .await;
    probe
        .assert_cross_account_returns_nothing(
            probe.b.account_id,
            "SELECT id FROM llm_usage WHERE id = $1",
            &[a.llm_usage_id],
        )
        .await;
    probe.assert_unscoped_hard_errors("SELECT id FROM llm_usage", &[]).await;

    // ── mcp_tool_calls — mcp/audit.rs::record
    probe
        .assert_scoped_returns_own_rows(
            probe.a.account_id,
            "SELECT id FROM mcp_tool_calls WHERE account_id = $1",
            &[probe.a.account_id],
            &[a.mcp_call_id],
        )
        .await;
    probe
        .assert_cross_account_returns_nothing(
            probe.a.account_id,
            "SELECT id FROM mcp_tool_calls WHERE id = $1",
            &[b.mcp_call_id],
        )
        .await;
    probe.assert_unscoped_hard_errors("SELECT id FROM mcp_tool_calls", &[]).await;

    probe.teardown().await;
}

/// The WRITE direction — the half a read-only probe can't see. Under
/// `coralswarm_app` an INSERT whose `account_id` isn't the scoped one is
/// rejected by the policy's `WITH CHECK` with SQLSTATE **42501**
/// (`insufficient_privilege`), NOT the `23503` FK violation an ownership
/// mismatch used to produce.
#[tokio::test]
async fn b3_connector_writes_cannot_cross_accounts() {
    let Some(probe) = RlsProbe::setup("orgt_rls_b3_connector_writes").await else {
        eprintln!("skipping b3_connector_writes_cannot_cross_accounts: set ORGT_TEST_BASE_URL");
        return;
    };

    // Scoped to A, try to write a slack_integrations row owned by B
    // (routes/slack.rs::oauth_callback's upsert shape).
    let mut tx = probe.scoped_tx(probe.a.account_id).await;
    let res = sqlx::query(
        "INSERT INTO slack_integrations (account_id, workspace_id, workspace_name, bot_token) \
         VALUES ($1, 'T-forged', 'Forged', 'xoxb-forged')",
    )
    .bind(probe.b.account_id)
    .execute(&mut *tx)
    .await;
    let err = res.expect_err("cross-account slack_integrations INSERT must be denied");
    assert_eq!(
        err.as_database_error().and_then(|e| e.code()).as_deref(),
        Some("42501"),
        "expected an RLS WITH CHECK denial (42501), got {err}"
    );
    tx.rollback().await.ok();

    // Same for an oauth_tokens upsert (routes/oauth.rs::store_token_bundle).
    let mut tx = probe.scoped_tx(probe.a.account_id).await;
    let res = sqlx::query(
        "INSERT INTO oauth_tokens (account_id, provider, scope, access_token) \
         VALUES ($1, 'microsoft', 'Mail.Read', $2)",
    )
    .bind(probe.b.account_id)
    .bind(b"cipher".to_vec())
    .execute(&mut *tx)
    .await;
    let err = res.expect_err("cross-account oauth_tokens INSERT must be denied");
    assert_eq!(
        err.as_database_error().and_then(|e| e.code()).as_deref(),
        Some("42501"),
        "expected an RLS WITH CHECK denial (42501), got {err}"
    );
    tx.rollback().await.ok();

    // And the write is a no-op, not a partial: A's own INSERT still works.
    let mut tx = probe.scoped_tx(probe.a.account_id).await;
    sqlx::query(
        "INSERT INTO oauth_tokens (account_id, provider, scope, access_token) \
         VALUES ($1, 'microsoft', 'Mail.Read', $2)",
    )
    .bind(probe.a.account_id)
    .bind(b"cipher".to_vec())
    .execute(&mut *tx)
    .await
    .expect("own-account oauth_tokens INSERT must succeed under scope");
    tx.commit().await.expect("commit own-account write");

    probe.teardown().await;
}

/// Migration 0086's three sweep resolvers: cross-account by design (that is
/// the point — they are the work-set discovery the renewal/backstop sweeps in
/// `workers/graph_subscriptions.rs` start from), executable by
/// `coralswarm_app` with NO `app.account_id` set, and returning bare ids only.
#[tokio::test]
async fn sweep_resolvers_are_cross_account_and_id_only() {
    let Some(probe) = RlsProbe::setup("orgt_rls_b3_sweeps").await else {
        eprintln!("skipping sweep_resolvers_are_cross_account_and_id_only: set ORGT_TEST_BASE_URL");
        return;
    };
    let a = seed_connector_rows(&probe, &probe.a, "a").await;
    let b = seed_connector_rows(&probe, &probe.b, "b").await;

    // Unscoped app-role connection — exactly how the sweeps call these.
    let pool = &probe.app_pool;

    let accounts: Vec<(Uuid,)> =
        sqlx::query_as("SELECT * FROM resolve_watched_conversation_accounts()")
            .fetch_all(pool)
            .await
            .expect("resolve_watched_conversation_accounts must run unscoped");
    let mut got: Vec<Uuid> = accounts.into_iter().map(|(x,)| x).collect();
    got.sort();
    let mut want = vec![probe.a.account_id, probe.b.account_id];
    want.sort();
    assert_eq!(got, want, "the Outlook backstop sweep must see BOTH accounts");

    let accounts: Vec<(Uuid,)> = sqlx::query_as("SELECT * FROM resolve_teams_watched_accounts()")
        .fetch_all(pool)
        .await
        .expect("resolve_teams_watched_accounts must run unscoped");
    let mut got: Vec<Uuid> = accounts.into_iter().map(|(x,)| x).collect();
    got.sort();
    assert_eq!(got, want, "the Teams backstop sweep must see BOTH accounts");

    let due: Vec<(String, Uuid)> = sqlx::query_as("SELECT * FROM resolve_expiring_subscriptions($1)")
        .bind(chrono::Utc::now() + chrono::Duration::hours(2))
        .fetch_all(pool)
        .await
        .expect("resolve_expiring_subscriptions must run unscoped");
    let mut got: Vec<String> = due.iter().map(|(s, _)| s.clone()).collect();
    got.sort();
    let mut want_subs = vec![a.subscription_id.clone(), b.subscription_id.clone()];
    want_subs.sort();
    assert_eq!(got, want_subs, "the renewal sweep must see BOTH accounts' due subscriptions");

    // Cutoff still filters (the resolver is not a blanket "everything" read).
    let none: Vec<(String, Uuid)> =
        sqlx::query_as("SELECT * FROM resolve_expiring_subscriptions($1)")
            .bind(chrono::Utc::now() - chrono::Duration::hours(2))
            .fetch_all(pool)
            .await
            .expect("resolve_expiring_subscriptions with a past cutoff");
    assert!(none.is_empty(), "nothing expires before a past cutoff — got {none:?}");

    // …and the follow-up detail read is STILL scoped: holding B's
    // subscription id, A cannot read its client_state.
    let mut tx = probe.scoped_tx(probe.a.account_id).await;
    let leaked: Option<String> =
        sqlx::query_scalar("SELECT client_state FROM graph_subscriptions WHERE subscription_id = $1")
            .bind(&b.subscription_id)
            .fetch_optional(&mut *tx)
            .await
            .expect("scoped detail read");
    assert!(leaked.is_none(), "sweep id + wrong scope must not yield another tenant's secret");
    tx.rollback().await.ok();

    probe.teardown().await;
}

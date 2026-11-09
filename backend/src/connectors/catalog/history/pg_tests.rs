//! Postgres tests for import history, driven through the real
//! `POST /v1/connectors/{provider}/sources/{source}/backfill` handler as
//! `coralswarm_app` (the production role, subject to RLS).
//!
//! Gated on `RELINFER_TEST_DATABASE_URL`, a migrated CONTROL database; without it
//! each test prints a skip line and returns. A skip is NOT a pass. On a
//! personal account the content plane is the control database, so `jobs` and
//! `github_watched_repos` are in the same database the test seeds.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::Router;
use serde_json::{json, Value};
use sqlx::PgPool;
use tower::ServiceExt;
use uuid::Uuid;

use crate::billing::test_support::{pg_test_pool, seed_personal_account, user_context};

async fn app_pool(owner: &PgPool) -> PgPool {
    use sqlx::postgres::PgPoolOptions;
    static ROLE: tokio::sync::OnceCell<()> = tokio::sync::OnceCell::const_new();
    ROLE.get_or_init(|| async {
        sqlx::query("ALTER ROLE coralswarm_app WITH LOGIN PASSWORD 'app_test_pw'")
            .execute(owner)
            .await
            .unwrap();
    })
    .await;
    let opts = owner.connect_options().as_ref().clone()
        .username("coralswarm_app")
        .password("app_test_pw");
    PgPoolOptions::new()
        .max_connections(4)
        .connect_with(opts)
        .await
        .unwrap()
}

fn router(app: PgPool, account: Uuid, user: Uuid) -> Router {
    let mut state = crate::test_app_state_owned_with_db(app.clone(), None);
    state.store = Some(std::sync::Arc::new(crate::store::PostgresStore::new(app)));
    let state = std::sync::Arc::new(state);
    Router::new()
        .nest(
            "/v1",
            crate::v1_routes().layer(axum::Extension(user_context(account, user))),
        )
        .with_state(state)
}

async fn send(router: &Router, method: &str, uri: &str, body: Option<Value>) -> (StatusCode, Value) {
    let mut req = Request::builder().method(method).uri(uri);
    let body = match body {
        Some(b) => {
            req = req.header("content-type", "application/json");
            Body::from(b.to_string())
        }
        None => Body::empty(),
    };
    let resp = router.clone().oneshot(req.body(body).unwrap()).await.unwrap();
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    (status, serde_json::from_slice(&bytes).unwrap_or(Value::Null))
}

const SLACK_BACKFILL: &str = "/v1/connectors/slack/sources/channels/backfill";

async fn backfill(r: &Router, uri: &str, days: Value) -> (StatusCode, Value) {
    send(r, "POST", uri, Some(json!({ "days": days }))).await
}

async fn slack_source(r: &Router) -> Value {
    let (_, slack) = send(r, "GET", "/v1/connectors/slack", None).await;
    slack
}

/// A member who finished the first three wizard steps for Slack: signed in
/// (their token and the workspace app's), the channels source on and not yet set
/// up, and `channels` channels indexed from their `users.conversations`.
async fn slack_member(owner: &PgPool, account: Uuid, user: Uuid, channels: usize) {
    for (owner_user, provider) in [(Some(user), "slack_user"), (None, "slack")] {
        sqlx::query(
            "INSERT INTO oauth_tokens (account_id, owner_user_id, provider, scope, access_token, provider_workspace_id) \
             VALUES ($1, $2, $3, $4, '\\x00', 'T1')",
        )
        .bind(account)
        .bind(owner_user)
        .bind(provider)
        .bind(crate::connectors::slack::slack_scopes())
        .execute(owner)
        .await
        .unwrap();
    }
    sqlx::query("INSERT INTO slack_integrations (account_id, workspace_id, workspace_name, bot_token) VALUES ($1, 'T1', 'Acme', 'x')")
        .bind(account)
        .execute(owner)
        .await
        .ok();
    sqlx::query(
        "INSERT INTO connector_sources (account_id, owner_user_id, provider, source, enabled, scope_mode) \
         VALUES ($1, $2, 'slack', 'channels', true, 'everything')",
    )
    .bind(account)
    .bind(user)
    .execute(owner)
    .await
    .unwrap();
    for i in 0..channels {
        sqlx::query(
            "INSERT INTO slack_member_channels (account_id, user_id, workspace_id, channel_id) VALUES ($1, $2, 'T1', $3)",
        )
        .bind(account)
        .bind(user)
        .bind(format!("C{i:03}"))
        .execute(owner)
        .await
        .unwrap();
    }
}

async fn history_jobs(owner: &PgPool, account: Uuid) -> Vec<(String, String, Option<i32>, Value)> {
    sqlx::query_as(
        "SELECT idempotency_key, status::text, NULL::int, payload FROM jobs \
         WHERE kind = 'HistoryBackfill' AND account_id = $1 ORDER BY idempotency_key",
    )
    .bind(account)
    .fetch_all(owner)
    .await
    .unwrap()
}

async fn end_jobs(owner: &PgPool, account: Uuid, status: &str, limit: i64) {
    sqlx::query(&format!(
        "UPDATE jobs SET status = '{status}'::job_status, finished_at = now() WHERE id IN \
         (SELECT id FROM jobs WHERE kind = 'HistoryBackfill' AND account_id = $1 \
          AND status IN ('pending', 'running') ORDER BY idempotency_key LIMIT $2)"
    ))
    .bind(account)
    .bind(limit)
    .execute(owner)
    .await
    .unwrap();
}

async fn source_row(owner: &PgPool, account: Uuid, user: Uuid, provider: &str, source: &str) -> (Option<i32>, bool, bool) {
    sqlx::query_as(
        "SELECT history_days, history_requested_at IS NOT NULL, setup_completed_at IS NOT NULL \
         FROM connector_sources WHERE account_id = $1 AND owner_user_id = $2 AND provider = $3 AND source = $4",
    )
    .bind(account)
    .bind(user)
    .bind(provider)
    .bind(source)
    .fetch_one(owner)
    .await
    .unwrap()
}

#[tokio::test]
async fn a_window_queues_one_job_per_channel_and_reads_importing_until_they_end_pg() {
    let Some(owner) = pg_test_pool("history::slack_window").await else {
        return;
    };
    let (account, user) = seed_personal_account(&owner).await;
    slack_member(&owner, account, user, 3).await;
    let r = router(app_pool(&owner).await, account, user);

    // The step before: set up has not finished.
    let before = slack_source(&r).await;
    assert_eq!(before["status"], "setting_up", "{before}");

    let (status, source) = backfill(&r, SLACK_BACKFILL, json!(7)).await;
    assert_eq!(status, StatusCode::ACCEPTED, "{source}");
    assert_eq!(source["key"], "channels");
    assert_eq!(source["status"], "importing", "{source}");
    assert_eq!(source["backfill"], json!({"percent": 0, "window_days": 7}));

    // One job per channel, each bounded to the chosen window, all for this member.
    let jobs = history_jobs(&owner, account).await;
    assert_eq!(jobs.len(), 3);
    for (key, _, _, payload) in &jobs {
        assert!(key.starts_with(&format!("connector_history:slack:{user}:")), "{key}");
        assert_eq!(payload["window_days"], 7);
        assert_eq!(payload["provider"], "slack");
        assert_eq!(payload["workspace_id"], "T1");
        assert_eq!(payload["target_user_id"], user.to_string());
    }
    assert_eq!(source_row(&owner, account, user, "slack", "channels").await, (Some(7), true, true));

    // The connector reports it too, and the choice finished setup.
    let slack = slack_source(&r).await;
    assert_eq!(slack["status"], "importing", "{slack}");
    assert_eq!(slack["importing"], json!({"source": "channels", "window_days": 7, "percent": 0}));
    assert_eq!(slack["resume_step"], Value::Null);

    // Progress is the share of jobs that have ended; a dead job has.
    end_jobs(&owner, account, "done", 1).await;
    assert_eq!(slack_source(&r).await["sources"][0]["backfill"]["percent"], 33);
    end_jobs(&owner, account, "dead", 1).await;
    assert_eq!(slack_source(&r).await["sources"][0]["backfill"]["percent"], 66);

    // The last one ends: the status goes back to active, by itself.
    end_jobs(&owner, account, "done", 1).await;
    let slack = slack_source(&r).await;
    assert_eq!(slack["status"], "active", "{slack}");
    assert_eq!(slack["sources"][0]["status"], "active");
    assert_eq!(slack["sources"][0]["backfill"], Value::Null);
    assert_eq!(slack["importing"], Value::Null);
}

#[tokio::test]
async fn a_repeat_request_while_running_is_refused_and_a_finished_run_can_be_asked_again_pg() {
    let Some(owner) = pg_test_pool("history::slack_repeat").await else {
        return;
    };
    let (account, user) = seed_personal_account(&owner).await;
    slack_member(&owner, account, user, 2).await;
    let r = router(app_pool(&owner).await, account, user);

    assert_eq!(backfill(&r, SLACK_BACKFILL, json!(30)).await.0, StatusCode::ACCEPTED);
    assert_eq!(history_jobs(&owner, account).await.len(), 2);

    // The same window, a different window and "None" are all refused while it runs,
    // and none of them queues anything or touches the recorded choice.
    for days in [30, 7, 0] {
        let (status, body) = backfill(&r, SLACK_BACKFILL, json!(days)).await;
        assert_eq!(status, StatusCode::CONFLICT, "{days}: {body}");
        assert_eq!(body["code"], "backfill_running");
    }
    assert_eq!(history_jobs(&owner, account).await.len(), 2);
    assert_eq!(source_row(&owner, account, user, "slack", "channels").await.0, Some(30));

    // Once it has ended, the member can ask again: a new request, new jobs, and
    // progress that counts only them (the finished run's jobs do not count).
    end_jobs(&owner, account, "done", 10).await;
    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    let (status, source) = backfill(&r, SLACK_BACKFILL, json!(7)).await;
    assert_eq!(status, StatusCode::ACCEPTED, "{source}");
    assert_eq!(source["backfill"], json!({"percent": 0, "window_days": 7}));
    let jobs = history_jobs(&owner, account).await;
    assert_eq!(jobs.len(), 4);
    assert_eq!(jobs.iter().filter(|j| j.1 == "pending").count(), 2);
    let prefixes: std::collections::BTreeSet<_> = jobs
        .iter()
        .map(|j| j.0.rsplit_once(':').unwrap().0.to_string())
        .collect();
    assert_eq!(prefixes.len(), 2, "two requests, two prefixes");
}

#[tokio::test]
async fn none_records_the_choice_finishes_setup_and_queues_nothing_pg() {
    let Some(owner) = pg_test_pool("history::slack_none").await else {
        return;
    };
    let (account, user) = seed_personal_account(&owner).await;
    slack_member(&owner, account, user, 3).await;
    let r = router(app_pool(&owner).await, account, user);
    assert_eq!(slack_source(&r).await["status"], "setting_up");

    let (status, source) = backfill(&r, SLACK_BACKFILL, json!(0)).await;
    assert_eq!(status, StatusCode::ACCEPTED, "{source}");
    assert_eq!(source["status"], "active", "{source}");
    assert_eq!(source["backfill"], Value::Null);
    assert!(history_jobs(&owner, account).await.is_empty());
    assert_eq!(source_row(&owner, account, user, "slack", "channels").await, (Some(0), true, true));

    let slack = slack_source(&r).await;
    assert_eq!(slack["status"], "active", "setup is complete: {slack}");
    assert_eq!(slack["resume_step"], Value::Null);

    // "None" runs nothing, so it never blocks the next request.
    assert_eq!(backfill(&r, SLACK_BACKFILL, json!(7)).await.0, StatusCode::ACCEPTED);
}

#[tokio::test]
async fn a_request_that_cannot_run_says_why_and_changes_nothing_pg() {
    let Some(owner) = pg_test_pool("history::refusals").await else {
        return;
    };
    let (account, user) = seed_personal_account(&owner).await;
    let r = router(app_pool(&owner).await, account, user);

    // Not connected.
    let (status, body) = backfill(&r, SLACK_BACKFILL, json!(7)).await;
    assert_eq!((status, &body["code"]), (StatusCode::NOT_FOUND, &json!("not_connected")), "{body}");

    slack_member(&owner, account, user, 2).await;
    // Not an allowed value.
    for days in [json!(120), json!(8), json!(1)] {
        let (status, body) = backfill(&r, SLACK_BACKFILL, days.clone()).await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{days}: {body}");
        assert_eq!(body["code"], "invalid_days");
        assert_eq!(body["allowed"], json!([0, 7, 30, 90]));
    }
    // A body that is not a non-negative whole number is a 422 as well.
    assert_eq!(backfill(&r, SLACK_BACKFILL, json!(-7)).await.0, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(send(&r, "POST", SLACK_BACKFILL, Some(json!({}))).await.0, StatusCode::UNPROCESSABLE_ENTITY);

    // A source that cannot backfill, with its reason code, connected or not.
    for (uri, reason) in [
        ("/v1/connectors/jira/sources/workspace/backfill", "starts_at_connect"),
        ("/v1/connectors/google/sources/calendar/backfill", "automatic_window"),
        ("/v1/connectors/google/sources/meet/backfill", "per_meeting"),
        ("/v1/connectors/microsoft/sources/teams_meetings/backfill", "per_meeting"),
        ("/v1/connectors/microsoft/sources/mail/backfill", "followed_threads_only"),
    ] {
        for days in [0, 7] {
            let (status, body) = backfill(&r, uri, json!(days)).await;
            assert_eq!(status, StatusCode::CONFLICT, "{uri}: {body}");
            assert_eq!(body["code"], "history_unsupported");
            assert_eq!(body["reason"], reason, "{uri}");
        }
    }
    assert!(!send(&r, "POST", "/v1/connectors/jira/sources/workspace/backfill", Some(json!({"days": 7})))
        .await
        .1
        .to_string()
        .to_lowercase()
        .contains("composio"));

    // Unknown connector and source.
    assert_eq!(backfill(&r, "/v1/connectors/bitbucket/sources/x/backfill", json!(7)).await.0, StatusCode::NOT_FOUND);
    assert_eq!(backfill(&r, "/v1/connectors/slack/sources/dms/backfill", json!(7)).await.0, StatusCode::NOT_FOUND);

    // A source that is off, or paused, takes no history.
    sqlx::query("UPDATE connector_sources SET enabled = false WHERE account_id = $1")
        .bind(account)
        .execute(&owner)
        .await
        .unwrap();
    let (status, body) = backfill(&r, SLACK_BACKFILL, json!(7)).await;
    assert_eq!((status, &body["code"]), (StatusCode::CONFLICT, &json!("source_off")), "{body}");
    sqlx::query("UPDATE connector_sources SET enabled = true, paused_at = now() WHERE account_id = $1")
        .bind(account)
        .execute(&owner)
        .await
        .unwrap();
    let (status, body) = backfill(&r, SLACK_BACKFILL, json!(7)).await;
    assert_eq!((status, &body["code"]), (StatusCode::CONFLICT, &json!("source_paused")), "{body}");

    // A dead sign-in has to be fixed first.
    sqlx::query("UPDATE connector_sources SET paused_at = NULL WHERE account_id = $1")
        .bind(account)
        .execute(&owner)
        .await
        .unwrap();
    sqlx::query("UPDATE oauth_tokens SET credential_invalid_at = now(), credential_invalid_reason = 'revoked' WHERE account_id = $1 AND provider = 'slack_user'")
        .bind(account)
        .execute(&owner)
        .await
        .unwrap();
    let (status, body) = backfill(&r, SLACK_BACKFILL, json!(7)).await;
    assert_eq!((status, &body["code"]), (StatusCode::CONFLICT, &json!("reconnect_needed")), "{body}");

    // None of that queued a job or recorded a choice.
    assert!(history_jobs(&owner, account).await.is_empty());
    assert_eq!(source_row(&owner, account, user, "slack", "channels").await, (None, false, false));
}

#[tokio::test]
async fn a_request_for_a_connection_made_before_the_catalog_keeps_its_sources_on_pg() {
    let Some(owner) = pg_test_pool("history::legacy").await else {
        return;
    };
    let (account, user) = seed_personal_account(&owner).await;
    slack_member(&owner, account, user, 1).await;
    // A connection from before the catalog has no rows.
    sqlx::query("DELETE FROM connector_sources WHERE account_id = $1")
        .bind(account)
        .execute(&owner)
        .await
        .unwrap();
    let r = router(app_pool(&owner).await, account, user);
    assert_eq!(slack_source(&r).await["status"], "active");

    let (status, source) = backfill(&r, SLACK_BACKFILL, json!(7)).await;
    assert_eq!(status, StatusCode::ACCEPTED, "{source}");
    assert_eq!(source["enabled"], true);
    assert_eq!(source["status"], "importing");
    assert_eq!(history_jobs(&owner, account).await.len(), 1);
}

#[tokio::test]
async fn one_members_import_is_theirs_alone_pg() {
    let Some(owner) = pg_test_pool("history::isolation").await else {
        return;
    };
    let (account, alice) = seed_personal_account(&owner).await;
    let bob: Uuid = sqlx::query_scalar("INSERT INTO users (email) VALUES ($1) RETURNING id")
        .bind(format!("{}@history.test", Uuid::new_v4()))
        .fetch_one(&owner)
        .await
        .unwrap();
    slack_member(&owner, account, alice, 2).await;
    let app = app_pool(&owner).await;
    let ra = router(app.clone(), account, alice);
    let rb = router(app, account, bob);

    assert_eq!(backfill(&ra, SLACK_BACKFILL, json!(7)).await.0, StatusCode::ACCEPTED);
    assert_eq!(slack_source(&ra).await["status"], "importing");

    // Bob shares the account and so the workspace app, but has no channels of his
    // own: his request finds nothing to import, and Alice's run is not his.
    let (status, body) = backfill(&rb, SLACK_BACKFILL, json!(7)).await;
    assert_eq!(status, StatusCode::ACCEPTED, "{body}");
    assert_eq!(body["backfill"], Value::Null, "{body}");
    let slack = slack_source(&rb).await;
    assert_eq!(slack["importing"], Value::Null);
    assert!(!slack.to_string().contains(&alice.to_string()));
    assert_eq!(history_jobs(&owner, account).await.len(), 2, "Alice's two, none for Bob");
    // Alice's run is still hers, and Bob's request did not block or end it.
    assert_eq!(slack_source(&ra).await["status"], "importing");

    // Someone in another account has no Slack at all.
    let (other_account, other) = seed_personal_account(&owner).await;
    let ro = router(app_pool(&owner).await, other_account, other);
    let (status, body) = backfill(&ro, SLACK_BACKFILL, json!(7)).await;
    assert_eq!((status, &body["code"]), (StatusCode::NOT_FOUND, &json!("not_connected")), "{body}");

    // And once Bob has channels, his run is his: its own window, its own progress.
    slack_member_tokens_only(&owner, account, bob).await;
    let (status, body) = backfill(&rb, SLACK_BACKFILL, json!(30)).await;
    assert_eq!(status, StatusCode::ACCEPTED, "{body}");
    assert_eq!(body["backfill"]["window_days"], 30);
    assert_eq!(slack_source(&ra).await["sources"][0]["backfill"]["window_days"], 7);
}

/// Bob's own sign-in and one indexed channel; the workspace app is shared.
async fn slack_member_tokens_only(owner: &PgPool, account: Uuid, user: Uuid) {
    sqlx::query(
        "INSERT INTO oauth_tokens (account_id, owner_user_id, provider, scope, access_token, provider_workspace_id) \
         VALUES ($1, $2, 'slack_user', $3, '\\x00', 'T1')",
    )
    .bind(account)
    .bind(user)
    .bind(crate::connectors::slack::slack_scopes())
    .execute(owner)
    .await
    .unwrap();
    sqlx::query("INSERT INTO slack_member_channels (account_id, user_id, workspace_id, channel_id) VALUES ($1, $2, 'T1', 'C900')")
        .bind(account)
        .bind(user)
        .execute(owner)
        .await
        .unwrap();
}

#[tokio::test]
async fn a_member_in_many_channels_queues_a_bounded_number_pg() {
    let Some(owner) = pg_test_pool("history::slack_cap").await else {
        return;
    };
    let (account, user) = seed_personal_account(&owner).await;
    slack_member(&owner, account, user, 40).await;
    let r = router(app_pool(&owner).await, account, user);
    assert_eq!(backfill(&r, SLACK_BACKFILL, json!(90)).await.0, StatusCode::ACCEPTED);
    assert_eq!(history_jobs(&owner, account).await.len(), super::slack::MAX_CHANNELS);
}

// ---------------------------------------------------------------------------
// GitHub
// ---------------------------------------------------------------------------

const GITHUB_BACKFILL: &str = "/v1/connectors/github/sources/repos/backfill";

/// A member with a GitHub install, their token, and one active repo that has
/// finished its automatic (unbounded) backfill.
async fn github_member(owner: &PgPool, account: Uuid, user: Uuid) -> Uuid {
    sqlx::query("INSERT INTO github_installations (account_id, installation_id, account_login, connected_by) VALUES ($1, $2, 'acme', $3)")
        .bind(account)
        .bind(rand_installation())
        .bind(user)
        .execute(owner)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO oauth_tokens (account_id, owner_user_id, provider, scope, access_token) VALUES ($1, $2, 'github', 'repo', '\\x00')",
    )
    .bind(account)
    .bind(user)
    .execute(owner)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO connector_sources (account_id, owner_user_id, provider, source, enabled, scope_mode) \
         VALUES ($1, $2, 'github', 'repos', true, 'everything')",
    )
    .bind(account)
    .bind(user)
    .execute(owner)
    .await
    .unwrap();
    let graph: Uuid = sqlx::query_scalar(
        "INSERT INTO graphs (account_id, name, kind, is_primary) VALUES ($1, 'Personal', 'personal', true) RETURNING id",
    )
    .bind(account)
    .fetch_one(owner)
    .await
    .unwrap();
    sqlx::query_scalar(
        "INSERT INTO github_watched_repos \
            (account_id, installation_id, repo_id, owner, name, active, graph_id, destination_kind, owner_user_id, \
             narrative_cursor, backfill_progress, last_synced_at) \
         VALUES ($1, 1, 42, 'acme', 'api', true, $2, 'personal', $3, \
                 '{\"pr\":{\"done\":true,\"seen\":9},\"issue\":{\"done\":true},\"release\":{\"done\":true},\"commit\":{\"done\":true}}', \
                 '{\"prs\":100,\"issues\":100,\"releases\":100,\"commits\":100}', now()) RETURNING id",
    )
    .bind(account)
    .bind(graph)
    .bind(user)
    .fetch_one(owner)
    .await
    .unwrap()
}

fn rand_installation() -> i64 {
    (Uuid::new_v4().as_u128() % 1_000_000_000) as i64 + 1000
}

#[tokio::test]
async fn a_github_window_restarts_the_repos_with_a_floor_and_leaves_the_automatic_backfill_unbounded_pg() {
    let Some(owner) = pg_test_pool("history::github").await else {
        return;
    };
    let (account, user) = seed_personal_account(&owner).await;
    let repo = github_member(&owner, account, user).await;
    let r = router(app_pool(&owner).await, account, user);

    // Settled before the request: the automatic backfill ran with no floor.
    let before: Value = sqlx::query_scalar("SELECT narrative_cursor FROM github_watched_repos WHERE id = $1")
        .bind(repo)
        .fetch_one(&owner)
        .await
        .unwrap();
    assert!(before.get("window_floor").is_none(), "{before}");
    let (_, gh) = send(&r, "GET", "/v1/connectors/github", None).await;
    assert_eq!(gh["sources"][0]["status"], "active", "{gh}");

    let (status, source) = backfill(&r, GITHUB_BACKFILL, json!(30)).await;
    assert_eq!(status, StatusCode::ACCEPTED, "{source}");
    assert_eq!(source["status"], "importing", "{source}");
    assert_eq!(source["backfill"], json!({"percent": 0, "window_days": 30}));

    // A fresh cursor with a floor 30 days back, and a job to run it.
    let after: Value = sqlx::query_scalar("SELECT narrative_cursor FROM github_watched_repos WHERE id = $1")
        .bind(repo)
        .fetch_one(&owner)
        .await
        .unwrap();
    let floor: chrono::DateTime<chrono::Utc> = after["window_floor"].as_str().unwrap().parse().unwrap();
    let want = chrono::Utc::now() - chrono::Duration::days(30);
    assert!((floor - want).num_seconds().abs() < 60, "{floor} vs {want}");
    for kind in ["pr", "issue", "release", "commit"] {
        assert_ne!(after[kind]["done"], true, "{kind} restarts: {after}");
    }
    let key = format!("github_backfill:{repo}");
    let queued: i64 = sqlx::query_scalar("SELECT count(*) FROM jobs WHERE kind = 'GithubBackfill' AND idempotency_key = $1 AND status = 'pending'")
        .bind(&key)
        .fetch_one(&owner)
        .await
        .unwrap();
    assert_eq!(queued, 1);

    // A second request while it runs is refused, and does not queue another.
    let (status, body) = backfill(&r, GITHUB_BACKFILL, json!(7)).await;
    assert_eq!((status, &body["code"]), (StatusCode::CONFLICT, &json!("backfill_running")), "{body}");
    let queued: i64 = sqlx::query_scalar("SELECT count(*) FROM jobs WHERE kind = 'GithubBackfill' AND idempotency_key = $1")
        .bind(&key)
        .fetch_one(&owner)
        .await
        .unwrap();
    assert_eq!(queued, 1);

    // The worker's progress ends it: every type at 100 reads settled again.
    sqlx::query("UPDATE github_watched_repos SET backfill_progress = '{\"prs\":100,\"issues\":100,\"releases\":100,\"commits\":100}' WHERE id = $1")
        .bind(repo)
        .execute(&owner)
        .await
        .unwrap();
    let (_, gh) = send(&r, "GET", "/v1/connectors/github", None).await;
    assert_eq!(gh["sources"][0]["status"], "active", "{gh}");
    assert_eq!(gh["sources"][0]["backfill"], Value::Null);
}

#[tokio::test]
async fn a_github_window_leaves_other_members_and_paused_repos_alone_pg() {
    let Some(owner) = pg_test_pool("history::github_scope").await else {
        return;
    };
    let (account, alice) = seed_personal_account(&owner).await;
    let alice_repo = github_member(&owner, account, alice).await;
    let paused_repo: Uuid = sqlx::query_scalar(
        "INSERT INTO github_watched_repos (account_id, installation_id, repo_id, owner, name, active, narrative_cursor) \
         VALUES ($1, 1, 43, 'acme', 'old', false, '{\"pr\":{\"done\":true}}') RETURNING id",
    )
    .bind(account)
    .fetch_one(&owner)
    .await
    .unwrap();
    let r = router(app_pool(&owner).await, account, alice);
    assert_eq!(backfill(&r, GITHUB_BACKFILL, json!(7)).await.0, StatusCode::ACCEPTED);

    let cursor = |id: Uuid| {
        let owner = owner.clone();
        async move {
            sqlx::query_scalar::<_, Value>("SELECT narrative_cursor FROM github_watched_repos WHERE id = $1")
                .bind(id)
                .fetch_one(&owner)
                .await
                .unwrap()
        }
    };
    assert!(cursor(alice_repo).await.get("window_floor").is_some());
    assert!(cursor(paused_repo).await.get("window_floor").is_none(), "an inactive repo is not restarted");
}

#[tokio::test]
async fn competing_request_is_rejected_while_first_initializes_pg() {
    let Ok(base) = std::env::var("ORGT_TEST_BASE_URL") else { eprintln!("SKIP: ORGT_TEST_BASE_URL required"); return; };
    let options: sqlx::postgres::PgConnectOptions = base.parse().unwrap();
    let database = crate::test_db::migrated_database(&options, crate::test_db::Plane::Control, "history_startup").await;
    let owner = sqlx::postgres::PgPoolOptions::new().max_connections(4)
        .connect_with(options.database(database.name())).await.unwrap();
    let (account, user) = seed_personal_account(&owner).await;
    slack_member(&owner, account, user, 1).await;
    let r = router(app_pool(&owner).await, account, user);
    let mut barrier = owner.begin().await.unwrap();
    // Reads still work, but the first provider job cannot be inserted.
    sqlx::query("LOCK TABLE jobs IN SHARE MODE").execute(&mut *barrier).await.unwrap();
    let first_router = r.clone();
    let first = tokio::spawn(async move { backfill(&first_router, SLACK_BACKFILL, json!(7)).await });
    tokio::time::timeout(std::time::Duration::from_secs(20), async {
        loop {
            let waiting: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_locks WHERE relation = 'jobs'::regclass AND NOT granted)")
                .fetch_one(&owner).await.unwrap();
            if waiting { break; }
            tokio::task::yield_now().await;
        }
    }).await.expect("first request must reach provider initialization");
    let second = backfill(&r, SLACK_BACKFILL, json!(30)).await;
    assert_eq!(second.0, StatusCode::CONFLICT, "{second:?}");
    barrier.rollback().await.unwrap();
    assert_eq!(first.await.unwrap().0, StatusCode::ACCEPTED);
    assert_eq!(source_row(&owner, account, user, "slack", "channels").await.0, Some(7));
}

//! End-to-end proof for the chat-per-user capture plan
//! (`docs/plans/chat-per-user-ingest-and-in-chat-ask.md`).
//!
//! # What "end-to-end" means here, precisely
//!
//! This suite drives the REAL identity-resolution path over REAL HTTP against
//! a REAL Postgres. The only thing simulated is Slack itself — a local axum
//! server bound to an ephemeral port, reached because `SLACK_API_BASE` now
//! exists (`connectors::slack::slack_api_base`). Before that seam, every Slack
//! call site hardcoded `https://slack.com/api` at 21 places across 6 files, so
//! nothing above the database layer could be tested at all: a test could
//! assert what a function did with data handed to it, and could never assert
//! that the connector asks Slack the right question or survives the answer it
//! actually gets. That gap is exactly where the bug below lived.
//!
//! What this does NOT cover: a real Slack workspace, real OAuth, or Slack's
//! own rate limiting. Those stop at the platform boundary by construction.
//!
//! # The bug this pins (plan §11 correction 6)
//!
//! A Slack bot token that lacks `users:read.email` does not produce an error.
//! Slack answers `ok: true` and simply omits the email field. The old code
//! mapped that to `SlackIdentity::Unmatched` — "this person has no CoralSwarm
//! account" — with no log, no health signal, and no way to tell an affected
//! workspace from a healthy one.
//!
//! That is not a cosmetic mislabel. The plan's step 5 emails exactly the
//! `Unmatched` members, so a single scope-broken workspace would have caused
//! CoralSwarm to email **every member of that workspace** telling them to sign
//! up. `Unavailable` — "we could not ask" — is the honest answer, and it is
//! the one that sends nothing.
//!
//! The four cases below are the whole decision table, and the third is the
//! regression:
//!
//! | Slack says | Granted scope | Correct verdict |
//! |---|---|---|
//! | `ok:true` + email that matches a user | has `users:read.email` | `matched` |
//! | `ok:true` + email that matches nobody | has `users:read.email` | `unmatched` |
//! | `ok:true`, email omitted | **lacks** `users:read.email` | `unavailable` ← was `unmatched` |
//! | member id is `USLACKBOT` | any | `bot` |
//!
//! A fifth case is covered too: a workspace whose granted scope cannot be
//! confirmed at all (no `oauth_tokens` row — also the shape a legacy-backfill
//! placeholder scope produces) must ALSO be `unavailable`. "We don't know
//! whether we could have asked" is not evidence that nobody has an account,
//! and guessing in the other direction is what emails a stranger.
//!
//! # Running it
//!
//! Env-gated like every other `*_pg.rs` here:
//!
//!   ORGT_TEST_BASE_URL=postgres://alex@localhost:5432/postgres \
//!     cargo test -p coralswarm-backend --test chat_per_user_e2e_pg -- --test-threads=1
//!
//! `--test-threads=1` is REQUIRED, not a preference: `SLACK_API_BASE` is
//! process-global, so two tests in this binary pointing it at two different
//! mock servers concurrently would race. The CI job pins it.

mod support;

use coralswarm_backend::test_support_resolve_slack_identity;
use support::rls_probe::RlsProbe;
use uuid::Uuid;

/// The scope string a healthy install carries — the current canonical set
/// (`connectors::slack::slack_scopes`). Only `users:read.email` matters here;
/// the rest are present so the fixture looks like a real grant rather than a
/// one-scope strawman that would pass a substring check by accident.
const SCOPE_WITH_EMAIL: &str = "channels:history channels:read users:read users:read.email \
                                reactions:read files:read canvases:read chat:write im:write \
                                channels:leave";

/// What the LEGACY OAuth start URL granted (`routes::slack::oauth_start`
/// before this work): no `users:read`, no `users:read.email`. A workspace
/// installed through it can never match anybody, and Slack never retro-grants
/// a scope — so this is not a hypothetical, it is the state of any workspace
/// that used that URL.
const SCOPE_WITHOUT_EMAIL: &str =
    "channels:history channels:read groups:history im:history mpim:history";

const WS_HEALTHY: &str = "T-HEALTHY";
const WS_SCOPE_BROKEN: &str = "T-SCOPEBROKEN";
const WS_UNKNOWN_SCOPE: &str = "T-NOROW";

/// A mock Slack `users.info`, bound to an ephemeral loopback port.
///
/// It answers from the `user` query parameter, which is how the real endpoint
/// is addressed, so the request the connector builds is genuinely exercised
/// rather than assumed. Three behaviours, matching the decision table above:
///
///   * `U-MATCHED` / `U-STRANGER` → `ok:true` with a profile email.
///   * `U-NOEMAIL` → `ok:true` with a profile that has **no email key** at
///     all. This is the exact shape Slack returns for a token missing
///     `users:read.email`, and reproducing it faithfully is the point of the
///     whole suite — an `ok:false` mock would test a branch that never fires
///     in production.
///
/// Returns the base URL to put in `SLACK_API_BASE`.
async fn spawn_mock_slack() -> String {
    use axum::{extract::Query, routing::get, Router};
    use std::collections::HashMap;

    async fn users_info(Query(q): Query<HashMap<String, String>>) -> axum::Json<serde_json::Value> {
        let user = q.get("user").map(String::as_str).unwrap_or("");
        let body = match user {
            "U-MATCHED" => serde_json::json!({
                "ok": true,
                "user": {
                    "id": "U-MATCHED",
                    "profile": { "email": "linked@company.example", "display_name": "Linked Person" }
                }
            }),
            "U-STRANGER" => serde_json::json!({
                "ok": true,
                "user": {
                    "id": "U-STRANGER",
                    "profile": { "email": "stranger@company.example", "display_name": "A Stranger" }
                }
            }),
            // The regression fixture: ok:true, and the email simply is not
            // there. No error, no signal — which is why the old code could
            // not tell this apart from "no such person".
            "U-NOEMAIL" => serde_json::json!({
                "ok": true,
                "user": { "id": "U-NOEMAIL", "profile": { "display_name": "Invisible" } }
            }),
            other => serde_json::json!({ "ok": false, "error": format!("unexpected user {other}") }),
        };
        axum::Json(body)
    }

    let app = Router::new().route("/users.info", get(users_info));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind mock slack listener");
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.ok();
    });
    format!("http://{addr}")
}

/// Seeds the `oauth_tokens` row that records what Slack ACTUALLY granted this
/// workspace. This is the only evidence the product has about a workspace's
/// scope, and it is real evidence: `routes::oauth::callback` writes it from
/// Slack's own OAuth response, not from what we asked for.
async fn seed_granted_scope(owner: &sqlx::PgPool, account_id: Uuid, workspace: &str, scope: &str) {
    sqlx::query(
        "INSERT INTO oauth_tokens \
             (account_id, provider, scope, access_token, provider_workspace_id, owner_user_id) \
         VALUES ($1, 'slack', $2, '\\x00'::bytea, $3, NULL)",
    )
    .bind(account_id)
    .bind(scope)
    .bind(workspace)
    .execute(owner)
    .await
    .expect("seed oauth_tokens granted scope");
}

/// The whole decision table in one scenario, against one mock Slack and one
/// database — deliberately one test rather than five, because the property
/// being proven is that these four inputs produce four DIFFERENT verdicts.
/// Split across separate tests, each assertion would still pass against an
/// implementation that collapsed two of them.
#[tokio::test]
async fn identity_verdicts_are_distinguished_end_to_end_pg() {
    let Some(probe) = RlsProbe::setup("orgt_chat_per_user_e2e").await else {
        eprintln!("skipping identity_verdicts_are_distinguished_end_to_end_pg: set ORGT_TEST_BASE_URL");
        return;
    };

    let base = spawn_mock_slack().await;
    std::env::set_var("SLACK_API_BASE", &base);

    let account_id = probe.a.account_id;

    // The person who WILL match: a real CoralSwarm user whose verified address
    // is the one Slack reports for `U-MATCHED`.
    sqlx::query("INSERT INTO users(email) VALUES ('linked@company.example')")
        .execute(&probe.owner)
        .await
        .expect("seed matchable user");

    seed_granted_scope(&probe.owner, account_id, WS_HEALTHY, SCOPE_WITH_EMAIL).await;
    seed_granted_scope(&probe.owner, account_id, WS_SCOPE_BROKEN, SCOPE_WITHOUT_EMAIL).await;
    // WS_UNKNOWN_SCOPE deliberately gets NO row.

    let http = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(5))
        .build()
        .unwrap();

    // ── 1. matched ───────────────────────────────────────────────────────
    let (verdict, user_id) = test_support_resolve_slack_identity(
        &probe.owner,
        &probe.owner,
        &http,
        account_id,
        WS_HEALTHY,
        "U-MATCHED",
        "xoxb-test",
    )
    .await;
    assert_eq!(verdict, "matched", "a verified address that matches a CoralSwarm user must match");
    assert!(user_id.is_some(), "a matched verdict must carry the user id it matched");

    // ── 2. unmatched — the ONLY verdict that may trigger an invite email ──
    let (verdict, _) = test_support_resolve_slack_identity(
        &probe.owner,
        &probe.owner,
        &http,
        account_id,
        WS_HEALTHY,
        "U-STRANGER",
        "xoxb-test",
    )
    .await;
    assert_eq!(
        verdict, "unmatched",
        "a workspace we COULD ask, about a person who genuinely has no CoralSwarm account, is \
         the one and only case that means 'unmatched'"
    );

    // ── 3. the regression: scope-broken must be unavailable ──────────────
    let (verdict, _) = test_support_resolve_slack_identity(
        &probe.owner,
        &probe.owner,
        &http,
        account_id,
        WS_SCOPE_BROKEN,
        "U-NOEMAIL",
        "xoxb-test",
    )
    .await;
    assert_eq!(
        verdict, "unavailable",
        "FAILS WITHOUT THE FIX: Slack answers ok:true and omits the email when the token lacks \
         users:read.email, which used to read as 'unmatched'. That would report every member of \
         a scope-broken workspace as having no account — and the invite step emails exactly the \
         unmatched, so the whole workspace would have been emailed."
    );

    // ── 4. unknown/unconfirmable scope must ALSO be unavailable ──────────
    let (verdict, _) = test_support_resolve_slack_identity(
        &probe.owner,
        &probe.owner,
        &http,
        account_id,
        WS_UNKNOWN_SCOPE,
        "U-NOEMAIL",
        "xoxb-test",
    )
    .await;
    assert_eq!(
        verdict, "unavailable",
        "a workspace whose granted scope cannot be confirmed (no oauth_tokens row, a DB error, \
         or a legacy-backfill placeholder) must fail toward 'we could not ask'. Not knowing \
         whether we could have asked is not evidence that nobody has an account."
    );

    // ── 5. Slackbot is never a person ────────────────────────────────────
    let (verdict, _) = test_support_resolve_slack_identity(
        &probe.owner,
        &probe.owner,
        &http,
        account_id,
        WS_HEALTHY,
        "USLACKBOT",
        "xoxb-test",
    )
    .await;
    assert_eq!(
        verdict, "bot",
        "Slackbot must classify as bot — as unmatched it would inflate every unidentified count \
         forever, and would be emailed an invitation"
    );

    std::env::remove_var("SLACK_API_BASE");
    probe.teardown().await;
}

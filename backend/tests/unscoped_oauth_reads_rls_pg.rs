//! RLS proof for four `oauth_tokens` reads that ran on the bare control pool
//! with no `app.account_id` set. Under `coralswarm_app` (migration
//! `0083_consumer_rls`, strict `current_setting('app.account_id')`) every
//! one of them hard-errored, and each caller hid the error:
//!
//!   * `connectors::cold_start::{slack,teams}_someone_can_be_emailed` mapped
//!     the error to `true` ("someone can already be emailed"), so the Slack
//!     and Teams in-container connect notices never posted.
//!   * `connectors::slack_member_capture::apply_membership_event` (from
//!     `routes::slack_events` `member_joined_channel` / `member_left_channel`)
//!     failed with a warning, so the member channel index never followed
//!     joins or leaves. It knows only the Slack workspace and user ids, so it
//!     now resolves owners through the control-only SECURITY DEFINER
//!     `resolve_slack_user_token_owners(text, text)` (migration 0281).
//!   * `connectors::slack_member_capture::invite_reader_user_id` (from
//!     `workers::slack_unmatched_email`) returned `Err`, so every
//!     unmatched-email job failed and retried.
//!
//! Every test drives the real function on `probe.app_pool`, a real
//! `coralswarm_app` login, so a test run as the owner cannot pass vacuously.
//!
//!   ORGT_TEST_BASE_URL=postgres://alex@localhost:5432/postgres \
//!     cargo test -p coralswarm-backend --test unscoped_oauth_reads_rls_pg -- --test-threads=1

#[path = "support/financial_fixture.rs"]
mod financial_fixture;

mod support;

use coralswarm_backend::{
    test_support_apply_membership_event, test_support_due_backfill_rows,
    test_support_invite_reader_user_id, test_support_member_bodies,
    test_support_member_capture_targets,
    test_support_slack_someone_can_be_emailed, test_support_teams_someone_can_be_emailed,
};
use support::rls_probe::{AccountFixture, RlsProbe};
use uuid::Uuid;

const WORKSPACE: &str = "T-unscoped";

async fn add_member(probe: &RlsProbe, f: &AccountFixture) {
    sqlx::query(
        "INSERT INTO account_members (account_id, user_id, role) VALUES ($1, $2, 'owner') \
         ON CONFLICT DO NOTHING",
    )
    .bind(f.account_id)
    .bind(f.user_id)
    .execute(&probe.owner)
    .await
    .expect("seed account_members");
}

/// Seed one `oauth_tokens` row through the owner pool. The token bytes are a
/// placeholder: none of the functions under test decrypt them.
async fn seed_token(
    probe: &RlsProbe,
    account_id: Uuid,
    owner_user_id: Option<Uuid>,
    provider: &str,
    scope: &str,
    provider_user_id: Option<&str>,
) {
    sqlx::query(
        "INSERT INTO oauth_tokens \
             (account_id, owner_user_id, provider, scope, access_token, \
              provider_workspace_id, provider_user_id) \
         VALUES ($1, $2, $3, $4, $5, $6, $7)",
    )
    .bind(account_id)
    .bind(owner_user_id)
    .bind(provider)
    .bind(scope)
    .bind(vec![0u8; 4])
    .bind(WORKSPACE)
    .bind(provider_user_id)
    .execute(&probe.owner)
    .await
    .expect("seed oauth_tokens");
}

async fn indexed(probe: &RlsProbe, channel_id: &str) -> Vec<(Uuid, Uuid)> {
    sqlx::query_as(
        "SELECT account_id, user_id FROM slack_member_channels \
         WHERE workspace_id = $1 AND channel_id = $2 ORDER BY account_id, user_id",
    )
    .bind(WORKSPACE)
    .bind(channel_id)
    .fetch_all(&probe.owner)
    .await
    .expect("read slack_member_channels")
}

/// Pre-fix, the unscoped read errored and the check answered `true`, so an
/// account with no Slack grant still looked reachable and the notice was
/// never posted.
#[tokio::test]
async fn slack_cold_start_check_reads_tokens_under_rls() {
    let Some(probe) = RlsProbe::setup("orgt_unscoped_oauth_slack_cold").await else {
        eprintln!("skipping slack_cold_start_check_reads_tokens_under_rls: set ORGT_TEST_BASE_URL");
        return;
    };
    add_member(&probe, &probe.a).await;
    add_member(&probe, &probe.b).await;
    // Only B has a member grant that can read emails.
    seed_token(
        &probe,
        probe.b.account_id,
        Some(probe.b.user_id),
        "slack_user",
        "channels:read users:read users:read.email",
        Some("U-b"),
    )
    .await;

    assert!(
        !test_support_slack_someone_can_be_emailed(&probe.app_pool, probe.a.account_id).await,
        "A has no slack_user grant, so nobody can be emailed and the notice may post. \
         `true` here means the oauth_tokens read failed under RLS and was read as `true`"
    );
    assert!(
        test_support_slack_someone_can_be_emailed(&probe.app_pool, probe.b.account_id).await,
        "B's own member grant must be visible under B's scope"
    );

    seed_token(
        &probe,
        probe.a.account_id,
        Some(probe.a.user_id),
        "slack_user",
        "channels:read users:read users:read.email",
        Some("U-a"),
    )
    .await;
    assert!(
        test_support_slack_someone_can_be_emailed(&probe.app_pool, probe.a.account_id).await,
        "once A has its own grant, the notice is retired"
    );
    probe.teardown().await;
}

/// Same defect on the Teams side, including the account-wide (NULL owner)
/// grant shape.
#[tokio::test]
async fn teams_cold_start_check_reads_tokens_under_rls() {
    let Some(probe) = RlsProbe::setup("orgt_unscoped_oauth_teams_cold").await else {
        eprintln!("skipping teams_cold_start_check_reads_tokens_under_rls: set ORGT_TEST_BASE_URL");
        return;
    };
    seed_token(
        &probe,
        probe.b.account_id,
        None,
        "microsoft",
        "https://graph.microsoft.com/TeamMember.Read.All",
        None,
    )
    .await;

    assert!(
        !test_support_teams_someone_can_be_emailed(&probe.app_pool, probe.a.account_id).await,
        "A has no Microsoft grant, so nobody can be emailed and the notice may post. \
         `true` here means the oauth_tokens read failed under RLS and was read as `true`"
    );
    assert!(
        test_support_teams_someone_can_be_emailed(&probe.app_pool, probe.b.account_id).await,
        "B's account-wide grant must be visible under B's scope"
    );
    probe.teardown().await;
}

/// `member_joined_channel` / `member_left_channel` carry only the Slack
/// workspace and user ids. The index update must find each connected
/// member's account without an account scope, and touch only that member.
#[tokio::test]
async fn membership_event_updates_index_under_rls() {
    let Some(probe) = RlsProbe::setup("orgt_unscoped_oauth_membership").await else {
        eprintln!("skipping membership_event_updates_index_under_rls: set ORGT_TEST_BASE_URL");
        return;
    };
    seed_token(&probe, probe.a.account_id, Some(probe.a.user_id), "slack_user", "channels:read", Some("U-a")).await;
    seed_token(&probe, probe.b.account_id, Some(probe.b.user_id), "slack_user", "channels:read", Some("U-b")).await;
    // An account-wide bot row for the same workspace is never a member.
    seed_token(&probe, probe.a.account_id, None, "slack", "channels:history", Some("U-a")).await;

    test_support_apply_membership_event(&probe.app_pool, WORKSPACE, "C1", "U-a", true)
        .await
        .expect("member_joined_channel must succeed as coralswarm_app");
    assert_eq!(
        indexed(&probe, "C1").await,
        vec![(probe.a.account_id, probe.a.user_id)],
        "a join indexes only the joining member"
    );

    test_support_apply_membership_event(&probe.app_pool, WORKSPACE, "C1", "U-b", true)
        .await
        .expect("second join");
    test_support_apply_membership_event(&probe.app_pool, WORKSPACE, "C1", "U-a", false)
        .await
        .expect("member_left_channel must succeed as coralswarm_app");
    assert_eq!(
        indexed(&probe, "C1").await,
        vec![(probe.b.account_id, probe.b.user_id)],
        "a leave removes only the leaving member"
    );

    // A dead token does not index on join, but a leave still removes the row.
    sqlx::query(
        "UPDATE oauth_tokens SET credential_invalid_at = now() \
         WHERE account_id = $1 AND provider = 'slack_user'",
    )
    .bind(probe.b.account_id)
    .execute(&probe.owner)
    .await
    .expect("mark B's token invalid");
    test_support_apply_membership_event(&probe.app_pool, WORKSPACE, "C2", "U-b", true)
        .await
        .expect("join with a dead token");
    assert!(indexed(&probe, "C2").await.is_empty(), "a dead token must not index");
    test_support_apply_membership_event(&probe.app_pool, WORKSPACE, "C1", "U-b", false)
        .await
        .expect("leave with a dead token");
    assert!(indexed(&probe, "C1").await.is_empty(), "a leave removes the row even when the token is dead");
    probe.teardown().await;
}

/// The unmatched-email worker's roster reader lookup. Pre-fix this returned
/// `Err` and the job failed and retried forever.
#[tokio::test]
async fn invite_reader_lookup_reads_tokens_under_rls() {
    let Some(probe) = RlsProbe::setup("orgt_unscoped_oauth_invite").await else {
        eprintln!("skipping invite_reader_lookup_reads_tokens_under_rls: set ORGT_TEST_BASE_URL");
        return;
    };
    seed_token(&probe, probe.a.account_id, Some(probe.a.user_id), "slack_user", "channels:read", Some("U-a")).await;
    sqlx::query(
        "INSERT INTO slack_member_channels (account_id, user_id, workspace_id, channel_id) \
         VALUES ($1, $2, $3, 'C1')",
    )
    .bind(probe.a.account_id)
    .bind(probe.a.user_id)
    .bind(WORKSPACE)
    .execute(&probe.owner)
    .await
    .expect("seed index row");

    let in_channel = test_support_invite_reader_user_id(&probe.app_pool, probe.a.account_id, WORKSPACE, "C1")
        .await
        .expect("reader lookup must succeed as coralswarm_app");
    assert_eq!(in_channel, Some(probe.a.user_id), "the indexed member is the reader");

    let fallback = test_support_invite_reader_user_id(&probe.app_pool, probe.a.account_id, WORKSPACE, "C-other")
        .await
        .expect("fallback lookup");
    assert_eq!(fallback, Some(probe.a.user_id), "any live member token is the fallback reader");

    // Cross-account isolation, without an `account_id` filter doing the work:
    // the same unfiltered read returns A's member token under A's scope and
    // nothing under B's. Only the RLS policy can make these differ.
    let unfiltered = "SELECT owner_user_id FROM oauth_tokens WHERE provider = 'slack_user'";
    probe
        .assert_scoped_returns_own_rows(probe.a.account_id, unfiltered, &[], &[probe.a.user_id])
        .await;
    probe.assert_cross_account_returns_nothing(probe.b.account_id, unfiltered, &[]).await;
    probe.teardown().await;
}

/// The new resolver follows the SECURITY DEFINER grant standard: pinned
/// `search_path`, no `PUBLIC` execute, callable by `coralswarm_app`.
#[tokio::test]
async fn slack_user_owner_resolver_is_locked_down() {
    let Some(probe) = RlsProbe::setup("orgt_unscoped_oauth_resolver").await else {
        eprintln!("skipping slack_user_owner_resolver_is_locked_down: set ORGT_TEST_BASE_URL");
        return;
    };
    let row: Option<(bool, Vec<String>, bool, bool)> = sqlx::query_as(
        "SELECT p.prosecdef, \
                COALESCE(p.proconfig, ARRAY[]::text[]), \
                has_function_privilege('coralswarm_app', p.oid, 'EXECUTE'), \
                EXISTS (SELECT 1 FROM aclexplode(p.proacl) a \
                        WHERE a.grantee = 0 AND a.privilege_type = 'EXECUTE') \
         FROM pg_proc p \
         WHERE p.oid = to_regprocedure('resolve_slack_user_token_owners(text,text)')",
    )
    .fetch_optional(&probe.owner)
    .await
    .expect("read pg_proc");
    let (secdef, config, app_can_execute, public_can_execute) =
        row.expect("resolve_slack_user_token_owners(text,text) must exist");
    assert!(secdef, "must be SECURITY DEFINER");
    assert!(
        config.iter().any(|c| c == "search_path=public, pg_temp"),
        "search_path must be pinned, got {config:?}"
    );
    assert!(app_can_execute, "coralswarm_app must be able to execute it");
    assert!(!public_can_execute, "PUBLIC must not be able to execute it");
    probe.teardown().await;
}

// ── Ex-member tokens ──────────────────────────────────────────────────────────
//
// A member who leaves an org keeps a live `slack_user` row until something
// revokes it. None of these paths may keep using it: not to index a channel
// join, not to read a roster for the unmatched-email worker, not to receive
// doorbell captures or history backfill. A current member's token still works.

/// Turns `probe.a`'s account into an org account and returns
/// `(org account id, current member user id, ex-member user id)`. The
/// current member is `probe.a.user_id`. The ex-member was a member, was
/// removed, and still has a live `slack_user` token. Both tokens sit in
/// `WORKSPACE` as `U-current` / `U-former`.
async fn org_with_ex_member(probe: &RlsProbe) -> (Uuid, Uuid, Uuid) {
    let account_id = probe.a.account_id;
    let current = probe.a.user_id;
    financial_fixture::execute(
        &probe.owner,
        account_id,
        sqlx::query("UPDATE accounts SET clerk_org_id = $2 WHERE id = $1")
            .bind(account_id)
            .bind(format!("org_exmember_{}", account_id.simple())),
    )
    .await
    .expect("make A an org account");
    let former: Uuid = sqlx::query_scalar("INSERT INTO users(email) VALUES ($1) RETURNING id")
        .bind(format!("former-{}@probe.test", Uuid::new_v4().simple()))
        .fetch_one(&probe.owner)
        .await
        .expect("insert former member");
    for user in [current, former] {
        add_org_member(probe, account_id, user).await;
    }
    sqlx::query("DELETE FROM account_members WHERE account_id = $1 AND user_id = $2")
        .bind(account_id)
        .bind(former)
        .execute(&probe.owner)
        .await
        .expect("remove the former member");
    seed_token(probe, account_id, Some(current), "slack_user", "channels:read", Some("U-current")).await;
    seed_token(probe, account_id, Some(former), "slack_user", "channels:read", Some("U-former")).await;
    (account_id, current, former)
}

async fn add_org_member(probe: &RlsProbe, account_id: Uuid, user_id: Uuid) {
    sqlx::query(
        "INSERT INTO account_members (account_id, user_id, role, seat_tier_id) \
         VALUES ($1, $2, 'member', \
                 (SELECT id FROM seat_tiers WHERE code = 'standard' AND is_active = true))",
    )
    .bind(account_id)
    .bind(user_id)
    .execute(&probe.owner)
    .await
    .expect("seed org account_members");
}

async fn seed_index_row(probe: &RlsProbe, account_id: Uuid, user_id: Uuid, channel_id: &str) {
    sqlx::query(
        "INSERT INTO slack_member_channels (account_id, user_id, workspace_id, channel_id) \
         VALUES ($1, $2, $3, $4)",
    )
    .bind(account_id)
    .bind(user_id)
    .bind(WORKSPACE)
    .bind(channel_id)
    .execute(&probe.owner)
    .await
    .expect("seed index row");
}

#[tokio::test]
async fn ex_member_live_token_does_not_index_on_join() {
    let Some(probe) = RlsProbe::setup("orgt_unscoped_oauth_exmember_join").await else {
        eprintln!("skipping ex_member_live_token_does_not_index_on_join: set ORGT_TEST_BASE_URL");
        return;
    };
    let (account_id, current, former) = org_with_ex_member(&probe).await;

    test_support_apply_membership_event(&probe.app_pool, WORKSPACE, "C1", "U-former", true)
        .await
        .expect("join for the ex-member must succeed as coralswarm_app");
    assert!(
        indexed(&probe, "C1").await.is_empty(),
        "an ex-member's live token must not index a channel join"
    );

    test_support_apply_membership_event(&probe.app_pool, WORKSPACE, "C1", "U-current", true)
        .await
        .expect("join for the current member");
    assert_eq!(
        indexed(&probe, "C1").await,
        vec![(account_id, current)],
        "a current member's token still indexes"
    );

    // A row the ex-member already had is still removed on leave.
    seed_index_row(&probe, account_id, former, "C2").await;
    test_support_apply_membership_event(&probe.app_pool, WORKSPACE, "C2", "U-former", false)
        .await
        .expect("leave for the ex-member");
    assert!(
        indexed(&probe, "C2").await.is_empty(),
        "a leave removes the index row regardless of membership"
    );

    // Re-joining the org restores it, so membership is what gated the join.
    add_org_member(&probe, account_id, former).await;
    test_support_apply_membership_event(&probe.app_pool, WORKSPACE, "C3", "U-former", true)
        .await
        .expect("join after rejoining the org");
    assert_eq!(indexed(&probe, "C3").await, vec![(account_id, former)]);
    probe.teardown().await;
}

#[tokio::test]
async fn ex_member_live_token_is_not_an_invite_reader() {
    let Some(probe) = RlsProbe::setup("orgt_unscoped_oauth_exmember_invite").await else {
        eprintln!("skipping ex_member_live_token_is_not_an_invite_reader: set ORGT_TEST_BASE_URL");
        return;
    };
    let (account_id, current, former) = org_with_ex_member(&probe).await;
    // The ex-member's token is the newest and the only one indexed in C1,
    // so both the in-channel and the fallback query would pick it.
    sqlx::query(
        "UPDATE oauth_tokens SET updated_at = now() + interval '1 hour' \
         WHERE account_id = $1 AND owner_user_id = $2",
    )
    .bind(account_id)
    .bind(former)
    .execute(&probe.owner)
    .await
    .expect("make the ex-member's token newest");
    seed_index_row(&probe, account_id, former, "C1").await;

    let in_channel = test_support_invite_reader_user_id(&probe.app_pool, account_id, WORKSPACE, "C1")
        .await
        .expect("reader lookup must succeed as coralswarm_app");
    assert_eq!(
        in_channel,
        Some(current),
        "the ex-member's token must not read the roster; the current member is the fallback"
    );

    sqlx::query("DELETE FROM oauth_tokens WHERE account_id = $1 AND owner_user_id = $2")
        .bind(account_id)
        .bind(current)
        .execute(&probe.owner)
        .await
        .expect("drop the current member's token");
    let none = test_support_invite_reader_user_id(&probe.app_pool, account_id, WORKSPACE, "C1")
        .await
        .expect("reader lookup with only an ex-member token");
    assert_eq!(none, None, "an ex-member's token alone is no reader");
    probe.teardown().await;
}

/// Index rows written while the ex-member was a member are not targets for
/// doorbell capture or history backfill.
#[tokio::test]
async fn ex_member_index_rows_are_not_capture_or_backfill_targets() {
    let Some(probe) = RlsProbe::setup("orgt_unscoped_oauth_exmember_targets").await else {
        eprintln!("skipping ex_member_index_rows_are_not_capture_or_backfill_targets: set ORGT_TEST_BASE_URL");
        return;
    };
    let (account_id, current, former) = org_with_ex_member(&probe).await;
    seed_index_row(&probe, account_id, current, "C1").await;
    seed_index_row(&probe, account_id, former, "C1").await;

    let targets = test_support_member_capture_targets(&probe.app_pool, WORKSPACE, "C1")
        .await
        .expect("capture target lookup");
    assert_eq!(targets, vec![(account_id, current)], "only the current member is a capture target");

    let due = test_support_due_backfill_rows(&probe.app_pool, Some(account_id))
        .await
        .expect("due backfill rows");
    let due_users: Vec<Uuid> = due.iter().map(|(_, user, _, _)| *user).collect();
    assert_eq!(due_users, vec![current], "only the current member's rows are backfilled");

    // Personal accounts (B): the account owner is the member. Someone else
    // with an index row on B's personal account and no membership row is not.
    seed_token(&probe, probe.b.account_id, Some(probe.b.user_id), "slack_user", "channels:read", Some("U-personal")).await;
    seed_index_row(&probe, probe.b.account_id, probe.b.user_id, "C9").await;
    let stranger: Uuid = sqlx::query_scalar("INSERT INTO users(email) VALUES ($1) RETURNING id")
        .bind(format!("stranger-{}@probe.test", Uuid::new_v4().simple()))
        .fetch_one(&probe.owner)
        .await
        .expect("insert non-owner");
    seed_index_row(&probe, probe.b.account_id, stranger, "C9").await;
    let personal = test_support_member_capture_targets(&probe.app_pool, WORKSPACE, "C9")
        .await
        .expect("personal capture targets");
    assert_eq!(
        personal,
        vec![(probe.b.account_id, probe.b.user_id)],
        "only the personal account's owner is a target; a non-owner with no membership row is not"
    );
    let personal_due = test_support_due_backfill_rows(&probe.app_pool, None)
        .await
        .expect("consumer-plane due rows");
    assert_eq!(
        personal_due.iter().map(|(a, u, _, _)| (*a, *u)).collect::<Vec<_>>(),
        vec![(probe.b.account_id, probe.b.user_id)]
    );
    probe.teardown().await;
}

// ── Doorbell capture: a denied token load never falls back to the payload ────
//
// `capture_message_event` writes the Slack event's own text into a member's
// ocean only when that member is current and has their own live grant.
// A disconnected member, a member who left, or a failed lookup gets nothing.

const EVENT_TS: &str = "1700000000.000100";
const EVENT_TEXT: &str = "event payload body";

/// The member leaves after the target lookup listed them and before their
/// token is loaded. Nothing may be written for them.
#[tokio::test]
async fn member_removed_between_target_lookup_and_token_load_gets_nothing() {
    let Some(probe) = RlsProbe::setup("orgt_unscoped_oauth_capture_race").await else {
        eprintln!("skipping member_removed_between_target_lookup_and_token_load_gets_nothing: set ORGT_TEST_BASE_URL");
        return;
    };
    let (account_id, leaving, staying) = org_with_ex_member(&probe).await;
    // `staying` is the fixture's ex-member; make them current so they act as
    // the control: a current member with a live grant still gets a fetched body.
    add_org_member(&probe, account_id, staying).await;
    seed_index_row(&probe, account_id, leaving, "C1").await;
    seed_index_row(&probe, account_id, staying, "C1").await;

    let owner = probe.owner.clone();
    let bodies = test_support_member_bodies(
        &probe.app_pool,
        &probe.app_pool,
        WORKSPACE,
        "C1",
        EVENT_TS,
        EVENT_TEXT,
        |acct, user| {
            let owner = owner.clone();
            async move {
                if user == leaving {
                    sqlx::query("DELETE FROM account_members WHERE account_id = $1 AND user_id = $2")
                        .bind(acct)
                        .bind(user)
                        .execute(&owner)
                        .await
                        .expect("member leaves mid-capture");
                }
            }
        },
    )
    .await
    .expect("member bodies");

    assert_eq!(
        bodies,
        vec![(account_id, staying, EVENT_TEXT.to_string())],
        "the member who left mid-capture must get nothing; the current member still gets the payload"
    );
    probe.teardown().await;
}

/// A disconnect after target selection cannot turn into raw-event capture.
#[tokio::test]
async fn member_disconnected_between_target_lookup_and_token_load_gets_nothing() {
    let Some(probe) = RlsProbe::setup("orgt_unscoped_oauth_disconnect_race").await else {
        eprintln!("skipping member_disconnected_between_target_lookup_and_token_load_gets_nothing: set ORGT_TEST_BASE_URL");
        return;
    };
    let (account_id, disconnecting, staying) = org_with_ex_member(&probe).await;
    add_org_member(&probe, account_id, staying).await;
    seed_index_row(&probe, account_id, disconnecting, "C1").await;
    seed_index_row(&probe, account_id, staying, "C1").await;
    let owner = probe.owner.clone();
    let bodies = test_support_member_bodies(
        &probe.app_pool, &probe.app_pool, WORKSPACE, "C1", EVENT_TS, EVENT_TEXT,
        |acct, user| {
            let owner = owner.clone();
            async move {
                if user == disconnecting {
                    sqlx::query("DELETE FROM oauth_tokens WHERE account_id=$1 AND owner_user_id=$2 AND provider='slack_user'")
                        .bind(acct).bind(user).execute(&owner).await.unwrap();
                }
            }
        },
    ).await.expect("member bodies after concurrent disconnect");
    assert_eq!(bodies, vec![(account_id, staying, EVENT_TEXT.to_string())]);
    let targets = test_support_member_capture_targets(&probe.app_pool, WORKSPACE, "C1")
        .await.expect("targets with a stale channel index");
    assert_eq!(targets, vec![(account_id, staying)]);
    probe.teardown().await;
}

/// A membership lookup that errors is not "no token": the member is skipped,
/// never given the raw event payload.
#[tokio::test]
async fn membership_lookup_error_skips_the_member_instead_of_writing_the_payload() {
    let Some(probe) = RlsProbe::setup("orgt_unscoped_oauth_capture_lookup_err").await else {
        eprintln!("skipping membership_lookup_error_skips_the_member_instead_of_writing_the_payload: set ORGT_TEST_BASE_URL");
        return;
    };
    let (account_id, current, _former) = org_with_ex_member(&probe).await;
    seed_index_row(&probe, account_id, current, "C1").await;

    // Control: with a working gate, the current member has a live grant
    // and gets a fetched body. An empty result below is the lookup error.
    let ok = test_support_member_bodies(
        &probe.app_pool,
        &probe.app_pool,
        WORKSPACE,
        "C1",
        EVENT_TS,
        EVENT_TEXT,
        |_, _| async {},
    )
    .await
    .expect("member bodies with a working gate");
    assert_eq!(ok, vec![(account_id, current, EVENT_TEXT.to_string())]);

    // A gate pool whose every query fails (its database does not exist).
    let base = std::env::var("ORGT_TEST_BASE_URL").expect("ORGT_TEST_BASE_URL");
    let (prefix, rest) = base.split_at(base.rfind('/').expect("url has a path /"));
    let params = rest.find('?').map(|i| &rest[i..]).unwrap_or("");
    let broken = sqlx::postgres::PgPoolOptions::new()
        .acquire_timeout(std::time::Duration::from_secs(5))
        .connect_lazy(&format!("{prefix}/orgt_no_such_database_for_gate{params}"))
        .expect("lazy pool");
    let bodies = test_support_member_bodies(
        &probe.app_pool,
        &broken,
        WORKSPACE,
        "C1",
        EVENT_TS,
        EVENT_TEXT,
        |_, _| async {},
    )
    .await
    .expect("target lookup itself runs on the working pool");
    assert!(
        bodies.is_empty(),
        "a failed membership lookup must skip the member, not write the raw payload; got {bodies:?}"
    );
    broken.close().await;
    probe.teardown().await;
}

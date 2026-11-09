//! Postgres tests for the pause gate and the Slack pick filter (layer B3): one
//! test per capture entry point, each proving that a paused source writes
//! nothing. The table in `pause.rs` lists the entry points; the Gmail push and the
//! two everything-mode workers keep their tests beside their private functions
//! (`routes::google_push`, `workers::{gmail,outlook}_everything_mode`).
//!
//! Where the code behind an entry point can run without a provider, the test
//! also runs the same call for an unpaused member and shows it getting past the
//! gate (a positive control), so a pass cannot come from the call failing early
//! for some other reason.
//!
//! Gated on `RELINFER_TEST_DATABASE_URL`, a migrated CONTROL database, like every
//! billing pg suite; without it each test prints a skip line and returns. A skip
//! is NOT a pass.

use std::collections::BTreeSet;
use std::sync::Arc;

use sqlx::PgPool;
use uuid::Uuid;

use super::pause::{self, SourceKey};
use crate::billing::test_support::{pg_test_pool, seed_personal_account};
use crate::AppState;

/// One member: a personal account with its owner, and the account's data key
/// (writing an atom encrypts under it; without one the ingest path answers 503
/// and a positive control would fail for the wrong reason).
async fn member(owner: &PgPool) -> (Uuid, Uuid) {
    use coralswarm_core::kek::{generate_dek, load_kek, wrap_dek};
    std::env::set_var("MASTER_KEK_HEX", "cc".repeat(32));
    let (account, user) = seed_personal_account(owner).await;
    let kek = load_kek().expect("test KEK loads");
    let wrapped = wrap_dek(&kek, &generate_dek()).expect("wrap dek");
    sqlx::query("UPDATE accounts SET dek_wrapped = $2 WHERE id = $1")
        .bind(account)
        .bind(&wrapped)
        .execute(owner)
        .await
        .expect("seed the account's data key");
    (account, user)
}

async fn colleague(pool: &PgPool, account: Uuid) -> Uuid {
    let user: Uuid = sqlx::query_scalar("INSERT INTO users (email) VALUES ($1) RETURNING id").bind(format!("{}@scope.test", Uuid::new_v4())).fetch_one(pool).await.unwrap();
    sqlx::query("INSERT INTO account_members (account_id,user_id,role) VALUES ($1,$2,'member')").bind(account).bind(user).execute(pool).await.unwrap();
    user
}

fn state_on(pool: &PgPool) -> Arc<AppState> {
    // Writing an atom encrypts it under the account's key, wrapped by this one.
    std::env::set_var("MASTER_KEK_HEX", "cc".repeat(32));
    let mut state = crate::test_app_state_owned_with_db(pool.clone(), None);
    state.store = Some(Arc::new(crate::store::PostgresStore::new(pool.clone())));
    Arc::new(state)
}

/// The member's `connector_sources` row, as the catalog would have written it.
async fn source_row(
    owner: &PgPool,
    account: Uuid,
    user: Uuid,
    key: SourceKey,
    enabled: bool,
    paused: bool,
    mode: Option<&str>,
    setup_done: bool,
) {
    sqlx::query(
        "INSERT INTO connector_sources \
             (account_id, owner_user_id, provider, source, enabled, paused_at, scope_mode, setup_completed_at) \
         VALUES ($1, $2, $3, $4, $5, CASE WHEN $6 THEN now() END, $7, CASE WHEN $8 THEN now() END) \
         ON CONFLICT (account_id, owner_user_id, provider, source) DO UPDATE \
             SET enabled = $5, paused_at = CASE WHEN $6 THEN now() END, scope_mode = $7, \
                 setup_completed_at = CASE WHEN $8 THEN now() END",
    )
    .bind(account)
    .bind(user)
    .bind(key.provider)
    .bind(key.source)
    .bind(enabled)
    .bind(paused)
    .bind(mode)
    .bind(setup_done)
    .execute(owner)
    .await
    .unwrap();
}

async fn set_paused(owner: &PgPool, account: Uuid, user: Uuid, key: SourceKey, paused: bool) {
    sqlx::query(
        "UPDATE connector_sources SET paused_at = CASE WHEN $5 THEN now() END \
         WHERE account_id = $1 AND owner_user_id = $2 AND provider = $3 AND source = $4",
    )
    .bind(account)
    .bind(user)
    .bind(key.provider)
    .bind(key.source)
    .bind(paused)
    .execute(owner)
    .await
    .unwrap();
}

async fn atoms_in_account(owner: &PgPool, account: Uuid) -> i64 {
    sqlx::query_scalar(
        "SELECT count(*) FROM atoms a JOIN graphs g ON g.id = a.graph_id WHERE g.account_id = $1",
    )
    .bind(account)
    .fetch_one(owner)
    .await
    .unwrap()
}

// ---------------------------------------------------------------------------
// The helper itself
// ---------------------------------------------------------------------------

#[tokio::test]
async fn is_paused_reads_only_the_callers_own_row_pg() {
    let Some(owner) = pg_test_pool("pause::is_paused").await else {
        return;
    };
    let (account, user) = member(&owner).await;
    let other: Uuid = sqlx::query_scalar("INSERT INTO users (email) VALUES ($1) RETURNING id")
        .bind(format!("{}@pause.test", Uuid::new_v4()))
        .fetch_one(&owner)
        .await
        .unwrap();
    let key = pause::MS_MAIL;

    assert!(
        !pause::is_paused(&owner, account, user, key).await,
        "no row: from before the catalog"
    );
    source_row(&owner, account, user, key, true, false, None, true).await;
    assert!(!pause::is_paused(&owner, account, user, key).await, "on");
    set_paused(&owner, account, user, key, true).await;
    assert!(pause::is_paused(&owner, account, user, key).await, "paused");
    set_paused(&owner, account, user, key, false).await;
    sqlx::query("UPDATE connector_sources SET enabled = false WHERE account_id = $1")
        .bind(account)
        .execute(&owner)
        .await
        .unwrap();
    assert!(pause::is_paused(&owner, account, user, key).await, "off");
    // A colleague in the same account is not paused by this member's row.
    assert!(!pause::is_paused(&owner, account, other, key).await);
    // Another source of the same member is its own row.
    assert!(!pause::is_paused(&owner, account, user, pause::MS_CALENDAR).await);
}

// ---------------------------------------------------------------------------
// Slack: the doorbell and the pick filter
// ---------------------------------------------------------------------------

async fn slack_member(
    owner: &PgPool,
    workspace: &str,
    channel: &str,
    selected: bool,
    row: Option<(bool, bool, Option<&str>, bool)>,
) -> (Uuid, Uuid) {
    let (account, user) = member(owner).await;
    sqlx::query(
        "INSERT INTO oauth_tokens (account_id, owner_user_id, provider, scope, access_token, provider_workspace_id) \
         VALUES ($1, $2, 'slack_user', 'scope', '\\x00', $3)",
    )
    .bind(account)
    .bind(user)
    .bind(workspace)
    .execute(owner)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO slack_member_channels (account_id, user_id, workspace_id, channel_id, selected) \
         VALUES ($1, $2, $3, $4, $5)",
    )
    .bind(account)
    .bind(user)
    .bind(workspace)
    .bind(channel)
    .bind(selected)
    .execute(owner)
    .await
    .unwrap();
    if let Some((enabled, paused, mode, setup_done)) = row {
        source_row(
            owner,
            account,
            user,
            pause::SLACK_CHANNELS,
            enabled,
            paused,
            mode,
            setup_done,
        )
        .await;
    }
    (account, user)
}

#[tokio::test]
async fn slack_doorbell_targets_follow_pause_and_the_pick_filter_pg() {
    let Some(owner) = pg_test_pool("pause::slack_doorbell").await else {
        return;
    };
    let ws = format!("T{}", &Uuid::new_v4().simple().to_string()[..8]);
    let ch = "C1";
    // (name, selected, row, takes the channel?)
    #[allow(clippy::type_complexity)]
    let cases: Vec<(&str, bool, Option<(bool, bool, Option<&str>, bool)>, bool)> = vec![
        (
            "no row: a member from before the catalog keeps everything",
            false,
            None,
            true,
        ),
        (
            "a legacy row (on, no mode, setup finished) keeps everything",
            false,
            Some((true, false, None, true)),
            true,
        ),
        (
            "everything",
            false,
            Some((true, false, Some("everything"), true)),
            true,
        ),
        (
            "pick, this channel picked",
            true,
            Some((true, false, Some("pick"), true)),
            true,
        ),
        (
            "pick, this channel not picked",
            false,
            Some((true, false, Some("pick"), true)),
            false,
        ),
        (
            "a new connection, source on, scope not chosen: pick with nothing",
            false,
            Some((true, false, None, false)),
            false,
        ),
        (
            "a new connection, source still off",
            true,
            Some((false, false, None, false)),
            false,
        ),
        (
            "PAUSED, everything",
            true,
            Some((true, true, Some("everything"), true)),
            false,
        ),
        (
            "PAUSED, a legacy member",
            true,
            Some((true, true, None, true)),
            false,
        ),
        (
            "PAUSED, pick and picked",
            true,
            Some((true, true, Some("pick"), true)),
            false,
        ),
        (
            "turned off",
            true,
            Some((false, false, Some("everything"), true)),
            false,
        ),
    ];
    let mut members = Vec::new();
    for (name, selected, row, takes) in &cases {
        let (account, user) = slack_member(&owner, &ws, ch, *selected, *row).await;
        members.push((*name, account, user, *takes));
    }

    let targets = crate::connectors::slack_member_capture::member_capture_targets(&owner, &ws, ch)
        .await
        .unwrap();
    let got: BTreeSet<Uuid> = targets.targets.iter().map(|t| t.user_id).collect();
    for (name, _, user, takes) in &members {
        assert_eq!(got.contains(user), *takes, "{name}");
    }

    // The doorbell ingests exactly the members that get a body: none of the
    // blocked ones does, so a paused Slack source writes no atom from an event.
    let bodies = crate::connectors::slack_member_capture::member_bodies(
        &owner,
        &ws,
        ch,
        "1700000000.000100",
        "hello",
        |_| async { crate::connectors::slack_member_capture::MemberToken::Token("xoxp".into()) },
        |_| async {
            crate::connectors::slack_member_capture::MemberFetch::FromMemberToken {
                ts: "1700000000.000100".into(),
                text: "hello".into(),
            }
        },
    )
    .await
    .unwrap();
    let with_body: BTreeSet<Uuid> = bodies.iter().map(|(t, _)| t.user_id).collect();
    assert_eq!(with_body, got);
}

#[tokio::test]
async fn slack_backfill_and_worker_skip_what_capture_does_not_take_pg() {
    let Some(owner) = pg_test_pool("pause::slack_backfill").await else {
        return;
    };
    let ws = format!("T{}", &Uuid::new_v4().simple().to_string()[..8]);
    let state = state_on(&owner);

    // A paused member: the connect/worker backfill reads nothing and writes nothing.
    let (account, user) = slack_member(
        &owner,
        &ws,
        "C-paused",
        true,
        Some((true, true, Some("everything"), true)),
    )
    .await;
    crate::connectors::slack_member_capture::backfill_one_channel(
        &state,
        &state.http_client,
        &owner,
        &owner,
        account,
        user,
        &ws,
        "C-paused",
        "xoxp-member",
        crate::routes::slack::AccountPlane::Personal,
        None,
    )
    .await
    .expect("a paused channel is skipped, not failed");
    assert_eq!(atoms_in_account(&owner, account).await, 0);
    let (cursor, complete): (Option<String>, bool) = sqlx::query_as(
        "SELECT history_cursor, backfill_complete FROM slack_member_channels \
         WHERE account_id = $1 AND user_id = $2 AND channel_id = 'C-paused'",
    )
    .bind(account)
    .bind(user)
    .fetch_one(&owner)
    .await
    .unwrap();
    assert!(
        (cursor, complete) == (None, false),
        "the paused channel's cursor did not move"
    );

    // The same for a pick that did not pick this channel.
    let (account2, user2) = slack_member(
        &owner,
        &ws,
        "C-unpicked",
        false,
        Some((true, false, Some("pick"), true)),
    )
    .await;
    assert!(
        !crate::connectors::catalog::scope::slack_channel_admitted(
            &owner,
            account2,
            user2,
            &ws,
            "C-unpicked"
        )
        .await
    );
    crate::connectors::slack_member_capture::backfill_one_channel(
        &state,
        &state.http_client,
        &owner,
        &owner,
        account2,
        user2,
        &ws,
        "C-unpicked",
        "xoxp-member",
        crate::routes::slack::AccountPlane::Personal,
        None,
    )
    .await
    .unwrap();
    assert_eq!(atoms_in_account(&owner, account2).await, 0);

    // The worker's due list: blocked rows do not crowd out an admitted one, even
    // with more of them than a tick takes, and they are not listed at all.
    let (account3, user3) = member(&owner).await;
    sqlx::query(
        "INSERT INTO oauth_tokens (account_id, owner_user_id, provider, scope, access_token, provider_workspace_id) \
         VALUES ($1, $2, 'slack_user', 'scope', '\\x00', $3)",
    )
    .bind(account3)
    .bind(user3)
    .bind(&ws)
    .execute(&owner)
    .await
    .unwrap();
    source_row(
        &owner,
        account3,
        user3,
        pause::SLACK_CHANNELS,
        true,
        false,
        Some("pick"),
        true,
    )
    .await;
    sqlx::query(
        "INSERT INTO slack_member_channels (account_id, user_id, workspace_id, channel_id, selected, updated_at) \
         SELECT $1, $2, $3, 'C-blocked-' || g, false, now() - interval '2 days' FROM generate_series(1, 150) g",
    )
    .bind(account3)
    .bind(user3)
    .bind(&ws)
    .execute(&owner)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO slack_member_channels (account_id, user_id, workspace_id, channel_id, selected) \
         VALUES ($1, $2, $3, 'C-picked', true)",
    )
    .bind(account3)
    .bind(user3)
    .bind(&ws)
    .execute(&owner)
    .await
    .unwrap();
    let due = crate::connectors::slack_member_capture::due_backfill_rows(&owner, Some(account3))
        .await
        .unwrap();
    assert_eq!(
        due.iter().map(|r| r.3.as_str()).collect::<Vec<_>>(),
        ["C-picked"]
    );
    set_paused(&owner, account3, user3, pause::SLACK_CHANNELS, true).await;
    let due = crate::connectors::slack_member_capture::due_backfill_rows(&owner, Some(account3))
        .await
        .unwrap();
    assert!(due.is_empty(), "a paused member has nothing due: {due:?}");
}

// ---------------------------------------------------------------------------
// Microsoft: the one write boundary of mail, Teams and meetings
// ---------------------------------------------------------------------------

fn ms_item(source_type: &str, source_id: &str) -> crate::connectors::NormalizedItem {
    crate::connectors::NormalizedItem {
        source_type: source_type.into(),
        source_id: source_id.into(),
        content: format!("A message captured as {source_type}."),
        participants: vec![],
        timestamp: chrono::Utc::now(),
        scope: None,
        thread_id: None,
        channel_id: matches!(source_type, "teams_chat" | "teams_channel").then(|| "scope-container".into()),
        author_id: None,
        author_name: None,
    }
}

#[tokio::test]
async fn microsoft_funnel_writes_nothing_for_a_paused_source_pg() {
    let Some(owner) = pg_test_pool("pause::microsoft_funnel").await else {
        return;
    };
    let (account, user) = member(&owner).await;
    let graph: Uuid = sqlx::query_scalar(
        "INSERT INTO graphs (account_id, name, kind, is_primary, owner_user_id) \
         VALUES ($1, 'primary', 'personal', true, NULL) RETURNING id",
    )
    .bind(account)
    .fetch_one(&owner)
    .await
    .unwrap_or_else(|_| panic!("seed graph"));
    let state = state_on(&owner);

    // Positive control: with no row (a connection from before the catalog) a mail
    // item is written through the same function.
    assert!(
        crate::routes::graph_webhook::ingest_message(
            &state,
            account,
            user,
            graph,
            ms_item("outlook", "ctl-1")
        )
        .await.expect("Microsoft ingest admission succeeds"),
        "an unpaused mail item is ingested"
    );
    assert_eq!(atoms_in_account(&owner, account).await, 1);

    // Everything discovers delegated watches; the funnel still requires the
    // acting member's watch, so another member cannot inherit this consent.
    for kind in ["chat", "channel"] {
        sqlx::query("INSERT INTO teams_watched_resources (account_id,user_id,graph_id,kind,team_id,channel_id,chat_id,auth_mode) VALUES ($1,$2,$3,$4,CASE WHEN $4='channel' THEN 'team' ELSE '' END,CASE WHEN $4='channel' THEN 'scope-container' ELSE '' END,CASE WHEN $4='chat' THEN 'scope-container' ELSE '' END,'delegated')")
            .bind(account).bind(user).bind(graph).bind(kind).execute(&owner).await.unwrap();
    }

    let colleague = colleague(&owner, account).await;
    // Each source the funnel carries, paused: nothing is written and the caller
    // is told it was not ingested (so its cursor stays).
    for (key, source_type) in [
        (pause::MS_MAIL, "outlook"),
        (pause::MS_TEAMS_CHATS, "teams_chat"),
        (pause::MS_TEAMS_CHANNELS, "teams_channel"),
        (pause::MS_TEAMS_MEETINGS, "teams_transcript"),
    ] {
        source_row(
            &owner,
            account,
            user,
            key,
            true,
            false,
            Some("everything"),
            true,
        )
        .await;
        if source_type != "teams_transcript" {
        assert!(
            crate::routes::graph_webhook::ingest_message(
                &state,
                account,
                user,
                graph,
                ms_item(source_type, &format!("everything-{source_type}"))
            )
            .await.expect("Microsoft ingest admission succeeds"),
            "Everything captures {source_type}"
        );
        }
        if matches!(source_type, "teams_chat" | "teams_channel") {
            assert!(!crate::routes::graph_webhook::ingest_message(
                &state, account, colleague, graph, ms_item(source_type, "colleague-without-watch")
            ).await.expect("Microsoft ingest admission succeeds"), "Everything must not borrow another member's watch");
        }
        source_row(
            &owner,
            account,
            user,
            key,
            true,
            true,
            Some("everything"),
            true,
        )
        .await;
        let before = atoms_in_account(&owner, account).await;
        let ok = crate::routes::graph_webhook::ingest_message(
            &state,
            account,
            user,
            graph,
            ms_item(source_type, &format!("paused-{source_type}")),
        )
        .await.expect("Microsoft ingest admission succeeds");
        assert!(!ok, "{source_type}: paused");
        assert_eq!(
            atoms_in_account(&owner, account).await,
            before,
            "{source_type}: wrote an atom while paused"
        );
    }

    // Pausing Teams chats leaves Outlook alone (its own row), and a resume lets
    // the next item through.
    source_row(
        &owner,
        account,
        user,
        pause::MS_MAIL,
        true,
        false,
        Some("pick"),
        true,
    )
    .await;
    assert!(
        crate::routes::graph_webhook::ingest_message(
            &state,
            account,
            user,
            graph,
            ms_item("outlook", "ctl-2")
        )
        .await.expect("Microsoft ingest admission succeeds")
    );
    assert_eq!(atoms_in_account(&owner, account).await, 5);
}

// ---------------------------------------------------------------------------
// Calendars (the two workers that read a member's calendar)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn calendar_passes_do_nothing_for_a_paused_source_pg() {
    use crate::workers::microsoft_calendar_sync::PassOutcome as MsOutcome;
    let Some(owner) = pg_test_pool("pause::calendars").await else {
        return;
    };
    let state = state_on(&owner);

    // Microsoft. A connected-looking member with no stored credential: an unpaused
    // pass gets as far as loading it and says so; a paused one never gets there.
    let (account, user) = member(&owner).await;
    sqlx::query(
        "INSERT INTO graphs (account_id, name, kind, is_primary, owner_user_id) \
         VALUES ($1, 'primary', 'personal', true, NULL)",
    )
    .bind(account)
    .execute(&owner)
    .await
    .unwrap();
    let before = crate::workers::microsoft_calendar_sync::sync_personal_account(
        &state, &owner, account, None,
    )
    .await;
    assert!(
        !matches!(before, Ok(MsOutcome::Skipped("source paused"))),
        "an unpaused member must get past the gate: {before:?}"
    );
    source_row(
        &owner,
        account,
        user,
        pause::MS_CALENDAR,
        true,
        true,
        Some("everything"),
        true,
    )
    .await;
    let paused = crate::workers::microsoft_calendar_sync::sync_personal_account(
        &state, &owner, account, None,
    )
    .await;
    assert!(
        matches!(paused, Ok(MsOutcome::Skipped("source paused"))),
        "{paused:?}"
    );
    assert_eq!(atoms_in_account(&owner, account).await, 0);

    // Google, personal.
    use crate::workers::google_calendar_sync::PassOutcome as GOutcome;
    let (account, user) = member(&owner).await;
    let before =
        crate::workers::google_calendar_sync::sync_personal_account(&state, &owner, account).await;
    assert!(
        !matches!(before, Ok(GOutcome::Skipped("source paused"))),
        "an unpaused member must get past the gate: {before:?}"
    );
    source_row(
        &owner,
        account,
        user,
        pause::GOOGLE_CALENDAR,
        true,
        true,
        Some("everything"),
        true,
    )
    .await;
    let paused =
        crate::workers::google_calendar_sync::sync_personal_account(&state, &owner, account).await;
    assert!(
        matches!(paused, Ok(GOutcome::Skipped("source paused"))),
        "{paused:?}"
    );
    assert_eq!(atoms_in_account(&owner, account).await, 0);

    // Google, an organization member's own pass.
    let (account, user) = member(&owner).await;
    let before = crate::workers::google_calendar_sync::sync_org_member(
        &state,
        &owner,
        &owner,
        account,
        "org_pause",
        user,
    )
    .await;
    assert!(
        !matches!(before, Ok(GOutcome::Skipped("source paused"))),
        "an unpaused member must get past the gate: {before:?}"
    );
    source_row(
        &owner,
        account,
        user,
        pause::GOOGLE_CALENDAR,
        true,
        true,
        Some("everything"),
        true,
    )
    .await;
    let paused = crate::workers::google_calendar_sync::sync_org_member(
        &state,
        &owner,
        &owner,
        account,
        "org_pause",
        user,
    )
    .await;
    assert!(
        matches!(paused, Ok(GOutcome::Skipped("source paused"))),
        "{paused:?}"
    );
    assert_eq!(atoms_in_account(&owner, account).await, 0);
}

// ---------------------------------------------------------------------------
// Meet
// ---------------------------------------------------------------------------

#[tokio::test]
async fn meet_writes_no_copy_for_a_paused_source_pg() {
    use crate::connectors::google_meet::{
        ingest_transcript_for_attendee, MeetPlane, ResolvedAttendee,
    };
    let Some(owner) = pg_test_pool("pause::meet").await else {
        return;
    };
    let state = state_on(&owner);
    let (account, user) = member(&owner).await;
    let attendee = ResolvedAttendee {
        account_id: account,
        owner_user_id: user,
    };
    let ts = chrono::Utc::now();

    // Positive control: with no row the copy lands in the member's own ocean.
    let landed = ingest_transcript_for_attendee(
        &MeetPlane::Personal,
        &state,
        &owner,
        attendee,
        "conferenceRecords/ctl",
        "Alice: hello there",
        &[],
        ts,
    )
    .await;
    assert!(
        matches!(landed, Ok(true)),
        "an unpaused copy lands: {landed:?}"
    );
    let before = atoms_in_account(&owner, account).await;
    assert!(before >= 1);

    source_row(
        &owner,
        account,
        user,
        pause::GOOGLE_MEET,
        true,
        true,
        Some("pick"),
        true,
    )
    .await;
    let paused = ingest_transcript_for_attendee(
        &MeetPlane::Personal,
        &state,
        &owner,
        attendee,
        "conferenceRecords/paused",
        "Alice: should not land",
        &[],
        ts,
    )
    .await;
    assert!(
        paused.is_err(),
        "a paused source is an error (the capture stays open), not Ok(false): {paused:?}"
    );
    assert_eq!(atoms_in_account(&owner, account).await, before);
}

// ---------------------------------------------------------------------------
// Chat
// ---------------------------------------------------------------------------

#[tokio::test]
async fn chat_syncs_nothing_for_a_paused_source_pg() {
    use crate::connectors::google_chat::IngestableSpace;
    let Some(owner) = pg_test_pool("pause::chat").await else {
        return;
    };
    let state = state_on(&owner);
    let (account, user) = member(&owner).await;
    let graph: Uuid = sqlx::query_scalar(
        "INSERT INTO graphs (account_id, name, kind, is_primary, owner_user_id) \
         VALUES ($1, 'member ocean', 'member', true, $2) RETURNING id",
    )
    .bind(account)
    .bind(user)
    .fetch_one(&owner)
    .await
    .unwrap();
    let space = IngestableSpace {
        space_id: "spaces/AAA".into(),
        mode: None,
        reader_user_id: Some(user),
        display_name: "Group".into(),
        history_state: "HISTORY_ON".into(),
        backfill_complete: false,
    };
    source_row(
        &owner,
        account,
        user,
        pause::GOOGLE_CHAT,
        true,
        true,
        Some("pick"),
        true,
    )
    .await;
    let out = crate::workers::google_chat_sweep::sync_mailbox_space_once(
        &state,
        &owner,
        &owner,
        &crate::tenancy::TenantId::Consumer,
        account,
        user,
        &space,
    )
    .await
    .expect("a paused source is skipped, not failed");
    assert_eq!(out, None, "nothing ran");
    assert_eq!(atoms_in_account(&owner, account).await, 0);
}

// ---------------------------------------------------------------------------
// GitHub
// ---------------------------------------------------------------------------

#[tokio::test]
async fn github_events_write_nothing_for_a_paused_source_pg() {
    let Some(owner) = pg_test_pool("pause::github").await else {
        return;
    };
    let state = state_on(&owner);
    let (account, user) = member(&owner).await;
    let graph: Uuid = sqlx::query_scalar(
        "INSERT INTO graphs (account_id, name, kind, is_primary, owner_user_id) \
         VALUES ($1, 'primary', 'personal', true, NULL) RETURNING id",
    )
    .bind(account)
    .fetch_one(&owner)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO github_watched_repos \
             (account_id, installation_id, repo_id, owner, name, default_branch, active, graph_id, \
              owner_user_id, destination_kind) \
         VALUES ($1, 42, 4242, 'acme', 'rocket', 'main', true, $2, $3, 'personal')",
    )
    .bind(account)
    .bind(graph)
    .bind(user)
    .execute(&owner)
    .await
    .unwrap();
    let payload = serde_json::json!({"repository": {"id": 4242}, "action": "opened", "pull_request": {"number": 1}});
    let tenant = crate::tenancy::TenantId::Consumer;

    // Positive control: an unpaused watch row gets as far as asking GitHub for an
    // installation token, which a test has none of.
    let live = crate::connectors::github_event_ingest::handle_event(
        &tenant,
        &owner,
        &state,
        account,
        42,
        "pull_request",
        &payload,
    )
    .await;
    assert!(
        live.as_ref()
            .is_err_and(|e| e.contains("installation token")),
        "an unpaused row proceeds: {:?}",
        live.as_ref().err()
    );

    source_row(
        &owner,
        account,
        user,
        pause::GITHUB_REPOS,
        true,
        true,
        Some("pick"),
        true,
    )
    .await;
    let paused = crate::connectors::github_event_ingest::handle_event(
        &tenant,
        &owner,
        &state,
        account,
        42,
        "pull_request",
        &payload,
    )
    .await
    .expect("a paused row is skipped before any GitHub call");
    assert!(matches!(
        paused,
        crate::connectors::github_event_ingest::EventOutcome::NothingToDo
    ));
    assert_eq!(atoms_in_account(&owner, account).await, 0);
}

// ---------------------------------------------------------------------------
// Composio
// ---------------------------------------------------------------------------

#[tokio::test]
async fn composio_poll_reads_nothing_for_a_paused_toolkit_pg() {
    use crate::connectors::composio::{recipe, Client};
    let Some(owner) = pg_test_pool("pause::composio").await else {
        return;
    };
    // `poll` needs a configured client to be called at all; the gate answers
    // before any request is made.
    std::env::set_var("COMPOSIO_API_KEY", "pause-test-key");
    let client = Client::configured().expect("client");
    let state = state_on(&owner);
    let (account, user) = member(&owner).await;
    let tenant = crate::tenancy::extractor::Tenant {
        store: state.store.clone(),
        pool: Some(owner.clone()),
        decision: None,
        class: crate::route_class::RouteClass::Other,
    };
    let user_ctx = crate::billing::test_support::user_context(account, user);
    let jira = recipe::get("jira").unwrap();
    let capture = Uuid::new_v4();

    // Unpaused: the poll goes looking for the capture and does not find it.
    let resp =
        crate::routes::composio::poll(&state, &tenant, &user_ctx, jira, capture, &client).await;
    let body = crate::billing::test_support::body_json(resp).await;
    assert_eq!(
        body["error"], "Capture is paused, disconnected or busy",
        "{body}"
    );

    sqlx::query(
        "INSERT INTO connector_sources (account_id, owner_user_id, provider, source, enabled, paused_at) \
         VALUES ($1, $2, 'jira', 'workspace', true, now())",
    )
    .bind(account)
    .bind(user)
    .execute(&owner)
    .await
    .unwrap();
    let resp =
        crate::routes::composio::poll(&state, &tenant, &user_ctx, jira, capture, &client).await;
    assert_eq!(resp.status(), axum::http::StatusCode::CONFLICT);
    let body = crate::billing::test_support::body_json(resp).await;
    assert_eq!(
        body["error"], "Capture is paused",
        "the gate answered, before any lease: {body}"
    );
}

#[tokio::test]
async fn everything_chat_discovery_selection_and_member_isolation_pg() {
    use super::{registry, AdapterCx, Facts, ScopeMode, ScopeSelection};
    use crate::connectors::{google_chat, TokenBundle, TokenOwner};
    use crate::routes::oauth::store_token_bundle;
    let Some(pool) = pg_test_pool("everything::chat").await else {
        return;
    };
    let (account, user) = member(&pool).await;
    let state = state_on(&pool);
    let tenant = crate::tenancy::extractor::Tenant {
        pool: Some(pool.clone()),
        store: state.store.clone(),
        decision: None,
        class: crate::route_class::RouteClass::Other,
    };
    let caller = crate::billing::test_support::user_context(account, user);
    sqlx::query("INSERT INTO graphs (account_id, name, kind, is_primary, owner_user_id) VALUES ($1, 'member ocean', 'member', true, $2)").bind(account).bind(user).execute(&pool).await.unwrap();
    let bundle = TokenBundle { access_token: "everything-member".into(), refresh_token: None, user_token: None, scope: "https://www.googleapis.com/auth/chat.spaces.readonly https://www.googleapis.com/auth/chat.messages.readonly".into(), expires_at: Some(chrono::Utc::now()+chrono::Duration::hours(1)), provider_workspace_id: "member@example.com".into(), provider_workspace_name: None, provider_user_id: "member".into() };
    let plane = crate::crypto::DekPlane::from_decision(None, &pool);
    store_token_bundle(
        &pool,
        &plane,
        account,
        "google_chat",
        TokenOwner::Member(user),
        &bundle,
    )
    .await
    .unwrap();
    async fn list() -> axum::Json<serde_json::Value> {
        axum::Json(
            serde_json::json!({"spaces":[{"name":"spaces/A","spaceType":"DIRECT_MESSAGE","historyState":"HISTORY_ON"},{"name":"spaces/B","spaceType":"GROUP_CHAT","historyState":"HISTORY_ON"}]}),
        )
    }
    async fn space(
        axum::extract::Path(id): axum::extract::Path<String>,
    ) -> axum::Json<serde_json::Value> {
        axum::Json(
            serde_json::json!({"name":format!("spaces/{id}"),"spaceType":"DIRECT_MESSAGE","historyState":"HISTORY_ON"}),
        )
    }
    async fn messages() -> axum::Json<serde_json::Value> {
        axum::Json(
            serde_json::json!({"messages":[{"name":"spaces/A/messages/one","text":"Everything captures this member message", "createTime":chrono::Utc::now().to_rfc3339(),"sender":{"name":"users/sender","type":"HUMAN","displayName":"Sender"},"thread":{"name":"spaces/A/threads/thread"}}]}),
        )
    }
    let app = axum::Router::new()
        .route("/spaces", axum::routing::get(list))
        .route("/spaces/{id}", axum::routing::get(space))
        .route("/spaces/{id}/messages", axum::routing::get(messages));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    google_chat::TEST_CHAT_API_BASE.scope(base, async {
        let facts = Facts::load(&state, &tenant, account, user).await.unwrap();
        let cx = AdapterCx::new(&facts, &state, &tenant, &caller);
        let adapter = registry::lookup("google").unwrap();
        let source = adapter.sources().iter().find(|s| s.key == "chat").unwrap();
        adapter.set_scope(&cx, source, &ScopeSelection { mode: ScopeMode::Everything, item_ids: vec![] }).await.unwrap();
        let rows: Vec<(String, Uuid)> = sqlx::query_as("SELECT space_id, owner_user_id FROM google_chat_watched_spaces WHERE account_id=$1 AND active ORDER BY space_id").bind(account).fetch_all(&pool).await.unwrap();
        assert_eq!(rows, vec![("spaces/A".into(), user), ("spaces/B".into(), user)]);
        source_row(&pool, account, user, pause::GOOGLE_CHAT, true, false, Some("everything"), true).await;
        let watched = google_chat::ingestable_mailbox_space(&pool, account, "spaces/A", user).await.unwrap().unwrap();
        let result = crate::workers::google_chat_sweep::sync_mailbox_space_once(&state, &pool, &pool, &crate::tenancy::TenantId::Consumer, account, user, &watched).await.unwrap();
        assert!(result.unwrap() > 0);
        assert!(atoms_in_account(&pool, account).await > 0);
        let stranger = colleague(&pool, account).await;
        assert!(google_chat::ingestable_mailbox_space(&pool, account, "spaces/A", stranger).await.unwrap().is_none());
        let worker = crate::workers::tenant_harness::WorkerTenant { tenant: crate::tenancy::TenantId::Consumer, pool: pool.clone(), store: state.store.clone().unwrap() };
        set_paused(&pool, account, user, pause::GOOGLE_CHAT, true).await;
        super::everything::refresh(&worker, &pool, &state, "google").await.unwrap();
        assert_eq!(crate::workers::google_chat_sweep::sync_mailbox_space_once(&state, &pool, &pool, &crate::tenancy::TenantId::Consumer, account, user, &watched).await.unwrap(), None);
        let error = adapter.set_scope(&cx, source, &ScopeSelection { mode: ScopeMode::Pick, item_ids: vec!["foreign".into()] }).await;
        assert!(error.is_err());
        assert_eq!(sqlx::query_scalar::<_,i64>("SELECT count(*) FROM google_chat_watched_spaces WHERE account_id=$1 AND active").bind(account).fetch_one(&pool).await.unwrap(), 2);
        adapter.set_scope(&cx, source, &ScopeSelection { mode: ScopeMode::Pick, item_ids: vec!["A".into()] }).await.unwrap();
        set_paused(&pool, account, user, pause::GOOGLE_CHAT, false).await;
        super::everything::refresh(&worker, &pool, &state, "google").await.unwrap();
        assert_eq!(sqlx::query_scalar::<_,i64>("SELECT count(*) FROM google_chat_watched_spaces WHERE account_id=$1 AND active AND owner_user_id=$2").bind(account).bind(user).fetch_one(&pool).await.unwrap(), 1);
    }).await;
    server.abort();
}

#[tokio::test]
async fn slack_install_destinations_honor_each_members_scope_pg() {
    use super::scope::{slack_destination_admitted, slack_set_selected};
    let Some(pool) = pg_test_pool("scope::slack_install_destination").await else { return; };
    let (account, alice) = member(&pool).await;
    let bob = colleague(&pool, account).await;
    let personal: Uuid = sqlx::query_scalar("INSERT INTO graphs(account_id,name,kind,is_primary) VALUES ($1,'personal','personal',true) RETURNING id").bind(account).fetch_one(&pool).await.unwrap();
    let bob_graph: Uuid = sqlx::query_scalar("INSERT INTO graphs(account_id,name,kind,owner_user_id,is_primary) VALUES ($1,'Bob','member',$2,true) RETURNING id").bind(account).bind(bob).fetch_one(&pool).await.unwrap();
    let shared: Uuid = sqlx::query_scalar("INSERT INTO graphs(account_id,name,kind) VALUES ($1,'shared','team') RETURNING id").bind(account).fetch_one(&pool).await.unwrap();
    // No catalog row preserves legacy capture for the personal account owner.
    assert!(slack_destination_admitted(&pool,&pool,account,personal,"W","A").await.unwrap());
    source_row(&pool,account,alice,pause::SLACK_CHANNELS,true,false,Some("pick"),true).await;
    slack_set_selected(&pool,account,alice,"W",&["A".into()]).await.unwrap();
    assert!(slack_destination_admitted(&pool,&pool,account,personal,"W","A").await.unwrap());
    assert!(!slack_destination_admitted(&pool,&pool,account,personal,"W","B").await.unwrap());
    source_row(&pool,account,bob,pause::SLACK_CHANNELS,true,false,Some("everything"),true).await;
    for (enabled, paused) in [(true,true),(false,false)] {
        source_row(&pool,account,alice,pause::SLACK_CHANNELS,enabled,paused,Some("everything"),true).await;
        assert!(!slack_destination_admitted(&pool,&pool,account,personal,"W","A").await.unwrap());
        assert!(slack_destination_admitted(&pool,&pool,account,bob_graph,"W","A").await.unwrap());
        assert!(slack_destination_admitted(&pool,&pool,account,shared,"W","A").await.unwrap());
    }
    // Another account cannot use a known graph ID; a departed member gets no copy.
    let (other, _) = member(&pool).await;
    assert!(!slack_destination_admitted(&pool,&pool,other,bob_graph,"W","A").await.unwrap());
    sqlx::query("DELETE FROM account_members WHERE account_id=$1 AND user_id=$2").bind(account).bind(bob).execute(&pool).await.unwrap();
    assert!(!slack_destination_admitted(&pool,&pool,account,bob_graph,"W","A").await.unwrap());
}

//! Integration tests for the `_scoped` `Store` trait surface added for the
//! chunk 2 table cluster (org-tenancy rollout, PR B.0 chunk 2):
//! conversations/conversation_messages/conversation_oceans/
//! conversation_surfaced_edges, meeting_transcripts/transcript_corrections/
//! meeting_shares, query_history, activity_events, teams/team_members/
//! team_keys/team_member_key_envelopes, node_atoms/node_merges/
//! supersession_edge/relationship_edge, op_log, and coding_sessions/
//! coding_session_shares/projects.
//!
//! Same pattern and rigor as `store_scoped_atoms_nodes_graphs_pg.rs` (chunk
//! 1): drives `PostgresStore` directly as a real `coralswarm_app` login.
//! Not every one of the ~78 new `_scoped` methods gets its own assertion
//! here — this covers a representative read+write per table (some tables,
//! e.g. `node_atoms`/`supersession_edge`, already got write-side coverage
//! from chunk 1's tests via methods that touch them incidentally, so this
//! file adds only the NEW read/write shapes chunk 2 introduces for them).
//!
//! Gated on `ORGT_TEST_BASE_URL`:
//!
//!   ORGT_TEST_BASE_URL=postgres://alex@localhost:5432/postgres \
//!     cargo test -p coralswarm-backend --test store_scoped_chunk2_pg

use std::str::FromStr;

use chrono::Utc;
use coralswarm_backend::relationships::{EdgeOrigin, EdgeStatus};
use coralswarm_backend::store::{NewMessage, NewOp, OpType, PostgresStore, Store};
use coralswarm_backend::tenancy::scope::begin_account_scoped;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::{Connection, Executor, PgConnection, PgPool};
use uuid::Uuid;
use coralswarm_backend::store::SessionIdentity;

const APP_ROLE_TEST_PASSWORD: &str = "orgt_store_scoped_chunk2_test_only_pw";

fn base_url() -> Option<String> {
    std::env::var("ORGT_TEST_BASE_URL").ok()
}
fn with_db(base: &str, db: &str) -> String {
    let (prefix, rest) = base.split_at(base.rfind('/').expect("url has a path /"));
    let params = rest.find('?').map(|i| &rest[i..]).unwrap_or("");
    format!("{prefix}/{db}{params}")
}
async fn admin(base: &str) -> PgConnection {
    PgConnection::connect(base).await.expect("connect admin")
}
async fn recreate(conn: &mut PgConnection, db: &str) {
    conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
        .await
        .unwrap();
    conn.execute(format!("CREATE DATABASE {db}").as_str())
        .await
        .unwrap();
}

/// A pool connected AS `coralswarm_app` (real login, not `SET ROLE`).
async fn app_role_pool(db_url: &str) -> PgPool {
    let opts = PgConnectOptions::from_str(db_url)
        .expect("parse scratch db url")
        .username("coralswarm_app")
        .password(APP_ROLE_TEST_PASSWORD);
    PgPoolOptions::new()
        .max_connections(2)
        .connect_with(opts)
        .await
        .expect("connect AS coralswarm_app")
}

/// One (user, account, graph, atom, node, second node) fixture, seeded via
/// the owner/BYPASSRLS pool. The second node exists purely to give
/// `relationship_edge` a distinct (src, dst) pair.
struct Seeded {
    user_id: Uuid,
    account_id: Uuid,
    graph_id: Uuid,
    node_id: Uuid,
    node_b_id: Uuid,
}

async fn seed_account(owner: &PgPool, email: &str) -> Seeded {
    let user_id: Uuid = sqlx::query_scalar("INSERT INTO users(email) VALUES ($1) RETURNING id")
        .bind(email)
        .fetch_one(owner)
        .await
        .expect("insert user");
    let account_id: Uuid =
        sqlx::query_scalar("INSERT INTO accounts(owner_user_id) VALUES ($1) RETURNING id")
            .bind(user_id)
            .fetch_one(owner)
            .await
            .expect("insert account");
    let graph_id: Uuid = sqlx::query_scalar(
        "INSERT INTO graphs(account_id, name, kind, is_primary) \
         VALUES ($1, 'home', 'personal', true) RETURNING id",
    )
    .bind(account_id)
    .fetch_one(owner)
    .await
    .expect("insert graph");
    let node_id: Uuid = sqlx::query_scalar(
        "INSERT INTO nodes(graph_id, type, title) VALUES ($1, 'entity', 'Probe Reef A') \
         RETURNING node_id",
    )
    .bind(graph_id)
    .fetch_one(owner)
    .await
    .expect("insert node");
    let node_b_id: Uuid = sqlx::query_scalar(
        "INSERT INTO nodes(graph_id, type, title) VALUES ($1, 'entity', 'Probe Reef B') \
         RETURNING node_id",
    )
    .bind(graph_id)
    .fetch_one(owner)
    .await
    .expect("insert second node");

    Seeded { user_id, account_id, graph_id, node_id, node_b_id }
}

/// Positive path: a representative read+write per chunk-2 table cluster,
/// all inside ONE `begin_account_scoped` transaction, correctly scoped to
/// that account — including a cross-account read proof for a graph_id-
/// mediated table (`conversations`, which is NOT direct-account_id at the
/// SQL predicate level the same way as `list_atoms_scoped` in chunk 1).
#[tokio::test]
async fn chunk2_scoped_methods_read_and_write_correctly_when_account_is_scoped() {
    let Some(base) = base_url() else {
        eprintln!(
            "skipping chunk2_scoped_methods_read_and_write_correctly_when_account_is_scoped: \
             set ORGT_TEST_BASE_URL"
        );
        return;
    };

    let db = "orgt_store_scoped_chunk2_rw";
    let _cleanup_db = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
    let mut admin_conn = admin(&base).await;
    recreate(&mut admin_conn, db).await;
    let db_url = with_db(&base, db);

    let owner: PgPool = PgPoolOptions::new().connect(&db_url).await.expect("connect owner");
    sqlx::migrate!("./migrations_control")
        .run(&owner)
        .await
        .expect("apply control tree incl. 0083_consumer_rls");
    sqlx::query(&format!(
        "ALTER ROLE coralswarm_app WITH PASSWORD '{APP_ROLE_TEST_PASSWORD}'"
    ))
    .execute(&owner)
    .await
    .expect("set test-only password on coralswarm_app");

    let a = seed_account(&owner, "chunk2-a@x.co").await;
    let b = seed_account(&owner, "chunk2-b@x.co").await;

    let app_pool = app_role_pool(&db_url).await;
    let store = PostgresStore::new(app_pool.clone());

    let mut tx = begin_account_scoped(&app_pool, a.account_id)
        .await
        .expect("begin_account_scoped for account A");

    // ── conversations / conversation_messages / conversation_oceans /
    // conversation_surfaced_edges ────────────────────────────────────────
    let convo = store
        .create_conversation_scoped(&mut tx, a.account_id, a.graph_id, a.user_id, "Probe Convo")
        .await
        .expect("create_conversation_scoped must succeed for A's own account/graph");

    let convo_read = store
        .get_conversation_scoped(&mut tx, convo.id)
        .await
        .expect("get_conversation_scoped must succeed");
    assert_eq!(convo_read.map(|c| c.id), Some(convo.id));

    let msg = store
        .append_message_scoped(
            &mut tx,
            NewMessage {
                conversation_id: convo.id,
                role: "user".to_string(),
                text: "hello".to_string(),
                citations: serde_json::json!([]),
                sources: serde_json::json!([]),
                mode: None,
                timing: serde_json::json!({}),
                scope_graph_ids: vec![a.graph_id],
            },
        )
        .await
        .expect("append_message_scoped must succeed for A's own conversation");
    let messages = store
        .list_messages_scoped(&mut tx, convo.id, None, coralswarm_backend::store::CONVERSATION_MESSAGE_PAGE)
        .await
        .expect("list_messages_scoped must succeed");
    assert_eq!(messages.iter().map(|m| m.id).collect::<Vec<_>>(), vec![msg.id]);

    store
        .replace_conversation_oceans_scoped(&mut tx, convo.id, &[a.graph_id])
        .await
        .expect("replace_conversation_oceans_scoped must succeed for A's own graph_id");
    let oceans = store
        .list_conversation_oceans_scoped(&mut tx, convo.id)
        .await
        .expect("list_conversation_oceans_scoped must succeed");
    assert_eq!(oceans, vec![a.graph_id]);

    // ── relationship_edge (needed before conversation_surfaced_edges, which
    // FKs to it) ──────────────────────────────────────────────────────────
    let edge = store
        .upsert_relationship_edge_scoped(
            &mut tx,
            a.graph_id,
            a.node_id,
            "knows",
            a.node_b_id,
            EdgeOrigin::Stated,
            EdgeStatus::Suggested,
            0.9,
            serde_json::json!({}),
        )
        .await
        .expect("upsert_relationship_edge_scoped must succeed for A's own graph_id");
    let edge_read = store
        .get_relationship_edge_scoped(&mut tx, edge.edge_id)
        .await
        .expect("get_relationship_edge_scoped must succeed");
    assert_eq!(edge_read.map(|e| e.edge_id), Some(edge.edge_id));

    store
        .record_surfaced_relationship_edges_scoped(&mut tx, convo.id, &[edge.edge_id])
        .await
        .expect("record_surfaced_relationship_edges_scoped must succeed");
    let surfaced = store
        .surfaced_relationship_edges_scoped(&mut tx, convo.id)
        .await
        .expect("surfaced_relationship_edges_scoped must succeed");
    assert_eq!(surfaced, vec![edge.edge_id]);

    // ── meeting_transcripts / transcript_corrections / meeting_shares ────
    store
        .upsert_meeting_transcript_scoped(
            &mut tx,
            a.graph_id,
            "probe-meeting-1",
            None,
            Some("hello world"),
            Some(1000),
            None,
        )
        .await
        .expect("upsert_meeting_transcript_scoped must succeed for A's own graph_id");
    let transcript = store
        .get_meeting_transcript_scoped(&mut tx, a.graph_id, "probe-meeting-1")
        .await
        .expect("get_meeting_transcript_scoped must succeed");
    assert!(transcript.is_some());

    let atom_id_for_correction: Uuid = sqlx::query_scalar(
        "INSERT INTO atoms(graph_id, source_type, source_id, content_blob_key, timestamp) \
         VALUES ($1, 'test', 'probe-meeting-1:chunk:0', 'blob-key', now()) RETURNING atom_id",
    )
    .bind(a.graph_id)
    .fetch_one(&owner)
    .await
    .expect("seed atom for transcript correction");
    store
        .insert_transcript_correction_scoped(
            &mut tx,
            a.graph_id,
            atom_id_for_correction,
            "probe-meeting-1",
            "Casey",
            "Aria",
            None,
            Some(0.8),
            "applied",
        )
        .await
        .expect("insert_transcript_correction_scoped must succeed for A's own graph_id");
    let corrections = store
        .list_transcript_corrections_scoped(&mut tx, a.graph_id, "probe-meeting-1")
        .await
        .expect("list_transcript_corrections_scoped must succeed");
    assert_eq!(corrections.len(), 1);

    let share = store
        .create_meeting_share_scoped(
            &mut tx,
            a.graph_id,
            "probe-meeting-1",
            "probe-token-abc",
            "link",
            a.user_id,
        )
        .await
        .expect("create_meeting_share_scoped must succeed for A's own graph_id");
    let shares = store
        .list_meeting_shares_scoped(&mut tx, a.graph_id, "probe-meeting-1")
        .await
        .expect("list_meeting_shares_scoped must succeed");
    assert_eq!(shares.iter().map(|s| s.id).collect::<Vec<_>>(), vec![share.id]);

    // ── query_history / activity_events ───────────────────────────────────
    store
        .insert_query_history_scoped(
            &mut tx,
            Uuid::new_v4(),
            a.account_id,
            a.graph_id,
            a.user_id,
            "what happened today?",
            "a probe answer",
            0,
            10,
            20,
            None,
        )
        .await
        .expect("insert_query_history_scoped must succeed for A's own account_id");
    let history = store
        .list_query_history_scoped(&mut tx, a.account_id, a.user_id, Some(a.graph_id), 10, 0)
        .await
        .expect("list_query_history_scoped must succeed");
    assert_eq!(history.len(), 1);

    // Billing-completeness audit: `model` round-trips through the scoped
    // insert path. `list_query_history_scoped`/`QueryHistoryRow` don't
    // project `model` (same shape as the unscoped `PostgresStore` read
    // paths), so assert it directly against the row via SQL on the
    // still-open scoped transaction rather than through the read path.
    store
        .insert_query_history_scoped(
            &mut tx,
            Uuid::new_v4(),
            a.account_id,
            a.graph_id,
            a.user_id,
            "what model?",
            "a probe answer",
            0,
            10,
            20,
            Some("claude-test-model"),
        )
        .await
        .expect("insert_query_history_scoped with a model must succeed for A's own account_id");
    let stored_model: Option<String> =
        sqlx::query_scalar("SELECT model FROM query_history WHERE query_text = $1")
            .bind("what model?")
            .fetch_one(&mut *tx)
            .await
            .expect("select model for the just-inserted query_history row");
    assert_eq!(stored_model.as_deref(), Some("claude-test-model"));

    let activity = store
        .list_activity_scoped(&mut tx, a.account_id, a.user_id, None, None, 10, 0)
        .await
        .expect("list_activity_scoped must succeed (empty is fine — no writer path via Store)");
    assert!(activity.is_empty());

    // ── teams / team_members / team_keys / team_member_key_envelopes ─────
    let created_team = store
        .create_team_tx_scoped(&mut tx, a.account_id, a.user_id, "Eng", &[1, 2, 3])
        .await
        .expect("create_team_tx_scoped must succeed for A's own account_id");
    assert!(
        store
            .team_exists_scoped(&mut tx, created_team.team_id)
            .await
            .expect("team_exists_scoped must succeed")
    );
    assert_eq!(
        store
            .count_team_admins_scoped(&mut tx, created_team.team_id)
            .await
            .expect("count_team_admins_scoped must succeed"),
        1
    );

    let new_member_id: Uuid = sqlx::query_scalar("INSERT INTO users(email) VALUES ($1) RETURNING id")
        .bind("chunk2-a-member@x.co")
        .fetch_one(&owner)
        .await
        .expect("insert new team member user");
    // Looked up on the SAME scoped transaction, not the owner pool: the row
    // hasn't committed yet (still inside `tx`), and `team_keys` is itself
    // RLS-covered (graph_id-mediated), so this doubles as another read-side
    // proof under scope.
    let team_key_id: Uuid = sqlx::query_scalar("SELECT id FROM team_keys WHERE graph_id = $1")
        .bind(created_team.graph_id)
        .fetch_one(&mut *tx)
        .await
        .expect("look up seeded team_key_id inside A's scoped transaction");
    store
        .add_team_member_tx_scoped(
            &mut tx,
            created_team.team_id,
            team_key_id,
            new_member_id,
            "member",
            &[9, 9],
            a.user_id,
        )
        .await
        .expect("add_team_member_tx_scoped must succeed for A's own team");

    // ── node_atoms / node_merges / supersession_edge: NEW read shapes not
    // already covered by chunk 1's write-path tests ─────────────────────
    let atom_for_node: Uuid = sqlx::query_scalar(
        "INSERT INTO atoms(graph_id, source_type, source_id, content_blob_key, timestamp) \
         VALUES ($1, 'test', 'probe-2', 'blob-key-2', now()) RETURNING atom_id",
    )
    .bind(a.graph_id)
    .fetch_one(&owner)
    .await
    .expect("seed atom for node_atoms");
    sqlx::query("INSERT INTO node_atoms(node_id, atom_id) VALUES ($1, $2)")
        .bind(a.node_id)
        .bind(atom_for_node)
        .execute(&owner)
        .await
        .expect("seed node_atoms link");
    let linked_atoms = store
        .node_atom_ids_scoped(&mut tx, a.node_id)
        .await
        .expect("node_atom_ids_scoped must succeed for A's own node");
    assert_eq!(linked_atoms, vec![atom_for_node]);

    let exists = store
        .supersession_edge_exists_scoped(&mut tx, a.node_id, a.node_b_id)
        .await
        .expect("supersession_edge_exists_scoped must succeed (false is a valid, non-error result)");
    assert!(!exists);

    // ── non_supersession_pair (D3/§3.7): brand-new scoped surface, no chunk-1
    // coverage to lean on ──────────────────────────────────────────────────
    let rejected_before = store
        .is_non_supersession_pair_scoped(&mut tx, a.node_id, a.node_b_id)
        .await
        .expect("is_non_supersession_pair_scoped must succeed for A's own nodes");
    assert!(!rejected_before);

    store
        .mark_non_supersession_pair_scoped(&mut tx, a.node_id, a.node_b_id, Some(a.user_id))
        .await
        .expect("mark_non_supersession_pair_scoped must succeed for A's own nodes");

    // Order-insensitive, exactly as the unscoped twin is.
    assert!(store
        .is_non_supersession_pair_scoped(&mut tx, a.node_b_id, a.node_id)
        .await
        .expect("is_non_supersession_pair_scoped (reversed args) must succeed"));

    // Idempotent: re-marking the same (RLS-covered) pair must not error.
    store
        .mark_non_supersession_pair_scoped(&mut tx, a.node_b_id, a.node_id, None)
        .await
        .expect("re-marking an already-rejected pair must be a no-op, not an error");

    // ── op_log ────────────────────────────────────────────────────────────
    let seq = store
        .append_op_scoped(
            &mut tx,
            &NewOp {
                op_id: Uuid::new_v4(),
                entity: "node".to_string(),
                entity_id: a.node_id.to_string(),
                op_type: OpType::Upsert,
                payload: serde_json::json!({}),
                version: 1,
                updated_at: Utc::now(),
                device_id: "test-device".to_string(),
                graph_id: Some(a.graph_id),
            },
        )
        .await
        .expect("append_op_scoped must succeed");
    let appended: bool = sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM op_log WHERE seq = $1)")
        .bind(seq)
        .fetch_one(&mut *tx)
        .await
        .expect("op_log read in the scoped transaction must succeed");
    assert!(appended);

    // ── coding_sessions / coding_session_shares / projects ────────────────
    store
        .upsert_coding_session_scoped(
            &mut tx,
            a.graph_id,
            "probe-session-1",
            "coding",
            Some("probe-project"),
            Some("Probe Session"),
            None,
            None,
            None,
            None,
            None,
            None,
            &SessionIdentity::default(),
        )
        .await
        .expect("upsert_coding_session_scoped must succeed for A's own graph_id");
    let session = store
        .get_coding_session_scoped(&mut tx, a.graph_id, "probe-session-1")
        .await
        .expect("get_coding_session_scoped must succeed");
    assert!(session.is_some());

    let session_share = store
        .create_session_share_scoped(
            &mut tx,
            a.graph_id,
            "probe-session-1",
            "probe-session-token",
            "link",
            a.user_id,
        )
        .await
        .expect("create_session_share_scoped must succeed for A's own graph_id");
    let session_shares = store
        .list_session_shares_scoped(&mut tx, a.graph_id, "probe-session-1")
        .await
        .expect("list_session_shares_scoped must succeed");
    assert_eq!(session_shares.iter().map(|s| s.id).collect::<Vec<_>>(), vec![session_share.id]);

    tx.commit().await.expect("commit A's scoped transaction");

    // ── cross-account IDOR-pivot proof: B's conversation must not be
    // writable via A's scoped transaction (conversations is graph_id-owned,
    // not a direct predicate baked into every query). Runs in its OWN fresh
    // transaction — a real Postgres error aborts whatever transaction it
    // happens in, so this must not share the transaction just committed
    // above (mirrors chunk 1's cross-account write probe).
    {
        let mut cross_tx = begin_account_scoped(&app_pool, a.account_id)
            .await
            .expect("begin_account_scoped for the cross-account write probe");
        let b_convo = store
            .create_conversation_scoped(&mut cross_tx, b.account_id, b.graph_id, b.user_id, "B's Convo")
            .await;
        assert!(
            b_convo.is_err(),
            "scoped to A, create_conversation_scoped for B's account/graph must be REJECTED — \
             got {b_convo:?}"
        );
        cross_tx.rollback().await.ok();
    }

    // ── verify persistence via the owner/BYPASSRLS pool ──────────────────
    let convo_owner_check: Uuid =
        sqlx::query_scalar("SELECT account_id FROM conversations WHERE id = $1")
            .bind(convo.id)
            .fetch_one(&owner)
            .await
            .expect("the committed conversation must exist");
    assert_eq!(convo_owner_check, a.account_id);

    let b_convo_leaked: i64 =
        sqlx::query_scalar("SELECT count(*) FROM conversations WHERE title = 'B''s Convo'")
            .fetch_one(&owner)
            .await
            .expect("count query as owner");
    assert_eq!(b_convo_leaked, 0, "the rejected cross-account conversation write must not exist");

    app_pool.close().await;
    owner.close().await;
    admin_conn
        .execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
        .await
        .ok();
}

/// Negative control: a representative sample of chunk-2 `_scoped` methods —
/// reads AND writes — hard-error on a `coralswarm_app` transaction where
/// `app.account_id` was NEVER set. Each gets its OWN fresh transaction (same
/// reasoning as chunk 1's negative-control test: a real Postgres error
/// aborts the whole transaction it happens in).
#[tokio::test]
async fn chunk2_scoped_methods_hard_error_when_account_id_is_never_set() {
    let Some(base) = base_url() else {
        eprintln!(
            "skipping chunk2_scoped_methods_hard_error_when_account_id_is_never_set: \
             set ORGT_TEST_BASE_URL"
        );
        return;
    };

    let db = "orgt_store_scoped_chunk2_backstop";
    let _cleanup_db = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
    let mut admin_conn = admin(&base).await;
    recreate(&mut admin_conn, db).await;
    let db_url = with_db(&base, db);

    let owner: PgPool = PgPoolOptions::new().connect(&db_url).await.expect("connect owner");
    sqlx::migrate!("./migrations_control")
        .run(&owner)
        .await
        .expect("apply control tree incl. 0083_consumer_rls");
    sqlx::query(&format!(
        "ALTER ROLE coralswarm_app WITH PASSWORD '{APP_ROLE_TEST_PASSWORD}'"
    ))
    .execute(&owner)
    .await
    .expect("set test-only password on coralswarm_app");

    let a = seed_account(&owner, "chunk2-backstop-a@x.co").await;

    let app_pool = app_role_pool(&db_url).await;
    let store = PostgresStore::new(app_pool.clone());

    // ── reads ────────────────────────────────────────────────────────────
    {
        let mut tx = app_pool.begin().await.expect("begin unscoped tx (app.account_id never set)");
        let result = store.list_conversations_scoped(&mut tx, a.graph_id, a.user_id, 10).await;
        assert!(result.is_err(), "list_conversations_scoped must hard-error — got {result:?}");
        tx.rollback().await.ok();
    }
    {
        let mut tx = app_pool.begin().await.expect("begin unscoped tx (app.account_id never set)");
        let result = store.get_meeting_transcript_scoped(&mut tx, a.graph_id, "probe").await;
        assert!(result.is_err(), "get_meeting_transcript_scoped must hard-error — got {result:?}");
        tx.rollback().await.ok();
    }
    {
        let mut tx = app_pool.begin().await.expect("begin unscoped tx (app.account_id never set)");
        let result = store.list_query_history_scoped(&mut tx, a.account_id, a.user_id, None, 10, 0).await;
        assert!(result.is_err(), "list_query_history_scoped must hard-error — got {result:?}");
        tx.rollback().await.ok();
    }

    // ── writes ───────────────────────────────────────────────────────────
    {
        let mut tx = app_pool.begin().await.expect("begin unscoped tx (app.account_id never set)");
        let result = store
            .create_conversation_scoped(&mut tx, a.account_id, a.graph_id, a.user_id, "Should Not Land")
            .await;
        assert!(result.is_err(), "create_conversation_scoped must hard-error — got {result:?}");
        tx.rollback().await.ok();
    }
    {
        let mut tx = app_pool.begin().await.expect("begin unscoped tx (app.account_id never set)");
        let result = store
            .upsert_meeting_transcript_scoped(
                &mut tx,
                a.graph_id,
                "should-not-land",
                None,
                Some("nope"),
                None,
                None,
            )
            .await;
        assert!(result.is_err(), "upsert_meeting_transcript_scoped must hard-error — got {result:?}");
        tx.rollback().await.ok();
    }
    {
        let mut tx = app_pool.begin().await.expect("begin unscoped tx (app.account_id never set)");
        let result = store
            .create_team_tx_scoped(&mut tx, a.account_id, a.user_id, "Should Not Land", &[1])
            .await;
        assert!(result.is_err(), "create_team_tx_scoped must hard-error — got {result:?}");
        tx.rollback().await.ok();
    }

    let leaked: i64 =
        sqlx::query_scalar("SELECT count(*) FROM conversations WHERE title = 'Should Not Land'")
            .fetch_one(&owner)
            .await
            .expect("count query as owner");
    assert_eq!(leaked, 0, "no unscoped write must ever land, even after rollback");

    app_pool.close().await;
    owner.close().await;
    admin_conn
        .execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
        .await
        .ok();
}

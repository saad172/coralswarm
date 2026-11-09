//! Live regression suite for T4 (slice 0, 2026-08-08): making the
//! supersession review flip reachable for decision reefs.
//!
//! Before T4, `Store::upsert_supersession_edge_with_kind{,_scoped}` only
//! flipped a superseded `type = 'decision'` reef's `body.review_status`
//! `confirmed -> pending` when `nodes.coral_count >= 3 OR` relationship
//! degree `>= 5`. Decision reefs are born with exactly 1 coral and are
//! hardcoded out of `relationship_edge` endpoints
//! (`workers::extract_relationships`), so that gate was structurally
//! unreachable for the one node type the flip exists to protect — no
//! auto-detected supersession ever reached the review queue. This file
//! proves, against REAL Postgres:
//!
//!   1. an `auto`-sourced edge (`upsert_supersession_edge_with_kind`) flips a
//!      low-structure (`coral_count = 1`, degree 0) confirmed decision to
//!      `pending` UNCONDITIONALLY — the old gate is gone;
//!   2. a `user`-sourced edge (`upsert_supersession_edge`, the manual
//!      `POST /supersede` route) never re-queues a confirmed decision — the
//!      human already decided;
//!   3. the new `supersession_edge.created_at` column (migrations
//!      tenant/0044, control/0137) is populated on insert.
//!
//! Gated on `RELINFER_TEST_DATABASE_URL` (a Postgres with `migrations_control`
//! applied — same convention as `graph_b3_supersession_targets_pg.rs`); a
//! normal `cargo test` with no DB configured SKIPS LOUDLY.
//!
//! Run with:
//!   RELINFER_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:55987/t4_verify \
//!     cargo test -p coralswarm-backend --test t4_supersession_review_flip_pg -- --nocapture

use coralswarm_backend::store::{PostgresStore, Store, SupersessionKind};
use serde_json::Value;
use sqlx::postgres::PgPoolOptions;
use uuid::Uuid;

const SKIP_REASON: &str = "skipping: set RELINFER_TEST_DATABASE_URL to run the live T4 \
     supersession-review-flip regression suite (e.g. RELINFER_TEST_DATABASE_URL=postgres://\
     postgres:postgres@localhost:55987/t4_verify)";

async fn setup(url: &str) -> (PostgresStore, sqlx::PgPool, Uuid) {
    let pool = PgPoolOptions::new()
        .max_connections(8)
        .connect(url)
        .await
        .expect("connect to test Postgres");

    let email = format!("t4-review-flip-{}@test.local", Uuid::new_v4());
    let user_id: Uuid = sqlx::query_scalar("INSERT INTO users (email) VALUES ($1) RETURNING id")
        .bind(&email)
        .fetch_one(&pool)
        .await
        .expect("insert user");
    let account_id: Uuid =
        sqlx::query_scalar("INSERT INTO accounts (owner_user_id) VALUES ($1) RETURNING id")
            .bind(user_id)
            .fetch_one(&pool)
            .await
            .expect("insert account");

    let store = PostgresStore::new(pool.clone());
    let graph = store
        .create_graph(account_id, &format!("graph-t4-{}", Uuid::new_v4()), "personal")
        .await
        .expect("create graph");
    (store, pool, graph.id)
}

/// Inserts a `type = 'decision'` reef, `body.review_status = 'confirmed'`,
/// with `coral_count = 1` and no relationship_edge rows — the realistic,
/// structurally-low-structure shape every decision reef is born in. Under the
/// pre-T4 gate (`coral_count >= 3 OR degree >= 5`) this row could NEVER be
/// flipped into review.
async fn insert_low_structure_confirmed_decision(pool: &sqlx::PgPool, graph_id: Uuid) -> Uuid {
    let node_id: Uuid = sqlx::query_scalar(
        "INSERT INTO nodes (node_id, graph_id, type, title, body, coral_count, created_at, updated_at) \
         VALUES (gen_random_uuid(), $1, 'decision', $2, \
                 jsonb_build_object('review_status', 'confirmed', 'decided_at', now()), \
                 1, now(), now()) \
         RETURNING node_id",
    )
    .bind(graph_id)
    .bind(format!("decision-{}", Uuid::new_v4()))
    .fetch_one(pool)
    .await
    .expect("insert low-structure confirmed decision");
    node_id
}

async fn review_status(pool: &sqlx::PgPool, node_id: Uuid) -> Option<String> {
    sqlx::query_scalar("SELECT body->>'review_status' FROM nodes WHERE node_id = $1")
        .bind(node_id)
        .fetch_one(pool)
        .await
        .expect("read review_status")
}

/// Same shape as [`insert_low_structure_confirmed_decision`], but with an
/// arbitrary `review_status` — used to prove the flip predicate's `body->>
/// 'review_status' = 'confirmed'` clause actually gates on the CURRENT
/// status, not just on `type = 'decision'`: a decision already `pending` or
/// `rejected` (a reviewer already looked at it, or is mid-review) must not
/// be touched by a fresh auto edge.
async fn insert_decision_with_status(pool: &sqlx::PgPool, graph_id: Uuid, status: &str) -> Uuid {
    let node_id: Uuid = sqlx::query_scalar(
        "INSERT INTO nodes (node_id, graph_id, type, title, body, coral_count, created_at, updated_at) \
         VALUES (gen_random_uuid(), $1, 'decision', $2, \
                 jsonb_build_object('review_status', $3::text, 'decided_at', now()), \
                 1, now(), now()) \
         RETURNING node_id",
    )
    .bind(graph_id)
    .bind(format!("decision-{}", Uuid::new_v4()))
    .bind(status)
    .fetch_one(pool)
    .await
    .expect("insert decision with status");
    node_id
}

/// Inserts a NON-decision reef (`type = 'entity'`) with `body.review_status
/// = 'confirmed'` anyway — a shape production code never actually produces
/// (only decisions carry `review_status`), but the strongest possible proof
/// that the flip's `type = 'decision'` clause is what gates it, not some
/// incidental absence of the field on other node types.
async fn insert_confirmed_entity(pool: &sqlx::PgPool, graph_id: Uuid) -> Uuid {
    let node_id: Uuid = sqlx::query_scalar(
        "INSERT INTO nodes (node_id, graph_id, type, title, body, coral_count, created_at, updated_at) \
         VALUES (gen_random_uuid(), $1, 'entity', $2, \
                 jsonb_build_object('review_status', 'confirmed'), \
                 1, now(), now()) \
         RETURNING node_id",
    )
    .bind(graph_id)
    .bind(format!("entity-{}", Uuid::new_v4()))
    .fetch_one(pool)
    .await
    .expect("insert confirmed entity");
    node_id
}

/// Directly forces `review_status` back to `confirmed` via raw SQL (bypassing
/// `Store::set_node_review_status`, which requires an `account_id` and a
/// `pending`-only guard neither of which this file's minimal `setup()`
/// threads through) — used to simulate "a reviewer already confirmed this
/// despite the conflict" between two edge-insert calls in the
/// re-detection/no-reflip tests below.
async fn force_review_status(pool: &sqlx::PgPool, node_id: Uuid, status: &str) {
    sqlx::query(
        "UPDATE nodes SET body = jsonb_set(body, '{review_status}', to_jsonb($2::text)) \
         WHERE node_id = $1",
    )
    .bind(node_id)
    .bind(status)
    .execute(pool)
    .await
    .expect("force review_status");
}

#[tokio::test]
async fn auto_edge_unconditionally_flips_low_structure_confirmed_decision_to_pending() {
    let Ok(url) = std::env::var("RELINFER_TEST_DATABASE_URL") else {
        eprintln!("{SKIP_REASON}");
        return;
    };
    let (store, pool, graph_id) = setup(&url).await;

    let superseded_id = insert_low_structure_confirmed_decision(&pool, graph_id).await;
    // The superseder's own type is irrelevant to the flip (only the
    // SUPERSEDED node's `type = 'decision'` gates it) — `thread` sidesteps
    // the unrelated `decisions_require_decided_at` CHECK on this node.
    let superseder_id = store
        .upsert_node(graph_id, "thread", "the newer, contradicting decision")
        .await
        .expect("upsert superseder node");

    assert_eq!(
        review_status(&pool, superseded_id).await,
        Some("confirmed".to_string()),
        "sanity: starts confirmed"
    );

    let edge = store
        .upsert_supersession_edge_with_kind(superseder_id, superseded_id, SupersessionKind::Change)
        .await
        .expect("upsert auto supersession edge");
    assert_eq!(edge.superseder_id, superseder_id);
    assert_eq!(edge.superseded_id, superseded_id);

    assert_eq!(
        review_status(&pool, superseded_id).await,
        Some("pending".to_string()),
        "T4: an auto-sourced edge must flip a low-structure (coral_count=1, degree=0) \
         confirmed decision to pending UNCONDITIONALLY — the coral_count/degree gate \
         is gone for this path"
    );

    let pending_since: Option<Value> =
        sqlx::query_scalar("SELECT body->'pending_since' FROM nodes WHERE node_id = $1")
            .bind(superseded_id)
            .fetch_one(&pool)
            .await
            .expect("read pending_since");
    assert!(
        pending_since.is_some() && !pending_since.as_ref().unwrap().is_null(),
        "pending_since must be stamped alongside the review_status flip"
    );

    // `supersession_edge.created_at` (migration tenant/0044, control/0137)
    // must be populated for the freshly-inserted row.
    let stored_created_at: Option<chrono::DateTime<chrono::Utc>> = sqlx::query_scalar(
        "SELECT created_at FROM supersession_edge WHERE superseder_id = $1 AND superseded_id = $2",
    )
    .bind(superseder_id)
    .bind(superseded_id)
    .fetch_one(&pool)
    .await
    .expect("read supersession_edge.created_at");
    assert!(
        stored_created_at.is_some(),
        "supersession_edge.created_at must be populated on insert"
    );

    println!("OK: auto-sourced edge flipped a structurally-low decision into review");
}

#[tokio::test]
async fn user_edge_never_requeues_a_confirmed_decision() {
    let Ok(url) = std::env::var("RELINFER_TEST_DATABASE_URL") else {
        eprintln!("{SKIP_REASON}");
        return;
    };
    let (store, pool, graph_id) = setup(&url).await;

    let superseded_id = insert_low_structure_confirmed_decision(&pool, graph_id).await;
    // See the sibling test for why `thread` (not `decision`) here.
    let superseder_id = store
        .upsert_node(graph_id, "thread", "the newer decision (manual supersede)")
        .await
        .expect("upsert superseder node");

    store
        .upsert_supersession_edge(superseder_id, superseded_id)
        .await
        .expect("upsert manual (source='user') supersession edge");

    assert_eq!(
        review_status(&pool, superseded_id).await,
        Some("confirmed".to_string()),
        "T4: a source='user' edge (the manual POST /supersede route) must NEVER \
         re-queue a confirmed decision into review — the human already decided"
    );

    println!("OK: user-sourced edge left the confirmed decision untouched");
}

/// FIX 4 (T4 review): the flip is documented as UNCONDITIONAL with respect to
/// `SupersessionKind` — `SupersessionKind::Change` and `::Correction` both
/// queue a fresh auto edge's superseded decision for review. Both existing
/// tests above only ever exercised `::Change`; this proves `::Correction`
/// takes the identical path (no kind-specific carve-out crept in).
#[tokio::test]
async fn auto_edge_with_correction_kind_also_flips_confirmed_decision_to_pending() {
    let Ok(url) = std::env::var("RELINFER_TEST_DATABASE_URL") else {
        eprintln!("{SKIP_REASON}");
        return;
    };
    let (store, pool, graph_id) = setup(&url).await;

    let superseded_id = insert_low_structure_confirmed_decision(&pool, graph_id).await;
    let superseder_id = store
        .upsert_node(graph_id, "thread", "an anchored correction of the decision")
        .await
        .expect("upsert superseder node");

    store
        .upsert_supersession_edge_with_kind(
            superseder_id,
            superseded_id,
            SupersessionKind::Correction,
        )
        .await
        .expect("upsert auto (Correction-kind) supersession edge");

    assert_eq!(
        review_status(&pool, superseded_id).await,
        Some("pending".to_string()),
        "T4: kind=Correction must flip a confirmed decision to pending exactly like \
         kind=Change — SupersessionKind is not consulted by the flip predicate"
    );

    println!("OK: Correction-kind auto edge flipped the confirmed decision into review");
}

/// FIX 4 (T4 review): the flip predicate is `body->>'review_status' =
/// 'confirmed'` — a decision that is already `pending` (mid-review) or
/// `rejected` (a reviewer already looked and said no) must be left exactly
/// as-is by a fresh auto edge, not silently reset or re-stamped.
#[tokio::test]
async fn auto_edge_leaves_already_reviewed_decisions_alone() {
    let Ok(url) = std::env::var("RELINFER_TEST_DATABASE_URL") else {
        eprintln!("{SKIP_REASON}");
        return;
    };
    let (store, pool, graph_id) = setup(&url).await;

    let already_pending = insert_decision_with_status(&pool, graph_id, "pending").await;
    let already_rejected = insert_decision_with_status(&pool, graph_id, "rejected").await;
    let superseder_pending = store
        .upsert_node(graph_id, "thread", "supersedes the already-pending decision")
        .await
        .expect("upsert superseder node (pending case)");
    let superseder_rejected = store
        .upsert_node(graph_id, "thread", "supersedes the already-rejected decision")
        .await
        .expect("upsert superseder node (rejected case)");

    store
        .upsert_supersession_edge_with_kind(
            superseder_pending,
            already_pending,
            SupersessionKind::Change,
        )
        .await
        .expect("upsert auto edge onto an already-pending decision");
    store
        .upsert_supersession_edge_with_kind(
            superseder_rejected,
            already_rejected,
            SupersessionKind::Change,
        )
        .await
        .expect("upsert auto edge onto an already-rejected decision");

    assert_eq!(
        review_status(&pool, already_pending).await,
        Some("pending".to_string()),
        "T4: a fresh auto edge onto an already-pending decision must leave it pending \
         (not re-stamp pending_since, not error)"
    );
    assert_eq!(
        review_status(&pool, already_rejected).await,
        Some("rejected".to_string()),
        "T4: a fresh auto edge onto an already-rejected decision must NOT resurrect it \
         into the review queue — a reviewer already said no"
    );

    println!("OK: auto edges left already-reviewed (pending/rejected) decisions alone");
}

/// FIX 4 (T4 review): the flip predicate is `type = 'decision'` — a fresh
/// auto edge against any other reef type must never touch `review_status`,
/// even in the pathological case where that reef happens to carry the same
/// field (production code never sets `review_status` on a non-decision reef;
/// this proves the type gate, not field absence, is what protects it).
#[tokio::test]
async fn auto_edge_never_flips_a_non_decision_reef() {
    let Ok(url) = std::env::var("RELINFER_TEST_DATABASE_URL") else {
        eprintln!("{SKIP_REASON}");
        return;
    };
    let (store, pool, graph_id) = setup(&url).await;

    let superseded_id = insert_confirmed_entity(&pool, graph_id).await;
    let superseder_id = store
        .upsert_node(graph_id, "thread", "supersedes a non-decision reef")
        .await
        .expect("upsert superseder node");

    store
        .upsert_supersession_edge_with_kind(superseder_id, superseded_id, SupersessionKind::Change)
        .await
        .expect("upsert auto edge onto a non-decision reef");

    assert_eq!(
        review_status(&pool, superseded_id).await,
        Some("confirmed".to_string()),
        "T4: the flip must only ever touch type='decision' reefs — a non-decision reef's \
         review_status (however it got one) must never be touched"
    );

    println!("OK: auto edge left a non-decision reef's review_status untouched");
}

/// FIX 2 (T4 review): `Store::insert_auto_supersession_edge` is the SECOND,
/// independent `source='auto'` write path — the `SUPERSESSION_AUTO_EDGES`
/// query-time writer (`services::supersession::create_supersession_edges*`),
/// which has no `kind` parameter to gate on. Proves both halves of its flip
/// contract against real Postgres:
///   1. a genuinely NEW edge on a confirmed decision flips it to pending
///      (mirrors `upsert_supersession_edge_with_kind`'s flip, unconditionally
///      — there is no kind to be conditional on here);
///   2. re-detecting the SAME pair (the `ON CONFLICT DO NOTHING` no-op path)
///      returns `false` (not a fresh insert) and must NOT re-flip a decision
///      a reviewer has since confirmed back — the idempotency guard this
///      whole area exists to protect.
#[tokio::test]
async fn insert_auto_supersession_edge_flips_new_and_never_reflips_duplicate() {
    let Ok(url) = std::env::var("RELINFER_TEST_DATABASE_URL") else {
        eprintln!("{SKIP_REASON}");
        return;
    };
    let (store, pool, graph_id) = setup(&url).await;

    let superseded_id = insert_low_structure_confirmed_decision(&pool, graph_id).await;
    let superseder_id = store
        .upsert_node(graph_id, "thread", "SUPERSESSION_AUTO_EDGES query-time detection")
        .await
        .expect("upsert superseder node");

    // 1. A genuinely new edge flips the confirmed decision to pending.
    let inserted = store
        .insert_auto_supersession_edge(superseded_id, superseder_id)
        .await
        .expect("insert_auto_supersession_edge (first call)");
    assert!(inserted, "the first call must report a genuinely NEW row");
    assert_eq!(
        review_status(&pool, superseded_id).await,
        Some("pending".to_string()),
        "T4 FIX 2: insert_auto_supersession_edge must flip a freshly-inserted edge's \
         confirmed superseded decision to pending, same as upsert_supersession_edge_with_kind"
    );

    // A reviewer confirms the decision despite the conflict.
    force_review_status(&pool, superseded_id, "confirmed").await;

    // 2. Re-detecting the identical pair must be a no-op: not a new row, and
    // must NOT re-flip the reviewer's confirmed verdict back to pending.
    let inserted_again = store
        .insert_auto_supersession_edge(superseded_id, superseder_id)
        .await
        .expect("insert_auto_supersession_edge (duplicate re-detection)");
    assert!(
        !inserted_again,
        "re-detecting the same pair must report NO new row (ON CONFLICT DO NOTHING)"
    );
    assert_eq!(
        review_status(&pool, superseded_id).await,
        Some("confirmed".to_string()),
        "T4 FIX 2: re-detecting an already-existing edge must NEVER re-flip a decision \
         a reviewer has since confirmed — the idempotency guard this whole area exists \
         to protect"
    );

    println!(
        "OK: insert_auto_supersession_edge flipped a new edge and never re-flipped a duplicate"
    );
}

//! Live regression suite for the decision review queue's keyset pagination
//! (2026-08-12).
//!
//! `GET /v1/candidates` used to take only a `limit`, so the `/review` screen
//! paged by re-asking with a bigger one. Two consequences this file pins shut:
//! the queue was unreachable past the route's 200 clamp (a larger queue could
//! not be worked to empty at all), and `ORDER BY n.created_at DESC` alone is
//! not a total order, so any page boundary landing inside a group of
//! same-instant rows could skip some and repeat others.
//!
//! The same-instant case is the realistic one, not a corner case: extraction
//! writes decision candidates in batches, so ties in `created_at` are ordinary.
//!
//! Proven here against REAL Postgres:
//!
//!   1. paging with a cursor visits every pending candidate EXACTLY ONCE when
//!      every row shares one `created_at` — the `node_id` tiebreak in both the
//!      `ORDER BY` and the keyset predicate is what makes this hold;
//!   2. the cursor row itself is excluded from the following page (a `<`
//!      boundary, not `<=`), so no row is rendered twice;
//!   3. ordering is newest-first across distinct instants, i.e. adding the
//!      tiebreak did not reorder the queue;
//!   4. a walk terminates and covers the whole queue even when the page size
//!      does not divide the queue evenly.
//!
//! Negative controls (both re-run by hand, both fail as designed):
//!   * drop `, n.node_id DESC` from the `ORDER BY` -> test 1 fails with rows
//!     both missing and duplicated.
//!   * weaken the keyset predicate to `<=` -> test 2 fails on a repeat.
//!
//! Gated on `RELINFER_TEST_DATABASE_URL` (a Postgres with `migrations_control`
//! applied — same convention as `t4_supersession_review_flip_pg.rs`); a normal
//! `cargo test` with no DB configured SKIPS LOUDLY.
//!
//! Run with:
//!   RELINFER_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:55987/cand_verify \
//!     cargo test -p coralswarm-backend --test candidates_keyset_pg -- --nocapture

use chrono::{DateTime, Duration, Utc};
use coralswarm_backend::store::{PostgresStore, Store};
use sqlx::postgres::PgPoolOptions;
use std::collections::HashSet;
use uuid::Uuid;

const SKIP_REASON: &str = "skipping: set RELINFER_TEST_DATABASE_URL to run the live \
     decision-candidate keyset-pagination suite (e.g. RELINFER_TEST_DATABASE_URL=postgres://\
     postgres:postgres@localhost:55987/cand_verify)";

async fn setup(url: &str) -> (PostgresStore, sqlx::PgPool, Uuid, Uuid) {
    let pool = PgPoolOptions::new()
        .max_connections(8)
        .connect(url)
        .await
        .expect("connect to test Postgres");

    let email = format!("cand-keyset-{}@test.local", Uuid::new_v4());
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
        .create_graph(account_id, &format!("graph-cand-{}", Uuid::new_v4()), "personal")
        .await
        .expect("create graph");
    (store, pool, account_id, graph.id)
}

/// One pending decision reef at an EXPLICIT `created_at`, so a test can force
/// a tie rather than hoping two inserts land in the same microsecond.
async fn insert_pending_candidate(
    pool: &sqlx::PgPool,
    graph_id: Uuid,
    created_at: DateTime<Utc>,
) -> Uuid {
    sqlx::query_scalar(
        "INSERT INTO nodes (node_id, graph_id, type, title, body, coral_count, created_at, updated_at) \
         VALUES (gen_random_uuid(), $1, 'decision', $2, \
                 jsonb_build_object('review_status', 'pending', 'decided_at', now()), \
                 1, $3, $3) \
         RETURNING node_id",
    )
    .bind(graph_id)
    .bind(format!("decision-{}", Uuid::new_v4()))
    .bind(created_at)
    .fetch_one(pool)
    .await
    .expect("insert pending decision candidate")
}

/// Walks the queue exactly as the route does: ask for `page_size`, take the
/// last row rendered as the next cursor, stop on a short page. Returns every
/// `node_id` in visit order INCLUDING duplicates — a caller asserting
/// uniqueness needs to see repeats, not have them silently collapsed.
async fn walk(store: &PostgresStore, account_id: Uuid, graph_id: Uuid, page_size: i64) -> Vec<Uuid> {
    let mut seen = Vec::new();
    let mut cursor: Option<(DateTime<Utc>, Uuid)> = None;
    // Bounded so a pagination bug that fails to advance surfaces as a clear
    // assertion rather than hanging the suite forever.
    for _ in 0..100 {
        let page = store
            .list_pending_decision_candidates(account_id, &[graph_id], cursor, page_size)
            .await
            .expect("list candidates page");
        if page.is_empty() {
            break;
        }
        for c in &page {
            seen.push(c.node_id);
        }
        let last = page.last().expect("non-empty page");
        cursor = Some((last.created_at, last.node_id));
        if (page.len() as i64) < page_size {
            break;
        }
    }
    seen
}

#[tokio::test]
async fn keyset_walks_a_same_instant_queue_exactly_once() {
    let Ok(url) = std::env::var("RELINFER_TEST_DATABASE_URL") else {
        eprintln!("{SKIP_REASON}");
        return;
    };
    let (store, pool, account_id, graph_id) = setup(&url).await;

    // Every row shares ONE instant: without the `node_id` tiebreak the
    // ordering is arbitrary and page boundaries fall inside the tie group.
    let tie = Utc::now();
    let mut inserted = HashSet::new();
    for _ in 0..7 {
        inserted.insert(insert_pending_candidate(&pool, graph_id, tie).await);
    }

    let seen = walk(&store, account_id, graph_id, 3).await;

    let unique: HashSet<Uuid> = seen.iter().copied().collect();
    assert_eq!(
        seen.len(),
        unique.len(),
        "a candidate was returned on more than one page: {seen:?}"
    );
    assert_eq!(
        unique, inserted,
        "the walk did not visit every pending candidate exactly once"
    );
}

#[tokio::test]
async fn a_cursor_excludes_its_own_row() {
    let Ok(url) = std::env::var("RELINFER_TEST_DATABASE_URL") else {
        eprintln!("{SKIP_REASON}");
        return;
    };
    let (store, pool, account_id, graph_id) = setup(&url).await;

    let tie = Utc::now();
    for _ in 0..4 {
        insert_pending_candidate(&pool, graph_id, tie).await;
    }

    let first = store
        .list_pending_decision_candidates(account_id, &[graph_id], None, 2)
        .await
        .expect("first page");
    assert_eq!(first.len(), 2, "expected a full first page");
    let boundary = first.last().expect("non-empty page");

    let second = store
        .list_pending_decision_candidates(
            account_id,
            &[graph_id],
            Some((boundary.created_at, boundary.node_id)),
            2,
        )
        .await
        .expect("second page");

    let first_ids: HashSet<Uuid> = first.iter().map(|c| c.node_id).collect();
    for c in &second {
        assert!(
            !first_ids.contains(&c.node_id),
            "cursor boundary is inclusive — {} appeared on both pages",
            c.node_id
        );
    }
}

#[tokio::test]
async fn distinct_instants_stay_newest_first() {
    let Ok(url) = std::env::var("RELINFER_TEST_DATABASE_URL") else {
        eprintln!("{SKIP_REASON}");
        return;
    };
    let (store, pool, account_id, graph_id) = setup(&url).await;

    // Oldest inserted first, so insertion order is the REVERSE of the expected
    // read order — a query that lost its `DESC` would still pass if the two
    // orders agreed.
    let base = Utc::now() - Duration::minutes(10);
    let mut oldest_to_newest = Vec::new();
    for i in 0..5 {
        oldest_to_newest
            .push(insert_pending_candidate(&pool, graph_id, base + Duration::minutes(i)).await);
    }
    let expected: Vec<Uuid> = oldest_to_newest.into_iter().rev().collect();

    let seen = walk(&store, account_id, graph_id, 2).await;
    assert_eq!(seen, expected, "queue is not ordered newest-first");
}

#[tokio::test]
async fn a_page_size_that_does_not_divide_the_queue_still_terminates() {
    let Ok(url) = std::env::var("RELINFER_TEST_DATABASE_URL") else {
        eprintln!("{SKIP_REASON}");
        return;
    };
    let (store, pool, account_id, graph_id) = setup(&url).await;

    let base = Utc::now() - Duration::minutes(30);
    let mut inserted = HashSet::new();
    for i in 0..10 {
        inserted.insert(insert_pending_candidate(&pool, graph_id, base + Duration::seconds(i)).await);
    }

    // 10 rows at 4 per page: 4 / 4 / 2. The final short page ends the walk
    // without a further empty round-trip.
    let seen = walk(&store, account_id, graph_id, 4).await;
    let unique: HashSet<Uuid> = seen.iter().copied().collect();
    assert_eq!(seen.len(), 10, "expected exactly the 10 inserted rows");
    assert_eq!(unique, inserted, "walk did not cover the queue");
}

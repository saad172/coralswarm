//! Live Postgres tests for `PostgresStore::list_session_atoms` — the
//! coding-session capture Phase 4 `GET /v1/sessions/{session_id}` detail
//! route's backing query. Pattern-copied from `coding_sessions_read_pg.rs`
//! (same tenancy-seeding helpers, same `RELINFER_TEST_DATABASE_URL` gate):
//! drives `PostgresStore` DIRECTLY so these are tight, fast "pg-side bind"
//! tests of the actual SQL — real `%`/`_`/`\` escaping via
//! `session_source_id_prefix`, real chronological ordering, real limit+1
//! truncation-detection — nothing here is mocked or re-implemented in Rust.
//!
//! Gated on `RELINFER_TEST_DATABASE_URL` (a Postgres with all migrations
//! applied); a normal `cargo test` with no DB configured skips every test in
//! this file.
//!
//! Run with:
//!   RELINFER_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:55432/coralswarm \
//!     cargo test -p coralswarm-backend --test sessions_detail_read_pg -- --nocapture

use chrono::Utc;
use coralswarm_backend::store::{PostgresStore, Store};
use sqlx::postgres::PgPoolOptions;
use uuid::Uuid;
use coralswarm_backend::store::{SessionIdentity, SessionListFilter};

macro_rules! test_pool {
    () => {{
        match std::env::var("RELINFER_TEST_DATABASE_URL") {
            Ok(u) => PgPoolOptions::new()
                .max_connections(4)
                .connect(&u)
                .await
                .expect("connect to test Postgres"),
            Err(_) => {
                eprintln!("skipping: set RELINFER_TEST_DATABASE_URL to run sessions_detail_read_pg");
                return;
            }
        }
    }};
}

/// Seed a fresh `(user, account, graph)` tenancy chain — pattern-copied from
/// `coding_sessions_read_pg.rs`/`store_provenance_pg.rs`.
async fn seed_tenancy(pool: &sqlx::PgPool) -> (Uuid, Uuid, Uuid) {
    let email = format!("{}@test.local", Uuid::new_v4());
    let user_id: Uuid = sqlx::query_scalar("INSERT INTO users (email) VALUES ($1) RETURNING id")
        .bind(&email)
        .fetch_one(pool)
        .await
        .expect("insert user");
    let account_id: Uuid =
        sqlx::query_scalar("INSERT INTO accounts (owner_user_id) VALUES ($1) RETURNING id")
            .bind(user_id)
            .fetch_one(pool)
            .await
            .expect("insert account");
    let store = PostgresStore::new(pool.clone());
    let graph = store
        .create_graph(account_id, &format!("sessions-detail-read-{}", Uuid::new_v4()), "personal")
        .await
        .expect("create graph");
    (user_id, account_id, graph.id)
}

async fn cleanup(pool: &sqlx::PgPool, account_id: Uuid, user_id: Uuid) {
    sqlx::query("DELETE FROM accounts WHERE id = $1")
        .bind(account_id)
        .execute(pool)
        .await
        .ok();
    sqlx::query("DELETE FROM users WHERE id = $1")
        .bind(user_id)
        .execute(pool)
        .await
        .ok();
}

/// Insert one atom at a given `source_id`/`timestamp` — mirrors
/// `coding_sessions_read_pg.rs`'s `seed_atom`, minus the content/FTS backfill
/// (not needed for these ordering/escaping tests).
async fn seed_atom_at(
    store: &PostgresStore,
    graph_id: Uuid,
    user_id: Uuid,
    source_id: &str,
    timestamp: chrono::DateTime<Utc>,
) -> Uuid {
    let atom_id = Uuid::new_v4();
    store
        .insert_atoms(
            &[atom_id],
            graph_id,
            "mcp",
            &[source_id.to_string()],
            &[format!("blob-{atom_id}")],
            &[],
            timestamp,
            user_id,
            "private",
            "english",
        )
        .await
        .expect("insert_atoms");
    atom_id
}

#[tokio::test]
async fn list_session_atoms_orders_chronologically_oldest_first() {
    let pool = test_pool!();
    let (user_id, account_id, graph_id) = seed_tenancy(&pool).await;
    let store = PostgresStore::new(pool.clone());

    let t0 = Utc::now() - chrono::Duration::hours(2);
    let t1 = Utc::now() - chrono::Duration::hours(1);
    let t2 = Utc::now();

    // Inserted out of chronological order — the query must still return them
    // oldest-timestamp-first, not insertion order.
    let second = seed_atom_at(&store, graph_id, user_id, "cc:sess-order:ckpt-2:chunk:0", t1).await;
    let first = seed_atom_at(&store, graph_id, user_id, "cc:sess-order:ckpt-1:chunk:0", t0).await;
    let third = seed_atom_at(&store, graph_id, user_id, "cc:sess-order:ckpt-3:chunk:0", t2).await;

    let rows = store
        .list_session_atoms(graph_id, "sess-order", 10)
        .await
        .expect("list_session_atoms");
    let ids: Vec<Uuid> = rows.iter().map(|a| a.atom_id).collect();
    assert_eq!(ids, vec![first, second, third], "must be oldest-timestamp-first: {rows:?}");

    cleanup(&pool, account_id, user_id).await;
}

/// The session_id prefix's `%`/`_`/`\` escaping AND the `:` separator that
/// stops one session id from prefix-matching a DIFFERENT session whose id
/// happens to start with the same text — both proven against a REAL
/// Postgres `LIKE` evaluation via `session_source_id_prefix`, mirroring
/// `coding_sessions_read_pg.rs`'s `keyword_search_session_id_filter_*` test.
#[tokio::test]
async fn list_session_atoms_escapes_metacharacters_and_never_widens() {
    let pool = test_pool!();
    let (user_id, account_id, graph_id) = seed_tenancy(&pool).await;
    let store = PostgresStore::new(pool.clone());

    let now = Utc::now();
    let a = seed_atom_at(&store, graph_id, user_id, "cc:sess-b:ckpt-1:chunk:0", now).await;
    let widener = seed_atom_at(&store, graph_id, user_id, "cc:sess-bx:ckpt-1:chunk:0", now).await;

    let rows = store
        .list_session_atoms(graph_id, "sess-b", 10)
        .await
        .expect("list_session_atoms");
    let ids: Vec<Uuid> = rows.iter().map(|a| a.atom_id).collect();
    assert!(ids.contains(&a), "must find the exact session's atom: {ids:?}");
    assert!(!ids.contains(&widener), "must NOT prefix-match \"sess-bx\": {ids:?}");

    // A session id containing literal `%`/`_` — must match only its own atom.
    let literal = seed_atom_at(&store, graph_id, user_id, "cc:a%b_c:ckpt-1:chunk:0", now).await;
    let decoy = seed_atom_at(&store, graph_id, user_id, "cc:aXbYc:ckpt-1:chunk:0", now).await;
    let rows = store
        .list_session_atoms(graph_id, "a%b_c", 10)
        .await
        .expect("list_session_atoms literal metacharacters");
    let ids: Vec<Uuid> = rows.iter().map(|a| a.atom_id).collect();
    assert_eq!(ids, vec![literal], "literal %/_ must not act as a wildcard: {ids:?} (decoy={decoy})");

    cleanup(&pool, account_id, user_id).await;
}

// ── `CodingSessionAtom.excerpt` (title-always-present PR) ──────────────────
//
// `routes::sessions::get_session` guarantees a non-empty `corals[].title` via
// `services::atom_title::display_title` (unit-tested exhaustively in that
// module, including the exact "title AND excerpt both absent" shape below —
// its `display_title_treats_empty_and_whitespace_fields_as_absent`/
// `display_title_never_returns_empty_even_with_no_provenance_at_all` tests).
// `display_title` itself is `pub(crate)` and unreachable from this external
// test crate, so what THIS file proves is the half that lives in SQL: that
// `list_session_atoms` actually carries `atoms.excerpt` through to
// `CodingSessionAtom.excerpt` correctly (the new column this PR added to that
// struct/query) — both when it's NULL (an untitled, unexcerpted checkpoint —
// the input `display_title` must still turn into a non-empty string) and
// when it's a real value.

#[tokio::test]
async fn list_session_atoms_carries_title_and_excerpt_both_ways() {
    let pool = test_pool!();
    let (user_id, account_id, graph_id) = seed_tenancy(&pool).await;
    let store = PostgresStore::new(pool.clone());

    // Untitled, unexcerpted checkpoint — the exact shape `display_title`'s
    // last fallback rung (source label + date) exists for.
    let untitled = seed_atom_at(&store, graph_id, user_id, "cc:sess-excerpt:ckpt-1:chunk:0", Utc::now())
        .await;

    // A titled, excerpted checkpoint, written directly (mirroring
    // `routes::ingest`'s synchronous fallback-title write this PR added) —
    // `insert_atoms` (used by `seed_atom_at` above) has no title/excerpt
    // parameters at all, so this one is seeded with a raw SQL UPDATE instead
    // of a second insert helper.
    let titled = seed_atom_at(&store, graph_id, user_id, "cc:sess-excerpt:ckpt-2:chunk:0", Utc::now())
        .await;
    sqlx::query("UPDATE atoms SET title = $1, title_kind = 'fallback', excerpt = $2 WHERE atom_id = $3")
        .bind("Fixed the auth bypass in the share route")
        .bind("Fixed the auth bypass in the share route.")
        .bind(titled)
        .execute(&pool)
        .await
        .expect("stamp title/title_kind/excerpt directly");

    let rows = store
        .list_session_atoms(graph_id, "sess-excerpt", 10)
        .await
        .expect("list_session_atoms");
    let by_id: std::collections::HashMap<Uuid, _> = rows.into_iter().map(|a| (a.atom_id, a)).collect();

    let u = by_id.get(&untitled).expect("untitled checkpoint must be present");
    assert_eq!(u.title, None, "an untitled checkpoint's title must round-trip as NULL, not \"\"");
    assert_eq!(u.excerpt, None, "an unexcerpted checkpoint's excerpt must round-trip as NULL");

    let t = by_id.get(&titled).expect("titled checkpoint must be present");
    assert_eq!(t.title.as_deref(), Some("Fixed the auth bypass in the share route"));
    assert_eq!(t.excerpt.as_deref(), Some("Fixed the auth bypass in the share route."));

    cleanup(&pool, account_id, user_id).await;
}

#[tokio::test]
async fn list_session_atoms_zero_corals_case_is_an_empty_list_not_an_error() {
    let pool = test_pool!();
    let (user_id, account_id, graph_id) = seed_tenancy(&pool).await;
    let store = PostgresStore::new(pool.clone());

    let rows = store
        .list_session_atoms(graph_id, "sess-never-existed", 10)
        .await
        .expect("list_session_atoms");
    assert!(rows.is_empty(), "an unmatched session id must return an empty list, not an error");

    cleanup(&pool, account_id, user_id).await;
}

/// The detail route passes `limit + 1` to detect truncation without a second
/// COUNT query (mirrors `list_coding_sessions`'s own convention) — proves the
/// store method honors an arbitrary `limit` exactly.
#[tokio::test]
async fn list_session_atoms_honors_the_limit_for_truncation_detection() {
    let pool = test_pool!();
    let (user_id, account_id, graph_id) = seed_tenancy(&pool).await;
    let store = PostgresStore::new(pool.clone());

    let now = Utc::now();
    for i in 0..5 {
        seed_atom_at(
            &store,
            graph_id,
            user_id,
            &format!("cc:sess-cap:ckpt-{i}:chunk:0"),
            now + chrono::Duration::seconds(i),
        )
        .await;
    }

    // Route-side cap simulation: ask for 3+1=4 rows over 5 real corals.
    let rows = store
        .list_session_atoms(graph_id, "sess-cap", 4)
        .await
        .expect("list_session_atoms");
    assert_eq!(rows.len(), 4, "must return exactly `limit` rows, proving truncation is detectable: {rows:?}");

    cleanup(&pool, account_id, user_id).await;
}

/// `count_session_atoms` reports the FULL persisted total, NOT the capped
/// display length — the CodeRabbit finding on the detail route (`coral_count`
/// used to be `corals_json.len()`, capped at `SESSION_DETAIL_CORAL_CAP`). We
/// prove it directly here (no need to seed 200+ rows to exercise the real cap):
/// with a display `limit` SMALLER than the true count, `list_session_atoms`
/// truncates but `count_session_atoms` returns the whole total — AND that total
/// is identical to the `coral_count` `list_coding_sessions` reports for the
/// same session, so the detail page and the `GET /v1/sessions` list can never
/// disagree.
#[tokio::test]
async fn count_session_atoms_is_the_uncapped_total_and_matches_list_coral_count() {
    let pool = test_pool!();
    let (user_id, account_id, graph_id) = seed_tenancy(&pool).await;
    let store = PostgresStore::new(pool.clone());

    // A `coding_sessions` row so the same session also shows up in
    // `list_coding_sessions` (whose `coral_count` we cross-check against).
    store
        .upsert_coding_session(graph_id, "sess-count", "coding", Some("proj-c"), Some("Count Session"), None, None, None, None, None, None, &SessionIdentity::default())
        .await
        .expect("upsert_coding_session");

    let now = Utc::now();
    let total = 5;
    for i in 0..total {
        seed_atom_at(
            &store,
            graph_id,
            user_id,
            &format!("cc:sess-count:ckpt-{i}:chunk:0"),
            now + chrono::Duration::seconds(i),
        )
        .await;
    }
    // A same-graph atom from an UNRELATED session must not inflate the count.
    seed_atom_at(&store, graph_id, user_id, "cc:sess-other:ckpt-0:chunk:0", now).await;

    // Display list capped below the true total (simulates the route's cap).
    let display_limit = 2;
    let displayed = store
        .list_session_atoms(graph_id, "sess-count", display_limit)
        .await
        .expect("list_session_atoms");
    assert_eq!(displayed.len() as i64, display_limit, "the display list is truncated to the cap: {displayed:?}");

    // The count is the FULL total, not the capped display length.
    let count = store
        .count_session_atoms(graph_id, "sess-count")
        .await
        .expect("count_session_atoms");
    assert_eq!(count, total, "count_session_atoms must be the uncapped persisted total, not the display length");
    assert!(count > display_limit, "count must exceed the capped display length — this is exactly the understated-count bug");

    // Consistency with the list route's own `coral_count` for the same session.
    let listed = store
        .list_coding_sessions(graph_id, None, None, None, None, 10, &SessionListFilter::default())
        .await
        .expect("list_coding_sessions");
    let list_count = listed
        .iter()
        .find(|s| s.session_id == "sess-count")
        .map(|s| s.coral_count)
        .expect("session present in list_coding_sessions");
    assert_eq!(
        count, list_count,
        "detail count_session_atoms must equal list_coding_sessions' coral_count for the same session"
    );

    cleanup(&pool, account_id, user_id).await;
}

/// A session id that matches zero atoms is a normal empty page, and a
/// caller's own atoms from an UNRELATED session must never leak in — this is
/// the store-level analog of the route's IDOR guard (which operates one
/// layer up, on `graph_id` reachability).
#[tokio::test]
async fn list_session_atoms_scopes_strictly_to_graph_id() {
    let pool = test_pool!();
    let (user_id_a, account_id_a, graph_a) = seed_tenancy(&pool).await;
    let (user_id_b, account_id_b, graph_b) = seed_tenancy(&pool).await;
    let store = PostgresStore::new(pool.clone());

    let now = Utc::now();
    let in_a = seed_atom_at(&store, graph_a, user_id_a, "cc:shared-name:ckpt-1:chunk:0", now).await;
    let in_b = seed_atom_at(&store, graph_b, user_id_b, "cc:shared-name:ckpt-1:chunk:0", now).await;

    let rows_a = store
        .list_session_atoms(graph_a, "shared-name", 10)
        .await
        .expect("list_session_atoms graph a");
    let ids_a: Vec<Uuid> = rows_a.iter().map(|a| a.atom_id).collect();
    assert_eq!(ids_a, vec![in_a]);
    assert!(!ids_a.contains(&in_b), "must not leak another graph's atom of the same session id");

    cleanup(&pool, account_id_a, user_id_a).await;
    cleanup(&pool, account_id_b, user_id_b).await;
}

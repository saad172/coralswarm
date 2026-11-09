//! Live regression suite for `Store::list_supersession_targets`'s §3.6
//! widening against a REAL Postgres (docs/plans/graph-temporal-memory.md
//! Workstream B, §3.6).
//!
//! Before this widening the method returned a bare `Vec<(Uuid, Uuid)>` —
//! `(superseded_id, superseder_id)` pairs with no `kind` (§3.5) and no
//! timestamp, so `routes::queries::multi_graph_retrieve` had no way to
//! render the `CLAIMS` block's `[asserted X, superseded Y]` annotation or
//! distinguish a `change` from a `correction`. This file proves the WIDENED
//! SQL — a JOIN from `supersession_edge` to the SUPERSEDER's own
//! `nodes.created_at`, plus the `kind` column added by migration 0110 —
//! against real Postgres.
//!
//! Gated on `RELINFER_TEST_DATABASE_URL` (a Postgres with `migrations_control`
//! applied — same single-database "personal" schema `expand_d1_pg.rs` uses);
//! a normal `cargo test` with no DB configured SKIPS LOUDLY.
//!
//! Run with:
//!   RELINFER_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:55987/graph_b3_verify \
//!     cargo test -p coralswarm-backend --test graph_b3_supersession_targets_pg -- --nocapture

use coralswarm_backend::store::{PostgresStore, Store, SupersessionKind};
use sqlx::postgres::PgPoolOptions;
use uuid::Uuid;

const SKIP_REASON: &str = "skipping: set RELINFER_TEST_DATABASE_URL to run the live §3.6 \
     list_supersession_targets regression suite (e.g. RELINFER_TEST_DATABASE_URL=postgres://\
     postgres:postgres@localhost:55987/graph_b3_verify)";

async fn setup(url: &str) -> (PostgresStore, sqlx::PgPool, Uuid, Uuid) {
    let pool = PgPoolOptions::new()
        .max_connections(8)
        .connect(url)
        .await
        .expect("connect to test Postgres");

    let email = format!("graph-b3-{}@test.local", Uuid::new_v4());
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
        .create_graph(account_id, &format!("graph-b3-{}", Uuid::new_v4()), "personal")
        .await
        .expect("create graph");
    (store, pool, account_id, graph.id)
}

#[tokio::test]
async fn list_supersession_targets_carries_kind_and_superseded_on_against_real_postgres() {
    let Ok(url) = std::env::var("RELINFER_TEST_DATABASE_URL") else {
        eprintln!("{SKIP_REASON}");
        return;
    };
    let (store, _pool, _account_id, graph_id) = setup(&url).await;

    // Two reefs, one superseding the other. `upsert_node` mints
    // `nodes.created_at` server-side (`now()`) — the SUPERSEDER's own
    // `created_at` is exactly the "superseded on" timestamp §3.6's
    // `SupersessionTarget::superseded_on` resolves.
    let superseded_id = store
        .upsert_node(graph_id, "thread", "the on-call rotation runs weekly")
        .await
        .expect("upsert superseded node");
    let superseder_id = store
        .upsert_node(graph_id, "thread", "the on-call rotation runs biweekly")
        .await
        .expect("upsert superseder node");

    store
        .upsert_supersession_edge(superseder_id, superseded_id)
        .await
        .expect("upsert supersession edge");

    // ── `change` (the column default — migration 0110's `DEFAULT 'change'`) ──
    let found = store
        .list_supersession_targets(&[superseded_id])
        .await
        .expect("list_supersession_targets");
    assert_eq!(found.len(), 1, "exactly one target for the superseded node");
    assert_eq!(found[0].superseded_id, superseded_id);
    assert_eq!(found[0].superseder_id, superseder_id);
    assert_eq!(
        found[0].kind,
        SupersessionKind::Change,
        "kind defaults to Change — no producer in this phase classifies text"
    );
    assert!(
        found[0].superseded_on.is_some(),
        "superseded_on must resolve from the superseder's real nodes.created_at"
    );

    // Scoped lookup on the superseder id alone must find nothing — this is
    // "which of these nodes has been superseded", not "involved in any edge"
    // (same contract the D1-era unscoped method documented).
    let superseder_only = store
        .list_supersession_targets(&[superseder_id])
        .await
        .expect("list_supersession_targets (superseder only)");
    assert!(superseder_only.is_empty());

    // ── `correction` — direct SQL, mirroring the real column update a future
    // §3.4a anchored-correction write would perform ──────────────────────────
    sqlx::query("UPDATE supersession_edge SET kind = 'correction' WHERE superseded_id = $1 AND superseder_id = $2")
        .bind(superseded_id)
        .bind(superseder_id)
        .execute(&_pool)
        .await
        .expect("set kind = correction");

    let found_after = store
        .list_supersession_targets(&[superseded_id])
        .await
        .expect("list_supersession_targets after kind update");
    assert_eq!(found_after.len(), 1);
    assert_eq!(
        found_after[0].kind,
        SupersessionKind::Correction,
        "kind must round-trip through the widened query, not just default"
    );

    println!(
        "OK: list_supersession_targets against real Postgres — kind={:?} superseded_on={:?}",
        found_after[0].kind, found_after[0].superseded_on
    );
}

#[tokio::test]
async fn list_supersession_targets_retains_multiple_superseders_against_real_postgres() {
    // §3.6's headline repair: the OLD single-valued `HashMap<Uuid, Uuid>`
    // downstream shape kept "an arbitrary one" when a node was superseded by
    // more than one successor. This proves the STORE layer itself already
    // returns every row (the repair is in `routes::queries::multi_graph_
    // retrieve`'s aggregation, but it can only work if the store gives it
    // every row to aggregate in the first place).
    let Ok(url) = std::env::var("RELINFER_TEST_DATABASE_URL") else {
        eprintln!("{SKIP_REASON}");
        return;
    };
    let (store, _pool, _account_id, graph_id) = setup(&url).await;

    let old = store
        .upsert_node(graph_id, "thread", "the launch date is March 3rd")
        .await
        .expect("upsert old node");
    let new_a = store
        .upsert_node(graph_id, "thread", "the launch date is March 10th (revision A)")
        .await
        .expect("upsert revision A");
    let new_b = store
        .upsert_node(graph_id, "thread", "the launch date is March 17th (revision B)")
        .await
        .expect("upsert revision B");

    store
        .upsert_supersession_edge(new_a, old)
        .await
        .expect("edge A");
    store
        .upsert_supersession_edge(new_b, old)
        .await
        .expect("edge B");

    let found = store
        .list_supersession_targets(&[old])
        .await
        .expect("list_supersession_targets");
    assert_eq!(
        found.len(),
        2,
        "BOTH superseders must come back from the store, not an arbitrary one: {found:?}"
    );
    let superseder_ids: std::collections::HashSet<Uuid> =
        found.iter().map(|t| t.superseder_id).collect();
    assert!(superseder_ids.contains(&new_a));
    assert!(superseder_ids.contains(&new_b));

    println!("OK: {} superseders retained for one superseded node", found.len());
}

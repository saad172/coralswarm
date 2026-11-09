//! Live tests for small `Store` methods that write/read directly off a
//! single row (no tenancy/RLS dimension) against a REAL Postgres.
//!
//! `Store::stamp_atom_provenance` — a trait method with the same
//! channel/author stamping capability `ingest::submit` has via its own
//! inline `has_metadata`-gated `UPDATE atoms` block. That block is left as-is (untouched, still exercised
//! end-to-end by `tests/mcp_pg_e2e.rs`'s
//! `add_context_stamps_caller_author_and_defaults_participants_...` test),
//! so `PostgresStore::stamp_atom_provenance` itself has no real call site
//! yet — this file is its only direct coverage.
//!
//! `Store::record_entity_type_hint` / `get_node_entity_type_hint` (T1,
//! "persist entity_type") — added alongside `stamp_atom_provenance` here
//! (same fixture shape: single account, node body writes, no RLS) rather
//! than in a new `*_pg.rs` file, per the T1 review's explicit note that
//! this file is the precedent to follow.
//!
//! Gated on `RELINFER_TEST_DATABASE_URL` (a Postgres with all migrations
//! applied); a normal `cargo test` with no DB configured skips both tests.
//!
//! Run with:
//!   RELINFER_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:55432/coralswarm \
//!     cargo test -p coralswarm-backend --test store_provenance_pg -- --nocapture

use chrono::Utc;
use coralswarm_backend::store::{PostgresStore, Store};
use sqlx::postgres::PgPoolOptions;
use uuid::Uuid;

#[tokio::test]
async fn stamp_atom_provenance_sets_then_coalesce_preserves_and_overwrites_against_real_postgres() {
    let url = match std::env::var("RELINFER_TEST_DATABASE_URL") {
        Ok(u) => u,
        Err(_) => {
            eprintln!("skipping: set RELINFER_TEST_DATABASE_URL to run the live provenance e2e");
            return;
        }
    };

    let pool = PgPoolOptions::new()
        .max_connections(4)
        .connect(&url)
        .await
        .expect("connect to test Postgres");

    // ── Seed tenancy chain: user → account → graph ────────────────────────────
    let email = format!("{}@test.local", Uuid::new_v4());
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
        .create_graph(account_id, "provenance-e2e", "personal")
        .await
        .expect("create graph");

    let atom_id = Uuid::new_v4();
    store
        .insert_atoms(
            &[atom_id],
            graph.id,
            "mcp",
            &["src-1".to_string()],
            &["blob-1".to_string()],
            &[],
            Utc::now(),
            user_id,
            "private",
            "english",
        )
        .await
        .expect("insert atom");

    // Initial stamp: all six fields.
    store
        .stamp_atom_provenance(
            &[atom_id],
            Some("chan-1"),
            Some("general"),
            Some(user_id.to_string().as_str()),
            Some("alice@example.com"),
            Some("coralswarm"),
            Some("claude-code/1.0"),
        )
        .await
        .expect("stamp provenance");

    let detail = store
        .atom_detail(atom_id, account_id)
        .await
        .expect("atom_detail query")
        .expect("atom exists");
    assert_eq!(detail.channel_id.as_deref(), Some("chan-1"));
    assert_eq!(detail.channel_name.as_deref(), Some("general"));
    assert_eq!(detail.author_id.as_deref(), Some(user_id.to_string()).as_deref());
    assert_eq!(detail.author_name.as_deref(), Some("alice@example.com"));
    assert_eq!(detail.project.as_deref(), Some("coralswarm"));
    assert_eq!(detail.mcp_client_name.as_deref(), Some("claude-code/1.0"));

    // A `None`-only call (e.g. only author_name freshly resolved) must never
    // clobber the fields already set — `COALESCE` semantics.
    store
        .stamp_atom_provenance(
            &[atom_id],
            None,
            None,
            None,
            Some("alice@newmail.example"),
            None,
            None,
        )
        .await
        .expect("re-stamp provenance");

    let detail = store
        .atom_detail(atom_id, account_id)
        .await
        .expect("atom_detail query")
        .expect("atom exists");
    assert_eq!(
        detail.channel_id.as_deref(),
        Some("chan-1"),
        "COALESCE must preserve the untouched channel_id"
    );
    assert_eq!(
        detail.channel_name.as_deref(),
        Some("general"),
        "COALESCE must preserve the untouched channel_name"
    );
    assert_eq!(detail.author_name.as_deref(), Some("alice@newmail.example"));
    assert_eq!(
        detail.project.as_deref(),
        Some("coralswarm"),
        "COALESCE must preserve the untouched project"
    );
    assert_eq!(
        detail.mcp_client_name.as_deref(),
        Some("claude-code/1.0"),
        "COALESCE must preserve the untouched mcp_client_name"
    );

    sqlx::query("DELETE FROM accounts WHERE id = $1")
        .bind(account_id)
        .execute(&pool)
        .await
        .ok();
    sqlx::query("DELETE FROM users WHERE id = $1")
        .bind(user_id)
        .execute(&pool)
        .await
        .ok();
}

/// Live test for `Store::record_entity_type_hint` / `get_node_entity_type_hint`
/// (T1, "persist entity_type") — the review's HIGH finding was that the PR
/// shipped with zero DB coverage; this follows the exact fixture pattern
/// above (own `[tokio::test]`, same `RELINFER_TEST_DATABASE_URL` gate, same
/// user/account/graph seeding) rather than adding a new `*_pg.rs` file, so
/// no new suite needs wiring into `.github/workflows/ci.yml` or
/// `scripts/pg_suite_coverage.manifest` (see that manifest's entry for this
/// file).
///
/// Covers: (1) a pre-T1 row with no `entity_type` key at all reads back
/// `None`, not an error or an empty string; (2) blank/whitespace is a true
/// no-op — `record_entity_type_hint` trims-and-checks before issuing any
/// `UPDATE`, so it must never turn "absent" into a stored empty string; (3)
/// recording a real hint round-trips; (4) recording never clobbers an
/// unrelated `body` key set first (here, the digest worker's `body.digest`)
/// — the `jsonb_set` in `record_entity_type_hint` must touch only the
/// `entity_type` key.
///
/// Run with:
///   RELINFER_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:55432/coralswarm \
///     cargo test -p coralswarm-backend --test store_provenance_pg -- --nocapture
#[tokio::test]
async fn entity_type_hint_backward_compat_write_and_preserves_sibling_body_keys_against_real_postgres()
{
    let url = match std::env::var("RELINFER_TEST_DATABASE_URL") {
        Ok(u) => u,
        Err(_) => {
            eprintln!(
                "skipping: set RELINFER_TEST_DATABASE_URL to run the live entity_type-hint e2e"
            );
            return;
        }
    };

    let pool = PgPoolOptions::new()
        .max_connections(4)
        .connect(&url)
        .await
        .expect("connect to test Postgres");

    // ── Seed tenancy chain: user → account → graph → atom → node ──────────────
    let email = format!("{}@test.local", Uuid::new_v4());
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
        .create_graph(account_id, "entity-type-hint-e2e", "personal")
        .await
        .expect("create graph");

    let atom_id = Uuid::new_v4();
    store
        .insert_atoms(
            &[atom_id],
            graph.id,
            "mcp",
            &["src-1".to_string()],
            &["blob-1".to_string()],
            &[],
            Utc::now(),
            user_id,
            "private",
            "english",
        )
        .await
        .expect("insert atom");

    let node_id = store
        .upsert_node_and_link_atom(graph.id, "entity", "Alice Johnson", atom_id)
        .await
        .expect("upsert node")
        .expect("node accepted (not rejected as a diarisation placeholder)");

    // (1) Pre-T1 row: no `entity_type` key at all yet.
    let hint = store
        .get_node_entity_type_hint(node_id)
        .await
        .expect("read entity_type hint (pre-T1 row)");
    assert_eq!(hint, None, "a node with no entity_type key must read back None");

    // (2) Blank/whitespace is a true no-op.
    store
        .record_entity_type_hint(node_id, "   ")
        .await
        .expect("record blank entity_type hint (must succeed as a no-op)");
    let hint = store
        .get_node_entity_type_hint(node_id)
        .await
        .expect("read entity_type hint after blank record");
    assert_eq!(hint, None, "a blank/whitespace entity_type must never be written");

    // (4, set up before (3)) Set an unrelated body key FIRST — a digest, the
    // way the digest worker does — so recording the hint next proves it
    // doesn't clobber sibling body content.
    let digest_json = r#"{"summary":"Alice leads the backend team.","claim_ids":[]}"#;
    store.set_node_digest(node_id, digest_json).await.expect("set digest");

    // (3) Recording a real hint round-trips.
    store
        .record_entity_type_hint(node_id, "person")
        .await
        .expect("record entity_type hint");
    let hint = store
        .get_node_entity_type_hint(node_id)
        .await
        .expect("read entity_type hint after record");
    assert_eq!(hint.as_deref(), Some("person"));

    // (4) The digest set before the hint is untouched.
    let (_, digest) = store
        .get_node_digest(node_id)
        .await
        .expect("read digest")
        .expect("node exists");
    let digest_value: serde_json::Value = serde_json::from_str(&digest.expect("digest still present"))
        .expect("digest is valid json");
    assert_eq!(
        digest_value,
        serde_json::json!({"summary": "Alice leads the backend team.", "claim_ids": []}),
        "record_entity_type_hint must not clobber sibling body keys"
    );

    sqlx::query("DELETE FROM accounts WHERE id = $1")
        .bind(account_id)
        .execute(&pool)
        .await
        .ok();
    sqlx::query("DELETE FROM users WHERE id = $1")
        .bind(user_id)
        .execute(&pool)
        .await
        .ok();
}

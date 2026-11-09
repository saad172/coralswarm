//! Live test for the `meeting_shares` Store methods (cs-uxp3b-library-search)
//! against a REAL Postgres.
//!
//! The `POST /v1/meetings/shares` route's "only INGESTED meetings are
//! shareable" happy path (an existing `meeting_transcripts` row + a
//! successful share creation) needs a real database: raw transcripts follow
//! the encrypted-R2 blob path, and `Store::get_meeting_transcript` reads the
//! row that path writes. This file proves it end-to-end against Postgres.
//!
//! Gated on `RELINFER_TEST_DATABASE_URL` (a Postgres with all migrations
//! applied, including `0068_meeting_shares`); a normal `cargo test` with no DB
//! configured skips it.
//!
//! Run with:
//!   RELINFER_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:55432/coralswarm \
//!     cargo test -p coralswarm-backend --test meeting_shares_pg -- --nocapture

use coralswarm_backend::store::{PostgresStore, Store};
use sqlx::postgres::PgPoolOptions;
use uuid::Uuid;

#[tokio::test]
async fn meeting_shares_full_lifecycle_against_real_postgres() {
    let url = match std::env::var("RELINFER_TEST_DATABASE_URL") {
        Ok(u) => u,
        Err(_) => {
            eprintln!("skipping: set RELINFER_TEST_DATABASE_URL to run the live meeting_shares e2e");
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
    sqlx::query("INSERT INTO account_members (account_id, user_id, role) VALUES ($1, $2, 'owner')")
        .bind(account_id)
        .bind(user_id)
        .execute(&pool)
        .await
        .expect("insert account_member");

    let store = PostgresStore::new(pool.clone());
    let graph = store
        .create_graph(account_id, "meeting-shares-e2e", "personal")
        .await
        .expect("create graph");
    let g = graph.id;
    let source_id = format!("meeting-{}", Uuid::new_v4());

    // ── The "no transcript row" gate: unshareable until ingested ──────────────
    assert!(
        store
            .get_meeting_transcript(g, &source_id)
            .await
            .expect("transcript lookup")
            .is_none(),
        "no meeting_transcripts row yet — the route's shareability gate must reject this"
    );

    // ── Ingest a transcript, which flips the meeting to shareable ─────────────
    store
        .upsert_meeting_transcript(g, &source_id, None, Some("hello world, verbatim transcript"), Some(12_345), None)
        .await
        .expect("upsert transcript");
    assert!(
        store
            .get_meeting_transcript(g, &source_id)
            .await
            .expect("transcript lookup")
            .is_some(),
        "transcript row now exists — the meeting is shareable"
    );

    // IDOR precondition the route also checks: the owning user can reach this graph.
    assert!(
        store
            .graph_reachable_by_user(g, user_id)
            .await
            .expect("reachability check")
    );

    // ── create_meeting_share: the full happy path ─────────────────────────────
    let token = format!("tok-{}", Uuid::new_v4());
    let share = store
        .create_meeting_share(g, &source_id, &token, "link", user_id)
        .await
        .expect("create share");
    assert_eq!(share.graph_id, g);
    assert_eq!(share.source_id, source_id);
    assert_eq!(share.token, token);
    assert_eq!(share.visibility, "link");
    assert_eq!(share.created_by, user_id);
    assert!(share.revoked_at.is_none(), "a freshly created share is active");

    // ── list_meeting_shares roundtrips it ─────────────────────────────────────
    let listed = store
        .list_meeting_shares(g, &source_id)
        .await
        .expect("list shares");
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].id, share.id);

    // ── meeting_share_graph_id (IDOR helper the revoke route uses) ────────────
    assert_eq!(
        store.meeting_share_graph_id(share.id).await.expect("lookup"),
        Some(g)
    );
    assert_eq!(
        store
            .meeting_share_graph_id(Uuid::new_v4())
            .await
            .expect("lookup miss"),
        None,
        "an unknown share id must read as None, not an error"
    );

    // ── get_active_meeting_share_by_token resolves the active share ──────────
    let active = store
        .get_active_meeting_share_by_token(&token)
        .await
        .expect("active lookup")
        .expect("share is active");
    assert_eq!(active.id, share.id);
    assert_eq!(active.visibility, "link");

    // ── revoke_meeting_share: permanently disables it ─────────────────────────
    store
        .revoke_meeting_share(share.id)
        .await
        .expect("revoke share");
    assert!(
        store
            .get_active_meeting_share_by_token(&token)
            .await
            .expect("active lookup after revoke")
            .is_none(),
        "a revoked token must no longer resolve as active"
    );

    // Revoking again is a no-op (idempotent), not an error.
    store
        .revoke_meeting_share(share.id)
        .await
        .expect("revoke share again is a no-op");

    // list_meeting_shares still surfaces the (now revoked) row for history/audit.
    let listed_after_revoke = store
        .list_meeting_shares(g, &source_id)
        .await
        .expect("list shares after revoke");
    assert_eq!(listed_after_revoke.len(), 1);
    assert!(listed_after_revoke[0].revoked_at.is_some());

    // ── token uniqueness is enforced at the DB layer ──────────────────────────
    let dup_source = format!("meeting-{}", Uuid::new_v4());
    store
        .upsert_meeting_transcript(g, &dup_source, None, Some("another transcript"), None, None)
        .await
        .expect("upsert second transcript");
    let dup_result = store
        .create_meeting_share(g, &dup_source, &token, "link", user_id)
        .await;
    assert!(
        dup_result.is_err(),
        "the UNIQUE constraint on meeting_shares.token must reject a reused token"
    );

    // ── Cleanup (cascades through graph → atoms; meeting_shares has no FK so
    // clean it explicitly) ────────────────────────────────────────────────────
    sqlx::query("DELETE FROM meeting_shares WHERE graph_id = $1").bind(g).execute(&pool).await.ok();
    sqlx::query("DELETE FROM meeting_transcripts WHERE graph_id = $1").bind(g).execute(&pool).await.ok();
    sqlx::query("DELETE FROM accounts WHERE id = $1").bind(account_id).execute(&pool).await.ok();
    sqlx::query("DELETE FROM users WHERE id = $1").bind(user_id).execute(&pool).await.ok();

    println!("PG meeting_shares e2e passed: full lifecycle (create/list/lookup/revoke/token-uniqueness) verified");
}

/// `get_active_meeting_share_by_token` / `_scoped` must both stop resolving a
/// still-ACTIVE share once its owning graph is archived — the `graphs.
/// archived_at` gate every other ocean-resolving path already enforces
/// (`resolve_reachable_graphs`/`resolve_graph_reachable_by_user`, migration
/// 0091). Without this join, a 'link' share minted before a member-private
/// ocean archived on offboarding would stay resolvable by token forever
/// (including through the whole 60-day purge window) even though every other
/// read on that ocean 404s.
#[tokio::test]
async fn archived_ocean_hides_an_otherwise_active_meeting_share_by_token() {
    let url = match std::env::var("RELINFER_TEST_DATABASE_URL") {
        Ok(u) => u,
        Err(_) => {
            eprintln!(
                "skipping: set RELINFER_TEST_DATABASE_URL to run the archived-ocean meeting-share test"
            );
            return;
        }
    };
    let pool = PgPoolOptions::new()
        .max_connections(4)
        .connect(&url)
        .await
        .expect("connect to test Postgres");

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
    sqlx::query("INSERT INTO account_members (account_id, user_id, role) VALUES ($1, $2, 'owner')")
        .bind(account_id)
        .bind(user_id)
        .execute(&pool)
        .await
        .expect("insert account_member");

    let store = PostgresStore::new(pool.clone());
    let graph = store
        .create_graph(account_id, "meeting-shares-archived-e2e", "personal")
        .await
        .expect("create graph");
    let g = graph.id;
    let source_id = format!("meeting-{}", Uuid::new_v4());
    store
        .upsert_meeting_transcript(g, &source_id, None, Some("archived-ocean transcript"), Some(1_000), None)
        .await
        .expect("upsert transcript");

    let token = format!("tok-arch-{}", Uuid::new_v4());
    let share = store
        .create_meeting_share(g, &source_id, &token, "link", user_id)
        .await
        .expect("create share");

    // Before archiving: both the unscoped and scoped lookups resolve.
    assert!(
        store.get_active_meeting_share_by_token(&token).await.expect("unscoped lookup").is_some(),
        "unscoped lookup must resolve the share before the ocean is archived"
    );
    let mut tx = pool.begin().await.expect("begin tx");
    assert!(
        store
            .get_active_meeting_share_by_token_scoped(&mut tx, &token)
            .await
            .expect("scoped lookup")
            .is_some(),
        "scoped lookup must resolve the share before the ocean is archived"
    );
    tx.rollback().await.ok();

    // Archive the owning ocean directly (bypassing the archive worker — this
    // test only proves the READ-side gate, not the archive lifecycle itself).
    sqlx::query("UPDATE graphs SET archived_at = now() WHERE id = $1")
        .bind(g)
        .execute(&pool)
        .await
        .expect("archive graph");

    // After archiving: the share is STILL `revoked_at IS NULL` (never
    // revoked) — only the new `archived_at` join hides it now.
    assert!(
        store.get_active_meeting_share_by_token(&token).await.expect("unscoped lookup after archive").is_none(),
        "an ACTIVE (non-revoked) share for an ARCHIVED ocean must stop resolving (unscoped)"
    );
    let mut tx = pool.begin().await.expect("begin tx");
    assert!(
        store
            .get_active_meeting_share_by_token_scoped(&mut tx, &token)
            .await
            .expect("scoped lookup after archive")
            .is_none(),
        "an ACTIVE (non-revoked) share for an ARCHIVED ocean must stop resolving (scoped)"
    );
    tx.rollback().await.ok();

    // The share row itself is untouched (still `revoked_at IS NULL`) —
    // proving the gate is the graph's `archived_at`, not a side-effect on
    // `meeting_shares` itself.
    let listed = store.list_meeting_shares(g, &source_id).await.expect("list shares");
    assert_eq!(listed.len(), 1);
    assert!(listed[0].revoked_at.is_none(), "archiving the ocean must not touch meeting_shares.revoked_at");
    assert_eq!(listed[0].id, share.id);

    sqlx::query("DELETE FROM meeting_shares WHERE graph_id = $1").bind(g).execute(&pool).await.ok();
    sqlx::query("DELETE FROM meeting_transcripts WHERE graph_id = $1").bind(g).execute(&pool).await.ok();
    sqlx::query("DELETE FROM accounts WHERE id = $1").bind(account_id).execute(&pool).await.ok();
    sqlx::query("DELETE FROM users WHERE id = $1").bind(user_id).execute(&pool).await.ok();

    println!("PG meeting_shares archived-ocean test passed: both unscoped and scoped lookups hide an active share once its ocean is archived");
}

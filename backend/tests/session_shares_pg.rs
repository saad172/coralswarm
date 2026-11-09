//! Live tests for the `coding_session_shares` Store methods (coding-session
//! capture Phase 5, audit amendment A7) against a REAL Postgres.
//!
//! A clone of `tests/meeting_shares_pg.rs`, extended with the additional
//! scenarios the coding-session-share design needs proven against real SQL:
//! cross-graph isolation and "a share's coral list contains only its own
//! session's corals" (`Store::list_shared_session_corals`'s `graph_id AND source_id
//! LIKE` scoping).
//!
//! This file carries most of this feature's coverage: the full
//! create/list/lookup/revoke/token-uniqueness lifecycle, PLUS revocation,
//! cross-graph isolation, and per-session coral scoping. See `routes::session_shares`'s `#[cfg(test)]`
//! module doc comment for why the HANDLER logic itself (IDOR guard, generic
//! 404 shape, rate limiting) is proven only by code-identity with
//! `routes::shares`'s already-unit-tested equivalents, not by a second HTTP
//! harness here.
//!
//! Gated on `RELINFER_TEST_DATABASE_URL` (a Postgres with all migrations
//! applied, including `0077_coding_session_shares`); a normal `cargo test`
//! with no DB configured skips it.
//!
//! Run with:
//!   RELINFER_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:55432/coralswarm \
//!     cargo test -p coralswarm-backend --test session_shares_pg -- --nocapture

use axum::body::Body;
use axum::http::{Request as HttpRequest, StatusCode};
use chrono::Utc;
use coralswarm_backend::store::{PostgresStore, Store};
use serde_json::Value;
use sqlx::postgres::PgPoolOptions;
use std::sync::Arc;
use tower::ServiceExt;
use uuid::Uuid;
use coralswarm_backend::store::SessionIdentity;

macro_rules! test_pool {
    () => {{
        match std::env::var("RELINFER_TEST_DATABASE_URL") {
            Ok(u) => PgPoolOptions::new()
                .max_connections(4)
                .connect(&u)
                .await
                .expect("connect to test Postgres"),
            Err(_) => {
                eprintln!("skipping: set RELINFER_TEST_DATABASE_URL to run session_shares_pg");
                return;
            }
        }
    }};
}

/// Seed a fresh `(user, account, graph)` tenancy chain — pattern-copied from
/// `coding_sessions_read_pg.rs` / `meeting_shares_pg.rs`.
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
    sqlx::query("INSERT INTO account_members (account_id, user_id, role) VALUES ($1, $2, 'owner')")
        .bind(account_id)
        .bind(user_id)
        .execute(pool)
        .await
        .expect("insert account_member");
    let store = PostgresStore::new(pool.clone());
    let graph = store
        .create_graph(account_id, &format!("session-shares-e2e-{}", Uuid::new_v4()), "personal")
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

/// Insert one atom with inline content at a given `source_id` — pattern-copied
/// from `coding_sessions_read_pg.rs::seed_atom` (trimmed: no `project` stamp,
/// not needed here).
async fn seed_atom(
    store: &PostgresStore,
    pool: &sqlx::PgPool,
    graph_id: Uuid,
    user_id: Uuid,
    source_id: &str,
    content: &str,
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
            Utc::now(),
            user_id,
            "private",
            "english",
        )
        .await
        .expect("insert_atoms");
    sqlx::query("UPDATE atoms SET content_inline = $1 WHERE atom_id = $2")
        .bind(content)
        .bind(atom_id)
        .execute(pool)
        .await
        .expect("backfill content_inline");
    atom_id
}

// ── Full lifecycle (create/list/lookup/revoke/token-uniqueness) ─────────────
// Direct clone of `meeting_shares_pg.rs`'s single test, retargeted at
// `coding_sessions`/`coding_session_shares`.

#[tokio::test]
async fn session_shares_full_lifecycle_against_real_postgres() {
    let pool = test_pool!();
    let (user_id, account_id, graph_id) = seed_tenancy(&pool).await;
    let store = PostgresStore::new(pool.clone());
    let session_id = format!("sess-{}", Uuid::new_v4());

    // ── The "no coding_sessions row" gate: unshareable until captured ────────
    assert!(
        store
            .get_coding_session(graph_id, &session_id)
            .await
            .expect("session lookup")
            .is_none(),
        "no coding_sessions row yet — the route's shareability gate must reject this"
    );

    // ── Capture the session, which flips it to shareable ──────────────────────
    store
        .upsert_coding_session(
            graph_id,
            &session_id,
            "coding",
            Some("proj-a"),
            Some("hello world session"),
            None,
            None,
            None,
            None,
            None,
            None,
            &SessionIdentity::default(),
        )
        .await
        .expect("upsert coding session");
    assert!(
        store
            .get_coding_session(graph_id, &session_id)
            .await
            .expect("session lookup")
            .is_some(),
        "coding_sessions row now exists — the session is shareable"
    );

    // IDOR precondition the route also checks: the owning user can reach this graph.
    assert!(
        store
            .graph_reachable_by_user(graph_id, user_id)
            .await
            .expect("reachability check")
    );

    // ── create_session_share: the full happy path ──────────────────────────────
    let token = format!("tok-{}", Uuid::new_v4());
    let share = store
        .create_session_share(graph_id, &session_id, &token, "link", user_id)
        .await
        .expect("create share");
    assert_eq!(share.graph_id, graph_id);
    assert_eq!(share.session_id, session_id);
    assert_eq!(share.token, token);
    assert_eq!(share.visibility, "link");
    assert_eq!(share.created_by, user_id);
    assert!(share.revoked_at.is_none(), "a freshly created share is active");

    // ── list_session_shares roundtrips it ───────────────────────────────────────
    let listed = store
        .list_session_shares(graph_id, &session_id)
        .await
        .expect("list shares");
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].id, share.id);

    // ── session_share_graph_id (IDOR helper the revoke route uses) ─────────────
    assert_eq!(
        store.session_share_graph_id(share.id).await.expect("lookup"),
        Some(graph_id)
    );
    assert_eq!(
        store
            .session_share_graph_id(Uuid::new_v4())
            .await
            .expect("lookup miss"),
        None,
        "an unknown share id must read as None, not an error"
    );

    // ── get_active_session_share_by_token resolves the active share ────────────
    let active = store
        .get_active_session_share_by_token(&token)
        .await
        .expect("active lookup")
        .expect("share is active");
    assert_eq!(active.id, share.id);
    assert_eq!(active.visibility, "link");

    // ── revoke_session_share: permanently disables it — the "revoked token
    // 404s" guarantee, proven at the Store layer (the route maps `None` here
    // straight to a generic 404) ────────────────────────────────────────────
    store
        .revoke_session_share(share.id)
        .await
        .expect("revoke share");
    assert!(
        store
            .get_active_session_share_by_token(&token)
            .await
            .expect("active lookup after revoke")
            .is_none(),
        "a revoked token must no longer resolve as active"
    );

    // Revoking again is a no-op (idempotent), not an error.
    store
        .revoke_session_share(share.id)
        .await
        .expect("revoke share again is a no-op");

    // list_session_shares still surfaces the (now revoked) row for history/audit.
    let listed_after_revoke = store
        .list_session_shares(graph_id, &session_id)
        .await
        .expect("list shares after revoke");
    assert_eq!(listed_after_revoke.len(), 1);
    assert!(listed_after_revoke[0].revoked_at.is_some());

    // ── token uniqueness is enforced at the DB layer ────────────────────────────
    let dup_session_id = format!("sess-{}", Uuid::new_v4());
    store
        .upsert_coding_session(graph_id, &dup_session_id, "coding", None, None, None, None, None, None, None, None, &SessionIdentity::default())
        .await
        .expect("upsert second session");
    let dup_result = store
        .create_session_share(graph_id, &dup_session_id, &token, "link", user_id)
        .await;
    assert!(
        dup_result.is_err(),
        "the UNIQUE constraint on coding_session_shares.token must reject a reused token"
    );

    // ── Composite FK: a share cannot reference a session that doesn't exist
    // in THIS graph (proves the FK to coding_sessions(graph_id, session_id),
    // not just graphs(id)) ──────────────────────────────────────────────────
    let never_captured = format!("sess-never-captured-{}", Uuid::new_v4());
    let fk_result = store
        .create_session_share(
            graph_id,
            &never_captured,
            &format!("tok-fk-{}", Uuid::new_v4()),
            "link",
            user_id,
        )
        .await;
    assert!(
        fk_result.is_err(),
        "the composite FK to coding_sessions(graph_id, session_id) must reject a share for an \
         uncaptured session"
    );

    cleanup(&pool, account_id, user_id).await;

    println!(
        "PG session_shares e2e passed: full lifecycle (create/list/lookup/revoke/token-uniqueness/FK) verified"
    );
}

// ── Cross-graph isolation ─────────────────────────────────────────────────────

/// A token minted for graph A's session must never resolve data belonging to
/// graph B — even when graph B has a session with the LITERAL SAME
/// `session_id` string (a caller-supplied value with no cross-graph
/// uniqueness guarantee).
#[tokio::test]
async fn session_share_token_never_leaks_a_same_named_session_in_a_different_graph() {
    let pool = test_pool!();
    let (user_a, account_a, graph_a) = seed_tenancy(&pool).await;
    let (user_b, account_b, graph_b) = seed_tenancy(&pool).await;
    let store = PostgresStore::new(pool.clone());

    // Same session_id text in BOTH graphs — the collision this test targets.
    let shared_session_id = "shared-name-sess";
    store
        .upsert_coding_session(
            graph_a,
            shared_session_id,
            "coding",
            None,
            Some("Graph A's session"),
            None,
            None,
            None,
            None,
            None,
            None,
            &SessionIdentity::default(),
        )
        .await
        .expect("upsert session in graph A");
    store
        .upsert_coding_session(
            graph_b,
            shared_session_id,
            "coding",
            None,
            Some("Graph B's session"),
            None,
            None,
            None,
            None,
            None,
            None,
            &SessionIdentity::default(),
        )
        .await
        .expect("upsert session in graph B");

    // Distinct corals per graph, same session_id convention.
    let atom_a = seed_atom(
        &store,
        &pool,
        graph_a,
        user_a,
        &format!("cc:{shared_session_id}:ckpt-1"),
        "graph A secret content",
    )
    .await;
    let atom_b = seed_atom(
        &store,
        &pool,
        graph_b,
        user_b,
        &format!("cc:{shared_session_id}:ckpt-1"),
        "graph B secret content",
    )
    .await;

    // Mint a share for graph A's session only.
    let token = format!("tok-{}", Uuid::new_v4());
    let share = store
        .create_session_share(graph_a, shared_session_id, &token, "link", user_a)
        .await
        .expect("create share for graph A");

    // The resolved share's graph_id is graph A, never graph B — this is what
    // the public route uses to scope the subsequent coral fetch, so getting
    // this right is the entire cross-graph isolation guarantee.
    let active = store
        .get_active_session_share_by_token(&token)
        .await
        .expect("active lookup")
        .expect("share is active");
    assert_eq!(active.graph_id, graph_a, "token must resolve to graph A, never graph B");
    assert_eq!(share.graph_id, graph_a);

    // Fetching corals with the RESOLVED graph_id returns only graph A's atom.
    let corals_a = store
        .list_shared_session_corals(active.graph_id, &active.session_id)
        .await
        .expect("list corals for graph A");
    let coral_a_ids: Vec<Uuid> = corals_a.iter().map(|c| c.atom_id).collect();
    assert!(coral_a_ids.contains(&atom_a), "must include graph A's own atom: {coral_a_ids:?}");
    assert!(
        !coral_a_ids.contains(&atom_b),
        "must NEVER include graph B's atom despite the identical session_id: {coral_a_ids:?}"
    );

    // Direct sanity check the other direction too: querying graph B's data
    // (as if a caller tried to smuggle graph B's id in) with graph A's
    // session_id returns nothing wrong either — the query is scoped by BOTH
    // graph_id AND source_id together.
    let corals_wrong_graph = store
        .list_shared_session_corals(graph_b, &active.session_id)
        .await
        .expect("list corals querying graph B directly");
    let wrong_ids: Vec<Uuid> = corals_wrong_graph.iter().map(|c| c.atom_id).collect();
    assert!(
        !wrong_ids.contains(&atom_a),
        "graph A's atom must never appear when querying graph B: {wrong_ids:?}"
    );
    assert!(
        wrong_ids.contains(&atom_b),
        "querying graph B directly must still correctly return graph B's own atom: {wrong_ids:?}"
    );

    // meeting_share_graph_id-equivalent: a share id always resolves to its
    // OWN graph, never the sibling graph with the colliding session name.
    assert_eq!(
        store.session_share_graph_id(share.id).await.expect("lookup"),
        Some(graph_a)
    );

    cleanup(&pool, account_a, user_a).await;
    cleanup(&pool, account_b, user_b).await;

    println!("PG session_shares cross-graph isolation test passed");
}

// ── Per-session coral scoping ─────────────────────────────────────────────────

/// A share exposes ONLY the one session's corals — never a sibling session's,
/// even within the SAME graph/account.
#[tokio::test]
async fn list_shared_session_corals_scopes_to_exactly_one_session_within_the_same_graph() {
    let pool = test_pool!();
    let (user_id, account_id, graph_id) = seed_tenancy(&pool).await;
    let store = PostgresStore::new(pool.clone());

    store
        .upsert_coding_session(graph_id, "sess-x", "coding", None, None, None, None, None, None, None, None, &SessionIdentity::default())
        .await
        .unwrap();
    store
        .upsert_coding_session(graph_id, "sess-x-sibling", "coding", None, None, None, None, None, None, None, None, &SessionIdentity::default())
        .await
        .unwrap();

    let x1 = seed_atom(&store, &pool, graph_id, user_id, "cc:sess-x:ckpt-1", "x checkpoint one").await;
    let x2 = seed_atom(&store, &pool, graph_id, user_id, "cc:sess-x:ckpt-2", "x checkpoint two").await;
    let sibling = seed_atom(
        &store,
        &pool,
        graph_id,
        user_id,
        "cc:sess-x-sibling:ckpt-1",
        "sibling session's coral — must never leak into sess-x's share",
    )
    .await;

    let corals = store
        .list_shared_session_corals(graph_id, "sess-x")
        .await
        .expect("list_shared_session_corals");
    let ids: Vec<Uuid> = corals.iter().map(|c| c.atom_id).collect();

    assert_eq!(ids.len(), 2, "exactly sess-x's two corals: {ids:?}");
    assert!(ids.contains(&x1) && ids.contains(&x2));
    assert!(
        !ids.contains(&sibling),
        "a same-graph SIBLING session's coral must never appear: {ids:?}"
    );

    // Chronological order (oldest first), matching the public payload's contract.
    assert_eq!(corals[0].atom_id, x1, "corals must be oldest-first");
    assert_eq!(corals[1].atom_id, x2);

    // Content resolves correctly too (inline path — no R2 configured in this
    // harness), confirming the fetch returns usable coral data, not just ids.
    assert_eq!(corals[0].content_inline.as_deref(), Some("x checkpoint one"));

    cleanup(&pool, account_id, user_id).await;

    println!("PG session_shares per-session coral scoping test passed");
}

// ── archived_at gate ──────────────────────────────────────────────────────

/// Session-share sibling of `meeting_shares_pg.rs`'s
/// `archived_ocean_hides_an_otherwise_active_meeting_share_by_token`:
/// `get_active_session_share_by_token` / `_scoped` must both stop resolving a
/// still-ACTIVE (never revoked) share once its owning graph is archived —
/// the same `graphs.archived_at` gate every other ocean-resolving path
/// enforces (`resolve_reachable_graphs`/`resolve_graph_reachable_by_user`,
/// migration 0091).
#[tokio::test]
async fn archived_ocean_hides_an_otherwise_active_session_share_by_token() {
    let pool = test_pool!();
    let (user_id, account_id, graph_id) = seed_tenancy(&pool).await;
    let store = PostgresStore::new(pool.clone());
    let session_id = format!("sess-arch-{}", Uuid::new_v4());

    store
        .upsert_coding_session(graph_id, &session_id, "coding", None, None, None, None, None, None, None, None, &SessionIdentity::default())
        .await
        .expect("upsert coding session");

    let token = format!("tok-arch-{}", Uuid::new_v4());
    let share = store
        .create_session_share(graph_id, &session_id, &token, "link", user_id)
        .await
        .expect("create share");

    // Before archiving: both the unscoped and scoped lookups resolve.
    assert!(
        store.get_active_session_share_by_token(&token).await.expect("unscoped lookup").is_some(),
        "unscoped lookup must resolve the share before the ocean is archived"
    );
    let mut tx = pool.begin().await.expect("begin tx");
    assert!(
        store
            .get_active_session_share_by_token_scoped(&mut tx, &token)
            .await
            .expect("scoped lookup")
            .is_some(),
        "scoped lookup must resolve the share before the ocean is archived"
    );
    tx.rollback().await.ok();

    // Archive the owning ocean directly (bypassing the archive worker — this
    // test only proves the READ-side gate, not the archive lifecycle itself).
    sqlx::query("UPDATE graphs SET archived_at = now() WHERE id = $1")
        .bind(graph_id)
        .execute(&pool)
        .await
        .expect("archive graph");

    // After archiving: the share is STILL `revoked_at IS NULL` (never
    // revoked) — only the new `archived_at` join hides it now.
    assert!(
        store.get_active_session_share_by_token(&token).await.expect("unscoped lookup after archive").is_none(),
        "an ACTIVE (non-revoked) share for an ARCHIVED ocean must stop resolving (unscoped)"
    );
    let mut tx = pool.begin().await.expect("begin tx");
    assert!(
        store
            .get_active_session_share_by_token_scoped(&mut tx, &token)
            .await
            .expect("scoped lookup after archive")
            .is_none(),
        "an ACTIVE (non-revoked) share for an ARCHIVED ocean must stop resolving (scoped)"
    );
    tx.rollback().await.ok();

    // The share row itself is untouched (still `revoked_at IS NULL`) —
    // proving the gate is the graph's `archived_at`, not a side-effect on
    // `coding_session_shares` itself.
    let listed = store.list_session_shares(graph_id, &session_id).await.expect("list shares");
    assert_eq!(listed.len(), 1);
    assert!(listed[0].revoked_at.is_none(), "archiving the ocean must not touch coding_session_shares.revoked_at");
    assert_eq!(listed[0].id, share.id);

    cleanup(&pool, account_id, user_id).await;

    println!("PG session_shares archived-ocean test passed: both unscoped and scoped lookups hide an active share once its ocean is archived");
}

#[tokio::test]
async fn list_shared_session_corals_zero_corals_case_is_empty_not_an_error() {
    let pool = test_pool!();
    let (user_id, account_id, graph_id) = seed_tenancy(&pool).await;
    let store = PostgresStore::new(pool.clone());

    store
        .upsert_coding_session(graph_id, "sess-empty", "coding", None, None, None, None, None, None, None, None, &SessionIdentity::default())
        .await
        .unwrap();

    let corals = store
        .list_shared_session_corals(graph_id, "sess-empty")
        .await
        .expect("list_shared_session_corals on a session with zero corals");
    assert!(corals.is_empty());

    cleanup(&pool, account_id, user_id).await;
}

// ── Titles-always-present (independent review finding M3) ──────────────────
//
// `routes::session_shares::public_session_share` is the ANONYMOUS
// `GET /v1/shared/sessions/{token}` route — real HTTP, through the full
// production router (`coralswarm_backend::test_support_app`), not a
// Store-level call. Proves `build_shared_session_payload`/
// `build_shared_corals` actually wire `display_session_title`/`display_title`
// into the JSON response rather than leaving the raw nullable
// `coding_sessions.title`/no title at all on the wire — the session here is
// captured with NO title (`upsert_coding_session(..., title: None, ...)`,
// the common "in-progress session" shape) and kind `"meeting"`, so a null/absent
// title (or a hardcoded "Coding session" label ignoring the kind) would both
// be regressions this test catches.
fn app_over(pool: &sqlx::PgPool) -> axum::Router {
    let mut state = (*coralswarm_backend::test_app_state()).clone();
    state.db = Some(pool.clone());
    state.store = Some(Arc::new(PostgresStore::new(pool.clone())));
    coralswarm_backend::test_support_app(Arc::new(state))
}

async fn body_json(resp: axum::response::Response) -> Value {
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    serde_json::from_slice(&bytes).unwrap_or(Value::Null)
}

#[tokio::test]
async fn public_session_share_never_emits_a_null_or_absent_title() {
    let pool = test_pool!();
    let (user_id, account_id, graph_id) = seed_tenancy(&pool).await;
    let store = PostgresStore::new(pool.clone());
    let session_id = format!("sess-{}", Uuid::new_v4());

    // No title, and kind = "meeting" (not the default "coding") — both must
    // still resolve to a non-empty, correctly-labelled title.
    store
        .upsert_coding_session(graph_id, &session_id, "meeting", None, None, None, None, None, None, None, None, &SessionIdentity::default())
        .await
        .expect("upsert coding session");

    seed_atom(
        &store,
        &pool,
        graph_id,
        user_id,
        &format!("cc:{session_id}:ckpt-1"),
        "Meeting checkpoint body with no persisted title.",
    )
    .await;

    let token = format!("tok-{}", Uuid::new_v4());
    store
        .create_session_share(graph_id, &session_id, &token, "link", user_id)
        .await
        .expect("create share");

    let app = app_over(&pool);
    let req = HttpRequest::builder()
        .method("GET")
        .uri(format!("/v1/shared/sessions/{token}"))
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp).await;

    let title = body["title"]
        .as_str()
        .unwrap_or_else(|| panic!("title must be a non-empty string, not null/absent: {body}"));
    assert!(!title.trim().is_empty(), "title must be non-empty: {body}");
    assert!(
        title.starts_with("Meeting session"),
        "an untitled meeting session with no checkpoint/project fallback must be labelled by its \
         own kind, not hardcoded to \"Coding session\": {body}"
    );

    let corals = body["corals"].as_array().expect("corals array");
    assert_eq!(corals.len(), 1, "{body}");
    let coral_title = corals[0]["title"]
        .as_str()
        .unwrap_or_else(|| panic!("coral title must be a non-empty string: {body}"));
    assert!(!coral_title.trim().is_empty(), "coral title must be non-empty: {body}");

    cleanup(&pool, account_id, user_id).await;
}

//! m365 enterprise train PR 4.1 ("history_from"): per-seat-grant retrieval
//! window, enforced by RLS on the live retrieval path. Ships
//! `docs/connectors/CONNECTOR_CONTRACT.md` §6 conformance tests #5 ("Window
//! enforcement") and #6 ("No silent truncation"), plus the migration
//! `0064_team_seat_grants_history_from` unit-level proofs (multi-grant
//! resolution) and the reconciler grant-population rule.
//!
//! Runs against a single tenant database, connected AS the real
//! `coralswarm_tenant_app` role (`migrations_tenant/0009_tenant_rls`,
//! `NOBYPASSRLS`) through the production `tenancy::scope::begin_tenant_scoped`
//! primitive — same discipline as `tenant_rls_phase2_pg.rs` (the
//! `as_app_role_dsn`/`set_app_role_password_with_retry` shape this file
//! copies) and `retrieval_scoped_pg.rs` (the live-retrieval-path shape this
//! file drives directly against `PostgresStore` rather than through HTTP,
//! matching `store_scoped_final_seven_pg.rs`'s convention for `Store`-level
//! suites).
//!
//! Env-gated, like every other `*_pg.rs` suite in this tree:
//!
//!   ORGT_TEST_BASE_URL=postgres://alex@localhost:5432/postgres \
//!     cargo test -p coralswarm-backend --test history_from_pg

mod support;

use std::str::FromStr;

use chrono::{DateTime, Duration, SubsecRound, Utc};
use coralswarm_backend::retrieval::RetrievalFilter;
use coralswarm_backend::store::{PostgresStore, Store};
use coralswarm_backend::tenancy::scope::begin_tenant_scoped;
use pgvector::Vector;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::{Connection, Executor, PgConnection, PgPool};
use support::tenant_role::{set_app_role_password_with_retry, APP_ROLE_TEST_PASSWORD};
use uuid::Uuid;

const EMBED_MODEL_NAME: &str = coralswarm_core::embed::EMBED_MODEL_NAME;
const EMBED_MODEL_CLOUD: &str = coralswarm_core::embed::EMBED_MODEL_CLOUD;

fn base_url() -> Option<String> {
    std::env::var("ORGT_TEST_BASE_URL").ok()
}

fn with_db(base: &str, db: &str) -> String {
    let (prefix, rest) = base.split_at(base.rfind('/').expect("url has a path /"));
    let params = rest.find('?').map(|i| &rest[i..]).unwrap_or("");
    format!("{prefix}/{db}{params}")
}

/// Same DSN-building shape as `tenant_rls_phase2_pg.rs::as_app_role_dsn` —
/// the literal string `TenantRegistry` resolves a `tenant_databases.
/// db_secret_ref` into in production, with the role swapped to
/// `coralswarm_tenant_app`.
fn as_app_role_dsn(db_url: &str) -> String {
    let opts = PgConnectOptions::from_str(db_url)
        .expect("parse scratch db url")
        .username("coralswarm_tenant_app")
        .password(APP_ROLE_TEST_PASSWORD);
    let host = opts.get_host();
    let port = opts.get_port();
    let db = db_url.rsplit('/').next().unwrap_or("postgres");
    format!("postgres://coralswarm_tenant_app:{APP_ROLE_TEST_PASSWORD}@{host}:{port}/{db}")
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

/// One tenant database with a team ocean, seeded via the OWNER (BYPASSRLS)
/// pool so seeding itself is never subject to the policies under test.
struct Fx {
    admin_conn: PgConnection,
    db_name: String,
    /// Owner/BYPASSRLS pool — seeding only.
    owner: PgPool,
    /// A real login AS `coralswarm_tenant_app` — every scoped call in this
    /// suite runs against this pool.
    app_pool: PgPool,
    account_id: Uuid,
    team_id: Uuid,
    graph_id: Uuid,
    /// Full-history member: `history_from = NULL`.
    user_a: Uuid,
    /// Windowed member: `history_from = cutoff`.
    user_b: Uuid,
    cutoff: DateTime<Utc>,
}

impl Fx {
    async fn setup(db_name: &str) -> Option<Self> {
        let base = base_url()?;
        let mut admin_conn = admin(&base).await;
        recreate(&mut admin_conn, db_name).await;
        let db_url = with_db(&base, db_name);

        let owner: PgPool = PgPoolOptions::new()
            .max_connections(4)
            .connect(&db_url)
            .await
            .expect("connect owner");
        sqlx::migrate!("./migrations_tenant")
            .run(&owner)
            .await
            .expect("apply tenant tree incl. 0064");
        sqlx::query("INSERT INTO tenant_meta (id, org_id) VALUES (true, $1)")
            .bind(db_name)
            .execute(&owner)
            .await
            .expect("seed tenant_meta");

        set_app_role_password_with_retry(&owner).await;

        let app_pool: PgPool = PgPoolOptions::new()
            .max_connections(4)
            .connect(&as_app_role_dsn(&db_url))
            .await
            .expect("connect AS coralswarm_tenant_app");

        let account_id = Uuid::new_v4();
        let user_a = Uuid::new_v4();
        let user_b = Uuid::new_v4();
        // Truncated to microsecond precision — `TIMESTAMPTZ` only carries
        // that much, and a nanosecond-precision `Utc::now()` compared
        // against the round-tripped-through-Postgres value would spuriously
        // fail the equality assertions below over a precision mismatch, not
        // a real behavioral difference.
        let cutoff: DateTime<Utc> = Utc::now().trunc_subsecs(6);

        let graph_id: Uuid = sqlx::query_scalar(
            "INSERT INTO graphs (account_id, name, kind) VALUES ($1, 'Team ocean', 'team') \
             RETURNING id",
        )
        .bind(account_id)
        .fetch_one(&owner)
        .await
        .expect("insert team graph");

        let team_id: Uuid = sqlx::query_scalar(
            "INSERT INTO teams (account_id, graph_id, name, created_by) \
             VALUES ($1, $2, 'Eng', $3) RETURNING id",
        )
        .bind(account_id)
        .bind(graph_id)
        .bind(user_a)
        .fetch_one(&owner)
        .await
        .expect("insert team");

        for u in [user_a, user_b] {
            sqlx::query(
                "INSERT INTO team_members (team_id, user_id, role) VALUES ($1, $2, 'member')",
            )
            .bind(team_id)
            .bind(u)
            .execute(&owner)
            .await
            .expect("seat team member");
        }

        // A: full history. B: windowed at `cutoff`.
        sqlx::query(
            "INSERT INTO team_seat_grants (account_id, team_id, user_id, source, history_from) \
             VALUES ($1, $2, $3, 'test:founding', NULL)",
        )
        .bind(account_id)
        .bind(team_id)
        .bind(user_a)
        .execute(&owner)
        .await
        .expect("seat A's full-history grant");
        sqlx::query(
            "INSERT INTO team_seat_grants (account_id, team_id, user_id, source, history_from) \
             VALUES ($1, $2, $3, 'test:late-join', $4)",
        )
        .bind(account_id)
        .bind(team_id)
        .bind(user_b)
        .bind(cutoff)
        .execute(&owner)
        .await
        .expect("seat B's windowed grant");

        Some(Fx {
            admin_conn,
            db_name: db_name.to_string(),
            owner,
            app_pool,
            account_id,
            team_id,
            graph_id,
            user_a,
            user_b,
            cutoff,
        })
    }

    async fn scoped_tx(&self, user_id: Uuid) -> sqlx::Transaction<'static, sqlx::Postgres> {
        begin_tenant_scoped(&self.app_pool, self.account_id, user_id)
            .await
            .expect("begin_tenant_scoped on the coralswarm_tenant_app pool")
    }

    /// Seed one atom with a fixed near-zero-distance embedding, matching
    /// keyword content, and a linking node — everything every retrieval
    /// modality can find. `ts` offsets from `self.cutoff` (negative = before
    /// the window, positive = after).
    async fn seed_atom(&self, offset_secs: i64, tag: &str) -> Uuid {
        let ts = self.cutoff + Duration::seconds(offset_secs);
        let atom_id: Uuid = sqlx::query_scalar(
            "INSERT INTO atoms(graph_id, source_type, source_id, content_blob_key, \
                                content_inline, content_tsv, embedding, embed_model, \
                                embedding_next, embed_model_next, \"timestamp\") \
             VALUES ($1, 'test', $2, 'blob', $3, \
                     to_tsvector('simple', 'quokka expedition briefing'), $4, $5, $4, $7, $6) \
             RETURNING atom_id",
        )
        .bind(self.graph_id)
        .bind(format!("hf-{tag}-{offset_secs}"))
        .bind(format!("quokka expedition briefing {tag}"))
        .bind(Vector::from(probe_vector()))
        .bind(EMBED_MODEL_NAME)
        .bind(ts)
        .bind(EMBED_MODEL_CLOUD)
        .fetch_one(&self.owner)
        .await
        .unwrap_or_else(|e| panic!("seed atom {tag}: {e}"));
        atom_id
    }

    async fn seat_grant_count(&self) -> i64 {
        sqlx::query_scalar("SELECT count(*) FROM team_seat_grants")
            .fetch_one(&self.owner)
            .await
            .unwrap()
    }

    async fn teardown(mut self) {
        self.app_pool.close().await;
        self.owner.close().await;
        self.admin_conn
            .execute(format!("DROP DATABASE IF EXISTS {} WITH (FORCE)", self.db_name).as_str())
            .await
            .ok();
    }
}

fn probe_vector() -> Vec<f32> {
    (0..384).map(|i| (i as f32 * 0.01).sin()).collect()
}

fn store(pool: &PgPool) -> Box<dyn Store> {
    Box::new(PostgresStore::new(pool.clone()))
}

// ── Conformance test #5: window enforcement, every live retrieval modality ─

#[tokio::test]
async fn windowed_member_sees_only_post_cutoff_atoms_on_every_search_modality_pg() {
    let Some(fx) = Fx::setup("orgt_hf_t5_modalities").await else {
        eprintln!(
            "skipping windowed_member_sees_only_post_cutoff_atoms_on_every_search_modality_pg: \
             set ORGT_TEST_BASE_URL"
        );
        return;
    };

    let pre = fx.seed_atom(-3600, "pre").await;
    let post = fx.seed_atom(3600, "post").await;
    let store = store(&fx.app_pool);
    let filter = RetrievalFilter::default();

    // ── vector_search_scoped ────────────────────────────────────────────
    {
        let mut tx_a = fx.scoped_tx(fx.user_a).await;
        let a_hits = store
            .vector_search_scoped(&mut tx_a, fx.graph_id, &probe_vector(), 10, &filter)
            .await
            .expect("A vector_search_scoped");
        tx_a.commit().await.ok();
        let a_ids: Vec<Uuid> = a_hits.iter().map(|(id, _)| *id).collect();
        assert!(a_ids.contains(&pre) && a_ids.contains(&post), "A (full history) must see both");

        let mut tx_b = fx.scoped_tx(fx.user_b).await;
        let b_hits = store
            .vector_search_scoped(&mut tx_b, fx.graph_id, &probe_vector(), 10, &filter)
            .await
            .expect("B vector_search_scoped");
        tx_b.commit().await.ok();
        let b_ids: Vec<Uuid> = b_hits.iter().map(|(id, _)| *id).collect();
        assert!(
            b_ids.contains(&post) && !b_ids.contains(&pre),
            "B (windowed at cutoff) must see only the post-cutoff atom via vector_search_scoped, \
             got {b_ids:?}"
        );
    }

    // ── keyword_search_scoped ───────────────────────────────────────────
    {
        let mut tx_b = fx.scoped_tx(fx.user_b).await;
        let b_hits = store
            .keyword_search_scoped(&mut tx_b, fx.graph_id, "quokka expedition briefing", 10, &filter)
            .await
            .expect("B keyword_search_scoped");
        tx_b.commit().await.ok();
        let b_ids: Vec<Uuid> = b_hits.iter().map(|(id, _)| *id).collect();
        assert!(
            b_ids.contains(&post) && !b_ids.contains(&pre),
            "B must see only the post-cutoff atom via keyword_search_scoped, got {b_ids:?}"
        );

        let mut tx_a = fx.scoped_tx(fx.user_a).await;
        let a_hits = store
            .keyword_search_scoped(&mut tx_a, fx.graph_id, "quokka expedition briefing", 10, &filter)
            .await
            .expect("A keyword_search_scoped");
        tx_a.commit().await.ok();
        let a_ids: Vec<Uuid> = a_hits.iter().map(|(id, _)| *id).collect();
        assert!(a_ids.contains(&pre) && a_ids.contains(&post), "A must see both");
    }

    // ── timestamp_window_atoms_scoped ───────────────────────────────────
    {
        let far_past = fx.cutoff - Duration::days(3650);
        let far_future = fx.cutoff + Duration::days(3650);

        let mut tx_b = fx.scoped_tx(fx.user_b).await;
        let b_hits = store
            .timestamp_window_atoms_scoped(&mut tx_b, fx.graph_id, far_past, far_future, None, &filter)
            .await
            .expect("B timestamp_window_atoms_scoped");
        tx_b.commit().await.ok();
        let b_ids: Vec<Uuid> = b_hits.iter().map(|(id, _)| *id).collect();
        assert!(
            b_ids.contains(&post) && !b_ids.contains(&pre),
            "B must see only the post-cutoff atom via timestamp_window_atoms_scoped, got {b_ids:?}"
        );

        let mut tx_a = fx.scoped_tx(fx.user_a).await;
        let a_hits = store
            .timestamp_window_atoms_scoped(&mut tx_a, fx.graph_id, far_past, far_future, None, &filter)
            .await
            .expect("A timestamp_window_atoms_scoped");
        tx_a.commit().await.ok();
        let a_ids: Vec<Uuid> = a_hits.iter().map(|(id, _)| *id).collect();
        assert!(a_ids.contains(&pre) && a_ids.contains(&post), "A must see both");
    }

    // ── direct-SQL probe, no app code at all ────────────────────────────
    {
        let mut tx_b = fx.scoped_tx(fx.user_b).await;
        let rows: Vec<Uuid> = sqlx::query_scalar(
            "SELECT atom_id FROM atoms WHERE graph_id = $1",
        )
        .bind(fx.graph_id)
        .fetch_all(&mut *tx_b)
        .await
        .expect("direct SQL probe");
        tx_b.rollback().await.ok();
        assert!(
            rows.contains(&post) && !rows.contains(&pre),
            "a bare `SELECT * FROM atoms WHERE graph_id = ...` under RLS, with no app code \
             involved, must already exclude the pre-cutoff row for B — got {rows:?}"
        );
    }

    fx.teardown().await;
}

// ── Conformance test #5, continued: the node_atoms/expand path ─────────────

#[tokio::test]
async fn windowed_member_expand_and_node_keyword_search_exclude_pre_cutoff_siblings_pg() {
    let Some(fx) = Fx::setup("orgt_hf_t5_expand").await else {
        eprintln!(
            "skipping windowed_member_expand_and_node_keyword_search_exclude_pre_cutoff_siblings_pg: \
             set ORGT_TEST_BASE_URL"
        );
        return;
    };

    let pre = fx.seed_atom(-3600, "pre").await;
    let post = fx.seed_atom(3600, "post").await;

    // One reef linking BOTH atoms — the shape that proves the join-level fix,
    // not just the base `atoms` window.
    let node_id: Uuid = sqlx::query_scalar(
        "INSERT INTO nodes(graph_id, type, title, body) \
         VALUES ($1, 'entity', 'quokka expedition briefing', '{}') RETURNING node_id",
    )
    .bind(fx.graph_id)
    .fetch_one(&fx.owner)
    .await
    .expect("insert reef node");
    for atom_id in [pre, post] {
        sqlx::query("INSERT INTO node_atoms(node_id, atom_id) VALUES ($1, $2)")
            .bind(node_id)
            .bind(atom_id)
            .execute(&fx.owner)
            .await
            .expect("link node_atoms");
    }

    let store = store(&fx.app_pool);
    let filter = RetrievalFilter::default();

    // ── expand_scoped: seeded from the POST atom, must not surface `pre` ──
    {
        let mut tx_b = fx.scoped_tx(fx.user_b).await;
        let expanded = store
            .expand_scoped(&mut tx_b, fx.graph_id, &[post], Some(&probe_vector()))
            .await
            .expect("B expand_scoped");
        tx_b.commit().await.ok();
        let all_atoms: Vec<Uuid> =
            expanded.iter().flat_map(|n| n.atom_ids.iter().copied()).collect();
        assert!(
            all_atoms.contains(&post) && !all_atoms.contains(&pre),
            "B's expand_scoped from the post-cutoff seed must not surface the pre-cutoff \
             sibling atom via node_atoms — got {all_atoms:?}"
        );

        let mut tx_a = fx.scoped_tx(fx.user_a).await;
        let expanded_a = store
            .expand_scoped(&mut tx_a, fx.graph_id, &[post], Some(&probe_vector()))
            .await
            .expect("A expand_scoped");
        tx_a.commit().await.ok();
        let all_a: Vec<Uuid> =
            expanded_a.iter().flat_map(|n| n.atom_ids.iter().copied()).collect();
        assert!(
            all_a.contains(&post) && all_a.contains(&pre),
            "A (full history) must see both atoms via expand_scoped — got {all_a:?}"
        );
    }

    // ── keyword_search_node_atoms_scoped ────────────────────────────────
    {
        let mut tx_b = fx.scoped_tx(fx.user_b).await;
        let hits = store
            .keyword_search_node_atoms_scoped(&mut tx_b, fx.graph_id, "Quokka Briefing", 10, &filter)
            .await
            .expect("B keyword_search_node_atoms_scoped");
        tx_b.commit().await.ok();
        let ids: Vec<Uuid> = hits.iter().map(|(id, _)| *id).collect();
        assert!(
            ids.contains(&post) && !ids.contains(&pre),
            "B's keyword_search_node_atoms_scoped must surface the post-cutoff atom (positive \
             control — an empty result would vacuously pass the exclusion check) and never the \
             pre-cutoff sibling, got {ids:?}"
        );

        let mut tx_a = fx.scoped_tx(fx.user_a).await;
        let hits_a = store
            .keyword_search_node_atoms_scoped(&mut tx_a, fx.graph_id, "Quokka Briefing", 10, &filter)
            .await
            .expect("A keyword_search_node_atoms_scoped");
        tx_a.commit().await.ok();
        let ids_a: Vec<Uuid> = hits_a.iter().map(|(id, _)| *id).collect();
        assert!(
            ids_a.contains(&pre) && ids_a.contains(&post),
            "A must see both atoms via keyword_search_node_atoms_scoped, got {ids_a:?}"
        );
    }

    fx.teardown().await;
}

// ── Conformance test #6: no silent truncation ───────────────────────────────

/// Seeds MORE post-cutoff atoms than the requested LIMIT, plus several
/// pre-cutoff atoms all sharing the IDENTICAL embedding (a deliberate tie —
/// see this file's module doc). If the window were an app-level post-filter
/// over an unwindowed top-K candidate set, a tied ORDER BY could return any
/// mix of pre/post rows within the LIMIT, so the windowed member would
/// non-deterministically get FEWER than K post-cutoff results. Because RLS
/// filters BEFORE the LIMIT is applied, the windowed member gets a FULL page
/// every time, deterministically.
#[tokio::test]
async fn windowed_member_still_gets_a_full_page_not_a_silently_truncated_one_pg() {
    let Some(fx) = Fx::setup("orgt_hf_t6_no_trunc").await else {
        eprintln!(
            "skipping windowed_member_still_gets_a_full_page_not_a_silently_truncated_one_pg: \
             set ORGT_TEST_BASE_URL"
        );
        return;
    };

    const LIMIT: usize = 5;
    let mut post_ids = Vec::new();
    for i in 0..LIMIT {
        post_ids.push(fx.seed_atom(3600 + i as i64, "post").await);
    }
    // More pre-cutoff atoms than post, so an unwindowed top-K draw would be
    // likely (not just possible) to include several of them under a tie.
    for i in 0..(LIMIT * 3) {
        fx.seed_atom(-3600 - i as i64, "pre").await;
    }

    let store = store(&fx.app_pool);
    let filter = RetrievalFilter::default();

    let mut tx_b = fx.scoped_tx(fx.user_b).await;
    let hits = store
        .vector_search_scoped(&mut tx_b, fx.graph_id, &probe_vector(), LIMIT, &filter)
        .await
        .expect("B vector_search_scoped");
    tx_b.commit().await.ok();

    assert_eq!(
        hits.len(),
        LIMIT,
        "a windowed member requesting {LIMIT} results, with at least {LIMIT} genuinely \
         in-window candidates available, must get a FULL page — got {} (RLS must be applied \
         INSIDE the query, not as a post-hoc filter over a capped, tie-broken candidate set)",
        hits.len()
    );
    for (id, _) in &hits {
        assert!(
            post_ids.contains(id),
            "every returned row must be a genuinely post-cutoff atom, got {id} which is not in \
             {post_ids:?}"
        );
    }

    fx.teardown().await;
}

// ── Multi-grant resolution (tenant_rls_visible_graph_windows) ──────────────

async fn history_floor_for(pool: &PgPool, account_id: Uuid, user_id: Uuid, graph_id: Uuid) -> Option<Option<DateTime<Utc>>> {
    let mut tx = begin_tenant_scoped(pool, account_id, user_id)
        .await
        .expect("begin_tenant_scoped");
    let row: Option<(Option<DateTime<Utc>>,)> = sqlx::query_as(
        "SELECT history_floor FROM tenant_rls_visible_graph_windows() WHERE graph_id = $1",
    )
    .bind(graph_id)
    .fetch_optional(&mut *tx)
    .await
    .expect("query tenant_rls_visible_graph_windows");
    tx.rollback().await.ok();
    row.map(|(floor,)| floor)
}

#[tokio::test]
async fn multi_grant_resolution_matches_the_documented_rules_pg() {
    let Some(fx) = Fx::setup("orgt_hf_multigrant").await else {
        eprintln!("skipping multi_grant_resolution_matches_the_documented_rules_pg: set ORGT_TEST_BASE_URL");
        return;
    };

    // Baseline from Fx::setup: A has one NULL grant (full history), B has one
    // timestamped grant (windowed at cutoff).
    assert_eq!(
        history_floor_for(&fx.app_pool, fx.account_id, fx.user_a, fx.graph_id).await,
        Some(None),
        "A (one NULL grant) must resolve to full history (no floor)"
    );
    assert_eq!(
        history_floor_for(&fx.app_pool, fx.account_id, fx.user_b, fx.graph_id).await,
        Some(Some(fx.cutoff)),
        "B (one timestamped grant) must resolve to that timestamp as the floor"
    );

    // Give B a SECOND grant with NULL — a NULL grant absorbs, full history.
    sqlx::query(
        "INSERT INTO team_seat_grants (account_id, team_id, user_id, source, history_from) \
         VALUES ($1, $2, $3, 'test:absorbing', NULL)",
    )
    .bind(fx.account_id)
    .bind(fx.team_id)
    .bind(fx.user_b)
    .execute(&fx.owner)
    .await
    .expect("add B's absorbing NULL grant");
    assert_eq!(
        history_floor_for(&fx.app_pool, fx.account_id, fx.user_b, fx.graph_id).await,
        Some(None),
        "B, now holding a NULL grant alongside the timestamped one, must resolve to full \
         history — a NULL grant absorbs"
    );

    // A third user with TWO timestamped grants (no NULL): MIN of the two.
    let user_c = Uuid::new_v4();
    sqlx::query("INSERT INTO team_members (team_id, user_id, role) VALUES ($1, $2, 'member')")
        .bind(fx.team_id)
        .bind(user_c)
        .execute(&fx.owner)
        .await
        .expect("seat C");
    let earlier = fx.cutoff - Duration::days(10);
    let later = fx.cutoff + Duration::days(10);
    sqlx::query(
        "INSERT INTO team_seat_grants (account_id, team_id, user_id, source, history_from) \
         VALUES ($1, $2, $3, 'test:c-later', $4), ($1, $2, $3, 'test:c-earlier', $5)",
    )
    .bind(fx.account_id)
    .bind(fx.team_id)
    .bind(user_c)
    .bind(later)
    .bind(earlier)
    .execute(&fx.owner)
    .await
    .expect("seat C's two timestamped grants");
    assert_eq!(
        history_floor_for(&fx.app_pool, fx.account_id, user_c, fx.graph_id).await,
        Some(Some(earlier)),
        "C, holding two timestamped grants and no NULL grant, must resolve to the EARLIER \
         (MIN) of the two"
    );

    // A fourth user: a `team_members` row but ZERO grants — legacy,
    // backward-compatible full history.
    let user_d = Uuid::new_v4();
    sqlx::query("INSERT INTO team_members (team_id, user_id, role) VALUES ($1, $2, 'member')")
        .bind(fx.team_id)
        .bind(user_d)
        .execute(&fx.owner)
        .await
        .expect("seat D with zero grants");
    assert_eq!(
        history_floor_for(&fx.app_pool, fx.account_id, user_d, fx.graph_id).await,
        Some(None),
        "D, a team member with ZERO team_seat_grants rows, must resolve to full history \
         (legacy/backward-compatible — pre-dates this feature)"
    );

    fx.teardown().await;
}

// ── Write-path regression: history never blocks ingest/edit ────────────────

#[tokio::test]
async fn windowed_member_can_still_insert_a_pre_window_timestamped_atom_pg() {
    let Some(fx) = Fx::setup("orgt_hf_write_regress").await else {
        eprintln!(
            "skipping windowed_member_can_still_insert_a_pre_window_timestamped_atom_pg: \
             set ORGT_TEST_BASE_URL"
        );
        return;
    };

    // B is windowed at `fx.cutoff`. B inserting an atom timestamped well
    // before that must still succeed — the write policies stay unwindowed
    // (0064's atoms INSERT/UPDATE/DELETE policies).
    let mut tx_b = fx.scoped_tx(fx.user_b).await;
    let old_ts = fx.cutoff - Duration::days(30);
    let insert = sqlx::query(
        "INSERT INTO atoms(graph_id, source_type, source_id, content_blob_key, \"timestamp\") \
         VALUES ($1, 'test', 'b-writes-pre-window', 'blob', $2)",
    )
    .bind(fx.graph_id)
    .bind(old_ts)
    .execute(&mut *tx_b)
    .await;
    assert!(
        insert.is_ok(),
        "B (windowed member) must still be able to INSERT an atom timestamped before their \
         own window — got {insert:?}"
    );
    tx_b.commit().await.ok();

    // The rest of the de-facto member-scoped write matrix (0064's header
    // documents it; CodeRabbit round 1 asked for it to be CI-locked):
    // Postgres applies SELECT policies to DML that READS rows (a WHERE
    // referencing columns, or RETURNING), so a windowed member cannot
    // target pre-window rows even though the write policies are unwindowed
    // — and an INSERT ... RETURNING of a pre-window row is refused outright.
    {
        // INSERT ... RETURNING of a pre-window atom: hard RLS refusal.
        let mut tx = fx.scoped_tx(fx.user_b).await;
        let refused = sqlx::query_scalar::<_, Uuid>(
            "INSERT INTO atoms(graph_id, source_type, source_id, content_blob_key, \"timestamp\") \
             VALUES ($1, 'test', 'b-returning-pre-window', 'blob', $2) RETURNING atom_id",
        )
        .bind(fx.graph_id)
        .bind(old_ts)
        .fetch_one(&mut *tx)
        .await;
        assert!(
            refused.is_err(),
            "INSERT ... RETURNING of a pre-window atom under member scope must be refused \
             (RETURNING requires the new row to pass the windowed SELECT policy) — got \
             {refused:?}"
        );
        tx.rollback().await.ok();

        // UPDATE/DELETE of the pre-window row B inserted above: 0 rows —
        // the WHERE read is filtered by the SELECT policy. Fail-closed and
        // consistent with reads: B cannot touch what B cannot see.
        let mut tx = fx.scoped_tx(fx.user_b).await;
        let updated = sqlx::query(
            "UPDATE atoms SET content_blob_key = 'blob-2' WHERE source_id = 'b-writes-pre-window'",
        )
        .execute(&mut *tx)
        .await
        .expect("UPDATE itself must not error");
        assert_eq!(updated.rows_affected(), 0, "member UPDATE of a pre-window row affects 0 rows");
        let deleted = sqlx::query("DELETE FROM atoms WHERE source_id = 'b-writes-pre-window'")
            .execute(&mut *tx)
            .await
            .expect("DELETE itself must not error");
        assert_eq!(deleted.rows_affected(), 0, "member DELETE of a pre-window row affects 0 rows");
        tx.rollback().await.ok();

        // The same operations on an IN-window row: 1 row each — the write
        // policies are live, only the read-composition windows them.
        let in_window = fx.seed_atom(7200, "write-matrix").await;
        let mut tx = fx.scoped_tx(fx.user_b).await;
        let updated = sqlx::query("UPDATE atoms SET content_blob_key = 'blob-3' WHERE atom_id = $1")
            .bind(in_window)
            .execute(&mut *tx)
            .await
            .expect("in-window UPDATE");
        assert_eq!(updated.rows_affected(), 1, "member UPDATE of an in-window row affects 1 row");
        let deleted = sqlx::query("DELETE FROM atoms WHERE atom_id = $1")
            .bind(in_window)
            .execute(&mut *tx)
            .await
            .expect("in-window DELETE");
        assert_eq!(deleted.rows_affected(), 1, "member DELETE of an in-window row affects 1 row");
        tx.rollback().await.ok();

        // System scope: pre-window UPDATE and DELETE both bite — the floor
        // is NULL for a system actor, so nothing is hidden from workers.
        let mut tx_sys = coralswarm_backend::tenancy::scope::begin_tenant_system_scoped(
            &fx.app_pool,
            fx.account_id,
        )
        .await
        .expect("begin_tenant_system_scoped");
        let updated = sqlx::query(
            "UPDATE atoms SET content_blob_key = 'blob-4' WHERE source_id = 'b-writes-pre-window'",
        )
        .execute(&mut *tx_sys)
        .await
        .expect("system UPDATE");
        assert_eq!(updated.rows_affected(), 1, "system UPDATE of a pre-window row affects 1 row");
        let deleted = sqlx::query("DELETE FROM atoms WHERE source_id = 'b-writes-pre-window'")
            .execute(&mut *tx_sys)
            .await
            .expect("system DELETE");
        assert_eq!(deleted.rows_affected(), 1, "system DELETE of a pre-window row affects 1 row");
        tx_sys.rollback().await.ok();
    }

    // And a system-scoped ingest (the shape real backfill/ingest workers
    // use) of an atom timestamped before EVERY member's window must also
    // succeed.
    let mut tx_sys = coralswarm_backend::tenancy::scope::begin_tenant_system_scoped(
        &fx.app_pool,
        fx.account_id,
    )
    .await
    .expect("begin_tenant_system_scoped");
    let sys_insert = sqlx::query(
        "INSERT INTO atoms(graph_id, source_type, source_id, content_blob_key, \"timestamp\") \
         VALUES ($1, 'test', 'system-writes-pre-window', 'blob', $2)",
    )
    .bind(fx.graph_id)
    .bind(old_ts)
    .execute(&mut *tx_sys)
    .await;
    assert!(
        sys_insert.is_ok(),
        "a system-scoped ingest of an atom timestamped before every member's window must \
         succeed — got {sys_insert:?}"
    );
    tx_sys.commit().await.ok();

    assert_eq!(fx.seat_grant_count().await, 2, "no grants were disturbed by this test");

    fx.teardown().await;
}

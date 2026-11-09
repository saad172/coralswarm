//! Reusable two-account RLS probe harness (org-tenancy Phase P.1).
//!
//! Every route/worker converted to `tenancy::scope::begin_account_scoped`
//! needs the SAME three proofs against the real `coralswarm_app` role
//! (migration `0083_consumer_rls`):
//!
//!   1. scoped to account A, a query returns exactly A's rows;
//!   2. scoped to account A, a query aimed at B's ids returns NOTHING;
//!   3. with `app.account_id` never set, the same query HARD-ERRORS (the
//!      strict `current_setting` predicate — never a quiet empty result).
//!
//! Rather than each conversion PR re-implementing the two-account fixture
//! (scratch DB, control-tree migrations, real `coralswarm_app` login, seeded
//! rows) that `consumer_rls_pg.rs` pioneered, this harness packages it once:
//! `RlsProbe::setup()` builds the world, the `assert_*` helpers take a
//! caller-supplied query (single-`uuid`-column SELECT, positional `$1..$n`
//! uuid binds) so each test file probes its OWN converted query shapes, and
//! `teardown()` drops the scratch DB.
//!
//! Env-gated on `ORGT_TEST_BASE_URL` (a base Postgres connection with CREATE
//! DATABASE + CREATE/ALTER ROLE rights), the same convention as every other
//! `*_pg.rs` test here:
//!
//!   ORGT_TEST_BASE_URL=postgres://alex@localhost:5432/postgres \
//!     cargo test -p coralswarm-backend --test rls_probe_pg
//!
//! Usage from a test file (see `rls_probe_pg.rs` for the full example):
//!
//! ```ignore
//! mod support;
//! use support::rls_probe::RlsProbe;
//!
//! #[tokio::test]
//! async fn my_converted_query_is_account_scoped() {
//!     let Some(probe) = RlsProbe::setup("orgt_rls_probe_myroute").await else {
//!         eprintln!("skipping: set ORGT_TEST_BASE_URL");
//!         return;
//!     };
//!     probe
//!         .assert_scoped_returns_own_rows(
//!             probe.a.account_id,
//!             "SELECT id FROM graphs",
//!             &[],
//!             &[probe.a.graph_id],
//!         )
//!         .await;
//!     probe
//!         .assert_cross_account_returns_nothing(
//!             probe.a.account_id,
//!             "SELECT atom_id FROM atoms WHERE graph_id = $1",
//!             &[probe.b.graph_id],
//!         )
//!         .await;
//!     probe
//!         .assert_unscoped_hard_errors("SELECT id FROM graphs", &[])
//!         .await;
//!     probe.teardown().await;
//! }
//! ```

use std::str::FromStr;

use coralswarm_backend::tenancy::scope::begin_account_scoped;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::{Connection, Executor, PgConnection, PgPool, Postgres, Transaction};
use uuid::Uuid;

/// A password set ONLY on the local/CI test cluster for the duration of a
/// test — `coralswarm_app` is created with NO password by migration 0083
/// itself (see its header); this is not a real credential and is never used
/// against a real deploy. Same idiom as `consumer_rls_pg.rs` /
/// `bootstrap_scope_pg.rs`.
const APP_ROLE_TEST_PASSWORD: &str = "orgt_rls_probe_test_only_pw";

/// Advisory-lock key serializing the control-tree application (and the
/// ALTER ROLE password step) across concurrently-running tests. Migration
/// 0083 creates the CLUSTER-WIDE `coralswarm_app` role, and its
/// concurrent-shared-catalog-DDL retry guard deliberately catches ONLY
/// SQLSTATE `XX000` — two tests applying the tree at once on a fresh cluster
/// (exactly what a per-job CI Postgres is) can still race `CREATE ROLE` into
/// a `23505 unique_violation` that escapes the guard (observed on CI). The
/// merged migration is append-only, so the harness serializes instead: the
/// session-scoped lock is taken on the admin connection, held across
/// migrate + ALTER ROLE, and released right after (or implicitly on
/// connection drop if setup panics). Arbitrary but stable value — shared by
/// every consumer of this harness, which is the point.
const SETUP_ADVISORY_LOCK_KEY: i64 = 0x0083_5C09;

/// One account's worth of seeded fixture rows, created via the owner
/// (BYPASSRLS) pool so seeding itself is never subject to the policies under
/// test. Covers both policy shapes in 0083: direct-`account_id` tables
/// (`graphs`, `queries`, `activity_events`) and graph-mediated tables
/// (`atoms`, `nodes` — scoped through their `graph_id`'s owning graph).
pub struct AccountFixture {
    pub user_id: Uuid,
    pub account_id: Uuid,
    pub graph_id: Uuid,
    pub atom_id: Uuid,
    pub node_id: Uuid,
    pub query_id: Uuid,
}

/// The assembled probe: a scratch DB with the full control migration tree
/// applied (so `coralswarm_app` + every 0083 policy is real), two seeded
/// accounts (`a` and `b`), an owner/BYPASSRLS pool for arbitrary extra
/// seeding, and an `app_pool` logged in AS `coralswarm_app` — the exact
/// connection path OPS-4 will point the app at.
pub struct RlsProbe {
    /// Dropped with the probe, so a panicking test leaves no database behind.
    db_name: coralswarm_backend::test_db::TestDatabase,
    admin: PgConnection,
    /// Owner/BYPASSRLS pool — use this to seed any extra per-table fixture
    /// rows a specific test needs beyond the built-in [`AccountFixture`]s
    /// (seeding must bypass the very policies the test then probes).
    pub owner: PgPool,
    /// Pool authenticated AS `coralswarm_app` (a real separate login, not
    /// `SET ROLE`) — every `assert_*` helper runs its query here.
    pub app_pool: PgPool,
    pub a: AccountFixture,
    pub b: AccountFixture,
}

fn base_url() -> Option<String> {
    std::env::var("ORGT_TEST_BASE_URL").ok()
}

fn with_db(base: &str, db: &str) -> String {
    let (prefix, rest) = base.split_at(base.rfind('/').expect("url has a path /"));
    let params = rest.find('?').map(|i| &rest[i..]).unwrap_or("");
    format!("{prefix}/{db}{params}")
}

async fn seed_account(owner: &PgPool, email: &str) -> AccountFixture {
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
    let graph_id: Uuid =
        sqlx::query_scalar("INSERT INTO graphs(account_id, name) VALUES ($1, 'home') RETURNING id")
            .bind(account_id)
            .fetch_one(owner)
            .await
            .expect("insert graph");
    // `content_inline` seeded ALONGSIDE the placeholder `content_blob_key`
    // — the real shape an R2-less server's ingest produces (`routes::
    // ingest`'s dev-mode branch always sets `content_inline` when `state.r2`
    // is `None`, on top of the `blob_key_for`-computed key it stamps
    // unconditionally at insert time). Every consumer of this fixture runs
    // with no R2 configured (`test_app_state()`'s default), so
    // `BlobCopyMode::NoR2` is what `promote_source`/`promote_session`
    // exercise against this atom — see `store::BlobCopyMode::NoR2`'s own
    // doc comment for why a row with no `content_inline` fails that path
    // closed instead of landing an unreadable copy.
    let atom_id: Uuid = sqlx::query_scalar(
        "INSERT INTO atoms(graph_id, source_type, source_id, content_blob_key, content_inline, \
                            timestamp) \
         VALUES ($1, 'test', 'probe-1', 'blob-key', 'probe atom content', now()) \
         RETURNING atom_id",
    )
    .bind(graph_id)
    .fetch_one(owner)
    .await
    .expect("insert atom");
    let node_id: Uuid = sqlx::query_scalar(
        "INSERT INTO nodes(graph_id, type, title, body) \
         VALUES ($1, 'entity', 'Probe Node', '{}') RETURNING node_id",
    )
    .bind(graph_id)
    .fetch_one(owner)
    .await
    .expect("insert node");
    let query_id: Uuid = sqlx::query_scalar(
        "INSERT INTO queries(account_id, user_id, text) VALUES ($1, $2, 'probe') RETURNING id",
    )
    .bind(account_id)
    .bind(user_id)
    .fetch_one(owner)
    .await
    .expect("insert query");
    sqlx::query("INSERT INTO activity_events(account_id, event_type) VALUES ($1, 'atom_created')")
        .bind(account_id)
        .execute(owner)
        .await
        .expect("insert activity event");
    AccountFixture {
        user_id,
        account_id,
        graph_id,
        atom_id,
        node_id,
        query_id,
    }
}

impl RlsProbe {
    /// Builds the whole probe world in a scratch database named `db_name`
    /// (each test file must pick a UNIQUE name — scratch DBs are per-file so
    /// parallel test binaries never collide).
    ///
    /// Returns `None` when `ORGT_TEST_BASE_URL` is unset — the caller prints
    /// its skip line and returns, matching every other env-gated `*_pg.rs`
    /// test in this directory.
    pub async fn setup(db_name: &str) -> Option<Self> {
        let base = base_url()?;
        assert!(
            db_name
                .chars()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_'),
            "scratch db name {db_name:?} must be a plain lowercase identifier \
             (it is interpolated into CREATE/DROP DATABASE)"
        );

        let guard = coralswarm_backend::test_db::TestDatabase::adopt(
            &PgConnectOptions::from_str(&base).expect("ORGT_TEST_BASE_URL"),
            db_name,
        );
        let mut admin = PgConnection::connect(&base).await.expect("connect admin");
        admin
            .execute(format!("DROP DATABASE IF EXISTS {db_name} WITH (FORCE)").as_str())
            .await
            .expect("drop stale scratch db");
        admin
            .execute(format!("CREATE DATABASE {db_name}").as_str())
            .await
            .expect("create scratch db");
        let db_url = with_db(&base, db_name);

        // Apply the full control tree — this creates `coralswarm_app` and
        // every 0083 policy, so the probe exercises the real migrations, not
        // a hand-maintained schema copy. Serialized cluster-wide via the
        // advisory lock (see SETUP_ADVISORY_LOCK_KEY) because the role
        // creation inside 0083 is shared-catalog DDL that races between
        // concurrent tests on a fresh cluster.
        sqlx::query("SELECT pg_advisory_lock($1)")
            .bind(SETUP_ADVISORY_LOCK_KEY)
            .execute(&mut admin)
            .await
            .expect("take setup advisory lock");

        let owner: PgPool = PgPoolOptions::new()
            .max_connections(4)
            .connect(&db_url)
            .await
            .expect("connect owner");
        sqlx::migrate!("./migrations_control")
            .run(&owner)
            .await
            .expect("apply control tree incl. 0083");

        // Test-only password so the harness can open a REAL separate
        // connection AS coralswarm_app rather than `SET ROLE` in the owner's
        // session — the exact connection path OPS-4 cuts over to. Inside the
        // lock too: ALTER ROLE is shared-catalog DDL as well (harmlessly
        // idempotent here since every harness consumer sets the same value,
        // but no reason to let it race).
        sqlx::query(&format!(
            "ALTER ROLE coralswarm_app WITH PASSWORD '{APP_ROLE_TEST_PASSWORD}'"
        ))
        .execute(&owner)
        .await
        .expect("set test-only password on coralswarm_app");

        sqlx::query("SELECT pg_advisory_unlock($1)")
            .bind(SETUP_ADVISORY_LOCK_KEY)
            .execute(&mut admin)
            .await
            .expect("release setup advisory lock");

        let a = seed_account(&owner, &format!("{db_name}-a@x.co")).await;
        let b = seed_account(&owner, &format!("{db_name}-b@x.co")).await;

        let opts = PgConnectOptions::from_str(&db_url)
            .expect("parse scratch db url")
            .username("coralswarm_app")
            .password(APP_ROLE_TEST_PASSWORD);
        let app_pool = PgPoolOptions::new()
            .max_connections(2)
            .connect_with(opts)
            .await
            .expect("connect AS coralswarm_app");

        Some(Self {
            db_name: guard,
            admin,
            owner,
            app_pool,
            a,
            b,
        })
    }

    /// A transaction on the `coralswarm_app` pool scoped to `account_id` via
    /// the PRODUCTION primitive (`tenancy::scope::begin_account_scoped`) —
    /// tests that need query shapes beyond the three `assert_*` helpers
    /// (multi-column selects, writes) build on this directly.
    pub async fn scoped_tx(&self, account_id: Uuid) -> Transaction<'static, Postgres> {
        begin_account_scoped(&self.app_pool, account_id)
            .await
            .expect("begin_account_scoped on the coralswarm_app pool")
    }

    async fn fetch_scoped(&self, account_id: Uuid, query: &str, binds: &[Uuid]) -> Vec<Uuid> {
        let mut tx = self.scoped_tx(account_id).await;
        let mut q = sqlx::query_scalar::<Postgres, Uuid>(query);
        for b in binds {
            q = q.bind(*b);
        }
        let rows = q
            .fetch_all(&mut *tx)
            .await
            .unwrap_or_else(|e| panic!("scoped query {query:?} failed: {e}"));
        tx.rollback().await.ok();
        rows
    }

    /// Scoped to `account_id`, `query` (a SELECT of a single `uuid` column,
    /// positional uuid `binds`) must return exactly `expected` — order
    /// ignored, duplicates significant.
    pub async fn assert_scoped_returns_own_rows(
        &self,
        account_id: Uuid,
        query: &str,
        binds: &[Uuid],
        expected: &[Uuid],
    ) {
        let mut rows = self.fetch_scoped(account_id, query, binds).await;
        rows.sort();
        let mut want = expected.to_vec();
        want.sort();
        assert_eq!(
            rows, want,
            "scoped to account {account_id}, {query:?} (binds {binds:?}) must return exactly \
             this account's own rows"
        );
    }

    /// Scoped to `account_id`, `query` — typically aimed at the OTHER
    /// account's ids via `binds` — must return ZERO rows (the policy filters
    /// them; never an error, never a leak).
    pub async fn assert_cross_account_returns_nothing(
        &self,
        account_id: Uuid,
        query: &str,
        binds: &[Uuid],
    ) {
        let rows = self.fetch_scoped(account_id, query, binds).await;
        assert!(
            rows.is_empty(),
            "scoped to account {account_id}, {query:?} (binds {binds:?}) aimed at another \
             account's rows must return ZERO rows — got {rows:?}"
        );
    }

    /// With `app.account_id` never set on the transaction, `query` must HARD
    /// ERROR — the strict `current_setting('app.account_id')::uuid` predicate
    /// (no `missing_ok` default) fails closed rather than returning an empty
    /// (or worse, unfiltered) result. This is what catches a route/worker
    /// that still runs an RLS-table query outside `begin_account_scoped`
    /// after the OPS-4 cutover.
    pub async fn assert_unscoped_hard_errors(&self, query: &str, binds: &[Uuid]) {
        let mut tx = self
            .app_pool
            .begin()
            .await
            .expect("begin unscoped tx as app role");
        let mut q = sqlx::query_scalar::<Postgres, Uuid>(query);
        for b in binds {
            q = q.bind(*b);
        }
        let result: Result<Vec<Uuid>, sqlx::Error> = q.fetch_all(&mut *tx).await;
        assert!(
            result.is_err(),
            "{query:?} (binds {binds:?}) run as coralswarm_app WITHOUT app.account_id set must \
             hard-error (strict current_setting predicate) — got {result:?}"
        );
        tx.rollback().await.ok();
    }

    /// Closes both pools and drops the scratch database. Call at the end of
    /// each test; a panicked test drops it when the probe is dropped.
    pub async fn teardown(mut self) {
        self.app_pool.close().await;
        self.owner.close().await;
        self.admin
            .execute(format!("DROP DATABASE IF EXISTS {} WITH (FORCE)", self.db_name).as_str())
            .await
            .ok();
    }
}

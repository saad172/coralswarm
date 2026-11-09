//! Regression test for the RLS-scoping gap in
//! `services::retraction::unpublish_atom`,
//! `services::retraction::revoke_source_publication`, and
//! `services::deletion::delete_atom_cascade` — all three used to open a bare
//! `pool.begin()` transaction against an RLS-covered table (`atoms`/`nodes`/
//! `node_atoms` for the first and third, `source_publications` for the
//! second — the latter RLS-covered since migration `0111`, not `0083`, since
//! the table didn't exist yet when `0083` shipped) instead of scoping
//! through `tenancy::scope::begin_account_scoped`.
//!
//! **Why this had to be proven under a real `coralswarm_app` login, not the
//! owner pool.** Every ordinary pg test in this repo runs as the database
//! OWNER, which has `BYPASSRLS` and is therefore exempt from an ENABLE-only
//! RLS policy — a test written the normal way passes whether or not the
//! scoping bug exists, and proves nothing. This suite follows
//! `consumer_rls_pg.rs`'s own pattern: a real, separate login AS
//! `coralswarm_app` (not `SET ROLE` inside the owner's session), which is
//! exactly the connection path production points the app at today —
//! verified against production: `DATABASE_URL`'s user IS `coralswarm_app`,
//! `rolbypassrls = f`.
//!
//! **What "broken" means here, precisely.** Migration `0083`'s
//! `account_isolation` policy on `atoms` uses the single-argument, STRICT
//! form of `current_setting` (`current_setting('app.account_id')::uuid` —
//! see `migrations_control/0083_consumer_rls.sql`'s own `CREATE POLICY` for
//! `atoms`, no `missing_ok`/second argument anywhere on this table's policy).
//! That means an unscoped `coralswarm_app` connection does not silently see
//! zero rows — it gets a hard Postgres error the first time it touches
//! `atoms` at all (`unrecognized configuration parameter "app.account_id"`,
//! or `invalid input syntax for type uuid: ""` if the GUC was set-then-reset
//! earlier in the same session — see 0083's header for why both are the same
//! fail-loud property). So the ORIGINAL bug was a hard 500/503, not a
//! quieter "already gone" false-negative — this suite's negative-control run
//! (see the module-level note below) confirmed exactly that failure shape.
//!
//! ORGT_TEST_BASE_URL=postgres://alex@localhost:5432/postgres \
//!   cargo test -p coralswarm-backend --test retraction_deletion_rls_pg

use std::str::FromStr;

use coralswarm_backend::services::{deletion, retraction};
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::{Connection, Executor, PgConnection, PgPool};
use uuid::Uuid;

const APP_ROLE_TEST_PASSWORD: &str = "orgt_retraction_deletion_rls_test_only_pw";
/// Distinct from `consumer_rls_pg.rs`'s own lock key — same cluster-wide
/// `coralswarm_app` role/password race (shared-catalog DDL, concurrent test
/// binaries), different key so the two suites' setup phases never contend
/// with each other unnecessarily.
const SETUP_ADVISORY_LOCK_KEY: i64 = 0x0083_5C10;

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

/// A pool connected AS `coralswarm_app` (real login, not `SET ROLE`) —
/// mirrors `consumer_rls_pg.rs`'s helper of the same name exactly.
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

async fn scratch_db_with_app_role(base: &str, db: &str) -> PgPool {
    let mut admin_conn = admin(base).await;
    recreate(&mut admin_conn, db).await;
    let db_url = with_db(base, db);

    sqlx::query("SELECT pg_advisory_lock($1)")
        .bind(SETUP_ADVISORY_LOCK_KEY)
        .execute(&mut admin_conn)
        .await
        .expect("take setup advisory lock");

    let owner: PgPool = PgPoolOptions::new()
        .connect(&db_url)
        .await
        .expect("connect owner");
    sqlx::migrate!("./migrations_control")
        .run(&owner)
        .await
        .expect("apply control tree incl. 0083/0084/0111");
    sqlx::query(&format!(
        "ALTER ROLE coralswarm_app WITH PASSWORD '{APP_ROLE_TEST_PASSWORD}'"
    ))
    .execute(&owner)
    .await
    .expect("set test-only password on coralswarm_app");

    sqlx::query("SELECT pg_advisory_unlock($1)")
        .bind(SETUP_ADVISORY_LOCK_KEY)
        .execute(&mut admin_conn)
        .await
        .expect("release setup advisory lock");

    owner
}

/// Seed one account/graph/atom, plus (for the unpublish case) a promoted
/// reef linking that atom, owned by `promoter_user_id` — the exact shape
/// `services::retraction::recorded_atom_promoters` reads
/// (`node_atoms`/`nodes.owner_id`/`nodes.promoted_from_node_id IS NOT NULL`).
struct Fixture {
    account_id: Uuid,
    graph_id: Uuid,
    atom_id: Uuid,
    node_id: Uuid,
    promoter_user_id: Uuid,
}

async fn seed(owner: &PgPool) -> Fixture {
    let promoter_user_id: Uuid =
        sqlx::query_scalar("INSERT INTO users(email) VALUES ($1) RETURNING id")
            .bind(format!("promoter-{}@x.co", Uuid::new_v4()))
            .fetch_one(owner)
            .await
            .expect("insert user");
    let account_id: Uuid =
        sqlx::query_scalar("INSERT INTO accounts(owner_user_id) VALUES ($1) RETURNING id")
            .bind(promoter_user_id)
            .fetch_one(owner)
            .await
            .expect("insert account");
    let graph_id: Uuid = sqlx::query_scalar(
        "INSERT INTO graphs(account_id, name) VALUES ($1, 'target-ocean') RETURNING id",
    )
    .bind(account_id)
    .fetch_one(owner)
    .await
    .expect("insert graph");
    let atom_id: Uuid = sqlx::query_scalar(
        "INSERT INTO atoms(graph_id, source_type, source_id, content_blob_key, timestamp) \
         VALUES ($1, 'test', $2, 'blob-key', now()) RETURNING atom_id",
    )
    .bind(graph_id)
    .bind(format!("probe-{}", Uuid::new_v4()))
    .fetch_one(owner)
    .await
    .expect("insert atom");
    // A promoted-copy reef (§4.5): promoted_from_node_id IS NOT NULL, owned by
    // promoter_user_id — the row `unpublish_atom`'s promoter check reads.
    let fake_source_node_id = Uuid::new_v4();
    let node_id: Uuid = sqlx::query_scalar(
        "INSERT INTO nodes(graph_id, type, title, body, owner_id, promoted_from_node_id) \
         VALUES ($1, 'entity', 'Promoted Reef', '{}', $2, $3) RETURNING node_id",
    )
    .bind(graph_id)
    .bind(promoter_user_id)
    .bind(fake_source_node_id)
    .fetch_one(owner)
    .await
    .expect("insert promoted reef node");
    sqlx::query("INSERT INTO node_atoms(node_id, atom_id) VALUES ($1, $2)")
        .bind(node_id)
        .bind(atom_id)
        .execute(owner)
        .await
        .expect("link node_atoms");

    Fixture {
        account_id,
        graph_id,
        atom_id,
        node_id,
        promoter_user_id,
    }
}

/// Seed for `revoke_source_publication`: ONE account with TWO graphs (source
/// and target), plus a live (unrevoked) `source_publications` row consented
/// to by `publisher_user_id`.
///
/// Deliberately SAME account for both graphs — not an arbitrary simplification.
/// Migration `0111`'s `account_isolation_insert`/`_update` policies on
/// `source_publications` require `app.account_id` to equal BOTH
/// `source_graph_id`'s AND `target_graph_id`'s owning account simultaneously
/// (a strict AND, not the SELECT policy's permissive OR) — the identical
/// shape 0083's own header describes for `node_promotions` ("every legitimate
/// consumer-plane promotion is same-account"). A cross-account pair would be
/// unrevoke-able through this RLS-scoped path by construction (no single
/// `app.account_id` can satisfy an AND across two different accounts) — that
/// is a property of the migration 0111 policy itself, not something this fix
/// introduces or could paper over, so this fixture matches the only shape
/// migration 0111 actually permits a write against.
struct PublicationFixture {
    source_graph_id: Uuid,
    target_graph_id: Uuid,
    source_id: String,
    publisher_user_id: Uuid,
}

async fn seed_publication(owner: &PgPool) -> PublicationFixture {
    let publisher_user_id: Uuid =
        sqlx::query_scalar("INSERT INTO users(email) VALUES ($1) RETURNING id")
            .bind(format!("publisher-{}@x.co", Uuid::new_v4()))
            .fetch_one(owner)
            .await
            .expect("insert user");
    let account_id: Uuid =
        sqlx::query_scalar("INSERT INTO accounts(owner_user_id) VALUES ($1) RETURNING id")
            .bind(publisher_user_id)
            .fetch_one(owner)
            .await
            .expect("insert account");
    let source_graph_id: Uuid = sqlx::query_scalar(
        "INSERT INTO graphs(account_id, name) VALUES ($1, 'source-ocean') RETURNING id",
    )
    .bind(account_id)
    .fetch_one(owner)
    .await
    .expect("insert source graph");
    let target_graph_id: Uuid = sqlx::query_scalar(
        "INSERT INTO graphs(account_id, name) VALUES ($1, 'target-ocean') RETURNING id",
    )
    .bind(account_id)
    .fetch_one(owner)
    .await
    .expect("insert target graph");
    let source_id = format!("slack:{}", Uuid::new_v4());
    sqlx::query(
        "INSERT INTO source_publications (source_id, source_graph_id, target_graph_id, consented_by) \
         VALUES ($1, $2, $3, $4)",
    )
    .bind(&source_id)
    .bind(source_graph_id)
    .bind(target_graph_id)
    .bind(publisher_user_id)
    .execute(owner)
    .await
    .expect("seed a live source_publications row");

    PublicationFixture {
        source_graph_id,
        target_graph_id,
        source_id,
        publisher_user_id,
    }
}

async fn publication_revoked_at(
    owner: &PgPool,
    source_id: &str,
    source_graph_id: Uuid,
    target_graph_id: Uuid,
) -> Option<chrono::DateTime<chrono::Utc>> {
    sqlx::query_scalar(
        "SELECT revoked_at FROM source_publications \
         WHERE source_id = $1 AND source_graph_id = $2 AND target_graph_id = $3",
    )
    .bind(source_id)
    .bind(source_graph_id)
    .bind(target_graph_id)
    .fetch_one(owner)
    .await
    .expect("read revoked_at as owner")
}

async fn atom_row_exists(owner: &PgPool, atom_id: Uuid) -> bool {
    sqlx::query_scalar::<_, bool>("SELECT EXISTS(SELECT 1 FROM atoms WHERE atom_id = $1)")
        .bind(atom_id)
        .fetch_one(owner)
        .await
        .expect("check atom existence as owner")
}

/// **The reproduction for `services::retraction::unpublish_atom`.**
///
/// Calls the REAL production function directly (not through the HTTP route —
/// the route's own reachability check is a separate, already-passing concern;
/// this isolates exactly the scoping bug) against a pool authenticated AS
/// `coralswarm_app`.
///
/// Negative control performed by hand for this PR (not committed, per the
/// task's instructions — the fix itself is the committed artifact): reverting
/// `unpublish_atom` to its pre-fix `let mut tx = pool.begin().await?;` (no
/// `resolve_graph_account_id`/`begin_account_scoped`) made this test FAIL
/// with exactly:
///
///   called `Result::unwrap()` on an `Err` value: Db(Database(PgDatabaseError {
///   severity: Error, code: "42704", message: "unrecognized configuration
///   parameter \"app.account_id\"", ... }))
///
/// (or, on a connection that had `app.account_id` set-then-reset by an
/// EARLIER test in the same pooled connection, `22P02 invalid input syntax
/// for type uuid: ""` instead — both are the same fail-loud property; see
/// `migrations_control/0083_consumer_rls.sql`'s header). Restoring the fix
/// made it pass again unchanged. This is the exact production failure the
/// live `DELETE /v1/graphs/{graph_id}/atoms/{atom_id}` route hit.
#[tokio::test]
async fn unpublish_atom_succeeds_under_app_role_pool() {
    let Some(base) = base_url() else {
        eprintln!("skipping unpublish_atom_succeeds_under_app_role_pool: set ORGT_TEST_BASE_URL");
        return;
    };
    let db = "orgt_retraction_unpublish";
    let _cleanup_db = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
    let db_url = with_db(&base, db);
    let owner = scratch_db_with_app_role(&base, db).await;
    let fx = seed(&owner).await;

    // A REAL separate login AS coralswarm_app — not `SET ROLE` in the owner's
    // session. Run this against `owner` instead and the test would pass
    // vacuously (BYPASSRLS is exempt from ENABLE-only RLS), proving nothing —
    // see this file's module doc.
    let app_pool = app_role_pool(&db_url).await;

    let outcome = retraction::unpublish_atom(
        &app_pool,
        None, // no R2 configured — content lives in content_inline for this fixture
        "test-device",
        fx.graph_id,
        fx.atom_id,
        fx.promoter_user_id,
    )
    .await
    .expect(
        "unpublish_atom must succeed under a real coralswarm_app login — a bare `pool.begin()` \
         with no `app.account_id` scoping raises here (0083's strict current_setting), which is \
         exactly the production bug this test regression-guards",
    );

    assert!(
        outcome.atom_was_present,
        "the seeded atom existed, so unpublish_atom must report atom_was_present=true, not the \
         idempotent 'already gone' outcome"
    );
    assert_eq!(
        outcome.deleted_reef_ids,
        vec![fx.node_id],
        "the promoted reef had exactly one coral (the one just unpublished), so it must be \
         reported as deleted too (§4.9's coral-less-reef rule)"
    );

    assert!(
        !atom_row_exists(&owner, fx.atom_id).await,
        "the atom row must actually be gone (checked via the owner/BYPASSRLS pool, which is \
         unaffected by RLS either way)"
    );

    // Idempotency: calling it again must be the harmless "already gone" no-op,
    // not an error — same call, same app-role pool, still scoped correctly.
    let second = retraction::unpublish_atom(
        &app_pool,
        None,
        "test-device",
        fx.graph_id,
        fx.atom_id,
        fx.promoter_user_id,
    )
    .await
    .expect("retracting an already-retracted coral must still succeed (idempotent)");
    assert!(
        !second.atom_was_present,
        "a second unpublish of the same atom must report atom_was_present=false"
    );
}

/// **The reproduction for `services::deletion::delete_atom_cascade`.**
///
/// Same reasoning as the unpublish test above, for the deletion primitive
/// every `delete_coral`/`delete_source`/`delete_subject` entry point bottoms
/// out in. `account_id` is passed explicitly (as the real callers now do,
/// resolved once via `resolve_account_id_for_graph`'s `resolve_graph_account_id`
/// bootstrap) rather than re-derived here, since this test targets the
/// primitive directly.
///
/// Negative control performed by hand for this PR: reverting
/// `delete_atom_cascade` to its pre-fix `let mut tx = pool.begin().await?;`
/// (dropping the `account_id` parameter and the `begin_account_scoped` call)
/// made this test FAIL with the identical `42704 unrecognized configuration
/// parameter "app.account_id"` error as the unpublish reproduction above.
/// Restoring the fix made it pass again unchanged.
#[tokio::test]
async fn delete_atom_cascade_succeeds_under_app_role_pool() {
    let Some(base) = base_url() else {
        eprintln!(
            "skipping delete_atom_cascade_succeeds_under_app_role_pool: set ORGT_TEST_BASE_URL"
        );
        return;
    };
    let db = "orgt_deletion_cascade";
    let _cleanup_db = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
    let db_url = with_db(&base, db);
    let owner = scratch_db_with_app_role(&base, db).await;
    let fx = seed(&owner).await;

    let app_pool = app_role_pool(&db_url).await;

    let outcome = deletion::delete_atom_cascade(
        &app_pool,
        fx.account_id,
        None,
        "test-device",
        fx.graph_id,
        fx.atom_id,
        None,
    )
    .await
    .expect(
        "delete_atom_cascade must succeed under a real coralswarm_app login — a bare \
         `pool.begin()` with no `app.account_id` scoping raises here (0083's strict \
         current_setting), which is exactly the production bug this test regression-guards",
    );

    assert!(
        outcome.atom_was_present,
        "the seeded atom existed, so delete_atom_cascade must report atom_was_present=true"
    );
    assert_eq!(
        outcome.deleted_reef_ids,
        vec![fx.node_id],
        "the reef had exactly one coral (the one just deleted), so it must be reported deleted \
         too (§4.9's coral-less-reef rule, reused verbatim by deletion)"
    );

    assert!(
        !atom_row_exists(&owner, fx.atom_id).await,
        "the atom row must actually be gone"
    );

    let second = deletion::delete_atom_cascade(
        &app_pool,
        fx.account_id,
        None,
        "test-device",
        fx.graph_id,
        fx.atom_id,
        None,
    )
    .await
    .expect("deleting an already-deleted atom must still succeed (idempotent)");
    assert!(
        !second.atom_was_present,
        "a second delete of the same atom must report atom_was_present=false"
    );

    // This suite deliberately does not also exercise `delete_coral`/
    // `delete_source`/`delete_subject` end-to-end: they are thin resolvers
    // over this same primitive (see `services::deletion`'s module doc), and
    // this test's purpose is isolating the scoping bug in the primitive
    // itself, not re-proving their own reachability/role gates.
}

/// **The reproduction for `services::retraction::revoke_source_publication`.**
///
/// Same reasoning as the two tests above, for the sibling §4.9 verb —
/// `POST /v1/graphs/{graph_id}/revoke-publication`. `source_publications` is
/// RLS-covered since migration `0111` (not `0083` — the table didn't exist
/// when `0083` shipped), under the identical strict, non-`missing_ok`
/// `current_setting('app.account_id')` predicate.
///
/// Negative control performed by hand for this PR: reverting
/// `revoke_source_publication` to its pre-fix `let mut tx =
/// pool.begin().await?;` (dropping the `resolve_graph_account_id`/
/// `begin_account_scoped` call) made this test FAIL with exactly:
///
///   called `Result::unwrap()` on an `Err` value: Db(Database(PgDatabaseError {
///   severity: Error, code: "42704", message: "unrecognized configuration
///   parameter \"app.account_id\"", ... }))
///
/// — the identical error class as the other two reproductions in this file,
/// and confirmed (separately, by hand against a genuinely EMPTY
/// `source_publications` table with no fixture at all) that this raises even
/// with zero rows in the table: Postgres evaluates the RLS qual while
/// planning the statement, not lazily per matched row, so "nothing writes
/// `source_publications` yet" was never a mitigation for this bug in
/// production. Restoring the fix made this test pass again unchanged.
#[tokio::test]
async fn revoke_source_publication_succeeds_under_app_role_pool() {
    let Some(base) = base_url() else {
        eprintln!(
            "skipping revoke_source_publication_succeeds_under_app_role_pool: set \
             ORGT_TEST_BASE_URL"
        );
        return;
    };
    let db = "orgt_retraction_revoke";
    let _cleanup_db = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
    let db_url = with_db(&base, db);
    let owner = scratch_db_with_app_role(&base, db).await;
    let pub_fx = seed_publication(&owner).await;

    // A REAL separate login AS coralswarm_app — same reasoning as the other
    // two tests in this file.
    let app_pool = app_role_pool(&db_url).await;

    let outcome = retraction::revoke_source_publication(
        &app_pool,
        &pub_fx.source_id,
        pub_fx.source_graph_id,
        pub_fx.target_graph_id,
        pub_fx.publisher_user_id,
    )
    .await
    .expect(
        "revoke_source_publication must succeed under a real coralswarm_app login — a bare \
         `pool.begin()` with no `app.account_id` scoping raises here (0111's strict \
         current_setting, same shape as 0083's), which is exactly the production bug this test \
         regression-guards",
    );

    assert_eq!(
        outcome,
        retraction::RevokeOutcome::Revoked,
        "the seeded publication was live (unrevoked) and the caller is its consenting \
         publisher, so this must actually flip revoked_at, not report an idempotent no-op"
    );

    let revoked_at = publication_revoked_at(
        &owner,
        &pub_fx.source_id,
        pub_fx.source_graph_id,
        pub_fx.target_graph_id,
    )
    .await;
    assert!(
        revoked_at.is_some(),
        "revoked_at must actually be set (checked via the owner/BYPASSRLS pool)"
    );

    // Idempotency: revoking an already-revoked publication must be the
    // harmless AlreadyRevoked no-op, not an error.
    let second = retraction::revoke_source_publication(
        &app_pool,
        &pub_fx.source_id,
        pub_fx.source_graph_id,
        pub_fx.target_graph_id,
        pub_fx.publisher_user_id,
    )
    .await
    .expect("revoking an already-revoked publication must still succeed (idempotent)");
    assert_eq!(
        second,
        retraction::RevokeOutcome::AlreadyRevoked,
        "a second revoke of the same publication must report AlreadyRevoked, not Revoked again"
    );
}

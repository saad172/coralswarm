//! Proof that a converted `JobScope::Org` arm really carries the tenant RLS
//! GUC into the database — against `migrations_tenant/0009_tenant_rls` live,
//! and a real `coralswarm_tenant_app` login that RLS actually applies to.
//!
//! # The problem this closes
//!
//! PR #840 converted 25 `JobScope::Org` arms. Each one used to run its query
//! straight against a pooled `&PgPool`; each one now opens
//! [`begin_tenant_system_scoped_without_account`] first. The only evidence
//! that the conversion does anything was that somebody read the code.
//!
//! That is the same evidence that already failed. The defect survived review
//! for months because the two match arms LOOK symmetrical — a `&PgPool` and a
//! `&mut Transaction` are both just "the executor" at the call site, and
//! nothing in the type system, the tests, or a green CI run could tell them
//! apart. `scripts/check_tenant_rls_coverage.sh` greps for the `begin_*`
//! call. A grep proves a function is CALLED. It cannot prove the GUC that
//! function sets reaches a policy, that the policy reads it, or that the row
//! would have been invisible without it.
//!
//! # The property, and both of its halves
//!
//! **Under a role that RLS applies to, an Org-scope read returns the tenant's
//! rows; without the scope, the same read returns ZERO rows.**
//!
//! Only the pair is a proof. A test that shows the scoped path works passes
//! unchanged if the GUC does nothing at all — every row is visible to
//! everyone, the scope is decoration, and the assertion is green. The
//! negative half is what makes the positive half mean something.
//!
//! This file asserts the negative half twice, two different ways, because
//! they close different holes:
//!
//! * **Unscoped, on the same pool and the same role** (`ambient_*` probes) —
//!   the seeded rows come back as a SUCCESSFUL EMPTY result. That proves RLS
//!   genuinely bites on this fixture: the rows are not visible for some
//!   reason unrelated to the scope.
//! * **Through the real worker tick, with the GUC's effect removed**
//!   (`tenant_rls_is_system()` replaced by a constant `false`) — the exact
//!   production code path claims ZERO atoms. Same fixture, same row, same
//!   function; the only thing that changed is whether the GUC the scope sets
//!   can still reach the policy. That is a controlled A/B on the causal link
//!   itself, and it cannot drift away from the production SQL because it
//!   never copies any.
//!
//! # What runs the positive half
//!
//! [`coralswarm_backend::workers::embed_retry::tick`] — the real, fully
//! public entry point `workers::tenant_harness::run_multi_tenant` calls every
//! sweep. Not a reimplementation of the claim, and not the private
//! `claim_org_scoped` behind it. `tick`'s own return value is the observable:
//! `Tick::processed` IS the number of rows the converted Org arm's
//! `UPDATE atoms ... RETURNING` saw.
//!
//! `embed_retry` is the one converted arm reachable this way. `JobScope` is
//! `pub(crate)` and the other 24 arms sit behind private functions that take
//! it, so an integration test cannot call them. `embed_retry::tick` needs no
//! such access: it takes a fully public `WorkerTenant` whose fields are all
//! public, and it reaches `claim_org_scoped` internally. No visibility in
//! `backend/src` was widened for this file.
//!
//! # What this file does NOT prove
//!
//! It does not prove the other 24 arms. It proves the primitive they all
//! call, against the live policy, under the real role — and it proves one of
//! them end to end. "Every arm calls the primitive" is
//! `check_tenant_rls_coverage.sh`'s job and it is a grep; "the primitive
//! works" is this file's job and a grep cannot do it. Neither half is a proof
//! alone.
//!
//! # Two distinct failure modes — do not confuse them
//!
//! 1. **Pool build refused.** `TenantRegistry::get_or_build_worker_pool`
//!    hard-refuses any role RLS would apply to
//!    (`worker_pool_rls_exemption_breach`, `tenancy/registry.rs`). A test
//!    going through `TenantRegistry` therefore cannot use a non-bypass role
//!    at all. This file opens a plain `sqlx` pool authenticated directly as
//!    `coralswarm_tenant_app` — same route `tenant_rls_phase2_pg.rs` and
//!    `atom_title_backfill_org_rls_pg.rs` take — so that tripwire never
//!    fires here and cannot be mistaken for what this file measures.
//! 2. **Rows invisible under RLS.** What this file measures.
//!
//! # Why the role must be checked, not assumed
//!
//! Three separate ways this suite could pass while proving nothing, each
//! asserted against before any other assertion runs:
//!
//! * The probe role has `BYPASSRLS` — policies are skipped entirely.
//! * The probe role OWNS the tables. `0009` §7 deliberately does NOT use
//!   `FORCE ROW LEVEL SECURITY`, so a table owner is exempt from its own
//!   `ENABLE`-only policies. This is not a hypothetical: the owner exemption
//!   is the actual, only reason the pre-#840 ambient arms worked in
//!   production. A suite that connected as the owner would reproduce that
//!   exemption and call it a pass.
//! * Row security is not enabled on the table at all (`relrowsecurity`).
//!
//! # Error is not empty
//!
//! Every unscoped probe matches `Ok(rows)` and asserts `rows.is_empty()`.
//! An `Err` is a DIFFERENT outcome — a permission failure, a missing table, a
//! dead connection — and only one of the two is the property. `is_err()`,
//! `unwrap_or_default()` and "no rows came back" would all accept both.
//!
//! # Skipping
//!
//! Gated on `ORGT_TEST_BASE_URL` (a base Postgres with CREATE DATABASE +
//! CREATE/ALTER ROLE rights), the same convention every sibling `_pg` suite
//! uses, because the workspace `cargo test` lane links and runs every test
//! binary in this directory with no database at all.
//!
//! A silent skip is the exact vacuity this file exists to avoid, so the skip
//! is made loud at the only place its absence is a defect — the CI step that
//! owns this suite. The step passes `--nocapture` and fails the build if
//! [`SKIP_MARKER`] appears in the log, or if [`PROVED_MARKER`] does not. The
//! test prints `PROVED_MARKER` only after the database half has run to the
//! end, so the step cannot go green on a suite that never touched Postgres.
//! A DSN that is present but unusable is never downgraded to a skip: every
//! connect here is an `.expect(...)` and panics.

mod support;

use std::str::FromStr;
use std::sync::Arc;

use coralswarm_backend::shutdown::Shutdown;
use coralswarm_backend::store::PostgresStore;
use coralswarm_backend::tenancy::scope::{
    begin_account_scoped, begin_tenant_system_scoped, begin_tenant_system_scoped_without_account,
};
use coralswarm_backend::tenancy::TenantId;
use coralswarm_backend::services::source_publication;
use coralswarm_backend::workers::embed_retry;
use coralswarm_backend::workers::tenant_harness::WorkerTenant;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::{Connection, Executor, PgConnection, PgPool, Row};
use support::tenant_role::{set_app_role_password_with_retry, APP_ROLE_TEST_PASSWORD};
use uuid::Uuid;

/// Printed, and nothing else done, when no DSN is configured. Unique enough
/// to grep for: the CI step that owns this suite fails if it appears.
const SKIP_MARKER: &str = "TENANT_RLS_ORG_ARM_SCOPE_PG: SKIPPED — no ORGT_TEST_BASE_URL (a base \
                           Postgres with CREATE DATABASE + CREATE/ALTER ROLE rights)";

/// Printed only after a test's last database assertion has passed. The CI
/// step fails if it is ABSENT, which is what makes a silent skip impossible
/// in the job that is supposed to run this.
const PROVED_MARKER: &str = "TENANT_RLS_ORG_ARM_SCOPE_PG: PROVED";

fn base_url() -> Option<String> {
    match std::env::var("ORGT_TEST_BASE_URL") {
        Ok(v) if !v.trim().is_empty() => Some(v),
        _ => None,
    }
}

fn with_db(base: &str, db: &str) -> String {
    let (prefix, rest) = base.split_at(base.rfind('/').expect("url has a path /"));
    let params = rest.find('?').map(|i| &rest[i..]).unwrap_or("");
    format!("{prefix}/{db}{params}")
}

/// The same database, authenticated as `coralswarm_tenant_app` — the literal
/// DSN shape `TenantRegistry` resolves in production once OPS-4-T repoints
/// tenant DSNs at the non-bypass role. Mirrors
/// `tenant_rls_phase2_pg.rs::as_app_role_dsn`.
fn as_app_role_dsn(db_url: &str) -> String {
    let opts = PgConnectOptions::from_str(db_url).expect("parse scratch db url");
    let host = opts.get_host();
    let port = opts.get_port();
    let db = db_url.rsplit('/').next().unwrap_or("postgres");
    format!("postgres://coralswarm_tenant_app:{APP_ROLE_TEST_PASSWORD}@{host}:{port}/{db}")
}

/// A migrated tenant scratch database, plus one pool per privilege level.
struct Fixture {
    base: String,
    db: String,
    /// Superuser/table-owner. Seeds fixtures and reads back ground truth, so
    /// an assertion about what the app role could NOT see is never confused
    /// with "the row was never written".
    owner: PgPool,
    /// The non-bypass, non-owner `coralswarm_tenant_app` login. Everything
    /// under test runs on this.
    app: PgPool,
}

impl Fixture {
    async fn build(base: &str, db: &str) -> Fixture {
        let mut admin = PgConnection::connect(base).await.expect("connect admin");
        admin
            .execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .expect("drop stale scratch db");
        admin
            .execute(format!("CREATE DATABASE {db}").as_str())
            .await
            .expect("create scratch db");
        admin.close().await.ok();

        let db_url = with_db(base, db);
        let owner: PgPool = PgPoolOptions::new()
            .max_connections(4)
            .connect(&db_url)
            .await
            .expect("connect owner");
        sqlx::migrate!("./migrations_tenant")
            .run(&owner)
            .await
            .expect("apply tenant tree incl. 0009_tenant_rls");
        set_app_role_password_with_retry(&owner).await;

        let app: PgPool = PgPoolOptions::new()
            .max_connections(4)
            .connect(&as_app_role_dsn(&db_url))
            .await
            .expect("connect as coralswarm_tenant_app");

        Fixture { base: base.to_string(), db: db.to_string(), owner, app }
    }

    /// Every way this suite could pass while measuring nothing. Runs before
    /// any assertion about visibility, so a misconfigured cluster fails with
    /// the reason rather than with a green tick.
    async fn assert_rls_actually_applies(&self, tables: &[&str]) {
        let who: String = sqlx::query_scalar("SELECT current_user")
            .fetch_one(&self.app)
            .await
            .expect("read current_user");
        assert_eq!(
            who, "coralswarm_tenant_app",
            "the probe pool must authenticate AS the non-bypass role, not merely reach a database"
        );

        // Both bypass routes, not just the obvious one. A SUPERUSER skips every
        // policy regardless of `rolbypassrls`, so checking `rolbypassrls` alone
        // leaves a role that would make this whole suite pass vacuously — the
        // exact failure it exists to prevent.
        let roles = sqlx::query(
            "SELECT rolbypassrls, rolsuper FROM pg_roles WHERE rolname = current_user",
        )
        .fetch_one(&self.app)
        .await
        .expect("read rolbypassrls/rolsuper");
        let bypass: bool = roles.get("rolbypassrls");
        let superuser: bool = roles.get("rolsuper");
        assert!(
            !bypass,
            "coralswarm_tenant_app must NOT have BYPASSRLS — under a bypass role every policy is \
             skipped, every row is visible, and every assertion below passes for the wrong reason"
        );
        assert!(
            !superuser,
            "coralswarm_tenant_app must NOT be SUPERUSER — a superuser bypasses every policy even \
             with rolbypassrls = false, so every assertion below would pass for the wrong reason"
        );

        for table in tables {
            let row = sqlx::query(
                "SELECT c.relrowsecurity, c.relforcerowsecurity, \
                        pg_catalog.pg_get_userbyid(c.relowner) AS owner \
                 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace \
                 WHERE n.nspname = 'public' AND c.relname = $1",
            )
            .bind(table)
            .fetch_one(&self.app)
            .await
            .unwrap_or_else(|e| panic!("read pg_class for {table}: {e}"));

            let enabled: bool = row.get("relrowsecurity");
            let owner: String = row.get("owner");
            assert!(enabled, "row security must be ENABLED on {table} — 0009 did not take");
            assert_ne!(
                owner, who,
                "the probe role must not OWN {table}. `0009` §7 deliberately does not FORCE row \
                 security, so a table owner is exempt from its own ENABLE-only policies — which \
                 is precisely the exemption the pre-#840 ambient arms were relying on. A suite \
                 that connects as the owner reproduces the bug and reports it as a pass."
            );
        }
    }

    async fn teardown(self) {
        self.app.close().await;
        self.owner.close().await;
        let mut admin = PgConnection::connect(&self.base).await.expect("connect admin");
        admin
            .execute(format!("DROP DATABASE IF EXISTS {} WITH (FORCE)", self.db).as_str())
            .await
            .ok();
        admin.close().await.ok();
    }
}

/// A member-kind ocean, owned by a user this suite never presents as
/// `app.user_id`.
///
/// The shape is load-bearing. `tenant_rls_visible_graph_ids()` admits a
/// `kind = 'member'` graph only via `tenant_rls_is_system()` or an
/// `owner_user_id` match. Any other kind takes the policy's
/// `g.kind <> 'member'` escape hatch and is visible with no scope at all —
/// which would make every negative assertion below pass while measuring
/// nothing.
async fn seed_member_ocean(owner: &PgPool, account_id: Uuid) -> Uuid {
    sqlx::query_scalar(
        "INSERT INTO graphs (id, account_id, name, kind, owner_user_id) \
         VALUES (gen_random_uuid(), $1, 'member ocean', 'member', $2) RETURNING id",
    )
    .bind(account_id)
    .bind(Uuid::new_v4())
    .fetch_one(owner)
    .await
    .expect("seed member-kind graph")
}

/// One atom the embed-retry sweep will find eligible: never embedded, failed
/// long enough ago to clear its own backoff window (attempt 0 ⇒ ~30 s, per
/// `workers::embed::EMBED_BACKOFF_BASE_SECS`), attempts under the cap.
async fn seed_retry_eligible_atom(owner: &PgPool, graph_id: Uuid, source_id: &str) -> Uuid {
    sqlx::query_scalar(
        "INSERT INTO atoms (graph_id, source_type, source_id, content_blob_key, content_inline, \
                            \"timestamp\", embedding, embed_failed_at, embed_attempts) \
         VALUES ($1, 'note', $2, 'blob', 'org arm scope proof', now(), NULL, \
                 now() - INTERVAL '1 hour', 0) \
         RETURNING atom_id",
    )
    .bind(graph_id)
    .bind(source_id)
    .fetch_one(owner)
    .await
    .expect("seed retry-eligible atom")
}

/// `SELECT <col> FROM <table> WHERE <col> = $1`, run AMBIENT — straight
/// against the pool, no transaction, so no `set_config(.., true)` can be in
/// force. Returns the raw result so the caller can tell `Ok(empty)` (the
/// property) from `Err` (something else entirely).
async fn ambient_pk_probe(
    pool: &PgPool,
    table: &str,
    col: &str,
    id: Uuid,
) -> Result<Vec<Uuid>, sqlx::Error> {
    sqlx::query_scalar(&format!("SELECT {col} FROM {table} WHERE {col} = $1"))
        .bind(id)
        .fetch_all(pool)
        .await
}

fn assert_invisible(what: &str, got: Result<Vec<Uuid>, sqlx::Error>) {
    match got {
        Ok(rows) => assert!(
            rows.is_empty(),
            "unscoped, the non-exempt role must see ZERO rows for {what} — got {rows:?}. A \
             non-empty result means RLS is not applying to this role/table, so every scoped \
             assertion in this suite would pass whether or not the GUC does anything."
        ),
        Err(e) => panic!(
            "the unscoped probe for {what} must SUCCEED with zero rows (RLS row-filtering), not \
             error — an error and an empty set are different outcomes and only one is the \
             property under test: {e}"
        ),
    }
}

// ───────────────────────────────────────────────────────────────────────────
// 1. The primitive, on the tables the converted arms read.
// ───────────────────────────────────────────────────────────────────────────

/// `begin_tenant_system_scoped_without_account` flips visibility, and nothing
/// else does.
///
/// Ambient on the pool: `tenant_rls_is_system()` is false and the seeded rows
/// are invisible. Inside the scope, on the SAME pool and the SAME role: true,
/// and the same rows are there. `graphs`, `atoms` and `nodes` carry the
/// identical `member_ocean_isolation` shape every one of #840's 25 arms
/// reads through.
#[tokio::test]
async fn the_scope_primitive_is_what_makes_tenant_rows_visible() {
    let Some(base) = base_url() else {
        eprintln!("{SKIP_MARKER}");
        return;
    };
    let fx = Fixture::build(&base, "orgt_org_arm_scope_primitive").await;
    fx.assert_rls_actually_applies(&["graphs", "atoms", "nodes"]).await;

    let account_id = Uuid::new_v4();
    let graph_id = seed_member_ocean(&fx.owner, account_id).await;
    let atom_id = seed_retry_eligible_atom(&fx.owner, graph_id, "primitive-1").await;
    let node_id: Uuid = sqlx::query_scalar(
        "INSERT INTO nodes (graph_id, type, title) VALUES ($1, 'entity', 'Ada') \
         RETURNING node_id",
    )
    .bind(graph_id)
    .fetch_one(&fx.owner)
    .await
    .expect("seed node");

    // ── Negative half ────────────────────────────────────────────────────
    let is_system: bool = sqlx::query_scalar("SELECT tenant_rls_is_system()")
        .fetch_one(&fx.app)
        .await
        .expect("call tenant_rls_is_system ambient");
    assert!(
        !is_system,
        "ambient on a pooled connection, `app.tenant_actor` cannot be set — it is \
         set_config(.., true), transaction-local. A true here means something is leaking the GUC \
         at session level and the negative half below would be measuring nothing."
    );
    assert_invisible("graphs.id", ambient_pk_probe(&fx.app, "graphs", "id", graph_id).await);
    assert_invisible("atoms.atom_id", ambient_pk_probe(&fx.app, "atoms", "atom_id", atom_id).await);
    assert_invisible("nodes.node_id", ambient_pk_probe(&fx.app, "nodes", "node_id", node_id).await);

    // ── Positive half, same pool, same role, one transaction later ───────
    let mut tx = begin_tenant_system_scoped_without_account(&fx.app)
        .await
        .expect("begin_tenant_system_scoped_without_account");
    let is_system: bool = sqlx::query_scalar("SELECT tenant_rls_is_system()")
        .fetch_one(&mut *tx)
        .await
        .expect("call tenant_rls_is_system inside the scope");
    assert!(is_system, "the scope must set app.tenant_actor='system' for this transaction");

    let seen_graph: Vec<Uuid> = sqlx::query_scalar("SELECT id FROM graphs WHERE id = $1")
        .bind(graph_id)
        .fetch_all(&mut *tx)
        .await
        .expect("scoped graphs read");
    let seen_atom: Vec<Uuid> = sqlx::query_scalar("SELECT atom_id FROM atoms WHERE atom_id = $1")
        .bind(atom_id)
        .fetch_all(&mut *tx)
        .await
        .expect("scoped atoms read");
    let seen_node: Vec<Uuid> = sqlx::query_scalar("SELECT node_id FROM nodes WHERE node_id = $1")
        .bind(node_id)
        .fetch_all(&mut *tx)
        .await
        .expect("scoped nodes read");
    tx.rollback().await.ok();

    assert_eq!(seen_graph, vec![graph_id], "scoped, the same role must see the member ocean");
    assert_eq!(seen_atom, vec![atom_id], "scoped, the same role must see its atom");
    assert_eq!(seen_node, vec![node_id], "scoped, the same role must see its node");

    fx.teardown().await;
    println!("{PROVED_MARKER}: scope primitive flips graphs/atoms/nodes visibility");
}

// ───────────────────────────────────────────────────────────────────────────
// 2. A real converted arm, end to end, through its public entry point.
// ───────────────────────────────────────────────────────────────────────────

/// `workers::embed_retry::tick`'s `JobScope::Org` arm claims the tenant's
/// atom — and claims nothing once the GUC its scope sets can no longer reach
/// the policy.
///
/// The observable is `tick`'s own return value: `Tick::processed` is the row
/// count of the arm's `UPDATE atoms ... RETURNING`. Nothing here
/// reimplements the claim, so nothing here can drift away from it.
#[tokio::test]
async fn embed_retry_org_arm_claims_only_because_of_the_scope() {
    let Some(base) = base_url() else {
        eprintln!("{SKIP_MARKER}");
        return;
    };
    let fx = Fixture::build(&base, "orgt_org_arm_scope_embed_retry").await;
    fx.assert_rls_actually_applies(&["graphs", "atoms"]).await;

    let account_id = Uuid::new_v4();
    let graph_id = seed_member_ocean(&fx.owner, account_id).await;
    let atom_id = seed_retry_eligible_atom(&fx.owner, graph_id, "embed-retry-1").await;

    // The worker's own view of a tenant. Every field is public, and `pool` is
    // the non-bypass login — this is the shape OPS-4-T produces, which is the
    // shape #840's conversion exists for.
    let tenant = WorkerTenant {
        tenant: TenantId::Org("org_tenant_rls_org_arm_scope".to_string()),
        pool: fx.app.clone(),
        store: Arc::new(PostgresStore::new(fx.app.clone())),
    };
    let shutdown = Shutdown::new();

    // ── Negative half A: the row is invisible unscoped, on this exact pool
    //    and role. Establishes that a claim seeing it can only be the scope.
    assert_invisible("atoms.atom_id", ambient_pk_probe(&fx.app, "atoms", "atom_id", atom_id).await);

    // ── Positive half: the real tick ─────────────────────────────────────
    let tick = embed_retry::tick(&tenant, &shutdown).await.expect("embed_retry::tick");
    assert_eq!(
        tick.processed, 1,
        "the converted Org arm must claim the tenant's one eligible atom. 0 means the arm read \
         ambient again and RLS filtered it away — the exact defect #840 fixed."
    );

    // The claim is an UPDATE ... RETURNING, so a bumped `embed_failed_at`
    // proves the WRITE landed on the row, not just that a SELECT matched.
    // Read back as the OWNER: ground truth, never the role under test.
    let bumped: bool = sqlx::query_scalar(
        "SELECT embed_failed_at > now() - INTERVAL '1 minute' FROM atoms WHERE atom_id = $1",
    )
    .bind(atom_id)
    .fetch_one(&fx.owner)
    .await
    .expect("read back embed_failed_at as owner");
    assert!(bumped, "the arm's UPDATE ... RETURNING must have re-stamped embed_failed_at");

    let enqueued: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM jobs WHERE kind = 'EmbedAtomsRetry' AND account_id = $1",
    )
    .bind(account_id)
    .fetch_one(&fx.owner)
    .await
    .expect("count enqueued retry jobs");
    assert_eq!(enqueued, 1, "a claimed atom must produce one EmbedAtomsRetry job for its account");

    // ── Negative half B: the same tick, the same row, the GUC neutered ───
    //
    // Put the fixture back exactly where it started, then sever the one link
    // the scope depends on: `tenant_rls_is_system()`, the function
    // `tenant_rls_visible_graph_ids()` consults and the ONLY thing
    // `app.tenant_actor` feeds. Everything else — role, pool, policy, SQL,
    // code path — is byte-identical to the positive run above.
    //
    // Why replace the function instead of copying the claim SQL and running
    // it ambient: a copy of production SQL in a test rots silently the day
    // production changes, and then proves nothing while still passing. This
    // cannot rot. If the function is renamed, `CREATE OR REPLACE` would
    // create a new unused one and the assertion below goes RED, not green;
    // the guard immediately above makes that a clear message instead of a
    // puzzle.
    sqlx::query("UPDATE atoms SET embed_failed_at = now() - INTERVAL '1 hour' WHERE atom_id = $1")
        .bind(atom_id)
        .execute(&fx.owner)
        .await
        .expect("re-arm the fixture for the negative run");
    sqlx::query("DELETE FROM jobs").execute(&fx.owner).await.expect("clear the job queue");

    let existing: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace \
         WHERE n.nspname = 'public' AND p.proname = 'tenant_rls_is_system' AND p.pronargs = 0",
    )
    .fetch_one(&fx.owner)
    .await
    .expect("look up tenant_rls_is_system");
    assert_eq!(
        existing, 1,
        "expected exactly one zero-argument public.tenant_rls_is_system() to neuter. If 0009 \
         renamed or re-signatured it, the CREATE OR REPLACE below would add a second, unused \
         function and this negative control would be inert."
    );
    sqlx::query(
        "CREATE OR REPLACE FUNCTION tenant_rls_is_system() RETURNS boolean \
         LANGUAGE sql STABLE SET search_path = public, pg_temp AS $$ SELECT false $$",
    )
    .execute(&fx.owner)
    .await
    .expect("neuter tenant_rls_is_system");

    // Prove the neuter took, from inside a real scope: the scope still runs,
    // still sets the GUC, and the GUC no longer reaches the policy.
    let mut tx = begin_tenant_system_scoped_without_account(&fx.app)
        .await
        .expect("begin scope after neutering");
    let still_system: bool = sqlx::query_scalar("SELECT tenant_rls_is_system()")
        .fetch_one(&mut *tx)
        .await
        .expect("call the neutered tenant_rls_is_system");
    tx.rollback().await.ok();
    assert!(!still_system, "the neuter must have taken, or the negative control below is inert");

    let tick = embed_retry::tick(&tenant, &shutdown)
        .await
        .expect("embed_retry::tick must SUCCEED with nothing claimed, not error");
    assert_eq!(
        tick.processed, 0,
        "with the GUC's effect removed the same arm must claim ZERO rows. A non-zero count means \
         the atom is visible for a reason that has nothing to do with the scope, so the positive \
         assertion above proves nothing."
    );
    let enqueued: i64 = sqlx::query_scalar("SELECT count(*) FROM jobs")
        .fetch_one(&fx.owner)
        .await
        .expect("count jobs after the neutered tick");
    assert_eq!(enqueued, 0, "nothing claimed ⇒ nothing enqueued");

    // Still there, all along. The negative half is RLS hiding a row, not a
    // missing fixture.
    let ground_truth: i64 = sqlx::query_scalar("SELECT count(*) FROM atoms WHERE atom_id = $1")
        .bind(atom_id)
        .fetch_one(&fx.owner)
        .await
        .expect("owner ground-truth read");
    assert_eq!(ground_truth, 1, "the owner must still see the atom the app role could not");

    fx.teardown().await;
    println!("{PROVED_MARKER}: embed_retry's Org arm claims 1 scoped, 0 with the GUC severed");
}
// ───────────────────────────────────────────────────────────────────────────
// 3. `source_publication::sweep_due`'s Org arm — driven through the REAL tick.
// ───────────────────────────────────────────────────────────────────────────

/// A control-plane scratch database, migrated, with one (user, account,
/// membership) triple seeded.
///
/// `source_publication::tick` needs BOTH planes: the tenant pool carries the
/// content and the `source_connections` row, and `authorized()` re-checks
/// `account_members` and `org_revocations` on the CONTROL pool before every
/// batch. The other two tests in this file are tenant-only and so need none of
/// this.
///
/// Deliberately connected as the OWNER, not as an app role. The property under
/// test is the TENANT plane's scoping; making the control plane non-bypass too
/// would add a second, unrelated way for this test to fail and would not make
/// the tenant-side assertion any stronger.
struct ControlPlane {
    base: String,
    db: String,
    pool: PgPool,
    user_id: Uuid,
    account_id: Uuid,
}

impl ControlPlane {
    async fn build(base: &str, db: &str) -> ControlPlane {
        let mut admin = PgConnection::connect(base).await.expect("connect admin");
        admin
            .execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .expect("drop stale control db");
        admin
            .execute(format!("CREATE DATABASE {db}").as_str())
            .await
            .expect("create control db");
        admin.close().await.ok();

        let pool: PgPool = PgPoolOptions::new()
            .max_connections(4)
            .connect(&with_db(base, db))
            .await
            .expect("connect control owner");
        sqlx::migrate!("./migrations_control")
            .run(&pool)
            .await
            .expect("apply control tree");

        let user_id: Uuid = sqlx::query_scalar("INSERT INTO users(email) VALUES ($1) RETURNING id")
            .bind(format!("org-arm-sweep-{}@x.co", Uuid::new_v4()))
            .fetch_one(&pool)
            .await
            .expect("seed control user");
        let account_id: Uuid =
            sqlx::query_scalar("INSERT INTO accounts(owner_user_id) VALUES ($1) RETURNING id")
                .bind(user_id)
                .fetch_one(&pool)
                .await
                .expect("seed control account");
        sqlx::query("INSERT INTO account_members(account_id, user_id, role) VALUES ($1,$2,'owner')")
            .bind(account_id)
            .bind(user_id)
            .execute(&pool)
            .await
            .expect("seed control membership");

        ControlPlane { base: base.to_string(), db: db.to_string(), pool, user_id, account_id }
    }

    async fn teardown(self) {
        self.pool.close().await;
        let mut admin = PgConnection::connect(&self.base).await.expect("connect admin");
        admin
            .execute(format!("DROP DATABASE IF EXISTS {} WITH (FORCE)", self.db).as_str())
            .await
            .ok();
        admin.close().await.ok();
    }
}

/// `SELECT id FROM source_connections WHERE id = $1` inside an already-open
/// transaction — the minimal "can this scope SEE this row" question.
///
/// Deliberately NOT a copy of `sweep_due`'s own `DUE_SQL`. A copy of
/// production SQL in a test rots silently the day production changes and then
/// proves nothing while still passing (test 2 above makes the same argument
/// for replacing a function rather than copying a query). What varies across
/// these probes is the SCOPE, so the query is held constant and trivial.
async fn visible_in(tx: &mut sqlx::Transaction<'_, sqlx::Postgres>, id: Uuid) -> Vec<Uuid> {
    sqlx::query_scalar("SELECT id FROM source_connections WHERE id = $1")
        .bind(id)
        .fetch_all(&mut **tx)
        .await
        .expect("scoped probe must succeed with zero or more rows, never error")
}

/// The due-connection read #871 shipped ambient, #874 left ambient, and
/// `scripts/check_org_arm_scope.sh` caught.
///
/// # Why this drives `tick` and not the scope primitive
///
/// An earlier cut of this test asserted the four scope primitives directly —
/// ambient/account-only/actor-only see nothing, `begin_tenant_system_scoped`
/// sees the row — and stopped there. CodeRabbit (Major, on PR #875) was right
/// to reject it, and the reason is worth keeping: **that version reconstructed
/// `sweep_due`'s decision instead of invoking it.** `sweep_due` picks its own
/// opener, independently, at `services/source_publication.rs`'s
/// `TenantId::Org` arm. Change that arm to
/// `begin_tenant_system_scoped_without_account` and the old test still emitted
/// every `PROVED` marker and stayed green; `check_org_arm_scope.sh` accepts
/// that helper as a valid opener, and the CI step counts markers. The
/// production sweep would have gone back to seeing zero Org connections with
/// every gate green — the exact defect this file exists to catch, re-armed.
///
/// A check that passes for the wrong reason is the same shape as the bug. So
/// the load-bearing assertion below is the REAL
/// [`coralswarm_backend::services::source_publication::tick`] — the public
/// entry point `workers::tenant_harness::run_multi_tenant` calls, the same
/// standard test 2 holds itself to with `embed_retry::tick` — and the
/// observable is the sweep's own durable side effect on the row.
///
/// **This binding was verified by mutation, not asserted.** Swapping the Org
/// arm's opener to `begin_tenant_system_scoped_without_account` and re-running:
///
/// ```text
/// scripts/check_org_arm_scope.sh   ok: ... 0 unaccounted for   <- still GREEN
/// this test                        FAILED (last_checked_at still NULL)
/// PROVED markers                   2, and ci.yml's step demands 3
/// ```
///
/// So under the mutation the grep guard is satisfied and two independent
/// things catch it: this assertion, and the marker count. That is the
/// property the earlier primitive-only version did not have — it emitted all
/// three markers and passed.
///
/// # The four probes that remain
///
/// They are no longer the proof; they are the EXPLANATION of which opener the
/// arm needs, and they stay because the middle two are not obvious:
///
/// * ambient — zero rows. `0081`'s `source_connection_scope` opens on
///   `account_id = current_setting('app.account_id', true)::uuid`, which
///   ambient is `account_id = NULL`: NULL for every row.
/// * `begin_account_scoped` — STILL zero rows, and this is the one a reader
///   most expects to be green. The account GUC satisfies the FIRST conjunct,
///   so "the obvious fix" gets that far, but the policy also says
///   `source_graph_id IN (SELECT id FROM graphs)` and tenant `graphs` carries
///   `0009`'s `member_ocean_isolation`: a `member` ocean needs an ACTOR.
/// * `begin_tenant_system_scoped_without_account` — zero rows. It is the one
///   wrong answer that would pass `check_org_arm_scope.sh`.
/// * `begin_tenant_system_scoped` — the row.
#[tokio::test]
async fn source_publication_org_arm_sweeps_a_due_connection_only_because_of_its_scope() {
    let Some(base) = base_url() else {
        eprintln!("{SKIP_MARKER}");
        return;
    };
    let fx = Fixture::build(&base, "orgt_org_arm_scope_source_publication").await;
    fx.assert_rls_actually_applies(&["graphs", "atoms", "source_connections"])
        .await;
    let ctl = ControlPlane::build(&base, "orgt_org_arm_scope_source_publication_ctl").await;
    let (account_id, owner_id) = (ctl.account_id, ctl.user_id);

    // Source is a MEMBER ocean owned by the connection's owner — the shape
    // that makes the `begin_account_scoped` probe sharp, and the shape a real
    // `promote-source` from a member ocean produces.
    let source_graph: Uuid = sqlx::query_scalar(
        "INSERT INTO graphs (account_id, name, kind, owner_user_id) \
         VALUES ($1, 'member ocean', 'member', $2) RETURNING id",
    )
    .bind(account_id)
    .bind(owner_id)
    .fetch_one(&fx.owner)
    .await
    .expect("seed member-kind source ocean");
    // Target is an ORG ocean, i.e. one visible with NO actor at all. So when
    // the account-scoped probe comes back empty, the source graph is the only
    // conjunct that can be responsible.
    let target_graph: Uuid = sqlx::query_scalar(
        "INSERT INTO graphs (account_id, name, kind) \
         VALUES ($1, 'commons', 'org') RETURNING id",
    )
    .bind(account_id)
    .fetch_one(&fx.owner)
    .await
    .expect("seed org-kind target ocean");
    // One atom for the sweep to find. Inserted BEFORE the connection so it
    // satisfies `candidates_for`'s `a.created_at <= c.created_at` bound for a
    // `once`-mode connection.
    sqlx::query(
        "INSERT INTO atoms (graph_id, source_type, source_id, content_blob_key, content_inline, \
                            \"timestamp\") \
         VALUES ($1, 'note', 'due-1', 'blob', 'org arm sweep proof', now())",
    )
    .bind(source_graph)
    .execute(&fx.owner)
    .await
    .expect("seed a publishable atom");

    let connection_id: Uuid = sqlx::query_scalar(
        "INSERT INTO source_connections \
           (account_id, owner_id, source_graph_id, target_graph_id, source_type, source_id, \
            label, mode, include_history) \
         VALUES ($1, $2, $3, $4, 'note', 'due-1', 'due-1', 'once', true) RETURNING id",
    )
    .bind(account_id)
    .bind(owner_id)
    .bind(source_graph)
    .bind(target_graph)
    .fetch_one(&fx.owner)
    .await
    .expect("seed a due source_connections row");

    // Read as the OWNER: this is the fixture precondition, never the role
    // under test. Without it the probes below would still be a visibility
    // proof, but "this is what the sweep would have found" would be unearned.
    let (due, checked, job): (bool, bool, Option<Uuid>) = sqlx::query_as(
        "SELECT NOT paused AND completed_at IS NULL AND next_check_at <= now(), \
                last_checked_at IS NOT NULL, current_job_id \
         FROM source_connections WHERE id = $1",
    )
    .bind(connection_id)
    .fetch_one(&fx.owner)
    .await
    .expect("read fixture state as owner");
    assert!(due, "the fixture row must satisfy the sweep's own due predicate");
    assert!(!checked && job.is_none(), "the fixture must start unswept, or the tick below proves nothing");

    // ── Explanation, not proof: which opener can see this row at all ──────
    assert_invisible(
        "source_connections.id (ambient — the arm as #871 shipped it)",
        ambient_pk_probe(&fx.app, "source_connections", "id", connection_id).await,
    );

    let mut tx = begin_account_scoped(&fx.app, account_id)
        .await
        .expect("begin_account_scoped on the app-role pool");
    let guc: Option<String> = sqlx::query_scalar("SELECT current_setting('app.account_id', true)")
        .fetch_one(&mut *tx)
        .await
        .expect("read back app.account_id");
    assert_eq!(
        guc.as_deref(),
        Some(account_id.to_string().as_str()),
        "begin_account_scoped must have set app.account_id, or the next assertion is inert"
    );
    let account_only = visible_in(&mut tx, connection_id).await;
    tx.rollback().await.ok();
    assert!(
        account_only.is_empty(),
        "an account-scoped read must NOT see a connection whose source is a MEMBER ocean — got \
         {account_only:?}. If this goes non-empty, `graphs`' member_ocean_isolation has stopped \
         reaching `source_connection_scope`'s graph conjuncts and the arm's choice of \
         begin_tenant_system_scoped no longer has a reason."
    );

    let mut tx = begin_tenant_system_scoped_without_account(&fx.app)
        .await
        .expect("begin_tenant_system_scoped_without_account on the app-role pool");
    let actor_only = visible_in(&mut tx, connection_id).await;
    tx.rollback().await.ok();
    assert!(
        actor_only.is_empty(),
        "a system actor with NO app.account_id must still see nothing here — got {actor_only:?}. \
         This is the substitution `check_org_arm_scope.sh` would ACCEPT, which is why the tick \
         below and not a primitive probe is this test's actual proof."
    );

    let mut tx = begin_tenant_system_scoped(&fx.app, account_id)
        .await
        .expect("begin_tenant_system_scoped on the app-role pool");
    let system_scoped = visible_in(&mut tx, connection_id).await;
    tx.rollback().await.ok();
    assert_eq!(
        system_scoped,
        vec![connection_id],
        "begin_tenant_system_scoped must make the due connection visible — account GUC for \
         `account_id = ...`, system actor for the two `IN (SELECT id FROM graphs)` conjuncts"
    );

    // ── THE PROOF: the real sweep, through the real public entry point ────
    //
    // `tick` dequeues first (the queue is empty on this pass) and then calls
    // `sweep_due`, which is where the Org arm lives. Nothing here reimplements
    // that choice — if the arm picks the wrong opener, or none, `sweep_due`
    // reads zero rows and every assertion below fails.
    let tenant = WorkerTenant {
        tenant: TenantId::Org("org_tenant_rls_org_arm_scope_publication".to_string()),
        pool: fx.app.clone(),
        store: Arc::new(PostgresStore::new(fx.app.clone())),
    };
    source_publication::tick(&tenant, &ctl.pool, None)
        .await
        .expect("source_publication::tick must SUCCEED");

    // The observable is the sweep's own durable side effect, read back as the
    // OWNER — ground truth, never the role under test. An arm that saw zero
    // rows leaves all three of these untouched, and `tick` still returns Ok:
    // that is precisely the silent shape this whole file exists to catch.
    let (checked, job, err): (bool, Option<Uuid>, Option<String>) = sqlx::query_as(
        "SELECT last_checked_at IS NOT NULL, current_job_id, error_message \
         FROM source_connections WHERE id = $1",
    )
    .bind(connection_id)
    .fetch_one(&fx.owner)
    .await
    .expect("read swept state as owner");
    assert_eq!(err, None, "the sweep must not have recorded a failure reason");
    assert!(
        checked,
        "`sweep_due`'s Org arm must have SWEPT this connection — `last_checked_at` is still NULL, \
         which means the arm read zero rows and `tick` returned Ok anyway. That is the defect: \
         the arm's opener is missing, or it is the `_without_account` form that leaves \
         `app.account_id` unset and puts `source_connection_scope`'s first conjunct back to NULL."
    );
    let job = job.expect(
        "the swept connection must carry the batch the sweep enqueued — a NULL current_job_id \
         with a non-NULL last_checked_at would mean the row was seen but its candidates were not",
    );
    let kind: String = sqlx::query_scalar("SELECT kind FROM jobs WHERE id = $1")
        .bind(job)
        .fetch_one(&fx.owner)
        .await
        .expect("the enqueued batch must exist on the tenant plane");
    assert_eq!(kind, "PublishSource");

    ctl.teardown().await;
    fx.teardown().await;
    println!(
        "{PROVED_MARKER}: source_publication::tick's Org arm swept a due member-ocean connection \
         and enqueued its batch"
    );
}

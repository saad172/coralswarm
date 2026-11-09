//! Proof that the transcript read path's ORG scope is what makes a member's
//! own meeting visible, and that it still hides everybody else's.
//!
//! # The defect this closes
//!
//! `services::transcripts::transcript_scope` returned "no scope" for
//! `TenancyDecision::Org`. `scoped_get_meeting_transcript` and
//! `scoped_list_transcript_corrections` then ran the plain `Store` methods
//! straight against the tenant pool, with no transaction, so no
//! `set_config(.., true)` GUC could be in force.
//!
//! `migrations_tenant/0009_tenant_rls` puts `member_ocean_isolation` on both
//! `meeting_transcripts` and `transcript_corrections`, resolved through
//! `tenant_rls_visible_graph_ids()`. That predicate does NOT deny everything
//! when unset — org and personal oceans stay readable, because both of its
//! conjuncts are trivially true for them. It denies exactly the two kinds the
//! member boundary protects: a `member` ocean, whose `owner_user_id` cannot
//! match a NULL actor, and a `team` ocean, whose membership `EXISTS` cannot
//! be satisfied by one. So the symptom was never "the API broke". It was "a
//! member-private or team meeting reports as missing", which is
//! indistinguishable from a meeting nobody ingested.
//!
//! Nothing in the repo could see it. The early return sits on a THIRD enum
//! (`TenancyDecision`), so `scripts/check_org_arm_scope.sh` — which reads
//! `JobScope::Org` and `TenantId::Org` match arms — reports the file clean,
//! and correctly so. `scripts/check_tenant_rls_coverage.sh` is a per-file
//! grep for a `begin_*` name; the file already contained one.
//!
//! # The property, and all three of its parts
//!
//! **Under a role RLS applies to: the member's own transcript is invisible
//! unscoped, visible under the member's scope, and STILL invisible under a
//! different member's scope.**
//!
//! Only the three together are a proof.
//!
//! * The positive half alone passes unchanged if the GUC does nothing — every
//!   row visible to everyone, the scope decoration, the assertion green.
//! * The negative half alone passes for a scope that satisfies the policy
//!   unconditionally. `begin_tenant_system_scoped_without_account` would do
//!   that: it is the reviewed service-principal stand-down, and it would have
//!   ended the silent not-found while answering no access-control question at
//!   all. [`the_service_principal_would_have_hidden_nothing`] measures that
//!   directly rather than asserting it in prose, so the primitive actually
//!   chosen — `begin_tenant_scoped`, which carries the acting member — is
//!   distinguished from the one that was not by a live A/B, not by a comment.
//!
//! # What this file drives: the primitive AND the dispatch
//!
//! Tests 1-3 drive the two things the production helpers are made of: the
//! primitive (`tenancy::scope::begin_tenant_scoped`) and the literal
//! statement (`Store::get_meeting_transcript_scoped` /
//! `list_transcript_corrections_scoped`, and their unscoped twins for the
//! negative half). No SQL is retyped here, so they cannot drift away from the
//! production query.
//!
//! **Those three do not prove the DISPATCH, and the dispatch is where the
//! defect was.** Measured, not suspected: with tests 1-3 as the only cover,
//! reverting `transcript_scope`'s org arm to `if member_user_id.is_some() {
//! return Ok(None) }` — the exact shape that shipped — leaves this suite 3/3
//! green, and so does swapping the production primitive back to
//! `begin_tenant_system_scoped_without_account`. The suite proved the
//! primitive was the right choice and proved nothing about whether an org
//! request ever reaches it.
//!
//! [`the_org_dispatch_is_what_makes_the_read_scoped`] closes that. It calls
//! `services::transcripts::transcript_scope` and
//! `services::transcripts::scoped_get_meeting_transcript` — the production
//! functions, not their ingredients — with a real
//! `TenancyDecision::Org(TenantContext { .. })`, and measures visibility
//! through them. Both mutations above turn it red.
//!
//! Those two functions are `pub` for that test, in the same
//! `services/mod.rs` idiom as `deletion`/`retraction`/`corrections`/
//! `query_audit`, each of which is `pub` so a live-Postgres integration test
//! in a separate compilation unit can drive it. Nothing else in
//! `services::transcripts` moved: `get_transcript`, the chunker and the error
//! types are still `pub(crate)`.
//!
//! And the widening does not hand out a way to FORGE a scope. `TranscriptScope`
//! is an opaque wrapper with a private field — it was briefly `pub type
//! TranscriptScope<'a> = Option<TranscriptScopeTarget<'a>>`, and a public
//! `Option` alias is a public constructor for its own `None`, i.e. for the one
//! input that selects the UNSCOPED `Store::get_meeting_transcript` branch this
//! whole series removed. `TranscriptScopeTarget` is `pub(crate)` with private
//! fields. So `transcript_scope` is the only way to obtain any scope from
//! here, which is why the pool-less assertion below resolves one through it
//! rather than writing `None`.
//!
//! # Why the role must be checked, not assumed
//!
//! Four ways this suite could pass while measuring nothing, all asserted
//! before any visibility assertion runs. See [`Fixture::assert_rls_applies`].
//! The fourth is the one that matters most here: `0009` §7 deliberately does
//! NOT use `FORCE ROW LEVEL SECURITY`, so the table OWNER is exempt from its
//! own `ENABLE`-only policies — and that owner exemption is the only reason
//! this unscoped path works in production today.
//!
//! # Error is not empty
//!
//! Every negative probe matches `Ok(..)` and asserts the row is absent. An
//! `Err` is a different outcome — a permission failure, a missing table, a
//! dead connection — and only one of the two is the property.
//!
//! # Skipping
//!
//! Gated on `ORGT_TEST_BASE_URL` (a base Postgres with CREATE DATABASE +
//! CREATE/ALTER ROLE rights), the convention every sibling `_pg` suite uses,
//! because the workspace `cargo test` lane links and runs every test binary
//! in this directory with no database at all. A silent skip is the exact
//! vacuity this file exists to avoid, so the CI step that owns it passes
//! `--nocapture`, fails on [`SKIP_MARKER`], and fails unless it counts every
//! [`PROVED_MARKER`]. A DSN that is present but unusable is never downgraded
//! to a skip: every connect here is an `.expect(..)` and panics.

mod support;

use std::str::FromStr;

use coralswarm_backend::services::transcripts::{
    scope_kind, scoped_get_meeting_transcript, transcript_scope,
};
use coralswarm_backend::store::{PostgresStore, Store};
use coralswarm_backend::tenancy::runtime::{TenancyDecision, TenantContext};
use coralswarm_backend::tenancy::scope::{
    begin_tenant_scoped, begin_tenant_system_scoped_without_account,
};
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::{Connection, Executor, PgConnection, PgPool, Row};
use support::tenant_role::{set_app_role_password_with_retry, APP_ROLE_TEST_PASSWORD};
use uuid::Uuid;

/// Printed, and nothing else done, when no DSN is configured. Unique enough
/// to grep for: the CI step that owns this suite fails if it appears.
const SKIP_MARKER: &str = "TRANSCRIPT_SCOPE_ORG_RLS_PG: SKIPPED — no ORGT_TEST_BASE_URL (a base \
                           Postgres with CREATE DATABASE + CREATE/ALTER ROLE rights)";

/// Printed only after a test's last database assertion has passed. The CI
/// step fails if the count is wrong, which is what makes a silent skip — or a
/// quietly deleted test — impossible in the job that runs this.
const PROVED_MARKER: &str = "TRANSCRIPT_SCOPE_ORG_RLS_PG: PROVED";

/// The meeting every test in this file reads.
const SOURCE_ID: &str = "transcript-scope-proof-1";

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

/// The same database, authenticated as `coralswarm_tenant_app` — the DSN
/// shape production resolves once OPS-4-T repoints tenant DSNs at the
/// non-bypass role. Mirrors `tenant_rls_org_arm_scope_pg.rs::as_app_role_dsn`.
fn as_app_role_dsn(db_url: &str) -> String {
    let opts = PgConnectOptions::from_str(db_url).expect("parse scratch db url");
    let host = opts.get_host();
    let port = opts.get_port();
    let db = db_url.rsplit('/').next().unwrap_or("postgres");
    format!("postgres://coralswarm_tenant_app:{APP_ROLE_TEST_PASSWORD}@{host}:{port}/{db}")
}

/// A migrated tenant scratch database, plus one pool per privilege level.
struct Fixture {
    _database: coralswarm_backend::test_db::TestDatabase,
    base: String,
    db: String,
    /// Superuser/table-owner. Seeds fixtures and reads back ground truth, so
    /// "the app role could not see it" is never confused with "the row was
    /// never written".
    owner: PgPool,
    /// The non-bypass, non-owner `coralswarm_tenant_app` login. Everything
    /// under test runs on this.
    app: PgPool,
}

impl Fixture {
    async fn build(base: &str, db: &str) -> Fixture {
        let database = coralswarm_backend::test_db::TestDatabase::adopt_from_url(base, db);
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

        Fixture {
            _database: database,
            base: base.to_string(),
            db: db.to_string(),
            owner,
            app,
        }
    }

    /// Every way this suite could pass while measuring nothing. Runs before
    /// any assertion about visibility, so a misconfigured cluster fails with
    /// the reason rather than with a green tick.
    async fn assert_rls_applies(&self, tables: &[&str]) {
        let who: String = sqlx::query_scalar("SELECT current_user")
            .fetch_one(&self.app)
            .await
            .expect("read current_user");
        assert_eq!(
            who, "coralswarm_tenant_app",
            "the probe pool must authenticate AS the non-bypass role, not merely reach a database"
        );

        // Both bypass routes. A SUPERUSER skips every policy regardless of
        // `rolbypassrls`, so checking `rolbypassrls` alone leaves a role that
        // makes this whole suite pass vacuously.
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
                "SELECT c.relrowsecurity, \
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
            assert!(
                enabled,
                "row security must be ENABLED on {table} — 0009 did not take"
            );
            assert_ne!(
                owner, who,
                "the probe role must not OWN {table}. `0009` §7 deliberately does not FORCE row \
                 security, so a table owner is exempt from its own ENABLE-only policies. That \
                 owner exemption is the only reason the unscoped transcript read works in \
                 production today, so a suite that connects as the owner reproduces the bug and \
                 reports it as a pass."
            );
        }
    }

    /// A `PostgresStore` on the non-bypass login — the same type production
    /// resolves, so the SQL under test is the production SQL.
    fn store(&self) -> PostgresStore {
        PostgresStore::new(self.app.clone())
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

/// One ocean of `kind`, plus the transcript and correction rows the read path
/// loads for it. Returns the ocean's id.
///
/// The `kind` is load-bearing. `tenant_rls_visible_graph_ids()` takes an
/// escape hatch for anything that is neither `member` nor `team`, so an org-
/// or personal-kind ocean is visible with no scope at all and every negative
/// assertion below would pass while measuring nothing.
///
/// `owner_user_id` is `Option` because `graphs_member_requires_owner`
/// (migration `0005`) makes the two inseparable: `(kind = 'member') =
/// (owner_user_id IS NOT NULL)`. A team ocean therefore has NO owner, which is
/// why the policy's team arm has to go through `team_members` at all.
async fn seed_meeting(
    owner: &PgPool,
    account_id: Uuid,
    kind: &str,
    owner_user_id: Option<Uuid>,
) -> Uuid {
    let graph_id: Uuid = sqlx::query_scalar(
        "INSERT INTO graphs (id, account_id, name, kind, owner_user_id) \
         VALUES (gen_random_uuid(), $1, 'proof ocean', $2, $3) RETURNING id",
    )
    .bind(account_id)
    .bind(kind)
    .bind(owner_user_id)
    .fetch_one(owner)
    .await
    .unwrap_or_else(|e| panic!("seed {kind}-kind graph: {e}"));

    sqlx::query(
        "INSERT INTO meeting_transcripts (graph_id, source_id, raw_inline, duration_ms) \
         VALUES ($1, $2, 'Ada: hello.\nGrace: hello.', 60000)",
    )
    .bind(graph_id)
    .bind(SOURCE_ID)
    .execute(owner)
    .await
    .expect("seed meeting_transcripts row");

    // `transcript_corrections.atom_id` is NOT NULL, so the correction needs a
    // real atom to hang off.
    let atom_id: Uuid = sqlx::query_scalar(
        "INSERT INTO atoms (graph_id, source_type, source_id, content_blob_key, content_inline, \
                            \"timestamp\") \
         VALUES ($1, 'meeting', $2, 'blob', 'Ada: hello.', now()) RETURNING atom_id",
    )
    .bind(graph_id)
    .bind(SOURCE_ID)
    .fetch_one(owner)
    .await
    .expect("seed meeting atom");

    sqlx::query(
        "INSERT INTO transcript_corrections \
             (graph_id, atom_id, source_id, surface_form, canonical, status) \
         VALUES ($1, $2, $3, 'Aida', 'Ada', 'applied')",
    )
    .bind(graph_id)
    .bind(atom_id)
    .bind(SOURCE_ID)
    .execute(owner)
    .await
    .expect("seed transcript_corrections row");

    graph_id
}

/// Put `graph_id` behind a team `user_id` belongs to.
async fn seed_team_membership(owner: &PgPool, account_id: Uuid, graph_id: Uuid, user_id: Uuid) {
    let team_id: Uuid = sqlx::query_scalar(
        "INSERT INTO teams (account_id, graph_id, name, created_by) \
         VALUES ($1, $2, 'proof team', $3) RETURNING id",
    )
    .bind(account_id)
    .bind(graph_id)
    .bind(user_id)
    .fetch_one(owner)
    .await
    .expect("seed team");

    sqlx::query("INSERT INTO team_members (team_id, user_id, role) VALUES ($1, $2, 'member')")
        .bind(team_id)
        .bind(user_id)
        .execute(owner)
        .await
        .expect("seed team membership");
}

/// Ground truth from the owner login, which no policy applies to. Proves the
/// seed landed, so "invisible" below never silently means "never written".
async fn assert_seed_landed(owner: &PgPool, graph_id: Uuid) {
    let transcripts: i64 =
        sqlx::query_scalar("SELECT count(*) FROM meeting_transcripts WHERE graph_id = $1")
            .bind(graph_id)
            .fetch_one(owner)
            .await
            .expect("count transcripts as owner");
    let corrections: i64 =
        sqlx::query_scalar("SELECT count(*) FROM transcript_corrections WHERE graph_id = $1")
            .bind(graph_id)
            .fetch_one(owner)
            .await
            .expect("count corrections as owner");
    assert_eq!(transcripts, 1, "the transcript row must exist on disk");
    assert_eq!(corrections, 1, "the correction row must exist on disk");
}

/// The PRE-FIX code path, verbatim: the plain `Store` methods, straight
/// against the pool. No transaction, so no `set_config(.., true)` GUC can be
/// in force, whatever pool the statement came from.
async fn read_unscoped(store: &PostgresStore, graph_id: Uuid, what: &str) -> (usize, usize) {
    let transcript = store
        .get_meeting_transcript(graph_id, SOURCE_ID)
        .await
        .unwrap_or_else(|e| {
            panic!(
                "the unscoped read of {what} must SUCCEED (RLS row-filtering), not error — an \
                 error and an empty result are different outcomes and only one is the property \
                 under test: {e}"
            )
        });
    let corrections = store
        .list_transcript_corrections(graph_id, SOURCE_ID)
        .await
        .unwrap_or_else(|e| panic!("the unscoped corrections read of {what} must SUCCEED: {e}"));
    (usize::from(transcript.is_some()), corrections.len())
}

/// The POST-FIX code path's primitive and statements: one short
/// `begin_tenant_scoped` transaction per read, as `acting_user`.
async fn read_as_member(
    store: &PostgresStore,
    pool: &PgPool,
    account_id: Uuid,
    acting_user: Uuid,
    graph_id: Uuid,
) -> (usize, usize) {
    let mut tx = begin_tenant_scoped(pool, account_id, acting_user)
        .await
        .expect("begin_tenant_scoped");
    let transcript = store
        .get_meeting_transcript_scoped(&mut tx, graph_id, SOURCE_ID)
        .await
        .expect("scoped transcript read");
    tx.rollback().await.ok();

    let mut tx = begin_tenant_scoped(pool, account_id, acting_user)
        .await
        .expect("begin_tenant_scoped");
    let corrections = store
        .list_transcript_corrections_scoped(&mut tx, graph_id, SOURCE_ID)
        .await
        .expect("scoped corrections read");
    tx.rollback().await.ok();

    (usize::from(transcript.is_some()), corrections.len())
}

// ───────────────────────────────────────────────────────────────────────────
// 1. A member-private meeting.
// ───────────────────────────────────────────────────────────────────────────

/// Invisible unscoped. Visible to its owner. Invisible to another member.
#[tokio::test]
async fn a_member_private_transcript_needs_its_own_members_scope() {
    let Some(base) = base_url() else {
        eprintln!("{SKIP_MARKER}");
        return;
    };
    let fx = Fixture::build(&base, "orgt_transcript_scope_member").await;
    fx.assert_rls_applies(&["graphs", "meeting_transcripts", "transcript_corrections"])
        .await;

    let account_id = Uuid::new_v4();
    let member = Uuid::new_v4();
    let stranger = Uuid::new_v4();
    let graph_id = seed_meeting(&fx.owner, account_id, "member", Some(member)).await;
    assert_seed_landed(&fx.owner, graph_id).await;

    let store = fx.store();

    // ── Negative half: the shape that shipped ────────────────────────────
    let (transcripts, corrections) = read_unscoped(&store, graph_id, "a member ocean").await;
    assert_eq!(
        (transcripts, corrections),
        (0, 0),
        "unscoped, the non-exempt role must see NEITHER the transcript nor its corrections. A \
         non-zero count means RLS is not applying to this role/table and every scoped assertion \
         below would pass whether or not the scope does anything."
    );

    // ── Positive half: same pool, same role, one transaction later ───────
    let (transcripts, corrections) =
        read_as_member(&store, &fx.app, account_id, member, graph_id).await;
    assert_eq!(
        (transcripts, corrections),
        (1, 1),
        "under the OWNING member's scope the same reads must return the row and its correction"
    );

    // ── The check the scope exists to perform, still performed ───────────
    let (transcripts, corrections) =
        read_as_member(&store, &fx.app, account_id, stranger, graph_id).await;
    assert_eq!(
        (transcripts, corrections),
        (0, 0),
        "another member's scope must NOT reveal a member-private transcript. If it does, the \
         scope satisfies the policy unconditionally and buys only the loud failure, not the \
         access-control answer."
    );

    println!("{PROVED_MARKER} member-private: 0 unscoped, 1 as owner, 0 as another member");
    fx.teardown().await;
}

// ───────────────────────────────────────────────────────────────────────────
// 2. A team meeting.
// ───────────────────────────────────────────────────────────────────────────

/// The team arm of the predicate is a different conjunct (a membership
/// `EXISTS`, not an `owner_user_id` match) and fails for a different reason
/// when the actor is NULL, so it is measured separately rather than assumed
/// to follow from the member case.
#[tokio::test]
async fn a_team_transcript_needs_a_team_members_scope() {
    let Some(base) = base_url() else {
        eprintln!("{SKIP_MARKER}");
        return;
    };
    let fx = Fixture::build(&base, "orgt_transcript_scope_team").await;
    fx.assert_rls_applies(&["graphs", "meeting_transcripts", "transcript_corrections"])
        .await;

    let account_id = Uuid::new_v4();
    let teammate = Uuid::new_v4();
    let outsider = Uuid::new_v4();
    // A team ocean has no owner at all (`graphs_member_requires_owner`), so
    // nothing here can pass through the member arm by accident.
    let graph_id = seed_meeting(&fx.owner, account_id, "team", None).await;
    seed_team_membership(&fx.owner, account_id, graph_id, teammate).await;
    assert_seed_landed(&fx.owner, graph_id).await;

    let store = fx.store();

    let (transcripts, corrections) = read_unscoped(&store, graph_id, "a team ocean").await;
    assert_eq!(
        (transcripts, corrections),
        (0, 0),
        "unscoped, a team meeting's transcript and corrections must both be invisible"
    );

    let (transcripts, corrections) =
        read_as_member(&store, &fx.app, account_id, teammate, graph_id).await;
    assert_eq!(
        (transcripts, corrections),
        (1, 1),
        "under a TEAM MEMBER's scope the same reads must return the row and its correction"
    );

    let (transcripts, corrections) =
        read_as_member(&store, &fx.app, account_id, outsider, graph_id).await;
    assert_eq!(
        (transcripts, corrections),
        (0, 0),
        "a non-member's scope must NOT reveal a team transcript"
    );

    println!("{PROVED_MARKER} team: 0 unscoped, 1 as teammate, 0 as an outsider");
    fx.teardown().await;
}

// ───────────────────────────────────────────────────────────────────────────
// 3. The primitive that was NOT chosen.
// ───────────────────────────────────────────────────────────────────────────

/// `begin_tenant_system_scoped_without_account` reveals a stranger's
/// member-private transcript.
///
/// This is not a defect in that primitive — it is the reviewed
/// service-principal stand-down, and background sweeps that act across every
/// ocean in a tenant need exactly this. It is here because it is the other
/// candidate fix for the same symptom, and the difference between the two is
/// invisible from the symptom alone: both make the transcript arrive. This
/// test is the record of what the rejected one does not enforce.
#[tokio::test]
async fn the_service_principal_would_have_hidden_nothing() {
    let Some(base) = base_url() else {
        eprintln!("{SKIP_MARKER}");
        return;
    };
    let fx = Fixture::build(&base, "orgt_transcript_scope_system").await;
    fx.assert_rls_applies(&["graphs", "meeting_transcripts", "transcript_corrections"])
        .await;

    let account_id = Uuid::new_v4();
    let member = Uuid::new_v4();
    let graph_id = seed_meeting(&fx.owner, account_id, "member", Some(member)).await;
    assert_seed_landed(&fx.owner, graph_id).await;

    let store = fx.store();

    let mut tx = begin_tenant_system_scoped_without_account(&fx.app)
        .await
        .expect("begin_tenant_system_scoped_without_account");
    let transcript = store
        .get_meeting_transcript_scoped(&mut tx, graph_id, SOURCE_ID)
        .await
        .expect("system-scoped transcript read");
    tx.rollback().await.ok();

    assert!(
        transcript.is_some(),
        "the service principal is expected to see EVERY ocean in the tenant — if it does not, \
         this test no longer describes the alternative it was written to describe"
    );

    // And the chosen primitive, on the same fixture, does not.
    let (transcripts, _) =
        read_as_member(&store, &fx.app, account_id, Uuid::new_v4(), graph_id).await;
    assert_eq!(
        transcripts, 0,
        "begin_tenant_scoped with a non-owning member must still hide it — that difference is \
         the entire reason the transcript path uses it instead"
    );

    println!("{PROVED_MARKER} service principal: 1 for a stranger's member ocean; member scope: 0");
    fx.teardown().await;
}

// ───────────────────────────────────────────────────────────────────────────
// 4. The DISPATCH — the shape that actually shipped the bug.
// ───────────────────────────────────────────────────────────────────────────

/// An `Org` decision reaching `services::transcripts::transcript_scope` yields
/// a scope, that scope carries the ACTING MEMBER, and the read made through it
/// answers the access-control question.
///
/// # Why this test exists when tests 1-3 are green
///
/// Tests 1-3 measure the primitive. They are blind to the dispatch, and the
/// dispatch is the whole defect. Two mutations were run against them:
///
/// 1. `transcript_scope`'s org arm reverted to the shape that shipped —
///    `if member_user_id.is_some() { return Ok(None) }`. Suite green, 3/3.
/// 2. `begin_transcript_scoped`'s org arm swapped back to
///    `begin_tenant_system_scoped_without_account`. Suite green, 3/3.
///
/// Neither is caught by a primitive-level proof, because neither changes the
/// primitives. This test calls the production functions themselves, so
/// mutation 1 makes the positive half fail (no scope, so the fall-through
/// unscoped read runs and RLS hides the row) and mutation 2 makes every
/// negative half fail (the service principal reveals every ocean in the
/// tenant).
///
/// # Why an integration test and not an in-module `#[cfg(test)]` one
///
/// Three reasons, in order of weight. It measures VISIBILITY through the real
/// entry points rather than asserting GUC strings, so it cannot pass while
/// `0009`'s predicate reads something else. It rides a CI step that already
/// fails on a skip marker and counts PROVED markers, whereas the workspace
/// `cargo test` lane has no Postgres at all and an in-module test would skip
/// there in silence. And it adds no `PgPool` to a `#[cfg(test)]` module, which
/// `scripts/check_scoped_shape_ratchet.sh` is blind to and would have counted.
///
/// # The two viewers who see nothing, and why that is the answer
///
/// A non-owning member reading a member-private ocean, and a non-teammate
/// reading a team ocean, both read empty. Both are `'link'`-share-reachable
/// (`view_share` runs no reachability check for `'link'`), and both therefore
/// degrade to an all-null share body. That is the documented, deliberate limit
/// of this scope: `0009`'s `member_ocean_isolation` has no share-capability
/// arm. It is asserted here so the limit is a measured property and not a
/// paragraph, and so that widening it cannot happen by accident.
///
/// # And the log an operator reads it off
///
/// Both empty reads now log `scope = <kind>` at `warn!`, and `org-member` is
/// the value that tells an operator the member-ocean policy was in force —
/// i.e. that an empty read may be a hidden row and not an un-ingested
/// meeting. A log line that mis-states which policy applied is worse than no
/// log line, so `services::transcripts::scope_kind`'s value is asserted here
/// on a real org scope rather than trusted by reading its match arms.
#[tokio::test]
async fn the_org_dispatch_is_what_makes_the_read_scoped() {
    let Some(base) = base_url() else {
        eprintln!("{SKIP_MARKER}");
        return;
    };
    let fx = Fixture::build(&base, "orgt_transcript_scope_dispatch").await;
    fx.assert_rls_applies(&["graphs", "meeting_transcripts", "transcript_corrections"])
        .await;

    let account_id = Uuid::new_v4();
    let owner_member = Uuid::new_v4();
    let stranger = Uuid::new_v4();
    let teammate = Uuid::new_v4();
    let outsider = Uuid::new_v4();

    let member_graph = seed_meeting(&fx.owner, account_id, "member", Some(owner_member)).await;
    let team_graph = seed_meeting(&fx.owner, account_id, "team", None).await;
    seed_team_membership(&fx.owner, account_id, team_graph, teammate).await;
    assert_seed_landed(&fx.owner, member_graph).await;
    assert_seed_landed(&fx.owner, team_graph).await;

    let store = fx.store();

    // ── The dispatch produces a scope at all ─────────────────────────────
    //
    // The narrowest statement of mutation 1. Asserted before any visibility
    // assertion so the reverted org arm fails HERE, naming itself, rather
    // than three assertions later as a puzzling empty read.
    let scope = transcript_scope(
        &store,
        Some(&fx.app),
        Some(&org_decision(&fx.app, account_id, owner_member)),
        member_graph,
    )
    .await
    .expect("transcript_scope must not refuse a seeded ocean on a live pool");
    assert!(
        scope.is_scoped(),
        "transcript_scope returned NO SCOPE for a TenancyDecision::Org. That is the exact shape \
         that shipped: the org arm early-returned Ok(None) and both reads behind it then ran \
         unscoped against RLS-covered tables. `None` may mean one thing only — no content-plane \
         pool at all — and this call was handed a live PgPool."
    );

    // ── The degrade log's discriminating field says the right thing ──────
    //
    // An empty read on this path now logs `scope = <kind>`, and the operator
    // reads "the member-ocean policy may have hidden this" off that one
    // field. A log line that mis-states which policy was in force is worse
    // than no log line, so the value is measured on a real org scope rather
    // than trusted by reading the match arms.
    assert_eq!(
        scope_kind(scope),
        "org-member",
        "an Org scope must log as `org-member` — that is the value that tells an operator \
         0009's member_ocean_isolation was in force, and therefore that an empty read may be a \
         hidden row rather than an un-ingested meeting"
    );
    // The pool-less plane, resolved the same way — through `transcript_scope`
    // itself, because there is no other way to obtain a scope. That is the
    // point: `TranscriptScope` is opaque, so no caller can hand
    // `scoped_get_meeting_transcript` a fabricated "no scope" and re-select
    // the unscoped `Store::get_meeting_transcript` branch this module exists
    // to remove. A public `Option` alias was exactly that constructor.
    let unscoped = transcript_scope(&store, None, None, member_graph)
        .await
        .expect("the pool-less plane has nothing to scope; it is not a refusal");
    assert!(
        !unscoped.is_scoped(),
        "no content-plane pool and no decision must yield the unscoped scope — the ONLY case \
         that may select the unscoped Store read"
    );
    assert_eq!(
        scope_kind(unscoped),
        "none",
        "the pool-less plane must not claim any policy was in force"
    );

    // ── Positive: the owning member reads their own ocean THROUGH the
    //    dispatch ──────────────────────────────────────────────────────────
    assert_eq!(
        dispatched_read(&store, &fx.app, account_id, owner_member, member_graph).await,
        1,
        "through transcript_scope + scoped_get_meeting_transcript, the OWNING member must see \
         their own member-private transcript. 0 here means the scope never carried the acting \
         member, so RLS hid the row from its own owner."
    );

    // ── Negative: the same ocean, a different member, same dispatch ──────
    assert_eq!(
        dispatched_read(&store, &fx.app, account_id, stranger, member_graph).await,
        0,
        "a non-owning member must NOT see a member-private transcript through this dispatch. 1 \
         here means the org arm opened a scope that satisfies the policy unconditionally — \
         begin_tenant_system_scoped_without_account is exactly that scope, and it ends the \
         silent not-found while answering no access-control question at all."
    );

    // ── The team arm, both ways ──────────────────────────────────────────
    //
    // A different conjunct of the same predicate (a membership EXISTS, not an
    // owner_user_id match), so it is measured rather than assumed to follow.
    assert_eq!(
        dispatched_read(&store, &fx.app, account_id, teammate, team_graph).await,
        1,
        "a TEAM MEMBER must see a team transcript through this dispatch"
    );
    assert_eq!(
        dispatched_read(&store, &fx.app, account_id, outsider, team_graph).await,
        0,
        "a NON-TEAMMATE must NOT see a team transcript through this dispatch. This is the team \
         half of the documented limit: an authed org member holding a 'link' share of a team \
         ocean they are not on reads an all-null body, identically to the member-private case."
    );

    println!(
        "{PROVED_MARKER} dispatch: Org decision yields a scope logging as org-member; \
         member-private 1 as owner / 0 as another member; team 1 as teammate / 0 as an outsider"
    );
    fx.teardown().await;
}

/// A `TenancyDecision::Org` shaped like the one the tenancy gate stamps.
///
/// `user_id` is the field under test: `transcript_scope` reads the acting
/// member from HERE and from nowhere else, which is the same invariant
/// `tenancy::reachability`'s `require_matching_user` enforces on every other
/// org path.
fn org_decision(pool: &PgPool, account_id: Uuid, user_id: Uuid) -> TenancyDecision {
    TenancyDecision::Org(TenantContext {
        org_id: "org_transcript_scope_dispatch".to_string(),
        account_id,
        user_id,
        pool: pool.clone(),
        page_pool: None,
    })
}

/// One transcript read driven entirely through the PRODUCTION dispatch:
/// `transcript_scope` picks the arm and `scoped_get_meeting_transcript` opens
/// it. Returns the row count (0 or 1).
///
/// A refusal and an error are both hard failures here, not zero: an empty
/// result is the property under test and only one of the three outcomes is
/// that property.
async fn dispatched_read(
    store: &PostgresStore,
    pool: &PgPool,
    account_id: Uuid,
    acting_user: Uuid,
    graph_id: Uuid,
) -> usize {
    let decision = org_decision(pool, account_id, acting_user);
    let scope = transcript_scope(store, Some(pool), Some(&decision), graph_id)
        .await
        .unwrap_or_else(|_| {
            panic!(
                "transcript_scope REFUSED a seeded ocean on a live pool — a refusal is a \
                 different outcome from an empty read and only the empty read is the property \
                 under test"
            )
        });
    let row = scoped_get_meeting_transcript(store, scope, graph_id, SOURCE_ID)
        .await
        .unwrap_or_else(|e| {
            panic!(
                "the dispatched read must SUCCEED (RLS row-filtering), not error — an error and \
                 an empty result are different outcomes: {e}"
            )
        });
    usize::from(row.is_some())
}

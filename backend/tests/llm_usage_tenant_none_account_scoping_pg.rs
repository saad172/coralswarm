//! Usage-metering PR5(b2): the transaction-scoping retrofit's DECISIVE proof.
//!
//! `llm::usage::record`/`reserve`/`finalize`'s `account_id: None` arm is the
//! shape every ORG TENANT plane worker call takes
//! (`JobScope::usage_account_id`/`scope.usage_account_id()` return `None` on
//! the Org arm BY DESIGN — one org per tenant DB; see `llm::usage::record`'s
//! own doc "History" section). Before this PR's retrofit, that arm was
//! ambient: a bare autocommit insert (`record`) or a connectionless
//! `pool.begin()` with no session GUC at all (`finalize`). Both only ever
//! worked because the app connects to a tenant database as the OWNER, and
//! `migrations_tenant/0009_tenant_rls` §7 leaves the owner exempt from
//! `ENABLE`-only RLS — a real, silent gap once Phase 3 repoints the tenant
//! DSN at the non-`BYPASSRLS` `coralswarm_tenant_app` role: `llm_usage`'s
//! (and `llm_usage_pending`'s) tenant-plane `member_ocean_isolation` policy
//! is anchored on `graph_id` via `tenant_rls_visible_graph_ids()`, and an
//! UNSCOPED session sees (and can therefore write) nothing but the org
//! commons — a member-private or team ocean's row would be silently
//! rejected.
//!
//! A suite run against the OWNER pool (the shape every other in-crate
//! `llm::usage` test uses, gated on `RELINFER_TEST_DATABASE_URL`) cannot
//! prove this: the owner exemption makes both the fixed code and the old
//! ambient code pass identically, so a green run there certifies nothing
//! about which one is actually running. This suite instead drives the real
//! functions against a REAL `coralswarm_tenant_app` login — a separate
//! login, not `SET ROLE` inside the owner's session — the same connection
//! path the Phase-3 cutover creates, following `tests/tenant_rls_pg.rs`'s
//! and `tests/tenant_rls_phase2_pg.rs`'s established harness.
//!
//! Two tests, each proving BOTH directions the task's negative control asks
//! for:
//!
//!   1. [`record_none_account_lands_a_member_ocean_row_under_the_app_role`] —
//!      `llm::usage::record()` (the six surviving legacy call sites'
//!      mechanism: `extract_batch`'s cache-hit/batch-result arms,
//!      `extract`/`extract_relationships`'s cache-hit arms).
//!   2. [`metered_complete_none_account_lands_under_the_app_role`] —
//!      `llm::usage::metered_complete()`, which exercises BOTH `reserve` and
//!      `finalize`'s `None`-account arm in one round trip (every metered
//!      worker call — `extract`, `extract_relationships`,
//!      `entity_resolution`, `infer_expertise`, `summary`, `journal_sweep`,
//!      `conflict_detection`, `digest` — flows through this).
//!
//! Each test:
//!   * seeds ONE member-private ocean (never the org commons — commons is
//!     visible even to a completely unscoped tenant session, since
//!     `tenant_rls_visible_graph_ids()`'s predicate only restricts
//!     `kind = 'member'`/`kind = 'team'` graphs; using it here would make
//!     the fixed and the ambient path indistinguishable, exactly the
//!     vacuous-test trap this suite exists to avoid);
//!   * asserts the retrofitted function lands a row for that ocean under the
//!     app role (the fix, forward direction);
//!   * inverts it: reverts to the EXACT pre-PR5 ambient shape (a bare
//!     autocommit insert / a bare `pool.begin()` with no GUC) against the
//!     SAME app-role connection and asserts it FAILS — proving the fix, not
//!     something incidental (e.g. the table being globally visible), is what
//!     makes the forward direction succeed.
//!
//! Gated on `ORGT_TEST_BASE_URL`, same scratch-DB convention as every other
//! `*_pg.rs` suite in this tree:
//!
//!   ORGT_TEST_BASE_URL=postgres://alex@localhost:5432/postgres \
//!     cargo test -p coralswarm-backend --test llm_usage_tenant_none_account_scoping_pg

mod support;

use std::str::FromStr;

use coralswarm_backend::llm::usage::UsageContext;
use coralswarm_backend::llm::{CompletionRequest, CompletionResponse, LlmError, LlmProvider, StreamEvent};
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::{Connection, Executor, PgConnection, PgPool};
use support::tenant_role::{set_app_role_password_with_retry, APP_ROLE_TEST_PASSWORD};
use uuid::Uuid;

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

/// A pool connected AS `coralswarm_tenant_app` — a real, separate login, not
/// `SET ROLE` inside the owner's session. Same helper shape as
/// `tenant_rls_pg.rs::app_role_pool`.
async fn app_role_pool(db_url: &str) -> PgPool {
    let opts = PgConnectOptions::from_str(db_url)
        .expect("parse scratch db url")
        .username("coralswarm_tenant_app")
        .password(APP_ROLE_TEST_PASSWORD);
    PgPoolOptions::new()
        .max_connections(2)
        .connect_with(opts)
        .await
        .expect("connect AS coralswarm_tenant_app")
}

/// Seed one org account with exactly one MEMBER-PRIVATE ocean (never the org
/// commons — see this file's module doc for why). Returns `(account, graph)`.
async fn seed_member_ocean(owner: &PgPool) -> (Uuid, Uuid) {
    let account = Uuid::new_v4();
    let user = Uuid::new_v4();
    let graph = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO graphs(id, account_id, name, kind, owner_user_id) \
         VALUES ($1, $2, 'member reef', 'member', $3)",
    )
    .bind(graph)
    .bind(account)
    .bind(user)
    .execute(owner)
    .await
    .expect("seed member ocean");
    (account, graph)
}

async fn usage_count(owner: &PgPool, task: &str) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM llm_usage WHERE task = $1")
        .bind(task)
        .fetch_one(owner)
        .await
        .expect("count llm_usage")
}

async fn pending_count(owner: &PgPool, task: &str) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM llm_usage_pending WHERE task = $1")
        .bind(task)
        .fetch_one(owner)
        .await
        .expect("count llm_usage_pending")
}

// ── 1. `record()` — the six surviving legacy call sites' mechanism ─────────

#[tokio::test]
async fn record_none_account_lands_a_member_ocean_row_under_the_app_role() {
    let Some(base) = base_url() else {
        eprintln!(
            "skipping record_none_account_lands_a_member_ocean_row_under_the_app_role: set \
             ORGT_TEST_BASE_URL"
        );
        return;
    };

    let db = "orgt_usage_record_none_scoping";
    let _cleanup_db = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
    let mut admin_conn = admin(&base).await;
    recreate(&mut admin_conn, db).await;
    let db_url = with_db(&base, db);

    let owner: PgPool = PgPoolOptions::new()
        .max_connections(4)
        .connect(&db_url)
        .await
        .expect("connect owner");
    sqlx::migrate!("./migrations_tenant")
        .run(&owner)
        .await
        .expect("apply tenant tree incl. 0009/0055");
    set_app_role_password_with_retry(&owner).await;

    let (_account, graph) = seed_member_ocean(&owner).await;
    let app = app_role_pool(&db_url).await;

    // ── The fix: record()'s None-account arm, driven for real ──────────────
    let task = format!("pr5_record_none_{}", Uuid::new_v4());
    coralswarm_backend::llm::usage::record(
        &app, None, None, Some(graph), &task, "test-model", 10, 5, 0, 0, None,
    )
    .await;
    assert_eq!(
        usage_count(&owner, &task).await,
        1,
        "record()'s None-account arm must land a row for a MEMBER ocean under the REAL \
         coralswarm_tenant_app role — this is exactly what \
         begin_tenant_system_scoped_without_account exists to fix. If this is 0, the retrofit \
         regressed back to resting on the owner exemption."
    );

    // ── Negative control, inverted: the EXACT pre-PR5 ambient shape ────────
    // `record()`'s old `None` arm was `insert.execute(pool).await` — a bare
    // autocommit statement, no transaction, no GUC at all. Reproduced
    // literally here, against the SAME app-role connection, to prove the fix
    // (not something incidental) is what makes the row above land.
    let ambient_task = format!("pr5_record_ambient_{}", Uuid::new_v4());
    let ambient_result = sqlx::query(
        "INSERT INTO llm_usage(account_id, graph_id, task, model, input_tokens, output_tokens, \
                                cache_creation_input_tokens, cache_read_input_tokens) \
         VALUES (NULL, $1, $2, 'test-model', 10, 5, 0, 0)",
    )
    .bind(graph)
    .bind(&ambient_task)
    .execute(&app)
    .await;
    assert!(
        ambient_result.is_err(),
        "an UNSCOPED autocommit insert (the pre-PR5 shape — no app.tenant_actor set) against a \
         MEMBER ocean must be REJECTED by member_ocean_isolation under the real app role. Got \
         {ambient_result:?}. If this succeeds, this test's positive half proves nothing: the \
         table would be writable either way and the retrofit would be unfalsifiable."
    );
    assert_eq!(
        usage_count(&owner, &ambient_task).await,
        0,
        "the rejected ambient insert must not have landed a row"
    );

    admin_conn
        .execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
        .await
        .ok();
}

// ── 2. `metered_complete()` — exercises BOTH reserve and finalize's fix ────

struct FixedLlm;

#[async_trait::async_trait]
impl LlmProvider for FixedLlm {
    async fn complete(&self, _req: CompletionRequest) -> Result<CompletionResponse, LlmError> {
        Ok(CompletionResponse {
            content: "ok".to_string(),
            input_tokens: 12,
            output_tokens: 34,
            model: "mock-served-model".to_string(),
            cache_creation_input_tokens: 0,
            cache_read_input_tokens: 0,
            stop_reason: Some("end_turn".to_string()),
            cost_usd: None,
        })
    }
    async fn stream_complete(
        &self,
        _req: CompletionRequest,
    ) -> Result<
        std::pin::Pin<Box<dyn futures::Stream<Item = Result<StreamEvent, LlmError>> + Send>>,
        LlmError,
    > {
        unimplemented!("not exercised by this test — see llm::usage's own pg_tests")
    }
}

fn req() -> CompletionRequest {
    use coralswarm_backend::llm::{Message, Role};
    CompletionRequest {
        messages: vec![Message { role: Role::User, content: "hello".into() }],
        model: "mock-requested-model".into(),
        max_tokens: 16,
        system: None,
        tool_schema: None,
        effort: None,
        reasoning: coralswarm_backend::llm::ReasoningSetting::Off,
    }
}

#[tokio::test]
async fn metered_complete_none_account_lands_under_the_app_role() {
    let Some(base) = base_url() else {
        eprintln!(
            "skipping metered_complete_none_account_lands_under_the_app_role: set \
             ORGT_TEST_BASE_URL"
        );
        return;
    };

    let db = "orgt_usage_metered_none_scoping";
    let _cleanup_db = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
    let mut admin_conn = admin(&base).await;
    recreate(&mut admin_conn, db).await;
    let db_url = with_db(&base, db);

    let owner: PgPool = PgPoolOptions::new()
        .max_connections(4)
        .connect(&db_url)
        .await
        .expect("connect owner");
    sqlx::migrate!("./migrations_tenant")
        .run(&owner)
        .await
        .expect("apply tenant tree incl. 0009/0055");
    set_app_role_password_with_retry(&owner).await;

    let (_account, graph) = seed_member_ocean(&owner).await;
    let app = app_role_pool(&db_url).await;

    // ── The fix: reserve() + finalize(), both driven for real via
    // metered_complete() ─────────────────────────────────────────────────
    let task = format!("pr5_metered_none_{}", Uuid::new_v4());
    let ctx = UsageContext::new(app.clone(), None, Some(graph));
    let resp = coralswarm_backend::llm::usage::metered_complete(&FixedLlm, req(), Some(&ctx), &task)
        .await
        .expect("mock provider call succeeds");
    assert_eq!(resp.model, "mock-served-model");

    assert_eq!(
        usage_count(&owner, &task).await,
        1,
        "finalize()'s None-account arm must land a row for a MEMBER ocean under the REAL \
         coralswarm_tenant_app role"
    );
    assert_eq!(
        pending_count(&owner, &task).await,
        0,
        "finalize() must have deleted the pending row reserve() wrote — both halves of the \
         reserve/finalize pair must succeed under the app role, not just one"
    );

    // ── Negative control, inverted: the EXACT pre-PR5 ambient shapes ───────
    // `reserve()`'s old None arm was a bare autocommit insert (same shape as
    // `record()`'s, proven in the sibling test above); `finalize()`'s old
    // None arm was a connectionless `pool.begin()` with NO GUC set — proven
    // here directly, since that shape is unique to finalize.
    let ambient_task = format!("pr5_metered_ambient_{}", Uuid::new_v4());
    let mut bare_tx = app.begin().await.expect("begin bare tx (pre-PR5 finalize shape)");
    let ambient_result = sqlx::query(
        "INSERT INTO llm_usage(account_id, graph_id, task, model, input_tokens, output_tokens, \
                                cache_creation_input_tokens, cache_read_input_tokens) \
         VALUES (NULL, $1, $2, 'test-model', 10, 5, 0, 0)",
    )
    .bind(graph)
    .bind(&ambient_task)
    .execute(&mut *bare_tx)
    .await;
    assert!(
        ambient_result.is_err(),
        "an insert on a BARE transaction (no app.tenant_actor set — the pre-PR5 finalize shape) \
         against a MEMBER ocean must be REJECTED by member_ocean_isolation under the real app \
         role. Got {ambient_result:?}. If this succeeds, the positive half above proves \
         nothing."
    );
    bare_tx.rollback().await.ok();
    assert_eq!(
        usage_count(&owner, &ambient_task).await,
        0,
        "the rejected ambient insert must not have landed a row"
    );

    admin_conn
        .execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
        .await
        .ok();
}

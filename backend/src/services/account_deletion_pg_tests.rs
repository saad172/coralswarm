//! Postgres tests for `services::account_deletion` (settings launch PR 2.4).
//!
//! `RELINFER_TEST_DATABASE_URL`-gated like every billing pg suite; without it
//! each test prints a skip line and returns. A skip is NOT a pass.
//!
//! **The job runs as `coralswarm_app`**, production's role: not the table
//! owner, subject to RLS and to 0083's deliberate revokes. Fixtures are seeded
//! and asserted through the owner pool, but every call the job or the request
//! path makes goes through the app-role pool, so a step that needs a privilege
//! the app role lacks fails here (the first version of this PR passed as
//! superuser and failed in prod at `keys`).
//!
//! Stripe, Clerk and email are local recorders (call counts asserted on the
//! wire). Connector teardown and the tenant plane are fakes; the tenant-plane
//! fake really removes the memberships it is asked to remove, so an ordering
//! bug (purging memberships before detaching) cannot hide behind a counter.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use serde_json::json;
use sqlx::PgPool;
use uuid::Uuid;

use super::*;
use crate::billing::test_support::*;
use sha2::Sha256;

const SECRET: &[u8] = b"test-link-secret";

fn key() -> [u8; 32] {
    derive_cancel_key(SECRET)
}

// ── fakes ────────────────────────────────────────────────────────────────

#[derive(Default)]
struct FakeConnectors {
    calls: Mutex<Vec<Uuid>>,
}

#[async_trait]
impl ConnectorTeardown for FakeConnectors {
    async fn revoke_upstream(&self, _control: &PgPool, account_id: Uuid) -> UpstreamReport {
        self.calls.lock().unwrap().push(account_id);
        UpstreamReport { attempted: 1, failed: 0, skipped: 0 }
    }
}

struct FakeTenant {
    owner: PgPool,
    /// Org account ids the detach step found the user a live member of.
    detached_from: Mutex<Vec<Uuid>>,
    remaining: AtomicUsize,
    fail_next: AtomicBool,
    flags: Mutex<(bool, bool)>,
}

impl FakeTenant {
    fn new(owner: PgPool) -> Self {
        Self {
            owner,
            detached_from: Mutex::new(vec![]),
            remaining: AtomicUsize::new(0),
            fail_next: AtomicBool::new(false),
            flags: Mutex::new((true, true)),
        }
    }
}

#[async_trait]
impl TenantPlane for FakeTenant {
    async fn detach_org_memberships(&self, user_id: Uuid, _clerk: Option<&str>) -> Result<(), String> {
        // Like the real thing: find the orgs through `account_members` and
        // remove the membership. If something already deleted those rows, this
        // finds nothing and the seat would never be released.
        let orgs = blocking_team_orgs(&self.owner, user_id).await.map_err(|e| e.to_string())?;
        for org in orgs {
            self.detached_from.lock().unwrap().push(org.account_id);
            sqlx::query("DELETE FROM account_members WHERE account_id = $1 AND user_id = $2")
                .bind(org.account_id)
                .bind(user_id)
                .execute(&self.owner)
                .await
                .map_err(|e| e.to_string())?;
        }
        Ok(())
    }
    async fn member_workspaces_remaining(&self, _user: Uuid) -> Result<usize, String> {
        if self.fail_next.swap(false, Ordering::SeqCst) {
            return Err("tenant plane unavailable".into());
        }
        Ok(self.remaining.load(Ordering::SeqCst))
    }
    fn reconciler_flags(&self) -> (bool, bool) {
        *self.flags.lock().unwrap()
    }
}

#[derive(Default)]
struct FakeMailer {
    sent: Mutex<Vec<Mail>>,
}

#[async_trait]
impl Mailer for FakeMailer {
    async fn send(&self, mail: Mail) -> Result<(), String> {
        self.sent.lock().unwrap().push(mail);
        Ok(())
    }
}

/// Records every Clerk Backend API request; `fail_next` makes that many
/// answers 500 before succeeding.
struct MockClerk {
    client: ClerkAdminClient,
    deletes: Arc<Mutex<Vec<String>>>,
    fail_next: Arc<AtomicUsize>,
}

impl MockClerk {
    async fn spawn() -> Self {
        let deletes = Arc::new(Mutex::new(Vec::<String>::new()));
        let fail_next = Arc::new(AtomicUsize::new(0));
        let (d, f) = (deletes.clone(), fail_next.clone());
        let app = axum::Router::new().fallback(axum::routing::any(
            move |method: axum::http::Method, uri: axum::http::Uri| {
                let (d, f) = (d.clone(), f.clone());
                async move {
                    use axum::response::IntoResponse;
                    if method == axum::http::Method::DELETE {
                        d.lock().unwrap().push(uri.path().to_string());
                        if f.load(Ordering::SeqCst) > 0 {
                            f.fetch_sub(1, Ordering::SeqCst);
                            return (axum::http::StatusCode::INTERNAL_SERVER_ERROR, "boom").into_response();
                        }
                        return (axum::http::StatusCode::OK, [("content-type", "application/json")], "{}")
                            .into_response();
                    }
                    axum::http::StatusCode::NOT_FOUND.into_response()
                }
            },
        ));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.ok();
        });
        let client = ClerkAdminClient::new(reqwest::Client::new(), "sk_test").with_base_url(format!("http://{addr}"));
        Self { client, deletes, fail_next }
    }
    fn delete_count(&self) -> usize {
        self.deletes.lock().unwrap().len()
    }
    fn paths(&self) -> Vec<String> {
        self.deletes.lock().unwrap().clone()
    }
}

// ── fixtures ─────────────────────────────────────────────────────────────

async fn quiesce(pool: &PgPool) {
    sqlx::query("DELETE FROM account_deletions WHERE state IN ('grace', 'running')").execute(pool).await.unwrap();
}

struct Seed {
    user: Uuid,
    account: Uuid,
    clerk_user_id: String,
    email: String,
    graph: Uuid,
    subscription: String,
    customer: String,
}

async fn seed(pool: &PgPool) -> Seed {
    // `tick` claims whichever deletion is due. Drop rows a previous (failed)
    // run left behind so a test only ever runs its own.
    quiesce(pool).await;
    let clerk_user_id = format!("user_{}", Uuid::new_v4().simple());
    let email = format!("{}@delete-account.test", Uuid::new_v4().simple());
    let user: Uuid = sqlx::query_scalar(
        "INSERT INTO users (email, clerk_user_id, name, timezone) VALUES ($1, $2, 'Ada Lovelace', 'America/Chicago') RETURNING id",
    )
    .bind(&email)
    .bind(&clerk_user_id)
    .fetch_one(pool)
    .await
    .unwrap();
    let account: Uuid =
        sqlx::query_scalar("INSERT INTO accounts (owner_user_id, dek_wrapped) VALUES ($1, '\\x0102'::bytea) RETURNING id")
            .bind(user)
            .fetch_one(pool)
            .await
            .unwrap();
    sqlx::query("INSERT INTO account_members (account_id, user_id, role) VALUES ($1, $2, 'owner')")
        .bind(account)
        .bind(user)
        .execute(pool)
        .await
        .unwrap();
    let graph: Uuid = sqlx::query_scalar(
        "INSERT INTO graphs (account_id, name, kind, is_primary) VALUES ($1, 'Mine', 'personal', true) RETURNING id",
    )
    .bind(account)
    .fetch_one(pool)
    .await
    .unwrap();
    let node: Uuid = sqlx::query_scalar(
        "INSERT INTO nodes (graph_id, type, title) VALUES ($1, 'entity', 'Alice') RETURNING node_id",
    )
    .bind(graph)
    .fetch_one(pool)
    .await
    .unwrap();
    for i in 0..3 {
        let atom = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO atoms (atom_id, graph_id, source_type, source_id, content_blob_key, \"timestamp\", content_inline, ingested_by) \
             VALUES ($1, $2, 'note', $3, $4, now(), 'private words', $5)",
        )
        .bind(atom)
        .bind(graph)
        .bind(format!("note-{i}"))
        .bind(coralswarm_core::r2::personal_blob_key(graph, atom))
        .bind(user)
        .execute(pool)
        .await
        .unwrap();
        sqlx::query("INSERT INTO node_atoms (node_id, atom_id) VALUES ($1, $2)")
            .bind(node)
            .bind(atom)
            .execute(pool)
            .await
            .unwrap();
    }
    sqlx::query(
        "INSERT INTO op_log (op_id, entity, entity_id, op_type, updated_at, device_id, graph_id) \
         VALUES ($1, 'atom', 'x', 'upsert', now(), 'dev', $2)",
    )
    .bind(Uuid::new_v4())
    .bind(graph)
    .execute(pool)
    .await
    .unwrap();
    sqlx::query("INSERT INTO conversations (account_id, graph_id, user_id, title) VALUES ($1, $2, $3, 'secret chat')")
        .bind(account)
        .bind(graph)
        .bind(user)
        .execute(pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO oauth_tokens (account_id, provider, scope, access_token) VALUES ($1, 'slack', 's', '\\x00'::bytea)")
        .bind(account)
        .execute(pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO addin_tokens (token_hash, owner_user_id, account_id) VALUES ($1, $2, $3)")
        .bind(Uuid::new_v4().to_string())
        .bind(user)
        .bind(account)
        .execute(pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO user_private_keys (user_id, blob) VALUES ($1, 'wrapped')")
        .bind(user)
        .execute(pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO sessions (user_id, device_id, token_hash, expires_at) VALUES ($1, 'd', $2, now() + interval '1 day')")
        .bind(user)
        .bind(Uuid::new_v4().to_string())
        .execute(pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO user_prefs (user_id) VALUES ($1)").bind(user).execute(pool).await.unwrap();
    // Tables the app role has no (or no DELETE) privilege on: the reason this
    // suite runs as that role.
    sqlx::query("INSERT INTO escrow_shares (account_id, admin_user_id, share) VALUES ($1, $2, '\\x01'::bytea)")
        .bind(account)
        .bind(user)
        .execute(pool)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO node_case_fold_log (graph_id, node_type, survivor_id, survivor_title, survivor_coral_count, \
         loser_id, loser_title, loser_coral_count, folded_by) VALUES ($1, 'entity', $2, 'Alice', 1, $3, 'alice', 1, 'test')",
    )
    .bind(graph)
    .bind(Uuid::new_v4())
    .bind(Uuid::new_v4())
    .execute(pool)
    .await
    .unwrap();
    // An append-only audit row that must SURVIVE.
    sqlx::query(
        "INSERT INTO retraction_audit_log (retraction_id, account_id, actor_kind, retracted_at, source_type, source_id, \
         graph_id, is_personal_ocean, surface, outcome) VALUES ($1, $2, 'system', now(), 'note', 'n1', $3, true, 'content', 'removed')",
    )
    .bind(Uuid::new_v4())
    .bind(account)
    .bind(graph)
    .execute(pool)
    .await
    .unwrap();
    // Financial rows that MUST survive.
    exec_fixture(
        pool,
        account,
        sqlx::query("INSERT INTO credit_ledger (account_id, entry_kind, credits, cost_usd) VALUES ($1, 'grant', 100, 0)")
            .bind(account),
    )
    .await
    .unwrap();
    exec_fixture(
        pool,
        account,
        sqlx::query(
            "INSERT INTO extra_usage_ledger (account_id, entry_kind, amount_micro_usd, idempotency_key, stripe_event_id) \
             VALUES ($1, 'topup', 5000000, $2, $3)",
        )
        .bind(account)
        .bind(format!("seed:{}", Uuid::new_v4()))
        .bind(format!("evt_{}", Uuid::new_v4().simple())),
    )
    .await
    .unwrap();
    exec_fixture(
        pool,
        account,
        sqlx::query("INSERT INTO extra_usage_balances (account_id, balance_micro_usd) VALUES ($1, 5000000)").bind(account),
    )
    .await
    .unwrap();
    exec_fixture(
        pool,
        account,
        sqlx::query(
            "INSERT INTO extra_usage_settings (account_id, enabled, monthly_limit_micro_usd, auto_reload_enabled, \
             auto_reload_threshold_micro_usd, auto_reload_amount_micro_usd) VALUES ($1, true, 50000000, true, 1000000, 5000000)",
        )
        .bind(account),
    )
    .await
    .unwrap();
    let subscription = format!("sub_{}", Uuid::new_v4().simple());
    let customer = format!("cus_{}", Uuid::new_v4().simple());
    seed_subscription(pool, account, &customer, Some(&subscription), "active").await;
    Seed { user, account, clerk_user_id, email, graph, subscription, customer }
}

async fn forget(pool: &PgPool, s: &Seed) {
    let _ = sqlx::query("DELETE FROM account_deletions WHERE user_id = $1").bind(s.user).execute(pool).await;
    let _ = sqlx::query("DELETE FROM jobs WHERE account_id = $1").bind(s.account).execute(pool).await;
    let _ = sqlx::query("DELETE FROM personal_orgs WHERE clerk_user_id = $1").bind(&s.clerk_user_id).execute(pool).await;
    let _ = cleanup_account(pool, s.account, &[s.user]).await;
}

fn stripe_mock_for(mock: &MockStripe, s: &Seed) {
    let path = format!("/v1/subscriptions/{}", s.subscription);
    let live = subscription_json(&s.subscription, "active", &[("si_1", "price_pro", 1)], false, None, None);
    let canceled = Arc::new(AtomicBool::new(false));
    let c2 = canceled.clone();
    mock.respond_with("DELETE", &path, move |_| {
        c2.store(true, Ordering::SeqCst);
        (200, json!({"id": "x", "status": "canceled"}))
    });
    let sub = s.subscription.clone();
    mock.respond_with("GET", &path, move |_| {
        if canceled.load(Ordering::SeqCst) {
            (200, subscription_json(&sub, "canceled", &[("si_1", "price_pro", 1)], false, None, None))
        } else {
            (200, live.clone())
        }
    });
    mock.respond("DELETE", &format!("/v1/customers/{}", s.customer), 200, json!({"id": s.customer, "deleted": true}));
}

async fn make_due(pool: &PgPool, id: Uuid) {
    sqlx::query("UPDATE account_deletions SET next_attempt_at = now(), lease_until = NULL WHERE id = $1")
        .bind(id)
        .execute(pool)
        .await
        .unwrap();
}

/// The grace period has elapsed.
async fn end_grace(pool: &PgPool, id: Uuid) {
    sqlx::query("UPDATE account_deletions SET purge_after = now() - interval '1 second' WHERE id = $1")
        .bind(id)
        .execute(pool)
        .await
        .unwrap();
    make_due(pool, id).await;
}

async fn count(pool: &PgPool, sql: &str, id: Uuid) -> i64 {
    sqlx::query_scalar(sql).bind(id).fetch_one(pool).await.unwrap()
}

async fn deletion(pool: &PgPool, user: Uuid) -> Deletion {
    sqlx::query_as("SELECT * FROM account_deletions WHERE user_id = $1 ORDER BY created_at DESC LIMIT 1")
        .bind(user)
        .fetch_one(pool)
        .await
        .unwrap()
}

/// A pool connected as `coralswarm_app`.
async fn app_pool(owner: &PgPool) -> PgPool {
    use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
    use std::str::FromStr;
    sqlx::query("ALTER ROLE coralswarm_app WITH LOGIN PASSWORD 'app_test_pw'").execute(owner).await.unwrap();
    let url = std::env::var("RELINFER_TEST_DATABASE_URL").unwrap();
    let opts = PgConnectOptions::from_str(&url).unwrap().username("coralswarm_app").password("app_test_pw");
    PgPoolOptions::new().max_connections(4).connect_with(opts).await.unwrap()
}

/// What the library purge worker and the blob-cleanup worker do between
/// passes: deliver the queued purge dependencies (the real SQL function, in
/// account scope, as the app role) and finish the `DeleteSourceBlob` jobs.
async fn drain(owner: &PgPool, app: &PgPool, account: Uuid) {
    loop {
        let mut tx = crate::tenancy::scope::begin_account_scoped(app, account).await.unwrap();
        let n: i32 = sqlx::query_scalar("SELECT library_drain_purge(100)").fetch_one(&mut *tx).await.unwrap();
        tx.commit().await.unwrap();
        if n == 0 {
            break;
        }
    }
    sqlx::query("UPDATE jobs SET status = 'done' WHERE account_id = $1 AND kind = 'DeleteSourceBlob'")
        .bind(account)
        .execute(owner)
        .await
        .unwrap();
}

struct Rig {
    owner: PgPool,
    app: PgPool,
    stripe: MockStripe,
    clerk: MockClerk,
    connectors: FakeConnectors,
    tenant: FakeTenant,
    mailer: FakeMailer,
    key: [u8; 32],
}

impl Rig {
    async fn new(owner: &PgPool) -> Self {
        Self {
            owner: owner.clone(),
            app: app_pool(owner).await,
            stripe: MockStripe::spawn().await,
            clerk: MockClerk::spawn().await,
            connectors: FakeConnectors::default(),
            tenant: FakeTenant::new(owner.clone()),
            mailer: FakeMailer::default(),
            key: key(),
        }
    }
    fn ctx(&self) -> Ctx<'_> {
        Ctx {
            control: &self.app,
            stripe: Some(&*self.stripe.client),
            clerk: Some(&self.clerk.client),
            blobs: None,
            connectors: &self.connectors,
            tenant_plane: &self.tenant,
            dek_cache: None,
            mailer: &self.mailer,
            cancel_key: Some(&self.key),
            web_base: "https://app.example.test",
        }
    }
    /// Run ticks until nothing is claimable, up to `n`.
    async fn tick(&self) -> bool {
        tick(&self.ctx()).await.unwrap()
    }
    async fn start_user(&self, s: &Seed) -> Deletion {
        start(&self.app, s.user, s.account, Some(&s.clerk_user_id), Initiator::User).await.unwrap().deletion
    }
    /// Drive a user-initiated deletion from POST to completion.
    async fn run_to_completion(&self, s: &Seed) -> Deletion {
        let d = self.start_user(s).await;
        assert!(self.tick().await);
        end_grace(&self.owner, d.id).await;
        assert!(self.tick().await);
        drain(&self.owner, &self.app, s.account).await;
        make_due(&self.owner, d.id).await;
        assert!(self.tick().await);
        deletion(&self.owner, s.user).await
    }
}

// ── tests ────────────────────────────────────────────────────────────────

#[tokio::test]
async fn every_table_is_classified() {
    let Some(owner) = pg_test_pool("account_deletion::every_table_is_classified").await else { return };
    // As the app role: information_schema would silently HIDE tables it has no
    // privilege on (exactly the ones that matter), so this reads pg_catalog.
    let pool = app_pool(&owner).await;
    use std::collections::{BTreeSet, HashSet};

    let user_keyed: Vec<(String, String)> =
        sqlx::query_as("SELECT tbl, col FROM account_deletion_user_keyed_tables()").fetch_all(&pool).await.unwrap();
    let mut known: HashSet<String> = HashSet::new();
    for list in [
        KEEP_FINANCIAL,
        DRAIN_QUEUES,
        KEEP_AUDIT,
        USER_POINTER_ONLY,
        PURGE_BY_GRAPH_NO_FK,
        PURGE_VIA_DEFINER,
        PURGE_BY_ACCOUNT,
        PURGE_VIA_GRAPH_OR_LATER,
    ] {
        known.extend(list.iter().map(|s| s.to_string()));
    }
    known.extend(user_keyed.iter().map(|(t, _)| t.clone()));
    known.extend(PURGE_BY_OTHER_ACCOUNT_COLUMN.iter().map(|(t, _)| t.to_string()));

    let tables: BTreeSet<String> = sqlx::query_scalar(
        "SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace \
          WHERE n.nspname = 'public' AND c.relkind = 'r'",
    )
    .fetch_all(&pool)
    .await
    .unwrap()
    .into_iter()
    .collect();
    for t in &known {
        assert!(tables.contains(t), "{t} is classified but does not exist");
    }
    let has_col = |t: String, c: String| {
        let pool = pool.clone();
        async move {
            sqlx::query_scalar::<_, i64>(
                "SELECT count(*) FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid \
                   JOIN pg_namespace n ON n.oid = c.relnamespace \
                  WHERE n.nspname = 'public' AND c.relname = $1 AND a.attname = $2 AND a.attnum > 0 AND NOT a.attisdropped",
            )
            .bind(t)
            .bind(c)
            .fetch_one(&pool)
            .await
            .unwrap()
        }
    };
    for (t, c) in &user_keyed {
        assert_eq!(has_col(t.clone(), c.clone()).await, 1, "{t}.{c} does not exist");
    }
    for (t, c) in PURGE_BY_OTHER_ACCOUNT_COLUMN {
        assert_eq!(has_col(t.to_string(), c.to_string()).await, 1, "{t}.{c} does not exist");
    }
    for t in PURGE_BY_ACCOUNT.iter().chain(CONNECTOR_TABLES.iter()) {
        assert_eq!(has_col(t.to_string(), "account_id".into()).await, 1, "{t} has no account_id column");
        assert!(PURGE_BY_ACCOUNT.contains(t), "{t} is a connector table but not in PURGE_BY_ACCOUNT");
    }
    for t in PURGE_BY_GRAPH_NO_FK {
        assert_eq!(has_col(t.to_string(), "graph_id".into()).await, 1, "{t} has no graph_id column");
    }

    // Anything carrying account/graph/user identity (a column, or a foreign key
    // of ANY delete rule to users/accounts/graphs) that a graph cascade does not
    // reach must be classified.
    let needy: Vec<(String, String)> = sqlx::query_as(
        "WITH graph_cascaded AS ( \
             SELECT conrelid::regclass::text AS t FROM pg_constraint \
              WHERE contype='f' AND confrelid='graphs'::regclass AND confdeltype='c'), \
         ident AS ( \
             SELECT c.relname::text AS t, 'column ' || a.attname AS why \
               FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid JOIN pg_namespace n ON n.oid = c.relnamespace \
              WHERE n.nspname='public' AND c.relkind='r' AND a.attnum > 0 AND NOT a.attisdropped \
                AND a.attname IN ('account_id','graph_id','source_graph_id') \
             UNION \
             SELECT conrelid::regclass::text, 'fk to ' || confrelid::regclass::text FROM pg_constraint \
              WHERE contype='f' AND confrelid IN ('accounts'::regclass, 'users'::regclass, 'graphs'::regclass)) \
         SELECT DISTINCT i.t, i.why FROM ident i WHERE i.t NOT IN (SELECT t FROM graph_cascaded) ORDER BY 1",
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    let missing: Vec<String> = needy
        .into_iter()
        .filter(|(t, _)| !known.contains(t))
        .map(|(t, why)| format!("{t} ({why})"))
        .collect();
    assert!(
        missing.is_empty(),
        "account deletion does not classify these tables. Add each to a PURGE_* list or the user-keyed SQL \
         function (delete it), or KEEP_*/USER_POINTER_ONLY (and say why): {missing:?}"
    );

    // Every column that would cascade-delete with the user must be handled.
    let cascading: Vec<(String, String)> = sqlx::query_as(
        "SELECT c.conrelid::regclass::text, a.attname::text FROM pg_constraint c \
           JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = ANY (c.conkey) \
          WHERE c.contype='f' AND c.confrelid='users'::regclass AND c.confdeltype='c' \
            AND c.conrelid::regclass::text NOT IN ( \
                SELECT conrelid::regclass::text FROM pg_constraint \
                 WHERE contype='f' AND confrelid='graphs'::regclass AND confdeltype='c') \
          ORDER BY 1, 2",
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    let covered: HashSet<(String, String)> = user_keyed.iter().cloned().collect();
    let uncovered: Vec<_> = cascading
        .into_iter()
        .filter(|p| !covered.contains(p) && p.0 != "member_ocean_desired_state")
        .collect();
    assert!(uncovered.is_empty(), "cascading user columns not purged by account_deletion_user_keyed_tables(): {uncovered:?}");
}

#[tokio::test]
async fn happy_path_runs_as_the_app_role_and_leaves_only_the_tombstone() {
    let Some(owner) = pg_test_pool("account_deletion::happy_path").await else { return };
    let s = seed(&owner).await;
    let unrelated = seed(&owner).await;
    let rig = Rig::new(&owner).await;
    stripe_mock_for(&rig.stripe, &s);

    const LISTING_TABLES: [&str; 7] = [
        "library_listing_state", "library_listing_changes", "library_listing_generations",
        "library_listing_source_heads", "library_listing_staged_sources", "library_listing_outcomes",
        "library_listing_staged_outcomes",
    ];
    async fn listing_rows(owner: &PgPool, graph: Uuid) -> Vec<serde_json::Value> {
        let mut rows = Vec::new();
        for table in LISTING_TABLES {
            rows.push(sqlx::query_scalar(&format!(
                "SELECT COALESCE(jsonb_agg(to_jsonb(t) ORDER BY to_jsonb(t)::text),'[]'::jsonb) FROM {table} t WHERE graph_id=$1"
            )).bind(graph).fetch_one(owner).await.unwrap());
        }
        rows
    }
    async fn populate_listing(owner: &PgPool, app: &PgPool, s: &Seed) {
        use crate::workers::library_evidence::{self as worker, Limits, ListingLimits, RefreshLimits};
        // The migration ships the switch On; stamp its barrier, as the
        // worker's first tick does.
        worker::stamp_listing_barrier(owner).await.unwrap();
        let origin = json!({"service_id":"slack","content_kind":"message","provider_namespace":"delete:listing",
            "connection":{"kind":"oauth_grant","id":"fixture"},
            "container":{"kind":"channel","id":"shared","provider_namespace":"delete:listing","label":"Delete fixture"}});
        let item: Uuid = sqlx::query_scalar("INSERT INTO source_items(graph_id,source_type,provider_namespace,source_id,origin,access_state) VALUES($1,'slack','delete:listing',$2,$3,'permitted') RETURNING source_item_id")
            .bind(s.graph).bind(Uuid::new_v4().to_string()).bind(origin).fetch_one(owner).await.unwrap();
        let version: Uuid = sqlx::query_scalar("INSERT INTO source_versions(graph_id,source_item_id,source_id,source_type,provider_namespace,origin,raw_inline,source_metadata,content_sha256,extraction_status) SELECT graph_id,source_item_id,source_id,source_type,provider_namespace,origin,$2,'{\"title\":\"Delete fixture\"}',$2,'done' FROM source_items WHERE source_item_id=$1 RETURNING version_id")
            .bind(item).bind(format!("{:064x}", Uuid::new_v4().as_u128())).fetch_one(owner).await.unwrap();
        sqlx::query("UPDATE source_items SET current_version_id=$2 WHERE source_item_id=$1")
            .bind(item).bind(version).execute(owner).await.unwrap();
        async fn outcome(owner: &PgPool, graph: Uuid, version: Uuid, ordinal: i32) {
            sqlx::query("INSERT INTO semantic_atoms(graph_id,source_version_id,ordinal,kind,statement,extraction_version,model,entities) VALUES($1,$2,$3,'fact','Delete fixture','source-atoms-v1','fixture','[]')")
                .bind(graph).bind(version).bind(ordinal).execute(owner).await.unwrap();
        }
        for ordinal in 0..3 { outcome(owner, s.graph, version, ordinal).await; }
        let limits = RefreshLimits { evidence: Limits::default(), listing: ListingLimits { outcomes: 1, ..ListingLimits::default() } };
        let mut ready = false;
        for _ in 0..200 {
            let mut tx = crate::tenancy::scope::begin_account_scoped(app, s.account).await.unwrap();
            worker::refresh_graph_tx(&mut tx, s.graph, limits).await.unwrap();
            ready = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM library_evidence_state WHERE graph_id=$1 AND ready_version=$2) AND EXISTS(SELECT 1 FROM library_listing_state WHERE graph_id=$1 AND ready_version=3) AND NOT EXISTS(SELECT 1 FROM library_listing_changes WHERE graph_id=$1) AND NOT EXISTS(SELECT 1 FROM library_evidence_changes WHERE graph_id=$1) AND NOT EXISTS(SELECT 1 FROM library_listing_generations WHERE graph_id=$1 AND phase NOT IN('complete','abandoned'))")
                .bind(s.graph).bind(crate::library_read::evidence::READ_MODEL_VERSION).fetch_one(&mut *tx).await.unwrap();
            tx.commit().await.unwrap();
            if ready { break; }
        }
        assert!(ready, "real bounded worker must publish listing facts before deletion");
        // Leave a second, genuinely staged generation with a live deferred guard.
        // Graph deletion must cascade through it as well as the published facts.
        outcome(owner, s.graph, version, 3).await;
        let mut staged = false;
        for _ in 0..200 {
            let mut tx = crate::tenancy::scope::begin_account_scoped(app, s.account).await.unwrap();
            worker::refresh_graph_tx(&mut tx, s.graph, limits).await.unwrap();
            staged = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM library_listing_generations g JOIN library_listing_staged_sources s USING(graph_id,source_item_id,generation) JOIN library_listing_staged_outcomes o USING(graph_id,source_item_id,generation) JOIN library_listing_changes c ON c.graph_id=g.graph_id AND c.id=g.guard_change_id WHERE g.graph_id=$1 AND g.phase NOT IN('complete','abandoned'))")
                .bind(s.graph).fetch_one(&mut *tx).await.unwrap();
            tx.commit().await.unwrap();
            if staged { break; }
        }
        assert!(staged, "fixture must include an unfinished, guarded generation with both staged row types");
    }
    populate_listing(&owner, &rig.app, &s).await;
    populate_listing(&owner, &rig.app, &unrelated).await;
    let unrelated_listing = listing_rows(&owner, unrelated.graph).await;
    for (table, (target, other)) in LISTING_TABLES.iter().zip(
        listing_rows(&owner, s.graph).await.iter().zip(&unrelated_listing)
    ) {
        assert!(!target.as_array().unwrap().is_empty(), "target fixture must populate {table}");
        assert!(!other.as_array().unwrap().is_empty(), "unrelated fixture must populate {table}");
    }

    // ── POST: grace. Locked out, Stripe ended, emailed; nothing destroyed.
    let d = rig.start_user(&s).await;
    assert_eq!(d.state, "grace");
    assert!(d.purge_after > Utc::now() + chrono::Duration::days(6));
    assert_eq!(count(&owner, "SELECT count(*) FROM addin_tokens WHERE owner_user_id = $1 AND revoked_at IS NULL", s.user).await, 0);
    assert_eq!(
        sqlx::query_scalar::<_, Option<String>>("SELECT account_deletion_state_for_clerk_user($1)")
            .bind(&s.clerk_user_id)
            .fetch_one(&owner)
            .await
            .unwrap()
            .as_deref(),
        Some("grace")
    );
    assert!(rig.tick().await);
    let in_grace = deletion(&owner, s.user).await;
    assert_eq!(in_grace.state, "grace");
    assert_eq!(in_grace.completed_steps, ["detach", "stripe_cancel", "revoke_tokens", "notify_started"]);
    let sub_path = format!("/v1/subscriptions/{}", s.subscription);
    assert_eq!(rig.stripe.calls("DELETE", &sub_path).len(), 1, "subscription ended at the START of grace");
    assert!(rig.stripe.calls("DELETE", &format!("/v1/customers/{}", s.customer)).is_empty(), "customer survives grace");
    {
        let sent = rig.mailer.sent.lock().unwrap();
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0].to, s.email);
        let link = sent[0].text.split_whitespace().find(|w| w.contains("/account/delete/cancel")).unwrap();
        assert!(!link.contains('?'), "the token is never in the query string: {link}");
        let token = urlencoding::decode(link.split("#token=").nth(1).expect("token in the fragment").trim_end_matches(['.', ')'])).unwrap().into_owned();
        assert_eq!(verify_cancel_token(&key(), &token), Some(d.id), "the emailed link cancels exactly this deletion");
    }
    // Nothing destructive yet.
    for (what, sql) in [
        ("graph", "SELECT count(*) FROM graphs WHERE account_id = $1"),
        ("oauth token", "SELECT count(*) FROM oauth_tokens WHERE account_id = $1"),
        ("escrow share", "SELECT count(*) FROM escrow_shares WHERE account_id = $1"),
    ] {
        assert_eq!(count(&owner, sql, s.account).await, 1, "{what} must survive grace");
    }
    assert_eq!(count(&owner, "SELECT balance_micro_usd FROM extra_usage_balances WHERE account_id = $1", s.account).await, 5_000_000);
    // Auto-reload charges the saved card independently of the subscription:
    // it is OFF from the start of grace (row and limits kept, nothing forfeited yet).
    let (enabled, reload, limit): (bool, bool, Option<i64>) = sqlx::query_as(
        "SELECT enabled, auto_reload_enabled, monthly_limit_micro_usd FROM extra_usage_settings WHERE account_id = $1",
    )
    .bind(s.account)
    .fetch_one(&owner)
    .await
    .unwrap();
    assert_eq!((enabled, reload, limit), (false, false, Some(50_000_000)));
    assert_eq!(rig.clerk.delete_count(), 0);
    let kept_email: String = sqlx::query_scalar("SELECT email FROM users WHERE id = $1").bind(s.user).fetch_one(&owner).await.unwrap();
    assert_eq!(kept_email, s.email);
    // Not due again until the grace ends; a repeated start is idempotent.
    assert!(!rig.tick().await);
    let again = start(&rig.app, s.user, s.account, Some(&s.clerk_user_id), Initiator::User).await.unwrap();
    assert!(!again.created && again.deletion.id == d.id);

    // ── Grace ends: destructive phase, parked at blob_drain.
    end_grace(&owner, d.id).await;
    assert!(rig.tick().await);
    let parked = deletion(&owner, s.user).await;
    assert_eq!(parked.state, "running");
    assert!(parked.running_since.is_some());
    assert_eq!(parked.next_step(), Some("blob_drain"), "steps: {:?}, error: {:?}", parked.completed_steps, parked.last_error);
    assert_eq!(count(&owner, "SELECT count(*) FROM jobs WHERE account_id = $1 AND kind = 'DeleteSourceBlob' AND status = 'pending'", s.account).await, 3);

    // The cleanup workers drain; the next pass finishes.
    drain(&owner, &rig.app, s.account).await;
    make_due(&owner, parked.id).await;
    assert!(rig.tick().await);
    let done = deletion(&owner, s.user).await;
    assert_eq!(done.state, "completed", "last error: {:?}", done.last_error);
    assert_eq!(done.completed_steps.len(), STEPS.len());
    assert_eq!(done.stripe_outcome.as_deref(), Some("ended_now"));
    assert_eq!(done.forfeited_extra_usage_micro_usd, 5_000_000);

    // Each side effect exactly once.
    assert_eq!(rig.stripe.calls("DELETE", &sub_path).len(), 1);
    let del = &rig.stripe.calls("DELETE", &sub_path)[0];
    assert_eq!(del.form.get("prorate").map(String::as_str), Some("false"));
    assert_eq!(del.form.get("invoice_now").map(String::as_str), Some("false"));
    assert_eq!(rig.stripe.calls("DELETE", &format!("/v1/customers/{}", s.customer)).len(), 1, "Stripe customer deleted after the subscription");
    assert_eq!(rig.clerk.paths(), vec![format!("/v1/users/{}", s.clerk_user_id)]);
    assert_eq!(rig.connectors.calls.lock().unwrap().as_slice(), &[s.account]);
    assert_eq!(rig.mailer.sent.lock().unwrap().len(), 2, "started + completed");
    assert!(rig.mailer.sent.lock().unwrap()[1].subject.contains("deleted"));

    // Everything personal is gone.
    for (what, sql) in [
        ("graphs", "SELECT count(*) FROM graphs WHERE account_id = $1"),
        ("conversations", "SELECT count(*) FROM conversations WHERE account_id = $1"),
        ("oauth_tokens", "SELECT count(*) FROM oauth_tokens WHERE account_id = $1"),
        ("addin_tokens", "SELECT count(*) FROM addin_tokens WHERE account_id = $1"),
        ("members", "SELECT count(*) FROM account_members WHERE account_id = $1"),
        ("extra_usage_settings", "SELECT count(*) FROM extra_usage_settings WHERE account_id = $1"),
        ("escrow_shares (app role cannot delete)", "SELECT count(*) FROM escrow_shares WHERE account_id = $1"),
    ] {
        assert_eq!(count(&owner, sql, s.account).await, 0, "{what} must be deleted");
    }
    for (what, sql) in [
        ("nodes", "SELECT count(*) FROM nodes WHERE graph_id = $1"),
        ("op_log", "SELECT count(*) FROM op_log WHERE graph_id = $1"),
        ("node_case_fold_log (app role cannot touch)", "SELECT count(*) FROM node_case_fold_log WHERE graph_id = $1"),
    ] {
        assert_eq!(count(&owner, sql, s.graph).await, 0, "{what} must be deleted");
    }
    for (what, sql) in [
        ("user_private_keys", "SELECT count(*) FROM user_private_keys WHERE user_id = $1"),
        ("sessions", "SELECT count(*) FROM sessions WHERE user_id = $1"),
        ("user_prefs", "SELECT count(*) FROM user_prefs WHERE user_id = $1"),
    ] {
        assert_eq!(count(&owner, sql, s.user).await, 0, "{what} must be deleted");
    }
    let dek: Option<Vec<u8>> = sqlx::query_scalar("SELECT dek_wrapped FROM accounts WHERE id = $1").bind(s.account).fetch_one(&owner).await.unwrap();
    assert!(dek.is_none(), "the account DEK is destroyed");

    // The tombstone: account row kept, user anonymized.
    let (email, name, clerk, tz, deleted_at): (String, Option<String>, Option<String>, Option<String>, Option<chrono::DateTime<chrono::Utc>>) =
        sqlx::query_as("SELECT email, name, clerk_user_id, timezone, deleted_at FROM users WHERE id = $1")
            .bind(s.user)
            .fetch_one(&owner)
            .await
            .unwrap();
    assert_eq!(email, format!("deleted-{}@deleted.invalid", s.user));
    assert!(name.is_none() && clerk.is_none() && tz.is_none() && deleted_at.is_some());
    assert_eq!(count(&owner, "SELECT count(*) FROM accounts WHERE id = $1", s.account).await, 1);

    // Kept: the financial audit trail, the append-only retraction log.
    assert_eq!(count(&owner, "SELECT count(*) FROM credit_ledger WHERE account_id = $1", s.account).await, 1);
    assert_eq!(count(&owner, "SELECT count(*) FROM stripe_subscriptions WHERE account_id = $1", s.account).await, 1);
    assert_eq!(count(&owner, "SELECT count(*) FROM retraction_audit_log WHERE account_id = $1", s.account).await, 1);
    let kinds: Vec<(String, i64)> = sqlx::query_as("SELECT entry_kind, amount_micro_usd FROM extra_usage_ledger WHERE account_id = $1")
        .bind(s.account)
        .fetch_all(&owner)
        .await
        .unwrap();
    assert!(kinds.contains(&("topup".to_string(), 5_000_000)) && kinds.contains(&("adjustment".to_string(), -5_000_000)));
    assert_eq!(count(&owner, "SELECT balance_micro_usd FROM extra_usage_balances WHERE account_id = $1", s.account).await, 0);

    assert!(!rig.tick().await);
    assert_eq!(rig.clerk.delete_count(), 1);
    for (table, rows) in LISTING_TABLES.iter().zip(listing_rows(&owner, s.graph).await) {
        assert!(rows.as_array().unwrap().is_empty(), "account deletion must cascade through {table}");
    }
    assert_eq!(listing_rows(&owner, unrelated.graph).await, unrelated_listing,
        "another account's published and unfinished listing facts must remain byte-identical");
    println!("PROVED account deletion cascades all 11 populated listing relations, including guarded staging; unrelated graph unchanged");
    forget(&owner, &unrelated).await;
    forget(&owner, &s).await;
}

#[tokio::test]
async fn cancel_in_grace_restores_access_and_the_link_is_single_use() {
    let Some(owner) = pg_test_pool("account_deletion::cancel").await else { return };
    let s = seed(&owner).await;
    let rig = Rig::new(&owner).await;
    stripe_mock_for(&rig.stripe, &s);
    let d = rig.start_user(&s).await;
    assert!(rig.tick().await);
    assert_eq!(deletion(&owner, s.user).await.completed_steps.len(), FIRST_DESTRUCTIVE_STEP);

    // A token for another deletion does not cancel this one; a forged one is invalid.
    assert_eq!(verify_cancel_token(&key(), &format!("{}.{}", d.id, "00".repeat(32))), None);
    assert_eq!(verify_cancel_token(&derive_cancel_key(b"other"), &mint_cancel_token(&key(), d.id)), None);
    let token = mint_cancel_token(&key(), d.id);
    let id = verify_cancel_token(&key(), &token).unwrap();
    assert_eq!(cancel(&rig.app, id).await.unwrap(), CancelResult::Cancelled);
    assert_eq!(cancel(&rig.app, id).await.unwrap(), CancelResult::AlreadyCancelled, "a replay is idempotent");
    assert_eq!(cancel(&rig.app, Uuid::new_v4()).await.unwrap(), CancelResult::NotFound);

    let c = deletion(&owner, s.user).await;
    assert_eq!(c.state, "cancelled");
    assert!(c.cancelled_at.is_some());
    // Access restored: the gate no longer sees it.
    assert_eq!(
        sqlx::query_scalar::<_, Option<String>>("SELECT account_deletion_state_for_clerk_user($1)")
            .bind(&s.clerk_user_id)
            .fetch_one(&owner)
            .await
            .unwrap(),
        None
    );
    // Even past the original deadline the worker never runs a cancelled deletion.
    end_grace(&owner, d.id).await;
    assert!(!rig.tick().await);
    for sql in [
        "SELECT count(*) FROM graphs WHERE account_id = $1",
        "SELECT count(*) FROM oauth_tokens WHERE account_id = $1",
        "SELECT count(*) FROM escrow_shares WHERE account_id = $1",
    ] {
        assert_eq!(count(&owner, sql, s.account).await, 1);
    }
    assert_eq!(rig.clerk.delete_count(), 0);
    // Extra usage / auto-reload stay OFF after a cancel (re-enabling is the user's call).
    let (enabled, reload): (bool, bool) =
        sqlx::query_as("SELECT enabled, auto_reload_enabled FROM extra_usage_settings WHERE account_id = $1").bind(s.account).fetch_one(&owner).await.unwrap();
    assert_eq!((enabled, reload), (false, false));
    assert_eq!(count(&owner, "SELECT balance_micro_usd FROM extra_usage_balances WHERE account_id = $1", s.account).await, 5_000_000, "credits are not forfeited by a cancelled deletion");
    // Stripe stays ended (they can resubscribe) and the tokens stay revoked.
    assert_eq!(rig.stripe.calls("DELETE", &format!("/v1/subscriptions/{}", s.subscription)).len(), 1);
    assert_eq!(count(&owner, "SELECT count(*) FROM addin_tokens WHERE owner_user_id = $1 AND revoked_at IS NULL", s.user).await, 0);
    // A later request starts a fresh deletion.
    let again = rig.start_user(&s).await;
    assert_ne!(again.id, d.id);
    assert_eq!(again.state, "grace");
    forget(&owner, &s).await;
}

#[tokio::test]
async fn a_cancel_racing_the_grace_transition_has_exactly_one_winner() {
    let Some(owner) = pg_test_pool("account_deletion::cancel_race").await else { return };
    let s = seed(&owner).await;
    let rig = Rig::new(&owner).await;
    stripe_mock_for(&rig.stripe, &s);
    let d = rig.start_user(&s).await;
    assert!(rig.tick().await);
    end_grace(&owner, d.id).await;
    // The worker wins: flips to running and starts destroying.
    assert!(rig.tick().await);
    assert_eq!(deletion(&owner, s.user).await.state, "running");
    assert_eq!(cancel(&rig.app, d.id).await.unwrap(), CancelResult::TooLate, "past the point of no return");
    assert_eq!(deletion(&owner, s.user).await.state, "running");
    forget(&owner, &s).await;

    // And the other way round: a cancel that lands first means the worker never starts.
    let s = seed(&owner).await;
    let d = rig.start_user(&s).await;
    assert!(rig.tick().await);
    end_grace(&owner, d.id).await;
    assert_eq!(cancel(&rig.app, d.id).await.unwrap(), CancelResult::Cancelled);
    assert!(!rig.tick().await);
    assert_eq!(count(&owner, "SELECT count(*) FROM graphs WHERE account_id = $1", s.account).await, 1);
    forget(&owner, &s).await;
}

#[tokio::test]
async fn a_user_who_became_ineligible_during_grace_is_cancelled_not_deleted() {
    let Some(owner) = pg_test_pool("account_deletion::revalidate").await else { return };
    let s = seed(&owner).await;
    let rig = Rig::new(&owner).await;
    stripe_mock_for(&rig.stripe, &s);
    let d = rig.start_user(&s).await;
    assert!(rig.tick().await);
    // Seven days later the user is in a team org (a membership that raced the request).
    let (org_account, org_owner) = seed_org_account(&owner, "owner").await;
    let tier: Uuid = sqlx::query_scalar("SELECT id FROM seat_tiers WHERE code = 'standard' AND is_active = true").fetch_one(&owner).await.unwrap();
    sqlx::query("INSERT INTO account_members (account_id, user_id, role, seat_tier_id) VALUES ($1, $2, 'member', $3)")
        .bind(org_account).bind(s.user).bind(tier).execute(&owner).await.unwrap();
    end_grace(&owner, d.id).await;
    assert!(rig.tick().await);
    assert_eq!(deletion(&owner, s.user).await.state, "cancelled");
    assert_eq!(count(&owner, "SELECT count(*) FROM graphs WHERE account_id = $1", s.account).await, 1);
    assert_eq!(rig.clerk.delete_count(), 0);
    {
        let sent = rig.mailer.sent.lock().unwrap();
        assert_eq!(sent.len(), 2, "the start email, then the cancellation notice");
        assert!(sent[1].subject.contains("cancelled") && sent[1].to == s.email);
    }
    let _ = cleanup_account(&owner, org_account, &[org_owner]).await;
    forget(&owner, &s).await;
}

#[tokio::test]
async fn a_shared_personal_account_is_refused_and_never_wiped() {
    let Some(owner) = pg_test_pool("account_deletion::shared").await else { return };
    let s = seed(&owner).await;
    let rig = Rig::new(&owner).await;
    stripe_mock_for(&rig.stripe, &s);
    let other: Uuid = sqlx::query_scalar("INSERT INTO users (email, name) VALUES ($1, 'Grace') RETURNING id")
        .bind(format!("{}@other.test", Uuid::new_v4().simple())).fetch_one(&owner).await.unwrap();
    sqlx::query("INSERT INTO account_members (account_id, user_id, role) VALUES ($1, $2, 'admin')")
        .bind(s.account).bind(other).execute(&owner).await.unwrap();

    match start(&rig.app, s.user, s.account, Some(&s.clerk_user_id), Initiator::User).await {
        Err(StartError::SharedAccount(m)) => {
            assert_eq!(m.len(), 1);
            assert_eq!((m[0].user_id, m[0].account_id, m[0].role.as_str()), (other, s.account, "admin"));
            assert_eq!(m[0].name.as_deref(), Some("Grace"));
        }
        other => panic!("expected SharedAccount, got {:?}", other.map(|s| s.created)),
    }
    assert_eq!(count(&owner, "SELECT count(*) FROM account_deletions WHERE user_id = $1", s.user).await, 0);
    assert!(deletable_account_ids(&rig.app, s.user).await.unwrap().is_empty());

    // Clerk already deleted the user: the job cannot refuse, so it removes only
    // the departing user's own rows and leaves the co-owner's workspace alone.
    start(&rig.app, s.user, s.account, Some(&s.clerk_user_id), Initiator::ClerkWebhook).await.unwrap();
    assert!(rig.tick().await);
    let d = deletion(&owner, s.user).await;
    drain(&owner, &rig.app, s.account).await;
    make_due(&owner, d.id).await;
    rig.tick().await;
    assert_eq!(deletion(&owner, s.user).await.state, "completed");
    assert_eq!(count(&owner, "SELECT count(*) FROM graphs WHERE account_id = $1", s.account).await, 1, "the shared workspace survives");
    assert_eq!(count(&owner, "SELECT count(*) FROM atoms WHERE graph_id = $1", s.graph).await, 3);
    assert_eq!(count(&owner, "SELECT count(*) FROM account_members WHERE account_id = $1", s.account).await, 1, "only the co-owner remains");
    assert_eq!(rig.stripe.calls("DELETE", &format!("/v1/subscriptions/{}", s.subscription)).len(), 0, "a shared account's subscription is not ended");
    assert_eq!(count(&owner, "SELECT count(*) FROM user_private_keys WHERE user_id = $1", s.user).await, 0);
    let _ = sqlx::query("DELETE FROM account_members WHERE user_id = $1").bind(other).execute(&owner).await;
    forget(&owner, &s).await;
    let _ = sqlx::query("DELETE FROM users WHERE id = $1").bind(other).execute(&owner).await;
}

#[tokio::test]
async fn a_crash_between_steps_resumes_without_double_side_effects() {
    let Some(owner) = pg_test_pool("account_deletion::crash_resume").await else { return };
    let s = seed(&owner).await;
    let rig = Rig::new(&owner).await;
    stripe_mock_for(&rig.stripe, &s);
    rig.tenant.fail_next.store(true, Ordering::SeqCst);
    rig.clerk.fail_next.store(1, Ordering::SeqCst);

    let d = rig.start_user(&s).await;
    assert!(rig.tick().await);
    end_grace(&owner, d.id).await;
    assert!(rig.tick().await);
    let after_crash = deletion(&owner, s.user).await;
    assert_eq!(after_crash.next_step(), Some("tenant_planes"));
    assert!(after_crash.last_error.is_some());
    assert!(after_crash.next_attempt_at > Utc::now(), "backoff is in the future");
    let queued_once = count(&owner, "SELECT count(*) FROM jobs WHERE account_id = $1 AND kind = 'DeleteSourceBlob'", s.account).await;
    assert_eq!(queued_once, 3, "one durable blob-delete job per atom");
    assert!(!rig.tick().await, "not due yet: nothing is claimed");

    // Resume: tenant plane ok, Stripe customer ok, Clerk 500 -> fails at `clerk`.
    make_due(&owner, after_crash.id).await;
    assert!(rig.tick().await);
    let at_clerk = deletion(&owner, s.user).await;
    assert_eq!(at_clerk.next_step(), Some("clerk"));
    assert_eq!(rig.clerk.delete_count(), 1);

    // Resume again: Clerk ok; then park at blob_drain.
    make_due(&owner, at_clerk.id).await;
    assert!(rig.tick().await);
    let parked = deletion(&owner, s.user).await;
    assert!(parked.clerk_user_deleted);
    assert_eq!(parked.next_step(), Some("blob_drain"));
    assert_eq!(count(&owner, "SELECT count(*) FROM jobs WHERE account_id = $1 AND kind = 'DeleteSourceBlob'", s.account).await, queued_once);
    drain(&owner, &rig.app, s.account).await;
    make_due(&owner, parked.id).await;
    assert!(rig.tick().await);
    assert_eq!(deletion(&owner, s.user).await.state, "completed");

    assert_eq!(rig.stripe.calls("DELETE", &format!("/v1/subscriptions/{}", s.subscription)).len(), 1);
    assert_eq!(rig.stripe.calls("DELETE", &format!("/v1/customers/{}", s.customer)).len(), 1);
    assert_eq!(rig.connectors.calls.lock().unwrap().len(), 1);
    assert_eq!(rig.clerk.delete_count(), 2, "Clerk: one failed attempt, one success, none after");
    assert_eq!(rig.mailer.sent.lock().unwrap().len(), 2, "no duplicate emails");
    assert_eq!(count(&owner, "SELECT count(*) FROM extra_usage_ledger WHERE account_id = $1 AND entry_kind = 'adjustment'", s.account).await, 1);
    forget(&owner, &s).await;
}

#[tokio::test]
async fn billing_fails_loudly_when_stripe_is_missing_and_nothing_else_runs() {
    let Some(owner) = pg_test_pool("account_deletion::stripe_missing").await else { return };
    let s = seed(&owner).await;
    let rig = Rig::new(&owner).await;
    rig.start_user(&s).await;
    let mut ctx = rig.ctx();
    ctx.stripe = None;
    assert!(tick(&ctx).await.unwrap());
    let d = deletion(&owner, s.user).await;
    assert_eq!(d.next_step(), Some("stripe_cancel"), "nothing runs past a subscription we could not end");
    assert!(d.last_error.unwrap().contains("stripe"));
    assert_eq!(d.state, "grace");
    forget(&owner, &s).await;
}

#[tokio::test]
async fn start_refuses_a_team_org_member_and_ignores_personal_and_revoked_orgs() {
    let Some(owner) = pg_test_pool("account_deletion::team_org_rule").await else { return };
    let s = seed(&owner).await;
    let rig = Rig::new(&owner).await;
    let (org_account, org_owner) = seed_org_account(&owner, "owner").await;
    let org_clerk_id: String = sqlx::query_scalar("SELECT clerk_org_id FROM accounts WHERE id = $1").bind(org_account).fetch_one(&owner).await.unwrap();
    let tier: Uuid = sqlx::query_scalar("SELECT id FROM seat_tiers WHERE code = 'standard' AND is_active = true").fetch_one(&owner).await.unwrap();
    sqlx::query("INSERT INTO account_members (account_id, user_id, role, seat_tier_id) VALUES ($1, $2, 'member', $3)")
        .bind(org_account).bind(s.user).bind(tier).execute(&owner).await.unwrap();

    let orgs = match start(&rig.app, s.user, s.account, Some(&s.clerk_user_id), Initiator::User).await {
        Err(StartError::BlockedByOrgs(orgs)) => orgs,
        other => panic!("expected BlockedByOrgs, got {:?}", other.map(|s| s.created)),
    };
    assert_eq!((orgs.len(), orgs[0].clerk_org_id.as_str(), orgs[0].account_id, orgs[0].role.as_str()), (1, org_clerk_id.as_str(), org_account, "member"));
    assert_eq!(count(&owner, "SELECT count(*) FROM account_deletions WHERE user_id = $1", s.user).await, 0, "nothing was started");
    assert_eq!(count(&owner, "SELECT count(*) FROM addin_tokens WHERE owner_user_id = $1 AND revoked_at IS NOT NULL", s.user).await, 0, "no lockout either");

    sqlx::query("INSERT INTO org_revocations (clerk_org_id) VALUES ($1)").bind(&org_clerk_id).execute(&owner).await.unwrap();
    assert!(blocking_team_orgs(&rig.app, s.user).await.unwrap().is_empty());
    sqlx::query("DELETE FROM org_revocations WHERE clerk_org_id = $1").bind(&org_clerk_id).execute(&owner).await.unwrap();
    sqlx::query("DELETE FROM account_members WHERE account_id = $1 AND user_id = $2").bind(org_account).bind(s.user).execute(&owner).await.unwrap();
    assert!(blocking_team_orgs(&rig.app, s.user).await.unwrap().is_empty());

    let po_org = format!("org_{}", Uuid::new_v4().simple());
    sqlx::query("INSERT INTO personal_orgs (clerk_org_id, clerk_user_id) VALUES ($1, $2)").bind(&po_org).bind(&s.clerk_user_id).execute(&owner).await.unwrap();
    let po_account: Uuid = sqlx::query_scalar("INSERT INTO accounts (owner_user_id, clerk_org_id) VALUES ($1, $2) RETURNING id")
        .bind(s.user).bind(&po_org).fetch_one(&owner).await.unwrap();
    sqlx::query("INSERT INTO account_members (account_id, user_id, role, seat_tier_id) VALUES ($1, $2, 'owner', $3)")
        .bind(po_account).bind(s.user).bind(tier).execute(&owner).await.unwrap();
    assert!(blocking_team_orgs(&rig.app, s.user).await.unwrap().is_empty());
    assert_eq!(personal_account_ids(&rig.app, s.user).await.unwrap(), vec![s.account, po_account]);
    let _ = cleanup_account(&owner, po_account, &[]).await;
    let _ = cleanup_account(&owner, org_account, &[org_owner]).await;
    forget(&owner, &s).await;
}

#[tokio::test]
async fn the_personal_org_account_is_purged_with_the_personal_one_and_its_clerk_org_deleted() {
    let Some(owner) = pg_test_pool("account_deletion::personal_org_account").await else { return };
    quiesce(&owner).await;
    let (po_account, user, po_org) = seed_personal_org_account(&owner).await;
    let clerk_user_id: String = sqlx::query_scalar("SELECT clerk_user_id FROM users WHERE id = $1").bind(user).fetch_one(&owner).await.unwrap();
    let graph: Uuid = sqlx::query_scalar("INSERT INTO graphs (account_id, name, kind, is_primary) VALUES ($1, 'PO', 'personal', true) RETURNING id")
        .bind(po_account).fetch_one(&owner).await.unwrap();
    let rig = Rig::new(&owner).await;
    let d = start(&rig.app, user, po_account, Some(&clerk_user_id), Initiator::User).await.unwrap().deletion;
    assert!(rig.tick().await);
    end_grace(&owner, d.id).await;
    assert!(rig.tick().await);
    let d = deletion(&owner, user).await;
    assert_eq!(d.state, "completed", "last error: {:?}", d.last_error);
    assert_eq!(count(&owner, "SELECT count(*) FROM graphs WHERE id = $1", graph).await, 0);
    assert_eq!(sqlx::query_scalar::<_, i64>("SELECT count(*) FROM personal_orgs WHERE clerk_org_id = $1").bind(&po_org).fetch_one(&owner).await.unwrap(), 0);
    let paths = rig.clerk.paths();
    assert_eq!(paths, vec![format!("/v1/organizations/{po_org}"), format!("/v1/users/{clerk_user_id}")], "the personal Clerk org goes before the user");
    let _ = sqlx::query("DELETE FROM account_deletions WHERE user_id = $1").bind(user).execute(&owner).await;
    let _ = cleanup_account(&owner, po_account, &[user]).await;
}

#[tokio::test]
async fn the_user_deleted_webhook_joins_a_running_deletion_and_is_a_no_op_after_it() {
    let Some(owner) = pg_test_pool("account_deletion::webhook").await else { return };
    let s = seed(&owner).await;
    let rig = Rig::new(&owner).await;
    stripe_mock_for(&rig.stripe, &s);
    let d = rig.start_user(&s).await;
    crate::routes::clerk_webhook::handle_user_deleted(&rig.app, &json!({"id": s.clerk_user_id, "deleted": true})).await.unwrap();
    let joined = deletion(&owner, s.user).await;
    assert!(joined.clerk_user_deleted, "the job will skip its own Clerk call");
    assert_eq!(joined.initiated_by, "user");
    assert_eq!(count(&owner, "SELECT count(*) FROM account_deletions WHERE user_id = $1", s.user).await, 1);

    assert!(rig.tick().await);
    end_grace(&owner, d.id).await;
    assert!(rig.tick().await);
    drain(&owner, &rig.app, s.account).await;
    make_due(&owner, d.id).await;
    assert!(rig.tick().await);
    assert_eq!(deletion(&owner, s.user).await.state, "completed");
    assert_eq!(rig.clerk.delete_count(), 0);

    let before = deletion(&owner, s.user).await;
    crate::routes::clerk_webhook::handle_user_deleted(&rig.app, &json!({"id": s.clerk_user_id, "deleted": true})).await.unwrap();
    let after = deletion(&owner, s.user).await;
    assert_eq!((before.completed_steps, after.state.as_str()), (after.completed_steps.clone(), "completed"));
    assert_eq!(count(&owner, "SELECT count(*) FROM account_deletions WHERE user_id = $1", s.user).await, 1);
    assert!(!rig.tick().await);
    forget(&owner, &s).await;
}

#[tokio::test]
async fn a_clerk_dashboard_delete_detaches_memberships_before_any_user_row_is_purged() {
    let Some(owner) = pg_test_pool("account_deletion::webhook_external").await else { return };
    let s = seed(&owner).await;
    let rig = Rig::new(&owner).await;
    stripe_mock_for(&rig.stripe, &s);
    let (org_account, org_owner) = seed_org_account(&owner, "owner").await;
    let tier: Uuid = sqlx::query_scalar("SELECT id FROM seat_tiers WHERE code = 'standard' AND is_active = true").fetch_one(&owner).await.unwrap();
    sqlx::query("INSERT INTO account_members (account_id, user_id, role, seat_tier_id) VALUES ($1, $2, 'member', $3)")
        .bind(org_account).bind(s.user).bind(tier).execute(&owner).await.unwrap();
    // A row of the user's in that live team org, which the purge must NOT touch
    // while they are still a member (and must clean once they are not).
    sqlx::query("INSERT INTO queries (account_id, user_id, text) VALUES ($1, $2, 'team question')").bind(org_account).bind(s.user).execute(&owner).await.unwrap();

    crate::routes::clerk_webhook::handle_user_deleted(&rig.app, &json!({"id": s.clerk_user_id, "deleted": true})).await.unwrap();
    let d = deletion(&owner, s.user).await;
    assert_eq!((d.initiated_by.as_str(), d.state.as_str(), d.clerk_user_deleted), ("clerk_webhook", "running", true));
    assert!(rig.tick().await);
    assert_eq!(
        rig.tenant.detached_from.lock().unwrap().as_slice(),
        &[org_account],
        "detach found the org through account_members, i.e. it ran BEFORE the user-keyed purge removed that row"
    );
    drain(&owner, &rig.app, s.account).await;
    make_due(&owner, d.id).await;
    rig.tick().await;
    assert_eq!(deletion(&owner, s.user).await.state, "completed");
    assert_eq!(rig.clerk.delete_count(), 0, "Clerk already deleted the user");
    assert_eq!(count(&owner, "SELECT count(*) FROM queries WHERE account_id = $1", org_account).await, 0, "cleaned after detaching");
    // A webhook for a Clerk user we never provisioned is acked and starts nothing.
    crate::routes::clerk_webhook::handle_user_deleted(&rig.app, &json!({"id": "user_never_seen"})).await.unwrap();
    let _ = cleanup_account(&owner, org_account, &[org_owner]).await;
    forget(&owner, &s).await;
}

#[tokio::test]
async fn user_deleted_for_a_user_with_no_personal_account_anonymizes_the_row() {
    let Some(owner) = pg_test_pool("account_deletion::webhook_no_account").await else { return };
    quiesce(&owner).await;
    let sub = format!("user_{}", Uuid::new_v4().simple());
    let user: Uuid = sqlx::query_scalar("INSERT INTO users (email, clerk_user_id, name) VALUES ($1, $2, 'Nobody') RETURNING id")
        .bind(format!("{}@nobody.test", Uuid::new_v4().simple())).bind(&sub).fetch_one(&owner).await.unwrap();
    let app = app_pool(&owner).await;
    crate::routes::clerk_webhook::handle_user_deleted(&app, &json!({"id": sub})).await.unwrap();
    let (email, name, clerk): (String, Option<String>, Option<String>) =
        sqlx::query_as("SELECT email, name, clerk_user_id FROM users WHERE id = $1").bind(user).fetch_one(&owner).await.unwrap();
    assert_eq!(email, format!("deleted-{user}@deleted.invalid"));
    assert!(name.is_none() && clerk.is_none());
    let _ = sqlx::query("DELETE FROM users WHERE id = $1").bind(user).execute(&owner).await;
}

#[tokio::test]
async fn rows_in_organizations_the_user_already_left_are_deleted_though_rls_hides_them() {
    let Some(owner) = pg_test_pool("account_deletion::left_orgs").await else { return };
    let s = seed(&owner).await;
    let rig = Rig::new(&owner).await;
    stripe_mock_for(&rig.stripe, &s);
    let (org_account, org_owner) = seed_org_account(&owner, "owner").await;
    // The user left: no account_members row, but their rows remain in that account.
    sqlx::query("INSERT INTO queries (account_id, user_id, text) VALUES ($1, $2, 'old team question')").bind(org_account).bind(s.user).execute(&owner).await.unwrap();
    sqlx::query("INSERT INTO user_account_prefs (user_id, account_id) VALUES ($1, $2)").bind(s.user).bind(org_account).execute(&owner).await.unwrap();
    sqlx::query("INSERT INTO org_membership_tombstones (clerk_org_id, user_id, email) VALUES ($1, $2, $3)")
        .bind(format!("org_{}", Uuid::new_v4().simple())).bind(s.user).bind(&s.email).execute(&owner).await.unwrap();
    // The app role, scoped to its OWN account, cannot see the org account's rows.
    let mut tx = crate::tenancy::scope::begin_account_scoped(&rig.app, s.account).await.unwrap();
    let visible: i64 = sqlx::query_scalar("SELECT count(*) FROM queries WHERE user_id = $1 AND account_id = $2").bind(s.user).bind(org_account).fetch_one(&mut *tx).await.unwrap();
    tx.rollback().await.unwrap();
    assert_eq!(visible, 0, "RLS hides the left org's rows from the app role");

    let d = rig.run_to_completion(&s).await;
    assert_eq!(d.state, "completed", "last error: {:?}", d.last_error);
    assert_eq!(count(&owner, "SELECT count(*) FROM queries WHERE user_id = $1", s.user).await, 0);
    assert_eq!(count(&owner, "SELECT count(*) FROM user_account_prefs WHERE user_id = $1", s.user).await, 0);
    assert_eq!(count(&owner, "SELECT count(*) FROM org_membership_tombstones WHERE user_id = $1", s.user).await, 0, "no email left behind");
    // A late tombstone row (delayed Clerk event) is cleaned by the final purge too.
    let _ = cleanup_account(&owner, org_account, &[org_owner]).await;
    forget(&owner, &s).await;
}

#[tokio::test]
async fn the_privileged_functions_refuse_outside_the_destructive_phase() {
    let Some(owner) = pg_test_pool("account_deletion::definer_guards").await else { return };
    let s = seed(&owner).await;
    let rig = Rig::new(&owner).await;
    let d = rig.start_user(&s).await;
    for f in ["account_deletion_purge_restricted", "account_deletion_purge_user_rows"] {
        let r = sqlx::query(&format!("SELECT {f}($1)")).bind(d.id).execute(&rig.app).await;
        assert!(r.is_err(), "{f} must refuse while the deletion is in grace");
        let r = sqlx::query(&format!("SELECT {f}($1)")).bind(Uuid::new_v4()).execute(&rig.app).await;
        assert!(r.is_err(), "{f} must refuse an unknown deletion");
    }
    assert_eq!(count(&owner, "SELECT count(*) FROM escrow_shares WHERE account_id = $1", s.account).await, 1);
    // PUBLIC cannot execute them.
    let public_can: bool = sqlx::query_scalar("SELECT has_function_privilege('public', 'account_deletion_purge_restricted(uuid)', 'EXECUTE')")
        .fetch_one(&owner).await.unwrap();
    assert!(!public_can);
    forget(&owner, &s).await;
}

#[tokio::test]
async fn the_tenant_planes_wait_names_both_flags_and_the_stuck_sweep_alerts() {
    let Some(owner) = pg_test_pool("account_deletion::tenant_wait_and_alerts").await else { return };
    let s = seed(&owner).await;
    let rig = Rig::new(&owner).await;
    stripe_mock_for(&rig.stripe, &s);
    rig.tenant.remaining.store(2, Ordering::SeqCst);
    *rig.tenant.flags.lock().unwrap() = (true, false);
    let d = rig.start_user(&s).await;
    assert!(rig.tick().await);
    end_grace(&owner, d.id).await;
    assert!(rig.tick().await);
    let waiting = deletion(&owner, s.user).await;
    assert_eq!(waiting.next_step(), Some("tenant_planes"));
    let why = waiting.last_error.unwrap();
    assert!(why.contains("MEMBER_OCEANS_ENABLED (currently on)") && why.contains("MEMBER_OCEAN_PURGE_ENABLED (currently OFF)"), "{why}");

    assert_eq!(alert_stuck(&owner, 6).await.unwrap(), 0, "just started");
    sqlx::query("UPDATE account_deletions SET running_since = now() - interval '7 hours' WHERE id = $1").bind(d.id).execute(&owner).await.unwrap();
    assert_eq!(alert_stuck(&owner, 6).await.unwrap(), 1, "running for more than the threshold");
    forget(&owner, &s).await;
}

#[tokio::test]
async fn a_deleted_clerk_id_can_never_provision_or_resolve_a_user() {
    let Some(owner) = pg_test_pool("account_deletion::no_reprovision").await else { return };
    let s = seed(&owner).await;
    let rig = Rig::new(&owner).await;
    stripe_mock_for(&rig.stripe, &s);
    let state = crate::test_app_state();
    // Mid-deletion the user row still carries the id: a late webhook must not resolve it.
    let d = rig.start_user(&s).await;
    let r = crate::clerk_provision::ensure_user(&state, &rig.app, &s.clerk_user_id, Some(&s.email), None).await;
    assert!(matches!(r, Err(crate::clerk_provision::ProvisionError::AccountDeleted)), "existing user mid-deletion: {r:?}");

    assert!(rig.tick().await);
    end_grace(&owner, d.id).await;
    assert!(rig.tick().await);
    drain(&owner, &rig.app, s.account).await;
    make_due(&owner, d.id).await;
    assert!(rig.tick().await);
    assert_eq!(deletion(&owner, s.user).await.state, "completed");
    let users_before: i64 = sqlx::query_scalar("SELECT count(*) FROM users").fetch_one(&owner).await.unwrap();
    let r = crate::clerk_provision::ensure_user(&state, &rig.app, &s.clerk_user_id, Some(&s.email), None).await;
    assert!(matches!(r, Err(crate::clerk_provision::ProvisionError::AccountDeleted)), "{r:?}");
    assert_eq!(users_before, sqlx::query_scalar::<_, i64>("SELECT count(*) FROM users").fetch_one(&owner).await.unwrap());
    let fresh = crate::clerk_provision::ensure_user(&state, &rig.app, &format!("user_{}", Uuid::new_v4().simple()), Some(&s.email), None).await.unwrap();
    assert_ne!(fresh, s.user, "the same email with a brand new Clerk user is allowed");
    let _ = sqlx::query("DELETE FROM users WHERE id = $1").bind(fresh).execute(&owner).await;
    forget(&owner, &s).await;
}

#[tokio::test]
async fn billing_events_for_a_deleted_account_are_refused_refunded_and_alerted() {
    let Some(owner) = pg_test_pool("account_deletion::stripe_webhook_refusal").await else { return };
    let s = seed(&owner).await;
    let rig = Rig::new(&owner).await;
    rig.stripe.respond("POST", "/v1/refunds", 200, json!({"id": "re_1"}));
    rig.stripe.respond("DELETE", "/v1/subscriptions/sub_new", 200, json!({"id": "sub_new", "status": "canceled"}));
    rig.start_user(&s).await;
    assert_eq!(state_for_account(&rig.app, s.account).await.unwrap().as_deref(), Some("grace"));
    // A Checkout paid after deletion started.
    let object = json!({"id": "cs_1", "customer": s.customer, "mode": "subscription", "subscription": "sub_new", "payment_intent": "pi_1"});
    let resp = crate::routes::stripe_webhook::refuse_for_deleted_account(
        Some(&*rig.stripe.client), s.account, "evt_1", "checkout.session.completed", "grace", &object,
    )
    .await;
    assert_eq!(resp.status(), axum::http::StatusCode::OK);
    let refunds = rig.stripe.calls("POST", "/v1/refunds");
    assert_eq!(refunds.len(), 1);
    assert_eq!(refunds[0].form.get("payment_intent").map(String::as_str), Some("pi_1"));
    assert_eq!(refunds[0].idempotency_key.as_deref(), Some("deleted-account-refund:evt_1"), "a redelivery refunds once");
    assert_eq!(rig.stripe.calls("DELETE", "/v1/subscriptions/sub_new").len(), 1, "the subscription it started is ended");
    // Nothing was credited or granted.
    assert_eq!(count(&owner, "SELECT balance_micro_usd FROM extra_usage_balances WHERE account_id = $1", s.account).await, 5_000_000);
    // A cancelled deletion no longer blocks billing.
    let d = deletion(&owner, s.user).await;
    assert_eq!(cancel(&rig.app, d.id).await.unwrap(), CancelResult::Cancelled);
    assert_eq!(state_for_account(&rig.app, s.account).await.unwrap(), None);
    forget(&owner, &s).await;
}

#[test]
fn backoff_doubles_and_caps_at_an_hour() {
    assert_eq!(backoff_secs(0), 30);
    assert_eq!(backoff_secs(1), 30);
    assert_eq!(backoff_secs(2), 60);
    assert_eq!(backoff_secs(5), 480);
    assert_eq!(backoff_secs(50), 3600);
}

#[test]
fn steps_match_the_migration_constraint_and_grace_boundary() {
    let sql = include_str!("../../migrations_control/0370_account_deletions.sql");
    for step in STEPS {
        assert!(sql.contains(&format!("'{step}'")), "{step} missing from account_deletions_steps_known");
    }
    assert!(!is_destructive("notify_started") && is_destructive("forfeit") && is_destructive("tombstone"));
    // The migration's "nothing destructive in grace" list is exactly the destructive steps.
    for step in STEPS.iter().skip(FIRST_DESTRUCTIVE_STEP) {
        let tail = sql.split("account_deletions_grace_is_not_destructive").nth(1).unwrap();
        assert!(tail.contains(&format!("'{step}'")), "{step} missing from the grace constraint");
    }
}

#[test]
fn reverification_follows_clerks_second_factor_rule() {
    // Second factor enrolled: ONLY its age counts (a fresh first factor is not enough).
    assert!(reverification_ok(Some([600, 1])), "MFA user who just re-verified their second factor");
    assert!(!reverification_ok(Some([1, 600])), "fresh first factor, stale second factor");
    assert!(!reverification_ok(Some([600, 600])));
    assert!(!reverification_ok(Some([1, 10])), "Clerk compares afterMinutes > age, so exactly 10 is stale");
    assert!(reverification_ok(Some([1, 9])));
    // No second factor: the first factor's age.
    assert!(reverification_ok(Some([1, -1])));
    assert!(reverification_ok(Some([9, -1])));
    assert!(!reverification_ok(Some([10, -1])));
    assert!(!reverification_ok(Some([600, -1])));
    // Neither enrolled, or a v1 token with no claim.
    assert!(!reverification_ok(Some([-1, -1])));
    assert!(!reverification_ok(None));
}

#[test]
fn the_cancel_key_is_not_the_oauth_state_secret() {
    // The raw secret also keys digests admins can read (services::query_audit);
    // a token signed with it would make that a signing oracle. Ours must differ.
    use hmac::{Hmac, Mac};
    let id = Uuid::new_v4();
    let mut raw = Hmac::<Sha256>::new_from_slice(SECRET).unwrap();
    raw.update(format!("account-deletion-cancel:v1:{id}").as_bytes());
    let forged = format!("{id}.{}", hex::encode(raw.finalize().into_bytes()));
    assert_eq!(verify_cancel_token(&key(), &forged), None, "a token signed with the raw secret must not verify");
    assert_ne!(key().as_slice(), SECRET);
    assert_ne!(derive_cancel_key(b"a"), derive_cancel_key(b"b"));
    assert_eq!(derive_cancel_key(b"a"), derive_cancel_key(b"a"));
    // It is the shared KDF's key for this purpose, and no other purpose's key
    // opens a cancel link.
    use crate::crypto::kdf::{derive_key, purpose};
    assert_eq!(key(), derive_key(SECRET, purpose::ACCOUNT_DELETION_CANCEL));
    assert_ne!(key(), derive_key(SECRET, purpose::OAUTH_STATE));
    let token = mint_cancel_token(&key(), Uuid::new_v4());
    assert_eq!(verify_cancel_token(&derive_key(SECRET, purpose::OAUTH_STATE), &token), None);
}

#[tokio::test]
async fn temp_tables_cannot_redirect_the_definer_functions() {
    let Some(owner) = pg_test_pool("account_deletion::temp_table_trick").await else { return };
    let victim = seed(&owner).await;
    let app = app_pool(&owner).await;
    // The attacker is any session of the app role. They shadow the table the
    // functions trust with a temp table holding a forged `running` deletion
    // for the victim, then call the privileged functions.
    let mut conn = app.acquire().await.unwrap();
    sqlx::query("CREATE TEMP TABLE account_deletions (LIKE public.account_deletions INCLUDING DEFAULTS)").execute(&mut *conn).await.unwrap();
    let forged = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO pg_temp.account_deletions (id, user_id, account_id, initiated_by, state, running_since) \
         VALUES ($1, $2, $3, 'user', 'running', now())",
    )
    .bind(forged)
    .bind(victim.user)
    .bind(victim.account)
    .execute(&mut *conn)
    .await
    .unwrap();
    // Also shadow the other tables the functions read.
    for t in ["accounts", "account_members", "users", "graphs"] {
        sqlx::query(&format!("CREATE TEMP TABLE {t} (LIKE public.{t} INCLUDING DEFAULTS)")).execute(&mut *conn).await.unwrap();
    }
    for f in ["account_deletion_purge_user_rows", "account_deletion_purge_restricted"] {
        let r = sqlx::query(&format!("SELECT public.{f}($1)")).bind(forged).execute(&mut *conn).await;
        assert!(r.is_err(), "{f} must not honor a forged row in a temp table");
    }
    // The state functions do not see it either.
    let st: Option<String> = sqlx::query_scalar("SELECT public.account_deletion_state_for_account($1)").bind(victim.account).fetch_one(&mut *conn).await.unwrap();
    assert_eq!(st, None);
    let c: String = sqlx::query_scalar("SELECT public.account_deletion_cancel($1)").bind(forged).fetch_one(&mut *conn).await.unwrap();
    assert_eq!(c, "not_found", "cancel looks at the real table only");
    drop(conn);
    // Nothing of the victim's was deleted.
    assert_eq!(count(&owner, "SELECT count(*) FROM escrow_shares WHERE account_id = $1", victim.account).await, 1);
    assert_eq!(count(&owner, "SELECT count(*) FROM user_private_keys WHERE user_id = $1", victim.user).await, 1);
    assert_eq!(count(&owner, "SELECT count(*) FROM account_members WHERE user_id = $1", victim.user).await, 1);
    assert_eq!(count(&owner, "SELECT count(*) FROM user_prefs WHERE user_id = $1", victim.user).await, 1);
    // And every definer function in the migration pins pg_temp last.
    let unsafe_fns: Vec<String> = sqlx::query_scalar(
        "SELECT p.proname::text FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace \
          WHERE n.nspname = 'public' AND p.proname LIKE 'account_deletion%' AND p.prosecdef \
            AND NOT EXISTS (SELECT 1 FROM unnest(p.proconfig) c WHERE c = 'search_path=pg_catalog, public, pg_temp')",
    )
    .fetch_all(&owner)
    .await
    .unwrap();
    assert!(unsafe_fns.is_empty(), "definer functions without a pinned search_path ending in pg_temp: {unsafe_fns:?}");
    forget(&owner, &victim).await;
}

#[tokio::test]
async fn a_clerk_dashboard_delete_of_a_co_owner_leaves_the_shared_accounts_billing_alone() {
    let Some(owner) = pg_test_pool("account_deletion::co_owner_billing").await else { return };
    let s = seed(&owner).await;
    let rig = Rig::new(&owner).await;
    rig.stripe.respond("POST", "/v1/refunds", 200, json!({"id": "re_1"}));
    // The seeded personal account is SHARED with a surviving co-owner.
    let other: Uuid = sqlx::query_scalar("INSERT INTO users (email, name) VALUES ($1, 'Grace') RETURNING id")
        .bind(format!("{}@other.test", Uuid::new_v4().simple())).fetch_one(&owner).await.unwrap();
    sqlx::query("INSERT INTO account_members (account_id, user_id, role) VALUES ($1, $2, 'owner')").bind(s.account).bind(other).execute(&owner).await.unwrap();

    // Only shared account: anchored on it, but billing is NOT treated as deleted.
    crate::routes::clerk_webhook::handle_user_deleted(&rig.app, &json!({"id": s.clerk_user_id})).await.unwrap();
    let d = deletion(&owner, s.user).await;
    assert_eq!(d.account_id, s.account, "the only account is the fallback anchor");
    assert_eq!(state_for_account(&rig.app, s.account).await.unwrap(), None, "the surviving co-owner's billing is not refused");

    // Second scenario: the user ALSO has a personal-org account of their own. The
    // deletion is anchored on THAT one; the shared one stays out of the guard.
    let _ = sqlx::query("DELETE FROM account_deletions WHERE user_id = $1").bind(s.user).execute(&owner).await;
    let po_org = format!("org_{}", Uuid::new_v4().simple());
    sqlx::query("INSERT INTO personal_orgs (clerk_org_id, clerk_user_id) VALUES ($1, $2)").bind(&po_org).bind(&s.clerk_user_id).execute(&owner).await.unwrap();
    let po_account: Uuid = sqlx::query_scalar("INSERT INTO accounts (owner_user_id, clerk_org_id) VALUES ($1, $2) RETURNING id")
        .bind(s.user).bind(&po_org).fetch_one(&owner).await.unwrap();
    let tier: Uuid = sqlx::query_scalar("SELECT id FROM seat_tiers WHERE code = 'standard' AND is_active = true").fetch_one(&owner).await.unwrap();
    sqlx::query("INSERT INTO account_members (account_id, user_id, role, seat_tier_id) VALUES ($1, $2, 'owner', $3)").bind(po_account).bind(s.user).bind(tier).execute(&owner).await.unwrap();
    crate::routes::clerk_webhook::handle_user_deleted(&rig.app, &json!({"id": s.clerk_user_id})).await.unwrap();
    let d = deletion(&owner, s.user).await;
    assert_eq!(d.account_id, po_account, "anchored on the first account the user may actually wipe");
    assert_eq!(state_for_account(&rig.app, po_account).await.unwrap().as_deref(), Some("running"));
    assert_eq!(state_for_account(&rig.app, s.account).await.unwrap(), None, "the shared account is never reported as deleted");
    // A payment on the shared account is processed normally: no refund is issued.
    assert!(rig.stripe.calls("POST", "/v1/refunds").is_empty());

    let _ = sqlx::query("DELETE FROM account_deletions WHERE user_id = $1").bind(s.user).execute(&owner).await;
    let _ = sqlx::query("DELETE FROM personal_orgs WHERE clerk_org_id = $1").bind(&po_org).execute(&owner).await;
    let _ = cleanup_account(&owner, po_account, &[]).await;
    let _ = sqlx::query("DELETE FROM account_members WHERE user_id = $1").bind(other).execute(&owner).await;
    forget(&owner, &s).await;
    let _ = sqlx::query("DELETE FROM users WHERE id = $1").bind(other).execute(&owner).await;
}

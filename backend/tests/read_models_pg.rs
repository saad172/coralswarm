//! Actual migrations, non-bypass logins, real compute and handler lifecycle.
//! Required local URL, never a green skipped suite.
use coralswarm_backend::{
    read_models::{
        self as rm,
        failure::{self, FailureKind},
        oceans::{WorkspaceCounts as Model, WorkspaceCountsParams as P},
        Params, ReadModel,
    },
    store::PostgresStore,
    tenancy::{scope::begin_tenant_scoped, TenantId},
    test_db::{migrated_database, Plane, TestDatabase},
    workers::{read_model_refresh, tenant_harness::WorkerTenant},
};
use serde_json::{json, Value};
use sha2::Digest;
use sqlx::{
    postgres::{PgConnectOptions, PgPoolOptions},
    Executor, PgPool,
};
use std::{
    str::FromStr,
    sync::Arc,
    time::{Duration, Instant},
};
use uuid::Uuid;
#[path = "support/conversation_list_bound.rs"]
mod conversation_list_bound;
#[path = "support/personal_addon_tokens.rs"]
mod personal_addon_tokens;
struct Fixture {
    _db: TestDatabase,
    owner: PgPool,
    app: PgPool,
    control: PgPool,
    _control_db: Option<TestDatabase>,
    account: Uuid,
    user: Uuid,
    other: Uuid,
    graph: Uuid,
    foreign: Uuid,
    org: bool,
}
impl Fixture {
    async fn domain_scope<M: rm::ReadModel>(
        &self,
        query: std::collections::BTreeMap<String, String>,
    ) -> rm::Scope<M::Params> {
        let mut tx = begin_tenant_scoped(&self.app, self.account, self.user)
            .await
            .unwrap();
        let p = M::Params::parse(Default::default(), query)
            .unwrap()
            .normalize(&mut tx, self.user)
            .await
            .unwrap();
        let mut viewer = rm::Viewer::load(&mut tx, self.account, self.user)
            .await
            .unwrap();
        if M::CONTROL_FACTS {
            let mut control = begin_tenant_scoped(&self.control, self.account, self.user)
                .await
                .unwrap();
            viewer.certify_control(&mut control).await.unwrap();
        }
        viewer.scope::<M>(p).unwrap()
    }
    async fn domain<M: rm::ReadModel>(
        &self,
        query: std::collections::BTreeMap<String, String>,
    ) -> Value {
        let mut tx = begin_tenant_scoped(&self.app, self.account, self.user)
            .await
            .unwrap();
        let p = M::Params::parse(Default::default(), query)
            .unwrap()
            .normalize(&mut tx, self.user)
            .await
            .unwrap();
        let mut viewer = rm::Viewer::load(&mut tx, self.account, self.user)
            .await
            .unwrap();
        if M::CONTROL_FACTS {
            let mut control = begin_tenant_scoped(&self.control, self.account, self.user)
                .await
                .unwrap();
            viewer.certify_control(&mut control).await.unwrap();
        }
        let scope = viewer.scope::<M>(p).unwrap();
        let missing = rm::store::read::<M>(&mut tx, &scope).await.unwrap();
        assert!(
            missing.computed_at.is_none(),
            "{} should miss before worker",
            M::NAME
        );
        tx.rollback().await.unwrap();
        rm::store::request_refresh::<M>(&self.app, &scope)
            .await
            .unwrap();
        let claim = self.claim().await;
        assert_eq!(claim.model, M::NAME);
        let (payload, snapshot) = self.compute(&claim).await;
        assert!(
            rm::store::publish(&self.app, &claim, payload.clone(), &snapshot)
                .await
                .unwrap()
        );
        let mut tx = begin_tenant_scoped(&self.app, self.account, self.user)
            .await
            .unwrap();
        let stored = rm::store::read::<M>(&mut tx, &scope).await.unwrap();
        assert!(
            stored.computed_at.is_some(),
            "{} must be filled by worker",
            M::NAME
        );
        assert_eq!(serde_json::to_value(stored.payload).unwrap(), payload);
        tx.rollback().await.unwrap();
        payload
    }
    async fn new(org: bool) -> Self {
        let url = std::env::var("READ_MODEL_TEST_BASE_URL")
            .expect("READ_MODEL_TEST_BASE_URL required; no skipped read-model proof");
        let opts = PgConnectOptions::from_str(&url).unwrap();
        assert!(matches!(opts.get_host(), "localhost" | "127.0.0.1"));
        let db = migrated_database(
            &opts,
            if org { Plane::Tenant } else { Plane::Control },
            "read_models",
        )
        .await;
        let opts = opts.database(db.name());
        let control_opts = opts.clone();
        let owner = PgPoolOptions::new()
            .max_connections(4)
            .connect_with(opts.clone())
            .await
            .unwrap();
        let role = format!("read_model_{}", Uuid::new_v4().simple());
        let password = Uuid::new_v4().to_string();
        let base = if org {
            "coralswarm_tenant_app"
        } else {
            "coralswarm_app"
        };
        owner.execute(format!("CREATE ROLE {role} LOGIN INHERIT NOBYPASSRLS NOSUPERUSER NOCREATEDB NOCREATEROLE PASSWORD '{password}' IN ROLE {base}").as_str()).await.unwrap();
        let app = PgPoolOptions::new()
            .max_connections(4)
            .connect_with(opts.username(&role).password(&password))
            .await
            .unwrap();
        let attrs:(bool,bool,i64)=sqlx::query_as("SELECT rolsuper,rolbypassrls,(SELECT count(*) FROM pg_class WHERE relnamespace='public'::regnamespace AND relowner=(SELECT oid FROM pg_roles WHERE rolname=current_user)) FROM pg_roles WHERE rolname=current_user").fetch_one(&app).await.unwrap();
        assert_eq!(attrs, (false, false, 0));
        let (account, user, other) = if org {
            sqlx::query("INSERT INTO tenant_meta(id,org_id) VALUES(true,'org_read_models')")
                .execute(&owner)
                .await
                .unwrap();
            (Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4())
        } else {
            let mut ids = vec![];
            for _ in 0..2 {
                let user: Uuid =
                    sqlx::query_scalar("INSERT INTO users(email) VALUES($1) RETURNING id")
                        .bind(format!("read-model-{}@test.local", Uuid::new_v4()))
                        .fetch_one(&owner)
                        .await
                        .unwrap();
                let acc: Uuid = sqlx::query_scalar(
                    "INSERT INTO accounts(owner_user_id) VALUES($1) RETURNING id",
                )
                .bind(user)
                .fetch_one(&owner)
                .await
                .unwrap();
                sqlx::query(
                    "INSERT INTO account_members(account_id,user_id,role) VALUES($1,$2,'owner')",
                )
                .bind(acc)
                .bind(user)
                .execute(&owner)
                .await
                .unwrap();
                ids.push((acc, user));
            }
            (ids[0].0, ids[0].1, ids[1].1)
        };
        let graph:Uuid=sqlx::query_scalar("INSERT INTO graphs(account_id,name,kind,owner_user_id) VALUES($1,'Read-model fixture','member',$2) RETURNING id").bind(account).bind(user).fetch_one(&owner).await.unwrap();
        let foreign:Uuid=sqlx::query_scalar("INSERT INTO graphs(account_id,name,kind,owner_user_id) VALUES($1,'Foreign fixture','member',$2) RETURNING id").bind(account).bind(other).fetch_one(&owner).await.unwrap();
        let (control, control_db) = if org {
            let db = migrated_database(
                &control_opts.clone().database("postgres"),
                Plane::Control,
                "read_model_identity",
            )
            .await;
            let owner = PgPoolOptions::new()
                .max_connections(2)
                .connect_with(control_opts.clone().database(db.name()))
                .await
                .unwrap();
            for actor in [user, other] {
                sqlx::query("INSERT INTO users(id,email) VALUES($1,$2)")
                    .bind(actor)
                    .bind(format!("rm-identity-{actor}@test.local"))
                    .execute(&owner)
                    .await
                    .unwrap();
            }
            sqlx::query("INSERT INTO accounts(id,owner_user_id,clerk_org_id) VALUES($1,$2,'org_read_models')").bind(account).bind(user).execute(&owner).await.unwrap();
            for actor in [user, other] {
                sqlx::query(
                    "INSERT INTO account_members(account_id,user_id,role,seat_tier_id) VALUES($1,$2,'member',(SELECT id FROM seat_tiers WHERE code='standard' AND is_active=true))",
                )
                .bind(account)
                .bind(actor)
                .execute(&owner)
                .await
                .unwrap();
            }
            let role = format!("read_model_control_{}", Uuid::new_v4().simple());
            owner.execute(format!("CREATE ROLE {role} LOGIN INHERIT NOBYPASSRLS NOSUPERUSER NOCREATEDB NOCREATEROLE PASSWORD '{password}' IN ROLE coralswarm_app").as_str()).await.unwrap();
            let control = PgPoolOptions::new()
                .max_connections(2)
                .connect_with(
                    control_opts
                        .database(db.name())
                        .username(&role)
                        .password(&password),
                )
                .await
                .unwrap();
            owner.close().await;
            (control, Some(db))
        } else {
            (app.clone(), None)
        };
        Self {
            _db: db,
            owner,
            app,
            control,
            _control_db: control_db,
            account,
            user,
            other,
            graph,
            foreign,
            org,
        }
    }
    async fn scope(&self) -> rm::Scope<P> {
        let mut tx = begin_tenant_scoped(&self.app, self.account, self.user)
            .await
            .unwrap();
        let viewer = rm::Viewer::load(&mut tx, self.account, self.user)
            .await
            .unwrap();
        viewer.scope::<Model>(P { graph: self.graph }).unwrap()
    }
    async fn answer(&self) -> rm::Stored<coralswarm_backend::store::WorkspaceCounts> {
        let scope = self.scope().await;
        let mut tx = begin_tenant_scoped(&self.app, self.account, self.user)
            .await
            .unwrap();
        rm::store::read::<Model>(&mut tx, &scope).await.unwrap()
    }
    fn worker(&self) -> WorkerTenant {
        WorkerTenant {
            tenant: if self.org {
                TenantId::Org("org_read_models".into())
            } else {
                TenantId::Consumer
            },
            pool: self.app.clone(),
            store: Arc::new(PostgresStore::new(self.app.clone())),
        }
    }
    async fn enqueue(&self) {
        rm::store::request_refresh::<Model>(&self.app, &self.scope().await)
            .await
            .unwrap();
    }
    async fn claim(&self) -> rm::store::Claim {
        let mut claims = rm::store::claim(&self.app).await.unwrap();
        assert_eq!(claims.len(), 1);
        claims.remove(0)
    }
    async fn compute(&self, c: &rm::store::Claim) -> (Value, String) {
        read_model_refresh::compute(&self.worker(), c, &self.control)
            .await
            .unwrap()
    }
    async fn finish(&self) {
        self.enqueue().await;
        let c = self.claim().await;
        let (v, s) = self.compute(&c).await;
        assert!(rm::store::publish(&self.app, &c, v, &s).await.unwrap());
    }
    async fn atom(&self, name: &str) -> Uuid {
        sqlx::query_scalar("INSERT INTO atoms(graph_id,source_type,source_id,content_blob_key,content_inline,timestamp) VALUES($1,'note',$2,'fixture','Synthetic evidence',now()) RETURNING atom_id").bind(self.graph).bind(name).fetch_one(&self.owner).await.unwrap()
    }
    async fn close(self) {
        self.control.close().await;
        self.app.close().await;
        self.owner.close().await;
    }
}
async fn l3_domain_lifecycle(org: bool) {
    use rm::{chats, connectors, home, today};
    let f = Fixture::new(org).await;
    let atom = f.atom("l3-evidence").await;
    sqlx::query("UPDATE atoms SET source_type='slack',ingested_by=$2 WHERE atom_id=$1")
        .bind(atom)
        .bind(f.user)
        .execute(&f.owner)
        .await
        .unwrap();
    sqlx::query("INSERT INTO conversations(account_id,graph_id,user_id,title) VALUES($1,$2,$3,'Unsafe generated title')")
        .bind(f.account).bind(f.graph).bind(f.user).execute(&f.owner).await.unwrap();
    let metrics = f.domain::<home::HomeMetrics>(Default::default()).await;
    assert_eq!(metrics["kpis"]["corals_ingested"]["total"], 1);
    f.domain::<home::WorkspaceActivity>(Default::default())
        .await;
    let now = chrono::Utc::now();
    let range: std::collections::BTreeMap<String, String> = [
        (
            "from".into(),
            (now - chrono::Duration::days(1)).to_rfc3339(),
        ),
        ("to".into(), (now + chrono::Duration::hours(1)).to_rfc3339()),
        ("tz".into(), "UTC".into()),
    ]
    .into();
    let activities = f.domain::<today::JournalActivities>(range.clone()).await;
    assert_eq!(activities["activities"].as_array().unwrap().len(), 1);
    f.domain::<today::JournalStrip>(Default::default()).await;
    f.domain::<today::JournalGist>(Default::default()).await;
    let mut rollups = range;
    rollups.remove("tz");
    f.domain::<today::JournalRollups>(rollups).await;
    let catalog = f
        .domain::<connectors::ConnectorsList>(Default::default())
        .await;
    assert!(!catalog["connectors"].as_array().unwrap().is_empty());
    f.domain::<connectors::ConnectorDetail>([("provider".into(), "google".into())].into())
        .await;
    f.domain::<connectors::ConnectorStatus>(Default::default())
        .await;
    f.domain::<connectors::TeamsTranscriptCaptures>(Default::default())
        .await;
    let list = f
        .domain::<chats::ConversationList>(Default::default())
        .await;
    let rows = list["conversations"].as_array().unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["title"], "Conversation");
    assert_eq!(rows[0]["oceans"][0]["ocean_name"], "Read-model fixture");
    f.close().await;
}
#[tokio::test]
async fn l3_all_models_real_worker_consumer() {
    l3_domain_lifecycle(false).await
}
#[tokio::test]
async fn l3_all_models_real_worker_tenant() {
    l3_domain_lifecycle(true).await
}

async fn control_fact_fence(org: bool) {
    use rm::connectors::ConnectorsList as C;
    let f = Fixture::new(org).await;
    let old = f.domain_scope::<C>(Default::default()).await;
    rm::store::request_refresh::<C>(&f.app, &old).await.unwrap();
    let claim = f.claim().await;
    let (payload, snapshot) = f.compute(&claim).await;
    // Mutation has no access to a tenant pool. Durable revision must still
    // make a pre-change first-fill publication unreachable after commit.
    let mut tx = begin_tenant_scoped(&f.control, f.account, f.user)
        .await
        .unwrap();
    sqlx::query("INSERT INTO oauth_tokens(account_id,provider,scope,owner_user_id,access_token) VALUES($1,'google_gmail','gmail.readonly',$2,'\\x00'::bytea)")
        .bind(f.account).bind(f.user).execute(&mut *tx).await.unwrap();
    tx.commit().await.unwrap();
    assert!(rm::store::publish(&f.app, &claim, payload, &snapshot)
        .await
        .unwrap());
    let current = f.domain_scope::<C>(Default::default()).await;
    assert_ne!(old.key(), current.key());
    let mut tx = begin_tenant_scoped(&f.app, f.account, f.user)
        .await
        .unwrap();
    assert!(rm::store::read::<C>(&mut tx, &current)
        .await
        .unwrap()
        .computed_at
        .is_none());
    tx.rollback().await.unwrap();
    // A worker that starts later refuses the old revision before reading facts.
    rm::store::request_refresh::<C>(&f.app, &old).await.unwrap();
    let obsolete = f.claim().await;
    assert!(
        read_model_refresh::compute(&f.worker(), &obsolete, &f.control)
            .await
            .is_err()
    );
    // Same-account credentials belonging to the other viewer never appear.
    let mut tx = begin_tenant_scoped(&f.control, f.account, f.user)
        .await
        .unwrap();
    sqlx::query("UPDATE oauth_tokens SET credential_invalid_at=now() WHERE account_id=$1 AND owner_user_id=$2")
        .bind(f.account).bind(f.user).execute(&mut *tx).await.unwrap();
    tx.commit().await.unwrap();
    assert_ne!(
        current.key(),
        f.domain_scope::<C>(Default::default()).await.key()
    );
    f.close().await;
}
#[tokio::test]
async fn l3_control_facts_fence_consumer() {
    control_fact_fence(false).await
}
#[tokio::test]
async fn l3_control_facts_fence_tenant() {
    control_fact_fence(true).await
}

async fn control_counter_member_writes_forbidden(org: bool) {
    let f = Fixture::new(org).await;
    let mut tx = begin_tenant_scoped(&f.control, f.account, f.user)
        .await
        .unwrap();
    let attrs: (bool, bool) =
        sqlx::query_as("SELECT rolsuper,rolbypassrls FROM pg_roles WHERE rolname=current_user")
            .fetch_one(&mut *tx)
            .await
            .unwrap();
    assert_eq!(attrs, (false, false));
    let before: (i64, i64) = sqlx::query_as(
        "SELECT revision,freshness FROM read_model_control_revisions WHERE account_id=$1",
    )
    .bind(f.account)
    .fetch_one(&mut *tx)
    .await
    .unwrap();
    tx.rollback().await.unwrap();
    for statement in [
        "INSERT INTO read_model_control_revisions(account_id,revision,freshness) VALUES($1,0,0) ON CONFLICT(account_id) DO UPDATE SET revision=0,freshness=0",
        "UPDATE read_model_control_revisions SET revision=0,freshness=0 WHERE account_id=$1",
        "DELETE FROM read_model_control_revisions WHERE account_id=$1",
    ] {
        let mut tx = begin_tenant_scoped(&f.control, f.account, f.user).await.unwrap();
        let result = sqlx::query(statement).bind(f.account).execute(&mut *tx).await;
        // Roll back even if this deliberately fails against the original ACL.
        tx.rollback().await.unwrap();
        let error = result.expect_err("member code must not rewrite authority/freshness counters");
        assert_eq!(error.as_database_error().and_then(|e| e.code()).as_deref(), Some("42501"));
    }
    let mut tx = begin_tenant_scoped(&f.control, f.account, f.user)
        .await
        .unwrap();
    let acl: (bool, bool, bool, bool) = sqlx::query_as("SELECT has_table_privilege(current_user,'read_model_control_revisions','SELECT'),has_table_privilege(current_user,'read_model_control_revisions','INSERT'),has_table_privilege(current_user,'read_model_control_revisions','UPDATE'),has_table_privilege(current_user,'read_model_control_revisions','DELETE')")
        .fetch_one(&mut *tx).await.unwrap();
    assert_eq!(acl, (true, false, false, false));
    let policy: String = sqlx::query_scalar("SELECT cmd FROM pg_policies WHERE schemaname='public' AND tablename='read_model_control_revisions' AND policyname='read_model_control_account'")
        .fetch_one(&mut *tx).await.unwrap();
    assert_eq!(policy, "SELECT");
    // Legitimate fact writes must still advance counters through the definer.
    sqlx::query("INSERT INTO oauth_tokens(account_id,provider,scope,owner_user_id,access_token) VALUES($1,'google_gmail','gmail.readonly',$2,'\\x00'::bytea)")
        .bind(f.account).bind(f.user).execute(&mut *tx).await.unwrap();
    let after: (i64, i64) = sqlx::query_as(
        "SELECT revision,freshness FROM read_model_control_revisions WHERE account_id=$1",
    )
    .bind(f.account)
    .fetch_one(&mut *tx)
    .await
    .unwrap();
    assert!(after.0 > before.0 && after.1 > before.1);
    tx.commit().await.unwrap();
    let mut tx = begin_tenant_scoped(&f.control, Uuid::new_v4(), f.user)
        .await
        .unwrap();
    let visible: i64 = sqlx::query_scalar("SELECT count(*) FROM read_model_control_revisions")
        .fetch_one(&mut *tx)
        .await
        .unwrap();
    assert_eq!(visible, 0, "SELECT remains account constrained");
    tx.rollback().await.unwrap();
    f.close().await;
}
#[tokio::test]
async fn l3_control_counter_member_writes_forbidden_consumer() {
    control_counter_member_writes_forbidden(false).await
}
#[tokio::test]
async fn l3_control_counter_member_writes_forbidden_tenant() {
    control_counter_member_writes_forbidden(true).await
}

async fn graphless_chat_dirty(org: bool) {
    let f = Fixture::new(org).await;
    let mut tx =
        coralswarm_backend::tenancy::scope::begin_tenant_system_scoped_without_account(&f.owner)
            .await
            .unwrap();
    rm::registry::sync_triggers(&mut tx).await.unwrap();
    tx.commit().await.unwrap();
    sqlx::query("INSERT INTO read_models(model,scope_key,version,account_id,graph_ids,viewer_user_id,compute_as) VALUES('conversation_list','graphless',1,$1,'{}',$2,$2)")
        .bind(f.account).bind(f.user).execute(&f.owner).await.unwrap();
    let id:Uuid=sqlx::query_scalar("INSERT INTO conversations(account_id,graph_id,user_id,title) VALUES($1,$2,$3,'Synthetic title') RETURNING id")
        .bind(f.account).bind(f.graph).bind(f.user).fetch_one(&f.owner).await.unwrap();
    sqlx::query(
        "UPDATE read_models SET dirty_at=NULL,dirty_bumped_at=NULL WHERE scope_key='graphless'",
    )
    .execute(&f.owner)
    .await
    .unwrap();
    sqlx::query("INSERT INTO conversation_messages(conversation_id,seq,role,text) VALUES($1,1,'user','Synthetic question')")
        .bind(id).execute(&f.owner).await.unwrap();
    let dirty: bool = sqlx::query_scalar(
        "SELECT dirty_at IS NOT NULL FROM read_models WHERE scope_key='graphless'",
    )
    .fetch_one(&f.owner)
    .await
    .unwrap();
    assert!(
        dirty,
        "message must dirty owner list even with no scope graphs"
    );
    sqlx::query(
        "UPDATE read_models SET dirty_at=NULL,dirty_bumped_at=NULL WHERE scope_key='graphless'",
    )
    .execute(&f.owner)
    .await
    .unwrap();
    sqlx::query("UPDATE conversations SET title='Changed' WHERE id=$1")
        .bind(id)
        .execute(&f.owner)
        .await
        .unwrap();
    let invalidated:bool=sqlx::query_scalar("SELECT dirty_at IS NOT NULL AND invalidated_xid IS NOT NULL FROM read_models WHERE scope_key='graphless'").fetch_one(&f.owner).await.unwrap();
    assert!(
        invalidated,
        "title mutation must invalidate graphless owner list"
    );
    f.close().await;
}
#[tokio::test]
async fn l3_graphless_chat_dirty_consumer() {
    graphless_chat_dirty(false).await
}
#[tokio::test]
async fn l3_graphless_chat_dirty_tenant() {
    graphless_chat_dirty(true).await
}

// No registered subscription or cached row may be needed to certify an
// owner's graph-independent mutation. Keep real member snapshots and compare
// additive freshness, reducing visibility, and the other viewer independently.
async fn actor_first_fill_fences(org: bool) {
    let f = Fixture::new(org).await;
    sqlx::raw_sql("DELETE FROM read_model_triggers; DELETE FROM read_models")
        .execute(&f.owner)
        .await
        .unwrap();
    let snapshot: String = sqlx::query_scalar("SELECT pg_current_snapshot()::text")
        .fetch_one(&f.app)
        .await
        .unwrap();
    let mut add = begin_tenant_scoped(&f.app, f.account, f.user)
        .await
        .unwrap();
    let id: Uuid = sqlx::query_scalar("INSERT INTO conversations(account_id,graph_id,user_id,title) VALUES($1,$2,$3,'Owner title') RETURNING id")
        .bind(f.account).bind(f.graph).bind(f.user).fetch_one(&mut *add).await.unwrap();
    add.commit().await.unwrap();
    async fn state(f: &Fixture, user: Uuid, snapshot: &str) -> i32 {
        let mut tx = begin_tenant_scoped(&f.app, f.account, user).await.unwrap();
        sqlx::query_scalar(
            "SELECT read_model_fence_state($1,'{}','conversation_list',$2,$3::text::pg_snapshot)",
        )
        .bind(f.account)
        .bind(user)
        .bind(snapshot)
        .fetch_one(&mut *tx)
        .await
        .unwrap()
    }
    assert_eq!(
        state(&f, f.user, &snapshot).await,
        1,
        "first-fill owner addition must remain valid stale"
    );
    assert_eq!(
        state(&f, f.other, &snapshot).await,
        2,
        "another owner's mutation cannot dirty private scope"
    );
    let snapshot: String = sqlx::query_scalar("SELECT pg_current_snapshot()::text")
        .fetch_one(&f.app)
        .await
        .unwrap();
    let mut change = begin_tenant_scoped(&f.app, f.account, f.user)
        .await
        .unwrap();
    sqlx::query("INSERT INTO conversation_messages(conversation_id,seq,role,text) VALUES($1,1,'user','Question')")
        .bind(id).execute(&mut *change).await.unwrap();
    // Same-XID coalescing must upgrade an additive message to a title reduction.
    sqlx::query("UPDATE conversations SET title='Reduced title' WHERE id=$1")
        .bind(id)
        .execute(&mut *change)
        .await
        .unwrap();
    change.commit().await.unwrap();
    assert_eq!(
        state(&f, f.user, &snapshot).await,
        0,
        "first-fill owner reduction must fence the snapshot"
    );
    assert_eq!(state(&f, f.other, &snapshot).await, 2);
    // Account events include the authoritative roster before any cached scope
    // exists; the canonical event helper must retain durable named fences.
    let snapshot: String = sqlx::query_scalar("SELECT pg_current_snapshot()::text")
        .fetch_one(&f.app)
        .await
        .unwrap();
    rm::dirty::mark_account(
        &f.app,
        f.account,
        &[f.user, f.other],
        &["conversation_list"],
    )
    .await
    .unwrap();
    assert_eq!(state(&f, f.user, &snapshot).await, 0);
    assert_eq!(state(&f, f.other, &snapshot).await, 0);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM read_models")
            .fetch_one(&f.owner)
            .await
            .unwrap(),
        0
    );
    f.close().await;
}
#[tokio::test]
async fn l3_actor_first_fill_consumer() {
    actor_first_fill_fences(false).await
}
#[tokio::test]
async fn l3_actor_first_fill_tenant() {
    actor_first_fill_fences(true).await
}

async fn deleted_inputs_retract_journal_outputs(org: bool) {
    let f = Fixture::new(org).await;
    let atom = f.atom("compiled-output-input").await;
    sqlx::query("INSERT INTO meeting_transcripts(graph_id,source_id,summary,summary_status) VALUES($1,'compiled-output-input','{\"overview\":\"Synthetic retained prose\"}','done')")
        .bind(f.graph).execute(&f.owner).await.unwrap();
    sqlx::query("INSERT INTO journal_rollups(account_id,user_id,graph_id,hour_start,summary,input_fingerprint,model,period_start) VALUES($1,$2,$3,date_trunc('hour',now()),'Synthetic retained prose','fixture','fixture',date_trunc('hour',now()))")
        .bind(f.account).bind(f.user).bind(f.graph).execute(&f.owner).await.unwrap();
    sqlx::query("INSERT INTO journal_gists(account_id,graph_id,scope,user_id,granularity,period_start,period_end,tz,gist,input_fingerprint,model) VALUES($1,$2,'member',$3,'day',date_trunc('day',now()),date_trunc('day',now())+interval '1 day','UTC','{\"done\":[\"Synthetic retained prose\"],\"decided\":[],\"open\":[]}','fixture','fixture')")
        .bind(f.account).bind(f.graph).bind(f.user).execute(&f.owner).await.unwrap();
    sqlx::query("DELETE FROM atoms WHERE atom_id=$1")
        .bind(atom)
        .execute(&f.owner)
        .await
        .unwrap();
    let count: i64 = sqlx::query_scalar(
        "SELECT (SELECT count(*) FROM journal_rollups)+(SELECT count(*) FROM journal_gists)",
    )
    .fetch_one(&f.owner)
    .await
    .unwrap();
    assert_eq!(
        count, 0,
        "hard-deleted input cannot be republished from retained journal prose"
    );
    let summary:Option<Value>=sqlx::query_scalar("SELECT summary FROM meeting_transcripts WHERE graph_id=$1 AND source_id='compiled-output-input'").bind(f.graph).fetch_one(&f.owner).await.unwrap();
    assert!(
        summary.is_none(),
        "retained transcript summary must lose deleted inputs too"
    );
    f.close().await;
}
#[tokio::test]
async fn l3_deleted_journal_input_consumer() {
    deleted_inputs_retract_journal_outputs(false).await
}
#[tokio::test]
async fn l3_deleted_journal_input_tenant() {
    deleted_inputs_retract_journal_outputs(true).await
}

async fn title_projection(org: bool) {
    use coralswarm_backend::services::journal_activities::project_node_titles;
    let f = Fixture::new(org).await;
    if org {
        sqlx::query("UPDATE graphs SET kind='team',owner_user_id=NULL WHERE id=$1")
            .bind(f.graph)
            .execute(&f.owner)
            .await
            .unwrap();
        let team:Uuid=sqlx::query_scalar("INSERT INTO teams(account_id,graph_id,name,created_by) VALUES($1,$2,'Title visibility',$3) RETURNING id").bind(f.account).bind(f.graph).bind(f.user).fetch_one(&f.owner).await.unwrap();
        for (actor, days) in [(f.user, None::<i32>), (f.other, Some(1))] {
            sqlx::query("INSERT INTO team_members(team_id,user_id,role) VALUES($1,$2,'member')")
                .bind(team)
                .bind(actor)
                .execute(&f.owner)
                .await
                .unwrap();
            sqlx::query("INSERT INTO team_seat_grants(account_id,team_id,user_id,source,history_from) VALUES($1,$2,$3,'fixture',CASE WHEN $4::int IS NULL THEN NULL ELSE now()-make_interval(days=>$4) END)").bind(f.account).bind(team).bind(actor).bind(days).execute(&f.owner).await.unwrap();
        }
    }
    let atom = f.atom("unrelated-current-support").await;
    let node:Uuid=sqlx::query_scalar("INSERT INTO nodes(graph_id,type,title) VALUES($1,'entity','Old secret label') RETURNING node_id").bind(f.graph).fetch_one(&f.owner).await.unwrap();
    sqlx::query("INSERT INTO node_atoms(node_id,atom_id) VALUES($1,$2)")
        .bind(node)
        .bind(atom)
        .execute(&f.owner)
        .await
        .unwrap();
    let item:Uuid=sqlx::query_scalar("INSERT INTO source_items(graph_id,source_type,provider_namespace,source_id) VALUES($1,'note','fixture','old-title-evidence') RETURNING source_item_id").bind(f.graph).fetch_one(&f.owner).await.unwrap();
    let version:Uuid=sqlx::query_scalar("INSERT INTO source_versions(graph_id,source_item_id,source_type,provider_namespace,source_id,captured_at) VALUES($1,$2,'note','fixture','old-title-evidence',now()-interval '2 days') RETURNING version_id").bind(f.graph).bind(item).fetch_one(&f.owner).await.unwrap();
    sqlx::query("UPDATE source_items SET current_version_id=$2 WHERE source_item_id=$1")
        .bind(item)
        .bind(version)
        .execute(&f.owner)
        .await
        .unwrap();
    let sem:Uuid=sqlx::query_scalar(r#"INSERT INTO semantic_atoms(graph_id,source_version_id,ordinal,kind,statement,extraction_version,model,entities) VALUES($1,$2,0,'decision','Old semantic evidence','v1','fixture','[{"mention":"Old secret label"}]') RETURNING semantic_atom_id"#).bind(f.graph).bind(version).fetch_one(&f.owner).await.unwrap();
    sqlx::query(
        "INSERT INTO node_semantic_atoms(graph_id,node_id,semantic_atom_id) VALUES($1,$2,$3)",
    )
    .bind(f.graph)
    .bind(node)
    .bind(sem)
    .execute(&f.owner)
    .await
    .unwrap();
    let corrected:Uuid=sqlx::query_scalar("INSERT INTO nodes(graph_id,type,title) VALUES($1,'entity','Verified interpretation') RETURNING node_id").bind(f.graph).fetch_one(&f.owner).await.unwrap();
    sqlx::query("INSERT INTO node_atoms(node_id,atom_id) VALUES($1,$2)")
        .bind(corrected)
        .bind(atom)
        .execute(&f.owner)
        .await
        .unwrap();
    let mut writer = begin_tenant_scoped(&f.owner, f.account, f.user)
        .await
        .unwrap();
    let correction:Uuid=sqlx::query_scalar("INSERT INTO reef_identity_corrections(graph_id,node_id,label,recorded_by,support_atom_id,support_kind,content_revision,input_sha256) SELECT $1,$2,'Verified interpretation',$3,a.atom_id,'legacy',a.content_revision,reef_identity_atom_fingerprint(a) FROM atoms a WHERE a.atom_id=$4 RETURNING correction_id")
        .bind(f.graph).bind(corrected).bind(f.user).bind(atom).fetch_one(&mut *writer).await.unwrap();
    writer.commit().await.unwrap();
    let mut tx = begin_tenant_scoped(&f.app, f.account, f.user)
        .await
        .unwrap();
    let bulk = project_node_titles(&mut tx, &[f.graph, f.graph], &[node, corrected])
        .await
        .unwrap();
    for id in [node, corrected] {
        let scalar: Option<String> = sqlx::query_scalar("SELECT library_node_title($1,$2)")
            .bind(f.graph)
            .bind(id)
            .fetch_one(&mut *tx)
            .await
            .unwrap();
        assert_eq!(bulk.get(&(f.graph, id)), scalar.as_ref());
    }
    assert_eq!(bulk[&(f.graph, corrected)], "Verified interpretation");
    tx.rollback().await.unwrap();
    if org {
        let mut tx = begin_tenant_scoped(&f.app, f.account, f.other)
            .await
            .unwrap();
        let bulk = project_node_titles(&mut tx, &[f.graph, f.graph], &[node, corrected])
            .await
            .unwrap();
        assert!(
            !bulk.contains_key(&(f.graph, node)),
            "pre-floor semantic label cannot ride an unrelated admitted current atom"
        );
        assert_eq!(bulk[&(f.graph, corrected)], "Verified interpretation");
        tx.rollback().await.unwrap();
    }
    sqlx::query("UPDATE reef_identity_corrections SET state='invalidated' WHERE correction_id=$1")
        .bind(correction)
        .execute(&f.owner)
        .await
        .unwrap();
    let mut tx = begin_tenant_scoped(&f.app, f.account, f.user)
        .await
        .unwrap();
    assert!(!project_node_titles(&mut tx, &[f.graph], &[corrected])
        .await
        .unwrap()
        .contains_key(&(f.graph, corrected)));
    assert!(
        project_node_titles(&mut tx, &[f.foreign], &[node])
            .await
            .unwrap()
            .is_empty(),
        "graph/node identity must match"
    );
    tx.rollback().await.unwrap();
    f.close().await;
}
#[tokio::test]
async fn l3_title_projection_consumer() {
    title_projection(false).await
}
#[tokio::test]
async fn l3_title_projection_tenant() {
    title_projection(true).await
}

/// Explicit 600k fixture run, separate from the fast regression suite. Always
/// runs as a non-owner/NOBYPASSRLS actor after owner-only synthetic seeding.
#[tokio::test]
#[ignore = "explicit 600k production-scale benchmark; run with --ignored"]
async fn l3_benchmark600k() {
    use rm::{chats, connectors, home, today};
    let f = Fixture::new(true).await;
    let role: String = sqlx::query_scalar("SELECT current_user::text")
        .fetch_one(&f.app)
        .await
        .unwrap();
    let artifact_path =
        std::env::var("READ_MODEL_BENCH_OUTPUT").expect("owned benchmark output path required");
    let fixture_path =
        std::path::Path::new(&artifact_path).with_file_name("benchmark-fixture.json");
    std::fs::write(fixture_path,serde_json::to_vec_pretty(&json!({"database":f._db.name(),"control_database":f._control_db.as_ref().map(|d|d.name()),"role":role,"account":f.account,"user":f.user,"other":f.other,"port":15821})).unwrap()).unwrap();
    sqlx::query("DELETE FROM graphs")
        .execute(&f.owner)
        .await
        .unwrap();
    let mut seed = include_str!("fixtures/page_models_600k.sql")
        .lines()
        .filter(|l| !l.starts_with('\\') && !l.starts_with("VACUUM"))
        .map(|l| l.split("--").next().unwrap_or(""))
        .collect::<Vec<_>>()
        .join("\n");
    for (name, id) in [("acct", f.account), ("u1", f.user), ("u2", f.other)] {
        seed = seed.replace(&format!(":{name}"), &format!("'{id}'"));
    }
    for (name, id) in [
        ("g_org", "22222222-0000-0000-0000-000000000001"),
        ("g_team", "22222222-0000-0000-0000-000000000002"),
        ("g_mem", "22222222-0000-0000-0000-000000000003"),
    ] {
        seed = seed.replace(&format!(":{name}"), &format!("'{id}'"));
    }
    let mut seed_connection = f.owner.acquire().await.unwrap();
    // Temp tables stay on this session; commit each statement before enabling
    // triggers again so deferred FK events do not block ALTER TABLE.
    for statement in seed.split(';').filter(|s| !s.trim().is_empty()) {
        sqlx::raw_sql(statement)
            .execute(&mut *seed_connection)
            .await
            .unwrap();
    }
    drop(seed_connection);
    for table in [
        "atoms",
        "nodes",
        "node_atoms",
        "source_items",
        "source_versions",
        "source_version_chunks",
        "semantic_atoms",
        "node_semantic_atoms",
    ] {
        sqlx::query(&format!("VACUUM ANALYZE {table}"))
            .execute(&f.owner)
            .await
            .unwrap();
    }
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM atoms")
        .fetch_one(&f.owner)
        .await
        .unwrap();
    assert_eq!(count, 600_000);
    sqlx::query("INSERT INTO conversations(id,account_id,graph_id,user_id,title) SELECT md5('l3-conv-'||i)::uuid,$1,'22222222-0000-0000-0000-000000000001'::uuid,$2,'Uncertified generated title '||i FROM generate_series(1,200) i")
        .bind(f.account).bind(f.user).execute(&f.owner).await.unwrap();
    sqlx::query("INSERT INTO conversation_messages(conversation_id,seq,role,text) SELECT id,1,'user','Synthetic question '||id FROM conversations")
        .execute(&f.owner).await.unwrap();
    let mut results = serde_json::Map::new();
    async fn bench<M: rm::ReadModel>(
        f: &Fixture,
        params: std::collections::BTreeMap<String, String>,
    ) -> Value {
        eprintln!("L3_BENCH_START {}", M::NAME);
        let scope = f.domain_scope::<M>(params).await;
        let mut computes = vec![];
        for _ in 0..3 {
            rm::store::request_refresh::<M>(&f.app, &scope)
                .await
                .unwrap();
            let c = f.claim().await;
            let start = Instant::now();
            let (value, snapshot) = f.compute(&c).await;
            computes.push(start.elapsed().as_secs_f64() * 1000.0);
            assert!(rm::store::publish(&f.app, &c, value, &snapshot)
                .await
                .unwrap());
        }
        let mut reads = vec![];
        let tenant = coralswarm_backend::tenancy::extractor::Tenant {
            store: None,
            pool: Some(f.app.clone()),
            decision: None,
            class: coralswarm_backend::route_class::RouteClass::PageRead,
        };
        for _ in 0..20 {
            let start = Instant::now();
            let mut tx = tenant.begin_member_scoped(f.account).await.unwrap();
            coralswarm_backend::tenancy::scope::set_tenant_user(&mut tx, f.user)
                .await
                .unwrap();
            let row = rm::store::read::<M>(&mut tx, &scope).await.unwrap();
            assert!(row.computed_at.is_some());
            tx.rollback().await.unwrap();
            reads.push(start.elapsed().as_secs_f64() * 1000.0);
        }
        reads.sort_by(f64::total_cmp);
        json!({"worker_compute_ms":computes,"stored_read_p50_ms":reads[9],"stored_read_p95_ms":reads[18],"stored_samples":20,"read_class":"page_read_readonly_1s","role":"nonowner_nobypassrls","graphs":scope.class.graphs()})
    }
    macro_rules! b {
        ($m:ty,$q:expr) => {
            results.insert(
                <$m as rm::ReadModel>::NAME.into(),
                bench::<$m>(&f, $q).await,
            );
        };
    }
    b!(home::HomeMetrics, Default::default());
    b!(home::WorkspaceActivity, Default::default());
    let now = chrono::Utc::now();
    let range: std::collections::BTreeMap<String, String> = [
        (
            "from".into(),
            (now - chrono::Duration::days(7)).to_rfc3339(),
        ),
        ("to".into(), now.to_rfc3339()),
        ("tz".into(), "UTC".into()),
    ]
    .into();
    b!(today::JournalActivities, range.clone());
    b!(today::JournalStrip, Default::default());
    b!(today::JournalGist, Default::default());
    let rollups = [
        (
            "from".into(),
            (now - chrono::Duration::days(1)).to_rfc3339(),
        ),
        ("to".into(), now.to_rfc3339()),
    ]
    .into();
    b!(today::JournalRollups, rollups);
    b!(connectors::ConnectorsList, Default::default());
    b!(
        connectors::ConnectorDetail,
        [("provider".into(), "google".into())].into()
    );
    b!(connectors::ConnectorStatus, Default::default());
    b!(connectors::TeamsTranscriptCaptures, Default::default());
    b!(chats::ConversationList, Default::default());
    let tenant = coralswarm_backend::tenancy::extractor::Tenant {
        store: None,
        pool: Some(f.app.clone()),
        decision: None,
        class: coralswarm_backend::route_class::RouteClass::Other,
    };
    let mut tx = tenant
        .begin_member_repeatable_read(f.account)
        .await
        .unwrap();
    coralswarm_backend::tenancy::scope::set_tenant_user(&mut tx, f.user)
        .await
        .unwrap();
    // The intentionally slow independent scalar oracle is validation only;
    // every model above retains the production 30s statement / 35s total bound.
    sqlx::query("SET LOCAL statement_timeout='180s'")
        .execute(&mut *tx)
        .await
        .unwrap();
    let graphs: Vec<Uuid> = rm::Viewer::load(&mut tx, f.account, f.user)
        .await
        .unwrap()
        .visible_graphs();
    let now = chrono::Utc::now();
    let (reef_mismatches, reefs, nodes): (i64, i64, i64) =
        sqlx::query_as(include_str!("fixtures/home_reefs_equality.sql"))
            .bind(&graphs)
            .bind(now - chrono::Duration::days(60))
            .bind(now + chrono::Duration::days(1))
            .bind(now - chrono::Duration::days(30))
            .bind(now - chrono::Duration::days(29))
            .fetch_one(&mut *tx)
            .await
            .unwrap();
    assert_eq!(
        reef_mismatches, 0,
        "set reef admission must equal authoritative membership"
    );
    let sample: Vec<(Uuid, Uuid)> = sqlx::query_as(
        "SELECT graph_id,node_id FROM nodes WHERE graph_id=ANY($1) ORDER BY node_id LIMIT 1000",
    )
    .bind(&graphs)
    .fetch_all(&mut *tx)
    .await
    .unwrap();
    let (gs, ns): (Vec<_>, Vec<_>) = sample.iter().copied().unzip();
    let projected =
        coralswarm_backend::services::journal_activities::project_node_titles(&mut tx, &gs, &ns)
            .await
            .unwrap();
    let scalar:Vec<(Uuid,Uuid,Option<String>)>=sqlx::query_as("SELECT n.graph_id,n.node_id,CASE WHEN library_node_permitted(n.graph_id,n.node_id) THEN library_node_title(n.graph_id,n.node_id) END FROM unnest($1::uuid[],$2::uuid[]) p(graph_id,node_id) JOIN nodes n ON n.graph_id=p.graph_id AND n.node_id=p.node_id")
        .bind(&gs).bind(&ns).fetch_all(&mut *tx).await.unwrap();
    let title_mismatches = scalar
        .iter()
        .filter(|(g, n, t)| projected.get(&(*g, *n)) != t.as_ref())
        .count();
    assert_eq!(
        title_mismatches, 0,
        "bulk titles must equal scalar oracle for current admitted seed evidence"
    );
    tx.rollback().await.unwrap();
    let output =
        std::env::var("READ_MODEL_BENCH_OUTPUT").expect("owned benchmark output path required");
    let artifact = json!({"atom_count":count,"seed_sha256":hex::encode(sha2::Sha256::digest(include_bytes!("fixtures/page_models_600k.sql"))),"models":results,"equality":{"reef_nodes_checked":nodes,"reefs_admitted":reefs,"reef_visibility_mismatches":reef_mismatches,"title_nodes_checked":scalar.len(),"title_visibility_mismatches":title_mismatches}});
    std::fs::write(output, serde_json::to_vec_pretty(&artifact).unwrap()).unwrap();
    eprintln!("L3_600K_BENCH={artifact}");
    f.close().await;
}
async fn lifecycle(org: bool) {
    let f = Fixture::new(org).await;
    let a = f.atom("first").await;
    let node:Uuid=sqlx::query_scalar("INSERT INTO nodes(graph_id,type,title,body) VALUES($1,'entity','Synthetic','{}') RETURNING node_id").bind(f.graph).fetch_one(&f.owner).await.unwrap();
    sqlx::query("INSERT INTO node_atoms(node_id,atom_id) VALUES($1,$2)")
        .bind(node)
        .bind(a)
        .execute(&f.owner)
        .await
        .unwrap();
    assert!(f.answer().await.computed_at.is_none());
    sqlx::query("INSERT INTO read_model_triggers(table_name,model) VALUES('coding_sessions','workspace_counts')").execute(&f.owner).await.unwrap();
    f.enqueue().await;
    let c = f.claim().await;
    let future:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM read_model_triggers WHERE table_name='coding_sessions' AND model='workspace_counts')").fetch_one(&f.owner).await.unwrap();
    assert!(
        future,
        "older registry sync must preserve newer-version subscriptions"
    );
    assert!(
        rm::store::claim(&f.app).await.unwrap().is_empty(),
        "concurrent claim cannot own a live claim"
    );
    let mut tx = rm::store::begin_compute(&f.app, &c, Duration::from_secs(30))
        .await
        .unwrap();
    let settings:(String,String,String)=sqlx::query_as("SELECT current_setting('transaction_read_only'),current_setting('statement_timeout'),current_setting('app.tenant_actor')").fetch_one(&mut *tx).await.unwrap();
    assert_eq!(settings, ("on".into(), "30s".into(), "".into()));
    let err = sqlx::query("UPDATE read_models SET last_error='forbidden'")
        .execute(&mut *tx)
        .await
        .unwrap_err();
    assert_eq!(
        err.as_database_error().unwrap().code().as_deref(),
        Some("25006")
    );
    tx.rollback().await.unwrap();
    let (v, s) = f.compute(&c).await;
    assert_eq!(
        v,
        json!({"reef_total":1,"reef_counts":{"entity":1,"decision":0,"thread":0,"reference":0},"atom_count":1,"source_count":1})
    );
    let mut truth_tx = begin_tenant_scoped(&f.app, f.account, f.user)
        .await
        .unwrap();
    let truth: (Value, i64, i64) =
        sqlx::query_as("SELECT * FROM graph_workspace_counts_compute($1)")
            .bind(f.graph)
            .fetch_one(&mut *truth_tx)
            .await
            .unwrap();
    assert_eq!(
        (truth.0["entity"].as_i64(), truth.1, truth.2),
        (Some(1), 1, 1)
    );
    truth_tx.rollback().await.unwrap();
    assert!(rm::store::publish(&f.app, &c, v.clone(), &s).await.unwrap());
    assert!(
        !rm::store::publish(&f.app, &c, v.clone(), &s).await.unwrap(),
        "publish is idempotent"
    );
    assert!(!f.answer().await.stale);
    // Cross-viewer and cross-graph policy tests on actual NOBYPASS role.
    let mut other = begin_tenant_scoped(&f.app, f.account, f.other)
        .await
        .unwrap();
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM read_models")
        .fetch_one(&mut *other)
        .await
        .unwrap();
    assert_eq!(count, 0);
    let viewer = rm::Viewer::load(&mut other, f.account, f.other)
        .await
        .unwrap();
    assert!(viewer.scope::<Model>(P { graph: f.graph }).is_err());
    other.rollback().await.unwrap();
    let mut wrong = begin_tenant_scoped(&f.app, Uuid::new_v4(), f.user)
        .await
        .unwrap();
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM read_models")
        .fetch_one(&mut *wrong)
        .await
        .unwrap();
    assert_eq!(count, 0);
    wrong.rollback().await.unwrap();
    let mut me = begin_tenant_scoped(&f.app, f.account, f.user)
        .await
        .unwrap();
    let viewer = rm::Viewer::load(&mut me, f.account, f.user).await.unwrap();
    assert!(viewer.scope::<Model>(P { graph: f.foreign }).is_err());
    me.rollback().await.unwrap();
    // Additive writes keep the last answer, but only after debounce (OR max delay).
    f.atom("second").await;
    let stale = f.answer().await;
    assert!(stale.stale);
    assert_eq!(stale.payload.atom_count, 1);
    assert!(stale.computed_at.is_some());
    assert!(rm::store::claim(&f.app).await.unwrap().is_empty());
    let eta:f64=sqlx::query_scalar("SELECT EXTRACT(epoch FROM LEAST(dirty_bumped_at+interval '30 seconds',dirty_at+interval '60 seconds')-now())::float8 FROM read_models").fetch_one(&f.owner).await.unwrap();
    assert!(eta > 28. && eta <= 30.);
    sqlx::query("UPDATE read_models SET dirty_at=now()-interval '61 seconds'")
        .execute(&f.owner)
        .await
        .unwrap();
    let c = f.claim().await;
    let (v, s) = f.compute(&c).await;
    f.atom("bump-during-compute").await;
    assert!(rm::store::publish(&f.app, &c, v, &s).await.unwrap());
    assert!(f.answer().await.stale, "newer generation survives publish");
    // Claim/store race with revoke: old content is served stale, never current.
    f.enqueue().await;
    let c = f.claim().await;
    let (v, s) = f.compute(&c).await;
    sqlx::query("UPDATE atoms SET evidence_retracted=true WHERE atom_id=$1")
        .bind(a)
        .execute(&f.owner)
        .await
        .unwrap();
    let behind = f.answer().await;
    assert!(
        behind.computed_at.is_some() && behind.stale,
        "revoke marks the last published answer stale, synchronously"
    );
    assert_eq!(behind.payload.atom_count, 2);
    rm::store::publish(&f.app, &c, v, &s).await.unwrap();
    let behind = f.answer().await;
    assert!(
        behind.computed_at.is_some() && behind.stale,
        "a pre-revoke snapshot publishes but is served stale, never current"
    );
    assert_eq!(behind.payload.atom_count, 3);
    f.finish().await;
    assert_eq!(f.answer().await.payload.atom_count, 2);
    // Stale claim recovery fences both publication and failure from its old owner.
    f.enqueue().await;
    let c1 = f.claim().await;
    let (v1, s1) = f.compute(&c1).await;
    sqlx::query("UPDATE read_models SET refresh_claimed_at=now()-interval '3 minutes'")
        .execute(&f.owner)
        .await
        .unwrap();
    let c2 = f.claim().await;
    assert_ne!(c1.claim_token, c2.claim_token);
    assert!(!rm::store::publish(&f.app, &c1, v1, &s1).await.unwrap());
    assert_eq!(
        rm::store::fail(&f.app, &c1, FailureKind::Other).await.unwrap(),
        None,
        "a lost claim records no failure"
    );
    let (v2, s2) = f.compute(&c2).await;
    assert!(rm::store::publish(&f.app, &c2, v2, &s2).await.unwrap());
    f.enqueue().await;
    let c = f.claim().await;
    assert_eq!(
        rm::store::fail(&f.app, &c, FailureKind::Deadlock).await.unwrap(),
        Some(1)
    );
    // Only the kind is persisted, never error text.
    let last_error: Option<String> = sqlx::query_scalar("SELECT last_error FROM read_models")
        .fetch_one(&f.owner)
        .await
        .unwrap();
    assert_eq!(last_error.as_deref(), Some("deadlock"));
    assert!(rm::store::claim(&f.app).await.unwrap().is_empty());
    let retry: f64 =
        sqlx::query_scalar("SELECT EXTRACT(epoch FROM retry_after-now())::float8 FROM read_models")
            .fetch_one(&f.owner)
            .await
            .unwrap();
    assert!(retry > 28.);
    rm::store::enqueue::<Model>(&f.app, &f.scope().await)
        .await
        .unwrap();
    assert!(
        rm::store::claim(&f.app).await.unwrap().is_empty(),
        "automatic GET enqueue must preserve failure backoff"
    );
    // Manual refresh bypasses error backoff and makes work immediately due.
    f.finish().await;
    assert!(!f.answer().await.stale);
    // XIDs are allocated before commit: a smaller transaction can invalidate
    // AFTER a higher XID committed. GREATEST(xid) would wrongly admit this
    // pre-revoke snapshot, so the last row-lock-serialized writer must win.
    let mut older = f.owner.begin().await.unwrap();
    let older_xid: String = sqlx::query_scalar("SELECT pg_current_xact_id()::text")
        .fetch_one(&mut *older)
        .await
        .unwrap();
    sqlx::query(
        "UPDATE atoms SET derived_copy_bound=true WHERE graph_id=$1 AND source_id='second'",
    )
    .bind(f.graph)
    .execute(&f.owner)
    .await
    .unwrap();
    f.enqueue().await;
    let c = f.claim().await;
    let (v, s) = f.compute(&c).await;
    sqlx::query("UPDATE atoms SET evidence_retracted=true WHERE graph_id=$1 AND source_id='bump-during-compute'").bind(f.graph).execute(&mut *older).await.unwrap();
    older.commit().await.unwrap();
    let last: String = sqlx::query_scalar("SELECT invalidated_xid::text FROM read_models")
        .fetch_one(&f.owner)
        .await
        .unwrap();
    assert_eq!(last, older_xid);
    rm::store::publish(&f.app, &c, v, &s).await.unwrap();
    let behind = f.answer().await;
    assert!(
        behind.computed_at.is_some() && behind.stale,
        "lower-XID later revoke must leave the published answer stale"
    );
    f.finish().await;
    // Batch trigger: one dirty generation bump for 10,000 nodes, not per row.
    let before: i64 = sqlx::query_scalar("SELECT dirty_generation FROM read_models")
        .fetch_one(&f.owner)
        .await
        .unwrap();
    let start = Instant::now();
    let plan:Value=sqlx::query_scalar("EXPLAIN (ANALYZE,FORMAT JSON) INSERT INTO nodes(graph_id,type,title,body) SELECT $1,'reference','Bulk synthetic '||i,'{}' FROM generate_series(1,10000) i").bind(f.graph).fetch_one(&f.owner).await.unwrap();
    let after: i64 = sqlx::query_scalar("SELECT dirty_generation FROM read_models")
        .fetch_one(&f.owner)
        .await
        .unwrap();
    assert_eq!(after - before, 1);
    let cost = plan[0]["Triggers"]
        .as_array()
        .unwrap()
        .iter()
        .find(|t| t["Trigger Name"] == "read_models_nodes_insert")
        .unwrap()["Time"]
        .as_f64()
        .unwrap();
    eprintln!(
        "bulk_statement plane={org} rows=10000 total_ms={} read_model_trigger_ms={cost}",
        start.elapsed().as_millis()
    );
    assert!(cost < 2.0, "read-model trigger exceeded 2 ms: {cost}");
    // Revalidate stored compute_as: changing membership/visibility refuses compute.
    f.enqueue().await;
    let c = f.claim().await;
    sqlx::query("UPDATE graphs SET owner_user_id=$2 WHERE id=$1")
        .bind(f.graph)
        .bind(f.other)
        .execute(&f.owner)
        .await
        .unwrap();
    let progress = read_model_refresh::Progress::default();
    let refused =
        read_model_refresh::compute_tracked(&f.worker(), &c, &f.control, None, &progress)
            .await
            .unwrap_err();
    // A graph that left the actor's visible set is the genuine refusal.
    let refused = failure::classify(&refused);
    assert_eq!(
        (refused.kind, refused.reason(), progress.phase()),
        (
            FailureKind::ScopeUnaddressable,
            Some(rm::Unaddressable::ScopeGraphs),
            read_model_refresh::Phase::Verify
        )
    );
    f.close().await;
}
#[tokio::test]
async fn tenant_read_model_lifecycle() {
    lifecycle(true).await
}
#[tokio::test]
async fn control_read_model_lifecycle() {
    lifecycle(false).await
}

#[test]
fn workspace_params_are_typed_and_path_authoritative() {
    let g = Uuid::new_v4().to_string();
    let h = Uuid::new_v4().to_string();
    assert!(P::parse(
        [("graph_id".into(), g.clone())].into(),
        [("graph".into(), h)].into()
    )
    .is_err());
    assert_eq!(
        P::parse(
            [("graph_id".into(), g.clone())].into(),
            [("window".into(), "7d".into())].into()
        )
        .unwrap()
        .graph
        .to_string(),
        g
    );
    assert!(P::parse(Default::default(), [("compute_as".into(), g)].into()).is_err());
}

#[tokio::test]
async fn tenant_windowed_viewers_and_floor_changes_are_isolated() {
    let f = Fixture::new(true).await;
    sqlx::query("UPDATE graphs SET kind='team',owner_user_id=NULL WHERE id=$1")
        .bind(f.graph)
        .execute(&f.owner)
        .await
        .unwrap();
    let team:Uuid=sqlx::query_scalar("INSERT INTO teams(account_id,graph_id,name,created_by) VALUES($1,$2,'Read model team',$3) RETURNING id").bind(f.account).bind(f.graph).bind(f.user).fetch_one(&f.owner).await.unwrap();
    for (u, days) in [(f.user, None::<i32>), (f.other, Some(1))] {
        sqlx::query("INSERT INTO team_members(team_id,user_id,role) VALUES($1,$2,'member')")
            .bind(team)
            .bind(u)
            .execute(&f.owner)
            .await
            .unwrap();
        sqlx::query("INSERT INTO team_seat_grants(account_id,team_id,user_id,source,history_from) VALUES($1,$2,$3,'fixture',CASE WHEN $4::int IS NULL THEN NULL ELSE now()-make_interval(days=>$4) END)").bind(f.account).bind(team).bind(u).bind(days).execute(&f.owner).await.unwrap();
    }
    let recent = f.atom("recent").await;
    let old = f.atom("old").await;
    sqlx::query("UPDATE atoms SET timestamp=now()-interval '2 days' WHERE atom_id=$1")
        .bind(old)
        .execute(&f.owner)
        .await
        .unwrap();
    f.finish().await;
    assert_eq!(f.answer().await.payload.atom_count, 2);
    let mut tx = begin_tenant_scoped(&f.app, f.account, f.other)
        .await
        .unwrap();
    let viewer = rm::Viewer::load(&mut tx, f.account, f.other).await.unwrap();
    let narrow = viewer.scope::<Model>(P { graph: f.graph }).unwrap();
    assert!(matches!(narrow.class, rm::ViewerClass::Windowed { .. }));
    assert_ne!(narrow.key(), f.scope().await.key());
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM read_models")
        .fetch_one(&mut *tx)
        .await
        .unwrap();
    assert_eq!(
        count, 0,
        "floored user cannot directly read full-history Shared row"
    );
    tx.rollback().await.unwrap();
    rm::store::enqueue::<Model>(&f.app, &narrow).await.unwrap();
    let c = f.claim().await;
    let (v, s) = f.compute(&c).await;
    assert_eq!(v["atom_count"], 1);
    rm::store::publish(&f.app, &c, v, &s).await.unwrap();
    // Timestamp is part of history-floor admission, even when all explicit
    // permission flags stay unchanged. Move an already-cached contribution
    // behind the member's floor and verify the stored answer is served stale.
    sqlx::query("UPDATE atoms SET timestamp=now()-interval '2 days' WHERE atom_id=$1")
        .bind(recent)
        .execute(&f.owner)
        .await
        .unwrap();
    let (fresh, _) = f.compute(&c).await;
    assert_eq!(fresh["atom_count"], 0);
    let mut member = begin_tenant_scoped(&f.app, f.account, f.other)
        .await
        .unwrap();
    let cached = rm::store::read::<Model>(&mut member, &narrow)
        .await
        .unwrap();
    assert!(
        cached.computed_at.is_some() && cached.stale,
        "timestamp exclusion must mark cached atom_count={} stale; fresh compute is zero",
        cached.payload.atom_count
    );
    assert_eq!(cached.payload.atom_count, 1);
    member.rollback().await.unwrap();
    let mut me = begin_tenant_scoped(&f.app, f.account, f.user)
        .await
        .unwrap();
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM read_models WHERE viewer_user_id=$1")
        .bind(f.other)
        .fetch_one(&mut *me)
        .await
        .unwrap();
    assert_eq!(
        count, 0,
        "other user's windowed row cannot be read even by full-history viewer"
    );
    me.rollback().await.unwrap();
    sqlx::query("UPDATE team_seat_grants SET history_from=now() WHERE team_id=$1 AND user_id=$2")
        .bind(team)
        .bind(f.other)
        .execute(&f.owner)
        .await
        .unwrap();
    let mut tx = begin_tenant_scoped(&f.app, f.account, f.other)
        .await
        .unwrap();
    let viewer = rm::Viewer::load(&mut tx, f.account, f.other).await.unwrap();
    let changed = viewer.scope::<Model>(P { graph: f.graph }).unwrap();
    assert_ne!(narrow.key(), changed.key());
    assert!(rm::store::read::<Model>(&mut tx, &changed)
        .await
        .unwrap()
        .computed_at
        .is_none());
    tx.rollback().await.unwrap();
    let obsolete = rm::store::enqueue::<Model>(&f.app, &narrow).await;
    assert!(
        obsolete.is_err(),
        "cannot enqueue a superseded viewer class"
    );
    f.close().await;
}

#[tokio::test]
async fn poisoned_scope_backs_off_while_next_scope_computes() {
    let f = Fixture::new(true).await;
    sqlx::query("INSERT INTO read_models(model,scope_key,version,account_id,graph_ids,compute_as,params,dirty_at,dirty_bumped_at) VALUES('workspace_counts','poisoned',1,$1,$2,$3,$4,now()-interval '1 minute',now()-interval '1 minute')")
        .bind(f.account).bind(vec![f.graph]).bind(f.other).bind(json!({"graph":f.graph})).execute(&f.owner).await.unwrap();
    f.enqueue().await;
    let tick = read_model_refresh::tick(&f.worker(), &f.control)
        .await
        .unwrap();
    assert_eq!(tick.processed, 2);
    assert!(
        f.answer().await.computed_at.is_some(),
        "healthy scope behind poisoned scope must compute"
    );
    let state:(Option<Value>,i32,bool,Option<Uuid>)=sqlx::query_as("SELECT payload,failed_attempts,retry_after>now(),claim_token FROM read_models WHERE scope_key='poisoned'").fetch_one(&f.owner).await.unwrap();
    assert_eq!(state, (None, 1, true, None));
    assert!(
        rm::store::claim(&f.app).await.unwrap().is_empty(),
        "poisoned scope respects retry backoff"
    );
    f.close().await;
}

#[tokio::test]
async fn stored_handler_cost_is_separate_from_worker_compute() {
    use axum::{
        body::{to_bytes, Body},
        http::Request,
    };
    use coralswarm_backend::{
        route_class::RouteClass,
        tenancy::{
            extractor::Tenant,
            runtime::{TenancyDecision, TenantContext},
        },
        UserContext,
    };
    use tower::ServiceExt;
    let f = Fixture::new(true).await;
    f.finish().await;
    let insert:Value=sqlx::query_scalar("EXPLAIN (ANALYZE,FORMAT JSON) INSERT INTO atoms(graph_id,source_type,source_id,content_blob_key,content_inline,timestamp) SELECT $1,'note','read-bench:'||i,'fixture','Synthetic benchmark evidence',now() FROM generate_series(1,10000) i").bind(f.graph).fetch_one(&f.owner).await.unwrap();
    let atom_trigger_ms = insert[0]["Triggers"]
        .as_array()
        .unwrap()
        .iter()
        .find(|t| t["Trigger Name"] == "read_models_atoms_insert")
        .unwrap()["Time"]
        .as_f64()
        .unwrap();
    assert!(
        atom_trigger_ms < 2.0,
        "hot atom trigger exceeded 2ms: {atom_trigger_ms}"
    );
    f.enqueue().await;
    let c = f.claim().await;
    let start = Instant::now();
    let (v, s) = f.compute(&c).await;
    let worker_ms = start.elapsed().as_secs_f64() * 1000.;
    assert_eq!(v["atom_count"], 10000);
    rm::store::publish(&f.app, &c, v, &s).await.unwrap();
    let tenant = Tenant {
        store: Some(Arc::new(PostgresStore::new(f.app.clone()))),
        pool: Some(f.app.clone()),
        decision: Some(TenancyDecision::Org(TenantContext {
            org_id: "org_read_models".into(),
            account_id: f.account,
            user_id: f.user,
            pool: f.app.clone(),
            page_pool: None,
        })),
        class: RouteClass::PageRead,
    };
    let user = UserContext {
        account_id: f.account,
        user_id: f.user,
        role: "owner".into(),
        device_id: "read-model-benchmark".into(),
    };
    let router = coralswarm_backend::test_support_workspace_app(tenant, user);
    let mut samples = vec![];
    for _ in 0..20 {
        let start = Instant::now();
        let res = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/v1/graphs/{}/stats", f.graph))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), 200);
        let answer: Value =
            serde_json::from_slice(&to_bytes(res.into_body(), 1_000_000).await.unwrap()).unwrap();
        assert!(!answer["computed_at"].is_null());
        assert_eq!(answer["payload"]["atom_count"], 10000);
        samples.push(start.elapsed().as_secs_f64() * 1000.);
    }
    samples.sort_by(f64::total_cmp);
    assert!(samples[18] < 250.);
    eprintln!(
        "READ_MODEL_BENCH {}",
        json!({"atoms":10000,"request_samples":20,"stored_handler_p50_ms":samples[9],"stored_handler_p95_ms":samples[18],"worker_compute_ms":worker_ms,"atom_trigger_ms":atom_trigger_ms})
    );
    f.close().await;
}

#[tokio::test]
async fn scheduling_shared_actor_handoffs_preserve_backoff() {
    for org in [false, true] {
        let f = Fixture::new(org).await;
        if !org {
            sqlx::query(
                "INSERT INTO account_members(account_id,user_id,role) VALUES($1,$2,'member')",
            )
            .bind(f.account)
            .bind(f.other)
            .execute(&f.owner)
            .await
            .unwrap();
        }
        sqlx::query("UPDATE graphs SET kind='team',owner_user_id=NULL WHERE id=$1")
            .bind(f.graph)
            .execute(&f.owner)
            .await
            .unwrap();
        let team: Uuid = sqlx::query_scalar("INSERT INTO teams(account_id,graph_id,name,created_by) VALUES($1,$2,'Shared retry team',$3) RETURNING id")
            .bind(f.account).bind(f.graph).bind(f.user).fetch_one(&f.owner).await.unwrap();
        for actor in [f.user, f.other] {
            sqlx::query("INSERT INTO team_members(team_id,user_id,role) VALUES($1,$2,'member')")
                .bind(team)
                .bind(actor)
                .execute(&f.owner)
                .await
                .unwrap();
            sqlx::query("INSERT INTO team_seat_grants(account_id,team_id,user_id,source,history_from) VALUES($1,$2,$3,'fixture',NULL)")
                .bind(f.account).bind(team).bind(actor).execute(&f.owner).await.unwrap();
        }
        let first = f.scope().await;
        let mut other = begin_tenant_scoped(&f.app, f.account, f.other)
            .await
            .unwrap();
        let second = rm::Viewer::load(&mut other, f.account, f.other)
            .await
            .unwrap()
            .scope::<Model>(P { graph: f.graph })
            .unwrap();
        other.rollback().await.unwrap();
        assert!(matches!(first.class, rm::ViewerClass::Shared { .. }));
        assert_eq!(
            first.key(),
            second.key(),
            "full-history viewers share an address"
        );
        rm::store::enqueue::<Model>(&f.app, &first).await.unwrap();
        let claim = f.claim().await;
        rm::store::fail(&f.app, &claim, FailureKind::Other).await.unwrap();
        let deadline: chrono::DateTime<chrono::Utc> =
            sqlx::query_scalar("SELECT retry_after FROM read_models")
                .fetch_one(&f.owner)
                .await
                .unwrap();
        for scope in [&second, &first, &second, &first] {
            rm::store::enqueue::<Model>(&f.app, scope).await.unwrap();
            let retry: Option<chrono::DateTime<chrono::Utc>> =
                sqlx::query_scalar("SELECT retry_after FROM read_models")
                    .fetch_one(&f.owner)
                    .await
                    .unwrap();
            assert_eq!(
                retry,
                Some(deadline),
                "automatic GET actor handoffs must preserve backoff on plane org={org}"
            );
            assert!(rm::store::claim(&f.app).await.unwrap().is_empty());
        }
        rm::store::request_refresh::<Model>(&f.app, &second)
            .await
            .unwrap();
        let next = f.claim().await;
        assert_eq!(
            next.compute_as, f.other,
            "explicit refresh may override the deadline"
        );
        f.close().await;
    }
}

#[tokio::test]
async fn scheduling_ignores_unknown_and_retired_model_versions() {
    let f = Fixture::new(true).await;
    f.enqueue().await;
    let claim = f.claim().await;
    rm::store::fail(&f.app, &claim, FailureKind::Other).await.unwrap();
    for (model, key, version) in [
        ("unknown_model", "unknown", 1),
        ("workspace_counts", "retired", 0),
        ("workspace_counts", "future", 2),
    ] {
        sqlx::query("INSERT INTO read_models(model,scope_key,version,account_id,graph_ids,compute_as,params,dirty_at,dirty_bumped_at) VALUES($1,$2,$3,$4,$5,$6,$7,now()-interval '1 hour',now()-interval '1 hour')")
            .bind(model).bind(key).bind(version).bind(f.account).bind(vec![f.graph]).bind(f.user)
            .bind(json!({"graph": f.graph})).execute(&f.owner).await.unwrap();
    }
    assert!(rm::store::claim(&f.app).await.unwrap().is_empty());
    let due = rm::store::next_due(&f.app).await.unwrap().unwrap();
    assert!(
        due > 20.,
        "only the registered version's retry deadline should schedule a tick, got {due}"
    );
    sqlx::query("DELETE FROM read_models WHERE model='workspace_counts' AND version=1")
        .execute(&f.owner)
        .await
        .unwrap();
    assert!(
        rm::store::next_due(&f.app).await.unwrap().is_none(),
        "unclaimable rows must permit idle backoff"
    );
    f.close().await;
}

#[tokio::test]
async fn worker_rechecks_live_control_membership_before_tenant_compute() {
    let f = Fixture::new(true).await;
    f.enqueue().await;
    let c = f.claim().await;
    assert!(read_model_refresh::compute(&f.worker(), &c, &f.control)
        .await
        .is_ok());
    let mut tx =
        coralswarm_backend::tenancy::scope::begin_tenant_scoped(&f.control, f.account, f.user)
            .await
            .unwrap();
    sqlx::query("DELETE FROM account_members WHERE account_id=$1 AND user_id=$2")
        .bind(f.account)
        .bind(f.user)
        .execute(&mut *tx)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    let progress = read_model_refresh::Progress::default();
    let refused =
        read_model_refresh::compute_tracked(&f.worker(), &c, &f.control, None, &progress)
            .await
            .expect_err("tenant windows alone cannot certify an offboarded actor");
    let refused = failure::classify(&refused);
    assert_eq!(
        (refused.kind, refused.reason(), progress.phase()),
        (
            FailureKind::ScopeUnaddressable,
            Some(rm::Unaddressable::Membership),
            read_model_refresh::Phase::Membership
        )
    );
    f.close().await;
}

/// The models that compute through a service (`home_metrics`,
/// `workspace_activity`) keep the driver's error, so a statement timeout in
/// them is `statement_timeout`/57014 rather than an unclassifiable string.
/// The statement waits on a lock held past the compute snapshot's budget.
#[tokio::test]
async fn service_backed_models_classify_statement_timeouts() {
    use rm::{chats_detail::ChatEmpty, home};
    let f = Fixture::new(false).await;
    async fn stored<M: ReadModel>(f: &Fixture) -> (&'static str, Value) {
        let scope = f.domain_scope::<M>(Default::default()).await;
        (M::NAME, serde_json::to_value(scope.params).unwrap())
    }
    let scopes = [
        stored::<home::HomeMetrics>(&f).await,
        stored::<home::WorkspaceActivity>(&f).await,
        stored::<ChatEmpty>(&f).await,
    ];
    // Every model above reads `graphs` first; chat_empty reads
    // `activity_events` first while the Home dashboard flag is off.
    let mut lock = f.owner.begin().await.unwrap();
    sqlx::query("LOCK TABLE graphs, activity_events IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *lock)
        .await
        .unwrap();
    for (name, params) in scopes {
        let model = rm::registry::by_name(name).unwrap();
        let claim = rm::store::Claim {
            model: name.into(),
            scope_key: String::new(),
            version: model.version(),
            account_id: f.account,
            graph_ids: vec![f.graph],
            compute_as: f.user,
            params: params.clone(),
            dirty_generation: 0,
            claim_token: Uuid::nil(),
            control_revision: 0,
            control_freshness: 0,
            immediate_due_at: None,
            claimed_at: chrono::Utc::now(),
            first_fill: true,
            due_to_claim_ms: None,
        };
        let mut tx = rm::store::begin_compute(&f.app, &claim, Duration::from_millis(200))
            .await
            .unwrap();
        let graphs = [f.graph];
        let error = model
            .compute_json(
                rm::ComputeCx {
                    tx: &mut tx,
                    blobs: None,
                    control_tx: None,
                    account_id: f.account,
                    compute_as: f.user,
                    graphs: &graphs,
                    now: chrono::Utc::now(),
                },
                params,
            )
            .await
            .expect_err(name);
        let failure = failure::classify(&error);
        assert_eq!(
            (failure.kind, failure.sqlstate()),
            (FailureKind::StatementTimeout, Some("57014")),
            "{name}"
        );
    }
    lock.rollback().await.unwrap();
    f.close().await;
}

// The AFTER STATEMENT trigger has completed, but its transaction is still open.
// No scope row existed for it to update. Enqueue, compute and publish use the
// actual non-bypass application/worker pool; no metadata fence is mocked.
async fn first_fill_permission_race(org: bool) {
    tokio::time::timeout(Duration::from_secs(60), async {
        let f = Fixture::new(org).await;
        for publish_before_commit in [false, true] {
            sqlx::query("DELETE FROM read_models").execute(&f.owner).await.unwrap();
            let atom = f.atom(&format!("first-fill-private-evidence-{publish_before_commit}")).await;
            let mut revoke = begin_tenant_scoped(&f.app, f.account, f.user).await.unwrap();
            // A nested/additive write may already have stamped this XID. The
            // following permission reduction must still upgrade that fence.
            sqlx::query("INSERT INTO nodes(graph_id,type,title,body) VALUES($1,'reference',$2,'{}')")
                .bind(f.graph).bind(format!("Additive before revoke {publish_before_commit}")).execute(&mut *revoke).await.unwrap();
            sqlx::query("UPDATE atoms SET evidence_retracted=true WHERE atom_id=$1")
                .bind(atom).execute(&mut *revoke).await.unwrap();
            // Explicit transaction barrier: the source UPDATE and its AFTER
            // trigger finished. The worker must see the pre-commit evidence.
            f.enqueue().await;
            let claim = f.claim().await;
            let (payload, snapshot) = f.compute(&claim).await;
            assert_eq!(payload["atom_count"], 1);
            if publish_before_commit {
                assert!(rm::store::publish(&f.app, &claim, payload.clone(), &snapshot).await.unwrap());
            }
            revoke.commit().await.unwrap();
            if !publish_before_commit {
                assert!(rm::store::publish(&f.app, &claim, payload, &snapshot).await.unwrap());
            }
            let answer = f.answer().await;
            assert!(answer.computed_at.is_some() && answer.stale,
                "pre-revocation first-fill answer must be served stale, never current: org={org}, publish_before_commit={publish_before_commit}, payload={:?}", answer.payload);
            assert_eq!(answer.payload.atom_count, 1);
            f.finish().await;
            assert_eq!(f.answer().await.payload.atom_count, 0);
        }
        f.close().await;
    }).await.expect("bounded first-fill transactions must finish, not deadlock");
}
#[tokio::test]
async fn control_first_fill_permission_race() {
    first_fill_permission_race(false).await;
}
#[tokio::test]
async fn tenant_first_fill_permission_race() {
    first_fill_permission_race(true).await;
}

#[tokio::test]
async fn first_fill_additive_changes_remain_served_and_refreshable() {
    for org in [false, true] {
        let f = Fixture::new(org).await;
        for publish_before_commit in [false, true] {
            sqlx::query("DELETE FROM read_models")
                .execute(&f.owner)
                .await
                .unwrap();
            let before = if publish_before_commit { 1 } else { 0 };
            let mut add = begin_tenant_scoped(&f.app, f.account, f.user)
                .await
                .unwrap();
            sqlx::query("INSERT INTO atoms(graph_id,source_type,source_id,content_blob_key,content_inline,timestamp) VALUES($1,'note',$2,'fixture','New evidence',now())")
                .bind(f.graph).bind(format!("additive-{publish_before_commit}")).execute(&mut *add).await.unwrap();
            f.enqueue().await;
            let claim = f.claim().await;
            let (payload, snapshot) = f.compute(&claim).await;
            assert_eq!(payload["atom_count"], before);
            if publish_before_commit {
                rm::store::publish(&f.app, &claim, payload.clone(), &snapshot)
                    .await
                    .unwrap();
            }
            add.commit().await.unwrap();
            if !publish_before_commit {
                rm::store::publish(&f.app, &claim, payload, &snapshot)
                    .await
                    .unwrap();
            }
            let answer = f.answer().await;
            assert!(
                answer.computed_at.is_some(),
                "additive write must retain valid stale payload"
            );
            assert!(
                answer.stale,
                "first-fill additive phantom must not look fresh"
            );
            assert_eq!(answer.payload.atom_count, before);
            if publish_before_commit {
                assert!(
                    answer.refresh_eta.is_none(),
                    "late commit after clean publication must cause handler enqueue"
                );
            }
            f.finish().await;
            assert_eq!(f.answer().await.payload.atom_count, before + 1);
            assert!(!f.answer().await.stale);
        }
        f.close().await;
    }
}

/// A reducing write on a computed row serves the last stored result marked
/// stale instead of withholding it; the owed recompute closes the gap. Only a
/// row that was never computed answers `computed_at: None`.
async fn reducing_write_serves_last_result_stale(org: bool) {
    let f = Fixture::new(org).await;
    let atom = f.atom("served-stale").await;
    f.finish().await;
    let fresh = f.answer().await;
    assert!(fresh.computed_at.is_some() && !fresh.stale);
    assert_eq!(fresh.payload.atom_count, 1);
    // Retraction is a reducing write: the trigger stamps the row-local
    // invalidation and the graph fence in the writer's transaction.
    sqlx::query("UPDATE atoms SET evidence_retracted=true WHERE atom_id=$1")
        .bind(atom)
        .execute(&f.owner)
        .await
        .unwrap();
    let scope = f.scope().await;
    let mut tx = begin_tenant_scoped(&f.app, f.account, f.user)
        .await
        .unwrap();
    let (valid, fence): (bool, i32) = sqlx::query_as("SELECT (invalidated_xid IS NULL OR pg_visible_in_snapshot(invalidated_xid,computed_snapshot)),read_model_fence_state(account_id,graph_ids,model,viewer_user_id,computed_snapshot) FROM read_models WHERE model=$1 AND scope_key=$2")
        .bind(Model::NAME).bind(scope.key()).fetch_one(&mut *tx).await.unwrap();
    tx.rollback().await.unwrap();
    assert_eq!(
        (valid, fence),
        (false, 0),
        "row invalidation and graph fence must both say the row is behind"
    );
    let behind = f.answer().await;
    assert_eq!(
        behind.computed_at, fresh.computed_at,
        "org={org}: the last stored result must be served, not withheld"
    );
    assert!(behind.stale, "org={org}: a behind row is served stale");
    assert!(
        behind.behind && !fresh.behind,
        "org={org}: decode reports the row as behind so the serving path owes the recompute"
    );
    assert_eq!(behind.payload.atom_count, 1);
    assert!(
        behind.refresh_eta.is_some(),
        "the trigger's dirty mark already owes the recompute"
    );
    f.finish().await;
    let recomputed = f.answer().await;
    assert!(!recomputed.stale);
    assert_eq!(recomputed.payload.atom_count, 0);
    f.close().await;
}
#[tokio::test]
async fn consumer_reducing_write_serves_last_result_stale() {
    reducing_write_serves_last_result_stale(false).await
}
#[tokio::test]
async fn tenant_reducing_write_serves_last_result_stale() {
    reducing_write_serves_last_result_stale(true).await
}

/// A GET of a behind row serves it stale and owes the recompute at once, as
/// every GET of a withheld row did: the row is made due now rather than at the
/// dirty debounce, and the ETA says so. A read after the caller's own reducing
/// write therefore settles on the client's next poll instead of waiting out
/// `DEBOUNCE` (30 s), which is what Home, chat feedback and the connector
/// pages show the user.
async fn behind_row_get_owes_immediate_recompute(org: bool) {
    use axum::{
        body::{to_bytes, Body},
        http::Request,
    };
    use coralswarm_backend::{
        route_class::RouteClass,
        tenancy::{
            extractor::Tenant,
            runtime::{TenancyDecision, TenantContext},
        },
        UserContext,
    };
    use tower::ServiceExt;
    let f = Fixture::new(org).await;
    let atom = f.atom("behind-get").await;
    f.finish().await;
    assert_eq!(f.answer().await.payload.atom_count, 1);
    sqlx::query("UPDATE atoms SET evidence_retracted=true WHERE atom_id=$1")
        .bind(atom)
        .execute(&f.owner)
        .await
        .unwrap();
    // The trigger's dirty mark is debounced: nothing is due or claimable yet.
    assert!(
        rm::store::claim(&f.app).await.unwrap().is_empty(),
        "org={org}: the trigger's dirty mark waits for the debounce"
    );
    let tenant = Tenant {
        store: Some(Arc::new(PostgresStore::new(f.app.clone()))),
        pool: Some(f.app.clone()),
        decision: Some(if org {
            TenancyDecision::Org(TenantContext {
                org_id: "org_read_models".into(),
                account_id: f.account,
                user_id: f.user,
                pool: f.app.clone(),
                page_pool: None,
            })
        } else {
            TenancyDecision::Personal
        }),
        class: RouteClass::PageRead,
    };
    let user = UserContext {
        account_id: f.account,
        user_id: f.user,
        role: "owner".into(),
        device_id: "behind-get".into(),
    };
    let router = coralswarm_backend::test_support_workspace_app(tenant, user);
    let before = chrono::Utc::now();
    let res = router
        .oneshot(
            Request::builder()
                .uri(format!("/v1/graphs/{}/stats", f.graph))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), 200);
    let answer: Value =
        serde_json::from_slice(&to_bytes(res.into_body(), 1_000_000).await.unwrap()).unwrap();
    assert!(
        !answer["computed_at"].is_null(),
        "org={org}: a behind row is served, not withheld: {answer}"
    );
    assert_eq!(answer["stale"], json!(true), "{answer}");
    assert_eq!(answer["payload"]["atom_count"], 1, "{answer}");
    let eta = chrono::DateTime::parse_from_rfc3339(answer["refresh_eta"].as_str().unwrap())
        .unwrap()
        .with_timezone(&chrono::Utc);
    assert!(
        eta <= before + chrono::Duration::seconds(6),
        "org={org}: a behind row owes the 5 s floor, not the debounce ETA: {answer}"
    );
    // The enqueue is detached; it backdates `dirty_bumped_at`, so the row is
    // due now and the next worker tick claims it.
    let mut claims = vec![];
    for _ in 0..50 {
        claims = rm::store::claim(&f.app).await.unwrap();
        if !claims.is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(
        claims.len(),
        1,
        "org={org}: the GET of a behind row must make it due at once"
    );
    let c = claims.remove(0);
    let (v, s) = f.compute(&c).await;
    assert!(rm::store::publish(&f.app, &c, v, &s).await.unwrap());
    let recomputed = f.answer().await;
    assert!(!recomputed.stale && !recomputed.behind);
    assert_eq!(recomputed.payload.atom_count, 0);
    f.close().await;
}
#[tokio::test]
async fn consumer_behind_row_get_owes_immediate_recompute() {
    behind_row_get_owes_immediate_recompute(false).await
}
#[tokio::test]
async fn tenant_behind_row_get_owes_immediate_recompute() {
    behind_row_get_owes_immediate_recompute(true).await
}

/// A behind row is served stale only while its recompute is healthy: that
/// recompute is what bounds the exposure of a reduced item. Once the last
/// compute failed the row is in failure backoff and nothing bounds it, so a
/// behind row is withheld again until a publish succeeds. A failed compute on
/// a row that is not behind changes nothing: it keeps serving stale.
async fn failed_recompute_withholds_behind_row(org: bool) {
    let f = Fixture::new(org).await;
    f.atom("kept").await;
    let retracted = f.atom("retracted-later").await;
    f.finish().await;
    assert_eq!(f.answer().await.payload.atom_count, 2);
    let scope = f.scope().await;
    async fn backoff(f: &Fixture, key: &str) -> (i32, Option<chrono::DateTime<chrono::Utc>>) {
        sqlx::query_as("SELECT failed_attempts,retry_after FROM read_models WHERE model=$1 AND scope_key=$2")
            .bind(Model::NAME)
            .bind(key)
            .fetch_one(&f.owner)
            .await
            .unwrap()
    }
    // An additive write followed by a failed compute: the row is valid, so the
    // failure alone never withholds it.
    f.atom("additive").await;
    f.enqueue().await;
    let c = f.claim().await;
    rm::store::fail(&f.app, &c, FailureKind::Other).await.unwrap();
    assert_eq!(backoff(&f, scope.key()).await.0, 1);
    let served = f.answer().await;
    assert!(
        served.computed_at.is_some() && served.stale,
        "org={org}: a failed compute on a valid row keeps serving it stale"
    );
    assert_eq!(served.payload.atom_count, 2);
    f.finish().await;
    assert_eq!(
        backoff(&f, scope.key()).await,
        (0, None),
        "publish clears the backoff"
    );
    assert_eq!(f.answer().await.payload.atom_count, 3);
    // The reduction with a healthy row: served stale, as the owner accepted.
    sqlx::query("UPDATE atoms SET evidence_retracted=true WHERE atom_id=$1")
        .bind(retracted)
        .execute(&f.owner)
        .await
        .unwrap();
    let behind = f.answer().await;
    assert!(
        behind.computed_at.is_some() && behind.stale,
        "org={org}: a reduction on a healthy row is served stale"
    );
    assert_eq!(behind.payload.atom_count, 3);
    // The same reduction once its recompute failed: withheld, and the refresh
    // ETA honours the backoff so the client does not poll a broken model.
    f.enqueue().await;
    let c = f.claim().await;
    rm::store::fail(&f.app, &c, FailureKind::Other).await.unwrap();
    let (failed, retry) = backoff(&f, scope.key()).await;
    assert_eq!(failed, 1);
    let retry = retry.expect("fail sets the retry deadline");
    let withheld = f.answer().await;
    assert!(
        withheld.computed_at.is_none(),
        "org={org}: a behind row whose last compute failed must be withheld, got {:?}",
        withheld.payload
    );
    assert_eq!(withheld.payload.atom_count, 0);
    assert!(withheld.stale);
    assert!(
        withheld.refresh_eta.is_some_and(|eta| eta >= retry),
        "the withheld envelope owes the recompute no earlier than the backoff"
    );
    // A successful recompute serves the reduced answer as current again.
    f.finish().await;
    let recomputed = f.answer().await;
    assert!(recomputed.computed_at.is_some() && !recomputed.stale);
    assert_eq!(recomputed.payload.atom_count, 2);
    f.close().await;
}
#[tokio::test]
async fn consumer_failed_recompute_withholds_behind_row() {
    failed_recompute_withholds_behind_row(false).await
}
#[tokio::test]
async fn tenant_failed_recompute_withholds_behind_row() {
    failed_recompute_withholds_behind_row(true).await
}

/// `read_model_fence_state` answers 0 not only for a fence behind the row but
/// also when the request's identity does not match the row (account, windowed
/// viewer) or the row has no compute snapshot. Those are authority refusals,
/// not "behind": they keep withholding. The probes go through the owner
/// connection so RLS cannot hide the row first; this proves that `decode`
/// itself refuses, independent of the policy.
async fn identity_refusal_still_withholds(org: bool) {
    let f = Fixture::new(org).await;
    f.atom("identity").await;
    f.finish().await;
    let scope = f.scope().await;
    async fn probe(
        f: &Fixture,
        scope: &rm::Scope<P>,
        account: Uuid,
        user: Uuid,
    ) -> rm::Stored<coralswarm_backend::store::WorkspaceCounts> {
        let mut tx = f.owner.begin().await.unwrap();
        sqlx::query("SELECT set_config('app.account_id',$1,true),set_config('app.user_id',$2,true),set_config('app.tenant_actor','',true)")
            .bind(account.to_string())
            .bind(user.to_string())
            .execute(&mut *tx)
            .await
            .unwrap();
        let stored = rm::store::read::<Model>(&mut tx, scope).await.unwrap();
        tx.rollback().await.unwrap();
        stored
    }
    let served = probe(&f, &scope, f.account, f.user).await;
    assert!(served.computed_at.is_some() && !served.stale);
    assert_eq!(served.payload.atom_count, 1);
    let foreign = probe(&f, &scope, Uuid::new_v4(), f.user).await;
    assert!(
        foreign.computed_at.is_none(),
        "org={org}: an account mismatch must withhold, got {:?}",
        foreign.payload
    );
    assert_eq!(foreign.payload.atom_count, 0);
    assert!(foreign.stale && foreign.refresh_eta.is_some());
    // A windowed row addressed by another viewer.
    sqlx::query("UPDATE read_models SET viewer_user_id=$3 WHERE model=$1 AND scope_key=$2")
        .bind(Model::NAME)
        .bind(scope.key())
        .bind(f.user)
        .execute(&f.owner)
        .await
        .unwrap();
    let other = probe(&f, &scope, f.account, f.other).await;
    assert!(
        other.computed_at.is_none(),
        "org={org}: a viewer mismatch must withhold, got {:?}",
        other.payload
    );
    assert!(
        probe(&f, &scope, f.account, f.user).await.computed_at.is_some(),
        "the windowed viewer itself is still served"
    );
    sqlx::query("UPDATE read_models SET viewer_user_id=NULL WHERE model=$1 AND scope_key=$2")
        .bind(Model::NAME)
        .bind(scope.key())
        .execute(&f.owner)
        .await
        .unwrap();
    // A computed row without a compute snapshot is the third refusal the fence
    // function folds into 0. The schema makes it unrepresentable (tenant 0223
    // `read_models_check`), so `identity` carries that clause for parity only.
    let unrepresentable = sqlx::query(
        "UPDATE read_models SET computed_snapshot=NULL WHERE model=$1 AND scope_key=$2",
    )
    .bind(Model::NAME)
    .bind(scope.key())
    .execute(&f.owner)
    .await;
    assert_eq!(
        unrepresentable
            .unwrap_err()
            .as_database_error()
            .and_then(|e| e.constraint())
            .unwrap_or_default(),
        "read_models_check",
        "org={org}: a computed row must keep its snapshot"
    );
    assert!(probe(&f, &scope, f.account, f.user).await.computed_at.is_some());
    f.close().await;
}
#[tokio::test]
async fn consumer_identity_refusal_still_withholds() {
    identity_refusal_still_withholds(false).await
}
#[tokio::test]
async fn tenant_identity_refusal_still_withholds() {
    identity_refusal_still_withholds(true).await
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
struct EmptyParams {}
impl Params for EmptyParams {}
// A real multi-graph SELECT exercises the generic store before L3 adds domain
// models. It shares the registered queue name/version, but is computed here
// through its typed ReadModel implementation rather than registry dispatch.
struct AllGraphs;
impl ReadModel for AllGraphs {
    const NAME: &'static str = "workspace_counts";
    const PATH: &'static str = "/test/all";
    const VERSION: i32 = 1;
    type Params = EmptyParams;
    type Payload = i64;
    fn compute<'a>(cx: rm::ComputeCx<'a>, _: &'a EmptyParams) -> rm::ComputeFuture<'a, i64> {
        Box::pin(async move {
            Ok(sqlx::query_scalar("SELECT count(*) FROM unnest($1::uuid[]) g(graph_id) CROSS JOIN LATERAL library_permitted_atoms(g.graph_id,NULL::uuid[]) a")
                .bind(cx.graphs).fetch_one(&mut **cx.tx).await?)
        })
    }
    fn empty(_: &EmptyParams) -> i64 {
        0
    }
    fn dirty_triggers() -> &'static [rm::dirty::DirtyTrigger] {
        &[]
    }
}
async fn all_scope(f: &Fixture) -> rm::Scope<EmptyParams> {
    let mut tx = begin_tenant_scoped(&f.app, f.account, f.user)
        .await
        .unwrap();
    rm::Viewer::load(&mut tx, f.account, f.user)
        .await
        .unwrap()
        .scope::<AllGraphs>(EmptyParams {})
        .unwrap()
}
async fn all_compute(f: &Fixture, c: &rm::store::Claim) -> (Value, String) {
    let mut tx = rm::store::begin_compute(&f.app, c, Duration::from_secs(30))
        .await
        .unwrap();
    let snapshot: String = sqlx::query_scalar("SELECT pg_current_snapshot()::text")
        .fetch_one(&mut *tx)
        .await
        .unwrap();
    let scope = rm::Viewer::load(&mut tx, f.account, f.user)
        .await
        .unwrap()
        .scope::<AllGraphs>(EmptyParams {})
        .unwrap();
    assert_eq!(scope.key(), c.scope_key);
    assert_eq!(scope.class.graphs(), c.graph_ids);
    let value = AllGraphs::compute(
        rm::ComputeCx {
            blobs: None,
            tx: &mut tx,
            control_tx: None,
            account_id: f.account,
            compute_as: f.user,
            graphs: &c.graph_ids,
            now: chrono::Utc::now(),
        },
        &EmptyParams {},
    )
    .await
    .unwrap();
    (json!(value), snapshot)
}
#[tokio::test]
async fn first_fill_multiple_graph_fences_and_lower_xid_later_commit() {
    for org in [false, true] {
        let f = Fixture::new(org).await;
        let first = f.atom("multi-a").await;
        let second_graph: Uuid = sqlx::query_scalar("INSERT INTO graphs(account_id,name,kind,owner_user_id) VALUES($1,'Second graph','member',$2) RETURNING id")
            .bind(f.account).bind(f.user).fetch_one(&f.owner).await.unwrap();
        let second: Uuid = sqlx::query_scalar("INSERT INTO atoms(graph_id,source_type,source_id,content_blob_key,content_inline,timestamp) VALUES($1,'note','multi-b','fixture','Second evidence',now()) RETURNING atom_id")
            .bind(second_graph).fetch_one(&f.owner).await.unwrap();
        let mut lower = begin_tenant_scoped(&f.app, f.account, f.user)
            .await
            .unwrap();
        let lower_xid: String = sqlx::query_scalar("SELECT pg_current_xact_id()::text")
            .fetch_one(&mut *lower)
            .await
            .unwrap();
        sqlx::query("UPDATE atoms SET evidence_retracted=true WHERE atom_id=$1")
            .bind(first)
            .execute(&mut *lower)
            .await
            .unwrap();
        // Independent graph invalidator commits first. Seeing its higher XID
        // must not certify the still-unseen lower-XID graph fence.
        let mut higher = begin_tenant_scoped(&f.app, f.account, f.user)
            .await
            .unwrap();
        let higher_xid: String = sqlx::query_scalar("SELECT pg_current_xact_id()::text")
            .fetch_one(&mut *higher)
            .await
            .unwrap();
        assert!(lower_xid.parse::<u64>().unwrap() < higher_xid.parse::<u64>().unwrap());
        sqlx::query("UPDATE atoms SET evidence_retracted=true WHERE atom_id=$1")
            .bind(second)
            .execute(&mut *higher)
            .await
            .unwrap();
        higher.commit().await.unwrap();
        let scope = all_scope(&f).await;
        assert_eq!(scope.class.graphs().len(), 2);
        rm::store::request_refresh::<AllGraphs>(&f.app, &scope)
            .await
            .unwrap();
        let claim = f.claim().await;
        let (payload, snapshot) = all_compute(&f, &claim).await;
        assert_eq!(payload, json!(1));
        lower.commit().await.unwrap();
        rm::store::publish(&f.app, &claim, payload, &snapshot)
            .await
            .unwrap();
        let mut tx = begin_tenant_scoped(&f.app, f.account, f.user)
            .await
            .unwrap();
        let answer = rm::store::read::<AllGraphs>(&mut tx, &scope).await.unwrap();
        assert!(
            answer.computed_at.is_some() && answer.stale,
            "one unseen graph invalidator must leave the multi-graph answer stale"
        );
        assert_eq!(answer.payload, 1);
        tx.rollback().await.unwrap();
        rm::store::request_refresh::<AllGraphs>(&f.app, &scope)
            .await
            .unwrap();
        let claim = f.claim().await;
        let (payload, snapshot) = all_compute(&f, &claim).await;
        assert_eq!(payload, json!(0));
        rm::store::publish(&f.app, &claim, payload, &snapshot)
            .await
            .unwrap();
        let mut tx = begin_tenant_scoped(&f.app, f.account, f.user)
            .await
            .unwrap();
        assert_eq!(
            rm::store::read::<AllGraphs>(&mut tx, &scope)
                .await
                .unwrap()
                .payload,
            0
        );
        tx.rollback().await.unwrap();
        f.close().await;
    }
}

struct EmptyFacts<const PRIVATE: bool>;
impl<const PRIVATE: bool> ReadModel for EmptyFacts<PRIVATE> {
    const NAME: &'static str = "workspace_counts";
    const PATH: &'static str = "/test/facts";
    const VERSION: i32 = 1;
    const PER_USER: bool = PRIVATE;
    const CONTROL_FACTS: bool = true;
    type Params = EmptyParams;
    type Payload = i64;
    fn graphs(_: &rm::Viewer, _: &EmptyParams) -> Vec<Uuid> {
        vec![]
    }
    fn compute<'a>(cx: rm::ComputeCx<'a>, _: &'a EmptyParams) -> rm::ComputeFuture<'a, i64> {
        Box::pin(async move {
            Ok(sqlx::query_scalar("SELECT count(*) FROM account_members WHERE account_id=$1 AND user_id=$2 AND role='owner'")
                .bind(cx.account_id).bind(cx.compute_as)
                .fetch_one(&mut **cx.control_tx.expect("control-fact transaction")).await?)
        })
    }
    fn empty(_: &EmptyParams) -> i64 {
        0
    }
    fn dirty_triggers() -> &'static [rm::dirty::DirtyTrigger] {
        &[]
    }
}
async fn absent_event_fence<const PRIVATE: bool>(org: bool, publish_before_commit: bool) {
    let f = Fixture::new(org).await;
    let opts =
        PgConnectOptions::from_str(&std::env::var("READ_MODEL_TEST_BASE_URL").unwrap()).unwrap();
    let control_owner = PgPoolOptions::new()
        .max_connections(1)
        .connect_with(opts.database(f._control_db.as_ref().map_or(f._db.name(), |db| db.name())))
        .await
        .unwrap();
    sqlx::query("UPDATE account_members SET role='owner' WHERE account_id=$1 AND user_id=$2")
        .bind(f.account)
        .bind(f.user)
        .execute(&control_owner)
        .await
        .unwrap();
    // Store-boundary adversarial ordering: a real member-scoped computation
    // holds its old repeatable snapshots while no cache row exists. The current
    // worker normally claims before computing; certification must nevertheless
    // reject any such old snapshot, independently of row presence at the event.
    let mut content = f
        .app
        .begin_with("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
        .await
        .unwrap();
    let mut control = f
        .control
        .begin_with("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
        .await
        .unwrap();
    for tx in [&mut content, &mut control] {
        sqlx::query("SELECT set_config('app.account_id',$1,true),set_config('app.user_id',$2,true),set_config('statement_timeout','30s',true)")
            .bind(f.account.to_string()).bind(f.user.to_string()).execute(&mut **tx).await.unwrap();
    }
    let snapshot: String = sqlx::query_scalar("SELECT pg_current_snapshot()::text")
        .fetch_one(&mut *content)
        .await
        .unwrap();
    let mut viewer = rm::Viewer::load(&mut content, f.account, f.user)
        .await
        .unwrap();
    viewer.certify_control(&mut control).await.unwrap();
    let scope = viewer.scope::<EmptyFacts<PRIVATE>>(EmptyParams {}).unwrap();
    assert!(scope.class.graphs().is_empty());
    let payload = EmptyFacts::<PRIVATE>::compute(
        rm::ComputeCx {
            blobs: None,
            tx: &mut content,
            control_tx: Some(&mut control),
            account_id: f.account,
            compute_as: f.user,
            graphs: &[],
            now: chrono::Utc::now(),
        },
        &EmptyParams {},
    )
    .await
    .unwrap();
    assert_eq!(payload, 1, "actual pre-reduction control-fact answer");
    content.rollback().await.unwrap();
    control.rollback().await.unwrap();
    // Fact commit precedes the event, as required for acknowledgement safety.
    sqlx::query("UPDATE account_members SET role='member' WHERE account_id=$1 AND user_id=$2")
        .bind(f.account)
        .bind(f.user)
        .execute(&control_owner)
        .await
        .unwrap();
    let event = if publish_before_commit {
        let mut event =
            coralswarm_backend::tenancy::scope::begin_tenant_system_scoped_without_account(&f.app)
                .await
                .unwrap();
        // Exactly mark_user's SQL, held at its post-statement/pre-commit barrier.
        sqlx::query("SELECT read_models_mark_user_dirty($1,$2,$3)")
            .bind(f.account)
            .bind(f.user)
            .bind(vec!["workspace_counts"])
            .execute(&mut *event)
            .await
            .unwrap();
        Some(event)
    } else {
        // The public helper is also a red-capable absent-row regression on the
        // old implementation: it silently updated zero rows, then publication
        // served the actual old privileged answer.
        rm::dirty::mark_user(&f.app, f.account, f.user, &["workspace_counts"])
            .await
            .unwrap();
        None
    };
    rm::store::request_refresh::<EmptyFacts<PRIVATE>>(&f.app, &scope)
        .await
        .unwrap();
    let claim = f.claim().await;
    if publish_before_commit {
        rm::store::publish(&f.app, &claim, json!(payload), &snapshot)
            .await
            .unwrap();
    }
    if let Some(event) = event {
        event.commit().await.unwrap();
    }
    if !publish_before_commit {
        rm::store::publish(&f.app, &claim, json!(payload), &snapshot)
            .await
            .unwrap();
    }
    let mut tx = begin_tenant_scoped(&f.app, f.account, f.user)
        .await
        .unwrap();
    let answer = rm::store::read::<EmptyFacts<PRIVATE>>(&mut tx, &scope)
        .await
        .unwrap();
    assert!(
        answer.computed_at.is_some() && answer.stale,
        "old control-fact payload behind the absent-row event fence is served stale, never current"
    );
    assert_eq!(answer.payload, 1);
    // The reduced fact also moved the control revision: a freshly certified
    // viewer addresses a different key, so no request reaches this row.
    let mut control = begin_tenant_scoped(&f.control, f.account, f.user)
        .await
        .unwrap();
    let mut current = rm::Viewer::load(&mut tx, f.account, f.user).await.unwrap();
    current.certify_control(&mut control).await.unwrap();
    control.rollback().await.unwrap();
    assert_ne!(
        scope.key(),
        current
            .scope::<EmptyFacts<PRIVATE>>(EmptyParams {})
            .unwrap()
            .key()
    );
    tx.rollback().await.unwrap();
    // The actual high-level helper persists the same durable keys with no row,
    // and errors (including a failed fence write) must propagate to its caller.
    sqlx::query("DELETE FROM read_models")
        .execute(&f.owner)
        .await
        .unwrap();
    rm::dirty::mark_user(&f.app, f.account, f.user, &["workspace_counts"])
        .await
        .unwrap();
    // A fresh, ordinary model computation sees the committed reduced fact.
    rm::store::request_refresh::<EmptyFacts<PRIVATE>>(&f.app, &scope)
        .await
        .unwrap();
    let claim = f.claim().await;
    let mut content = rm::store::begin_compute(&f.app, &claim, Duration::from_secs(30))
        .await
        .unwrap();
    let fresh_snapshot: String = sqlx::query_scalar("SELECT pg_current_snapshot()::text")
        .fetch_one(&mut *content)
        .await
        .unwrap();
    let mut control = rm::store::begin_compute(&f.control, &claim, Duration::from_secs(30))
        .await
        .unwrap();
    let value = EmptyFacts::<PRIVATE>::compute(
        rm::ComputeCx {
            blobs: None,
            tx: &mut content,
            control_tx: Some(&mut control),
            account_id: f.account,
            compute_as: f.user,
            graphs: &[],
            now: chrono::Utc::now(),
        },
        &EmptyParams {},
    )
    .await
    .unwrap();
    assert_eq!(value, 0);
    content.rollback().await.unwrap();
    control.rollback().await.unwrap();
    rm::store::publish(&f.app, &claim, json!(value), &fresh_snapshot)
        .await
        .unwrap();
    let mut tx = begin_tenant_scoped(&f.app, f.account, f.user)
        .await
        .unwrap();
    let answer = rm::store::read::<EmptyFacts<PRIVATE>>(&mut tx, &scope)
        .await
        .unwrap();
    assert!(answer.computed_at.is_some());
    assert_eq!(answer.payload, 0);
    tx.rollback().await.unwrap();
    control_owner.close().await;
    f.close().await;
}
async fn event_fences_on_plane(org: bool) {
    tokio::time::timeout(Duration::from_secs(60), async {
        for early in [false, true] {
            absent_event_fence::<false>(org, early).await;
            absent_event_fence::<true>(org, early).await;
        }
    })
    .await
    .expect("bounded event first-fill proof");
}
#[tokio::test]
async fn control_events_certify_absent_shared_and_private_empty_graph_scopes() {
    event_fences_on_plane(false).await;
}
#[tokio::test]
async fn tenant_events_certify_absent_shared_and_private_empty_graph_scopes() {
    event_fences_on_plane(true).await;
}
#[tokio::test]
async fn fence_metadata_and_invalidation_are_system_only() {
    for org in [false, true] {
        let f = Fixture::new(org).await;
        rm::dirty::mark_user(&f.app, f.account, f.user, &["workspace_counts"])
            .await
            .unwrap();
        let mut member = begin_tenant_scoped(&f.app, f.account, f.user)
            .await
            .unwrap();
        for table in ["read_model_graph_fences", "read_model_user_fences"] {
            let count: i64 = sqlx::query_scalar(&format!("SELECT count(*) FROM {table}"))
                .fetch_one(&mut *member)
                .await
                .unwrap();
            assert_eq!(
                count, 0,
                "member cannot enumerate graph or private event metadata"
            );
            let actual: i64 = sqlx::query_scalar(&format!("SELECT count(*) FROM {table}"))
                .fetch_one(&f.owner)
                .await
                .unwrap();
            assert!(actual > 0, "non-vacuous raw metadata denial");
        }
        let error = sqlx::query("SELECT read_models_mark_user_dirty($1,$2,$3)")
            .bind(f.account)
            .bind(f.user)
            .bind(vec!["workspace_counts"])
            .execute(&mut *member)
            .await
            .unwrap_err();
        assert_eq!(
            error.as_database_error().unwrap().code().as_deref(),
            Some("42501")
        );
        member.rollback().await.unwrap();
        let mut member = begin_tenant_scoped(&f.app, f.account, f.user)
            .await
            .unwrap();
        let error = sqlx::query("INSERT INTO read_model_user_fences(account_id,model,user_id,invalidated_xid) VALUES($1,'workspace_counts',$2,pg_current_xact_id())")
            .bind(f.account).bind(f.user).execute(&mut *member).await.unwrap_err();
        assert_eq!(
            error.as_database_error().unwrap().code().as_deref(),
            Some("42501")
        );
        member.rollback().await.unwrap();
        // A real SQL failure remains an acknowledgement-blocking error.
        sqlx::query("ALTER TABLE read_model_user_fences ADD CONSTRAINT reject_test_fence CHECK(false) NOT VALID").execute(&f.owner).await.unwrap();
        assert!(
            rm::dirty::mark_user(&f.app, f.account, f.user, &["workspace_counts"])
                .await
                .is_err()
        );
        f.close().await;
    }
}

#[path = "support/l3_corrections.rs"]
mod l3_corrections;

#[path = "support/read_model_queue.rs"]
mod read_model_queue;

#[path = "support/read_model_first_fill.rs"]
mod read_model_first_fill;

#[path = "support/oauth_change_attribution.rs"]
mod oauth_change_attribution;

#[path = "support/oceans_phases.rs"]
mod oceans_phases;
#[path = "support/oceans_counts.rs"]
mod oceans_counts;

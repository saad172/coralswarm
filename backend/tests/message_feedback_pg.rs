mod support;

use std::{str::FromStr, sync::Arc};

use axum::{
    body::to_bytes,
    extract::{rejection::JsonRejection, Path},
    http::StatusCode,
    response::Response,
    Extension, Json,
};
use coralswarm_backend::{
    services::message_feedback::FeedbackBody,
    store::PostgresStore,
    tenancy::{
        extractor::Tenant,
        runtime::{TenancyDecision, TenantContext},
    },
    test_db::{migrated_database, Plane},
    AppState, UserContext,
};
use serde_json::{json, Value};
use sqlx::{
    postgres::{PgConnectOptions, PgPoolOptions},
    Executor, PgPool,
};
use tokio::sync::Mutex;
use uuid::Uuid;

static ENV_LOCK: Mutex<()> = Mutex::const_new(());
const FLAG: &str = "MESSAGE_FEEDBACK_ENABLED";

struct Conv {
    id: Uuid,
    question: Uuid,
    answer: Uuid,
}

struct Fixture {
    admin: PgPool,
    control_app: PgPool,
    owner: PgPool,
    app: PgPool,
    names: Vec<coralswarm_backend::test_db::TestDatabase>,
    org_id: Option<String>,
    account: Uuid,
    a: Uuid,
    b: Uuid,
    private: Conv,
    unshared: Conv,
    shared: Conv,
}

impl Fixture {
    async fn new(org: bool) -> Option<Self> {
        let Ok(dsn) = std::env::var("ORGT_TEST_BASE_URL") else {
            assert!(std::env::var_os("REQUIRE_PG_SUITE").is_none(), "REQUIRE_PG_SUITE set without ORGT_TEST_BASE_URL");
            return None;
        };
        let opts = PgConnectOptions::from_str(&dsn).unwrap();
        let admin = PgPoolOptions::new().max_connections(2).connect_with(opts.clone()).await.unwrap();
        let tenant_name = migrated_database(&opts, Plane::Tenant, "message_feedback_tenant").await;
        let control_name = migrated_database(&opts, Plane::Control, "message_feedback_control").await;
        let control_owner = PgPoolOptions::new().max_connections(3).connect_with(opts.clone().database(&control_name)).await.unwrap();
        support::app_role::set_app_role_password_with_retry(&control_owner).await;
        let control_app = PgPoolOptions::new().max_connections(4)
            .connect_with(opts.clone().database(&control_name).username("coralswarm_app").password(support::app_role::APP_ROLE_TEST_PASSWORD))
            .await.unwrap();
        let mut users = Vec::new();
        for tag in ["a", "b"] {
            users.push(sqlx::query_scalar::<_, Uuid>("INSERT INTO users(email) VALUES($1) RETURNING id")
                .bind(format!("feedback-{tag}-{}@test.local", Uuid::new_v4())).fetch_one(&control_owner).await.unwrap());
        }
        let (a, b) = (users[0], users[1]);
        let org_id = org.then(|| format!("org_fake_{}", Uuid::new_v4().simple()));
        let account: Uuid = sqlx::query_scalar("INSERT INTO accounts(owner_user_id,clerk_org_id) VALUES($1,$2) RETURNING id")
            .bind(a).bind(&org_id).fetch_one(&control_owner).await.unwrap();
        for (u, role) in [(a, "owner"), (b, "member")] {
            sqlx::query("INSERT INTO account_members(account_id,user_id,role,seat_tier_id) VALUES($1,$2,$3,(SELECT CASE WHEN a.is_org_account THEN (SELECT id FROM seat_tiers WHERE code='standard' AND is_active) END FROM accounts a WHERE a.id=$1))")
                .bind(account).bind(u).bind(role).execute(&control_owner).await.unwrap();
        }
        let (owner, app) = if org {
            let owner = PgPoolOptions::new().max_connections(3).connect_with(opts.clone().database(&tenant_name)).await.unwrap();
            support::tenant_role::set_app_role_password_with_retry(&owner).await;
            let app = PgPoolOptions::new().max_connections(4)
                .connect_with(opts.database(&tenant_name).username("coralswarm_tenant_app").password(support::tenant_role::APP_ROLE_TEST_PASSWORD))
                .await.unwrap();
            control_owner.close().await;
            (owner, app)
        } else {
            (control_owner, control_app.clone())
        };
        let privs: (bool, bool) = sqlx::query_as("SELECT rolsuper,rolbypassrls FROM pg_roles WHERE rolname=current_user")
            .fetch_one(&app).await.unwrap();
        assert_eq!(privs, (false, false), "the proof must run on a role RLS applies to");
        let common: Uuid = sqlx::query_scalar("INSERT INTO graphs(account_id,name) VALUES($1,'Platform') RETURNING id")
            .bind(account).fetch_one(&owner).await.unwrap();
        let mine: Uuid = sqlx::query_scalar("INSERT INTO graphs(account_id,name,kind,owner_user_id) VALUES($1,'Private A','member',$2) RETURNING id")
            .bind(account).bind(a).fetch_one(&owner).await.unwrap();
        let mut convs = Vec::new();
        for (graph, shared) in [(mine, false), (common, false), (common, true)] {
            let id: Uuid = sqlx::query_scalar("INSERT INTO conversations(account_id,graph_id,user_id,title,shared) VALUES($1,$2,$3,'Halcyon',$4) RETURNING id")
                .bind(account).bind(graph).bind(a).bind(shared).fetch_one(&owner).await.unwrap();
            let mut ids = Vec::new();
            for (seq, role, text) in [(1, "user", "When is the Halcyon launch?"), (2, "assistant", "March 14.")] {
                ids.push(sqlx::query_scalar::<_, Uuid>("INSERT INTO conversation_messages(conversation_id,seq,role,text) VALUES($1,$2,$3,$4) RETURNING id")
                    .bind(id).bind(seq).bind(role).bind(text).fetch_one(&owner).await.unwrap());
            }
            convs.push(Conv { id, question: ids[0], answer: ids[1] });
        }
        let names = vec![control_name, tenant_name];
        let shared = convs.pop().unwrap();
        let unshared = convs.pop().unwrap();
        let private = convs.pop().unwrap();
        Some(Self { admin, control_app, owner, app, names, org_id, account, a, b, private, unshared, shared })
    }

    fn tenant(&self, user: Uuid) -> Tenant {
        Tenant {
            store: Some(Arc::new(PostgresStore::new(self.app.clone()))),
            pool: Some(self.app.clone()),
            decision: Some(match &self.org_id {
                Some(org_id) => TenancyDecision::Org(TenantContext {
                    org_id: org_id.clone(),
                    account_id: self.account,
                    user_id: user,
                    pool: self.app.clone(),
                    page_pool: None,
                }),
                None => TenancyDecision::Personal,
            }),
            class: coralswarm_backend::route_class::RouteClass::Other,
        }
    }

    fn ctx(&self, user: Uuid) -> UserContext {
        UserContext { user_id: user, account_id: self.account, role: "member".into(), device_id: "feedback-fixture".into() }
    }

    fn state(&self) -> Arc<AppState> {
        Arc::new(coralswarm_backend::test_app_state_owned_with_db_and_store(self.control_app.clone(), None))
    }

    async fn put(&self, user: Uuid, conv: Uuid, message: Uuid, body: Value) -> (StatusCode, Value) {
        let body: Result<Json<FeedbackBody>, JsonRejection> = Ok(Json(serde_json::from_value(body).unwrap()));
        read(coralswarm_backend::message_feedback_put(self.tenant(user), Extension(self.ctx(user)), Path((conv, message)), body).await).await
    }

    async fn delete(&self, user: Uuid, conv: Uuid, message: Uuid) -> (StatusCode, Value) {
        read(coralswarm_backend::message_feedback_delete(self.tenant(user), Extension(self.ctx(user)), Path((conv, message))).await).await
    }

    async fn read_as_a(&self, conv: Uuid) -> serde_json::Map<String, Value> {
        let (status, body) = read(coralswarm_backend::test_conversation_read(self.state(), self.tenant(self.a), self.ctx(self.a), conv).await).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        body["messages"].as_array().unwrap().iter().map(|m| (m["id"].as_str().unwrap().to_string(), m.clone())).collect()
    }

    async fn rows(&self) -> Vec<(Uuid, Uuid, i16, Vec<String>, Option<String>, chrono::DateTime<chrono::Utc>)> {
        sqlx::query_as("SELECT message_id,user_id,vote,reasons,note,updated_at FROM message_feedback ORDER BY message_id,user_id")
            .fetch_all(&self.owner).await.unwrap()
    }

    async fn detail_scope(&self, user: Uuid, id: Uuid) -> coralswarm_backend::read_models::Scope<coralswarm_backend::read_models::chats_detail::DetailParams> {
        use coralswarm_backend::{read_models as rm, tenancy::scope::begin_tenant_scoped};
        let mut tx = begin_tenant_scoped(&self.app, self.account, user).await.unwrap();
        let viewer = rm::Viewer::load(&mut tx, self.account, user).await.unwrap();
        viewer.scope::<rm::chats_detail::ConversationDetail>(rm::chats_detail::DetailParams { id }).unwrap()
    }

    async fn stored_detail(&self, user: Uuid, id: Uuid) -> coralswarm_backend::read_models::Stored<coralswarm_backend::read_models::chats_detail::DetailPayload> {
        use coralswarm_backend::read_models as rm;
        let scope = self.detail_scope(user, id).await;
        let mut tx = self.app.begin_with("BEGIN READ ONLY").await.unwrap();
        sqlx::query("SELECT set_config('app.account_id',$1,true), set_config('app.user_id',$2,true)")
            .bind(self.account.to_string()).bind(user.to_string()).execute(&mut *tx).await.unwrap();
        let stored = rm::store::read::<rm::chats_detail::ConversationDetail>(&mut tx, &scope).await.unwrap();
        tx.rollback().await.unwrap();
        stored
    }

    async fn fill_detail(&self, user: Uuid, id: Uuid) {
        use coralswarm_backend::{read_models::{self as rm, chats_detail::ConversationDetail}, workers::{read_model_refresh, tenant_harness::WorkerTenant}, tenancy::TenantId};
        rm::store::request_refresh::<ConversationDetail>(&self.app, &self.detail_scope(user, id).await).await.unwrap();
        let worker = WorkerTenant { tenant: self.org_id.clone().map(TenantId::Org).unwrap_or(TenantId::Consumer), pool: self.app.clone(), store: Arc::new(PostgresStore::new(self.app.clone())) };
        let claims = rm::store::claim(&self.app).await.unwrap();
        assert!(!claims.is_empty());
        for claim in claims {
            let (payload, snapshot) = read_model_refresh::compute(&worker, &claim, &self.control_app).await.unwrap();
            assert!(rm::store::publish(&self.app, &claim, payload, &snapshot).await.unwrap());
        }
    }

    async fn done(self) {
        self.app.close().await;
        self.control_app.close().await;
        self.owner.close().await;
        for name in self.names {
            self.admin.execute(format!("DROP DATABASE {name} WITH (FORCE)").as_str()).await.unwrap();
        }
        self.admin.close().await;
    }
}

async fn read(r: Response) -> (StatusCode, Value) {
    let status = r.status();
    let bytes = to_bytes(r.into_body(), 4_000_000).await.unwrap();
    (status, serde_json::from_slice(&bytes).unwrap_or(Value::Null))
}

async fn prove(org: bool) {
    let _env = ENV_LOCK.lock().await;
    let Some(f) = Fixture::new(org).await else { return };
    let plane = if org { "org" } else { "personal" };
    let (a, b) = (f.a, f.b);
    let down = json!({"vote": "down", "reasons": ["outdated", "wrong", "outdated"], "note": "The date moved again."});
    let stored = json!({"vote": "down", "reasons": ["wrong", "outdated"], "note": "The date moved again."});
    let c = &f.private;

    std::env::remove_var(FLAG);
    assert_eq!(f.put(a, c.id, c.answer, down.clone()).await.0, StatusCode::NOT_FOUND, "{plane}: PUT while off");
    assert_eq!(f.delete(a, c.id, c.answer).await.0, StatusCode::NOT_FOUND, "{plane}: DELETE while off");
    let off = f.read_as_a(c.id).await;
    assert!(off.values().all(|m| m.get("feedback").is_none()), "{plane}: no feedback key while off");
    assert!(f.rows().await.is_empty());
    std::env::set_var(FLAG, "1");

    assert_eq!(f.put(a, c.id, c.answer, down.clone()).await, (StatusCode::OK, stored.clone()), "{plane}: PUT");
    let on = f.read_as_a(c.id).await;
    assert_eq!(on[&c.answer.to_string()]["feedback"], stored, "{plane}: read returns the vote");
    assert_eq!(on[&c.question.to_string()]["feedback"], Value::Null, "{plane}: question has none");
    let first = f.rows().await;
    assert_eq!(first.len(), 1);
    assert_eq!((first[0].0, first[0].1, first[0].2), (c.answer, a, -1));

    assert_eq!(f.put(a, c.id, c.answer, down.clone()).await, (StatusCode::OK, stored.clone()));
    assert_eq!(f.rows().await, first, "{plane}: a repeat PUT changes nothing");

    assert_eq!(f.put(a, c.id, c.answer, json!({"vote": "up"})).await, (StatusCode::OK, json!({"vote": "up", "reasons": [], "note": null})));
    let flipped = f.rows().await;
    assert_eq!(flipped.len(), 1);
    assert_eq!((flipped[0].2, flipped[0].3.len(), flipped[0].4.clone()), (1, 0, None));
    assert_eq!(f.put(a, c.id, c.answer, down.clone()).await.0, StatusCode::OK);
    let before_bad = f.rows().await;

    for bad in [
        json!({"vote": "down", "reasons": ["bogus"]}),
        json!({"vote": "down", "note": "x".repeat(2001)}),
        json!({"vote": "up", "reasons": ["wrong"]}),
        json!({"vote": "sideways"}),
    ] {
        assert_eq!(f.put(a, c.id, c.answer, bad.clone()).await.0, StatusCode::UNPROCESSABLE_ENTITY, "{plane}: {bad}");
    }
    assert_eq!(f.put(a, c.id, c.question, json!({"vote": "up"})).await.0, StatusCode::UNPROCESSABLE_ENTITY, "{plane}: question id");
    assert_eq!(f.delete(a, c.id, c.question).await.0, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(f.rows().await, before_bad);

    let missing = f.put(b, Uuid::new_v4(), Uuid::new_v4(), json!({"vote": "up"})).await;
    assert_eq!(missing, (StatusCode::NOT_FOUND, json!({"error": "not found"})));
    assert_eq!(f.put(b, c.id, c.answer, json!({"vote": "up"})).await, missing, "{plane}: B on A's private ocean");
    let u = &f.unshared;
    assert_eq!(f.put(b, u.id, u.answer, json!({"vote": "up"})).await, missing, "{plane}: B on an unshared thread");
    assert_eq!(f.put(a, c.id, u.answer, json!({"vote": "up"})).await, missing, "{plane}: answer of another conversation");
    assert_eq!(f.delete(b, c.id, c.answer).await, missing);
    assert_eq!(f.rows().await, before_bad);

    let s = &f.shared;
    assert_eq!(f.put(a, s.id, s.answer, json!({"vote": "up"})).await.0, StatusCode::OK);
    assert_eq!(f.put(b, s.id, s.answer, down.clone()).await, (StatusCode::OK, stored.clone()), "{plane}: B on a shared thread");
    let shared_rows: Vec<(Uuid, i16)> = f.rows().await.into_iter().filter(|r| r.0 == s.answer).map(|r| (r.1, r.2)).collect();
    assert_eq!(shared_rows.len(), 2);
    assert!(shared_rows.contains(&(a, 1)) && shared_rows.contains(&(b, -1)));
    assert_eq!(f.read_as_a(s.id).await[&s.answer.to_string()]["feedback"]["vote"], "up", "{plane}: A reads only A's vote");

    assert_eq!(f.delete(a, c.id, c.answer).await.0, StatusCode::NO_CONTENT);
    assert_eq!(f.delete(a, c.id, c.answer).await.0, StatusCode::NO_CONTENT);
    assert_eq!(f.read_as_a(c.id).await[&c.answer.to_string()]["feedback"], Value::Null);

    let mut tx = f.app.begin().await.unwrap();
    sqlx::query("SELECT set_config('app.account_id',$1,true), set_config('app.user_id',$2,true)")
        .bind(f.account.to_string()).bind(b.to_string()).execute(&mut *tx).await.unwrap();
    let forged = sqlx::query("INSERT INTO message_feedback(message_id,user_id,account_id,vote) VALUES($1,$2,$3,1)")
        .bind(u.answer).bind(a).bind(f.account).execute(&mut *tx).await;
    assert!(forged.is_err(), "{plane}: inserting a row for another user is refused");
    tx.rollback().await.unwrap();
    let mut tx = f.app.begin().await.unwrap();
    sqlx::query("SELECT set_config('app.account_id',$1,true), set_config('app.user_id',$2,true)")
        .bind(f.account.to_string()).bind(b.to_string()).execute(&mut *tx).await.unwrap();
    let updated = sqlx::query("UPDATE message_feedback SET vote=-1 WHERE user_id=$1").bind(a).execute(&mut *tx).await.unwrap();
    let deleted = sqlx::query("DELETE FROM message_feedback WHERE user_id=$1").bind(a).execute(&mut *tx).await.unwrap();
    assert_eq!((updated.rows_affected(), deleted.rows_affected()), (0, 0), "{plane}: B cannot touch A's rows");
    tx.rollback().await.unwrap();

    sqlx::query("DELETE FROM conversations WHERE id=$1").bind(s.id).execute(&f.owner).await.unwrap();
    assert!(f.rows().await.iter().all(|r| r.0 != s.answer), "{plane}: cascade");

    std::env::remove_var(FLAG);
    f.done().await;
}

#[tokio::test]
async fn message_feedback_personal_plane() {
    prove(false).await;
}

#[tokio::test]
async fn message_feedback_org_tenant_plane() {
    prove(true).await;
}

async fn prove_stored_feedback(org: bool) {
    let _guard = ENV_LOCK.lock().await;
    let Some(f) = Fixture::new(org).await else { return };
    std::env::set_var(FLAG, "1");
    let c = &f.private;
    f.fill_detail(f.a, c.id).await;
    let initial = f.stored_detail(f.a, c.id).await;
    assert!(initial.computed_at.is_some());
    let feedback = |s: &coralswarm_backend::read_models::Stored<coralswarm_backend::read_models::chats_detail::DetailPayload>, id: Uuid| {
        s.payload.messages.iter().find(|m| m["id"] == id.to_string()).unwrap()["feedback"].clone()
    };
    assert_eq!(feedback(&initial, c.answer), Value::Null);
    let down = json!({"vote":"down","reasons":["outdated"],"note":"Date changed"});
    assert_eq!(f.put(f.a, c.id, c.answer, down.clone()).await, (StatusCode::OK, down.clone()));
    assert_eq!(f.rows().await[0].2, -1, "the mutation fact really committed");
    // Deliberately do not run any worker after the mutation. Exercise exactly
    // the certified, pre-write page that a fast document reload can read.
    let reload = f.stored_detail(f.a, c.id).await;
    assert_eq!(reload.computed_at, initial.computed_at);
    assert!(reload.stale, "feedback must not certify the transcript fresh");
    assert_eq!(feedback(&reload, c.answer), down, "stored reload must retain the caller's saved vote");
    assert_eq!(feedback(&reload, c.question), Value::Null);
    let first = f.rows().await;
    assert_eq!(f.put(f.a, c.id, c.answer, down.clone()).await.0, StatusCode::OK);
    assert_eq!(f.rows().await, first);
    let up = json!({"vote":"up","reasons":[],"note":null});
    assert_eq!(f.put(f.a, c.id, c.answer, up.clone()).await.0, StatusCode::OK);
    assert_eq!(feedback(&f.stored_detail(f.a, c.id).await, c.answer), up);
    assert_eq!(f.delete(f.a, c.id, c.answer).await.0, StatusCode::NO_CONTENT);
    assert_eq!(feedback(&f.stored_detail(f.a, c.id).await, c.answer), Value::Null);

    let s = &f.shared;
    f.fill_detail(f.a, s.id).await;
    f.fill_detail(f.b, s.id).await;
    assert_eq!(f.put(f.a, s.id, s.answer, up.clone()).await.0, StatusCode::OK);
    assert_eq!(f.put(f.b, s.id, s.answer, down.clone()).await.0, StatusCode::OK);
    assert_eq!(feedback(&f.stored_detail(f.a, s.id).await, s.answer), up);
    assert_eq!(feedback(&f.stored_detail(f.b, s.id).await, s.answer), down);
    // Cache an actual vote, then revoke sharing without letting a worker run.
    f.fill_detail(f.b, s.id).await;
    sqlx::query("UPDATE conversations SET shared=false WHERE id=$1").bind(s.id).execute(&f.owner).await.unwrap();
    assert_eq!(feedback(&f.stored_detail(f.b, s.id).await, s.answer), Value::Null, "revoked conversation cannot supply a live vote or fall back to its cached vote");

    let scope = f.detail_scope(f.a, s.id).await;
    for (account, actor) in [(f.account, f.b), (Uuid::new_v4(), f.a)] {
        let mut tx = f.app.begin_with("BEGIN READ ONLY").await.unwrap();
        sqlx::query("SELECT set_config('app.account_id',$1,true),set_config('app.user_id',$2,true)")
            .bind(account.to_string()).bind(actor.to_string()).execute(&mut *tx).await.unwrap();
        let denied = coralswarm_backend::read_models::store::read::<coralswarm_backend::read_models::chats_detail::ConversationDetail>(&mut tx, &scope).await.unwrap();
        assert!(denied.computed_at.is_none() && denied.payload.messages.is_empty(), "no overlay on actor/account identity refusal");
    }

    // A live feedback read failure must fail the request, not reuse a saved
    // snapshot's control value. This lock is local to the disposable test DB.
    let mut barrier = f.owner.begin().await.unwrap();
    barrier.execute("LOCK TABLE message_feedback IN ACCESS EXCLUSIVE MODE").await.unwrap();
    let mut tx = f.app.begin_with("BEGIN READ ONLY").await.unwrap();
    sqlx::query("SELECT set_config('app.account_id',$1,true),set_config('app.user_id',$2,true),set_config('lock_timeout','30ms',true)")
        .bind(f.account.to_string()).bind(f.a.to_string()).execute(&mut *tx).await.unwrap();
    assert!(coralswarm_backend::read_models::store::read::<coralswarm_backend::read_models::chats_detail::ConversationDetail>(&mut tx, &scope).await.is_err());
    tx.rollback().await.unwrap();
    barrier.rollback().await.unwrap();

    sqlx::query("UPDATE read_models SET failed_attempts=1 WHERE model='conversation_detail'")
        .execute(&f.owner).await.unwrap();
    let withheld = f.stored_detail(f.a, s.id).await;
    assert!(withheld.computed_at.is_none() && withheld.payload.messages.is_empty(), "failed behind row remains withheld");
    sqlx::query("UPDATE read_models SET failed_attempts=0 WHERE model='conversation_detail'")
        .execute(&f.owner).await.unwrap();
    std::env::remove_var(FLAG);
    assert!(f.stored_detail(f.a, s.id).await.payload.messages.iter().all(|m| m.get("feedback").is_none()));
    f.done().await;
}

#[tokio::test]
async fn stored_feedback_personal_read_your_writes() {
    prove_stored_feedback(false).await;
}

#[tokio::test]
async fn stored_feedback_org_read_your_writes() {
    prove_stored_feedback(true).await;
}

#[tokio::test]
#[ignore = "isolated before/after stored-read cost probe; run alone"]
async fn stored_feedback_read_cost() {
    use coralswarm_backend::read_models::{self as rm, chats_detail::ConversationDetail as Detail, ReadModel};
    // Same real stored row/certificate/query, with the pre-fix no-op hook.
    // This control isolates the added read from cross-build/PG-version noise.
    struct TranscriptOnly;
    impl ReadModel for TranscriptOnly {
        const NAME: &'static str = Detail::NAME;
        const PATH: &'static str = Detail::PATH;
        const VERSION: i32 = Detail::VERSION;
        const PER_USER: bool = Detail::PER_USER;
        type Params = <Detail as ReadModel>::Params;
        type Payload = <Detail as ReadModel>::Payload;
        fn compute<'a>(cx: rm::ComputeCx<'a>, params: &'a Self::Params) -> rm::ComputeFuture<'a, Self::Payload> { Detail::compute(cx, params) }
        fn empty(params: &Self::Params) -> Self::Payload { Detail::empty(params) }
        fn dirty_triggers() -> &'static [rm::dirty::DirtyTrigger] { Detail::dirty_triggers() }
    }
    let _guard = ENV_LOCK.lock().await;
    let f = Fixture::new(false).await.expect("live database required");
    std::env::set_var(FLAG, "1");
    let c = &f.private;
    for seq in 3..=40 {
        sqlx::query("INSERT INTO conversation_messages(conversation_id,seq,role,text) VALUES($1,$2,$3,'cost probe')")
            .bind(c.id).bind(seq).bind(if seq % 2 == 0 { "assistant" } else { "user" }).execute(&f.owner).await.unwrap();
    }
    for (id,) in sqlx::query_as::<_, (Uuid,)>("SELECT id FROM conversation_messages WHERE conversation_id=$1 AND role='assistant'")
        .bind(c.id).fetch_all(&f.owner).await.unwrap() {
        assert_eq!(f.put(f.a, c.id, id, json!({"vote":"down"})).await.0, StatusCode::OK);
    }
    f.fill_detail(f.a, c.id).await;
    let hot = f.detail_scope(f.a, c.id).await;
    let cold = f.detail_scope(f.a, f.unshared.id).await;
    for (kind, scope) in [("cold", cold), ("hot", hot)] {
        let mut samples = [Vec::new(), Vec::new()];
        for i in 0..220 {
            for variant in if i % 2 == 0 { [0, 1] } else { [1, 0] } {
                let mut tx = f.app.begin_with("BEGIN READ ONLY").await.unwrap();
                sqlx::query("SELECT set_config('app.account_id',$1,true),set_config('app.user_id',$2,true)")
                    .bind(f.account.to_string()).bind(f.a.to_string()).execute(&mut *tx).await.unwrap();
                let start = std::time::Instant::now();
                let stored = if variant == 0 { rm::store::read::<TranscriptOnly>(&mut tx, &scope).await.unwrap() }
                    else { rm::store::read::<Detail>(&mut tx, &scope).await.unwrap() };
                let elapsed = start.elapsed().as_secs_f64() * 1000.0;
                assert_eq!(stored.computed_at.is_some(), kind == "hot");
                if kind == "hot" { assert_eq!(stored.payload.messages.len(), 40); }
                tx.rollback().await.unwrap();
                if i >= 20 { samples[variant].push(elapsed); }
            }
        }
        for s in &mut samples { s.sort_by(f64::total_cmp); }
        eprintln!("stored-feedback {kind}: 200 samples each; transcript-only p50={:.3}ms p95={:.3}ms; current-feedback p50={:.3}ms p95={:.3}ms",
            samples[0][99], samples[0][189], samples[1][99], samples[1][189]);
    }
    std::env::remove_var(FLAG);
    f.done().await;
}

#[tokio::test]
#[ignore = "live performance probe; run alone on an idle database"]
async fn message_feedback_performance() {
    let _guard = ENV_LOCK.lock().await;
    let f = Fixture::new(false).await.expect("live database required");
    let c = &f.private;
    for seq in 3..=40 {
        sqlx::query("INSERT INTO conversation_messages(conversation_id,seq,role,text) VALUES($1,$2,$3,'performance fixture')")
            .bind(c.id).bind(seq).bind(if seq % 2 == 0 { "assistant" } else { "user" })
            .execute(&f.owner).await.unwrap();
    }
    let mut read_p95 = Vec::new();
    for enabled in [false, true] {
        std::env::set_var(FLAG, if enabled { "1" } else { "0" });
        let mut samples = Vec::new();
        for i in 0..220 {
            let start = std::time::Instant::now();
            assert_eq!(f.read_as_a(c.id).await.len(), 40);
            if i >= 20 { samples.push(start.elapsed()); }
        }
        samples.sort();
        read_p95.push(samples[189].as_secs_f64() * 1000.0);
    }
    let mut puts = Vec::new();
    for i in 0..220 {
        let start = std::time::Instant::now();
        assert_eq!(f.put(f.a, c.id, c.answer, json!({"vote": if i % 2 == 0 { "up" } else { "down" }})).await.0, StatusCode::OK);
        if i >= 20 { puts.push(start.elapsed()); }
    }
    puts.sort();
    let put_p95 = puts[189].as_secs_f64() * 1000.0;
    eprintln!("20-turn read p95 flag-off={}ms flag-on={}ms; PUT p95={}ms", read_p95[0], read_p95[1], put_p95);
    std::env::remove_var(FLAG);
    f.done().await;
    assert!(read_p95[1] <= read_p95[0] * 1.10, "feedback read overhead exceeds 10%");
    assert!(put_p95 < 50.0, "PUT p95 exceeds 50ms");
}

//! Real worker/store/HTTP regression for embedding-bookkeeping invalidation.
//! Includes strict fresh-page safe-stale cases and permission-reduction controls.
//! Execution requires an explicitly released CPU window and owned isolated PG.
//! The embedding case calls the actual public worker seam, not an atom UPDATE.
//! The separate status cases exercise the exact private set_status SQL/trigger
//! boundary; they do not claim to execute the complete extraction worker.

#[path = "support/page_model_fixture.rs"]
mod fixture;

use axum::{
    body::Body,
    http::{Request, StatusCode},
    Extension, Router,
};
use coralswarm_backend::{
    jobs::{queue, JobKind},
    llm::{CompletionRequest, CompletionResponse, LlmError, LlmProvider, StreamEvent},
    read_models::{
        self as rm,
        billing::{UsageParams, UsageSummary},
        chats_detail::{ConversationDetail, DetailParams},
    },
    tenancy::{
        runtime::{TenancyDecision, TenantContext},
        scope::begin_tenant_scoped,
    },
    test_db::TestDatabase,
    workers::read_model_refresh,
    UserContext,
};
use coralswarm_core::embed_openrouter::OpenRouterEmbedder;
use fixture::Fixture;
use serde_json::{json, Value};
use sha2::{Digest, Sha256, Sha384};
use sqlx::{postgres::PgConnectOptions, Row};
use std::{
    collections::BTreeMap,
    path::PathBuf,
    pin::Pin,
    sync::{Arc, Mutex},
};
use tower::ServiceExt;
use uuid::Uuid;

const JOBS: usize = 8;
// Verbatim decision predicate from workers/source_atoms.rs::set_status.
const STATUS_UPDATE: &str = "UPDATE source_versions SET extraction_status=$4, extraction_error=$5 WHERE graph_id=$1 AND version_id=$2 AND next_start_char=$3 AND extraction_status<>'done'";

struct NeverLlm;
#[async_trait::async_trait]
impl LlmProvider for NeverLlm {
    async fn complete(&self, _: CompletionRequest) -> Result<CompletionResponse, LlmError> {
        panic!("embedding settlement regression must never call an LLM")
    }
    async fn stream_complete(
        &self,
        _: CompletionRequest,
    ) -> Result<Pin<Box<dyn futures::Stream<Item = Result<StreamEvent, LlmError>> + Send>>, LlmError>
    {
        panic!("embedding settlement regression must never call a streaming LLM")
    }
}

struct Harness {
    app: Router,
    f: Fixture,
    // Existing shared fixture owns its DBs but does not adopt generated roles.
    // These guards adopt ONLY those exact scratch DBs/logins for panic cleanup.
    owned_tenant: TestDatabase,
    owned_control: TestDatabase,
    atoms: Vec<Uuid>,
    node: Uuid,
    scopes: Vec<rm::Scope<DetailParams>>,
    version: Option<Uuid>,
    case: &'static str,
    output: PathBuf,
    compute_claims: Mutex<BTreeMap<String, rm::store::Claim>>,
}

impl Harness {
    async fn new(case: &'static str, with_version: bool) -> Self {
        assert_eq!(
            std::env::var("CS_EMBED_SETTLEMENT_RUN").as_deref(),
            Ok("owned-isolated-15911"),
            "explicit isolated test execution acknowledgement required; never skip green"
        );
        let base = std::env::var("PAGE_MODEL_TEST_BASE_URL")
            .expect("isolated PG maintenance URL required");
        let options: PgConnectOptions = base.parse().unwrap();
        assert_eq!(options.get_host(), "127.0.0.1");
        assert_eq!(
            options.get_port(),
            15911,
            "protected L6 port15861 is forbidden"
        );
        assert_eq!(options.get_database(), Some("postgres"));
        assert_ne!(
            std::env::var("ORGT_TEST_RETAIN_DATABASES").as_deref(),
            Ok("1")
        );
        let output = PathBuf::from(
            std::env::var("CS_EMBED_SETTLEMENT_EVIDENCE_DIR")
                .expect("private evidence output directory required"),
        );
        assert!(output.starts_with("/tmp/cs-page-load-l6/embedding-settlement"));
        std::fs::create_dir_all(&output).unwrap();
        // Synthetic test-only key, never a copied runtime/production secret.
        std::env::set_var("MASTER_KEK_HEX", "59".repeat(32));
        std::env::remove_var("MASTER_KEK_HEX_NEXT");
        let f = Fixture::new(true).await;
        let owned_tenant =
            TestDatabase::adopt(&options, f.owner.connect_options().get_database().unwrap());
        owned_tenant.adopt_role(f.app.connect_options().get_username());
        let owned_control = TestDatabase::adopt(
            &options,
            f.control_owner.connect_options().get_database().unwrap(),
        );
        owned_control.adopt_role(f.control.connect_options().get_username());
        let wrapped = coralswarm_core::kek::wrap_dek(&[0x59; 32], &[0x43; 32]).unwrap();
        sqlx::query("INSERT INTO account_keys(account_id,dek_wrapped) VALUES($1,$2)")
            .bind(f.account)
            .bind(wrapped)
            .execute(&f.owner)
            .await
            .unwrap();
        let node: Uuid = sqlx::query_scalar("INSERT INTO nodes(graph_id,type,title,body) VALUES($1,'entity','Synthetic settlement evidence','{}') RETURNING node_id")
            .bind(f.graph).fetch_one(&f.owner).await.unwrap();
        let mut atoms = Vec::new();
        for _ in 0..JOBS {
            let atom = Uuid::new_v4();
            // Same invalid two-component blob-key shape as the canonical bulk
            // fixture. Inline content exists; validation must fail before send.
            sqlx::query("INSERT INTO atoms(atom_id,graph_id,source_type,source_id,content_blob_key,content_inline,timestamp,ingested_by) VALUES($1,$2,'note',$3,$4,'Synthetic settlement evidence',now(),$5)")
                .bind(atom).bind(f.graph).bind(format!("synthetic-{atom}"))
                .bind(format!("blob/{atom}")).bind(f.user).execute(&f.owner).await.unwrap();
            sqlx::query("INSERT INTO node_atoms(node_id,atom_id) VALUES($1,$2)")
                .bind(node)
                .bind(atom)
                .execute(&f.owner)
                .await
                .unwrap();
            atoms.push(atom);
        }
        let version = if with_version {
            let item: Uuid = sqlx::query_scalar("INSERT INTO source_items(graph_id,source_type,provider_namespace,source_id,access_state) VALUES($1,'note','synthetic-settlement',$2,'permitted') RETURNING source_item_id")
                .bind(f.graph).bind(Uuid::new_v4().to_string()).fetch_one(&f.owner).await.unwrap();
            let version: Uuid = sqlx::query_scalar("INSERT INTO source_versions(graph_id,source_item_id,source_id,source_type,provider_namespace,raw_inline,extraction_status,extraction_version,next_start_char) VALUES($1,$2,'synthetic-version','note','synthetic-settlement','Synthetic settlement evidence','pending','source-atoms-v1',0) RETURNING version_id")
                .bind(f.graph).bind(item).fetch_one(&f.owner).await.unwrap();
            sqlx::query("UPDATE source_items SET current_version_id=$2 WHERE source_item_id=$1")
                .bind(item)
                .bind(version)
                .execute(&f.owner)
                .await
                .unwrap();
            sqlx::query("INSERT INTO source_version_chunks(graph_id,source_version_id,atom_id) VALUES($1,$2,$3)")
                .bind(f.graph).bind(version).bind(atoms[0]).execute(&f.owner).await.unwrap();
            Some(version)
        } else {
            None
        };
        let mut scopes = Vec::new();
        for _ in 0..3 {
            let id: Uuid = sqlx::query_scalar("INSERT INTO conversations(account_id,graph_id,user_id) VALUES($1,$2,$3) RETURNING id")
                .bind(f.account).bind(f.graph).bind(f.user).fetch_one(&f.owner).await.unwrap();
            scopes.push(Self::scope(&f, id).await);
        }
        let context = TenancyDecision::Org(TenantContext {
            org_id: "org_read_models".into(),
            account_id: f.account,
            user_id: f.user,
            pool: f.app.clone(),
            page_pool: None,
        });
        let state = Arc::new(coralswarm_backend::test_app_state_owned_with_db_and_store(
            f.control.clone(),
            None,
        ));
        let app = coralswarm_backend::test_support_v1_app(
            state,
            UserContext {
                user_id: f.user,
                account_id: f.account,
                role: "member".into(),
                device_id: "synthetic-settlement".into(),
            },
        )
        .layer(Extension(context));
        Self {
            app,
            f,
            owned_tenant,
            owned_control,
            atoms,
            node,
            scopes,
            version,
            case,
            output,
            compute_claims: Mutex::new(BTreeMap::new()),
        }
    }

    async fn authorize_usage(&self) {
        // A real privileged fixture member, never a content-RLS elevation.
        assert_eq!(
            sqlx::query(
                "UPDATE account_members SET role='admin' WHERE account_id=$1 AND user_id=$2"
            )
            .bind(self.f.account)
            .bind(self.f.user)
            .execute(&self.f.control_owner)
            .await
            .unwrap()
            .rows_affected(),
            1
        );
    }

    async fn usage_scope(&self) -> rm::Scope<UsageParams> {
        let mut tx = begin_tenant_scoped(&self.f.app, self.f.account, self.f.user)
            .await
            .unwrap();
        let mut viewer = rm::Viewer::load(&mut tx, self.f.account, self.f.user)
            .await
            .unwrap();
        let mut control = begin_tenant_scoped(&self.f.control, self.f.account, self.f.user)
            .await
            .unwrap();
        viewer.certify_control(&mut control).await.unwrap();
        let scope = viewer
            .scope::<UsageSummary>(UsageParams::default())
            .unwrap();
        control.rollback().await.unwrap();
        tx.rollback().await.unwrap();
        scope
    }

    async fn warm_usage(&self) -> rm::Scope<UsageParams> {
        let scope = self.usage_scope().await;
        rm::store::request_refresh::<UsageSummary>(&self.f.app, &scope)
            .await
            .unwrap();
        for _ in 0..8 {
            // Only this isolated harness's real registered keys can exist.
            for claim in rm::store::claim(&self.f.app).await.unwrap() {
                let (payload, snapshot) =
                    read_model_refresh::compute(&self.f.worker(), &claim, &self.f.control)
                        .await
                        .unwrap();
                assert!(rm::store::publish(&self.f.app, &claim, payload, &snapshot)
                    .await
                    .unwrap());
                self.compute_claims
                    .lock()
                    .unwrap()
                    .insert(claim.scope_key.clone(), claim);
            }
            let mut tx = begin_tenant_scoped(&self.f.app, self.f.account, self.f.user)
                .await
                .unwrap();
            let stored = rm::store::read::<UsageSummary>(&mut tx, &scope)
                .await
                .unwrap();
            tx.rollback().await.unwrap();
            if stored.computed_at.is_some() && !stored.stale && stored.refresh_eta.is_none() {
                return scope;
            }
            tokio::task::yield_now().await;
        }
        panic!("actual worker did not certify whole-account usage");
    }

    async fn usage_snapshot(&self, original: &rm::Scope<UsageParams>) -> Value {
        let scope = self.usage_scope().await;
        assert_eq!(
            original.key(),
            scope.key(),
            "no authority/key change in metering control"
        );
        let mut tx = begin_tenant_scoped(&self.f.app, self.f.account, self.f.user)
            .await
            .unwrap();
        let stored = rm::store::read::<UsageSummary>(&mut tx, &scope)
            .await
            .unwrap();
        let row = sqlx::query("SELECT computed_at,computed_snapshot::text snapshot,invalidated_xid::text invalidator,dirty_generation,dirty_at,(invalidated_xid IS NULL OR pg_visible_in_snapshot(invalidated_xid,computed_snapshot)) local_valid,read_model_fence_state(account_id,ARRAY['00000000-0000-0000-0000-000000000000'::uuid],model,viewer_user_id,computed_snapshot) fence_state FROM read_models WHERE account_id=$1 AND model='usage_summary' AND scope_key=$2")
            .bind(self.f.account).bind(scope.key()).fetch_one(&mut *tx).await.unwrap();
        tx.rollback().await.unwrap();
        let period = scope
            .params
            .calendar_month
            .and_hms_opt(0, 0, 0)
            .unwrap()
            .and_utc();
        let truth: i64 = sqlx::query_scalar("SELECT count(*) FROM atoms a JOIN graphs g ON g.id=a.graph_id WHERE g.account_id=$1 AND a.created_at >= $2")
            .bind(self.f.account).bind(period).fetch_one(&self.f.owner).await.unwrap();
        // Real Usage access policy and HTTP handler; synthetic, verified admin.
        let state = Arc::new(coralswarm_backend::test_app_state_owned_with_db_and_store(
            self.f.control.clone(),
            None,
        ));
        let app = coralswarm_backend::test_support_v1_app(
            state,
            UserContext {
                user_id: self.f.user,
                account_id: self.f.account,
                role: "admin".into(),
                device_id: "synthetic-metering".into(),
            },
        )
        .layer(Extension(TenancyDecision::Org(TenantContext {
            org_id: "org_read_models".into(),
            account_id: self.f.account,
            user_id: self.f.user,
            pool: self.f.app.clone(),
            page_pool: None,
        })));
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/usage/summary")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let bytes = axum::body::to_bytes(response.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let body: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(body["model"], "usage_summary");
        assert_eq!(body["computed_at"], json!(stored.computed_at));
        json!({"key":scope.key(),"truth_atoms_ingested":truth,
            "stored_computed_at":row.get::<Option<chrono::DateTime<chrono::Utc>>,_>("computed_at"),
            "snapshot":row.get::<Option<String>,_>("snapshot"),"invalidator":row.get::<Option<String>,_>("invalidator"),
            "generation":row.get::<i64,_>("dirty_generation"),"dirty_at":row.get::<Option<chrono::DateTime<chrono::Utc>>,_>("dirty_at"),
            "local_valid":row.get::<Option<bool>,_>("local_valid"),"fence_state":row.get::<i32,_>("fence_state"),
            "served":{"computed_at":body["computed_at"],"stale":body["stale"],"refresh_eta":body["refresh_eta"],"atoms_ingested":body["payload"]["atoms_ingested"]}})
    }

    async fn graph_fences(&self, accounts: &[Uuid], graphs: &[Uuid]) -> Value {
        sqlx::query_scalar("SELECT COALESCE(jsonb_agg(to_jsonb(f) ORDER BY f.account_id,f.graph_id),'[]'::jsonb) FROM (SELECT account_id,graph_id,changed_xid::text,invalidated_xid::text FROM read_model_graph_fences WHERE account_id=ANY($1) AND graph_id=ANY($2) LIMIT 12) f")
            .bind(accounts).bind(graphs).fetch_one(&self.f.owner).await.unwrap()
    }

    async fn hidden_atom(&self) -> Uuid {
        let mut tx = begin_tenant_scoped(&self.f.app, self.f.account, self.f.user)
            .await
            .unwrap();
        assert!(!rm::Viewer::load(&mut tx, self.f.account, self.f.user)
            .await
            .unwrap()
            .visible_graphs()
            .contains(&self.f.foreign));
        tx.rollback().await.unwrap();
        sqlx::query_scalar("INSERT INTO atoms(graph_id,source_type,source_id,content_blob_key,timestamp) VALUES($1,'note',$2,'synthetic-metering',now()) RETURNING atom_id")
            .bind(self.f.foreign).bind(Uuid::new_v4().to_string()).fetch_one(&self.f.owner).await.unwrap()
    }

    async fn migration_receipt(&self) {
        let tenant = include_bytes!("../migrations_tenant/0227_page_model_operational_fences.sql");
        let control =
            include_bytes!("../migrations_control/0416_page_model_operational_fences.sql");
        let tenant_freshness =
            include_bytes!("../migrations_tenant/0228_page_model_operational_freshness.sql");
        let control_freshness =
            include_bytes!("../migrations_control/0417_page_model_operational_freshness.sql");
        let mut receipt = Vec::new();
        for (plane, pool, version, sql) in [
            ("tenant", &self.f.owner, 227_i64, tenant.as_slice()),
            (
                "control",
                &self.f.control_owner,
                416_i64,
                control.as_slice(),
            ),
            (
                "tenant",
                &self.f.owner,
                228_i64,
                tenant_freshness.as_slice(),
            ),
            (
                "control",
                &self.f.control_owner,
                417_i64,
                control_freshness.as_slice(),
            ),
        ] {
            let expected = format!("{:x}", Sha384::digest(sql));
            let installed: String = sqlx::query_scalar(
                "SELECT encode(checksum,'hex') FROM _sqlx_migrations WHERE version=$1 AND success",
            )
            .bind(version)
            .fetch_one(pool)
            .await
            .unwrap();
            assert_eq!(
                installed, expected,
                "installed migration must match exact embedded bytes"
            );
            let rows = sqlx::query("SELECT p.proname,p.prosecdef,p.proconfig::text config,NOT EXISTS(SELECT 1 FROM aclexplode(COALESCE(p.proacl,acldefault('f',p.proowner))) x WHERE x.grantee=0 AND x.privilege_type='EXECUTE') public_execute_revoked FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='public' AND p.proname IN ('read_models_mark_dirty','read_models_metering_dirty') AND p.pronargs=0 ORDER BY p.proname")
                .fetch_all(pool).await.unwrap();
            assert_eq!(rows.len(), 2);
            let functions:Vec<Value> = rows.into_iter().map(|row| {
                let config:String = row.get("config");
                assert!(config.replace(' ',"").contains("search_path=pg_catalog,public,pg_temp"));
                assert!(row.get::<bool,_>("prosecdef"));
                assert!(row.get::<bool,_>("public_execute_revoked"));
                json!({"name":row.get::<String,_>("proname"),"security_definer":true,"public_execute_revoked":true,"config":config})
            }).collect();
            receipt.push(json!({"plane":plane,"version":version,"embedded_sql_sha384":expected,"installed_sql_sha384":installed,"functions":functions}));
        }
        self.evidence("migration-receipt", json!(receipt));
    }

    async fn prepare_nonempty_page_facts(&self) {
        let mut library =
            coralswarm_backend::library_read::LibraryScope::exact_node(self.f.graph, self.node);
        library.normalize().unwrap();
        for detail in &self.scopes {
            sqlx::query("UPDATE conversations SET library_scope=$2 WHERE id=$1")
                .bind(detail.params.id)
                .bind(json!(library))
                .execute(&self.f.owner)
                .await
                .unwrap();
            sqlx::query("INSERT INTO conversation_messages(conversation_id,seq,role,text) VALUES($1,1,'user','Synthetic operational bookkeeping question')")
                .bind(detail.params.id).execute(&self.f.owner).await.unwrap();
        }
    }

    async fn recompute_actual(&self, key: &str) -> (Value, String) {
        // Metadata from an ACTUAL published claim. Its retired token is never
        // used for publication/refresh/failure. The real public worker rechecks
        // live membership, Viewer keys/windows and native org Usage authority,
        // then reads in NEW read-only repeatable snapshots without writing.
        let claim = {
            let claims = self.compute_claims.lock().unwrap();
            let c = claims
                .get(key)
                .expect("an actually warmed claim is required");
            rm::store::Claim {
                model: c.model.clone(),
                scope_key: c.scope_key.clone(),
                version: c.version,
                account_id: c.account_id,
                graph_ids: c.graph_ids.clone(),
                compute_as: c.compute_as,
                params: c.params.clone(),
                dirty_generation: c.dirty_generation,
                claim_token: c.claim_token,
                control_revision: c.control_revision,
                control_freshness: c.control_freshness,
                immediate_due_at: c.immediate_due_at,
                claimed_at: c.claimed_at,
                first_fill: c.first_fill,
                due_to_claim_ms: c.due_to_claim_ms,
            }
        };
        read_model_refresh::compute(&self.f.worker(), &claim, &self.f.control)
            .await
            .unwrap()
    }

    async fn actual_payloads(&self, usage: &rm::Scope<UsageParams>) -> Value {
        let mut details = Vec::new();
        for scope in &self.scopes {
            let (payload, snapshot) = self.recompute_actual(scope.key()).await;
            assert_eq!(payload["messages"].as_array().unwrap().len(), 1);
            assert_eq!(payload["conversation"]["scope_status"], "resolved");
            details.push(json!({"id":scope.params.id,"payload":payload,"snapshot":snapshot}));
        }
        let (payload, snapshot) = self.recompute_actual(usage.key()).await;
        json!({"details":details,"usage":{"payload":payload,"snapshot":snapshot}})
    }

    async fn scope(f: &Fixture, id: Uuid) -> rm::Scope<DetailParams> {
        let mut tx = begin_tenant_scoped(&f.app, f.account, f.user)
            .await
            .unwrap();
        let viewer = rm::Viewer::load(&mut tx, f.account, f.user).await.unwrap();
        let scope = viewer
            .scope::<ConversationDetail>(DetailParams { id })
            .unwrap();
        tx.rollback().await.unwrap();
        scope
    }

    async fn warm(&self) {
        for scope in &self.scopes {
            rm::store::request_refresh::<ConversationDetail>(&self.f.app, scope)
                .await
                .unwrap();
            let mut settled = false;
            // An actual missing HTTP read may have asynchronously enqueued the
            // same key. Drain through real claims/publication, never clear its
            // dirty generation or bypass a fence to make the fixture green.
            for _ in 0..8 {
                let claims = rm::store::claim(&self.f.app).await.unwrap();
                for claim in claims
                    .into_iter()
                    .filter(|c| c.model == "conversation_detail")
                {
                    let (payload, snapshot) =
                        read_model_refresh::compute(&self.f.worker(), &claim, &self.f.control)
                            .await
                            .unwrap();
                    assert!(rm::store::publish(&self.f.app, &claim, payload, &snapshot)
                        .await
                        .unwrap());
                    self.compute_claims
                        .lock()
                        .unwrap()
                        .insert(claim.scope_key.clone(), claim);
                }
                tokio::task::yield_now().await;
                let mut tx = begin_tenant_scoped(&self.f.app, self.f.account, self.f.user)
                    .await
                    .unwrap();
                let stored = rm::store::read::<ConversationDetail>(&mut tx, scope)
                    .await
                    .unwrap();
                tx.rollback().await.unwrap();
                if stored.computed_at.is_some() && !stored.stale && stored.refresh_eta.is_none() {
                    settled = true;
                    break;
                }
            }
            assert!(settled, "actual worker did not certify the warmed detail");
        }
    }

    async fn served_certificate(&self, id: Uuid) -> Value {
        let response = self
            .app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/v1/load/conversations/{id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let bytes = axum::body::to_bytes(response.into_body(), 65536)
            .await
            .unwrap();
        let body: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(body["model"], "conversation_detail");
        assert_eq!(body["version"], 1);
        json!({"model":body["model"],"version":body["version"],
            "computed_at":body["computed_at"],"stale":body["stale"],"refresh_eta":body["refresh_eta"],
            "conversation_id":body["payload"]["conversation"]["id"]})
    }

    async fn snapshot(&self) -> Value {
        let mut tx = begin_tenant_scoped(&self.f.app, self.f.account, self.f.user)
            .await
            .unwrap();
        let admitted: Vec<Uuid> = sqlx::query_scalar(
            "SELECT atom_id FROM library_permitted_atoms($1,$2) ORDER BY atom_id",
        )
        .bind(self.f.graph)
        .bind(&self.atoms)
        .fetch_all(&mut *tx)
        .await
        .unwrap();
        let supports: Vec<(Uuid, Uuid)> = sqlx::query_as("SELECT node_id,atom_id FROM node_atoms WHERE node_id=$1 AND atom_id=ANY($2) ORDER BY atom_id")
            .bind(self.node).bind(&admitted).fetch_all(&mut *tx).await.unwrap();
        let windows: Vec<(Uuid, Option<chrono::DateTime<chrono::Utc>>)> = sqlx::query_as(
            "SELECT graph_id,history_floor FROM read_model_visible_windows() ORDER BY graph_id",
        )
        .fetch_all(&mut *tx)
        .await
        .unwrap();
        tx.rollback().await.unwrap();
        let graph_fences: Value = sqlx::query_scalar("SELECT COALESCE(jsonb_agg(to_jsonb(f)),'[]'::jsonb) FROM (SELECT graph_id,changed_xid::text,invalidated_xid::text FROM read_model_graph_fences WHERE account_id=$1 AND graph_id=$2) f")
            .bind(self.f.account).bind(self.f.graph).fetch_one(&self.f.owner).await.unwrap();
        let user_fences: Value = sqlx::query_scalar("SELECT COALESCE(jsonb_agg(to_jsonb(f)),'[]'::jsonb) FROM (SELECT model,user_id,changed_xid::text,invalidated_xid::text FROM read_model_user_fences WHERE account_id=$1 AND model IN ('conversation_detail','*') AND user_id IN ($2,'00000000-0000-0000-0000-000000000000'::uuid) ORDER BY model,user_id LIMIT 4) f")
            .bind(self.f.account).bind(self.f.user).fetch_one(&self.f.owner).await.unwrap();
        let atom_state: Value = sqlx::query_scalar("SELECT jsonb_agg(to_jsonb(a)) FROM (SELECT atom_id,embed_attempts,embed_failed_at,embed_model_next,md5(jsonb_build_array(graph_id,evidence_retracted,derived_copy_bound,source_evidence_bound,timestamp,content_inline,participants,source_type,source_id)::text) admission_tuple_digest,md5((to_jsonb(atoms)-ARRAY['embed_failed_at','embed_attempts','embed_model_next'])::text) non_bookkeeping_digest,COALESCE(embedding_next IS NOT NULL AND embed_model_next=$3,false) current_cloud_eligible FROM atoms WHERE graph_id=$1 AND atom_id=ANY($2) ORDER BY atom_id) a")
            .bind(self.f.graph).bind(&self.atoms).bind(coralswarm_core::embed::EMBED_MODEL_CLOUD).fetch_one(&self.f.owner).await.unwrap();
        let mut details = Vec::new();
        for scope in &self.scopes {
            let current = Self::scope(&self.f, scope.params.id).await;
            let mut member = begin_tenant_scoped(&self.f.app, self.f.account, self.f.user)
                .await
                .unwrap();
            let row = sqlx::query("SELECT computed_at,computed_snapshot::text snapshot,invalidated_xid::text invalidator,dirty_generation,dirty_at,retry_after,(invalidated_xid IS NULL OR pg_visible_in_snapshot(invalidated_xid,computed_snapshot)) local_valid,read_model_fence_state(account_id,graph_ids,model,viewer_user_id,computed_snapshot) fence_state FROM read_models WHERE account_id=$1 AND model='conversation_detail' AND scope_key=$2 AND version=1")
                .bind(self.f.account).bind(current.key()).fetch_optional(&mut *member).await.unwrap();
            member.rollback().await.unwrap();
            let metadata = row.map(|r| json!({
                "stored_computed_at":r.get::<Option<chrono::DateTime<chrono::Utc>>,_>("computed_at"),
                "snapshot":r.get::<Option<String>,_>("snapshot"),"invalidator":r.get::<Option<String>,_>("invalidator"),
                "generation":r.get::<i64,_>("dirty_generation"),"dirty_at":r.get::<Option<chrono::DateTime<chrono::Utc>>,_>("dirty_at"),
                "retry_after":r.get::<Option<chrono::DateTime<chrono::Utc>>,_>("retry_after"),
                "local_valid":r.get::<Option<bool>,_>("local_valid"),"fence_state":r.get::<i32,_>("fence_state")
            }));
            details.push(json!({"id":scope.params.id,"key":current.key(),"row_found":metadata.is_some(),
                "row_before_HTTP":metadata,"served":self.served_certificate(scope.params.id).await}));
        }
        let version = if let Some(id) = self.version {
            sqlx::query_scalar::<_, Value>("SELECT jsonb_build_object('id',version_id,'status',extraction_status,'error',extraction_error,'cursor',next_start_char,'row_digest',md5(to_jsonb(v)::text)) FROM source_versions v WHERE graph_id=$1 AND version_id=$2")
                .bind(self.f.graph).bind(id).fetch_one(&self.f.owner).await.unwrap()
        } else {
            Value::Null
        };
        json!({"account":self.f.account,"actor":self.f.user,"graph":self.f.graph,"admitted_atom_ids":admitted,
            "admitted_node_atom_support_ids":supports,"viewer_windows":windows,"graph_fences":graph_fences,
            "user_fences":user_fences,"atom_state":atom_state,"details":details,"source_version":version})
    }

    fn evidence(&self, stage: &str, value: Value) {
        // Deliberately curate every field above: no payload/text/DEK/URL/token.
        let path = self.output.join(format!("{}-{stage}.json", self.case));
        std::fs::write(path, serde_json::to_vec_pretty(&value).unwrap()).unwrap();
    }

    async fn positive_removal(&self) {
        self.warm().await;
        let before = self.snapshot().await;
        assert_fresh(&before);
        let mut tx = self.f.owner.begin().await.unwrap();
        assert_eq!(
            sqlx::query("DELETE FROM node_atoms WHERE node_id=$1 AND atom_id=$2")
                .bind(self.node)
                .bind(self.atoms[JOBS - 1])
                .execute(&mut *tx)
                .await
                .unwrap()
                .rows_affected(),
            1
        );
        let xid: String = sqlx::query_scalar("SELECT pg_current_xact_id()::text")
            .fetch_one(&mut *tx)
            .await
            .unwrap();
        tx.commit().await.unwrap();
        let after = self.snapshot().await;
        self.evidence(
            "genuine-support-removal",
            json!({"writer_xid":xid,"before":before,"after":after}),
        );
        assert_eq!(before["viewer_windows"], after["viewer_windows"]);
        assert_ne!(
            before["admitted_node_atom_support_ids"],
            after["admitted_node_atom_support_ids"]
        );
        for (a, b) in before["details"]
            .as_array()
            .unwrap()
            .iter()
            .zip(after["details"].as_array().unwrap())
        {
            assert_eq!(a["key"], b["key"]);
            assert_eq!(b["row_before_HTTP"]["fence_state"], 0);
            assert!(b["served"]["computed_at"].is_string());
            assert_eq!(
                a["served"]["computed_at"], b["served"]["computed_at"],
                "genuine reduction serves the last answer stale until recompute"
            );
            assert_eq!(b["served"]["stale"], true);
        }
    }

    async fn close(self) {
        let Self {
            app,
            f,
            owned_tenant,
            owned_control,
            ..
        } = self;
        drop(app);
        f.close().await;
        drop(owned_tenant);
        drop(owned_control);
    }
}

fn assert_fresh(s: &Value) {
    assert_eq!(s["details"].as_array().unwrap().len(), 3);
    for d in s["details"].as_array().unwrap() {
        assert_eq!(d["row_found"], true);
        assert_eq!(d["row_before_HTTP"]["local_valid"], true);
        assert_eq!(d["row_before_HTTP"]["fence_state"], 2);
        assert!(d["served"]["computed_at"].is_string());
        assert_eq!(d["served"]["stale"], false);
        assert!(d["served"]["refresh_eta"].is_null());
        assert_eq!(d["served"]["conversation_id"], d["id"]);
    }
}

fn assert_same_admission_and_keys(before: &Value, after: &Value) {
    assert_eq!(before["admitted_atom_ids"], after["admitted_atom_ids"]);
    assert_eq!(
        before["admitted_node_atom_support_ids"],
        after["admitted_node_atom_support_ids"]
    );
    assert_eq!(before["viewer_windows"], after["viewer_windows"]);
    for (a, b) in before["details"]
        .as_array()
        .unwrap()
        .iter()
        .zip(after["details"].as_array().unwrap())
    {
        assert_eq!(a["key"], b["key"]);
        assert_eq!(
            a["row_before_HTTP"]["snapshot"],
            b["row_before_HTTP"]["snapshot"]
        );
        assert_eq!(
            a["row_before_HTTP"]["stored_computed_at"],
            b["row_before_HTTP"]["stored_computed_at"]
        );
    }
}

fn payload_digest(v: &Value) -> String {
    format!("{:x}", Sha256::digest(serde_json::to_vec(v).unwrap()))
}

fn changed_paths(a: &Value, b: &Value, prefix: &str, out: &mut Vec<String>) {
    if a == b || out.len() >= 32 {
        return;
    }
    match (a, b) {
        (Value::Object(a), Value::Object(b)) => {
            let mut keys: Vec<_> = a.keys().chain(b.keys()).collect();
            keys.sort();
            keys.dedup();
            for k in keys {
                changed_paths(
                    a.get(k).unwrap_or(&Value::Null),
                    b.get(k).unwrap_or(&Value::Null),
                    &format!("{prefix}/{k}"),
                    out,
                );
            }
        }
        (Value::Array(a), Value::Array(b)) if a.len() == b.len() => {
            for (i, (a, b)) in a.iter().zip(b).enumerate() {
                changed_paths(a, b, &format!("{prefix}/{i}"), out);
            }
        }
        _ => out.push(prefix.to_owned()),
    }
}

fn compare_actual_payloads(before: &Value, after: &Value) -> Value {
    let mut details = Vec::new();
    let mut all = true;
    assert_eq!(before["details"].as_array().unwrap().len(), 3);
    assert_eq!(after["details"].as_array().unwrap().len(), 3);
    for (a, b) in before["details"]
        .as_array()
        .unwrap()
        .iter()
        .zip(after["details"].as_array().unwrap())
    {
        let equal = a["payload"] == b["payload"];
        all &= equal;
        let mut paths = Vec::new();
        changed_paths(&a["payload"], &b["payload"], "", &mut paths);
        details.push(json!({"id":a["id"],"full_payload_equal":equal,"before_sha256":payload_digest(&a["payload"]),"after_sha256":payload_digest(&b["payload"]),"changed_paths":paths,"before_snapshot":a["snapshot"],"after_snapshot":b["snapshot"]}));
    }
    let u = &before["usage"]["payload"];
    let v = &after["usage"]["payload"];
    let mut aligned = v.clone();
    // The public native-org worker seam has no clock injection. billing.rs
    // sets ONLY period_end=cx.now. Keep raw hashes/diff paths; align exactly
    // that external clock input, then compare EVERY other UsagePayload field.
    // Do not construct the private BlobReader or use a personal-plane fallback.
    assert!(u["period_end"].is_string() && v["period_end"].is_string());
    aligned["period_end"] = u["period_end"].clone();
    let equal = *u == aligned;
    all &= equal;
    let mut paths = Vec::new();
    changed_paths(u, v, "", &mut paths);
    json!({"all_page_facts_equal":all,"details":details,"usage":{"raw_full_payload_equal":u==v,"raw_changed_paths":paths,
        "raw_before_sha256":payload_digest(u),"raw_after_sha256":payload_digest(v),"clock_aligned_full_payload_equal":equal,
        "aligned_after_sha256":payload_digest(&aligned),"period_end_before":u["period_end"],"period_end_after":v["period_end"],
        "clock_alignment":"ONLY period_end, directly assigned ComputeCx.now in billing.rs; no model state excluded",
        "before_snapshot":before["usage"]["snapshot"],"after_snapshot":after["usage"]["snapshot"]}})
}

fn assert_unchanged_pages_fresh(
    before: &Value,
    after: &Value,
    usage_before: &Value,
    usage_after: &Value,
) {
    assert_same_admission_and_keys(before, after);
    for (a, b) in before["atom_state"]
        .as_array()
        .unwrap()
        .iter()
        .zip(after["atom_state"].as_array().unwrap())
    {
        assert_eq!(a["non_bookkeeping_digest"], b["non_bookkeeping_digest"]);
        assert_eq!(a["admission_tuple_digest"], b["admission_tuple_digest"]);
        assert_eq!(a["current_cloud_eligible"], b["current_cloud_eligible"]);
    }
    // Evidence is saved before the intentionally RED-capable assertions.
    // No worker refresh or publication repairs dirty state between readings.
    for b in after["details"].as_array().unwrap() {
        assert!(b["served"]["computed_at"].is_string());
        assert_eq!(
            b["served"]["stale"], false,
            "operational-only dirty made an unchanged actual detail payload stale"
        );
        assert!(b["served"]["refresh_eta"].is_null());
        assert_eq!(b["row_before_HTTP"]["fence_state"], 2);
    }
    assert_eq!(
        usage_before["served"]["computed_at"],
        usage_after["served"]["computed_at"]
    );
    assert_eq!(
        usage_after["served"]["stale"], false,
        "operational-only dirty made unchanged usage facts stale"
    );
    assert!(usage_after["served"]["refresh_eta"].is_null());
    assert_eq!(usage_after["fence_state"], 2);
}

#[tokio::test]
async fn eight_actual_dead_settlements_preserve_unchanged_page_freshness() {
    let h = Harness::new("embedding-eight-dead", false).await;
    h.migration_receipt().await;
    h.authorize_usage().await;
    // This existing public worker seam passes blobs=None. With inline text,
    // that mode deliberately tolerates an invalid key and reaches the managed
    // principal gate instead. Remove only the synthetic inline fallback BEFORE
    // warming so the real validator returns the retained pre-send error. The
    // job/atom settlement and trigger path is unchanged; this does not claim
    // to reproduce the production blob-store configuration.
    assert_eq!(
        sqlx::query("UPDATE atoms SET content_inline=NULL,embed_model_next=CASE WHEN atom_id=ANY($3) THEN 'blocked:managed_purpose_unsupported' ELSE NULL END WHERE graph_id=$1 AND atom_id=ANY($2)")
            .bind(h.f.graph)
            .bind(&h.atoms)
            .bind(&h.atoms[..JOBS/2])
            .execute(&h.f.owner)
            .await
            .unwrap()
            .rows_affected(),
        JOBS as u64
    );
    h.prepare_nonempty_page_facts().await;
    let mut ids = Vec::new();
    for atom in &h.atoms {
        ids.push(
            queue::enqueue(
                &h.f.app,
                &JobKind::EmbedAtomsRetry {
                    atom_ids: vec![*atom],
                },
                Some(h.f.account),
                None,
                0,
            )
            .await
            .unwrap()
            .unwrap(),
        );
    }
    sqlx::query("UPDATE jobs SET attempts=max_attempts-1 WHERE account_id=$1 AND id=ANY($2)")
        .bind(h.f.account)
        .bind(&ids)
        .execute(&h.f.owner)
        .await
        .unwrap();
    // Claim actual rows before warming, leaving ONLY the settlement as writer.
    let jobs = queue::dequeue_ids(&h.f.app, &["EmbedAtomsRetry"], &ids)
        .await
        .unwrap();
    assert_eq!(jobs.len(), JOBS);
    h.warm().await;
    let usage_scope = h.warm_usage().await;
    let usage_before = h.usage_snapshot(&usage_scope).await;
    let payloads_before = h.actual_payloads(&usage_scope).await;
    let before = h.snapshot().await;
    assert_fresh(&before);
    assert!(!before["admitted_node_atom_support_ids"]
        .as_array()
        .unwrap()
        .is_empty());
    let receipt_count: i64 = sqlx::query_scalar("SELECT count(*) FROM managed_attempt_receipts WHERE account_id=$1 AND purpose='cloud_embedding'")
        .bind(h.f.account).fetch_one(&h.f.owner).await.unwrap();
    let jobs_before: Value = sqlx::query_scalar("SELECT jsonb_agg(to_jsonb(j)) FROM (SELECT id,kind,status::text,attempts,max_attempts FROM jobs WHERE account_id=$1 AND id=ANY($2) ORDER BY id) j")
        .bind(h.f.account).bind(&ids).fetch_one(&h.f.owner).await.unwrap();
    h.evidence(
        "before-worker",
        json!({"snapshot":before,"jobs":jobs_before,"send_receipts":receipt_count}),
    );
    assert_eq!(receipt_count, 0);
    assert_eq!(jobs_before.as_array().unwrap().len(), JOBS);
    for job in jobs_before.as_array().unwrap() {
        assert_eq!(job["status"], "running");
    }
    // Real sealed transport, no replacement implementation. If code ever
    // reaches send unexpectedly, HTTPS-only rejects this local HTTP endpoint;
    // no real provider or local listener exists. The exact blob-key cause and
    // unchanged receipt count below prove the intended pre-send branch.
    let cloud = OpenRouterEmbedder::new(
        reqwest::Client::builder()
            .https_only(true)
            .no_proxy()
            .build()
            .unwrap(),
        "synthetic-unused-key",
    )
    .with_base_url("http://127.0.0.1:1");
    let tenant = h.f.worker().tenant;
    futures::future::join_all(jobs.into_iter().map(|job| {
        coralswarm_backend::test_p10_automatic_worker(
            &tenant,
            &h.f.app,
            &h.f.control,
            &cloud,
            &NeverLlm,
            job,
        )
    }))
    .await;
    let after = h.snapshot().await;
    let usage_after = h.usage_snapshot(&usage_scope).await;
    let payloads_after = h.actual_payloads(&usage_scope).await;
    let payload_proof = compare_actual_payloads(&payloads_before, &payloads_after);
    h.evidence("actual-recompute-proof", payload_proof.clone());
    let jobs: Value = sqlx::query_scalar("SELECT jsonb_agg(to_jsonb(j)) FROM (SELECT id,status::text,attempts,max_attempts,error_message,finished_at IS NOT NULL finished FROM jobs WHERE account_id=$1 AND id=ANY($2) ORDER BY id) j")
        .bind(h.f.account).bind(&ids).fetch_one(&h.f.owner).await.unwrap();
    let atoms: Value = sqlx::query_scalar("SELECT jsonb_agg(to_jsonb(a)) FROM (SELECT atom_id,embed_attempts,embed_failed_at IS NOT NULL failure_stamped,embed_model_next FROM atoms WHERE graph_id=$1 AND atom_id=ANY($2) ORDER BY atom_id) a")
        .bind(h.f.graph).bind(&h.atoms).fetch_one(&h.f.owner).await.unwrap();
    let receipts_after: i64 = sqlx::query_scalar("SELECT count(*) FROM managed_attempt_receipts WHERE account_id=$1 AND purpose='cloud_embedding'")
        .bind(h.f.account).fetch_one(&h.f.owner).await.unwrap();
    h.evidence("actual-worker",json!({"before":before,"after":after,"jobs_before":jobs_before,"jobs_after":jobs,"atoms_after":atoms,
        "receipts_before":receipt_count,"receipts_after":receipts_after,"usage_before":usage_before,"usage_after":usage_after,"path":"test_p10_automatic_worker -> embed::process_job -> process_managed_job -> process -> settle_unprocessed -> bulk_mark_atoms_failed"}));
    assert_eq!(
        receipts_after, receipt_count,
        "pre-send failure must not create send receipts"
    );
    assert_eq!(jobs.as_array().unwrap().len(), JOBS);
    assert_eq!(atoms.as_array().unwrap().len(), JOBS);
    for j in jobs.as_array().unwrap() {
        assert_eq!(j["status"], "dead");
        assert_eq!(j["attempts"], j["max_attempts"]);
        assert_eq!(j["finished"], true);
        assert_eq!(
            j["error_message"],
            "managed embedding unavailable: invalid blob key"
        );
    }
    for a in atoms.as_array().unwrap() {
        assert_eq!(a["failure_stamped"], true);
        assert_eq!(a["embed_attempts"], 1);
    }
    assert!(
        payload_proof["all_page_facts_equal"] == true,
        "actual page facts changed: {}",
        payload_proof
    );
    assert_same_admission_and_keys(&before, &after);
    assert_eq!(
        usage_before["served"]["computed_at"],
        usage_after["served"]["computed_at"]
    );
    assert_eq!(
        usage_before["truth_atoms_ingested"],
        usage_after["truth_atoms_ingested"]
    );
    assert_eq!(usage_before["snapshot"], usage_after["snapshot"]);
    assert_eq!(usage_after["local_valid"], true);
    assert_eq!(before["user_fences"], after["user_fences"]);
    for (a, b) in before["atom_state"]
        .as_array()
        .unwrap()
        .iter()
        .zip(after["atom_state"].as_array().unwrap())
    {
        assert_eq!(a["admission_tuple_digest"], b["admission_tuple_digest"]);
    }
    for (a, b) in before["details"]
        .as_array()
        .unwrap()
        .iter()
        .zip(after["details"].as_array().unwrap())
    {
        assert_eq!(b["row_before_HTTP"]["local_valid"], true);
        assert!(b["row_before_HTTP"]["fence_state"].as_i64().unwrap() > 0);
        assert_eq!(
            a["served"]["computed_at"], b["served"]["computed_at"],
            "operational settlement withheld an unchanged authorized payload; retain causal RED"
        );
    }
    assert_unchanged_pages_fresh(&before, &after, &usage_before, &usage_after);
    h.close().await;
}

#[tokio::test]
async fn identical_non_done_status_update_preserves_unchanged_support_certificates() {
    let h = Harness::new("identical-version-status", true).await;
    h.warm().await;
    let before = h.snapshot().await;
    assert_fresh(&before);
    let changed = sqlx::query(STATUS_UPDATE)
        .bind(h.f.graph)
        .bind(h.version.unwrap())
        .bind(0_i64)
        .bind("pending")
        .bind(Option::<&str>::None)
        .execute(&h.f.owner)
        .await
        .unwrap()
        .rows_affected();
    let after = h.snapshot().await;
    h.evidence(
        "matched-SQL",
        json!({"matched_rows":changed,"before":before,"after":after,
        "coverage":"verbatim private set_status SQL + actual triggers, not extraction worker"}),
    );
    assert_eq!(
        changed, 1,
        "must exercise matched no-op, not zero-row UPDATE"
    );
    assert_eq!(
        before["source_version"], after["source_version"],
        "source inputs must truly be unchanged"
    );
    assert_same_admission_and_keys(&before, &after);
    // Intentionally red-capable: current unconditional version UPDATE
    // classifier may fail this. Never change its expected value to bless NULL.
    for (a, b) in before["details"]
        .as_array()
        .unwrap()
        .iter()
        .zip(after["details"].as_array().unwrap())
    {
        assert_eq!(
            a["served"]["computed_at"], b["served"]["computed_at"],
            "matched operational no-op withheld unchanged support; preserve RED evidence"
        );
        assert_eq!(b["row_before_HTTP"]["local_valid"], true);
        assert_eq!(b["row_before_HTTP"]["fence_state"], 1);
        assert_eq!(b["served"]["stale"], true);
        assert!(
            b["row_before_HTTP"]["generation"].as_i64().unwrap()
                > a["row_before_HTTP"]["generation"].as_i64().unwrap(),
            "matched no-op must still dirty subscribed status readers"
        );
    }
    h.close().await;
}

#[tokio::test]
async fn real_status_transition_is_recorded_separately_from_support_removal() {
    let h = Harness::new("real-version-status-transition", true).await;
    h.warm().await;
    let before = h.snapshot().await;
    assert_fresh(&before);
    let changed = sqlx::query(STATUS_UPDATE)
        .bind(h.f.graph)
        .bind(h.version.unwrap())
        .bind(0_i64)
        .bind("running")
        .bind(Option::<&str>::None)
        .execute(&h.f.owner)
        .await
        .unwrap()
        .rows_affected();
    let after = h.snapshot().await;
    h.evidence("matched-SQL",json!({"matched_rows":changed,"before":before,"after":after,
        "classification":"real pending->running operational change; inspect recorded admission/fence difference, do not claim permission loss"}));
    assert_eq!(changed, 1);
    assert_eq!(before["source_version"]["status"], "pending");
    assert_eq!(after["source_version"]["status"], "running");
    assert_ne!(before["source_version"], after["source_version"]);
    assert_same_admission_and_keys(&before, &after);
    // Check consistency of the current certificate decision, without equating
    // an extraction status transition with evidence/permission removal.
    for d in after["details"].as_array().unwrap() {
        assert!(d["served"]["computed_at"].is_string());
        if d["row_before_HTTP"]["fence_state"] == 0 || d["row_before_HTTP"]["local_valid"] == false
        {
            assert_eq!(d["served"]["stale"], true);
        }
    }
    h.close().await;
}

#[tokio::test]
async fn genuine_admitted_support_removal_marks_all_three_stale() {
    let h = Harness::new("genuine-support-removal", false).await;
    h.positive_removal().await;
    h.close().await;
}

fn assert_usage_served_stale(before: &Value, after: &Value) {
    assert_eq!(before["key"], after["key"]);
    assert_eq!(before["snapshot"], after["snapshot"]);
    assert_eq!(before["stored_computed_at"], after["stored_computed_at"]);
    assert_eq!(after["fence_state"], 0);
    assert!(after["served"]["computed_at"].is_string());
    assert_eq!(before["served"]["computed_at"], after["served"]["computed_at"]);
    assert_eq!(after["served"]["stale"], true);
}

#[tokio::test]
async fn actual_metering_created_at_change_marks_whole_account_usage_stale() {
    let h = Harness::new("metering-created-at", false).await;
    h.authorize_usage().await;
    let atom = h.hidden_atom().await;
    h.warm().await;
    let pages_before = h.snapshot().await;
    let scope = h.warm_usage().await;
    let before = h.usage_snapshot(&scope).await;
    assert_eq!(before["truth_atoms_ingested"], JOBS + 1);
    assert_eq!(before["served"]["atoms_ingested"], JOBS + 1);
    assert_eq!(before["fence_state"], 2);
    assert_eq!(
        sqlx::query(
            "UPDATE atoms SET created_at='2000-01-01T00:00:00Z' WHERE graph_id=$1 AND atom_id=$2"
        )
        .bind(h.f.foreign)
        .bind(atom)
        .execute(&h.f.owner)
        .await
        .unwrap()
        .rows_affected(),
        1
    );
    let after = h.usage_snapshot(&scope).await;
    let pages_after = h.snapshot().await;
    h.evidence("actual-UPDATE",json!({"before":before,"after":after,"pages_before":pages_before,"pages_after":pages_after,
        "case":"created_at exits counted period on content-invisible graph; actual metering count decreases"}));
    assert_eq!(after["truth_atoms_ingested"], JOBS);
    assert_usage_served_stale(&before, &after);
    assert_same_admission_and_keys(&pages_before, &pages_after);
    for (a, b) in pages_before["details"]
        .as_array()
        .unwrap()
        .iter()
        .zip(pages_after["details"].as_array().unwrap())
    {
        assert_eq!(
            a["served"]["computed_at"], b["served"]["computed_at"],
            "invisible graph cannot revoke unrelated content"
        );
    }
    let current = h.warm_usage().await;
    let refreshed = h.usage_snapshot(&current).await;
    assert_eq!(refreshed["served"]["atoms_ingested"], JOBS);
    assert_eq!(refreshed["fence_state"], 2);
    h.evidence("certified-recompute", refreshed);
    h.close().await;
}

#[tokio::test]
async fn metering_insert_is_additive_and_delete_marks_usage_stale() {
    let h = Harness::new("metering-insert-delete", false).await;
    h.authorize_usage().await;
    let scope = h.warm_usage().await;
    let before = h.usage_snapshot(&scope).await;
    assert_eq!(before["truth_atoms_ingested"], JOBS);
    let atom = h.hidden_atom().await;
    let inserted = h.usage_snapshot(&scope).await;
    assert_eq!(inserted["truth_atoms_ingested"], JOBS + 1);
    assert_eq!(
        before["served"]["computed_at"],
        inserted["served"]["computed_at"]
    );
    assert_eq!(inserted["fence_state"], 1);
    assert_eq!(inserted["served"]["stale"], true);
    let scope = h.warm_usage().await;
    let before_delete = h.usage_snapshot(&scope).await;
    assert_eq!(before_delete["served"]["atoms_ingested"], JOBS + 1);
    assert_eq!(
        sqlx::query("DELETE FROM atoms WHERE graph_id=$1 AND atom_id=$2")
            .bind(h.f.foreign)
            .bind(atom)
            .execute(&h.f.owner)
            .await
            .unwrap()
            .rows_affected(),
        1
    );
    let deleted = h.usage_snapshot(&scope).await;
    h.evidence("actual-INSERT-DELETE",json!({"before":before,"inserted":inserted,"before_delete":before_delete,"deleted":deleted}));
    assert_eq!(deleted["truth_atoms_ingested"], JOBS);
    assert_usage_served_stale(&before_delete, &deleted);
    h.close().await;
}

#[tokio::test]
async fn atom_move_still_fences_both_graph_account_boundaries_without_widening_scope() {
    let h = Harness::new("metering-cross-account-move", false).await;
    h.authorize_usage().await;
    let atom = h.hidden_atom().await;
    let other_account = Uuid::new_v4();
    let new_graph:Uuid = sqlx::query_scalar("INSERT INTO graphs(account_id,name,kind,owner_user_id) VALUES($1,'Synthetic other account','member',$2) RETURNING id")
        .bind(other_account).bind(h.f.other).fetch_one(&h.f.owner).await.unwrap();
    h.warm().await;
    let pages_before = h.snapshot().await;
    let scope = h.warm_usage().await;
    let before = h.usage_snapshot(&scope).await;
    let accounts = [h.f.account, other_account];
    let graphs = [h.f.graph, h.f.foreign, new_graph];
    let fences_before = h.graph_fences(&accounts, &graphs).await;
    let mut tx = h.f.owner.begin().await.unwrap();
    assert_eq!(
        sqlx::query("UPDATE atoms SET graph_id=$3 WHERE graph_id=$1 AND atom_id=$2")
            .bind(h.f.foreign)
            .bind(atom)
            .bind(new_graph)
            .execute(&mut *tx)
            .await
            .unwrap()
            .rows_affected(),
        1
    );
    let xid: String = sqlx::query_scalar("SELECT pg_current_xact_id()::text")
        .fetch_one(&mut *tx)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    let after = h.usage_snapshot(&scope).await;
    let pages_after = h.snapshot().await;
    let fences_after = h.graph_fences(&accounts, &graphs).await;
    h.evidence("actual-graph-move",json!({"before":before,"after":after,"pages_before":pages_before,"pages_after":pages_after,
        "old_account":h.f.account,"new_account":other_account,"old_graph":h.f.foreign,"new_graph":new_graph,"writer_xid":xid,
        "fences_before":fences_before,"fences_after":fences_after}));
    assert_eq!(before["truth_atoms_ingested"], JOBS + 1);
    assert_eq!(after["truth_atoms_ingested"], JOBS);
    assert_usage_served_stale(&before, &after);
    for (account, graph) in [(h.f.account, h.f.foreign), (other_account, new_graph)] {
        let fence = fences_after
            .as_array()
            .unwrap()
            .iter()
            .find(|f| f["account_id"] == json!(account) && f["graph_id"] == json!(graph))
            .unwrap();
        assert_eq!(
            fence["invalidated_xid"], xid,
            "both graph/account boundaries retain reducing fences"
        );
    }
    let own = |fences: &Value| {
        fences
            .as_array()
            .unwrap()
            .iter()
            .find(|f| f["graph_id"] == json!(h.f.graph))
            .unwrap()["invalidated_xid"]
            .clone()
    };
    assert_eq!(
        own(&fences_before),
        own(&fences_after),
        "unrelated graph invalidator unchanged"
    );
    assert_same_admission_and_keys(&pages_before, &pages_after);
    for (a, b) in pages_before["details"]
        .as_array()
        .unwrap()
        .iter()
        .zip(pages_after["details"].as_array().unwrap())
    {
        assert_eq!(a["served"]["computed_at"], b["served"]["computed_at"]);
    }
    let mut member = begin_tenant_scoped(&h.f.app, h.f.account, h.f.user)
        .await
        .unwrap();
    let viewer = rm::Viewer::load(&mut member, h.f.account, h.f.user)
        .await
        .unwrap();
    assert!(!viewer.visible_graphs().contains(&new_graph));
    assert!(!viewer.visible_graphs().contains(&h.f.foreign));
    let visible: i64 = sqlx::query_scalar("SELECT count(*) FROM atoms WHERE atom_id=$1")
        .bind(atom)
        .fetch_one(&mut *member)
        .await
        .unwrap();
    assert_eq!(visible, 0, "moving a row cannot widen ordinary member RLS");
    let refused:i32 = sqlx::query_scalar("SELECT read_model_fence_state($1,ARRAY[$2]::uuid[],'conversation_detail',$3,pg_current_snapshot())")
        .bind(other_account).bind(new_graph).bind(h.f.user).fetch_one(&mut *member).await.unwrap();
    assert_eq!(
        refused, 0,
        "metadata helper still refuses a foreign account"
    );
    member.rollback().await.unwrap();
    h.close().await;
}

#[tokio::test]
async fn actual_retry_sweep_and_nonterminal_failure_preserve_unchanged_page_freshness() {
    let h = Harness::new("retry-sweep-safe-stale", false).await;
    h.authorize_usage().await;
    h.prepare_nonempty_page_facts().await;
    // Tiny real sweep fixture: eight post-gate orphans older than its actual
    // grace, including the legacy marker arm. All setup precedes publication.
    sqlx::query("UPDATE atoms SET content_inline=NULL,created_at=now()-interval '2 hours',embed_model_next=CASE WHEN atom_id=ANY($3) THEN 'blocked:managed_purpose_unsupported' ELSE NULL END WHERE graph_id=$1 AND atom_id=ANY($2)")
        .bind(h.f.graph).bind(&h.atoms).bind(&h.atoms[..JOBS/2]).execute(&h.f.owner).await.unwrap();
    h.warm().await;
    let usage_scope = h.warm_usage().await;
    let before = h.snapshot().await;
    assert_fresh(&before);
    let usage_before = h.usage_snapshot(&usage_scope).await;
    let payloads_before = h.actual_payloads(&usage_scope).await;
    // Exact public sweep, not a substitute UPDATE. Its private claim bumps
    // failure time/clears the legacy marker before real enqueue and claim.
    coralswarm_backend::workers::embed_retry::tick(
        &h.f.worker(),
        &coralswarm_backend::shutdown::Shutdown::new(),
    )
    .await
    .unwrap();
    let jobs = queue::dequeue(&h.f.app, &["EmbedAtomsRetry"], 8)
        .await
        .unwrap();
    assert_eq!(
        jobs.len(),
        1,
        "8rows fit the real64-atom sweep job partition"
    );
    let ids: Vec<_> = jobs.iter().map(|j| j.id).collect();
    let cloud = OpenRouterEmbedder::new(
        reqwest::Client::builder()
            .https_only(true)
            .no_proxy()
            .build()
            .unwrap(),
        "synthetic-unused-key",
    )
    .with_base_url("http://127.0.0.1:1");
    let tenant = h.f.worker().tenant;
    for job in jobs {
        coralswarm_backend::test_p10_automatic_worker(
            &tenant,
            &h.f.app,
            &h.f.control,
            &cloud,
            &NeverLlm,
            job,
        )
        .await;
    }
    let after = h.snapshot().await;
    let usage_after = h.usage_snapshot(&usage_scope).await;
    let payloads_after = h.actual_payloads(&usage_scope).await;
    let proof = compare_actual_payloads(&payloads_before, &payloads_after);
    let jobs_after:Value=sqlx::query_scalar("SELECT jsonb_agg(to_jsonb(j)) FROM (SELECT id,status::text,attempts,max_attempts,error_message FROM jobs WHERE account_id=$1 AND id=ANY($2) ORDER BY id) j")
        .bind(h.f.account).bind(&ids).fetch_one(&h.f.owner).await.unwrap();
    h.evidence("real-sweep-nonterminal-worker",json!({"before":before,"after":after,"usage_before":usage_before,"usage_after":usage_after,"payload_proof":proof,"jobs_after":jobs_after,
        "path":"actual embed_retry::tick -> find_retry_eligible_atoms claim UPDATE -> queue enqueue/dequeue -> real worker invalid-key unavailable -> settle_unprocessed Failed dead:false; no terminal bulk_mark_atoms_failed"}));
    assert_eq!(jobs_after[0]["status"], "pending");
    assert_eq!(jobs_after[0]["attempts"], 1);
    assert_eq!(
        jobs_after[0]["error_message"],
        "managed embedding unavailable: invalid blob key"
    );
    assert!(
        proof["all_page_facts_equal"] == true,
        "actual page facts changed: {}",
        proof
    );
    assert_same_admission_and_keys(&before, &after);
    assert_eq!(before["user_fences"], after["user_fences"]);
    for atom in after["atom_state"].as_array().unwrap() {
        assert_eq!(
            atom["embed_attempts"], 0,
            "nonterminal settlement must not be mislabeled terminal atom charge"
        );
        assert!(
            !atom["embed_failed_at"].is_null(),
            "the actual sweep must claim all8atoms"
        );
    }
    assert_unchanged_pages_fresh(&before, &after, &usage_before, &usage_after);
    h.close().await;
}

fn assert_relevant_atom_update_still_stales(before: &Value, after: &Value) {
    assert_same_admission_and_keys(before, after);
    for (a, b) in before["details"]
        .as_array()
        .unwrap()
        .iter()
        .zip(after["details"].as_array().unwrap())
    {
        assert_eq!(b["row_before_HTTP"]["local_valid"], true);
        assert_eq!(b["row_before_HTTP"]["fence_state"], 1);
        assert_eq!(a["served"]["computed_at"], b["served"]["computed_at"]);
        assert_eq!(
            b["served"]["stale"], true,
            "real tag/vector/future-field changes must remain conservative"
        );
        assert!(!b["served"]["refresh_eta"].is_null());
        assert!(
            b["row_before_HTTP"]["generation"].as_i64().unwrap()
                > a["row_before_HTTP"]["generation"].as_i64().unwrap()
        );
    }
}

#[tokio::test]
async fn current_cloud_tag_vector_and_future_column_changes_remain_relevant() {
    let h = Harness::new("cloud-vector-future-relevance", false).await;
    // Owned disposable fixture ONLY. This proves the complete native atoms
    // row comparison automatically includes columns unknown to the migration.
    sqlx::query(
        "ALTER TABLE atoms ADD COLUMN synthetic_future_page_fact integer NOT NULL DEFAULT 0",
    )
    .execute(&h.f.owner)
    .await
    .unwrap();
    let atom = h.atoms[0];
    sqlx::query("UPDATE atoms SET embedding_next=array_fill(0.1::real,ARRAY[384])::vector,embed_model_next='synthetic-old-model' WHERE graph_id=$1 AND atom_id=$2")
        .bind(h.f.graph).bind(atom).execute(&h.f.owner).await.unwrap();
    for (stage,sql) in [
        ("gain-current-cloud-tag","UPDATE atoms SET embed_model_next=$3 WHERE graph_id=$1 AND atom_id=$2"),
        ("change-current-cloud-vector","UPDATE atoms SET embedding_next=array_fill(0.2::real,ARRAY[384])::vector WHERE graph_id=$1 AND atom_id=$2 AND $3::text IS NOT NULL"),
        ("lose-current-cloud-tag","UPDATE atoms SET embed_model_next=NULL WHERE graph_id=$1 AND atom_id=$2 AND $3::text IS NOT NULL"),
        ("future-unknown-column","UPDATE atoms SET synthetic_future_page_fact=1 WHERE graph_id=$1 AND atom_id=$2 AND $3::text IS NOT NULL"),
        ("different-blocked-tag-clear","UPDATE atoms SET embed_model_next=NULL WHERE graph_id=$1 AND atom_id=$2 AND $3::text IS NOT NULL"),
    ] {
        if stage=="different-blocked-tag-clear" {
            sqlx::query("UPDATE atoms SET embed_model_next='blocked:provider_unavailable' WHERE graph_id=$1 AND atom_id=$2")
                .bind(h.f.graph).bind(atom).execute(&h.f.owner).await.unwrap();
        }
        h.warm().await;
        let before=h.snapshot().await;assert_fresh(&before);
        assert_eq!(sqlx::query(sql).bind(h.f.graph).bind(atom).bind(coralswarm_core::embed::EMBED_MODEL_CLOUD)
            .execute(&h.f.owner).await.unwrap().rows_affected(),1);
        let after=h.snapshot().await;
        h.evidence(stage,json!({"before":before,"after":after,"atom_id":atom,"case":"real model-tag/vector or future complete-row-field change must not be filtered as failure bookkeeping"}));
        assert_relevant_atom_update_still_stales(&before,&after);
        let eligibility=|snapshot:&Value|snapshot["atom_state"].as_array().unwrap().iter().find(|a|a["atom_id"]==json!(atom)).unwrap()["current_cloud_eligible"].clone();
        if stage=="gain-current-cloud-tag" {assert_eq!(eligibility(&before),false);assert_eq!(eligibility(&after),true);}
        if stage=="lose-current-cloud-tag" {assert_eq!(eligibility(&before),true);assert_eq!(eligibility(&after),false);}
    }
    h.close().await;
}

#[tokio::test]
async fn mixed_operational_and_real_move_filters_only_operational_graph() {
    let h = Harness::new("mixed-operational-real-move", false).await;
    h.authorize_usage().await;
    let operational = h.atoms[0];
    sqlx::query("UPDATE atoms SET embed_model_next='blocked:managed_purpose_unsupported' WHERE graph_id=$1 AND atom_id=$2")
        .bind(h.f.graph).bind(operational).execute(&h.f.owner).await.unwrap();
    let moved = h.hidden_atom().await;
    let other_account = Uuid::new_v4();
    let new_graph:Uuid=sqlx::query_scalar("INSERT INTO graphs(account_id,name,kind,owner_user_id) VALUES($1,'Synthetic mixed-update target','member',$2) RETURNING id")
        .bind(other_account).bind(h.f.other).fetch_one(&h.f.owner).await.unwrap();
    h.warm().await;
    let usage_scope = h.warm_usage().await;
    let before = h.snapshot().await;
    assert_fresh(&before);
    let usage_before = h.usage_snapshot(&usage_scope).await;
    let accounts = [h.f.account, other_account];
    let graphs = [h.f.graph, h.f.foreign, new_graph];
    let fences_before = h.graph_fences(&accounts, &graphs).await;
    let mut tx = h.f.owner.begin().await.unwrap();
    assert_eq!(sqlx::query("UPDATE atoms SET embed_failed_at=CASE WHEN atom_id=$1 THEN now() ELSE embed_failed_at END,embed_attempts=CASE WHEN atom_id=$1 THEN embed_attempts+1 ELSE embed_attempts END,embed_model_next=CASE WHEN atom_id=$1 THEN NULL ELSE embed_model_next END,graph_id=CASE WHEN atom_id=$2 THEN $3 ELSE graph_id END WHERE atom_id=ANY($4)")
        .bind(operational).bind(moved).bind(new_graph).bind(vec![operational,moved]).execute(&mut *tx).await.unwrap().rows_affected(),2);
    let xid: String = sqlx::query_scalar("SELECT pg_current_xact_id()::text")
        .fetch_one(&mut *tx)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    let after = h.snapshot().await;
    let usage_after = h.usage_snapshot(&usage_scope).await;
    let fences_after = h.graph_fences(&accounts, &graphs).await;
    h.evidence("single-mixed-UPDATE",json!({"before":before,"after":after,"usage_before":usage_before,"usage_after":usage_after,
        "operational_atom":operational,"real_moved_atom":moved,"old_graph":h.f.foreign,"new_graph":new_graph,"new_account":other_account,
        "fences_before":fences_before,"fences_after":fences_after,"writer_xid":xid}));
    assert_same_admission_and_keys(&before, &after);
    assert_fresh(&after);
    for (a, b) in before["details"]
        .as_array()
        .unwrap()
        .iter()
        .zip(after["details"].as_array().unwrap())
    {
        assert_eq!(
            a["row_before_HTTP"]["generation"], b["row_before_HTTP"]["generation"],
            "graphA operational row must not dirty its cached models because graphB really moved"
        );
        assert!(b["row_before_HTTP"]["dirty_at"].is_null());
    }
    assert_eq!(
        before["graph_fences"], after["graph_fences"],
        "operational graphA must be absent from BOTH generic and metering affected sets"
    );
    assert_eq!(usage_before["truth_atoms_ingested"], JOBS + 1);
    assert_eq!(usage_after["truth_atoms_ingested"], JOBS);
    assert_usage_served_stale(&usage_before, &usage_after);
    for (account, graph) in [(h.f.account, h.f.foreign), (other_account, new_graph)] {
        let f = fences_after
            .as_array()
            .unwrap()
            .iter()
            .find(|f| f["account_id"] == json!(account) && f["graph_id"] == json!(graph))
            .unwrap();
        assert_eq!(
            f["invalidated_xid"], xid,
            "real row retains BOTH old/new graph/account reduction boundaries"
        );
    }
    let mut member = begin_tenant_scoped(&h.f.app, h.f.account, h.f.user)
        .await
        .unwrap();
    let viewer = rm::Viewer::load(&mut member, h.f.account, h.f.user)
        .await
        .unwrap();
    assert!(!viewer.visible_graphs().contains(&h.f.foreign));
    assert!(!viewer.visible_graphs().contains(&new_graph));
    let visible: i64 = sqlx::query_scalar("SELECT count(*) FROM atoms WHERE atom_id=$1")
        .bind(moved)
        .fetch_one(&mut *member)
        .await
        .unwrap();
    assert_eq!(visible, 0);
    member.rollback().await.unwrap();
    h.close().await;
}

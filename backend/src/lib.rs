use axum::{
    extract::{DefaultBodyLimit, Request, State},
    http::StatusCode,
    middleware::{self as axum_middleware, Next},
    response::{IntoResponse, Response},
    routing::{delete, get, patch, post, put},
    Json, Router,
};
use coralswarm_core::r2::R2Client;
use serde_json::{json, Value};
use sqlx::PgPool;
use std::sync::Arc;
use std::time::Instant;
use tower_http::cors::{AllowOrigin, Any, CorsLayer};
use tower_http::trace::TraceLayer;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};
use utoipa::OpenApi as _;
use uuid::Uuid;

// Engine modules are `pub` so the crate's binaries and integration tests can
// drive the engine without the HTTP server. The
// HTTP-server-internal modules (`auth`, `routes`, `workers`, …) stay
// crate-private — only `run()`/`app()` below use them.
pub mod admission;
pub mod route_class;
/// Billing / usage-metering support (usage-metering PR1: DB-versioned rate
/// sheet, `PriceSheet`). `pub(crate)` — HTTP-server-internal, same as
/// `routes`/`workers` above.
// `pub` rather than `pub(crate)` so `bin/grant_plan.rs` — a separate crate that
// consumes this lib — can call `billing::renewal::roll_period` instead of
// re-typing the per-member allowance formula. Keeping the formula in exactly
// one place is the explicit discipline `billing::renewal`'s module doc sets out.
pub mod billing;
pub mod clerk;
#[cfg(any(test, feature = "test-support"))]
pub mod sqlx_statement_log;
/// A local failing S3 stand-in for storage-outage tests.
#[cfg(any(test, feature = "test-support"))]
pub mod test_object_store;
// `pub` rather than `pub(crate)` for exactly the reason `billing` above is:
// `bin/create_personal_orgs.rs` is a separate crate consuming this lib, and it
// needs `ClerkAdminClient::create_organization`/`list_user_organizations`/
// `list_all_users`. Re-typing a Clerk Backend API client inside a bin — a
// second copy of the auth header, the error-shape parsing and the pagination
// cap — is precisely the drift this module exists to prevent, and the one
// `grant_plan`'s widening of `billing` was accepted to avoid.
/// Per-org connector flag registry (Foundation train PR 1.3): env-var
/// defaults (`ORG_SLACK_OAUTH_ENABLED`, `ORG_TEAMS_ENABLED`) overridable
/// per-org via the `org_connector_flags` control-plane table.
pub mod calendar_pass_gate;
pub mod personal_account;
pub mod clerk_admin;
mod clerk_provision;
pub mod connector_flags;
mod connectors;
pub mod crypto;
/// TypeSafe System One / Jev decision seam (ADR-0013 / ADR-0014).
pub mod decision;
pub mod dek_cache;
/// Retrieval evaluation harness (Phase 1 quality gate). Measurement-only — does
/// not touch the production retrieval path. See `backend/eval/README.md`.
pub mod embedding;
pub mod eval;
pub mod graph_rate_limit;
pub mod history_floor;
/// The journal's atom grouping as SQL text, shared by the hour rows and the
/// activity cards.
pub(crate) mod journal_grouping;
pub mod read_access;
pub mod read_models;
// `pub` for `tests/jobs_account_scope_pg.rs` (org-tenancy Phase W.1): the
// enqueue→dequeue account_id round-trip and the pre-W.1 legacy-row backfill
// are integration-tested against a real migrated scratch DB, which the
// in-module `RELINFER_TEST_DATABASE_URL` tests can't self-provision.
pub mod jobs;
/// The reusable core of the one-shot `backfill_unattributed_job_accounts`
/// binary (attributes historical `ExtractEntities` `jobs` rows so
/// `workers::billing_derivation`'s unattributed-jobs alert stops firing on
/// residue). `pub` for the same reason as `judgement_backfill` below:
/// `backend/src/bin/` AND `backend/tests/jobs_account_backfill_pg.rs` both
/// drive it, so the gates an operator runs are the exact gates CI proves.
pub mod jobs_account_backfill;
/// The core of the one-shot `purge_desktop_data` binary (ADR 0015). `pub`
/// so `backend/src/bin/` and `backend/tests/desktop_purge_pg.rs` drive the
/// same code.
pub mod desktop_purge;
/// The reusable core of the one-shot `backfill_reef_judgement_from_history`
/// binary (judgement-graph resolver slice, PR5). `pub` for the same reason
/// as `jobs`/`services::deletion` above: `backend/src/bin/` AND
/// `backend/tests/judgement_backfill_pg.rs` both drive it, so the gates an
/// operator runs are the exact gates CI proves — the binary itself is only
/// argument parsing and DSN resolution.
pub mod judgement_backfill;
pub mod llm;
pub mod meeting_evidence;
pub mod source_evidence;
pub mod chunk_reconcile;
pub mod ingest;
pub mod selected_publication;
// The e2e harness's one seeding route (`e2e/`). Compiled only with the `e2e`
// feature, which cannot be combined with an optimized build:
#[cfg(all(feature = "e2e", not(debug_assertions)))]
compile_error!(
    "the `e2e` feature is for the e2e harness's debug build only; it must never be part of a \
     release (deployable) binary"
);
#[cfg(feature = "e2e")]
mod e2e_seed;
pub mod capture_upload_cleanup;
pub mod source_origin;
pub mod capture_admission_health;
/// Per-stage, per-source-type pipeline health and its alert rules.
pub mod pipeline_stage_health;
pub mod corpus_enrichment;
mod managed_history;
/// Managed (background) spend: the per-attempt money ledger (COR-21).
pub(crate) mod managed;
#[cfg(feature = "test-support")]
pub async fn test_standing_principal(
    tenant: tenancy::TenantId, content: &sqlx::PgPool, control: &sqlx::PgPool,
    account: Uuid, graph: Uuid, run: Uuid, profile: &str, expected: Uuid,
) -> Result<String, String> {
    use managed::standing::{PrincipalRequest, StandingOwner, StandingProfile};
    let worker = workers::tenant_harness::WorkerTenant {
        tenant, pool: content.clone(), store: Arc::new(store::PostgresStore::new(content.clone())),
    };
    let request = PrincipalRequest { account_id: account, graph_id: graph, run_id: run,
        profile: StandingProfile::parse(profile).map_err(|e| format!("{e:?}"))?, expected_principal: expected };
    StandingOwner::for_worker(&worker, control).reconcile(request).await
        .map(|result| format!("{result:?}")).map_err(|error| format!("{error:?}"))
}

#[cfg(feature="test-support")]
pub use managed::standing::probe as test_standing_probe;

#[cfg(feature="test-support")]
pub async fn test_standing_sweep(worker: &workers::tenant_harness::WorkerTenant, control: &sqlx::PgPool) -> Result<workers::tenant_harness::Tick, String> {
    managed::standing::execution::sweep_tick(worker, control).await
}

#[cfg(feature="test-support")]
pub async fn test_p10_promote(state:Arc<AppState>,tenant:tenancy::extractor::Tenant,user:UserContext,run:Uuid,body:serde_json::Value)->(axum::http::StatusCode,Json<serde_json::Value>) {
    managed_history::promote(State(state),tenant,axum::Extension(user),axum::extract::Path(run),Json(serde_json::from_value(body).unwrap())).await
}
#[cfg(feature="test-support")]
pub async fn test_p10_source_worker(tenant:&tenancy::TenantId,pool:&sqlx::PgPool,control:&sqlx::PgPool,llm:&dyn llm::LlmProvider,job:jobs::queue::JobRow) {
    workers::source_atoms::process_job(tenant,pool,control,&store::PostgresStore::new(pool.clone()),llm,None,job).await;
}
#[cfg(feature="test-support")]
pub async fn test_p10_legacy_summary_worker(tenant:&tenancy::TenantId,pool:&sqlx::PgPool,control:&sqlx::PgPool,llm:&dyn llm::LlmProvider,job:jobs::queue::JobRow) {
    workers::summary::process_job(tenant,pool,control,&store::PostgresStore::new(pool.clone()),llm,None,job).await;
}

#[cfg(feature="test-support")]
pub async fn test_p10_automatic_worker(tenant:&tenancy::TenantId,pool:&sqlx::PgPool,control:&sqlx::PgPool,embed:&dyn coralswarm_core::embed::EmbedProvider,llm:&dyn llm::LlmProvider,job:jobs::queue::JobRow) {
 let store=store::PostgresStore::new(pool.clone());
 match job.kind.as_str() {
  "EmbedAtoms"|"EmbedAtomsRetry"=>workers::embed::process_job(tenant,pool,&store,embed,Some(embed),Some(control),None,job).await,
  "ReembedOutdatedChunks"=>workers::reembed::process_job(tenant,pool,&store,embed,Some(embed),None,job).await,
  "GenerateMeetingSummary"=>workers::summary::process_job(tenant,pool,control,&store,llm,None,job).await,
  "RegenerateDigest"=>workers::digest::process_job(tenant,pool,control,&store,embed,llm,None,job).await,
  "EntityResolution"=>workers::entity_resolution::process_job(tenant,pool,control,&store,llm,job).await,
  "BackfillAtomTitles"=>workers::atom_title_backfill::process_job(tenant,pool,control,Some(llm),None,job).await,
  "InferRelationships"=>workers::inference_closure::process_job(tenant,pool,&store,job).await,
  "JournalSweep"=>{let worker=workers::tenant_harness::WorkerTenant{tenant:tenant.clone(),pool:pool.clone(),store:Arc::new(store)};let tick=workers::journal_sweep::tick(&worker,control,llm).await.unwrap();assert_eq!(tick.processed,0);},
  other=>panic!("unsupported test dispatch {other}"),
 }
}

pub mod library_read;
/// Busy-graph change set: distinct dirty keys and their relevance to a read.
pub mod library_changes;
pub mod capture_context;
pub mod library_history;
pub mod library_disagreements;
pub mod temporal;
/// Supersession over extraction v2's temporal facts (source-agnostic plan PR 9).
pub mod co_occurrence;
pub mod fact_supersession;
pub mod scoped_ask;
mod mcp_scoped;

#[cfg(feature = "test-support")]
pub mod test_db;

#[cfg(feature = "test-support")]
pub async fn test_scoped_query_json(state: Arc<AppState>, tenant: tenancy::extractor::Tenant, user: UserContext, body: serde_json::Value) -> axum::response::Response {
    use axum::response::IntoResponse;
    let request = serde_json::from_value(body).expect("synthetic JSON query fixture");
    routes::queries::create_query(State(state), tenant, axum::Extension(user), axum::Extension("synthetic-json-test".to_owned()), Json(request)).await.into_response()
}
#[cfg(feature = "test-support")]
pub async fn test_scoped_query_stream(state: Arc<AppState>,tenant: tenancy::extractor::Tenant,user: UserContext,body: serde_json::Value) -> axum::response::Response {
    let request=serde_json::from_value(body).expect("synthetic query fixture");
    routes::queries::create_query_stream(State(state),tenant,axum::Extension(user),axum::Extension("synthetic-test".to_owned()),Json(request)).await
}
#[cfg(feature = "test-support")]
pub async fn test_conversation_read(state: Arc<AppState>,tenant: tenancy::extractor::Tenant,user: UserContext,id: Uuid) -> axum::response::Response {
    test_conversation_page(state,tenant,user,id,None).await
}
#[cfg(feature = "test-support")]
pub async fn test_conversation_page(state: Arc<AppState>,tenant: tenancy::extractor::Tenant,user: UserContext,id: Uuid,before_seq: Option<i32>) -> axum::response::Response {
    use axum::response::IntoResponse;
    let page=routes::conversations::MessagePageParams{before_seq};
    // No `DoorAdmission`: this seam has no `require_auth`, so the control
    // re-check always runs here (#1496).
    routes::conversations::get_conversation(State(state),tenant,axum::Extension(user),axum::extract::Path(id),axum::extract::Query(page),None).await.into_response()
}
#[cfg(feature = "test-support")]
pub async fn test_retained_source_read(state: Arc<AppState>,tenant: tenancy::extractor::Tenant,user: UserContext,graph:Uuid,item:Uuid) -> axum::response::Response {
    use axum::response::IntoResponse;
    source_evidence::get_evidence(State(state),tenant,axum::Extension(user),axum::extract::Query(source_evidence::EvidenceQuery{graph_id:graph,source_item_id:item,version_id:None,before:None,atoms_after:None,atom_id:None})).await.into_response()
}
#[cfg(feature = "test-support")]
pub async fn test_modern_cached_task_admission(state: &AppState,tenant: &tenancy::extractor::Tenant,user: &UserContext) {
    let task=state.mcp_task_manager.spawn_owned(user.user_id,rmcp::task_manager::TaskOptions::new(),|_| Box::pin(async {Ok(rmcp::model::CallToolResult::structured(serde_json::json!({"answer":"synthetic cached legacy secret"})))}));
    let mut completed=false;
    for _ in 0..100 {
        let cached=state.mcp_task_manager.get_task(user.user_id,&task.task_id).unwrap();
        if let rmcp::model::TaskPayload::Completed{result}=cached.payload {
            assert!(serde_json::to_string(&result).unwrap().contains("synthetic cached legacy secret"));
            completed=true;break;
        }
        tokio::task::yield_now().await;
    }
    assert!(completed,"cached task fixture must finish before checking cloud denial");
    assert!(state.mcp_task_manager.get_scoped_task(state,tenant,user,&task.task_id,None).await.is_err());
    assert!(state.mcp_task_manager.get_task(Uuid::new_v4(),&task.task_id).is_err());
}
/// Synthetic fixture dispatch uses the existing verified-context harness;
/// production authentication is unchanged and no token is minted.
#[cfg(feature = "test-support")]
pub async fn test_retained_mcp_dispatch(state:Arc<AppState>,tenant:&tenancy::extractor::Tenant,user:&UserContext,request:serde_json::Value)->Result<serde_json::Value,String> {
    let mut auth=mcp::test_support::auth_for(user.user_id,user.account_id,false);
    auth.user=user.clone();auth.decision=tenant.decision.clone();
    let request=serde_json::from_value::<rmcp::model::ClientRequest>(request).map_err(|e|e.to_string())?;
    mcp::test_support::dispatch_result(Arc::new(mcp::server::CoralSwarmMcp::new(state)),request,auth).await.map(|v|serde_json::to_value(v).unwrap()).map_err(|e|e.to_string())
}
#[cfg(feature = "test-support")]
pub async fn test_retained_mcp_claim(
    state: &AppState,
    tenant: &tenancy::extractor::Tenant,
    user: &UserContext,
    request: scoped_ask::AskRequest,
    key: Option<String>,
) -> Result<(Uuid, bool, scoped_ask::AskRequest), String> {
    mcp_scoped::claim(state, tenant, user, request, key)
        .await
        .map(|c| (c.id, c.inserted, c.request))
        .map_err(|e| e.to_string())
}
#[cfg(feature = "test-support")]
pub async fn test_retained_mcp_run(
    state: Arc<AppState>,
    tenant: tenancy::extractor::Tenant,
    user: UserContext,
    id: Uuid,
    request: scoped_ask::AskRequest,
) -> bool {
    mcp_scoped::run(state, tenant, user, id, request)
        .await
        .map_err(|e| eprintln!("retained MCP run failed: {e:?}"))
        .is_ok()
}
#[cfg(feature = "test-support")]
pub async fn test_retained_mcp_poll(
    state: &AppState,
    tenant: &tenancy::extractor::Tenant,
    user: &UserContext,
    id: Uuid,
) -> Result<serde_json::Value, String> {
    mcp_scoped::poll(state, tenant, user, id)
        .await
        .map(|d| serde_json::to_value(d).unwrap())
        .map_err(|e| e.to_string())
}
#[cfg(feature = "test-support")]
pub async fn test_retained_mcp_run_before_completion(
    state: Arc<AppState>, tenant: tenancy::extractor::Tenant, user: UserContext,
    id: Uuid, request: scoped_ask::AskRequest,
    ready: Arc<tokio::sync::Notify>, resume: Arc<tokio::sync::Notify>,
) -> bool {
    mcp_scoped::run_before_completion(state, tenant, user, id, request, ready, resume).await.is_ok()
}
#[cfg(feature = "test-support")]
pub fn test_retained_mcp_modern_reference(
    state: &AppState,
    user: &UserContext,
    id: Uuid,
) -> String {
    let task = state.mcp_task_manager.spawn_scoped(
        user.user_id,
        user.account_id,
        id,
        rmcp::task_manager::TaskOptions::new(),
        move |_| {
            Box::pin(async move {
                Ok(rmcp::model::CallToolResult::structured(
                    serde_json::json!({"scoped_task_id":id}),
                ))
            })
        },
    );
    task.task_id
}
#[cfg(feature = "test-support")]
pub async fn test_retained_mcp_modern_get(
    state: &AppState,
    tenant: &tenancy::extractor::Tenant,
    user: &UserContext,
    id: &str,
) -> Result<serde_json::Value, String> {
    state
        .mcp_task_manager
        .get_scoped_task(state, tenant, user, id, None)
        .await
        .map(|t| serde_json::to_value(t).unwrap())
        .map_err(|e| e.to_string())
}
mod mcp;
mod middleware;
/// The access log's tenancy fields — re-exported so the org-tenancy probe
/// suites can assert end-to-end that a served response really carries them
/// (claim-based tenancy PR 1.4). `middleware` itself stays private; this is
/// the one type outside it that integration tests need to name.
pub use middleware::auth::TenancyLogFields;
mod microsoft_sso;
mod oauth;
// `pub` (Foundation train PR 1.2, CS-031): `bin/backfill_oauth_tokens_wrapped_dek`
// links this crate as an ordinary external dependency (every `src/bin/*.rs`
// does), and the pg-gated integration test for the cutover backfill
// (`backend/tests/oauth_token_wrapped_dek_pg.rs`) does too — both need
// `reencrypt_oauth_tokens` directly rather than duplicating its logic.
pub mod oauth_dek_cutover;
mod openapi;
/// Pinned, versioned normalizer that turns a raw git remote URL (or a
/// fallback folder basename) into a canonical `coding_sessions.project_key`
/// (coding-session capture Phase 1, audit amendment A5). See the module doc
/// for the P0 credential-stripping rationale.
pub(crate) mod project_key;
/// The reusable core of the one-shot `backfill_promoted_blobs` binary (the
/// promote-content-loss bug's backfill). `pub` for the same reason as
/// `jobs_account_backfill`/`judgement_backfill` above: `backend/src/bin/`
/// AND `backend/tests/promoted_blob_backfill_pg.rs` both drive it.
pub mod promoted_blob_backfill;
mod rate_limit;
pub mod relationships;
pub mod retrieval;
pub mod review_policy;
mod routes;
pub mod seats;
pub mod self_context;
// `pub` (same reason as `judgement_backfill`/`reextract` above):
// `backend/src/bin/backfill_self_reefs.rs` is a thin wrapper around this
// module, and `backend/tests/identity_door_pg.rs` drives its per-graph
// function directly so the gates an operator runs are the exact gates the
// test proves.
pub mod self_reef_backfill;
// Shared unique-parent rule for G2 bind-or-drop and the G4 fold binary — `docs/plans/knowledge-graph-connectivity.md` §12.
pub mod subtopic;
// `pub`: `backend/src/bin/backfill_provenance_links.rs` is a thin wrapper
// around this module (same split as `self_reef_backfill`). G1 stock repair —
// `docs/plans/knowledge-graph-connectivity.md` §12; default dry-run.
pub mod provenance_link_backfill;
// `pub`: the live `workers::source_atoms` dual write plus the reusable core of
// `backend/src/bin/backfill_node_atoms_bridge.rs`. BRIDGE: deleted by the
// source-agnostic pipeline plan's PR 11 (readers move to `reef_corals`).
pub mod node_atoms_bridge;
pub mod source_carry_forward;
pub mod source_reextract;
/// Operator core of `retry_failed_extractions_v2`: failed versions -> v2.
pub mod failed_extraction_retry;
/// Operator core of `dedupe_reefs`: merge per-version duplicate reefs.
pub mod mention_sweep;
pub mod reef_dedupe;
/// The attributed, reader-verified read of a reef's stored digest
/// (`GET /v1/nodes/{id}/digest`, MCP `get_reef`).
mod reef_digest_read;
// `pub`: `backend/src/bin/ocean_health.rs` is a thin wrapper around this
// module (same split as `self_reef_backfill`). G3 alarms —
// `docs/plans/knowledge-graph-connectivity.md` §12.3.
pub mod ocean_health;
// `pub`: `backend/src/bin/fold_subtopics.rs` is a thin wrapper around this
// module (same split as `self_reef_backfill`). G4 stock fold —
// `docs/plans/knowledge-graph-connectivity.md` §12.5; default dry-run,
// `--execute` gated on database name (plan §13.5).
pub mod fold_subtopics;
// `pub` (not `pub(crate)`) solely so the Postgres integration tests in
// `backend/tests/` can exercise `services::promotion` (org-tenancy PR 2.5)
// directly — the inner transport-shared services (`ask`, `supersession`,
// `transcripts`) remain `pub(crate)` and are NOT exported.
pub mod services;
// BACKEND_ROLE=all|http|workers (#1495): which halves of `run()` start.
pub mod role;
// `pub` (org-tenancy PR 2.1): the worker-harness Postgres integration tests
// (`backend/tests/worker_harness_pg.rs`) drive `workers::tenant_harness::
// run_multi_tenant` directly, which takes a `shutdown::Shutdown`.
pub mod shutdown;
mod startup;
// `pub` (Foundation train PR 1.5): `bin/backfill_slack_oauth_tokens` links
// this crate as an ordinary external dependency, and the pg-gated
// integration test (`backend/tests/slack_oauth_backfill_pg.rs`) does too —
// both need `backfill_slack_oauth_tokens`/`preview_slack_oauth_backfill`
// directly rather than duplicating the logic (same split as
// `oauth_dek_cutover`, above).
pub mod slack_oauth_backfill;
pub mod store;
pub mod synthesis;
/// Today's date in the asker's timezone, appended to every Ask system prompt.
pub mod answer_date;
pub mod request_timing;
pub mod telemetry;
pub mod tenancy;
pub mod workers;

/// The authenticated caller identity injected into request extensions.
pub use middleware::auth::UserContext;

#[cfg(feature = "test-support")]
#[doc(hidden)]
pub use ingest::{test_ingest_with_source_context, test_ingest_with_origin_context};
#[cfg(feature = "test-support")]
#[doc(hidden)]
pub use routes::shares::test_scoped_shared_payload;
#[cfg(feature = "test-support")]
#[doc(hidden)]
pub use routes::composio as test_composio;
/// The meetings list handler, for `history_floor_surfaces_pg`'s owner-role
/// proof that a floored member's meeting list is floored (#1545).
#[cfg(feature = "test-support")]
#[doc(hidden)]
pub use routes::meetings::{list_meetings as test_list_meetings, ListMeetingsQuery as TestListMeetingsQuery};

/// The Slack App Home review section's items for one viewer
/// (`connectors::slack_review::review_items_for_viewer`), each reduced to its
/// rendered label (a decision's title; an edge's `src predicate dst`). Lets
/// `backend/tests/review_read_scope_pg.rs` pin the read scope of a surface
/// whose only other exit is a Slack `views.publish` call.
#[cfg(feature = "test-support")]
#[doc(hidden)]
pub async fn test_slack_review_items_for_viewer(
    decision: Option<&tenancy::runtime::TenancyDecision>,
    tenant_pool: &sqlx::PgPool,
    account_id: uuid::Uuid,
    viewer_user_id: uuid::Uuid,
) -> Result<Vec<String>, String> {
    use connectors::slack_review::Clarification;
    Ok(connectors::slack_review::review_items_for_viewer(
        decision,
        tenant_pool,
        account_id,
        viewer_user_id,
    )
    .await?
        .into_iter()
        .map(|c| match c {
            Clarification::Decision { title, .. } => title,
            Clarification::Edge { src_title, predicate, dst_title, .. } => {
                format!("{src_title} {predicate} {dst_title}")
            }
        })
        .collect())
}
#[cfg(feature = "test-support")]
#[doc(hidden)]
pub use connectors::composio::{
    budget as test_composio_budget, recipe as test_composio_recipe, Client as TestComposioClient,
};

/// `journal_hour_items`/`JournalHourItemsQueryParams` are exported (same
/// "private module, narrow `pub use` re-export" idiom as `pub mod services`/
/// `pub mod shutdown` above) so `backend/tests/journal_endpoint_pg.rs`'s
/// same-account-second-member probe can invoke the REAL production handler
/// directly with a hand-built [`UserContext`] sharing another caller's
/// `account_id` — the "two members, one account" shape a plain personal
/// Clerk token can never itself produce (see that test's own doc comment for
/// why), so there is no way to reach it through `test_support_app`'s normal
/// JWT-minting harness. Calling the handler function directly still exercises
/// its real RLS scoping (`Tenant::begin_member_scoped`) and real `Store`
/// calls against the real `coralswarm_app` role — only the Clerk-token-to-
/// `account_id` resolution step is bypassed, which is an orthogonal concern
/// this probe isn't about.
pub use routes::journal::{journal_hour_items, JournalHourItemsQueryParams};

/// The Cmd-K title-search handler, exported for the same reason:
/// `backend/tests/find_results_pg.rs` calls it with a hand-built
/// [`UserContext`] to prove a second member never reads another member's
/// private ocean.
pub use routes::find_results::{title_search as find_titles, TitleParams as FindTitlesParams};
/// The Today activity handlers, exported for the same reason as
/// `journal_hour_items`: `backend/tests/journal_activities_pg.rs` calls them
/// with a hand-built [`UserContext`] to prove a second member of one account
/// never reads another member's private ocean.
pub use routes::journal_activities::{
    get_activity as journal_activity_detail, list_activities as journal_activities_list,
    DetailParams as JournalActivityDetailParams, ListParams as JournalActivitiesListParams,
};
/// The Today gist handler, exported for the same reason: `backend/tests/
/// journal_gist_pg.rs` calls it with a hand-built [`UserContext`] to prove a
/// second member of one account never reads another member's private ocean.
pub use routes::journal_gist::{get_gist as journal_gist, GistParams as JournalGistParams};
pub use routes::message_feedback::{
    delete_feedback as message_feedback_delete, put_feedback as message_feedback_put,
};

/// `merge_node`/`dismiss_merge_suggestion` (+ their request bodies) are
/// exported for the same reason `journal_hour_items` is, immediately above:
/// judgement-graph PR2's route coverage
/// (`backend/tests/judgement_graph_worker_pg.rs`) invokes these REAL
/// production handlers directly, with a hand-built `Tenant`/`UserContext`,
/// rather than standing up the full HTTP+JWKS router harness just to prove
/// the founder-Q4 bypass-coverage judgement writes land correctly. No
/// existing `*_pg.rs` file called `routes/nodes.rs` handlers before this —
/// see that test file's own doc comment.
///
/// `undo_merge`/`dedup_entities` (review round 2 fix pass) join the same
/// export for the same reason: F1's undo-annul proof needs the REAL
/// `undo_merge` handler (not a direct `Store::unmerge_nodes` call — the
/// route is what a user actually hits), and F2's dedup-skips-active-NEGATIVE
/// proof needs the REAL `dedup_entities` sweep handler.
///
/// `list_merge_review`/`ListMergeReviewQuery` join the same export for the
/// same reason (judgement-graph PR4): the merge-review queue's listing
/// behaviour — live-only rows, queue order, keyset pagination, the flag gate
/// — is provable only against a real `reef_judgement` table, and the suite
/// that owns those proofs calls the handler directly.
/// `confirm_merge_review`/`ConfirmMergeReviewRequest` likewise: the bug that
/// route exists to fix (Confirm 404ing on an ocean the caller's ACTIVE
/// account does not own) is invisible to any test that does not build a real
/// two-account, RLS-shaped fixture.
///
/// `get_node_by_id`/`get_node_atoms`/`patch_node`/`PatchNodeRequest` join the
/// same export for the same reason (the `GET /v1/nodes/{id}` fix):
/// `backend/tests/get_node_by_id_pg.rs`'s team-ocean-via-team-membership
/// probe needs a caller whose ACTIVE account differs from a
/// team ocean's OWNING account (reachable through `account_members` +
/// `team_members`, not through `node_owned_by_account`) — a shape a plain
/// personal or single-org Clerk token can't produce through
/// `test_support_app`'s JWT-minting harness, so the same "direct handler
/// call, hand-built `Tenant`/`UserContext`" idiom applies. `patch_node`
/// joins it to prove/disprove, in the same fixture, whether `PATCH`'s
/// narrower ownership check 403s where `GET`'s reachability check succeeds
/// (see that PR's description, "Follow-up").
pub use routes::nodes::{
    confirm_merge_review, dedup_entities, dismiss_merge_suggestion, get_node_atoms, get_node_by_id,
    list_merge_review, merge_node, patch_node, undo_merge, ConfirmMergeReviewRequest,
    DismissMergeRequest, ListMergeReviewQuery, MergeRequest, PatchNodeRequest,
};

/// `review_count`/`ReviewCountQuery` — same rationale as the `routes::nodes`
/// export above: judgement-graph PR4 adds a `merge_reviews` total to the
/// sidebar badge, and the only honest proof that the key is present AND
/// correct AND that the other two totals are unchanged is calling the REAL
/// handler against a real database.
pub use routes::source_connections::{
    create as create_source_connection, list as list_source_connections,
    update as update_source_connection, Create as CreateSourceConnection,
    Update as UpdateSourceConnection,
};

pub use routes::relationships::{review_count, ReviewCountQuery};

/// The join-request queue's own handlers (CS-437, org-onboarding plan §3
/// items 4 + 14) — same "private module, narrow `pub use` re-export" idiom
/// as `journal_hour_items` above, for the identical reason: the admin trio
/// (`list`/`approve`/`reject`) takes a [`tenancy::extractor::Tenant`], and
/// `backend/tests/org_join_requests_pg.rs` needs to drive them with a
/// hand-built `Tenant`/`UserContext` rather than standing up the full
/// `TenantRuntime`/multi-DB routing harness just to prove D3's Clerk-first
/// approve sequence (reservation-then-Clerk-then-mirror, unwind-and-leave-
/// open on failure) lands correctly — the org id/account id/admin role are
/// exactly what that suite already controls directly. Renamed on import
/// (`create as create_join_request`, etc.) since the bare names collide with
/// several other route modules' own `create`/`list`/`approve`/`reject`.
pub use routes::org_join_requests::{
    approve as approve_join_request, count as count_join_requests, create as create_join_request,
    list as list_join_requests, list_mine as list_my_join_requests, reject as reject_join_request,
    ApproveJoinRequestRequest, CreateJoinRequestRequest, JoinRequestCountResponse,
    ListJoinRequestsQuery,
};

/// The kill trigger's per-account window query (judgement-graph PR4), so the
/// same PG suite can bind the REAL query text against a real
/// `reef_judgement` table — see the const's own doc for why a runtime-checked
/// query inside a health endpoint needs a live test and not just the pure
/// predicate's unit tests.
pub use routes::admin::CHAIN_HEALTH_LLM_ADJUDICATION_WINDOW_SQL;

/// `admin_chain_health` itself, for the one property that cannot be proven
/// any other way (judgement-graph PR4, review round 1): that an unqueryable
/// `reef_judgement` degrades ONLY the kill trigger and still renders the
/// three incident-detecting invariants. A pure test cannot see that — the
/// failure has to come from a real database refusing a real query.
pub use routes::admin::admin_chain_health;
pub use routes::admin::{admin_pipeline_stage_health, PipelineStageHealthQuery};

/// The plan-entitlement query, so `tests/billing_grant_seats_pg.rs` runs the
/// REAL one instead of a re-typed copy — the rest of `billing` stays
/// crate-private. That suite previously defined its own duplicate of the
/// SQL, which meant a regression in the handler's query left every test in
/// it green; review caught that, and this narrow re-export is what closes
/// it. Same reasoning as `CHAIN_HEALTH_LLM_ADJUDICATION_WINDOW_SQL` above:
/// a money query needs its live test bound to the production text, not to a
/// copy a reader is invited to eyeball.
pub use billing::entitlement;

use dek_cache::DekCache;
use rate_limit::InMemoryRateLimiter;

// ── Request-id header name ────────────────────────────────────────────────────
const X_REQUEST_ID: &str = "x-request-id";

/// The `APP_ENV`-derived deploy-mode flags, resolved ONCE from the process
/// environment (test-flakiness follow-up to org-tenancy PR 3.3).
///
/// Before this type existed, `app()` (via `public_cors_layer`'s `is_dev` and
/// this function's own `is_production`/`dev_routes` OpenAPI-docs gate) read
/// `std::env::var("APP_ENV")` itself, every time `app()` was built. Two tests
/// mutated the process-global `APP_ENV` mid-test (`remove_var`) to exercise
/// the non-production branch; because `APP_ENV` is shared by the whole test
/// binary, ANY other concurrently-running test that also built `app()` could
/// observe the var mid-mutation and flip its own `is_dev`/`is_production` —
/// an inherently racy design that a lock around just the two mutators could
/// not fix (dozens of other tests build `app()` without that lock).
///
/// The fix: resolve `APP_ENV` exactly once, at boot (see [`AppEnv::resolve`],
/// called from `run()`), and carry the resolved bools on [`AppState`] instead
/// — `app()` and everything under it now reads `state.env`, never the
/// process env, so building `app()` is a pure function of the `AppState` it
/// was given. Tests construct an explicit `AppEnv` (or rely on
/// [`AppEnv::default`]'s production-like posture via `test_app_state()`)
/// instead of touching global env at all, so parallel tests can never race
/// each other through this flag again.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AppEnv {
    /// `APP_ENV == "development"` EXACTLY (fail-closed, P2 hardening from the
    /// security review of org-tenancy PR 3.3): only this exact value opens
    /// the dev-permissive localhost-CORS allowance in
    /// [`is_allowed_cors_origin`] — an unset or mistyped `APP_ENV` must NOT
    /// silently behave like dev.
    pub is_dev: bool,
    /// `APP_ENV == "production"` exactly. Gates whether `app()` mounts the
    /// OpenAPI/Swagger-UI docs router (`dev_routes`) — docs are mounted
    /// whenever this is `false`, i.e. everywhere except a real `production`
    /// deploy.
    pub is_production: bool,
}

impl AppEnv {
    /// Resolve once from the process environment. Call this EXACTLY once, at
    /// boot ([`run`]) — every other consumer should receive the result
    /// through [`AppState::env`], never read `APP_ENV` itself.
    pub fn resolve() -> Self {
        let raw = std::env::var("APP_ENV").ok();
        Self {
            is_dev: raw.as_deref() == Some("development"),
            is_production: raw.as_deref() == Some("production"),
        }
    }
}

impl Default for AppEnv {
    /// Production-like posture (`is_dev: false`, `is_production: true`) —
    /// the strictest behavior (no dev-permissive CORS, no exposed OpenAPI
    /// docs), so a test that builds an `AppState` without explicitly
    /// overriding `env` fails safe rather than silently getting dev
    /// behavior. `test_app_state()` uses this default; tests that need the
    /// non-production branch (e.g. the OpenAPI-docs gate) override it
    /// explicitly instead of mutating `APP_ENV`.
    fn default() -> Self {
        Self {
            is_dev: false,
            is_production: true,
        }
    }
}

// ── Shared application state ──────────────────────────────────────────────────
// The pool is `Option<PgPool>` so the binary can start without DATABASE_URL
// in test / local-dev environments.  Handlers that need the pool check for
// `None` and return 503.
#[derive(Clone)]
pub struct AppState {
    pub db: Option<PgPool>,
    /// Storage abstraction over the Postgres backend. Present whenever `db` is — the
    /// two are constructed together and migrated handler-by-handler off `db`.
    pub store: Option<Arc<dyn store::Store>>,
    /// Resend API key for sending transactional email.
    /// When `None` the email-send handlers return 503.
    pub resend_api_key: Option<String>,
    /// Cloudflare R2 blob storage client.
    /// When `None` blob uploads are skipped (dev/test mode).
    pub r2: Option<Arc<R2Client>>,
    /// Shared HTTP client with connection pooling, sized for the LLM path —
    /// a 60-second default timeout (`LLM_TIMEOUT_SECS`), not a short one.
    /// Callers with a tighter outbound budget (e.g.
    /// `clerk_admin::clerk_http_client`'s 10s) build their own dedicated
    /// client rather than reusing this one.
    pub http_client: reqwest::Client,
    /// Embedding provider for query vectorisation.
    pub embedder: Arc<dyn coralswarm_core::embed::EmbedProvider + Send + Sync>,
    /// LLM provider for synthesis.
    /// When `None` the query endpoint runs in retrieval-only mode (HTTP 200 with
    /// ranked `sources` and no synthesized answer) rather than returning an error.
    pub llm: Option<Arc<dyn llm::LlmProvider + Send + Sync>>,
    /// Per-user rate limiter.  The trait object allows swapping to a Redis
    /// backend for multi-instance deployments without touching handler code.
    pub rate_limiter: Arc<dyn rate_limit::RateLimiter>,
    /// W2 admission-control settings, resolved ONCE at boot (see
    /// [`admission::AdmissionConfig::from_env`]) rather than read per request.
    /// Defaults to observe-only: counted and logged, never enforced.
    pub admission: admission::AdmissionConfig,
    /// W6 metrics sink. `NoopMetrics` unless `CORALSWARM_METRICS` selects an
    /// exporter, so this is inert by default and costs nothing per request.
    pub metrics: Arc<dyn telemetry::Metrics>,
    /// W2 per-pool in-flight counters. Keyed by the pool a request touches
    /// (`control`, `org:{clerk_org_id}`) — NOT by tenant identity, because
    /// contention is a property of the pool, not of who is asking.
    pub in_flight: Arc<admission::InFlightRegistry>,
    /// Short-lived in-process cache for unwrapped Data Encryption Keys.
    /// Avoids a DB round-trip on every query within the TTL window (5 min).
    pub dek_cache: DekCache,
    /// Short-lived in-process cache for resolved per-org connector flag
    /// decisions (Foundation train PR 1.3). Same TTL/shape as `dek_cache`
    /// above — see `connector_flags` module doc.
    pub connector_flag_cache: connector_flags::ConnectorFlagCache,
    /// Database-per-org binding-gate runtime (registry + routing cache + apex +
    /// flag). `None` or `enabled == false` → the gate is a pass-through and
    /// behavior is byte-identical to single-tenant. Wired in cloud `run()`.
    pub tenant_runtime: Option<Arc<tenancy::runtime::TenantRuntime>>,
    /// Process-wide per-tenant scheduling signal shared with every worker
    /// harness. Lives on `AppState` for ONE reason: request/webhook paths that
    /// enqueue a `jobs` row for an ORG tenant must be able to call
    /// [`workers::tenant_harness::TenantActivity::note_enqueue`] so the
    /// harness pulls that tenant forward instead of leaving the row to wait
    /// out an idle-backoff slot (up to 20 min). See that method's doc.
    pub worker_activity: Arc<workers::tenant_harness::TenantActivity>,
    /// The `APP_ENV`-derived deploy-mode flags, resolved ONCE at boot (see
    /// [`AppEnv::resolve`], called from `run()`) and threaded through here so
    /// `app()`, `public_cors_layer`, and the OpenAPI-docs gate read this
    /// field instead of `std::env::var("APP_ENV")` — eliminating the
    /// process-global env read (and the test race it caused) from every
    /// `app()` build. Tests set this explicitly (or take
    /// [`AppEnv::default`]'s production-like posture via `test_app_state()`)
    /// rather than mutating `APP_ENV`.
    pub env: AppEnv,
    /// Tracks handler-spawned background tasks that run AFTER a response has
    /// already been sent (Slack webhook message ingest, member-joined
    /// destination/backfill prompts, the huddle-probe persist) so `run()` can
    /// give them a bounded grace-period drain on shutdown.
    ///
    /// `with_graceful_shutdown` on the two axum listeners in `run()` only
    /// drains in-flight HTTP *connections* — it has no visibility into work a
    /// handler kicked off with `tokio::spawn` and then already returned 200
    /// for. That work runs after the response completes, so axum never sees
    /// it, and `#[tokio::main]` silently aborts it the instant `run()`
    /// returns. Fly sends SIGTERM before stopping/redeploying a machine on
    /// every deploy, so this was a live, routine drop, not a hypothetical one.
    ///
    /// Deliberately reuses `tokio_util::task::TaskTracker` rather than
    /// inventing a second shutdown mechanism alongside `crate::shutdown`'s
    /// job-worker `Shutdown` flag: that flag's contract is "stop *claiming*
    /// new queued jobs", which has no meaning for a one-shot task a handler
    /// spawns outside the job queue. A `TaskTracker` instead tracks the tasks
    /// themselves and gives `run()` a `wait()` future that resolves once every
    /// tracked task (spawned via `state.task_tracker.spawn(..)` instead of the
    /// bare `tokio::spawn`) has finished — bounded by the same grace window
    /// (`worker_shutdown_grace`) the job workers get, in `run()`'s drain step.
    pub task_tracker: tokio_util::task::TaskTracker,
    /// Cancellation token handed to rmcp's `StreamableHttpService`
    /// (`mcp::mcp_router_with_validator`, `StreamableHttpServerConfig::
    /// with_cancellation_token`). rmcp cancels every active `/mcp` session
    /// and stops accepting new requests when this fires.
    ///
    /// Wired to the process's graceful-shutdown signal in `run()`, the same
    /// spot that triggers `crate::shutdown::Shutdown` — see that call site's
    /// comment. A fresh, never-cancelled token here (every non-`run()`
    /// constructor, including all test helpers) is inert: nothing observes
    /// it, so it changes no behavior for tests/one-shot synthetic
    /// states that never front a real `/mcp` listener.
    pub mcp_cancellation_token: tokio_util::sync::CancellationToken,
    /// MCP spec 2026-07-28 PR 2: the in-process SEP-2663 task store/executor
    /// backing `ask_ocean`'s MODERN-era response (`mcp::server::CoralSwarmMcp::
    /// get_task`/`update_task`/`cancel_task`, and the `spawn_owned` call in
    /// `mcp::tools::read::ask_ocean`). See [`mcp::server::McpTaskRegistry`]
    /// for why this wraps rmcp's own `TaskManager` rather than using it bare.
    /// One instance per PROCESS, shared across every `/mcp` request — the
    /// streamable-HTTP transport builds a fresh `CoralSwarmMcp` handler per
    /// request (stateless mode, see `mcp/mod.rs`'s module doc), so a registry
    /// owned by the handler itself would lose every spawned task the instant
    /// that request's response was sent, before a client's first `tasks/get`
    /// poll could ever reach it. Living here instead means a poll landing on
    /// the SAME process (Fly machine) that spawned the task can always find
    /// it.
    ///
    /// **Known limitation, accepted (same class as the rate limiter's own
    /// per-VM caveat in `mcp::McpConfig::from_env`, and the cancellation-token
    /// orphaning documented on `mcp/mod.rs`'s `mcp_service`): this is
    /// PER-PROCESS, not shared across a multi-machine Fly deployment.** A
    /// `tasks/get` poll routed to a DIFFERENT machine than the one that
    /// spawned the task will not find it (`-32602` "unknown task"). Acceptable
    /// today because MCP traffic runs on a single replica (the same
    /// constraint `RATE_LIMIT_BACKEND=pg` exists to warn about); revisit
    /// alongside that constraint if MCP is ever fronted by more than one
    /// machine. The LEGACY era does not share this limitation — its task
    /// state lives in Postgres (`mcp_tasks`, migration 0140/0047), reachable
    /// from any machine.
    pub mcp_task_manager: mcp::server::McpTaskRegistry,
    /// Issue #593 item 2: in-process, per-user counter of `ask_ocean` tasks
    /// currently in the `working` state, bounding how many syntheses a
    /// single caller may hold open at once (a rejected N+1th call is a plain
    /// tool error, never a block). Lives here rather than on `CoralSwarmMcp`
    /// for the identical stateless-per-request reason as `mcp_task_manager`
    /// — see [`mcp::server::AskInFlight`]'s own doc comment, including its
    /// accepted per-process (not cross-replica) scope.
    pub mcp_ask_in_flight: mcp::server::AskInFlight,
    /// Outbound Stripe REST client — `STRIPE_SECRET_KEY` read ONCE at boot
    /// (the `llm` pattern, `lib.rs:2433-2439`, not the `clerk.rs`
    /// per-call-re-read pattern): the key sits on a user-facing request path
    /// (checkout, seat quantity push), so a per-call `std::env::var` buys
    /// nothing and makes "is billing configured?" answerable only at request
    /// time. `None` when `STRIPE_SECRET_KEY` is unset — every outbound call
    /// site must answer `StripeError::NotConfigured` and the consuming route
    /// must answer 503, charging nothing. Fail-closed, mirroring the inbound
    /// webhook verifier's `VerifyResult::SecretNotSet` ⇒ 503 posture
    /// (`routes/stripe_webhook.rs:242,252-255,598-608`). A key rotation
    /// therefore needs a process restart — acceptable, and stated in the
    /// runbook: `fly secrets set` restarts the machine anyway.
    pub stripe: Option<Arc<billing::stripe_client::StripeClient>>,
}

impl AppState {
    /// The CONTROL/consumer-plane pool — the same `Option<PgPool>` as the `db`
    /// field, under a name that says which plane it is (org-tenancy PR 1.6).
    ///
    /// Route handlers must not reach for the ambient `state.db` for
    /// CONTENT-plane work (that's what the `Tenant` extractor resolves, and
    /// what the `scripts/check_no_ambient_pool.sh` CI lint checks under
    /// `backend/src/routes/`). A handful of route call sites legitimately
    /// read CONTROL-only tables (`accounts`, `account_prefs`,
    /// `account_members`, `calendar_prefs`) that exist only on this plane —
    /// those go through this accessor, both to document the plane choice at
    /// the call site and so the grep-guard's `state.db` pattern doesn't flag
    /// them.
    ///
    /// **This is a CONTROL-TABLE-ONLY accessor and a documented escape hatch
    /// in the guard**: it returns the SAME physical consumer pool as `db`,
    /// and the grep-guard cannot see what a call site queries through it. A
    /// content-table read (`atoms`, `graphs`, `nodes`, `meeting_*`, …) via
    /// `control_db()` silently reintroduces the exact ambient-pool bug the
    /// guard exists to prevent — forbidden by convention, enforced by
    /// review, NOT by CI. PR 4.2's mechanical `db` → `control_db` field
    /// rename subsumes this accessor.
    pub fn control_db(&self) -> Option<&PgPool> {
        self.db.as_ref()
    }

    /// The CONTROL/consumer-plane `Store` — same rationale AND same
    /// control-table-only / escape-hatch caveat as [`AppState::control_db`],
    /// for the `Store`-mediated control-table reads (`account_is_org`,
    /// `account_members` lookups, DEK-wrapped reads on the consumer plane).
    /// Calling a content-plane `Store` method through this accessor is the
    /// ambient-pool bug under a different name — forbidden by convention,
    /// not caught by the guard.
    pub fn control_store(&self) -> Option<&Arc<dyn store::Store>> {
        self.store.as_ref()
    }

    // ── User-facing web links (org-slack step 5 — the FRONTEND_URL fix) ────
    //
    // A single static `FRONTEND_URL` cannot be correct once the binding gate is
    // enforcing: the web origin is `app.{WEB_APEX_DOMAIN}` for EVERY plane
    // (per-org `{slug}.{WEB_APEX_DOMAIN}` subdomains are retired — claim-based
    // tenancy names the org by the token's `org_id` claim, not the host), but
    // pre-cutover / flag-off deploys still need the `FRONTEND_URL` value. Every
    // user-facing URL this crate builds (OAuth-install redirects; the
    // Teams/Outlook meeting-pane return; digest/nudge emails; meeting/session
    // share links) must go through one of the methods below instead of reading
    // `FRONTEND_URL` directly, so no call site can drift out of step with that
    // rule the way `FRONTEND_URL` itself once drifted out of step with the
    // org-tenancy apex split.

    /// The web origin (scheme + host, NO trailing slash) a PERSONAL
    /// account's user-facing links/redirects should point at:
    /// `https://app.{web_apex}` once the binding gate is actually enforcing
    /// (`tenant_runtime.enabled` — pre-cutover / `MULTITENANT_ENABLED` off
    /// deploys keep today's behavior byte-identical), using the SAME
    /// `web_apex` value `is_allowed_cors_origin` already trusts as the UI's
    /// own origin family (see [`tenancy::runtime::TenantRuntime::web_apex`]'s
    /// doc comment) — else `FRONTEND_URL` (env, default
    /// `http://localhost:3000`), now a LOCAL-DEV / pre-cutover override
    /// only; see `.env.example`.
    ///
    /// Never derived from a request's own `Host` header: only server
    /// config is consulted here, so a caller can never redirect a browser
    /// off this origin by spoofing `Host`.
    pub fn personal_web_base_url(&self) -> String {
        // e2e only: the harness serves the UI from plain-http localhost while
        // the gate enforces, so `https://app.{web_apex}` cannot exist there.
        #[cfg(feature = "e2e")]
        if let Some(origin) = e2e_origin("E2E_WEB_ORIGIN") {
            return origin;
        }
        match self.tenant_runtime.as_ref() {
            Some(rt) if rt.enabled => format!("https://app.{}", rt.web_apex),
            _ => std::env::var("FRONTEND_URL").unwrap_or_else(|_| "http://localhost:3000".into()),
        }
    }

    /// The web origin for an ORG account's user-facing links/redirects.
    ///
    /// **Org subdomains are retired: this is [`Self::personal_web_base_url`] for
    /// every org.** Claim-based tenancy serves every tenant — personal and org —
    /// from the ONE web host (`https://app.{web_apex}`); the org is named by the
    /// token's `org_id` claim, never by the host. This used to build
    /// `https://{slug}.{web_apex}` from the org's `tenant_databases` routing row,
    /// which now points a browser at a host that no longer serves a UI (the
    /// OAuth-return redirects, e.g. the Teams/Outlook meeting-pane
    /// `/teams/meeting/connected` return, and the digest/nudge emails all land
    /// here). The signature is kept so the call sites that already thread
    /// `(control_pool, clerk_org_id)` need no churn, and so a future per-org
    /// destination has one place to come back to; neither argument is consulted
    /// and **no database query is made**.
    ///
    /// Same "never trusts a request `Host` header" contract as
    /// [`Self::personal_web_base_url`].
    pub async fn org_web_base_url(&self, _control_pool: &PgPool, _clerk_org_id: &str) -> String {
        self.personal_web_base_url()
    }

    /// Single dispatcher for every user-facing link/redirect this crate
    /// builds (org-slack step 5) — the one call every site that used to
    /// read `FRONTEND_URL` directly now goes through. `clerk_org_id: None`
    /// resolves via [`Self::personal_web_base_url`] (sync, no DB call);
    /// `Some(id)` via [`Self::org_web_base_url`].
    ///
    /// **Not every caller should pass the account's real plane here** — see
    /// `routes::shares`/`routes::session_shares`'s own call sites for the
    /// documented exception (their public viewer route is consumer-plane
    /// only by design, so an org-owned share deliberately still resolves
    /// through the personal base, never its own org host).
    pub async fn web_base_url(&self, control_pool: &PgPool, clerk_org_id: Option<&str>) -> String {
        match clerk_org_id {
            Some(id) => self.org_web_base_url(control_pool, id).await,
            None => self.personal_web_base_url(),
        }
    }

    /// Public API origin. Claim-based tenancy
    /// binds org tokens through the configured API apex, so every plane uses
    /// the same origin. Before tenancy is enabled, use the existing BASE_URL
    /// configuration (localhost:8080 by default).
    pub fn api_base_url(&self) -> String {
        // e2e only: the harness's API is plain-http localhost (see
        // `personal_web_base_url`).
        #[cfg(feature = "e2e")]
        if let Some(origin) = e2e_origin("E2E_API_ORIGIN") {
            return origin;
        }
        match self.tenant_runtime.as_ref() {
            Some(rt) if rt.enabled => format!("https://{}", rt.apex),
            _ => std::env::var("BASE_URL").unwrap_or_else(|_| "http://localhost:8080".into()),
        }
    }
}

/// The e2e harness's origin overrides (`E2E_WEB_ORIGIN`, `E2E_API_ORIGIN`):
/// the one place both are read. `e2e` builds only.
#[cfg(feature = "e2e")]
fn e2e_origin(name: &str) -> Option<String> {
    let v = std::env::var(name).ok()?;
    let v = v.trim().trim_end_matches('/');
    (!v.is_empty()).then(|| v.to_string())
}

// ── Structured-logging initialisation ────────────────────────────────────────
fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    tracing_subscriber::registry()
        .with(filter)
        .with(tracing_subscriber::fmt::layer().json())
        .init();
}

// ── Middleware: attach request_id and emit latency log ────────────────────────
async fn request_id_and_latency(mut req: Request, next: Next) -> Response {
    let request_id = req
        .headers()
        .get(X_REQUEST_ID)
        .and_then(|v| v.to_str().ok())
        .map(String::from)
        .unwrap_or_else(|| Uuid::new_v4().to_string());

    // Stash the id in extensions so handlers can access it if needed.
    req.extensions_mut().insert(request_id.clone());

    let start = Instant::now();
    let method = req.method().clone();
    // Redact any share token embedded in the path BEFORE it reaches a log line —
    // the share-view routes carry the secret token as a path segment, and this
    // access log runs on the public router (M: token in logs).
    let uri = redact_share_token(req.uri());

    let route = request_timing::route_of(&req);
    let class = route_class::RouteClass::of(req.method(), req.extensions());

    let (mut response, timing) = request_timing::scope(start, class, next.run(req)).await;

    let elapsed = start.elapsed();
    let latency_ms = elapsed.as_millis();
    let phases = timing.phases(elapsed);
    let status = response.status().as_u16();
    response
        .headers_mut()
        .insert("server-timing", phases.header_value());
    request_timing::LEDGER.record(&route, status, request_timing::now_secs());
    if class == route_class::RouteClass::PageRead {
        request_timing::PAGE_READ_LEDGER.record(
            &route,
            elapsed,
            status,
            timing.page_read_signals(),
            request_timing::now_secs(),
        );
    }

    // Which plane served this request (claim-based tenancy PR 1.4, decision
    // 12). `require_auth` lifts these onto the RESPONSE precisely so this
    // outermost layer can read them — the request was moved into `next.run`
    // long ago. Absent on unauthenticated routes, where the fields are simply not logged rather than logged as a default.
    let tenancy = response
        .extensions()
        .get::<crate::middleware::auth::TenancyLogFields>()
        .cloned();

    // One line per request, as before; `route` is the template to group by,
    // `uri` stays for finding one request. 5xx and >1s go out at `warn` so
    // they can be filtered on level alone; demoting the rest to `debug` would
    // delete the only access log production has.
    macro_rules! access_log {
        ($level:expr) => {
            tracing::event!(
                $level,
                request_id = %request_id,
                method     = %method,
                route      = %route,
                uri        = %uri,
                status,
                latency_ms,
                auth_ms    = phases.auth_ms(),
                app_ms     = phases.app_ms(),
                tx_open_ms = phases.tx_open_ms(),
                tx_count   = phases.tx_count,
                route_class = class.as_str(),
                plane      = tenancy.as_ref().map(|t| t.plane),
                org_id     = tenancy.as_ref().and_then(|t| t.org_id.as_deref()),
                account_id = tenancy.as_ref().and_then(|t| t.account_id).map(tracing::field::display),
                "request completed"
            )
        };
    }
    if status >= 500 || elapsed >= request_timing::SLOW {
        access_log!(tracing::Level::WARN);
    } else {
        access_log!(tracing::Level::INFO);
    }

    response
}

/// Redact the unguessable share token from a request URI before it is logged.
///
/// `GET /v1/shared/{token}` (public) and `GET /v1/meetings/shares/view/{token}`
/// (authed) both carry the bearer secret as a PATH SEGMENT — as does the
/// coding-session-share clone's `GET /v1/shared/sessions/{token}` (public;
/// there is no authed counterpart in this phase, see
/// `routes::session_shares`'s header comment). The access log
/// (`request_id_and_latency`) otherwise records the full URI at `info` on every
/// request, capturing that secret in app logs / SIEM. Replace the token segment
/// with `{token}` (dropping any query string, which these routes don't use) so
/// the route is still observable without leaking the secret. Non-share URIs are
/// returned verbatim (path + query) exactly as before.
fn redact_share_token(uri: &axum::http::Uri) -> String {
    let path = uri.path();
    for prefix in [
        "/v1/shared/sessions/",
        "/v1/shared/",
        "/v1/meetings/shares/view/",
    ] {
        if let Some(rest) = path.strip_prefix(prefix) {
            // Only redact when there's actually a token segment to hide.
            if !rest.is_empty() && !rest.contains('/') {
                return format!("{prefix}{{token}}");
            }
        }
    }
    match uri.query() {
        Some(q) => format!("{path}?{q}"),
        None => path.to_string(),
    }
}

// ── Handlers ──────────────────────────────────────────────────────────────────

/// Liveness probe — does not require a DB connection.
async fn healthz() -> (StatusCode, Json<Value>) {
    (StatusCode::OK, Json(json!({"status": "ok"})))
}

/// Readiness probe.
///
/// Returns 200 only when the Postgres pool is present and can answer a
/// `SELECT 1`.  If DATABASE_URL was absent at startup the pool will be `None`
/// and this returns 503 so the load-balancer / orchestrator knows the replica
/// is not yet ready to serve traffic.
async fn readyz(State(state): State<Arc<AppState>>) -> (StatusCode, Json<Value>) {
    match &state.db {
        None => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"status": "unavailable", "reason": "no database pool"})),
        ),
        Some(pool) => {
            // Use sqlx::query (not the macro form) so it compiles without a
            // live DATABASE_URL at compile time (offline mode not required).
            match sqlx::query("SELECT 1").execute(pool).await {
                Ok(_) => (StatusCode::OK, Json(json!({"status": "ready"}))),
                Err(e) => {
                    tracing::warn!(error = %e, "readyz DB ping failed");
                    (
                        StatusCode::SERVICE_UNAVAILABLE,
                        Json(json!({"status": "unavailable", "reason": "db ping failed"})),
                    )
                }
            }
        }
    }
}

// ── Stub embedder (dev / test mode) ──────────────────────────────────────────

/// Zero-vector embedder used when ONNX model files are absent.
/// Returns a 384-dimensional zero vector — sufficient for the binary to start
/// without model files in dev/test mode.
pub(crate) struct StubEmbedder;

impl coralswarm_core::embed::EmbedProvider for StubEmbedder {
    fn embed_batch<'a>(
        &'a self,
        inputs: &'a [coralswarm_core::embed::EmbedInput],
        _: coralswarm_core::embed::ProviderPermit,
    ) -> coralswarm_core::embed::EmbedBatchFuture<'a> {
        let n = inputs.len();
        Box::pin(async move { Ok(vec![vec![0.0f32; 384]; n]) })
    }
}

// ── Routers ───────────────────────────────────────────────────────────────────

/// The `/v1` API route table, without any authentication layer or state.
///
/// [`app`] applies `require_auth` on top; the test-support
/// `test_support_v1_app` injects a fixed [`UserContext`] instead.
fn v1_routes() -> Router<Arc<AppState>> {
    Router::new()
        // Caller identity / resolved active account
        .merge(routes::page_read::plain("/me", routes::me::get_me))
        // The app shell's one boot read: `me`, the reachable graphs and the
        // join-request badge count, each built by its own handler above/below.
        // See `routes::bootstrap`.
        .merge(routes::page_read::plain(
            "/bootstrap",
            routes::bootstrap::get_bootstrap,
        ))
        // Settings launch PR 2.4: delete my account. Interactive Clerk session
        // only (see routes::account_deletion's module doc).
        .merge(routes::page_read::plain(
            "/me/delete-account",
            routes::account_deletion::get_delete_account,
        ))
        .route(
            "/me/delete-account",
            post(routes::account_deletion::post_delete_account),
        )
        .route(
            "/me/delete-account/cancel",
            post(routes::account_deletion::post_cancel_delete_account),
        )
        // Boot-time IANA timezone report (Today-journal tier feature) — see
        // `routes::me`'s module doc.
        .route("/me/timezone", put(routes::me::put_me_timezone))
        // The caller's active ocean, per (user, account) — claim-based
        // tenancy decision 8, migration `0201_user_active_graph`. The READ
        // rides `GET /v1/me` above (already on the client's boot path); this
        // is the write half. See `routes::me`'s module doc.
        .route("/me/active-graph", put(routes::me::put_me_active_graph))
        // W6: the client half of the metrics pipeline. Browser-side failures
        // — `ERR_INSUFFICIENT_RESOURCES`, client timeouts, requests refused
        // by W1's queue — never reach the server, so no server-side metric
        // can show them. Write-only, returns 204, reads no database. Mounted
        // inside the authenticated tree so reports are attributable and W2's
        // per-user ceiling already covers it.
        .route(
            "/telemetry/client",
            post(routes::telemetry::report_client_metrics),
        )
        // Per-account preferences (LLM/synthesis mode)
        .route(
            "/prefs",
            get(routes::prefs::get_prefs).put(routes::prefs::put_prefs),
        )
        // Usage-metering PR12: real numbers behind the org Usage/Billing
        // pages and the personal Usage page — see routes::usage's module doc.
        // Billing self-serve PR D: outbound Stripe Checkout — see
        // routes::checkout's module doc. Completion (grant issuance / pack
        // credit) is routes::stripe_webhook's job, not this route's.
        .route(
            "/billing/checkout/subscription",
            post(routes::checkout::create_subscription_checkout),
        )
        .route(
            "/billing/checkout/pack",
            post(routes::checkout::create_pack_checkout),
        )
        // Settings launch PR 3.3: Stripe Billing Portal session, the billing
        // summary, and the seat-change price quote. Admin-only for orgs; see
        // routes::billing_manage's module doc.
        .route("/billing/portal", post(routes::billing_manage::create_portal_session))
        .merge(routes::page_read::plain(
            "/billing/summary",
            routes::billing_manage::get_billing_summary,
        ))
        .merge(routes::page_read::plain(
            "/billing/trial-banner",
            routes::billing_manage::get_trial_banner,
        ))
        .route("/billing/seats/quote", get(routes::billing_manage::get_seat_quote))
        // Settings launch PR 3.4: extra-usage settings and the top-up
        // Checkout. Admin-only for orgs; see routes::extra_usage's module doc.
        .route("/billing/extra-usage", put(routes::extra_usage::put_extra_usage))
        .route("/billing/extra-usage/topup", post(routes::extra_usage::create_topup))
        // Billing self-serve PR F: the one new user-facing seat-lifecycle
        // route — moving an ALREADY-PURCHASED seat between tiers. See
        // routes::billing_seats' module doc for why "invite"/"remove" are
        // NOT routes (org membership is entirely Clerk-managed; those are
        // side effects of clerk_provision's webhook handlers instead).
        .route(
            "/billing/members/{user_id}/seat-tier",
            post(routes::billing_seats::change_seat_tier),
        )
        // CS-442 (item 40, D8): the active seat-tier catalog — the
        // join-request approval row's tier picker (CS-441, web-side) reads
        // this, falling back to `standard` if it 404s. Any signed-in
        // caller (see that route's own module doc for why no further
        // org/admin gate is needed — global reference data).
        .merge(routes::page_read::plain(
            "/billing/seat-tiers",
            routes::billing_seats::list_seat_tiers,
        ))
        // Settings launch plan 3.6 (decision 15): the caller's own billing
        // position for the "Your usage" card. Any signed-in member; credits
        // and percentages only, never dollars. See `billing::me`.
        .merge(routes::page_read::plain(
            "/billing/me",
            routes::billing_me::get_billing_me,
        ))
        // The caller's passphrase-wrapped private-ocean key (cross-device enroll)
        .route(
            "/me/private-key",
            get(routes::private_key::get_private_key)
                .put(routes::private_key::put_private_key)
                .delete(routes::private_key::delete_private_key),
        )
        // Org-escrow recovery (D6): server stores only opaque client-encrypted
        // blobs + the recovery PUBLIC key. Shamir shares of the private key are
        // admin-held out-of-band and never touch the server (it stays blind).
        .route(
            "/escrow/wrapped-key",
            get(routes::escrow::get_wrapped_key).put(routes::escrow::put_wrapped_key),
        )
        .route(
            "/escrow/recovery-pubkey",
            get(routes::escrow::get_recovery_pubkey).put(routes::escrow::put_recovery_pubkey),
        )
        .route(
            "/escrow/recovery-log",
            get(routes::escrow::list_recovery_log).post(routes::escrow::post_recovery_log),
        )
        .route(
            "/escrow/recovered-key",
            put(routes::escrow::put_recovered_key),
        )
        .route(
            "/graphs/{graph_id}/source-connections",
            get(routes::source_connections::list).post(routes::source_connections::create),
        )
        .route(
            "/graphs/{graph_id}/source-connections/{id}",
            axum::routing::patch(routes::source_connections::update),
        )
        // Graphs
        .merge(routes::page_read::plain(
            "/graphs",
            routes::graphs::list_graphs,
        ))
        .route("/graphs", post(routes::graphs::create_graph))
        .route("/graphs/{graph_id}", get(routes::graphs::get_graph))
        .route("/graphs/{graph_id}/atoms", get(routes::graphs::list_atoms))
        // W3 (docs/plans/request-reliability-and-scale.md §6.3): the bulk
        // reef↔coral edge contract that replaces one `GET /v1/nodes/{id}/atoms`
        // per reef. `?facets=true` on the same route returns exact
        // server-computed counts, so a client whose drain budget bound can
        // still render truthful facets instead of ones derived from 3% of the
        // edges. One route, so there is exactly one place where "who may read
        // this graph's edges" is decided.
        .route(
            "/graphs/{graph_id}/node-atoms",
            get(routes::nodes::list_graph_node_atoms),
        )
        // True reef/coral/source totals for the Reefs page tab bar (CS-reef-nav-counts)
        .merge(read_models::registry::router())
        .merge(routes::page_read::plain("/load/oceans",routes::oceans_index::get))
        .route("/load/oceans/refresh",post(routes::oceans_index::refresh))
        // Per-ocean knowledge graph (nodes + links) for the Atlas canvas
        // Ingested sources grouped from this ocean's corals (for the Sources view)
        .route(
            "/graphs/{graph_id}/sources",
            get(routes::graphs::list_sources),
        )
        // Perf-fix pair for the route above: a channel-level aggregate rollup, and per-group
        // metadata + a paginated page of corals. See routes::graphs's doc
        // comment above `list_source_groups`/`get_source_group`.
        .route(
            "/graphs/{graph_id}/source-groups",
            get(routes::graphs::list_source_groups),
        )
        .route(
            "/graphs/{graph_id}/source-groups/{channel_key}",
            get(routes::graphs::get_source_group),
        )
        // Source-level promotion (ADR-0007 D8): promote a whole source's reefs
        .route(
            "/graphs/{graph_id}/promote-source",
            post(routes::nodes::promote_source),
        )
        // Session-level promotion: publish a whole coding session — its
        // container row, all its corals, and the reefs they grew — together.
        // Not expressible through `promote-source`, which keys on ONE exact
        // `source_id` while a session spans one per checkpoint.
        .route(
            "/graphs/{graph_id}/promote-session",
            post(routes::nodes::promote_session),
        )
        // Entity-dedup sweep (auto-realize): merge duplicate entity reefs.
        .route(
            "/graphs/{graph_id}/dedup-entities",
            post(routes::nodes::dedup_entities),
        )
        // Retraction (§4.9 of docs/plans/graph-temporal-memory.md): revoke
        // stops future corals flowing from a published source; unpublish
        // (below, under /atoms) targets one already-promoted coral.
        .route(
            "/graphs/{graph_id}/revoke-publication",
            post(routes::nodes::revoke_publication),
        )
        // Unpublish: retract one coral from a shared ocean (§4.9).
        .route(
            "/graphs/{graph_id}/atoms/{atom_id}",
            delete(routes::nodes::unpublish_atom_route),
        )
        // Deletion (§7.3 of docs/plans/graph-temporal-memory.md) — three entry
        // points over the SAME delete_atom_cascade primitive. Deliberately NOT
        // under /atoms/{atom_id}: that path is unpublish (§4.9), a different
        // operation on a different object — see routes::deletion's module doc.
        .route(
            "/graphs/{graph_id}/corals/{atom_id}",
            delete(routes::deletion::delete_coral_route),
        )
        .route(
            "/graphs/{graph_id}/sources",
            delete(routes::deletion::delete_source_route),
        )
        .route(
            "/graphs/{graph_id}/subjects/{node_id}",
            delete(routes::deletion::delete_subject_route),
        )
        // Suggested entity merges for the Review tab + "keep separate" dismissal.
        // The standing merge-suggestions LISTING is gone (founder decision,
        // 2026-08-05): a speculative O(N^2) lexical sweep can never be
        // high-signal without usage evidence — two precision rounds of prod
        // junk proved it. Duplicates surface where they demonstrably matter:
        // auto-dedup at extraction, targeted in-chat cards, manual workbench
        // merge. The dismiss/merge ACTION routes below stay for those.
        .route(
            "/graphs/{graph_id}/merge-suggestions/dismiss",
            post(routes::nodes::dismiss_merge_suggestion),
        )
        // The standing MERGE-REVIEW queue (judgement graph, PR4) — unlike the
        // retired lexical listing above it, these are rows the ER worker
        // deliberately declined to decide, not speculative pairs. Read-only:
        // the two routes immediately around it are what resolves an item.
        .route(
            "/graphs/{graph_id}/merge-review",
            get(routes::nodes::list_merge_review),
        )
        // Confirm lives HERE, graph-anchored, rather than reusing
        // /nodes/{id}/merge — see routes::nodes' merge-review section header
        // for the team-ocean scoping bug that forced the split.
        .route(
            "/graphs/{graph_id}/merge-review/confirm",
            post(routes::nodes::confirm_merge_review),
        )
        // Nodes
        .route("/library", get(routes::library::list_library))
        .route("/nodes", get(routes::nodes::list_nodes))
        .route(
            "/nodes/{id}",
            get(routes::nodes::get_node_by_id).patch(routes::nodes::patch_node),
        )
        .route(
            "/nodes/{id}/candidates",
            get(routes::nodes::get_node_candidates),
        )
        .route("/nodes/{id}/digest", get(routes::nodes::node_digest))
        .route("/nodes/{id}/merge", post(routes::nodes::merge_node))
        // Reverse a recorded entity-resolution merge (CS-ER undo-log).
        .route(
            "/nodes/merges/{merge_id}/undo",
            post(routes::nodes::undo_merge),
        )
        .route(
            "/nodes/{id}/atoms",
            get(routes::nodes::get_node_atoms).post(routes::nodes::add_node_atom),
        )
        // Decision review queue (CS-069)
        .route("/candidates", get(routes::nodes::list_decision_candidates))
        .route("/nodes/{id}/confirm", post(routes::nodes::confirm_node))
        .route("/nodes/{id}/reject", post(routes::nodes::reject_node))
        // Promotion: publish a reef into a wider shared ocean (ADR-0006)
        .route("/nodes/{id}/promote", post(routes::nodes::promote_node))
        // Background job status/progress polling (async-promote PR) —
        // generic over every job `kind`, not promote-specific; see
        // `routes::jobs`'s module doc.
        .route("/jobs/{id}", get(routes::jobs::get_job))
        // Atoms (drill-through coral detail)
        .route("/atoms/{atom_id}", get(routes::atoms::get_atom))
        // Supersession edges
        .route(
            "/nodes/{id}/supersede",
            post(routes::supersession::create_supersession),
        )
        .route(
            "/nodes/{id}/supersede/{other_id}",
            delete(routes::supersession::delete_supersession),
        )
        .route(
            "/nodes/{id}/supersessions",
            get(routes::supersession::list_supersessions),
        )
        // Relationship review queue (ADR-0008): list inferred/suggested edges
        // for an ocean, then confirm or reject each one.
        .route(
            "/graphs/{graph_id}/relationships",
            get(routes::relationships::list_relationships),
        )
        .route(
            "/relationships/{edge_id}/confirm",
            post(routes::relationships::confirm_relationship),
        )
        .route(
            "/relationships/{edge_id}/reject",
            post(routes::relationships::reject_relationship),
        )
        // Review-sidebar badge totals (decisions + suggested edges) as two
        // COUNTs instead of two full-row list fetches per navigation.
        .route("/review/count", get(routes::relationships::review_count))
        // Ingest
        .route("/ingest/atom", post(routes::ingest::ingest_atom))
        // Transcript cleanup
        .route(
            "/transcript/cleanup",
            post(routes::transcript::cleanup_transcript),
        )
        // Post-meeting summary + name-correction revert controls (cs-raw-provenance)
        .route("/meetings", get(routes::meetings::list_meetings))
        .route("/meetings/evidence", get(meeting_evidence::get_evidence).layer(axum::middleware::from_fn(library_read::no_store)))
        .route("/meetings/evidence/retry", post(meeting_evidence::retry_evidence).layer(axum::middleware::from_fn(library_read::no_store)))
        .route("/sources/publications", post(selected_publication::publish).layer(axum::middleware::from_fn(library_read::no_store)))
        .route("/sources/publications/{id}", delete(selected_publication::retract).layer(axum::middleware::from_fn(library_read::no_store)))
        .route("/sources/evidence", get(source_evidence::get_evidence).layer(axum::middleware::from_fn(library_read::no_store)))
        .route("/sources/evidence/retry", post(source_evidence::retry_evidence).layer(axum::middleware::from_fn(library_read::no_store)))
        .route("/sources/items", get(source_evidence::list_items).layer(axum::middleware::from_fn(library_read::no_store)))
        .route("/library/search", post(library_read::search).layer(axum::middleware::from_fn(library_read::no_store)))
        .route("/library/resolve-scope", post(library_read::resolve_scope).layer(axum::middleware::from_fn(library_read::no_store)))
        .route("/library/neighborhood", post(library_read::neighborhood).layer(axum::middleware::from_fn(library_read::no_store)))
        .route("/library/disagreements", post(library_disagreements::disagreements).layer(axum::middleware::from_fn(library_read::no_store)))
        .route("/library/node-sessions", post(library_read::node_sessions).layer(axum::middleware::from_fn(library_read::no_store)))
        .route("/library/facets", post(library_read::facets).layer(axum::middleware::from_fn(library_read::no_store)))
        .route("/library/history", post(library_history::history).layer(axum::middleware::from_fn(library_read::no_store)))
        .route("/library/temporal/state", post(temporal::state).layer(axum::middleware::from_fn(library_read::no_store)))
        .route("/library/temporal/record", post(temporal::record).layer(axum::middleware::from_fn(library_read::no_store)))
        .route("/sources/enrichment/runs", post(corpus_enrichment::create_run))
        .route("/sources/enrichment/runs/{run_id}", get(corpus_enrichment::get_run))
        .route("/sources/enrichment/runs/{run_id}/units", get(corpus_enrichment::get_units))
        .route("/sources/enrichment/runs/{run_id}/promote", post(managed_history::promote))
        .route("/sources/enrichment/runs/{run_id}/advance", post(corpus_enrichment::advance_run))
        .route("/sources/enrichment/runs/{run_id}/pause", post(corpus_enrichment::pause_run))
        .route("/sources/enrichment/runs/{run_id}/resume", post(corpus_enrichment::resume_run))
        .route("/sources/items/{id}", delete(routes::deletion::delete_source_item_route))
        .route("/nodes/{id}/semantic-atoms", get(meeting_evidence::get_node_atoms).layer(axum::middleware::from_fn(library_read::no_store)))
        .route("/meetings/summary", get(routes::meetings::meeting_summary))
        // Create a user speaker-relabel as a first-class correction (cs-uxp3c).
        .route(
            "/transcript-corrections",
            post(routes::meetings::create_speaker_relabel),
        )
        .route(
            "/meetings/summary/generate",
            post(routes::meetings::generate_meeting_summary),
        )
        .route(
            "/transcript-corrections/{correction_id}/revert",
            post(routes::meetings::revert_correction),
        )
        .route(
            "/transcript-corrections/{correction_id}/restore",
            post(routes::meetings::restore_correction),
        )
        // Revocable per-meeting share links (cs-uxp3b-library-search). The
        // matching PUBLIC (no-auth) viewer `GET /v1/shared/{token}` lives in
        // the `open` router in `app()`, not here.
        .route(
            "/meetings/shares",
            post(routes::shares::create_share).get(routes::shares::list_shares),
        )
        .route(
            "/meetings/shares/{id}/revoke",
            post(routes::shares::revoke_share),
        )
        .merge(routes::page_read::plain(
            "/meetings/shares/view/{token}",
            routes::shares::view_share,
        ))
        // AI coding-agent session capture — read surface (coding-session
        // capture Phase 4, audit A8). Mirrors the meetings-shares list route
        // immediately above (query-param IDOR, same `graph_reachable_by_user`
        // guard).
        .route("/sessions", get(routes::sessions::list_sessions))
        // Static path segment, mounted ahead of the `{session_id}` param
        // route immediately below — axum's `matchit`-based router always
        // prefers a static segment over a path parameter at the same
        // position regardless of registration order, so `GET
        // /v1/sessions/facets` reaches this handler and never `get_session`
        // with `session_id="facets"` (see `routes::sessions`' module doc and
        // its route test proving this).
        .route("/sessions/facets", get(routes::sessions::session_facets))
        .route("/sessions/{session_id}", get(routes::sessions::get_session))
        // Revocable per-coding-session share links (coding-session capture
        // Phase 5, audit A7) — a faithful clone of the meeting-share routes
        // above. The matching PUBLIC (no-auth) viewer
        // `GET /v1/shared/sessions/{token}` lives in the `open` router in
        // `app()`, not here. Deliberately no list/authed-view route (see
        // `routes::session_shares`'s header comment).
        .route(
            "/sessions/shares",
            post(routes::session_shares::create_session_share),
        )
        .route(
            "/sessions/shares/{id}/revoke",
            post(routes::session_shares::revoke_session_share),
        )
        // Merge-undo alias for the frozen contract path; the canonical route is
        // `/nodes/merges/{merge_id}/undo`. Both reuse the node_merges undo-log.
        .route("/merges/{merge_id}/undo", post(routes::nodes::undo_merge))
        // Queries
        .route("/queries", get(routes::queries::list_queries))
        .route("/queries", post(routes::queries::create_query))
        .merge(route_class::classify(
            route_class::RouteClass::Stream,
            axum::http::Method::POST,
            "/queries/stream",
            routes::queries::create_query_stream,
        ))
        // Query-audit disclosure log (m365 enterprise train PR 4.3, plan
        // decision #11 part 2) — org-admin-only paginated read; see
        // `routes::query_audit`'s module doc.
        .route("/query-audit", get(routes::query_audit::list_query_audit))
        // D6 UI's read of `retraction_audit_log` (routed separately from B2
        // unit 02/#718 — see docs/plans/capture/02-b2-retract-fixes.md's "D6
        // UI" section and `routes::retractions`' own module doc). No
        // route-level role gate: this table's SELECT policy already encodes
        // D8's full visibility rule in SQL.
        .merge(routes::page_read::plain(
            "/retractions",
            routes::retractions::list_retractions,
        ))
        .route(
            "/retractions/{retraction_id}",
            get(routes::retractions::get_retraction),
        )
        // Finding 3 (independent review of PR #729, BLOCKING): every
        // retraction group that ever touched ONE coding session, across
        // however many per-checkpoint retract calls it took — see
        // `routes::retractions::get_session_retractions`'s own doc. A
        // 3-segment literal path, so it cannot collide with the 2-segment
        // `{retraction_id}` route above regardless of route-matching order.
        .route(
            "/retractions/session/{session_id}",
            get(routes::retractions::get_session_retractions),
        )
        // Conversations (cs-ask-chat-a)
        .route(
            "/conversations/{id}",
            get(routes::conversations::get_conversation)
                .patch(routes::conversations::patch_conversation)
                .delete(routes::conversations::delete_conversation),
        )
        .route(
            "/conversations/{id}/messages/{message_id}/feedback",
            put(routes::message_feedback::put_feedback)
                .delete(routes::message_feedback::delete_feedback),
        )
        // Teams
        .merge(routes::page_read::plain(
            "/teams",
            routes::teams::list_teams,
        ))
        .route("/teams", post(routes::teams::create_team))
        .merge(routes::page_read::plain(
            "/teams/members",
            routes::teams::list_members,
        ))
        // Team-ocean lifecycle management (rename/archive/restore) — a
        // workspace can now be renamed, archived, and (small, additive)
        // restored, not just created and rostered. See
        // `routes::teams::archive_team`'s doc comment for why archiving is
        // an ocean-level `graphs.archived_at` stamp, never a content delete.
        .route(
            "/teams/{team_id}",
            patch(routes::teams::rename_team).delete(routes::teams::archive_team),
        )
        .route(
            "/teams/{team_id}/restore",
            post(routes::teams::restore_team),
        )
        // Read half of the lifecycle trio — archived team oceans for the
        // caller's account (gated on account-level authority, NOT team
        // admin; see `routes::teams::list_archived_teams`'s doc comment).
        // Registered as a literal path ahead of `/teams/{team_id}` in this
        // list purely for readability; axum's router matches static
        // segments before dynamic ones regardless of registration order, so
        // there is no `{team_id}` collision either way.
        .merge(routes::page_read::plain(
            "/teams/archived",
            routes::teams::list_archived_teams,
        ))
        // Account-level membership: canonical paths are `/account/members`
        // (org-team-membership design doc decision #15 — these write
        // `account_members`, not `teams`); `/teams/invite` and
        // `/teams/members/{user_id}` are kept as deprecated compatibility
        // aliases for one release, backed by thin wrapper handlers
        // (`*_legacy`) so OpenAPI can mark them deprecated independently of
        // the canonical routes — see `routes::teams::invite_member_legacy`'s
        // doc comment for why this needs two functions rather than one
        // handler mounted twice.
        .route("/teams/invite", post(routes::teams::invite_member_legacy))
        .route(
            "/teams/members/{user_id}",
            delete(routes::teams::remove_member_legacy),
        )
        .route("/account/members", post(routes::teams::invite_member))
        .route(
            "/account/members/{user_id}",
            delete(routes::teams::remove_member),
        )
        .merge(routes::page_read::plain(
            "/teams/{team_id}/members",
            routes::teams::list_team_members,
        ))
        .route("/teams/{team_id}/members", post(routes::teams::add_member))
        .route(
            "/teams/{team_id}/members/{user_id}",
            delete(routes::teams::remove_team_member).patch(routes::teams::assign_team_member_role),
        )
        .merge(routes::page_read::plain(
            "/teams/{team_id}/events",
            routes::teams::list_team_events,
        ))
        // Org directory — backs the team-roster "who can I add" picker
        // (org-team-membership design doc decision #10). Sibling of the
        // `/teams/*` family but under its own `/orgs` prefix since it isn't
        // team-scoped.
        .route("/orgs/members", get(routes::teams::list_org_members))
        // Org-invite lifecycle (org-invites train PR 1): custom invite
        // system — Clerk stays the identity store, but invitations, email,
        // and lifecycle are ours (the prod Clerk instance has native
        // invitations disabled). See `routes::org_invites`'s own module doc.
        .route("/orgs/invites", post(routes::org_invites::create_invite))
        .route(
            "/orgs/invites/{user_id}/resend",
            post(routes::org_invites::resend_invite),
        )
        .route(
            "/orgs/invites/{user_id}",
            delete(routes::org_invites::revoke_invite),
        )
        // Active-member management (settings launch PR 2.2): remove a member
        // and change their admin/member role, via Clerk's membership API.
        // See `routes::org_members`'s module doc.
        .route(
            "/orgs/members/{user_id}",
            delete(routes::org_members::remove_org_member)
                .patch(routes::org_members::change_org_member_role),
        )
        // Slot pool (CS-435, org-onboarding plan §3 item 7, D1/D7/D15): the
        // admin "buy N slots" action, and the derived purchased/used/
        // reserved/available arithmetic the members page reads back. See
        // `routes::org_slots`'s own module doc for the GET-vs-POST auth
        // split.
        .merge(routes::page_read::plain(
            "/orgs/slots",
            routes::org_slots::get_slots,
        ))
        .route("/orgs/slots", post(routes::org_slots::purchase_slots))
        // CS-444: the reduce half of the founder's "the pool only changes
        // when an admin explicitly adds or reduces slots". A sub-path rather
        // than a `DELETE` on `/orgs/slots`: this is a partial, quantified
        // action with a request body (tier + quantity + idempotency key),
        // not the removal of the collection the GET returns.
        .route("/orgs/slots/reduce", post(routes::org_slots::reduce_slots))
        // Self-serve signup PR C′: "resume payment" for a new team org whose
        // trial Checkout was not completed (24 h window). Names the org in
        // the path and runs on the creator's personal session; see
        // routes::org_trial_checkout's module doc.
        .route(
            "/orgs/{org_id}/trial-checkout",
            post(routes::org_trial_checkout::resume_org_trial_checkout),
        )
        // Self-serve signup PR A2: create a team org (Clerk org, accounts
        // row, admin membership, trial Checkout), poll its setup, and retry
        // a failed setup. Called on the creator's PERSONAL session; none of
        // them skips the binding gate. 404 while ORG_SELFSERVE_SIGNUP_ENABLED
        // is unset. See routes::orgs_create's module doc.
        .route("/orgs", post(routes::orgs_create::create_org))
        .merge(routes::page_read::plain(
            "/orgs/{org_id}/provisioning",
            routes::orgs_create::get_provisioning,
        ))
        .route(
            "/orgs/{org_id}/provisioning/retry",
            post(routes::orgs_create::retry_provisioning),
        )
        // In-app org deletion (control 0387): status, request, undo, and the
        // owner's export during grace. 404 while ORG_DELETION_ENABLED is
        // unset. The auth door lets exactly these four paths through the
        // org-deletion lockout; the handlers admit the owner alone to act.
        // See routes::org_deletion's module doc.
        .merge(routes::page_read::plain(
            "/orgs/{org_id}/deletion",
            routes::org_deletion::get_org_deletion,
        ))
        .route(
            "/orgs/{org_id}/deletion",
            post(routes::org_deletion::post_org_deletion),
        )
        .route(
            "/orgs/{org_id}/deletion/cancel",
            post(routes::org_deletion::post_cancel_org_deletion),
        )
        .route(
            "/orgs/{org_id}/deletion/export",
            get(routes::org_deletion::get_org_deletion_export),
        )
        // Auto-add (CS-447, org-onboarding plan §3 item 9, D1/D11/D12/D16):
        // the admin toggle and the tier it may spend. Admin-only on BOTH
        // verbs — unlike the slot pool's GET, this decides whether
        // strangers join and at what cost. OFF for every org that has never
        // touched it. See `routes::org_auto_add`'s own module doc.
        .route(
            "/orgs/auto-add",
            get(routes::org_auto_add::get_settings).put(routes::org_auto_add::put_settings),
        )
        // D11's "ask once" (CS-449, item 38): the toggle's own follow-up
        // action for a backlog that was already queued when it flipped on.
        // Admin-only, same as the toggle above. See `routes::org_auto_add`'s
        // own module doc, "Item 38 (D11's 'ask once')".
        .route(
            "/orgs/auto-add/queued-preview",
            get(routes::org_auto_add::queued_preview),
        )
        .route(
            "/orgs/auto-add/admit-queued",
            post(routes::org_auto_add::admit_queued),
        )
        // Org domain verification (CS-443, org-onboarding plan §3 item 37,
        // D11): claim a domain, prove it by DNS TXT or by a confirmation
        // link mailed to an address AT that domain, and show which one
        // proved it. Admin-only, including the LIST — a verification token
        // is a credential. The emailed link's own PUBLIC confirm route is
        // mounted on the `open` router below, not here. See
        // `routes::org_domains`' own module doc.
        .merge(routes::page_read::plain(
            "/orgs/domains",
            routes::org_domains::list_domains,
        ))
        .route("/orgs/domains", post(routes::org_domains::claim_domain))
        .route(
            "/orgs/domains/{id}",
            delete(routes::org_domains::remove_domain),
        )
        .route(
            "/orgs/domains/{id}/verify",
            post(routes::org_domains::verify_dns),
        )
        .route(
            "/orgs/domains/{id}/confirmation",
            post(routes::org_domains::request_confirmation),
        )
        // Join-request queue (CS-437, org-onboarding plan §3 items 4/14,
        // D3/D9/D10/D19): a signed-in non-member asks to join an org, an
        // admin approves (Clerk-first, D3) or rejects (silent, durable,
        // D10). See `routes::org_join_requests`'s own module doc for the
        // full route table and why the requester routes run on
        // `UserContext` + the control pool rather than `Tenant`.
        .route("/join-requests", post(routes::org_join_requests::create))
        .merge(routes::page_read::plain(
            "/me/join-requests",
            routes::org_join_requests::list_mine,
        ))
        .merge(routes::page_read::plain(
            "/orgs/join-requests",
            routes::org_join_requests::list,
        ))
        // CS-438 (D13): the members-page badge's own cheap open-count read.
        .merge(routes::page_read::plain(
            "/orgs/join-requests/count",
            routes::org_join_requests::count,
        ))
        .route(
            "/orgs/join-requests/{id}/approve",
            post(routes::org_join_requests::approve),
        )
        .route(
            "/orgs/join-requests/{id}/reject",
            post(routes::org_join_requests::reject),
        )
        // Connector health (CS-446, org-onboarding plan §3 item 16, D16):
        // the admin-facing read of `connector_health` (migration 0243) —
        // today, "the member whose Google grant backed your directory has
        // left, so auto-add now decides on the weaker rung". Read-only by
        // design; see `routes::connector_health`'s own module doc for why
        // there is no dismiss.
        .route(
            "/orgs/connector-health",
            get(routes::connector_health::list),
        )
        // Home dashboard: per-connection health (healthy/stale/failing), last
        // sync and 30-day volume. 404 while HOME_DASHBOARD_ENABLED is unset.
        // See services::connector_status's module doc for the state rule.

        // Activity feed
        .merge(routes::page_read::plain(
            "/activity",
            routes::activity::list_activity,
        ))
        // Home dashboard metrics (KPI tiles, ingested-per-day matrix, oceans by
        // volume). 404 while HOME_DASHBOARD_ENABLED is unset. See
        // services::home_metrics and docs/implementation/home-metrics-api.md.

        // Home launchpad (Continue, What's new, Needs attention): plain
        // index-bounded page reads, never stored models, so never withheld
        // on a fence. 404 while HOME_DASHBOARD_ENABLED is unset. See
        // services::home_launchpad and docs/implementation/home-launchpad-api.md.
        .merge(routes::page_read::plain(
            "/home/continue",
            routes::home_launchpad::get_continue,
        ))
        .merge(routes::page_read::plain(
            "/home/whats-new",
            routes::home_launchpad::get_whats_new,
        ))
        .merge(routes::page_read::plain(
            "/home/attention",
            routes::home_launchpad::get_attention,
        ))
        // Home dashboard "From your workspaces": recent activity per shared
        // ocean. 404 while HOME_DASHBOARD_ENABLED is unset. See
        // services::workspace_activity and
        // docs/implementation/workspace-activity-api.md.

        // Today-journal read endpoint (Today-journal PR C, stacked on PR A's
        // `journal_rollups` migration/Store layer and PR B's compile worker).

        // Chat find: hidden Cmd-K title-only source search (flag
        // CHAT_FIND_RESULTS_ENABLED, 404 while unset). The chat tiles ride on
        // the scoped answer. See services::find_results.
        .route(
            "/find/titles",
            get(routes::find_results::title_search).layer(axum::middleware::from_fn(library_read::no_store)),
        )
        // Today activity cards + detail panel (flag TODAY_ACTIVITIES_ENABLED,
        // 404 while unset). See services::journal_activities.

        .route(
            "/journal/activities/{key}",
            get(routes::journal_activities::get_activity).layer(axum::middleware::from_fn(library_read::no_store)),
        )
        // Today-journal hour drill-down (ticket 16) — the uncapped "everything
        // this hour" view GET /v1/journal's own aggregate/rollup summaries
        // link out to.
        .route(
            "/journal/hour/{hour_start}/items",
            get(routes::journal::journal_hour_items),
        )
        // Today workstream gist (flag TODAY_GIST_ENABLED, 404 while unset).
        // See services::journal_gist.

        // Admin: the former `/admin/users` 501 stub (no role check at all) was
        // deleted (platform-admin-metrics PR 6 of 7) — admin/operator surfaces
        // now live exclusively on `internal_app()`'s `/internal/admin/*`
        // routes, behind `X-Internal-Token`, never on this authenticated-but-
        // unprivileged `/v1` router.
        // Slack integrations (OAuth + management)
        .route(
            "/integrations/slack/oauth/start",
            get(routes::slack::oauth_start),
        )
        .route(
            "/integrations/slack/oauth/callback",
            get(routes::slack::oauth_callback),
        )
        .route("/integrations/slack", get(routes::slack::list_integrations))
        .route(
            "/integrations/slack/{workspace_id}",
            delete(routes::slack::delete_integration),
        )
        // OAuth connections (authenticated — manage connected providers)
        // Mint a signed state token (CS-110) before starting a connect flow.
        .route("/oauth/state", get(routes::oauth::mint_state))
        .merge(routes::page_read::plain(
            "/oauth/connections",
            routes::oauth::list_connections,
        ))
        .route(
            "/oauth/connections/{provider}",
            axum::routing::delete(routes::oauth::delete_connection),
        )
        // Connector compliance audit log (m365 enterprise train PR 4.2,
        // plan decision #11 part 1) — org-admin-only read, mirroring
        // `/query-audit`'s shape.
        .route(
            "/connector-audit",
            get(routes::connector_audit::list_connector_audit),
        )
        // Calendar read API (cs-uxp4a-calendar): upcoming events for the
        // pre-meeting nudge + title/roster attachment, connection status, and the
        // pre-meeting reminder toggle. Connect/disconnect is the generic OAuth
        // path above (provider "google").
        .route("/calendar/upcoming", get(routes::calendar::upcoming))
        .route("/calendar/status", get(routes::calendar::status))
        .route(
            "/calendar/reminders",
            axum::routing::put(routes::calendar::set_reminders),
        )
        // The generic connector catalog (connectors redesign B1). Axum prefers
        // retained static `/connectors/slack/...` paths
        // below to the `{provider}` capture, so every retained route keeps its
        // handler; `v1_routes_keep_their_handlers_beside_the_catalog` pins that.

        .route(
            "/connectors/{provider}",
            delete(routes::connector_catalog::disconnect),
        )
        .route(
            "/connectors/{provider}/captures",
            get(routes::connector_catalog::captures),
        )
        .route(
            "/connectors/{provider}/recover",
            post(routes::connector_catalog::recover),
        )
        .route(
            "/connectors/{provider}/connect",
            post(routes::connector_catalog::connect_start),
        )
        .route(
            "/connectors/{provider}/connect/complete",
            post(routes::connector_catalog::connect_complete),
        )
        .route(
            "/connectors/{provider}/sources/{source}",
            axum::routing::patch(routes::connector_catalog::patch_source),
        )
        .route(
            "/connectors/{provider}/sources/{source}/scope-items",
            get(routes::connector_catalog::scope_items),
        )
        .route(
            "/connectors/{provider}/sources/{source}/scope",
            axum::routing::put(routes::connector_catalog::put_scope),
        )
        .route(
            "/connectors/{provider}/sources/{source}/backfill",
            post(routes::connector_catalog::backfill),
        )
        // Connector pull-sync ("sync now", self-owned data e.g. calendar).
        .route(
            "/connectors/{provider}/sync",
            post(routes::connectors::trigger_sync),
        )
        // Explicit per-item opt-in ingest (e.g. "Ingest this thread" — CS-116).
        .route(
            "/connectors/{provider}/ingest",
            post(routes::connectors::ingest_selected),
        )
        // Provider-generic watched-resource status/toggle/list (gmail-options
        // backend train, PR 1 of 4): backs the Gmail add-on's saved-state
        // check, the follow toggle both the add-on and the Outlook add-in
        // use, and the web app's saved-threads list. See
        // `routes::connectors`'s own module doc for the wire contract.
        .route(
            "/connectors/{provider}/watched",
            get(routes::connectors::list_watched),
        )
        .merge(routes::page_read::plain(
            "/connectors/{provider}/watched/{thread_id}",
            routes::connectors::get_watched,
        ))
        .route(
            "/connectors/{provider}/watched/{thread_id}",
            put(routes::connectors::put_watched).delete(routes::connectors::delete_watched),
        )
        // Gmail Add-on pairing — the AUTHENTICATED half (`routes::addin`'s
        // module doc). `pair/approve` is called from the web approval page
        // with a normal Clerk bearer; the add-on itself never calls it.
        .route("/addin/pair/approve", post(routes::addin::pair_approve))
        // Microsoft add-in pairing (`routes::addin_microsoft`): the approve
        // page's workspace picker reads this (Clerk-authenticated), and the
        // three link routes are authenticated by the add-in's own `csms_`
        // session credential (they 403 for any other bearer shape). Teams and
        // Outlook share this ONE link.
        .merge(routes::page_read::plain(
            "/addin/pair/info",
            routes::addin_microsoft::pair_info,
        ))
        .merge(routes::page_read::plain(
            "/addin/microsoft/link",
            routes::addin_microsoft::get_link,
        ))
        .route(
            "/addin/microsoft/link",
            delete(routes::addin_microsoft::disconnect_link).post(routes::addin_microsoft::link),
        )
        // Workspace display name for the add-ins / web header: any Clerk
        // session, `csms_`, or add-in OAuth token (see `shared_scope_allows`).
        .merge(routes::page_read::plain(
            "/addin/workspace",
            routes::addin_microsoft::get_workspace,
        ))
        .route(
            "/addin/microsoft/link/workspace",
            put(routes::addin_microsoft::change_workspace),
        )
        // List the caller's own live add-on tokens — what the personal
        // `/connectors` page renders a per-row Revoke button from. Same
        // owner-scoping (from `UserContext`, never a parameter) and the same
        // personal-plane-only 400 as `pair/approve`; see the handler's own
        // doc comment.
        .merge(routes::page_read::plain(
            "/addin/tokens",
            routes::addin::list_tokens,
        ))
        // Revoke one of the caller's own add-on tokens (owner-scoped inside
        // the handler — see its own doc comment).
        .route("/addin/tokens/{id}", delete(routes::addin::revoke_token))
        // Personal Slack onboarding/history now use the catalog.
        // Per-channel Slack management (slack-onboarding PR follow-up): the
        // read-only detail page behind a connected Slack workspace tile —
        // "what is CoralSwarm actually reading, where does each channel
        // land, and are huddles being saved there". See
        // `routes::slack::channel_management`'s doc comment.
        .route(
            "/connectors/slack/channels",
            get(routes::slack::channel_management),
        )
        // Per-channel Slack management — WRITE half (wt-slack-actions):
        // change destination, stop watching, and the impact preview that
        // backs both confirmation dialogs. See
        // `routes::slack::set_channel_destination`'s doc comment for the
        // full contract.
        .route(
            "/connectors/slack/channels/{channel_id}/destination",
            axum::routing::put(routes::slack::set_channel_destination),
        )
        .route(
            "/connectors/slack/channels/{channel_id}",
            axum::routing::delete(routes::slack::stop_watching_channel),
        )
        .route(
            "/connectors/slack/channels/{channel_id}/impact",
            get(routes::slack::channel_impact),
        )
        // Google Chat: the watched-space picker (gsuite Train G4 PR 3 —
        // design doc §3.2/§5.5). `google` is a *surface* provider ("chat"),
        // not a new top-level one — path namespaced under
        // `/connectors/google/chat/...` for the same "provider-specific
        // handlers stay in their provider's own module, only the path is
        // namespaced" convention the Slack routes above and the Teams
        // routes below both follow. See `routes::google_chat`'s doc
        // comment for the full contract.
        .route(
            "/connectors/google/chat/spaces",
            get(routes::google_chat::list_spaces),
        )
        .route(
            "/connectors/google/chat/spaces/{space_id}/destination",
            post(routes::google_chat::set_space_destination),
        )
        // The MAILBOX archetype's opt-in (gsuite Train G4 PR 5 / G4.5 —
        // `docs/connectors/google-workspace.md` §9): `GROUP_CHAT`/
        // `DIRECT_MESSAGE` spaces have no destination to pick (content
        // always lands in the caller's own private ocean), so this is a
        // single POST-to-opt-in / DELETE-to-opt-out pair rather than the
        // container archetype's destination route.
        .route(
            "/connectors/google/chat/spaces/{space_id}/optin",
            post(routes::google_chat::opt_in_to_mailbox_space)
                .delete(routes::google_chat::opt_out_of_mailbox_space),
        )
        // GitHub connector (PR A, sync plumbing — docs/connectors/github.md).
        // Connect/disconnect is the generic OAuth path above (provider
        // "github"); these are the selection/management surface, mirroring
        // the Slack/Chat provider-specific-module-under-`/connectors`-path
        // convention every block above and below this one follows.
        .route(
            "/connectors/github/installs",
            get(routes::github::list_installs),
        )
        .route(
            "/connectors/github/installs/{installation_id}",
            axum::routing::delete(routes::github::delete_install),
        )
        .route(
            "/connectors/github/repos",
            get(routes::github::list_repos).put(routes::github::put_repos),
        )
        // Outlook ingest workflows (auto-ingest rules — CS-118). Microsoft-only
        // today; the path carries {provider} for symmetry but handlers are
        // Microsoft-specific.
        .route(
            "/connectors/microsoft/workflows",
            post(routes::workflows::create_workflow).get(routes::workflows::list_workflows),
        )
        .route(
            "/connectors/microsoft/workflows/{id}",
            axum::routing::patch(routes::workflows::update_workflow)
                .delete(routes::workflows::delete_workflow),
        )
        // Microsoft Teams ingest (CS-119): list + per-resource opt-in + backfill.
        .route(
            "/connectors/microsoft/teams/onboarding",
            post(routes::teams_onboarding::onboard),
        )
        .route(
            "/connectors/microsoft/teams/transcripts/sync",
            post(routes::teams_ingest::sync_transcripts),
        )
        .route(
            "/connectors/microsoft/teams/transcripts/sync-meeting",
            post(routes::teams_ingest::sync_meeting_transcripts),
        )
        .route(
            "/connectors/microsoft/teams/transcripts/connect",
            post(routes::teams_ingest::connect_meeting_capture),
        )
        .route(
            "/connectors/microsoft/teams/transcripts/meeting-status",
            post(routes::teams_ingest::meeting_capture_status),
        )
        .route(
            "/connectors/microsoft/teams/transcripts/meeting-context",
            post(routes::teams_ingest::resolve_meeting_context),
        )
        .route(
            "/connectors/microsoft/teams/transcripts/capture",
            post(routes::teams_ingest::capture_meeting_transcripts)
                .delete(routes::teams_ingest::revoke_meeting_transcripts),
        )

        // Outlook compose capture v2: capture the meeting being scheduled
        // (docs/plans/outlook-compose-capture-v2.md). Ordinary /v1 auth (Clerk or
        // the Microsoft SSO `csms_` credential, see `addin_microsoft::scope_allows`).
        .route(
            "/outlook-capture-schedule/intents",
            post(routes::outlook_schedule::create_intent),
        )
        .merge(routes::page_read::plain(
            "/outlook-capture-schedule/intents/{intent_id}",
            routes::outlook_schedule::get_intent,
        ))
        .route(
            "/outlook-capture-schedule/intents/{intent_id}",
            delete(routes::outlook_schedule::delete_intent),
        )
        .merge(routes::page_read::plain(
            "/outlook-capture-schedule/by-nonce/{nonce}",
            routes::outlook_schedule::by_nonce,
        ))
        .merge(routes::page_read::plain(
            "/outlook-capture-schedule/by-event",
            routes::outlook_schedule::by_event,
        ))
        // Teams app (RSC) install — owner added CoralSwarm to a channel/chat (CS-121).
        .route(
            "/connectors/microsoft/teams/install",
            post(routes::teams_ingest::install_resource),
        )
        // Outlook everything-mode opt-in (m365 enterprise train PR 3.4):
        // owner-self-service only — see `routes::outlook_everything_mode`'s
        // own doc comment for the contract §7 carve-out this enforces.
        .route(
            "/connectors/microsoft/everything-mode",
            post(routes::outlook_everything_mode::set_everything_mode_route)
                .get(routes::outlook_everything_mode::get_everything_mode_route),
        )
        // Gmail everything-mode opt-in (gsuite enterprise train PR G2.4):
        // owner-self-service only — see `routes::gmail_everything_mode`'s
        // own doc comment for the contract §7 carve-out this enforces (the
        // FIRST implementation of this carve-out, m365 PR 3.4's `/connectors/
        // microsoft/everything-mode` above, is the precedent this route
        // mirrors).
        .route(
            "/connectors/google/gmail/everything-mode",
            post(routes::gmail_everything_mode::set_everything_mode_route)
                .get(routes::gmail_everything_mode::get_everything_mode_route),
        )
        // Google Meet per-meeting opt-in (founder decision 2026-10-06): a
        // member allows INDIVIDUAL meetings; the sweep ingests nothing else.
        // Clerk-session only for now; see `routes::google_meet`'s module doc
        // (TODO(meet-addin): add to the add-in allowlist later).
        .route(
            "/connectors/google/meet/meetings",
            get(routes::google_meet::list_meetings),
        )
        .route(
            "/connectors/google/meet/meetings/{conference_record_id}/allow",
            post(routes::google_meet::allow_meeting).delete(routes::google_meet::revoke_meeting),
        )
        .merge(routes::page_read::plain(
            "/connectors/google/meet/captures",
            routes::google_meet::list_captures,
        ))
        .route(
            "/connectors/google/meet/captures",
            post(routes::google_meet::pre_approve),
        )
        .route(
            "/connectors/google/meet/captures/{capture_id}",
            axum::routing::delete(routes::google_meet::revoke_capture),
        )
}

/// Whether `raw_origin` is a plain `http://localhost:<port>` or
/// `http://127.0.0.1:<port>` origin (any port, no path) — the general "some
/// local dev server" shape (`next dev` on a non-default port, Vite, Storybook,
/// …). Only consulted by [`is_allowed_cors_origin`] when `is_dev`.
fn is_localhost_dev_origin(raw_origin: &str) -> bool {
    for prefix in ["http://localhost:", "http://127.0.0.1:"] {
        if let Some(port) = raw_origin.strip_prefix(prefix) {
            if !port.is_empty() && port.bytes().all(|b| b.is_ascii_digit()) {
                return true;
            }
        }
    }
    false
}

/// Cloud CORS origin policy (org-tenancy PR 3.3): whether `raw_origin` (an
/// `Origin` request header value, e.g. `https://acme.coralswarm.example`) may be
/// granted `Access-Control-Allow-Origin` on the public cloud router.
///
/// Only called once an `apex` is actually configured (`TENANT_APEX_DOMAIN`
/// set) — see [`public_cors_layer`] for the flag-off (`apex` unconfigured)
/// case, which keeps today's permissive behavior unchanged.
///
/// Allowed:
/// * [`is_localhost_dev_origin`] — any PLAIN-`http://` `localhost`/
///   `127.0.0.1` port, only when `is_dev` (a local Next.js/Vite/etc. dev
///   server on a non-default port; dev servers are never served over TLS).
/// * `https://` + either apex itself or its `app.`/`www.` hosts — using the
///   EXACT SAME [`tenancy::gate::classify_host`] the binding gate uses to
///   route a request, so an arbitrary site is never a valid CORS origin.
///   **Org subdomains are NOT accepted** (claim-based tenancy PR 1.3): no
///   browser page is served from one any more, so an `Origin` claiming to be
///   a tenant subdomain is a stale client or a probe.  `https://` is REQUIRED here (P2 hardening,
///   security review of org-tenancy PR 3.3): a same-host but
///   scheme-downgraded `http://acme.<apex>` origin is rejected, matching this
///   doc's stated https-only policy for real apex/subdomain traffic — the
///   dev-localhost branch above is the only origin allowed to use a
///   non-`https` scheme, and it is an enumerated exception, not a generic
///   "any scheme" fallback.
///
/// **Two apexes, still deliberately.** `api_apex` (`TENANT_APEX_DOMAIN`) is
/// where the request is ROUTED; `web_apex` (`WEB_APEX_DOMAIN`, defaulting to
/// `api_apex`) is where the browser page it came FROM is served. Those differ
/// — UI at `coralswarm.example`, API at `api.coralswarm.example` — so classifying an
/// `Origin` against the API apex alone would make `https://app.coralswarm.example`
/// `Foreign` and reject every real user. An origin is accepted when it is the
/// `Apex` of EITHER. This widens the CORS surface by exactly one additional
/// operator-configured domain and its `app.`/`www.` labels, nothing more.
/// This predicate is independent of tenant ROUTING and doesn't change it:
/// [`tenancy::runtime::TenantRuntime::precheck_host`] still classifies the
/// request `Host` (and, for a subdomain, resolves its routing row) exactly
/// as before this PR — only which `Origin` header values may read a
/// cross-origin response is affected here.
///
/// This layer stays bearer-only / credential-less (`app()` never sets
/// `allow_credentials`), so an allowed Origin can only ever read a response
/// whose request ALSO carried a valid Bearer token — this predicate is
/// defense-in-depth against a hostile page's cross-origin `fetch`, not the
/// authorization boundary itself (that's `require_auth` + the binding gate).
fn is_allowed_cors_origin(raw_origin: &str, api_apex: &str, web_apex: &str, is_dev: bool) -> bool {
    if is_dev && is_localhost_dev_origin(raw_origin) {
        return true;
    }
    // Real apex/subdomain traffic must be `https://` — see the doc comment's
    // P2 hardening note. Any other scheme (including plain `http://` against
    // a valid apex host) is rejected here, before `classify_host` ever runs.
    let Some(host) = raw_origin.strip_prefix("https://") else {
        return false;
    };
    // Claim-based tenancy PR 1.3: `Apex` ONLY. This used to accept anything
    // that was not `Foreign`, which included every `<slug>.<apex>` subdomain.
    // No browser page is served from a tenant subdomain any more, so an
    // origin claiming to be one is either a stale client or someone probing.
    // Matching the exact variant rather than negating `Foreign` also means a
    // future `HostTarget` variant cannot widen this by default — it has to be
    // added here on purpose.
    [api_apex, web_apex].iter().any(|apex| {
        matches!(
            tenancy::gate::classify_host(host, apex),
            tenancy::gate::HostTarget::Apex
        )
    })
}

/// Build the public cloud router's CORS layer.
///
/// Flag-aware (org-tenancy PR 3.3): with no [`tenancy::runtime::TenantRuntime`]
/// configured (`TENANT_APEX_DOMAIN` unset — today's single-domain deploy),
/// this returns EXACTLY today's policy (`allow_origin(Any)`) — byte-identical
/// until the tenancy apex is actually configured, per this PR's flag-aware
/// requirement. Once an apex is known, origins are restricted by
/// [`is_allowed_cors_origin`] regardless of `MULTITENANT_ENABLED` /
/// `org_selfserve_disabled` — CORS scoping is a strictly-more-restrictive,
/// browser-side control with no server-side authorization consequence (the
/// real enforcement is `require_auth` + the binding gate), so there is no
/// reason to gate it further behind those request-routing flags.
///
/// Both apexes come from the runtime: `apex` (`TENANT_APEX_DOMAIN`, the API
/// domain that routes) and `web_apex` (`WEB_APEX_DOMAIN`, the UI domain that
/// origins come from, defaulting to `apex`) — see [`is_allowed_cors_origin`].
fn public_cors_layer(state: &AppState) -> CorsLayer {
    let allow_origin = match state
        .tenant_runtime
        .as_ref()
        .map(|rt| (rt.apex.clone(), rt.web_apex.clone()))
    {
        Some((api_apex, web_apex)) => {
            // Fail CLOSED (P2 hardening, security review of org-tenancy PR
            // 3.3): dev-permissive CORS (the localhost/127.0.0.1 allowance in
            // `is_allowed_cors_origin`) requires an EXPLICIT dev signal —
            // `state.env.is_dev` (`APP_ENV=development`, the value
            // `.env.example` sets for local dev, resolved ONCE at boot by
            // `AppEnv::resolve` — see that type's doc comment) — never the
            // inverse ("anything that isn't literally 'production'"). That
            // inverse is what `app()`'s own `is_production`/`dev_routes` uses
            // for the OpenAPI/Swagger gate, which is fine for exposing docs
            // but WRONG here: it would mean a prod deploy that simply forgets
            // to set `APP_ENV` (or a mistyped value) silently opens localhost
            // CORS. `fly.toml`/`fly.staging.toml` both set
            // `APP_ENV=production` explicitly (staging included), so this
            // never accidentally fires in a real deployment either way.
            let is_dev = state.env.is_dev;
            AllowOrigin::predicate(move |origin, _parts| {
                origin
                    .to_str()
                    .map(|o| is_allowed_cors_origin(o, &api_apex, &web_apex, is_dev))
                    .unwrap_or(false)
            })
        }
        None => AllowOrigin::any(),
    };
    CorsLayer::new()
        .allow_origin(allow_origin)
        .allow_methods(Any)
        .allow_headers(Any)
        // `allow_headers` doesn't cover response headers, so `GET /v1/nodes`'s `X-Has-More`
        // (CS-reef-pagination) needs an explicit exposure or `web/lib/api.ts`
        // can't read it.
        .expose_headers([
            axum::http::HeaderName::from_static("x-has-more"),
            axum::http::HeaderName::from_static("server-timing"),
        ])
}

/// Public-facing router: open routes + authenticated API routes.
/// Bound to `0.0.0.0:PORT` (configurable via `PORT` env var, default 8080).
fn app(state: Arc<AppState>) -> Router {
    // Open routes: no authentication required.
    let open = Router::new()
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        // Unified Slack Event API endpoint — the single Request URL. Handles
        // message ingestion, huddle reaction opt-in/out, and
        // member_joined_channel backfill prompts. Signature verified inside.
        .route("/v1/slack/events", post(routes::slack_events::slack_events))
        // Slack Block Kit interactivity (backfill button choices). Signature
        // verified inside the handler.
        .route(
            "/v1/slack/interactivity",
            post(routes::slack_interactivity::slack_interactivity),
        )
        // Clerk Organizations webhook — public (Svix signature verified inside).
        .route(
            "/v1/webhooks/clerk",
            post(routes::clerk_webhook::clerk_webhook),
        )
        // Stripe webhook — the renewal clock (Phase 4a). Public by necessity:
        // Stripe holds no Clerk session, so the `Stripe-Signature` HMAC is the
        // ENTIRE authentication boundary. Verified inside the handler, which
        // also enforces the 300s replay window and per-event idempotency.
        .route(
            "/v1/stripe/webhook",
            post(routes::stripe_webhook::stripe_webhook),
        )
        // Microsoft Graph change-notifications — public (validation-token echo).
        // The per-subscription clientState is verified at the route, against
        // the control plane, before any receive job is written
        // (routes::graph_webhook::route_secrets), and checked again by the
        // receive worker on the row it acts on. Backs Outlook reply
        // auto-capture (CS-117).
        .route(
            "/v1/webhooks/graph",
            post(routes::graph_webhook::graph_notifications),
        )
        // Google Cloud Pub/Sub push — public (Google-signed OIDC bearer
        // token verified inside). Backs Gmail `users.watch`, Chat and Meet
        // push notifications, held as receive jobs (ADR-0016) — see
        // routes::google_push's module doc.
        .route(
            "/v1/webhooks/google",
            post(routes::google_push::google_push),
        )
        // Google Calendar `events.watch` channel notifications — public
        // (per-channel `X-Goog-Channel-Token` verified inside, constant-time;
        // a SEPARATE trust boundary from the Pub/Sub route above — Calendar
        // pushes directly to us with no OIDC bearer at all). Backs Calendar
        // event ingest (gsuite Train G3 PR 1). See
        // routes::google_calendar_push's module doc.
        .route(
            "/v1/webhooks/google/calendar",
            post(routes::google_calendar_push::google_calendar_push),
        )
        // Microsoft Teams bot messaging endpoint — public (Bot Framework JWT
        // verified inside). Presence-driven ingestion: adding CoralSwarm to a
        // channel/group chat starts ingest; removing it stops (CS-122).
        .route("/v1/webhooks/teams", post(routes::teams_bot::teams_bot))
        // GitHub App webhook — public (`X-Hub-Signature-256` HMAC verified
        // inside, same posture as `/v1/slack/events`). docs/connectors/
        // github.md §3.7.
        .route(
            "/v1/github/events",
            post(routes::github::github_events).layer(DefaultBodyLimit::max(
                routes::github::MAX_WEBHOOK_BODY_BYTES,
            )),
        )
        // OAuth authorize + callback are browser-redirect flows — no Bearer token.
        .route(
            "/v1/oauth/{provider}/authorize",
            get(routes::oauth::authorize),
        )
        // Org-wide admin consent (tenant admin authorizes the whole company).
        // Returns a normal code to the standard callback below.
        .route(
            "/v1/oauth/{provider}/admin-consent",
            get(routes::oauth::admin_consent),
        )
        .route(
            "/v1/oauth/{provider}/callback",
            get(routes::oauth::callback),
        )
        // Public (no-auth) share-link viewer (cs-uxp3b-library-search):
        // `'link'`-visibility shares only, rate-limited per token inside the
        // handler. The AUTHED counterpart (`'link'` + `'team'`) is
        // `GET /v1/meetings/shares/view/{token}` in `v1_routes()`.
        .merge(routes::page_read::plain_open(
            "/v1/shared/{token}",
            routes::shares::public_share,
        ))
        // Public (no-auth) coding-session share-link viewer (coding-session
        // capture Phase 5, audit A7): `'link'`-visibility shares only,
        // rate-limited per token inside the handler. A distinct
        // `/v1/shared/sessions/{token}` path (not a second handler on
        // `/v1/shared/{token}`) — axum's router prefers this more specific
        // static segment over the meeting-share route's `{token}` wildcard,
        // so both coexist with no ambiguity. No authed counterpart in this
        // phase (see `routes::session_shares`'s header comment).
        .merge(routes::page_read::plain_open(
            "/v1/shared/sessions/{token}",
            routes::session_shares::public_session_share,
        ))
        // Gmail Add-on pairing handshake — both UNAUTHENTICATED by design
        // (see `routes::addin`'s module doc): `pair/start` runs before the
        // add-on has any CoralSwarm identity at all, and `pair/poll` is
        // gated on its own `poll_secret` rather than a Bearer token. The
        // AUTHENTICATED half of the flow (`pair/approve`, and token revoke)
        // lives in `v1_routes()` below, alongside every other Clerk-gated
        // route.
        .route("/v1/addin/pair/start", post(routes::addin::pair_start))
        .route("/v1/addin/pair/poll", post(routes::addin::pair_poll))
        // Microsoft SSO exchange for the Teams / Outlook panes — UNAUTHENTICATED
        // by design: the Microsoft access token in the body IS the credential,
        // validated against Microsoft's JWKS inside the handler
        // (`routes::addin_microsoft`'s module doc). Feature-flagged off by
        // default (`ADDIN_MICROSOFT_SSO_ENABLED`).
        .route("/v1/addin/microsoft/session", post(routes::addin_microsoft::session))
        // Settings launch PR 2.4: cancel a pending account deletion from the
        // signed link in the email. No session: the user is locked out of
        // normal auth during grace; the HMAC token is the credential.
        .route(
            "/v1/account-deletion/cancel",
            post(routes::account_deletion::post_cancel_by_token),
        )
        // One-click unsubscribe (RFC 8058) for the unmatched-member nudge
        // email (`services::unmatched_recipient_email`). Public by
        // necessity — the recipient has no CoralSwarm account, let alone a
        // session, which is the entire premise of that email. GET is the
        // visible in-body link a human clicks; POST is the target a
        // compliant mail provider submits on the recipient's behalf with no
        // session at all (`List-Unsubscribe-Post: List-Unsubscribe=
        // One-Click`). See `routes::email_unsubscribe`'s own module doc for
        // the consumer-plane-only scope this shares with `public_share`.
        .route(
            "/v1/email/unsubscribe",
            get(routes::email_unsubscribe::unsubscribe_get)
                .post(routes::email_unsubscribe::unsubscribe_post),
        )
        // Signup-prefill token resolver (CodeRabbit round 1, #1005): the
        // nudge email's signup link carries an opaque token, never the raw
        // address — see `routes::email_prefill`'s own module doc. Public for
        // the identical reason the unsubscribe route above is: the caller
        // has no CoralSwarm account and therefore no session.
        .merge(routes::page_read::plain_open(
            "/v1/email/prefill",
            routes::email_prefill::resolve_prefill,
        ))
        // Domain-confirmation link (CS-443, item 37, D11's second proof).
        // Public by necessity — the person who controls `postmaster@acme.com`
        // may not be a CoralSwarm admin, or a CoralSwarm user at all, which
        // is the entire premise of mailing them. GET renders a form and
        // VERIFIES NOTHING (mail-security link scanners fetch every URL in a
        // message body; a GET that verified would hand them the proof), POST
        // is what a human submits. Same split, same reason, as the
        // unsubscribe pair above. See `routes::org_domains`' own module doc.
        .route(
            "/v1/orgs/domains/confirm",
            get(routes::org_domains::confirm_get).post(routes::org_domains::confirm_post),
        );

    // v1 API routes: the shared route list (see `v1_routes`), protected here by
    // the Postgres-backed Bearer-token middleware.
    //
    // W2 admission control is layered INSIDE `require_auth`. Layer order
    // matters and is easy to get backwards: `.layer(A).layer(B)` makes B the
    // OUTERMOST wrapper, so listing admission first and auth second means auth
    // runs first — which is required, because admission reads the
    // `UserContext` and `TenancyDecision` that `require_auth` stamps onto the
    // request. Reversed, it would see neither and silently stand down on every
    // request.
    //
    // W6's RED metrics sit BETWEEN auth and admission. That is the only
    // placement that records what actually happened: outside admission, so a
    // 429/503 it produced is counted as a real outcome rather than dropped
    // invisibly; inside auth, so an unauthenticated probe cannot create
    // series. Innermost-listed layer runs last, so the order below reads
    // bottom-up as auth → metrics → admission → handler.
    let v1 = v1_routes().route("/embed", post(routes::embed::embed_texts));
    // Behind the same auth/admission layers as every other /v1 route.
    #[cfg(feature = "e2e")]
    let v1 = v1
        .route("/e2e/seed/extraction", post(e2e_seed::seed_extraction))
        .route("/e2e/slow", get(e2e_seed::slow_work))
        .route("/e2e/read-model-computes", get(e2e_seed::read_model_computes));
    let v1 = v1
        // `csms_` add-in session scope (`routes::addin_microsoft::session_scope`):
        // a no-op for every request that did not authenticate with one.
        .layer(axum_middleware::from_fn(
            routes::addin_microsoft::session_scope,
        ))
        .layer(axum_middleware::from_fn_with_state(
            state.clone(),
            admission::admission_control,
        ))
        .layer(axum_middleware::from_fn_with_state(
            state.clone(),
            telemetry::http_metrics,
        ))
        // Directly inside `require_auth`: the `auth` phase of Server-Timing
        // ends where this runs.
        .layer(axum_middleware::from_fn(request_timing::mark_authenticated))
        .layer(axum_middleware::from_fn_with_state(
            state.clone(),
            middleware::auth::require_auth,
        ))
        // Private responses, including admission/auth errors and SSE, remain
        // live reads. Public/static destinations use their existing policy.
        .layer(axum_middleware::from_fn(library_read::no_store));

    // OpenAPI spec — only exposed outside production.
    // SwaggerUi registers both `/swagger-ui/` and the spec URL `/openapi.json`.
    // Resolved ONCE at boot (`AppEnv::resolve`, called from `run()`) and
    // carried on `state.env` — see `AppEnv`'s doc comment for why this no
    // longer reads `std::env::var("APP_ENV")` directly here.
    let is_production = state.env.is_production;
    let dev_routes = if !is_production {
        Router::new().merge(
            utoipa_swagger_ui::SwaggerUi::new("/swagger-ui")
                .url("/openapi.json", openapi::ApiDoc::openapi()),
        )
    } else {
        Router::new()
    };

    let router = Router::new().merge(open).nest("/v1", v1).merge(dev_routes);

    // Remote MCP server (docs/plans/remote-mcp-v1.md). Resolved once per
    // `app()` call — NOT per-request — from the environment; `None` (flag off,
    // or flag on but `CLERK_ISSUER` unset) mounts nothing, so `/mcp` and the
    // `/.well-known/oauth-*` routes 404 exactly as today. `internal_app` never
    // sees this — MCP is a public-cloud-only surface.
    //
    // The mcp sub-router carries its own state type (`mcp::McpState`, not
    // `Arc<AppState>` — it also needs the resolved `McpConfig` and an
    // injectable token validator), so it can't be `.merge()`d directly the way
    // `dev_routes` is (that only works because `dev_routes`'s `Router<S>` gets
    // unified with this function's `S` by inference at the call site, not
    // across a function boundary with a fixed concrete return type). Instead
    // it's built fully self-contained (its own `.with_state()` already
    // applied) and mounted as an opaque `tower::Service` at each of its exact
    // paths via `route_service` — the same pattern the real `rmcp`
    // streamable-HTTP transport will use for `/mcp` itself in Phase 2
    // (`nest_service`, per the module's architecture note).
    let router = match mcp::McpConfig::from_env() {
        Some(cfg) => {
            let mcp_service = mcp::mcp_router(state.clone(), cfg);
            router
                .route_service("/mcp", mcp_service.clone())
                .route_service("/.well-known/oauth-protected-resource", mcp_service.clone())
                .route_service(
                    "/.well-known/oauth-protected-resource/mcp",
                    mcp_service.clone(),
                )
                .route_service("/.well-known/oauth-authorization-server", mcp_service)
        }
        None => router,
    };

    router
        // The retired org API address (claim-based tenancy Phase 4 cutover
        // step 3). Added FIRST so it ends up the innermost of the four layers
        // below and therefore the outermost thing any request sees before it
        // reaches a route: `/mcp`, `/.well-known/oauth-*`, the open webhook
        // routes and `/v1` all sit inside it. That placement is the whole
        // reason it is a layer rather than a gate arm: `tenancy::gate` only
        // sees routes behind `require_auth` and `/mcp`'s own auth, so the open
        // routes and the PRM documents that tell an MCP client this resource
        // exists would otherwise keep answering on a retired address. It is
        // also why the gate's own `Subdomain` arm is left ALONE here — deleting
        // it is cutover step 4, a separate, not-yet-authorized change. Nothing
        // in production reaches that arm anymore, from either surface: this
        // guard already 410s every REST path on a retired host before the
        // gate ever runs, and claim-based-tenancy §Phase 3 PR 3.3 retired
        // `/mcp`'s OWN, independent org-host classification the same way
        // (`mcp::McpConfig::resolve_resource`'s `Subdomain` arm now collapses
        // to `personal_identity`) — so `/mcp` no longer needs, and no longer
        // gets, an exemption from this guard either. (There used to be one,
        // `RETIRED_ORG_HOST_MCP_EXEMPT_PATHS`, held open exactly until
        // whichever of this guard or PR 3.3 landed second — that has now
        // happened, and the exemption is deleted.)
        //
        // It is still inside `request_id_and_latency`, so every 410 lands in
        // the access log with its path and latency.
        .layer(axum_middleware::from_fn_with_state(
            state.clone(),
            retired_org_host_guard,
        ))
        // TraceLayer emits span-level tracing for every request (pairs well
        // with the JSON subscriber for structured output).
        .layer(
            TraceLayer::new_for_http()
                .make_span_with(|_req: &Request<_>| tracing::info_span!("http_request")),
        )
        .layer(axum_middleware::from_fn(request_id_and_latency))
        .layer(public_cors_layer(&state))
        .layer(axum_middleware::map_response(request_timing::timing_allow_origin))
        .with_state(state)
}

/// Answer 410 on `<slug>.<apex>` — the retired org API address (claim-based
/// tenancy Phase 4 **cutover step 3**, `docs/plans/claim-based-tenancy.md`).
///
/// **This is step 3, not step 4.** Step 3 stops SERVING the address; step 4
/// deletes `tenancy::gate`'s `Subdomain` arm. They are separate because that
/// arm is still the only thing standing between `/mcp`'s org resource and a
/// tenant pool, and MCP's own host classification belongs to Phase 3's PR 3.3
/// (§Phase 3, correction 2). So this guard sits OUTSIDE the gate, as the plan
/// specifies for the sibling web redirect — *"purely host-based, so it sits at
/// the edge or outermost and must never reach the gate"* — and leaves the
/// gate's policy untouched.
///
/// **Host-only, pre-auth, no database.** It reads `axum::http::header::HOST`
/// and nothing else — never `X-Forwarded-Host` or `Forwarded`, exactly like
/// the binding gate — and its verdict is a pure function of the Host shape, so
/// it is identical for a slug with a live routing row and one that never
/// existed. That last property is deliberate: an answer that varied would be
/// the org-existence oracle the plan's `[SEC]` note says removing subdomains
/// was meant to close.
///
/// **Why 410 and not a 308 redirect**, given the plan offered both halves. A
/// redirect is the wrong tool for an authenticated API host. Well-behaved HTTP
/// clients strip `Authorization` when a redirect crosses origins, so the
/// caller would see a confusing 401 at the apex instead of an instruction; and
/// a client that does NOT strip it would silently deliver an org-audience
/// token to the apex. When this guard was written, `/mcp` stripped the org
/// claim there by design and served the caller's PERSONAL plane — a
/// wrong-tenant answer with a 200 on it, precisely the silent failure class
/// decision 9 already accepted once. (Since #825 that same token would bind
/// its own organization instead, which is no longer silently wrong; the
/// argument for 410 over a redirect stands on the 401 half regardless.) 410
/// is terminal, carries the new base in its body, and is distinguishable in
/// the access log from the fail-closed 404 a host that never routed anywhere
/// gets.
///
/// A no-op when no tenant runtime is configured or enabled (`TENANT_APEX_DOMAIN`
/// unset — every local/dev deploy), because with no apex
/// there is no `<slug>.<apex>` to classify.
async fn retired_org_host_guard(
    State(state): State<Arc<AppState>>,
    req: Request,
    next: Next,
) -> Response {
    if let Some(rt) = state.tenant_runtime.as_ref().filter(|r| r.enabled) {
        let host = req
            .headers()
            .get(axum::http::header::HOST)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        if is_retired_org_host_request(host, &rt.apex) {
            // Countable: the whole point of a distinct 410 is that "an old
            // client is still calling the retired org address" is a fact
            // someone can look up rather than guess. WARN with the Host and
            // path so the caller can be identified and moved to the apex.
            //
            // The path goes through `redact_share_token` for the same reason
            // the access log does (`request_id_and_latency`, which this guard
            // runs INSIDE): `GET /v1/shared/{token}` and
            // `GET /v1/shared/sessions/{token}` are unauthenticated routes
            // where the path segment IS the whole credential. This guard runs
            // before routing and logs whatever path the client sent, so
            // without this the same request would produce one redacted line
            // from the access log and one raw line from here.
            //
            // `is_retired_org_host_request` above no longer takes a path at
            // all (see its own doc — the exemption that once needed one is
            // gone), so there is nothing left for redaction to accidentally
            // feed the decision: only this log line ever sees the path, and
            // only in its redacted form.
            tracing::warn!(
                host = %host,
                path = %redact_share_token(req.uri()),
                "request to a retired org API address (claim-based tenancy cutover step 3)"
            );
            return retired_org_host_response(&rt.apex);
        }
    }
    next.run(req).await
}

/// The whole of [`retired_org_host_guard`]'s decision, as a pure function so it
/// can be exercised over every host shape without a database, a router or a
/// token — see
/// `tests::the_retired_org_address_is_answered_for_every_shape_with_no_exceptions`.
///
/// `true` means "410 this". Only a single-label `<slug>.<apex>` under our own
/// apex qualifies: the apex itself, `app.`/`www.`, multi-label hosts and
/// foreign domains all fall through to the router, which answers them exactly
/// as it did before.
///
/// **A function of `host` alone — no `path` parameter, deliberately.** This
/// used to also take a `path` and exempt four of them
/// (`RETIRED_ORG_HOST_MCP_EXEMPT_PATHS`, since deleted): `/mcp` and its three
/// `/.well-known/oauth-*` documents, kept alive on the retired host only until
/// claim-based-tenancy §Phase 3 PR 3.3 retired `/mcp`'s OWN, independent
/// org-host classification (`mcp::McpConfig::resolve_resource`'s `Subdomain`
/// arm — see that function's doc). That has now happened: nothing reaches
/// `/mcp` for org content on this host anymore either, so a retired org
/// address is retired with no exceptions at all, and there is no longer a
/// path input for this decision to depend on.
pub(crate) fn is_retired_org_host_request(host: &str, apex: &str) -> bool {
    matches!(
        tenancy::gate::classify_host(host, apex),
        tenancy::gate::HostTarget::Subdomain(_)
    )
}

/// 410 for the retired org API address, naming the base to use instead.
///
/// The message is deliberately built from the CONFIGURED apex rather than a
/// constant, so a deployment can never advertise a base it is not serving. It
/// says *why* as well as *where*: a client author who only reads "use
/// api.coralswarm.example" will go looking for the org slug elsewhere in the URL,
/// and there isn't one.
fn retired_org_host_response(apex: &str) -> Response {
    (
        StatusCode::GONE,
        Json(serde_json::json!({
            "error": format!(
                "this organization address has been retired. Use https://{apex} — your \
                 organization is selected by your login, not by the address."
            ),
        })),
    )
        .into_response()
}

/// Public-port router for `BACKEND_ROLE=workers` (#1495): liveness and
/// readiness only, so a stray request to a worker machine's :8080 can never
/// reach the API. Fly routes no public traffic here (`[http_service]` is
/// `processes = ["app"]`); the `[checks]` in fly.toml probe it over 6PN.
fn worker_health_app(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        .with_state(state)
}

/// Internal-only router: admin/operator routes protected by X-Internal-Token.
/// Bound to `INTERNAL_BIND_ADDR:INTERNAL_PORT` — see `run()`'s listener setup
/// for the address's default (`127.0.0.1`, byte-identical to pre-platform-
/// admin-metrics behavior) and production override (`fly-local-6pn`).
///
/// `rotate-dek` takes an optional `?tenant=<clerk_org_id>` query parameter
/// (org-tenancy PR 1.3) selecting which plane is rotated — see
/// `routes::admin`'s module doc. Without it, this handler would have no path
/// at all to an org's tenant-DB DEK once DEKs are relocated there.
///
/// `/internal/admin/*` (platform-admin-metrics PR 6 of 7, plus later
/// additions — `chain-health` and usage-metering PR13's `credits`): seven
/// read-only, cross-account platform-operator reporting endpoints, plus one
/// WRITE endpoint (`connector-flags`, Foundation train PR 1.3 — upserts a
/// per-org connector flag override) — see `routes::admin`'s module doc for
/// the full contract and RLS/plane notes.
fn internal_app(state: Arc<AppState>) -> Router {
    Router::new()
        .route(
            "/internal/accounts/{account_id}/rotate-dek",
            post(routes::admin::rotate_dek),
        )
        .route(
            "/internal/backfill-atom-metadata",
            post(routes::admin::backfill_atom_metadata),
        )
        .route(
            "/internal/admin/overview",
            get(routes::admin::admin_overview),
        )
        .route(
            "/internal/admin/where-from",
            get(routes::admin::admin_where_from),
        )
        .route("/internal/admin/spend", get(routes::admin::admin_spend))
        .route("/internal/admin/credits", get(routes::admin::admin_credits))
        .route("/internal/admin/fleet", get(routes::admin::admin_fleet))
        .route(
            "/internal/admin/waitlist",
            get(routes::admin::admin_waitlist),
        )
        .route(
            "/internal/admin/chain-health",
            get(routes::admin::admin_chain_health),
        )
        .route(
            "/internal/admin/pipeline-stage-health",
            get(routes::admin::admin_pipeline_stage_health),
        )
        .route(
            "/internal/admin/page-read-health",
            get(routes::admin::admin_page_read_health),
        )
        .route(
            "/internal/admin/connector-flags",
            post(routes::admin::set_connector_flag),
        )
        .layer(
            TraceLayer::new_for_http()
                .make_span_with(|_req: &Request<_>| tracing::info_span!("internal_request")),
        )
        .with_state(state)
}

// ── Graceful shutdown ─────────────────────────────────────────────────────────

async fn shutdown_signal() {
    // Listen for SIGTERM (sent by container runtimes / Fly.io) and CTRL-C so
    // both production and local development get a clean drain.
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install CTRL+C handler");
    };

    #[cfg(unix)]
    let sigterm = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };

    // On non-Unix targets SIGTERM is not available; only CTRL-C is used.
    #[cfg(not(unix))]
    let sigterm = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c  => tracing::info!("received CTRL-C, shutting down"),
        _ = sigterm => tracing::info!("received SIGTERM, shutting down"),
    }
}

/// How long the shutdown-aware job workers (embed, slack_name_backfill) get, after a
/// shutdown signal, to finish whatever they had already claimed before this
/// process gives up waiting and exits anyway — at which point anything still
/// in flight becomes exactly the orphaned-`running`-job scenario
/// `workers::reaper` exists to recover from.
///
/// Defaults to 25s, kept comfortably under typical SIGTERM→SIGKILL budgets
/// (Fly's platform default and most container orchestrators give ~30s)
/// so the process actually gets to exit cleanly instead of being hard-killed
/// mid-drain. Configurable via `WORKER_SHUTDOWN_GRACE_SECS` for environments
/// with a larger stop timeout.
fn worker_shutdown_grace() -> std::time::Duration {
    std::env::var("WORKER_SHUTDOWN_GRACE_SECS")
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .map(std::time::Duration::from_secs)
        .unwrap_or(std::time::Duration::from_secs(25))
}

/// Read the operator-tunable per-call LLM request timeout (`LLM_TIMEOUT_SECS`,
/// default 60s) applied to the shared `reqwest::Client` used for Anthropic
/// calls (see the "LLM provider" section of `run()` below).
///
/// Factored out into its own accessor so `workers::reaper` can derive its
/// stale-job threshold from the *same configured value* (see
/// `workers::reaper::stale_after`) instead of hardcoding an assumption about
/// it that could silently drift out of sync with whatever an operator has
/// actually tuned `LLM_TIMEOUT_SECS` to.
pub(crate) fn llm_timeout_secs() -> u64 {
    std::env::var("LLM_TIMEOUT_SECS")
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .unwrap_or(60)
}

/// How long a *streaming* LLM response may go without producing a single byte
/// before we give up on it (`LLM_STREAM_STALL_SECS`, default 60s).
///
/// Deliberately a stall budget rather than a total deadline — see
/// [`llm_stream_client`].
fn llm_stream_stall_secs() -> u64 {
    std::env::var("LLM_STREAM_STALL_SECS")
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .unwrap_or(60)
}

/// Build the `reqwest::Client` used for **streaming** LLM calls.
///
/// This exists because `ClientBuilder::timeout` is the wrong tool for a
/// stream. It is a *total* deadline — reqwest wraps the response body in
/// `TotalTimeoutBody`, which fails the stream the instant the deadline passes
/// even while tokens are arriving perfectly normally. Applied to synthesis it
/// stops bounding "how long may a stall last" and starts bounding "how long an
/// answer may be": every ask whose generation ran past `LLM_TIMEOUT_SECS` was
/// cut off mid-sentence and reported to the user as "synthesis interrupted".
/// On a Sonnet-class model producing `SYNTHESIS_MAX_TOKENS` of prose that is
/// most of them, which is why "ask always times out" was the reported symptom.
///
/// `read_timeout` is the correct bound: a per-read deadline that resets after
/// every successful read, so it fires on a connection that has genuinely gone
/// quiet and never on a long-but-healthy generation. `connect_timeout` still
/// bounds the dial. Note that a stream is additionally bounded from above by
/// Anthropic's own `max_tokens`, so dropping the total deadline does not leave
/// the request unbounded.
pub fn llm_stream_client() -> reqwest::Client {
    reqwest::Client::builder()
        .connect_timeout(std::time::Duration::from_secs(10))
        .read_timeout(std::time::Duration::from_secs(llm_stream_stall_secs()))
        .build()
        .expect("failed to build streaming HTTP client")
}

/// Build the production cloud LLM provider from an already-resolved
/// OpenRouter API key. `None` when the key is missing or blank. This is the
/// one place `run()`'s `AppState::llm` wiring and the eval-harness binaries
/// (`bin/gen_golden`, `bin/gen_prompt_golden`) build a provider.
/// `OPENROUTER_BASE_URL`, when set, overrides the default base URL.
pub fn build_cloud_llm_provider(
    http_client: reqwest::Client,
    openrouter_api_key: Option<String>,
) -> Option<Arc<dyn llm::LlmProvider + Send + Sync>> {
    let key = openrouter_api_key.filter(|s| !s.trim().is_empty())?;
    let mut provider = llm::openai::OpenAiProvider::openrouter(http_client, key)
        .with_stream_client(llm_stream_client());
    if let Ok(base_url) = std::env::var("OPENROUTER_BASE_URL") {
        provider = provider.with_base_url(base_url);
    }
    Some(Arc::new(provider) as Arc<dyn llm::LlmProvider + Send + Sync>)
}

/// Default `max_connections` for the production Postgres pool (RCA: the pool
/// is shared by every HTTP request AND ~13 spawned background workers; sqlx's
/// own default of 10 leaves no headroom under concurrent load). Override with
/// `DB_MAX_CONNECTIONS`.
const DEFAULT_DB_MAX_CONNECTIONS: u32 = 20;
/// Upper bound on `DB_MAX_CONNECTIONS` (CodeRabbit, PR #255): an operator typo
/// (a stray extra zero, say) must not let a single machine's pool balloon past
/// what Fly Postgres can actually give every machine headroom for — see the
/// 2×`db_max_connections()` vs. the server's ~300-connection default note at
/// the call site in `run()`. A value above this ceiling falls back to the
/// default rather than being honored.
const DB_MAX_CONNECTIONS_CEILING: u32 = 100;
/// Default pool `acquire_timeout`, in seconds (sqlx's own default is 30s,
/// which lets a starved pool hold requests far too long before failing fast).
/// Override with `DB_ACQUIRE_TIMEOUT_SECS`.
const DEFAULT_DB_ACQUIRE_TIMEOUT_SECS: u64 = 10;
/// Upper bound on `DB_ACQUIRE_TIMEOUT_SECS`, in seconds (CodeRabbit, PR #255):
/// the whole point of tuning this down from sqlx's 30s default is to fail
/// fast under a starved pool, so an operator-supplied value must not be able
/// to reintroduce (or exceed) that same multi-minute stall. A value above this
/// ceiling falls back to the default rather than being honored.
const DB_ACQUIRE_TIMEOUT_CEILING_SECS: u64 = 60;

/// Pure parser for `DB_MAX_CONNECTIONS` (CodeRabbit, PR #255): takes the raw
/// env value as `Option<&str>` instead of reading `std::env::var` itself, so
/// tests can exercise every branch directly without mutating process-global
/// env state (no lock, no cross-test leakage on panic). Falls back to
/// [`DEFAULT_DB_MAX_CONNECTIONS`] on a missing, unparsable, zero, or
/// above-[`DB_MAX_CONNECTIONS_CEILING`] value.
fn parse_db_max_connections(raw: Option<&str>) -> u32 {
    raw.and_then(|v| v.trim().parse::<u32>().ok())
        .filter(|n| *n > 0 && *n <= DB_MAX_CONNECTIONS_CEILING)
        .unwrap_or(DEFAULT_DB_MAX_CONNECTIONS)
}

/// Read the operator-tunable Postgres pool `max_connections`
/// (`DB_MAX_CONNECTIONS`, default [`DEFAULT_DB_MAX_CONNECTIONS`], capped at
/// [`DB_MAX_CONNECTIONS_CEILING`]). Thin env-reading wrapper around the pure
/// [`parse_db_max_connections`], which carries the actual test coverage.
pub(crate) fn db_max_connections() -> u32 {
    parse_db_max_connections(std::env::var("DB_MAX_CONNECTIONS").ok().as_deref())
}

/// Pure parser for `DB_ACQUIRE_TIMEOUT_SECS` (CodeRabbit, PR #255) — see
/// [`parse_db_max_connections`]'s doc comment for why this takes
/// `Option<&str>` rather than reading env itself. Falls back to
/// [`DEFAULT_DB_ACQUIRE_TIMEOUT_SECS`] on a missing, unparsable, zero, or
/// above-[`DB_ACQUIRE_TIMEOUT_CEILING_SECS`] value.
fn parse_db_acquire_timeout_secs(raw: Option<&str>) -> u64 {
    raw.and_then(|v| v.trim().parse::<u64>().ok())
        .filter(|n| *n > 0 && *n <= DB_ACQUIRE_TIMEOUT_CEILING_SECS)
        .unwrap_or(DEFAULT_DB_ACQUIRE_TIMEOUT_SECS)
}

/// Pure ALLOW-list classifier for an already-trimmed `STRIPE_SECRET_KEY`
/// value (finding #3, PR #654 round 2): a real key must be nonempty and
/// start with one of Stripe's four secret/restricted-key prefixes with
/// actual key material after the prefix. Everything else — empty, a bare
/// prefix with nothing following it, or any other shape entirely
/// (`pk_live_…` publishable, `whsec_…` webhook secret, arbitrary text) —
/// is rejected. An ALLOW-list rather than a denylist of specific bad shapes
/// on purpose: a denylist only fails closed on the exact patterns someone
/// thought to enumerate, while an allow-list fails closed on every shape it
/// doesn't explicitly recognize, including ones nobody has seen yet.
fn is_valid_stripe_secret_key(key: &str) -> bool {
    const VALID_PREFIXES: [&str; 4] = ["sk_test_", "sk_live_", "rk_test_", "rk_live_"];
    !key.is_empty()
        && VALID_PREFIXES
            .iter()
            .any(|p| key.starts_with(p) && key.len() > p.len())
}

/// Read the operator-tunable Postgres pool `acquire_timeout`, in seconds
/// (`DB_ACQUIRE_TIMEOUT_SECS`, default [`DEFAULT_DB_ACQUIRE_TIMEOUT_SECS`],
/// capped at [`DB_ACQUIRE_TIMEOUT_CEILING_SECS`]). Thin env-reading wrapper
/// around the pure [`parse_db_acquire_timeout_secs`], which carries the actual
/// test coverage.
pub(crate) fn db_acquire_timeout_secs() -> u64 {
    parse_db_acquire_timeout_secs(std::env::var("DB_ACQUIRE_TIMEOUT_SECS").ok().as_deref())
}

// ── CONTROL migration tree ────────────────────────────────────────────────────

/// The embedded CONTROL migration tree (identity, accounts, routing).
///
/// Embedded as a `static` — rather than inline at a single call site — because
/// it has two consumers with deliberately different privileges:
///
///   * `backend/src/bin/migrate_control.rs`, the out-of-band migrator, which
///     **applies** it under an elevated DSN ([`migrator_database_url`]). On
///     Fly that binary is the deploy `release_command`; locally it is
///     `cargo run -p coralswarm-backend --bin migrate_control`.
///   * [`run`]'s boot-time schema preflight ([`verify_control_schema`]), which
///     only reads [`control_head_version`] to confirm the database it is about
///     to serve is at least as new as this binary expects.
///
/// The serving process deliberately **never runs** this tree — see `run()`'s
/// preflight comment for the full rationale.
pub static CONTROL_MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("./migrations_control");

/// Head (max) version of the embedded control tree — the control-schema
/// version this binary's queries assume. Mirrors
/// `tenancy::migrate::head_version()` for the tenant tree.
pub fn control_head_version() -> i64 {
    CONTROL_MIGRATOR
        .iter()
        .map(|m| m.version)
        .max()
        .unwrap_or(0)
}

/// Pure resolver for the elevated migration DSN used by the out-of-band
/// `migrate_control` binary: `migrator` (`MIGRATOR_DATABASE_URL`) wins when it
/// is `Some` and non-blank, otherwise fall back to `app_url` (`DATABASE_URL`),
/// otherwise `None` — neither is configured, which the binary reports as a
/// usage error and exits non-zero on rather than connecting to an empty DSN.
/// Blank/whitespace-only values are treated as unset.
///
/// Pure `Option<&str>` in / `Option<String>` out — same rationale as
/// [`parse_db_max_connections`] — so the precedence rules carry unit coverage
/// without touching process-global env.
pub fn resolve_migrator_url(app_url: Option<&str>, migrator: Option<&str>) -> Option<String> {
    fn non_blank(s: Option<&str>) -> Option<String> {
        s.map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string)
    }
    non_blank(migrator).or_else(|| non_blank(app_url))
}

/// Resolve the DSN the `migrate_control` binary applies the CONTROL tree with
/// (`MIGRATOR_DATABASE_URL`, falling back to `DATABASE_URL` when unset or
/// blank — the pre-OPS-4 config, where `DATABASE_URL` still IS the elevated
/// owner role).
///
/// Why the split exists (OPS-4, migration `0083_consumer_rls`): that migration
/// creates a least-privilege `coralswarm_app` role and — correctly —
/// `REVOKE ALL ON _sqlx_migrations FROM coralswarm_app`, on the theory that
/// the live app never touches sqlx's own bookkeeping ledger. That is true of
/// request/worker traffic, but `run()` **used to** run
/// `sqlx::migrate!("./migrations_control").run(&pool)` at every boot; once
/// OPS-4 repoints `DATABASE_URL` at `coralswarm_app`, that inline migrate call
/// fails with `permission denied for table _sqlx_migrations` on every restart.
///
/// The first fix (PR #283) kept migrating at boot but on a short-lived
/// connection built from `MIGRATOR_DATABASE_URL`. That worked, but left the
/// elevated DSN sitting in the **serving** process's environment for the whole
/// process lifetime, readable by any post-boot compromise of that process —
/// which would hand an attacker exactly the RLS-bypassing, schema-dropping,
/// `tenant_databases`-rewriting role OPS-4 exists to take away. So boot-time
/// migration is gone entirely: the control tree is applied out-of-band by
/// `bin/migrate_control.rs` (Fly `release_command`; `cargo run --bin
/// migrate_control` in dev), and this function is called only from there —
/// never from `run()`.
///
/// See `docs/plans/org-tenancy-implementation-plan.md`'s "OPS-4 corrected
/// sequence" note under PR 3.4 for the operator-facing rollout. Migrations are
/// append-only (CI's `check_migrations.sh` immutability guard), so
/// `migrations_control/0083_consumer_rls.sql`'s own header comment predates
/// all of this and still describes the naive (unsafe) sequence; the plan doc
/// is the up-to-date source of truth.
pub fn migrator_database_url() -> Option<String> {
    resolve_migrator_url(
        std::env::var("DATABASE_URL").ok().as_deref(),
        std::env::var("MIGRATOR_DATABASE_URL").ok().as_deref(),
    )
}

/// Boot-time CONTROL-schema preflight — what replaced migrate-on-boot.
///
/// The serving process no longer applies migrations (see
/// [`migrator_database_url`]), so it must still refuse to serve a database
/// that the out-of-band migrator has not caught up: silently answering
/// requests against a missing or stale schema is exactly the "serving broken"
/// mode migrate-on-boot used to prevent. This is a read-only check, and any
/// failure returns `Err` so `main()` logs it and exits 1 (the fail-fast
/// contract in this module's `run()` doc comment — never a `panic!`).
///
/// Two paths, because the check itself is privilege-sensitive:
///
///   * **Ledger readable** (dev, CI, and prod pre-OPS-4, where `DATABASE_URL`
///     is still the owner role): compare `max(version)` in `_sqlx_migrations`
///     against [`control_head_version`]. Missing table, empty table, or a
///     version behind this build are all hard failures with an explicit
///     remedy in the message.
///   * **Ledger not readable** (post-OPS-4: `0083` REVOKEs ALL on
///     `_sqlx_migrations` from `coralswarm_app`, so this role gets `42501`):
///     fall back to proving the control schema exists at all via
///     `to_regclass('public.users')` (a catalog lookup, which needs no table
///     privileges). Honest limitation, logged as such: in this mode a
///     *stale-but-present* schema cannot be detected from the app role. The
///     ordering guarantee there is the deploy pipeline — Fly runs the
///     `release_command` migrator to completion **before** routing traffic to
///     the new image, so a machine can only boot against an already-migrated
///     database.
async fn verify_control_schema(pool: &PgPool) -> Result<(), String> {
    const REMEDY: &str = "apply the control tree out-of-band first \
         (`cargo run -p coralswarm-backend --bin migrate_control`; on Fly this is the deploy \
         `release_command`, run with MIGRATOR_DATABASE_URL set to the elevated owner DSN)";

    let head = control_head_version();
    // `WHERE success` matters: a `-- no-transaction` migration that died
    // half-way leaves a row with `success = false`, and counting it would let
    // the server happily serve a partially-applied schema. sqlx itself refuses
    // to proceed past such a row, so this keeps the preflight's notion of
    // "applied" identical to the migrator's.
    let ledger = sqlx::query_scalar::<_, Option<i64>>(
        "SELECT max(version) FROM _sqlx_migrations WHERE success",
    )
    .fetch_one(pool)
    .await;

    match ledger {
        Ok(Some(applied)) if applied >= head => {
            tracing::info!(
                applied_version = applied,
                head_version = head,
                "control schema verified"
            );
            Ok(())
        }
        Ok(Some(applied)) => Err(format!(
            "control schema is BEHIND this build: the database is at v{applied} but this binary \
             expects v{head}. Refusing to serve — {REMEDY}."
        )),
        Ok(None) => Err(format!(
            "control schema is UNMIGRATED: `_sqlx_migrations` exists but is empty, so no \
             migration has ever been applied to this database (expected v{head}). Refusing to \
             serve — {REMEDY}."
        )),
        Err(e) => {
            let code = match &e {
                sqlx::Error::Database(db) => db.code().map(|c| c.into_owned()),
                _ => None,
            };
            match code.as_deref() {
                // 42P01 undefined_table — the ledger itself is absent, i.e.
                // this database has never been migrated at all. By far the
                // most likely dev/CI failure ("I created the DB and ran the
                // server"), so the message spells out the missing step.
                Some("42P01") => Err(format!(
                    "control schema is MISSING: `_sqlx_migrations` does not exist, so this \
                     database has never been migrated (expected v{head}). Refusing to serve — \
                     {REMEDY}."
                )),
                // 42501 insufficient_privilege — EXPECTED post-OPS-4, when
                // DATABASE_URL is `coralswarm_app`. Degrade to a presence
                // check rather than treating the intended lockout as an error.
                Some("42501") => {
                    let present: bool =
                        sqlx::query_scalar("SELECT to_regclass('public.users') IS NOT NULL")
                            .fetch_one(pool)
                            .await
                            .map_err(|e| {
                                format!("control-schema preflight (presence check) failed: {e}")
                            })?;
                    if present {
                        tracing::info!(
                            head_version = head,
                            "control schema present; `_sqlx_migrations` is not readable by this \
                             role (expected post-OPS-4) — applied version left unverified, the \
                             deploy release_command is the ordering guarantee"
                        );
                        Ok(())
                    } else {
                        Err(format!(
                            "control schema is MISSING: `_sqlx_migrations` is not readable by \
                             this role AND the base table `public.users` does not exist, so this \
                             database has never been migrated (expected v{head}). Refusing to \
                             serve — {REMEDY}."
                        ))
                    }
                }
                _ => Err(format!(
                    "control-schema preflight could not read `_sqlx_migrations`: {e}"
                )),
            }
        }
    }
}

// ── Entry point ───────────────────────────────────────────────────────────────

/// Wall-clock budget for the boot-time OpenRouter embed probe. A hung
/// upstream must not delay listener bind; timeout is a failed probe. Release
/// builds refuse to start (search is fail-closed). Debug falls back to the
/// zero-vector stub.
const CLOUD_EMBED_PROBE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(8);

/// Collapse a `tokio::time::timeout` result around the boot-time cloud
/// embed probe. Timeout takes the same path as a probe error.
fn map_cloud_embed_probe<E: std::fmt::Display>(
    timed: Result<Result<Vec<f32>, E>, tokio::time::error::Elapsed>,
) -> Result<Vec<f32>, String> {
    match timed {
        Ok(Ok(vector)) => Ok(vector),
        Ok(Err(e)) => Err(e.to_string()),
        Err(_) => Err("timed out".to_string()),
    }
}

/// Boot the cloud HTTP server: connect Postgres, verify (never apply) the
/// control schema, build `AppState`, spawn workers, and serve the public +
/// internal routers until a shutdown signal. The thin `main.rs` binary just
/// `.await`s this.
///
/// Migrations are NOT applied here — `bin/migrate_control.rs` (Fly deploy
/// `release_command`) owns the control tree and `bin/migrate_tenants.rs` owns
/// the tenant tree, both under their own elevated DSNs. See the "NO migrations
/// here, by design" comment inside.
///
/// Returns `Err` (rather than panicking) for a startup failure that should
/// exit the process cleanly and non-zero — the schema preflight above, and see
/// the cloud embedder-probe branch below, which is release-mode fatal. `main()`
/// is responsible for turning an `Err` into a logged message + `exit(1)`
/// (the same fail-fast startup idiom the now-removed `mcp-server` stdio crate
/// used to follow); a `panic!` here would instead surface as an uncontrolled
/// crash and bypass that path (CodeRabbit review, PR #193).
pub async fn run() -> Result<(), String> {
    init_tracing();
    // Belt and braces for the build-time guards (`build.rs`, the
    // `compile_error!` above): an e2e build refuses to serve anywhere that
    // looks deployed.
    #[cfg(feature = "e2e")]
    if std::env::var_os("FLY_APP_NAME").is_some()
        || std::env::var_os("FLY_MACHINE_ID").is_some()
        || std::env::var("APP_ENV").is_ok_and(|v| v.eq_ignore_ascii_case("production"))
    {
        return Err("this binary was built with the e2e harness feature and refuses to run on a deployment (FLY_APP_NAME/FLY_MACHINE_ID set or APP_ENV=production)"
            .to_string());
    }
    // The Library Model path is the default; LIBRARY_READ_MODEL=off is the
    // kill switch. The proof run that once gated it is gone, so its secrets
    // decide nothing.
    library_read::read_model::mode();
    for obsolete in ["LIBRARY_PROOF_RUN_ID", "LIBRARY_PROOF_OPERATOR_ACCOUNT_ID"] {
        if std::env::var_os(obsolete).is_some() {
            tracing::info!(name = obsolete, "obsolete Library proof setting is set and ignored; unset it");
        }
    }
    // LIBRARY_LISTING_FACTS no longer decides anything: each plane's
    // library_listing_switch row does, flipped only by the operator binary.
    // A set value is ignored with a warning. The reader checks that row per
    // read (it serves the model only under the switch's current generation),
    // so a shadow/on reader over an Off plane falls back to Live there.
    workers::library_evidence::warn_on_legacy_listing_setting();
    // #1495: which halves of this function run. Unset = `all` = pre-#1495
    // behaviour. See `role`'s module doc for the Fly process-group wiring.
    let backend_role = role::BackendRole::from_env()?;
    tracing::info!(backend_role = backend_role.as_str(), "backend role resolved");

    // ── Cross-flag dependency assert: ORG_TEAMS_ENABLED requires
    //    MEMBER_OCEANS_ENABLED ──────────────────────────────────────────────
    // org-team-membership-design.md decision #13. The org-offboard team-ocean
    // cascade (stamping `team_members.departing_at` + writing
    // `membership_events` for a departed org member) ships INSIDE the
    // member-ocean reconciler's archive arm
    // (`workers::member_ocean_reconciler::archive_arm`, its "step 3b") rather
    // than as a standalone worker — see that function's own doc comment. If
    // `ORG_TEAMS_ENABLED` (gating the org-facing team routes in
    // `routes/teams.rs`) is ever on while `MEMBER_OCEANS_ENABLED` (gating the
    // reconciler tick that step 3b lives inside) is off, org admins could
    // create/manage team rosters through the API while the ONE place that
    // cascades an org departure onto those same rosters silently never runs —
    // the exact "rejoin restores every team ocean with no admin action and no
    // audit trail" defect this whole feature exists to close, just reopened
    // by a flag misconfiguration instead of a code bug. This is a deployment
    // misconfiguration, not a runtime condition, so it is checked ONCE here
    // at boot (unlike the two flags themselves, which are re-read per tick /
    // per request so they can be flipped without a redeploy) and fails the
    // process rather than serving team routes with a broken cascade
    // underneath them. `?`-free `return Err` (not `.expect`/`panic!`)
    // preserves this function's documented fail-fast contract (see its own
    // doc comment above) — `main()` logs it and exits 1.
    //
    // **Foundation train PR 1.3 scope note**: this assert deliberately still
    // checks only the GLOBAL env-var pair, exactly as before. Since PR 1.3, an
    // operator can also enable `"teams"` for ONE org via an
    // `org_connector_flags` row without touching `ORG_TEAMS_ENABLED` at all —
    // that action never goes through a redeploy, so there is no boot to
    // assert against. Making this genuinely per-org would mean asserting over
    // every org's resolved value at every boot (a DB read this function has
    // deliberately never needed) for a check that is already covered, per
    // request, by `routes::teams::org_teams_active_for` — which composes the
    // registry lookup with a live `member_oceans_enabled()` check on every
    // single call, unconditionally, regardless of whether that call resolved
    // via the env default or a per-org override. That per-request check (not
    // this boot-time assert) is therefore the thing that actually stops a
    // per-org override from enabling teams while member oceans are off
    // fleet-wide — see its own doc comment for the full argument. This assert
    // stays scoped to the global-default misconfiguration it always caught;
    // extend `org_teams_active_for` (or the admin setter's own guard), not
    // this assert, if a future PR needs stricter per-org coverage here too.
    if workers::member_ocean_reconciler::org_teams_enabled()
        && !workers::member_ocean_reconciler::member_oceans_enabled()
    {
        return Err(
            "invalid configuration: ORG_TEAMS_ENABLED=1 requires MEMBER_OCEANS_ENABLED=1 — the \
             org-offboard team-membership cascade (team_members.departing_at + \
             membership_events) ships inside the member-ocean reconciler's archive arm, so org \
             teams cannot be safely enabled while member oceans are off. Set both, or neither. \
             Refusing to start."
                .to_string(),
        );
    }

    let CloudBoot { state, cloud_embedder, tenant_secrets } = build_cloud_state().await?;

    // ── Shutdown coordination for job workers ─────────────────────────────────
    // `shutdown_signal()` already gives the two axum listeners below a graceful
    // HTTP drain. The `tokio::spawn`ed job workers had no equivalent: SIGTERM
    // (sent by Fly before stopping/redeploying a machine) did nothing to them —
    // this is the root cause of the orphaned-`running`-job production incident
    // that `workers::reaper` (spawned further down) exists to clean up after
    // the fact. `shutdown` is flipped as soon as the signal fires and is
    // threaded into the embed/extract workers below — the two directly in the
    // incident's causal chain — so they stop claiming new jobs immediately;
    // `run()` then gives them a bounded grace window (see
    // `worker_shutdown_grace`) to finish in-flight work before this process
    // exits regardless. `slack_name_backfill` is wired to the same signal too
    // (unrelated to that original incident, but the same class of "async
    // outbound work must be checked for missing cancellation" gap — see
    // `workers::slack_name_backfill::run`'s doc comment). See `crate::shutdown`
    // for the documented residual gap (the remaining job-queue workers aren't
    // wired to this signal in this pass).
    let shutdown = shutdown::Shutdown::new();
    {
        let shutdown = shutdown.clone();
        // Cancelling `mcp_cancellation_token` here — in the SAME task that
        // triggers the job-worker `shutdown` flag, on the SAME signal — is
        // still better than the detached, never-cancelled token rmcp
        // defaults to, but it is NOT the same "let in-flight work finish"
        // treatment the axum HTTP listeners get from `shutdown_signal()`
        // below. Our `/mcp` config is stateless (no sessions for rmcp to
        // terminate); cancelling this token instead makes rmcp immediately
        // error out any `/mcp` request whose handler hasn't produced its
        // response yet, rather than draining it — see the
        // `with_cancellation_token` doc comment in
        // `mcp::mcp_router_with_validator` for the exact mechanics.
        let mcp_cancellation_token = state.mcp_cancellation_token.clone();
        tokio::spawn(async move {
            shutdown_signal().await;
            tracing::warn!(
                grace_secs = worker_shutdown_grace().as_secs(),
                "shutdown signalled: job workers will stop claiming new work; in-flight jobs get a grace window before the process exits (the reaper is the backstop for anything that doesn't finish in time)"
            );
            shutdown.trigger();
            mcp_cancellation_token.cancel();
        });
    }

    // W6 pool-health sampler. Spawned here, right after `shutdown` exists and
    // before the job workers, so it observes the pool across the whole process
    // lifetime rather than only once workers are up. A no-op — it does not
    // even spawn — unless an exporter is configured.
    telemetry::spawn_pool_sampler(state.clone(), shutdown.clone());

    // #1495 cross-process enqueue wake: an HTTP-only process relays each
    // `note_enqueue` to the worker process; a workers-only process listens.
    // `all` needs neither (the harness reads the same in-process map).
    let wake_bridge_handle = match backend_role {
        role::BackendRole::All => None,
        role::BackendRole::Http => state.db.clone().and_then(|pool| {
            workers::wake_bridge::spawn_publisher(pool, &state.worker_activity, shutdown.clone())
        }),
        role::BackendRole::Workers => match workers::wake_bridge::listener_url_from_env() {
            Some(url) => Some(workers::wake_bridge::spawn_listener(
                url,
                state.worker_activity.clone(),
                shutdown.clone(),
            )),
            None => {
                tracing::error!(
                    "BACKEND_ROLE=workers without DATABASE_URL/WORKER_WAKE_DATABASE_URL: org jobs \
                     enqueued by HTTP machines wait out idle backoff (up to 20 min)"
                );
                None
            }
        },
    };
    if !backend_role.runs_workers() {
        tracing::warn!(
            "BACKEND_ROLE=http: this process spawns NO job workers; a `workers` process group \
             must be running or nothing drains the queue"
        );
    }

    // ── Worker tenant harness context (org-tenancy PR 2.1) ────────────────────
    // The job workers below split into two groups — see the decision table in
    // `workers::tenant_harness`'s module doc for the per-worker rationale:
    //
    //  * TENANT LOOP (embed, embed_retry, extract, summary, the four
    //    relationship-graph workers, reaper, tombstone_purge): spawned through
    //    `run_multi_tenant`, which iterates `[consumer] ++ active tenants`
    //    with per-tenant (pool, store) pairs, idle backoff, the
    //    schema-version gate, and per-tenant failure isolation. With
    //    `MULTITENANT_ENABLED` off the list is exactly `[consumer]` and
    //    behavior matches the old per-worker loops.
    //  * CONSUMER-ONLY (reembed, slack_name_backfill, content_tsv_backfill,
    //    huddle, graph_subscriptions): spawned exactly as before — connector
    //    state and one-shot legacy backfills live on the shared plane only.
    //
    // `worker_activity` is the process-wide per-tenant "last found work" clock
    // shared by every harness so dormant-tenant polls align into one burst per
    // dormant period (the Neon scale-to-zero design — see the harness module
    // doc) and any worker finding work snaps the causally-downstream workers
    // back to fast polling.
    // Constructed on `AppState` (not here) so enqueueing request/webhook paths
    // share this exact instance — see `AppState::worker_activity`.
    let worker_runtime = state.tenant_runtime.clone();

    // ── Tenant pool idle eviction sweep ───────────────────────────────────────
    // `TenantRegistry::evict_idle` needs a scheduler or its 30-minute idle TTL
    // never actually fires and suspended/offboarded tenants' pools (and their
    // idle connections) persist until a restart. Listed-active tenants are
    // touched by worker polls at least once per dormant period (< TTL) so this
    // only ever reaps tenants that left the active list — exactly the intent.
    // Harmless with the flag off: no tenant pool is ever built, the sweep is a
    // no-op over an empty map.
    if let Some(rt) = worker_runtime.clone() {
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(std::time::Duration::from_secs(10 * 60));
            ticker.tick().await; // consume the immediate first tick
            loop {
                ticker.tick().await;
                let evicted = rt.registry.evict_idle().await;
                if evicted > 0 {
                    tracing::info!(evicted, "tenant pool idle eviction sweep");
                }
                // Tenant RLS Phase 2 (Category C): the separate worker pool
                // (`get_or_build_worker_pool`) has its own idle-eviction sweep
                // — without this, a tenant that leaves the harness's active
                // list would leak its worker pool forever.
                let evicted_workers = rt.registry.evict_idle_worker_pools().await;
                if evicted_workers > 0 {
                    tracing::info!(
                        evicted = evicted_workers,
                        "tenant WORKER pool idle eviction sweep"
                    );
                }
            }
        });
        tracing::info!("tenant pool idle-eviction sweep spawned");
    }

    // Search already uses `embedding_next`. This count is leftover hold-era
    // rows (bge present, cloud missing). New ingest writes only the cloud side,
    // so it is not a flip gate.
    if backend_role.runs_workers() && cloud_embedder.is_some() {
        if let Some(pool) = state.db.as_ref() {
            match sqlx::query_scalar::<_, i64>(
                "SELECT count(*)::bigint FROM atoms \
                 WHERE embedding IS NOT NULL \
                   AND (embedding_next IS NULL OR embed_model_next IS DISTINCT FROM $1)",
            )
            .bind(coralswarm_core::embed::EMBED_MODEL_CLOUD)
            .fetch_one(pool)
            .await
            {
                Ok(missing) => {
                    tracing::info!(
                    missing,
                    flip_ready = coralswarm_core::embed::cloud_flip_ready(true, missing),
                    "cloud embed backfill on the control plane — org tenant databases are separate"
                );
                    if missing > 0 {
                        if let Err(e) = crate::jobs::queue::enqueue(
                            pool,
                            &crate::jobs::JobKind::ReembedOutdatedChunks {
                                after_atom_id: None,
                            },
                            None,
                            Some("reembed_cloud_seed"),
                            0,
                        )
                        .await
                        {
                            tracing::warn!(error = %e, "failed to enqueue the cloud re-embed seed");
                        }
                    }
                }
                Err(e) => tracing::warn!(error = %e, "cloud embed backfill count failed"),
            }
        }
    }

    // #1495: `BACKEND_ROLE=http` spawns none of the workers below; every
    // handle stays `None` and the drain at the end is a no-op for it.
    let workers_on = backend_role.runs_workers();

    let startup::EmbeddingWorkers { embed_handle, reembed_handle, propagate_corrections_handle } = if workers_on { startup::embedding(&state, &shutdown, &cloud_embedder) } else { Default::default() };

    let startup::SemanticWorkers { digest_handle, ocean_health_handle, summary_handle, source_atoms_handle, source_blob_cleanup_handle } = if workers_on { startup::semantics(&state, &shutdown) } else { Default::default() };

    let startup::CollaborationWorkers { slack_name_backfill_handle, promote_handle, slack_seat_reconcile_handle, slack_unmatched_email_handle, chat_invite_handle, content_tsv_backfill_handle, atom_title_backfill_handle, huddle_handle, teams_seat_reconcile_handle, teams_unmatched_email_handle } = if workers_on { startup::collaboration(&state, &shutdown) } else { Default::default() };

    let startup::ConnectorWorkers { slot_reservations_handle, member_connect_nudge_handle, org_join_request_digest_handle } = if workers_on { startup::connectors(&state, &shutdown) } else { Default::default() };

    // ── Geo backfill worker (platform-admin-metrics PR 4 of 7) ───────────────
    // Batched, offline resolution of `user_access_log.ip` → country/region/
    // city (see `workers::geo_backfill`'s module doc for the full "why
    // batched, why local-file, why swappable" rationale). Only spawned when
    // `GeoResolver::from_env` actually finds a usable GeoLite2 database —
    // `GEOIP_DB_PATH` unset (the default) or an unreadable file means this
    // worker never runs at all, and `country`/`region`/`city` simply stay
    // NULL forever. No outbound network call either way.
    //
    // `GeoResolver::from_env` opens the `.mmdb` file with a synchronous
    // `std::fs` read (`maxminddb::Reader::open_readfile`) that can be
    // multiple MB, so it runs on `spawn_blocking`'s dedicated thread pool
    // rather than inline on this async fn's task — by this point several
    // other workers are already spawned on the same runtime and the HTTP
    // listeners haven't bound yet, so a blocking read here would stall all
    // of them for its duration, worst-case on a constrained-vCPU Fly
    // machine. A `JoinError` here means the blocking closure itself
    // panicked (never a legitimate "file missing"/"unreadable" outcome —
    // those are `from_env`'s own `None` return, logged and handled by the
    // `if let Some(resolver)` below same as always); that is worth a `warn`
    // so a panicking resolver load isn't silently invisible, but it still
    // must NOT escalate into a hard startup failure — a missing/unusable
    // GeoIP DB is an explicitly soft "skip the worker" condition, not the
    // kind of genuine misconfiguration this fn's fail-fast contract exists
    // for.
    if let Some(pool) = state.db.clone().filter(|_| workers_on) {
        let resolver = match tokio::task::spawn_blocking(
            workers::geo_backfill::GeoResolver::from_env,
        )
        .await
        {
            Ok(resolver) => resolver,
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    "geo backfill: GeoResolver::from_env panicked on the blocking pool; \
                     geo backfill worker will not run (user_access_log.country/region/city stay NULL)"
                );
                None
            }
        };
        if let Some(resolver) = resolver {
            tokio::spawn(workers::geo_backfill::run(pool, resolver, shutdown.clone()));
            tracing::info!("geo-backfill worker spawned");
        }
    }

    let startup::IngestionWorkers { history_backfill_handle, github_backfill_handle, github_event_ingest_handle, ingest_receive_handle, github_reconcile_sweep_handle, journal_sweep_handle } = if workers_on { startup::ingestion(&state, &shutdown) } else { Default::default() };

    // ── Self-serve tenant provisioning worker (org signup A1) ─────────────────
    // Workers process group only (#1495): a `BACKEND_ROLE=http` machine never
    // creates a Neon project. Idles unless ORG_SELFSERVE_SIGNUP_ENABLED is set;
    // not spawned at all when a dependency (control pool, tenant runtime,
    // provisioner, Clerk, KEK) is missing — each absence is a boot-time warn.
    // An enqueue from an HTTP machine is picked up by the worker's
    // control-table poll, so it needs no wake bridge.
    let tenant_provisioning_handle = if workers_on { startup::tenant_provisioning(&state, &shutdown, tenant_secrets.clone()) } else { None };

    // ── HTTP listeners ────────────────────────────────────────────────────────
    let port = std::env::var("PORT").unwrap_or_else(|_| "8080".to_owned());
    let addr = format!("0.0.0.0:{port}");
    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .expect("failed to bind public TCP listener");
    tracing::info!(%addr, "public server listening");

    // `INTERNAL_BIND_ADDR` (platform-admin-metrics PR 6 of 7): the admin app
    // (PR #360) is a SEPARATE Fly app and can only reach this one over Fly's
    // private network, which requires binding the private IPv6 interface
    // (`fly-local-6pn`) — loopback (`127.0.0.1`, the default below) is not
    // reachable from another Fly app at all. Defaults to `127.0.0.1` so
    // NOTHING changes for any existing deployment unless this var is
    // explicitly set — this endpoint set must never silently widen exposure.
    // Production sets `INTERNAL_BIND_ADDR=fly-local-6pn`.
    //
    // Deliberately NOT `[::]` (or `0.0.0.0`): either would bind every
    // interface, including the public one, making every `/internal/*` route
    // (X-Internal-Token only, no session auth, cross-account by design)
    // reachable from the public internet — exactly the exposure this
    // listener's whole reason for existing on a separate port is meant to
    // prevent. `fly-local-6pn` is Fly's private-network-only interface: it
    // resolves to an address routable ONLY from other machines in the same
    // Fly private network (WireGuard-backed 6PN), never from the public
    // internet, so it grants exactly the "other Fly apps in this org can
    // reach this" access this feature needs and nothing more.
    let internal_port = std::env::var("INTERNAL_PORT").unwrap_or_else(|_| "8081".to_owned());
    let internal_bind_host =
        std::env::var("INTERNAL_BIND_ADDR").unwrap_or_else(|_| "127.0.0.1".to_owned());
    let internal_addr = format!("{internal_bind_host}:{internal_port}");
    // `INTERNAL_BIND_ADDR`/`INTERNAL_PORT` are operator-supplied — a typo, or a
    // host where `fly-local-6pn` doesn't resolve, must fail fast via `?` (not
    // `.expect`) per this function's documented fail-fast contract (see the
    // comment above `verify_control_schema`'s call site), not panic.
    let internal_listener = tokio::net::TcpListener::bind(&internal_addr)
        .await
        .map_err(|e| {
            format!(
                "failed to bind internal listener on {internal_addr} — check INTERNAL_BIND_ADDR/INTERNAL_PORT: {e}"
            )
        })?;
    tracing::info!(%internal_addr, "internal server listening");

    // Run both servers concurrently. Previously this used `tokio::select!`,
    // which waits for only the FIRST listener's graceful shutdown to finish
    // and then drops (silently cancels) the other's still-in-progress drain.
    // On a real SIGTERM the near-idle internal listener reliably finishes
    // first, which could cut the public listener's in-flight-request drain
    // short. `tokio::join!` waits for both to actually finish.
    // `state.clone()` (not a bare move) on BOTH branches: the drain step below
    // needs `state.task_tracker` after both listeners finish, so the original
    // `state` binding must survive this join, not be consumed by it.
    let (public_result, internal_result) = tokio::join!(
        // #1495: a workers-only machine has no `[http_service]`; its public
        // port answers liveness/readiness for the `[checks]` in fly.toml only.
        axum::serve(
            listener,
            if backend_role.serves_http() { app(state.clone()) } else { worker_health_app(state.clone()) },
        )
        .with_graceful_shutdown(shutdown_signal()),
        axum::serve(internal_listener, internal_app(state.clone()))
            .with_graceful_shutdown(shutdown_signal()),
    );
    public_result.expect("public server error");
    internal_result.expect("internal server error");

    // ── Drain shutdown-aware job workers ──────────────────────────────────────
    // Both HTTP listeners have now finished their graceful shutdown, so if we
    // got here via a real shutdown signal, `shutdown` is already triggered
    // (see the "Shutdown coordination" comment above `embed_handle`). Give
    // the embed/extract/slack_name_backfill workers a bounded window to
    // finish whatever they'd already claimed before this process exits.
    // Anything that doesn't land in time — or any OTHER job-queue worker,
    // which isn't wired to `shutdown` at all in this pass (documented gap in
    // `crate::shutdown`) — is left for `workers::reaper` to reclaim on its
    // next sweep.
    //
    // All three are drained CONCURRENTLY under one shared `grace` window
    // (`tokio::join!`), not sequentially. A prior version awaited each
    // worker behind its own independent `timeout(grace, handle)` in a `for`
    // loop, so a worst case where every worker stalled took up to
    // `N * grace` (~50s for two workers at the 25s default) — blowing past
    // the ~30s SIGTERM→SIGKILL budget `worker_shutdown_grace`'s doc comment
    // cites, getting the process hard-killed mid-drain anyway (the exact
    // failure mode this drain exists to avoid). Joining all of them under a
    // single `grace` window bounds the total drain at `grace`, not
    // `N * grace`.
    let grace = worker_shutdown_grace();
    async fn drain_one(
        name: &'static str,
        handle: Option<tokio::task::JoinHandle<()>>,
        grace: std::time::Duration,
    ) {
        let Some(handle) = handle else { return };
        match tokio::time::timeout(grace, handle).await {
            Ok(Ok(())) => tracing::info!(worker = name, "worker drained cleanly"),
            Ok(Err(e)) => {
                tracing::warn!(worker = name, error = %e, "worker task ended abnormally during drain")
            }
            Err(_) => tracing::warn!(
                worker = name,
                grace_secs = grace.as_secs(),
                "worker did not finish in-flight jobs within the grace window; any still-claimed job is left for the reaper"
            ),
        }
    }
    // ── Drain handler-spawned background tasks (Slack webhook, etc.) ─────────
    // Both HTTP listeners have finished their graceful drain of in-flight
    // *connections* above, but that says nothing about work a handler already
    // kicked off with `state.task_tracker.spawn(..)` and returned 200 for
    // before this point (Slack message ingest, member-joined destination/
    // backfill prompts, the huddle-probe persist — see `AppState::task_tracker`'s
    // doc comment). `close()` must happen here, AFTER both listeners are done,
    // not at spawn time or earlier in shutdown: closing it while either
    // listener could still be accepting a request that spawns a tracked task
    // would make that spawn panic (`TaskTracker::spawn` panics if the tracker
    // is already closed). Bounded by the SAME `grace` window the job workers
    // get above, and joined concurrently with them for the same reason
    // `drain_one`'s doc comment gives: a worst case must not be additive.
    let task_tracker_drain = async {
        state.task_tracker.close();
        match tokio::time::timeout(grace, state.task_tracker.wait()).await {
            Ok(()) => tracing::info!("handler-spawned background tasks drained cleanly"),
            Err(_) => tracing::warn!(
                grace_secs = grace.as_secs(),
                "handler-spawned background tasks did not finish within the grace window; \
                 any still in flight were aborted at process exit"
            ),
        }
    };

    tokio::join!(
        drain_one("embed", embed_handle, grace),
        drain_one("reembed_backfill", reembed_handle, grace),
        drain_one("propagate_corrections", propagate_corrections_handle, grace),
        drain_one("digest", digest_handle, grace),
        drain_one("ocean_health", ocean_health_handle, grace),
        drain_one("summary", summary_handle, grace),
        drain_one("source_atoms", source_atoms_handle, grace),
        drain_one("source_blob_cleanup", source_blob_cleanup_handle, grace),
        drain_one("slack_name_backfill", slack_name_backfill_handle, grace),
        drain_one("slack_seat_reconcile", slack_seat_reconcile_handle, grace),
        drain_one("slack_unmatched_email", slack_unmatched_email_handle, grace),
        drain_one("chat_invite", chat_invite_handle, grace),
        drain_one("history_backfill", history_backfill_handle, grace),
        drain_one("member_connect_nudge", member_connect_nudge_handle, grace),
        drain_one(
            "org_join_request_digest",
            org_join_request_digest_handle,
            grace
        ),
        drain_one("github_backfill", github_backfill_handle, grace),
        drain_one("github_event_ingest", github_event_ingest_handle, grace),
        drain_one("ingest_receive", ingest_receive_handle, grace),
        drain_one(
            "github_reconcile_sweep",
            github_reconcile_sweep_handle,
            grace
        ),
        drain_one("teams_seat_reconcile", teams_seat_reconcile_handle, grace),
        drain_one("teams_unmatched_email", teams_unmatched_email_handle, grace),
        drain_one("huddle", huddle_handle, grace),
        drain_one("promote", promote_handle, grace),
        drain_one("content_tsv_backfill", content_tsv_backfill_handle, grace),
        drain_one("atom_title_backfill", atom_title_backfill_handle, grace),
        drain_one("journal_sweep", journal_sweep_handle, grace),
        drain_one("slot_reservations", slot_reservations_handle, grace),
        drain_one("wake_bridge", wake_bridge_handle, grace),
        drain_one("tenant_provisioning", tenant_provisioning_handle, grace),
        task_tracker_drain,
    );
    Ok(())
}

/// Everything `run()` builds before it spawns a worker or binds a port: the
/// control pool + schema preflight, store, embedders (with the release-mode
/// probe), LLM, rate limiter and the full [`AppState`]. Split out so operator
/// bins (`quality_eval_ask`) can drive request-path code in-process against
/// the identical production state without starting the server or workers.
pub struct CloudBoot {
    pub state: Arc<AppState>,
    pub cloud_embedder: Option<Arc<dyn coralswarm_core::embed::EmbedProvider + Send + Sync>>,
    /// The same resolver `state.tenant_runtime`'s registry was built with —
    /// handed to the self-serve provisioning worker so its migrate and
    /// fingerprint steps resolve a brand-new tenant's DSN the way the
    /// request path will.
    pub(crate) tenant_secrets: Arc<dyn tenancy::secrets::SecretResolver>,
}

/// The tenant-DSN resolver the serving process hands to `TenantRuntime`:
/// environment first (every concierge tenant resolves byte-identically to
/// before), then the KEK-encrypted `tenant_db_credentials` control table
/// (self-serve tenants, migration 0381). Falls back to the environment-only
/// resolver — today's behaviour exactly — when there is no control pool or
/// `MASTER_KEK_HEX` is unset/malformed, and says so once at boot.
fn tenant_secret_resolver(db: Option<&PgPool>) -> std::sync::Arc<dyn tenancy::secrets::SecretResolver> {
    let Some(pool) = db else {
        return std::sync::Arc::new(tenancy::secrets::EnvSecretResolver::new());
    };
    match tenancy::secrets::ChainSecretResolver::from_env(pool.clone()) {
        Ok(chain) => std::sync::Arc::new(chain),
        Err(e) => {
            tracing::warn!(
                error = %e,
                "MASTER_KEK_HEX unavailable: tenant DSNs resolve from the environment only \
                 (self-serve tenant credentials in tenant_db_credentials are unreachable)"
            );
            std::sync::Arc::new(tenancy::secrets::EnvSecretResolver::new())
        }
    }
}

/// See [`CloudBoot`]. Fail-fast contract identical to `run()`.
pub async fn build_cloud_state() -> Result<CloudBoot, String> {
    // ── Database pool + schema preflight ───────────────────────────────────────
    // DATABASE_URL is optional so the binary can start in dev without a live
    // control-plane Postgres.  /readyz will return 503 in that case.
    let db: Option<PgPool> = match std::env::var("DATABASE_URL") {
        Err(_) => {
            tracing::warn!(
                "DATABASE_URL not set — starting without a database pool; /readyz will return 503"
            );
            None
        }
        Ok(url) => {
            // RCA: this single pool is shared by every HTTP request AND ~13
            // spawned background workers; a plain `PgPool::connect` fell back
            // to sqlx's defaults (max_connections=10, acquire_timeout=30s),
            // which under concurrent worker + request load starved requests
            // for up to 30s before failing. Both knobs are env-tunable.
            // Fly Postgres (coralswarm-db, single primary) needs headroom for
            // 2 machines × `db_max_connections()` connections; the server's
            // default `max_connections` is typically 300, so 2×20=40 is safe.
            let max_connections = db_max_connections();
            let acquire_timeout = std::time::Duration::from_secs(db_acquire_timeout_secs());

            // ── NO migrations here, by design ──────────────────────────────
            // Neither tree is applied by the serving process:
            //   * CONTROL tree → `bin/migrate_control.rs`, wired as the Fly
            //     deploy `release_command` (and `cargo run --bin
            //     migrate_control` in dev/CI). It connects with the elevated
            //     owner DSN; the serving container never receives one.
            //   * TENANT tree → `bin/migrate_tenants.rs`, as before.
            //
            // Two reasons, in order of importance:
            //   1. Security (the point of this change): migration `0083`
            //      REVOKEs `_sqlx_migrations` from `coralswarm_app`, so a
            //      boot-time migrate needs an elevated DSN — and holding one
            //      in the serving process's env for its whole lifetime hands
            //      any post-boot compromise the RLS-bypassing owner role that
            //      OPS-4 exists to take away.
            //   2. Correctness: N booting machines used to race the same
            //      migration behind sqlx's advisory lock. Fly runs
            //      `release_command` to completion *before* routing traffic to
            //      the new image, and aborts the deploy on a non-zero exit —
            //      strictly safer ordering.
            //
            // What remains here is a read-only preflight (below, after the
            // pool is built) that refuses to serve an unmigrated/stale
            // database instead of 500ing every request.

            // ── Long-lived pool for actual app/worker traffic ──────────────
            // RCA: this single pool is shared by every HTTP request AND ~13
            // spawned background workers; a plain `PgPool::connect` fell back
            // to sqlx's defaults (max_connections=10, acquire_timeout=30s),
            // which under concurrent worker + request load starved requests
            // for up to 30s before failing. Both knobs are env-tunable.
            // Fly Postgres (coralswarm-db, single primary) needs headroom for
            // 2 machines × `db_max_connections()` connections; the server's
            // default `max_connections` is typically 300, so 2×20=40 is safe.
            let pool = sqlx::postgres::PgPoolOptions::new()
                .max_connections(max_connections)
                .acquire_timeout(acquire_timeout)
                .connect(&url)
                .await
                .map_err(|e| format!("failed to connect to Postgres — check DATABASE_URL: {e}"))?;
            tracing::info!(
                max_connections,
                acquire_timeout_secs = acquire_timeout.as_secs(),
                "postgres pool options configured"
            );

            // Read-only schema preflight — fail fast, with a message naming
            // the missing step, rather than booting and 500ing every request
            // against a database the out-of-band migrator has not caught up.
            // `?` (not `.expect`) preserves the fail-fast contract in this
            // function's doc comment: `Err` → `main()` logs + `exit(1)`,
            // never an uncontrolled panic (CodeRabbit, PRs #193/#255).
            verify_control_schema(&pool).await?;

            Some(pool)
        }
    };

    // The store mirrors `db`: present iff a pool exists. Constructed here so it
    // shares the same pool the legacy inline-SQL handlers still use.
    let store: Option<Arc<dyn store::Store>> = db
        .as_ref()
        .map(|pool| Arc::new(store::PostgresStore::new(pool.clone())) as Arc<dyn store::Store>);

    // ── Resend API key ─────────────────────────────────────────────────────────
    let resend_api_key = match std::env::var("RESEND_API_KEY") {
        Ok(k) => {
            tracing::info!("Resend API key loaded");
            Some(k)
        }
        Err(_) => {
            tracing::warn!("RESEND_API_KEY not set — email sending disabled");
            None
        }
    };

    // ── R2 blob storage client ─────────────────────────────────────────────────
    let r2: Option<Arc<R2Client>> = match (
        std::env::var("R2_ACCOUNT_ID"),
        std::env::var("R2_ACCESS_KEY_ID"),
        std::env::var("R2_SECRET_ACCESS_KEY"),
        std::env::var("R2_BUCKET_NAME"),
    ) {
        (Ok(account_id), Ok(access_key_id), Ok(secret_access_key), Ok(bucket)) => {
            // The e2e build alone may point R2 at MinIO; every other build
            // has no such code path (see `R2Client::new`'s doc).
            #[cfg(feature = "e2e")]
            let client = match std::env::var("R2_ENDPOINT_OVERRIDE").ok().filter(|v| !v.trim().is_empty()) {
                Some(endpoint) => {
                    R2Client::new_with_endpoint_override(
                        &account_id,
                        &access_key_id,
                        &secret_access_key,
                        &bucket,
                        endpoint.trim(),
                    )
                    .await
                }
                None => R2Client::new(&account_id, &access_key_id, &secret_access_key, &bucket).await,
            };
            #[cfg(not(feature = "e2e"))]
            let client =
                R2Client::new(&account_id, &access_key_id, &secret_access_key, &bucket).await;
            tracing::info!("R2 client initialised");
            Some(Arc::new(client))
        }
        _ => {
            tracing::warn!(
                "R2 credentials not fully set (R2_ACCOUNT_ID / R2_ACCESS_KEY_ID / \
                 R2_SECRET_ACCESS_KEY / R2_BUCKET_NAME) — blob storage disabled"
            );
            None
        }
    };

    // Embedding provider is constructed after `http_client` below. The server
    // does not load ONNX; query embed and document vectors share the OpenRouter
    // cloud model.

    // ── LLM provider ──────────────────────────────────────────────────────────
    // The LLM client gets its own (longer) timeout: synthesis over a large
    // context routinely takes longer than the 10s general-purpose default, which
    // silently truncated answers. Audit S6 — configurable via `LLM_TIMEOUT_SECS`
    // (default 60); see `llm_timeout_secs()`.
    //
    // This total deadline is correct for the NON-streaming path only. The
    // streaming path gets `llm_stream_client()` instead, whose doc comment
    // explains why a total deadline silently truncated every long answer.
    let llm_timeout_secs: u64 = llm_timeout_secs();
    let http_client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(llm_timeout_secs))
        .build()
        .expect("failed to build HTTP client");

    // Warm the Microsoft add-in signing keys so the first pane load does not pay
    // (or fail) the cold fetch. Non-fatal: a failure is logged and the first
    // request retries.
    if microsoft_sso::sso_enabled() {
        let http = http_client.clone();
        tokio::spawn(async move {
            microsoft_sso::prewarm(&microsoft_sso::MicrosoftSsoConfig::from_env(), &http).await;
        });
    }

    let openrouter_api_key = std::env::var("OPENROUTER_API_KEY").ok();

    // Server search and new ingest write `embedding_next` with this embedder.
    let (embedder, cloud_embedder): (
        Arc<dyn coralswarm_core::embed::EmbedProvider + Send + Sync>,
        Option<Arc<dyn coralswarm_core::embed::EmbedProvider + Send + Sync>>,
    ) = match openrouter_api_key.as_deref() {
        Some(key) => {
            let client = coralswarm_core::embed_openrouter::OpenRouterEmbedder::new(
                http_client.clone(),
                key,
            );
            // e2e only: the embedder follows OPENROUTER_BASE_URL to the
            // in-runner fake, as the LLM provider already does.
            #[cfg(feature = "e2e")]
            let client = match std::env::var("OPENROUTER_BASE_URL") {
                Ok(base) if !base.trim().is_empty() => client.with_base_url(base),
                _ => client,
            };
            match map_cloud_embed_probe(
                tokio::time::timeout(
                    CLOUD_EMBED_PROBE_TIMEOUT,
                    // Paid, but no account to bill: metered with no usage row.
                    embedding::submit_one(
                        &client,
                        embedding::Purpose::Metered { usage: None, task: "embed_probe" },
                        "coralswarm",
                    ),
                )
                .await,
            ) {
                Ok(vector)
                    if vector.len() == coralswarm_core::embed::EMBED_DIM
                        && vector.iter().any(|n| *n != 0.0) =>
                {
                    tracing::info!(
                        model = coralswarm_core::embed::EMBED_MODEL_CLOUD,
                        "cloud embed probe ok — server search uses embedding_next"
                    );
                    let arc: Arc<dyn coralswarm_core::embed::EmbedProvider + Send + Sync> =
                        Arc::new(client);
                    (arc.clone(), Some(arc))
                }
                Ok(vector) => {
                    let msg = format!(
                        "cloud embed probe returned an unexpected vector (width {})",
                        vector.len()
                    );
                    if cfg!(debug_assertions) {
                        tracing::error!(error = %msg, "using stub embedder (dev mode)");
                        (Arc::new(StubEmbedder), None)
                    } else {
                        return Err(format!("{msg} — refusing to start; search is fail-closed"));
                    }
                }
                Err(e) => {
                    if cfg!(debug_assertions) {
                        tracing::error!(
                            error = %e,
                            "cloud embed probe failed — using stub embedder (dev mode)"
                        );
                        (Arc::new(StubEmbedder), None)
                    } else {
                        return Err(format!(
                            "cloud embed probe failed ({e}) — refusing to start; search is fail-closed"
                        ));
                    }
                }
            }
        }
        None => {
            if cfg!(debug_assertions) {
                tracing::warn!("OPENROUTER_API_KEY unset — using stub embedder (dev mode)");
                (Arc::new(StubEmbedder), None)
            } else {
                return Err(
                    "OPENROUTER_API_KEY unset — refusing to start without a cloud embedder. \
                     Search is fail-closed."
                        .to_string(),
                );
            }
        }
    };

    let llm: Option<Arc<dyn llm::LlmProvider + Send + Sync>> =
        build_cloud_llm_provider(http_client.clone(), openrouter_api_key.clone());

    match &llm {
        Some(_) => tracing::info!("OpenRouter LLM provider configured"),
        None => {}
    }

    if llm.is_none() {
        tracing::warn!(
            "OPENROUTER_API_KEY is not set — synthesis disabled; \
             /v1/queries serves retrieval-only results (ranked sources, HTTP 200, no synthesized \
             answer)"
        );
    }

    // ── Rate limiter ──────────────────────────────────────────────────────────
    // Opt-in shared limiter (audit S4). Default (`unset` or `memory`) keeps the
    // in-process DashMap limiter so behavior is unchanged; `pg` uses the shared
    // Postgres-backed limiter (correct on multi-replica Fly) when a pool exists.
    let rate_limiter: Arc<dyn rate_limit::RateLimiter> = match std::env::var("RATE_LIMIT_BACKEND")
        .as_deref()
    {
        Ok("pg") => match db.clone() {
            Some(pool) => {
                tracing::info!("Rate limiter: Postgres-backed (shared across replicas)");
                Arc::new(rate_limit::PgRateLimiter::new(pool))
            }
            None => {
                tracing::warn!(
                        "RATE_LIMIT_BACKEND=pg but no DATABASE_URL/pool — falling back to in-memory limiter"
                    );
                Arc::new(InMemoryRateLimiter::new())
            }
        },
        _ => Arc::new(InMemoryRateLimiter::new()),
    };

    // ── DEK cache ─────────────────────────────────────────────────────────────
    let dek_cache = DekCache::new();

    // ── Stripe outbound client (billing self-serve PR C) ────────────────────
    // `STRIPE_SECRET_KEY` read ONCE here, same posture as `OPENROUTER_API_KEY`
    // above — see `AppState::stripe`'s own doc comment for why this is the
    // `llm` pattern, not the `clerk.rs` per-call-re-read pattern. `None`
    // (unset) is fail-closed: every outbound call answers `NotConfigured`
    // and every consuming route (PR D/F/G/H) answers 503, charging nothing.
    // Trim BEFORE storing: a trailing newline from a piped
    // `fly secrets set STRIPE_SECRET_KEY < file` (or a stray shell
    // quoting mistake) would otherwise land in the `Authorization:
    // Bearer <key>` header on every outbound call and fail as an
    // opaque per-request 401 instead of at boot, where it belongs. Read into
    // a local (rather than folding straight into the `and_then` below) so
    // the two boot-time warnings below can tell "genuinely unset/blank" and
    // "set but not a recognized key format" apart — they are mutually
    // exclusive diagnostics, not a pair that should both fire for one cause.
    let stripe_secret_key_raw = std::env::var("STRIPE_SECRET_KEY").ok();
    let stripe_secret_key_trimmed = stripe_secret_key_raw
        .as_deref()
        .map(str::trim)
        .unwrap_or("");
    let stripe: Option<Arc<billing::stripe_client::StripeClient>> =
        if stripe_secret_key_trimmed.is_empty() {
            None
        } else {
            // ALLOW-list (finding #3, PR #654 round 2), not a denylist of
            // specific bad shapes: empty, a bare prefix with nothing after
            // it (e.g. copied verbatim from `.env.example`), and any other
            // format entirely (a `pk_live_…` publishable key, a `whsec_…`
            // webhook secret, arbitrary pasted text) all fail closed to
            // `None` here rather than building a client that is
            // "configured" but can only ever fail auth. `rk_*` is allowed
            // alongside `sk_*` for the same reason `StripeClient::livemode`
            // checks both prefix families. See
            // [`is_valid_stripe_secret_key`] for the actual check — it
            // carries the unit-test coverage.
            if is_valid_stripe_secret_key(stripe_secret_key_trimmed) {
                tracing::info!("Stripe outbound client configured");
                let client = billing::stripe_client::StripeClient::new(
                    billing::stripe_client::stripe_http_client(),
                    stripe_secret_key_trimmed.to_string(),
                );
                // e2e only: Stripe is an in-runner fake.
                #[cfg(feature = "e2e")]
                let client = match std::env::var("STRIPE_API_BASE") {
                    Ok(base) if !base.trim().is_empty() => client.with_base_url(base.trim().trim_end_matches('/')),
                    _ => client,
                };
                Some(Arc::new(client))
            } else {
                // Never log the key value here — an unrecognized value is,
                // by construction, exactly the case that might be a secret
                // in the wrong shape (a truncated key, a webhook secret
                // pasted into the wrong env var, ...) rather than a
                // placeholder.
                tracing::warn!(
                    "STRIPE_SECRET_KEY is set but is not a recognized Stripe key format \
                     (sk_test_/sk_live_/rk_test_/rk_live_ with key material after the prefix) — \
                     treating Stripe as unconfigured rather than building a client that can only \
                     ever fail authentication"
                );
                None
            }
        };
    if stripe_secret_key_trimmed.is_empty() {
        tracing::warn!(
            "STRIPE_SECRET_KEY not set — outbound Stripe calls disabled; checkout/seat/refill/billing-management routes will answer 503 and charge nothing"
        );
    }

    let tenant_secrets = tenant_secret_resolver(db.as_ref());
    let tenant_secrets_for_provisioning = tenant_secrets.clone();
    let state = Arc::new(AppState {
        db,
        store,
        resend_api_key,
        r2,
        http_client,
        embedder: embedder.clone(),
        llm,
        rate_limiter,
        // W2 admission control. Resolved once here, like `env` below, so no
        // request path ever reads `std::env::var`. Ships OBSERVE-ONLY unless
        // `RATE_LIMIT_ENFORCE` / `CONCURRENCY_ENFORCE` are explicitly set —
        // see `admission::AdmissionConfig::default`'s comment for why the
        // ceilings must come from a week of real data rather than from the
        // plan document, and why enforcement must not precede W3.
        admission: admission::AdmissionConfig::from_env(),
        in_flight: Arc::new(admission::InFlightRegistry::new()),
        // W6. Resolved once at boot like `admission` and `env`. Inert unless
        // `CORALSWARM_METRICS` selects an exporter — this is the ONLY
        // construction site that reads the environment; every other one
        // (tests, the Slack backfill's synthetic state) stays a no-op.
        metrics: telemetry::init(),
        dek_cache,
        connector_flag_cache: connector_flags::ConnectorFlagCache::new(),
        tenant_runtime: tenancy::runtime::TenantRuntime::from_env(tenant_secrets),
        worker_activity: workers::tenant_harness::TenantActivity::new(),
        // Resolved exactly once, here at boot — see `AppEnv`'s doc comment.
        // Everything downstream (`app()`, `public_cors_layer`, the OpenAPI
        // gate) reads `state.env`, never `std::env::var("APP_ENV")` again.
        env: AppEnv::resolve(),
        task_tracker: tokio_util::task::TaskTracker::new(),
        // Cancelled below, alongside `shutdown.trigger()`, once the process
        // graceful-shutdown signal fires — see that call site's comment.
        mcp_cancellation_token: tokio_util::sync::CancellationToken::new(),
        // One per process, shared by every `/mcp` request this machine serves
        // — see the field's own doc comment for the per-process (not
        // cross-replica) scope this implies.
        mcp_task_manager: mcp::server::McpTaskRegistry::new(),
        // Same per-process scope as `mcp_task_manager` immediately above.
        mcp_ask_in_flight: mcp::server::AskInFlight::new(),
        stripe,
    });
    Ok(CloudBoot { state, cloud_embedder, tenant_secrets: tenant_secrets_for_provisioning })
}

// ── Test helpers (available to all modules in the crate) ─────────────────────

/// Build a minimal `AppState` with all optional fields set to `None`.
///
/// `pub` (not `pub(crate)`) under `cfg(any(test, feature = "test-support"))` so
/// both in-crate unit tests AND the external integration tests under
/// `backend/tests/` can reuse it without duplicating the `AppState` struct
/// literal — the latter link this crate as an ordinary dependency and so never
/// see plain `#[cfg(test)]` items (only this crate's own unit-test build does).
/// See the `test-support` feature + self-dependency note in `Cargo.toml`.
#[cfg(any(test, feature = "test-support"))]
pub fn test_app_state() -> Arc<AppState> {
    test_app_state_with_tenant_runtime(None)
}

/// [`test_app_state`] with an explicit tenancy runtime, for tests that need the
/// multi-tenant shape (an apex to classify Hosts against). `None` is the
/// flag-off default every other test uses.
#[cfg(any(test, feature = "test-support"))]
pub fn test_app_state_with_tenant_runtime(
    tenant_runtime: Option<Arc<tenancy::runtime::TenantRuntime>>,
) -> Arc<AppState> {
    Arc::new(test_app_state_owned(tenant_runtime))
}

/// The same test state, UNWRAPPED, for the few tests that need to override a
/// field before sharing it. `AppState` is not `Clone` (it holds trait objects
/// and a `DekCache`), so a test that needs, say, a non-default
/// [`admission::AdmissionConfig`] cannot patch an `Arc<AppState>` after the
/// fact — it has to build one. Exposing the owned value keeps that from
/// meaning "copy the whole literal into every such test", which is how field
/// drift starts.
#[cfg(any(test, feature = "test-support"))]
pub fn test_app_state_owned(
    tenant_runtime: Option<Arc<tenancy::runtime::TenantRuntime>>,
) -> AppState {
    AppState {
        db: None,
        store: None,
        resend_api_key: None,
        r2: None,
        http_client: reqwest::Client::new(),
        embedder: Arc::new(StubEmbedder),
        llm: None,
        rate_limiter: Arc::new(InMemoryRateLimiter::new()),
        admission: admission::AdmissionConfig::default(),
        in_flight: Arc::new(admission::InFlightRegistry::new()),
        metrics: std::sync::Arc::new(crate::telemetry::NoopMetrics),
        dek_cache: DekCache::new(),
        connector_flag_cache: connector_flags::ConnectorFlagCache::new(),
        tenant_runtime,
        // Production-like default (`is_dev: false`, `is_production: true`) —
        // see `AppEnv::default`'s doc comment. Tests that need the
        // non-production / dev branch override `.env` explicitly instead of
        // mutating `APP_ENV`.
        env: AppEnv::default(),
        // Fresh per test state: no harness runs under `cargo test`, so there
        // is nothing to wake. Tests that assert on the wake signal construct
        // a `TenantActivity` directly (see `workers::tenant_harness`'s tests).
        worker_activity: workers::tenant_harness::TenantActivity::new(),
        task_tracker: tokio_util::task::TaskTracker::new(),
        // Fresh per test state: no `/mcp` listener runs under `cargo test`,
        // so nothing ever cancels or observes this.
        mcp_cancellation_token: tokio_util::sync::CancellationToken::new(),
        // Fresh per test state — see the field's own doc comment.
        mcp_task_manager: mcp::server::McpTaskRegistry::new(),
        // Fresh per test state — see the field's own doc comment.
        mcp_ask_in_flight: mcp::server::AskInFlight::new(),
        stripe: None,
    }
}

/// [`test_app_state_owned`] with the consumer pool already attached. Route
/// tests that drive a real handler against Postgres need `state.db` set, but
/// a literal `state.db = …` inside a `routes/` file is indistinguishable from
/// an ambient-pool access to `scripts/check_no_ambient_pool.sh` (a line-based
/// grep with no test/`cfg` awareness — deliberately, per its header). Setting
/// the field HERE keeps the assignment out of the guard's scan instead of
/// teaching the guard to parse test modules.
#[cfg(any(test, feature = "test-support"))]
pub fn test_app_state_owned_with_db(
    db: sqlx::PgPool,
    tenant_runtime: Option<Arc<tenancy::runtime::TenantRuntime>>,
) -> AppState {
    let mut state = test_app_state_owned(tenant_runtime);
    state.db = Some(db);
    state
}

/// Same as [`test_app_state_owned_with_db`] but also wires the matching
/// [`store::PostgresStore`], for tests that exercise a CONTROL-plane read
/// (`state.control_store()`) rather than only a raw pool.
///
/// It lives HERE, in the crate root, rather than in the test module that wants
/// it: `scripts/check_no_ambient_pool.sh` forbids `state.db = ` / `state.store
/// = ` anywhere under `backend/src/routes/`, and rightly so — a route handler
/// reaching for the ambient pool instead of the `Tenant` extractor is a
/// cross-plane leak. That guard greps text and cannot tell a `#[cfg(test)]`
/// fixture from a handler, and the correct answer is NOT to add the route file
/// to the allowlist (which would blind the guard to that whole file forever,
/// weakening a security check to satisfy a test). Building the fixture outside
/// `routes/` keeps the guard at full strength and puts the assignment where
/// test scaffolding belongs.
#[cfg(feature = "test-support")]
pub fn test_app_state_owned_with_db_and_store(
    db: sqlx::PgPool,
    tenant_runtime: Option<Arc<tenancy::runtime::TenantRuntime>>,
) -> AppState {
    let mut state = test_app_state_owned_with_db(db.clone(), tenant_runtime);
    state.store = Some(Arc::new(store::PostgresStore::new(db)));
    state
}

/// [`test_app_state_owned_with_db_and_store`]'s INVERSE — `state.
/// control_store()` is wired (so an admin gate that needs it, e.g.
/// `routes::org_domains::refuse_personal_org`, still succeeds), but `state.
/// control_db()` reads `None`.
///
/// For a test that needs to reach a HANDLER'S OWN `let Some(control) =
/// state.control_db() else { … }` branch specifically, distinct from an
/// EARLIER admin-gate check that already needs `control_store()` to
/// succeed before the handler body even runs — `routes::org_domains::
/// pg_tests::the_two_503_causes_on_request_confirmation_are_distinguishable`
/// is the one that does. Lives here for the exact reason
/// [`test_app_state_owned_with_db_and_store`]'s own doc comment gives:
/// `scripts/check_no_ambient_pool.sh` forbids `state.db = ` under
/// `backend/src/routes/`, so the fixture is built outside that tree.
#[cfg(feature = "test-support")]
pub fn test_app_state_owned_with_store_only(
    db: sqlx::PgPool,
    tenant_runtime: Option<Arc<tenancy::runtime::TenantRuntime>>,
) -> AppState {
    let mut state = test_app_state_owned(tenant_runtime);
    state.store = Some(Arc::new(store::PostgresStore::new(db)));
    state
}

/// Test-only entry point onto the private [`app`] router, for the same reason
/// `test_app_state` above is exposed this way — `backend/tests/mcp_http.rs`
/// needs to drive real HTTP requests (`tower::oneshot`) against the actual
/// public router (open routes, `/v1`, and the conditionally-mounted MCP
/// routes) exactly as `run()` builds it, without duplicating `app()`'s route
/// table.
#[cfg(feature = "test-support")]
pub fn test_support_app(state: Arc<AppState>) -> Router {
    app(state)
}

/// The `/v1` route table as an already authenticated `user`: no `require_auth`
/// layer, the caller's [`UserContext`] injected as a request extension.
/// Integration fixtures layer their own `TenancyDecision` extension on top to
/// drive real handlers over HTTP without minting a Clerk session.
#[cfg(feature = "test-support")]
pub fn test_support_v1_app(state: Arc<AppState>, user: UserContext) -> Router {
    Router::new()
        .nest("/v1", v1_routes().layer(axum::Extension(user)))
        .layer(axum_middleware::from_fn(request_id_and_latency))
        .with_state(state)
}

/// The two workspace read handlers with an already authenticated tenant and actor.
/// Integration fixtures use this narrow HTTP seam to exercise query parsing,
/// response headers and handler authorization without exporting private routes.
#[cfg(feature = "test-support")]
pub fn test_support_workspace_app(tenant: tenancy::extractor::Tenant, user: UserContext) -> Router {
    let nodes_tenant = tenant.clone();
    let nodes_user = user.clone();
    let mut state = (*test_app_state()).clone();
    state.db = tenant.pool.clone();
    state.store = tenant.store.clone();
    let state = Arc::new(state);
    Router::new().route("/v1/nodes",axum::routing::get(move |query:axum::extract::Query<routes::nodes::ListNodesQuery>|{
        let tenant=nodes_tenant.clone();let user=nodes_user.clone();
        async move{routes::nodes::list_nodes(tenant,axum::Extension(user),query).await}
    })).route("/v1/graphs/{graph_id}/stats",axum::routing::get(move |path:axum::extract::Path<std::collections::BTreeMap<String,String>>,query:axum::extract::Query<std::collections::BTreeMap<String,String>>|{
        let mut tenant=tenant.clone();tenant.class=route_class::RouteClass::PageRead;
        let user=user.clone();let state=state.clone();
        async move{routes::graphs::get_ocean_stats(axum::extract::State(state),tenant,axum::Extension(user),None,path,query).await}
    }))
}

/// Project only actual legacy retrieval IDs for the cross-ocean supersession
/// regression. Cloud MCP uses retained evidence, so it cannot expose this
/// legacy annotation as a positive control. No model or provider is invoked.
#[cfg(feature = "test-support")]
pub async fn test_support_legacy_supersession_projection(
    store: &dyn store::Store,
    pool: &sqlx::PgPool,
    user: uuid::Uuid,
    graph: uuid::Uuid,
    query: &str,
) -> (Vec<uuid::Uuid>, std::collections::HashMap<uuid::Uuid, Vec<uuid::Uuid>>) {
    let retrieved = routes::queries::multi_graph_retrieve(
        store, Some(pool), user, &[graph], &[0.0; 384], query,
        &retrieval::RetrievalFilter::default(), 20,
    ).await;
    let atoms = retrieved.reranked.iter().map(|atom| atom.atom_id).collect();
    let superseders = retrieved.superseded_atoms.into_iter().map(|(atom, claims)| {
        (atom, claims.into_iter().map(|claim| claim.superseder_id).collect())
    }).collect();
    (atoms, superseders)
}

/// Same reasoning as [`test_support_app`], but for the separate [`internal_app`]
/// router (bound to its own loopback listener in `run()`, never merged into
/// `app()`) — `backend/tests/rotate_dek_pg.rs` needs to drive real HTTP
/// requests against `/internal/accounts/{id}/rotate-dek` (including its
/// `?tenant=` selector, org-tenancy PR 1.3) exactly as deployed.
#[cfg(feature = "test-support")]
pub fn test_support_internal_app(state: Arc<AppState>) -> Router {
    internal_app(state)
}

/// Test-only re-export of the Slack identity matcher's public alias.
///
/// `mod connectors` is private (unlike `pub mod workers`, which is why
/// `workers::huddle::test_support_slack_bot_token` needs no re-export), so
/// marking the function itself `pub` is not enough to make it reachable from
/// `backend/tests/*.rs`. Re-exported rather than making the whole `connectors`
/// module public, which would widen the real API surface for a test's
/// convenience — the thing this whole idiom exists to avoid.
///
/// Used by `backend/tests/chat_per_user_e2e_pg.rs`.
#[cfg(feature = "test-support")]
pub use connectors::slack::test_support_resolve_slack_identity;

/// Test-only redirect of the explicit-meeting Teams provider calls to a local mock
/// (feature `test-support` only). Same narrow re-export idiom as above.
#[cfg(feature = "test-support")]
pub use connectors::teams_targeted_transcripts::test_support_set_graph_origin;

/// Test-only re-export of the credential-death durable state (CS-453) —
/// `connectors::credential_health`'s set/clear/read helpers plus the three
/// small types their signatures need (`TokenOwner`, `ConnectorError`,
/// `CredentialDeathReason`). Same idiom, and the same reason, as
/// `test_support_resolve_slack_identity` just above — `mod connectors` is
/// private, so a narrow re-export is this crate's seam for a `backend/
/// tests/*.rs` integration test to reach an internal function without
/// widening the real API surface (making the whole module `pub`) for a
/// test's convenience.
///
/// Used by `backend/tests/credential_invalid_pg.rs`.
#[cfg(feature = "test-support")]
pub use connectors::credential_health::{
    clear_credential_invalid, is_credential_live, mark_credential_invalid,
    mark_slack_provider_user_revoked,
};
#[cfg(feature = "test-support")]
pub use connectors::{ConnectorError, CredentialDeathReason, TokenOwner};

/// Invoke the real disconnect endpoint against production-role RLS fixtures.
#[cfg(feature = "test-support")]
pub use routes::oauth::delete_connection;

/// Test-only re-exports for `backend/tests/unscoped_oauth_reads_rls_pg.rs`:
/// the Slack/Teams cold-start "someone can be emailed" checks and the two
/// Slack member-capture `oauth_tokens` reads. Same narrow-re-export idiom as
/// `test_support_resolve_slack_identity` above.
#[cfg(feature = "test-support")]
pub use connectors::cold_start::{
    test_support_slack_someone_can_be_emailed, test_support_teams_someone_can_be_emailed,
};
#[cfg(feature = "test-support")]
pub use connectors::slack_member_capture::{
    test_support_apply_membership_event, test_support_due_backfill_rows,
    test_support_invite_reader_user_id, test_support_member_bodies,
    test_support_member_capture_targets,
};

// ── Shared pg-tests catalog-price lock (CodeRabbit round 3, PR #660) ────────
//
// `clerk_provision::pg_tests`, `billing::seat_lifecycle::pg_tests`,
// `routes::billing_seats::pg_tests`, and `routes::checkout::pg_tests` each
// mutate the SAME global reference-data rows (`plans`/`seat_tiers`/
// `credit_packs` — no RLS, no per-test scoping possible), and this repo's
// `cargo test` default runs a single process's tests concurrently. Before
// this consolidation, three of those four modules each declared their OWN
// `CATALOG_PRICE_LOCK` (and `seat_lifecycle` declared none at all) — four
// independent mutexes guarding the SAME rows serialize nothing against EACH
// OTHER, so one suite could still capture another's in-flight temporary
// price as its own "original" and restore that bogus value once it
// finished. One lock, declared once, referenced by every module that
// touches this data, is the only way the serialization is real.
#[cfg(test)]
pub(crate) mod pg_catalog_test_support {
    /// `tokio::sync::Mutex` (not `std::sync::Mutex`): every caller holds the
    /// guard across `.await` points (DB round-trips, and in
    /// `routes::checkout`/`routes::billing_seats`'s case a full HTTP handler
    /// call) — holding a `std::sync::Mutex` guard across an await point is a
    /// real footgun under a multi-threaded executor and is correctly
    /// clippy-denied (`await_holding_lock`). Mirrors `billing::enforcement`'s
    /// `KILL_SWITCH_ENV_LOCK` for the identical "shared mutable global,
    /// parallel test threads" shape.
    pub(crate) static CATALOG_PRICE_LOCK: tokio::sync::Mutex<()> =
        tokio::sync::Mutex::const_new(());

    /// Sets `seat_tiers.stripe_price_id` for `code`, returning the PREVIOUS
    /// value. Every caller takes [`CATALOG_PRICE_LOCK`] first, then restores
    /// this return value (via a second call) and runs its own fixture
    /// cleanup BEFORE asserting anything: capture → restore → cleanup →
    /// assert. Restoring or cleaning up only on the all-assertions-passed
    /// path would leave the shared row (or an orphaned fixture) behind for
    /// good the moment any assertion in between panics.
    ///
    /// `Option<&str>`, not `&str` with an empty-string fallback on restore:
    /// `seat_tiers.stripe_price_id` starts `NULL` for every tier until real
    /// provisioning runs (migration `0165`), so the FIRST test in a given
    /// run to touch a tier's price captures `None` as its "original" —
    /// restoring via `unwrap_or("")` would then write the empty string,
    /// which trips `seat_tiers_stripe_price_id_check` (`stripe_price_id IS
    /// NULL OR stripe_price_id <> ''`) the moment any LATER test in the same
    /// run reads that poisoned row back as ITS "original" to restore to.
    /// Binding `Option<&str>` directly lets a `None` original restore to
    /// genuine `NULL`.
    pub(crate) async fn set_seat_tier_price(
        pool: &sqlx::PgPool,
        code: &str,
        price_id: Option<&str>,
    ) -> Option<String> {
        let previous: Option<String> =
            sqlx::query_scalar("SELECT stripe_price_id FROM seat_tiers WHERE code = $1")
                .bind(code)
                .fetch_one(pool)
                .await
                .expect("read seat_tiers stripe_price_id");
        sqlx::query("UPDATE seat_tiers SET stripe_price_id = $2 WHERE code = $1")
            .bind(code)
            .bind(price_id)
            .execute(pool)
            .await
            .expect("set seat_tiers stripe_price_id");
        previous
    }
}

// gsuite Train G2 PR 7 (CodeRabbit round 1): a `coralswarm_app`-role test-pool
// helper for `src/`-local pg-gated tests that need to prove behavior under
// the real least-privilege role rather than the superuser/table-owner
// connection. `backend/tests/support/app_role.rs` is the analogous helper for
// the `backend/tests/*_pg.rs` integration binaries, but — per that module's
// own doc comment — it is unreachable from here (`src/` and `tests/` are
// separate compilation units). Before this module existed, every `src/`-local
// suite that needed this role carried its own byte-identical copy
// (`workers::usage_pending_sweep`, `workers::mcp_task_sweep`, and — until this
// PR consolidated them — `workers::google_watch_renewal` and
// `connectors::google_gmail`). Consolidating those two PRE-EXISTING copies
// too is a separate follow-up, out of scope here — same "one copy per PR that
// actually needs a shared one, not a retroactive sweep" discipline
// `tests/support/app_role.rs`'s own doc comment already applies to its own
// four pre-existing siblings.
#[cfg(test)]
pub(crate) mod app_role_test_support {
    use sqlx::PgPool;

    /// Test-only password for the shared, cluster-wide `coralswarm_app` role
    /// (migration `0083_consumer_rls` creates it with no password). Not a
    /// secret — every scratch database this backs is ephemeral.
    pub(crate) const APP_ROLE_TEST_PASSWORD: &str = "gws_g27_shared_app_role_test_only_pw";

    /// `ALTER ROLE coralswarm_app WITH PASSWORD ...`, retried against the
    /// cluster-wide `pg_authid` race `tests/support/app_role.rs` documents
    /// and catches (`23505`/`42710`/`XX000` — two sessions racing the same
    /// shared-catalog write).
    pub(crate) async fn set_app_role_password_with_retry(pool: &PgPool) {
        let mut attempt: u32 = 0;
        loop {
            let result = sqlx::query(&format!(
                "ALTER ROLE coralswarm_app WITH PASSWORD '{APP_ROLE_TEST_PASSWORD}'"
            ))
            .execute(pool)
            .await;
            match result {
                Ok(_) => return,
                Err(sqlx::Error::Database(e))
                    if matches!(
                        e.code().as_deref(),
                        Some("23505") | Some("42710") | Some("XX000")
                    ) =>
                {
                    attempt += 1;
                    if attempt >= 10 {
                        panic!(
                            "set coralswarm_app test password: exhausted retries on a shared-catalog \
                             race ({}): {e}",
                            e.code().as_deref().unwrap_or("?")
                        );
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(20 * attempt as u64)).await;
                }
                Err(e) => panic!("set coralswarm_app test password failed: {e}"),
            }
        }
    }

    /// A pool connected AS `coralswarm_app` (a real login, not `SET ROLE`)
    /// against the same database `owner_pool`/`url` point at.
    pub(crate) async fn app_role_pool(owner_pool: &PgPool, url: &str) -> PgPool {
        set_app_role_password_with_retry(owner_pool).await;
        let opts = <sqlx::postgres::PgConnectOptions as std::str::FromStr>::from_str(url)
            .expect("parse test DB url")
            .username("coralswarm_app")
            .password(APP_ROLE_TEST_PASSWORD);
        sqlx::postgres::PgPoolOptions::new()
            .max_connections(4)
            .connect_with(opts)
            .await
            .expect("connect AS coralswarm_app")
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{Request as HttpRequest, StatusCode};
    use tower::ServiceExt; // for `oneshot`

    fn test_state() -> Arc<AppState> {
        // No database pool, Resend key, R2 client, or LLM — exercises the None
        // branch without requiring a live DB, email service, R2 credentials,
        // or ONNX model files.
        crate::test_app_state()
    }

    // ── DB pool env parsing (CodeRabbit, PR #255) ───────────────────────────────
    // These test the pure `parse_db_*` helpers directly with `Option<&str>`
    // inputs rather than mutating process-global env vars, so they need no
    // lock and can't leak state into other concurrently-running tests (the
    // prior version of these tests did both).

    #[test]
    fn cloud_embed_probe_success_and_error_pass_through() {
        let ok = map_cloud_embed_probe(Ok(Ok::<_, &'static str>(vec![0.1, 0.2])));
        assert_eq!(ok.unwrap(), vec![0.1, 0.2]);

        let fail = map_cloud_embed_probe(Ok(Err::<Vec<f32>, _>("upstream refused")));
        assert_eq!(fail.unwrap_err(), "upstream refused");
    }

    #[tokio::test]
    async fn cloud_embed_probe_timeout_maps_to_failure() {
        let timed = tokio::time::timeout(std::time::Duration::from_millis(20), async {
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
            Ok::<Vec<f32>, &'static str>(vec![1.0])
        })
        .await;
        assert!(
            timed.is_err(),
            "fixture future must exceed the probe budget"
        );
        assert_eq!(map_cloud_embed_probe(timed).unwrap_err(), "timed out");
    }

    #[test]
    fn db_max_connections_defaults_when_unset() {
        assert_eq!(parse_db_max_connections(None), DEFAULT_DB_MAX_CONNECTIONS);
    }

    #[test]
    fn db_max_connections_reads_override() {
        assert_eq!(parse_db_max_connections(Some("50")), 50);
    }

    #[test]
    fn db_max_connections_falls_back_on_invalid_or_zero() {
        for bad in ["not-a-number", "0", ""] {
            assert_eq!(
                parse_db_max_connections(Some(bad)),
                DEFAULT_DB_MAX_CONNECTIONS,
                "expected fallback for DB_MAX_CONNECTIONS={bad:?}"
            );
        }
    }

    #[test]
    fn db_max_connections_ceiling_boundary() {
        // At the ceiling: honored.
        assert_eq!(
            parse_db_max_connections(Some(&DB_MAX_CONNECTIONS_CEILING.to_string())),
            DB_MAX_CONNECTIONS_CEILING
        );
        // One above the ceiling: falls back (an operator typo must not blow
        // past what Fly Postgres has headroom for).
        assert_eq!(
            parse_db_max_connections(Some(&(DB_MAX_CONNECTIONS_CEILING + 1).to_string())),
            DEFAULT_DB_MAX_CONNECTIONS
        );
        // Wildly oversized (e.g. an extra zero or two) also falls back, and
        // parsing itself must not panic.
        assert_eq!(
            parse_db_max_connections(Some("100000")),
            DEFAULT_DB_MAX_CONNECTIONS
        );
    }

    #[test]
    fn db_acquire_timeout_secs_defaults_when_unset() {
        assert_eq!(
            parse_db_acquire_timeout_secs(None),
            DEFAULT_DB_ACQUIRE_TIMEOUT_SECS
        );
    }

    #[test]
    fn db_acquire_timeout_secs_reads_override() {
        assert_eq!(parse_db_acquire_timeout_secs(Some("15")), 15);
    }

    #[test]
    fn db_acquire_timeout_secs_falls_back_on_invalid_or_zero() {
        for bad in ["not-a-number", "0", ""] {
            assert_eq!(
                parse_db_acquire_timeout_secs(Some(bad)),
                DEFAULT_DB_ACQUIRE_TIMEOUT_SECS,
                "expected fallback for DB_ACQUIRE_TIMEOUT_SECS={bad:?}"
            );
        }
    }

    #[test]
    fn db_acquire_timeout_secs_ceiling_boundary() {
        // At the ceiling: honored.
        assert_eq!(
            parse_db_acquire_timeout_secs(Some(&DB_ACQUIRE_TIMEOUT_CEILING_SECS.to_string())),
            DB_ACQUIRE_TIMEOUT_CEILING_SECS
        );
        // One above the ceiling: falls back (this knob exists specifically to
        // fail faster than sqlx's 30s default, so it must not be able to
        // reintroduce — or exceed — a multi-minute stall).
        assert_eq!(
            parse_db_acquire_timeout_secs(Some(&(DB_ACQUIRE_TIMEOUT_CEILING_SECS + 1).to_string())),
            DEFAULT_DB_ACQUIRE_TIMEOUT_SECS
        );
        assert_eq!(
            parse_db_acquire_timeout_secs(Some("999999")),
            DEFAULT_DB_ACQUIRE_TIMEOUT_SECS
        );
    }

    // ── STRIPE_SECRET_KEY ALLOW-list (finding #3, PR #654 round 2) ──────────────
    // Same "pure function, no env mutation" convention as the DB pool knobs
    // above. Each real prefix is checked both as a bare prefix (rejected)
    // and with genuine key material after it (accepted), plus formats that
    // are real Stripe artifacts but not secret/restricted keys at all.

    #[test]
    fn stripe_secret_key_rejects_empty() {
        assert!(!is_valid_stripe_secret_key(""));
    }

    #[test]
    fn stripe_secret_key_rejects_bare_prefixes() {
        for bare in ["sk_test_", "sk_live_", "rk_test_", "rk_live_"] {
            assert!(
                !is_valid_stripe_secret_key(bare),
                "expected rejection for {bare:?}"
            );
        }
    }

    #[test]
    fn stripe_secret_key_accepts_real_keys_of_every_valid_prefix() {
        for good in [
            "sk_test_abc123",
            "sk_live_abc123",
            "rk_test_abc123",
            "rk_live_abc123",
        ] {
            assert!(
                is_valid_stripe_secret_key(good),
                "expected acceptance for {good:?}"
            );
        }
    }

    #[test]
    fn stripe_secret_key_rejects_other_stripe_artifact_formats() {
        // A publishable key and a webhook signing secret are real Stripe
        // values — just not ones this client should ever authenticate
        // with. The old denylist (bare-prefix only) would have let both of
        // these straight through to `StripeClient::new`.
        for wrong_shape in ["pk_test_REPLACE_WITH_YOUR_KEY", "pk_test_REPLACE_WITH_YOUR_KEY", "whsec_abc123"] {
            assert!(
                !is_valid_stripe_secret_key(wrong_shape),
                "expected rejection for {wrong_shape:?}"
            );
        }
    }

    #[test]
    fn stripe_secret_key_rejects_arbitrary_text() {
        for bad in ["not-a-key-at-all", "sk_test", "SK_TEST_abc123"] {
            assert!(
                !is_valid_stripe_secret_key(bad),
                "expected rejection for {bad:?}"
            );
        }
    }

    // ── Migration DSN resolution (`bin/migrate_control.rs`) ─────────────────────
    // Same "pure function, no env mutation" convention as the DB pool knobs
    // above — `resolve_migrator_url` takes its inputs as plain `Option<&str>`
    // so these tests need no lock and can't leak state into other
    // concurrently-running tests. Note the consumer: this resolution now runs
    // only inside the out-of-band migrator binary, never in `run()`.

    #[test]
    fn migrator_url_falls_back_to_app_url_when_unset() {
        // Pre-OPS-4: MIGRATOR_DATABASE_URL is unset and DATABASE_URL still IS
        // the elevated owner role, so the migrator uses it unchanged.
        assert_eq!(
            resolve_migrator_url(Some("postgres://owner@db/app"), None).as_deref(),
            Some("postgres://owner@db/app")
        );
    }

    #[test]
    fn migrator_url_falls_back_on_blank_override() {
        // An empty or whitespace-only override is treated the same as unset,
        // not as "connect to an empty DSN".
        for blank in ["", "   "] {
            assert_eq!(
                resolve_migrator_url(Some("postgres://owner@db/app"), Some(blank)).as_deref(),
                Some("postgres://owner@db/app"),
                "expected fallback for MIGRATOR_DATABASE_URL={blank:?}"
            );
        }
    }

    #[test]
    fn migrator_url_prefers_explicit_override() {
        // Post-OPS-4: DATABASE_URL is the least-privilege coralswarm_app role;
        // MIGRATOR_DATABASE_URL (the still-elevated owner DSN) must win.
        assert_eq!(
            resolve_migrator_url(
                Some("postgres://coralswarm_app@db/app"),
                Some("postgres://coralswarm_backend@db/app")
            )
            .as_deref(),
            Some("postgres://coralswarm_backend@db/app")
        );
    }

    #[test]
    fn migrator_url_trims_whitespace_on_override() {
        assert_eq!(
            resolve_migrator_url(
                Some("postgres://owner@db/app"),
                Some("  postgres://elevated@db/app  ")
            )
            .as_deref(),
            Some("postgres://elevated@db/app")
        );
    }

    #[test]
    fn migrator_url_is_none_when_neither_is_configured() {
        // Nothing to connect to — the binary must report a usage error and
        // exit non-zero rather than dialing an empty DSN.
        assert_eq!(resolve_migrator_url(None, None), None);
        assert_eq!(resolve_migrator_url(Some("  "), Some("")), None);
    }

    #[test]
    fn migrator_url_works_without_database_url_at_all() {
        // The migrator is an ops tool: an operator may export ONLY the
        // elevated DSN (the recommended shape — nothing else needs to be set).
        assert_eq!(
            resolve_migrator_url(None, Some("postgres://owner@db/app")).as_deref(),
            Some("postgres://owner@db/app")
        );
    }

    #[test]
    fn control_head_version_is_positive_and_matches_the_embedded_tree() {
        // Guards the preflight's comparison basis: `verify_control_schema`
        // refuses to serve when the DB's max applied version is below this, so
        // a head of 0 (an empty/misresolved embed) would silently disable it.
        let head = control_head_version();
        assert!(head > 0, "embedded control tree must be non-empty");
        assert_eq!(
            head,
            CONTROL_MIGRATOR.iter().map(|m| m.version).max().unwrap(),
            "head must be the max embedded version, not the last-listed one"
        );
    }

    #[test]
    fn redact_share_token_hides_token_segment() {
        let u = |s: &str| s.parse::<axum::http::Uri>().unwrap();
        // Share-view routes: the token segment is replaced.
        assert_eq!(
            redact_share_token(&u("/v1/shared/abc123SECRET")),
            "/v1/shared/{token}"
        );
        assert_eq!(
            redact_share_token(&u("/v1/meetings/shares/view/abc123SECRET")),
            "/v1/meetings/shares/view/{token}"
        );
        // Any query on a share-view route is dropped (these routes carry none).
        assert_eq!(
            redact_share_token(&u("/v1/shared/tok?foo=bar")),
            "/v1/shared/{token}"
        );
        // Non-token share routes and everything else are logged verbatim — the
        // revoke path carries a share *id* (a UUID), not the secret token.
        assert_eq!(
            redact_share_token(&u("/v1/meetings/shares?graph_id=g&source_id=s")),
            "/v1/meetings/shares?graph_id=g&source_id=s"
        );
        assert_eq!(
            redact_share_token(&u(
                "/v1/meetings/shares/11111111-1111-1111-1111-111111111111/revoke"
            )),
            "/v1/meetings/shares/11111111-1111-1111-1111-111111111111/revoke"
        );
        assert_eq!(redact_share_token(&u("/healthz")), "/healthz");
        // The bare prefix with no token is not mangled.
        assert_eq!(redact_share_token(&u("/v1/shared/")), "/v1/shared/");
    }

    #[tokio::test]
    async fn healthz_returns_200() {
        let router = app(test_state());
        let req = HttpRequest::builder()
            .uri("/healthz")
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    /// The connector catalog adds `GET /v1/connectors` and
    /// `GET /v1/connectors/{provider}` beside many static `/v1/connectors/...`
    /// paths. Axum prefers a static segment to a capture, so the old paths must
    /// keep their handlers; this proves both sides resolve with no database, by
    /// the answer each handler gives before it needs one.
    #[tokio::test]
    async fn v1_routes_keep_their_handlers_beside_the_catalog() {
        let user = UserContext {
            user_id: Uuid::new_v4(),
            account_id: Uuid::new_v4(),
            role: "member".into(),
            device_id: String::new(),
        };
        let router = Router::new()
            .nest("/v1", v1_routes().layer(axum::Extension(user)))
            .with_state(test_state());
        let call = |method: &'static str, uri: &'static str| {
            let router = router.clone();
            async move {
                let req = HttpRequest::builder()
                    .method(method)
                    .uri(uri)
                    .body(Body::empty())
                    .unwrap();
                let resp = router.oneshot(req).await.unwrap();
                let status = resp.status();
                let bytes = axum::body::to_bytes(resp.into_body(), 1 << 20).await.unwrap();
                (status, String::from_utf8_lossy(&bytes).into_owned())
            }
        };

        // New: the catalog. No database, so a known connector answers 503 and an
        // unknown one 404, which only the catalog handler does in that order.
        let (status, body) = call("GET", "/v1/connectors").await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{body}");
        let (status, body) = call("GET", "/v1/connectors/slack").await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{body}");
        let (status, body) = call("GET", "/v1/connectors/jira").await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{body}");
        let (status, body) = call("GET", "/v1/connectors/bitbucket").await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
        assert!(body.contains("unknown connector"), "{body}");

        // New: the scope and pause routes sit under `{provider}/sources/{source}`,
        // beside the static `/connectors/slack/channels` and friends. Each answers
        // from its own handler, in the order that handler checks: shape and
        // body first (no database), then the caller's connection (503 here).
        let send = |method: &'static str, uri: &'static str, body: &'static str| {
            let router = router.clone();
            async move {
                let req = HttpRequest::builder()
                    .method(method)
                    .uri(uri)
                    .header("content-type", "application/json")
                    .body(Body::from(body))
                    .unwrap();
                let resp = router.oneshot(req).await.unwrap();
                let status = resp.status();
                let bytes = axum::body::to_bytes(resp.into_body(), 1 << 20).await.unwrap();
                (status, String::from_utf8_lossy(&bytes).into_owned())
            }
        };
        let (status, body) =
            call("GET", "/v1/connectors/slack/sources/channels/scope-items?q=gen").await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{body}");
        let (status, body) =
            call("GET", "/v1/connectors/bitbucket/sources/repos/scope-items").await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
        let (status, body) = send(
            "PUT",
            "/v1/connectors/slack/sources/channels/scope",
            r#"{"mode":"pick","item_ids":["C1"]}"#,
        )
        .await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{body}");
        // A mode the shape does not allow is refused before the database is read.
        let (status, body) = send(
            "PUT",
            "/v1/connectors/jira/sources/workspace/scope",
            r#"{"mode":"pick","item_ids":["x"]}"#,
        )
        .await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
        let (status, body) = send(
            "PUT",
            "/v1/connectors/microsoft/sources/teams_meetings/scope",
            r#"{"mode":"everything"}"#,
        )
        .await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
        let (status, body) = send("PATCH", "/v1/connectors/slack/sources/channels", "{}").await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
        assert!(body.contains("nothing_to_change"), "{body}");
        let (status, body) = send(
            "PATCH",
            "/v1/connectors/slack/sources/channels",
            r#"{"paused":true}"#,
        )
        .await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{body}");
        let (status, body) = send("POST", "/v1/connectors/slack/sources/channels/backfill", r#"{"days":7}"#).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{body}");
        let (status, body) = send("POST", "/v1/connectors/slack/sources/dms/backfill", r#"{"days":7}"#).await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
        assert!(body.contains("unknown source"), "{body}");
        let (status, body) = send("POST", "/v1/connectors/bitbucket/sources/x/backfill", r#"{"days":7}"#).await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
        assert!(body.contains("unknown connector"), "{body}");

        // Old: `/connectors/composio` is the toolkit list, a static path the
        // `{provider}` capture must not swallow.
        // Retired toolkit UI endpoints must not fall through to legacy handlers.
        for (method, uri) in [
            ("GET", "/v1/oauth/google/workspace"),
            ("GET", "/v1/connectors/composio"),
            ("GET", "/v1/connectors/composio/jira"),
            ("POST", "/v1/connectors/composio/jira/connect"),
            ("POST", "/v1/connectors/composio/jira/connections/test/verify"),
            ("POST", "/v1/connectors/composio/jira/connections/test/disconnect"),
            ("GET", "/v1/connectors/slack/onboarding"),
            ("POST", "/v1/connectors/slack/backfill"),
            ("GET", "/v1/connectors/microsoft/teams"),
            ("GET", "/v1/connectors/microsoft/teams/channels"),
            ("GET", "/v1/connectors/microsoft/teams/chats"),
            ("POST", "/v1/connectors/microsoft/teams/ingest"),
            ("GET", "/v1/connectors/microsoft/teams/watched"),
            ("DELETE", "/v1/connectors/microsoft/teams/watched/00000000-0000-0000-0000-000000000001"),
        ] {
            let (status, body) = call(method, uri).await;
            assert_eq!(status, StatusCode::NOT_FOUND, "{method} {uri}: {body}");
        }

        // Old, more static paths and the `{provider}/sync` capture route: each
        // still resolves to something other than the catalog's 404.
        for (method, uri) in [
            ("GET", "/v1/connectors/slack/channels"),
            ("POST", "/v1/connectors/microsoft/teams/onboarding"),
            ("POST", "/v1/connectors/microsoft/teams/install"),
            ("PUT", "/v1/connectors/github/repos"),
            ("GET", "/v1/connectors/github/installs"),
            ("GET", "/v1/connectors/github/repos?installation_id=1"),
            ("GET", "/v1/connectors/microsoft/workflows"),
            ("GET", "/v1/connectors/google/meet/meetings"),
            ("GET", "/v1/connectors/google/chat/spaces"),
            ("GET", "/v1/connectors/slack/watched"),
            ("GET", "/v1/oauth/connections"),
            ("GET", "/v1/addin/tokens"),
            ("DELETE", "/v1/addin/tokens/00000000-0000-0000-0000-000000000001"),
            ("POST", "/v1/connectors/google/meet/captures"),
            ("POST", "/v1/connectors/microsoft/teams/transcripts/capture"),
            ("GET", "/v1/connectors/microsoft/everything-mode"),
            ("GET", "/v1/connectors/google/gmail/everything-mode"),
            ("POST", "/v1/connectors/slack/sync"),
        ] {
            let (status, body) = call(method, uri).await;
            assert_ne!(status, StatusCode::NOT_FOUND, "{method} {uri}: {body}");
            assert_ne!(status, StatusCode::METHOD_NOT_ALLOWED, "{method} {uri}: {body}");
            assert!(!body.contains("unknown connector"), "{method} {uri}: {body}");
        }
    }

    #[tokio::test]
    async fn readyz_returns_503_without_db() {
        let router = app(test_state());
        let req = HttpRequest::builder()
            .uri("/readyz")
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn openapi_json_returns_200_in_dev() {
        // Explicit non-production `AppEnv` (test-flakiness follow-up to
        // org-tenancy PR 3.3) instead of mutating the process-global
        // `APP_ENV` — `test_state()`'s default is production-like
        // (`is_production: true`), which would mount NO OpenAPI docs router
        // at all, so this test overrides `env` directly to exercise the
        // non-production branch deterministically. No lock needed: this
        // `AppState` is local to this test and never touches global env, so
        // it cannot race any other concurrently-running `app()`-building
        // test in this binary.
        let mut state = (*test_state()).clone();
        state.env = AppEnv {
            is_dev: true,
            is_production: false,
        };
        let router = app(Arc::new(state));
        let req = HttpRequest::builder()
            .uri("/openapi.json")
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn v1_graphs_requires_auth() {
        let router = app(test_state());
        let req = HttpRequest::builder()
            .uri("/v1/graphs")
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        // No auth header → 401 from require_auth middleware.
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn v1_graphs_bearer_no_db_returns_503() {
        let router = app(test_state());
        let req = HttpRequest::builder()
            .uri("/v1/graphs")
            .header("Authorization", "Bearer sometoken")
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        // db=None → 503 from require_auth middleware.
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    // ── POST /v1/queries tests ────────────────────────────────────────────────

    #[tokio::test]
    async fn v1_queries_requires_auth() {
        let router = app(test_state());
        let req = HttpRequest::builder()
            .method("POST")
            .uri("/v1/queries")
            .header("content-type", "application/json")
            .body(Body::from(r#"{"text":"hello"}"#))
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        // No auth header → 401 from require_auth middleware.
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn v1_queries_bearer_no_db_returns_503() {
        let router = app(test_state());
        let req = HttpRequest::builder()
            .method("POST")
            .uri("/v1/queries")
            .header("Authorization", "Bearer sometoken")
            .header("content-type", "application/json")
            .body(Body::from(r#"{"text":"hello"}"#))
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        // db=None → 503 from require_auth middleware before we reach the handler.
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    // ── Retired org API address (claim-based tenancy Phase 4, step 3) ────────

    /// The retirement, over every host shape — **with no exceptions**.
    ///
    /// `is_retired_org_host_request` is the ENTIRE decision
    /// `retired_org_host_guard` makes, extracted so it can be exercised here
    /// without a database, a router or a token. Replaces
    /// `the_retired_org_address_is_answered_for_every_shape_but_the_mcp_seam`
    /// (this same test, pre-Phase-3-PR-3.3: `/mcp` and its three
    /// `/.well-known/oauth-*` documents were the seam
    /// `RETIRED_ORG_HOST_MCP_EXEMPT_PATHS` held open) with the new truth: PR
    /// 3.3 retired `/mcp`'s OWN, independent org-host classification, so
    /// there is no longer anything left on this host for those four paths to
    /// exempt, and `is_retired_org_host_request` no longer even takes a path
    /// — it is a pure function of `host` alone now (see its own doc).
    ///
    /// The one property this still pins that a status-code test would not:
    /// **No oracle.** `acme` (a real slug in production) and `zzz-nobody`
    /// (never provisioned) must be treated identically. The decision reads no
    /// routing row precisely so it cannot differ.
    #[test]
    fn the_retired_org_address_is_answered_for_every_shape_with_no_exceptions() {
        const APEX: &str = "api.coralswarm.example";
        /// Host shapes covered — written as a literal, not `.len()`.
        const HOST_SHAPE_COUNT: usize = 6;

        // (host, is a retired org address?)
        let hosts: [(&str, bool); HOST_SHAPE_COUNT] = [
            // A slug that really exists in production...
            ("acme.api.coralswarm.example", true),
            // ...and one that never has. Identical treatment or it is an oracle.
            ("zzz-nobody.api.coralswarm.example", true),
            // Canonicalization must not be a bypass.
            ("  ACME.API.CoralSwarm.com.:443 ", true),
            // The replacement address itself.
            (APEX, false),
            // Not ours.
            ("evil.com", false),
            // Nested label — `classify_host` calls this Foreign, and it must
            // keep getting the router's own 404 rather than a "gone" that
            // implies we once served it.
            ("a.b.api.coralswarm.example", false),
        ];

        let mut checked = 0usize;
        let mut retired = 0usize;
        for (host, expected) in hosts {
            assert_eq!(
                is_retired_org_host_request(host, APEX),
                expected,
                "host={host}: expected retired={expected}"
            );
            checked += 1;
            if expected {
                retired += 1;
            }
        }
        assert_eq!(
            checked, HOST_SHAPE_COUNT,
            "only {checked} hosts were checked — a matrix that checks nothing passes for free"
        );
        assert!(
            retired > 0,
            "nothing was classified as retired, so this asserted only that the guard \
             never fires"
        );
        assert!(
            checked - retired > 0,
            "everything was classified as retired, so the guard would 410 the apex too"
        );

        // Named separately as well as covered above, because it is the exact
        // pair a reviewer will look for: the real slug and the imaginary one
        // must be indistinguishable.
        assert_eq!(
            is_retired_org_host_request("acme.api.coralswarm.example", APEX),
            is_retired_org_host_request("zzz-nobody.api.coralswarm.example", APEX),
            "a slug that exists and one that does not must be answered identically, or \
             the 410 is an org-existence oracle"
        );
    }

    /// **Where the guard sits in the layer stack**, pinned as behaviour.
    ///
    /// `retired_org_host_guard` is added FIRST in `app()`, which makes it the
    /// innermost of the four outer layers and therefore outside every route —
    /// including the ones the binding gate never sees. Nothing in the type
    /// system says so: move the `.layer(...)` call onto the `/v1` sub-router,
    /// or fold the check into `require_auth`, and every other test in this
    /// repository still passes while the open routes and the OAuth discovery
    /// documents quietly keep answering on a retired address.
    ///
    /// `/healthz` is the pin because it is the clearest route that lives
    /// OUTSIDE `require_auth` (`app()`'s `open` router). If this returns 200,
    /// the guard has been moved behind authentication and the retirement now
    /// covers only `/v1`.
    ///
    /// Three properties, one request each:
    ///
    /// * an unauthenticated open route on a retired host is 410 — so the guard
    ///   is outside `require_auth` AND runs before any token check (a 401 here
    ///   would mean auth ran first);
    /// * the SAME route on the apex is untouched, so this is not a guard that
    ///   fires on everything;
    /// * a `/v1` route with no token is 410 rather than the 401 it gets on the
    ///   apex — the same ordering property, on the other side of the layer.
    #[tokio::test]
    async fn the_retired_host_guard_sits_outside_require_auth_not_inside_it() {
        const APEX: &str = "api.coralswarm.example";
        let mut state = (*test_state()).clone();
        state.tenant_runtime = Some(crate::tenancy::runtime::TenantRuntime::new(
            true,
            APEX,
            Arc::new(crate::tenancy::registry::TenantRegistry::new(Arc::new(
                crate::tenancy::secrets::StaticMapResolver::new(Default::default()),
            ))),
        ));
        let router = app(Arc::new(state));

        let get = |host: &'static str, uri: &'static str| {
            HttpRequest::builder()
                .uri(uri)
                .header("host", host)
                .body(Body::empty())
                .unwrap()
        };

        // An OPEN route — `require_auth` never runs for it. This is the whole
        // point of the layer's position.
        let resp = router
            .clone()
            .oneshot(get("acme.api.coralswarm.example", "/healthz"))
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::GONE,
            "/healthz on a retired org host must be 410. A 200 means the guard has been \
             moved behind `require_auth`, so the open routes and the /.well-known/oauth-* \
             discovery documents are still being served on an address that is supposed to \
             be gone"
        );

        // Control: the same open route on the apex is untouched. Without this,
        // a guard that 410'd everything would satisfy the assertion above.
        let resp = router.clone().oneshot(get(APEX, "/healthz")).await.unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::OK,
            "the apex must still serve /healthz — the guard fires on the retired host only"
        );

        // The other side of the same ordering property: a protected route with
        // NO token. 410 means the Host was classified before the token was
        // looked at; 401 would mean authentication ran first.
        let resp = router
            .clone()
            .oneshot(get("acme.api.coralswarm.example", "/v1/graphs"))
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::GONE,
            "an unauthenticated /v1 request on a retired org host must be 410, not 401 — \
             the retired address must not ask for credentials it will never accept"
        );
        // ...and the same request on the apex still asks for a token, proving
        // the 410 above is the guard and not a broken auth layer.
        let resp = router
            .clone()
            .oneshot(get(APEX, "/v1/graphs"))
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::UNAUTHORIZED,
            "control: the apex still challenges an unauthenticated /v1 request"
        );

        // `/mcp` itself, end to end, through the REAL router — the strongest
        // available proof that `RETIRED_ORG_HOST_MCP_EXEMPT_PATHS`'s deletion
        // (claim-based-tenancy §Phase 3 PR 3.3) actually took effect, not just
        // that `is_retired_org_host_request`'s unit test agrees with itself.
        // `MCP_ENABLED` is unset in this harness, so `/mcp` isn't even mounted
        // — and that is exactly the point: this guard's verdict must not
        // depend on whether the path resolves to a real route at all, only on
        // the Host.
        let resp = router
            .oneshot(get("acme.api.coralswarm.example", "/mcp"))
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::GONE,
            "/mcp on a retired org host must now be 410 too — there is no more exemption"
        );
    }

    // `redaction_can_never_become_a_guard_bypass` (guarded the risk that
    // redacting a share token before deciding could accidentally manufacture
    // one of `RETIRED_ORG_HOST_MCP_EXEMPT_PATHS`'s four exempt paths) is
    // DELETED, not merely updated, as of claim-based-tenancy §Phase 3 PR 3.3:
    // `is_retired_org_host_request` no longer takes a path at all (see its
    // own doc), so there is no longer an "exempt path" for redaction to
    // accidentally produce — the property this test guarded has no
    // representation left to guard. This is a deliberate deletion of the
    // mechanism, not an oversight; contrast with the vestigial-but-not-
    // orphaned code elsewhere in this PR that is merely unreachable.

    /// The 410 body must NAME the replacement base, and must name the
    /// CONFIGURED one — a deployment that advertises a base it does not serve
    /// is worse than a bare status code.
    #[tokio::test]
    async fn the_retired_org_address_response_names_the_configured_apex() {
        let resp = retired_org_host_response("api.example.test");
        assert_eq!(resp.status(), StatusCode::GONE);
        let body = axum::body::to_bytes(resp.into_body(), 64 * 1024)
            .await
            .unwrap();
        let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let msg = v["error"].as_str().unwrap_or_default();
        assert!(
            msg.contains("https://api.example.test"),
            "the 410 must name the base to use instead: {msg}"
        );
        assert!(
            !msg.contains("coralswarm.example"),
            "the message must come from the configured apex, not a hardcoded one: {msg}"
        );
    }

    // ── Cloud CORS predicate (org-tenancy PR 3.3) ───────────────────────────────

    const CORS_APEX: &str = "coralswarm.example";

    #[test]
    fn cors_allows_apex_and_app_www_hosts() {
        for origin in [
            "https://coralswarm.example",
            "https://app.coralswarm.example",
            "https://www.coralswarm.example",
        ] {
            assert!(
                is_allowed_cors_origin(origin, CORS_APEX, CORS_APEX, false),
                "expected {origin} to be allowed"
            );
        }
    }

    /// INVERTED by claim-based tenancy PR 1.3 (was
    /// `cors_allows_valid_org_subdomain`). A tenant subdomain is no longer a
    /// place a browser page is served from, so it is no longer a legitimate
    /// origin — even though `classify_host` still resolves it while the
    /// `Subdomain` arm survives for the transition.
    #[test]
    fn cors_rejects_org_subdomain_origins() {
        for origin in [
            "https://acme.coralswarm.example",
            "https://testorg.coralswarm.example",
            "https://acme.api.coralswarm.example",
        ] {
            assert!(
                !is_allowed_cors_origin(origin, CORS_APEX, CORS_APEX, false),
                "expected the tenant subdomain origin {origin} to be rejected"
            );
        }
    }

    #[test]
    fn cors_rejects_arbitrary_and_lookalike_origins() {
        for origin in [
            "https://evil.com",
            "https://coralswarm.example.evil.com",
            "https://acme.coralswarm.example.evil.com",
            "https://a.b.coralswarm.example", // nested subdomain — not a single-label tenant
        ] {
            assert!(
                !is_allowed_cors_origin(origin, CORS_APEX, CORS_APEX, false),
                "expected {origin} to be rejected"
            );
        }
    }

    /// The desktop app is retired: its webview origins (and the release build's
    /// `http://localhost:47821` asset origin) are no longer cloud CORS origins.
    #[test]
    fn cors_rejects_retired_desktop_webview_origins_in_prod() {
        for origin in [
            "tauri://localhost",
            "http://tauri.localhost",
            "http://localhost:3000",
            "http://localhost:47821",
        ] {
            assert!(!is_allowed_cors_origin(origin, CORS_APEX, CORS_APEX, false), "{origin}");
        }
    }

    #[test]
    fn cors_localhost_dev_port_allowed_only_in_dev() {
        let origin = "http://localhost:5173";
        assert!(is_allowed_cors_origin(origin, CORS_APEX, CORS_APEX, true));
        assert!(!is_allowed_cors_origin(origin, CORS_APEX, CORS_APEX, false));
    }

    #[test]
    fn cors_localhost_dev_rejects_non_numeric_port_and_paths() {
        assert!(!is_localhost_dev_origin("http://localhost:abc"));
        assert!(!is_localhost_dev_origin("http://localhost"));
        assert!(!is_localhost_dev_origin("http://localhost:3000/evil"));
        assert!(is_localhost_dev_origin("http://127.0.0.1:5173"));
    }

    /// P2 hardening (security review of org-tenancy PR 3.3): real
    /// apex/subdomain origins must be `https://` — a scheme-downgraded
    /// `http://` origin against the SAME, otherwise-valid subdomain host must
    /// be rejected, not silently allowed through by ignoring the scheme.
    #[test]
    fn cors_requires_https_for_apex_origins() {
        // Every host here is an ACCEPTED apex host over https, so each pair
        // isolates the scheme rule. (PR 1.3 narrowed this test off the
        // `acme.` subdomain it used to use: that host is now rejected on both
        // schemes, which would have made the http case pass for the wrong
        // reason — a scheme test that no longer tests the scheme.)
        for host in ["coralswarm.example", "app.coralswarm.example", "www.coralswarm.example"] {
            assert!(
                is_allowed_cors_origin(&format!("https://{host}"), CORS_APEX, CORS_APEX, false),
                "https://{host} must be allowed"
            );
            assert!(
                !is_allowed_cors_origin(&format!("http://{host}"), CORS_APEX, CORS_APEX, false),
                "http://{host} must be refused — scheme downgrade"
            );
        }
    }

    /// Split apexes: UI at `coralswarm.example`, API at `api.coralswarm.example`.
    /// With `TENANT_APEX_DOMAIN=api.coralswarm.example` the browser's `Origin`
    /// classifies `Foreign` against the API apex, so accepting only that apex
    /// would reject the entire product's own UI. Both apexes are matched;
    /// nothing else is.
    #[test]
    fn cors_allows_both_apexes_and_nothing_below_them() {
        const API_APEX: &str = "api.coralswarm.example";
        const WEB_APEX: &str = "coralswarm.example";

        for origin in [
            // The UI apex and its reserved labels — what a real browser sends.
            "https://coralswarm.example",
            "https://www.coralswarm.example",
            "https://app.coralswarm.example",
            // The API apex itself.
            "https://api.coralswarm.example",
        ] {
            assert!(
                is_allowed_cors_origin(origin, API_APEX, WEB_APEX, false),
                "expected {origin} to be allowed under the split-apex topology"
            );
        }

        for origin in [
            "https://evil.com",
            "https://coralswarm.example.evil.com",
            "https://acme.coralswarm.example.evil.com",
            // Nested/multi-label against BOTH apexes.
            "https://a.b.coralswarm.example",
            // Scheme downgrade on the web apex.
            "http://coralswarm.example",
            // NARROWED by PR 1.3: tenant subdomains on either apex. These
            // were allowed before; nothing is served from them now.
            "https://acme.coralswarm.example",
            "https://acme.api.coralswarm.example",
        ] {
            assert!(
                !is_allowed_cors_origin(origin, API_APEX, WEB_APEX, false),
                "expected {origin} to be rejected under the split-apex topology"
            );
        }
    }

    /// `WEB_APEX_DOMAIN` unset ⇒ `web_apex == apex` ⇒ byte-identical to the
    /// single-apex policy: the second apex can only ever ADD the operator's
    /// own UI domain, never change today's verdicts.
    #[test]
    fn cors_web_apex_defaults_to_api_apex() {
        let rt = crate::tenancy::runtime::TenantRuntime::new(
            true,
            CORS_APEX,
            Arc::new(crate::tenancy::registry::TenantRegistry::new(Arc::new(
                crate::tenancy::secrets::StaticMapResolver::new(Default::default()),
            ))),
        );
        assert_eq!(rt.web_apex, rt.apex);
        for origin in [
            "https://coralswarm.example",
            "https://acme.coralswarm.example",
            "https://evil.com",
            "http://acme.coralswarm.example",
        ] {
            assert_eq!(
                is_allowed_cors_origin(origin, &rt.apex, &rt.web_apex, false),
                is_allowed_cors_origin(origin, CORS_APEX, CORS_APEX, false),
                "{origin}: an unset WEB_APEX_DOMAIN must not change any verdict"
            );
        }
    }

    /// Flag-off (no `TenantRuntime` — `TENANT_APEX_DOMAIN` unset): the public
    /// router's CORS layer must stay EXACTLY today's `allow_origin(Any)`
    /// wildcard, byte-identical to the current single-domain deploy.
    #[tokio::test]
    async fn public_cors_flag_off_preserves_wildcard() {
        let router = app(test_state());
        let req = HttpRequest::builder()
            .uri("/healthz")
            .header("origin", "https://totally-arbitrary-site.example")
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(
            resp.headers()
                .get("access-control-allow-origin")
                .and_then(|v| v.to_str().ok()),
            Some("*"),
            "flag-off CORS must remain the wildcard today's single-domain deploy relies on"
        );
    }

    /// Flag-on (`TenantRuntime` configured with an apex): the real public
    /// router's CORS layer must allow the apex + a valid org subdomain and
    /// reject an arbitrary origin — proving the wiring in `app()`, not just
    /// the pure predicate above.
    #[tokio::test]
    async fn public_cors_flag_on_restricts_to_the_apex_only() {
        let mut state = (*test_state()).clone();
        state.tenant_runtime = Some(crate::tenancy::runtime::TenantRuntime::new(
            true,
            CORS_APEX,
            Arc::new(crate::tenancy::registry::TenantRegistry::new(Arc::new(
                crate::tenancy::secrets::StaticMapResolver::new(Default::default()),
            ))),
        ));
        let router = app(Arc::new(state));

        let hit = |origin: &'static str| {
            HttpRequest::builder()
                .uri("/healthz")
                .header("origin", origin)
                .body(Body::empty())
                .unwrap()
        };

        // Apex itself: allowed.
        let resp = router
            .clone()
            .oneshot(hit("https://coralswarm.example"))
            .await
            .unwrap();
        assert_eq!(
            resp.headers()
                .get("access-control-allow-origin")
                .and_then(|v| v.to_str().ok()),
            Some("https://coralswarm.example")
        );

        // The app label: allowed.
        let resp = router
            .clone()
            .oneshot(hit("https://app.coralswarm.example"))
            .await
            .unwrap();
        assert_eq!(
            resp.headers()
                .get("access-control-allow-origin")
                .and_then(|v| v.to_str().ok()),
            Some("https://app.coralswarm.example")
        );

        // NARROWED by claim-based tenancy PR 1.3: a tenant subdomain used to
        // be granted here. Nothing is served from one now, so it gets no
        // grant — asserted through the REAL router, not just the predicate.
        let resp = router
            .clone()
            .oneshot(hit("https://acme.coralswarm.example"))
            .await
            .unwrap();
        assert!(
            resp.headers().get("access-control-allow-origin").is_none(),
            "a tenant subdomain origin must no longer be granted CORS access"
        );

        // An arbitrary origin: no CORS header at all (rejected).
        let resp = router
            .clone()
            .oneshot(hit("https://evil.example"))
            .await
            .unwrap();
        assert!(
            resp.headers().get("access-control-allow-origin").is_none(),
            "an arbitrary origin must not be granted CORS access under the apex-scoped policy"
        );
    }

    /// The T1 deploy shape, end-to-end through the REAL router: apex
    /// `api.coralswarm.example` (what `TENANT_APEX_DOMAIN` will be set to) with a
    /// web apex of `coralswarm.example`, `is_dev == false` as in production. The
    /// UI's own org origin must get a CORS grant; a foreign origin must not.
    #[tokio::test]
    async fn public_cors_split_apex_allows_ui_in_prod() {
        let mut state = (*test_state()).clone();
        state.env = AppEnv {
            is_dev: false,
            is_production: true,
        };
        state.tenant_runtime = Some(crate::tenancy::runtime::TenantRuntime::new_with_web_apex(
            true,
            "api.coralswarm.example",
            "coralswarm.example",
            Arc::new(crate::tenancy::registry::TenantRegistry::new(Arc::new(
                crate::tenancy::secrets::StaticMapResolver::new(Default::default()),
            ))),
        ));
        let router = app(Arc::new(state));

        let hit = |origin: &str| {
            HttpRequest::builder()
                .uri("/healthz")
                .header("origin", origin)
                .body(Body::empty())
                .unwrap()
        };
        let allow_origin = |resp: &Response| {
            resp.headers()
                .get("access-control-allow-origin")
                .and_then(|v| v.to_str().ok())
                .map(str::to_owned)
        };

        // The UI origin — `Foreign` against the API apex, allowed via the web
        // apex. This is `app.<web_apex>` now rather than `<slug>.<web_apex>`:
        // claim-based tenancy serves every tenant from the one host.
        let resp = router
            .clone()
            .oneshot(hit("https://app.coralswarm.example"))
            .await
            .unwrap();
        assert_eq!(
            allow_origin(&resp).as_deref(),
            Some("https://app.coralswarm.example"),
            "the UI origin must be granted CORS by the real router"
        );

        // ...and the tenant subdomain it replaced must not be.
        let resp = router
            .clone()
            .oneshot(hit("https://acme.coralswarm.example"))
            .await
            .unwrap();
        assert_eq!(
            allow_origin(&resp).as_deref(),
            None,
            "a tenant subdomain origin must not be granted CORS under the split-apex topology"
        );

        // Genuinely foreign, or the retired desktop app's webview: rejected.
        for origin in [
            "https://evil.example",
            "https://acme.coralswarm.example.evil.com",
            "http://localhost:47821",
        ] {
            let resp = router.clone().oneshot(hit(origin)).await.unwrap();
            assert!(
                allow_origin(&resp).is_none(),
                "{origin} must not be granted CORS access under the split-apex policy"
            );
        }
    }

    /// Org subdomains are retired: EVERY caller of `org_web_base_url` /
    /// `web_base_url` (the OAuth-return redirects incl. the Teams/Outlook
    /// meeting-pane `/teams/meeting/connected` return, digest/nudge emails,
    /// cold-start links) must land on the apex app host, whatever the org and
    /// whatever its routing row says. Proven with a pool that CANNOT connect:
    /// the old implementation queried `tenant_databases` and would have fallen
    /// back (with a warning) rather than answering — this one must not need the
    /// database at all. FAILS against the pre-fix `{slug}.{web_apex}` builder
    /// (see the pg suite `route_migration_org_pg` for the routing-row cases).
    #[tokio::test]
    async fn org_web_base_url_is_the_apex_app_host_for_every_org_without_querying() {
        let mut state = (*test_state()).clone();
        state.tenant_runtime = Some(crate::tenancy::runtime::TenantRuntime::new_with_web_apex(
            true,
            "api.coralswarm.example",
            "coralswarm.example",
            Arc::new(crate::tenancy::registry::TenantRegistry::new(Arc::new(
                crate::tenancy::secrets::StaticMapResolver::new(Default::default()),
            ))),
        ));
        let dead = sqlx::postgres::PgPoolOptions::new()
            .acquire_timeout(std::time::Duration::from_millis(50))
            .connect_lazy("postgres://127.0.0.1:1/none")
            .expect("lazy pool construction never connects");
        for org in ["org_acme", "org_with_a_routing_row", "org_with_none", ""] {
            assert_eq!(
                state.org_web_base_url(&dead, org).await,
                "https://app.coralswarm.example",
                "{org:?}: an org link must land on the apex app host"
            );
            assert_eq!(
                state.web_base_url(&dead, Some(org)).await,
                "https://app.coralswarm.example",
                "{org:?}: the dispatcher must agree"
            );
        }
        assert_eq!(state.web_base_url(&dead, None).await, "https://app.coralswarm.example");
        assert_eq!(
            state.org_web_base_url(&dead, "org_acme").await,
            state.personal_web_base_url()
        );
    }

    /// P2 hardening (security review of org-tenancy PR 3.3): `is_dev` must
    /// fail CLOSED. With `APP_ENV` unset (a misconfigured-prod scenario, or
    /// plain CI), a localhost dev origin must NOT be granted CORS access —
    /// only an explicit `APP_ENV=development` may open that door.
    #[tokio::test]
    async fn public_cors_localhost_rejected_when_app_env_unset() {
        // Explicit `AppEnv` — the APP_ENV-unset-equivalent posture — instead
        // of mutating the process-global `APP_ENV` (test-flakiness follow-up
        // to org-tenancy PR 3.3). `test_state()`'s default (`AppEnv::default`)
        // already has `is_dev: false`, exactly matching "APP_ENV unset"
        // semantics, but it's set explicitly here so the test's intent reads
        // clearly and doesn't silently depend on `test_state()`'s default
        // never changing. This `AppState` is local to this test and never
        // touches global env, so — unlike the old `remove_var` version — it
        // cannot race `openapi_json_returns_200_in_dev` or any other
        // concurrently-running `app()`-building test in this binary; no lock
        // needed.
        let mut state = (*test_state()).clone();
        state.env = AppEnv {
            is_dev: false,
            is_production: true,
        };
        state.tenant_runtime = Some(crate::tenancy::runtime::TenantRuntime::new(
            true,
            CORS_APEX,
            Arc::new(crate::tenancy::registry::TenantRegistry::new(Arc::new(
                crate::tenancy::secrets::StaticMapResolver::new(Default::default()),
            ))),
        ));
        let router = app(Arc::new(state));

        let req = HttpRequest::builder()
            .uri("/healthz")
            .header("origin", "http://localhost:5173")
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert!(
            resp.headers().get("access-control-allow-origin").is_none(),
            "APP_ENV unset must fail CLOSED: no dev-permissive localhost CORS without an \
             explicit APP_ENV=development signal"
        );
    }

    // ── Server-Timing + access log (perf observability step 1) ───────────────

    #[derive(Clone, Default)]
    struct LogBuf(Arc<std::sync::Mutex<Vec<u8>>>);

    impl std::io::Write for LogBuf {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for LogBuf {
        type Writer = Self;
        fn make_writer(&'a self) -> Self {
            self.clone()
        }
    }

    /// Same layer stack shape as `app()`: a nested `/v1` router whose
    /// `require_auth` slot is the marker, wrapped by the real access-log
    /// middleware at the top. The 5xx route proves the log level split and
    /// the ledger key both use the template.
    #[tokio::test(flavor = "current_thread")]
    async fn access_log_uses_the_route_template_and_response_carries_server_timing() {
        let v1 = Router::new()
            .route(
                "/graphs/{graph_id}/stats",
                get(|| async {
                    crate::request_timing::record_tx_open(std::time::Duration::from_micros(700));
                    StatusCode::OK
                }),
            )
            .route(
                "/library/{graph_id}/boom",
                get(|| async { StatusCode::SERVICE_UNAVAILABLE }),
            )
            .layer(axum_middleware::from_fn(request_timing::mark_authenticated));
        let router = Router::new()
            .nest("/v1", v1)
            .layer(axum_middleware::from_fn(request_id_and_latency));

        let buf = LogBuf::default();
        let subscriber = tracing_subscriber::fmt()
            .json()
            .with_writer(buf.clone())
            .with_ansi(false)
            .finish();
        let _guard = tracing::subscriber::set_default(subscriber);

        let id = "3f346a2a-71ca-4779-9701-baf2e5e7bddd";
        let res = router
            .clone()
            .oneshot(
                HttpRequest::builder()
                    .uri(format!("/v1/graphs/{id}/stats?window=7d"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let timing = res
            .headers()
            .get("server-timing")
            .expect("every response carries Server-Timing")
            .to_str()
            .unwrap()
            .to_string();
        crate::request_timing::assert_server_timing_grammar(&timing);
        for metric in ["auth;dur=", "txopen;dur=", "desc=\"1 tx\"", "app;dur=", "total;dur="] {
            assert!(timing.contains(metric), "{metric} missing from {timing}");
        }

        let res = router
            .oneshot(
                HttpRequest::builder()
                    .uri(format!("/v1/library/{id}/boom"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert!(res.headers().contains_key("server-timing"));

        let logs = String::from_utf8(buf.0.lock().unwrap().clone()).unwrap();
        let lines: Vec<serde_json::Value> = logs
            .lines()
            .filter_map(|l| serde_json::from_str::<serde_json::Value>(l).ok())
            .filter(|v| v["fields"]["message"] == "request completed")
            .collect();
        assert_eq!(lines.len(), 2, "one access line per request: {logs}");

        let ok = &lines[0];
        assert_eq!(ok["fields"]["route"], "/v1/graphs/{graph_id}/stats");
        assert!(!ok["fields"]["route"].as_str().unwrap().contains(id));
        assert_eq!(ok["level"], "INFO");
        assert_eq!(ok["fields"]["tx_count"], 1);
        assert!(ok["fields"]["auth_ms"].is_u64());
        assert!(ok["fields"]["app_ms"].is_u64());

        let failed = &lines[1];
        assert_eq!(failed["fields"]["route"], "/v1/library/{graph_id}/boom");
        assert_eq!(failed["level"], "WARN");
        assert_eq!(failed["fields"]["status"], 503);

        assert!(
            request_timing::LEDGER
                .violations(request_timing::now_secs())
                .iter()
                .all(|v| !v.route.contains(id)),
            "the 5xx ledger is keyed by template, never by a concrete path"
        );
    }

    /// The real public router: the browser can read Server-Timing cross-origin
    /// and Resource Timing gets it via Timing-Allow-Origin.
    #[tokio::test]
    async fn public_router_exposes_server_timing_cross_origin() {
        let res = app(test_state())
            .oneshot(
                HttpRequest::builder()
                    .uri("/healthz")
                    .header("origin", "https://app.coralswarm.example")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let timing = res.headers().get("server-timing").expect("Server-Timing on /healthz");
        crate::request_timing::assert_server_timing_grammar(timing.to_str().unwrap());
        let exposed = res
            .headers()
            .get("access-control-expose-headers")
            .expect("expose-headers set")
            .to_str()
            .unwrap()
            .to_ascii_lowercase();
        assert!(exposed.contains("server-timing"), "{exposed}");
        assert_eq!(
            res.headers().get("timing-allow-origin"),
            res.headers().get("access-control-allow-origin"),
        );
        assert!(res.headers().contains_key("timing-allow-origin"));
    }
}

/// Independent legacy truth surface for metering equality tests. Never routed;
/// generic stored GETs cannot reach this aggregate handler.
#[cfg(feature="test-support")]
pub async fn test_support_usage_truth(state:Arc<AppState>,tenant:tenancy::extractor::Tenant,user:UserContext)->axum::response::Response {
    routes::usage::get_usage_summary(axum::extract::State(state),tenant,axum::Extension(user)).await
}

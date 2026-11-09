//! Source-publication integration suite (live Postgres).
//!
//! ## How this suite gets a database
//!
//! Two ways, in this order:
//!
//! 1. `PUBLICATION_TEST_DATABASE_URL` — an ALREADY-MIGRATED scratch database,
//!    used verbatim. The local convention; unchanged.
//! 2. `ORGT_TEST_BASE_URL` — the DSN every other `*_pg.rs` suite here is gated
//!    on, pointing at a `CREATE DATABASE`-capable Postgres. This suite then
//!    stands up its own scratch database and applies `migrations_control` to
//!    it, the same way `waitlist_sync_pg`/`worker_harness_pg` do.
//!
//! (2) exists because of what (1) alone meant in CI (CodeRabbit round 1 on
//! #871): `.github/workflows/ci.yml` set `PUBLICATION_TEST_DATABASE_URL`
//! nowhere, so under the plain workspace `cargo test` job both tests hit their
//! env-var guard, returned before their first assertion, and printed
//! `test result: ok`. A suite covering connection OWNERSHIP that silently
//! proves nothing is worse than no suite at all — it makes a future regression
//! look tested. `scripts/check_pg_suite_coverage.sh` is the gate for exactly
//! this failure mode, and it only recognises a suite named in a
//! `cargo test --test ...` step of a job that sets `ORGT_TEST_BASE_URL:` or
//! `RELINFER_TEST_DATABASE_URL:`. Wired into the `cross-tenant-probe` job.
//!
//! With neither variable set the suite still skips — and says so, loudly.
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Extension, Json,
};
use coralswarm_backend::{
    create_source_connection,
    jobs::{publication, queue, JobKind},
    list_source_connections,
    services::source_publication,
    store::PostgresStore,
    tenancy::extractor::Tenant,
    update_source_connection,
    CreateSourceConnection as Create, UpdateSourceConnection as Update, UserContext,
};
use sqlx::postgres::PgPoolOptions;
use sqlx::{Connection, Executor, PgConnection, PgPool};
use std::sync::Arc;
use uuid::Uuid;

mod support;

/// Swap the database name in a base DSN, preserving any query parameters.
fn with_db(base: &str, db: &str) -> String {
    let (prefix, rest) = base.split_at(base.rfind('/').expect("url has a path /"));
    let params = rest.find('?').map(|i| &rest[i..]).unwrap_or("");
    format!("{prefix}/{db}{params}")
}

/// A migrated database for one test, or `None` with a loud skip line.
///
/// `scratch` is the database name used for the `ORGT_TEST_BASE_URL` path; each
/// test passes its own so two tests never share one (and so a re-run starts
/// from a known-empty schema).
async fn migrated_pool(scratch: &str) -> Option<(Option<coralswarm_backend::test_db::TestDatabase>, PgPool)> {
    if let Ok(url) = std::env::var("PUBLICATION_TEST_DATABASE_URL") {
        return Some((None,
            PgPool::connect(&url)
                .await
                .expect("connect PUBLICATION_TEST_DATABASE_URL"),
        ));
    }
    let Ok(base) = std::env::var("ORGT_TEST_BASE_URL") else {
        // The literal marker CI greps for. Every `*_pg.rs` file here links
        // and RUNS under the DSN-less workspace `cargo test` job, where
        // skipping is correct; in the one job that is supposed to run this
        // suite, a skip is a defect, and a silent one looks exactly like a
        // pass. See this file's module doc.
        eprintln!(
            "SOURCE_PUBLICATION_PG: SKIPPED {scratch} — set PUBLICATION_TEST_DATABASE_URL \
             (an already-migrated scratch DB) or ORGT_TEST_BASE_URL (a CREATE \
             DATABASE-capable Postgres, which this suite migrates itself)"
        );
        return None;
    };
    let database = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, scratch);
    let mut admin = PgConnection::connect(&base).await.expect("connect admin");
    admin
        .execute(format!("DROP DATABASE IF EXISTS {scratch} WITH (FORCE)").as_str())
        .await
        .expect("drop scratch db");
    admin
        .execute(format!("CREATE DATABASE {scratch}").as_str())
        .await
        .expect("create scratch db");
    let pool = PgPoolOptions::new()
        .max_connections(8)
        .connect(&with_db(&base, scratch))
        .await
        .expect("connect scratch db");
    sqlx::migrate!("./migrations_control")
        .run(&pool)
        .await
        .expect("apply migrations_control");
    Some((Some(database), pool))
}

#[tokio::test]
async fn selected_publication_is_scoped_exact_idempotent_and_never_auto_publishes() {
    let Some((_database, pool)) = migrated_pool("source_publication_scoped_pg").await else {
        return;
    };
    let user: Uuid = sqlx::query_scalar("INSERT INTO users(email) VALUES($1) RETURNING id")
        .bind(format!("{}@publication.test", Uuid::new_v4()))
        .fetch_one(&pool)
        .await
        .unwrap();
    let account: Uuid =
        sqlx::query_scalar("INSERT INTO accounts(owner_user_id) VALUES($1) RETURNING id")
            .bind(user)
            .fetch_one(&pool)
            .await
            .unwrap();
    sqlx::query("INSERT INTO account_members(account_id,user_id,role) VALUES($1,$2,'owner') ON CONFLICT DO NOTHING")
        .bind(account).bind(user).execute(&pool).await.unwrap();
    let source: Uuid = sqlx::query_scalar("INSERT INTO graphs(account_id,name,kind,owner_user_id) VALUES($1,'Private synthetic publication source','member',$2) RETURNING id")
        .bind(account).bind(user).fetch_one(&pool).await.unwrap();
    let target: Uuid = sqlx::query_scalar("INSERT INTO graphs(account_id,name,kind) VALUES($1,'Selected evidence destination','org') RETURNING id")
        .bind(account).fetch_one(&pool).await.unwrap();
    let stranger: Uuid = sqlx::query_scalar("INSERT INTO users(email) VALUES($1) RETURNING id")
        .bind(format!("{}@publication.test", Uuid::new_v4()))
        .fetch_one(&pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO account_members(account_id,user_id,role) VALUES($1,$2,'member')")
        .bind(account)
        .bind(stranger)
        .execute(&pool)
        .await
        .unwrap();
    let foreign_account: Uuid =
        sqlx::query_scalar("INSERT INTO accounts(owner_user_id) VALUES($1) RETURNING id")
            .bind(stranger)
            .fetch_one(&pool)
            .await
            .unwrap();
    let foreign_target: Uuid = sqlx::query_scalar("INSERT INTO graphs(account_id,name,kind) VALUES($1,'Foreign destination','org') RETURNING id")
        .bind(foreign_account).fetch_one(&pool).await.unwrap();

    // Fixture setup uses the owner; every public publication call uses the
    // ordinary application role and its actual account/actor fences.
    support::app_role::set_app_role_password_with_retry(&pool).await;
    let app = PgPoolOptions::new()
        .max_connections(4)
        .connect_with(
            pool.connect_options()
                .as_ref()
                .clone()
                .username("coralswarm_app")
                .password(support::app_role::APP_ROLE_TEST_PASSWORD),
        )
        .await
        .unwrap();
    let privileges: (bool,bool,i64) = sqlx::query_as("SELECT rolsuper,rolbypassrls,(SELECT count(*) FROM pg_tables WHERE schemaname='public' AND tableowner=current_user) FROM pg_roles WHERE rolname=current_user")
        .fetch_one(&app).await.unwrap();
    assert_eq!(privileges, (false, false, 0));
    let store = Arc::new(PostgresStore::new(app.clone()));
    let tenant = || Tenant {
        store: Some(store.clone()),
        pool: Some(app.clone()),
        decision: None,
        class: coralswarm_backend::route_class::RouteClass::Other,
    };
    let ctx = |actor| UserContext {
        user_id: actor,
        account_id: account,
        role: "owner".into(),
        device_id: "test".into(),
    };
    let state = Arc::new(coralswarm_backend::test_app_state_owned_with_db(
        app.clone(),
        None,
    ));
    let text = "PRIVATE PREFIX🙂 approved evidence PRIVATE SUFFIX";
    let (item, version, passage) =
        retained_source(&pool, source, None, "selected-original", text).await;
    let private = retained_source(
        &pool,
        source,
        None,
        "unrelated-private",
        "UNRELATED PRIVATE EVIDENCE",
    )
    .await;
    let start = "PRIVATE PREFIX🙂 ".chars().count() as i32;
    let end = start + "approved evidence".chars().count() as i32;
    let consent = serde_json::json!({"request_id":Uuid::new_v4(),"source_graph_id":source,"source_item_id":item,"source_version_id":version,"target_graph_id":target,"passages":[{"passage_id":passage,"start_char":start,"end_char":end}]});
    let publish = |q: serde_json::Value, actor| {
        coralswarm_backend::selected_publication::publish(
            State(state.clone()),
            tenant(),
            Extension(ctx(actor)),
            coralswarm_backend::library_read::LibraryJson(serde_json::from_value(q).unwrap()),
        )
    };
    let target_versions = || {
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM source_versions WHERE graph_id=$1")
            .bind(target)
            .fetch_one(&pool)
    };

    // The former whole-channel/live consent is intentionally unavailable in
    // cloud. It must refuse before creating a connection, copy or job.
    let legacy = create_source_connection(
        tenant(),
        Extension(ctx(user)),
        Path(source),
        Json(Create {
            source_id: "selected-original".into(),
            target_graph_id: target,
            keep_connected: true,
            channel: true,
            include_history: true,
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(legacy.0, StatusCode::CONFLICT);
    assert_eq!(legacy.1 .0["code"], "selected_evidence_consent_required");
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM source_connections")
            .fetch_one(&pool)
            .await
            .unwrap(),
        0
    );
    assert_eq!(target_versions().await.unwrap(), 0);

    // Pausing a historical connection remains an available safety action.
    // Use a separate CONTROL personal graph: both account members can reach
    // it, so the nonowner denial below isolates connection ownership.
    let historical_source:Uuid=sqlx::query_scalar("INSERT INTO graphs(account_id,name,kind) VALUES($1,'Historical shared-account personal source','personal') RETURNING id")
        .bind(account).fetch_one(&pool).await.unwrap();
    let (_,historical_version,_)=retained_source(&pool,historical_source,None,"historical-original","historical permitted evidence").await;
    let original:Uuid=sqlx::query_scalar("INSERT INTO atoms(graph_id,source_type,source_id,content_blob_key,content_inline,timestamp) VALUES($1,'note','historical-original',$2,'historical permitted evidence',now()) RETURNING atom_id")
        .bind(historical_source).bind(format!("{historical_source}/retained-note")).fetch_one(&pool).await.unwrap();
    sqlx::query("INSERT INTO source_version_chunks(graph_id,source_version_id,atom_id) VALUES($1,$2,$3)")
        .bind(historical_source).bind(historical_version).bind(original).execute(&pool).await.unwrap();
    let copied:Uuid=sqlx::query_scalar("INSERT INTO atoms(graph_id,source_type,source_id,content_blob_key,content_inline,timestamp) VALUES($1,'note','historical-copy',$2,'historical permitted evidence',now()) RETURNING atom_id")
        .bind(target).bind(format!("{target}/historical-copy")).fetch_one(&pool).await.unwrap();
    let connection:Uuid=sqlx::query_scalar("INSERT INTO source_connections(account_id,owner_id,source_graph_id,target_graph_id,source_type,source_id,label,mode,paused,include_history) VALUES($1,$2,$3,$4,'note','historical-original','Historical consent fixture','live',false,true) RETURNING id")
        .bind(account).bind(user).bind(historical_source).bind(target).fetch_one(&pool).await.unwrap();
    sqlx::query("INSERT INTO source_connection_atoms(connection_id,atom_id,revision,target_atom_id) SELECT $1,atom_id,content_revision,$3 FROM atoms WHERE atom_id=$2")
        .bind(connection).bind(original).bind(copied).execute(&pool).await.unwrap();
    let connection_before:serde_json::Value=sqlx::query_scalar("SELECT to_jsonb(c) FROM source_connections c WHERE id=$1")
        .bind(connection).fetch_one(&pool).await.unwrap();
    let target_before:serde_json::Value=sqlx::query_scalar("SELECT COALESCE(jsonb_agg(to_jsonb(a) ORDER BY atom_id),'[]'::jsonb) FROM atoms a WHERE graph_id=$1")
        .bind(target).fetch_one(&pool).await.unwrap();
    assert_eq!(target_before.as_array().unwrap().len(),1);
    let Json(owner_list)=list_source_connections(tenant(),Extension(ctx(user)),Path(historical_source),Query(Default::default())).await.unwrap();
    assert_eq!(owner_list["connections"].as_array().unwrap().len(),1);
    assert_eq!(owner_list["connections"][0]["id"],serde_json::json!(connection));
    let Json(nonowner_list)=list_source_connections(tenant(),Extension(ctx(stranger)),Path(historical_source),Query(Default::default())).await.expect("same-account nonowner clears source access and list guards");
    assert_eq!(nonowner_list["connections"],serde_json::json!([]));
    let denied=update_source_connection(tenant(),Extension(ctx(stranger)),Path((historical_source,connection)),Json(Update{action:"pause".into()})).await.unwrap_err();
    assert_eq!(denied.0,StatusCode::NOT_FOUND);
    assert_eq!(sqlx::query_scalar::<_,serde_json::Value>("SELECT to_jsonb(c) FROM source_connections c WHERE id=$1").bind(connection).fetch_one(&pool).await.unwrap(),connection_before);
    assert_eq!(sqlx::query_scalar::<_,serde_json::Value>("SELECT COALESCE(jsonb_agg(to_jsonb(a) ORDER BY atom_id),'[]'::jsonb) FROM atoms a WHERE graph_id=$1").bind(target).fetch_one(&pool).await.unwrap(),target_before);
    let Json(paused)=update_source_connection(tenant(),Extension(ctx(user)),Path((historical_source,connection)),Json(Update{action:"pause".into()})).await.unwrap();
    assert_eq!(paused,serde_json::json!({"id":connection,"paused":true}));
    assert!(sqlx::query_scalar::<_,bool>("SELECT paused FROM source_connections WHERE id=$1").bind(connection).fetch_one(&pool).await.unwrap());
    let after_pause:serde_json::Value=sqlx::query_scalar("SELECT to_jsonb(c) FROM source_connections c WHERE id=$1").bind(connection).fetch_one(&pool).await.unwrap();
    for action in ["resume","retry"] {
        let refused=update_source_connection(tenant(),Extension(ctx(user)),Path((historical_source,connection)),Json(Update{action:action.into()})).await.unwrap_err();
        assert_eq!(refused.0,StatusCode::CONFLICT);
        assert_eq!(refused.1.0["code"],"selected_evidence_consent_required");
        assert_eq!(sqlx::query_scalar::<_,serde_json::Value>("SELECT to_jsonb(c) FROM source_connections c WHERE id=$1").bind(connection).fetch_one(&pool).await.unwrap(),after_pause);
    }
    assert_eq!(sqlx::query_scalar::<_,serde_json::Value>("SELECT COALESCE(jsonb_agg(to_jsonb(a) ORDER BY atom_id),'[]'::jsonb) FROM atoms a WHERE graph_id=$1").bind(target).fetch_one(&pool).await.unwrap(),target_before);
    assert_eq!(sqlx::query_scalar::<_,i64>("SELECT count(*) FROM jobs").fetch_one(&pool).await.unwrap(),0);

    let mut foreign = consent.clone();
    foreign["target_graph_id"] = serde_json::json!(foreign_target);
    assert_eq!(
        response_json(publish(foreign, user).await).await.0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        response_json(publish(consent.clone(), stranger).await)
            .await
            .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(target_versions().await.unwrap(), 0);
    let (status, receipt) = response_json(publish(consent.clone(), user).await).await;
    assert_eq!(status, StatusCode::CREATED, "{receipt}");
    assert_eq!(
        receipt["processing_status"],
        "deterministic_selected_passages"
    );
    assert_eq!(
        receipt["managed_consumers_status"],
        "pending_explicit_budget"
    );
    let destination: Uuid = serde_json::from_value(receipt["source_version_id"].clone()).unwrap();
    let publication: Uuid = serde_json::from_value(receipt["publication_id"].clone()).unwrap();
    let selected: (String,serde_json::Value,serde_json::Value,String,String,String) = sqlx::query_as("SELECT v.raw_inline,v.source_metadata,v.origin,a.statement,a.model,p.quote FROM source_versions v JOIN semantic_atoms a ON a.graph_id=v.graph_id AND a.source_version_id=v.version_id JOIN semantic_atom_passages p ON p.graph_id=a.graph_id AND p.semantic_atom_id=a.semantic_atom_id WHERE v.graph_id=$1 AND v.version_id=$2")
        .bind(target).bind(destination).fetch_one(&pool).await.unwrap();
    assert_eq!(selected.0, "approved evidence");
    assert_eq!(selected.3, selected.0);
    assert_eq!(selected.4, "deterministic-selection");
    assert_eq!(selected.5, selected.0);
    assert_eq!(selected.1["independence"], "derived_copy");
    assert_eq!(selected.2["transport"], "synthetic");
    for value in [&selected.1, &selected.2, &receipt] {
        let serialized = value.to_string();
        for forbidden in ["PRIVATE".to_owned(), source.to_string(), item.to_string(), private.0.to_string()] {
            assert!(
                !serialized.contains(&forbidden),
                "private metadata must not be copied: {serialized}"
            );
        }
    }
    let admission: (String,String,Option<Uuid>) = sqlx::query_as("SELECT state,extraction_version,run_id FROM corpus_capture_admissions WHERE graph_id=$1 AND source_version_id=$2")
        .bind(target).bind(destination).fetch_one(&pool).await.unwrap();
    assert_eq!(
        admission,
        ("processed".into(), "selected-passages-v1".into(), None)
    );
    assert_eq!(target_versions().await.unwrap(), 1);
    let repeated = response_json(publish(consent.clone(), user).await).await;
    assert_eq!(repeated.0, StatusCode::OK);
    assert_eq!(
        repeated.1, receipt,
        "same consent resumes the same sealed destination"
    );
    let mut altered = consent.clone();
    altered["passages"][0]["start_char"] = serde_json::json!(start + 1);
    assert_eq!(
        response_json(publish(altered, user).await).await.0,
        StatusCode::CONFLICT
    );
    assert!(sqlx::query(
        "UPDATE source_versions SET raw_inline='PRIVATE REBIND' WHERE version_id=$1"
    )
    .bind(version)
    .execute(&pool)
    .await
    .is_err());

    // New current source bytes require NEW explicit exact-version consent.
    // Neither the prior snapshot nor unrelated source content is refreshed or
    // automatically widened by a future capture.
    let (_, future_version, future_passage) = retained_source(
        &pool,
        source,
        Some(item),
        "selected-original",
        "future evidence PRIVATE FUTURE SUFFIX",
    )
    .await;
    assert_eq!(target_versions().await.unwrap(), 1);
    let unchanged: String =
        sqlx::query_scalar("SELECT raw_inline FROM source_versions WHERE version_id=$1")
            .bind(destination)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(unchanged, "approved evidence");
    let mut future = consent.clone();
    future["request_id"] = serde_json::json!(Uuid::new_v4());
    future["source_version_id"] = serde_json::json!(future_version);
    future["passages"] = serde_json::json!([{"passage_id":future_passage,"start_char":0,"end_char":"future evidence".chars().count()}]);
    let future_response = response_json(publish(future.clone(), user).await).await;
    assert_eq!(
        future_response.0,
        StatusCode::CREATED,
        "{}",
        future_response.1
    );
    let future_destination: Uuid =
        serde_json::from_value(future_response.1["source_version_id"].clone()).unwrap();
    assert_ne!(future_destination, destination);
    let future_text: String =
        sqlx::query_scalar("SELECT raw_inline FROM source_versions WHERE version_id=$1")
            .bind(future_destination)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(future_text, "future evidence");
    assert_eq!(target_versions().await.unwrap(), 2);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM jobs")
            .fetch_one(&pool)
            .await
            .unwrap(),
        0,
        "selected deterministic publication never synthesizes generic processing jobs"
    );

    // A reachable shared reader does not own the private publication receipt.
    let denied = response_json(
        coralswarm_backend::selected_publication::retract(
            State(state.clone()),
            tenant(),
            Extension(ctx(stranger)),
            Path(publication),
        )
        .await,
    )
    .await;
    assert_eq!(denied.0, StatusCode::NOT_FOUND);
    assert_eq!(target_versions().await.unwrap(), 2);
    let withdrawn = response_json(
        coralswarm_backend::selected_publication::retract(
            State(state.clone()),
            tenant(),
            Extension(ctx(user)),
            Path(publication),
        )
        .await,
    )
    .await;
    assert_eq!(withdrawn.0, StatusCode::OK);
    assert_eq!(target_versions().await.unwrap(), 1);
    assert_eq!(
        response_json(publish(consent.clone(), user).await).await.0,
        StatusCode::CONFLICT,
        "retracted consent cannot be replayed"
    );

    sqlx::query(
        "UPDATE source_items SET access_state='restricted' WHERE graph_id=$1 AND source_item_id=$2",
    )
    .bind(source)
    .bind(item)
    .execute(&pool)
    .await
    .unwrap();
    assert_eq!(
        target_versions().await.unwrap(),
        0,
        "loss of exact source support retracts its remaining selected copy"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM source_versions WHERE version_id=ANY($1)"
        )
        .bind(vec![version, future_version, private.1])
        .fetch_one(&pool)
        .await
        .unwrap(),
        3,
        "canonical originals and unrelated private evidence remain retained"
    );
    assert_eq!(
        response_json(publish(future, user).await).await.0,
        StatusCode::NOT_FOUND
    );
    // Isolate account revocation from the restricted item's refusal: this
    // second source is still permitted with exact retained passage support.
    assert_eq!(sqlx::query_scalar::<_,String>("SELECT access_state FROM source_items WHERE graph_id=$1 AND source_item_id=$2")
        .bind(source).bind(private.0).fetch_one(&pool).await.unwrap(),"permitted");
    assert!(coralswarm_backend::source_evidence::still_admitted(&state,&tenant(),&ctx(user),source,account).await);
    sqlx::query("DELETE FROM account_members WHERE account_id=$1 AND user_id=$2")
        .bind(account).bind(user).execute(&pool).await.unwrap();
    assert!(!coralswarm_backend::source_evidence::still_admitted(&state,&tenant(),&ctx(user),source,account).await);
    let mut no_membership=consent;
    no_membership["request_id"]=serde_json::json!(Uuid::new_v4());
    no_membership["source_item_id"]=serde_json::json!(private.0);
    no_membership["source_version_id"]=serde_json::json!(private.1);
    no_membership["passages"]=serde_json::json!([{"passage_id":private.2,"start_char":0,"end_char":"UNRELATED PRIVATE EVIDENCE".chars().count()}]);
    assert_eq!(response_json(publish(no_membership,user).await).await.0,StatusCode::NOT_FOUND);
    assert_eq!(target_versions().await.unwrap(), 0);
    println!("SOURCE_PUBLICATION_PG: PROVED selected_scoped_exact_idempotent_never_auto_publishes");
    app.close().await;
}

async fn response_json(response: axum::response::Response) -> (StatusCode, serde_json::Value) {
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), 1024 * 1024)
        .await
        .unwrap();
    (status, serde_json::from_slice(&bytes).unwrap())
}

/// Genuine immutable synthetic retained bytes and an exact persisted passage;
/// no source metadata, inferred output or fabricated provider capture is used
/// as publication authority.
async fn retained_source(
    pool: &PgPool,
    graph: Uuid,
    item: Option<Uuid>,
    id: &str,
    text: &str,
) -> (Uuid, Uuid, Uuid) {
    let origin = serde_json::json!({"service_id":"fixture","transport":"synthetic","content_kind":"note","capture_surface":"synthetic_demo"});
    let item=match item {
        Some(item)=>item,
        None=>sqlx::query_scalar("INSERT INTO source_items(graph_id,source_type,provider_namespace,source_id,origin) VALUES($1,'note','publication:synthetic',$2,$3) RETURNING source_item_id")
            .bind(graph).bind(id).bind(&origin).fetch_one(pool).await.unwrap(),
    };
    let version:Uuid=sqlx::query_scalar("INSERT INTO source_versions(graph_id,source_item_id,source_id,source_type,provider_namespace,raw_inline,extraction_version,source_metadata,origin) VALUES($1,$2,$3,'note','publication:synthetic',$4,'source-atoms-v1','{\"title\":\"PRIVATE ANCESTOR TITLE\"}',$5) RETURNING version_id")
        .bind(graph).bind(item).bind(id).bind(text).bind(&origin).fetch_one(pool).await.unwrap();
    sqlx::query("UPDATE source_items SET current_version_id=$2 WHERE source_item_id=$1")
        .bind(item)
        .bind(version)
        .execute(pool)
        .await
        .unwrap();
    let semantic:Uuid=sqlx::query_scalar("INSERT INTO semantic_atoms(graph_id,source_version_id,ordinal,kind,statement,extraction_version,model) VALUES($1,$2,0,'observation',$3,'source-atoms-v1','synthetic-fixture') RETURNING semantic_atom_id")
        .bind(graph).bind(version).bind(text).fetch_one(pool).await.unwrap();
    let passage:Uuid=sqlx::query_scalar("INSERT INTO semantic_atom_passages(graph_id,source_version_id,semantic_atom_id,start_char,end_char,quote) VALUES($1,$2,$3,0,$4,$5) RETURNING passage_id")
        .bind(graph).bind(version).bind(semantic).bind(text.chars().count() as i32).bind(text).fetch_one(pool).await.unwrap();
    (item, version, passage)
}

#[tokio::test]
async fn shared_debounced_jobs_are_dependencies_of_both_publications() {
    let Some((_database, pool)) = migrated_pool("source_publication_shared_pg").await else {
        return;
    };
    let user: Uuid = sqlx::query_scalar("INSERT INTO users(email) VALUES($1) RETURNING id")
        .bind(format!("{}@publication.test", Uuid::new_v4()))
        .fetch_one(&pool)
        .await
        .unwrap();
    let account: Uuid =
        sqlx::query_scalar("INSERT INTO accounts(owner_user_id) VALUES($1) RETURNING id")
            .bind(user)
            .fetch_one(&pool)
            .await
            .unwrap();
    let graph: Uuid = sqlx::query_scalar("INSERT INTO graphs(account_id,name,kind) VALUES($1,'Causal queue fixture','personal') RETURNING id")
        .bind(account).fetch_one(&pool).await.unwrap();
    // Causal debounce is a pending-job contract. Use admitted deterministic
    // maintenance work: Unsupported inference purposes are terminal on enqueue.
    let p1 = queue::enqueue(
        &pool,
        &JobKind::OceanHealth { graph_id: graph },
        Some(account),
        None,
        0,
    )
    .await
    .unwrap()
    .unwrap();
    let p2 = queue::enqueue(
        &pool,
        &JobKind::OceanHealth { graph_id: graph },
        Some(account),
        None,
        0,
    )
    .await
    .unwrap()
    .unwrap();
    // BackfillAtomTitles is admitted ManagedEnrichment since migration 0338.
    // IngestDoor is genuinely retired (0327, retained by the current 0347
    // admission trigger) and has no JobKind variant. Insert legacy pending
    // rows directly to prove the trigger makes them terminal before dedup.
    let retired_key = format!("test-retired:{graph}");
    let mut retired_ids = Vec::new();
    for _ in 0..2 {
        let id: Uuid = sqlx::query_scalar("INSERT INTO jobs(kind,payload,account_id,idempotency_key,status) VALUES('IngestDoor',$1,$2,$3,'pending') RETURNING id")
            .bind(serde_json::json!({"graph_id": graph}))
            .bind(account).bind(&retired_key).fetch_one(&pool).await.unwrap();
        retired_ids.push(id);
    }
    assert_ne!(retired_ids[0], retired_ids[1], "terminal retired receipts are not pending shared work");
    let dead: i64 = sqlx::query_scalar("SELECT count(*) FROM jobs WHERE id=ANY($1) AND account_id=$2 AND kind='IngestDoor' AND idempotency_key=$3 AND status='dead' AND error_message='managed_purpose_unsupported' AND finished_at IS NOT NULL")
        .bind(&retired_ids).bind(account).bind(&retired_key).fetch_one(&pool).await.unwrap();
    assert_eq!(dead, 2);
    assert_eq!(sqlx::query("DELETE FROM jobs WHERE id=ANY($1) AND account_id=$2 AND kind='IngestDoor'")
        .bind(&retired_ids).bind(account).execute(&pool).await.unwrap().rows_affected(), 2);
    let key = format!("test-shared:{graph}");
    let child1 = publication::scope(
        p1,
        queue::enqueue(
            &pool,
            &JobKind::ComputeDrift { graph_id: graph },
            Some(account),
            Some(&key),
            0,
        ),
    )
    .await
    .unwrap()
    .unwrap();
    let child2 = publication::scope(
        p2,
        queue::enqueue(
            &pool,
            &JobKind::ComputeDrift { graph_id: graph },
            Some(account),
            Some(&key),
            0,
        ),
    )
    .await
    .unwrap()
    .unwrap();
    let pending: i64 = sqlx::query_scalar("SELECT count(*) FROM jobs WHERE id=ANY($1) AND account_id=$2 AND status='pending' AND error_message IS NULL")
        .bind(vec![p1,p2,child1]).bind(account).fetch_one(&pool).await.unwrap();
    assert_eq!(
        pending, 3,
        "two admitted parents and the shared admitted child remain pending"
    );
    assert_eq!(child1, child2);
    for p in [p1, p2] {
        queue::complete(&pool, p, queue::JobOutcome::DidWork)
            .await
            .unwrap();
        assert_eq!(
            publication::status(&pool, p, account)
                .await
                .unwrap()
                .expect("root visible to its own account")
                .state,
            "processing"
        );
    }
    sqlx::query("UPDATE jobs SET status='dead' WHERE id=$1")
        .bind(child1)
        .execute(&pool)
        .await
        .unwrap();
    for p in [p1, p2] {
        assert_eq!(
            publication::status(&pool, p, account)
                .await
                .unwrap()
                .expect("root visible to its own account")
                .state,
            "attention"
        );
    }
    // `job_dependencies` retention: links whose parent is terminal and older
    // than the horizon are retired; a RECENT terminal parent's links are not
    // (a client may still be polling it).
    let fresh = publication::purge_terminal_dependencies(&pool, 500).await.unwrap();
    assert_eq!(
        fresh, 0,
        "a publication that finished seconds ago keeps its causal links"
    );
    sqlx::query("UPDATE jobs SET finished_at = now() - interval '400 days' WHERE id = ANY($1)")
        .bind(&[p1, p2][..])
        .execute(&pool)
        .await
        .unwrap();
    let purged = publication::purge_terminal_dependencies(&pool, 500).await.unwrap();
    assert!(
        purged >= 2,
        "links of a long-terminal parent are retired, purged {purged}"
    );

    // An unknown / not-yours publication root is ABSENT, never "ready" — the
    // whole reason `status` returns `Option`. Same query, an account that owns
    // nothing.
    assert!(
        publication::status(&pool, p1, Uuid::new_v4())
            .await
            .unwrap()
            .is_none(),
        "another account's publication root must not resolve, least of all to `ready`"
    );
    println!("SOURCE_PUBLICATION_PG: PROVED shared_debounced_jobs_are_dependencies_of_both");
}

#[test]
fn source_scope_uses_literal_provider_identity() {
    assert_eq!(
        source_publication::selector("C123/1:chunk:2", "slack", true).unwrap(),
        ("C123/".into(), true)
    );
    assert_eq!(
        source_publication::selector("doc%_:chunk:2", "document", false).unwrap(),
        ("doc%_".into(), false)
    );
    assert!(source_publication::selector("C123/1", "document", true).is_err());
    assert!(source_publication::selector("C%/1", "slack", true).is_err());
}

//! Required real PostgreSQL proof. These tests never return green on a missing URL.
#[path = "support/page_model_fixture.rs"]
mod fixture;
use coralswarm_backend::{
    read_models::{self as rm, ReadModel},
    tenancy::scope::begin_tenant_scoped,
    workers::read_model_refresh,
};
use fixture::Fixture;
use serde_json::{json, Value};
use uuid::Uuid;

async fn scope<M: ReadModel>(f: &Fixture, p: M::Params) -> rm::Scope<M::Params> {
    let mut tx = begin_tenant_scoped(&f.app, f.account, f.user)
        .await
        .unwrap();
    let mut viewer = rm::Viewer::load(&mut tx, f.account, f.user).await.unwrap();
    if M::CONTROL_FACTS {
        let mut control = begin_tenant_scoped(&f.control, f.account, f.user)
            .await
            .unwrap();
        viewer.certify_control(&mut control).await.unwrap();
        control.rollback().await.unwrap();
    }
    viewer.scope::<M>(p).unwrap()
}
async fn read<M: ReadModel>(f: &Fixture, s: &rm::Scope<M::Params>) -> rm::Stored<M::Payload> {
    let mut tx = begin_tenant_scoped(&f.app, f.account, f.user)
        .await
        .unwrap();
    rm::store::read::<M>(&mut tx, s).await.unwrap()
}
/// A reducing write serves the warm answer stale, never as current, until the
/// recompute replaces it.
async fn stale_behind<M: ReadModel>(f: &Fixture, s: &rm::Scope<M::Params>) -> rm::Stored<M::Payload> {
    let stored = read::<M>(f, s).await;
    assert!(
        stored.computed_at.is_some() && stored.stale,
        "{} must be served stale after a reducing write",
        M::NAME
    );
    stored
}
async fn fill<M: ReadModel>(f: &Fixture, s: &rm::Scope<M::Params>) -> Value {
    fill_timed::<M>(f, s).await.0
}
async fn fill_timed<M: ReadModel>(f: &Fixture, s: &rm::Scope<M::Params>) -> (Value, f64) {
    rm::store::request_refresh::<M>(&f.app, s).await.unwrap();
    let claims = rm::store::claim(&f.app).await.unwrap();
    let c = claims
        .iter()
        .find(|c| c.model == M::NAME && c.scope_key == s.key() && c.version == M::VERSION)
        .expect("real registered claim");
    let start = std::time::Instant::now();
    let (v, snapshot) = read_model_refresh::compute(&f.worker(), c, &f.control)
        .await
        .unwrap();
    let compute_ms = start.elapsed().as_secs_f64() * 1000.;
    assert!(rm::store::publish(&f.app, c, v.clone(), &snapshot)
        .await
        .unwrap());
    (v, compute_ms)
}
async fn first_fill_unlink<M: ReadModel>(
    f: &Fixture,
    s: &rm::Scope<M::Params>,
    node: Uuid,
    semantic: Uuid,
) {
    for publish_before_commit in [false, true] {
        sqlx::query("DELETE FROM read_models WHERE account_id=$1 AND model=$2 AND scope_key=$3")
            .bind(f.account)
            .bind(M::NAME)
            .bind(s.key())
            .execute(&f.owner)
            .await
            .unwrap();
        let mut writer = f.owner.begin().await.unwrap();
        // Stamp this transaction additively first; removing the last title
        // support must upgrade the same XID to reducing, without a scope row.
        sqlx::query("INSERT INTO nodes(graph_id,type,title,body) VALUES($1,'reference',$2,'{}')")
            .bind(f.graph)
            .bind(format!(
                "First fill additive prerequisite {}",
                Uuid::new_v4()
            ))
            .execute(&mut *writer)
            .await
            .unwrap();
        sqlx::query("DELETE FROM node_semantic_atoms WHERE node_id=$1 AND semantic_atom_id=$2")
            .bind(node)
            .bind(semantic)
            .execute(&mut *writer)
            .await
            .unwrap();
        rm::store::request_refresh::<M>(&f.app, s).await.unwrap();
        let claims = rm::store::claim(&f.app).await.unwrap();
        let c = claims
            .iter()
            .find(|c| c.model == M::NAME)
            .expect("new model first fill claim");
        let (payload, snapshot) = read_model_refresh::compute(&f.worker(), c, &f.control)
            .await
            .unwrap();
        if publish_before_commit {
            assert!(rm::store::publish(&f.app, c, payload.clone(), &snapshot)
                .await
                .unwrap());
        }
        writer.commit().await.unwrap();
        if !publish_before_commit {
            assert!(rm::store::publish(&f.app, c, payload, &snapshot)
                .await
                .unwrap());
        }
        let behind = read::<M>(f, s).await;
        assert!(
            behind.computed_at.is_some() && behind.stale,
            "{} first-fill unlink must serve the former title stale, never current; publication first={publish_before_commit}",
            M::NAME
        );
        sqlx::query(
            "INSERT INTO node_semantic_atoms(graph_id,node_id,semantic_atom_id) VALUES($1,$2,$3)",
        )
        .bind(f.graph)
        .bind(node)
        .bind(semantic)
        .execute(&f.owner)
        .await
        .unwrap();
    }
    fill::<M>(f, s).await;
}
async fn content_lifecycle(org: bool) {
    use rm::library::{CaptureDetail, CaptureKind, CaptureParams, CoralDetail, CoralParams};
    use rm::oceans_pages::{Anchors, Atlas, AtlasParams, ReefPage, ReefPageParams};
    use rm::topics::{TopicPage, TopicParams};
    let f = Fixture::new(org).await;
    let atom = f.atom("l4-first").await;
    let node:Uuid=sqlx::query_scalar("INSERT INTO nodes(graph_id,type,title,body) VALUES($1,'entity','L4 source truth','{}') RETURNING node_id").bind(f.graph).fetch_one(&f.owner).await.unwrap();
    sqlx::query("INSERT INTO node_atoms(node_id,atom_id) VALUES($1,$2)")
        .bind(node)
        .bind(atom)
        .execute(&f.owner)
        .await
        .unwrap();
    let item:Uuid=sqlx::query_scalar("INSERT INTO source_items(graph_id,source_type,provider_namespace,source_id) VALUES($1,'note','fixture:l4','l4-first') RETURNING source_item_id").bind(f.graph).fetch_one(&f.owner).await.unwrap();
    let version:Uuid=sqlx::query_scalar("INSERT INTO source_versions(graph_id,source_item_id,source_type,provider_namespace,source_id,raw_inline,extraction_version) VALUES($1,$2,'note','fixture:l4','l4-first','Synthetic evidence','source-atoms-v1') RETURNING version_id").bind(f.graph).bind(item).fetch_one(&f.owner).await.unwrap();
    sqlx::query("UPDATE source_items SET current_version_id=$2 WHERE source_item_id=$1")
        .bind(item)
        .bind(version)
        .execute(&f.owner)
        .await
        .unwrap();
    let semantic:Uuid=sqlx::query_scalar("INSERT INTO semantic_atoms(graph_id,source_version_id,ordinal,kind,statement,extraction_version,model,entities) VALUES($1,$2,0,'fact','L4 source truth has retained support','source-atoms-v1','fixture',$3) RETURNING semantic_atom_id").bind(f.graph).bind(version).bind(json!([{"mention":"L4 source truth"}])).fetch_one(&f.owner).await.unwrap();
    sqlx::query(
        "INSERT INTO node_semantic_atoms(graph_id,node_id,semantic_atom_id) VALUES($1,$2,$3)",
    )
    .bind(f.graph)
    .bind(node)
    .bind(semantic)
    .execute(&f.owner)
    .await
    .unwrap();
    let rp = scope::<ReefPage>(
        &f,
        ReefPageParams {
            graph: f.graph,
            node_type: None,
            page: 1,
            page_size: 20,
        },
    )
    .await;
    assert!(read::<ReefPage>(&f, &rp).await.computed_at.is_none());
    let page = fill::<ReefPage>(&f, &rp).await;
    assert_eq!(page["nodes"][0]["title"], "L4 source truth");
    let mut tx = begin_tenant_scoped(&f.app, f.account, f.user)
        .await
        .unwrap();
    let truth =
        coralswarm_backend::store::page_sources::list_nodes_scoped(&mut tx, f.graph, None, 21, 0)
            .await
            .unwrap();
    assert_eq!(page["nodes"], json!(truth));
    tx.rollback().await.unwrap();
    let atlas = scope::<Atlas>(
        &f,
        AtlasParams {
            graph: f.graph,
            cooccur: None,
            anchors: Anchors::Top,
        },
    )
    .await;
    let atlas_value = fill::<Atlas>(&f, &atlas).await;
    assert!(atlas_value["nodes"]
        .as_array()
        .unwrap()
        .iter()
        .any(|n| n["label"] == "L4 source truth"));
    let coral = scope::<CoralDetail>(&f, CoralParams { atom }).await;
    let coral_value = fill::<CoralDetail>(&f, &coral).await;
    assert_eq!(coral_value["atom"]["content"], "Synthetic evidence");
    let topic = scope::<TopicPage>(&f, TopicParams { node }).await;
    let topic_value = fill::<TopicPage>(&f, &topic).await;
    assert_eq!(topic_value["node"]["title"], "L4 source truth");
    assert!(topic_value["sessions"]["items"].is_array());
    let capture = scope::<CaptureDetail>(
        &f,
        CaptureParams {
            graph: f.graph,
            source: item.to_string(),
            kind: CaptureKind::Source,
            version_id: None,
            atom_id: None,
        },
    )
    .await;
    let captured = fill::<CaptureDetail>(&f, &capture).await;
    assert!(captured["evidence"].is_object(), "{captured}");
    first_fill_unlink::<ReefPage>(&f, &rp, node, semantic).await;
    first_fill_unlink::<Atlas>(&f, &atlas, node, semantic).await;
    first_fill_unlink::<CoralDetail>(&f, &coral, node, semantic).await;
    first_fill_unlink::<TopicPage>(&f, &topic, node, semantic).await;
    first_fill_unlink::<CaptureDetail>(&f, &capture, node, semantic).await;
    // A node can remain readable via an unrelated admitted legacy atom after
    // its last label-support link is removed. Every warmed composite is marked
    // stale immediately; recomputation must never resurrect that label.
    sqlx::query(
        "DELETE FROM node_semantic_atoms WHERE graph_id=$1 AND node_id=$2 AND semantic_atom_id=$3",
    )
    .bind(f.graph)
    .bind(node)
    .bind(semantic)
    .execute(&f.owner)
    .await
    .unwrap();
    stale_behind::<ReefPage>(&f, &rp).await;
    stale_behind::<Atlas>(&f, &atlas).await;
    stale_behind::<TopicPage>(&f, &topic).await;
    stale_behind::<CoralDetail>(&f, &coral).await;
    let unlinked = fill::<ReefPage>(&f, &rp).await;
    assert!(
        !unlinked.to_string().contains("L4 source truth"),
        "{unlinked}"
    );
    let unlinked = fill::<TopicPage>(&f, &topic).await;
    assert!(
        !unlinked.to_string().contains("L4 source truth"),
        "{unlinked}"
    );
    let unlinked = fill::<CoralDetail>(&f, &coral).await;
    assert!(
        !unlinked.to_string().contains("L4 source truth"),
        "{unlinked}"
    );
    sqlx::query(
        "INSERT INTO node_semantic_atoms(graph_id,node_id,semantic_atom_id) VALUES($1,$2,$3)",
    )
    .bind(f.graph)
    .bind(node)
    .bind(semantic)
    .execute(&f.owner)
    .await
    .unwrap();
    fill::<ReefPage>(&f, &rp).await;
    fill::<Atlas>(&f, &atlas).await;
    fill::<CoralDetail>(&f, &coral).await;
    fill::<TopicPage>(&f, &topic).await;
    let second_node:Uuid=sqlx::query_scalar("INSERT INTO nodes(graph_id,type,title,body) VALUES($1,'entity','Additional admitted edge target','{}') RETURNING node_id").bind(f.graph).fetch_one(&f.owner).await.unwrap();
    sqlx::query("INSERT INTO node_atoms(node_id,atom_id) VALUES($1,$2)")
        .bind(second_node)
        .bind(atom)
        .execute(&f.owner)
        .await
        .unwrap();
    let edge:Uuid=sqlx::query_scalar("INSERT INTO relationship_edge(graph_id,src_node_id,predicate,dst_node_id,status,provenance) VALUES($1,$2,'works_on',$3,'confirmed',$4) RETURNING edge_id")
        .bind(f.graph).bind(node).bind(second_node).bind(json!({"atom_ids":[atom]})).fetch_one(&f.owner).await.unwrap();
    let with_edge = fill::<Atlas>(&f, &atlas).await;
    assert!(
        with_edge["links"]
            .as_array()
            .unwrap()
            .iter()
            .any(|l| l["edge_id"] == json!(edge)),
        "{with_edge}"
    );
    sqlx::query("UPDATE relationship_edge SET status='rejected' WHERE edge_id=$1")
        .bind(edge)
        .execute(&f.owner)
        .await
        .unwrap();
    let behind = stale_behind::<Atlas>(&f, &atlas).await;
    assert!(
        serde_json::to_value(&behind.payload).unwrap()["links"]
            .as_array()
            .unwrap()
            .iter()
            .any(|l| l["edge_id"] == json!(edge)),
        "rejected relation stays in the stale answer until recompute"
    );
    let without = fill::<Atlas>(&f, &atlas).await;
    assert!(!without["links"]
        .as_array()
        .unwrap()
        .iter()
        .any(|l| l["edge_id"] == json!(edge)));
    let before_delete = fill::<ReefPage>(&f, &rp).await;
    assert!(before_delete["nodes"]
        .as_array()
        .unwrap()
        .iter()
        .any(|n| n["node_id"] == json!(second_node)));
    sqlx::query("DELETE FROM nodes WHERE node_id=$1")
        .bind(second_node)
        .execute(&f.owner)
        .await
        .unwrap();
    stale_behind::<ReefPage>(&f, &rp).await;
    stale_behind::<Atlas>(&f, &atlas).await;
    fill::<ReefPage>(&f, &rp).await;
    fill::<Atlas>(&f, &atlas).await;
    fill::<CoralDetail>(&f, &coral).await;
    fill::<TopicPage>(&f, &topic).await;
    fill::<CaptureDetail>(&f, &capture).await;
    // Addition is a real stale serve, and an immediate reduction marks every
    // composite payload from its earlier certified snapshot stale (served,
    // never current) until the recompute.
    f.atom("l4-additive").await;
    assert!(read::<ReefPage>(&f, &rp).await.stale);
    assert!(read::<ReefPage>(&f, &rp).await.computed_at.is_some());
    sqlx::query("UPDATE source_items SET access_state='restricted' WHERE source_item_id=$1")
        .bind(item)
        .execute(&f.owner)
        .await
        .unwrap();
    sqlx::query("UPDATE atoms SET evidence_retracted=true WHERE atom_id=$1")
        .bind(atom)
        .execute(&f.owner)
        .await
        .unwrap();
    stale_behind::<ReefPage>(&f, &rp).await;
    stale_behind::<Atlas>(&f, &atlas).await;
    stale_behind::<CoralDetail>(&f, &coral).await;
    stale_behind::<TopicPage>(&f, &topic).await;
    stale_behind::<CaptureDetail>(&f, &capture).await;
    let fresh = fill::<ReefPage>(&f, &rp).await;
    assert_eq!(fresh["nodes"], json!([]));
    f.close().await;
}
#[tokio::test]
async fn control_content_equality_and_reductions() {
    content_lifecycle(false).await
}
#[tokio::test]
async fn tenant_content_equality_and_reductions() {
    content_lifecycle(true).await
}

#[path = "support/page_model_api.rs"]
mod api;
#[tokio::test]
async fn whole_org_usage_and_live_privilege_gates() {
    use axum::http::StatusCode;
    use coralswarm_backend::{
        route_class::RouteClass,
        store::PostgresStore,
        tenancy::{
            extractor::Tenant,
            runtime::{TenancyDecision, TenantContext},
        },
        UserContext,
    };
    use rm::{billing::UsageSummary, org::MembersAdmin, NoParams};
    use std::sync::Arc;
    let f = Fixture::new(true).await;
    let api = api::Api::new(&f).await;
    // This private graph is invisible to the admin's ordinary member context.
    let mut tx = begin_tenant_scoped(&f.app, f.account, f.user)
        .await
        .unwrap();
    let visible = rm::Viewer::load(&mut tx, f.account, f.user)
        .await
        .unwrap()
        .visible_graphs();
    assert!(!visible.contains(&f.foreign));
    tx.rollback().await.unwrap();
    sqlx::query("INSERT INTO llm_usage(account_id,graph_id,task,model,input_tokens,output_tokens) VALUES(NULL,$1,'synthesize','claude-sonnet-5',1000000,1000000)").bind(f.foreign).execute(&f.owner).await.unwrap();
    sqlx::query("INSERT INTO query_history(account_id,graph_id,user_id,query_text) VALUES($1,$2,$3,'private usage fixture')").bind(f.account).bind(f.foreign).bind(f.other).execute(&f.owner).await.unwrap();
    sqlx::query("INSERT INTO atoms(graph_id,source_type,source_id,content_blob_key,timestamp) VALUES($1,'note','private-usage','fixture',now())").bind(f.foreign).execute(&f.owner).await.unwrap();
    let s = scope::<UsageSummary>(&f, rm::billing::UsageParams::default()).await;
    let (status, missing) = api.call("GET", "/v1/usage/summary", &api.admin).await;
    assert_eq!(status, StatusCode::OK, "{missing}");
    assert!(missing["computed_at"].is_null());
    let v = fill::<UsageSummary>(&f, &s).await;
    assert_eq!(v["by_workspace"][0]["graph_id"], json!(f.foreign));
    assert_eq!(v["by_workspace"][0]["name"], "Foreign fixture");
    assert!(v["by_workspace"][0]["cost_usd"].as_f64().unwrap() > 0.);
    assert_eq!(v["top_askers"][0]["user_id"], json!(f.other));
    assert_eq!(v["atoms_ingested"], 1);
    // Compare the existing whole-org response, excluding only its to-date now.
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
        class: RouteClass::Other,
    };
    let state = coralswarm_backend::test_app_state_owned_with_db_and_store(f.control.clone(), None);
    let user = UserContext {
        user_id: f.user,
        account_id: f.account,
        role: "admin".into(),
        device_id: "l4".into(),
    };
    let response =
        coralswarm_backend::test_support_usage_truth(Arc::new(state), tenant, user).await;
    assert_eq!(response.status(), StatusCode::OK);
    let bytes = axum::body::to_bytes(response.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let mut truth: Value = serde_json::from_slice(&bytes).unwrap();
    let mut expected = v.clone();
    truth.as_object_mut().unwrap().remove("period_end");
    expected.as_object_mut().unwrap().remove("period_end");
    assert_eq!(expected, truth);
    let (status, warm) = api.call("GET", "/v1/usage/summary", &api.admin).await;
    assert_eq!(status, StatusCode::OK, "{warm}");
    assert!(warm["computed_at"].is_string());
    // A private-graph write is outside every ordinary visible-graph dirty
    // subscription. The account-facts fence still marks this whole-org report.
    sqlx::query("INSERT INTO llm_usage(account_id,graph_id,task,model,input_tokens,output_tokens) VALUES(NULL,$1,'synthesize','claude-sonnet-5',100,100)")
        .bind(f.foreign).execute(&f.owner).await.unwrap();
    let dirty = read::<UsageSummary>(&f, &s).await;
    assert!(dirty.stale && dirty.computed_at.is_some());
    sqlx::query("DELETE FROM llm_usage WHERE graph_id=$1 AND input_tokens=100")
        .bind(f.foreign)
        .execute(&f.owner)
        .await
        .unwrap();
    stale_behind::<UsageSummary>(&f, &s).await;
    fill::<UsageSummary>(&f, &s).await;
    for path in ["/v1/usage/summary", "/v1/load/org/members"] {
        for token in [&api.member, &api.oauth] {
            let (status, b) = api.call("GET", path, token).await;
            assert_eq!(status, StatusCode::FORBIDDEN, "{path} {b}");
            let (status, b) = api.call("POST", &format!("{path}/refresh"), token).await;
            assert_eq!(status, StatusCode::FORBIDDEN, "{path} {b}");
        }
    }
    let members = scope::<MembersAdmin>(&f, NoParams {}).await;
    let mv = fill::<MembersAdmin>(&f, &members).await;
    assert_eq!(mv["members"].as_array().unwrap().len(), 2);
    // Role downgrade leaves content graphs and all scope keys unchanged.
    sqlx::query("UPDATE account_members SET role='member' WHERE account_id=$1 AND user_id=$2")
        .bind(f.account)
        .bind(f.user)
        .execute(&f.control)
        .await
        .unwrap();
    assert_eq!(
        s.class.graphs(),
        scope::<UsageSummary>(&f, rm::billing::UsageParams::default())
            .await
            .class
            .graphs()
    );
    let retained:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM read_models WHERE model='usage_summary' AND scope_key=$1 AND payload IS NOT NULL)").bind(s.key()).fetch_one(&f.owner).await.unwrap();
    assert!(retained);
    for path in ["/v1/usage/summary", "/v1/load/org/members"] {
        for method in ["GET", "POST"] {
            let path = if method == "POST" {
                format!("{path}/refresh")
            } else {
                path.to_owned()
            };
            let (status, b) = api.call(method, &path, &api.admin).await;
            assert_eq!(
                status,
                StatusCode::FORBIDDEN,
                "downgraded {method} {path} {b}"
            );
        }
    }
    rm::store::request_refresh::<UsageSummary>(&f.app, &s)
        .await
        .unwrap();
    let claims = rm::store::claim(&f.app).await.unwrap();
    let c = claims.iter().find(|c| c.model == "usage_summary").unwrap();
    assert!(
        read_model_refresh::compute(&f.worker(), c, &f.control)
            .await
            .is_err(),
        "worker must revalidate current role"
    );
    // Mint the CURRENT revision as an internal adversarial enqueue. This
    // passes key certification, so refusal must come from the live policy.
    let current = scope::<UsageSummary>(&f, rm::billing::UsageParams::default()).await;
    rm::store::request_refresh::<UsageSummary>(&f.app, &current)
        .await
        .unwrap();
    let claims = rm::store::claim(&f.app).await.unwrap();
    let c = claims
        .iter()
        .find(|c| c.model == "usage_summary" && c.scope_key == current.key())
        .expect("current certified adversarial claim");
    let error = read_model_refresh::compute(&f.worker(), c, &f.control)
        .await
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("no longer an eligible org admin"),
        "current-key compute must recheck role: {error}"
    );

    drop(api);
    f.close().await;
}

#[tokio::test]
async fn whole_org_metering_fences_keep_independent_graph_writers_independent() {
    use rm::billing::{UsageParams, UsageSummary};
    let f = Fixture::new(true).await;
    let api = api::Api::new(&f).await;
    let s = scope::<UsageSummary>(&f, UsageParams::default()).await;
    fill::<UsageSummary>(&f, &s).await;
    let mut first = f.owner.begin().await.unwrap();
    let mut second = f.owner.begin().await.unwrap();
    sqlx::query("SET LOCAL lock_timeout='500ms'")
        .execute(&mut *second)
        .await
        .unwrap();
    // Warm report spans both private graphs. The first source transaction
    // remains open while the unrelated graph's write must complete.
    sqlx::query("INSERT INTO llm_usage(account_id,graph_id,task,model,input_tokens,output_tokens) VALUES($1,$2,'synthesize','claude-sonnet-5',100,100)")
        .bind(f.account).bind(f.graph).execute(&mut *first).await.unwrap();
    sqlx::query("INSERT INTO llm_usage(account_id,graph_id,task,model,input_tokens,output_tokens) VALUES($1,$2,'synthesize','claude-sonnet-5',100,100)")
        .bind(f.account).bind(f.foreign).execute(&mut *second).await.expect("independent graph must not wait for an account sentinel/cache row");
    second.commit().await.unwrap();
    let stale = read::<UsageSummary>(&f, &s).await;
    assert!(stale.stale && stale.computed_at.is_some());
    first.commit().await.unwrap();
    let current = fill::<UsageSummary>(&f, &s).await;
    assert_eq!(current["by_workspace"].as_array().unwrap().len(), 2);
    drop(api);
    f.close().await;
}

#[tokio::test]
async fn committed_control_mutation_does_not_ack_success_when_invalidation_fails() {
    let f = Fixture::new(true).await;
    let api = api::Api::new(&f).await;
    // A real database failure in the actual content invalidator, after the
    // settings service commits on control. No handler or cache is mocked.
    sqlx::raw_sql("CREATE FUNCTION l4_fail_invalidation() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'owned invalidation failure fixture'; END $$; CREATE TRIGGER l4_fail_invalidation BEFORE INSERT OR UPDATE ON read_model_user_fences FOR EACH ROW EXECUTE FUNCTION l4_fail_invalidation();")
        .execute(&f.owner).await.unwrap();
    let (status, body) = api
        .call_json(
            "PUT",
            "/v1/orgs/auto-add",
            &api.admin,
            Some(json!({"enabled":true,"tier":"standard"})),
        )
        .await;
    assert_eq!(
        status,
        axum::http::StatusCode::SERVICE_UNAVAILABLE,
        "{body}"
    );
    assert_eq!(body["error"], "read model invalidation unavailable");
    let enabled: bool =
        sqlx::query_scalar("SELECT enabled FROM org_auto_add_settings WHERE account_id=$1")
            .bind(f.account)
            .fetch_one(&f.control_owner)
            .await
            .unwrap();
    assert!(
        enabled,
        "control fact must commit before awaited invalidation"
    );
    drop(api);
    f.close().await;
}

async fn public_activity_first_fill(org: bool, graphless: bool) {
    use rm::{chats_detail::ChatEmpty, home::WorkspaceActivityParams};
    let f = Fixture::new(org).await;
    let p = WorkspaceActivityParams {
        limit: 6,
        since_days: 7,
        calendar_day: chrono::Utc::now().date_naive(),
    };
    let scope = scope::<ChatEmpty>(&f, p).await;
    for publish_before_commit in [false, true] {
        sqlx::query("DELETE FROM read_models WHERE model='chat_empty'")
            .execute(&f.owner)
            .await
            .unwrap();
        let event:Uuid=sqlx::query_scalar("INSERT INTO activity_events(account_id,event_type,payload,graph_id) VALUES($1,'atom_ingested',$2,$3) RETURNING id").bind(f.account).bind(json!({"synthetic":"l4-public-activity"})).bind(if graphless {None}else{Some(f.graph)}).fetch_one(&f.owner).await.unwrap();
        let mut writer = f.owner.begin().await.unwrap();
        sqlx::query("DELETE FROM activity_events WHERE id=$1")
            .bind(event)
            .execute(&mut *writer)
            .await
            .unwrap();
        rm::store::request_refresh::<ChatEmpty>(&f.app, &scope)
            .await
            .unwrap();
        let claims = rm::store::claim(&f.app).await.unwrap();
        let c = claims.iter().find(|c| c.model == "chat_empty").unwrap();
        let (payload, snapshot) = read_model_refresh::compute(&f.worker(), c, &f.control)
            .await
            .unwrap();
        assert!(payload.to_string().contains("l4-public-activity"));
        if publish_before_commit {
            assert!(rm::store::publish(&f.app, c, payload.clone(), &snapshot)
                .await
                .unwrap());
        }
        writer.commit().await.unwrap();
        if !publish_before_commit {
            assert!(rm::store::publish(&f.app, c, payload, &snapshot)
                .await
                .unwrap());
        }
        let behind = read::<ChatEmpty>(&f,&scope).await;
        assert!(behind.computed_at.is_some() && behind.stale,"removed public graphless event must be served stale, never current, on org={org}, publication first={publish_before_commit}");
        let v = fill::<ChatEmpty>(&f, &scope).await;
        assert_eq!(v["activity"], json!([]));
    }
    f.close().await;
}
#[tokio::test]
async fn control_public_graphless_activity_first_fill() {
    public_activity_first_fill(false, true).await
}
#[tokio::test]
async fn tenant_public_graph_activity_first_fill() {
    public_activity_first_fill(true, false).await
}

#[tokio::test]
async fn tenant_graphless_activity_retains_fail_closed_rls() {
    use rm::{chats_detail::ChatEmpty, home::WorkspaceActivityParams};
    let f = Fixture::new(true).await;
    sqlx::query(
        "INSERT INTO activity_events(account_id,event_type,payload) VALUES($1,'atom_ingested',$2)",
    )
    .bind(f.account)
    .bind(json!({"synthetic":"must remain hidden"}))
    .execute(&f.owner)
    .await
    .unwrap();
    let s = scope::<ChatEmpty>(
        &f,
        WorkspaceActivityParams {
            limit: 6,
            since_days: 7,
            calendar_day: chrono::Utc::now().date_naive(),
        },
    )
    .await;
    let v = fill::<ChatEmpty>(&f, &s).await;
    assert_eq!(
        v["activity"],
        json!([]),
        "tenant 0009 deliberately withholds records without a graph anchor"
    );
    f.close().await;
}

async fn conversation_contract(org: bool) {
    use coralswarm_backend::library_read::LibraryScope;
    use rm::chats_detail::{ConversationDetail, DetailParams};
    let f = Fixture::new(org).await;
    let api = api::Api::new(&f).await;
    let mut library = LibraryScope::exact_node(f.graph, Uuid::nil());
    library.node_id = None;
    library.normalize().unwrap();
    let id:Uuid=sqlx::query_scalar("INSERT INTO conversations(account_id,graph_id,user_id,library_scope) VALUES($1,$2,$3,$4) RETURNING id")
        .bind(f.account).bind(f.graph).bind(f.user).bind(json!(library)).fetch_one(&f.owner).await.unwrap();
    for seq in 1..=3 {
        sqlx::query("INSERT INTO conversation_messages(conversation_id,seq,role,text) VALUES($1,$2,'user',$3)")
            .bind(id).bind(seq).bind(format!("question {seq}")).execute(&f.owner).await.unwrap();
    }
    let (status, miss) = api
        .call("GET", &format!("/v1/load/conversations/{id}"), &api.admin)
        .await;
    assert_eq!(status, axum::http::StatusCode::OK, "{miss}");
    assert!(miss["computed_at"].is_null());
    let s = scope::<ConversationDetail>(&f, DetailParams { id }).await;
    let v = fill::<ConversationDetail>(&f, &s).await;
    assert_eq!(v["conversation"]["scope_status"], "resolved");
    assert_eq!(v["resolved_scope"], json!(library));
    assert_eq!(v["messages"].as_array().unwrap().len(), 3);
    assert_eq!(v["messages"][2]["text"], "question 3");
    let (status, warm) = api
        .call("GET", &format!("/v1/load/conversations/{id}"), &api.admin)
        .await;
    assert_eq!(status, axum::http::StatusCode::OK, "{warm}");
    assert_eq!(warm["payload"]["resolved_scope"], json!(library));
    let (status, older) = api
        .call(
            "GET",
            &format!("/v1/conversations/{id}?before_seq=3"),
            &api.admin,
        )
        .await;
    assert_eq!(status, axum::http::StatusCode::OK, "{older}");
    assert_eq!(older["messages"].as_array().unwrap().len(), 2, "{older}");
    // An unshared foreign thread in an otherwise reachable ocean is served
    // stale (never as current) until the recompute empties it.
    sqlx::query("UPDATE conversations SET user_id=$2,shared=false WHERE id=$1")
        .bind(id)
        .bind(f.other)
        .execute(&f.owner)
        .await
        .unwrap();
    stale_behind::<ConversationDetail>(&f, &s).await;
    let v = fill::<ConversationDetail>(&f, &s).await;
    assert!(v["conversation"].is_null());
    assert_eq!(v["messages"], json!([]));
    drop(api);
    f.close().await;
}
#[tokio::test]
async fn personal_resolved_conversation_contract() {
    conversation_contract(false).await
}
#[tokio::test]
async fn tenant_resolved_conversation_contract() {
    conversation_contract(true).await
}

#[tokio::test]
async fn personal_plane_and_typed_route_contracts() {
    use axum::http::StatusCode;
    use rm::billing::UsageSummary;
    let f = Fixture::new(false).await;
    let api = api::Api::new(&f).await;
    for method in ["GET", "POST"] {
        let path = if method == "GET" {
            "/v1/load/org/members"
        } else {
            "/v1/load/org/members/refresh"
        };
        let (status, v) = api.call(method, path, &api.admin).await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{v}");
    }
    let (status, miss) = api.call("GET", "/v1/usage/summary", &api.admin).await;
    assert_eq!(status, StatusCode::OK, "{miss}");
    let s = scope::<UsageSummary>(&f, rm::billing::UsageParams::default()).await;
    fill::<UsageSummary>(&f, &s).await;
    let (status, warm) = api.call("GET", "/v1/usage/summary", &api.admin).await;
    assert_eq!(status, StatusCode::OK, "{warm}");
    assert!(warm["computed_at"].is_string());
    for path in [
        format!("/v1/load/reefs?graph={}&page=2", f.graph),
        format!("/v1/load/reefs?graph={}&user_id={}", f.graph, f.other),
        format!("/v1/load/reefs?graph={}&graph_id={}", f.graph, f.foreign),
        format!("/v1/graphs/{}/atlas?graph={}", f.graph, f.foreign),
        format!(
            "/v1/load/conversations/{}?id={}",
            Uuid::new_v4(),
            Uuid::new_v4()
        ),
    ] {
        let (status, v) = api.call("GET", &path, &api.admin).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{path} {v}");
    }
    let (status, v) = api
        .call(
            "GET",
            &format!("/v1/nodes?graph_id={}&page=2&page_size=1", f.graph),
            &api.admin,
        )
        .await;
    assert_eq!(status, StatusCode::OK, "interactive pagination {v}");
    assert!(
        v.is_array(),
        "interactive route retains its bare array contract"
    );
    drop(api);
    f.close().await;
}

#[tokio::test]
async fn ocean_index_and_chat_empty_actual_payload_reject_obsolete_title() {
    use rm::{chats_detail::ChatEmpty, home::WorkspaceActivityParams, oceans_pages::OceansIndex};
    fn assert_admitted_titles(value: &Value) {
        match value {
            Value::Object(fields) => {
                for (key, value) in fields {
                    if key == "title" {
                        assert!(
                            !value.as_str().is_some_and(|s| s.contains("WITHHELD")),
                            "obsolete extraction used as a current node title: {value}"
                        );
                    }
                    assert_admitted_titles(value);
                }
            }
            Value::Array(items) => {
                for item in items {
                    assert_admitted_titles(item);
                }
            }
            _ => {}
        }
    }
    let f = Fixture::new(true).await;
    std::env::set_var("HOME_DASHBOARD_ENABLED", "true");
    sqlx::query("UPDATE graphs SET kind='org',owner_user_id=NULL WHERE id=$1")
        .bind(f.graph)
        .execute(&f.owner)
        .await
        .unwrap();
    let atom = f.atom("l4-chat-title").await;
    let node:Uuid=sqlx::query_scalar("INSERT INTO nodes(graph_id,type,title,body) VALUES($1,'entity','WITHHELD obsolete semantic label','{}') RETURNING node_id").bind(f.graph).fetch_one(&f.owner).await.unwrap();
    sqlx::query("INSERT INTO node_atoms(node_id,atom_id) VALUES($1,$2)")
        .bind(node)
        .bind(atom)
        .execute(&f.owner)
        .await
        .unwrap();
    let item:Uuid=sqlx::query_scalar("INSERT INTO source_items(graph_id,source_type,provider_namespace,source_id) VALUES($1,'note','fixture:l4','chat-source') RETURNING source_item_id").bind(f.graph).fetch_one(&f.owner).await.unwrap();
    let version:Uuid=sqlx::query_scalar("INSERT INTO source_versions(graph_id,source_item_id,source_type,provider_namespace,source_id,raw_inline,extraction_version) VALUES($1,$2,'note','fixture:l4','chat-source','Synthetic evidence','current-v2') RETURNING version_id").bind(f.graph).bind(item).fetch_one(&f.owner).await.unwrap();
    sqlx::query("UPDATE source_items SET current_version_id=$2 WHERE source_item_id=$1")
        .bind(item)
        .bind(version)
        .execute(&f.owner)
        .await
        .unwrap();
    let semantic:Uuid=sqlx::query_scalar("INSERT INTO semantic_atoms(graph_id,source_version_id,ordinal,kind,statement,extraction_version,model,entities) VALUES($1,$2,0,'fact','WITHHELD obsolete semantic label','obsolete-v1','fixture',$3) RETURNING semantic_atom_id")
        .bind(f.graph).bind(version).bind(json!([{"mention":"WITHHELD obsolete semantic label"}])).fetch_one(&f.owner).await.unwrap();
    sqlx::query(
        "INSERT INTO node_semantic_atoms(graph_id,node_id,semantic_atom_id) VALUES($1,$2,$3)",
    )
    .bind(f.graph)
    .bind(node)
    .bind(semantic)
    .execute(&f.owner)
    .await
    .unwrap();
    // Library deliberately continues serving the previous extraction while a
    // replacement is unfinished. Complete it before asserting that the old
    // extraction can no longer witness a label.
    sqlx::query("UPDATE source_versions SET extraction_status='done' WHERE version_id=$1")
        .bind(version)
        .execute(&f.owner)
        .await
        .unwrap();
    let mut admitted = begin_tenant_scoped(&f.app, f.account, f.user)
        .await
        .unwrap();
    let obsolete_served: i64 =
        sqlx::query_scalar("SELECT count(*) FROM served_semantic_atoms WHERE semantic_atom_id=$1")
            .bind(semantic)
            .fetch_one(&mut *admitted)
            .await
            .unwrap();
    assert_eq!(
        obsolete_served, 0,
        "the fixture must exercise an actually superseded extraction"
    );
    admitted.rollback().await.unwrap();
    let index = scope::<OceansIndex>(&f, rm::oceans_pages::OceansParams::default()).await;
    let v = fill::<OceansIndex>(&f, &index).await;
    assert_eq!(v["oceans"].as_array().unwrap().len(), 1, "{v}");
    assert_eq!(v["oceans"][0]["graph"]["id"], json!(f.graph));
    assert_eq!(v["oceans"][0]["counts"]["atom_count"], 1, "{v}");
    let topic = scope::<rm::topics::TopicPage>(&f, rm::topics::TopicParams { node }).await;
    let v = fill::<rm::topics::TopicPage>(&f, &topic).await;
    assert_admitted_titles(&v);
    assert_eq!(v["node"]["title"], "");
    // Topic's existing explicit all_retained Library scope keeps historical
    // outcome statements. They must not certify a current node label; deleting
    // these outcomes would silently change the Library history contract.
    assert_eq!(v["search"]["scope"]["outcome_versions"], "all_retained");
    assert_eq!(
        v["search"]["outcomes"]["items"][0]["statement"],
        "WITHHELD obsolete semantic label"
    );
    let coral = scope::<rm::library::CoralDetail>(&f, rm::library::CoralParams { atom }).await;
    let v = fill::<rm::library::CoralDetail>(&f, &coral).await;
    assert_admitted_titles(&v);
    let p = WorkspaceActivityParams {
        limit: 5,
        since_days: 30,
        calendar_day: chrono::Utc::now().date_naive(),
    };
    let chat = scope::<ChatEmpty>(&f, p).await;
    let v = fill::<ChatEmpty>(&f, &chat).await;
    assert_eq!(
        v["workspace_activity"]["workspaces"]
            .as_array()
            .unwrap()
            .len(),
        1,
        "{v}"
    );
    assert!(
        !v.to_string().contains("WITHHELD"),
        "actual ChatEmpty must inherit admitted title projection: {v}"
    );
    std::env::remove_var("HOME_DASHBOARD_ENABLED");
    f.close().await;
}

#[tokio::test]
async fn personal_ocean_fan_in_preserves_nine_account_components_and_revocation() {
    use axum::http::StatusCode;
    let f = Fixture::new(false).await;
    let api = api::Api::new(&f).await;
    let mut foreign = vec![];
    for n in 0..8 {
        let owner: Uuid = sqlx::query_scalar("INSERT INTO users(email) VALUES($1) RETURNING id")
            .bind(format!("fan-in-{}@test.local", Uuid::new_v4()))
            .fetch_one(&f.owner)
            .await
            .unwrap();
        let account: Uuid =
            sqlx::query_scalar("INSERT INTO accounts(owner_user_id) VALUES($1) RETURNING id")
                .bind(owner)
                .fetch_one(&f.owner)
                .await
                .unwrap();
        sqlx::query("INSERT INTO account_members(account_id,user_id,role) VALUES($1,$2,'owner'),($1,$3,'member')").bind(account).bind(owner).bind(f.user).execute(&f.owner).await.unwrap();
        let graph: Uuid = sqlx::query_scalar(
            "INSERT INTO graphs(account_id,name,kind) VALUES($1,$2,'org') RETURNING id",
        )
        .bind(account)
        .bind(format!("Shared account {n}"))
        .fetch_one(&f.owner)
        .await
        .unwrap();
        sqlx::query("INSERT INTO atoms(graph_id,source_type,source_id,content_inline,content_blob_key,timestamp) VALUES($1,'note','fan-in-fixture','Shared retained fixture','fixture',now())").bind(graph).execute(&f.owner).await.unwrap();
        foreign.push((account, graph));
    }
    let (status, missing) = api.call("GET", "/v1/load/oceans", &api.admin).await;
    assert_eq!(status, StatusCode::OK, "{missing}");
    assert!(
        missing.get("computed_at").is_none(),
        "no fictitious outer certification"
    );
    assert_eq!(
        missing["components"].as_array().unwrap().len(),
        9,
        "{missing}"
    );
    assert!(missing["components"]
        .as_array()
        .unwrap()
        .iter()
        .all(|c| c["stored"]["computed_at"].is_null()));
    let (status, refresh) = api
        .call("POST", "/v1/load/oceans/refresh", &api.admin)
        .await;
    assert_eq!(status, StatusCode::ACCEPTED, "{refresh}");
    assert_eq!(refresh["components"].as_array().unwrap().len(), 9);
    for _ in 0..3 {
        read_model_refresh::tick(&f.worker(), &f.control)
            .await
            .unwrap();
    }
    let (status, warm) = api.call("GET", "/v1/load/oceans", &api.admin).await;
    assert_eq!(status, StatusCode::OK, "{warm}");
    let components = warm["components"].as_array().unwrap();
    for (account, graph) in &foreign {
        let component = components
            .iter()
            .find(|c| c["account_id"] == json!(account))
            .unwrap();
        assert!(
            component["stored"]["computed_at"].is_string(),
            "{component}"
        );
        assert_eq!(
            component["stored"]["payload"]["oceans"][0]["graph"]["id"],
            json!(graph)
        );
        assert_eq!(
            component["stored"]["payload"]["oceans"][0]["counts"]["atom_count"],
            1
        );
        assert!(
            component["stored"]["payload"]["archived"].is_null(),
            "active account admin is not a foreign account admin"
        );
        let (status, selected) = api
            .call(
                "GET",
                &format!("/v1/load/oceans/component?graph={graph}"),
                &api.admin,
            )
            .await;
        assert_eq!(status, StatusCode::OK, "{selected}");
        assert_eq!(
            selected["payload"], component["stored"]["payload"],
            "verified account selection"
        );
    }
    // Indexed fan-in wall measurements use actual authenticated requests, not
    // worker computation, with all nine independently scoped rows already warm.
    let mut ms = vec![];
    for _ in 0..20 {
        let at = std::time::Instant::now();
        let (status, v) = api.call("GET", "/v1/load/oceans", &api.admin).await;
        assert_eq!(status, StatusCode::OK, "{v}");
        assert_eq!(v["components"].as_array().unwrap().len(), 9);
        ms.push(at.elapsed().as_secs_f64() * 1000.);
    }
    ms.sort_by(f64::total_cmp);
    let result = json!({"accounts":9,"foreign_accounts":8,"browser_requests":1,"samples":20,"http_p50_ms":ms[9],"http_p95_ms":ms[18],"read_only":true,"statement_timeout_ms":1000});
    eprintln!("L4_FAN_IN_BENCH={result}");
    if let Ok(path) = std::env::var("PAGE_MODEL_FAN_IN_OUTPUT") {
        std::fs::write(path, serde_json::to_vec_pretty(&result).unwrap()).unwrap();
    }
    let (revoked, anchor) = foreign[0];
    sqlx::query("DELETE FROM account_members WHERE account_id=$1 AND user_id=$2")
        .bind(revoked)
        .bind(f.user)
        .execute(&f.owner)
        .await
        .unwrap();
    let (status, v) = api.call("GET", "/v1/load/oceans", &api.admin).await;
    assert_eq!(status, StatusCode::OK, "{v}");
    assert_eq!(v["components"].as_array().unwrap().len(), 8);
    assert!(v["components"]
        .as_array()
        .unwrap()
        .iter()
        .all(|c| c["account_id"] != json!(revoked)));
    for method in ["GET", "POST"] {
        let suffix = if method == "GET" { "" } else { "/refresh" };
        let (status, v) = api
            .call(
                method,
                &format!("/v1/load/oceans/component{suffix}?graph={anchor}"),
                &api.admin,
            )
            .await;
        assert_eq!(status, StatusCode::FORBIDDEN, "revoked {v}");
        let (status, v) = api
            .call(
                method,
                &format!("/v1/load/oceans{suffix}?account_id={}", foreign[1].0),
                &api.admin,
            )
            .await;
        assert_eq!(
            status,
            StatusCode::BAD_REQUEST,
            "arbitrary account selector {v}"
        );
    }
    let routed_account: Uuid = sqlx::query_scalar(
        "INSERT INTO accounts(owner_user_id,clerk_org_id) VALUES($1,'org_l4_refused') RETURNING id",
    )
    .bind(f.other)
    .fetch_one(&f.owner)
    .await
    .unwrap();
    sqlx::query("INSERT INTO account_members(account_id,user_id,role,seat_tier_id) VALUES($1,$2,'member',(SELECT id FROM seat_tiers WHERE code='standard' AND is_active))").bind(routed_account).bind(f.user).execute(&f.owner).await.unwrap();
    let routed_graph:Uuid=sqlx::query_scalar("INSERT INTO graphs(account_id,name,kind) VALUES($1,'Refused tenant shadow','org') RETURNING id").bind(routed_account).fetch_one(&f.owner).await.unwrap();
    let routed = (routed_account, routed_graph);
    let mut tx = coralswarm_backend::tenancy::scope::begin_account_scoped(&f.owner, routed.0)
        .await
        .unwrap();
    coralswarm_backend::billing::renewal::lock_period_rotation(&mut tx, routed.0)
        .await
        .unwrap();
    sqlx::query("INSERT INTO tenant_databases(clerk_org_id,account_id,slug,db_secret_ref,status) VALUES('org_l4_refused',$1,'l4refused','NEVER_RESOLVE_THIS','active')").bind(routed.0).execute(&mut *tx).await.unwrap();
    tx.commit().await.unwrap();
    for method in ["GET", "POST"] {
        let suffix = if method == "GET" { "" } else { "/refresh" };
        let (status, v) = api
            .call(
                method,
                &format!("/v1/load/oceans/component{suffix}?graph={}", routed.1),
                &api.admin,
            )
            .await;
        assert_eq!(status, StatusCode::NOT_FOUND, "cross-tenant selector {v}");
    }
    drop(api);
    f.close().await;
}

/// The personal fan-in enqueues a recompute for a stale component exactly like
/// `page_read::serve`, so it must also owe the client an ETA: the web treats a
/// null `refresh_eta` as settled and never polls for the recompute.
#[tokio::test]
async fn personal_ocean_fan_in_owes_refresh_eta_for_stale_components() {
    use axum::http::StatusCode;
    let f = Fixture::new(false).await;
    let api = api::Api::new(&f).await;
    let owner: Uuid = sqlx::query_scalar("INSERT INTO users(email) VALUES($1) RETURNING id")
        .bind(format!("fan-in-eta-{}@test.local", Uuid::new_v4()))
        .fetch_one(&f.owner)
        .await
        .unwrap();
    let shared: Uuid =
        sqlx::query_scalar("INSERT INTO accounts(owner_user_id) VALUES($1) RETURNING id")
            .bind(owner)
            .fetch_one(&f.owner)
            .await
            .unwrap();
    sqlx::query("INSERT INTO account_members(account_id,user_id,role) VALUES($1,$2,'owner'),($1,$3,'member')").bind(shared).bind(owner).bind(f.user).execute(&f.owner).await.unwrap();
    sqlx::query("INSERT INTO graphs(account_id,name,kind) VALUES($1,'Shared ETA fixture','org')")
        .bind(shared)
        .execute(&f.owner)
        .await
        .unwrap();
    let (status, refresh) = api
        .call("POST", "/v1/load/oceans/refresh", &api.admin)
        .await;
    assert_eq!(status, StatusCode::ACCEPTED, "{refresh}");
    for _ in 0..3 {
        read_model_refresh::tick(&f.worker(), &f.control)
            .await
            .unwrap();
    }
    let (status, warm) = api.call("GET", "/v1/load/oceans", &api.admin).await;
    assert_eq!(status, StatusCode::OK, "{warm}");
    let components = warm["components"].as_array().unwrap();
    assert_eq!(components.len(), 2, "{warm}");
    for c in components {
        assert!(c["stored"]["computed_at"].is_string(), "{c}");
        assert_eq!(c["stored"]["stale"], json!(false), "{c}");
        assert!(c["stored"]["refresh_eta"].is_null(), "{c}");
    }
    // Age every component past MAX_DELAY without dirtying it: stale, with no
    // recompute owed yet. The fan-in must enqueue one and say when to poll.
    sqlx::query("UPDATE read_models SET computed_at=computed_at-interval '1 day' WHERE model='oceans_index'")
        .execute(&f.owner)
        .await
        .unwrap();
    let (status, aged) = api.call("GET", "/v1/load/oceans", &api.admin).await;
    assert_eq!(status, StatusCode::OK, "{aged}");
    let components = aged["components"].as_array().unwrap();
    assert_eq!(components.len(), 2, "{aged}");
    for c in components {
        assert!(c["stored"]["computed_at"].is_string(), "{c}");
        assert_eq!(c["stored"]["stale"], json!(true), "{c}");
        assert!(
            c["stored"]["refresh_eta"].is_string(),
            "a stale component the fan-in enqueues must owe an ETA: {c}"
        );
    }
    // The enqueue is detached; the worker then replaces every aged row.
    let mut settled = None;
    for _ in 0..40 {
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        read_model_refresh::tick(&f.worker(), &f.control)
            .await
            .unwrap();
        let (status, v) = api.call("GET", "/v1/load/oceans", &api.admin).await;
        assert_eq!(status, StatusCode::OK, "{v}");
        if v["components"]
            .as_array()
            .unwrap()
            .iter()
            .all(|c| c["stored"]["stale"] == json!(false))
        {
            settled = Some(v);
            break;
        }
    }
    let settled = settled.expect("the owed recompute replaces every aged component");
    for c in settled["components"].as_array().unwrap() {
        assert!(c["stored"]["refresh_eta"].is_null(), "{c}");
    }
    drop(api);
    f.close().await;
}

#[tokio::test]
async fn ocean_index_timeout_is_read_only_and_accounted_exactly_once() {
    use coralswarm_backend::request_timing::{now_secs, PAGE_READ_LEDGER};
    let f = Fixture::new(false).await;
    let api = api::Api::new(&f).await;
    let (status, v) = api
        .call("POST", "/v1/load/oceans/refresh", &api.admin)
        .await;
    assert_eq!(status, axum::http::StatusCode::ACCEPTED, "{v}");
    read_model_refresh::tick(&f.worker(), &f.control)
        .await
        .unwrap();
    let (status, v) = api.call("GET", "/v1/load/oceans", &api.admin).await;
    assert_eq!(status, axum::http::StatusCode::OK, "{v}");
    sqlx::raw_sql("CREATE FUNCTION l4_index_requires_read_only() RETURNS boolean LANGUAGE plpgsql AS $$ BEGIN IF current_setting('transaction_read_only')<>'on' THEN RAISE EXCEPTION 'index transaction is writable'; END IF; RETURN true; END $$; GRANT EXECUTE ON FUNCTION l4_index_requires_read_only() TO coralswarm_app; CREATE POLICY l4_index_requires_read_only ON read_models AS RESTRICTIVE FOR SELECT USING (l4_index_requires_read_only());")
        .execute(&f.owner).await.unwrap();
    let (status, healthy) = api.call("GET", "/v1/load/oceans", &api.admin).await;
    assert_eq!(status, axum::http::StatusCode::OK, "{healthy}");
    assert!(
        healthy["components"]
            .as_array()
            .unwrap()
            .iter()
            .all(|c| c["stored"]["computed_at"].is_string()),
        "actual stored RLS policy requires a READ ONLY request transaction"
    );
    let before = PAGE_READ_LEDGER
        .snapshot(now_secs())
        .per_route
        .into_iter()
        .find(|r| r.route == "/v1/load/oceans")
        .unwrap();
    let mut blocker = f.owner.begin().await.unwrap();
    sqlx::query("SET LOCAL lock_timeout='3s'")
        .execute(&mut *blocker)
        .await
        .unwrap();
    sqlx::query("LOCK TABLE read_models IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *blocker)
        .await
        .unwrap();
    let at = std::time::Instant::now();
    let (status, v) = api.call("GET", "/v1/load/oceans", &api.admin).await;
    assert_eq!(status, axum::http::StatusCode::SERVICE_UNAVAILABLE, "{v}");
    assert!(
        at.elapsed() < std::time::Duration::from_secs(2),
        "indexed fan-in retains the 1s statement timeout"
    );
    blocker.rollback().await.unwrap();
    let after = PAGE_READ_LEDGER
        .snapshot(now_secs())
        .per_route
        .into_iter()
        .find(|r| r.route == "/v1/load/oceans")
        .unwrap();
    assert_eq!(after.statement_timeouts, before.statement_timeouts + 1);
    assert_eq!(after.errors_5xx, before.errors_5xx + 1);
    assert_eq!(after.acquire_timeouts, before.acquire_timeouts);
    drop(api);
    f.close().await;
}

#[tokio::test]
async fn members_admin_preserves_recent_decisions_and_target_account_identity_fences() {
    use axum::http::StatusCode;
    use chrono::{Duration, Utc};
    use rm::{org::MembersAdmin, NoParams};
    let f = Fixture::new(true).await;
    let api = api::Api::new(&f).await;
    let now = Utc::now();
    let mut applicants = vec![];
    for i in 0..5 {
        let id: Uuid =
            sqlx::query_scalar("INSERT INTO users(name,email) VALUES($1,$2) RETURNING id")
                .bind(format!("History applicant {i}"))
                .bind(format!("history-{i}-{}@test.local", Uuid::new_v4()))
                .fetch_one(&f.control_owner)
                .await
                .unwrap();
        applicants.push(id);
    }
    let foreign:Uuid=sqlx::query_scalar("INSERT INTO accounts(owner_user_id,clerk_org_id) VALUES($1,'org_history_foreign') RETURNING id")
        .bind(f.other).fetch_one(&f.control_owner).await.unwrap();
    async fn seed(
        f: &Fixture,
        account: Uuid,
        user: Uuid,
        requested: chrono::DateTime<Utc>,
        decision: Option<(&str, chrono::DateTime<Utc>)>,
    ) -> Uuid {
        sqlx::query_scalar("INSERT INTO org_join_requests(account_id,user_id,source,evidence,requested_at,decision,decided_at,decided_by) VALUES($1,$2,'self_request','{}',$3,$4,$5,$6) RETURNING id")
            .bind(account).bind(user).bind(requested).bind(decision.map(|d|d.0))
            .bind(decision.map(|d|d.1)).bind(decision.map(|_|f.user))
            .fetch_one(&f.control_owner).await.unwrap()
    }
    let new_open = seed(&f, f.account, applicants[0], now - Duration::days(2), None).await;
    let old_open = seed(&f, f.account, applicants[1], now - Duration::days(3), None).await;
    let approved = seed(
        &f,
        f.account,
        applicants[2],
        now - Duration::days(4),
        Some(("approved", now - Duration::hours(5))),
    )
    .await;
    let rejected = seed(
        &f,
        f.account,
        applicants[3],
        now - Duration::days(7),
        Some(("rejected", now - Duration::hours(1))),
    )
    .await;
    let excluded = seed(
        &f,
        f.account,
        applicants[4],
        now - Duration::days(20),
        Some(("approved", now - Duration::days(15))),
    )
    .await;
    let foreign_open = seed(&f, foreign, applicants[4], now - Duration::days(1), None).await;
    let foreign_decided = seed(
        &f,
        foreign,
        applicants[4],
        now - Duration::days(2),
        Some(("rejected", now - Duration::hours(1))),
    )
    .await;
    let (status, cold) = api.call("GET", "/v1/load/org/members", &api.admin).await;
    assert_eq!(status, StatusCode::OK, "{cold}");
    assert!(cold["computed_at"].is_null());
    let initial = scope::<MembersAdmin>(&f, NoParams {}).await;
    let payload = fill::<MembersAdmin>(&f, &initial).await;
    let ids = |v: &Value| {
        v["join_requests"]
            .as_array()
            .unwrap()
            .iter()
            .map(|r| r["id"].as_str().unwrap().parse::<Uuid>().unwrap())
            .collect::<Vec<_>>()
    };
    assert_eq!(ids(&payload), vec![old_open, new_open, rejected, approved]);
    for absent in [excluded, foreign_open, foreign_decided] {
        assert!(!ids(&payload).contains(&absent));
    }
    let (status, legacy) = api
        .call(
            "GET",
            "/v1/orgs/join-requests?include_decided=14",
            &api.admin,
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{legacy}");
    assert_eq!(
        payload["join_requests"], legacy,
        "same DTO, queue order and decided-history contract as the existing UI API"
    );
    let (status, warm) = api.call("GET", "/v1/load/org/members", &api.admin).await;
    assert_eq!(status, StatusCode::OK, "{warm}");
    assert!(warm["computed_at"].is_string());
    for token in [&api.member, &api.oauth] {
        for (method, path) in [
            ("GET", "/v1/load/org/members"),
            ("POST", "/v1/load/org/members/refresh"),
        ] {
            let (status, body) = api.call(method, path, token).await;
            assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
        }
    }
    // Authentication lifecycle facts are now settled for both actors. Isolate
    // the following identity edit from any first-login control revision.
    let initial = scope::<MembersAdmin>(&f, NoParams {}).await;
    fill::<MembersAdmin>(&f, &initial).await;
    let foreign_revision: i64 =
        sqlx::query_scalar("SELECT revision FROM read_model_control_revisions WHERE account_id=$1")
            .bind(foreign)
            .fetch_one(&f.control_owner)
            .await
            .unwrap();
    // This applicant has no membership/owned account. Its queue identity still
    // labels this target org, and must not dirty a different org's applicant.
    sqlx::query("UPDATE users SET name='Renamed applicant' WHERE id=$1")
        .bind(applicants[0])
        .execute(&f.control_owner)
        .await
        .unwrap();
    let renamed = scope::<MembersAdmin>(&f, NoParams {}).await;
    assert_ne!(
        initial.key(),
        renamed.key(),
        "nonmember applicant identity must fence the target account"
    );
    let unchanged: i64 =
        sqlx::query_scalar("SELECT revision FROM read_model_control_revisions WHERE account_id=$1")
            .bind(foreign)
            .fetch_one(&f.control_owner)
            .await
            .unwrap();
    assert_eq!(foreign_revision, unchanged);
    let (status, pending) = api.call("GET", "/v1/load/org/members", &api.admin).await;
    assert_eq!(status, StatusCode::OK, "{pending}");
    assert!(pending["computed_at"].is_null());
    let payload = fill::<MembersAdmin>(&f, &renamed).await;
    assert_eq!(
        payload["join_requests"][1]["user"]["name"],
        "Renamed applicant"
    );
    let (status, body) = api
        .call_body(
            "POST",
            &format!("/v1/orgs/join-requests/{old_open}/reject"),
            &api.admin,
            None,
        )
        .await;
    assert_eq!(status, StatusCode::NO_CONTENT, "{body:?}");
    let decided = scope::<MembersAdmin>(&f, NoParams {}).await;
    assert_ne!(renamed.key(), decided.key());
    let (status, pending) = api.call("GET", "/v1/load/org/members", &api.admin).await;
    assert_eq!(status, StatusCode::OK, "{pending}");
    assert!(pending["computed_at"].is_null());
    let payload = fill::<MembersAdmin>(&f, &decided).await;
    assert_eq!(
        ids(&payload),
        vec![new_open, old_open, rejected, approved],
        "rejection moves the open row into the same recent-history array"
    );
    assert_eq!(payload["join_requests"][1]["decision"], "rejected");
    assert_eq!(
        serde_json::to_value(&decided.params).unwrap(),
        json!({}),
        "no per-request clock in the key"
    );
    assert_eq!(
        MembersAdmin::MAX_DELAY.as_secs(),
        60,
        "normal stored-model aging bounds quiet-window refresh eligibility"
    );
    // Deterministic model clock proof in real authorized READ ONLY snapshots;
    // never publish this future-clock payload under the present certification.
    let calendar_boundary = seed(
        &f,
        f.account,
        applicants[4],
        "2026-10-21T11:30:00Z".parse().unwrap(),
        Some(("approved", "2026-10-22T11:30:00Z".parse().unwrap())),
    )
    .await;
    let clock_scope = scope::<MembersAdmin>(&f, NoParams {}).await;
    rm::store::request_refresh::<MembersAdmin>(&f.app, &clock_scope)
        .await
        .unwrap();
    let claims = rm::store::claim(&f.app).await.unwrap();
    let claim = claims
        .iter()
        .find(|c| c.model == MembersAdmin::NAME && c.scope_key == clock_scope.key())
        .unwrap();
    let mut content = rm::store::begin_compute(&f.app, claim, MembersAdmin::COMPUTE_TIMEOUT)
        .await
        .unwrap();
    let mut control = rm::store::begin_compute(&f.control, claim, MembersAdmin::COMPUTE_TIMEOUT)
        .await
        .unwrap();
    let future = MembersAdmin::compute(
        rm::ComputeCx {
            tx: &mut content,
            control_tx: Some(&mut control),
            blobs: None,
            account_id: f.account,
            compute_as: f.user,
            graphs: decided.class.graphs(),
            now: now + Duration::days(16),
        },
        &NoParams {},
    )
    .await
    .unwrap();
    assert_eq!(
        ids(&serde_json::to_value(future).unwrap()),
        vec![new_open],
        "history expires according to the supplied compute clock while open requests remain"
    );
    sqlx::query("SET LOCAL timezone='America/New_York'")
        .execute(&mut *control)
        .await
        .unwrap();
    let calendar = MembersAdmin::compute(
        rm::ComputeCx {
            tx: &mut content,
            control_tx: Some(&mut control),
            blobs: None,
            account_id: f.account,
            compute_as: f.user,
            graphs: clock_scope.class.graphs(),
            now: "2026-11-05T12:00:00Z".parse().unwrap(),
        },
        &NoParams {},
    )
    .await
    .unwrap();
    assert!(ids(&serde_json::to_value(calendar).unwrap()).contains(&calendar_boundary),
        "legacy calendar-day subtraction crosses DST: cutoff is Oct22 11:00Z, not a fixed-UTC-duration 12:00Z");
    content.rollback().await.unwrap();
    control.rollback().await.unwrap();
    sqlx::query("UPDATE account_members SET role='member' WHERE account_id=$1 AND user_id=$2")
        .bind(f.account)
        .bind(f.user)
        .execute(&f.control_owner)
        .await
        .unwrap();
    for (method, path) in [
        ("GET", "/v1/load/org/members"),
        ("POST", "/v1/load/org/members/refresh"),
    ] {
        let (status, body) = api.call(method, path, &api.admin).await;
        assert_eq!(
            status,
            StatusCode::FORBIDDEN,
            "warmed history after role downgrade: {body}"
        );
    }
    drop(api);
    f.close().await;
}

#[tokio::test]
async fn member_nudge_refreshes_privileged_payload_without_changing_address() {
    use rm::{org::MembersAdmin, NoParams};
    let f = Fixture::new(true).await;
    let api = api::Api::new(&f).await;
    let initial = scope::<MembersAdmin>(&f, NoParams {}).await;
    fill::<MembersAdmin>(&f, &initial).await;
    assert!(!read::<MembersAdmin>(&f, &initial).await.stale);
    let email: String = sqlx::query_scalar("SELECT email FROM users WHERE id=$1")
        .bind(f.other)
        .fetch_one(&f.control_owner)
        .await
        .unwrap();
    for offset in [1, 2] {
        let notified: chrono::DateTime<chrono::Utc> = sqlx::query_scalar(
            "INSERT INTO slack_unmatched_email_notices(account_id,provider,email,last_notified_at) VALUES($1,'member_connect',$2,now()+make_interval(mins=>$3)) ON CONFLICT(provider,email) DO UPDATE SET last_notified_at=EXCLUDED.last_notified_at RETURNING last_notified_at")
            .bind(f.account).bind(&email).bind(offset).fetch_one(&f.control_owner).await.unwrap();
        let current = scope::<MembersAdmin>(&f, NoParams {}).await;
        assert_eq!(
            initial.key(),
            current.key(),
            "ordinary nudge observations retain the warmed address"
        );
        let previous = read::<MembersAdmin>(&f, &current).await;
        assert!(previous.computed_at.is_some() && previous.stale);
        let fresh = fill::<MembersAdmin>(&f, &current).await;
        let member = fresh["members"]
            .as_array()
            .unwrap()
            .iter()
            .find(|m| m["user_id"] == json!(f.other))
            .unwrap();
        let actual =
            chrono::DateTime::parse_from_rfc3339(member["connect_nudged_at"].as_str().unwrap())
                .unwrap();
        assert_eq!(actual.with_timezone(&chrono::Utc), notified);
        assert!(!read::<MembersAdmin>(&f, &current).await.stale);
    }
    drop(api);
    f.close().await;
}

#[tokio::test]
async fn bootstrap_journal_flags_match_independent_model_gates() {
    struct Restore(Vec<(&'static str, Option<std::ffi::OsString>)>);
    impl Drop for Restore {
        fn drop(&mut self) {
            for (k, v) in &self.0 {
                match v {
                    Some(v) => std::env::set_var(k, v),
                    None => std::env::remove_var(k),
                }
            }
        }
    }
    let _restore = Restore(
        ["TODAY_ACTIVITIES_ENABLED", "TODAY_GIST_ENABLED"]
            .into_iter()
            .map(|k| (k, std::env::var_os(k)))
            .collect(),
    );
    let f = Fixture::new(false).await;
    let api = api::Api::new(&f).await;
    let now = chrono::Utc::now();
    let from = (now - chrono::Duration::days(1)).to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
    let to = now.to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
    for (activities, gist) in [(false, false), (false, true), (true, false), (true, true)] {
        std::env::set_var(
            "TODAY_ACTIVITIES_ENABLED",
            if activities { "1" } else { "0" },
        );
        std::env::set_var("TODAY_GIST_ENABLED", if gist { "1" } else { "0" });
        let (status, body) = api.call("GET", "/v1/bootstrap", &api.admin).await;
        assert_eq!(status, axum::http::StatusCode::OK, "{body}");
        assert_eq!(body["activities_enabled"], activities);
        assert_eq!(body["gist_enabled"], gist);
        for path in [
            format!("/v1/journal/activities?from={from}&to={to}&who=mine&tz=UTC"),
            "/v1/journal/strip?tz=UTC".into(),
        ] {
            let (status, body) = api.call("GET", &path, &api.admin).await;
            assert_eq!(
                status,
                if activities {
                    axum::http::StatusCode::OK
                } else {
                    axum::http::StatusCode::NOT_FOUND
                },
                "{path}: {body}"
            );
        }
        let (status, body) = api.call("GET", "/v1/journal/gist?tz=UTC", &api.admin).await;
        assert_eq!(
            status,
            if gist {
                axum::http::StatusCode::OK
            } else {
                axum::http::StatusCode::NOT_FOUND
            },
            "independent gist gate: {body}"
        );
    }
    drop(api);
    f.close().await;
}

async fn receipt_purpose_isolation(org: bool) {
    use rm::oceans_pages::{ReefPage, ReefPageParams};
    let f = Fixture::new(org).await;
    let s = scope::<ReefPage>(
        &f,
        ReefPageParams {
            graph: f.graph,
            node_type: None,
            page: 1,
            page_size: 20,
        },
    )
    .await;
    fill::<ReefPage>(&f, &s).await;
    for purpose in ["cloud_embedding", "managed_enrichment"] {
        sqlx::query("INSERT INTO managed_budget_periods(account_id,purpose,period_start,limit_micro_usd) VALUES($1,$2,current_date,1000000)")
            .bind(f.account).bind(purpose).execute(&f.owner).await.unwrap();
        let receipt: Uuid = sqlx::query_scalar("INSERT INTO managed_attempt_receipts(account_id,purpose,period_start,graph_id,logical_call,attempt_no,candidate_models,input_bound_tokens,max_tokens,reserved_micro_usd,protocol_version,dispatch_phase,content_plane,request_hash,attributed_user_id) VALUES($1,$2,current_date,$3,gen_random_uuid(),1,ARRAY['fixture'],1,1,1000,1,'armed',$4,repeat('a',64),$5) RETURNING receipt_id")
            .bind(f.account).bind(purpose).bind(f.graph).bind(if org {"org:org_read_models"} else {"consumer"}).bind(f.user).fetch_one(&f.owner).await.unwrap();
        let inserted = read::<ReefPage>(&f, &s).await;
        assert!(inserted.computed_at.is_some());
        assert_eq!(
            inserted.stale,
            purpose == "managed_enrichment",
            "subscription follows the actual digest reader's purpose predicate"
        );
        if inserted.stale {
            fill::<ReefPage>(&f, &s).await;
        }
        sqlx::query("UPDATE managed_attempt_receipts SET state='billed',settled_micro_usd=10,served_model='fixture',settled_at=now() WHERE receipt_id=$1")
            .bind(receipt).execute(&f.owner).await.unwrap();
        let updated = read::<ReefPage>(&f, &s).await;
        assert!(updated.computed_at.is_some(), "a computed row is always served");
        assert_eq!(updated.stale, purpose == "managed_enrichment", "unrelated embedding settlement cannot dirty content; digest receipt updates remain reducing and are served stale");
        if purpose == "cloud_embedding" {
            assert!(!updated.stale);
        } else {
            fill::<ReefPage>(&f, &s).await;
        }
        let deletion = sqlx::query("DELETE FROM managed_attempt_receipts WHERE receipt_id=$1")
            .bind(receipt)
            .execute(&f.owner)
            .await;
        if purpose == "managed_enrichment" {
            assert!(deletion
                .unwrap_err()
                .to_string()
                .contains("source receipt history cannot be deleted"));
        } else {
            deletion.unwrap();
        }
        let deleted = read::<ReefPage>(&f, &s).await;
        assert!(deleted.computed_at.is_some() && !deleted.stale,
            "unrelated receipt deletion is harmless; protected digest history rejects deletion before any write");
    }
    f.close().await;
}

#[tokio::test]
async fn control_digest_receipt_subscription_excludes_embedding_bookkeeping() {
    receipt_purpose_isolation(false).await;
}
#[tokio::test]
async fn tenant_digest_receipt_subscription_excludes_embedding_bookkeeping() {
    receipt_purpose_isolation(true).await;
}

/// Explicitly selected scale proof; not credited when filtered out of fast tests.
#[tokio::test]
#[ignore = "run explicitly with --ignored and PAGE_MODEL_BENCH_OUTPUT"]
async fn atlas_topic_benchmark600k() {
    use rm::{
        oceans_pages::{Anchors, Atlas, AtlasParams},
        topics::{TopicPage, TopicParams},
    };
    use std::time::Instant;
    let _ = tracing_subscriber::fmt()
        .with_env_filter("coralswarm_backend::routes::page_read=warn")
        .with_test_writer()
        .try_init();
    let output =
        std::env::var("PAGE_MODEL_BENCH_OUTPUT").expect("owned benchmark artifact required");
    let f = Fixture::new(true).await;
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
    let mut conn = f.owner.acquire().await.unwrap();
    for statement in seed.split(';').filter(|s| !s.trim().is_empty()) {
        sqlx::raw_sql(statement).execute(&mut *conn).await.unwrap();
    }
    drop(conn);
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
    assert_eq!(count, 600000);
    let disabled:i64=sqlx::query_scalar("SELECT count(*) FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid WHERE c.relnamespace='public'::regnamespace AND NOT t.tgisinternal AND t.tgenabled='D'").fetch_one(&f.owner).await.unwrap();
    assert_eq!(disabled, 0);
    let api = api::Api::new(&f).await;
    async fn bench<M: ReadModel>(f: &Fixture, api: &api::Api, p: M::Params, path: &str) -> Value {
        eprintln!("L4_BENCH_START={}", M::NAME);
        let s = scope::<M>(f, p).await;
        let mut computes = vec![];
        let mut fills = vec![];
        for _ in 0..3 {
            let start = Instant::now();
            let (_, compute_ms) = fill_timed::<M>(f, &s).await;
            computes.push(compute_ms);
            fills.push(start.elapsed().as_secs_f64() * 1000.);
        }
        let mut stored = vec![];
        let mut http = vec![];
        let mut handler_body = vec![];
        let mut client_decode = vec![];
        for _ in 0..20 {
            let start = Instant::now();
            let mut tx = f.app.begin_with("BEGIN READ ONLY").await.unwrap();
            sqlx::query("SELECT set_config('app.account_id',$1,true)")
                .bind(f.account.to_string())
                .execute(&mut *tx)
                .await
                .unwrap();
            coralswarm_backend::tenancy::scope::set_tenant_user(&mut tx, f.user)
                .await
                .unwrap();
            sqlx::query("SET LOCAL statement_timeout='1s'")
                .execute(&mut *tx)
                .await
                .unwrap();
            assert!(rm::store::read::<M>(&mut tx, &s)
                .await
                .unwrap()
                .computed_at
                .is_some());
            tx.rollback().await.unwrap();
            stored.push(start.elapsed().as_secs_f64() * 1000.);
            let start = Instant::now();
            let (status, body) = api.call_body("GET", path, &api.admin, None).await;
            handler_body.push(start.elapsed().as_secs_f64() * 1000.);
            let decode_start = Instant::now();
            let v: Value = serde_json::from_slice(&body).unwrap();
            client_decode.push(decode_start.elapsed().as_secs_f64() * 1000.);
            assert_eq!(status, axum::http::StatusCode::OK, "{v}");
            assert!(v["computed_at"].is_string());
            http.push(start.elapsed().as_secs_f64() * 1000.);
        }
        stored.sort_by(f64::total_cmp);
        http.sort_by(f64::total_cmp);
        handler_body.sort_by(f64::total_cmp);
        client_decode.sort_by(f64::total_cmp);
        json!({"worker_fill_ms":fills,"worker_compute_ms":computes,"stored_sql_p50_ms":stored[9],"stored_sql_p95_ms":stored[18],"authenticated_handler_body_p95_ms":handler_body[18],"test_client_json_decode_p95_ms":client_decode[18],"actual_http_p50_ms":http[9],"actual_http_p95_ms":http[18],"samples":20})
    }
    let graph: Uuid = "22222222-0000-0000-0000-000000000001".parse().unwrap();
    let node:Uuid=sqlx::query_scalar("SELECT n.node_id FROM nodes n WHERE n.graph_id=$1 AND n.type='entity' AND library_node_permitted(n.graph_id,n.node_id) ORDER BY n.coral_count DESC,n.node_id LIMIT 1")
        .bind(graph).fetch_one(&f.owner).await.unwrap();
    let atlas = bench::<Atlas>(
        &f,
        &api,
        AtlasParams {
            graph,
            cooccur: None,
            anchors: Anchors::Top,
        },
        &format!("/v1/graphs/{graph}/atlas"),
    )
    .await;
    std::fs::write(
        format!("{output}.atlas-only.json"),
        serde_json::to_vec_pretty(
            &json!({"atoms":count,"atlas":atlas,"status":"Atlas passed; Topic still required"}),
        )
        .unwrap(),
    )
    .unwrap();
    let topic = bench::<TopicPage>(
        &f,
        &api,
        TopicParams { node },
        &format!("/v1/load/topics/{node}"),
    )
    .await;
    use sha2::Digest;
    let result = json!({"atoms":count,"seed_sha256":hex::encode(sha2::Sha256::digest(include_bytes!("fixtures/page_models_600k.sql"))),"atlas":atlas,"topic":topic,"port":15761,"serving_role":"nonowner_nobypassrls","ordinary_compute_statement_timeout_seconds":30,"content_graph_scope":"member"});
    std::fs::write(output, serde_json::to_vec_pretty(&result).unwrap()).unwrap();
    eprintln!("L4_BENCH_600K={result}");
    drop(api);
    f.close().await;
}

async fn capture_selector_contract(org: bool) {
    use axum::http::StatusCode;
    use rm::library::{CaptureDetail, CaptureKind, CaptureParams};
    use rm::Params;
    let f = Fixture::new(org).await;
    let api = api::Api::new(&f).await;
    let team=if org {
        sqlx::query("UPDATE graphs SET kind='team',owner_user_id=NULL WHERE id=$1").bind(f.graph).execute(&f.owner).await.unwrap();
        let team:Uuid=sqlx::query_scalar("INSERT INTO teams(account_id,graph_id,name,created_by) VALUES($1,$2,'Capture floor',$3) RETURNING id").bind(f.account).bind(f.graph).bind(f.user).fetch_one(&f.owner).await.unwrap();
        sqlx::query("INSERT INTO team_members(team_id,user_id,role) VALUES($1,$2,'member')").bind(team).bind(f.user).execute(&f.owner).await.unwrap();
        sqlx::query("INSERT INTO team_seat_grants(account_id,team_id,user_id,source,history_from) VALUES($1,$2,$3,'fixture',NULL)").bind(f.account).bind(team).bind(f.user).execute(&f.owner).await.unwrap();
        Some(team)
    } else { None };
    let item:Uuid=sqlx::query_scalar("INSERT INTO source_items(graph_id,source_type,provider_namespace,source_id) VALUES($1,'note','fixture:l4','selector-history') RETURNING source_item_id").bind(f.graph).fetch_one(&f.owner).await.unwrap();
    let versions:Vec<Uuid>=sqlx::query_scalar("INSERT INTO source_versions(graph_id,source_item_id,source_type,provider_namespace,source_id,raw_inline,extraction_version,extraction_status,captured_at) SELECT $1,$2,'note','fixture:l4','selector-history','Historical selector evidence','v1','done',now()-n*interval '1 hour' FROM generate_series(1,25) n RETURNING version_id").bind(f.graph).bind(item).fetch_all(&f.owner).await.unwrap();
    let current=versions[0]; let historical=versions[24];
    sqlx::query("UPDATE source_items SET current_version_id=$2 WHERE source_item_id=$1").bind(item).bind(current).execute(&f.owner).await.unwrap();
    let atoms:Vec<Uuid>=sqlx::query_scalar("INSERT INTO semantic_atoms(graph_id,source_version_id,ordinal,kind,statement,extraction_version,model) SELECT $1,$2,n,'fact','Focused historical evidence '||n,'v1','fixture' FROM generate_series(0,219) n RETURNING semantic_atom_id").bind(f.graph).bind(historical).fetch_all(&f.owner).await.unwrap();
    let focus=atoms[115];
    let p=CaptureParams{graph:f.graph,source:item.to_string(),kind:CaptureKind::Source,version_id:Some(historical),atom_id:Some(focus)};
    let s=scope::<CaptureDetail>(&f,p.clone()).await;
    let path=format!("/v1/load/captures?graph={}&source={item}&version_id={historical}&atom_id={focus}",f.graph);
    let (status,miss)=api.call("GET",&path,&api.admin).await;
    assert_eq!(status,StatusCode::OK,"{miss}"); assert!(miss["computed_at"].is_null());
    let payload=fill::<CaptureDetail>(&f,&s).await;
    let ev=&payload["evidence"];
    assert_eq!(ev["selected_version"]["version_id"],json!(historical));
    assert_eq!(ev["selected_version"]["raw_text"],"Historical selector evidence");
    assert_eq!(ev["selected_version"]["atoms"][0]["atom_id"],json!(focus));
    assert_eq!(ev["selected_version"]["atoms"].as_array().unwrap().len(),100);
    assert!(ev["versions"].as_array().unwrap().iter().any(|v|v["version_id"]==json!(historical)));
    assert_eq!(ev["versions"].as_array().unwrap().len(),21);
    let (status,warm)=api.call("GET",&path,&api.admin).await; assert_eq!(status,StatusCode::OK); assert_eq!(warm["payload"],payload,"{warm}");
    // Later atom and version cursors still use the existing interactive route.
    let cursor=ev["selected_version"]["atoms_next_cursor"].as_str().unwrap();
    let interactive=format!("/v1/sources/evidence?graph_id={}&source_item_id={item}&version_id={historical}&atoms_after={cursor}",f.graph);
    let (status,page)=api.call("GET",&interactive,&api.admin).await; assert_eq!(status,StatusCode::OK,"{page}");
    assert_eq!(page["selected_version"]["atoms"][0]["atom_id"],json!(atoms[215]));
    let cursor=ev["next_cursor"].as_str().unwrap();
    let (status,page)=api.call("GET",&format!("/v1/sources/evidence?graph_id={}&source_item_id={item}&before={cursor}",f.graph),&api.admin).await;
    assert_eq!(status,StatusCode::OK,"{page}"); assert_eq!(page["versions"].as_array().unwrap().len(),5);
    let other:Uuid=sqlx::query_scalar("INSERT INTO source_items(graph_id,source_type,provider_namespace,source_id) VALUES($1,'note','fixture:l4','wrong-source') RETURNING source_item_id").bind(f.graph).fetch_one(&f.owner).await.unwrap();
    let other_atom:Uuid=sqlx::query_scalar("INSERT INTO semantic_atoms(graph_id,source_version_id,ordinal,kind,statement,extraction_version,model) VALUES($1,$2,0,'fact','Other version','v1','fixture') RETURNING semantic_atom_id").bind(f.graph).bind(current).fetch_one(&f.owner).await.unwrap();
    for (graph,source,version,atom,expected) in [
        (f.graph,item,Uuid::new_v4(),focus,StatusCode::NOT_FOUND),
        (f.graph,other,historical,focus,StatusCode::NOT_FOUND),
        (f.foreign,item,historical,focus,StatusCode::NOT_FOUND),
        (f.graph,item,historical,other_atom,StatusCode::BAD_REQUEST),
        (f.graph,item,current,focus,StatusCode::BAD_REQUEST),
    ] {
        for (method,suffix) in [("GET",""),("POST","/refresh")] {
            let path=format!("/v1/load/captures{suffix}?graph={graph}&source={source}&version_id={version}&atom_id={atom}");
            let (status,v)=api.call(method,&path,&api.admin).await; assert_eq!(status,expected,"{method} {path}: {v}");
        }
    }
    sqlx::query("INSERT INTO meeting_transcripts(graph_id,source_id,source_item_id,current_version_id) VALUES($1,'selector-history',$2,$3)").bind(f.graph).bind(item).bind(current).execute(&f.owner).await.unwrap();
    let meeting=CaptureParams{source:"selector-history".into(),kind:CaptureKind::Meeting,..p.clone()};
    let ms=scope::<CaptureDetail>(&f,meeting).await;
    let mv=fill::<CaptureDetail>(&f,&ms).await;
    assert_eq!(mv["evidence"]["selected_version"]["version_id"],json!(historical));
    assert_eq!(mv["evidence"]["selected_version"]["atoms"][0]["atom_id"],json!(focus));
    assert_eq!(mv["summary"]["projection_status"],"unverified_lineage");
    if let Some(team) = team {
        sqlx::query("UPDATE team_seat_grants SET history_from=now()-interval '12 hours' WHERE team_id=$1 AND user_id=$2").bind(team).bind(f.user).execute(&f.owner).await.unwrap();
        for (method,suffix) in [("GET",""),("POST","/refresh")] {
            let path=format!("/v1/load/captures{suffix}?graph={}&source={item}&version_id={historical}&atom_id={focus}",f.graph);
            let (status,v)=api.call(method,&path,&api.admin).await; assert_eq!(status,StatusCode::NOT_FOUND,"pre-floor {v}");
        }
        sqlx::query("UPDATE team_seat_grants SET history_from=NULL WHERE team_id=$1 AND user_id=$2").bind(team).bind(f.user).execute(&f.owner).await.unwrap();
    }
    let passage:Uuid=sqlx::query_scalar("INSERT INTO semantic_atom_passages(graph_id,source_version_id,semantic_atom_id,quote,start_char,end_char) VALUES($1,$2,$3,'Old historical passage',0,10) RETURNING passage_id").bind(f.graph).bind(historical).bind(focus).fetch_one(&f.owner).await.unwrap();
    fill::<CaptureDetail>(&f,&s).await;
    sqlx::query("UPDATE semantic_atom_passages SET quote='Corrected historical passage' WHERE passage_id=$1").bind(passage).execute(&f.owner).await.unwrap();
    stale_behind::<CaptureDetail>(&f,&s).await;
    let corrected=fill::<CaptureDetail>(&f,&s).await;
    assert_eq!(corrected["evidence"]["selected_version"]["atoms"][0]["passages"][0]["quote"],"Corrected historical passage");
    sqlx::query("UPDATE source_versions SET raw_inline='Corrected historical source' WHERE version_id=$1").bind(historical).execute(&f.owner).await.unwrap();
    stale_behind::<CaptureDetail>(&f,&s).await;
    assert_eq!(fill::<CaptureDetail>(&f,&s).await["evidence"]["selected_version"]["raw_text"],"Corrected historical source");
    // A focus may exist but be obsolete; it must never be forced into served output.
    sqlx::query("UPDATE semantic_atoms SET extraction_version='obsolete' WHERE semantic_atom_id=$1").bind(focus).execute(&f.owner).await.unwrap();
    stale_behind::<CaptureDetail>(&f,&s).await;
    let payload=fill::<CaptureDetail>(&f,&s).await;
    assert!(!payload["evidence"]["selected_version"]["atoms"].as_array().unwrap().iter().any(|v|v["atom_id"]==json!(focus)));
    assert_eq!(payload["evidence"]["selected_version"]["atoms"][0]["atom_id"],json!(atoms[116]));
    sqlx::query("UPDATE source_items SET access_state='restricted' WHERE source_item_id=$1").bind(item).execute(&f.owner).await.unwrap();
    for (method,suffix) in [("GET",""),("POST","/refresh")] {
        let path=format!("/v1/load/captures{suffix}?graph={}&source={item}&version_id={historical}&atom_id={focus}",f.graph);
        let (status,v)=api.call(method,&path,&api.admin).await; assert_eq!(status,StatusCode::NOT_FOUND,"{v}");
    }
    // The new indexed selector seam retains request budgets and exactly-once accounting.
    use coralswarm_backend::request_timing::{now_secs, PAGE_READ_LEDGER};
    let route_stats=|| PAGE_READ_LEDGER.snapshot(now_secs()).per_route.into_iter().find(|r|r.route=="/v1/load/captures").unwrap();
    let before=route_stats();
    let mut blocker=f.owner.begin().await.unwrap();
    sqlx::query("LOCK TABLE source_items IN ACCESS EXCLUSIVE MODE").execute(&mut *blocker).await.unwrap();
    let started=std::time::Instant::now();
    let (status,v)=api.call("GET",&path,&api.admin).await;
    assert_eq!(status,StatusCode::SERVICE_UNAVAILABLE,"{v}");
    assert!(started.elapsed()<std::time::Duration::from_secs(2));
    blocker.rollback().await.unwrap();
    let after=route_stats();
    assert_eq!(after.statement_timeouts,before.statement_timeouts+1);
    assert_eq!(after.errors_5xx,before.errors_5xx+1);
    assert_eq!(after.acquire_timeouts,before.acquire_timeouts);
    // UI passage highlighting and interactive cursors are deliberately not model keys.
    for key in ["passage_id","before","atoms_after"] {
        assert!(CaptureParams::parse(Default::default(),[("graph".into(),f.graph.to_string()),("source".into(),item.to_string()),(key.into(),Uuid::new_v4().to_string())].into()).is_err());
    }
    f.close().await;
}
#[tokio::test]
async fn control_capture_historical_focused_selectors() { capture_selector_contract(false).await }
#[tokio::test]
async fn tenant_capture_historical_focused_selectors() { capture_selector_contract(true).await }

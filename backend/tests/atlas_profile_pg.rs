//! Explicit local release/dev comparison; never a production latency claim.
#[path = "support/page_model_api.rs"]
mod api;
#[path = "support/page_model_fixture.rs"]
mod fixture;
use coralswarm_backend::{
    read_models::{self as rm, ReadModel},
    tenancy::scope::begin_tenant_scoped,
    workers::read_model_refresh,
};
use fixture::Fixture;
use rm::oceans_pages::{Anchors, Atlas, AtlasParams};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{path::Path, time::Instant};
use uuid::Uuid;

fn sha(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}
fn canonical(v: &Value) -> Value {
    // Atlas array order has no API sorting contract; compare every field/row.
    match v {
        Value::Array(a) => {
            let mut a: Vec<_> = a.iter().map(canonical).collect();
            a.sort_by_cached_key(|v| serde_json::to_string(v).unwrap());
            Value::Array(a)
        }
        Value::Object(o) => {
            Value::Object(o.iter().map(|(k, v)| (k.clone(), canonical(v))).collect())
        }
        v => v.clone(),
    }
}
async fn fill(f: &Fixture, p: AtlasParams) -> (rm::Scope<AtlasParams>, Value, f64) {
    let mut tx = begin_tenant_scoped(&f.app, f.account, f.user)
        .await
        .unwrap();
    let s = rm::Viewer::load(&mut tx, f.account, f.user)
        .await
        .unwrap()
        .scope::<Atlas>(p)
        .unwrap();
    tx.rollback().await.unwrap();
    rm::store::request_refresh::<Atlas>(&f.app, &s)
        .await
        .unwrap();
    let claims = rm::store::claim(&f.app).await.unwrap();
    let c = claims
        .iter()
        .find(|c| c.model == Atlas::NAME && c.scope_key == s.key())
        .unwrap();
    let start = Instant::now();
    let model = rm::registry::by_name(Atlas::NAME).unwrap();
    let (value, snapshot) = tokio::time::timeout(
        read_model_refresh::compute_total_timeout(model),
        read_model_refresh::compute(&f.worker(), c, &f.control),
    )
    .await
    .expect("unchanged total compute budget")
    .unwrap();
    let elapsed = start.elapsed().as_secs_f64() * 1000.;
    assert!(rm::store::publish(&f.app, c, value.clone(), &snapshot)
        .await
        .unwrap());
    (s, value, elapsed)
}
async fn seed(f: &Fixture) {
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
    for (key, id) in [("acct", f.account), ("u1", f.user), ("u2", f.other)] {
        seed = seed.replace(&format!(":{key}"), &format!("'{id}'"));
    }
    for (key, id) in [
        ("g_org", "22222222-0000-0000-0000-000000000001"),
        ("g_team", "22222222-0000-0000-0000-000000000002"),
        ("g_mem", "22222222-0000-0000-0000-000000000003"),
    ] {
        seed = seed.replace(&format!(":{key}"), &format!("'{id}'"));
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
}
async fn fingerprint(f: &Fixture) -> Value {
    // Server-side digests include all seeded content rows, including NULLs and
    // timestamps. Cache/identity bookkeeping is intentionally a separate domain.
    let mut rows = serde_json::Map::new();
    for (table, key) in [
        ("atoms", "atom_id"),
        ("nodes", "node_id"),
        ("node_atoms", "node_id,atom_id"),
        ("source_items", "source_item_id"),
        ("source_versions", "version_id"),
        ("source_version_chunks", "source_version_id,atom_id"),
        ("semantic_atoms", "semantic_atom_id"),
        ("node_semantic_atoms", "node_id,semantic_atom_id"),
        ("team_seat_grants", "id"),
    ] {
        let v:Value=sqlx::query_scalar(&format!("SELECT jsonb_build_object('count',count(*),'md5',md5(string_agg(md5(to_jsonb(t)::text),'' ORDER BY {key}))) FROM {table} t")).fetch_one(&f.owner).await.unwrap();
        rows.insert(table.into(), v);
    }
    Value::Object(rows)
}
fn stats(samples: &[f64]) -> Value {
    let mut v = samples.to_vec();
    v.sort_by(f64::total_cmp);
    json!({"p50":v[(v.len() as f64*0.50).ceil() as usize-1],"p95":v[(v.len() as f64*0.95).ceil() as usize-1],"max":v.last(),"samples":samples})
}
#[tokio::test]
#[ignore = "explicit profile measurement with owned fixture manifest/output"]
async fn retained_atlas_profile600k() {
    let manifest =
        std::env::var("ATLAS_PROFILE_FIXTURE").expect("private retained fixture manifest required");
    let output = std::env::var("ATLAS_PROFILE_OUTPUT").expect("profile artifact required");
    let profile = std::env::var("ATLAS_PROFILE_LABEL").expect("explicit profile label required");
    assert_eq!(
        std::env::var("ORGT_TEST_RETAIN_DATABASES").as_deref(),
        Ok("1")
    );
    let reused = Path::new(&manifest).exists();
    let f = if reused {
        Fixture::reopen_benchmark(Path::new(&manifest)).await
    } else {
        Fixture::new(true).await
    };
    if !reused {
        seed(&f).await;
    }
    let api = if reused {
        api::Api::reopen_benchmark(&f).await
    } else {
        api::Api::new(&f).await
    };
    if !reused {
        f.retain_benchmark(Path::new(&manifest));
    }
    let attrs:(String,bool,bool,i64)=sqlx::query_as("SELECT current_user,rolsuper,rolbypassrls,(SELECT count(*) FROM pg_class WHERE relnamespace='public'::regnamespace AND relowner=(SELECT oid FROM pg_roles WHERE rolname=current_user)) FROM pg_roles WHERE rolname=current_user").fetch_one(&f.app).await.unwrap();
    assert_eq!((attrs.1, attrs.2, attrs.3), (false, false, 0));
    let before = fingerprint(&f).await;
    assert_eq!(before["atoms"]["count"], 600000);
    let disabled:i64=sqlx::query_scalar("SELECT count(*) FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid WHERE c.relnamespace='public'::regnamespace AND NOT t.tgisinternal AND t.tgenabled='D'").fetch_one(&f.owner).await.unwrap();
    assert_eq!(disabled, 0);
    let graph: Uuid = "22222222-0000-0000-0000-000000000001".parse().unwrap();
    let path = format!("/v1/graphs/{graph}/atlas");
    let mut computes = vec![];
    let mut expected = Value::Null;
    let mut scope = None;
    for _ in 0..3 {
        let (s, v, ms) = fill(
            &f,
            AtlasParams {
                graph,
                cooccur: None,
                anchors: Anchors::Top,
            },
        )
        .await;
        expected = v;
        scope = Some(s);
        computes.push(ms);
    }
    let scope = scope.unwrap();
    let expected_raw = expected;
    let expected = canonical(&expected_raw);
    let payload_sha = sha(&serde_json::to_vec(&expected).unwrap());
    // Actual worker-filled payload, full field comparison, never fabricated JSON.
    // First authenticated router request includes a cold JWKS/tenant resolver.
    // Actual worker fills have warmed the DB; PG/OS caches are not flushed.
    let mut body_ms = vec![];
    let mut parse_ms = vec![];
    let mut full_ms = vec![];
    let mut sizes = vec![];
    let mut first = Value::Null;
    for i in 0..56 {
        let start = Instant::now();
        let (status, body) = api.call_body("GET", &path, &api.admin, None).await;
        let handler = start.elapsed().as_secs_f64() * 1000.;
        let parse_start = Instant::now();
        let value: Value = serde_json::from_slice(&body).unwrap();
        let parse = parse_start.elapsed().as_secs_f64() * 1000.;
        let full = start.elapsed().as_secs_f64() * 1000.;
        assert_eq!(status, axum::http::StatusCode::OK);
        assert!(value["computed_at"].is_string());
        assert_eq!(value["stale"],false,"actual fresh worker-filled envelope");
        assert_eq!(
            &value["payload"],
            &expected_raw,
            "full eligible payload including nullable fields"
        );
        if i == 0 {
            first = json!({"handler_body_ms":handler,"client_parse_ms":parse,"full_client_ms":full,"bytes":body.len()});
        }
        if i >= 6 {
            body_ms.push(handler);
            parse_ms.push(parse);
            full_ms.push(full);
            sizes.push(body.len());
        }
    }
    let mut stored = vec![];
    for _ in 0..50 {
        let start = Instant::now();
        let mut tx = f.app.begin_with("BEGIN READ ONLY").await.unwrap();
        sqlx::query("SELECT set_config('app.account_id',$1,true),set_config('app.user_id',$2,true),set_config('app.tenant_actor','',true),set_config('statement_timeout','1s',true)").bind(f.account.to_string()).bind(f.user.to_string()).execute(&mut *tx).await.unwrap();
        let v = rm::store::read::<Atlas>(&mut tx, &scope).await.unwrap();
        assert!(v.computed_at.is_some());
        tx.rollback().await.unwrap();
        stored.push(start.elapsed().as_secs_f64() * 1000.);
    }
    let full_path = |g: Uuid| format!("/v1/graphs/{g}/atlas");
    let private: Uuid = "22222222-0000-0000-0000-000000000003".parse().unwrap();
    let (_, _, private_compute) = fill(
        &f,
        AtlasParams {
            graph: private,
            cooccur: None,
            anchors: Anchors::Top,
        },
    )
    .await;
    let (status, own) = api.call("GET", &full_path(private), &api.admin).await;
    assert_eq!(status, axum::http::StatusCode::OK);
    assert!(own["computed_at"].is_string());
    let mut negatives = serde_json::Map::new();
    for (label, method, url, token, expected_status) in [
        (
            "warmed_private_get",
            "GET",
            full_path(private),
            api.member.as_str(),
            403,
        ),
        (
            "warmed_private_refresh",
            "POST",
            format!("{}/refresh", full_path(private)),
            api.member.as_str(),
            403,
        ),
        ("invalid_credential", "GET", path.clone(), "invalid", 401),
        (
            "foreign_graph",
            "GET",
            full_path(Uuid::new_v4()),
            api.admin.as_str(),
            403,
        ),
    ] {
        let (s, _) = api.call(method, &url, token).await;
        assert_eq!(s.as_u16(), expected_status, "{label}");
        negatives.insert(label.into(), json!(s.as_u16()));
    }
    assert_eq!(
        before,
        fingerprint(&f).await,
        "fixture inputs changed during measurement"
    );
    let result = json!({"profile":profile,"build_source_commit":env!("CORALSWARM_GIT_SHA_FULL"),"test_source_sha256":sha(include_bytes!("atlas_profile_pg.rs")),"fixture_helper_sha256":sha(include_bytes!("support/page_model_fixture.rs")),"api_helper_sha256":sha(include_bytes!("support/page_model_api.rs")),"fixture_reused":reused,"fixture":before,"seed_sha256":sha(include_bytes!("fixtures/page_models_600k.sql")),"serving_role":attrs.0,"role_superuser":false,"role_bypassrls":false,"role_owned_relations":0,"scope":"member graph only","params":{"cooccur":null,"anchors":"top"},"payload_sha256":payload_sha,"nodes":expected["nodes"].as_array().unwrap().len(),"links":expected["links"].as_array().unwrap().len(),"payload":expected,"negative_oracles":negatives,"worker_compute_ms":computes,"private_worker_compute_ms":private_compute,"handler_body_ms":stats(&body_ms),"client_json_parse_ms":stats(&parse_ms),"full_client_ms":stats(&full_ms),"stored_sql_typed_decode_ms":stats(&stored),"body_bytes":sizes,"first_atlas_request":first,"warmup_requests":6,"sample_count":50,"request_statement_timeout_ms":1000,"compute_statement_timeout_ms":30000,"compute_total_timeout_ms":35000,"method":"local authenticated in-process router + complete body collection; real worker claims/fill; first authenticated router request separately reported (cold JWKS/resolver; DB warmed by real worker fills); six warmups excluded, fifty consecutive measured requests; PG/OS caches not flushed; no deployed-network claim"});
    std::fs::write(output, serde_json::to_vec_pretty(&result).unwrap()).unwrap();
    eprintln!(
        "ATLAS_PROFILE_RESULT profile={profile} p95={} payload_sha256={payload_sha}",
        result["handler_body_ms"]["p95"]
    );
    drop(api);
    f.close().await;
}

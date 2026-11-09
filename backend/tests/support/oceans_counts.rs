//! Real COUNTS_SQL work must scale with admitted evidence, not unrelated links.
use super::*;
use rm::oceans::COUNTS_SQL;

async fn plan(f: &Fixture, graph: Uuid, owner: bool) -> Value {
    plan_sql(f, graph, owner, COUNTS_SQL).await
}

async fn plan_sql(f: &Fixture, graph: Uuid, owner: bool, sql: &str) -> Value {
    let mut tx = begin_tenant_scoped(if owner { &f.owner } else { &f.app }, f.account, f.user)
        .await
        .unwrap();
    sqlx::query("SET TRANSACTION READ ONLY")
        .execute(&mut *tx)
        .await
        .unwrap();
    let p = sqlx::query_scalar(&format!("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {sql}"))
        .bind(graph)
        .fetch_one(&mut *tx)
        .await
        .unwrap();
    tx.rollback().await.unwrap();
    p
}

fn blocks(p: &Value) -> u64 {
    p[0]["Plan"]["Shared Hit Blocks"].as_u64().unwrap()
        + p[0]["Plan"]["Shared Read Blocks"].as_u64().unwrap()
}

fn link_rows(n: &Value) -> f64 {
    let own = if n["Relation Name"] == "node_atoms" {
        (n["Actual Rows"].as_f64().unwrap()
            + n["Rows Removed by Filter"].as_f64().unwrap_or(0.)
            + n["Rows Removed by Index Recheck"].as_f64().unwrap_or(0.))
            * n["Actual Loops"].as_f64().unwrap()
    } else {
        0.
    };
    own + n["Plans"]
        .as_array()
        .map(|children| children.iter().map(link_rows).sum::<f64>())
        .unwrap_or(0.)
}

#[test]
fn oceans_plan_work_counter_counts_bitmap_recheck_rejects() {
    let p = json!({
        "Node Type": "Aggregate",
        "Plans": [{
            "Node Type": "Bitmap Heap Scan",
            "Relation Name": "node_atoms",
            "Actual Rows": 1,
            "Rows Removed by Index Recheck": 5,
            "Actual Loops": 128,
            "Plans": [{
                "Node Type": "Bitmap Index Scan",
                "Index Name": "node_atoms_pkey",
                "Actual Rows": 768,
                "Actual Loops": 1
            }]
        }]
    });
    assert_eq!(link_rows(&p), 768.);
    assert!(
        link_rows(&p) > 640.,
        "recheck rejects must not evade the work bound"
    );
}

#[test]
fn oceans_plan_work_counter_preserves_filter_and_loop_work() {
    let p = json!({
        "Node Type": "Append",
        "Plans": [
            {
                "Relation Name": "node_atoms",
                "Actual Rows": 1,
                "Rows Removed by Filter": 2,
                "Rows Removed by Index Recheck": 3,
                "Actual Loops": 4
            },
            { "Relation Name": "node_atoms", "Actual Rows": 5, "Actual Loops": 2 },
            { "Relation Name": "nodes", "Actual Rows": 999, "Actual Loops": 1 }
        ]
    });
    assert_eq!(link_rows(&p), 34.);
}

async fn seed_links(f: &Fixture, graph: Uuid, nodes: i32, atoms: i32) {
    sqlx::query("INSERT INTO atoms(graph_id,source_type,source_id,content_blob_key,content_inline,timestamp) SELECT $1,'note','cost:'||i,'fixture','Synthetic count evidence',now() FROM generate_series(1,$2) i")
        .bind(graph).bind(atoms).execute(&f.owner).await.unwrap();
    sqlx::query("INSERT INTO nodes(graph_id,type,title,body) SELECT $1,'entity','Synthetic count reef '||i,'{}' FROM generate_series(1,$2) i")
        .bind(graph).bind(nodes).execute(&f.owner).await.unwrap();
    // Five links per node, with ten admitted target atoms including unlinked ones.
    sqlx::query("INSERT INTO node_atoms(node_id,atom_id) SELECT n.node_id,a.atom_id FROM nodes n CROSS JOIN LATERAL (SELECT atom_id FROM atoms WHERE graph_id=$1 ORDER BY atom_id LIMIT 5) a WHERE n.graph_id=$1")
        .bind(graph).execute(&f.owner).await.unwrap();
}

type Counts = (Value, i64, i64);

async fn count_pair(f: &Fixture, graph: Uuid, actor: Uuid, owner: bool) -> Counts {
    let mut tx = begin_tenant_scoped(if owner { &f.owner } else { &f.app }, f.account, actor)
        .await
        .unwrap();
    sqlx::query("SET TRANSACTION READ ONLY")
        .execute(&mut *tx)
        .await
        .unwrap();
    let old: Counts = sqlx::query_as("SELECT * FROM graph_workspace_counts_compute($1)")
        .bind(graph)
        .fetch_one(&mut *tx)
        .await
        .unwrap();
    let new: Counts = sqlx::query_as(COUNTS_SQL)
        .bind(graph)
        .fetch_one(&mut *tx)
        .await
        .unwrap();
    assert_eq!(
        old, new,
        "released SQL function versus actual counts seam; owner={owner}"
    );
    tx.rollback().await.unwrap();
    new
}

async fn node(f: &Fixture, kind: Option<&str>) -> Uuid {
    sqlx::query_scalar("INSERT INTO nodes(graph_id,type,title,body) VALUES($1,$2,$3,'{\"decided_at\":\"2026-01-01\"}') RETURNING node_id")
        .bind(f.graph).bind(kind).bind(format!("Count parity reef {}",Uuid::new_v4())).fetch_one(&f.owner).await.unwrap()
}

struct Evidence {
    item: Uuid,
    version: Uuid,
    atom: Uuid,
    semantic: Uuid,
}
async fn evidence(f: &Fixture, node: Uuid, tag: &str, access: &str, days: i32) -> Evidence {
    let origin = json!({"service_id":"slack","content_kind":"message","provider_namespace":tag,"connection":{"kind":"oauth_grant","id":"fixture"},"container":{"kind":"channel","id":"fixture","provider_namespace":tag}});
    let item = sqlx::query_scalar("INSERT INTO source_items(graph_id,source_type,provider_namespace,source_id,origin,access_state) VALUES($1,'slack',$2,$2,$3,$4) RETURNING source_item_id")
        .bind(f.graph).bind(tag).bind(&origin).bind(access).fetch_one(&f.owner).await.unwrap();
    let version = sqlx::query_scalar("INSERT INTO source_versions(graph_id,source_item_id,source_id,source_type,provider_namespace,origin,raw_inline,source_metadata,occurred_at,captured_at,extraction_status) VALUES($1,$2,$3,'slack',$3,$4,'Count parity evidence','{}',now()-make_interval(days=>$5),now(),'done') RETURNING version_id")
        .bind(f.graph).bind(item).bind(tag).bind(&origin).bind(days).fetch_one(&f.owner).await.unwrap();
    sqlx::query("UPDATE source_items SET current_version_id=$2 WHERE source_item_id=$1")
        .bind(item)
        .bind(version)
        .execute(&f.owner)
        .await
        .unwrap();
    let atom = sqlx::query_scalar("INSERT INTO atoms(graph_id,source_type,source_id,content_blob_key,content_inline,timestamp,source_evidence_bound) VALUES($1,'slack',$2,'fixture','Count parity evidence',now()-make_interval(days=>$3),true) RETURNING atom_id")
        .bind(f.graph).bind(format!("{tag}:chunk:1")).bind(days).fetch_one(&f.owner).await.unwrap();
    sqlx::query(
        "INSERT INTO source_version_chunks(graph_id,source_version_id,atom_id) VALUES($1,$2,$3)",
    )
    .bind(f.graph)
    .bind(version)
    .bind(atom)
    .execute(&f.owner)
    .await
    .unwrap();
    sqlx::query("INSERT INTO node_atoms(node_id,atom_id) VALUES($1,$2)")
        .bind(node)
        .bind(atom)
        .execute(&f.owner)
        .await
        .unwrap();
    let semantic = sqlx::query_scalar("INSERT INTO semantic_atoms(graph_id,source_version_id,ordinal,kind,statement,extraction_version,model,entities) VALUES($1,$2,0,'fact','Count parity outcome','source-atoms-v1','fixture','[]') RETURNING semantic_atom_id")
        .bind(f.graph).bind(version).fetch_one(&f.owner).await.unwrap();
    sqlx::query(
        "INSERT INTO node_semantic_atoms(graph_id,node_id,semantic_atom_id) VALUES($1,$2,$3)",
    )
    .bind(f.graph)
    .bind(node)
    .bind(semantic)
    .execute(&f.owner)
    .await
    .unwrap();
    Evidence {
        item,
        version,
        atom,
        semantic,
    }
}

#[tokio::test]
async fn oceans_counts_preserve_evidence_boundaries() {
    for org in [false, true] {
        let f = Fixture::new(org).await;
        let mut evidence_rows = Vec::new();
        for (kind, tag, access, days) in [
            (Some("entity"), "current", "permitted", 0),
            (Some("decision"), "old", "permitted", 3),
            (Some("thread"), "denied", "restricted", 0),
            (Some("reference"), "versioned", "permitted", 0),
        ] {
            let n = node(&f, kind).await;
            evidence_rows.push(evidence(&f, n, tag, access, days).await);
        }
        f.atom("unlinked-count-evidence").await;
        let unbound = f.atom("restricted-unbound:chunk:42").await;
        sqlx::query("INSERT INTO source_items(graph_id,source_type,provider_namespace,source_id,origin,access_state) VALUES($1,'note','fixture','restricted-unbound','{}','restricted')")
            .bind(f.graph).execute(&f.owner).await.unwrap();
        let denied_node = node(&f, Some("thread")).await;
        sqlx::query("INSERT INTO node_atoms(node_id,atom_id) VALUES($1,$2)")
            .bind(denied_node)
            .bind(unbound)
            .execute(&f.owner)
            .await
            .unwrap();
        // One admitted atom supports multiple reefs; two admission arms still
        // count each reef once. No evidence means no reef.
        let n = node(&f, Some("entity")).await;
        sqlx::query("INSERT INTO node_atoms(node_id,atom_id) VALUES($1,$2)")
            .bind(n)
            .bind(evidence_rows[0].atom)
            .execute(&f.owner)
            .await
            .unwrap();
        node(&f, Some("reference")).await;
        for owner in [false, true] {
            let c = count_pair(&f, f.graph, f.user, owner).await;
            assert_eq!(c, (json!({"entity":2,"decision":1,"reference":1}), 4, 4));
            assert_eq!(
                count_pair(&f, Uuid::new_v4(), f.user, owner).await,
                (json!({}), 0, 0)
            );
        }
        // All retained versions are eligible when their extraction is valid;
        // changing current_version_id alone must not discard old evidence.
        sqlx::query("UPDATE source_items SET current_version_id=NULL WHERE source_item_id=$1")
            .bind(evidence_rows[3].item)
            .execute(&f.owner)
            .await
            .unwrap();
        let fork:Uuid = sqlx::query_scalar("INSERT INTO source_versions(graph_id,source_item_id,source_id,source_type,provider_namespace,origin,raw_inline,source_metadata,occurred_at,captured_at,extraction_status,extraction_version) SELECT graph_id,source_item_id,source_id,source_type,provider_namespace,origin,'Forked evidence',source_metadata,occurred_at,now(),'done','source-atoms-v2' FROM source_versions WHERE version_id=$1 RETURNING version_id")
            .bind(evidence_rows[3].version).fetch_one(&f.owner).await.unwrap();
        sqlx::query("UPDATE source_items SET current_version_id=$2 WHERE source_item_id=$1")
            .bind(evidence_rows[3].item)
            .bind(fork)
            .execute(&f.owner)
            .await
            .unwrap();
        for owner in [false, true] {
            assert_eq!(
                count_pair(&f, f.graph, f.user, owner).await,
                (json!({"entity":2,"decision":1,"reference":1}), 4, 4),
                "retained valid versions survive a current-version fork"
            );
        }
        // Retracted and derived legacy rows cannot support reefs; independent
        // semantic support remains valid until its extraction is obsolete.
        sqlx::query("UPDATE atoms SET evidence_retracted=true WHERE atom_id=$1")
            .bind(evidence_rows[0].atom)
            .execute(&f.owner)
            .await
            .unwrap();
        sqlx::query("UPDATE atoms SET derived_copy_bound=true WHERE atom_id=$1")
            .bind(evidence_rows[3].atom)
            .execute(&f.owner)
            .await
            .unwrap();
        sqlx::query(
            "UPDATE semantic_atoms SET extraction_version='obsolete' WHERE semantic_atom_id=$1",
        )
        .bind(evidence_rows[3].semantic)
        .execute(&f.owner)
        .await
        .unwrap();
        // Any denied mapping vetoes legacy evidence even alongside a permitted
        // mapping. Semantic admission retains its own version/source boundary.
        sqlx::query("INSERT INTO source_version_chunks(graph_id,source_version_id,atom_id) VALUES($1,$2,$3)")
            .bind(f.graph).bind(evidence_rows[2].version).bind(evidence_rows[1].atom)
            .execute(&f.owner).await.unwrap();
        for owner in [false, true] {
            assert_eq!(
                count_pair(&f, f.graph, f.user, owner).await,
                (json!({"entity":1,"decision":1}), 1, 1)
            );
        }
        f.close().await;
    }
}

fn counts_dto((reefs, atoms, sources): Counts) -> Value {
    let kinds = ["entity", "decision", "thread", "reference"];
    let counts: serde_json::Map<String, Value> = kinds
        .into_iter()
        .map(|kind| (kind.into(), json!(reefs[kind].as_i64().unwrap_or(0))))
        .collect();
    json!({"reef_total":counts.values().map(|n|n.as_i64().unwrap()).sum::<i64>(),
        "reef_counts":counts,"atom_count":atoms,"source_count":sources})
}

#[tokio::test]
async fn oceans_full_payload_matches_released_counts_in_one_snapshot() {
    use rm::oceans_pages::{OceansIndex as Oceans, OceansParams};
    for org in [false, true] {
        let f = Fixture::new(org).await;
        for graphs_n in [1, 5, 0] {
            if graphs_n == 1 {
                let n = node(&f, Some("entity")).await;
                evidence(&f, n, "full-dto", "permitted", 0).await;
            } else if graphs_n == 5 {
                for _ in 0..4 {
                    sqlx::query("INSERT INTO graphs(account_id,name,kind,owner_user_id) VALUES($1,'Empty count parity graph','member',$2)")
                        .bind(f.account).bind(f.user).execute(&f.owner).await.unwrap();
                }
            } else {
                sqlx::query("UPDATE graphs SET archived_at=now() WHERE account_id=$1")
                    .bind(f.account)
                    .execute(&f.owner)
                    .await
                    .unwrap();
            }
            // Actual registry/claim/compute/publication/certified read path.
            let served = f.domain::<Oceans>(Default::default()).await;
            assert_eq!(served["oceans"].as_array().unwrap().len(), graphs_n);
            let scope = f.domain_scope::<Oceans>(Default::default()).await;
            rm::store::request_refresh::<Oceans>(&f.app, &scope)
                .await
                .unwrap();
            let claim = f.claim().await;
            for owner in [false, true] {
                let mut tx = rm::store::begin_compute(
                    if owner { &f.owner } else { &f.app },
                    &claim,
                    Duration::from_secs(30),
                )
                .await
                .unwrap();
                let mut control =
                    rm::store::begin_compute(&f.control, &claim, Duration::from_secs(30))
                        .await
                        .unwrap();
                let mut viewer = rm::Viewer::load(&mut tx, f.account, f.user).await.unwrap();
                viewer.certify_control(&mut control).await.unwrap();
                let params = OceansParams::default();
                let graphs = Oceans::graphs(&viewer, &params);
                let actual = Oceans::compute(
                    rm::ComputeCx {
                        tx: &mut tx,
                        blobs: None,
                        control_tx: Some(&mut control),
                        account_id: f.account,
                        compute_as: f.user,
                        graphs: &graphs,
                        now: chrono::Utc::now(),
                    },
                    &params,
                )
                .await
                .unwrap();
                let actual = serde_json::to_value(actual).unwrap();
                let mut expected = actual.clone();
                for row in expected["oceans"].as_array_mut().unwrap() {
                    let g: Uuid = row["graph"]["id"].as_str().unwrap().parse().unwrap();
                    let old: Counts =
                        sqlx::query_as("SELECT * FROM graph_workspace_counts_compute($1)")
                            .bind(g)
                            .fetch_one(&mut *tx)
                            .await
                            .unwrap();
                    row["counts"] = counts_dto(old);
                }
                assert_eq!(
                    actual, expected,
                    "complete payload parity in the identical actor/snapshot"
                );
                assert_eq!(
                    actual, served,
                    "real worker certificate/read matches direct body"
                );
                tx.rollback().await.unwrap();
                control.rollback().await.unwrap();
            }
            let mut owner_worker = f.worker();
            owner_worker.pool = f.owner.clone();
            owner_worker.store = Arc::new(PostgresStore::new(f.owner.clone()));
            let (owner_payload, snapshot) =
                read_model_refresh::compute(&owner_worker, &claim, &f.control)
                    .await
                    .unwrap();
            assert_eq!(
                owner_payload, served,
                "owner-role real worker must certify the identical payload"
            );
            assert!(rm::store::publish(&f.app, &claim, owner_payload, &snapshot)
                .await
                .unwrap());
        }
        f.close().await;
    }
}

#[tokio::test]
async fn oceans_counts_work_is_bounded_by_target_graph() {
    for org in [false, true] {
        let f = Fixture::new(org).await;
        seed_links(&f, f.graph, 128, 10).await;
        for table in ["atoms", "nodes", "node_atoms"] {
            f.owner
                .execute(format!("ANALYZE {table}").as_str())
                .await
                .unwrap();
        }
        let before = plan(&f, f.graph, true).await;
        seed_links(&f, f.foreign, 20_000, 1_000).await;
        for table in ["atoms", "nodes", "node_atoms"] {
            f.owner
                .execute(format!("ANALYZE {table}").as_str())
                .await
                .unwrap();
        }
        let after = plan(&f, f.graph, true).await;
        let legacy = plan_sql(
            &f,
            f.graph,
            true,
            "SELECT * FROM graph_workspace_counts_compute($1)",
        )
        .await;
        for owner in [false, true] {
            assert_eq!(
                count_pair(&f, f.graph, f.user, owner).await,
                (json!({"entity":128}), 10, 10)
            );
        }
        // Also run the real five-graph OceansIndex first fill, not just EXPLAIN.
        for _ in 0..4 {
            sqlx::query("INSERT INTO graphs(account_id,name,kind,owner_user_id) VALUES($1,'Empty work-bound sibling','member',$2)")
                .bind(f.account).bind(f.user).execute(&f.owner).await.unwrap();
        }
        let payload = f
            .domain::<rm::oceans_pages::OceansIndex>(Default::default())
            .await;
        assert_eq!(payload["oceans"].as_array().unwrap().len(), 5);
        assert_eq!(
            payload["oceans"]
                .as_array()
                .unwrap()
                .iter()
                .map(|o| o["counts"]["reef_total"].as_i64().unwrap())
                .sum::<i64>(),
            128
        );
        if let Ok(dir) = std::env::var("OCEANS_COUNTS_PLAN_DIR") {
            for (name, p) in [
                ("small", &before),
                ("unrelated", &after),
                ("legacy-unrelated", &legacy),
            ] {
                std::fs::write(
                    format!("{dir}/{org}-{name}.json"),
                    serde_json::to_vec_pretty(p).unwrap(),
                )
                .unwrap();
            }
        }
        println!(
            "{}",
            json!({"scenario":"oceans_counts_work", "org":org,
            "target_nodes":128,"target_atoms":10,"target_links":640,
            "unrelated_nodes":20000,"unrelated_atoms":1000,"unrelated_links":100000,
            "before_blocks":blocks(&before),"after_blocks":blocks(&after),
            "legacy_blocks":blocks(&legacy),"legacy_link_rows":link_rows(&legacy[0]["Plan"]),"legacy_ms":legacy[0]["Execution Time"],
            "before_link_rows":link_rows(&before[0]["Plan"]),"after_link_rows":link_rows(&after[0]["Plan"]),
            "before_ms":before[0]["Execution Time"],"after_ms":after[0]["Execution Time"]})
        );
        f.close().await;
        assert!(
            link_rows(&after[0]["Plan"]) <= 640.,
            "counts must visit only the target graph links: {} > 640",
            link_rows(&after[0]["Plan"])
        );
    }
}

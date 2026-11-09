//! Complete graph-owned evidence fixtures shared by both RLS policy proofs.
use sqlx::{Executor, PgPool};
use uuid::Uuid;

pub const TABLES: &[&str] = &[
    "source_items",
    "source_versions",
    "source_version_chunks",
    "semantic_atoms",
    "semantic_atom_passages",
    "node_semantic_atoms",
    "semantic_atom_windows",
    "corpus_enrichment_runs",
    "corpus_enrichment_units",
    // P10 noncontent managed capture admission receipts.
    "corpus_capture_admissions",
    "source_retraction_outbox",
    "assertion_disagreements",
    // P7 scoped answer receipts and P11 recorded temporal state.
    "answer_support_receipts",
    "source_version_observations",
    "temporal_evidence_records",
    "answer_temporal_receipts",
];

/// Seed one real row per evidence table, with valid same-graph foreign keys.
/// The caller supplies an existing atom and node owned by this graph.
pub async fn seed(owner: &PgPool, graph: Uuid, atom: Uuid, node: Uuid) {
    let item = Uuid::new_v4();
    let version = Uuid::new_v4();
    let semantic = Uuid::new_v4();
    let run = Uuid::new_v4();
    let receipt = Uuid::new_v4();
    let policy_receipt_purpose = "cloud_embedding";
    let peer: Option<Uuid> = sqlx::query_scalar(
        "SELECT node_id FROM nodes WHERE graph_id=$1 AND node_id<>$2 ORDER BY node_id LIMIT 1",
    )
    .bind(graph)
    .bind(node)
    .fetch_optional(owner)
    .await
    .unwrap();
    let peer = match peer {
        Some(peer) => peer,
        None => {
            let peer = Uuid::new_v4();
            sqlx::query("INSERT INTO nodes(node_id,graph_id,type,title,body) SELECT $1,graph_id,type,$2,body FROM nodes WHERE node_id=$3")
            .bind(peer).bind(format!("rls-fixture-peer-{peer}")).bind(node).execute(owner).await.unwrap();
            peer
        }
    };
    // Only generated UUIDs enter this SQL. Seeding bypasses the policies being
    // tested; each app-role proof must independently show access and denial.
    owner.execute(format!(r#"
INSERT INTO source_items(source_item_id,graph_id,source_type,provider_namespace,source_id,origin)
 VALUES('{item}','{graph}','note','rls:fixture','rls-evidence-1','{{"service_id":"fixture","transport":"native","content_kind":"note"}}');
INSERT INTO source_versions(version_id,source_item_id,graph_id,source_id,source_type,provider_namespace,raw_inline,origin)
 VALUES('{version}','{item}','{graph}','rls-evidence-1','note','rls:fixture','private evidence','{{"service_id":"fixture","transport":"native","content_kind":"note"}}');
UPDATE source_items SET current_version_id='{version}' WHERE source_item_id='{item}';
INSERT INTO source_version_chunks(graph_id,source_version_id,atom_id)
 VALUES('{graph}','{version}','{atom}');
INSERT INTO semantic_atoms(semantic_atom_id,graph_id,source_version_id,ordinal,kind,statement,extraction_version,model)
 VALUES('{semantic}','{graph}','{version}',0,'fact','private fact','source-atoms-v1','fixture');
INSERT INTO semantic_atom_passages(semantic_atom_id,source_version_id,graph_id,start_char,end_char,quote)
 VALUES('{semantic}','{version}','{graph}',0,7,'private');
INSERT INTO node_semantic_atoms(node_id,semantic_atom_id,graph_id)
 VALUES('{node}','{semantic}','{graph}');
INSERT INTO semantic_atom_windows(source_version_id,graph_id,extraction_version,start_char,end_char)
 VALUES('{version}','{graph}','source-atoms-v1',0,16);
INSERT INTO corpus_enrichment_runs(run_id,graph_id,account_id,requested_by,mode,status,extraction_version,model,max_items,max_characters,max_model_calls)
 SELECT '{run}','{graph}',account_id,'{semantic}','dry_run','ready','source-atoms-v1','fixture',1,16,1 FROM graphs WHERE id='{graph}';
INSERT INTO corpus_enrichment_units(run_id,graph_id,kind,holding_id,source_item_id,extraction_version,state)
 VALUES('{run}','{graph}','retained_version','{version}','{item}','source-atoms-v1','pending');
INSERT INTO corpus_capture_admissions(source_version_id,graph_id,account_id,source_item_id,extraction_version,state,reason)
 SELECT '{version}','{graph}',account_id,'{item}','source-atoms-v1','blocked','managed_admission_required' FROM graphs WHERE id='{graph}'
 ON CONFLICT(source_version_id) DO NOTHING;
INSERT INTO source_retraction_outbox(graph_id,source_item_id,event_kind)
 VALUES('{graph}','{item}','deleted');
INSERT INTO assertion_disagreements(graph_id,left_node_id,right_node_id)
 VALUES('{graph}',LEAST('{node}'::uuid,'{peer}'::uuid),GREATEST('{node}'::uuid,'{peer}'::uuid));
-- source_version_observations is written by the version-registration trigger.
INSERT INTO answer_support_receipts(receipt_id,graph_id,owner_user_id,support,evidence_complete)
 VALUES('{receipt}','{graph}','{semantic}','[]'::jsonb,true);
INSERT INTO temporal_evidence_records(graph_id,subject_node_id,state_key,source_item_id,source_version_id,semantic_atom_id,passage_id,observation_id,operation,value,recording_method,recorded_by)
 SELECT '{graph}','{node}','fixture.status','{item}','{version}','{semantic}',p.passage_id,o.observation_id,'assert','private','human_curated_v1','{semantic}'
 FROM semantic_atom_passages p JOIN source_version_observations o ON o.source_version_id=p.source_version_id
 WHERE p.semantic_atom_id='{semantic}' LIMIT 1;
INSERT INTO managed_budget_periods(account_id,purpose,period_start,limit_micro_usd)
 SELECT account_id,'{policy_receipt_purpose}',DATE '2026-10-14',10000000 FROM graphs WHERE id='{graph}';
INSERT INTO managed_attempt_receipts(account_id,purpose,period_start,graph_id,logical_call,attempt_no,candidate_models,input_bound_tokens,max_tokens,reserved_micro_usd)
 SELECT account_id,'{policy_receipt_purpose}',DATE '2026-10-14','{graph}','{run}',1,ARRAY['fixture'],1,1,1 FROM graphs WHERE id='{graph}';
INSERT INTO answer_temporal_receipts(receipt_id,graph_id,owner_user_id,temporal_selection,event_ids)
 SELECT '{receipt}','{graph}','{semantic}','{{"mode":"ordinary","revision":1}}'::jsonb,array_agg(event_id) FROM temporal_evidence_records WHERE source_version_id='{version}';
"#).as_str()).await.expect("seed complete source evidence fixture");
}

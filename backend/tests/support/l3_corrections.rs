//! Actual stored payload, publication and control-notification regressions.
use super::*;

// Actual registered payloads and durable publication/read paths. This file is
// deliberately independent of the parent's conversation-list correction tests.
async fn shared_graph(f: &Fixture) {
    sqlx::query("UPDATE graphs SET kind='team',owner_user_id=NULL WHERE id=$1")
        .bind(f.graph)
        .execute(&f.owner)
        .await
        .unwrap();
    let team:Uuid=sqlx::query_scalar("INSERT INTO teams(account_id,graph_id,name,created_by) VALUES($1,$2,'Title correction',$3) RETURNING id").bind(f.account).bind(f.graph).bind(f.user).fetch_one(&f.owner).await.unwrap();
    sqlx::query("INSERT INTO team_members(team_id,user_id,role) VALUES($1,$2,'member')")
        .bind(team)
        .bind(f.user)
        .execute(&f.owner)
        .await
        .unwrap();
    sqlx::query("INSERT INTO team_seat_grants(account_id,team_id,user_id,source,history_from) VALUES($1,$2,$3,'fixture',now()-interval '1 day')").bind(f.account).bind(team).bind(f.user).execute(&f.owner).await.unwrap();
}
async fn support(f: &Fixture, label: &str, extraction: &str, old: bool) -> (Uuid, Uuid, Uuid) {
    let node: Uuid = sqlx::query_scalar(
        "INSERT INTO nodes(graph_id,type,title) VALUES($1,'entity',$2) RETURNING node_id",
    )
    .bind(f.graph)
    .bind(label)
    .fetch_one(&f.owner)
    .await
    .unwrap();
    let atom = f.atom(label).await;
    sqlx::query("UPDATE atoms SET source_type='slack',ingested_by=$2 WHERE atom_id=$1")
        .bind(atom)
        .bind(f.user)
        .execute(&f.owner)
        .await
        .unwrap();
    sqlx::query("INSERT INTO node_atoms(node_id,atom_id) VALUES($1,$2)")
        .bind(node)
        .bind(atom)
        .execute(&f.owner)
        .await
        .unwrap();
    let item:Uuid=sqlx::query_scalar("INSERT INTO source_items(graph_id,source_type,provider_namespace,source_id) VALUES($1,'note','fixture',$2) RETURNING source_item_id").bind(f.graph).bind(label).fetch_one(&f.owner).await.unwrap();
    let version:Uuid=sqlx::query_scalar("INSERT INTO source_versions(graph_id,source_item_id,source_type,provider_namespace,source_id,captured_at,extraction_version,extraction_status) VALUES($1,$2,'note','fixture',$3,CASE WHEN $4 THEN now()-interval '2 days' ELSE now() END,'current','done') RETURNING version_id").bind(f.graph).bind(item).bind(label).bind(old).fetch_one(&f.owner).await.unwrap();
    sqlx::query("UPDATE source_items SET current_version_id=$2 WHERE source_item_id=$1")
        .bind(item)
        .bind(version)
        .execute(&f.owner)
        .await
        .unwrap();
    let sem:Uuid=sqlx::query_scalar("INSERT INTO semantic_atoms(graph_id,source_version_id,ordinal,kind,statement,extraction_version,model,entities) VALUES($1,$2,0,'fact','unrelated statement',$3,'fixture',jsonb_build_array(jsonb_build_object('mention',$4::text))) RETURNING semantic_atom_id").bind(f.graph).bind(version).bind(extraction).bind(label).fetch_one(&f.owner).await.unwrap();
    sqlx::query(
        "INSERT INTO node_semantic_atoms(graph_id,node_id,semantic_atom_id) VALUES($1,$2,$3)",
    )
    .bind(f.graph)
    .bind(node)
    .bind(sem)
    .execute(&f.owner)
    .await
    .unwrap();
    (node, atom, sem)
}
async fn actual_title_payloads(org: bool) {
    use rm::{connectors::ConnectorDetail as D, home::WorkspaceActivity as W};
    let f = Fixture::new(org).await;
    shared_graph(&f).await;
    let (old, _, _) = support(&f, "PRE_FLOOR_SECRET", "current", true).await;
    let (obsolete, _, _) = support(&f, "OBSOLETE_SECRET", "obsolete", false).await;
    let (current, current_atom, _) = support(&f, "Eligible current label", "current", false).await;
    let corrected:Uuid=sqlx::query_scalar("INSERT INTO nodes(graph_id,type,title) VALUES($1,'entity','Verified interpretation') RETURNING node_id").bind(f.graph).fetch_one(&f.owner).await.unwrap();
    sqlx::query("INSERT INTO node_atoms(node_id,atom_id) VALUES($1,$2)")
        .bind(corrected)
        .bind(current_atom)
        .execute(&f.owner)
        .await
        .unwrap();
    let mut writer = begin_tenant_scoped(&f.owner, f.account, f.user)
        .await
        .unwrap();
    sqlx::query("INSERT INTO reef_identity_corrections(graph_id,node_id,label,recorded_by,support_atom_id,support_kind,content_revision,input_sha256) SELECT $1,$2,'Verified interpretation',$3,a.atom_id,'legacy',a.content_revision,reef_identity_atom_fingerprint(a) FROM atoms a WHERE a.atom_id=$4").bind(f.graph).bind(corrected).bind(f.user).bind(current_atom).execute(&mut *writer).await.unwrap();
    writer.commit().await.unwrap();
    // Full history must still admit current extraction evidence exactly.
    let detail = f
        .domain::<D>([("provider".into(), "slack".into())].into())
        .await;
    let topics = detail["topics"].as_array().unwrap();
    assert!(
        topics
            .iter()
            .any(|t| t["id"] == current.to_string() && t["label"] == "Eligible current label"),
        "positive current evidence: {detail}"
    );
    assert!(
        topics
            .iter()
            .any(|t| t["id"] == corrected.to_string() && t["label"] == "Verified interpretation"),
        "explicit supported corrections must survive actual payload projection"
    );
    assert!(detail["captures"]["items"]
        .as_array()
        .unwrap()
        .iter()
        .flat_map(|c| c["topics"].as_array().unwrap())
        .any(|t| t["id"] == corrected.to_string() && t["label"] == "Verified interpretation"));
    assert!(
        !detail.to_string().contains("OBSOLETE_SECRET"),
        "obsolete semantic title escaped through actual ConnectorDetail: {detail}"
    );
    if org {
        assert!(
            !detail.to_string().contains("PRE_FLOOR_SECRET"),
            "pre-floor semantic title escaped through actual ConnectorDetail: {detail}"
        );
    } else {
        assert!(topics.iter().any(|t|t["id"]==old.to_string() && t["label"]=="PRE_FLOOR_SECRET"),"control plane has no history window: eligible current full-history evidence must remain");
    }
    let activity = f.domain::<W>(Default::default()).await;
    assert!(
        !activity.to_string().contains("OBSOLETE_SECRET"),
        "obsolete WorkspaceActivity title: {activity}"
    );
    if org {
        assert!(
            !activity.to_string().contains("PRE_FLOOR_SECRET"),
            "pre-floor WorkspaceActivity title: {activity}"
        );
    }
    // Exercise each unsafe candidate as the highest-ranked headline candidate.
    for node in [obsolete, old] {
        if node == old && !org {
            continue;
        }
        sqlx::query("UPDATE nodes SET updated_at=now()+interval '1 day' WHERE node_id=$1")
            .bind(node)
            .execute(&f.owner)
            .await
            .unwrap();
        let scope = f.domain_scope::<W>(Default::default()).await;
        rm::store::request_refresh::<W>(&f.app, &scope)
            .await
            .unwrap();
        let c = f.claim().await;
        let (v, s) = f.compute(&c).await;
        assert!(rm::store::publish(&f.app, &c, v.clone(), &s).await.unwrap());
        assert!(
            !v.to_string().contains("SECRET"),
            "headline must use only eligible titles: {v}"
        );
    }
    f.close().await;
}
#[tokio::test]
async fn title_payloads_consumer() {
    actual_title_payloads(false).await
}
#[tokio::test]
async fn title_payloads_tenant() {
    actual_title_payloads(true).await
}

async fn title_link_reduction(
    org: bool,
    semantic: bool,
    reassign: bool,
    absent: bool,
    publish_before_commit: bool,
) {
    use rm::connectors::ConnectorDetail as D;
    let f = Fixture::new(org).await;
    let (node, atom, sem) = support(&f, "Supported label", "current", false).await;
    if !semantic {
        sqlx::query("UPDATE atoms SET content_inline='Supported label' WHERE atom_id=$1")
            .bind(atom)
            .execute(&f.owner)
            .await
            .unwrap();
        sqlx::query("DELETE FROM node_semantic_atoms WHERE node_id=$1")
            .bind(node)
            .execute(&f.owner)
            .await
            .unwrap();
    }
    // Retain a different admitted link so node visibility survives label loss.
    let unrelated = f.atom("still-admitted").await;
    sqlx::query("INSERT INTO node_atoms(node_id,atom_id) VALUES($1,$2)")
        .bind(node)
        .bind(unrelated)
        .execute(&f.owner)
        .await
        .unwrap();
    let target:Uuid=sqlx::query_scalar("INSERT INTO nodes(graph_id,type,title) VALUES($1,'entity','Other label') RETURNING node_id").bind(f.foreign).fetch_one(&f.owner).await.unwrap();
    let target_sem: Uuid = sqlx::query_scalar("WITH item AS (INSERT INTO source_items(graph_id,source_type,provider_namespace,source_id) VALUES($1,'note','fixture','target') RETURNING source_item_id), v AS (INSERT INTO source_versions(graph_id,source_item_id,source_type,provider_namespace,source_id) SELECT $1,source_item_id,'note','fixture','target' FROM item RETURNING version_id) INSERT INTO semantic_atoms(graph_id,source_version_id,ordinal,kind,statement,extraction_version,model) SELECT $1,version_id,0,'fact','target evidence','current','fixture' FROM v RETURNING semantic_atom_id")
        .bind(f.foreign).fetch_one(&f.owner).await.unwrap();
    let scope = f
        .domain_scope::<D>([("provider".into(), "slack".into())].into())
        .await;
    if !absent {
        let v = f
            .domain::<D>([("provider".into(), "slack".into())].into())
            .await;
        assert!(v.to_string().contains("Supported label"));
    }
    sqlx::query("INSERT INTO journal_rollups(account_id,user_id,graph_id,hour_start,summary,input_fingerprint,model,period_start) VALUES($1,$2,$3,date_trunc('hour',now()),'Label-dependent retained prose','fixture','fixture',date_trunc('hour',now()))").bind(f.account).bind(f.user).bind(f.graph).execute(&f.owner).await.unwrap();
    // With absent=true the trigger completes before first enqueue and before
    // its commit. Same-XID additive work must upgrade to a reduction.
    let mut writer = f.owner.begin().await.unwrap();
    let xid: String = sqlx::query_scalar("SELECT pg_current_xact_id()::text")
        .fetch_one(&mut *writer)
        .await
        .unwrap();
    sqlx::query("INSERT INTO nodes(graph_id,type,title) VALUES($1,'entity','Additive in reducing transaction')").bind(f.graph).execute(&mut *writer).await.unwrap();
    let sql=match(semantic,reassign){
      (true,true)=>"UPDATE node_semantic_atoms SET graph_id=$3,node_id=$4,semantic_atom_id=$5 WHERE node_id=$1 AND semantic_atom_id=$2",
      (true,false)=>"DELETE FROM node_semantic_atoms WHERE node_id=$1 AND semantic_atom_id=$2",
      (false,true)=>"UPDATE node_atoms SET node_id=$4 WHERE node_id=$1 AND atom_id=$2 AND $3::uuid IS NOT NULL",
      (false,false)=>"DELETE FROM node_atoms WHERE node_id=$1 AND atom_id=$2",
    };
    let q = sqlx::query(sql)
        .bind(node)
        .bind(if semantic { sem } else { atom });
    if reassign {
        let q = q.bind(f.foreign).bind(target);
        if semantic {
            q.bind(target_sem).execute(&mut *writer).await.unwrap();
        } else {
            q.execute(&mut *writer).await.unwrap();
        }
    } else {
        q.execute(&mut *writer).await.unwrap();
    }
    let graphs = if reassign {
        vec![f.graph, f.foreign]
    } else {
        vec![f.graph]
    };
    let fences:Vec<String>=sqlx::query_scalar("SELECT invalidated_xid::text FROM read_model_graph_fences WHERE account_id=$1 AND graph_id=ANY($2)").bind(f.account).bind(&graphs).fetch_all(&mut *writer).await.unwrap();
    if absent {
        rm::store::request_refresh::<D>(&f.app, &scope)
            .await
            .unwrap();
        let c = f.claim().await;
        let (v, s) = f.compute(&c).await;
        assert!(v.to_string().contains("Supported label"));
        if publish_before_commit {
            assert!(rm::store::publish(&f.app, &c, v, &s).await.unwrap());
            writer.commit().await.unwrap();
        } else {
            writer.commit().await.unwrap();
            assert!(rm::store::publish(&f.app, &c, v, &s).await.unwrap());
            let dirty: bool = sqlx::query_scalar(
                "SELECT dirty_at IS NOT NULL FROM read_models WHERE model=$1 AND scope_key=$2",
            )
            .bind(D::NAME)
            .bind(scope.key())
            .fetch_one(&f.owner)
            .await
            .unwrap();
            assert!(
                dirty,
                "publication must not clear a missed reducing notification"
            );
        }
    } else {
        writer.commit().await.unwrap();
    }
    let mut tx = begin_tenant_scoped(&f.app, f.account, f.user)
        .await
        .unwrap();
    let stored = rm::store::read::<D>(&mut tx, &scope).await.unwrap();
    assert!(stored.computed_at.is_some() && stored.stale,"committed title support loss must leave the stored answer stale (served, never current) before refresh: semantic={semantic},reassign={reassign},absent={absent}, payload={}",stored.payload);
    assert_eq!(fences.len(), graphs.len());
    assert!(
        fences.iter().all(|v| v == &xid),
        "old/new graphs need reducing fences, including same-XID upgrade"
    );
    let retained: i64 =
        sqlx::query_scalar("SELECT count(*) FROM journal_rollups WHERE graph_id=$1")
            .bind(f.graph)
            .fetch_one(&f.owner)
            .await
            .unwrap();
    assert_eq!(retained, 0, "link support loss must retract compiled prose");
    tx.rollback().await.unwrap();
    let mut tx = begin_tenant_scoped(&f.app, f.account, f.user)
        .await
        .unwrap();
    assert!(
        sqlx::query_scalar::<_, bool>("SELECT library_node_permitted($1,$2)")
            .bind(f.graph)
            .bind(node)
            .fetch_one(&mut *tx)
            .await
            .unwrap(),
        "node must remain visible"
    );
    assert!(
        coralswarm_backend::services::journal_activities::project_node_titles(
            &mut tx,
            &[f.graph],
            &[node]
        )
        .await
        .unwrap()
        .is_empty()
    );
    tx.rollback().await.unwrap();
    f.close().await;
}
#[tokio::test]
async fn title_link_fences_consumer() {
    for semantic in [false, true] {
        for reassign in [false, true] {
            for absent in [false, true] {
                title_link_reduction(false, semantic, reassign, absent, false).await;
                if absent {
                    title_link_reduction(false, semantic, reassign, true, true).await;
                }
            }
        }
    }
}
#[tokio::test]
async fn title_link_fences_tenant() {
    for semantic in [false, true] {
        for reassign in [false, true] {
            for absent in [false, true] {
                title_link_reduction(true, semantic, reassign, absent, false).await;
                if absent {
                    title_link_reduction(true, semantic, reassign, true, true).await;
                }
            }
        }
    }
}

async fn telemetry_churn(org: bool) {
    use rm::connectors::ConnectorsList as C;
    let f = Fixture::new(org).await;
    let old = f.domain::<C>(Default::default()).await;
    let scope = f.domain_scope::<C>(Default::default()).await;
    for _ in 0..5 {
        let mut tx = begin_tenant_scoped(&f.control, f.account, f.user)
            .await
            .unwrap();
        sqlx::query("INSERT INTO mcp_tool_calls(account_id,user_id,tool,scope_used,outcome) VALUES($1,$2,'ocean_list','read','ok')").bind(f.account).bind(f.user).execute(&mut *tx).await.unwrap();
        tx.commit().await.unwrap();
        let current = f.domain_scope::<C>(Default::default()).await;
        assert_eq!(
            scope.key(),
            current.key(),
            "ordinary MCP reads must not invalidate all control-fact addresses"
        );
        let mut tx = begin_tenant_scoped(&f.app, f.account, f.user)
            .await
            .unwrap();
        let s = rm::store::read::<C>(&mut tx, &current).await.unwrap();
        assert!(s.computed_at.is_some());
        assert_eq!(s.payload, old);
        assert!(s.stale, "telemetry must durably schedule freshness");
        tx.rollback().await.unwrap();
        rm::store::enqueue::<C>(&f.app, &current).await.unwrap();
        let c = f.claim().await;
        let (v, s) = f.compute(&c).await;
        let mut tx = begin_tenant_scoped(&f.control, f.account, f.user)
            .await
            .unwrap();
        sqlx::query("INSERT INTO mcp_tool_calls(account_id,user_id,tool,outcome) VALUES($1,$2,'ocean_list','ok')").bind(f.account).bind(f.user).execute(&mut *tx).await.unwrap();
        tx.commit().await.unwrap();
        assert!(
            rm::store::publish(&f.app, &c, v, &s).await.unwrap(),
            "ongoing telemetry must not supersede claim"
        );
    }
    // Once the latest notification is observed, compute converges to a clean
    // usable answer; repeated telemetry never changes the authority key.
    let current = f.domain_scope::<C>(Default::default()).await;
    rm::store::request_refresh::<C>(&f.app, &current)
        .await
        .unwrap();
    let c = f.claim().await;
    let (v, s) = f.compute(&c).await;
    assert!(rm::store::publish(&f.app, &c, v, &s).await.unwrap());
    let mut tx = begin_tenant_scoped(&f.app, f.account, f.user)
        .await
        .unwrap();
    let served = rm::store::read::<C>(&mut tx, &current).await.unwrap();
    assert!(served.computed_at.is_some() && !served.stale);
    tx.rollback().await.unwrap();
    // A genuine control reduction remains atomic even with no tenant enqueue.
    let mut tx = begin_tenant_scoped(&f.control, f.account, f.user)
        .await
        .unwrap();
    sqlx::query("INSERT INTO oauth_tokens(account_id,provider,scope,owner_user_id,access_token) VALUES($1,'google_gmail','gmail.readonly',$2,'\\x00'::bytea)").bind(f.account).bind(f.user).execute(&mut *tx).await.unwrap();
    tx.commit().await.unwrap();
    let granted = f.domain_scope::<C>(Default::default()).await;
    assert_ne!(current.key(), granted.key());
    let mut tx = begin_tenant_scoped(&f.control, f.account, f.user)
        .await
        .unwrap();
    sqlx::query("UPDATE oauth_tokens SET credential_invalid_at=now() WHERE account_id=$1")
        .bind(f.account)
        .execute(&mut *tx)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    let revoked = f.domain_scope::<C>(Default::default()).await;
    assert_ne!(granted.key(), revoked.key());
    let mut tx = begin_tenant_scoped(&f.app, f.account, f.user)
        .await
        .unwrap();
    assert!(rm::store::read::<C>(&mut tx, &revoked)
        .await
        .unwrap()
        .computed_at
        .is_none());
    tx.rollback().await.unwrap();
    f.close().await;
}
#[tokio::test]
async fn telemetry_stable_consumer() {
    telemetry_churn(false).await
}
#[tokio::test]
async fn telemetry_stable_tenant() {
    telemetry_churn(true).await
}

async fn operational_control_updates(org: bool) {
    use rm::connectors::ConnectorsList as C;
    let f = Fixture::new(org).await;
    let mut tx = begin_tenant_scoped(&f.control, f.account, f.user)
        .await
        .unwrap();
    sqlx::query("INSERT INTO graphs(account_id,name,kind,owner_user_id) SELECT $1,'Control polling fixture','member',$2 WHERE NOT EXISTS(SELECT 1 FROM graphs WHERE account_id=$1 AND owner_user_id=$2)").bind(f.account).bind(f.user).execute(&mut *tx).await.unwrap();
    sqlx::query("INSERT INTO oauth_tokens(account_id,provider,scope,owner_user_id,access_token) VALUES($1,'google_gmail','gmail.readonly',$2,'\\x00'::bytea)").bind(f.account).bind(f.user).execute(&mut *tx).await.unwrap();
    sqlx::query("INSERT INTO addin_tokens(account_id,owner_user_id,token_hash) VALUES($1,$2,$3)")
        .bind(f.account)
        .bind(f.user)
        .bind(Uuid::new_v4().to_string())
        .execute(&mut *tx)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO microsoft_calendar_sync_state(account_id,owner_user_id,graph_id) SELECT $1,$2,id FROM graphs WHERE account_id=$1 AND owner_user_id=$2 LIMIT 1",
    )
    .bind(f.account)
    .bind(f.user)
    .execute(&mut *tx)
    .await
    .unwrap();
    tx.commit().await.unwrap();
    let scope = f.domain_scope::<C>(Default::default()).await;
    f.domain::<C>(Default::default()).await;
    for sql in [
        "UPDATE oauth_tokens SET access_token='\\x01'::bytea,refresh_token='\\x02'::bytea,expires_at=now()+interval '1 hour',updated_at=now() WHERE account_id=$1 AND owner_user_id=$2",
        "UPDATE addin_tokens SET last_used_at=now() WHERE account_id=$1 AND owner_user_id=$2",
        "UPDATE microsoft_calendar_sync_state SET last_synced_at=now(),delta_link='next-delta',last_error='transient' WHERE account_id=$1 AND owner_user_id=$2",
    ] {
        let mut tx=begin_tenant_scoped(&f.control,f.account,f.user).await.unwrap();
        sqlx::query(sql).bind(f.account).bind(f.user).execute(&mut *tx).await.unwrap();tx.commit().await.unwrap();
        let changed=f.domain_scope::<C>(Default::default()).await;assert_eq!(scope.key(),changed.key(),"routine control update churned address: {sql}");
        let mut tx=begin_tenant_scoped(&f.app,f.account,f.user).await.unwrap();let stored=rm::store::read::<C>(&mut tx,&changed).await.unwrap();assert!(stored.computed_at.is_some() && stored.stale);tx.rollback().await.unwrap();
    }
    let mut tx = begin_tenant_scoped(&f.control, f.account, f.user)
        .await
        .unwrap();
    let before: (i64, i64) = sqlx::query_as(
        "SELECT revision,freshness FROM read_model_control_revisions WHERE account_id=$1",
    )
    .bind(f.account)
    .fetch_one(&mut *tx)
    .await
    .unwrap();
    sqlx::query(
        "UPDATE oauth_tokens SET updated_at=now() WHERE account_id=$1 AND owner_user_id=$2",
    )
    .bind(f.account)
    .bind(f.user)
    .execute(&mut *tx)
    .await
    .unwrap();
    let after: (i64, i64) = sqlx::query_as(
        "SELECT revision,freshness FROM read_model_control_revisions WHERE account_id=$1",
    )
    .bind(f.account)
    .fetch_one(&mut *tx)
    .await
    .unwrap();
    assert_eq!(before, after, "timestamp-only poll must be a no-op");
    tx.commit().await.unwrap();
    let mut tx = begin_tenant_scoped(&f.control, f.account, f.user)
        .await
        .unwrap();
    sqlx::query(
        "UPDATE addin_tokens SET revoked_at=now() WHERE account_id=$1 AND owner_user_id=$2",
    )
    .bind(f.account)
    .bind(f.user)
    .execute(&mut *tx)
    .await
    .unwrap();
    tx.commit().await.unwrap();
    assert_ne!(
        scope.key(),
        f.domain_scope::<C>(Default::default()).await.key(),
        "token revocation must remain atomic authority"
    );
    f.close().await;
}
#[tokio::test]
async fn operational_notifications_consumer() {
    operational_control_updates(false).await
}
#[tokio::test]
async fn operational_notifications_tenant() {
    operational_control_updates(true).await
}

async fn text_and_node_support_changes(org: bool) {
    use rm::connectors::ConnectorDetail as D;
    for kind in ["content", "participants", "node_graph", "node_delete"] {
        let f = Fixture::new(org).await;
        let (node, atom, _) = support(&f, "Last supported label", "current", false).await;
        sqlx::query("DELETE FROM node_semantic_atoms WHERE node_id=$1")
            .bind(node)
            .execute(&f.owner)
            .await
            .unwrap();
        sqlx::query("UPDATE atoms SET content_inline=CASE WHEN $2 THEN 'Last supported label' ELSE 'unrelated' END, participants=CASE WHEN $2 THEN '{}'::text[] ELSE ARRAY['Last supported label'] END WHERE atom_id=$1").bind(atom).bind(kind!="participants").execute(&f.owner).await.unwrap();
        let unrelated = f.atom("other visible evidence").await;
        sqlx::query("INSERT INTO node_atoms(node_id,atom_id) VALUES($1,$2)")
            .bind(node)
            .bind(unrelated)
            .execute(&f.owner)
            .await
            .unwrap();
        sqlx::query("INSERT INTO journal_rollups(account_id,user_id,graph_id,hour_start,summary,input_fingerprint,model,period_start) VALUES($1,$2,$3,date_trunc('hour',now()),'Retained label prose','fixture','fixture',date_trunc('hour',now()))").bind(f.account).bind(f.user).bind(f.graph).execute(&f.owner).await.unwrap();
        let scope = f
            .domain_scope::<D>([("provider".into(), "slack".into())].into())
            .await;
        assert!(f
            .domain::<D>([("provider".into(), "slack".into())].into())
            .await
            .to_string()
            .contains("Last supported label"));
        match kind {
            "content" => {
                sqlx::query("UPDATE atoms SET content_inline='removed label' WHERE atom_id=$1")
                    .bind(atom)
                    .execute(&f.owner)
                    .await
                    .unwrap();
            }
            "participants" => {
                sqlx::query("UPDATE atoms SET participants='{}' WHERE atom_id=$1")
                    .bind(atom)
                    .execute(&f.owner)
                    .await
                    .unwrap();
            }
            "node_delete" => {
                sqlx::query("DELETE FROM nodes WHERE node_id=$1")
                    .bind(node)
                    .execute(&f.owner)
                    .await
                    .unwrap();
            }
            _ => {
                sqlx::query("UPDATE nodes SET graph_id=$2 WHERE node_id=$1")
                    .bind(node)
                    .bind(f.foreign)
                    .execute(&f.owner)
                    .await
                    .unwrap();
            }
        }
        let prose: i64 =
            sqlx::query_scalar("SELECT count(*) FROM journal_rollups WHERE graph_id=$1")
                .bind(f.graph)
                .fetch_one(&f.owner)
                .await
                .unwrap();
        assert_eq!(prose, 0, "support mutation must retract prose: {kind}");
        let mut tx = begin_tenant_scoped(&f.app, f.account, f.user)
            .await
            .unwrap();
        let behind = rm::store::read::<D>(&mut tx, &scope).await.unwrap();
        assert!(
            behind.computed_at.is_some() && behind.stale,
            "support change must immediately mark the stored answer stale (served, never current): {kind}"
        );
        tx.rollback().await.unwrap();
        f.close().await;
    }
}
#[tokio::test]
async fn text_and_node_support_consumer() {
    text_and_node_support_changes(false).await
}
#[tokio::test]
async fn text_and_node_support_tenant() {
    text_and_node_support_changes(true).await
}

/// The all-model fixture's Google provider has no matching atom source types.
/// Measure populated Slack titles/captures explicitly on that SAME retained600k
/// fixture; use fresh nonowner logins without changing its original credentials.
#[tokio::test]
#[ignore = "explicit populated connector600k benchmark on the retained owned fixture"]
async fn populated_connector_benchmark600k() {
    use rm::connectors::ConnectorDetail as D;
    assert_eq!(
        std::env::var("ORGT_TEST_RETAIN_DATABASES").as_deref(),
        Ok("1")
    );
    let path = std::env::var("READ_MODEL_BENCH_FIXTURE").expect("exact owned fixture required");
    let ids: Value = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
    let opts =
        PgConnectOptions::from_str(&std::env::var("READ_MODEL_TEST_BASE_URL").unwrap()).unwrap();
    assert!(matches!(opts.get_host(), "localhost" | "127.0.0.1"));
    assert_eq!(ids["port"].as_u64().unwrap(), u64::from(opts.get_port()));
    let content = ids["database"].as_str().unwrap();
    let control_name = ids["control_database"].as_str().unwrap();
    assert!(
        content.starts_with("cstest_read_models_")
            && control_name.starts_with("cstest_read_model_identity_")
    );
    let db = TestDatabase::adopt(&opts, content);
    let control_db = TestDatabase::adopt(&opts, control_name);
    let owner = PgPoolOptions::new()
        .max_connections(4)
        .connect_with(opts.clone().database(content))
        .await
        .unwrap();
    let mut pools = Vec::new();
    let mut roles = Vec::new();
    for (name, base, tag) in [
        (content, "coralswarm_tenant_app", "l3title_bench_content"),
        (control_name, "coralswarm_app", "l3title_bench_control"),
    ] {
        let role = if base == "coralswarm_app" {
            control_db.role(tag)
        } else {
            db.role(tag)
        };
        let password = Uuid::new_v4().to_string();
        owner.execute(format!("CREATE ROLE {role} LOGIN INHERIT NOSUPERUSER NOBYPASSRLS PASSWORD '{password}' IN ROLE {base}").as_str()).await.unwrap();
        let app = PgPoolOptions::new()
            .max_connections(4)
            .connect_with(
                opts.clone()
                    .database(name)
                    .username(&role)
                    .password(&password),
            )
            .await
            .unwrap();
        let attrs:(bool,bool,i64)=sqlx::query_as("SELECT rolsuper,rolbypassrls,(SELECT count(*) FROM pg_class WHERE relnamespace='public'::regnamespace AND relowner=(SELECT oid FROM pg_roles WHERE rolname=current_user)) FROM pg_roles WHERE rolname=current_user").fetch_one(&app).await.unwrap();
        assert_eq!(attrs, (false, false, 0));
        pools.push(app);
        roles.push(role);
    }
    let f = Fixture {
        _db: db,
        _control_db: Some(control_db),
        owner,
        app: pools.remove(0),
        control: pools.remove(0),
        account: Uuid::parse_str(ids["account"].as_str().unwrap()).unwrap(),
        user: Uuid::parse_str(ids["user"].as_str().unwrap()).unwrap(),
        other: Uuid::parse_str(ids["other"].as_str().unwrap()).unwrap(),
        graph: Uuid::parse_str("22222222-0000-0000-0000-000000000001").unwrap(),
        foreign: Uuid::parse_str("22222222-0000-0000-0000-000000000002").unwrap(),
        org: true,
    };
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM atoms")
        .fetch_one(&f.owner)
        .await
        .unwrap();
    assert_eq!(count, 600_000);
    let scope = f
        .domain_scope::<D>([("provider".into(), "slack".into())].into())
        .await;
    let mut computes = Vec::new();
    let mut payload = Value::Null;
    for _ in 0..3 {
        rm::store::request_refresh::<D>(&f.app, &scope)
            .await
            .unwrap();
        let c = f.claim().await;
        let now = Instant::now();
        let (v, s) = tokio::time::timeout(Duration::from_secs(35), f.compute(&c))
            .await
            .expect("production total compute budget is35s");
        computes.push(now.elapsed().as_secs_f64() * 1000.0);
        assert!(
            v["topics"].as_array().unwrap().len() > 0
                && v["captures"]["items"].as_array().unwrap().len() == 25,
            "must exercise populated title paths"
        );
        assert!(rm::store::publish(&f.app, &c, v.clone(), &s).await.unwrap());
        payload = v;
    }
    let tenant = coralswarm_backend::tenancy::extractor::Tenant {
        store: None,
        pool: Some(f.app.clone()),
        decision: None,
        class: coralswarm_backend::route_class::RouteClass::PageRead,
    };
    let mut reads = Vec::new();
    for _ in 0..20 {
        let now = Instant::now();
        let mut tx = tenant.begin_member_scoped(f.account).await.unwrap();
        coralswarm_backend::tenancy::scope::set_tenant_user(&mut tx, f.user)
            .await
            .unwrap();
        let v = rm::store::read::<D>(&mut tx, &scope).await.unwrap();
        assert!(v.computed_at.is_some());
        assert_eq!(v.payload, payload);
        tx.rollback().await.unwrap();
        reads.push(now.elapsed().as_secs_f64() * 1000.0);
    }
    reads.sort_by(f64::total_cmp);
    let mut titles: Vec<(Uuid, String)> = payload["topics"]
        .as_array()
        .unwrap()
        .iter()
        .chain(
            payload["captures"]["items"]
                .as_array()
                .unwrap()
                .iter()
                .flat_map(|c| c["topics"].as_array().unwrap()),
        )
        .filter_map(|t| {
            let label = t["label"].as_str().unwrap();
            (!label.is_empty()).then(|| {
                (
                    Uuid::parse_str(t["id"].as_str().unwrap()).unwrap(),
                    label.to_owned(),
                )
            })
        })
        .collect();
    titles.sort();
    titles.dedup();
    assert!(titles.len() > 10);
    let (nodes, labels): (Vec<_>, Vec<_>) = titles.iter().cloned().unzip();
    let mut tx = begin_tenant_scoped(&f.app, f.account, f.user)
        .await
        .unwrap();
    let mismatches:i64=sqlx::query_scalar("SELECT count(*) FROM unnest($1::uuid[],$2::text[]) t(node_id,label) JOIN nodes n ON n.node_id=t.node_id WHERE t.label IS DISTINCT FROM CASE WHEN library_node_permitted(n.graph_id,n.node_id) THEN library_node_title(n.graph_id,n.node_id) END").bind(&nodes).bind(labels).fetch_one(&mut *tx).await.unwrap();
    assert_eq!(mismatches, 0);
    tx.rollback().await.unwrap();
    // Exercise the final WorkspaceActivity body on the same600k fixture too.
    type W = rm::home::WorkspaceActivity;
    let ws_scope = f.domain_scope::<W>(Default::default()).await;
    let mut ws_compute = Vec::new();
    for _ in 0..3 {
        rm::store::request_refresh::<W>(&f.app, &ws_scope)
            .await
            .unwrap();
        let c = f.claim().await;
        let now = Instant::now();
        let (v, s) = tokio::time::timeout(Duration::from_secs(35), f.compute(&c))
            .await
            .unwrap();
        ws_compute.push(now.elapsed().as_secs_f64() * 1000.0);
        assert!(!v["workspaces"].as_array().unwrap().is_empty());
        assert!(rm::store::publish(&f.app, &c, v, &s).await.unwrap());
    }
    let mut ws_reads = Vec::new();
    for _ in 0..20 {
        let now = Instant::now();
        let mut tx = tenant.begin_member_scoped(f.account).await.unwrap();
        coralswarm_backend::tenancy::scope::set_tenant_user(&mut tx, f.user)
            .await
            .unwrap();
        assert!(rm::store::read::<W>(&mut tx, &ws_scope)
            .await
            .unwrap()
            .computed_at
            .is_some());
        tx.rollback().await.unwrap();
        ws_reads.push(now.elapsed().as_secs_f64() * 1000.0);
    }
    ws_reads.sort_by(f64::total_cmp);
    let artifact = json!({"atom_count":count,"provider":"slack","roles":roles,"topics":payload["topics"].as_array().unwrap().len(),"captures":payload["captures"]["items"].as_array().unwrap().len(),"eligible_labels_checked":titles.len(),"visibility_mismatches":mismatches,"worker_compute_ms":computes,"stored_read_p95_ms":reads[18],"stored_samples":20,"read_class":"page_read_readonly_1s","payload":payload,"workspace_activity":{"worker_compute_ms":ws_compute,"stored_read_p95_ms":ws_reads[18],"stored_samples":20}});
    std::fs::write(
        std::env::var("READ_MODEL_BENCH_OUTPUT").unwrap(),
        serde_json::to_vec_pretty(&artifact).unwrap(),
    )
    .unwrap();
    eprintln!(
        "POPULATED_CONNECTOR600K={}",
        json!({"worker_compute_ms":computes,"stored_read_p95_ms":reads[18],"eligible_labels_checked":titles.len(),"visibility_mismatches":mismatches})
    );
    f.close().await;
}

async fn titles_after_hidden_batch(org: bool) {
    use rm::connectors::ConnectorDetail as D;
    let f = Fixture::new(org).await;
    shared_graph(&f).await;
    let atom = f.atom("unrelated batch atom").await;
    sqlx::query("UPDATE atoms SET source_type='slack',ingested_by=$2 WHERE atom_id=$1")
        .bind(atom)
        .bind(f.user)
        .execute(&f.owner)
        .await
        .unwrap();
    // Every hidden candidate sorts before the eligible one. Its title is not
    // mentioned by any admitted evidence, but its node stays visible.
    sqlx::query("INSERT INTO nodes(node_id,graph_id,type,title) SELECT ('00000000-0000-0000-0000-'||lpad(i::text,12,'0'))::uuid,$1,'entity','Unsupported batch label '||i FROM generate_series(1,65) i").bind(f.graph).execute(&f.owner).await.unwrap();
    sqlx::query(
        "INSERT INTO node_atoms(node_id,atom_id) SELECT node_id,$2 FROM nodes WHERE graph_id=$1",
    )
    .bind(f.graph)
    .bind(atom)
    .execute(&f.owner)
    .await
    .unwrap();
    let eligible = f.atom("late eligible atom").await;
    sqlx::query("UPDATE atoms SET source_type='slack',ingested_by=$2,content_inline='Visible after hidden candidates' WHERE atom_id=$1").bind(eligible).bind(f.user).execute(&f.owner).await.unwrap();
    sqlx::query("INSERT INTO nodes(node_id,graph_id,type,title) VALUES('ffffffff-ffff-ffff-ffff-ffffffffffff',$1,'entity','Visible after hidden candidates')").bind(f.graph).execute(&f.owner).await.unwrap();
    sqlx::query(
        "INSERT INTO node_atoms(node_id,atom_id) VALUES('ffffffff-ffff-ffff-ffff-ffffffffffff',$1)",
    )
    .bind(eligible)
    .execute(&f.owner)
    .await
    .unwrap();
    sqlx::query("UPDATE nodes SET updated_at=now()+interval '1 day' WHERE graph_id=$1 AND title LIKE 'Unsupported batch label %'").bind(f.graph).execute(&f.owner).await.unwrap();
    let payload = f
        .domain::<D>([("provider".into(), "slack".into())].into())
        .await;
    let topics = payload["topics"].as_array().unwrap();
    assert_eq!(topics.len(), 1);
    assert_eq!(topics[0]["label"], "Visible after hidden candidates");
    assert!(!payload.to_string().contains("Unsupported batch label"));
    let activity = f
        .domain::<rm::home::WorkspaceActivity>(Default::default())
        .await;
    assert_eq!(
        activity["workspaces"][0]["headline"]["reef"]["title"], "Visible after hidden candidates",
        "a hidden first reef batch must not suppress later eligible titles"
    );
    f.close().await;
}
#[tokio::test]
async fn eligible_after_hidden_batch_consumer() {
    titles_after_hidden_batch(false).await
}
#[tokio::test]
async fn eligible_after_hidden_batch_tenant() {
    titles_after_hidden_batch(true).await
}

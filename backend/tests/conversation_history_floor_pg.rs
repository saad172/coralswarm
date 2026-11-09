//! Core saved-answer floor authorization, distinct from optional Find cards.
//! Control's production floor reader is NULL; its paired helper is additionally
//! exercised with a clearly injected floor-reader adapter in this isolated DB.
#[path = "support/page_model_fixture.rs"]
mod fixture;
use coralswarm_backend::{
    read_models::{
        self as rm,
        chats_detail::{ConversationDetail, DetailParams},
        ReadModel,
    },
    tenancy::scope::begin_tenant_scoped,
    workers::read_model_refresh,
};
use fixture::Fixture;
use serde_json::{json, Value};
use uuid::Uuid;
static ENV_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

async fn scope(f: &Fixture, id: Uuid) -> rm::Scope<DetailParams> {
    let mut tx = begin_tenant_scoped(&f.app, f.account, f.user)
        .await
        .unwrap();
    rm::Viewer::load(&mut tx, f.account, f.user)
        .await
        .unwrap()
        .scope::<ConversationDetail>(DetailParams { id })
        .unwrap()
}
async fn fill(f: &Fixture, s: &rm::Scope<DetailParams>) -> Value {
    rm::store::request_refresh::<ConversationDetail>(&f.app, s)
        .await
        .unwrap();
    let claims = rm::store::claim(&f.app).await.unwrap();
    let claim = claims
        .iter()
        .find(|c| c.model == ConversationDetail::NAME && c.scope_key == s.key())
        .unwrap();
    let (payload, snapshot) = read_model_refresh::compute(&f.worker(), claim, &f.control)
        .await
        .unwrap();
    assert!(
        rm::store::publish(&f.app, claim, payload.clone(), &snapshot)
            .await
            .unwrap()
    );
    let mut tx = begin_tenant_scoped(&f.app, f.account, f.user)
        .await
        .unwrap();
    let stored = rm::store::read::<ConversationDetail>(&mut tx, s)
        .await
        .unwrap();
    assert!(stored.computed_at.is_some());
    assert_eq!(serde_json::to_value(stored.payload).unwrap(), payload);
    payload
}
fn message(v: &Value, id: Uuid) -> &Value {
    v["messages"]
        .as_array()
        .unwrap()
        .iter()
        .find(|m| m["id"] == json!(id))
        .unwrap()
}
async fn passage(f: &Fixture, item: Uuid, days: i32, text: &str) -> Value {
    let version:Uuid=sqlx::query_scalar("INSERT INTO source_versions(graph_id,source_item_id,source_type,provider_namespace,source_id,raw_inline,occurred_at,extraction_version,extraction_status) VALUES($1,$2,'note','fixture:receipt-floor','receipt-floor',$3,now()-make_interval(days=>$4),'v1','done') RETURNING version_id")
        .bind(f.graph).bind(item).bind(text).bind(days).fetch_one(&f.owner).await.unwrap();
    let semantic:Uuid=sqlx::query_scalar("INSERT INTO semantic_atoms(graph_id,source_version_id,ordinal,kind,statement,extraction_version,model) VALUES($1,$2,0,'fact',$3,'v1','fixture') RETURNING semantic_atom_id")
        .bind(f.graph).bind(version).bind(text).fetch_one(&f.owner).await.unwrap();
    let passage:Uuid=sqlx::query_scalar("INSERT INTO semantic_atom_passages(graph_id,source_version_id,semantic_atom_id,start_char,end_char,quote) VALUES($1,$2,$3,0,$4,$5) RETURNING passage_id")
        .bind(f.graph).bind(version).bind(semantic).bind(text.chars().count() as i32).bind(text).fetch_one(&f.owner).await.unwrap();
    json!({"kind":"source_passage","graph_id":f.graph,"source_item_id":item,"source_version_id":version,"semantic_atom_id":semantic,"passage_id":passage})
}
async fn answer(
    f: &Fixture,
    conv: Uuid,
    seq: i32,
    text: &str,
    citations: Value,
    support: Value,
) -> Uuid {
    sqlx::query("INSERT INTO conversation_messages(conversation_id,seq,role,text) VALUES($1,$2,'user','What was recorded?')").bind(conv).bind(seq).execute(&f.owner).await.unwrap();
    let id:Uuid=sqlx::query_scalar("INSERT INTO conversation_messages(conversation_id,seq,role,text,mode,timing,scope_graph_ids) VALUES($1,$2,'assistant',$3,'synthesized',$4,$5) RETURNING id")
        .bind(conv).bind(seq+1).bind(text).bind(json!({"retained_citations":citations})).bind(json!([f.graph])).fetch_one(&f.owner).await.unwrap();
    sqlx::query("INSERT INTO answer_support_receipts(receipt_id,graph_id,owner_user_id,support,evidence_complete) VALUES($1,$2,$3,$4,true)").bind(id).bind(f.graph).bind(f.user).bind(support).execute(&f.owner).await.unwrap();
    sqlx::query("INSERT INTO query_history(id,account_id,graph_id,user_id,query_text,answer_summary,citation_count) VALUES($1,$2,$3,$4,'What was recorded?',$5,1)")
        .bind(id).bind(f.account).bind(f.graph).bind(f.user).bind(text).execute(&f.owner).await.unwrap();
    id
}
/// Exact upgrade race: the scan sees a NULL floor, a writer attempts to bound
/// it while the replacement helper is uncommitted, and an old worker captures
/// its snapshot. The real migration lock must defer that writer until commit.
async fn concurrent_null_floor_upgrade(
    f: &Fixture,
    conv: Uuid,
    team: Uuid,
    old: Uuid,
    historical: Uuid,
) {
    use std::time::Duration;
    let sql = if f.org {
        include_str!("../migrations_tenant/0226_saved_answer_history_floor.sql")
    } else {
        include_str!("../migrations_control/0415_saved_answer_history_floor.sql")
    };
    const LOCK: &str = "LOCK TABLE public.team_seat_grants IN SHARE MODE;";
    assert_eq!(
        sql.matches(LOCK).count(),
        1,
        "upgrade serialization must be explicit"
    );
    assert!(sql.find(LOCK).unwrap() < sql.find("CREATE OR REPLACE FUNCTION").unwrap());
    // Test-only causal RED switch: reproduce the reviewed pre-lock migration
    // without changing any migration file or production feature/configuration.
    let omit_lock = std::env::var("CS_CORE_TEST_UNLOCKED_UPGRADE").as_deref() == Ok("1");
    let sql = if omit_lock {
        sql.replace(LOCK, "")
    } else {
        sql.to_owned()
    };
    let split = sql
        .find("INSERT INTO public.read_model_graph_fences")
        .unwrap();
    let (replacement, scan) = sql.split_at(split);
    sqlx::raw_sql(include_str!(
        "../migrations_tenant/0197_answer_permitted_uuid_probes.sql"
    ))
    .execute(&f.owner)
    .await
    .unwrap();
    let mut migration = f.owner.begin().await.unwrap();
    let migration_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *migration)
        .await
        .unwrap();
    sqlx::raw_sql(replacement)
        .execute(&mut *migration)
        .await
        .unwrap();
    let selected = sqlx::query(scan)
        .execute(&mut *migration)
        .await
        .unwrap()
        .rows_affected();
    assert_eq!(
        selected, 0,
        "the initially NULL graph is absent from this upgrade scan"
    );
    let pool = f.owner.clone();
    let actor = f.user;
    let (attempt_tx, attempt_rx) = tokio::sync::oneshot::channel();
    let (done_tx, mut done_rx) = tokio::sync::oneshot::channel();
    let writer = tokio::spawn(async move {
        let mut tx = pool.begin().await.unwrap();
        let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
            .fetch_one(&mut *tx)
            .await
            .unwrap();
        attempt_tx.send(pid).unwrap();
        sqlx::query("UPDATE team_seat_grants SET history_from=now()-interval '3 days' WHERE team_id=$1 AND user_id=$2")
            .bind(team).bind(actor).execute(&mut *tx).await.unwrap();
        tx.commit().await.unwrap();
        done_tx.send(()).unwrap();
    });
    let writer_pid = attempt_rx.await.unwrap();
    // Observe an actual PostgreSQL lock wait or actual writer commit. The
    // timeout is a harness failure bound, not a latency/benchmark assertion.
    let writer_committed_before_migration=tokio::time::timeout(Duration::from_secs(5),async {
        loop {
            match done_rx.try_recv() {
                Ok(())=>break true,
                Err(tokio::sync::oneshot::error::TryRecvError::Empty)=>{},
                Err(e)=>panic!("writer channel failed: {e}"),
            }
            let blocked:bool=sqlx::query_scalar("SELECT $1=ANY(pg_blocking_pids($2)) AND EXISTS(SELECT 1 FROM pg_locks WHERE pid=$2 AND relation='public.team_seat_grants'::regclass AND mode='RowExclusiveLock' AND NOT granted)")
                .bind(migration_pid).bind(writer_pid).fetch_one(&f.owner).await.unwrap();
            if blocked {break false;}
            tokio::task::yield_now().await;
        }
    }).await.expect("writer must either commit or be blocked by this migration");
    let s = scope(f, conv).await;
    rm::store::request_refresh::<ConversationDetail>(&f.app, &s)
        .await
        .unwrap();
    let claim = rm::store::claim(&f.app)
        .await
        .unwrap()
        .into_iter()
        .find(|c| c.model == ConversationDetail::NAME && c.scope_key == s.key())
        .unwrap();
    let (old_payload, old_snapshot) = read_model_refresh::compute(&f.worker(), &claim, &f.control)
        .await
        .unwrap();
    assert_eq!(
        message(&old_payload, old)["text"],
        "EXCLUDED_ASSISTANT_PROSE",
        "worker still sees uncommitted replacement's old helper"
    );
    migration.commit().await.unwrap();
    if !writer_committed_before_migration {
        tokio::time::timeout(Duration::from_secs(5), &mut done_rx)
            .await
            .expect("floor writer must finish after migration commit")
            .unwrap();
    }
    writer.await.unwrap();
    let mut tx = begin_tenant_scoped(&f.app, f.account, f.user)
        .await
        .unwrap();
    let new_allowed: bool = sqlx::query_scalar("SELECT library_answer_permitted($1)")
        .bind(old)
        .fetch_one(&mut *tx)
        .await
        .unwrap();
    let final_fence: i32 =
        sqlx::query_scalar("SELECT read_model_fence_state($1,$2,$3,$4,$5::text::pg_snapshot)")
            .bind(f.account)
            .bind(s.class.graphs())
            .bind(ConversationDetail::NAME)
            .bind(f.user)
            .bind(&old_snapshot)
            .fetch_one(&mut *tx)
            .await
            .unwrap();
    tx.rollback().await.unwrap();
    eprintln!("upgrade-null-floor org={} omitted_lock={} scan_rows={} writer_committed_before_migration={} old_helper_prose=true new_helper_allowed={} old_snapshot_fence={}",f.org,omit_lock,selected,writer_committed_before_migration,new_allowed,final_fence);
    assert!(!new_allowed);
    assert!(
        rm::store::publish(&f.app, &claim, old_payload, &old_snapshot)
            .await
            .unwrap()
    );
    let mut tx = begin_tenant_scoped(&f.app, f.account, f.user)
        .await
        .unwrap();
    let stored = rm::store::read::<ConversationDetail>(&mut tx, &s)
        .await
        .unwrap();
    assert_eq!(
        final_fence, 0,
        "NULL-floor upgrade race must fence the old-helper snapshot"
    );
    assert!(
        !writer_committed_before_migration,
        "SHARE lock must serialize the floor write until migration commit"
    );
    assert!(
        stored.computed_at.is_some() && stored.stale,
        "the fenced old-helper publication is served stale, never as current"
    );
    tx.rollback().await.unwrap();
    let fixed = fill(f, &scope(f, conv).await).await;
    assert_eq!(message(&fixed, old)["support_status"], "unavailable");
    assert_eq!(
        message(&fixed, historical)["text"],
        "ADMITTED_HISTORICAL_PROSE"
    );
}
async fn contract(org: bool) {
    let _lock = ENV_LOCK.lock().await;
    let old_flag = std::env::var_os("CHAT_FIND_RESULTS_ENABLED");
    std::env::set_var("CHAT_FIND_RESULTS_ENABLED", "false");
    let f = Fixture::new(org).await;
    if let Ok(path) = std::env::var("CS_FIND_OWNED_RESOURCE_LOG") {
        use std::io::Write;
        let record = json!({"org":org,"app_role":f.app.connect_options().get_username(),"control_role":f.control.connect_options().get_username(),"database":f.owner.connect_options().get_database(),"control_database":f.control_owner.connect_options().get_database()});
        writeln!(
            std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(path)
                .unwrap(),
            "{record}"
        )
        .unwrap();
    }
    let applied: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM _sqlx_migrations WHERE version=$1 AND success)",
    )
    .bind(if org { 226_i64 } else { 415_i64 })
    .fetch_one(&f.owner)
    .await
    .unwrap();
    assert!(applied, "test binary must embed the new paired migration");
    use sha2::Digest;
    let canonical = if org {
        include_str!("../migrations_tenant/0226_saved_answer_history_floor.sql")
    } else {
        include_str!("../migrations_control/0415_saved_answer_history_floor.sql")
    };
    let checksum: Vec<u8> =
        sqlx::query_scalar("SELECT checksum FROM _sqlx_migrations WHERE version=$1 AND success")
            .bind(if org { 226_i64 } else { 415_i64 })
            .fetch_one(&f.owner)
            .await
            .unwrap();
    assert_eq!(
        checksum,
        sha2::Sha384::digest(canonical.as_bytes()).to_vec(),
        "compiled migration embedding must match the current reviewed lock bytes"
    );
    if org {
        sqlx::query("UPDATE graphs SET kind='team',owner_user_id=NULL WHERE id=$1")
            .bind(f.graph)
            .execute(&f.owner)
            .await
            .unwrap();
    }
    // On control keep the owned member ocean reachable and only use these
    // rows as a test floor input. Consumer reachability policy is unchanged.
    let team:Uuid=sqlx::query_scalar("INSERT INTO teams(account_id,graph_id,name,created_by) VALUES($1,$2,'Receipt floor',$3) RETURNING id").bind(f.account).bind(f.graph).bind(f.user).fetch_one(&f.owner).await.unwrap();
    sqlx::query("INSERT INTO team_members(team_id,user_id,role) VALUES($1,$2,'member')")
        .bind(team)
        .bind(f.user)
        .execute(&f.owner)
        .await
        .unwrap();
    sqlx::query("INSERT INTO team_seat_grants(account_id,team_id,user_id,source,history_from) VALUES($1,$2,$3,'fixture',NULL)").bind(f.account).bind(team).bind(f.user).execute(&f.owner).await.unwrap();
    let item:Uuid=sqlx::query_scalar("INSERT INTO source_items(graph_id,source_type,provider_namespace,source_id) VALUES($1,'note','fixture:receipt-floor','receipt-floor') RETURNING source_item_id").bind(f.graph).fetch_one(&f.owner).await.unwrap();
    let excluded = passage(&f, item, 5, "EXCLUDED_SOURCE_QUOTE").await;
    let admitted = passage(&f, item, 2, "ADMITTED_HISTORICAL_QUOTE").await;
    let current = passage(&f, item, 1, "CURRENT_VERSION_QUOTE").await;
    sqlx::query("UPDATE source_items SET current_version_id=$2 WHERE source_item_id=$1")
        .bind(item)
        .bind(
            current["source_version_id"]
                .as_str()
                .unwrap()
                .parse::<Uuid>()
                .unwrap(),
        )
        .execute(&f.owner)
        .await
        .unwrap();
    let conv: Uuid = sqlx::query_scalar(
        "INSERT INTO conversations(account_id,graph_id,user_id) VALUES($1,$2,$3) RETURNING id",
    )
    .bind(f.account)
    .bind(f.graph)
    .bind(f.user)
    .fetch_one(&f.owner)
    .await
    .unwrap();
    let mut excluded_cite = excluded.clone();
    excluded_cite["quote"] = json!("EXCLUDED_SOURCE_QUOTE");
    let mut admitted_cite = admitted.clone();
    admitted_cite["quote"] = json!("ADMITTED_HISTORICAL_QUOTE");
    let old = answer(
        &f,
        conv,
        0,
        "EXCLUDED_ASSISTANT_PROSE",
        json!([excluded_cite]),
        json!([excluded]),
    )
    .await;
    let historical = answer(
        &f,
        conv,
        2,
        "ADMITTED_HISTORICAL_PROSE",
        json!([admitted_cite]),
        json!([admitted]),
    )
    .await;
    let mixed = answer(
        &f,
        conv,
        4,
        "MIXED_REQUIRED_SUPPORT_PROSE",
        json!([admitted_cite]),
        json!([excluded, admitted]),
    )
    .await;
    let warm_scope = scope(&f, conv).await;
    let warm = fill(&f, &warm_scope).await;
    for id in [old, historical, mixed] {
        assert_eq!(message(&warm, id)["support_status"], "complete");
    }
    // Native consumer policy has no floor. Preserve that documented behavior.
    // Then inject only the shared floor-reader dependency for the paired SQL
    // contract; this never runs outside this fixture's owned database.
    if !org {
        sqlx::query("UPDATE team_seat_grants SET history_from=now()-interval '3 days' WHERE team_id=$1 AND user_id=$2").bind(team).bind(f.user).execute(&f.owner).await.unwrap();
        let mut tx = begin_tenant_scoped(&f.app, f.account, f.user)
            .await
            .unwrap();
        let floors: Option<Value> = sqlx::query_scalar("SELECT public.reader_history_floors()")
            .fetch_one(&mut *tx)
            .await
            .unwrap();
        assert!(
            floors.is_none(),
            "consumer production reader remains unfloored"
        );
        let native_allowed: bool = sqlx::query_scalar("SELECT library_answer_permitted($1)")
            .bind(old)
            .fetch_one(&mut *tx)
            .await
            .unwrap();
        assert!(native_allowed,"native consumer NULL floor still admits historical support even when seat rows have bounds");
        tx.rollback().await.unwrap();
        sqlx::query("CREATE OR REPLACE FUNCTION public.reader_history_floors() RETURNS jsonb LANGUAGE sql STABLE SECURITY DEFINER SET search_path=pg_catalog,public,pg_temp AS $$ SELECT jsonb_object_agg(t.graph_id::text,g.history_from) FROM public.team_seat_grants g JOIN public.teams t ON t.id=g.team_id WHERE g.user_id=NULLIF(current_setting('app.user_id',true),'')::uuid AND g.account_id=NULLIF(current_setting('app.account_id',true),'')::uuid AND g.history_from IS NOT NULL $$").execute(&f.owner).await.unwrap();
    }
    sqlx::query("UPDATE team_seat_grants SET history_from=now()-interval '3 days' WHERE team_id=$1 AND user_id=$2").bind(team).bind(f.user).execute(&f.owner).await.unwrap();
    // The old warmed scope is immediately marked stale (served, never as
    // current) before any recompute; the next fill below applies the floor.
    let mut tx = begin_tenant_scoped(&f.app, f.account, f.user)
        .await
        .unwrap();
    let behind = rm::store::read::<ConversationDetail>(&mut tx, &warm_scope)
        .await
        .unwrap();
    assert!(behind.computed_at.is_some() && behind.stale);
    let verdicts:Vec<bool>=sqlx::query_scalar("SELECT library_answer_permitted(id) FROM unnest($1::uuid[]) WITH ORDINALITY r(id,n) ORDER BY n").bind(vec![old,historical,mixed]).fetch_all(&mut *tx).await.unwrap();
    assert_eq!(
        verdicts,
        vec![false, true, false],
        "only exact receipted versions above the current floor may authorize prose"
    );
    tx.rollback().await.unwrap();
    let restricted = fill(&f, &scope(&f, conv).await).await;
    for id in [old, mixed] {
        let m = message(&restricted, id);
        assert_eq!(m["support_status"], "unavailable");
        assert!(m["text"].as_str().unwrap().contains("Evidence unavailable"));
        assert_eq!(m["retained_citations"], json!([]));
        assert_eq!(m["citations"], json!([]));
        assert_eq!(m["sources"], json!([]));
    }
    let body = restricted.to_string();
    for excluded in [
        "EXCLUDED_ASSISTANT_PROSE",
        "EXCLUDED_SOURCE_QUOTE",
        "MIXED_REQUIRED_SUPPORT_PROSE",
    ] {
        assert!(
            !body.contains(excluded),
            "fresh publication leaked {excluded}"
        );
    }
    assert_eq!(
        message(&restricted, historical)["text"],
        "ADMITTED_HISTORICAL_PROSE"
    );
    assert_eq!(
        message(&restricted, historical)["retained_citations"][0]["quote"],
        "ADMITTED_HISTORICAL_QUOTE"
    );
    // Interactive older-page reads use the same corrected core receipt helper.
    let mut tx = begin_tenant_scoped(&f.app, f.account, f.user)
        .await
        .unwrap();
    let (messages, _, _) =
        coralswarm_backend::services::conversation_page::messages(&mut tx, f.user, conv, None)
            .await
            .unwrap();
    let interactive = json!({"messages":messages});
    assert_eq!(message(&interactive, old)["support_status"], "unavailable");
    assert_eq!(
        message(&interactive, historical)["support_status"],
        "complete"
    );
    use sqlx::Row;
    let rows = sqlx::query(coralswarm_backend::store::verdict::QUERY_HISTORY_PAGE_SQL)
        .bind(f.account)
        .bind(f.user)
        .bind(200_i64)
        .bind(0_i64)
        .fetch_all(&mut *tx)
        .await
        .unwrap();
    let summaries: std::collections::HashMap<Uuid, String> = rows
        .iter()
        .map(|r| (r.get("id"), r.get("answer_summary")))
        .collect();
    assert_eq!(summaries[&old], "");
    assert_eq!(summaries[&mixed], "");
    assert_eq!(summaries[&historical], "ADMITTED_HISTORICAL_PROSE");
    tx.rollback().await.unwrap();
    // An actor-specific floor reduction changes read authorization only. It
    // must not purge/rewrite stored history or invalidate admitted versions.
    let (text,timing,complete):(String,Value,bool)=sqlx::query_as("SELECT m.text,m.timing,r.evidence_complete FROM conversation_messages m JOIN answer_support_receipts r ON r.receipt_id=m.id WHERE m.id=$1").bind(old).fetch_one(&f.owner).await.unwrap();
    assert_eq!(text, "EXCLUDED_ASSISTANT_PROSE");
    assert!(timing.to_string().contains("EXCLUDED_SOURCE_QUOTE"));
    assert!(complete);
    // Reproduce deployment upgrade, with no second floor mutation: an old
    // helper can freshly publish unsafe content after the floor already moved.
    // Also retain an in-flight old-worker snapshot for publication rejection.
    sqlx::raw_sql(include_str!(
        "../migrations_tenant/0197_answer_permitted_uuid_probes.sql"
    ))
    .execute(&f.owner)
    .await
    .unwrap();
    let upgrade_scope = scope(&f, conv).await;
    let unsafe_warm = fill(&f, &upgrade_scope).await;
    assert_eq!(
        message(&unsafe_warm, old)["text"],
        "EXCLUDED_ASSISTANT_PROSE"
    );
    rm::store::request_refresh::<ConversationDetail>(&f.app, &upgrade_scope)
        .await
        .unwrap();
    let claims = rm::store::claim(&f.app).await.unwrap();
    let old_claim = claims
        .iter()
        .find(|c| c.model == ConversationDetail::NAME && c.scope_key == upgrade_scope.key())
        .unwrap();
    let (unsafe_racing, racing_snapshot) =
        read_model_refresh::compute(&f.worker(), old_claim, &f.control)
            .await
            .unwrap();
    assert_eq!(
        message(&unsafe_racing, old)["text"],
        "EXCLUDED_ASSISTANT_PROSE"
    );
    let tenant_migration = include_str!("../migrations_tenant/0226_saved_answer_history_floor.sql");
    let control_migration =
        include_str!("../migrations_control/0415_saved_answer_history_floor.sql");
    assert_eq!(
        tenant_migration.lines().skip(1).collect::<Vec<_>>(),
        control_migration.lines().skip(1).collect::<Vec<_>>()
    );
    let mut migration = f.owner.begin().await.unwrap();
    sqlx::raw_sql(if org {
        tenant_migration
    } else {
        control_migration
    })
    .execute(&mut *migration)
    .await
    .unwrap();
    migration.commit().await.unwrap();
    let mut tx = begin_tenant_scoped(&f.app, f.account, f.user)
        .await
        .unwrap();
    let behind = rm::store::read::<ConversationDetail>(&mut tx, &upgrade_scope)
        .await
        .unwrap();
    assert!(
        behind.computed_at.is_some() && behind.stale,
        "upgrade barrier marks the old warm payload stale without another floor event"
    );
    tx.rollback().await.unwrap();
    // The framework may store a fenced payload while retaining its dirty mark;
    // its authority oracle must keep it stale on every read after publication.
    assert!(
        rm::store::publish(&f.app, old_claim, unsafe_racing, &racing_snapshot)
            .await
            .unwrap()
    );
    let mut tx = begin_tenant_scoped(&f.app, f.account, f.user)
        .await
        .unwrap();
    let behind = rm::store::read::<ConversationDetail>(&mut tx, &upgrade_scope)
        .await
        .unwrap();
    assert!(
        behind.computed_at.is_some() && behind.stale,
        "racing pre-fix publication stays stale under the upgrade fence"
    );
    tx.rollback().await.unwrap();
    let upgraded = fill(&f, &scope(&f, conv).await).await;
    assert_eq!(message(&upgraded, old)["support_status"], "unavailable");
    assert_eq!(message(&upgraded, mixed)["support_status"], "unavailable");
    assert_eq!(
        message(&upgraded, historical)["text"],
        "ADMITTED_HISTORICAL_PROSE"
    );
    sqlx::query("UPDATE team_seat_grants SET history_from=NULL WHERE team_id=$1 AND user_id=$2")
        .bind(team)
        .bind(f.user)
        .execute(&f.owner)
        .await
        .unwrap();
    let restored = fill(&f, &scope(&f, conv).await).await;
    assert_eq!(message(&restored, old)["support_status"], "complete");
    assert_eq!(message(&restored, old)["text"], "EXCLUDED_ASSISTANT_PROSE");
    // Opposite ordering was proved above: a bound floor commits before the
    // migration scan and the migration's own graph stamp fences that snapshot.
    // Now exercise a write attempted after the initially empty scan.
    concurrent_null_floor_upgrade(&f, conv, team, old, historical).await;
    f.close().await;
    match old_flag {
        Some(v) => std::env::set_var("CHAT_FIND_RESULTS_ENABLED", v),
        None => std::env::remove_var("CHAT_FIND_RESULTS_ENABLED"),
    }
}
#[tokio::test]
async fn tenant_saved_answer_history_floor() {
    contract(true).await;
}
#[tokio::test]
async fn control_paired_receipt_floor_sql_contract() {
    contract(false).await;
}

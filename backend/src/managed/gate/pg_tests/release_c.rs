use super::*;
use crate::workers::billing_derivation;
use sqlx::types::Decimal;
use std::sync::Arc;

async fn content_tx(f: &Fixture) -> Transaction<'static,Postgres> {
    match f.tenant {
        TenantId::Consumer => begin_account_scoped(&f.control.app,f.account).await.unwrap(),
        TenantId::Org(_) => crate::tenancy::scope::begin_tenant_system_scoped(f.app(),f.account).await.unwrap(),
    }
}

fn worker(f: &Fixture) -> WorkerTenant {
    WorkerTenant { tenant:f.tenant.clone(),pool:f.app().clone(),
        store:Arc::new(crate::store::PostgresStore::new(f.app().clone())) }
}

async fn legacy_rows(f: &Fixture, control_plane: bool, count:i64, reserved:bool, duplicate:Option<Uuid>) -> Vec<Uuid> {
    let pool=if control_plane { &f.control.owner } else { f.owner() };
    sqlx::query("INSERT INTO managed_budget_periods(account_id,purpose,period_start,limit_micro_usd,reserved_micro_usd,charged_micro_usd) \
        VALUES($1,'source_extraction',(now() AT TIME ZONE 'utc')::date,1000000000,$2,$3) \
        ON CONFLICT(account_id,purpose,period_start) DO UPDATE SET \
        reserved_micro_usd=managed_budget_periods.reserved_micro_usd+EXCLUDED.reserved_micro_usd, \
        charged_micro_usd=managed_budget_periods.charged_micro_usd+EXCLUDED.charged_micro_usd")
        .bind(f.account).bind(if reserved {count*1000} else {0}).bind(if reserved {0} else {count*1000})
        .execute(pool).await.unwrap();
    sqlx::query_scalar("INSERT INTO managed_attempt_receipts(receipt_id,account_id,purpose,period_start,graph_id,attributed_user_id,logical_call,attempt_no,candidate_models,input_bound_tokens,max_tokens,reserved_micro_usd,state,settled_micro_usd,created_at,settled_at) \
        SELECT COALESCE($5,gen_random_uuid()),$1,'source_extraction',(now() AT TIME ZONE 'utc')::date,$2,$3,gen_random_uuid(),1,ARRAY['legacy-local'],1,1,1000, \
        CASE WHEN $4 THEN 'reserved' ELSE 'billed' END,CASE WHEN $4 THEN NULL ELSE 1000 END,now()-interval '1 hour', \
        CASE WHEN $4 THEN NULL ELSE now()-interval '30 minutes' END FROM generate_series(1,$6::bigint) RETURNING receipt_id")
        .bind(f.account).bind(f.graph).bind(f.payer).bind(reserved).bind(duplicate).bind(count)
        .fetch_all(pool).await.unwrap()
}

async fn debit_count(f:&Fixture) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM credit_ledger WHERE account_id=$1 AND source_receipt_id IS NOT NULL")
        .bind(f.account).fetch_one(&f.control.owner).await.unwrap()
}

async fn hold_then_resume(org:bool) {
    let f=Fixture::at_boundary(org,false).await;
    let receipts=legacy_rows(&f,false,1,true,None).await;
    f.install_fence().await;
    f.balance(Decimal::from(10000)).await;
    let mut observations=content_tx(&f).await;
    sqlx::query("INSERT INTO corpus_capture_admissions(source_version_id,graph_id,account_id,source_item_id,captured_by,extraction_version,state,reason) SELECT version_id,graph_id,$2,source_item_id,$3,extraction_version,'blocked','legacy_managed_liabilities_pending' FROM source_versions WHERE version_id=$1 ON CONFLICT(source_version_id) DO NOTHING")
        .bind(f.version).bind(f.account).bind(f.payer).execute(&mut *observations).await.unwrap();
    for (profile,reason) in [("source-atoms-v1","operator_paused"),("meeting-atoms-v1","credits_exhausted")] {
        sqlx::query("INSERT INTO corpus_enrichment_runs(graph_id,account_id,requested_by,mode,corpus_kind,status,pause_reason,extraction_version,model,max_items,max_characters,max_model_calls) VALUES($1,$2,$3,'execute','real','paused',$4,$5,'deepseek/deepseek-v4-flash-0731',1000,8000000,10000)")
            .bind(f.graph).bind(f.account).bind(f.payer).bind(reason).bind(profile).execute(&mut *observations).await.unwrap();
    }
    observations.commit().await.unwrap();
    let plane=f.plane(); let req=completion_request();
    let gate=ManagedGate::open(f.call(&plane),&req).await.unwrap();
    assert_eq!(gate.reserve_attempt(1).await.unwrap_err(),Deny::Hold(HoldCode::LegacyManagedLiabilitiesPending));
    assert_eq!(debit_count(&f).await,0);
    let mut content=content_tx(&f).await;
    assert!(sqlx::query_scalar::<_,bool>("SELECT managed_settle($1,'billed',1000,'legacy-local',NULL,NULL)")
        .bind(receipts[0]).fetch_one(&mut *content).await.unwrap());
    content.commit().await.unwrap();
    billing_derivation::tick(&worker(&f),&f.control.app).await.unwrap();
    assert_eq!(debit_count(&f).await,1);
    billing_derivation::tick(&worker(&f),&f.control.app).await.unwrap();
    assert_eq!(debit_count(&f).await,1);
    let states:Vec<(String,String,Option<String>)>=sqlx::query_as("SELECT extraction_version,status,pause_reason FROM corpus_enrichment_runs WHERE graph_id=$1 ORDER BY extraction_version")
        .bind(f.graph).fetch_all(f.owner()).await.unwrap();
    assert_eq!(states.len(),3);
    assert!(states.contains(&("source-atoms-v1".into(),"running".into(),None)),"the actual legacy financial hold must resume");
    assert!(states.contains(&("source-atoms-v1".into(),"paused".into(),Some("operator_paused".into()))));
    assert!(states.contains(&("meeting-atoms-v1".into(),"paused".into(),Some("credits_exhausted".into()))));
    let basis:String=sqlx::query_scalar("SELECT hold_basis FROM corpus_capture_admissions WHERE source_version_id=$1")
        .bind(f.version).fetch_one(f.owner()).await.unwrap();
    assert_eq!(basis,"unverified_transition","financial resume must not upgrade unknown capture clocks");
    let permit=coralswarm_core::send_gate::acquire(&gate,2).await.expect("real legacy settle and scheduled billing automatically resume admission");
    gate.settle(permit.into_sent(),&SendOutcome::NotGenerated).await;
    drop(gate);drop(plane);f.finish().await;
}
#[tokio::test]
async fn c1_consumer_legacy_hold_real_settle_and_automatic_resume() { hold_then_resume(false).await; }
#[tokio::test]
async fn c1_org_legacy_hold_real_settle_and_automatic_resume() { hold_then_resume(true).await; }

#[tokio::test]
async fn c1_org_full_three_page_legacy_handoff_preserves_all_cursors_and_historical_credits() {
    let f=Fixture::at_boundary(true,false).await;
    let control_ids=legacy_rows(&f,true,1,false,None).await;
    legacy_rows(&f,false,1001,false,None).await;
    let cursor=Uuid::new_v4();
    sqlx::query("INSERT INTO credit_balances(account_id,balance,receipt_cursor_at,receipt_cursor_id,job_axis_cursor_finished_at,job_axis_cursor_job_id,query_axis_cursor_created_at,query_axis_cursor_usage_id,journal_axis_cursor_period,journal_axis_cursor_usage_id) \
        VALUES($1,10000,now(),$2,now(),$2,now(),$2,now(),$2)").bind(f.account).bind(cursor).execute(&f.control.owner).await.unwrap();
    sqlx::query("INSERT INTO credit_ledger(account_id,entry_kind,credits,source_receipt_id,graph_id,cost_usd,attributed_user_id) VALUES($1,'usage_managed',-7,$2,$3,0.001,$4)")
        .bind(f.account).bind(control_ids[0]).bind(f.graph).bind(f.payer).execute(&f.control.owner).await.unwrap();
    f.install_fence().await;
    let before:serde_json::Value=sqlx::query_scalar("SELECT to_jsonb(b)-'balance'-'managed_debited_credits'-'updated_at' FROM credit_balances b WHERE account_id=$1")
        .bind(f.account).fetch_one(&f.control.owner).await.unwrap();
    let plane=f.plane();let req=completion_request();let gate=ManagedGate::open(f.call(&plane),&req).await.unwrap();
    let start=std::time::Instant::now();
    let permit=coralswarm_core::send_gate::acquire(&gate,1).await.expect("complete real Org fund/arm operation must reconcile all pages");
    let elapsed=start.elapsed();
    let stale_after=crate::workers::reaper::stale_after();
    eprintln!("C1 actual complete1002 receipts/three tenant pages elapsed {elapsed:?}; managed statement limit5s; actual source job reaper stale threshold {stale_after:?}");
    assert!(elapsed<stale_after,"finite full handoff must finish before its actual source job can be reclaimed");
    assert_eq!(debit_count(&f).await,1002);
    let after:serde_json::Value=sqlx::query_scalar("SELECT to_jsonb(b)-'balance'-'managed_debited_credits'-'updated_at' FROM credit_balances b WHERE account_id=$1")
        .bind(f.account).fetch_one(&f.control.owner).await.unwrap();
    assert_eq!(before,after,"PreserveCursors must omit all cursor assignments");
    let historical:Decimal=sqlx::query_scalar("SELECT credits FROM credit_ledger WHERE source_receipt_id=$1")
        .bind(control_ids[0]).fetch_one(&f.control.owner).await.unwrap();
    assert_eq!(historical,Decimal::from(-7),"historical exact debit must never be repriced");
    gate.settle(permit.into_sent(),&SendOutcome::NotGenerated).await;
    drop(gate);drop(plane);f.finish().await;
}

#[tokio::test]
async fn c1_identical_cross_plane_receipt_with_matching_ledger_refuses() {
    let f=Fixture::at_boundary(true,false).await;
    let id=Uuid::new_v4();legacy_rows(&f,true,1,false,Some(id)).await;legacy_rows(&f,false,1,false,Some(id)).await;
    sqlx::query("INSERT INTO credit_ledger(account_id,entry_kind,credits,source_receipt_id,graph_id,cost_usd,attributed_user_id) VALUES($1,'usage_managed',-1,$2,$3,0.001,$4)")
        .bind(f.account).bind(id).bind(f.graph).bind(f.payer).execute(&f.control.owner).await.unwrap();
    f.install_fence().await;f.balance(Decimal::from(100)).await;
    let request=f.funding(1000);let mut tx=f.financial().await;
    let error=reserve_with_content(&mut tx,&request,f.legacy_context()).await.err().expect("identical duplicate provenance must refuse");
    assert!(error.to_string().contains("duplicate managed receipt provenance"),"{error}");tx.rollback().await.unwrap();
    assert_eq!(debit_count(&f).await,1);
    let holds:i64=sqlx::query_scalar("SELECT count(*) FROM managed_credit_reservations").fetch_one(&f.control.owner).await.unwrap();assert_eq!(holds,0);
    f.finish().await;
}

#[tokio::test]
async fn c1_normal_entry_savepoint_top_level_account_isolation_and_real_cascade() {
    let f=Fixture::new(false).await;f.balance(Decimal::from(10)).await;
    let (other,_) = extra_account(&f,None).await;
    let mut tx=begin_account_scoped(&f.control.app,f.account).await.unwrap();
    crate::billing::renewal::lock_period_rotation(&mut tx,f.account).await.unwrap();
    sqlx::query("SELECT set_config('app.account_id',$1,true)").bind(other.to_string()).execute(&mut *tx).await.unwrap();
    let error=sqlx::query("INSERT INTO credit_balances(account_id,balance) VALUES($1,1)").bind(other).execute(&mut *tx).await.unwrap_err();
    assert!(error.to_string().contains("compatible financial entry required"),"another account's same-transaction entry is insufficient");tx.rollback().await.unwrap();
    let mut tx=begin_account_scoped(&f.control.app,f.account).await.unwrap();
    sqlx::query("SAVEPOINT before_entry").execute(&mut *tx).await.unwrap();
    crate::billing::renewal::lock_period_rotation(&mut tx,f.account).await.unwrap();
    sqlx::query("ROLLBACK TO before_entry").execute(&mut *tx).await.unwrap();
    let error=sqlx::query("UPDATE credit_balances SET balance=999 WHERE account_id=$1").bind(f.account).execute(&mut *tx).await.unwrap_err();
    assert!(error.to_string().contains("compatible financial entry required"));tx.rollback().await.unwrap();
    let mut tx=f.financial().await;
    sqlx::query("SAVEPOINT after_entry").execute(&mut *tx).await.unwrap();
    sqlx::query("UPDATE credit_balances SET balance=11 WHERE account_id=$1").bind(f.account).execute(&mut *tx).await.unwrap();
    sqlx::query("ROLLBACK TO after_entry").execute(&mut *tx).await.unwrap();
    crate::billing::renewal::lock_period_rotation(&mut tx,f.account).await.unwrap();
    sqlx::query("UPDATE credit_balances SET balance=12 WHERE account_id=$1").bind(f.account).execute(&mut *tx).await.unwrap();
    tx.rollback().await.unwrap();
    let mut tx=begin_account_scoped(&f.control.app,f.account).await.unwrap();
    let error=sqlx::query("UPDATE credit_balances SET balance=999 WHERE account_id=$1").bind(f.account).execute(&mut *tx).await.unwrap_err();
    assert!(error.to_string().contains("compatible financial entry required"));tx.rollback().await.unwrap();
    let mut tx=f.financial().await;
    let grant:Uuid=sqlx::query_scalar("INSERT INTO grants(account_id,plan_id,plan_is_seat_priced,seats) SELECT $1,id,true,1 FROM plans WHERE code='team' RETURNING id")
        .bind(f.account).fetch_one(&mut *tx).await.unwrap();
    sqlx::query("INSERT INTO grant_seat_tiers(grant_id,seat_tier_id,seats) SELECT $1,id,1 FROM seat_tiers WHERE code='standard'")
        .bind(grant).execute(&mut *tx).await.unwrap();
    sqlx::query("DELETE FROM grants WHERE id=$1").bind(grant).execute(&mut *tx).await.unwrap();
    tx.commit().await.unwrap();
    let mut tx=begin_account_scoped(&f.control.app,other).await.unwrap();
    crate::billing::renewal::lock_period_rotation(&mut tx,other).await.unwrap();
    let grant:Uuid=sqlx::query_scalar("INSERT INTO grants(account_id,plan_id,plan_is_seat_priced,seats) SELECT $1,id,true,1 FROM plans WHERE code='team' RETURNING id")
        .bind(other).fetch_one(&mut *tx).await.unwrap();
    sqlx::query("INSERT INTO grant_seat_tiers(grant_id,seat_tier_id,seats) SELECT $1,id,1 FROM seat_tiers WHERE code='standard'")
        .bind(grant).execute(&mut *tx).await.unwrap();
    sqlx::query("DELETE FROM accounts WHERE id=$1").bind(other).execute(&mut *tx).await.unwrap();
    tx.commit().await.unwrap();
    let children:i64=sqlx::query_scalar("SELECT count(*) FROM grant_seat_tiers WHERE grant_id=$1").bind(grant).fetch_one(&f.control.owner).await.unwrap();assert_eq!(children,0);
    let mut tx=f.financial().await;
    let error=sqlx::query("TRUNCATE credit_balances").execute(&mut *tx).await.unwrap_err();
    assert!(error.to_string().contains("cannot be truncated"));tx.rollback().await.unwrap();
    let private_table:sqlx::postgres::types::Oid=sqlx::query_scalar("SELECT c.oid FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='billing_private' AND c.relname='account_entry'")
        .fetch_one(&f.control.app).await.unwrap();
    let protected:bool=sqlx::query_scalar("SELECT NOT has_schema_privilege(current_user,'billing_private','USAGE,CREATE') \
        AND NOT has_table_privilege(current_user,$1::oid,'SELECT,INSERT,UPDATE,DELETE,TRUNCATE') \
        AND NOT pg_has_role(current_user,'coralswarm_billing_entry_owner','MEMBER')")
        .bind(private_table).fetch_one(&f.control.app).await.unwrap();assert!(protected);
    for table in ["credit_conversion","plans","seat_tiers","model_rates","credit_packs"] {
        let writable:bool=sqlx::query_scalar("SELECT has_table_privilege(current_user,$1,'INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER') OR has_any_column_privilege(current_user,$1,'INSERT,UPDATE,REFERENCES')")
            .bind(table).fetch_one(&f.control.app).await.unwrap();assert!(!writable,"{table}");
    }
    f.finish().await;
}

#[tokio::test]
async fn c1_source_seal_terminal_zero_identity_and_cloud_embedding_preserved_both_planes() {
    for org in [false,true] {
        let f=Fixture::at_boundary(org,false).await;let ids=legacy_rows(&f,false,1,true,None).await;
        f.install_fence().await;
        let mut tx=content_tx(&f).await;
        let error=sqlx::query("INSERT INTO managed_attempt_receipts(account_id,purpose,period_start,graph_id,logical_call,attempt_no,candidate_models,input_bound_tokens,max_tokens,reserved_micro_usd) VALUES($1,'source_extraction',(now() AT TIME ZONE 'utc')::date,$2,gen_random_uuid(),1,ARRAY['m'],1,1,1000)")
            .bind(f.account).bind(f.graph).execute(&mut *tx).await.unwrap_err();assert!(error.to_string().contains("requires funded dispatch generation"));tx.rollback().await.unwrap();
        let mut tx=content_tx(&f).await;
        assert!(sqlx::query_scalar::<_,bool>("SELECT managed_settle($1,'not_generated',0,NULL,NULL,NULL)").bind(ids[0]).fetch_one(&mut *tx).await.unwrap());tx.commit().await.unwrap();
        for query in ["UPDATE managed_attempt_receipts SET settled_micro_usd=1000,state='billed' WHERE receipt_id=$1",
                      "UPDATE managed_attempt_receipts SET receipt_id=gen_random_uuid() WHERE receipt_id=$1",
                      "UPDATE managed_attempt_receipts SET created_at=now()-interval '2 hours' WHERE receipt_id=$1"] {
            let mut tx=content_tx(&f).await;
            assert!(sqlx::query(query).bind(ids[0]).execute(&mut *tx).await.is_err());tx.rollback().await.unwrap();
        }
        let mut owner=f.owner().begin().await.unwrap();
        let error=sqlx::query("DELETE FROM managed_attempt_receipts WHERE receipt_id=$1").bind(ids[0]).execute(&mut *owner).await.unwrap_err();
        assert!(error.to_string().contains("source receipt history cannot be deleted"));owner.rollback().await.unwrap();
        let mut tx=content_tx(&f).await;
        sqlx::query("INSERT INTO managed_budget_periods(account_id,purpose,period_start,limit_micro_usd,reserved_micro_usd) VALUES($1,'cloud_embedding',(now() AT TIME ZONE 'utc')::date,10000,1000)")
            .bind(f.account).execute(&mut *tx).await.unwrap();
        let embedding:Uuid=sqlx::query_scalar("INSERT INTO managed_attempt_receipts(account_id,purpose,period_start,graph_id,logical_call,attempt_no,candidate_models,input_bound_tokens,max_tokens,reserved_micro_usd) VALUES($1,'cloud_embedding',(now() AT TIME ZONE 'utc')::date,$2,gen_random_uuid(),1,ARRAY['text-embedding-3-small'],1,0,1000) RETURNING receipt_id")
            .bind(f.account).bind(f.graph).fetch_one(&mut *tx).await.unwrap();
        assert!(sqlx::query_scalar::<_,bool>("SELECT managed_settle($1,'billed',10,'text-embedding-3-small',NULL,NULL)").bind(embedding).fetch_one(&mut *tx).await.unwrap());
        tx.commit().await.unwrap();f.finish().await;
    }
}

async fn extra_account(f:&Fixture,org:Option<&str>) -> (Uuid,Uuid) {
    let account=Uuid::new_v4();let user=Uuid::new_v4();
    sqlx::query("INSERT INTO users(id,email) VALUES($1,$2)").bind(user).bind(format!("{user}@recovery.test")).execute(&f.control.owner).await.unwrap();
    sqlx::query("INSERT INTO accounts(id,owner_user_id,clerk_org_id) VALUES($1,$2,$3)")
        .bind(account).bind(user).bind(org).execute(&f.control.owner).await.unwrap();
    (account,user)
}

#[tokio::test]
async fn c1_disabled_admission_ordinary_error_unavailable_org_and_healthy_legacy_and_pinned_v1_recovery() {
    let f=Fixture::at_boundary(false,false).await;
    let (healthy_account,healthy_user)=extra_account(&f,None).await;
    let healthy=Fixture {
        admin:f.admin.clone(),control:Database {owner:f.control.owner.clone(),app:f.control.app.clone(),db:f.control.db.clone(),role:f.control.role.clone()},
        content:None,tenant:TenantId::Consumer,account:healthy_account,payer:healthy_user,
        graph:Uuid::new_v4(),version:Uuid::new_v4(),unit:Uuid::new_v4(),
    };
    legacy_rows(&healthy,true,1,false,None).await;
    let unavailable_org=format!("org_unavailable_{}",Uuid::new_v4().simple());
    let (unavailable_account,_)=extra_account(&f,Some(&unavailable_org)).await;
    sqlx::query("INSERT INTO tenant_databases(clerk_org_id,account_id,slug,db_secret_ref,status) VALUES($1,$2,$3,'UNAVAILABLE_TEST_TENANT','active')")
        .bind(&unavailable_org).bind(unavailable_account).bind(format!("bad-{}",&unavailable_account.simple().to_string()[..20]))
        .execute(&f.control.owner).await.unwrap();
    f.install_fence().await;f.balance(Decimal::from(10000)).await;
    let period=f.allowance(Decimal::from(10)).await;
    let plane=f.plane();let req=completion_request();let gate=ManagedGate::open(f.call(&plane),&req).await.unwrap();
    let permit=coralswarm_core::send_gate::acquire(&gate,1).await.unwrap();
    let sent=permit.into_sent();let receipt=sent.receipt_id().unwrap();
    let mut content=content_tx(&f).await;
    assert!(sqlx::query_scalar::<_,bool>("SELECT managed_settle_v1($1,'billed',44,'local',NULL,NULL)")
        .bind(receipt).fetch_one(&mut *content).await.unwrap());content.commit().await.unwrap();
    let mut financial=f.financial().await;
    sqlx::query("UPDATE member_allowance_periods SET period_end=clock_timestamp() WHERE id=$1").bind(period).execute(&mut *financial).await.unwrap();
    financial.commit().await.unwrap();
    sqlx::query("INSERT INTO credit_conversion(effective_from,reference_cost_per_credit,note) VALUES(clock_timestamp(),9,'after actual v1 arm')")
        .execute(&f.control.owner).await.unwrap();
    let mut observations=content_tx(&f).await;
    sqlx::query("INSERT INTO corpus_capture_admissions(source_version_id,graph_id,account_id,source_item_id,captured_by,extraction_version,state,reason) SELECT version_id,graph_id,$2,source_item_id,$3,extraction_version,'blocked','operator_paused' FROM source_versions WHERE version_id=$1 ON CONFLICT(source_version_id) DO NOTHING")
        .bind(f.version).bind(f.account).bind(f.payer).execute(&mut *observations).await.unwrap();
    sqlx::query("UPDATE corpus_enrichment_runs SET status='paused',pause_reason='operator_paused' WHERE account_id=$1")
        .bind(f.account).execute(&mut *observations).await.unwrap();
    observations.commit().await.unwrap();
    let before:Vec<(Uuid,serde_json::Value)>=sqlx::query_as("SELECT account_id,to_jsonb(b)-'balance'-'managed_debited_credits'-'updated_at' FROM credit_balances b ORDER BY account_id")
        .fetch_all(&f.control.owner).await.unwrap();
    let pinned_before:serde_json::Value=sqlx::query_scalar("SELECT to_jsonb(r)-'state'-'settled_micro_usd'-'settled_at' FROM managed_credit_reservations r WHERE receipt_id=$1")
        .bind(receipt).fetch_one(&f.control.owner).await.unwrap();
    let old_disabled=std::env::var_os("MANAGED_PROCESSING_DISABLED");
    std::env::set_var("MANAGED_PROCESSING_DISABLED","true");
    let disabled_gate=ManagedGate::open(f.call(&plane),&req).await.unwrap();
    assert_eq!(disabled_gate.reserve_attempt(2).await.unwrap_err(),Deny::Hold(HoldCode::ProcessingDisabled));
    drop(disabled_gate);
    use sqlx::Connection;
    let mut barrier=PgConnection::connect_with(&f.control.app.connect_options().as_ref().clone()
        .application_name("cor22-config-publication-barrier")).await.unwrap();
    let first_account:Uuid=sqlx::query_scalar("SELECT id FROM accounts ORDER BY id LIMIT 1")
        .fetch_one(&f.control.owner).await.unwrap();
    let worker_pid:i32=sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&f.control.app).await.unwrap();
    for expected_changes in [2,0] {
        let mut publication=barrier.begin().await.unwrap();
        sqlx::query("SELECT set_config('app.account_id',$1,true)").bind(first_account.to_string())
            .execute(&mut *publication).await.unwrap();
        crate::billing::renewal::lock_period_rotation(&mut publication,first_account).await.unwrap();
        let publication_pid:i32=sqlx::query_scalar("SELECT pg_backend_pid()")
            .fetch_one(&mut *publication).await.unwrap();
        let conversion_history:serde_json::Value=sqlx::query_scalar("SELECT COALESCE(jsonb_agg(to_jsonb(c) ORDER BY id),'[]'::jsonb) FROM credit_conversion c")
            .fetch_one(&f.control.owner).await.unwrap();
        sqlx::query("DELETE FROM credit_conversion").execute(&f.control.owner).await.unwrap();
        let t=worker(&f);
        let tick=billing_derivation::tick(&t,&f.control.app);tokio::pin!(tick);
        let observe=async {
            let deadline=tokio::time::Instant::now()+Duration::from_secs(15);
            loop {
                let waiting:bool=sqlx::query_scalar("SELECT $1=ANY(pg_blocking_pids($2))")
                    .bind(publication_pid).bind(worker_pid).fetch_one(&f.control.owner).await.unwrap();
                if waiting {break;}
                assert!(tokio::time::Instant::now()<deadline,"real recovery must reach the account-key barrier after ordinary configuration refusal");
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        };
        tokio::select! { result=&mut tick => panic!("real tick returned before configuration publication: {result:?}"), _=observe=>{} }
        sqlx::query("INSERT INTO credit_conversion SELECT * FROM jsonb_populate_recordset(NULL::credit_conversion,$1)")
            .bind(&conversion_history).execute(&f.control.owner).await.unwrap();
        let restored_history:serde_json::Value=sqlx::query_scalar("SELECT COALESCE(jsonb_agg(to_jsonb(c) ORDER BY id),'[]'::jsonb) FROM credit_conversion c")
            .fetch_one(&f.control.owner).await.unwrap();
        assert_eq!(conversion_history,restored_history,"publication restores every original conversion row before appending current configuration");
        sqlx::query("INSERT INTO credit_conversion(effective_from,reference_cost_per_credit,note) VALUES(clock_timestamp(),9,'valid configuration published during scheduled recovery')")
            .execute(&f.control.owner).await.unwrap();
        publication.commit().await.unwrap();
        let outcome=tick.await.unwrap_err();
        assert!(outcome.contains("ordinary derivation: Err(\"billing derivation: no active credit_conversion row\")"),"{outcome}");
        assert!(outcome.contains(&format!("committed {expected_changes} changes")),"{outcome}");
        assert_eq!(debit_count(&healthy).await,1);assert_eq!(debit_count(&f).await,1);
    }
    barrier.close().await.unwrap();
    let pinned_after:serde_json::Value=sqlx::query_scalar("SELECT to_jsonb(r)-'state'-'settled_micro_usd'-'settled_at' FROM managed_credit_reservations r WHERE receipt_id=$1")
        .bind(receipt).fetch_one(&f.control.owner).await.unwrap();
    assert_eq!(pinned_before,pinned_after,"recovery preserves the original financial binding including conversion and member period");
    let (state,pinned,consumed):(String,Uuid,Decimal)=sqlx::query_as("SELECT r.state,r.member_period_id,p.managed_consumed_credits FROM managed_credit_reservations r JOIN member_allowance_periods p ON p.id=r.member_period_id WHERE r.receipt_id=$1")
        .bind(receipt).fetch_one(&f.control.owner).await.unwrap();
    assert_eq!(state,"settled");assert_eq!(pinned,period);assert_eq!(consumed,Decimal::new(44,6)/Decimal::new(44,4));
    let after:Vec<(Uuid,serde_json::Value)>=sqlx::query_as("SELECT account_id,to_jsonb(b)-'balance'-'managed_debited_credits'-'updated_at' FROM credit_balances b WHERE account_id<>$1 ORDER BY account_id")
        .bind(healthy.account).fetch_all(&f.control.owner).await.unwrap();
    assert_eq!(before,after,"failed ordinary operation must not assign any cursor");
    let healthy_cursors:bool=sqlx::query_scalar("SELECT receipt_cursor_at IS NULL AND job_axis_cursor_finished_at IS NULL AND query_axis_cursor_created_at IS NULL AND journal_axis_cursor_period IS NULL FROM credit_balances WHERE account_id=$1")
        .bind(healthy.account).fetch_one(&f.control.owner).await.unwrap();assert!(healthy_cursors);
    let (status,reason):(String,Option<String>)=sqlx::query_as("SELECT status,pause_reason FROM corpus_enrichment_runs WHERE account_id=$1")
        .bind(f.account).fetch_one(f.owner()).await.unwrap();
    assert_eq!((status,reason),("paused".into(),Some("operator_paused".into())),"financial recovery cannot clear an operator pause");
    let basis:String=sqlx::query_scalar("SELECT hold_basis FROM corpus_capture_admissions WHERE source_version_id=$1")
        .bind(f.version).fetch_one(f.owner()).await.unwrap();
    assert_eq!(basis,"unverified_transition","financial recovery cannot upgrade an unknown capture clock");
    match old_disabled {Some(v)=>std::env::set_var("MANAGED_PROCESSING_DISABLED",v),None=>std::env::remove_var("MANAGED_PROCESSING_DISABLED")}
    drop(sent);drop(gate);drop(plane);drop(healthy);f.finish().await;
}

#[tokio::test]
async fn c1_real_issuance_future_period_rollover_slot_and_stripe_claim_writers() {
    for org in [false,true] {
        let f=Fixture::new(org).await;
        let plan=crate::billing::issuance::resolve_active_plan(&f.control.app,if org {"team"} else {"pro"}).await.unwrap().unwrap();
        crate::billing::issuance::issue_grant(&f.control.app,f.account,&plan,org.then_some(2),None,Some("C1 real issuance")).await.unwrap();
        if org {
            let mut tx=f.financial().await;
            let added=sqlx::query("INSERT INTO grant_seat_tiers(grant_id,seat_tier_id,seats) SELECT g.id,t.id,2 FROM grants g CROSS JOIN seat_tiers t WHERE g.account_id=$1 AND g.plan_id=$2 AND g.revoked_at IS NULL AND t.code='standard' AND t.is_active")
                .bind(f.account).bind(plan.id).execute(&mut *tx).await.unwrap();
            assert_eq!(added.rows_affected(),1,"the current team plan purchases slots through the tier ledger");
            tx.commit().await.unwrap();
            let slot=crate::billing::slot_pool::reserve_slot(&f.control.app,f.account,f.payer,None,chrono::Duration::minutes(5)).await.unwrap();
            assert!(slot.is_some(),"current first-entry slot owner still admits the unallocated purchased seat");
        }
        let subscription=format!("sub_c1_{}",f.account.simple());
        let mut tx=f.financial().await;
        sqlx::query("INSERT INTO stripe_subscriptions(account_id,stripe_customer_id,stripe_subscription_id,status) VALUES($1,$2,$3,'active')")
            .bind(f.account).bind(format!("cus_c1_{}",f.account.simple())).bind(&subscription).execute(&mut *tx).await.unwrap();tx.commit().await.unwrap();
        let start=Utc::now()+chrono::Duration::days(60);let end=start+chrono::Duration::days(30);
        let mut tx=f.financial().await;
        sqlx::query("INSERT INTO member_allowance_periods(account_id,user_id,period_start,period_end,granted_credits) VALUES($1,$2,$3,$4,5)")
            .bind(f.account).bind(f.payer).bind(end+chrono::Duration::days(1)).bind(end+chrono::Duration::days(31)).execute(&mut *tx).await.unwrap();tx.commit().await.unwrap();
        assert!(crate::routes::stripe_webhook::native_invoice_writer(&f.control.app,f.account,"evt_c1_invoice",&subscription,start,end).await.unwrap());
        assert!(!crate::routes::stripe_webhook::native_invoice_writer(&f.control.app,f.account,"evt_c1_invoice",&subscription,start,end).await.unwrap());
        assert!(crate::routes::stripe_webhook::native_subscription_writer(&f.control.app,f.account,"evt_c1_update",start,end).await.unwrap());
        assert!(crate::routes::stripe_webhook::native_cancel_writer(&f.control.app,f.account,"evt_c1_cancel",&subscription).await.unwrap());
        let claims:i64=sqlx::query_scalar("SELECT count(*) FROM stripe_webhook_events WHERE account_id=$1 AND outcome IS NOT NULL")
            .bind(f.account).fetch_one(&f.control.owner).await.unwrap();assert_eq!(claims,3);
        let future:i64=sqlx::query_scalar("SELECT count(*) FROM member_allowance_periods WHERE account_id=$1 AND period_start>$2")
            .bind(f.account).bind(end).fetch_one(&f.control.owner).await.unwrap();assert_eq!(future,0,"actual period owner deletes invalid future allocations");
        f.finish().await;
    }
}

#[tokio::test]
async fn c1_cancelled_partial_legacy_page_rolls_back_then_full_operation_retries() {
    let f=Fixture::at_boundary(false,false).await;legacy_rows(&f,true,501,false,None).await;
    f.install_fence().await;f.balance(Decimal::from(10000)).await;
    let period=f.allowance(Decimal::from(10000)).await;
    let mut reader=begin_account_scoped(&f.control.owner,f.account).await.unwrap();
    sqlx::query("SELECT id FROM member_allowance_periods WHERE id=$1 FOR UPDATE").bind(period).fetch_one(&mut *reader).await.unwrap();
    let plane=f.plane();let req=completion_request();let gate=ManagedGate::open(f.call(&plane),&req).await.unwrap();
    {
        let reserve=gate.reserve_attempt(1);tokio::pin!(reserve);
        let observe=async {
            let deadline=tokio::time::Instant::now()+Duration::from_secs(15);
            loop {
                let waiting:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=$1 AND wait_event_type='Lock' AND query LIKE 'UPDATE member_allowance_periods%')")
                    .bind(f.control.db.name()).fetch_one(&f.control.owner).await.unwrap();
                if waiting {break;}
                assert!(tokio::time::Instant::now()<deadline,"actual first-page member update must block after inserting its ledger page");
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        };
        tokio::select! { result=&mut reserve => panic!("real operation returned before controlled cancellation: {result:?}"), _=observe=>{} }
    }
    reader.commit().await.unwrap();
    let mut drained=begin_account_scoped(&f.control.app,f.account).await.unwrap();
    sqlx::query("SELECT 1").execute(&mut *drained).await.unwrap();drained.rollback().await.unwrap();
    assert_eq!(debit_count(&f).await,0,"cancelled transaction cannot retain a paid prefix");
    let permit=coralswarm_core::send_gate::acquire(&gate,2).await.unwrap();assert_eq!(debit_count(&f).await,501);
    gate.settle(permit.into_sent(),&SendOutcome::NotGenerated).await;
    drop(gate);drop(plane);f.finish().await;
}

//! `managed_enrichment` scaffold (source-agnostic plan PR 3): the shared
//! graph-level purpose through the real funded gate, on both planes.
use super::*;
use crate::managed::enrichment::{
    self, EnrichmentInput, EnrichmentJob, EnrichmentWork, InputCheck, Invalidated,
};
use sqlx::types::Decimal;
use std::ffi::OsString;

/// Process env for one test; restored on drop. The gate suite runs with
/// `--test-threads=1`.
struct Env(Vec<(&'static str, Option<OsString>)>);

impl Env {
    fn set(pairs: &[(&'static str, Option<&str>)]) -> Self {
        let saved = pairs.iter().map(|(key, _)| (*key, std::env::var_os(key))).collect();
        for (key, value) in pairs {
            match value {
                Some(value) => std::env::set_var(key, value),
                None => std::env::remove_var(key),
            }
        }
        Self(saved)
    }
}

impl Drop for Env {
    fn drop(&mut self) {
        for (key, value) in self.0.drain(..) {
            match value {
                Some(value) => std::env::set_var(key, value),
                None => std::env::remove_var(key),
            }
        }
    }
}

const ON: &[(&str, Option<&str>)] = &[
    ("ENRICHMENT_PROCESSING_ENABLED", Some("true")),
    ("MANAGED_PROCESSING_DISABLED", None),
    ("MANAGED_DAILY_CAP_USD_ENRICHMENT", None),
    ("MANAGED_PER_VERSION_CAP_USD", None),
];

async fn funded(org: bool) -> Fixture {
    let f = Fixture::new(org).await;
    f.balance(Decimal::from(100)).await;
    f
}

impl Fixture {
    async fn scoped(&self) -> Transaction<'static, Postgres> {
        match self.tenant {
            TenantId::Consumer => begin_account_scoped(&self.control.app, self.account).await.unwrap(),
            TenantId::Org(_) => crate::tenancy::scope::begin_tenant_system_scoped(self.app(), self.account)
                .await
                .unwrap(),
        }
    }

    /// A claimed queue row the enrichment fence can own.
    async fn running_job(&self) -> EnrichmentJob {
        let id = crate::jobs::queue::enqueue(
            self.app(),
            &crate::jobs::JobKind::InferRelationships { graph_id: self.graph },
            Some(self.account),
            Some(&format!("enrichment-fixture:{}", Uuid::new_v4())),
            0,
        )
        .await
        .unwrap()
        .unwrap();
        let row = crate::jobs::queue::dequeue(self.app(), &["InferRelationships"], 16)
            .await
            .unwrap()
            .into_iter()
            .find(|job| job.id == id)
            .unwrap();
        EnrichmentJob { id, kind: row.kind, attempts: row.attempts }
    }

    fn enrichment<'a>(&'a self, plane: &'a JobScope<'a>, principal: Uuid, work: &'a EnrichmentWork) -> ManagedCall<'a> {
        ManagedCall {
            plane,
            control: &self.control.app,
            tenant: &self.tenant,
            account_id: self.account,
            principal,
            fallback_model: None,
            subject: Subject::Enrichment { work },
        }
    }

    async fn count(&self, sql: &str) -> i64 {
        sqlx::query_scalar(sql).fetch_one(self.owner()).await.unwrap()
    }
}

fn inputs() -> Vec<EnrichmentInput> {
    vec![
        EnrichmentInput::new("atom", Uuid::new_v4(), 3, b"coral alpha"),
        EnrichmentInput::new("node", Uuid::new_v4(), 1, b"reef beta"),
        EnrichmentInput::new("journal_hour", Uuid::new_v4(), 0, b"hour gamma"),
    ]
}

fn billed(cost_usd: f64) -> SendOutcome {
    SendOutcome::Billed {
        reported_cost_usd: Some(cost_usd),
        input_tokens: 2,
        output_tokens: 2,
        served_model: "deepseek/deepseek-v4-flash-0731".into(),
        upstream: None,
        generation_id: Some("gen-enrichment-fixture".into()),
    }
}

/// Reserves and settles one call; returns its receipt.
async fn sent_and_settled(f: &Fixture, work: &EnrichmentWork, outcome: &SendOutcome) -> Uuid {
    let plane = f.plane();
    let req = completion_request();
    let gate = ManagedGate::open(f.enrichment(&plane, f.payer, work), &req).await.unwrap();
    let permit = coralswarm_core::send_gate::acquire(&gate, 1).await.unwrap();
    let sent = permit.into_sent();
    let receipt = sent.receipt_id().unwrap();
    gate.settle(sent, outcome).await;
    receipt
}

async fn input_outcomes(f: &Fixture, receipt: Uuid) -> Vec<String> {
    sqlx::query_scalar("SELECT outcome FROM managed_enrichment_inputs WHERE receipt_id=$1 ORDER BY ordinal")
        .bind(receipt)
        .fetch_all(f.owner())
        .await
        .unwrap()
}

#[tokio::test]
async fn enrichment_reserve_settle_binds_every_input_under_the_shared_cap_both_planes() {
    let _env = Env::set(ON);
    for org in [false, true] {
        let f = funded(org).await;
        let job = f.running_job().await;
        let work = EnrichmentWork::new(f.account, f.graph, Some(job.clone()), None, inputs()).unwrap();
        let plane = f.plane();
        let principal = enrichment::select_principal(&plane, &f.control.app, &f.tenant, &work).await.unwrap();
        assert_eq!(principal, f.payer, "executor selection charges the graph's owner");
        let receipt = sent_and_settled(&f, &work, &billed(0.000044)).await;

        let shape: (String, Option<Uuid>, Option<Uuid>, Option<Uuid>, Uuid, Uuid, String, String, i64, i16) = sqlx::query_as(
            "SELECT purpose,unit_id,source_version_id,job_id,attributed_user_id,logical_call,dispatch_phase,state,settled_micro_usd,protocol_version \
               FROM managed_attempt_receipts WHERE receipt_id=$1",
        ).bind(receipt).fetch_one(f.owner()).await.unwrap();
        assert_eq!(
            shape,
            ("managed_enrichment".into(), None, None, Some(job.id), f.payer, work.logical_call(), "armed".into(), "billed".into(), 44, 1),
            "a graph-level receipt has no unit or version and names its job, payer and logical call"
        );
        let rows: Vec<(i16, String, Uuid, i64, Vec<u8>, String, Uuid, Uuid)> = sqlx::query_as(
            "SELECT ordinal,input_kind,input_id,content_revision,content_sha256,outcome,account_id,graph_id \
               FROM managed_enrichment_inputs WHERE receipt_id=$1 ORDER BY ordinal",
        ).bind(receipt).fetch_all(f.owner()).await.unwrap();
        assert_eq!(rows.len(), 3, "every input is written with its receipt");
        for (row, (ordinal, input)) in rows.iter().zip(work.inputs().iter().enumerate()) {
            assert_eq!(
                (row.0 as usize, row.1.as_str(), row.2, row.3, row.4.as_slice(), row.5.as_str(), row.6, row.7),
                (ordinal, input.kind.as_str(), input.id, input.content_revision, input.content_sha256.as_slice(), "sent", f.account, f.graph)
            );
        }
        let period: (i64, i64, i64, i64) = sqlx::query_as(
            "SELECT limit_micro_usd,reserved_micro_usd,charged_micro_usd,attempts FROM managed_budget_periods \
              WHERE account_id=$1 AND purpose='managed_enrichment'",
        ).bind(f.account).fetch_one(f.owner()).await.unwrap();
        assert_eq!(period, (2_000_000, 0, 44, 1), "default cap is $2 per account per day");

        // A second consumer (no queue job: a sweep) draws on the same period.
        let sweep = EnrichmentWork::new(f.account, f.graph, None, Some(f.payer), inputs()).unwrap();
        let second = sent_and_settled(&f, &sweep, &billed(0.00001)).await;
        let job_id: Option<Uuid> = sqlx::query_scalar("SELECT job_id FROM managed_attempt_receipts WHERE receipt_id=$1")
            .bind(second).fetch_one(f.owner()).await.unwrap();
        assert_eq!(job_id, None);
        let shared: (i64, i64) = sqlx::query_as(
            "SELECT count(*),sum(charged_micro_usd)::bigint FROM managed_budget_periods WHERE account_id=$1",
        ).bind(f.account).fetch_one(f.owner()).await.unwrap();
        assert_eq!(shared, (1, 54), "one shared managed_enrichment period for every graph-level consumer");

        let debits: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM credit_ledger WHERE account_id=$1 AND entry_kind='usage_managed' AND source_receipt_id=ANY($2)",
        ).bind(f.account).bind(vec![receipt, second]).fetch_one(&f.control.owner).await.unwrap();
        assert_eq!(debits, 2, "each billed receipt is debited once at settle");

        // Job recovery after a crash is idempotent over settled receipts: no
        // state change and no second debit.
        enrichment::recover_job(&plane, &f.control.app, &f.tenant, f.account, job.id).await.unwrap();
        let state: String = sqlx::query_scalar("SELECT state FROM managed_attempt_receipts WHERE receipt_id=$1")
            .bind(receipt).fetch_one(f.owner()).await.unwrap();
        assert_eq!(state, "billed");
        let debits_after: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM credit_ledger WHERE account_id=$1 AND source_receipt_id=ANY($2)",
        ).bind(f.account).bind(vec![receipt, second]).fetch_one(&f.control.owner).await.unwrap();
        assert_eq!(debits_after, 2, "recovery never re-debits");

        // Publish: unchanged inputs verify fresh, then are marked written.
        let mut tx = f.scoped().await;
        let InputCheck::Fresh(verified) = enrichment::verify_inputs_locked(&mut tx, &work, work.inputs()).await.unwrap() else {
            panic!("unchanged inputs must verify fresh");
        };
        assert_eq!(verified.receipt(), receipt);
        verified.mark_written(&mut tx).await.unwrap();
        tx.commit().await.unwrap();
        assert_eq!(input_outcomes(&f, receipt).await, ["written"; 3]);
        let mut tx = f.scoped().await;
        assert!(matches!(
            enrichment::verify_inputs_locked(&mut tx, &work, work.inputs()).await.unwrap(),
            InputCheck::AlreadyWritten
        ));
        tx.rollback().await.unwrap();
        drop(plane);
        f.finish().await;
    }
}

#[tokio::test]
async fn enrichment_publish_reverify_rejects_changed_inputs_and_deletion_invalidates_both_planes() {
    let _env = Env::set(ON);
    for org in [false, true] {
        let f = funded(org).await;
        let job = f.running_job().await;
        let new_work = || EnrichmentWork::new(f.account, f.graph, Some(job.clone()), None, inputs()).unwrap();

        // A changed revision, a changed digest, a missing (deleted) input and
        // an extra input are all stale.
        for case in 0..4 {
            let work = new_work();
            let receipt = sent_and_settled(&f, &work, &billed(0.00001)).await;
            let mut current = work.inputs().to_vec();
            match case {
                0 => current[0].content_revision += 1,
                1 => current[1].content_sha256 = EnrichmentInput::new("node", current[1].id, 1, b"edited").content_sha256,
                2 => {
                    current.remove(2);
                }
                _ => current.push(EnrichmentInput::new("atom", Uuid::new_v4(), 1, b"new coral")),
            }
            let mut tx = f.scoped().await;
            assert!(
                matches!(enrichment::verify_inputs_locked(&mut tx, &work, &current).await.unwrap(), InputCheck::Stale),
                "case {case} must be stale"
            );
            tx.commit().await.unwrap();
            assert_eq!(input_outcomes(&f, receipt).await, ["discarded_stale"; 3], "case {case}");
        }

        // Deletion while in flight: one query flips the input; publish then sees stale.
        let in_flight = new_work();
        let in_flight_receipt = sent_and_settled(&f, &in_flight, &billed(0.00001)).await;
        // Already published: the same query names the receipt for retraction.
        let published = new_work();
        let published_receipt = sent_and_settled(&f, &published, &billed(0.00001)).await;
        let mut tx = f.scoped().await;
        let InputCheck::Fresh(verified) = enrichment::verify_inputs_locked(&mut tx, &published, published.inputs()).await.unwrap() else {
            panic!("fresh");
        };
        verified.mark_written(&mut tx).await.unwrap();
        tx.commit().await.unwrap();

        let deleted = [in_flight.inputs()[0].id, published.inputs()[0].id];
        let mut tx = f.scoped().await;
        let invalidated = enrichment::invalidate_inputs(&mut tx, "atom", &deleted).await.unwrap();
        let mut expected_in_flight = vec![in_flight_receipt];
        expected_in_flight.sort();
        assert_eq!(invalidated, Invalidated { in_flight: expected_in_flight, published: vec![published_receipt] });
        assert!(enrichment::invalidate_inputs(&mut tx, "node", &deleted).await.unwrap() == Invalidated::default(),
            "the kind is part of an input's identity");
        tx.commit().await.unwrap();
        assert_eq!(input_outcomes(&f, in_flight_receipt).await, ["invalidated", "sent", "sent"]);
        assert_eq!(input_outcomes(&f, published_receipt).await, ["written"; 3], "published outcomes are history");
        let mut tx = f.scoped().await;
        assert!(matches!(
            enrichment::verify_inputs_locked(&mut tx, &in_flight, in_flight.inputs()).await.unwrap(),
            InputCheck::Stale
        ), "an invalidated input is stale even when the caller's re-read still matches");
        tx.commit().await.unwrap();
        assert_eq!(input_outcomes(&f, in_flight_receipt).await, ["invalidated", "discarded_stale", "discarded_stale"]);

        // A call that was never sent has no receipt to verify against.
        let unsent = new_work();
        let mut tx = f.scoped().await;
        assert_eq!(enrichment::verify_inputs_locked(&mut tx, &unsent, unsent.inputs()).await.unwrap_err(), Deny::Unavailable);
        tx.rollback().await.unwrap();

        // Input history: the binding is immutable, outcomes are terminal, no deletes.
        for (sql, message) in [
            ("UPDATE managed_enrichment_inputs SET outcome='sent' WHERE receipt_id=$1", "outcome is terminal"),
            ("UPDATE managed_enrichment_inputs SET content_revision=content_revision+1 WHERE receipt_id=$1", "binding is immutable"),
            ("DELETE FROM managed_enrichment_inputs WHERE receipt_id=$1", "cannot be deleted"),
            ("DELETE FROM managed_attempt_receipts WHERE receipt_id=$1", "source receipt history cannot be deleted"),
        ] {
            let error = sqlx::query(sql).bind(published_receipt).execute(f.owner()).await.unwrap_err();
            assert!(error.to_string().contains(message), "{sql}: {error}");
        }
        let error = sqlx::query("TRUNCATE managed_enrichment_inputs").execute(f.owner()).await.unwrap_err();
        assert!(error.to_string().contains("cannot be truncated"), "{error}");
        f.finish().await;
    }
}

#[tokio::test]
async fn enrichment_daily_cap_holds_visibly_without_a_receipt_both_planes() {
    let _env = Env::set(ON);
    for org in [false, true] {
        let f = funded(org).await;
        let req = completion_request();
        let plane = f.plane();
        let first = EnrichmentWork::new(f.account, f.graph, None, Some(f.payer), inputs()).unwrap();
        let bound = ManagedGate::open(f.enrichment(&plane, f.payer, &first), &req).await.unwrap().worst.per_attempt.get();
        // Room for one and a quarter attempts: the first call bills half its
        // bound, so the second reservation cannot fit.
        let cap = format!("{}", (bound * 5 / 4) as f64 / 1_000_000.0);
        let _cap = Env::set(&[("MANAGED_DAILY_CAP_USD_ENRICHMENT", Some(&cap))]);
        let receipt = sent_and_settled(&f, &first, &billed((bound / 2) as f64 / 1_000_000.0)).await;
        let second = EnrichmentWork::new(f.account, f.graph, Some(f.running_job().await), None, inputs()).unwrap();
        let gate = ManagedGate::open(f.enrichment(&plane, f.payer, &second), &req).await.unwrap();
        assert_eq!(
            coralswarm_core::send_gate::acquire(&gate, 1).await.unwrap_err(),
            Deny::Hold(HoldCode::DailyBudgetExhausted),
            "the shared enrichment cap holds the next consumer"
        );
        let (limit, reserved, charged, attempts): (i64, i64, i64, i64) = sqlx::query_as(
            "SELECT limit_micro_usd,reserved_micro_usd,charged_micro_usd,attempts FROM managed_budget_periods \
              WHERE account_id=$1 AND purpose='managed_enrichment'",
        ).bind(f.account).fetch_one(f.owner()).await.unwrap();
        assert_eq!((reserved, attempts), (0, 1));
        assert!((bound / 2..=bound / 2 + 1).contains(&charged), "charged {charged} of bound {bound}");
        assert!(limit >= bound && charged + bound > limit, "the visible period explains the hold: limit {limit}");
        assert_eq!(
            f.count("SELECT count(*) FROM managed_attempt_receipts").await,
            1,
            "a held reservation leaves no receipt"
        );
        let held_inputs: i64 = sqlx::query_scalar("SELECT count(*) FROM managed_enrichment_inputs WHERE receipt_id<>$1")
            .bind(receipt).fetch_one(f.owner()).await.unwrap();
        assert_eq!(held_inputs, 0, "and no inputs");
        assert_eq!(
            f.count("SELECT count(*) FROM managed_budget_periods WHERE purpose<>'managed_enrichment'").await,
            0,
            "the enrichment cap is its own period; extraction and embedding are untouched"
        );
        drop(gate);
        drop(plane);
        f.finish().await;
    }
}

#[tokio::test]
async fn enrichment_switch_is_off_by_default_and_fences_every_reserve() {
    let _env = Env::set(&[
        ("ENRICHMENT_PROCESSING_ENABLED", None),
        ("MANAGED_PROCESSING_DISABLED", None),
    ]);
    assert!(!enrichment::enabled() && !enrichment::claim_allowed(), "off when unset");
    for org in [false, true] {
        let f = funded(org).await;
        let req = completion_request();
        let plane = f.plane();
        let work = EnrichmentWork::new(f.account, f.graph, Some(f.running_job().await), None, inputs()).unwrap();
        for value in [None, Some("false"), Some("1"), Some("TRUE")] {
            let _switch = Env::set(&[("ENRICHMENT_PROCESSING_ENABLED", value)]);
            assert!(!enrichment::claim_allowed(), "{value:?}");
            assert_eq!(
                ManagedGate::open(f.enrichment(&plane, f.payer, &work), &req).await.err(),
                Some(Deny::Hold(HoldCode::ProcessingDisabled)),
                "{value:?} keeps enrichment off before any write"
            );
        }
        // Switched off between open and reserve: the locked fence holds.
        let gate = {
            let _on = Env::set(&[("ENRICHMENT_PROCESSING_ENABLED", Some("true"))]);
            assert!(enrichment::claim_allowed());
            ManagedGate::open(f.enrichment(&plane, f.payer, &work), &req).await.unwrap()
        };
        assert_eq!(
            coralswarm_core::send_gate::acquire(&gate, 1).await.unwrap_err(),
            Deny::Hold(HoldCode::ProcessingDisabled)
        );
        drop(gate);
        // The global kill switch still wins with the purpose switched on.
        let _on = Env::set(&[("ENRICHMENT_PROCESSING_ENABLED", Some("true")), ("MANAGED_PROCESSING_DISABLED", Some("true"))]);
        assert!(!enrichment::claim_allowed());
        let gate = ManagedGate::open(f.enrichment(&plane, f.payer, &work), &req).await.unwrap();
        assert_eq!(
            coralswarm_core::send_gate::acquire(&gate, 1).await.unwrap_err(),
            Deny::Hold(HoldCode::ProcessingDisabled)
        );
        drop(gate);
        for table in ["managed_attempt_receipts", "managed_budget_periods", "managed_enrichment_inputs"] {
            assert_eq!(f.count(&format!("SELECT count(*) FROM {table}")).await, 0, "{table} untouched while off");
        }
        drop(plane);
        f.finish().await;
    }
}

#[tokio::test]
async fn enrichment_refuses_and_fences_principal_and_job_but_has_no_version_cap_both_planes() {
    let _env = Env::set(ON);
    for org in [false, true] {
        let f = funded(org).await;
        let plane = f.plane();
        let req = completion_request();
        let job = f.running_job().await;

        // No unit, so no VersionCostCap: a per-version cap that would refuse a
        // window does not apply to a graph-level call.
        {
            let _tiny = Env::set(&[("MANAGED_PER_VERSION_CAP_USD", Some("0.000001"))]);
            let work = EnrichmentWork::new(f.account, f.graph, Some(job.clone()), None, inputs()).unwrap();
            sent_and_settled(&f, &work, &SendOutcome::NotGenerated).await;
        }
        // The per-attempt cap still refuses before any write.
        let work = EnrichmentWork::new(f.account, f.graph, Some(job.clone()), None, inputs()).unwrap();
        let mut huge = completion_request();
        huge.max_tokens = 1_000_000;
        assert_eq!(
            ManagedGate::open(f.enrichment(&plane, f.payer, &work), &huge).await.err(),
            Some(Deny::Refuse(RefuseCode::CallCostCap))
        );
        let gate = ManagedGate::open(f.enrichment(&plane, f.payer, &work), &req).await.unwrap();
        assert_eq!(gate.reserve_attempt(enrichment::MAX_ATTEMPTS + 1).await.unwrap_err(), Deny::Unavailable);
        drop(gate);

        // Principal: an explicit attributed user must be a current member who
        // reaches the graph; the call must charge exactly that user.
        let stranger = Uuid::new_v4();
        sqlx::query("INSERT INTO users(id,email,clerk_user_id) VALUES($1,$2,$2)")
            .bind(stranger).bind(format!("{stranger}@enrichment.test")).execute(&f.control.owner).await.unwrap();
        let journal = |user| EnrichmentWork::new(f.account, f.graph, None, Some(user), inputs()).unwrap();
        assert_eq!(
            enrichment::select_principal(&plane, &f.control.app, &f.tenant, &journal(stranger)).await,
            Err(Deny::Hold(HoldCode::RequesterAccessLost))
        );
        assert_eq!(enrichment::select_principal(&plane, &f.control.app, &f.tenant, &journal(f.payer)).await, Ok(f.payer));
        let pinned = journal(f.payer);
        assert_eq!(
            ManagedGate::open(f.enrichment(&plane, stranger, &pinned), &req).await.err(),
            Some(Deny::Unavailable),
            "an attributed user cannot be swapped for another payer"
        );
        // Executor work charged to anyone but the selected executor is fenced.
        let gate = ManagedGate::open(f.enrichment(&plane, stranger, &work), &req).await.unwrap();
        assert_eq!(coralswarm_core::send_gate::acquire(&gate, 1).await.unwrap_err(), Deny::Hold(HoldCode::RequesterAccessLost));
        drop(gate);

        // The job fence: a job no longer running cannot reserve.
        let mut content = f.scoped().await;
        sqlx::query("UPDATE jobs SET status='done',finished_at=now() WHERE id=$1").bind(job.id).execute(&mut *content).await.unwrap();
        content.commit().await.unwrap();
        let gate = ManagedGate::open(f.enrichment(&plane, f.payer, &work), &req).await.unwrap();
        assert_eq!(coralswarm_core::send_gate::acquire(&gate, 1).await.unwrap_err(), Deny::Hold(HoldCode::Fenced));
        drop(gate);
        assert_eq!(
            f.count("SELECT count(*) FROM managed_attempt_receipts WHERE dispatch_phase='armed'").await,
            1,
            "only the uncapped call was armed"
        );
        drop(plane);
        f.finish().await;
    }
}

/// The production tenant runtime connects as a Neon-shaped table owner
/// (NOSUPERUSER, BYPASSRLS) that inherits coralswarm_tenant_app (tenant 0159);
/// the control runtime is a coralswarm_app member. A superuser fixture would
/// skip every privilege check, so the tenant tree is migrated by that owner.
#[tokio::test]
async fn enrichment_inputs_work_for_the_owner_runtime_and_app_roles() {
    let _env = Env::set(ON);
    let url = std::env::var("ORGT_TEST_BASE_URL").expect("isolated localhost ORGT_TEST_BASE_URL required");
    let options = sqlx::postgres::PgConnectOptions::from_str(&url).unwrap();
    assert!(matches!(options.get_host(), "localhost" | "127.0.0.1" | "::1"));
    let admin = PgPoolOptions::new().max_connections(2).connect_with(options.clone()).await.unwrap();
    sqlx::raw_sql("DO $$ BEGIN IF NOT EXISTS(SELECT 1 FROM pg_roles WHERE rolname='coralswarm_app') THEN CREATE ROLE coralswarm_app NOLOGIN NOSUPERUSER NOBYPASSRLS; END IF; IF NOT EXISTS(SELECT 1 FROM pg_roles WHERE rolname='coralswarm_tenant_app') THEN CREATE ROLE coralswarm_tenant_app NOLOGIN NOSUPERUSER NOBYPASSRLS; END IF; END $$;")
        .execute(&admin).await.unwrap();
    let db = crate::test_db::TestDatabase::new(&options, "cor22_enrich");
    let (role, password) = (db.role("cor22_enrich_owner"), Uuid::new_v4().simple().to_string());
    sqlx::query(&format!("CREATE ROLE {role} LOGIN NOSUPERUSER CREATEDB CREATEROLE BYPASSRLS NOREPLICATION PASSWORD '{password}'"))
        .execute(&admin).await.unwrap();
    sqlx::query(&format!("CREATE DATABASE {db} OWNER {role}")).execute(&admin).await.unwrap();
    let superuser_in_db = PgPoolOptions::new().max_connections(1).connect_with(options.clone().database(&db)).await.unwrap();
    sqlx::raw_sql("CREATE EXTENSION IF NOT EXISTS pg_trgm WITH SCHEMA public; CREATE EXTENSION IF NOT EXISTS vector WITH SCHEMA public;")
        .execute(&superuser_in_db).await.unwrap();
    superuser_in_db.close().await;
    sqlx::raw_sql(&format!("DO $$ BEGIN GRANT coralswarm_tenant_app TO {role} WITH ADMIN TRUE, INHERIT FALSE, SET FALSE; IF EXISTS(SELECT 1 FROM pg_roles WHERE rolname='coralswarm_managed_clock') THEN GRANT coralswarm_managed_clock TO {role} WITH ADMIN TRUE, INHERIT FALSE, SET FALSE; END IF; END $$;"))
        .execute(&admin).await.unwrap();
    let owner = PgPoolOptions::new().max_connections(2)
        .connect_with(options.clone().database(&db).username(&role).password(&password)).await.unwrap();
    sqlx::migrate!("./migrations_tenant").run(&owner).await.unwrap();
    let shape: (bool, bool, bool, bool) = sqlx::query_as(
        "SELECT r.rolsuper,r.rolbypassrls,pg_get_userbyid(c.relowner)=current_user,pg_has_role(current_user,'coralswarm_tenant_app','USAGE') \
           FROM pg_roles r,pg_class c WHERE r.rolname=current_user AND c.oid='public.managed_enrichment_inputs'::regclass",
    ).fetch_one(&owner).await.unwrap();
    assert_eq!(shape, (false, true, true, true), "non-superuser owner runtime owns the table and inherits the app role");
    for (grantee, privilege, expected) in [
        ("coralswarm_tenant_app", "SELECT,INSERT,UPDATE", true),
        // Tenant default privileges (0009) grant arwd on every table; the
        // history trigger is what refuses a delete there.
        ("coralswarm_tenant_app", "DELETE", true),
        ("coralswarm_app", "SELECT,INSERT,UPDATE", true),
        ("coralswarm_app", "DELETE", false),
    ] {
        let granted: bool = sqlx::query_scalar("SELECT has_table_privilege($1,'public.managed_enrichment_inputs',$2)")
            .bind(grantee).bind(privilege).fetch_one(&owner).await.unwrap();
        assert_eq!(granted, expected, "{grantee} {privilege}");
    }

    // The gate's own statements, run as the owner runtime in system scope.
    let account = Uuid::new_v4();
    let payer = Uuid::new_v4();
    let work = EnrichmentWork::new(account, Uuid::new_v4(), None, Some(payer), inputs()).unwrap();
    let mut tx = crate::tenancy::scope::begin_tenant_system_scoped_without_account(&owner).await.unwrap();
    sqlx::query("INSERT INTO managed_budget_periods(account_id,purpose,period_start,limit_micro_usd) VALUES($1,'managed_enrichment',(clock_timestamp() AT TIME ZONE 'UTC')::date,2000000)")
        .bind(account).execute(&mut *tx).await.unwrap();
    let receipt: Uuid = sqlx::query_scalar(
        "INSERT INTO managed_attempt_receipts(account_id,purpose,period_start,graph_id,attributed_user_id,logical_call,attempt_no,candidate_models,input_bound_tokens,max_tokens,reserved_micro_usd,protocol_version,dispatch_phase,content_plane,request_hash) \
         VALUES($1,'managed_enrichment',(clock_timestamp() AT TIME ZONE 'UTC')::date,$2,$3,$4,1,ARRAY['m'],1,1,100,1,'prepared','org:owner',$5) RETURNING receipt_id",
    ).bind(account).bind(work.graph_id()).bind(payer).bind(work.logical_call()).bind("b".repeat(64))
        .fetch_one(&mut *tx).await.unwrap();
    sqlx::query("UPDATE managed_budget_periods SET reserved_micro_usd=100,attempts=1 WHERE account_id=$1").bind(account).execute(&mut *tx).await.unwrap();
    work.insert_inputs(&mut tx, receipt).await.unwrap();
    assert!(sqlx::query_scalar::<_, bool>("SELECT managed_arm_v1($1)").bind(receipt).fetch_one(&mut *tx).await.unwrap());
    assert!(sqlx::query_scalar::<_, bool>("SELECT managed_settle_v1($1,'billed',40,'m',NULL,NULL)").bind(receipt).fetch_one(&mut *tx).await.unwrap());
    let InputCheck::Fresh(verified) = enrichment::verify_inputs_locked(&mut tx, &work, work.inputs()).await.unwrap() else {
        panic!("fresh");
    };
    verified.mark_written(&mut tx).await.unwrap();
    let invalidated = enrichment::invalidate_inputs(&mut tx, "node", &[work.inputs()[1].id]).await.unwrap();
    assert_eq!(invalidated.published, vec![receipt]);
    tx.commit().await.unwrap();
    let error = sqlx::query("INSERT INTO managed_attempt_receipts(account_id,purpose,period_start,graph_id,logical_call,attempt_no,candidate_models,input_bound_tokens,max_tokens,reserved_micro_usd) VALUES($1,'managed_enrichment',(clock_timestamp() AT TIME ZONE 'UTC')::date,$2,gen_random_uuid(),1,ARRAY['m'],1,1,1)")
        .bind(account).bind(work.graph_id()).execute(&owner).await.unwrap_err();
    assert!(error.to_string().contains("managed enrichment requires funded dispatch generation"), "{error}");
    let error = sqlx::query("DELETE FROM managed_enrichment_inputs WHERE receipt_id=$1").bind(receipt).execute(&owner).await.unwrap_err();
    assert!(error.to_string().contains("cannot be deleted"), "the owner's RLS bypass does not bypass history: {error}");
    let error = sqlx::query("INSERT INTO managed_budget_periods(account_id,purpose,period_start,limit_micro_usd) VALUES($1,'graph_magic',current_date,1)")
        .bind(account).execute(&owner).await.unwrap_err();
    assert!(error.to_string().contains("managed_budget_periods_purpose_check"), "{error}");
    owner.close().await;
    admin.close().await;
    eprintln!("retained owner-runtime enrichment fixture database={db} role={role}");
}

/// No double billing: an enrichment receipt is debited once (at settle, and
/// never again by later derivation passes), and the job axis never bills a
/// graph-level enrichment kind. `ExtractEntities` is the positive control that
/// proves the job axis ran.
#[tokio::test]
async fn enrichment_receipts_bill_once_and_enrichment_kinds_never_on_the_job_axis_both_planes() {
    use crate::workers::billing_derivation::{self, JOB_AXIS_KINDS};
    use crate::workers::tenant_harness::WorkerTenant;
    let _env = Env::set(ON);
    for org in [false, true] {
        let f = funded(org).await;
        let job = f.running_job().await;
        let work = EnrichmentWork::new(f.account, f.graph, Some(job), None, inputs()).unwrap();
        let receipt = sent_and_settled(&f, &work, &billed(0.000044)).await;
        let mut content = f.scoped().await;
        let candidates = ["EntityResolution", "RegenerateDigest", "BackfillAtomTitles", "GenerateMeetingSummary"];
        let mut enrichment_jobs = Vec::new();
        for kind in candidates.iter().chain(["ExtractEntities"].iter()) {
            assert_eq!(JOB_AXIS_KINDS.contains(kind), *kind == "ExtractEntities");
            let id: Uuid = sqlx::query_scalar(
                "INSERT INTO jobs(kind,payload,account_id,status,attempts,finished_at) \
                 VALUES($1,'{}'::jsonb,$2,'done',1,now()-interval '1 hour') RETURNING id",
            ).bind(kind).bind(f.account).fetch_one(&mut *content).await.unwrap();
            sqlx::query(
                "INSERT INTO llm_usage(account_id,graph_id,provider,task,model,input_tokens,output_tokens,source_job_id,created_at) \
                 VALUES($1,$2,'openrouter','enrichment_fixture','deepseek/deepseek-v4-flash-0731',100000,100000,$3,now()-interval '1 hour')",
            ).bind(f.account).bind(f.graph).bind(id).execute(&mut *content).await.unwrap();
            if *kind != "ExtractEntities" {
                enrichment_jobs.push(id);
            }
        }
        content.commit().await.unwrap();
        let tenant = WorkerTenant {
            tenant: f.tenant.clone(),
            pool: f.app().clone(),
            store: std::sync::Arc::new(crate::store::PostgresStore::new(f.app().clone())),
        };
        for _ in 0..2 {
            billing_derivation::tick(&tenant, &f.control.app).await.unwrap();
        }
        let receipt_debits: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM credit_ledger WHERE account_id=$1 AND source_receipt_id=$2",
        ).bind(f.account).bind(receipt).fetch_one(&f.control.owner).await.unwrap();
        assert_eq!(receipt_debits, 1, "one debit per enrichment receipt across settle and two derivation passes");
        let job_debits: Vec<(String, Option<Uuid>)> = sqlx::query_as(
            "SELECT entry_kind,source_job_id FROM credit_ledger WHERE account_id=$1 AND entry_kind<>'usage_managed' ORDER BY entry_kind",
        ).bind(f.account).fetch_all(&f.control.owner).await.unwrap();
        assert_eq!(job_debits.len(), 1, "only the job-axis positive control is billed: {job_debits:?}");
        assert_eq!(job_debits[0].0, "usage_job");
        assert!(job_debits[0].1.is_some_and(|id| !enrichment_jobs.contains(&id)));
        f.finish().await;
    }
}

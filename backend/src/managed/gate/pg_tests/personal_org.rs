//! COR-114: one liability-plane resolver for managed admission and billing,
//! and the shared retry rule for held jobs.
//!
//! A personal org is a consumer-plane account WITH a `clerk_org_id`
//! (`tenancy::gate::TokenKind::PersonalOrg`). Every managed admission check
//! accepted the consumer plane only when that column was NULL, and billing
//! reconciliation refused the same shape, so prod account 78d0d2ba was held
//! `requester_access_lost` for every purpose and its digests re-enqueued
//! every 15 minutes, forever. The embedding purpose's end-to-end run is in
//! `embedding_workers` (`c23_personal_org_real_embed_worker_...`).
use super::admission_latency::{billed, open_uncapped, worker};
use super::*;
use crate::jobs::queue::{self, Held};
use crate::jobs::JobKind;
use crate::managed::enrichment::{self, EnrichmentInput, EnrichmentWork};
use crate::managed::interactive_ask::AskWork;
use crate::managed::plane::{resolve_liability_plane, LiabilityPlane, PlaneError};
use crate::workers::billing_derivation::{self, LegacyContent};
use std::ffi::OsString;
use std::sync::{atomic::AtomicBool, Arc};

/// Process env for one test; restored on drop (the suite runs with
/// `--test-threads=1`).
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
    ("MANAGED_DAILY_CAP_USD_INTERACTIVE_ASK", None),
    ("MANAGED_PER_ATTEMPT_CAP_USD", None),
];

async fn funded(f: Fixture) -> Fixture {
    sqlx::query("INSERT INTO credit_conversion(effective_from,reference_cost_per_credit,note) VALUES(now()-interval '1 day',1,'personal org fixture')")
        .execute(&f.control.owner).await.unwrap();
    f.balance(Decimal::from(1_000_000)).await;
    f
}

/// The resolver as the app role reads it, in an account-scoped transaction.
async fn resolve(f: &Fixture, account: Uuid) -> Result<LiabilityPlane, PlaneError> {
    let mut tx = begin_account_scoped(&f.control.app, account).await.unwrap();
    let plane = resolve_liability_plane(&mut tx, account, true).await;
    tx.rollback().await.unwrap();
    plane
}

/// One admitted attempt through `gate`, settled billed; its receipt.
async fn send(gate: &ManagedGate<'_>) -> Uuid {
    let permit = coralswarm_core::send_gate::acquire(gate, 1)
        .await
        .unwrap_or_else(|deny| panic!("must be admitted, got {deny:?}"));
    let sent = permit.into_sent();
    let receipt = sent.receipt_id().unwrap();
    gate.settle(sent, &billed()).await;
    receipt
}

fn work(f: &Fixture, attributed: Option<Uuid>) -> EnrichmentWork {
    EnrichmentWork::new(f.account, f.graph, None, attributed, vec![EnrichmentInput::new("atom", Uuid::new_v4(), 1, b"coral")]).unwrap()
}

fn enrichment_call<'a>(f: &'a Fixture, plane: &'a JobScope<'a>, principal: Uuid, work: &'a EnrichmentWork) -> ManagedCall<'a> {
    ManagedCall {
        plane,
        control: &f.control.app,
        tenant: &f.tenant,
        account_id: f.account,
        principal,
        fallback_model: None,
        subject: Subject::Enrichment { work },
    }
}

fn ask_call<'a>(f: &'a Fixture, plane: &'a JobScope<'a>, account: Uuid, work: &'a AskWork) -> ManagedCall<'a> {
    ManagedCall {
        plane,
        control: &f.control.app,
        tenant: &f.tenant,
        account_id: account,
        principal: f.payer,
        fallback_model: None,
        subject: Subject::InteractiveAsk { work },
    }
}

fn ask(f: &Fixture, account: Uuid) -> AskWork {
    AskWork::new(account, f.payer, f.graph, Uuid::new_v4(), Some("pack"), Arc::new(AtomicBool::new(false)))
}

async fn opened(call: ManagedCall<'_>) -> Result<ManagedGate<'_>, Deny> {
    ManagedGate::open(call, &completion_request()).await
}

async fn receipt_count(f: &Fixture, account: Uuid) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM managed_attempt_receipts WHERE account_id=$1")
        .bind(account).fetch_one(f.owner()).await.unwrap()
}

/// Source extraction, enrichment (executor and attributed member) and
/// interactive Ask, through the real funded gate, then the billing pass.
#[tokio::test]
async fn a_personal_org_is_admitted_for_every_managed_purpose_and_its_receipts_reconcile() {
    let _env = Env::set(ON);
    let f = funded(Fixture::personal_org().await).await;
    assert_eq!(f.tenant, TenantId::Consumer, "a personal org's work runs on the consumer plane");
    assert_eq!(resolve(&f, f.account).await.unwrap(), LiabilityPlane::Consumer);
    let plane = f.plane();
    let mut receipts = Vec::new();
    receipts.push(send(&open_uncapped(f.call(&plane)).await).await);
    for attributed in [None, Some(f.payer)] {
        let work = work(&f, attributed);
        let principal = enrichment::select_principal(&plane, &f.control.app, &f.tenant, &work)
            .await
            .unwrap_or_else(|deny| panic!("principal for {attributed:?}: {deny:?}"));
        assert_eq!(principal, f.payer);
        receipts.push(send(&opened(enrichment_call(&f, &plane, principal, &work)).await.unwrap()).await);
    }
    let asked = ask(&f, f.account);
    receipts.push(send(&opened(ask_call(&f, &plane, f.account, &asked)).await.unwrap()).await);

    let planes: Vec<(String, String)> = sqlx::query_as(
        "SELECT purpose,content_plane FROM managed_attempt_receipts WHERE receipt_id=ANY($1) ORDER BY purpose",
    )
    .bind(&receipts).fetch_all(f.owner()).await.unwrap();
    assert_eq!(planes.len(), 4);
    assert!(planes.iter().all(|(_, plane)| plane == "consumer"), "{planes:?}");
    let purposes: std::collections::BTreeSet<_> = planes.iter().map(|(purpose, _)| purpose.as_str()).collect();
    assert_eq!(purposes, ["interactive_ask", "managed_enrichment", "source_extraction"].into_iter().collect());

    // The billing pass reconciles the account on control (it refused a
    // personal org's receipts there), and every receipt is debited once.
    billing_derivation::tick(&worker(f.app(), &f.tenant), &f.control.app)
        .await
        .expect("billing reconciles a personal org on the consumer plane");
    for receipt in &receipts {
        let debits: i64 = sqlx::query_scalar("SELECT count(*) FROM credit_ledger WHERE source_receipt_id=$1")
            .bind(receipt).fetch_one(&f.control.owner).await.unwrap();
        assert_eq!(debits, 1, "receipt {receipt} is debited exactly once");
    }
    let queued: i64 = sqlx::query_scalar("SELECT count(*) FROM managed_receipt_recovery_queue WHERE account_id=$1")
        .bind(f.account).fetch_one(f.owner()).await.unwrap();
    assert_eq!(queued, 0, "every terminal receipt confirmed");
    let mut financial = f.financial().await;
    billing_derivation::reconcile_legacy_managed_tx(&mut financial, f.account, LegacyContent::Control)
        .await
        .expect("billing reads the same resolver: a personal org is Control");
    financial.rollback().await.unwrap();

    // Nor is it admitted on its own org id's plane: it has no tenant database.
    let org: String = sqlx::query_scalar("SELECT clerk_org_id FROM accounts WHERE id=$1")
        .bind(f.account).fetch_one(&f.control.owner).await.unwrap();
    let mut tx = begin_account_scoped(&f.control.app, f.account).await.unwrap();
    assert_eq!(
        crate::managed::PlanePermit::admit(&mut tx, f.account, &TenantId::Org(org)).await.unwrap_err(),
        Deny::Hold(HoldCode::RequesterAccessLost)
    );
    tx.rollback().await.unwrap();
    drop(plane);
    f.finish().await;
}

#[tokio::test]
async fn an_org_account_still_resolves_to_its_tenant_plane() {
    let f = funded(Fixture::new(true).await).await;
    let TenantId::Org(org) = f.tenant.clone() else { unreachable!() };
    assert_eq!(resolve(&f, f.account).await.unwrap(), LiabilityPlane::Org(org.clone()));
    let mut tx = begin_account_scoped(&f.control.app, f.account).await.unwrap();
    assert_eq!(
        crate::managed::PlanePermit::admit(&mut tx, f.account, &TenantId::Consumer).await.unwrap_err(),
        Deny::Hold(HoldCode::RequesterAccessLost),
        "an org account is never admitted on the consumer plane"
    );
    let permit = crate::managed::PlanePermit::admit(&mut tx, f.account, &f.tenant).await.unwrap();
    assert_eq!(permit.label(), format!("org:{org}"));
    tx.rollback().await.unwrap();
    let plane = f.plane();
    let receipt = send(&open_uncapped(f.call(&plane)).await).await;
    let content_plane: String = sqlx::query_scalar("SELECT content_plane FROM managed_attempt_receipts WHERE receipt_id=$1")
        .bind(receipt).fetch_one(f.owner()).await.unwrap();
    assert_eq!(content_plane, format!("org:{org}"));
    billing_derivation::tick(&worker(f.app(), &f.tenant), &f.control.app).await.expect("the org pass reconciles");
    let debits: i64 = sqlx::query_scalar("SELECT count(*) FROM credit_ledger WHERE source_receipt_id=$1")
        .bind(receipt).fetch_one(&f.control.owner).await.unwrap();
    assert_eq!(debits, 1);
    drop(plane);
    f.finish().await;
}

#[tokio::test]
async fn an_ambiguous_account_is_refused_explicitly_by_admission_and_billing() {
    let _env = Env::set(ON);
    let f = funded(Fixture::personal_org().await).await;
    let plane = f.plane();

    // A personal org that ALSO has a tenant database: two planes claim it.
    let org: String = sqlx::query_scalar("SELECT clerk_org_id FROM accounts WHERE id=$1")
        .bind(f.account).fetch_one(&f.control.owner).await.unwrap();
    let mut tx = f.financial().await;
    sqlx::query("INSERT INTO tenant_databases(clerk_org_id,account_id,slug,db_secret_ref,status,schema_version) VALUES($1,$2,$3,$4,'active',153)")
        .bind(&org).bind(f.account).bind(format!("amb-{}", &f.account.simple().to_string()[..20]))
        .bind(format!("TENANT_AMB_{}", f.account.simple())).execute(&mut *tx).await.unwrap();
    tx.commit().await.unwrap();
    assert!(matches!(resolve(&f, f.account).await, Err(PlaneError::Unresolvable(_))));
    let asked = ask(&f, f.account);
    let Err(refused) = opened(ask_call(&f, &plane, f.account, &asked)).await else { panic!("an ambiguous account must not be admitted") };
    assert_eq!(refused, Deny::Hold(HoldCode::PrincipalUnavailable), "refused, never guessed onto a plane");
    let work = work(&f, None);
    assert_eq!(
        enrichment::select_principal(&plane, &f.control.app, &f.tenant, &work).await.unwrap_err(),
        Deny::Hold(HoldCode::PrincipalUnavailable),
        "principal selection reads the same resolver"
    );
    let mut financial = f.financial().await;
    assert!(
        billing_derivation::reconcile_legacy_managed_tx(&mut financial, f.account, LegacyContent::Control).await.is_err(),
        "billing refuses the account admission refuses"
    );
    financial.rollback().await.unwrap();
    assert_eq!(receipt_count(&f, f.account).await, 0, "nothing was receipted");

    // An org account with no tenant database (prod 8494ebe1's shape).
    let orphan = Uuid::new_v4();
    sqlx::query("INSERT INTO accounts(id,owner_user_id,clerk_org_id) VALUES($1,$2,$3)")
        .bind(orphan).bind(f.payer).bind(format!("org_orphan_{}", orphan.simple()))
        .execute(&f.control.owner).await.unwrap();
    assert!(matches!(resolve(&f, orphan).await, Err(PlaneError::Unresolvable(_))));
    let asked = ask(&f, orphan);
    let Err(refused) = opened(ask_call(&f, &plane, orphan, &asked)).await else { panic!("an org with no tenant database must not be admitted") };
    assert_eq!(refused, Deny::Hold(HoldCode::PrincipalUnavailable));
    assert_eq!(receipt_count(&f, orphan).await, 0);

    // An account that no longer exists: access lost, as before.
    assert!(matches!(resolve(&f, Uuid::new_v4()).await, Err(PlaneError::UnknownAccount)));
    drop(plane);
    f.finish().await;
}

// ── The shared retry rule (jobs::queue::hold) ────────────────────────────────

async fn digest_job(f: &Fixture, node: Uuid) -> Uuid {
    queue::enqueue(f.app(), &JobKind::RegenerateDigest { node_id: node, graph_id: f.graph }, Some(f.account), Some(&format!("digest:{node}")), 0)
        .await.unwrap().expect("a fresh digest job")
}

/// Make `id` due and claim it, as the worker would.
async fn claim(f: &Fixture, id: Uuid) {
    sqlx::query("UPDATE jobs SET run_after=now() WHERE id=$1").bind(id).execute(f.owner()).await.unwrap();
    let claimed = queue::dequeue_ids(f.app(), &["RegenerateDigest"], &[id]).await.unwrap();
    assert_eq!(claimed.len(), 1, "job {id} claimed");
}

/// (status, hold_streak, attempts, seconds until due, error, failure_class, key)
async fn row(f: &Fixture, id: Uuid) -> (String, i32, i32, i64, Option<String>, Option<String>, Option<String>) {
    sqlx::query_as(
        "SELECT status::text,hold_streak,attempts,extract(epoch FROM run_after-now())::bigint,error_message,failure_class,idempotency_key \
           FROM jobs WHERE id=$1",
    )
    .bind(id).fetch_one(f.owner()).await.unwrap()
}

async fn digest_rows(f: &Fixture, node: Uuid) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM jobs WHERE kind='RegenerateDigest' AND payload->>'node_id'=$1::text")
        .bind(node).fetch_one(f.owner()).await.unwrap()
}

/// The 78d0d2ba loop: a reef held `requester_access_lost` wrote a fresh row
/// every 15 minutes, forever. It now stays one row, waits 1 h, 4 h, 16 h,
/// and dead-letters on the fourth hold in a row.
#[tokio::test]
async fn a_hold_that_cannot_clear_backs_off_in_place_then_dead_letters() {
    let f = Fixture::new(false).await;
    let node = Uuid::new_v4();
    let id = digest_job(&f, node).await;
    for (streak, expected) in [(1, Held::Pending(3600)), (2, Held::Pending(4 * 3600)), (3, Held::Pending(16 * 3600))] {
        claim(&f, id).await;
        assert_eq!(queue::hold(f.app(), id, "requester_access_lost").await.unwrap(), expected);
        let (status, held, attempts, due, error, class, key) = row(&f, id).await;
        let Held::Pending(secs) = expected else { unreachable!() };
        assert_eq!((status.as_str(), held, attempts), ("pending", streak, 0), "hold {streak}: back in place, no attempt charged");
        assert!((secs - 5..=secs).contains(&due), "hold {streak}: due in {due}s, expected {secs}s");
        assert_eq!(error.as_deref(), Some("held: requester_access_lost"));
        assert_eq!((class, key), (None, Some(format!("digest:{node}"))), "keeps its dedup key");
        assert_eq!(digest_rows(&f, node).await, 1, "hold {streak}: no fresh row");
    }
    claim(&f, id).await;
    assert_eq!(queue::hold(f.app(), id, "requester_access_lost").await.unwrap(), Held::DeadLettered);
    let (status, held, attempts, _, error, class, key) = row(&f, id).await;
    assert_eq!((status.as_str(), held, attempts), ("dead", 4, 0));
    assert_eq!(error.as_deref(), Some("held: requester_access_lost"));
    assert_eq!(class, None, "not `structural`: the chain-health structural invariant must not count it");
    assert_eq!(key, None);
    assert_eq!(digest_rows(&f, node).await, 1);
    assert!(digest_job(&f, node).await != id, "the next coral change can queue the reef again");
    f.finish().await;
}

#[tokio::test]
async fn a_hold_that_can_clear_backs_off_to_a_cap_and_never_dead_letters() {
    let f = Fixture::new(false).await;
    let node = Uuid::new_v4();
    let id = digest_job(&f, node).await;
    for (streak, secs) in [900, 1800, 3600, 7200, 14400, 21600, 21600, 21600].into_iter().enumerate() {
        claim(&f, id).await;
        assert_eq!(queue::hold(f.app(), id, "credits_exhausted").await.unwrap(), Held::Pending(secs));
        let (status, held, _, due, ..) = row(&f, id).await;
        assert_eq!((status.as_str(), held), ("pending", streak as i32 + 1));
        assert!((secs - 5..=secs).contains(&due), "hold {streak}: due in {due}s, expected {secs}s");
    }
    assert_eq!(digest_rows(&f, node).await, 1);
    // A known clear time (the next UTC day) leaves the streak alone.
    claim(&f, id).await;
    assert_eq!(queue::hold_until(f.app(), id, "daily_bound", 600).await.unwrap(), Held::Pending(600));
    let (status, held, _, due, ..) = row(&f, id).await;
    assert_eq!((status.as_str(), held), ("pending", 8));
    assert!((595..=600).contains(&due));
    f.finish().await;
}

/// A coral change while the reef ran queued a newer pass on the same key:
/// the held one yields to it instead of colliding with the dedup index.
#[tokio::test]
async fn a_held_job_yields_to_a_newer_pending_pass_on_its_key() {
    let f = Fixture::new(false).await;
    let node = Uuid::new_v4();
    let id = digest_job(&f, node).await;
    claim(&f, id).await;
    let newer = digest_job(&f, node).await;
    assert_eq!(queue::hold(f.app(), id, "credits_exhausted").await.unwrap(), Held::Superseded);
    let (status, ..) = row(&f, id).await;
    assert_eq!(status, "done");
    let (status, held, ..) = row(&f, newer).await;
    assert_eq!((status.as_str(), held), ("pending", 0));
    // A job no longer running (reaped) is left alone.
    assert_eq!(queue::hold(f.app(), id, "credits_exhausted").await.unwrap(), Held::NotRunning);
    f.finish().await;
}

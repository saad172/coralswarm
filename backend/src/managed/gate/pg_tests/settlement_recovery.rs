//! Invariants around the funded org admission's phase-2 -> phase-3 window and
//! the billing worker's managed recovery queue (control 0345 / tenant 0177).
//!
//! The org path commits the receipt (phase 1) and the control reservation
//! (phase 2) before it re-checks funding and arms (phase 3). Anything that
//! happens to the receipt or the reservation in between must leave no armed
//! receipt and no ledger debit. The recovery pass reads only receipts whose
//! terminal fact control has not confirmed yet, so its cost follows what
//! changed, not settled history; these tests pin that it still settles late
//! and out-of-order receipts, re-confirms after a lost drain, refuses a
//! conflicting fact on every pass, and refuses a plane without the trigger.
use super::admission_latency::{
    billed, funded_org, open_uncapped, settle_calls, settled_history, upstream, worker, Wire,
};
use super::*;
use crate::workers::billing_derivation;
use std::sync::Arc;

async fn funded_consumer() -> Fixture {
    let f = Fixture::new(false).await;
    sqlx::query("INSERT INTO credit_conversion(effective_from,reference_cost_per_credit,note) VALUES(now()-interval '1 day',1,'settlement recovery fixture')")
        .execute(&f.control.owner).await.unwrap();
    f.balance(Decimal::from(1_000_000)).await;
    f
}

/// The tenant runtime under test: the restricted app role, or the
/// Neon-shaped table owner production connects as.
async fn runtime(f: &Fixture, owner: bool) -> PgPool {
    if owner {
        super::entity_resolution::owner_runtime(f).await
    } else {
        f.app().clone()
    }
}

fn plane_of<'a>(f: &'a Fixture, pool: &'a PgPool) -> JobScope<'a> {
    match f.tenant {
        TenantId::Consumer => JobScope::Consumer { pool, account_id: f.account },
        TenantId::Org(_) => JobScope::Org(pool),
    }
}

async fn receipt_phase(f: &Fixture, receipt: Uuid) -> (String, String) {
    sqlx::query_as("SELECT dispatch_phase,state FROM managed_attempt_receipts WHERE receipt_id=$1")
        .bind(receipt).fetch_one(f.owner()).await.unwrap()
}

async fn reservation(f: &Fixture, receipt: Uuid) -> Option<(String, Option<i64>, String)> {
    sqlx::query_as("SELECT state,settled_micro_usd,request_hash FROM managed_credit_reservations WHERE receipt_id=$1")
        .bind(receipt).fetch_optional(&f.control.owner).await.unwrap()
}

async fn ledger_rows(f: &Fixture, receipt: Uuid) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM credit_ledger WHERE source_receipt_id=$1")
        .bind(receipt).fetch_one(&f.control.owner).await.unwrap()
}

async fn account_ledger_rows(f: &Fixture) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM credit_ledger WHERE account_id=$1 AND source_receipt_id IS NOT NULL")
        .bind(f.account).fetch_one(&f.control.owner).await.unwrap()
}

async fn queued(f: &Fixture) -> Vec<Uuid> {
    sqlx::query_scalar("SELECT receipt_id FROM managed_receipt_recovery_queue WHERE account_id=$1 ORDER BY receipt_id")
        .bind(f.account).fetch_all(f.owner()).await.unwrap()
}

async fn armed_receipts(f: &Fixture) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM managed_attempt_receipts WHERE account_id=$1 AND dispatch_phase='armed'")
        .bind(f.account).fetch_one(f.owner()).await.unwrap()
}

/// The receipt of the one admission on `logical_call`.
async fn receipt_of(f: &Fixture, logical_call: Uuid) -> Uuid {
    sqlx::query_scalar("SELECT receipt_id FROM managed_attempt_receipts WHERE logical_call=$1")
        .bind(logical_call).fetch_one(f.owner()).await.unwrap()
}

/// Settles a receipt on its content plane only, the way a live settle that
/// lost its control half (or a reaper) leaves it: control still `held`.
async fn settle_content_only(plane: &JobScope<'_>, receipt: Uuid, cost: i64) {
    let mut content = begin_plane(plane).await.unwrap();
    assert!(sqlx::query_scalar::<_, bool>("SELECT managed_settle_v1($1,'billed',$2,'deepseek/deepseek-v4-flash-0731',NULL,NULL)")
        .bind(receipt).bind(cost).fetch_one(&mut *content).await.unwrap());
    content.commit().await.unwrap();
}

/// Runs `acquire` with `hook` installed in the phase-2 -> phase-3 window.
async fn acquire_with_hook(
    gate: &ManagedGate<'_>,
    hook: impl Fn(Uuid) -> futures::future::BoxFuture<'static, ()> + Send + Sync + 'static,
) -> Result<coralswarm_core::send_gate::SendPermit, Deny> {
    let hook: funded::PhaseHook = Arc::new(hook);
    funded::BETWEEN_FUND_AND_ARM
        .scope(hook, coralswarm_core::send_gate::acquire(gate, 1))
        .await
}

async fn financial_owner(control_owner: &PgPool, account: Uuid) -> Transaction<'static, Postgres> {
    let mut tx = begin_account_scoped(control_owner, account).await.unwrap();
    crate::billing::renewal::lock_period_rotation(&mut tx, account).await.unwrap();
    tx
}

#[derive(Clone, Copy, Debug)]
enum Window {
    /// Another actor (`recover_logical_call`) cancels the receipt and
    /// releases its reservation.
    Released,
    /// The receipt is cancelled on the content plane; control still holds.
    Cancelled,
    /// The reservation disappears.
    Missing,
    /// The reservation is replaced by one bound to a different request.
    Mismatched,
}

/// A change in the phase-2 -> phase-3 window never arms the receipt and never
/// debits the ledger.
async fn window_never_arms(owner: bool) {
    let f = funded_org().await;
    let pool = runtime(&f, owner).await;
    let plane = plane_of(&f, &pool);
    // Unavailable outcomes first: the CreditsExhausted hold of `Released`
    // fences the window for later admissions.
    for window in [Window::Cancelled, Window::Missing, Window::Mismatched, Window::Released] {
        let gate = open_uncapped(f.call(&plane)).await;
        let logical = gate.logical_call;
        let (content_pool, control_owner, control_app) = (pool.clone(), f.control.owner.clone(), f.control.app.clone());
        let (tenant, account) = (f.tenant.clone(), f.account);
        let result = acquire_with_hook(&gate, move |receipt| {
            let (content_pool, control_owner, control_app, tenant) =
                (content_pool.clone(), control_owner.clone(), control_app.clone(), tenant.clone());
            Box::pin(async move {
                match window {
                    Window::Released => {
                        funded::recover_logical_call(&JobScope::Org(&content_pool), &control_app, &tenant, account, logical)
                            .await.unwrap();
                    }
                    Window::Cancelled => {
                        let mut content = begin_plane(&JobScope::Org(&content_pool)).await.unwrap();
                        assert!(sqlx::query_scalar::<_, bool>("SELECT managed_cancel_unsent_v1($1)")
                            .bind(receipt).fetch_one(&mut *content).await.unwrap());
                        content.commit().await.unwrap();
                    }
                    Window::Missing | Window::Mismatched => {
                        let mut tx = financial_owner(&control_owner, account).await;
                        let deleted = sqlx::query("DELETE FROM managed_credit_reservations WHERE receipt_id=$1")
                            .bind(receipt).execute(&mut *tx).await.unwrap().rows_affected();
                        assert_eq!(deleted, 1, "phase 2 committed the reservation before the window");
                        if matches!(window, Window::Mismatched) {
                            sqlx::query("INSERT INTO managed_credit_reservations \
                                 (plane,receipt_id,account_id,payer_id,graph_id,logical_call,request_hash, \
                                  bound_micro_usd,state,settled_micro_usd,settled_at) \
                                 SELECT 'org:'||clerk_org_id,$1,$2,owner_user_id,gen_random_uuid(),$3,$4,1,'released',0,now() \
                                   FROM accounts WHERE id=$2")
                                .bind(receipt).bind(account).bind(logical).bind("b".repeat(64))
                                .execute(&mut *tx).await.unwrap();
                        }
                        tx.commit().await.unwrap();
                    }
                }
            })
        })
        .await;
        let receipt = receipt_of(&f, logical).await;
        let expected_hold = matches!(window, Window::Released);
        assert!(
            if expected_hold {
                matches!(result, Err(Deny::Hold(HoldCode::CreditsExhausted)))
            } else {
                matches!(result, Err(Deny::Unavailable))
            },
            "{window:?}: a change between phases must refuse the permit; actual={result:?}"
        );
        assert_eq!(receipt_phase(&f, receipt).await, ("cancelled_unsent".into(), "not_generated".into()),
            "{window:?}: the receipt is never armed and ends cancelled unsent");
        assert_eq!(ledger_rows(&f, receipt).await, 0, "{window:?}: nothing is debited");
        let stored = reservation(&f, receipt).await;
        match window {
            Window::Mismatched => assert_eq!(stored, Some(("released".into(), Some(0), "b".repeat(64))),
                "the mismatched reservation is refused, not overwritten"),
            _ => assert_eq!(stored.map(|(state, cost, _)| (state, cost)), Some(("released".into(), Some(0))),
                "{window:?}: whatever funding phase 2 held is released"),
        }
        let mut content = begin_plane(&plane).await.unwrap();
        assert!(!sqlx::query_scalar::<_, bool>("SELECT managed_arm_v1($1)")
            .bind(receipt).fetch_one(&mut *content).await.unwrap(), "{window:?}: the receipt can never arm later");
        content.commit().await.unwrap();
        drop(gate);
    }
    assert_eq!(armed_receipts(&f).await, 0);
    assert_eq!(account_ledger_rows(&f).await, 0);
    drop(plane);
    drop(pool);
    f.finish().await;
}

#[tokio::test]
async fn phase_window_release_cancel_missing_or_mismatch_never_arms() {
    window_never_arms(false).await;
}

#[tokio::test]
async fn phase_window_release_cancel_missing_or_mismatch_never_arms_tenant_owner() {
    window_never_arms(true).await;
}

/// The release side of the skip: a recovery pass over receipts that ended
/// cancelled unsent with their reservation already released reads them in
/// pages and changes nothing, instead of re-releasing each one.
async fn released_replay_is_skipped(owner: bool) {
    let f = funded_org().await;
    let pool = runtime(&f, owner).await;
    let plane = plane_of(&f, &pool);
    const N: usize = 60;
    let control_wire = Wire::new(upstream(), Duration::ZERO).await;
    let control = control_wire.pool(&f.control.app, 2).await;
    let t = worker(&pool, &f.tenant);
    let steady = steady_tick_trips(&t, &control_wire, &control).await;
    let mut receipts = Vec::new();
    for _ in 0..N {
        let gate = open_uncapped(f.call(&plane)).await;
        let logical = gate.logical_call;
        let content_pool = pool.clone();
        let result = acquire_with_hook(&gate, move |receipt| {
            let content_pool = content_pool.clone();
            Box::pin(async move {
                let mut content = begin_plane(&JobScope::Org(&content_pool)).await.unwrap();
                sqlx::query_scalar::<_, bool>("SELECT managed_cancel_unsent_v1($1)")
                    .bind(receipt).fetch_one(&mut *content).await.unwrap();
                content.commit().await.unwrap();
            })
        })
        .await;
        assert!(matches!(result, Err(Deny::Unavailable)), "actual={result:?}");
        receipts.push(receipt_of(&f, logical).await);
        drop(gate);
    }
    for receipt in &receipts {
        assert_eq!(reservation(&f, *receipt).await.map(|(s, c, _)| (s, c)), Some(("released".into(), Some(0))));
    }
    assert_eq!(queued(&f).await.len(), N, "every terminal receipt is queued for confirmation");
    let trips = recovery_trips(&t, &control_wire, &control).await;
    eprintln!("managed recovery round trips: steady {steady}, confirming {N} released receipts {trips}");
    // One page costs a fixed handful of round trips; re-releasing each
    // receipt costs three more per receipt.
    assert!(trips < steady + N as u64 / 2,
        "confirming {N} already-released receipts took {trips} control round trips against {steady} for a steady pass; it must be paged, not per receipt");
    assert!(queued(&f).await.is_empty(), "confirmed receipts leave the queue");
    billing_derivation::tick(&t, &control).await.unwrap();
    for receipt in &receipts {
        assert_eq!(reservation(&f, *receipt).await.map(|(s, c, _)| (s, c)), Some(("released".into(), Some(0))),
            "replaying released receipts changes nothing");
        assert_eq!(ledger_rows(&f, *receipt).await, 0);
    }
    drop(control);
    drop(plane);
    drop(pool);
    f.finish().await;
}

#[tokio::test]
async fn recovery_skips_released_unsent_receipts_without_rereleasing() {
    released_replay_is_skipped(false).await;
}

#[tokio::test]
async fn recovery_skips_released_unsent_receipts_without_rereleasing_tenant_owner() {
    released_replay_is_skipped(true).await;
}

/// A settled reservation whose amount differs from its receipt is a
/// conflicting financial fact. Recovery must refuse it on every pass, and
/// the receipt must stay queued, never skipped as "already applied".
#[tokio::test]
async fn settled_amount_mismatch_errors_every_pass_and_is_never_skipped() {
    let f = funded_org().await;
    let plane = f.plane();
    let gate = open_uncapped(f.call(&plane)).await;
    let receipt = coralswarm_core::send_gate::acquire(&gate, 1).await.unwrap().into_sent().receipt_id().unwrap();
    settle_content_only(&plane, receipt, 44).await;
    let request = FundingRequest {
        plane: match &f.tenant { TenantId::Org(org) => format!("org:{org}"), TenantId::Consumer => unreachable!() },
        receipt,
        account: f.account,
        payer: f.payer,
        graph: f.graph,
        logical_call: gate.logical_call,
        request_hash: gate.request_hash.clone(),
        bound_micro_usd: gate.worst.per_attempt.get(),
    };
    let mut financial = begin_account_scoped(&f.control.app, f.account).await.unwrap();
    assert!(credit::settle_tx(&mut financial, &request, 55).await.unwrap());
    financial.commit().await.unwrap();
    assert_eq!(reservation(&f, receipt).await.map(|(s, c, _)| (s, c)), Some(("settled".into(), Some(55))));
    let t = worker(f.app(), &f.tenant);
    for pass in 0..2 {
        let result = billing_derivation::tick(&t, &f.control.app).await;
        let error = result.expect_err("a settled amount that differs from the receipt must fail recovery");
        assert!(error.contains("conflicts with terminal financial state"), "pass {pass}: {error}");
        assert_eq!(queued(&f).await, vec![receipt], "pass {pass}: the conflicting receipt stays queued");
        assert_eq!(reservation(&f, receipt).await.map(|(s, c, _)| (s, c)), Some(("settled".into(), Some(55))));
        assert_eq!(ledger_rows(&f, receipt).await, 1, "pass {pass}: no second debit");
    }
    drop(gate);
    drop(plane);
    f.finish().await;
}

/// Control round trips of one managed recovery pass (the part of a tick that
/// holds the account's financial lock), apart from the ordinary derivation.
async fn recovery_trips(t: &WorkerTenant, wire: &Wire, control: &PgPool) -> u64 {
    let before = wire.count();
    billing_derivation::recover_managed_only(t, control).await.unwrap();
    wire.count() - before
}

/// Round trips of a steady recovery pass, after a full tick drained the
/// queue.
async fn steady_tick_trips(t: &WorkerTenant, wire: &Wire, control: &PgPool) -> u64 {
    billing_derivation::tick(t, control).await.unwrap();
    recovery_trips(t, wire, control).await
}

/// A steady recovery pass costs exactly the same over 603 settled receipts as
/// over 3: it reads only what control has not confirmed yet.
async fn cost_independent_of_history(org: bool) {
    let f = if org { funded_org().await } else { funded_consumer().await };
    settled_history(&f, 3).await;
    let wire = Wire::new(upstream(), Duration::ZERO).await;
    let control = wire.pool(&f.control.app, 2).await;
    let content = if org { wire.pool(f.app(), 2).await } else { control.clone() };
    let t = worker(&content, &f.tenant);
    let small = steady_tick_trips(&t, &wire, &control).await;
    settled_history(&f, 600).await;
    assert_eq!(queued(&f).await.len(), 600, "newly settled receipts are queued");
    let large = steady_tick_trips(&t, &wire, &control).await;
    eprintln!("steady managed recovery round trips (org={org}): 3 settled {small}, 603 settled {large}");
    assert!(queued(&f).await.is_empty(), "the draining tick confirmed the whole queue");
    assert_eq!(large, small,
        "a steady recovery pass over 603 settled receipts made {large} round trips against {small} over 3; it must not grow with settled history");
    assert_eq!(account_ledger_rows(&f).await, 603, "each settled receipt is debited exactly once");
    drop((control, content));
    f.finish().await;
}

#[tokio::test]
async fn recovery_cost_does_not_grow_with_settled_history() {
    cost_independent_of_history(true).await;
}

#[tokio::test]
async fn recovery_cost_does_not_grow_with_settled_history_consumer() {
    cost_independent_of_history(false).await;
}

/// A receipt admitted before a run of others but settled after the queue
/// drained them is still recovered: queue order is not settlement order and
/// nothing is skipped past.
async fn late_out_of_order(org: bool, owner: bool) {
    let f = if org { funded_org().await } else { funded_consumer().await };
    let pool = if org { runtime(&f, owner).await } else { f.app().clone() };
    let plane = plane_of(&f, &pool);
    let early = open_uncapped(f.call(&plane)).await;
    let receipt = coralswarm_core::send_gate::acquire(&early, 1).await.unwrap().into_sent().receipt_id().unwrap();
    settle_calls(&f, 20).await;
    let t = worker(&pool, &f.tenant);
    billing_derivation::tick(&t, &f.control.app).await.unwrap();
    assert!(queued(&f).await.is_empty(), "the later receipts were confirmed");
    assert_eq!(reservation(&f, receipt).await.unwrap().0, "held", "an armed, unsettled receipt is not queued");
    settle_content_only(&plane, receipt, 44).await;
    assert_eq!(queued(&f).await, vec![receipt], "the late settlement queues the receipt in its own transaction");
    assert_eq!(reservation(&f, receipt).await.unwrap().0, "held", "content settled, control not yet");
    billing_derivation::tick(&t, &f.control.app).await.unwrap();
    assert_eq!(reservation(&f, receipt).await.map(|(s, c, _)| (s, c)), Some(("settled".into(), Some(44))),
        "the late receipt is settled at its charge");
    assert_eq!(ledger_rows(&f, receipt).await, 1);
    assert!(queued(&f).await.is_empty());
    billing_derivation::tick(&t, &f.control.app).await.unwrap();
    assert_eq!(account_ledger_rows(&f).await, 21, "replay debits nothing twice");
    drop(early);
    drop(plane);
    drop(pool);
    f.finish().await;
}

#[tokio::test]
async fn recovery_settles_a_late_out_of_order_receipt_after_the_queue_drained() {
    late_out_of_order(true, false).await;
}

#[tokio::test]
async fn recovery_settles_a_late_out_of_order_receipt_after_the_queue_drained_tenant_owner() {
    late_out_of_order(true, true).await;
}

#[tokio::test]
async fn recovery_settles_a_late_out_of_order_receipt_after_the_queue_drained_consumer() {
    late_out_of_order(false, false).await;
}

/// The owner-rights reaper (`managed_reap_stale_v1`, SECURITY DEFINER) is the
/// backstop for an armed receipt nobody settled. Its ambiguous settlement
/// must enqueue the receipt like a live settle does, so recovery charges the
/// reservation exactly once and drains it.
async fn reaped_receipt(org: bool, owner: bool) {
    let f = if org { funded_org().await } else { funded_consumer().await };
    let pool = if org { runtime(&f, owner).await } else { f.app().clone() };
    let plane = plane_of(&f, &pool);
    let gate = open_uncapped(f.call(&plane)).await;
    let receipt = coralswarm_core::send_gate::acquire(&gate, 1).await.unwrap().into_sent().receipt_id().unwrap();
    assert!(queued(&f).await.is_empty(), "an armed, unsettled receipt is not queued");
    // Age it past the reaper's 10-minute floor. created_at is sealed by
    // managed_source_receipt_history, so the seal is lifted for this write only.
    let mut tx = f.owner().begin().await.unwrap();
    sqlx::query("ALTER TABLE managed_attempt_receipts DISABLE TRIGGER managed_source_receipt_history")
        .execute(&mut *tx).await.unwrap();
    sqlx::query("UPDATE managed_attempt_receipts SET created_at=now()-interval '1 hour' WHERE receipt_id=$1")
        .bind(receipt).execute(&mut *tx).await.unwrap();
    sqlx::query("ALTER TABLE managed_attempt_receipts ENABLE TRIGGER managed_source_receipt_history")
        .execute(&mut *tx).await.unwrap();
    tx.commit().await.unwrap();
    let t = worker(&pool, &f.tenant);
    crate::managed::gate::reap_tick(&t).await.unwrap();
    assert_eq!(receipt_phase(&f, receipt).await, ("armed".into(), "ambiguous".into()));
    assert_eq!(queued(&f).await, vec![receipt], "the reaper's settlement queues the receipt in its own transaction");
    let reserved: i64 = sqlx::query_scalar("SELECT reserved_micro_usd FROM managed_attempt_receipts WHERE receipt_id=$1")
        .bind(receipt).fetch_one(f.owner()).await.unwrap();
    billing_derivation::tick(&t, &f.control.app).await.unwrap();
    assert_eq!(reservation(&f, receipt).await.map(|(s, c, _)| (s, c)), Some(("settled".into(), Some(reserved))),
        "an ambiguous receipt is charged its full reservation");
    assert_eq!(ledger_rows(&f, receipt).await, 1);
    assert!(queued(&f).await.is_empty());
    billing_derivation::tick(&t, &f.control.app).await.unwrap();
    assert_eq!(ledger_rows(&f, receipt).await, 1, "replay debits nothing twice");
    drop(gate);
    drop(plane);
    drop(pool);
    f.finish().await;
}

#[tokio::test]
async fn reaped_receipt_is_queued_and_charged_once() {
    reaped_receipt(true, false).await;
}

#[tokio::test]
async fn reaped_receipt_is_queued_and_charged_once_tenant_owner() {
    reaped_receipt(true, true).await;
}

#[tokio::test]
async fn reaped_receipt_is_queued_and_charged_once_consumer() {
    reaped_receipt(false, false).await;
}

/// Control committed but the tenant queue drain did not (a crash between
/// the two): the rows stay queued, the next pass re-confirms them without
/// changing anything, and then drains them.
#[tokio::test]
async fn lost_tenant_drain_is_reconfirmed_without_double_charging() {
    let f = funded_org().await;
    settled_history(&f, 5).await;
    let t = worker(f.app(), &f.tenant);
    billing_derivation::tick(&t, &f.control.app).await.unwrap();
    assert!(queued(&f).await.is_empty());
    let restored = sqlx::query("INSERT INTO managed_receipt_recovery_queue(receipt_id,account_id) \
         SELECT receipt_id,account_id FROM managed_attempt_receipts WHERE account_id=$1 AND state<>'reserved'")
        .bind(f.account).execute(f.owner()).await.unwrap().rows_affected();
    assert_eq!(restored, 5);
    let reservations = |f: &Fixture| {
        let control = f.control.owner.clone();
        let account = f.account;
        async move {
            sqlx::query_as::<_, (Uuid, String, Option<i64>)>(
                "SELECT receipt_id,state,settled_micro_usd FROM managed_credit_reservations WHERE account_id=$1 ORDER BY receipt_id",
            ).bind(account).fetch_all(&control).await.unwrap()
        }
    };
    let before = reservations(&f).await;
    let balance = |f: &Fixture| {
        let control = f.control.owner.clone();
        let account = f.account;
        async move {
            sqlx::query_scalar::<_, Decimal>("SELECT balance FROM credit_balances WHERE account_id=$1")
                .bind(account).fetch_one(&control).await.unwrap()
        }
    };
    let balance_before = balance(&f).await;
    billing_derivation::tick(&t, &f.control.app).await.unwrap();
    assert!(queued(&f).await.is_empty(), "re-confirmed rows are drained");
    assert_eq!(reservations(&f).await, before, "re-confirmation changes no reservation");
    assert_eq!(balance(&f).await, balance_before, "and debits nothing");
    assert_eq!(account_ledger_rows(&f).await, 5);
    f.finish().await;
}

/// Without the enqueue trigger a receipt could turn terminal unqueued, so
/// recovery refuses that plane instead of trusting the queue.
#[tokio::test]
async fn recovery_refuses_a_plane_without_the_queue_trigger() {
    let f = funded_org().await;
    settled_history(&f, 1).await;
    let t = worker(f.app(), &f.tenant);
    for db in [f.owner().clone(), f.control.owner.clone()] {
        sqlx::query("ALTER TABLE managed_attempt_receipts DISABLE TRIGGER managed_receipt_recovery_queue")
            .execute(&db).await.unwrap();
        let error = billing_derivation::tick(&t, &f.control.app).await
            .expect_err("a plane without the queue trigger must refuse recovery");
        assert!(error.contains("recovery queue trigger is not installed"), "{error}");
        assert_eq!(queued(&f).await.len(), 1, "nothing drains while refused");
        sqlx::query("ALTER TABLE managed_attempt_receipts ENABLE TRIGGER managed_receipt_recovery_queue")
            .execute(&db).await.unwrap();
    }
    billing_derivation::tick(&t, &f.control.app).await.unwrap();
    assert!(queued(&f).await.is_empty());
    f.finish().await;
}

/// A caller that drops `reserve` mid-admission (a timeout while it waits)
/// no longer leaves a prepared receipt and a held reservation for the
/// 10-minute reaper: the abandoned admission is cancelled and released.
async fn dropped_admission(owner: bool) {
    let f = funded_org().await;
    let pool = runtime(&f, owner).await;
    let plane = plane_of(&f, &pool);
    let gate = open_uncapped(f.call(&plane)).await;
    let logical = gate.logical_call;
    let parked = acquire_with_hook(&gate, |_| Box::pin(futures::future::pending()));
    assert!(tokio::time::timeout(Duration::from_millis(1500), parked).await.is_err(),
        "the admission is parked between phases and dropped by the caller's timeout");
    let receipt = receipt_of(&f, logical).await;
    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    loop {
        let phase = receipt_phase(&f, receipt).await;
        let held = reservation(&f, receipt).await.map(|(s, c, _)| (s, c));
        if phase == ("cancelled_unsent".into(), "not_generated".into())
            && held == Some(("released".into(), Some(0)))
        {
            break;
        }
        assert!(std::time::Instant::now() < deadline,
            "a dropped admission must be cancelled and released promptly; receipt {phase:?}, reservation {held:?}");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(ledger_rows(&f, receipt).await, 0);
    assert_eq!(armed_receipts(&f).await, 0);
    // The cleanup never touches an admission that completed: a normal one
    // afterwards arms, bills and settles once.
    let next = open_uncapped(f.call(&plane)).await;
    let permit = coralswarm_core::send_gate::acquire(&next, 1).await.unwrap();
    next.settle(permit.into_sent(), &billed()).await;
    assert_eq!(account_ledger_rows(&f).await, 1);
    drop((gate, next));
    drop(plane);
    drop(pool);
    f.finish().await;
}

#[tokio::test]
async fn dropped_admission_is_cancelled_and_released_without_the_reaper() {
    dropped_admission(false).await;
}

#[tokio::test]
async fn dropped_admission_is_cancelled_and_released_without_the_reaper_tenant_owner() {
    dropped_admission(true).await;
}

/// The Consumer billing pass enumerates every account. An org account with
/// no tenant database (nothing for this pass to read) used to fail the whole
/// pass every run ("Org recovery routing unavailable or ambiguous"), so the
/// worker retried after its 5 s error backoff forever. It now has nothing to
/// recover, unless control holds a receipt for it, which still refuses.
#[tokio::test]
async fn consumer_pass_skips_an_unmapped_org_account_with_no_receipts() {
    let f = funded_consumer().await;
    settled_history(&f, 1).await;
    let org_account = Uuid::new_v4();
    sqlx::query("INSERT INTO accounts(id,owner_user_id,clerk_org_id) VALUES($1,$2,$3)")
        .bind(org_account).bind(f.payer).bind(format!("org_unmapped_{}", org_account.simple()))
        .execute(&f.control.owner).await.unwrap();
    let t = worker(f.app(), &f.tenant);
    billing_derivation::tick(&t, &f.control.app).await
        .expect("an org account with no tenant database and no receipts has nothing to recover");
    let mut tx = financial_owner(&f.control.owner, org_account).await;
    sqlx::query("INSERT INTO managed_budget_periods(account_id,purpose,period_start,limit_micro_usd) \
         SELECT $1,purpose,period_start,limit_micro_usd FROM managed_budget_periods WHERE account_id=$2 LIMIT 1")
        .bind(org_account).bind(f.account).execute(&mut *tx).await.unwrap();
    sqlx::query("INSERT INTO managed_attempt_receipts(account_id,purpose,period_start,graph_id,attributed_user_id, \
           logical_call,attempt_no,candidate_models,input_bound_tokens,max_tokens,reserved_micro_usd, \
           protocol_version,dispatch_phase,content_plane,request_hash) \
         SELECT $1,purpose,period_start,graph_id,attributed_user_id,gen_random_uuid(),attempt_no,candidate_models, \
           input_bound_tokens,max_tokens,reserved_micro_usd,1,'prepared',content_plane,request_hash \
           FROM managed_attempt_receipts WHERE account_id=$2 LIMIT 1")
        .bind(org_account).bind(f.account).execute(&mut *tx).await.unwrap();
    tx.commit().await.unwrap();
    let error = billing_derivation::tick(&t, &f.control.app).await
        .expect_err("an unmapped org account with receipts on control still refuses");
    assert!(error.contains("no tenant database"), "{error}");
    f.finish().await;
}

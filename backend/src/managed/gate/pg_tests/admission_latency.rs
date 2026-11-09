use super::*;
use crate::workers::billing_derivation;
use sqlx::types::Decimal;
use std::net::SocketAddr;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// A loopback TCP relay in front of the fixture Postgres. Every chunk waits
/// `one_way` in each direction, so a round trip costs what it does from a Fly
/// machine to its databases, and every client chunk is counted.
pub(super) struct Wire {
    addr: SocketAddr,
    round_trips: Arc<AtomicU64>,
    task: tokio::task::JoinHandle<()>,
}

impl Wire {
    pub(super) async fn new(upstream: SocketAddr, one_way: Duration) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let round_trips = Arc::new(AtomicU64::new(0));
        let counter = round_trips.clone();
        let task = tokio::spawn(async move {
            while let Ok((client, _)) = listener.accept().await {
                let Ok(server) = tokio::net::TcpStream::connect(upstream).await else {
                    continue;
                };
                let (mut client_read, mut client_write) = client.into_split();
                let (mut server_read, mut server_write) = server.into_split();
                let counter = counter.clone();
                tokio::spawn(async move {
                    let mut buf = vec![0u8; 64 * 1024];
                    while let Ok(n) = client_read.read(&mut buf).await {
                        if n == 0 {
                            break;
                        }
                        counter.fetch_add(1, Ordering::Relaxed);
                        tokio::time::sleep(one_way).await;
                        if server_write.write_all(&buf[..n]).await.is_err() {
                            break;
                        }
                    }
                });
                tokio::spawn(async move {
                    let mut buf = vec![0u8; 64 * 1024];
                    while let Ok(n) = server_read.read(&mut buf).await {
                        if n == 0 {
                            break;
                        }
                        tokio::time::sleep(one_way).await;
                        if client_write.write_all(&buf[..n]).await.is_err() {
                            break;
                        }
                    }
                });
            }
        });
        Self { addr, round_trips, task }
    }

    pub(super) fn count(&self) -> u64 {
        self.round_trips.load(Ordering::Relaxed)
    }

    pub(super) async fn pool(&self, direct: &PgPool, connections: u32) -> PgPool {
        PgPoolOptions::new()
            .max_connections(connections)
            .acquire_timeout(Duration::from_secs(60))
            .connect_with(
                direct
                    .connect_options()
                    .as_ref()
                    .clone()
                    .host("127.0.0.1")
                    .port(self.addr.port()),
            )
            .await
            .unwrap()
    }
}

impl Drop for Wire {
    fn drop(&mut self) {
        self.task.abort();
    }
}

pub(super) fn upstream() -> SocketAddr {
    let url = std::env::var("ORGT_TEST_BASE_URL").unwrap();
    let options = PgConnectOptions::from_str(&url).unwrap();
    let host = match options.get_host() {
        "localhost" => "127.0.0.1",
        other => other,
    };
    format!("{host}:{}", options.get_port()).parse().unwrap()
}

pub(super) fn billed() -> SendOutcome {
    SendOutcome::Billed {
        reported_cost_usd: Some(0.000044),
        input_tokens: 2,
        output_tokens: 2,
        served_model: "deepseek/deepseek-v4-flash-0731".into(),
        upstream: None,
        generation_id: None,
    }
}

fn window_call<'a>(f: &'a Fixture, plane: &'a JobScope<'a>, control: &'a PgPool) -> ManagedCall<'a> {
    let mut call = f.call(plane);
    call.control = control;
    call
}

pub(super) async fn open_uncapped<'a>(call: ManagedCall<'a>) -> ManagedGate<'a> {
    let mut gate = ManagedGate::open(call, &completion_request()).await.unwrap();
    gate.caps.per_version = MicroUsd::from_usd_ceil(1_000_000.0);
    gate.caps.daily_extraction = MicroUsd::from_usd_ceil(1_000_000.0);
    gate
}

/// `calls` funded, billed and settled extraction calls.
pub(super) async fn settle_calls(f: &Fixture, calls: usize) {
    let plane = f.plane();
    for _ in 0..calls {
        let gate = open_uncapped(f.call(&plane)).await;
        let permit = coralswarm_core::send_gate::acquire(&gate, 1).await.unwrap();
        gate.settle(permit.into_sent(), &billed()).await;
    }
}

/// One funded, billed and fully reconciled extraction call, the shape every
/// production attempt leaves behind once it settles.
pub(super) async fn settled_history(f: &Fixture, calls: usize) {
    settle_calls(f, calls).await;
    let held: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM managed_credit_reservations WHERE account_id=$1 AND state='held'",
    )
    .bind(f.account)
    .fetch_one(&f.control.owner)
    .await
    .unwrap();
    assert_eq!(held, 0, "history fixture must be fully settled on control");
}

pub(super) async fn funded_org() -> Fixture {
    let f = Fixture::new(true).await;
    sqlx::query("INSERT INTO credit_conversion(effective_from,reference_cost_per_credit,note) VALUES(now()-interval '1 day',1,'admission latency fixture')")
        .execute(&f.control.owner).await.unwrap();
    f.balance(Decimal::from(1_000_000)).await;
    f
}

pub(super) fn worker(pool: &PgPool, tenant: &TenantId) -> WorkerTenant {
    WorkerTenant {
        tenant: tenant.clone(),
        pool: pool.clone(),
        store: Arc::new(crate::store::PostgresStore::new(pool.clone())),
    }
}

fn percentile(sorted: &[Duration], p: f64) -> Duration {
    let rank = ((sorted.len() as f64) * p).ceil() as usize;
    sorted[rank.clamp(1, sorted.len()) - 1]
}

/// The scheduled billing pass re-settled every historical funded receipt one
/// by one under the account's financial lock. Its cost must not grow with
/// settled history: a re-run over already-settled receipts reads each page in
/// one control round trip per query instead of three per receipt.
#[tokio::test]
async fn recovery_over_settled_history_is_paged_not_per_receipt() {
    let f = funded_org().await;
    settled_history(&f, 600).await;
    let control_wire = Wire::new(upstream(), Duration::ZERO).await;
    let content_wire = Wire::new(upstream(), Duration::ZERO).await;
    let control = control_wire.pool(&f.control.app, 2).await;
    let content = content_wire.pool(f.app(), 2).await;
    let t = worker(&content, &f.tenant);
    billing_derivation::tick(&t, &control).await.unwrap();
    let (control_before, content_before) = (control_wire.count(), content_wire.count());
    billing_derivation::tick(&t, &control).await.unwrap();
    let control_trips = control_wire.count() - control_before;
    let content_trips = content_wire.count() - content_before;
    eprintln!("billing tick over 600 settled receipts: control round trips {control_trips}, content {content_trips}");
    assert!(
        control_trips < 150,
        "a billing tick over 600 settled receipts made {control_trips} control round trips; per-receipt re-settlement makes 1800+"
    );
    let ledger: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM credit_ledger WHERE account_id=$1 AND source_receipt_id IS NOT NULL",
    )
    .bind(f.account)
    .fetch_one(&f.control.owner)
    .await
    .unwrap();
    assert_eq!(ledger, 600, "each settled receipt is debited exactly once");
    drop((control, content));
    f.finish().await;
}

/// Skipping settled history must not skip a receipt that still needs its
/// financial settlement.
async fn held_behind_settled(tenant_owner: bool) {
    let f = funded_org().await;
    settled_history(&f, 3).await;
    let plane = f.plane();
    let gate = open_uncapped(f.call(&plane)).await;
    let receipt = coralswarm_core::send_gate::acquire(&gate, 1)
        .await
        .unwrap()
        .into_sent()
        .receipt_id()
        .unwrap();
    let mut content = begin_plane(&plane).await.unwrap();
    assert!(sqlx::query_scalar::<_, bool>("SELECT managed_settle_v1($1,'billed',44,'deepseek/deepseek-v4-flash-0731',NULL,NULL)")
        .bind(receipt).fetch_one(&mut *content).await.unwrap());
    content.commit().await.unwrap();
    let reservation = |receipt: Uuid| {
        sqlx::query_as::<_, (String, Option<i64>)>(
            "SELECT state,settled_micro_usd FROM managed_credit_reservations WHERE receipt_id=$1",
        )
        .bind(receipt)
        .fetch_one(&f.control.owner)
    };
    assert_eq!(reservation(receipt).await.unwrap().0, "held", "tenant settled, control not yet");
    let t = worker(if tenant_owner { f.owner() } else { f.app() }, &f.tenant);
    billing_derivation::tick(&t, &f.control.app).await.unwrap();
    assert_eq!(
        reservation(receipt).await.unwrap(),
        ("settled".into(), Some(44)),
        "a held reservation behind a settled receipt is settled at the receipt's charge"
    );
    let ledger: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM credit_ledger WHERE account_id=$1 AND source_receipt_id IS NOT NULL",
    )
    .bind(f.account)
    .fetch_one(&f.control.owner)
    .await
    .unwrap();
    assert_eq!(ledger, 4);
    billing_derivation::tick(&t, &f.control.app).await.unwrap();
    assert_eq!(
        reservation(receipt).await.unwrap(),
        ("settled".into(), Some(44)),
        "replaying settled history changes nothing"
    );
    drop(gate);
    drop(plane);
    f.finish().await;
}

#[tokio::test]
async fn recovery_still_settles_a_held_reservation_behind_a_settled_receipt() {
    held_behind_settled(false).await;
}

#[tokio::test]
async fn recovery_still_settles_a_held_reservation_behind_a_settled_receipt_tenant_owner() {
    held_behind_settled(true).await;
}

/// Admission latency for concurrent org callers while the billing worker
/// runs, behind a relay that delays every round trip. CI runs a small smoke
/// configuration. The measurement in #1356 used
/// `ADMISSION_BENCH_HISTORY=4000 ADMISSION_BENCH_ONE_WAY_MS=1 ADMISSION_BENCH_CALLERS=6`,
/// which reproduces production's 64-80 s billing pass; the recovery-queue PR
/// also ran `ADMISSION_BENCH_HISTORY=40000`.
#[tokio::test]
async fn org_admission_latency_under_billing_recovery() {
    let history: usize = std::env::var("ADMISSION_BENCH_HISTORY").ok().and_then(|v| v.parse().ok()).unwrap_or(50);
    let one_way_ms: u64 = std::env::var("ADMISSION_BENCH_ONE_WAY_MS").ok().and_then(|v| v.parse().ok()).unwrap_or(0);
    let callers: usize = std::env::var("ADMISSION_BENCH_CALLERS").ok().and_then(|v| v.parse().ok()).unwrap_or(2);
    let per_caller: usize = std::env::var("ADMISSION_BENCH_CALLS").ok().and_then(|v| v.parse().ok()).unwrap_or(2);
    let tick_gap_ms: u64 = std::env::var("ADMISSION_BENCH_TICK_GAP_MS").ok().and_then(|v| v.parse().ok()).unwrap_or(200);
    let f = funded_org().await;
    let seeded = std::time::Instant::now();
    settled_history(&f, history).await;
    eprintln!("seeded {history} settled receipts in {:?}", seeded.elapsed());
    let one_way = Duration::from_millis(one_way_ms);
    let control_wire = Wire::new(upstream(), one_way).await;
    let content_wire = Wire::new(upstream(), one_way).await;
    let control = control_wire.pool(&f.control.app, 20).await;
    let content = content_wire.pool(f.app(), 20).await;
    let plane = JobScope::Org(&content);

    let solo = std::time::Instant::now();
    let (c0, t0) = (control_wire.count(), content_wire.count());
    let gate = open_uncapped(window_call(&f, &plane, &control)).await;
    let opened = solo.elapsed();
    let permit = coralswarm_core::send_gate::acquire(&gate, 1).await.unwrap();
    let reserved = solo.elapsed() - opened;
    let (c1, t1) = (control_wire.count(), content_wire.count());
    gate.settle(permit.into_sent(), &billed()).await;
    eprintln!(
        "solo admission: open {opened:?} reserve {reserved:?}; reserve round trips control {} content {}; settle round trips control {} content {}",
        c1 - c0, t1 - t0, control_wire.count() - c1, content_wire.count() - t1
    );
    drop(gate);

    let t = worker(&content, &f.tenant);
    // The first tick confirms every receipt queued since the last pass (here
    // the whole seeded history, in bounded transactions); the second is the
    // steady pass every later tick pays.
    for label in ["first (drains the recovery queue)", "steady"] {
        let (c0, t0) = (control_wire.count(), content_wire.count());
        let tick = std::time::Instant::now();
        billing_derivation::tick(&t, &control).await.unwrap();
        eprintln!(
            "solo billing tick, {label}, over {history} settled receipts: {:?}, round trips control {} content {}",
            tick.elapsed(), control_wire.count() - c0, content_wire.count() - t0
        );
    }

    let stop = std::sync::atomic::AtomicBool::new(false);
    let ticks = async {
        let mut durations = Vec::new();
        while !stop.load(Ordering::Relaxed) {
            let started = std::time::Instant::now();
            billing_derivation::tick(&t, &control).await.unwrap();
            durations.push(started.elapsed());
            tokio::time::sleep(Duration::from_millis(tick_gap_ms)).await;
        }
        durations
    };
    let admissions = async {
        let lanes = (0..callers).map(|_| async {
            let mut waits = Vec::new();
            for _ in 0..per_caller {
                let started = std::time::Instant::now();
                let gate = open_uncapped(window_call(&f, &plane, &control)).await;
                let permit = coralswarm_core::send_gate::acquire(&gate, 1).await.unwrap();
                waits.push(started.elapsed());
                gate.settle(permit.into_sent(), &billed()).await;
            }
            waits
        });
        let mut all: Vec<Duration> = futures::future::join_all(lanes).await.into_iter().flatten().collect();
        stop.store(true, Ordering::Relaxed);
        all.sort();
        all
    };
    let (all, tick_durations) = tokio::join!(admissions, ticks);
    eprintln!(
        "{callers} callers x {per_caller} admissions under billing ticks every {tick_gap_ms} ms (history {history}, one-way {one_way_ms} ms): p50 {:?} p95 {:?} max {:?}; billing ticks {:?}",
        percentile(&all, 0.50), percentile(&all, 0.95), all.last().unwrap(), tick_durations
    );
    drop(plane);
    drop((control, content));
    f.finish().await;
}

/// #1497 (prod, 2026-10-26): same-account admissions queued on the billing
/// lock each held a control connection while they waited, and five of them
/// at once took the pool away from authentication. Four concurrent org
/// admissions for one account on a two-connection control pool must leave
/// the second connection free the whole time: waiters queue in process
/// (`billing::entry_queue`) and hold no connection. Every admission still
/// goes through, and each is funded exactly once.
#[tokio::test]
async fn same_account_admissions_queue_without_holding_control_connections() {
    let f = funded_org().await;
    let control = PgPoolOptions::new()
        .max_connections(2)
        .acquire_timeout(Duration::from_millis(500))
        .connect_with(f.control.app.connect_options().as_ref().clone())
        .await
        .unwrap();
    let plane = f.plane();
    let callers = 4;
    let mut gates = Vec::with_capacity(callers);
    for _ in 0..callers {
        gates.push(open_uncapped(window_call(&f, &plane, &control)).await);
    }
    let stop = std::sync::atomic::AtomicBool::new(false);
    let probes = AtomicU64::new(0);
    let starved = AtomicU64::new(0);
    // An unrelated request (auth) needs one control connection, briefly,
    // over and over, while the admissions contend.
    let probe = async {
        while !stop.load(Ordering::Relaxed) {
            probes.fetch_add(1, Ordering::Relaxed);
            let got = tokio::time::timeout(
                Duration::from_secs(1),
                sqlx::query("SELECT 1").execute(&control),
            )
            .await;
            if !matches!(got, Ok(Ok(_))) {
                starved.fetch_add(1, Ordering::Relaxed);
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    };
    let admissions = async {
        let outcomes = futures::future::join_all(
            gates.iter().map(|gate| coralswarm_core::send_gate::acquire(gate, 1)),
        )
        .await;
        stop.store(true, Ordering::Relaxed);
        outcomes
    };
    let (outcomes, ()) = tokio::join!(admissions, probe);
    assert!(probes.load(Ordering::Relaxed) > 0, "the probe ran");
    assert_eq!(
        starved.load(Ordering::Relaxed), 0,
        "an unrelated request could not get a control connection while {callers} same-account admissions queued"
    );
    let mut receipts = Vec::new();
    for (gate, outcome) in gates.iter().zip(outcomes) {
        let permit = outcome.unwrap_or_else(|deny| panic!("every queued admission is admitted: {deny:?}"));
        let sent = permit.into_sent();
        receipts.push(sent.receipt_id().unwrap());
        gate.settle(sent, &billed()).await;
    }
    let held: Vec<(Uuid, String)> = sqlx::query_as(
        "SELECT receipt_id,state FROM managed_credit_reservations WHERE account_id=$1 ORDER BY receipt_id",
    )
    .bind(f.account)
    .fetch_all(&f.control.owner)
    .await
    .unwrap();
    assert_eq!(held.len(), callers, "one reservation per admission, none duplicated: {held:?}");
    for (receipt, state) in &held {
        assert!(receipts.contains(receipt), "reservation {receipt} belongs to one of the admissions");
        assert_eq!(state, "settled", "every admission settled once: {held:?}");
    }
    drop(gates);
    drop(plane);
    f.finish().await;
}

/// Everything a billing pass can change for the account, read for an
/// exact before/after comparison: each reservation's state and charge, the
/// receipt-backed ledger rows, the plan balance, and the tenant's recovery
/// queue.
async fn financial_state(f: &Fixture) -> (Vec<(Uuid, String, Option<i64>)>, i64, Option<Decimal>, i64) {
    let reservations = sqlx::query_as(
        "SELECT receipt_id,state,settled_micro_usd FROM managed_credit_reservations WHERE account_id=$1 ORDER BY receipt_id",
    )
    .bind(f.account)
    .fetch_all(&f.control.owner)
    .await
    .unwrap();
    let ledger = sqlx::query_scalar(
        "SELECT count(*) FROM credit_ledger WHERE account_id=$1 AND source_receipt_id IS NOT NULL",
    )
    .bind(f.account)
    .fetch_one(&f.control.owner)
    .await
    .unwrap();
    let balance = sqlx::query_scalar("SELECT balance FROM credit_balances WHERE account_id=$1")
        .bind(f.account)
        .fetch_optional(&f.control.owner)
        .await
        .unwrap();
    let queued = sqlx::query_scalar("SELECT count(*) FROM managed_receipt_recovery_queue WHERE account_id=$1")
        .bind(f.account)
        .fetch_one(f.owner())
        .await
        .unwrap();
    (reservations, ledger, balance, queued)
}

/// #1497: a lock timeout that fires while a billing pass has work to do must
/// mean "skip this account, retry next tick" and nothing else. Three
/// receipts are settled on the tenant plane but not yet on control (each
/// reservation still `held`, each queued for recovery). With the lock held
/// by a connection outside the queue, a tick fails for the account and
/// changes nothing: no reservation moves, no ledger row, no balance change,
/// the queue keeps its rows. Once the lock is free the next tick settles
/// each receipt exactly once (ledger and balance land on the expected
/// totals), and a further tick changes nothing again.
#[tokio::test]
async fn a_lock_timeout_mid_pass_writes_nothing_and_the_next_pass_settles_each_receipt_once() {
    let f = funded_org().await;
    let plane = f.plane();
    let calls = 3usize;
    let charge_micro_usd = 44i64;
    let mut receipts = Vec::with_capacity(calls);
    for _ in 0..calls {
        let gate = open_uncapped(f.call(&plane)).await;
        let receipt = coralswarm_core::send_gate::acquire(&gate, 1)
            .await
            .unwrap()
            .into_sent()
            .receipt_id()
            .unwrap();
        let mut content = begin_plane(&plane).await.unwrap();
        assert!(sqlx::query_scalar::<_, bool>(
            "SELECT managed_settle_v1($1,'billed',$2,'deepseek/deepseek-v4-flash-0731',NULL,NULL)",
        )
        .bind(receipt)
        .bind(charge_micro_usd)
        .fetch_one(&mut *content)
        .await
        .unwrap());
        content.commit().await.unwrap();
        receipts.push(receipt);
    }
    let before = financial_state(&f).await;
    assert_eq!(before.0.len(), calls);
    assert!(before.0.iter().all(|(_, state, _)| state == "held"), "tenant settled, control not yet: {:?}", before.0);
    assert_eq!(before.1, 0, "no ledger debit yet");
    assert_eq!(before.3, calls as i64, "every tenant-settled receipt is queued for control");
    let opening_balance = before.2.expect("the fixture seeded a plan balance");

    let t = worker(f.app(), &f.tenant);
    let mut holder = begin_account_scoped(&f.control.owner, f.account).await.unwrap();
    crate::billing::renewal::lock_period_rotation(&mut holder, f.account).await.unwrap();
    let blocked = tokio::time::timeout(Duration::from_secs(30), billing_derivation::tick(&t, &f.control.app)).await;
    let Ok(Err(error)) = blocked else {
        panic!("a tick behind a held lock must fail for the account, not hang or succeed");
    };
    assert!(error.contains("lock timeout"), "the failure is the bounded lock wait: {error}");
    assert_eq!(financial_state(&f).await, before, "the blocked pass wrote nothing and marked nothing");
    assert!(
        crate::billing::entry_queue::consecutive_skips(f.account) >= 1,
        "the skip was counted against the account"
    );
    holder.rollback().await.unwrap();

    billing_derivation::tick(&t, &f.control.app).await.expect("the next pass goes through");
    let after = financial_state(&f).await;
    assert_eq!(after.0.len(), calls, "one reservation per receipt, none duplicated: {:?}", after.0);
    for (receipt, state, settled) in &after.0 {
        assert!(receipts.contains(receipt), "reservation {receipt} belongs to one of the receipts");
        assert_eq!((state.as_str(), *settled), ("settled", Some(charge_micro_usd)), "receipt {receipt}");
    }
    assert_eq!(after.1, calls as i64, "exactly one ledger debit per receipt");
    let expected_balance = opening_balance - Decimal::new(charge_micro_usd * calls as i64, 6);
    assert_eq!(after.2, Some(expected_balance), "the balance moved by exactly the three charges at the fixture rate of 1");
    assert_eq!(after.3, 0, "the recovery queue drained");
    assert_eq!(
        crate::billing::entry_queue::consecutive_skips(f.account), 0,
        "a successful entry cleared the account's skip run"
    );

    billing_derivation::tick(&t, &f.control.app).await.expect("a further pass");
    assert_eq!(financial_state(&f).await, after, "replaying settled history changes nothing");
    drop(plane);
    f.finish().await;
}

/// #1497, the cross-process case for the billing worker: the account's lock
/// is held by a connection that never came through this process's entry
/// queue. The worker's recovery pass takes a turn (and a slot), then must
/// give up on the lock within `entry_queue::LOCK_WAIT`, report the account
/// as unavailable rather than hang, and release the turn so the account is
/// not pinned in this process.
#[tokio::test]
async fn billing_recovery_behind_a_held_lock_gives_up_within_the_bound_and_frees_its_turn() {
    let f = funded_org().await;
    let plane = f.plane();
    let t = worker(f.app(), &f.tenant);
    let mut holder = begin_account_scoped(&f.control.owner, f.account).await.unwrap();
    crate::billing::renewal::lock_period_rotation(&mut holder, f.account).await.unwrap();
    let started = std::time::Instant::now();
    let outcome = tokio::time::timeout(
        Duration::from_secs(20),
        billing_derivation::recover_managed_only(&t, &f.control.app),
    )
    .await;
    let waited = started.elapsed();
    let error = match outcome {
        Ok(Err(error)) => error,
        other => panic!("recovery behind a held lock must fail, not hang or succeed: {other:?} after {waited:?}"),
    };
    assert!(
        waited < crate::billing::entry_queue::LOCK_WAIT + Duration::from_secs(5),
        "recovery waited {waited:?}; the lock wait is bounded at {:?}",
        crate::billing::entry_queue::LOCK_WAIT
    );
    assert!(
        error.contains("lock timeout") || error.contains("canceling statement"),
        "the failure names the lock bound, not something else: {error}"
    );
    let turn = tokio::time::timeout(Duration::from_millis(500), crate::billing::entry_queue::wait_turn(f.account))
        .await
        .expect("the account's turn is free after the bounded failure")
        .unwrap();
    drop(turn);
    holder.rollback().await.unwrap();
    // With the holder gone the same pass goes through.
    billing_derivation::recover_managed_only(&t, &f.control.app).await.expect("recovery once the lock is free");
    drop(plane);
    f.finish().await;
}

/// 2026-10-20: an org admission waited on the account's billing lock with no
/// bound, holding a control connection the whole time. Behind a held lock the
/// admission must give up within the financial lock bound, return its
/// connection, and leave nothing reserved.
#[tokio::test]
async fn org_admission_behind_a_held_billing_lock_gives_up_and_returns_its_connection() {
    let f = funded_org().await;
    let control = PgPoolOptions::new()
        .max_connections(1)
        .acquire_timeout(Duration::from_secs(1))
        .connect_with(f.control.app.connect_options().as_ref().clone())
        .await
        .unwrap();
    let plane = f.plane();
    let gate = open_uncapped(window_call(&f, &plane, &control)).await;
    let mut holder = begin_account_scoped(&f.control.owner, f.account).await.unwrap();
    crate::billing::renewal::lock_period_rotation(&mut holder, f.account).await.unwrap();
    let started = std::time::Instant::now();
    let admitted = tokio::time::timeout(Duration::from_secs(20), coralswarm_core::send_gate::acquire(&gate, 1)).await;
    let waited = started.elapsed();
    assert!(matches!(admitted, Ok(Err(Deny::Unavailable))), "admission behind a held billing lock must fail fast as Unavailable, not wait for the holder ({waited:?})");
    assert!(waited < Duration::from_secs(10), "admission waited {waited:?} behind the billing lock");
    sqlx::query("SELECT 1").execute(&control).await.expect("the admission returned its control connection");
    // #1497: the holder did not come through the entry queue (another
    // process, in production). The failed admission must also have given
    // its in-process turn back, or every later admission for this account
    // would queue behind a frame that no longer exists.
    let turn = tokio::time::timeout(Duration::from_millis(500), crate::billing::entry_queue::wait_turn(f.account))
        .await
        .expect("the account's entry-queue turn is free after a lock-timeout failure")
        .unwrap();
    drop(turn);
    holder.rollback().await.unwrap();
    let reserved: i64 = sqlx::query_scalar("SELECT count(*) FROM managed_credit_reservations WHERE account_id=$1")
        .bind(f.account).fetch_one(&f.control.owner).await.unwrap();
    assert_eq!(reserved, 0, "a refused admission reserves no credit");
}

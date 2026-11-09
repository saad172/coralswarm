//! Read-model refresh on the existing worker pool (never a request connection).
use super::tenant_harness::{Tick, WorkerTenant};
use crate::read_models::failure::{self, Failure, FailureKind};
use crate::read_models::{self as rm, Error, Unaddressable};
use crate::tenancy::TenantId;
use futures::{FutureExt, StreamExt};
use sqlx::PgPool;
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicU8, Ordering};
use std::time::{Duration, Instant};
pub const BASE_POLL: Duration = Duration::from_secs(5);
pub const BATCH: usize = 4;
/// Bound the entire claim lifecycle, including publication/failure bookkeeping.
pub const COMPUTE_CONCURRENCY: usize = 2;
fn dispatch_concurrency(t: &WorkerTenant, control: &PgPool) -> usize {
    // A control-facts compute retains BOTH snapshots until its body finishes.
    // Personal content/control share a pool: two computes can hold four slots.
    // On separate org pools this is conservative (two tenant, two control).
    // Never increase a pool limit or let two tenant snapshots deadlock waiting
    // for control snapshots in a small shared pool.
    if t.pool.options().get_max_connections() >= 4 && control.options().get_max_connections() >= 2 {
        COMPUTE_CONCURRENCY
    } else {
        1
    }
}
/// Extra wall-clock room around the model's per-statement compute budget.
pub const COMPUTE_GRACE: Duration = Duration::from_secs(5);
pub fn compute_total_timeout(model: &dyn rm::registry::ReadModelDyn) -> Duration {
    model.compute_timeout() + COMPUTE_GRACE
}
/// The refresh step a failure happened in, which also says which database it
/// was waiting on: `membership` and `control` use the control-plane Postgres,
/// `viewer` and `publish` the tenant database, and `verify` does no I/O.
/// `compute` runs the model in the tenant snapshot (and, for a control-facts
/// model, the control snapshot too); both are already open, so a
/// `pool_timeout` cannot happen there.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Phase {
    /// Re-reading the compute actor's live org membership.
    Membership,
    /// Opening the tenant snapshot and loading the viewer.
    Viewer,
    /// Opening and certifying the control snapshot, and the access policy.
    Control,
    /// Checking the claim's scope and graphs against the live viewer.
    Verify,
    /// The model's own compute, and closing both snapshots.
    Compute,
    /// Storing the computed result.
    Publish,
}
impl Phase {
    const ALL: [Phase; 6] = [
        Phase::Membership,
        Phase::Viewer,
        Phase::Control,
        Phase::Verify,
        Phase::Compute,
        Phase::Publish,
    ];
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Membership => "membership",
            Self::Viewer => "viewer",
            Self::Control => "control",
            Self::Verify => "verify",
            Self::Compute => "compute",
            Self::Publish => "publish",
        }
    }
}
/// The step a compute is in, still readable after it errs, panics or runs out
/// of time. Atomic so the compute future stays `Send`.
#[derive(Debug, Default)]
pub struct Progress {
    phase: AtomicU8,
    model_started: std::sync::Mutex<Option<Instant>>,
    model_elapsed: std::sync::Mutex<Option<Duration>>,
}
impl Progress {
    fn enter(&self, phase: Phase) {
        self.phase.store(phase as u8, Ordering::Relaxed);
        if phase == Phase::Compute {
            *self.model_started.lock().unwrap() = Some(Instant::now());
        }
    }
    pub fn phase(&self) -> Phase {
        Phase::ALL[usize::from(self.phase.load(Ordering::Relaxed))]
    }
}
pub async fn compute(
    t: &WorkerTenant,
    c: &rm::store::Claim,
    control: &PgPool,
) -> Result<(serde_json::Value, String), Error> {
    compute_with_blobs(t, c, control, None).await
}
pub async fn compute_with_blobs(
    t: &WorkerTenant,
    c: &rm::store::Claim,
    control: &PgPool,
    r2: Option<&coralswarm_core::r2::R2Client>,
) -> Result<(serde_json::Value, String), Error> {
    compute_tracked(t, c, control, r2, &Progress::default()).await
}
/// [`compute_with_blobs`], recording each step in `progress` as it starts.
pub async fn compute_tracked(
    t: &WorkerTenant,
    c: &rm::store::Claim,
    control: &PgPool,
    r2: Option<&coralswarm_core::r2::R2Client>,
    progress: &Progress,
) -> Result<(serde_json::Value, String), Error> {
    if let crate::tenancy::TenantId::Org(org) = &t.tenant {
        progress.enter(Phase::Membership);
        let member =
            crate::middleware::auth::live_org_membership(control, org, c.compute_as).await?;
        if !member.is_some_and(|(account, _)| account == c.account_id) {
            return Err(Box::new(Unaddressable::Membership));
        }
    }
    let model = rm::registry::by_name(&c.model).ok_or("unknown model")?;
    progress.enter(Phase::Viewer);
    let mut tx = rm::store::begin_compute(&t.pool, c, model.compute_timeout()).await?;
    let snapshot: String = sqlx::query_scalar("SELECT pg_current_snapshot()::text")
        .fetch_one(&mut *tx)
        .await?;
    let mut viewer = rm::Viewer::load(&mut tx, c.account_id, c.compute_as).await?;
    let mut control_tx = if model.control_facts() {
        progress.enter(Phase::Control);
        Some(rm::store::begin_compute(control, c, model.compute_timeout()).await?)
    } else {
        None
    };
    if let Some(control_tx) = control_tx.as_mut() {
        viewer.certify_control(control_tx).await?;
        // Graph/membership/viewer refusals can recover. Only a monotonic
        // revision advance proves this claimed address can never be live again.
        if viewer
            .control_revision
            .is_some_and(|revision| revision > c.control_revision)
        {
            return Err(Box::new(Unaddressable::ControlRevision));
        }
    }
    progress.enter(Phase::Verify);
    model.verify_scope(&viewer, c.params.clone(), &c.scope_key, &c.graph_ids)?;
    // Verification also fences tampered/stale graph metadata. Compute uses only
    // the class derived from the verified member, never a system content scope.
    let mut graphs = c.graph_ids.clone();
    graphs.sort_unstable();
    graphs.dedup();
    if graphs != c.graph_ids || graphs.iter().any(|g| !viewer.visible_graphs().contains(g)) {
        return Err(Box::new(Unaddressable::ClaimedGraphs));
    }
    let org = match &t.tenant {
        crate::tenancy::TenantId::Org(org) => Some(org.as_str()),
        _ => None,
    };
    // Admin policy requires the same control facts snapshot used by compute.
    // Live auth is also re-read before any content snapshot above.
    if model.access() != rm::access::AccessPolicy::Member {
        progress.enter(Phase::Control);
        let policy_tx = control_tx
            .as_mut()
            .ok_or("privileged model requires control facts")?;
        model
            .access()
            .compute(&mut **policy_tx, c.account_id, c.compute_as, org)
            .await?;
    }
    let blobs = rm::blobs::BlobReader {
        r2,
        tenant: &t.tenant,
    };
    progress.enter(Phase::Compute);
    let payload = model
        .compute_json(
            rm::ComputeCx {
                tx: &mut tx,
                blobs: Some(&blobs),
                control_tx: control_tx.as_mut(),
                account_id: c.account_id,
                compute_as: c.compute_as,
                graphs: &graphs,
                now: chrono::Utc::now(),
            },
            c.params.clone(),
        )
        .await?;
    *progress.model_elapsed.lock().unwrap() = progress
        .model_started
        .lock()
        .unwrap()
        .map(|at| at.elapsed());
    if let Some(control_tx) = control_tx {
        control_tx.rollback().await?;
    }
    tx.rollback().await?;
    Ok((payload, snapshot))
}
pub async fn tick(t: &WorkerTenant, control: &PgPool) -> Result<Tick, String> {
    tick_with_blobs(t, control, None).await
}
pub async fn tick_with_blobs(
    t: &WorkerTenant,
    control: &PgPool,
    r2: Option<&coralswarm_core::r2::R2Client>,
) -> Result<Tick, String> {
    let claims = rm::store::claim(&t.pool).await.map_err(|e| e.to_string())?;
    let count = claims.len();
    let batch_claimed = Instant::now();
    // Poll at most two complete claim lifecycles. Do not use try_collect:
    // an error (or panic) must not drop other claimed work. No detached tasks;
    // cancelling the tick still drops all compute snapshots for lease recovery.
    let outcomes = futures::stream::iter(claims)
        .map(|c| AssertUnwindSafe(refresh_claim(t, c, control, r2, batch_claimed)).catch_unwind())
        .buffer_unordered(dispatch_concurrency(t, control))
        .collect::<Vec<_>>()
        .await;
    let mut first_error = None;
    let mut first_panic = None;
    for outcome in outcomes {
        match outcome {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                first_error.get_or_insert(error);
            }
            Err(panic) => {
                first_panic.get_or_insert(panic);
            }
        }
    }
    if let Some(panic) = first_panic {
        std::panic::resume_unwind(panic);
    }
    if let Some(error) = first_error {
        return Err(error);
    }
    let next = rm::store::next_due(&t.pool)
        .await
        .map_err(|e| e.to_string())?;
    Ok(match next {
        Some(s) => Tick::until_next_work(
            count,
            Duration::from_secs_f64(s.max(0.1).min(BASE_POLL.as_secs_f64())),
        ),
        None if count > 0 => Tick::worked(count),
        None => Tick::idle(),
    })
}
/// One internally claimed row owns its actor-scoped transactions and token.
async fn refresh_claim(
    t: &WorkerTenant,
    c: rm::store::Claim,
    control: &PgPool,
    r2: Option<&coralswarm_core::r2::R2Client>,
    batch_claimed: Instant,
) -> Result<(), String> {
    let batch_wait = batch_claimed.elapsed();
    tracing::info!(model=%c.model,version=c.version,scope_key=%rm::store::scope_correlation(&c.scope_key),
        generation=c.dirty_generation,revision=c.control_revision,
        batch_wait_ms=batch_wait.as_secs_f64()*1000.,"read-model refresh started");
    // Bound total compute, not only each SQL statement; RAII drops/rolls back
    // an interrupted transaction before a retry can publish anything.
    let timeout = compute_total_timeout(rm::registry::by_name(&c.model).unwrap());
    let progress = Progress::default();
    let started = Instant::now();
    let compute = tokio::time::timeout(timeout, compute_tracked(t, &c, control, r2, &progress));
    let outcome = AssertUnwindSafe(compute).catch_unwind().await;
    // Compute time only: the bookkeeping below is not part of it.
    let elapsed = started.elapsed();
    let failure = match outcome {
        Err(panic) => {
            // Name the model, then unwind as before: the harness isolates
            // the tenant, and a panic still never reaches `fail`.
            let panicked = Failure::of(FailureKind::Panic);
            log_failure(
                &t.tenant,
                &c,
                &panicked,
                progress.phase(),
                &Backoff::NotAttempted,
                elapsed,
            );
            std::panic::resume_unwind(panic);
        }
        Ok(Ok(Ok((payload, snapshot)))) => {
            let publish_started = Instant::now();
            let published = rm::store::publish(&t.pool, &c, payload, &snapshot).await;
            let publish_elapsed = publish_started.elapsed();
            if let Err(ref e) = published {
                let failure = failure::classify(e);
                log_failure(
                    &t.tenant,
                    &c,
                    &failure,
                    Phase::Publish,
                    &Backoff::NotAttempted,
                    elapsed,
                );
                return Err(e.to_string());
            }
            let published = published.unwrap();
            record_completion(
                &c,
                &progress,
                batch_wait,
                elapsed,
                publish_elapsed,
                published,
            );
            return Ok(());
        }
        Ok(Ok(Err(error))) => failure::classify(&error),
        Ok(Err(_elapsed)) => Failure::of(FailureKind::WallTimeout),
    };
    // Log before propagating a bookkeeping error, so the cause survives it.
    let recorded = if failure.reason() == Some(Unaddressable::ControlRevision) {
        rm::store::retire(&t.pool, &c).await.map(|retired| {
            if retired {
                Backoff::Retired
            } else {
                Backoff::ClaimLost
            }
        })
    } else {
        rm::store::fail(&t.pool, &c, failure.kind)
            .await
            .map(|attempts| attempts.map_or(Backoff::ClaimLost, Backoff::Recorded))
    };
    let backoff = match &recorded {
        Ok(backoff) => backoff.clone(),
        Err(e) => Backoff::Failed(failure::classify(e)),
    };
    log_failure(&t.tenant, &c, &failure, progress.phase(), &backoff, elapsed);
    recorded.map_err(|e| e.to_string())?;
    Ok(())
}
/// Process-local counters have fixed dimensions (published/lost).
/// Correlation fields belong only to internal log receipts, never metric labels.
static PUBLISHED: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
static CLAIM_LOST: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
fn record_completion(
    c: &rm::store::Claim,
    progress: &Progress,
    batch_wait: Duration,
    compute: Duration,
    publish: Duration,
    published: bool,
) {
    let counter = if published { &PUBLISHED } else { &CLAIM_LOST };
    let total = counter.fetch_add(1, Ordering::Relaxed) + 1;
    let model_compute = *progress.model_elapsed.lock().unwrap();
    tracing::info!(model=%c.model,version=c.version,scope_key=%rm::store::scope_correlation(&c.scope_key),
        generation=c.dirty_generation,revision=c.control_revision,first_fill=c.first_fill,
        immediate_due_at=?c.immediate_due_at,claimed_at=%c.claimed_at,
        due_to_claim_ms=c.due_to_claim_ms,batch_wait_ms=batch_wait.as_secs_f64()*1000.,
        compute_ms=compute.as_secs_f64()*1000.,model_compute_ms=model_compute.map(|d|d.as_secs_f64()*1000.),
        publish_ms=publish.as_secs_f64()*1000.,published,count=1,total,
        "read-model refresh completed");
}
/// What `store::fail` did with a failed refresh. Each case logs its own
/// field, so a missing count is never ambiguous.
#[derive(Debug, Clone)]
enum Backoff {
    /// A strictly older control address was deleted by its owning claim.
    Retired,
    /// The scope is backed off; its consecutive failure count.
    Recorded(i32),
    /// A newer worker holds the claim, so nothing was recorded.
    ClaimLost,
    /// The bookkeeping write itself failed; its class, never its text.
    Failed(Failure),
    /// `fail` is not called: a panic unwinds to the harness, and a failed
    /// publish is reclaimed once its claim lease lapses.
    NotAttempted,
}
impl Backoff {
    const fn as_str(&self) -> &'static str {
        match self {
            Self::Retired => "retired",
            Self::Recorded(_) => "recorded",
            Self::ClaimLost => "claim_lost",
            Self::Failed(_) => "bookkeeping_failed",
            Self::NotAttempted => "not_attempted",
        }
    }
}
/// The one line per failed refresh. Every field is a code literal, an internal
/// id (tenant, account, a prefix of the scope key, itself a SHA-256) or a
/// number; never the error's text, SQL, binds or payload, which can quote
/// private content. `elapsed_ms` is the compute's own time, excluding the
/// bookkeeping after it. It stays on this module's default target: production
/// drops `coralswarm::metrics`, which this is not.
fn log_failure(
    tenant: &TenantId,
    c: &rm::store::Claim,
    failure: &Failure,
    phase: Phase,
    backoff: &Backoff,
    elapsed: Duration,
) {
    let (failed_attempts, bookkeeping) = match backoff {
        Backoff::Recorded(attempts) => (Some(*attempts), None),
        Backoff::Failed(bookkeeping) => (None, Some(bookkeeping)),
        Backoff::Retired | Backoff::ClaimLost | Backoff::NotAttempted => (None, None),
    };
    tracing::warn!(
        model = %c.model,
        version = c.version,
        tenant = ?tenant,
        account_id = %c.account_id,
        scope_key = %rm::store::scope_correlation(&c.scope_key),
        generation = c.dirty_generation,
        revision = c.control_revision,
        first_fill = c.first_fill,
        due_to_claim_ms = c.due_to_claim_ms,
        phase = phase.as_str(),
        kind = failure.kind.as_str(),
        sqlstate = failure.sqlstate(),
        reason = failure.reason().map(Unaddressable::as_str),
        backoff = backoff.as_str(),
        failed_attempts,
        bookkeeping_kind = bookkeeping.map(|b| b.kind.as_str()),
        bookkeeping_sqlstate = bookkeeping.and_then(Failure::sqlstate),
        elapsed_ms = u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX),
        "read-model refresh failed"
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::read_models::failure::tests::db;
    use std::sync::{Arc, Mutex};

    /// Private content as Postgres, sqlx and serde quote it back.
    const SECRET: &str = "Q3 layoffs: alice@example.com";

    #[derive(Clone, Default)]
    struct Capture(Arc<Mutex<Vec<u8>>>);
    impl std::io::Write for Capture {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    /// Emit one failure line through the production JSON formatter.
    fn line(failure: &Failure, phase: Phase, backoff: &Backoff) -> serde_json::Value {
        let capture = Capture::default();
        let writer = capture.clone();
        let subscriber = tracing_subscriber::fmt()
            .json()
            .with_writer(move || writer.clone())
            .finish();
        let claim = rm::store::Claim {
            model: "journal_strip".into(),
            scope_key: "0123456789abcdef".repeat(4),
            version: 3,
            account_id: uuid::Uuid::from_u128(1),
            graph_ids: vec![uuid::Uuid::from_u128(2)],
            compute_as: uuid::Uuid::from_u128(3),
            params: serde_json::json!({ "q": SECRET }),
            dirty_generation: 7,
            claim_token: uuid::Uuid::from_u128(4),
            control_revision: 0,
            control_freshness: 0,
            immediate_due_at: None,
            claimed_at: chrono::Utc::now(),
            first_fill: true,
            due_to_claim_ms: None,
        };
        tracing::subscriber::with_default(subscriber, || {
            log_failure(
                &TenantId::Org("org_1".into()),
                &claim,
                failure,
                phase,
                backoff,
                Duration::from_millis(1234),
            )
        });
        let out = String::from_utf8(capture.0.lock().unwrap().clone()).unwrap();
        assert!(!out.contains(SECRET), "private text reached the log: {out}");
        assert!(
            !out.contains("Q3 layoffs") && !out.contains("alice@"),
            "{out}"
        );
        let mut lines = out.lines();
        let line = serde_json::from_str(lines.next().expect("one failure line")).unwrap();
        assert!(lines.next().is_none(), "{out}");
        line
    }

    #[test]
    fn completion_receipt_distinguishes_lost_claim_and_excludes_private_inputs() {
        let capture = Capture::default();
        let writer = capture.clone();
        let subscriber = tracing_subscriber::fmt()
            .json()
            .with_writer(move || writer.clone())
            .finish();
        let claim = rm::store::Claim {
            model: "connectors_list".into(),
            scope_key: SECRET.into(),
            version: 1,
            account_id: uuid::Uuid::from_u128(1),
            graph_ids: vec![uuid::Uuid::from_u128(2)],
            compute_as: uuid::Uuid::from_u128(3),
            params: serde_json::json!({"private":SECRET}),
            dirty_generation: 4,
            claim_token: uuid::Uuid::from_u128(4),
            control_revision: 8,
            control_freshness: 9,
            immediate_due_at: None,
            claimed_at: chrono::Utc::now(),
            first_fill: true,
            due_to_claim_ms: None,
        };
        let progress = Progress::default();
        *progress.model_elapsed.lock().unwrap() = Some(Duration::from_millis(2));
        tracing::subscriber::with_default(subscriber, || {
            for published in [true, false] {
                record_completion(
                    &claim,
                    &progress,
                    Duration::from_millis(1),
                    Duration::from_millis(3),
                    Duration::from_millis(4),
                    published,
                );
            }
        });
        let raw = String::from_utf8(capture.0.lock().unwrap().clone()).unwrap();
        assert!(!raw.contains(SECRET));
        let lines: Vec<serde_json::Value> = raw
            .lines()
            .map(|l| serde_json::from_str(l).unwrap())
            .collect();
        assert_eq!(lines.len(), 2);
        for (line, published) in lines.iter().zip([true, false]) {
            assert_eq!(line["level"], "INFO");
            let fields = &line["fields"];
            assert_eq!(fields["published"], published);
            assert_eq!(fields["count"], 1);
            assert_eq!(fields["scope_key"], rm::store::scope_correlation(SECRET));
            assert_eq!(fields["generation"], 4);
            assert_eq!(fields["revision"], 8);
            assert_eq!(fields["batch_wait_ms"], 1.);
            assert_eq!(fields["compute_ms"], 3.);
            assert_eq!(fields["model_compute_ms"], 2.);
            assert_eq!(fields["publish_ms"], 4.);
            for absent in [
                "account_id",
                "compute_as",
                "graph_ids",
                "claim_token",
                "params",
                "payload",
                "sql",
                "due_to_claim_ms",
            ] {
                assert!(fields.get(absent).is_none(), "{absent}");
            }
        }
    }

    /// A compute failure whose backoff was recorded (`None` is a panic).
    fn logged(error: Option<Error>) -> serde_json::Value {
        let failure = error.map_or(Failure::of(FailureKind::Panic), |e| failure::classify(&e));
        line(&failure, Phase::Compute, &Backoff::Recorded(2))
    }

    #[test]
    fn failure_line_carries_the_class_never_the_error_text() {
        let line = logged(Some(db(Some("57014"), SECRET).into()));
        assert_eq!(line["level"], "WARN");
        let target = line["target"].as_str().unwrap();
        assert_eq!(target, "coralswarm_backend::workers::read_model_refresh");
        assert!(!target.starts_with(crate::telemetry::METRICS_TARGET));
        let fields = &line["fields"];
        assert_eq!(fields["message"], "read-model refresh failed");
        assert_eq!(fields["phase"], "compute");
        assert_eq!(fields["kind"], "statement_timeout");
        assert_eq!(fields["sqlstate"], "57014");
        assert_eq!(fields["model"], "journal_strip");
        assert_eq!(fields["version"], 3);
        assert_eq!(fields["backoff"], "recorded");
        assert_eq!(fields["failed_attempts"], 2);
        assert_eq!(fields["elapsed_ms"], 1234);
        assert_eq!(
            fields["scope_key"],
            rm::store::scope_correlation(&"0123456789abcdef".repeat(4))
        );
        assert_eq!(fields["account_id"], uuid::Uuid::from_u128(1).to_string());
        let names: Vec<_> = fields.as_object().unwrap().keys().cloned().collect();
        for absent in [
            "error",
            "params",
            "payload",
            "sql",
            "reason",
            "bookkeeping_kind",
            "bookkeeping_sqlstate",
        ] {
            assert!(!names.iter().any(|n| n == absent), "{absent} in {names:?}");
        }
    }

    #[test]
    fn backoff_outcomes_are_distinct_fields() {
        let timeout = Failure::of(FailureKind::WallTimeout);
        // A lost claim and a failed bookkeeping write never pass for each
        // other, or for a recorded backoff, by a merely missing count.
        let lost = &line(&timeout, Phase::Compute, &Backoff::ClaimLost)["fields"];
        assert_eq!(lost["backoff"], "claim_lost");
        assert!(lost.get("failed_attempts").is_none());
        assert!(lost.get("bookkeeping_kind").is_none());
        let bookkeeping =
            failure::classify(&crate::store::StoreError::from(db(Some("53300"), SECRET)).into());
        let failed = &line(&timeout, Phase::Compute, &Backoff::Failed(bookkeeping))["fields"];
        assert_eq!(failed["backoff"], "bookkeeping_failed");
        assert_eq!(failed["bookkeeping_kind"], "database");
        assert_eq!(failed["bookkeeping_sqlstate"], "53300");
        assert_eq!(failed["kind"], "wall_timeout");
        assert!(failed.get("sqlstate").is_none());
        assert!(failed.get("failed_attempts").is_none());
        let pool = Failure::of(FailureKind::PoolTimeout);
        let publish = &line(&pool, Phase::Publish, &Backoff::NotAttempted)["fields"];
        assert_eq!(publish["backoff"], "not_attempted");
        assert_eq!(publish["phase"], "publish");
        assert!(publish.get("failed_attempts").is_none());
        assert!(publish.get("bookkeeping_kind").is_none());
    }

    #[test]
    fn retirement_has_a_distinct_privacy_safe_settlement() {
        let failure = failure::classify(&Unaddressable::ControlRevision.into());
        let event = line(&failure, Phase::Control, &Backoff::Retired);
        assert_eq!(event["fields"]["backoff"], "retired");
        assert_eq!(event["fields"]["reason"], "control_revision");
        assert!(event["fields"]["failed_attempts"].is_null());
        assert!(!event.to_string().contains(SECRET));
    }

    #[test]
    fn unaddressable_logs_its_variant_name() {
        for (reason, name) in [
            (Unaddressable::ControlRevision, "control_revision"),
            (Unaddressable::Membership, "membership"),
            (Unaddressable::ScopeGraphs, "scope_graphs"),
            (Unaddressable::ViewerClass, "viewer_class"),
            (Unaddressable::ClaimedGraphs, "claimed_graphs"),
        ] {
            let fields = &logged(Some(Box::new(reason)))["fields"];
            assert_eq!(fields["kind"], "scope_unaddressable");
            assert_eq!(fields["reason"], name);
        }
    }

    #[test]
    fn progress_reports_the_phase_it_last_entered() {
        let progress = Progress::default();
        assert_eq!(progress.phase(), Phase::Membership);
        let mut names = std::collections::BTreeSet::new();
        for phase in Phase::ALL {
            progress.enter(phase);
            assert_eq!(progress.phase(), phase);
            assert!(names.insert(phase.as_str()));
        }
    }

    #[test]
    fn no_error_shape_leaks_its_text() {
        let column = sqlx::Error::ColumnDecode {
            index: SECRET.into(),
            source: SECRET.into(),
        };
        let json = serde_json::from_value::<u32>(serde_json::json!(SECRET)).unwrap_err();
        assert!(
            json.to_string().contains(SECRET),
            "the probe must carry the text"
        );
        for (error, kind) in [
            (Error::from(db(Some("40P01"), SECRET)), "deadlock"),
            (db(None, SECRET).into(), "database"),
            (
                crate::store::StoreError::from(db(Some("42P01"), SECRET)).into(),
                "database",
            ),
            (column.into(), "decode"),
            (sqlx::Error::Protocol(SECRET.into()).into(), "connection"),
            (json.into(), "json"),
            (SECRET.into(), "other"),
        ] {
            assert_eq!(logged(Some(error))["fields"]["kind"], kind);
        }
        let panic = logged(None);
        assert_eq!(panic["fields"]["kind"], "panic");
        assert!(panic["fields"].get("sqlstate").is_none());
    }
}

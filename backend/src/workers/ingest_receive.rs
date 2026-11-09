//! The receive stage of ADR-0016's two-stage ingest. A webhook verifies its
//! provider's signature, writes one `JobKind::IngestReceive` job through
//! [`enqueue`] and answers; this worker claims the job, fetches the full
//! content and stores it through `ingest::submit()`. The webhook makes no
//! provider call and handles no content, so a delivery survives a restart
//! between the answer and the ingest.
//!
//! # Plane
//!
//! Consumer only. A webhook cannot know which accounts a delivery reaches
//! (a Gmail mailbox can be watched under a personal and an org account), so
//! every receive job lands on the consumer `jobs` table and the provider
//! handler resolves each account's own plane, as it did before this stage
//! existed. Org tenant entries of the harness idle without a query.
//!
//! # Retries and idempotency
//!
//! A handler answers a [`ReceiveError`]. A transient one ([`transient`]: the
//! provider answered 429 or 5xx, the request never completed, or our own
//! storage failed) fails the job onto the queue's backoff ladder, so a fetch
//! that fails mid-way is retried rather than dropped. A permanent one (the
//! provider answered a 4xx that a retry cannot change, the held payload does
//! not parse, no connection, a revoked credential) completes the job without
//! a retry: retrying would hit the same wall. Re-running a job is safe: each
//! handler diffs from its stored cursor or re-fetches by id, and `submit()`
//! dedupes on the content hash. The webhook's dedupe key drops a redelivery
//! while the first job is still pending, and a job waiting on backoff keeps
//! its key ([`queue::fail_keeping_key`]), so during an outage every new push
//! for the same mailbox or resource coalesces into the one waiting job; the
//! retry covers them all. A delivery that reaches several accounts follows
//! [`FanOut`]: a transient failure for any of them retries the job, a
//! permanent one for some of them does not.
//!
//! A receive job gets its own attempt budget ([`JobKind::max_attempts`]):
//! the queue's backoff doubles from 60 s, so the table default of three
//! attempts dead-letters a delivery after about three minutes, and a Graph
//! `deleted` notification has no backstop sweep to recover it. Nine attempts
//! span 60 + 120 + ... + 7680 s, about four and a quarter hours, which
//! outlasts a provider or tenant-plane outage of a few hours.
//!
//! Whatever the outcome, the row that settles (`done` or `dead`) keeps a
//! scrubbed payload ([`redacted`]); only a row awaiting a retry keeps the
//! full one, since the retry runs from it. A worker that dies mid-job
//! leaves the row `running` with its payload until the reaper resets it
//! (`workers::reaper`): a reset row retries from that payload (the reaper
//! drops its key, as for every kind), and one the reaper dead-letters is
//! redacted in the same statement that dead-letters it ([`REAP_REDACTION`]),
//! so no crash between the two can leave a dead row with its payload.

use std::sync::Arc;
use std::time::Duration;

use crate::connectors::ConnectorError;
use crate::crypto::DekPlaneError;
use crate::jobs::queue::{self, DeadPayloadRedaction, FailedKeepingKey, JobOutcome, JobRow};
use crate::jobs::{JobKind, ReceivedEvent};
use crate::routes::{google_push, graph_webhook};
use crate::tenancy::TenantId;
use crate::workers::tenant_harness::{Tick, WorkerTenant};
use crate::AppState;

const KIND: &str = "IngestReceive";

/// Jobs claimed per poll, run concurrently: deliveries for different
/// mailboxes and subscriptions are independent, and a Gmail pass can wait on
/// its org's pass slot.
const JOB_BATCH_SIZE: usize = 8;

pub const EMPTY_POLL_SLEEP: Duration = Duration::from_millis(500);

/// Why a held delivery did not run to completion.
#[derive(Debug)]
pub(crate) enum ReceiveError {
    /// The held payload cannot be processed by any attempt: it does not
    /// parse as what the webhook wrote. Never retried.
    Malformed(String),
    /// A provider or storage failure, retried when [`transient`].
    Connector(ConnectorError),
}

impl ReceiveError {
    /// Whether the job should go back on the backoff ladder.
    pub(crate) fn retryable(&self) -> bool {
        match self {
            ReceiveError::Malformed(_) => false,
            ReceiveError::Connector(e) => transient(e),
        }
    }
}

impl std::fmt::Display for ReceiveError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ReceiveError::Malformed(what) => write!(f, "malformed delivery: {what}"),
            ReceiveError::Connector(e) => write!(f, "{e}"),
        }
    }
}

impl From<ConnectorError> for ReceiveError {
    fn from(e: ConnectorError) -> Self {
        ReceiveError::Connector(e)
    }
}

/// Whether a failed delivery is worth retrying.
///
/// * A provider's non-2xx ([`ConnectorError::UpstreamStatus`]) is decided on
///   the status it carries as a field: 408, 425, 429 and every 5xx retry.
///   Any other 4xx (400, 401, 403, 404, 409, 410) is permanent: the message
///   is gone, the request is wrong, or access is refused, and a retry gets
///   the same answer. One exception: Gmail answers a rate limit with `403`
///   naming one of `google::GOOGLE_RATE_LIMIT_REASONS`, which retries.
/// * An upstream failure with no status ([`ConnectorError::Exchange`]: a
///   network error, a timeout, a response that did not parse) retries.
///   Nothing is read back out of the message text, so a 4xx phrased in a
///   way no parser expects cannot be mistaken for one of these.
/// * Our own storage failing ([`ConnectorError::Internal`]) retries.
/// * No connection, a revoked credential, an unimplemented provider: permanent.
pub(crate) fn transient(e: &ConnectorError) -> bool {
    match e {
        ConnectorError::UpstreamStatus { status: 403, message } => {
            crate::connectors::google::message_names_rate_limit(message)
        }
        ConnectorError::UpstreamStatus { status: 408 | 425 | 429, .. } => true,
        ConnectorError::UpstreamStatus { status, .. } => *status >= 500,
        ConnectorError::Exchange(_) | ConnectorError::Internal(_) => true,
        ConnectorError::NotConfigured(_)
        | ConnectorError::NotImplemented(_)
        | ConnectorError::CredentialRevoked { .. } => false,
    }
}

/// A tenancy-plane resolution failure as a connector error, so the receive
/// worker classifies it like any other ([`transient`]). One rule for every
/// provider: a lookup that failed or timed out, or a pool that could not be
/// built, retries ([`ConnectorError::Internal`]); an account that no longer
/// exists, a write-frozen tenant or a routing row naming another account is
/// permanent ([`ConnectorError::NotConfigured`], the class a revoked org and
/// an unknown mailbox already use), since no retry changes the answer.
pub(crate) fn plane_error(e: DekPlaneError) -> ConnectorError {
    match e {
        DekPlaneError::AccountNotFound(_) | DekPlaneError::TenantNotWritable { .. } | DekPlaneError::AccountMismatch { .. } => {
            ConnectorError::NotConfigured(format!("tenancy plane: {e}"))
        }
        DekPlaneError::AccountLookup(_) | DekPlaneError::RoutingLookup(_) | DekPlaneError::PoolBuild(_) => {
            ConnectorError::Internal(format!("tenancy plane: {e}"))
        }
    }
}

/// The rule for a delivery that reaches several accounts (a Gmail mailbox
/// watched under a personal and an org account, a Chat space watched by
/// several accounts): every account runs; one account's transient failure
/// fails the job, so the delivery retries for it (the retry re-runs the
/// healthy accounts too, which is safe: each diffs from its own cursor); a
/// permanent failure ([`transient`] is false) is logged and does not fail
/// the job, since a retry would get the same answer for it and only re-run
/// the others. The first transient failure becomes the job's error.
pub(crate) struct FanOut {
    transient: Option<ConnectorError>,
}

impl FanOut {
    pub(crate) fn new() -> Self {
        FanOut { transient: None }
    }

    /// Note one account's failure. `log` is called with the error and
    /// whether the job will retry for it, so the caller's log line can say
    /// so.
    pub(crate) fn failed(&mut self, e: ConnectorError, log: impl FnOnce(&ConnectorError, bool)) {
        let retry = transient(&e);
        log(&e, retry);
        if retry {
            self.transient.get_or_insert(e);
        }
    }

    /// `Ok(ok)` unless some account failed transiently.
    pub(crate) fn finish<T>(self, ok: T) -> Result<T, ConnectorError> {
        match self.transient {
            Some(e) => Err(e),
            None => Ok(ok),
        }
    }
}

/// Write `events` as receive jobs on the consumer pool, in one transaction.
/// `false` (logged) when they could not be written: the webhook then answers
/// 503 so the provider redelivers.
pub(crate) async fn enqueue(state: &AppState, events: Vec<(ReceivedEvent, Option<String>)>) -> bool {
    if events.is_empty() {
        return true;
    }
    let Some(pool) = state.db.as_ref() else {
        tracing::error!("ingest receive: no consumer pool — cannot hold the delivery");
        return false;
    };
    let deliveries = events.len();
    let result = async {
        let mut tx = pool.begin().await?;
        for (event, key) in events {
            let kind = JobKind::IngestReceive(event);
            queue::enqueue_tx(&mut tx, &kind, None, key.as_deref(), 0).await?;
        }
        tx.commit().await
    }
    .await;
    if let Err(e) = result {
        tracing::error!(error = %e, deliveries, "ingest receive: failed to write the receive jobs");
        return false;
    }
    true
}

pub async fn tick(t: &WorkerTenant, state: &Arc<AppState>) -> Result<Tick, String> {
    if t.tenant != TenantId::Consumer {
        return Ok(Tick::idle());
    }
    let jobs = queue::dequeue(&t.pool, &[KIND], JOB_BATCH_SIZE)
        .await
        .map_err(|e| format!("dequeue failed: {e}"))?;
    if jobs.is_empty() {
        return Ok(Tick::idle());
    }
    let n = jobs.len();
    futures::future::join_all(jobs.into_iter().map(|job| process_job(t, state, job, &google_push::gmail_pass))).await;
    Ok(Tick::worked(n))
}

/// Run one receive job and settle it. `gmail_pass` is the Gmail round trip
/// (`google_push::gmail_pass` in production).
async fn process_job(t: &WorkerTenant, state: &Arc<AppState>, job: JobRow, gmail_pass: &google_push::GmailPass) {
    let pool = &t.pool;
    let tagged = serde_json::json!({ "kind": job.kind, "payload": job.payload });
    let event = match serde_json::from_value::<JobKind>(tagged) {
        Ok(JobKind::IngestReceive(event)) => event,
        Ok(_) | Err(_) => {
            // Not a delivery this binary can read: a bug or a version skew,
            // never something a retry fixes. Dead-lettered at once, scrubbed.
            let msg = format!("malformed {KIND} payload");
            tracing::error!(job_id = %job.id, "ingest receive: {msg}");
            if let Err(e) = queue::dead_letter_with_payload(pool, job.id, &msg, redacted(job.payload)).await {
                tracing::error!(error = %e, job_id = %job.id, "ingest receive: failed to dead-letter a malformed job");
            }
            return;
        }
    };

    // Boxed: each provider's future is large, and one inline state machine
    // holding them all overflows a 2 MiB thread stack in a debug build.
    let result: Result<JobOutcome, ReceiveError> = match event {
        ReceivedEvent::GmailPush { email_address } => {
            Box::pin(google_push::run_gmail_sync_for_email_with(state, &email_address, gmail_pass))
                .await
                .map(|n| if n > 0 { JobOutcome::DidWork } else { JobOutcome::NothingToDo })
                .map_err(ReceiveError::from)
        }
        ReceivedEvent::GoogleWorkspaceEvent { envelope } => {
            Box::pin(google_push::receive_workspace_event(state.clone(), envelope)).await.map(|()| JobOutcome::DidWork)
        }
        ReceivedEvent::GraphNotification { notification } => {
            Box::pin(graph_webhook::receive_notification(state.clone(), notification))
                .await
                .map(|()| JobOutcome::DidWork)
        }
    };

    let settled = match result {
        Ok(outcome) => queue::complete_with_payload(pool, job.id, outcome, redacted(job.payload)).await,
        Err(e) if e.retryable() => {
            tracing::warn!(error = %e, job_id = %job.id, attempts = job.attempts, "ingest receive: delivery failed — retrying");
            queue::fail_keeping_key(pool, job.id, &e.to_string(), redacted(job.payload)).await.map(|failed| {
                if failed == FailedKeepingKey::Superseded {
                    tracing::info!(job_id = %job.id, "ingest receive: a newer job holds this delivery's key and carries the retry");
                }
            })
        }
        Err(e) => {
            tracing::warn!(error = %e, job_id = %job.id, "ingest receive: delivery cannot be processed — not retrying");
            queue::complete_with_payload(pool, job.id, JobOutcome::NothingToDo, redacted(job.payload)).await
        }
    };
    if let Err(e) = settled {
        tracing::error!(error = %e, job_id = %job.id, "ingest receive: failed to settle the job");
    }
}

/// What a settled receive job's payload keeps in place of the fields that
/// must not outlive the job.
const REDACTED: &str = "[redacted on completion]";

/// The fields of a receive payload that must not outlive the job, as JSON
/// paths: the Gmail mailbox address, a Workspace event's message data, a
/// Graph `clientState`. One list for the worker's own settle
/// ([`redacted`]) and for the reaper's dead-letter statement
/// ([`REAP_REDACTION`]), so the two cannot drift.
const REDACTED_PATHS: &[&[&str]] = &[&["email_address"], &["envelope", "message", "data"], &["notification", "clientState"]];

/// The redaction the reaper applies, in the same `UPDATE` that dead-letters
/// a receive job whose worker died mid-job (`workers::reaper` hands it to
/// [`queue::reap`]): such a job never ran its own completion path, and a
/// separate statement afterwards could be lost to a crash in between,
/// leaving the mailbox address, message data or `clientState` on the
/// terminal row for good. A reaped job that goes back to `pending` keeps
/// its payload for the retry.
pub(crate) const REAP_REDACTION: DeadPayloadRedaction =
    DeadPayloadRedaction { kinds: &[KIND], paths: REDACTED_PATHS, replacement: REDACTED };

/// `payload` with every [`REDACTED_PATHS`] field present replaced by
/// [`REDACTED`]; a missing field is not added.
fn redacted(mut payload: serde_json::Value) -> serde_json::Value {
    for path in REDACTED_PATHS {
        let pointer = path.iter().fold(String::new(), |mut p, seg| {
            p.push('/');
            p.push_str(seg);
            p
        });
        if let Some(field) = payload.pointer_mut(&pointer) {
            *field = REDACTED.into();
        }
    }
    payload
}

/// Run every due receive job on the consumer pool until none is left; how
/// many ran.
#[cfg(test)]
pub(crate) async fn drain(state: &Arc<AppState>, gmail_pass: &google_push::GmailPass) -> usize {
    let t = WorkerTenant { tenant: TenantId::Consumer, pool: state.db.clone().unwrap(), store: state.store.clone().unwrap() };
    let mut ran = 0;
    loop {
        let jobs = queue::dequeue(&t.pool, &[KIND], JOB_BATCH_SIZE).await.unwrap();
        if jobs.is_empty() {
            return ran;
        }
        for job in jobs {
            ran += 1;
            process_job(&t, state, job, gmail_pass).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_receive_job_round_trips_through_the_jobs_columns() {
        let kind = JobKind::IngestReceive(ReceivedEvent::GmailPush { email_address: "a@example.com".into() });
        let tagged = serde_json::to_value(&kind).unwrap();
        assert_eq!(tagged["kind"], "IngestReceive");
        assert_eq!(tagged["payload"], serde_json::json!({"provider": "gmail_push", "email_address": "a@example.com"}));
        assert!(matches!(
            serde_json::from_value::<JobKind>(tagged).unwrap(),
            JobKind::IngestReceive(ReceivedEvent::GmailPush { email_address }) if email_address == "a@example.com"
        ));
    }

    #[test]
    fn completion_scrubs_the_mailbox_message_data_and_client_state_only() {
        let gmail = redacted(serde_json::json!({"provider": "gmail_push", "email_address": "a@example.com"}));
        assert_eq!(gmail, serde_json::json!({"provider": "gmail_push", "email_address": "[redacted on completion]"}));
        let workspace = redacted(serde_json::json!({"provider": "google_workspace_event", "envelope": {
            "subscription": "projects/p/subscriptions/chat-events-sub",
            "message": {"data": "c2VjcmV0", "messageId": "m-1"},
        }}));
        assert_eq!(workspace["envelope"]["message"]["data"], "[redacted on completion]");
        assert_eq!(workspace["envelope"]["message"]["messageId"], "m-1");
        let graph = redacted(serde_json::json!({"provider": "graph_notification", "notification": {
            "subscriptionId": "s", "clientState": "secret", "resourceData": {"id": "m"},
        }}));
        assert_eq!(graph["notification"]["clientState"], "[redacted on completion]");
        assert_eq!(graph["notification"]["subscriptionId"], "s");
        let no_data = redacted(serde_json::json!({"provider": "google_workspace_event", "envelope": {"message": {}}}));
        assert!(no_data["envelope"]["message"].get("data").is_none(), "nothing is added");
        let no_state = redacted(serde_json::json!({"provider": "graph_notification", "notification": {"subscriptionId": "s"}}));
        assert!(no_state["notification"].get("clientState").is_none(), "nothing is added");
    }

    /// The real failures the provider clients build (`microsoft::graph_get`,
    /// `google::google_api_error`, `chat_app_identity`, the subscription
    /// delete, the targeted transcript resolver), classified by the status
    /// they carry as a field, whatever the message says; a failure with no
    /// status, and a storage failure, retry.
    #[test]
    fn upstream_failures_retry_on_throttling_and_server_errors_only() {
        let upstream = |status: u16, msg: &str| ConnectorError::upstream_code(status, msg);
        for (status, permanent) in [
            (404, "graph GET https://graph.microsoft.com/v1.0/me/messages/m failed: 404 Not Found: {\"error\":{}}"),
            (403, "graph GET https://graph.microsoft.com/v1.0/teams/t/channels/c/messages/m failed: 403 Forbidden: "),
            (400, "graph GET https://graph.microsoft.com/v1.0/me/messages/m failed: 400 Bad Request: "),
            (401, "gmail history.list failed: 401 Unauthorized: status=UNAUTHENTICATED"),
            (410, "gmail messages.get failed: 410 Gone: "),
            (403, "gmail history.list failed: 403 Forbidden: reasons=[insufficientPermissions] status=PERMISSION_DENIED"),
            // Phrased without `failed: NNN`: the status is still the field.
            (403, "Microsoft rejected this meeting request (403)"),
            (404, "subscription delete: 404 Not Found"),
            (403, "spaces.messages.create status 403"),
            (409, "subscription create failed: 409 Conflict: already exists"),
        ] {
            assert!(!transient(&upstream(status, permanent)), "{status} {permanent}");
        }
        for (status, retry) in [
            (503, "graph GET https://graph.microsoft.com/v1.0/me/messages/m failed: 503 Service Unavailable: "),
            (429, "graph GET https://graph.microsoft.com/v1.0/me/messages/m failed: 429 Too Many Requests: "),
            (500, "gmail history.list failed: 500 Internal Server Error: "),
            (403, "gmail history.list failed: 403 Forbidden: reasons=[rateLimitExceeded,RATE_LIMIT_EXCEEDED] status=PERMISSION_DENIED"),
            (408, "graph GET https://graph.microsoft.com/v1.0/me/messages/m failed: 408 Request Timeout: "),
            (502, "Microsoft rejected this meeting request (502)"),
            (503, "subscription delete: 503 Service Unavailable"),
        ] {
            assert!(transient(&upstream(status, retry)), "{status} {retry}");
        }
        // No status: whatever the text says, including a status someone
        // wrote into it, it is not read back out.
        for no_status in [
            "graph request error: connection reset by peer",
            "graph parse: EOF while parsing a value",
            "gmail history.list failed: 503",
            "gmail history.list failed: 404 Not Found",
        ] {
            let e = ConnectorError::Exchange(no_status.into());
            assert_eq!(e.upstream_status(), None, "{no_status}");
            assert!(transient(&e), "{no_status}");
        }
        assert_eq!(upstream(404, "x").upstream_status(), Some(404));
        assert_eq!(
            ConnectorError::upstream(reqwest::StatusCode::NOT_FOUND, "gmail messages.get failed: 404 Not Found").to_string(),
            "upstream exchange failed: gmail messages.get failed: 404 Not Found",
            "the message shape is unchanged for logs and `last_error`"
        );
        assert!(transient(&ConnectorError::Internal("db".into())));
        assert!(!transient(&ConnectorError::NotConfigured("gmail not connected".into())));
        assert!(!transient(&ConnectorError::NotImplemented("x".into())));
        assert!(!transient(&ConnectorError::CredentialRevoked {
            provider: "google",
            reason: crate::connectors::CredentialDeathReason::GoogleInvalidGrant,
        }));
    }

    /// Permanent failures alone complete the fan-out; one transient failure
    /// fails it, with the first transient one as the error.
    #[test]
    fn a_fan_out_fails_only_on_a_transient_failure() {
        let mut only_permanent = FanOut::new();
        only_permanent.failed(ConnectorError::upstream_code(403, "forbidden"), |_, retry| assert!(!retry));
        only_permanent.failed(ConnectorError::NotConfigured("gone".into()), |_, retry| assert!(!retry));
        assert_eq!(only_permanent.finish(3usize).unwrap(), 3);

        let mut mixed = FanOut::new();
        mixed.failed(ConnectorError::upstream_code(404, "gone"), |_, retry| assert!(!retry));
        mixed.failed(ConnectorError::upstream_code(503, "first transient"), |_, retry| assert!(retry));
        mixed.failed(ConnectorError::Internal("second transient".into()), |_, retry| assert!(retry));
        let err = mixed.finish(0usize).unwrap_err();
        assert!(matches!(err, ConnectorError::UpstreamStatus { status: 503, .. }), "{err}");

        assert_eq!(FanOut::new().finish(1usize).unwrap(), 1, "no failure at all");
    }

    #[test]
    fn a_malformed_delivery_never_retries() {
        assert!(!ReceiveError::Malformed("bad envelope".into()).retryable());
        assert!(ReceiveError::from(ConnectorError::Internal("db".into())).retryable());
        assert_eq!(ReceiveError::Malformed("bad envelope".into()).to_string(), "malformed delivery: bad envelope");
    }

    /// One classifier for every provider's tenancy-plane failure: a lookup
    /// or pool failure retries, an account that is gone, a frozen tenant or
    /// a routing row for another account completes.
    #[test]
    fn a_plane_failure_retries_only_when_a_retry_can_change_it() {
        let account = uuid::Uuid::new_v4();
        for permanent in [
            DekPlaneError::AccountNotFound(account),
            DekPlaneError::TenantNotWritable { org_id: "org_1".into(), status: "suspended".into() },
            DekPlaneError::AccountMismatch { org_id: "org_1".into(), row_account: uuid::Uuid::new_v4(), requested: account },
        ] {
            let text = permanent.to_string();
            let e = plane_error(permanent);
            assert!(!transient(&e), "{e}");
            assert!(matches!(&e, ConnectorError::NotConfigured(m) if m.contains(&text)), "{e}");
        }
        for retry in [
            DekPlaneError::AccountLookup("timed out".into()),
            DekPlaneError::RoutingLookup("connection reset".into()),
            DekPlaneError::PoolBuild("fingerprint mismatch".into()),
        ] {
            let text = retry.to_string();
            let e = plane_error(retry);
            assert!(transient(&e), "{e}");
            assert!(matches!(&e, ConnectorError::Internal(m) if m.contains(&text)), "{e}");
        }
    }

    /// The receive budget on the queue's own ladder: nine attempts wait
    /// 15 300 s between them, four hours and a quarter, where the table
    /// default of three dead-letters after three minutes.
    #[test]
    fn the_receive_budget_outlasts_an_outage_of_hours() {
        let budget = JobKind::IngestReceive(ReceivedEvent::GmailPush { email_address: "a@example.com".into() }).max_attempts();
        assert_eq!(budget, 9);
        let window = |max_attempts: i32| (0..max_attempts).map_while(|a| queue::backoff_secs(a, max_attempts)).sum::<i64>();
        assert_eq!(window(budget), 15_300);
        assert_eq!(window(queue::DEFAULT_MAX_ATTEMPTS), 180);
        assert_eq!(
            JobKind::GenerateMeetingSummary { graph_id: uuid::Uuid::new_v4(), source_id: "m".into(), allow_cloud_override: false }
                .max_attempts(),
            queue::DEFAULT_MAX_ATTEMPTS
        );
    }
}

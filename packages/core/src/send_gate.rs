//! Per-attempt admission for paid provider sends (COR-21).
//!
//! Every HTTP request to a paid provider is preceded by exactly one
//! [`acquire`] whose [`SendPermit`] that request consumes, and followed by
//! exactly one [`settle`] that reports what the attempt cost. A caller with
//! no money gate passes [`Unmanaged`] explicitly; there is no default.
//!
//! A permit cannot be minted any other way: its fields are private and
//! [`acquire`] is the only constructor.
//!
//! ```compile_fail
//! use coralswarm_core::send_gate::{SendPermit, Unmanaged};
//! async fn mint(_gate: &Unmanaged) -> SendPermit {
//!     SendPermit { receipt_id: None, attempt_no: 1 }
//! }
//! ```
//!
//! ```
//! use coralswarm_core::send_gate::{SendPermit, Unmanaged};
//! async fn mint(gate: &Unmanaged) -> SendPermit {
//!     coralswarm_core::send_gate::acquire(gate, 1).await.unwrap()
//! }
//! ```

use std::cell::RefCell;
use std::future::Future;
use std::pin::Pin;

use uuid::Uuid;

pub type GateFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// The terms a gate priced. The transport sends exactly these:
/// `model = models[0]`, the fallback list = `models[1..]`.
#[derive(Clone, Debug, PartialEq)]
pub struct SendTerms {
    pub models: Vec<String>,
    pub max_price: Option<MaxPrice>,
}

/// USD per million tokens, the OpenRouter `provider.max_price` wire unit.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct MaxPrice {
    pub prompt: f64,
    pub completion: f64,
}

/// What a gate returns from [`SendGate::reserve`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Reservation {
    pub receipt_id: Option<Uuid>,
}

/// Proof that a gate admitted exactly one attempt.
#[must_use = "a permit admits one send; drop it only if nothing is sent"]
#[derive(Debug)]
pub struct SendPermit {
    receipt_id: Option<Uuid>,
    attempt_no: u32,
}

/// The permit after its request was handed to the network. Only [`settle`]
/// consumes it. Dropped unsettled (cancellation, panic, a failed settle), the
/// gate's reaper settles it as ambiguous.
#[must_use = "a sent attempt must be settled"]
#[derive(Debug)]
pub struct Sent {
    receipt_id: Option<Uuid>,
    attempt_no: u32,
}

impl SendPermit {
    /// Called by the transport immediately before the request is sent.
    pub fn into_sent(self) -> Sent {
        Sent {
            receipt_id: self.receipt_id,
            attempt_no: self.attempt_no,
        }
    }

    pub fn attempt_no(&self) -> u32 {
        self.attempt_no
    }
}

impl Sent {
    pub fn receipt_id(&self) -> Option<Uuid> {
        self.receipt_id
    }

    pub fn attempt_no(&self) -> u32 {
        self.attempt_no
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum SendOutcome {
    /// The provider answered with usage, so it generated and billed.
    /// `reported_cost_usd` is the provider's own figure when it sends one.
    Billed {
        reported_cost_usd: Option<f64>,
        input_tokens: u64,
        output_tokens: u64,
        served_model: String,
        upstream: Option<String>,
        generation_id: Option<String>,
    },
    /// An HTTP answer with no generation, or a request that was never written.
    NotGenerated,
    /// The request was written, then no usable answer came back.
    Ambiguous { generation_id: Option<String> },
}

impl SendOutcome {
    pub fn reported_cost_usd(&self) -> Option<f64> {
        match self {
            SendOutcome::Billed {
                reported_cost_usd, ..
            } => *reported_cost_usd,
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum Deny {
    /// The gate recorded a hold; retry later.
    #[error("held: {}", .0.code())]
    Hold(HoldCode),
    /// Terminal for this subject.
    #[error("refused: {}", .0.code())]
    Refuse(RefuseCode),
    /// The gate could not decide (storage). Nothing was sent.
    #[error("gate unavailable")]
    Unavailable,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HoldCode {
    ProcessingDisabled,
    CreditsExhausted,
    LegacyManagedLiabilitiesPending,
    DailyBudgetExhausted,
    Fenced,
    PrincipalUnavailable,
    RequesterAccessLost,
    CloudProcessingDisabled,
    SourceRemovedOrAccessLost,
    ManagedRunPaused,
    LifecycleContractNotReady,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RefuseCode {
    UnpricedModel,
    CallCostCap,
    VersionCostCap,
    Ungated,
}

impl HoldCode {
    pub const fn code(self) -> &'static str {
        match self {
            HoldCode::ProcessingDisabled => "managed_processing_disabled",
            HoldCode::CreditsExhausted => "credits_exhausted",
            HoldCode::LegacyManagedLiabilitiesPending => "legacy_managed_liabilities_pending",
            HoldCode::DailyBudgetExhausted => "daily_budget_exhausted",
            HoldCode::Fenced => "fenced",
            HoldCode::PrincipalUnavailable => "principal_unavailable",
            HoldCode::RequesterAccessLost => "requester_access_lost",
            HoldCode::CloudProcessingDisabled => "cloud_processing_disabled",
            HoldCode::SourceRemovedOrAccessLost => "source_removed_or_access_lost",
            HoldCode::ManagedRunPaused => "managed_run_paused",
            HoldCode::LifecycleContractNotReady => "lifecycle_contract_not_ready",
        }
    }
}

impl RefuseCode {
    pub const fn code(self) -> &'static str {
        match self {
            RefuseCode::UnpricedModel => "unpriced_model",
            RefuseCode::CallCostCap => "call_cost_cap",
            RefuseCode::VersionCostCap => "version_cost_cap",
            RefuseCode::Ungated => "provider_not_gated",
        }
    }
}

/// How the transport runs each attempt a gate admits. The default leaves the
/// transport as it is: its client's total timeout, a blocking read, no
/// provider sort.
///
/// Every bound here ends an attempt INSIDE the transport, so the attempt is
/// settled where it ran. A caller that drops the future instead leaves its
/// receipt to the reaper, which charges it in full ten minutes later.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct TransportPolicy {
    /// Wall-clock bound on one attempt, from send to the last byte. Replaces
    /// the client's own total timeout for this request.
    pub attempt_timeout: Option<std::time::Duration>,
    /// Read the completion as a server-sent-event stream, cut only when no
    /// event arrives for this long. A stream names its generation id in its
    /// first event, so an attempt cut later is settled with that id.
    pub stream_stall: Option<std::time::Duration>,
    /// No attempt starts at or after this instant, and a running attempt is
    /// cut there.
    pub deadline: Option<std::time::Instant>,
    /// OpenRouter `provider.sort` (`"throughput"`, `"latency"`, `"price"`).
    /// An ordering only: the provider policy and the priced terms still
    /// bound which hosts and models may serve.
    pub provider_sort: Option<&'static str>,
}

impl TransportPolicy {
    /// Whether this policy, rather than the client, bounds an attempt. An
    /// attempt cut by such a bound was sent and is charged in full, and the
    /// same request against the same bound mostly fails the same way, so the
    /// transport never retries it; the caller owns that retry.
    pub fn bounds_attempts(&self) -> bool {
        self.attempt_timeout.is_some() || self.stream_stall.is_some() || self.deadline.is_some()
    }
}

pub trait SendGate: Send + Sync {
    /// The priced terms the transport must send, or `None` for its defaults.
    fn terms(&self) -> Option<&SendTerms>;
    /// How the transport runs each attempt. Defaults to the transport's own
    /// behaviour.
    fn transport(&self) -> TransportPolicy {
        TransportPolicy::default()
    }
    fn reserve<'a>(&'a self, attempt_no: u32) -> GateFuture<'a, Result<Reservation, Deny>>;
    fn settle<'a>(&'a self, sent: Sent, outcome: &'a SendOutcome) -> GateFuture<'a, ()>;
}

/// The explicit choice for a caller with no money gate.
#[derive(Clone, Copy, Debug, Default)]
pub struct Unmanaged;

impl SendGate for Unmanaged {
    fn terms(&self) -> Option<&SendTerms> {
        None
    }

    fn reserve<'a>(&'a self, _attempt_no: u32) -> GateFuture<'a, Result<Reservation, Deny>> {
        Box::pin(async { Ok(Reservation::default()) })
    }

    fn settle<'a>(&'a self, _sent: Sent, _outcome: &'a SendOutcome) -> GateFuture<'a, ()> {
        Box::pin(async {})
    }
}

pub async fn acquire(gate: &dyn SendGate, attempt_no: u32) -> Result<SendPermit, Deny> {
    let reservation = gate.reserve(attempt_no).await?;
    Ok(SendPermit {
        receipt_id: reservation.receipt_id,
        attempt_no,
    })
}

/// Appends to the attempt log (if a scope is open), then settles at the gate.
/// Transports call only this.
pub async fn settle(gate: &dyn SendGate, sent: Sent, outcome: SendOutcome) {
    let _ = ATTEMPT_LOG.try_with(|log| {
        log.borrow_mut().push(AttemptRecord {
            attempt_no: sent.attempt_no,
            outcome: outcome.clone(),
        })
    });
    gate.settle(sent, &outcome).await;
}

#[derive(Clone, Debug, PartialEq)]
pub struct AttemptRecord {
    pub attempt_no: u32,
    pub outcome: SendOutcome,
}

tokio::task_local! {
    static ATTEMPT_LOG: RefCell<Vec<AttemptRecord>>;
}

/// Runs `f` with a fresh attempt log and returns every attempt the transport
/// settled while it ran, in order. The log is task-local, so any wrapper
/// running `f` in the same task cannot lose it.
pub async fn with_attempt_log<F: Future>(f: F) -> (F::Output, Vec<AttemptRecord>) {
    ATTEMPT_LOG
        .scope(RefCell::new(Vec::new()), async {
            let out = f.await;
            let log = ATTEMPT_LOG.with(|log| log.take());
            (out, log)
        })
        .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Mutex;

    struct Counting {
        reserves: AtomicU32,
        settled: Mutex<Vec<(u32, SendOutcome)>>,
        deny_on: Option<u32>,
    }

    impl SendGate for Counting {
        fn terms(&self) -> Option<&SendTerms> {
            None
        }
        fn reserve<'a>(&'a self, attempt_no: u32) -> GateFuture<'a, Result<Reservation, Deny>> {
            Box::pin(async move {
                self.reserves.fetch_add(1, Ordering::SeqCst);
                if self.deny_on == Some(attempt_no) {
                    return Err(Deny::Hold(HoldCode::DailyBudgetExhausted));
                }
                Ok(Reservation {
                    receipt_id: Some(Uuid::nil()),
                })
            })
        }
        fn settle<'a>(&'a self, sent: Sent, outcome: &'a SendOutcome) -> GateFuture<'a, ()> {
            Box::pin(async move {
                self.settled
                    .lock()
                    .unwrap()
                    .push((sent.attempt_no(), outcome.clone()));
            })
        }
    }

    #[tokio::test]
    async fn settle_reaches_the_gate_and_the_open_log() {
        let gate = Counting {
            reserves: AtomicU32::new(0),
            settled: Mutex::new(Vec::new()),
            deny_on: Some(3),
        };
        let ((), log) = with_attempt_log(async {
            for n in 1..=2 {
                let permit = acquire(&gate, n).await.unwrap();
                assert_eq!(permit.attempt_no(), n);
                settle(&gate, permit.into_sent(), SendOutcome::NotGenerated).await;
            }
            assert_eq!(
                acquire(&gate, 3).await.unwrap_err(),
                Deny::Hold(HoldCode::DailyBudgetExhausted)
            );
        })
        .await;
        assert_eq!(gate.reserves.load(Ordering::SeqCst), 3);
        assert_eq!(gate.settled.lock().unwrap().len(), 2);
        assert_eq!(
            log.iter().map(|r| r.attempt_no).collect::<Vec<_>>(),
            vec![1, 2]
        );
    }

    #[tokio::test]
    async fn settle_outside_a_log_scope_is_harmless() {
        let permit = acquire(&Unmanaged, 1).await.unwrap();
        settle(&Unmanaged, permit.into_sent(), SendOutcome::NotGenerated).await;
    }

    #[test]
    fn the_default_policy_leaves_the_transport_alone() {
        assert_eq!(Unmanaged.transport(), TransportPolicy::default());
        assert!(!TransportPolicy::default().bounds_attempts());
        let streamed = TransportPolicy {
            stream_stall: Some(std::time::Duration::from_secs(1)),
            ..Default::default()
        };
        assert!(streamed.bounds_attempts());
        let sorted = TransportPolicy {
            provider_sort: Some("throughput"),
            ..Default::default()
        };
        assert!(!sorted.bounds_attempts(), "an ordering bounds nothing");
    }

    #[test]
    fn codes_are_the_persisted_reason_strings() {
        assert_eq!(HoldCode::DailyBudgetExhausted.code(), "daily_budget_exhausted");
        assert_eq!(RefuseCode::Ungated.code(), "provider_not_gated");
        assert_eq!(
            Deny::Refuse(RefuseCode::CallCostCap).to_string(),
            "refused: call_cost_cap"
        );
    }
}

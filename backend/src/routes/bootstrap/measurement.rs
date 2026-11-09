//! One bounded record for one bootstrap invocation. No input-derived strings,
//! identifiers, payloads, errors, or global registries cross this boundary.
use std::{cell::RefCell, future::Future};

use axum::http::StatusCode;
use serde::Serialize;
use tokio::time::Instant;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Outcome {
    #[default]
    NotReached,
    Ok,
    Skipped,
    Timeout,
    Refused,
    Error,
    Cancelled,
}

impl Outcome {
    pub(crate) fn status(status: StatusCode) -> Self {
        if status.is_success() {
            Self::Ok
        } else if status.is_client_error() {
            Self::Refused
        } else {
            Self::Error
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Serialize)]
pub(super) struct Timing {
    pub start_us: Option<u64>,
    pub end_us: Option<u64>,
    pub elapsed_us: Option<u64>,
    pub executed: bool,
    pub outcome: Outcome,
}

#[derive(Clone, Copy)]
pub(crate) enum Phase {
    Me,
    Graphs,
    AdminJoinCount,
    PendingOrgs,
    SelfserveOrg,
    MyJoinRequests,
    BillingBanner,
    BillingDb,
    BillingProvider,
}

#[derive(Default, Serialize)]
pub(super) struct Branches {
    pub me: Timing,
    pub graphs: Timing,
    pub admin_join_count: Timing,
    pub pending_orgs: Timing,
    pub selfserve_org: Timing,
    pub my_join_requests: Timing,
    pub billing_banner: Timing,
}

#[derive(Default, Serialize)]
pub(super) struct Billing {
    pub db: Timing,
    pub provider: Timing,
}

#[derive(Clone, Copy, Default, Serialize)]
#[serde(rename_all = "snake_case")]
enum Role {
    Owner,
    Admin,
    Member,
    #[default]
    Other,
}

#[derive(Default, Serialize)]
pub(super) struct Record {
    schema_version: u8,
    role: Role,
    pub branches: Branches,
    pub billing: Billing,
    pub required_pair_ready_us: u64,
    pub required_pair_ok: bool,
    pub assembly_start_us: u64,
    pub assembly_end_us: u64,
    pub assembly_elapsed_us: u64,
    pub complete_elapsed_us: u64,
    pub outcome: Outcome,
}

struct Measurement {
    start: Instant,
    record: Record,
}

tokio::task_local! { static CURRENT: RefCell<Measurement>; }

impl Measurement {
    fn elapsed_us(&self) -> u64 {
        self.start.elapsed().as_micros().min(u64::MAX as u128) as u64
    }
    fn phase(&mut self, phase: Phase) -> &mut Timing {
        match phase {
            Phase::Me => &mut self.record.branches.me,
            Phase::Graphs => &mut self.record.branches.graphs,
            Phase::AdminJoinCount => &mut self.record.branches.admin_join_count,
            Phase::PendingOrgs => &mut self.record.branches.pending_orgs,
            Phase::SelfserveOrg => &mut self.record.branches.selfserve_org,
            Phase::MyJoinRequests => &mut self.record.branches.my_join_requests,
            Phase::BillingBanner => &mut self.record.branches.billing_banner,
            Phase::BillingDb => &mut self.record.billing.db,
            Phase::BillingProvider => &mut self.record.billing.provider,
        }
    }
}

pub(super) async fn scope<T>(role: &str, future: impl Future<Output = T>) -> (T, Record) {
    let measurement = Measurement {
        start: Instant::now(),
        record: Record {
            schema_version: 1,
            role: match role {
                "owner" => Role::Owner,
                "admin" => Role::Admin,
                "member" => Role::Member,
                _ => Role::Other,
            },
            ..Record::default()
        },
    };
    CURRENT
        .scope(RefCell::new(measurement), async {
            let output = future.await;
            let record = CURRENT.with(|cell| {
                let mut m = cell.borrow_mut();
                m.record.complete_elapsed_us = m.elapsed_us();
                std::mem::take(&mut m.record)
            });
            (output, record)
        })
        .await
}

/// A subphase is inert outside bootstrap; in particular standalone billing
/// does not allocate a record or emit another event. Drop marks cancellation
/// when the existing outer optional deadline drops a running provider/DB read.
pub(crate) struct Guard {
    phase: Phase,
    active: bool,
}
impl Guard {
    pub fn start(phase: Phase) -> Self {
        let active = CURRENT
            .try_with(|cell| {
                let mut m = cell.borrow_mut();
                let now = m.elapsed_us();
                *m.phase(phase) = Timing {
                    start_us: Some(now),
                    executed: true,
                    ..Timing::default()
                };
            })
            .is_ok();
        Self { phase, active }
    }
    pub fn finish(&mut self, outcome: Outcome) {
        if !self.active {
            return;
        }
        let _ = CURRENT.try_with(|cell| {
            let mut m = cell.borrow_mut();
            let now = m.elapsed_us();
            let timing = m.phase(self.phase);
            timing.end_us = Some(now);
            timing.elapsed_us = Some(now.saturating_sub(timing.start_us.unwrap_or(now)));
            timing.executed = outcome != Outcome::Skipped;
            timing.outcome = outcome;
        });
        self.active = false;
    }
}
impl Drop for Guard {
    fn drop(&mut self) {
        self.finish(Outcome::Cancelled);
    }
}

pub(crate) fn skipped(phase: Phase) {
    Guard::start(phase).finish(Outcome::Skipped);
}

pub(super) fn assembly_start(required_pair_ok: bool) {
    CURRENT.with(|cell| {
        let mut m = cell.borrow_mut();
        m.record.required_pair_ready_us = m
            .record
            .branches
            .me
            .end_us
            .unwrap()
            .max(m.record.branches.graphs.end_us.unwrap());
        m.record.required_pair_ok = required_pair_ok;
        m.record.assembly_start_us = m.elapsed_us();
    });
}

pub(super) fn assembly_end(status: StatusCode) {
    CURRENT.with(|cell| {
        let mut m = cell.borrow_mut();
        m.record.assembly_end_us = m.elapsed_us();
        m.record.assembly_elapsed_us = m
            .record
            .assembly_end_us
            .saturating_sub(m.record.assembly_start_us);
        m.record.outcome = Outcome::status(status);
    });
}

pub(super) fn emit(record: &Record) {
    // Serialization cannot encounter a dynamic value or a nonfinite float.
    tracing::info!(target: "bootstrap", record = %serde_json::to_string(record).expect("fixed bootstrap timing schema"), "bootstrap completed");
}

pub(super) fn count_decoded(outcome: Outcome) {
    CURRENT.with(|cell| cell.borrow_mut().record.branches.admin_join_count.outcome = outcome);
}

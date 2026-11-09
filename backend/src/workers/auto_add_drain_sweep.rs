//! The auto-add drain's own safety-net sweep — CS-449 (org-onboarding plan
//! §3 item 38, D11's "ask once", and the drain).
//!
//! # This worker is a safety net, not the primary trigger
//!
//! [`crate::routes::org_join_requests::drain_org`] is also called
//! synchronously, right after the two events that actually free or grow a
//! slot:
//!   * `clerk_provision::remove_org_membership`, right after
//!     `billing::seat_lifecycle::release_seat` frees one, and
//!   * `routes::org_slots::purchase_slots`, right after a purchase commits
//!     and grows the pool.
//!
//! Those two calls are what make a drain feel immediate. This worker exists
//! for everything they cannot cover: a reservation that simply EXPIRES
//! (`workers::slot_reservations::tick`, which frees a slot with no admin
//! action and no webhook to hang a call off of), a race between two
//! near-simultaneous events, or either trigger call itself failing —
//! `drain_org` is best-effort at both of its other call sites, by design
//! (access revocation and a Stripe purchase must never wait on, or fail
//! over, a drain). A `pool_full` backlog left behind by any of those is
//! picked up here within one sweep interval at the latest — see [`tick`]'s
//! own doc comment ("item 8 fix") for what actually makes that true: this
//! worker reports non-idle for as long as a backlog remains, so the org's
//! poll cadence never backs off away from `sweep_interval` while there is
//! still something here to catch.
//!
//! # Shape: `tenant_harness` per-org worker, same as `org_join_request_digest`
//!
//! Registered with [`crate::workers::tenant_harness::HarnessConfig::
//! sweep_worker`], and [`tick`] takes the identical extra `state: &Arc<
//! AppState>` parameter `workers::org_join_request_digest::tick` does — both
//! need it for the same reason: [`crate::routes::org_join_requests::
//! try_auto_add`] provisions a real Clerk membership, several calls deep.
//! `TenantId::Consumer` is a clean no-op (auto-add is an org-only feature;
//! no ordinary org lives on the shared personal-account plane), and every
//! `TenantId::Org` tick resolves its own `account_id` and calls
//! [`crate::routes::org_join_requests::drain_org`] once — which itself
//! reads `org_auto_add_settings` first and returns immediately for the
//! (overwhelmingly common) org that has never turned auto-add on, so a tick
//! against a non-participating org costs one indexed settings read.

use std::sync::Arc;
use std::time::Duration;

use sqlx::PgPool;

use crate::tenancy::TenantId;
use crate::workers::billing_derivation::resolve_org_account_id;
use crate::workers::tenant_harness::{Tick, WorkerTenant};
use crate::AppState;

/// Default sweep cadence — deliberately close to
/// [`crate::workers::slot_reservations::DEFAULT_SWEEP_INTERVAL_SECS`] (60s):
/// this worker exists largely to catch what THAT sweep's own expiries free,
/// so lagging far behind it would leave a freed slot sitting unclaimed for
/// no reason. Same `_INTERVAL_SECS` env-override convention as every other
/// sweep worker in this directory.
pub const DEFAULT_INTERVAL_SECS: u64 = 90;

pub fn sweep_interval() -> Duration {
    std::env::var("AUTO_ADD_DRAIN_SWEEP_INTERVAL_SECS")
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .filter(|secs| *secs > 0)
        .map(Duration::from_secs)
        .unwrap_or(Duration::from_secs(DEFAULT_INTERVAL_SECS))
}

/// One pass for one [`WorkerTenant`] — see the module doc's "Shape"
/// section for why every `TenantId::Consumer` tick is a clean no-op.
///
/// # Item 8 fix, CS-449 review round 2 — a `pool_full` backlog must not
/// # count as "idle"
///
/// [`Tick::idle()`] lets `tenant_harness`'s scheduler back this org's poll
/// interval off toward `HarnessConfig::max_org_idle_backoff`
/// (`max(sweep_interval, DORMANT_POLL_PERIOD)` — 20 minutes at this
/// worker's own 90s interval, `HarnessConfig::sweep_worker`'s own doc
/// comment). This module's doc comment promises "a `pool_full` backlog … is
/// picked up here within one sweep interval at the latest" — a promise
/// `drain_org` returning zero admits on one pass (the pool is still full,
/// or every remaining candidate refuses for its own reason) used to break:
/// the org would back off, and a reservation expiry this worker exists to
/// catch could sit unclaimed for up to 20 minutes instead of 90 seconds.
///
/// Fixed by treating `report.not_admitted` — the leftover backlog THIS pass
/// re-gated but could not admit — as sweep activity, the same way
/// `Tick::worked`'s own doc comment already generalises "jobs (OR SWEEP
/// ITEMS) processed": a candidate this tick looked at and could not yet
/// clear is exactly the kind of item a sweep keeps polling for. Only once
/// BOTH `admitted` and `not_admitted` are empty — a drain that found no
/// candidates at all, i.e. the org's `pool_full` queue is genuinely, fully
/// empty — does this report idle and let the org back off. [`drain_org`]'s
/// own item-9 fix (permanently-doomed stale-classification rows leave the
/// candidate set once observed) is what keeps this from becoming a
/// backlog-of-one that never lets an org idle again: without it, a single
/// structurally-unpassable row would report non-idle FOREVER and this
/// worker would never back off for that org at all.
pub async fn tick(t: &WorkerTenant, control: &PgPool, state: &Arc<AppState>) -> Result<Tick, String> {
    let clerk_org_id = match &t.tenant {
        TenantId::Consumer => return Ok(Tick::idle()),
        TenantId::Org(id) => id.clone(),
    };

    let account_id = resolve_org_account_id(control, &clerk_org_id).await?;
    let report = crate::routes::org_join_requests::drain_org(state, control, account_id, &clerk_org_id).await;

    if !report.admitted.is_empty() {
        tracing::info!(
            account_id = %account_id, clerk_org_id = %clerk_org_id,
            admitted = report.admitted.len(), not_admitted = report.not_admitted.len(),
            "auto_add_drain_sweep: admitted from the pool_full backlog"
        );
    }

    Ok(if report.admitted.is_empty() && report.not_admitted.is_empty() {
        Tick::idle()
    } else {
        Tick::worked(report.admitted.len() + report.not_admitted.len())
    })
}

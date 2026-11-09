//! Self-serve tenant provisioning worker (org signup PR A1b): drives
//! `tenancy::provisioning` jobs on a control-plane poll.
//!
//! Idles — one `info!` at boot and no database access — unless
//! `ORG_SELFSERVE_SIGNUP_ENABLED` is set, so a deploy with the flag unset
//! changes nothing for any existing tenant. The flag is re-read every tick
//! so it can be flipped by a secrets change (which restarts the fleet anyway)
//! without a code path change.
//!
//! Spawned from `lib.rs::run` only when the pieces it needs exist: a
//! control pool, the `TenantRuntime` (its registry builds the tenant pool
//! for the DEK step), `MASTER_KEK_HEX`, a provisioner
//! (`TENANT_PROVISIONER=neon` + `NEON_API_KEY`, or `TENANT_PROVISIONER=local`
//! + `TENANT_PROVISIONER_ADMIN_DSN` on a dev box), `CLERK_SECRET_KEY`, and
//! `PROVISIONER_DATABASE_URL` (a `coralswarm_provisioner` login: the
//! serving role cannot advance a job or write routing, control 0381). Each
//! missing piece is a `warn!` at boot and the worker is not spawned — never a
//! half-configured run. A missing `PROVISIONER_DATABASE_URL` with the flag on
//! is an alertable `error!`: provisioning was asked for and cannot run.

use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::time::Duration;

use futures::FutureExt;
use sqlx::PgPool;

use crate::clerk_admin::ClerkAdminClient;
use crate::shutdown::Shutdown;
use crate::tenancy::provisioner::{LocalPgProvisioner, NeonProvisioner, TenantDbProvisioner};
use crate::tenancy::provisioning::{self, Deps, RetryPolicy, RunOutcome, PROVISIONER_DATABASE_URL};
use crate::tenancy::runtime::TenantRuntime;
use crate::tenancy::secrets::SecretResolver;

const POLL: Duration = Duration::from_secs(5);
const IDLE_POLL: Duration = Duration::from_secs(60);

/// The worker's own control pool: a `coralswarm_provisioner` login, from
/// [`PROVISIONER_DATABASE_URL`]. Lazy (no connection at boot) and small: one
/// job runs at a time. `Ok(None)` when the variable is unset or empty;
/// `Err` (never naming the DSN) when it does not parse.
pub fn provisioner_pool_from_env() -> Result<Option<PgPool>, String> {
    let Some(url) = std::env::var(PROVISIONER_DATABASE_URL).ok().filter(|v| !v.trim().is_empty()) else {
        return Ok(None);
    };
    sqlx::postgres::PgPoolOptions::new()
        .max_connections(4)
        .acquire_timeout(Duration::from_secs(30))
        .connect_lazy(&url)
        .map(Some)
        .map_err(|_| format!("{PROVISIONER_DATABASE_URL} is not a valid Postgres connection string"))
}

/// Whether the worker may start for a given provisioner pool (`Some`) and
/// flag. Flag off: nothing to say beyond an info line. Flag on and no pool:
/// refuse, loudly. Split out so the decision is unit-tested without env.
fn provisioner_pool_decision(pool: Option<PgPool>, flag_on: bool) -> Option<PgPool> {
    match (pool, flag_on) {
        (Some(p), _) => Some(p),
        (None, true) => {
            tracing::error!(
                connector_health = true,
                "tenant provisioning REFUSED to start: {PROVISIONER_DATABASE_URL} is unset but \
                 ORG_SELFSERVE_SIGNUP_ENABLED is on. The worker writes through a coralswarm_provisioner \
                 login (control 0381); set that secret (see the PR A1 operator steps)"
            );
            None
        }
        (None, false) => {
            tracing::info!("tenant provisioning worker not spawned: {PROVISIONER_DATABASE_URL} is unset and self-serve signup is off");
            None
        }
    }
}

/// Build the worker's dependencies from the environment, or say what is
/// missing. `None` means "do not spawn".
pub fn deps_from_env(control: PgPool, runtime: &Arc<TenantRuntime>, resolver: Arc<dyn SecretResolver>) -> Option<Deps> {
    let provisioner: Arc<dyn TenantDbProvisioner> = match std::env::var("TENANT_PROVISIONER").ok().as_deref() {
        Some("neon") => match NeonProvisioner::from_env() {
            Some(p) => Arc::new(p),
            None => {
                tracing::warn!("tenant provisioning worker not spawned: TENANT_PROVISIONER=neon but NEON_API_KEY is unset");
                return None;
            }
        },
        Some("local") => match std::env::var("TENANT_PROVISIONER_ADMIN_DSN") {
            Ok(dsn) if !dsn.trim().is_empty() => Arc::new(LocalPgProvisioner::new(dsn)),
            _ => {
                tracing::warn!("tenant provisioning worker not spawned: TENANT_PROVISIONER=local needs TENANT_PROVISIONER_ADMIN_DSN");
                return None;
            }
        },
        Some(other) => {
            tracing::warn!(provisioner = other, "tenant provisioning worker not spawned: unknown TENANT_PROVISIONER");
            return None;
        }
        None => {
            // Today's prod shape (flag off, nothing configured): not a warning.
            if provisioning::selfserve_signup_enabled() {
                tracing::warn!("tenant provisioning worker not spawned: TENANT_PROVISIONER is unset (neon|local)");
            } else {
                tracing::info!("tenant provisioning worker not spawned: TENANT_PROVISIONER is unset and self-serve signup is off");
            }
            return None;
        }
    };
    let provisioner_control = match provisioner_pool_from_env() {
        Ok(pool) => provisioner_pool_decision(pool, provisioning::selfserve_signup_enabled())?,
        Err(e) => {
            tracing::error!(connector_health = true, error = %e, "tenant provisioning worker not spawned");
            return None;
        }
    };
    let Some(clerk) = ClerkAdminClient::from_env() else {
        tracing::warn!("tenant provisioning worker not spawned: CLERK_SECRET_KEY is unset");
        return None;
    };
    let kek = match coralswarm_core::kek::load_kek() {
        Ok(k) => k,
        Err(e) => {
            tracing::warn!(error = %e, "tenant provisioning worker not spawned: MASTER_KEK_HEX unavailable");
            return None;
        }
    };
    let next_kek = match coralswarm_core::kek::load_kek_next() {
        Ok(n) => n,
        Err(e) => {
            tracing::warn!(error = %e, "tenant provisioning worker not spawned: MASTER_KEK_HEX_NEXT malformed");
            return None;
        }
    };
    let owner = format!(
        "{}:{}",
        std::env::var("FLY_MACHINE_ID").unwrap_or_else(|_| "local".into()),
        std::process::id()
    );
    Some(Deps {
        control,
        provisioner_control,
        provisioner,
        directory: Arc::new(clerk),
        registry: runtime.registry.clone(),
        resolver,
        kek,
        next_kek,
        policy: RetryPolicy::default(),
        owner,
        fault: None,
    })
}

/// The poll loop. Runs until `shutdown` triggers.
///
/// Supervised: each tick runs under `catch_unwind`, so a panic inside one
/// job's run (a bug, a provider response nobody anticipated) is logged at
/// error level with the alertable `connector_health` field and the loop
/// carries on. Unsupervised, the panic would end this spawned task, and
/// self-serve provisioning would stop with nothing but a JoinError nobody
/// awaits until shutdown. The panic is charged to the job as one attempt
/// ([`provisioning::record_panic`]: backoff, and abandon once the budget is
/// spent), so a job that panics on every run ends `failed` instead of
/// alerting forever.
pub async fn run(deps: Deps, shutdown: Shutdown) {
    tracing::info!(
        owner = %deps.owner,
        provider = deps.provisioner.provider_name(),
        enabled = provisioning::selfserve_signup_enabled(),
        "tenant provisioning worker started"
    );
    // The provisioner pool is lazy and the flag may be off at boot, so the
    // self-check runs before the first job is claimed, not at spawn: with
    // the flag off the worker still makes no database access.
    let mut verified = false;
    loop {
        if shutdown.is_triggered() {
            tracing::info!("tenant provisioning worker stopping");
            return;
        }
        if !provisioning::selfserve_signup_enabled() {
            sleep_unless_shutdown(IDLE_POLL, &shutdown).await;
            continue;
        }
        if !verified {
            match self_check_verdict(provisioning::provisioner_self_check(&deps.provisioner_control).await) {
                SelfCheck::Pass => verified = true,
                SelfCheck::RetryLater => {
                    sleep_unless_shutdown(POLL, &shutdown).await;
                    continue;
                }
                SelfCheck::Refuse => return,
            }
        }
        let ran_one = report(tick(&deps).await);
        if ran_one {
            // More may be due; go straight back round.
            continue;
        }
        sleep_unless_shutdown(POLL, &shutdown).await;
    }
}

#[derive(Debug, PartialEq, Eq)]
enum SelfCheck {
    Pass,
    RetryLater,
    Refuse,
}

/// Log the boot self-check's result and decide. A wrong login (the serving
/// role, a member of it, an owner-level login that can UPDATE routing, or
/// one without EXECUTE on the claim definer) or a `coralswarm_provisioner`
/// group role carrying LOGIN/SUPERUSER/... is an alertable refusal: the worker stops and provisions nothing. An
/// unreachable control plane is retried.
fn self_check_verdict(result: Result<String, provisioning::ProvisionerSelfCheckError>) -> SelfCheck {
    match result {
        Ok(login) => {
            tracing::info!(login = %login, "tenant provisioning worker: provisioner login verified");
            SelfCheck::Pass
        }
        Err(provisioning::ProvisionerSelfCheckError::Unreachable(e)) => {
            tracing::warn!(error = %e, "tenant provisioning worker: provisioner self-check could not run; retrying");
            SelfCheck::RetryLater
        }
        Err(refusal) => {
            tracing::error!(
                connector_health = true,
                reason = %refusal,
                "tenant provisioning REFUSED to start: the provisioner login failed its self-check"
            );
            SelfCheck::Refuse
        }
    }
}

/// One poll: claim one due job and run it, under `catch_unwind`. A
/// panicking run is logged (alertable) and charged to its job
/// ([`provisioning::record_panic`], itself supervised: if charging it panics
/// too, the lease simply expires). `Ok(None)` when nothing was due. Public
/// for the integration suite; [`run`] is the only production caller.
pub async fn tick(deps: &Deps) -> Result<Option<(String, RunOutcome)>, sqlx::Error> {
    let job = match AssertUnwindSafe(provisioning::claim_next_due(deps)).catch_unwind().await {
        Ok(claimed) => match claimed? {
            Some(job) => job,
            None => return Ok(None),
        },
        Err(panic) => {
            tracing::error!(
                connector_health = true,
                panic = %panic_message(&*panic),
                "tenant provisioning worker: claiming a job panicked; the worker continues"
            );
            return Ok(None);
        }
    };
    let org = job.clerk_org_id.clone();
    match AssertUnwindSafe(provisioning::run_claimed_job(deps, job)).catch_unwind().await {
        Ok(outcome) => outcome.map(|o| Some((org, o))),
        Err(panic) => {
            tracing::error!(
                connector_health = true,
                org = %org,
                panic = %panic_message(&*panic),
                "tenant provisioning worker: a job run panicked; charging it as an attempt"
            );
            match AssertUnwindSafe(provisioning::record_panic(deps, &org)).catch_unwind().await {
                Ok(outcome) => outcome.map(|o| Some((org, o))),
                Err(again) => {
                    tracing::error!(
                        connector_health = true,
                        org = %org,
                        panic = %panic_message(&*again),
                        "tenant provisioning worker: charging a panicked run panicked too; the lease will expire"
                    );
                    Ok(None)
                }
            }
        }
    }
}

/// The payload of a caught panic, as text (`panic!` carries a `&str` or a
/// `String`; anything else is named, never formatted). Pass the payload
/// itself (`&*boxed`): a `&Box<dyn Any>` would coerce to the Box as the
/// `Any` and never downcast.
fn panic_message(panic: &(dyn std::any::Any + Send)) -> String {
    panic
        .downcast_ref::<&str>()
        .map(|s| s.to_string())
        .or_else(|| panic.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "non-string panic payload".into())
}

/// Sleep `total`, waking every second to check `shutdown`, so the shutdown
/// drain in `lib.rs::run` never waits out a whole idle poll.
async fn sleep_unless_shutdown(total: Duration, shutdown: &Shutdown) {
    let slice = Duration::from_secs(1);
    let mut left = total;
    while !left.is_zero() && !shutdown.is_triggered() {
        let step = left.min(slice);
        tokio::time::sleep(step).await;
        left = left.saturating_sub(step);
    }
}

/// Log one claim's result. Returns whether a job was actually run.
fn report(claimed: Result<Option<(String, RunOutcome)>, sqlx::Error>) -> bool {
    match claimed {
        Ok(Some((org, outcome))) => {
            match outcome {
                RunOutcome::Ready => tracing::info!(org = %org, "tenant provisioning job ready"),
                RunOutcome::Deferred { step, attempts, reason } => tracing::info!(
                    org = %org, step = step.as_str(), attempts, reason = %reason,
                    "tenant provisioning job deferred"
                ),
                RunOutcome::Failed { reason } => {
                    tracing::warn!(org = %org, reason = %reason, "tenant provisioning job failed")
                }
                RunOutcome::LeaseLost => tracing::warn!(org = %org, "tenant provisioning lease lost mid-run"),
            }
            true
        }
        Ok(None) => false,
        Err(e) => {
            tracing::error!(error = %e, "tenant provisioning worker: control-plane error on claim");
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn no_provisioner_pool_refuses_only_when_the_flag_is_on() {
        assert!(provisioner_pool_decision(None, true).is_none());
        assert!(provisioner_pool_decision(None, false).is_none());
        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgres://provisioner@localhost:1/x")
            .unwrap();
        assert!(provisioner_pool_decision(Some(pool.clone()), true).is_some());
        assert!(provisioner_pool_decision(Some(pool), false).is_some());
    }

    #[test]
    fn a_failed_self_check_refuses_and_an_unreachable_one_retries() {
        use provisioning::ProvisionerSelfCheckError as E;
        assert_eq!(self_check_verdict(Ok("coralswarm_provisioner_login".into())), SelfCheck::Pass);
        assert_eq!(
            self_check_verdict(Err(E::ServingRole { current_user: "coralswarm_app".into() })),
            SelfCheck::Refuse
        );
        assert_eq!(
            self_check_verdict(Err(E::MissingClaimExecute { current_user: "someone".into() })),
            SelfCheck::Refuse
        );
        assert_eq!(
            self_check_verdict(Err(E::OwnerLevelLogin { current_user: "neondb_owner".into() })),
            SelfCheck::Refuse
        );
        assert_eq!(
            self_check_verdict(Err(E::GroupRoleAttributes {
                role: "coralswarm_provisioner".into(),
                attributes: vec!["login".into()],
            })),
            SelfCheck::Refuse
        );
        assert_eq!(self_check_verdict(Err(E::Unreachable("connection refused".into()))), SelfCheck::RetryLater);
    }

    #[tokio::test]
    async fn a_panicking_tick_is_caught_and_described() {
        let caught = AssertUnwindSafe(async { panic!("boom in a job") }).catch_unwind().await;
        let payload = caught.expect_err("the panic is caught, not propagated");
        assert_eq!(panic_message(&*payload), "boom in a job");
        let caught = AssertUnwindSafe(async { std::panic::panic_any(7u8) }).catch_unwind().await;
        assert_eq!(panic_message(&*caught.unwrap_err()), "non-string panic payload");
    }
}

//! Which halves of [`crate::run`] this process starts (#1495).
//!
//! `BACKEND_ROLE` decides whether the cloud binary serves HTTP, runs the
//! background job workers, or both:
//!
//! | value              | public :8080            | internal :8081 | job workers |
//! |--------------------|-------------------------|----------------|-------------|
//! | unset / `all`      | full API                | yes            | yes         |
//! | `http`             | full API                | yes            | no          |
//! | `workers`          | `/healthz` + `/readyz`  | yes            | yes         |
//!
//! Unset means `all`, which is exactly the pre-#1495 behaviour, so a binary
//! carrying this module changes nothing until a config sets the variable.
//!
//! Production (`fly.toml`) sets `BACKEND_ROLE = 'http'` app-wide in `[env]` and
//! starts the `workers` process group as
//! `/usr/bin/env BACKEND_ROLE=workers /app/coralswarm-funded-backend`. The
//! `env` prefix overrides the app-wide value for that group only; Fly has no
//! per-process-group `[env]`. Two properties follow and are deliberate:
//!
//! * `[processes] app` stays byte-identical, which
//!   `scripts/deploy_financial_backend.py` checks before every release.
//! * An image that predates this module ignores `BACKEND_ROLE`, so rolling the
//!   image back while the two-group config stays deployed leaves BOTH groups
//!   running `all`: double workers, which the queue's `FOR UPDATE SKIP LOCKED`
//!   claim already tolerates (two app machines do it today), never zero.
//!
//! Both roles bind the internal router because `flyctl proxy 18081:8081 -a
//! coralswarm-backend` (chain-health-check.yml) and `coralswarm-backend.internal`
//! resolve to any started machine in the app, including the worker one.
//!
//! An unrecognised value refuses to start rather than guessing: guessing
//! `http` silently stops every worker, guessing `all` silently doubles them.

/// What [`crate::run`] starts in this process. See the module doc.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BackendRole {
    /// HTTP and workers in one process (the default, pre-#1495 behaviour).
    All,
    /// Full HTTP API, no job workers.
    Http,
    /// Job workers; the public port serves only liveness/readiness.
    Workers,
}

impl BackendRole {
    /// Pure parser so tests need no process-global env. `None`, empty and
    /// whitespace-only mean [`BackendRole::All`].
    pub fn parse(raw: Option<&str>) -> Result<Self, String> {
        match raw.map(str::trim).map(str::to_ascii_lowercase).as_deref() {
            None | Some("") | Some("all") => Ok(BackendRole::All),
            Some("http") => Ok(BackendRole::Http),
            Some("workers") => Ok(BackendRole::Workers),
            Some(other) => Err(format!(
                "invalid configuration: BACKEND_ROLE={other:?} — expected `all`, `http` or \
                 `workers` (unset means `all`). Refusing to start rather than guess which \
                 half of the backend to run."
            )),
        }
    }

    /// Read `BACKEND_ROLE` from the process environment.
    pub fn from_env() -> Result<Self, String> {
        Self::parse(std::env::var("BACKEND_ROLE").ok().as_deref())
    }

    /// Whether this process serves the full public API on :8080.
    pub fn serves_http(self) -> bool {
        !matches!(self, BackendRole::Workers)
    }

    /// Whether this process spawns the background job workers.
    pub fn runs_workers(self) -> bool {
        !matches!(self, BackendRole::Http)
    }

    pub fn as_str(self) -> &'static str {
        match self {
            BackendRole::All => "all",
            BackendRole::Http => "http",
            BackendRole::Workers => "workers",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unset_or_blank_is_all() {
        for raw in [None, Some(""), Some("   ")] {
            assert_eq!(BackendRole::parse(raw), Ok(BackendRole::All), "{raw:?}");
        }
    }

    #[test]
    fn known_values_parse_case_and_space_insensitively() {
        assert_eq!(BackendRole::parse(Some("all")), Ok(BackendRole::All));
        assert_eq!(BackendRole::parse(Some(" HTTP ")), Ok(BackendRole::Http));
        assert_eq!(BackendRole::parse(Some("Workers")), Ok(BackendRole::Workers));
    }

    #[test]
    fn unknown_value_refuses_to_start() {
        for raw in ["worker", "both", "api", "1"] {
            assert!(BackendRole::parse(Some(raw)).is_err(), "{raw}");
        }
    }

    #[test]
    fn every_role_runs_at_least_one_half() {
        for role in [BackendRole::All, BackendRole::Http, BackendRole::Workers] {
            assert!(role.serves_http() || role.runs_workers(), "{role:?}");
        }
        assert!(BackendRole::All.serves_http() && BackendRole::All.runs_workers());
        assert!(BackendRole::Http.serves_http() && !BackendRole::Http.runs_workers());
        assert!(!BackendRole::Workers.serves_http() && BackendRole::Workers.runs_workers());
    }
}

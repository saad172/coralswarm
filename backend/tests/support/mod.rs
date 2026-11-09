//! Shared helpers for the integration tests in `backend/tests/`.
//!
//! Cargo treats every top-level `.rs` file under `tests/` as its own test
//! binary, but NOT subdirectories — so `tests/support/` is the one place
//! test-only code can be shared across those binaries. A test file opts in
//! with a plain `mod support;` declaration (see `rls_probe_pg.rs` for the
//! canonical example); cargo then compiles this module into that binary.
//!
//! Because each test binary only uses the subset of helpers it needs, the
//! rest would trip `dead_code` per-binary — hence the allow below (scoped to
//! this module tree only, not the tests themselves).
#![allow(dead_code)]

pub mod app_role;
pub mod library_fixture;
pub mod managed_extraction;
pub mod provisioner_role;
pub mod read_models;
pub mod rls_probe;
pub mod scoped_ask_fixture;
pub mod source_evidence;
pub mod tenant_role;

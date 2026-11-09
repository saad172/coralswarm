//! OAuth provider helpers.

/// Consumed-`jti` replay store for signed `state` tokens (CS-110 follow-up,
/// foundation train PR 1.4). See that module's own doc for the atomicity and
/// fail-closed contract.
pub mod jti_store;
pub mod slack;
pub mod state;

//! Compatibility exports for queued/embedded meeting extraction callers.
//! Production polling is owned by `source_atoms`, which accepts both job kinds
//! and selects the prompt from each immutable source version's pinned contract.

pub use super::source_atoms::{
    process_job, tick, AtomKind, EntityMention, EntityType, ValidatedAtom,
};
pub const EXTRACTION_VERSION: &str = super::source_atoms::LEGACY_EXTRACTION_VERSION;

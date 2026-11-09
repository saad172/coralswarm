//! The embedding gateway: the one door every embedding call goes through.
//!
//! [`submit`] embeds texts for a [`Purpose`]; the managed corpus path
//! (`managed::embedding`) uses [`plan`] and [`send`] instead, because it
//! binds each provider request to its own receipt before sending it. All
//! three sit on `coralswarm_core::embed_gateway`, which owns size bounding
//! (split, never truncate), batching with split texts isolated, the provider
//! call and error classification. This module adds the backend's halves:
//!
//! - **metering**: [`Purpose::Metered`] writes one `llm_usage` row per paid
//!   HTTP attempt on the caller's [`UsageContext`]. Receipts and billing for
//!   the managed purpose are `managed::embedding`'s gate, passed in as
//!   [`Purpose::Managed`].
//! - **attempt counting and backoff** for stored rows: one cap
//!   ([`MAX_EMBED_ATTEMPTS`]), one ladder ([`embed_backoff_secs`]), and the
//!   hold code a failure becomes ([`hold_code`]). A permanent failure jumps
//!   straight to the cap; a transient one costs one attempt.
//!
//! No code outside `packages/core` can call a provider (every provider
//! method takes a `ProviderPermit` only the core gateway makes), and no
//! backend code but this module may name the core gateway
//! (`scripts/check_embedding_gateway.sh`).
//!
//! Plane admission (COR-114) is the managed gate's, not this module's: a
//! [`Purpose::Managed`] send carries a `managed::ManagedGate`, whose
//! front door resolved the account's liability plane (`managed::plane`)
//! before the gate existed, and whose every reservation re-confirms it. No
//! production type but `ManagedGate` implements `SendGate`
//! (`scripts/check_managed_front_door.sh`), so a managed embedding cannot
//! reach the provider unresolved. [`Purpose::Metered`] embeddings (Ask and
//! search queries, digest titles) are charged on the caller's
//! [`UsageContext`] and are not managed spend.

use coralswarm_core::embed::EmbedProvider;
use coralswarm_core::embed_gateway::{self as gateway, AttemptMeter};
use coralswarm_core::send_gate::{AttemptRecord, GateFuture, SendGate};

pub use coralswarm_core::embed_gateway::{
    plan, Disposition, Failure, Plan, Request, Unbilled, EMBED_INPUT_MAX_BYTES, REQUEST_MAX_BYTES,
};

use crate::llm::usage::UsageContext;

// ── Purpose ───────────────────────────────────────────────────────────────────

/// What an embedding is for, which decides how it is paid for.
#[derive(Clone, Copy)]
pub enum Purpose<'a> {
    /// A paid embedding outside the managed corpus: Ask and search queries,
    /// digest titles, `/v1/embed`, the boot probe. One `llm_usage` row per
    /// paid HTTP attempt, under `task`, on `usage`. `None` is a caller with
    /// no attributable account (a pool-less engine, the boot probe): the
    /// send is still bounded and classified, and writes no row.
    Metered {
        usage: Option<&'a UsageContext>,
        task: &'a str,
    },
    /// Managed corpus embedding under `managed::embedding`'s receipted gate.
    /// One gate admits one request: send each [`Request`] from [`plan`]
    /// under its own gate with [`send`].
    Managed(&'a dyn SendGate),
    /// A model with no paid transport, and why. Refused on a paid provider.
    Unbilled(Unbilled),
}

/// Embed `texts`, one vector each, in order. Fails as a whole on the first
/// failure; [`Failure::disposition`] says whether it is worth a retry.
pub async fn submit(
    provider: &dyn EmbedProvider,
    purpose: Purpose<'_>,
    texts: &[&str],
) -> Result<Vec<Vec<f32>>, Failure> {
    match purpose {
        Purpose::Metered { usage, task } => {
            let meter = UsageMeter { usage, task };
            gateway::submit(provider, gateway::Purpose::Metered(&meter), texts).await
        }
        Purpose::Managed(gate) => {
            gateway::submit(provider, gateway::Purpose::Managed(gate), texts).await
        }
        Purpose::Unbilled(why) => {
            gateway::submit(provider, gateway::Purpose::Unbilled(why), texts).await
        }
    }
}

/// [`submit`] for one text.
pub async fn submit_one(
    provider: &dyn EmbedProvider,
    purpose: Purpose<'_>,
    text: &str,
) -> Result<Vec<f32>, Failure> {
    let mut vectors = submit(provider, purpose, &[text]).await?;
    Ok(vectors.pop().unwrap_or_default())
}

/// Send one planned request; one vector per text in [`Request::items`]
/// order.
pub(crate) async fn send(
    provider: &dyn EmbedProvider,
    purpose: Purpose<'_>,
    request: &Request,
) -> Result<Vec<Vec<f32>>, Failure> {
    match purpose {
        Purpose::Metered { usage, task } => {
            let meter = UsageMeter { usage, task };
            gateway::send(provider, gateway::Purpose::Metered(&meter), request).await
        }
        Purpose::Managed(gate) => {
            gateway::send(provider, gateway::Purpose::Managed(gate), request).await
        }
        Purpose::Unbilled(why) => {
            gateway::send(provider, gateway::Purpose::Unbilled(why), request).await
        }
    }
}

/// [`Purpose::Metered`]'s meter: an `llm_usage` row per billed or ambiguous
/// attempt, none for an attempt that generated nothing.
struct UsageMeter<'a> {
    usage: Option<&'a UsageContext>,
    task: &'a str,
}

impl AttemptMeter for UsageMeter<'_> {
    fn record<'a>(&'a self, input_bytes: usize, attempt: &'a AttemptRecord) -> GateFuture<'a, ()> {
        Box::pin(async move {
            match self.usage {
                Some(usage) => {
                    crate::llm::usage::insert_embed_attempt_row(usage, self.task, input_bytes, attempt)
                        .await
                }
                None => tracing::debug!(task = self.task, input_bytes, outcome = ?attempt.outcome, "unattributed embedding attempt"),
            }
        })
    }
}

// ── Attempts and backoff for stored rows ──────────────────────────────────────

/// Cap on per-atom embed attempts before a failure is treated as permanent.
///
/// Below this cap, a failed atom is retried with backoff (see
/// [`embed_backoff_secs`]) rather than being excluded from
/// `workers::embed::load_pending_atoms` forever. At/above it,
/// `embed_failed_at` stays permanent. A [`Disposition::Permanent`] failure
/// sets the count straight to this cap (`managed::embedding`'s hold).
///
/// Shared by `workers::embed`'s candidate query, `workers::embed_retry`'s
/// sweep and `pipeline_stage_health`, which must all use the same threshold.
pub const MAX_EMBED_ATTEMPTS: i32 = 6;

/// Backoff floor in seconds — mirrors `jobs::queue::backoff_secs`'s 30s base so
/// the two retry systems (job-level and atom-level) read consistently in logs.
pub(crate) const EMBED_BACKOFF_BASE_SECS: i64 = 30;

/// Compute the backoff delay (in seconds) an atom with `attempts` prior failed
/// embed attempts must wait before it's retry-eligible again: `2^attempts *
/// EMBED_BACKOFF_BASE_SECS`, mirroring `jobs::queue::backoff_secs`'s shape.
/// Exponent is capped at 30 so `2^exp` can never overflow `i64` regardless of
/// how large `attempts` gets (defensive only — [`MAX_EMBED_ATTEMPTS`] means
/// `attempts` never realistically approaches that range).
///
/// Pure function so the formula embedded in `load_pending_atoms`'s SQL (which
/// must match this exactly — see that query's `power(2, LEAST(embed_attempts,
/// 30)) * INTERVAL '30 seconds'`) is independently unit-testable without a
/// database.
pub(crate) fn embed_backoff_secs(attempts: i32) -> i64 {
    let exp = attempts.clamp(0, 30) as u32;
    2_i64.saturating_pow(exp) * EMBED_BACKOFF_BASE_SECS
}

/// Hold code for a text too long for any provider request, or one the
/// provider refused as over its per-input limit. Permanent: the hold sets
/// `embed_attempts` to the cap, so neither the retry sweep nor the pending
/// query picks the atom up again. Its text stays keyword-searchable;
/// `backfill_chunk_links --rechunk-oversized` finds it by its
/// `blocked:input_too_long` stamp and re-chunks its source.
pub(crate) const INPUT_TOO_LONG: &str = "input_too_long";
/// Hold code for any other permanent refusal (a 4xx other than 429, an
/// unusable answer). Permanent.
pub(crate) const PROVIDER_REJECTED: &str = "provider_rejected";
/// Hold code for a transient failure (429, 5xx, transport). One attempt.
pub(crate) const PROVIDER_UNAVAILABLE: &str = "provider_unavailable";
/// The hold codes that set `embed_attempts` straight to
/// [`MAX_EMBED_ATTEMPTS`]: no retry can help.
pub(crate) const PERMANENT_HOLDS: [&str; 2] = [INPUT_TOO_LONG, PROVIDER_REJECTED];

/// Whether the provider refused the request itself, before generating
/// anything: a 4xx other than 429 (an over-limit input included). Such a
/// refusal of several texts may be one text's fault and costs nothing, so a
/// managed caller re-sends each text alone to pin it. A permanent failure
/// after the provider answered (an unusable 200, which is charged) is not
/// re-sent.
pub(crate) fn refused_unsent(failure: &Failure) -> bool {
    use coralswarm_core::embed::EmbedError;
    failure.is_permanent() && matches!(failure.error, EmbedError::Http { .. } | EmbedError::InputTooLong(_))
}

/// The hold code a provider failure becomes. A gate's own denial is not a
/// provider failure; callers record it under the gate's code.
pub(crate) fn hold_code(failure: &Failure) -> &'static str {
    match (failure.disposition, &failure.error) {
        (Disposition::Transient, _) => PROVIDER_UNAVAILABLE,
        (Disposition::Permanent, coralswarm_core::embed::EmbedError::InputTooLong(_)) => INPUT_TOO_LONG,
        (Disposition::Permanent, _) => PROVIDER_REJECTED,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use coralswarm_core::embed::EmbedError;

    #[test]
    fn a_failure_becomes_a_hold_by_its_verdict() {
        let failure = |error: EmbedError| Failure::from(error);
        let http = |status| EmbedError::Http { status, body: String::new() };
        assert_eq!(hold_code(&failure(http(503))), PROVIDER_UNAVAILABLE);
        assert_eq!(hold_code(&failure(http(429))), PROVIDER_UNAVAILABLE);
        assert_eq!(hold_code(&failure(EmbedError::Transport("reset".into()))), PROVIDER_UNAVAILABLE);
        assert_eq!(hold_code(&failure(http(400))), PROVIDER_REJECTED);
        assert_eq!(hold_code(&failure(http(422))), PROVIDER_REJECTED);
        assert_eq!(hold_code(&failure(EmbedError::InferenceFailed("bad".into()))), PROVIDER_REJECTED);
        assert_eq!(hold_code(&failure(EmbedError::InputTooLong("8192".into()))), INPUT_TOO_LONG);
        for code in PERMANENT_HOLDS {
            assert_ne!(code, PROVIDER_UNAVAILABLE);
        }
    }

    #[test]
    fn only_an_unsent_refusal_is_re_sent_input_by_input() {
        let failure = |error: EmbedError| Failure::from(error);
        let http = |status| EmbedError::Http { status, body: String::new() };
        assert!(refused_unsent(&failure(http(400))));
        assert!(refused_unsent(&failure(http(422))));
        assert!(refused_unsent(&failure(EmbedError::InputTooLong("8192".into()))));
        // Transient: held and retried, not split.
        assert!(!refused_unsent(&failure(http(429))));
        assert!(!refused_unsent(&failure(http(503))));
        // A charged, unusable answer: re-sending would charge again.
        assert!(!refused_unsent(&failure(EmbedError::InferenceFailed("width 1".into()))));
    }
}

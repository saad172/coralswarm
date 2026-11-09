//! Managed (background) spend: per-attempt reserve and settle on a money
//! ledger (COR-21), charged to credits by `workers::billing_derivation`
//! (COR-88).
//!
//! Data shape:
//! - one `managed_budget_periods` row per (account, purpose, UTC day); a
//!   guarded UPDATE keeps `reserved + charged <= limit`;
//! - one `managed_attempt_receipts` row per provider HTTP attempt, `reserved`
//!   until settled once to `billed`, `not_generated`, `ambiguous` or `overrun`;
//! - the settle arithmetic lives only in the SQL function `managed_settle`,
//!   which the live settle and the reaper both call.

pub(crate) mod embedding;
pub(crate) mod enrichment;
pub(crate) mod gate;
pub(crate) mod interactive_ask;
pub(crate) mod plane;
pub(crate) mod standing;
pub(crate) mod transport;

use chrono::{DateTime, Utc};
use coralswarm_core::send_gate::{MaxPrice, RefuseCode, SendTerms};
use sqlx::PgPool;
use uuid::Uuid;

use crate::billing::rates::PriceSheet;
use crate::llm::CompletionRequest;
use crate::tenancy::TenantId;
use crate::workers::embed::JobScope;

pub(crate) use gate::{reap_tick, ManagedGate};
pub(crate) use plane::{LiabilityPlane, PlanePermit};

/// Money in millionths of a US dollar. The ledger never holds `f64`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Default)]
pub struct MicroUsd(i64);

impl MicroUsd {
    /// Rounds up, so a bound computed from it never under-reserves.
    pub fn from_usd_ceil(usd: f64) -> Self {
        if !usd.is_finite() || usd <= 0.0 {
            return Self(0);
        }
        Self((usd * 1_000_000.0).ceil().min(i64::MAX as f64) as i64)
    }

    pub const fn get(self) -> i64 {
        self.0
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ManagedPurpose {
    SourceExtraction,
    #[allow(dead_code)]
    CloudEmbedding,
    /// Graph-level consumers (entity resolution, journal, titles, digests,
    /// meeting summaries) under one shared daily cap. See
    /// [`enrichment`]. No worker constructs it yet.
    #[allow(dead_code)]
    Enrichment,
    /// Jev's calls inside a user-initiated Ask (catalog scan, pack, cite,
    /// capture and contradiction judges), under their own per-account daily
    /// cap. See [`interactive_ask`].
    InteractiveAsk,
}

impl ManagedPurpose {
    pub const fn as_str(self) -> &'static str {
        match self {
            ManagedPurpose::SourceExtraction => "source_extraction",
            ManagedPurpose::CloudEmbedding => "cloud_embedding",
            ManagedPurpose::Enrichment => "managed_enrichment",
            ManagedPurpose::InteractiveAsk => "interactive_ask",
        }
    }
}

/// Job kinds whose spend is charged from receipts. Billing derivation's job
/// axis must never select these, or a managed window would be charged twice.
#[cfg_attr(not(test), allow(dead_code))]
pub const MANAGED_JOB_KINDS: &[&str] = &[
    "ExtractSourceAtoms", "ExtractMeetingAtoms",
    "ExtractStandingSourceAtoms", "ExtractStandingMeetingAtoms",
    "EmbedAtoms", "EmbedAtomsRetry", "ReembedOutdatedChunks", "EmbedStatements",
];

/// Job kinds moved onto the shared `managed_enrichment` purpose. A kind moves
/// here in the same change that maps it to
/// `managed_history::AutomaticPurpose::ManagedEnrichment`, and never appears
/// in `billing_derivation::JOB_AXIS_KINDS`: its receipts are the bill.
/// `EntityResolution` moved first (source-agnostic plan PR 5), then
/// `RegenerateDigest` and `BackfillAtomTitles` (PRs 6-8), then
/// `GenerateMeetingSummary` (PR 10). The journal sweep
/// (`workers::journal_sweep`) is on the purpose too but has no `jobs` row:
/// its usage rows carry neither a job nor a journal key
/// (`llm::usage::metered_complete_managed`), so no legacy axis can select
/// them and its receipts are the only bill.
#[cfg_attr(not(test), allow(dead_code))]
pub const MANAGED_ENRICHMENT_JOB_KINDS: &[&str] =
    &["EntityResolution", "RegenerateDigest", "BackfillAtomTitles", "GenerateMeetingSummary"];

/// Whether a job kind's spend is charged from managed receipts, on any
/// managed purpose.
#[cfg_attr(not(test), allow(dead_code))]
pub fn is_managed_job_kind(kind: &str) -> bool {
    MANAGED_JOB_KINDS.contains(&kind) || MANAGED_ENRICHMENT_JOB_KINDS.contains(&kind)
}

/// Prompt tokens not visible in the request bytes: chat template, tool-call
/// framing and role markers.
pub const PROMPT_OVERHEAD_TOKENS: u64 = 512;

/// Operator-only bounds, read from env with defaults in code.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Caps {
    pub disabled: bool,
    pub credit_enforcement: bool,
    pub daily_extraction: MicroUsd,
    pub daily_embedding: MicroUsd,
    /// One shared cap for every `managed_enrichment` consumer.
    pub daily_enrichment: MicroUsd,
    /// Jev's Ask-time calls (`interactive_ask`). Separate from the enrichment
    /// cap so background work never starves an Ask, and the reverse.
    pub daily_interactive_ask: MicroUsd,
    pub per_attempt: MicroUsd,
    pub per_version: MicroUsd,
    /// Multiplier over the card rate for the price ceiling. Measured: at the
    /// card rate 3 of 5 calls were refused (429); at 2x, 7 of 7 were served.
    pub headroom: f64,
    pub reap_after_secs: u64,
}

impl Caps {
    pub fn from_env() -> Self {
        Self {
            disabled: crate::admission::env_flag("MANAGED_PROCESSING_DISABLED"),
            credit_enforcement: !crate::admission::env_flag("MANAGED_CREDIT_ENFORCEMENT_DISABLED"),
            daily_extraction: usd_env("MANAGED_DAILY_CAP_USD_EXTRACTION", 10.0),
            daily_embedding: usd_env("MANAGED_DAILY_CAP_USD_EMBEDDING", 1.0),
            daily_enrichment: usd_env("MANAGED_DAILY_CAP_USD_ENRICHMENT", 2.0),
            daily_interactive_ask: usd_env("MANAGED_DAILY_CAP_USD_INTERACTIVE_ASK", 1.0),
            per_attempt: usd_env("MANAGED_PER_ATTEMPT_CAP_USD", 0.05),
            per_version: usd_env("MANAGED_PER_VERSION_CAP_USD", 0.25),
            headroom: f64_env("MANAGED_PRICE_HEADROOM", 2.0).max(1.0),
            reap_after_secs: std::env::var("MANAGED_REAP_AFTER_SECS")
                .ok()
                .and_then(|v| v.trim().parse::<u64>().ok())
                .filter(|s| *s > 0)
                .unwrap_or_else(|| 600u64.max(5 * crate::llm_timeout_secs())),
        }
    }

    pub fn daily(&self, purpose: ManagedPurpose) -> MicroUsd {
        match purpose {
            ManagedPurpose::SourceExtraction => self.daily_extraction,
            ManagedPurpose::CloudEmbedding => self.daily_embedding,
            ManagedPurpose::Enrichment => self.daily_enrichment,
            ManagedPurpose::InteractiveAsk => self.daily_interactive_ask,
        }
    }
}

fn f64_env(key: &str, default: f64) -> f64 {
    match std::env::var(key) {
        Err(_) => default,
        Ok(raw) => match raw.trim().parse::<f64>() {
            Ok(v) if v.is_finite() && v >= 0.0 => v,
            _ => {
                tracing::warn!(key, value = %raw, default, "managed caps: invalid value, using the default");
                default
            }
        },
    }
}

fn usd_env(key: &str, default: f64) -> MicroUsd {
    MicroUsd::from_usd_ceil(f64_env(key, default))
}

/// Upper bound for one HTTP attempt over every model the request may be
/// served by, plus the terms that make the provider honour it.
#[derive(Clone, Debug, PartialEq)]
pub struct WorstCase {
    pub per_attempt: MicroUsd,
    pub input_bound_tokens: u64,
    pub max_tokens: u32,
    pub terms: SendTerms,
}

impl WorstCase {
    /// The price ceiling (USD per million tokens) that `max_price` carries.
    pub fn ceiling(&self) -> MaxPrice {
        self.terms.max_price.unwrap_or(MaxPrice {
            prompt: 0.0,
            completion: 0.0,
        })
    }
}

/// Input tokens are at most the UTF-8 bytes of the system prompt, messages and
/// tool schema (byte-level BPE), plus [`PROMPT_OVERHEAD_TOKENS`]. Output is at
/// most `max_tokens`. The ceiling rate is the dearest candidate's card rate
/// times `headroom`, so the bound and `max_price` come from one price list.
/// USD per million tokens times tokens is micro-USD.
///
/// `fallback` replaces the global fallback map for this call (see
/// [`ManagedCall::fallback_model`]); `None` keeps it. Every candidate must
/// be priced.
pub fn worst_case(
    req: &CompletionRequest,
    fallback: Option<&str>,
    sheet: &PriceSheet,
    headroom: f64,
    as_of: DateTime<Utc>,
) -> Result<WorstCase, RefuseCode> {
    let models = crate::llm::openai::candidate_models(&req.model, fallback);
    let mut prompt: f64 = 0.0;
    let mut completion: f64 = 0.0;
    for model in &models {
        let rate = sheet.rate("openrouter", model, as_of);
        if !rate.matched {
            return Err(RefuseCode::UnpricedModel);
        }
        prompt = prompt.max(rate.input_per_million);
        completion = completion.max(rate.output_per_million);
    }
    let headroom = headroom.max(1.0);
    let ceiling = MaxPrice {
        prompt: prompt * headroom,
        completion: completion * headroom,
    };
    let mut bytes = req.system.as_deref().map(str::len).unwrap_or(0);
    for m in &req.messages {
        bytes = bytes.saturating_add(m.content.len());
    }
    if let Some(tool) = &req.tool_schema {
        bytes = bytes
            .saturating_add(tool.name.len())
            .saturating_add(tool.schema.to_string().len());
    }
    let input_bound_tokens = bytes as u64 + PROMPT_OVERHEAD_TOKENS;
    let micro = input_bound_tokens as f64 * ceiling.prompt
        + f64::from(req.max_tokens) * ceiling.completion;
    let per_attempt = MicroUsd::from_usd_ceil(micro / 1_000_000.0).max(MicroUsd(1));
    Ok(WorstCase {
        per_attempt,
        input_bound_tokens,
        max_tokens: req.max_tokens,
        terms: SendTerms {
            models,
            max_price: Some(ceiling),
        },
    })
}

/// Everything the managed gate needs to reserve for one logical call.
pub(crate) struct ManagedCall<'a> {
    pub plane: &'a JobScope<'a>,
    pub control: &'a PgPool,
    pub tenant: &'a TenantId,
    pub account_id: Uuid,
    /// The one member the receipt, the credit debit and the credit check
    /// name. See `workers::source_atoms::charge_principal`.
    pub principal: Uuid,
    pub subject: Subject<'a>,
    /// The OpenRouter fallback this call prices and sends, replacing the
    /// global `llm::openai::default_fallback_model` map. `None` keeps the
    /// global map. Only source extraction sets it
    /// (`model_config::source_extraction_fallback_model`, profiles v2 and v3); the Jev seat on
    /// the same standing window does not.
    pub fallback_model: Option<String>,
}

pub(crate) enum Subject<'a> {
    EmbeddingBatch { batch: &'a embedding::PreparedBatch },
    StandingWindow {
        work: &'a StandingWork,
    },
    Window {
        graph: Uuid,
        version: Uuid,
        profile: &'a str,
        job: Uuid,
    },
    /// A graph-level `managed_enrichment` call. Every input is bound into the
    /// request hash and written to `managed_enrichment_inputs` before the
    /// send; there is no per-version unit, so only the per-attempt cap and
    /// the shared daily cap apply. Entity resolution is the first consumer.
    Enrichment {
        work: &'a enrichment::EnrichmentWork,
    },
    /// One Jev question set inside a user-initiated Ask, charged to the asker.
    /// No unit or version: the per-attempt cap and the `interactive_ask`
    /// daily cap are the bounds.
    InteractiveAsk {
        work: &'a interactive_ask::AskWork,
    },
}

#[derive(Clone, Debug)]
pub(crate) struct StandingWork {
    pub account_id: Uuid,
    pub graph_id: Uuid,
    pub run_id: Uuid,
    pub unit_id: Uuid,
    pub version_id: Uuid,
    pub job_id: Uuid,
    pub profile: standing::StandingProfile,
    pub generation: i64,
    pub logical_call: Uuid,
    pub start_cursor: i64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::billing::rates::RateRow;
    use crate::llm::{Message, Role, ToolSchema};

    fn row(provider: &str, pattern: &str, input: f64, output: f64, blend: bool) -> RateRow {
        RateRow {
            provider: provider.into(),
            model_pattern: pattern.into(),
            priority: if blend { 1000 } else { 0 },
            input_per_million: input,
            output_per_million: output,
            cache_write_multiplier: 1.0,
            cache_read_multiplier: 0.2,
            effective_from: DateTime::<Utc>::from_timestamp(0, 0).unwrap(),
            effective_to: None,
            is_blend_fallback: blend,
        }
    }

    fn sheet() -> PriceSheet {
        PriceSheet::from_rows(vec![
            row("openrouter", "deepseek-v4-flash", 0.08, 0.18, false),
            row("openrouter", "glm-5.3-flash", 0.075, 0.25, false),
            row("anthropic", "blend", 5.7, 27.5, true),
        ])
    }

    fn window_request(model: &str, max_tokens: u32) -> CompletionRequest {
        CompletionRequest {
            messages: vec![Message {
                role: Role::User,
                content: "x".repeat(12_000),
            }],
            model: model.into(),
            max_tokens,
            system: Some("s".repeat(1_000)),
            tool_schema: Some(ToolSchema {
                strict: false,
                name: "emit_source_atoms".into(),
                schema: serde_json::json!({"type":"object"}),
            }),
            effort: None,
            reasoning: crate::llm::ReasoningSetting::Off,
        }
    }

    #[test]
    fn worst_case_prices_dearer_candidate_with_headroom() {
        let req = window_request("deepseek/deepseek-v4-flash-0731", 8_000);
        let w = worst_case(&req, None, &sheet(), 2.0, Utc::now()).unwrap();
        assert_eq!(
            w.terms.models,
            vec!["deepseek/deepseek-v4-flash-0731".to_string(), "z-ai/glm-5.3-flash".to_string()]
        );
        let ceiling = w.ceiling();
        assert!((ceiling.prompt - 0.16).abs() < 1e-12, "deepseek input is dearer");
        assert!((ceiling.completion - 0.5).abs() < 1e-12, "glm output is dearer");
        let tool_bytes = "emit_source_atoms".len() + serde_json::json!({"type":"object"}).to_string().len();
        let input = (13_000 + tool_bytes) as u64 + PROMPT_OVERHEAD_TOKENS;
        assert_eq!(w.input_bound_tokens, input);
        let expected = (input as f64 * 0.16 + 8_000.0 * 0.5).ceil() as i64;
        assert_eq!(w.per_attempt.get(), expected);
    }

    #[test]
    fn extraction_fallback_replaces_glm_and_must_be_priced() {
        let req = window_request("deepseek/deepseek-v4-flash-0731", 8_000);
        let v41 = Some("deepseek/deepseek-v4.1-flash");
        assert_eq!(
            worst_case(&req, v41, &sheet(), 2.0, Utc::now()).unwrap_err(),
            RefuseCode::UnpricedModel,
            "the 0731 pattern must not price v4.1"
        );
        let mut rows = vec![row("openrouter", "deepseek-v4.1-flash", 0.11, 0.34, false)];
        rows.extend([
            row("openrouter", "deepseek-v4-flash", 0.08, 0.18, false),
            row("openrouter", "glm-5.3-flash", 0.075, 0.25, false),
            row("anthropic", "blend", 5.7, 27.5, true),
        ]);
        let priced = PriceSheet::from_rows(rows);
        let w = worst_case(&req, v41, &priced, 2.0, Utc::now()).unwrap();
        assert_eq!(
            w.terms.models,
            vec!["deepseek/deepseek-v4-flash-0731".to_string(), "deepseek/deepseek-v4.1-flash".to_string()]
        );
        assert!((w.ceiling().prompt - 0.22).abs() < 1e-12);
        assert!((w.ceiling().completion - 0.68).abs() < 1e-12);
        // No override: the global map, unchanged.
        let other = worst_case(&req, None, &priced, 2.0, Utc::now()).unwrap();
        assert_eq!(
            other.terms.models,
            vec!["deepseek/deepseek-v4-flash-0731".to_string(), "z-ai/glm-5.3-flash".to_string()]
        );
    }

    #[test]
    fn worst_case_refuses_unpriced_candidate() {
        let req = window_request("mystery/model", 100);
        assert_eq!(
            worst_case(&req, None, &sheet(), 2.0, Utc::now()).unwrap_err(),
            RefuseCode::UnpricedModel
        );
    }

    #[test]
    fn worst_case_above_attempt_cap_refuses() {
        let caps_attempt = MicroUsd::from_usd_ceil(0.05);
        let modest = worst_case(&window_request("deepseek/deepseek-v4-flash-0731", 8_000), None, &sheet(), 2.0, Utc::now()).unwrap();
        assert!(modest.per_attempt <= caps_attempt);
        let huge = worst_case(&window_request("deepseek/deepseek-v4-flash-0731", 200_000), None, &sheet(), 2.0, Utc::now()).unwrap();
        assert!(huge.per_attempt > caps_attempt);
        assert_eq!(gate::attempt_cap_check(&huge, caps_attempt), Err(RefuseCode::CallCostCap));
        assert_eq!(gate::attempt_cap_check(&modest, caps_attempt), Ok(()));
    }

    #[test]
    fn micro_usd_rounds_up() {
        assert_eq!(MicroUsd::from_usd_ceil(0.0000001).get(), 1);
        assert_eq!(MicroUsd::from_usd_ceil(10.0).get(), 10_000_000);
        assert_eq!(MicroUsd::from_usd_ceil(-1.0).get(), 0);
    }
}

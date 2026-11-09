//! Jev, the decision seam (ADR-0013 / ADR-0014).
//!
//! Jev answers typed System One questions (Choice / Score / Noul). Callers
//! (ask-path packing, citation, catalog scan, judges, the ingest door) speak
//! only [`DecisionProvider`]; which backend answers is chosen once per
//! process by [`decision_provider`]:
//!
//! * **OpenRouter** (default since 2026-10-08) — [`openrouter`], one
//!   forced-tool completion through the shared production OpenRouter client
//!   (same ZDR / no-training provider policy, metered into `llm_usage`).
//! * **TypeSafe System One** — [`typesafe`], kept behind
//!   `JEV_PROVIDER=typesafe` so it can come back. Its credits ran out (every
//!   call 402'd, i.e. Jev was permanently fail-open).
//!
//! Every caller treats any `Err` as fail-open.
//!
//! Metering (2026-10-18): every [`JevOp::Ask`] call runs on the managed
//! `interactive_ask` budget ([`AskBudget`], [`with_ask_budget`]): per-attempt
//! receipts reserved before the send and settled after, under a per-account
//! daily cap (`MANAGED_DAILY_CAP_USD_INTERACTIVE_ASK`). An Ask-op call with no
//! budget in scope is refused before anything is sent and fails open, so no
//! Ask-time Jev call can bypass metering. Judge and Ingest calls keep
//! [`with_usage`] attribution.
//!
//! Egress (ADR-0007 D11): only the two ask-path packing sites check
//! [`jev_may_send_content`] before calling a provider; the ask-path cite
//! and event-window calls run only once synthesis / Tier-2 egress is already
//! allowed. The background seats (ingest door, entity resolution, conflict
//! detection, digest, mention adjudicator, kind re-eval, GitHub summary)
//! have NO D11 check — they send the same text their own LLM fallbacks
//! already send to OpenRouter, so it is not new exposure, but it is
//! ungated (tracked separately).

pub mod cite;
pub mod ingest;
pub mod judge;
pub mod openrouter;
pub mod pack;
pub mod typesafe;

use std::collections::BTreeMap;
use std::sync::{Arc, OnceLock};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;

pub use cite::{
    apply_cite_verdicts, claim_sentence, claim_window, claims_from_atom_ids, compose_cite,
    extract_numbered_cites, normalize_quote, quote_in_source, unverified_compose, verify_citations,
    CiteClaim, CiteCompose, CiteVerdict, CITE_CHOICE_CRITERIA, CITE_CHOICE_INSTRUCTIONS,
    CITE_WINDOW_MAX, CITE_WINDOW_MIN, HONESTY_ALL_CITES_FAILED,
};
pub use ingest::{
    compose_pass2, extra_pairs_needed, hold_from_score, nearest_level, DoorHit, DoorHow, HoldLevel,
    IdentityAction, MentionJudgment, PASS1_CHUNK_IDS,
};
pub use judge::{
    apply_catalog_fetch, catalog_fail_open_scores, clip_catalog_digest, jev_catalog_scan_mode, jev_judge_mode,
    judge_capture, judge_catalog_scan, judge_catalog_scan_with_retry, judge_conflict,
    judge_contradiction_pair, judge_digest_keep, judge_event_period, judge_kind,
    judge_mention, judge_resolve_pair, judge_resolve_pair_served, judge_weekday, merge_catalog_cards, pick_corals_per_reef,
    pick_hybrid_or_membership, pick_hybrid_per_reef, reef_keyword_tsquery, seat_catalog_cards, select_yes_reefs,
    select_yes_reefs_typed, topic_first_pool, CatalogCard, CatalogNoul, JevCatalogScanMode,
    JevJudgeMode, MentionJudge, CATALOG_DIGEST_CHARS, CATALOG_TABLE_CAP, CATALOG_TYPE_FLOOR,
    CORALS_PER_YES_REEF, KEYWORD_STATE_TOKENS, MAX_YES_REEFS_FETCH, YES_REEF_TYPE_FLOOR,
};
pub use pack::{
    compose_pack, digest_excerpts, digest_first_last, evaluate_pack, evaluate_pack_by_reef,
    evaluate_pack_with_retry, group_digest_cards_by_reef, merge_pack_judgments, nearest_pack_role,
    pack_context_directive, pack_questions, packing_historical_only, packing_honesty_no_answer,
    seat_pack_cards, shrink_pack_cards, AtomJudgment, DigestCard, PackBlock, PackCompose, PackRole,
    PackedAtom, ReefSkinny, NOUL_YES_THRESHOLD, PACK_DIGEST_CHARS, PACK_REEF_FLOOR, PACK_SCORE_CRITERIA,
    PACK_SCORE_INSTRUCTIONS, PACK_TABLE_CAP,
};
pub use openrouter::OpenRouterDecisionClient;
pub use typesafe::TypeSafeClient;

/// `JEV_ASK_PATH_MODE` — ask-path packing + citation (ADR-0014).
///
/// Unset (tests) is **off**: retrieve → rerank → cap.
/// Production `fly.toml` is `live`. A Fly secret of the same name overrides
/// `[env]` — unset it on cutover so packing+cite actually replace the turn.
/// `shadow` runs Jev and logs without changing the turn.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JevAskPathMode {
    Off,
    Shadow,
    Live,
}

impl JevAskPathMode {
    pub fn from_env() -> Self {
        match std::env::var("JEV_ASK_PATH_MODE") {
            Ok(v) => Self::from_env_str(&v),
            Err(_) => Self::Off,
        }
    }

    pub fn from_env_str(v: &str) -> Self {
        match v.trim().to_ascii_lowercase().as_str() {
            "shadow" => Self::Shadow,
            "live" | "on" | "1" | "true" => Self::Live,
            _ => Self::Off,
        }
    }

    pub fn is_active(self) -> bool {
        matches!(self, Self::Shadow | Self::Live)
    }

    pub fn is_live(self) -> bool {
        matches!(self, Self::Live)
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Off => "off",
            Self::Shadow => "shadow",
            Self::Live => "live",
        }
    }
}

/// `JEV_INGEST_DOOR_MODE` — sequenced ingest-door worker (ADR-0013).
///
/// The `IngestDoor` worker and job kind are retired (2026-10-18). The mode is
/// still read by [`identifier_auto_merge_enabled`] (EntityResolution), so the
/// flag keeps its meaning until that worker's managed restoration decides it.
///
/// Default **off** in tests/unset. Production `fly.toml` sets `live`:
/// sequenced proposer then four Jev passes, then reef/edge writes.
/// `shadow` logs vs live extract without writing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JevIngestDoorMode {
    Off,
    Shadow,
    Live,
}

impl JevIngestDoorMode {
    pub fn from_env() -> Self {
        match std::env::var("JEV_INGEST_DOOR_MODE") {
            Ok(v) => Self::from_env_str(&v),
            Err(_) => Self::Off,
        }
    }

    pub fn from_env_str(v: &str) -> Self {
        match v.trim().to_ascii_lowercase().as_str() {
            "shadow" => Self::Shadow,
            "live" | "on" | "1" | "true" => Self::Live,
            _ => Self::Off,
        }
    }

    pub fn is_active(self) -> bool {
        matches!(self, Self::Shadow | Self::Live)
    }

    pub fn writes_live(self) -> bool {
        matches!(self, Self::Live)
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Off => "off",
            Self::Shadow => "shadow",
            Self::Live => "live",
        }
    }
}

pub fn jev_ingest_door_mode() -> JevIngestDoorMode {
    JevIngestDoorMode::from_env()
}

/// Identifier auto-merge is off in ingest-door shadow and after cutover
/// (ADR-0013). When the ingest-door worker is not running, today's ER
/// auto-merge stays.
pub fn identifier_auto_merge_enabled() -> bool {
    !jev_ingest_door_mode().is_active()
}

/// Specified enqueue of `reprocess_atom` for `identity_suspect` nouls.
/// **Off** until the ADR-0013 ingest-door worker writes for real.
pub fn enqueue_reprocess_from_str(v: &str) -> bool {
    matches!(
        v.trim().to_ascii_lowercase().as_str(),
        "1" | "true" | "yes" | "on"
    )
}

pub fn jev_ask_enqueue_reprocess() -> bool {
    match std::env::var("JEV_ASK_ENQUEUE_REPROCESS") {
        Ok(v) => enqueue_reprocess_from_str(&v),
        Err(_) => jev_ingest_door_mode().writes_live(),
    }
}

/// Which backend answers Jev questions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JevProviderKind {
    OpenRouter,
    TypeSafe,
}

impl JevProviderKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::OpenRouter => "openrouter",
            Self::TypeSafe => "typesafe",
        }
    }
}

/// Pure provider selection (unit-tested; [`jev_provider_kind`] feeds env).
///
/// * `JEV_PROVIDER=typesafe` → TypeSafe, iff `JEV_API_KEY` is set.
/// * `JEV_PROVIDER=openrouter`, unset, blank, or unrecognised → OpenRouter,
///   iff `OPENROUTER_API_KEY` is set. TypeSafe is never picked implicitly,
///   so a stale `JEV_API_KEY` secret cannot route Jev back to a provider
///   with no credits.
/// * The chosen provider's key missing → `None` (Jev off, callers fail-open).
pub fn select_provider(
    jev_provider: Option<&str>,
    openrouter_key_set: bool,
    typesafe_key_set: bool,
) -> Option<JevProviderKind> {
    let want = jev_provider.map(|v| v.trim().to_ascii_lowercase());
    match want.as_deref() {
        Some("typesafe") => typesafe_key_set.then_some(JevProviderKind::TypeSafe),
        _ => openrouter_key_set.then_some(JevProviderKind::OpenRouter),
    }
}

fn env_nonblank(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

/// `JEV_PROVIDER` + key presence → the process's provider kind.
pub fn jev_provider_kind() -> Option<JevProviderKind> {
    let raw = env_nonblank("JEV_PROVIDER");
    if let Some(v) = raw.as_deref() {
        if !matches!(v.to_ascii_lowercase().as_str(), "openrouter" | "typesafe") {
            tracing::warn!(value = v, "JEV_PROVIDER unrecognised — using openrouter");
        }
    }
    select_provider(
        raw.as_deref(),
        env_nonblank("OPENROUTER_API_KEY").is_some(),
        env_nonblank("JEV_API_KEY").is_some(),
    )
}

/// What a Jev request is for — picks the default model tier and labels the
/// `llm_usage.task` (`jev_ask` / `jev_judge` / `jev_ingest`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum JevOp {
    /// Interactive ask path: pack Score, cite Choice, catalog-scan Noul,
    /// event-window period/weekday, capture Nouls, contradiction pairs.
    /// Latency-bound → cheap tier.
    #[default]
    Ask,
    /// Background judges: kind, conflict, resolve-pair, mention, digest
    /// keep, GitHub summary staleness. Cheap tier, the same model each
    /// worker's own LLM fallback already uses.
    Judge,
    /// Ingest door passes 1–4 (identity binding over up to 255 reef ids,
    /// entity type, hold/supersession Scores). These write reefs and edges
    /// durably and run off the request path → capable tier.
    Ingest,
}

impl JevOp {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Ask => "ask",
            Self::Judge => "judge",
            Self::Ingest => "ingest",
        }
    }

    pub fn usage_task(self) -> &'static str {
        match self {
            Self::Ask => "jev_ask",
            Self::Judge => "jev_judge",
            Self::Ingest => "jev_ingest",
        }
    }

    fn model_env(self) -> &'static str {
        match self {
            Self::Ask => "JEV_MODEL_ASK",
            Self::Judge => "JEV_MODEL_JUDGE",
            Self::Ingest => "JEV_MODEL_INGEST",
        }
    }

    /// Default OpenRouter model for this op.
    pub fn default_openrouter_model(self) -> &'static str {
        match self {
            Self::Ask | Self::Judge => crate::llm::model_config::MODEL_FAST,
            Self::Ingest => crate::llm::model_config::MODEL_SMART,
        }
    }
}

/// TypeSafe's model id when `JEV_MODEL` is unset.
pub const TYPESAFE_DEFAULT_MODEL: &str = "jev-latest";

/// Pure model resolution (unit-tested; [`jev_model_for`] feeds env).
///
/// * TypeSafe: `JEV_MODEL`, else `jev-latest`.
/// * OpenRouter: `JEV_MODEL_<OP>`, else `JEV_MODEL`, else the op's tier
///   default. A value without a `/` is not an OpenRouter id (e.g. a stale
///   `JEV_MODEL=jev-latest` secret) and is ignored — sending it would 400
///   every call.
pub fn resolve_jev_model(
    kind: Option<JevProviderKind>,
    op: JevOp,
    jev_model: Option<&str>,
    op_model: Option<&str>,
) -> String {
    let clean = |v: Option<&str>| v.map(str::trim).filter(|s| !s.is_empty()).map(str::to_string);
    match kind {
        Some(JevProviderKind::TypeSafe) => {
            clean(jev_model).unwrap_or_else(|| TYPESAFE_DEFAULT_MODEL.to_string())
        }
        Some(JevProviderKind::OpenRouter) | None => clean(op_model)
            .filter(|m| m.contains('/'))
            .or_else(|| clean(jev_model).filter(|m| m.contains('/')))
            .unwrap_or_else(|| op.default_openrouter_model().to_string()),
    }
}

/// The three per-op model ids, resolved once per process.
struct JevModels {
    ask: String,
    judge: String,
    ingest: String,
}

static JEV_MODELS: OnceLock<JevModels> = OnceLock::new();

fn jev_models() -> &'static JevModels {
    JEV_MODELS.get_or_init(|| {
        let kind = jev_provider_kind();
        let jev = env_nonblank("JEV_MODEL");
        let resolve = |op: JevOp| {
            let per_op = env_nonblank(op.model_env());
            // Warn ONCE, here, not per request.
            if kind != Some(JevProviderKind::TypeSafe) {
                for (name, v) in [("JEV_MODEL", jev.as_deref()), (op.model_env(), per_op.as_deref())] {
                    if let Some(v) = v.filter(|v| !v.contains('/')) {
                        tracing::warn!(var = name, value = v, "not an OpenRouter model id — ignored");
                    }
                }
            }
            resolve_jev_model(kind, op, jev.as_deref(), per_op.as_deref())
        };
        JevModels {
            ask: resolve(JevOp::Ask),
            judge: resolve(JevOp::Judge),
            ingest: resolve(JevOp::Ingest),
        }
    })
}

/// Model id for a Jev request of kind `op` under the configured provider.
/// Resolved from env once per process ([`resolve_jev_model`] is the pure,
/// tested rule).
pub fn jev_model_for(op: JevOp) -> String {
    let m = jev_models();
    match op {
        JevOp::Ask => m.ask.clone(),
        JevOp::Judge => m.judge.clone(),
        JevOp::Ingest => m.ingest.clone(),
    }
}

/// Default whole-call budget for an ask/judge Jev request.
pub const JEV_DEFAULT_TIMEOUT_SECS: u64 = 8;
/// Default budget for an ingest-door Jev pass. Pass 1 on a medium ocean is
/// ~1.8k output tokens, ~12–20s on GLM-flash; off the request path, so a
/// generous ceiling costs nothing but a slower worker tick.
pub const JEV_DEFAULT_INGEST_TIMEOUT_SECS: u64 = 60;

fn parse_secs(v: Option<&str>) -> Option<u64> {
    v.and_then(|v| v.trim().parse::<u64>().ok()).filter(|&n| n > 0)
}

/// Pure per-op timeout rule (unit-tested; [`jev_timeout_for`] feeds env).
///
/// * Ask / Judge: `JEV_TIMEOUT_SECS`, default 8s. TypeSafe answered in
///   ~70–500ms; on OpenRouter expect ~0.5–2s for a judge Choice and
///   ~1.5–5s for a 48-card pack table (output tokens dominate).
/// * Ingest: `JEV_TIMEOUT_SECS_INGEST`, default 60s. Deliberately NOT
///   inherited from `JEV_TIMEOUT_SECS`, so tightening the ask budget never
///   starves the ingest door into timing out every pass.
pub fn resolve_jev_timeout(op: JevOp, base: Option<&str>, ingest: Option<&str>) -> std::time::Duration {
    let secs = match op {
        JevOp::Ask | JevOp::Judge => parse_secs(base).unwrap_or(JEV_DEFAULT_TIMEOUT_SECS),
        JevOp::Ingest => parse_secs(ingest).unwrap_or(JEV_DEFAULT_INGEST_TIMEOUT_SECS),
    };
    std::time::Duration::from_secs(secs)
}

pub fn jev_timeout_for(op: JevOp) -> std::time::Duration {
    resolve_jev_timeout(
        op,
        std::env::var("JEV_TIMEOUT_SECS").ok().as_deref(),
        std::env::var("JEV_TIMEOUT_SECS_INGEST").ok().as_deref(),
    )
}

/// TypeSafe's single request budget (ask/judge rule; TypeSafe was fast
/// enough that it never needed a separate ingest budget).
pub fn jev_timeout_secs() -> u64 {
    jev_timeout_for(JevOp::Ask).as_secs()
}

/// Grace on top of the per-call budget for a whole ask-path phase, so a
/// single slow call's own deadline (which finalizes its `llm_usage` row as
/// `_errored`) normally fires before the phase-level cut does.
pub const ASK_PHASE_GRACE: std::time::Duration = std::time::Duration::from_millis(250);

/// Bound a WHOLE ask-path phase — its first call plus any 400-shrink
/// retries — so each of catalog scan, pack and cite costs at most
/// `JEV_TIMEOUT_SECS` + [`ASK_PHASE_GRACE`] (≤ ~24.75s for all three at the
/// 8s default), and then fails open with [`DecisionError::Timeout`].
pub async fn within_ask_budget<T, F>(fut: F) -> Result<T, DecisionError>
where
    F: std::future::Future<Output = Result<T, DecisionError>>,
{
    within_budget(jev_timeout_for(JevOp::Ask) + ASK_PHASE_GRACE, fut).await
}

tokio::task_local! {
    static ASK_PHASE: &'static str;
    static ASK_PHASE_DEADLINE: tokio::time::Instant;
}

pub(crate) fn current_ask_phase_deadline() -> Option<tokio::time::Instant> {
    ASK_PHASE_DEADLINE.try_with(|deadline| *deadline).ok()
}

pub(crate) fn current_ask_phase() -> Option<&'static str> {
    ASK_PHASE.try_with(|phase| *phase).ok()
}

/// Name the bounded phase for attribution. Same-task combinators inherit it;
/// spawned metering tasks explicitly re-enter the scope below.
pub(crate) async fn in_ask_phase<F: std::future::Future>(phase: Option<&'static str>, fut: F) -> F::Output {
    match phase {
        Some(phase) => ASK_PHASE.scope(phase, fut).await,
        None => fut.await,
    }
}

pub(crate) async fn within_ask_phase<T, F>(phase: &'static str, fut: F) -> Result<T, DecisionError>
where F: std::future::Future<Output = Result<T, DecisionError>> {
    in_ask_phase(Some(phase), async {
        let budget = jev_timeout_for(JevOp::Ask) + ASK_PHASE_GRACE;
        let deadline = tokio::time::Instant::now() + budget;
        let result = ASK_PHASE_DEADLINE.scope(deadline, within_budget(budget, fut)).await;
        if let Err(error) = &result {
            let error_class = match error {
                DecisionError::Timeout => "timeout",
                DecisionError::Auth(_) => "auth",
                DecisionError::RateLimit(_) => "rate_limit",
                DecisionError::Parse(_) => "parse",
                DecisionError::NotConfigured => "not_configured",
                DecisionError::Upstream { .. } => "upstream",
                DecisionError::Budget(_) => "budget",
            };
            let source_query_id = current_usage().and_then(|ctx| ctx.source_query_id);
            tracing::warn!(phase, error_class, source_query_id = ?source_query_id,
                fallback = true, "ask decision phase failed open");
        }
        result
    }).await
}

pub async fn within_budget<T, F>(budget: std::time::Duration, fut: F) -> Result<T, DecisionError>
where
    F: std::future::Future<Output = Result<T, DecisionError>>,
{
    tokio::time::timeout(budget, fut)
        .await
        .unwrap_or(Err(DecisionError::Timeout))
}

/// Whether the event-window classifier should consult Jev before its own
/// Tier-2 LLM call. Only for TypeSafe: on OpenRouter Jev is the same model
/// class as that fallback (`deepseek-v4-flash`) answering the same closed
/// question, so it adds one or two serial round-trips (period, then
/// weekday) inside a 1.2s budget for no quality gain — the fallback alone
/// fits the budget, Jev-then-fallback does not.
pub fn jev_in_event_window(kind: Option<JevProviderKind>) -> bool {
    kind == Some(JevProviderKind::TypeSafe)
}

/// Process-wide provider, built once (see [`jev_provider_kind`]).
type ProviderCell = (Option<JevProviderKind>, Option<Arc<dyn DecisionProvider>>);

static DECISION_PROVIDER: OnceLock<ProviderCell> = OnceLock::new();

fn provider_cell() -> &'static ProviderCell {
    DECISION_PROVIDER.get_or_init(|| {
        let kind = jev_provider_kind();
        let provider = kind.and_then(build_decision_provider);
        (provider.as_ref().and(kind), provider)
    })
}

pub fn decision_provider() -> Option<Arc<dyn DecisionProvider>> {
    #[cfg(feature = "test-support")]
    if let Some(provider) = TEST_DECISION_PROVIDER.read().ok().and_then(|p| p.clone()) {
        return Some(provider);
    }
    provider_cell().1.clone()
}

#[cfg(feature = "test-support")]
static TEST_DECISION_PROVIDER: std::sync::RwLock<Option<Arc<dyn DecisionProvider>>> =
    std::sync::RwLock::new(None);

/// Test support: make [`decision_provider`] return `provider` (or the
/// env-built one again with `None`), so an integration test can count every
/// Jev request a real Ask route makes.
#[cfg(feature = "test-support")]
pub fn test_override_decision_provider(provider: Option<Arc<dyn DecisionProvider>>) {
    *TEST_DECISION_PROVIDER.write().unwrap() = provider;
}

/// The kind of the provider [`decision_provider`] returns (`None` iff it
/// returns `None`).
pub fn decision_provider_kind() -> Option<JevProviderKind> {
    provider_cell().0
}

fn build_decision_provider(kind: JevProviderKind) -> Option<Arc<dyn DecisionProvider>> {
    match kind {
        JevProviderKind::TypeSafe => {
            let key = env_nonblank("JEV_API_KEY")?;
            match TypeSafeClient::from_api_key(&key) {
                Ok(client) => {
                    tracing::info!("Jev DecisionProvider: typesafe");
                    Some(Arc::new(client) as Arc<dyn DecisionProvider>)
                }
                Err(e) => {
                    tracing::error!(error = %e, "Jev: TypeSafe client failed to build — fail-open");
                    None
                }
            }
        }
        JevProviderKind::OpenRouter => {
            let timeouts = openrouter::JevTimeouts {
                default: jev_timeout_for(JevOp::Ask),
                ingest: jev_timeout_for(JevOp::Ingest),
            };
            // The per-call deadline lives in `OpenRouterDecisionClient`; the
            // reqwest timeout is only a backstop above the longest one.
            let http = match reqwest::Client::builder()
                .timeout(timeouts.default.max(timeouts.ingest) + std::time::Duration::from_secs(2))
                .build()
            {
                Ok(c) => c,
                Err(e) => {
                    tracing::error!(error = %e, "Jev: OpenRouter HTTP client failed to build — fail-open");
                    return None;
                }
            };
            let llm = crate::build_cloud_llm_provider(http, env_nonblank("OPENROUTER_API_KEY"))?;
            // Managed Ask calls get a transport whose HTTP timeout is the Ask
            // budget, so a slow attempt ends (and settles its receipt) inside
            // the gated transport rather than being dropped mid-send.
            let ask_llm = reqwest::Client::builder()
                .timeout(timeouts.default + std::time::Duration::from_secs(1))
                .build()
                .ok()
                .and_then(|http| crate::build_cloud_llm_provider(http, env_nonblank("OPENROUTER_API_KEY")));
            tracing::info!(
                ask = %jev_model_for(JevOp::Ask),
                judge = %jev_model_for(JevOp::Judge),
                ingest = %jev_model_for(JevOp::Ingest),
                timeout_secs = timeouts.default.as_secs(),
                ingest_timeout_secs = timeouts.ingest.as_secs(),
                "Jev DecisionProvider: openrouter"
            );
            let mut client = OpenRouterDecisionClient::new(llm, timeouts);
            if let Some(ask_llm) = ask_llm {
                client = client.with_ask_transport(ask_llm);
            }
            Some(Arc::new(client) as Arc<dyn DecisionProvider>)
        }
    }
}

tokio::task_local! {
    /// The `llm_usage` attribution for Jev calls made inside
    /// [`with_usage`]. A task-local rather than a `DecisionRequest` field so
    /// the six request builders and every judge signature stay unchanged.
    /// Note that `tokio::spawn` does NOT inherit it; same-task combinators
    /// (`join_all`, `buffer_unordered`) do.
    static JEV_USAGE: Option<crate::llm::usage::UsageContext>;
}

/// Run `fut` with `ctx` as the `llm_usage` attribution for every Jev call
/// it makes. TypeSafe ignores it; OpenRouter meters through it.
pub async fn with_usage<F: std::future::Future>(
    ctx: Option<crate::llm::usage::UsageContext>,
    fut: F,
) -> F::Output {
    JEV_USAGE.scope(ctx, fut).await
}

pub(crate) fn current_usage() -> Option<crate::llm::usage::UsageContext> {
    JEV_USAGE.try_with(Clone::clone).ok().flatten()
}

/// Who pays for, and may be fenced out of, the Jev calls of one
/// user-initiated Ask: the managed `interactive_ask` budget
/// (`managed::interactive_ask`). Owned, so the spawned metering task can carry
/// it past a phase deadline.
///
/// The payer is the asker on their own account, the same pair the HTTP and
/// MCP interactive credit gate (`billing::enforcement::is_blocked`) checks.
/// `content` is the plane the receipts live on: the control pool for a
/// consumer account, the org's tenant pool otherwise.
#[derive(Clone, Debug)]
pub struct AskBudget {
    pub(crate) content: sqlx::PgPool,
    pub(crate) control: sqlx::PgPool,
    pub(crate) tenant: crate::tenancy::TenantId,
    pub(crate) account_id: uuid::Uuid,
    pub(crate) asker: uuid::Uuid,
    pub(crate) graph_id: uuid::Uuid,
    pub(crate) query_id: uuid::Uuid,
}

impl AskBudget {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        content: sqlx::PgPool,
        control: sqlx::PgPool,
        tenant: crate::tenancy::TenantId,
        account_id: uuid::Uuid,
        asker: uuid::Uuid,
        graph_id: uuid::Uuid,
        query_id: uuid::Uuid,
    ) -> Self {
        Self { content, control, tenant, account_id, asker, graph_id, query_id }
    }

    /// The budget for an Ask request, or `None` when the request has no
    /// Postgres content plane or no control plane. With
    /// `None`, every Ask-op Jev call fails open unsent.
    pub(crate) fn for_request(
        control: Option<&sqlx::PgPool>,
        content: Option<&sqlx::PgPool>,
        tenancy: Option<&crate::tenancy::runtime::TenancyDecision>,
        user: &crate::UserContext,
        graph_id: uuid::Uuid,
        query_id: uuid::Uuid,
    ) -> Option<Self> {
        let tenant = match tenancy {
            Some(crate::tenancy::runtime::TenancyDecision::Org(ctx)) => ctx.tenant_id(),
            _ => crate::tenancy::TenantId::Consumer,
        };
        Some(Self::new(
            content?.clone(),
            control?.clone(),
            tenant,
            user.account_id,
            user.user_id,
            graph_id,
            query_id,
        ))
    }

    pub fn query_id(&self) -> uuid::Uuid {
        self.query_id
    }
}

tokio::task_local! {
    /// The managed Ask budget for Jev calls made inside [`with_ask_budget`].
    /// Separate from [`JEV_USAGE`], so an inner [`with_usage`] keeps it.
    static JEV_ASK_BUDGET: Option<AskBudget>;
}

/// Run `fut` with `budget` paying for every [`JevOp::Ask`] call it makes.
/// Same-task combinators inherit it; `tokio::spawn` does not.
pub async fn with_ask_budget<F: std::future::Future>(budget: Option<AskBudget>, fut: F) -> F::Output {
    JEV_ASK_BUDGET.scope(budget, fut).await
}

pub(crate) fn current_ask_budget() -> Option<AskBudget> {
    JEV_ASK_BUDGET.try_with(Clone::clone).ok().flatten()
}

/// ADR-0007 D11 gate for the ask path: may this turn send coral text to
/// the Jev provider at all? Same rule as synthesis — never for a
/// sources-only turn, a turn with no LLM, or an ocean whose owner forbids
/// cloud egress. Provider-agnostic on purpose: OpenRouter is cloud egress
/// exactly as TypeSafe was. Callers check this BEFORE `decision_provider()`.
pub fn jev_may_send_content(sources_only: bool, llm_available: bool, egress_blocked: bool) -> bool {
    !sources_only && llm_available && !egress_blocked
}

/// A typed System One request. Question ids are for code; they are not sent
/// to the model as meaning.
#[derive(Debug, Clone, Serialize)]
pub struct DecisionRequest {
    pub state: Value,
    pub model: String,
    pub questions: BTreeMap<String, Question>,
    /// Not on the System One wire; picks the `llm_usage` task on OpenRouter.
    #[serde(skip)]
    pub op: JevOp,
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum Question {
    Noul {
        instructions: Value,
        #[serde(skip_serializing_if = "Option::is_none")]
        criteria: Option<NoulCriteria>,
    },
    Choice {
        instructions: Value,
        criteria: BTreeMap<String, Option<String>>,
    },
    Score {
        instructions: Value,
        criteria: Vec<String>,
    },
}

#[derive(Debug, Clone, Serialize)]
pub struct NoulCriteria {
    #[serde(rename = "true")]
    pub yes: String,
    #[serde(rename = "false")]
    pub no: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct DecisionResponse {
    pub model: String,
    pub answers: BTreeMap<String, Answer>,
    #[serde(default)]
    pub usage: Option<DecisionUsage>,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct DecisionUsage {
    #[serde(default)]
    pub input_tokens: u32,
    #[serde(default)]
    pub output_tokens: u32,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum Answer {
    Noul {
        noul: f64,
    },
    Choice {
        choice: String,
        #[serde(default)]
        probabilities: BTreeMap<String, f64>,
        #[serde(default)]
        confidence: Option<f64>,
    },
    Score {
        score: f64,
        #[serde(default)]
        legend: BTreeMap<String, String>,
        #[serde(default)]
        probabilities: BTreeMap<String, f64>,
        #[serde(default)]
        confidence: f64,
    },
}

impl Answer {
    pub fn as_noul(&self) -> Option<f64> {
        match self {
            Self::Noul { noul } => Some(*noul),
            _ => None,
        }
    }

    pub fn as_choice(&self) -> Option<&str> {
        match self {
            Self::Choice { choice, .. } => Some(choice.as_str()),
            _ => None,
        }
    }

    /// Explicit Choice confidence, else the selected choice's probability, else 0.0.
    pub fn choice_confidence(&self) -> f64 {
        match self {
            Self::Choice {
                choice,
                confidence,
                probabilities,
            } => confidence.unwrap_or_else(|| probabilities.get(choice).copied().unwrap_or(0.0)),
            _ => 0.0,
        }
    }

    pub fn as_score(&self) -> Option<f64> {
        match self {
            Self::Score { score, .. } => Some(*score),
            _ => None,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum DecisionError {
    #[error("timeout")]
    Timeout,
    #[error("auth error: {0}")]
    Auth(String),
    #[error("rate limited: {0}")]
    RateLimit(String),
    #[error("upstream error {status}: {body}")]
    Upstream { status: u16, body: String },
    #[error("parse error: {0}")]
    Parse(String),
    #[error("not configured")]
    NotConfigured,
    /// The managed `interactive_ask` budget held or refused the call, or no
    /// budget was in scope. Nothing was sent for the denied attempt.
    /// Fail-open, never retried by the shrink path.
    #[error("managed budget: {0}")]
    Budget(String),
}

impl DecisionError {
    pub fn is_fail_open(&self) -> bool {
        !matches!(self, Self::Parse(_))
    }

    /// A `400` — TypeSafe `max_tokens_exceeded`, or an OpenRouter
    /// context-length/request rejection. Not transient; the same payload
    /// will 400 again. Pack/cite/catalog shrink once, then fail-open.
    pub fn is_context_overflow(&self) -> bool {
        matches!(self, Self::Upstream { status: 400, .. })
    }

    /// Same payload → same failure: a parse/validation error (including a
    /// `max_tokens` truncation) or a `400`. Re-sending it re-bills the same
    /// call, so job retries must be capped (see `workers::ingest_door`).
    /// Timeouts, 429s, 5xx, auth and credit errors are NOT deterministic.
    pub fn is_deterministic(&self) -> bool {
        matches!(self, Self::Parse(_)) || self.is_context_overflow()
    }
}

#[async_trait]
pub trait DecisionProvider: Send + Sync {
    async fn evaluate(&self, req: DecisionRequest) -> Result<DecisionResponse, DecisionError>;
}

/// Shared "these sources are prior state" line for a historical-only pack.
pub const HISTORICAL_ONLY_DIRECTIVE: &str = "\
These CONTEXT sources are prior state, not current fact. Answer historically \
when they are the only evidence. Do not present a superseded or older account \
as the current answer.";

/// Honesty line when packing admits nothing current (no evidence, no conflict).
pub const PACK_NO_ANSWER_PROSE: &str = "\
I don't have current evidence for this in the selected oceans. The retrieved \
sources were off-topic, empty, or only prior state that shouldn't be treated \
as an answer.";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn context_overflow_is_400() {
        assert!(DecisionError::Upstream {
            status: 400,
            body: "max_tokens_exceeded".into(),
        }
        .is_context_overflow());
        assert!(!DecisionError::Timeout.is_context_overflow());
        assert!(!DecisionError::Upstream {
            status: 529,
            body: "overloaded".into(),
        }
        .is_context_overflow());
    }

    #[test]
    fn provider_selection_defaults_to_openrouter_and_never_implicitly_typesafe() {
        use JevProviderKind::*;
        // Default: OpenRouter whenever its key is present.
        assert_eq!(select_provider(None, true, false), Some(OpenRouter));
        assert_eq!(select_provider(Some(""), true, true), Some(OpenRouter));
        assert_eq!(select_provider(Some("  OpenRouter "), true, true), Some(OpenRouter));
        // A leftover JEV_API_KEY alone must not route back to TypeSafe.
        assert_eq!(select_provider(None, false, true), None);
        assert_eq!(select_provider(Some("openrouter"), false, true), None);
        // Garbage behaves like unset.
        assert_eq!(select_provider(Some("anthropic"), true, true), Some(OpenRouter));
        // TypeSafe only when asked for AND keyed.
        assert_eq!(select_provider(Some("typesafe"), true, true), Some(TypeSafe));
        assert_eq!(select_provider(Some("TYPESAFE"), true, true), Some(TypeSafe));
        assert_eq!(select_provider(Some("typesafe"), true, false), None);
        assert_eq!(select_provider(None, false, false), None);
    }

    #[test]
    fn model_resolution_per_provider_and_op() {
        use JevProviderKind::*;
        let fast = crate::llm::model_config::MODEL_FAST;
        let smart = crate::llm::model_config::MODEL_SMART;
        // OpenRouter tier defaults.
        assert_eq!(resolve_jev_model(Some(OpenRouter), JevOp::Ask, None, None), fast);
        assert_eq!(resolve_jev_model(Some(OpenRouter), JevOp::Judge, None, None), fast);
        assert_eq!(resolve_jev_model(Some(OpenRouter), JevOp::Ingest, None, None), smart);
        // JEV_MODEL overrides every op; JEV_MODEL_<OP> overrides JEV_MODEL.
        assert_eq!(
            resolve_jev_model(Some(OpenRouter), JevOp::Ingest, Some("x/y"), None),
            "x/y"
        );
        assert_eq!(
            resolve_jev_model(Some(OpenRouter), JevOp::Ask, Some("x/y"), Some(" a/b ")),
            "a/b"
        );
        // A stale TypeSafe id is not an OpenRouter id — ignored, never sent.
        assert_eq!(
            resolve_jev_model(Some(OpenRouter), JevOp::Ask, Some("jev-latest"), None),
            fast
        );
        assert_eq!(
            resolve_jev_model(Some(OpenRouter), JevOp::Ingest, Some("x/y"), Some("jev-2")),
            "x/y"
        );
        // TypeSafe keeps its own ids.
        assert_eq!(resolve_jev_model(Some(TypeSafe), JevOp::Ingest, None, None), "jev-latest");
        assert_eq!(
            resolve_jev_model(Some(TypeSafe), JevOp::Ask, Some("jev-next"), Some("a/b")),
            "jev-next"
        );
    }

    #[test]
    fn egress_gate_blocks_every_no_send_case() {
        assert!(jev_may_send_content(false, true, false));
        assert!(!jev_may_send_content(true, true, false), "sources-only");
        assert!(!jev_may_send_content(false, false, false), "no LLM");
        assert!(!jev_may_send_content(false, true, true), "D11 egress blocked");
        assert!(!jev_may_send_content(true, false, true));
    }

    #[test]
    fn usage_task_labels_are_distinct() {
        assert_eq!(JevOp::Ask.usage_task(), "jev_ask");
        assert_eq!(JevOp::Judge.usage_task(), "jev_judge");
        assert_eq!(JevOp::Ingest.usage_task(), "jev_ingest");
    }

    // ── Ask-path phase bounds (JEV_ASK_PATH_MODE=live stays on) ─────────────

    /// Sleeps `delay`, then either 400s (driving the shrink retries) or
    /// answers nothing.
    struct SlowProvider {
        delay: std::time::Duration,
        overflow: bool,
    }

    #[async_trait]
    impl DecisionProvider for SlowProvider {
        async fn evaluate(&self, req: DecisionRequest) -> Result<DecisionResponse, DecisionError> {
            tokio::time::sleep(self.delay).await;
            if self.overflow {
                Err(DecisionError::Upstream { status: 400, body: "context length".into() })
            } else {
                Ok(DecisionResponse { model: req.model, answers: BTreeMap::new(), usage: None })
            }
        }
    }

    fn catalog() -> Vec<CatalogCard> {
        (0..4u128)
            .map(|i| CatalogCard {
                id: uuid::Uuid::from_u128(i + 1),
                title: format!("reef {i}"),
                excerpt: "digest".into(),
                reef_type: "topic".into(),
                coral_count: 3,
                bound: i % 2 == 0,
            })
            .collect()
    }

    fn pack_cards() -> Vec<DigestCard> {
        let reef = uuid::Uuid::from_u128(99);
        (0..6u128)
            .map(|i| DigestCard {
                atom_id: uuid::Uuid::from_u128(i + 10),
                reef_title: "r".into(),
                reef_type: "topic".into(),
                digest: "d".into(),
                reef_id: Some(reef),
            })
            .collect()
    }

    fn cite_inputs() -> (Vec<CiteClaim>, std::collections::HashMap<uuid::Uuid, String>) {
        let id = uuid::Uuid::from_u128(7);
        (
            vec![CiteClaim { n: Some(1), atom_id: id, claim: "x".into(), quote: None }],
            std::collections::HashMap::from([(id, "body ".repeat(200))]),
        )
    }

    /// Each phase — including its 400-shrink retry chain — fails open with
    /// `Timeout` within one budget, and the three together stay well under
    /// the 60s stream-stall / proxy-idle limits and MCP's 180s sync wait.
    #[tokio::test(start_paused = true)]
    async fn each_ask_phase_is_bounded_and_fails_open() {
        let cap = jev_timeout_for(JevOp::Ask) + ASK_PHASE_GRACE;
        let (claims, bodies) = cite_inputs();
        let mut total = std::time::Duration::ZERO;
        for provider in [
            // Stalled host.
            SlowProvider { delay: std::time::Duration::from_secs(1000), overflow: false },
            // Slow 400s: unbounded, catalog would run 3 serial calls (18s+)
            // and pack/cite 2 (12s).
            SlowProvider { delay: std::time::Duration::from_secs(6), overflow: true },
        ] {
            let t = tokio::time::Instant::now();
            let r = judge_catalog_scan_with_retry(&provider, "ask", &catalog(), CatalogNoul::Source).await;
            assert!(matches!(r, Err(DecisionError::Timeout)), "catalog: {r:?}");
            let catalog_t = t.elapsed();

            let t = tokio::time::Instant::now();
            let r = evaluate_pack_with_retry(&provider, "ask", "q", &pack_cards()).await;
            assert!(matches!(r, Err(DecisionError::Timeout)), "pack: {r:?}");
            let pack_t = t.elapsed();

            let t = tokio::time::Instant::now();
            let r = cite::evaluate_cite_with_retry(&provider, "answer", &claims, &bodies).await;
            assert!(matches!(r, Err(DecisionError::Timeout)), "cite: {r:?}");
            let cite_t = t.elapsed();

            for (name, d) in [("catalog", catalog_t), ("pack", pack_t), ("cite", cite_t)] {
                assert!(d <= cap, "{name} took {d:?} > {cap:?}");
            }
            total = total.max(catalog_t + pack_t + cite_t);
        }
        assert!(total <= cap * 3);
        assert!(total < std::time::Duration::from_secs(60), "{total:?}");
    }

    #[test]
    fn timeouts_are_per_op_and_ingest_is_independent() {
        use std::time::Duration;
        assert_eq!(resolve_jev_timeout(JevOp::Ask, None, None), Duration::from_secs(8));
        assert_eq!(resolve_jev_timeout(JevOp::Judge, None, None), Duration::from_secs(8));
        assert_eq!(resolve_jev_timeout(JevOp::Ingest, None, None), Duration::from_secs(60));
        // Tightening the ask budget never shrinks ingest.
        assert_eq!(resolve_jev_timeout(JevOp::Ingest, Some("3"), None), Duration::from_secs(60));
        assert_eq!(resolve_jev_timeout(JevOp::Ask, Some("3"), Some("90")), Duration::from_secs(3));
        assert_eq!(resolve_jev_timeout(JevOp::Ingest, Some("3"), Some(" 90 ")), Duration::from_secs(90));
        // Garbage / zero fall back.
        assert_eq!(resolve_jev_timeout(JevOp::Ingest, None, Some("0")), Duration::from_secs(60));
        assert_eq!(resolve_jev_timeout(JevOp::Ask, Some("x"), None), Duration::from_secs(8));
    }

    #[test]
    fn deterministic_errors_are_parse_and_400_only() {
        assert!(DecisionError::Parse("bad id".into()).is_deterministic());
        assert!(DecisionError::Upstream { status: 400, body: String::new() }.is_deterministic());
        for e in [
            DecisionError::Timeout,
            DecisionError::RateLimit(String::new()),
            DecisionError::Auth(String::new()),
            DecisionError::Upstream { status: 402, body: String::new() },
            DecisionError::Upstream { status: 503, body: String::new() },
        ] {
            assert!(!e.is_deterministic(), "{e}");
        }
    }

    #[test]
    fn event_window_consults_jev_only_on_typesafe() {
        assert!(jev_in_event_window(Some(JevProviderKind::TypeSafe)));
        assert!(!jev_in_event_window(Some(JevProviderKind::OpenRouter)));
        assert!(!jev_in_event_window(None));
    }

    #[test]
    fn mode_defaults_off() {
        // Cannot unset process env safely alongside other tests; just parse.
        assert_eq!(JevAskPathMode::from_env_str(""), JevAskPathMode::Off);
        assert_eq!(JevAskPathMode::from_env_str("off"), JevAskPathMode::Off);
        assert_eq!(
            JevAskPathMode::from_env_str("SHADOW"),
            JevAskPathMode::Shadow
        );
        assert_eq!(JevAskPathMode::from_env_str("live"), JevAskPathMode::Live);
        assert_eq!(JevAskPathMode::from_env_str("garbage"), JevAskPathMode::Off);
        assert_eq!(JevIngestDoorMode::from_env_str(""), JevIngestDoorMode::Off);
        assert_eq!(
            JevIngestDoorMode::from_env_str("shadow"),
            JevIngestDoorMode::Shadow
        );
        assert_eq!(
            JevIngestDoorMode::from_env_str("LIVE"),
            JevIngestDoorMode::Live
        );
        assert!(JevIngestDoorMode::Live.writes_live());
        assert!(!JevIngestDoorMode::Shadow.writes_live());
    }

    #[test]
    fn enqueue_defaults_off() {
        assert!(!enqueue_reprocess_from_str(""));
        assert!(!enqueue_reprocess_from_str("off"));
        assert!(enqueue_reprocess_from_str("true"));
    }

    #[test]
    fn ask_fixture_covers_gate_categories() {
        let raw = include_str!("../../eval/jev_ask_fixture.json");
        let v: Value = serde_json::from_str(raw).expect("jev_ask_fixture.json");
        let qs = v["questions"].as_array().expect("questions");
        assert!(qs.len() >= 20 && qs.len() <= 30, "synthetic fixture is 20–30");
        let mut cats = std::collections::HashSet::new();
        for q in qs {
            cats.insert(q["category"].as_str().unwrap_or_default());
        }
        for need in [
            "fabrication",
            "hub_pollution",
            "superseded",
            "unanswerable",
            "cross_ocean",
        ] {
            assert!(cats.contains(need), "missing category {need}");
        }
    }
}

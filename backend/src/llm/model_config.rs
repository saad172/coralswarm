//! Central per-task LLM model + cap configuration (audit S1 / H4).
//!
//! Every call site that talks to the LLM previously hardcoded its model id and
//! its token / entity caps. This module folds those into one place and lets each
//! be overridden via an environment variable. **Defaults equal the previous
//! hardcoded constants** — with no env vars set, behavior is byte-for-byte the
//! same as before this module existed.
//!
//! Resolution is intentionally per-call (`std::env::var` on each lookup) rather
//! than cached: these functions run on the job-worker / request path, not in a
//! hot loop, and reading the env keeps tests and live re-config trivial. The
//! results are cheap `String`/scalar values.

// ── Default model ids (the previous hardcoded constants) ──────────────────────

/// Default model for entity extraction (`workers/extract.rs`).
pub const DEFAULT_EXTRACT_MODEL: &str = "deepseek/deepseek-v4-flash-0731";
/// Default model for entity-resolution adjudication (`workers/entity_resolution.rs`).
pub const DEFAULT_RESOLVE_MODEL: &str = "deepseek/deepseek-v4-flash-0731";
/// Default model for query synthesis / **answer prose** on paths with no
/// per-user model picker. Matches `SYNTHESIS_MODEL` in `routes/queries.rs` so
/// the existing env var keeps its meaning when synthesis is wired to this
/// module.
///
/// Capable-tier GLM (OpenRouter).
///
/// **This is no longer the aux-model default** — see [`DEFAULT_AUX_MODEL`]/
/// [`aux_model`]. Auxiliary/cheap-tier call sites (bulk atom-title backfill)
/// stay on the cheap DeepSeek tier independently of this synthesis default.
pub const DEFAULT_SYNTHESIS_MODEL: &str = "z-ai/glm-5.3-flash";
/// Default model for auxiliary / cheap-tier LLM passes that are NOT the
/// user-facing answer prose: bulk atom-title backfill
/// (`workers::atom_title_backfill`). Cheap-tier DeepSeek — high-volume,
/// low-stakes — and deliberately INDEPENDENT of [`DEFAULT_SYNTHESIS_MODEL`]
/// now that the latter is capable-tier GLM; see that constant's doc comment for
/// why this split exists.
pub const DEFAULT_AUX_MODEL: &str = "deepseek/deepseek-v4-flash-0731";
/// Default model for expertise inference (`workers/infer_expertise.rs`). This
/// is a judgment task — picking the likeliest topic owner and a defensible
/// confidence from the surrounding evidence — so it defaults to a
/// synthesis-grade (GLM) model rather than the cheap-tier extraction model.
pub const DEFAULT_EXPERTISE_MODEL: &str = "z-ai/glm-5.3-flash";

// ── Default caps (the previous hardcoded constants) ───────────────────────────

/// Default `max_tokens` for the entity-resolution adjudication call.
pub const DEFAULT_RESOLVE_MAX_TOKENS: u32 = 256;
/// Default `max_tokens` for the expertise-inference review call.
pub const DEFAULT_EXPERTISE_MAX_TOKENS: u32 = 512;
/// Default minimum verdict confidence to auto-merge two reefs.
pub const DEFAULT_MERGE_CONFIDENCE_THRESHOLD: f64 = 0.85;
/// Default confidence threshold below which extraction escalates to a stronger
/// model. Only consulted when the (opt-in) escalation model is configured.
pub const DEFAULT_EXTRACT_ESCALATE_THRESHOLD: f32 = 0.75;

/// Identity-resolution-at-the-door design doc §10.2: reef mass at/above which
/// a near-match candidate is "consequential" enough to spend a door-side
/// adjudication call on, and (§10.5) at/above which an ER pair endpoint counts
/// toward the "both endpoints consequential" escalation gate. Shared by
/// whichever builder's code reaches this constant first — B1 (the door seam)
/// and B3 (ER + expertise) both read the same knob by design (§10.1's file
/// ownership note), so a duplicate definition on merge is a trivial, textually
/// identical conflict.
pub const DEFAULT_DOOR_ADJUDICATE_MIN_CORALS: i64 = 25;
/// Companion relationship-edge-degree threshold — see
/// [`DEFAULT_DOOR_ADJUDICATE_MIN_CORALS`].
pub const DEFAULT_DOOR_ADJUDICATE_MIN_DEGREE: i64 = 100;
/// Default model for §3.4 conflict-detection adjudication
/// (`workers/conflict_detection.rs`) — the SAME tier as entity-resolution
/// adjudication (a cheap yes/no/unsure verdict), on its own env var rather
/// than sharing `RESOLVE_MODEL` so the two can be tuned independently even
/// though they start identical.
pub const DEFAULT_CONFLICT_MODEL: &str = "deepseek/deepseek-v4-flash-0731";
/// Default `max_tokens` for the conflict-detection adjudication call.
pub const DEFAULT_CONFLICT_MAX_TOKENS: u32 = 256;
/// Default minimum verdict confidence to write a conflict-detected
/// supersession edge. Mirrors [`DEFAULT_MERGE_CONFIDENCE_THRESHOLD`]'s value
/// (same cost asymmetry: a false positive here creates a real graph edge).
pub const DEFAULT_CONFLICT_CONFIDENCE_THRESHOLD: f64 = 0.85;
/// Default number of days an "unsure" `conflict_dismissal` row stays live
/// (`workers::conflict_detection`'s D3-analogue negative marker — see
/// `migrations_control/0112_conflict_dismissal.sql`'s header for the full
/// policy). NOT a new number invented for this table: it is this repo's
/// existing default retention/revisit cadence, already established by
/// [`crate::store::TOMBSTONE_RETENTION_DAYS`] and
/// `workers::access_log_purge::ACCESS_LOG_RETENTION_DAYS` (both 30). A
/// "different" dismissal never expires by time at all — this constant only
/// ever bounds "unsure" rows.
pub const DEFAULT_UNSURE_DISMISSAL_TTL_DAYS: i64 = 30;

// ── Env-var helpers ───────────────────────────────────────────────────────────

/// Read a model id from `var`, falling back to `default` when unset or blank.
///
/// `pub(crate)` so the two `DIGEST_MODEL` call sites outside this module use
/// it too. They previously read the var raw, which made `DIGEST_MODEL` the
/// one model var out of fifteen where a trailing newline from
/// `fly secrets set` survived into the request body — OpenRouter matches
/// requested model ids by exact string equality, so an untrimmed value fails
/// closed (a 400/404 from the model-id validation) before any real work
/// happens.
pub(crate) fn model_from_env(var: &str, default: &str) -> String {
    match std::env::var(var) {
        Ok(v) if !v.trim().is_empty() => v.trim().to_string(),
        _ => default.to_string(),
    }
}

/// Read a positive (nonzero) `u32` from `var`, falling back to `default` when
/// unset, blank, unparseable, OR zero. A parsed `0` would break the LLM call
/// it bounds (an empty completion), so it gets the same fallback as an unset
/// var rather than being forwarded. Shared by every `max_tokens` resolver in
/// this module — [`summary_map_max_tokens`], [`summary_reduce_max_tokens`],
/// [`synthesis_max_tokens`], [`digest_max_tokens`] — so none of them can drift
/// from this policy independently.
fn parse_positive_u32_from_env(var: &str, default: u32) -> u32 {
    match parse_from_env(var, default) {
        0 => default,
        n => n,
    }
}

/// Read a parseable value from `var`, falling back to `default` when unset,
/// blank, or unparseable.
fn parse_from_env<T: std::str::FromStr>(var: &str, default: T) -> T {
    match std::env::var(var) {
        Ok(v) => v.trim().parse::<T>().unwrap_or(default),
        Err(_) => default,
    }
}

// ── Per-task model resolvers ──────────────────────────────────────────────────

/// Model for entity extraction. Env: `EXTRACT_MODEL`.
pub fn extract_model() -> String {
    model_from_env("EXTRACT_MODEL", DEFAULT_EXTRACT_MODEL)
}

/// Default OpenRouter `models` fallback for standing source extraction
/// (`workers/source_atoms.rs`, profiles v2 and v3) ONLY. Every other caller keeps the global
/// cross-fallback in `llm::openai::default_fallback_model`.
///
/// 2026-10-20 blind eval (n=46 real windows): with GLM in the candidate set,
/// `llm::openai::reasoning_json_for` had to send `{"effort":"low"}` to the
/// DeepSeek primary on every window, and GLM itself was mostly served by a
/// slow host. `deepseek/deepseek-v4.1-flash` accepts `{"enabled": false}`,
/// so the whole request runs with reasoning off.
pub const DEFAULT_SOURCE_EXTRACTION_FALLBACK_MODEL: &str = "deepseek/deepseek-v4.1-flash";

/// The fallback model source extraction prices and sends. Env:
/// `SOURCE_EXTRACTION_FALLBACK_MODEL` — the kill switch: setting it to
/// `z-ai/glm-5.3-flash` restores the pre-2026-10-20 candidate set (and its
/// `{"effort":"low"}` reasoning floor) without a deploy. Whatever is named
/// here needs a `model_rates` row, or the managed gate refuses the window
/// with `UnpricedModel`.
pub fn source_extraction_fallback_model() -> String {
    model_from_env(
        "SOURCE_EXTRACTION_FALLBACK_MODEL",
        DEFAULT_SOURCE_EXTRACTION_FALLBACK_MODEL,
    )
}

/// Default OpenRouter `models` fallback for Jev's interactive Ask calls (the
/// scoped Ask citation check and pack). Same reason as
/// [`DEFAULT_SOURCE_EXTRACTION_FALLBACK_MODEL`]: with the global GLM
/// fallback in the candidate set, `reasoning_json_for` sent
/// `{"effort":"low"}` to the DeepSeek primary. OpenRouter's generation
/// records for the 2026-10-20 quality-eval Asks show 30-375 reasoning tokens
/// on every citation check and 1.7-7.2 s of generation for ~200 answer
/// tokens. With this fallback the whole request runs with reasoning off.
pub const DEFAULT_JEV_ASK_FALLBACK_MODEL: &str = "deepseek/deepseek-v4.1-flash";

/// The fallback model Jev's Ask calls price and send. Env:
/// `JEV_ASK_FALLBACK_MODEL` (`z-ai/glm-5.3-flash` restores the old candidate
/// set). It needs a `model_rates` row, or the gate refuses the call
/// (`UnpricedModel`) and the phase fails open.
pub fn jev_ask_fallback_model() -> String {
    model_from_env("JEV_ASK_FALLBACK_MODEL", DEFAULT_JEV_ASK_FALLBACK_MODEL)
}

/// Model for entity-resolution adjudication. Env: `RESOLVE_MODEL`.
pub fn resolve_model() -> String {
    model_from_env("RESOLVE_MODEL", DEFAULT_RESOLVE_MODEL)
}

/// Model for query synthesis / answer prose. Env: `SYNTHESIS_MODEL`
/// (pre-existing var, kept). See [`DEFAULT_SYNTHESIS_MODEL`]'s doc comment —
/// this is NOT the aux-model default; see [`aux_model`] for that.
pub fn synthesis_model() -> String {
    model_from_env("SYNTHESIS_MODEL", DEFAULT_SYNTHESIS_MODEL)
}

/// Model for auxiliary / cheap-tier LLM passes (never the user-facing answer
/// prose). Env: `AUX_MODEL`. See [`DEFAULT_AUX_MODEL`]'s doc comment for the
/// full list of call sites and why this is split out from [`synthesis_model`].
pub fn aux_model() -> String {
    model_from_env("AUX_MODEL", DEFAULT_AUX_MODEL)
}

/// Model for §3.4 conflict-detection adjudication. Env: `CONFLICT_MODEL`.
pub fn conflict_model() -> String {
    model_from_env("CONFLICT_MODEL", DEFAULT_CONFLICT_MODEL)
}

/// `max_tokens` for conflict-detection adjudication. Env: `CONFLICT_MAX_TOKENS`.
pub fn conflict_max_tokens() -> u32 {
    parse_from_env("CONFLICT_MAX_TOKENS", DEFAULT_CONFLICT_MAX_TOKENS)
}

/// Minimum verdict confidence to write a conflict-detected supersession edge.
/// Env: `CONFLICT_CONFIDENCE_THRESHOLD`.
pub fn conflict_confidence_threshold() -> f64 {
    parse_from_env("CONFLICT_CONFIDENCE_THRESHOLD", DEFAULT_CONFLICT_CONFIDENCE_THRESHOLD)
}

/// Days an "unsure" `conflict_dismissal` row stays live before the query-time
/// gate stops honoring it. Env: `CONFLICT_UNSURE_DISMISSAL_TTL_DAYS`.
pub fn unsure_dismissal_ttl_days() -> i64 {
    parse_from_env("CONFLICT_UNSURE_DISMISSAL_TTL_DAYS", DEFAULT_UNSURE_DISMISSAL_TTL_DAYS)
}

// ── Post-meeting summary map-reduce (cs-uxp3a-meeting-summary) ─────────────────

/// Default model for the summary MAP pass (per-chunk decision/action extraction).
/// Cheap-tier DeepSeek — the same precision-biased extraction tier used elsewhere.
pub const DEFAULT_SUMMARY_MAP_MODEL: &str = "deepseek/deepseek-v4-flash-0731";
/// Default model for the summary REDUCE pass (merge partials into the final
/// overview + deduped decisions/action items). Capable-tier GLM.
pub const DEFAULT_SUMMARY_REDUCE_MODEL: &str = "z-ai/glm-5.3-flash";
/// Default `max_tokens` for a single summary MAP call.
pub const DEFAULT_SUMMARY_MAP_MAX_TOKENS: u32 = 1024;
/// Default `max_tokens` for the summary REDUCE call.
pub const DEFAULT_SUMMARY_REDUCE_MAX_TOKENS: u32 = 1536;

/// Model for the summary MAP pass. Env: `SUMMARY_MAP_MODEL`.
pub fn summary_map_model() -> String {
    model_from_env("SUMMARY_MAP_MODEL", DEFAULT_SUMMARY_MAP_MODEL)
}

/// Model for the summary REDUCE pass. Env: `SUMMARY_REDUCE_MODEL`.
pub fn summary_reduce_model() -> String {
    model_from_env("SUMMARY_REDUCE_MODEL", DEFAULT_SUMMARY_REDUCE_MODEL)
}

// ── Synthesis / digest `max_tokens` (production regression, 2026-08) ──────────
//
// Ask synthesis and the digest worker were failing in production: LLM JSON
// responses were being truncated at `max_tokens`, and the code never read
// Anthropic's `stop_reason` to notice, so truncation surfaced only as a
// downstream "JSON parse error: EOF while parsing..." and the ask path
// degraded to sources mode (clients saw a null answer). Part of the fix is simply more
// headroom by default — these two caps were tight enough that a normal
// Sonnet-class answer or a coral-rich digest routinely hit them.

/// Default `max_tokens` for the synthesis call. Was `2048`; raised — see the
/// module-section doc comment above. Env-overridable so an operator
/// can tune it without a redeploy; see [`synthesis_max_tokens`].
pub const DEFAULT_SYNTHESIS_MAX_TOKENS: u32 = 4096;

/// `max_tokens` for the synthesis call. Env: `SYNTHESIS_MAX_TOKENS`. A parsed
/// `0` falls back to the default rather than being forwarded (would break the
/// LLM call — see [`parse_positive_u32_from_env`]).
pub fn synthesis_max_tokens() -> u32 {
    parse_positive_u32_from_env("SYNTHESIS_MAX_TOKENS", DEFAULT_SYNTHESIS_MAX_TOKENS)
}

/// Default `max_tokens` for the digest worker's regeneration call
/// (`workers::digest::regenerate`). Was `1500`; raised — see the
/// module-section doc comment above. Env-overridable; see
/// [`digest_max_tokens`].
pub const DEFAULT_DIGEST_MAX_TOKENS: u32 = 4000;

/// `max_tokens` for the digest worker's regeneration call. Env:
/// `DIGEST_MAX_TOKENS`. A parsed `0` falls back to the default (see
/// [`parse_positive_u32_from_env`]).
pub fn digest_max_tokens() -> u32 {
    parse_positive_u32_from_env("DIGEST_MAX_TOKENS", DEFAULT_DIGEST_MAX_TOKENS)
}

/// `max_tokens` for a summary MAP call. Env: `SUMMARY_MAP_MAX_TOKENS`. A parsed
/// `0` (which would break the LLM call — an empty completion) falls back to the
/// default rather than being forwarded.
pub fn summary_map_max_tokens() -> u32 {
    parse_positive_u32_from_env("SUMMARY_MAP_MAX_TOKENS", DEFAULT_SUMMARY_MAP_MAX_TOKENS)
}

/// `max_tokens` for the summary REDUCE call. Env: `SUMMARY_REDUCE_MAX_TOKENS`. A
/// parsed `0` falls back to the default (see [`summary_map_max_tokens`]).
pub fn summary_reduce_max_tokens() -> u32 {
    parse_positive_u32_from_env("SUMMARY_REDUCE_MAX_TOKENS", DEFAULT_SUMMARY_REDUCE_MAX_TOKENS)
}

// ── Ask v2 user-facing synthesis-model picker (ADR-0011 D15/D16) ──────────────
//
// Two-model-world (2026-09-03): the tier → model-id mapping is ENV-DRIVEN
// ([`ask_model_fast`], [`ask_model_smart`], [`ask_model_max`]). Defaults are
// DeepSeek V4 Flash (Fast) and GLM 5.3 Flash (Smart and Max — Max pins
// reasoning effort to `high`, see [`resolve_effort`]).

/// Default (env-unset) answer-prose model for the **Fast**
/// capability label. See [`ask_model_fast`] for the env-driven resolver
/// actually used by [`resolve_synthesis_model`].
pub const MODEL_FAST: &str = "deepseek/deepseek-v4-flash-0731";
/// Default (env-unset) answer-prose model for the **Smart**
/// capability label. See [`ask_model_smart`]. Capable-tier GLM.
pub const MODEL_SMART: &str = "z-ai/glm-5.3-flash";
/// Default (env-unset) answer-prose model for the **Max**
/// capability label. See [`ask_model_max`]. Same capable-tier GLM as Smart;
/// Max only pins reasoning effort to `high`.
pub const MODEL_MAX: &str = "z-ai/glm-5.3-flash";

/// Answer-prose model id for the **Fast** tier. Env: `ASK_MODEL_FAST`,
/// defaulting to [`MODEL_FAST`] when unset or blank.
pub fn ask_model_fast() -> String {
    model_from_env("ASK_MODEL_FAST", MODEL_FAST)
}

/// Answer-prose model id for the **Smart** tier. Env: `ASK_MODEL_SMART`,
/// defaulting to [`MODEL_SMART`] when unset or blank.
pub fn ask_model_smart() -> String {
    model_from_env("ASK_MODEL_SMART", MODEL_SMART)
}

/// Answer-prose model id for the **Max** tier. Env: `ASK_MODEL_MAX`,
/// defaulting to [`MODEL_MAX`] when unset or blank.
pub fn ask_model_max() -> String {
    model_from_env("ASK_MODEL_MAX", MODEL_MAX)
}

/// Resolve a client-supplied synthesis-model choice to a **server-allowlisted**
/// model id (ADR-0011 D16). The client never reaches the provider with a raw
/// string: only Fast/Smart/Max capability labels (case-insensitive, plus the
/// legacy `haiku`/`sonnet`/`opus` aliases) — or, for resilience, the exact
/// currently-allowlisted ids ([`ask_model_fast`]/[`ask_model_smart`]/
/// [`ask_model_max`]) — are honored. Anything unrecognized (incl. `None`)
/// falls back to the instance default ([`synthesis_model`]).
///
/// This governs the **answer prose only**; auxiliary passes (contradiction
/// detection, extraction, condense, capture, titles) always stay on the
/// cheap-tier [`aux_model`].
///
/// The exact-id passthrough arm compares against the CURRENT env-resolved
/// tier ids, not the bare [`MODEL_FAST`]/[`MODEL_SMART`]/[`MODEL_MAX`]
/// constants — so after the OpenRouter cutover sets `ASK_MODEL_*`, a client
/// that sends the literal OpenRouter id still allowlist-matches. Judged
/// acceptable to keep as defense-in-depth even though the only production
/// caller, `web/lib/askScope.ts`'s `MODELS` map feeding `routes::queries::
/// create_query_stream`'s `CreateQueryStreamRequest.model`, sends the stable
/// TIER KEYWORD (`"fast"`/`"smart"`/`"max"`) — never a raw model id.
pub fn resolve_synthesis_model(requested: Option<&str>) -> String {
    let Some(raw) = requested else {
        return synthesis_model();
    };
    let fast = ask_model_fast();
    let smart = ask_model_smart();
    let max = ask_model_max();
    match raw.trim().to_ascii_lowercase().as_str() {
        "fast" | "haiku" => fast.clone(),
        "smart" | "sonnet" => smart.clone(),
        "max" | "opus" => max.clone(),
        // Exact allowlisted ids pass through unchanged (defense-in-depth: still an
        // allowlist match against the CURRENT env-resolved ids, never an
        // arbitrary client string).
        other if other == fast.as_str() || other == smart.as_str() || other == max.as_str() => other.to_string(),
        // Unknown / blank → instance default. Never forward the raw string.
        _ => synthesis_model(),
    }
}

/// The three Ask v2 capability tiers, as an enum rather than a bare `&str` —
/// deliberately, after a review of this PR found the string-typed version
/// let a model id be passed where a tier was expected and let an unknown tier
/// silently become `"smart"` with no compile-time signal at the call site.
/// [`resolve_ask_tier`] is the only place client input becomes one of these;
/// everything downstream ([`resolve_effort`]) takes the enum, not a string.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AskTier {
    Fast,
    Smart,
    Max,
}

/// Resolve a client-supplied model request to one of the three capability
/// tiers, mirroring [`resolve_synthesis_model`]'s normalization (tier
/// keywords, legacy `haiku`/`sonnet`/`opus` aliases, and exact env-resolved
/// ids) but returning the tier rather than the model id. Unrecognized/
/// blank/`None` defaults to [`AskTier::Smart`] (today's instance default is
/// Sonnet-class, i.e. the Smart tier).
///
/// Exists so [`resolve_effort`] can be TIER-aware rather than model-name-
/// aware — see that function's doc comment for why: post-cutover, Fast and
/// Smart/Max no longer differ by a Haiku-substring check (Fast is
/// `deepseek/deepseek-v4-flash-0731`, Smart/Max both
/// `z-ai/glm-5.3-flash`), so the only thing that still distinguishes their
/// effort handling is which TIER was requested.
pub fn resolve_ask_tier(requested: Option<&str>) -> AskTier {
    let Some(raw) = requested else {
        return AskTier::Smart;
    };
    let fast = ask_model_fast();
    let smart = ask_model_smart();
    let max = ask_model_max();
    match raw.trim().to_ascii_lowercase().as_str() {
        "fast" | "haiku" => AskTier::Fast,
        "smart" | "sonnet" => AskTier::Smart,
        "max" | "opus" => AskTier::Max,
        other if other == fast.as_str() => AskTier::Fast,
        other if other == smart.as_str() => AskTier::Smart,
        other if other == max.as_str() => AskTier::Max,
        _ => AskTier::Smart,
    }
}

// ── Reasoning-effort allowlist (ADR-0011 answer-prose extension) ──────────────

/// Whether `model` is a leftover Haiku-class id. Substring match so any
/// alias/date-suffixed variant is caught if an env override still points at
/// Claude Haiku — those ids 400 if sent an `effort` value. Not a serving
/// default; production is DeepSeek/GLM.
fn is_haiku_model(model: &str) -> bool {
    model.to_ascii_lowercase().contains("haiku")
}

/// Resolve a client-supplied reasoning-effort request against the resolved
/// answer-prose TIER ([`AskTier`], from [`resolve_ask_tier`]) AND the
/// resolved answer-prose model id (from [`resolve_synthesis_model`]).
///
/// Two-model-world amendment (2026-09-03): effort resolution is now
/// TIER-aware, not model-name-aware. The dial itself collapsed from five
/// Anthropic-only rungs (`low`/`medium`/`high`/`xhigh`/`max`) to four
/// (`off`/`low`/`medium`/`high`), default `medium`, applicable to the Smart
/// tier only:
///   * **Fast**: reasoning is always off, no dial — resolves to `None`.
///   * **Smart**: the four-rung dial, default `medium`. `off` resolves to
///     `None` — same as Fast — rather than the literal string `"off"`:
///     Anthropic's `output_config.effort` accepts only `low`/`medium`/
///     `high`/`xhigh`/`max`, and forwarding `"off"` verbatim 400s (found in
///     review of this PR: the client's `effortForWire` sends `"off"` on the
///     wire, and this resolver must never forward that literal string to a
///     provider). Legacy `xhigh`/`max` requests (old UI, in-flight clients)
///     normalize down to `high` rather than erroring.
///   * **Max**: always pinned to `high`, dial hidden client-side — resolves
///     to `Some("high")` regardless of what was requested.
///
/// **Layered with a Haiku guard that overrides every tier above:** when
/// `model` is Haiku-class ([`is_haiku_model`]), this always returns `None`
/// regardless of `tier` — `output_config.effort` 400s on that pairing full
/// stop, and a Haiku id can reach here under a non-Fast tier via
/// `SYNTHESIS_MODEL` (no per-request tier ⇒ defaults to Smart) or a
/// misconfigured `ASK_MODEL_SMART`/`ASK_MODEL_MAX`.
///
/// The returned `Option<String>`, when `Some`, is always one of the five
/// Anthropic-accepted values (`low`/`medium`/`high`/`xhigh`/`max`) — never
/// `"off"` and never an arbitrary string — so it is always safe to forward
/// verbatim as `output_config.effort` for a non-Haiku model.
pub fn resolve_effort(requested: Option<&str>, tier: AskTier, model: &str) -> Option<String> {
    if is_haiku_model(model) {
        return None;
    }
    match tier {
        AskTier::Fast => None,
        AskTier::Max => Some("high".to_string()),
        AskTier::Smart => {
            let normalized = requested
                .map(|r| r.trim().to_ascii_lowercase())
                .filter(|r| !r.is_empty());
            match normalized.as_deref() {
                // "off" ≡ no reasoning ≡ `None` — the same value Fast
                // resolves to. Anthropic has no `"off"` rung; forwarding the
                // literal string 400s.
                Some("off") => None,
                None => Some("medium".to_string()),
                Some("low") => Some("low".to_string()),
                Some("medium") => Some("medium".to_string()),
                Some("high") | Some("xhigh") | Some("max") => Some("high".to_string()),
                // Unrecognized string → the same safe default as no request at all.
                Some(_) => Some("medium".to_string()),
            }
        }
    }
}

/// Model for expertise inference. Env: `EXPERTISE_MODEL`.
pub fn expertise_model() -> String {
    model_from_env("EXPERTISE_MODEL", DEFAULT_EXPERTISE_MODEL)
}

/// Opt-in stronger model for confidence-gated expertise-inference escalation.
/// Mirrors [`extract_escalate_model`] exactly — same default-OFF switch, same
/// shape — for `workers::infer_expertise`'s single per-topic verdict rather
/// than extraction's entity/decision lists.
/// Env: `EXPERTISE_ESCALATE_MODEL`. Returns `None` when the var is unset or
/// blank — with no value set there is no second LLM call and behavior is
/// unchanged.
pub fn expertise_escalate_model() -> Option<String> {
    match std::env::var("EXPERTISE_ESCALATE_MODEL") {
        Ok(v) if !v.trim().is_empty() => Some(v.trim().to_string()),
        _ => None,
    }
}

/// Default confidence threshold below which an expertise verdict is
/// considered uncertain and (if [`expertise_escalate_model`] is configured)
/// re-run on the stronger model.
///
/// Deliberately equal to [`crate::relationships::AUTO_CONFIRM_CONFIDENCE`]
/// (0.85) rather than a separate, softer number like
/// [`DEFAULT_EXTRACT_ESCALATE_THRESHOLD`] (0.75, comfortably below
/// extraction's own write bar). `infer_expertise`'s assert-or-drop gate's
/// condition 1 discards a verdict outright the instant its confidence falls
/// below `AUTO_CONFIRM_CONFIDENCE` — there is no softer, still-usable band
/// below it the way extraction has one above `EXTRACT_ESCALATE_THRESHOLD`.
/// So the only verdicts worth spending a second, stronger-model call on are
/// exactly the ones condition 1 is about to throw away entirely: escalation
/// gives the SAME bar the verdict just missed one more attempt, on a better
/// model, before the topic is blanked into the evaluation ledger.
pub const DEFAULT_EXPERTISE_ESCALATE_THRESHOLD: f32 = crate::relationships::AUTO_CONFIRM_CONFIDENCE;

/// Confidence threshold below which an expertise verdict escalates. Env:
/// `EXPERTISE_ESCALATE_THRESHOLD`, default
/// `DEFAULT_EXPERTISE_ESCALATE_THRESHOLD`, clamped to `[0.0, 1.0]`.
pub fn expertise_escalate_threshold() -> f32 {
    parse_from_env("EXPERTISE_ESCALATE_THRESHOLD", DEFAULT_EXPERTISE_ESCALATE_THRESHOLD)
        .clamp(0.0, 1.0)
}

// ── Today-journal hourly compile (Today-journal PR B) ─────────────────────────

/// Default model for the hourly journal-compile call
/// (`workers::journal_sweep`). Capable-tier GLM, not the cheap extraction
/// default: compiling a readable 1-3 sentence summary of an hour's work is a
/// synthesis-grade judgment call.
pub const DEFAULT_JOURNAL_COMPILE_MODEL: &str = "z-ai/glm-5.3-flash";

/// Model for the hourly journal-compile call. Env: `JOURNAL_COMPILE_MODEL`.
pub fn journal_compile_model() -> String {
    model_from_env("JOURNAL_COMPILE_MODEL", DEFAULT_JOURNAL_COMPILE_MODEL)
}

/// Model for the Today gist (`services::journal_gist`). Env:
/// `JOURNAL_GIST_MODEL`, else the journal's own compile model (the same
/// capable tier: picking the 3-5 results that matter is a synthesis call).
pub fn journal_gist_model() -> String {
    match std::env::var("JOURNAL_GIST_MODEL") {
        Ok(v) if !v.trim().is_empty() => v.trim().to_string(),
        _ => journal_compile_model(),
    }
}

/// Opt-in stronger model for confidence-gated extraction escalation.
/// Env: `EXTRACT_ESCALATE_MODEL`. Returns `None` when the var is unset or blank
/// — this is the switch that keeps the escalation feature **default-OFF**: with
/// no value set there is no second LLM call and behavior is unchanged. Returns
/// `Some(trimmed)` otherwise (the model id to re-run uncertain extractions on).
pub fn extract_escalate_model() -> Option<String> {
    match std::env::var("EXTRACT_ESCALATE_MODEL") {
        Ok(v) if !v.trim().is_empty() => Some(v.trim().to_string()),
        _ => None,
    }
}

/// Self-reported confidence threshold below which an extraction is considered
/// uncertain and (if `extract_escalate_model()` is set) re-run on the stronger
/// model. Env: `EXTRACT_ESCALATE_THRESHOLD`, default
/// `DEFAULT_EXTRACT_ESCALATE_THRESHOLD`, clamped to `[0.0, 1.0]`.
pub fn extract_escalate_threshold() -> f32 {
    parse_from_env("EXTRACT_ESCALATE_THRESHOLD", DEFAULT_EXTRACT_ESCALATE_THRESHOLD)
        .clamp(0.0, 1.0)
}

/// Opt-in stronger model for `workers::entity_resolution`'s UNSURE-band
/// escalation gate (identity-resolution-at-the-door design doc §10.5): a pair
/// that would otherwise be parked as an UNSURE `reef_judgement` row, but
/// touches the self reef or has two consequential endpoints (see
/// [`door_adjudicate_min_corals`]/[`door_adjudicate_min_degree`]), is retried
/// once on this model before the UNSURE row is written.
///
/// Env: `RESOLVE_ESCALATE_MODEL`. Unlike every other `*_ESCALATE_MODEL` in
/// this module, an unset/blank value does NOT unconditionally mean "off" —
/// it falls back to whatever [`extract_escalate_model`] resolves to (§10.5:
/// "default = `EXTRACT_ESCALATE_MODEL`'s value, off when unset"), so turning
/// on extraction's escalation rung arms this gate for free. `None` only when
/// BOTH vars are unset/blank, which is still the default-OFF steady state.
pub fn resolve_escalate_model() -> Option<String> {
    match std::env::var("RESOLVE_ESCALATE_MODEL") {
        Ok(v) if !v.trim().is_empty() => Some(v.trim().to_string()),
        _ => extract_escalate_model(),
    }
}

/// Reef mass (coral count) at/above which a reef is "consequential" for the
/// identity-door adjudication gate (§10.2) and the ER escalation gate
/// (§10.5). Env: `DOOR_ADJUDICATE_MIN_CORALS`, default
/// [`DEFAULT_DOOR_ADJUDICATE_MIN_CORALS`].
pub fn door_adjudicate_min_corals() -> i64 {
    parse_from_env("DOOR_ADJUDICATE_MIN_CORALS", DEFAULT_DOOR_ADJUDICATE_MIN_CORALS)
}

/// Companion relationship-edge-degree threshold — see
/// [`door_adjudicate_min_corals`]. Env: `DOOR_ADJUDICATE_MIN_DEGREE`, default
/// [`DEFAULT_DOOR_ADJUDICATE_MIN_DEGREE`].
pub fn door_adjudicate_min_degree() -> i64 {
    parse_from_env("DOOR_ADJUDICATE_MIN_DEGREE", DEFAULT_DOOR_ADJUDICATE_MIN_DEGREE)
}

// ── Per-task cap resolvers ────────────────────────────────────────────────────

/// `max_tokens` for entity-resolution adjudication. Env: `RESOLVE_MAX_TOKENS`.
pub fn resolve_max_tokens() -> u32 {
    parse_from_env("RESOLVE_MAX_TOKENS", DEFAULT_RESOLVE_MAX_TOKENS)
}

/// `max_tokens` for the expertise-inference review call. Env: `EXPERTISE_MAX_TOKENS`.
pub fn expertise_max_tokens() -> u32 {
    parse_from_env("EXPERTISE_MAX_TOKENS", DEFAULT_EXPERTISE_MAX_TOKENS)
}

/// Minimum verdict confidence to auto-merge. Env: `MERGE_CONFIDENCE_THRESHOLD`.
pub fn merge_confidence_threshold() -> f64 {
    parse_from_env("MERGE_CONFIDENCE_THRESHOLD", DEFAULT_MERGE_CONFIDENCE_THRESHOLD)
}

// ── Tests ─────────────────────────────────────────────────────────────────────

/// Serializes every test (in this file AND `workers/extract.rs`) that mutates
/// `EXTRACT_ESCALATE_MODEL` / `EXTRACT_ESCALATE_THRESHOLD` — both are
/// process-global env vars read by `extract_escalate_model()` /
/// `extract_escalate_threshold()`, and `cargo test` runs `#[test]`s in
/// parallel threads within one process, so two tests racing on the same var
/// (even across files) can flake. `pub(crate)` so `workers::extract`'s
/// `extract_with_escalation` tests can share this exact lock rather than a
/// second, unsynchronized one.
///
/// `tokio::sync::Mutex` rather than `std::sync::Mutex`: `workers::extract`'s
/// consumer is an `async fn` test that holds the guard across an `.await`
/// (calling `extract_with_escalation`) — a std `MutexGuard` held across an
/// await point is a clippy `await_holding_lock` lint (and a real footgun on a
/// multi-threaded runtime). `blocking_lock()` keeps this file's own
/// non-async `#[test]` usage unchanged.
#[cfg(test)]
pub(crate) static EXTRACT_ESCALATE_ENV_LOCK: tokio::sync::Mutex<()> =
    tokio::sync::Mutex::const_new(());

/// Same convention as [`EXTRACT_ESCALATE_ENV_LOCK`], for
/// `EXPERTISE_ESCALATE_MODEL` / `EXPERTISE_ESCALATE_THRESHOLD` — a distinct
/// lock rather than sharing the extract one, because they mutate different
/// env vars and would only ever contend for no reason. `pub(crate)` so
/// `workers::infer_expertise`'s escalation tests can share this exact lock
/// with this file's own `expertise_escalate_env_behaviour` rather than a
/// second, unsynchronized one — same `tokio::sync::Mutex` reasoning too: an
/// async consumer there holds the guard across an `.await`.
#[cfg(test)]
pub(crate) static EXPERTISE_ESCALATE_ENV_LOCK: tokio::sync::Mutex<()> =
    tokio::sync::Mutex::const_new(());

/// Serializes every test in this file that reads OR writes `ASK_MODEL_FAST` /
/// `ASK_MODEL_SMART` / `ASK_MODEL_MAX` — all three are process-global env vars
/// read by [`ask_model_fast`]/[`ask_model_smart`]/[`ask_model_max`] (and,
/// transitively, by [`resolve_synthesis_model`]/[`resolve_ask_tier`]).
/// `cargo test` runs `#[test]`s in parallel threads within one process, so a
/// test that merely READS these vars (e.g. asserting the unset-env default)
/// can observe a value a concurrently-running test has temporarily SET —
/// found in review of this PR: `resolve_synthesis_model_allowlists_
/// capability_labels` (reads via `ask_model_fast()` etc., no explicit env
/// touch of its own) was racing `resolve_synthesis_model_and_tier_honor_
/// ask_model_env_override` (explicit set/restore), which is exactly the
/// "read by no other test" convention `EXTRACT_ESCALATE_ENV_LOCK` above
/// exists to avoid violating. All non-async `#[test]`s here, so a plain
/// `std::sync::Mutex` — no need for `EXTRACT_ESCALATE_ENV_LOCK`'s
/// `tokio::sync::Mutex` (that one exists only because an ASYNC consumer in
/// `workers::extract` holds the guard across an `.await`).
#[cfg(test)]
static ASK_MODEL_ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

#[cfg(test)]
mod tests {
    use super::*;

    // Env-mutating tests share the process, so each uses a unique var name or
    // restores state; we only assert the default path here to stay isolation-safe.

    #[test]
    fn defaults_match_previous_hardcoded_values() {
        // These guard against an accidental change to the historical defaults.
        assert_eq!(DEFAULT_EXTRACT_MODEL, "deepseek/deepseek-v4-flash-0731");
        assert_eq!(DEFAULT_RESOLVE_MODEL, "deepseek/deepseek-v4-flash-0731");
        assert_eq!(DEFAULT_SYNTHESIS_MODEL, "z-ai/glm-5.3-flash");
        assert_eq!(DEFAULT_AUX_MODEL, "deepseek/deepseek-v4-flash-0731");
        assert_eq!(DEFAULT_RESOLVE_MAX_TOKENS, 256);
        assert!((DEFAULT_MERGE_CONFIDENCE_THRESHOLD - 0.85).abs() < 1e-9);
        assert!((DEFAULT_EXTRACT_ESCALATE_THRESHOLD - 0.75).abs() < 1e-6);
        // Deliberately NOT a hardcoded literal: see
        // `DEFAULT_EXPERTISE_ESCALATE_THRESHOLD`'s doc for why it must track
        // `AUTO_CONFIRM_CONFIDENCE` rather than pin its own number.
        assert!(
            (DEFAULT_EXPERTISE_ESCALATE_THRESHOLD - crate::relationships::AUTO_CONFIRM_CONFIDENCE)
                .abs()
                < 1e-9
        );
    }

    /// `synthesis_model()` and `aux_model()` must be independently overridable
    /// — the whole point of splitting `DEFAULT_AUX_MODEL` out from
    /// `DEFAULT_SYNTHESIS_MODEL` is that an operator (or a future call site)
    /// can pin the aux/cheap tier without moving the answer-prose default, and
    /// vice versa. This var is read by no other test, so a serialized
    /// set/reset here is race-free under cargo's parallel runner.
    #[test]
    fn synthesis_model_and_aux_model_are_independently_overridable() {
        let prev_synth = std::env::var("SYNTHESIS_MODEL").ok();
        let prev_aux = std::env::var("AUX_MODEL").ok();

        std::env::remove_var("SYNTHESIS_MODEL");
        std::env::remove_var("AUX_MODEL");
        assert_eq!(synthesis_model(), DEFAULT_SYNTHESIS_MODEL);
        assert_eq!(aux_model(), DEFAULT_AUX_MODEL);
        assert_ne!(
            synthesis_model(),
            aux_model(),
            "the two defaults must have diverged — that is the entire point of the split"
        );

        std::env::set_var("SYNTHESIS_MODEL", "claude-opus-5");
        assert_eq!(synthesis_model(), "claude-opus-5");
        assert_eq!(
            aux_model(),
            DEFAULT_AUX_MODEL,
            "overriding SYNTHESIS_MODEL must not move AUX_MODEL"
        );

        std::env::set_var("AUX_MODEL", "claude-haiku-4-5-20251001");
        assert_eq!(aux_model(), "claude-haiku-4-5-20251001");
        assert_eq!(
            synthesis_model(),
            "claude-opus-5",
            "overriding AUX_MODEL must not move SYNTHESIS_MODEL"
        );

        match prev_synth {
            Some(v) => std::env::set_var("SYNTHESIS_MODEL", v),
            None => std::env::remove_var("SYNTHESIS_MODEL"),
        }
        match prev_aux {
            Some(v) => std::env::set_var("AUX_MODEL", v),
            None => std::env::remove_var("AUX_MODEL"),
        }
    }



    // `EXTRACT_ESCALATE_MODEL` / `EXTRACT_ESCALATE_THRESHOLD` mutate process-global
    // env, so all their cases live in ONE serialized test that saves + restores the
    // prior values — splitting them into separate `#[test]`s lets cargo's parallel
    // runner race two tests on the same var (the cause of an intermittent failure).
    // Mirrors `entity_resolution::tests::blocking_mode_env_switch_defaults_to_rust`.
    #[test]
    fn extract_escalate_env_behaviour() {
        let _guard = EXTRACT_ESCALATE_ENV_LOCK.blocking_lock();
        let prev_model = std::env::var("EXTRACT_ESCALATE_MODEL").ok();
        let prev_thresh = std::env::var("EXTRACT_ESCALATE_THRESHOLD").ok();

        // ── Model: None unless explicitly set (this is the default-OFF invariant) ──
        std::env::remove_var("EXTRACT_ESCALATE_MODEL");
        assert_eq!(extract_escalate_model(), None, "unset ⇒ feature OFF");
        std::env::set_var("EXTRACT_ESCALATE_MODEL", "claude-sonnet-4-6");
        assert_eq!(extract_escalate_model().as_deref(), Some("claude-sonnet-4-6"));
        std::env::set_var("EXTRACT_ESCALATE_MODEL", "   ");
        assert_eq!(extract_escalate_model(), None, "blank ⇒ feature OFF");

        // ── Threshold: default when unset/garbage, parsed + clamped otherwise ──────
        std::env::remove_var("EXTRACT_ESCALATE_THRESHOLD");
        assert!((extract_escalate_threshold() - DEFAULT_EXTRACT_ESCALATE_THRESHOLD).abs() < 1e-6);
        std::env::set_var("EXTRACT_ESCALATE_THRESHOLD", "0.6");
        assert!((extract_escalate_threshold() - 0.6).abs() < 1e-6);
        std::env::set_var("EXTRACT_ESCALATE_THRESHOLD", "5.0");
        assert!((extract_escalate_threshold() - 1.0).abs() < 1e-6, "clamps above 1.0");
        std::env::set_var("EXTRACT_ESCALATE_THRESHOLD", "-2.0");
        assert!((extract_escalate_threshold() - 0.0).abs() < 1e-6, "clamps below 0.0");
        std::env::set_var("EXTRACT_ESCALATE_THRESHOLD", "not-a-number");
        assert!((extract_escalate_threshold() - DEFAULT_EXTRACT_ESCALATE_THRESHOLD).abs() < 1e-6);

        // ── Restore ────────────────────────────────────────────────────────────────
        match prev_model {
            Some(v) => std::env::set_var("EXTRACT_ESCALATE_MODEL", v),
            None => std::env::remove_var("EXTRACT_ESCALATE_MODEL"),
        }
        match prev_thresh {
            Some(v) => std::env::set_var("EXTRACT_ESCALATE_THRESHOLD", v),
            None => std::env::remove_var("EXTRACT_ESCALATE_THRESHOLD"),
        }
    }

    /// `resolve_escalate_model` reads BOTH `RESOLVE_ESCALATE_MODEL` and (as
    /// its fallback) `EXTRACT_ESCALATE_MODEL`, so this shares
    /// `EXTRACT_ESCALATE_ENV_LOCK` with `extract_escalate_env_behaviour`
    /// rather than a second, unsynchronized lock — the same "read by no
    /// other un-locked test" discipline that lock's own doc describes.
    #[test]
    fn resolve_escalate_model_env_behaviour() {
        let _guard = EXTRACT_ESCALATE_ENV_LOCK.blocking_lock();
        let prev_resolve = std::env::var("RESOLVE_ESCALATE_MODEL").ok();
        let prev_extract = std::env::var("EXTRACT_ESCALATE_MODEL").ok();

        // Both unset ⇒ off (the shared default-OFF invariant).
        std::env::remove_var("RESOLVE_ESCALATE_MODEL");
        std::env::remove_var("EXTRACT_ESCALATE_MODEL");
        assert_eq!(resolve_escalate_model(), None, "both unset ⇒ feature OFF");

        // Only EXTRACT_ESCALATE_MODEL set ⇒ RESOLVE_ESCALATE_MODEL falls back to it.
        std::env::set_var("EXTRACT_ESCALATE_MODEL", "claude-sonnet-4-6");
        assert_eq!(
            resolve_escalate_model().as_deref(),
            Some("claude-sonnet-4-6"),
            "unset RESOLVE_ESCALATE_MODEL falls back to EXTRACT_ESCALATE_MODEL"
        );

        // RESOLVE_ESCALATE_MODEL set ⇒ it wins over the fallback.
        std::env::set_var("RESOLVE_ESCALATE_MODEL", "z-ai/glm-5.3-flash");
        assert_eq!(resolve_escalate_model().as_deref(), Some("z-ai/glm-5.3-flash"));

        // Blank RESOLVE_ESCALATE_MODEL falls back exactly like unset.
        std::env::set_var("RESOLVE_ESCALATE_MODEL", "   ");
        assert_eq!(
            resolve_escalate_model().as_deref(),
            Some("claude-sonnet-4-6"),
            "blank RESOLVE_ESCALATE_MODEL falls back to EXTRACT_ESCALATE_MODEL, not OFF"
        );

        // Restore.
        match prev_resolve {
            Some(v) => std::env::set_var("RESOLVE_ESCALATE_MODEL", v),
            None => std::env::remove_var("RESOLVE_ESCALATE_MODEL"),
        }
        match prev_extract {
            Some(v) => std::env::set_var("EXTRACT_ESCALATE_MODEL", v),
            None => std::env::remove_var("EXTRACT_ESCALATE_MODEL"),
        }
    }

    #[test]
    fn door_adjudicate_thresholds_default_and_override() {
        // Not env-shared with any other test (unique var names), so no lock needed.
        let prev_corals = std::env::var("DOOR_ADJUDICATE_MIN_CORALS").ok();
        let prev_degree = std::env::var("DOOR_ADJUDICATE_MIN_DEGREE").ok();

        std::env::remove_var("DOOR_ADJUDICATE_MIN_CORALS");
        std::env::remove_var("DOOR_ADJUDICATE_MIN_DEGREE");
        assert_eq!(door_adjudicate_min_corals(), DEFAULT_DOOR_ADJUDICATE_MIN_CORALS);
        assert_eq!(door_adjudicate_min_degree(), DEFAULT_DOOR_ADJUDICATE_MIN_DEGREE);
        assert_eq!(DEFAULT_DOOR_ADJUDICATE_MIN_CORALS, 25);
        assert_eq!(DEFAULT_DOOR_ADJUDICATE_MIN_DEGREE, 100);

        std::env::set_var("DOOR_ADJUDICATE_MIN_CORALS", "40");
        std::env::set_var("DOOR_ADJUDICATE_MIN_DEGREE", "200");
        assert_eq!(door_adjudicate_min_corals(), 40);
        assert_eq!(door_adjudicate_min_degree(), 200);

        std::env::set_var("DOOR_ADJUDICATE_MIN_CORALS", "not-a-number");
        assert_eq!(door_adjudicate_min_corals(), DEFAULT_DOOR_ADJUDICATE_MIN_CORALS);

        match prev_corals {
            Some(v) => std::env::set_var("DOOR_ADJUDICATE_MIN_CORALS", v),
            None => std::env::remove_var("DOOR_ADJUDICATE_MIN_CORALS"),
        }
        match prev_degree {
            Some(v) => std::env::set_var("DOOR_ADJUDICATE_MIN_DEGREE", v),
            None => std::env::remove_var("DOOR_ADJUDICATE_MIN_DEGREE"),
        }
    }

    /// Mirrors `extract_escalate_env_behaviour` exactly, for
    /// `EXPERTISE_ESCALATE_MODEL` / `EXPERTISE_ESCALATE_THRESHOLD`. The one
    /// substantive difference worth calling out: the default threshold here
    /// is `AUTO_CONFIRM_CONFIDENCE` (0.85), not a separate, softer number —
    /// see `DEFAULT_EXPERTISE_ESCALATE_THRESHOLD`'s doc for why.
    #[test]
    fn expertise_escalate_env_behaviour() {
        let _guard = EXPERTISE_ESCALATE_ENV_LOCK.blocking_lock();
        let prev_model = std::env::var("EXPERTISE_ESCALATE_MODEL").ok();
        let prev_thresh = std::env::var("EXPERTISE_ESCALATE_THRESHOLD").ok();

        // ── Model: None unless explicitly set (default-OFF invariant) ──────────────
        std::env::remove_var("EXPERTISE_ESCALATE_MODEL");
        assert_eq!(expertise_escalate_model(), None, "unset ⇒ feature OFF");
        std::env::set_var("EXPERTISE_ESCALATE_MODEL", "z-ai/glm-5.3-flash");
        assert_eq!(expertise_escalate_model().as_deref(), Some("z-ai/glm-5.3-flash"));
        std::env::set_var("EXPERTISE_ESCALATE_MODEL", "   ");
        assert_eq!(expertise_escalate_model(), None, "blank ⇒ feature OFF");

        // ── Threshold: default equals AUTO_CONFIRM_CONFIDENCE, parsed + clamped ────
        std::env::remove_var("EXPERTISE_ESCALATE_THRESHOLD");
        assert!(
            (expertise_escalate_threshold() - crate::relationships::AUTO_CONFIRM_CONFIDENCE).abs()
                < 1e-6
        );
        std::env::set_var("EXPERTISE_ESCALATE_THRESHOLD", "0.6");
        assert!((expertise_escalate_threshold() - 0.6).abs() < 1e-6);
        std::env::set_var("EXPERTISE_ESCALATE_THRESHOLD", "5.0");
        assert!((expertise_escalate_threshold() - 1.0).abs() < 1e-6, "clamps above 1.0");
        std::env::set_var("EXPERTISE_ESCALATE_THRESHOLD", "-2.0");
        assert!((expertise_escalate_threshold() - 0.0).abs() < 1e-6, "clamps below 0.0");
        std::env::set_var("EXPERTISE_ESCALATE_THRESHOLD", "not-a-number");
        assert!(
            (expertise_escalate_threshold() - crate::relationships::AUTO_CONFIRM_CONFIDENCE).abs()
                < 1e-6
        );

        // ── Restore ────────────────────────────────────────────────────────────────
        match prev_model {
            Some(v) => std::env::set_var("EXPERTISE_ESCALATE_MODEL", v),
            None => std::env::remove_var("EXPERTISE_ESCALATE_MODEL"),
        }
        match prev_thresh {
            Some(v) => std::env::set_var("EXPERTISE_ESCALATE_THRESHOLD", v),
            None => std::env::remove_var("EXPERTISE_ESCALATE_THRESHOLD"),
        }
    }

    #[test]
    fn resolve_synthesis_model_allowlists_capability_labels() {
        // Reads ASK_MODEL_* indirectly (via `ask_model_fast`/`ask_model_smart`/
        // `ask_model_max`, transitively through `resolve_synthesis_model`) —
        // takes the same lock as the test below that WRITES those vars, or
        // this can observe a torn-mid-override value and flake. Also clears
        // (and restores) them itself rather than merely asserting they're
        // already unset: an ambient `ASK_MODEL_*` in the shell running the
        // suite would otherwise fail this test for a reason unrelated to what
        // it actually checks.
        let _guard = ASK_MODEL_ENV_LOCK.lock().unwrap();
        let prev_fast = std::env::var("ASK_MODEL_FAST").ok();
        let prev_smart = std::env::var("ASK_MODEL_SMART").ok();
        let prev_max = std::env::var("ASK_MODEL_MAX").ok();
        std::env::remove_var("ASK_MODEL_FAST");
        std::env::remove_var("ASK_MODEL_SMART");
        std::env::remove_var("ASK_MODEL_MAX");

        // Capability labels map to the allowlisted ids (case-insensitive), and
        // the legacy `haiku`/`sonnet`/`opus` aliases keep resolving too.
        assert_eq!(resolve_synthesis_model(Some("fast")), MODEL_FAST);
        assert_eq!(resolve_synthesis_model(Some("Haiku")), MODEL_FAST);
        assert_eq!(resolve_synthesis_model(Some("Smart")), MODEL_SMART);
        assert_eq!(resolve_synthesis_model(Some("sonnet")), MODEL_SMART);
        assert_eq!(resolve_synthesis_model(Some("MAX")), MODEL_MAX);
        assert_eq!(resolve_synthesis_model(Some("opus")), MODEL_MAX);
        // Exact allowlisted ids pass through.
        assert_eq!(resolve_synthesis_model(Some(MODEL_SMART)), MODEL_SMART);
        // A raw/unknown client string is NEVER forwarded — falls back to default.
        assert_eq!(
            resolve_synthesis_model(Some("gpt-4o; DROP TABLE")),
            synthesis_model()
        );
        assert_eq!(resolve_synthesis_model(None), synthesis_model());

        match prev_fast {
            Some(v) => std::env::set_var("ASK_MODEL_FAST", v),
            None => std::env::remove_var("ASK_MODEL_FAST"),
        }
        match prev_smart {
            Some(v) => std::env::set_var("ASK_MODEL_SMART", v),
            None => std::env::remove_var("ASK_MODEL_SMART"),
        }
        match prev_max {
            Some(v) => std::env::set_var("ASK_MODEL_MAX", v),
            None => std::env::remove_var("ASK_MODEL_MAX"),
        }
    }

    /// Two-model-world amendment: the tier → model-id mapping is env-driven
    /// (`ASK_MODEL_FAST`/`ASK_MODEL_SMART`/`ASK_MODEL_MAX`). Guarded by
    /// `ASK_MODEL_ENV_LOCK` (held for the whole test) — every test in this
    /// file that reads OR writes those three vars must take the same lock, or
    /// a concurrently-running one can observe a torn-mid-override value (see
    /// the lock's own doc comment for the flake this caught in review).
    /// Deliberately folds [`resolve_ask_tier`] assertions AND the
    /// ASK_MODEL_SMART/ASK_MODEL_MAX-pointed-at-a-Haiku-id defensive-guard
    /// scenario in here too, rather than separate tests, so nothing else
    /// needs to touch these vars.
    #[test]
    fn resolve_synthesis_model_and_tier_honor_ask_model_env_override() {
        let _guard = ASK_MODEL_ENV_LOCK.lock().unwrap();
        let prev_fast = std::env::var("ASK_MODEL_FAST").ok();
        let prev_smart = std::env::var("ASK_MODEL_SMART").ok();
        let prev_max = std::env::var("ASK_MODEL_MAX").ok();
        std::env::remove_var("ASK_MODEL_FAST");
        std::env::remove_var("ASK_MODEL_SMART");
        std::env::remove_var("ASK_MODEL_MAX");

        // ── Env unset ⇒ defaults equal the previous hardcoded constants,
        // byte-for-byte, and the tier resolver agrees with the model resolver.
        assert_eq!(ask_model_fast(), MODEL_FAST);
        assert_eq!(ask_model_smart(), MODEL_SMART);
        assert_eq!(ask_model_max(), MODEL_MAX);
        assert_eq!(resolve_ask_tier(Some("fast")), AskTier::Fast);
        assert_eq!(resolve_ask_tier(Some("Haiku")), AskTier::Fast);
        assert_eq!(resolve_ask_tier(Some("Smart")), AskTier::Smart);
        assert_eq!(resolve_ask_tier(Some("sonnet")), AskTier::Smart);
        assert_eq!(resolve_ask_tier(Some("MAX")), AskTier::Max);
        assert_eq!(resolve_ask_tier(Some("opus")), AskTier::Max);
        assert_eq!(resolve_ask_tier(Some(MODEL_FAST)), AskTier::Fast);
        assert_eq!(resolve_ask_tier(Some("gpt-4o; DROP TABLE")), AskTier::Smart);
        assert_eq!(resolve_ask_tier(None), AskTier::Smart);

        // ── Env override ⇒ the tier keywords (and exact-id passthrough) follow
        // ASK_MODEL_* without any other code change — this is the whole point
        // of the amendment: the later cutover deploy sets these three vars to
        // the OpenRouter ids and nothing else has to move.
        std::env::set_var("ASK_MODEL_FAST", "deepseek/deepseek-v4-flash-0731");
        std::env::set_var("ASK_MODEL_SMART", "z-ai/glm-5.3-flash");
        std::env::set_var("ASK_MODEL_MAX", "z-ai/glm-5.3-flash");

        assert_eq!(resolve_synthesis_model(Some("fast")), "deepseek/deepseek-v4-flash-0731");
        assert_eq!(resolve_synthesis_model(Some("haiku")), "deepseek/deepseek-v4-flash-0731");
        assert_eq!(resolve_synthesis_model(Some("smart")), "z-ai/glm-5.3-flash");
        assert_eq!(resolve_synthesis_model(Some("max")), "z-ai/glm-5.3-flash");
        // Exact env-resolved id still allowlist-matches.
        assert_eq!(
            resolve_synthesis_model(Some("deepseek/deepseek-v4-flash-0731")),
            "deepseek/deepseek-v4-flash-0731"
        );
        // A leftover Claude id is not allowlisted — falls back to the instance
        // default, same "never forward an unrecognized raw string" contract.
        assert_eq!(
            resolve_synthesis_model(Some("claude-haiku-4-5")),
            synthesis_model()
        );

        assert_eq!(resolve_ask_tier(Some("fast")), AskTier::Fast);
        assert_eq!(resolve_ask_tier(Some("max")), AskTier::Max);
        assert_eq!(resolve_ask_tier(Some("deepseek/deepseek-v4-flash-0731")), AskTier::Fast);
        assert_eq!(resolve_ask_tier(Some("z-ai/glm-5.3-flash")), AskTier::Smart);
        // Unrecognized leftover Claude id ⇒ Smart default, same as any other
        // unrecognized string.
        assert_eq!(resolve_ask_tier(Some("claude-haiku-4-5")), AskTier::Smart);

        // ── Defensive Haiku guard, exercised end-to-end through the real env
        // vars: an operator misconfiguring ASK_MODEL_SMART or ASK_MODEL_MAX to
        // a Haiku id must still get `None` back from `resolve_effort` despite
        // the tier being Smart/Max — `output_config.effort` 400s on Haiku full
        // stop, and tier alone can no longer stand in for "is this Haiku"
        // once tiers are env-driven.
        std::env::set_var("ASK_MODEL_SMART", "claude-haiku-4-5");
        let smart_model = resolve_synthesis_model(Some("smart"));
        let smart_tier = resolve_ask_tier(Some("smart"));
        assert_eq!(smart_model, "claude-haiku-4-5");
        assert_eq!(smart_tier, AskTier::Smart);
        assert_eq!(
            resolve_effort(Some("high"), smart_tier, &smart_model),
            None,
            "ASK_MODEL_SMART misconfigured to a Haiku id must still resolve no effort"
        );
        std::env::set_var("ASK_MODEL_SMART", "z-ai/glm-5.3-flash"); // restore for the next block

        std::env::set_var("ASK_MODEL_MAX", "claude-haiku-4-5-20251001");
        let max_model = resolve_synthesis_model(Some("max"));
        let max_tier = resolve_ask_tier(Some("max"));
        assert_eq!(max_model, "claude-haiku-4-5-20251001");
        assert_eq!(max_tier, AskTier::Max);
        assert_eq!(
            resolve_effort(None, max_tier, &max_model),
            None,
            "ASK_MODEL_MAX misconfigured to a Haiku id must still resolve no effort, \
             not the usual Max-tier `high` pin"
        );

        match prev_fast {
            Some(v) => std::env::set_var("ASK_MODEL_FAST", v),
            None => std::env::remove_var("ASK_MODEL_FAST"),
        }
        match prev_smart {
            Some(v) => std::env::set_var("ASK_MODEL_SMART", v),
            None => std::env::remove_var("ASK_MODEL_SMART"),
        }
        match prev_max {
            Some(v) => std::env::set_var("ASK_MODEL_MAX", v),
            None => std::env::remove_var("ASK_MODEL_MAX"),
        }
    }

    // ── resolve_effort (tier-aware, two-model-world amendment) ────────────────
    //
    // Every test below feeds a literal non-Haiku model string (never through
    // ASK_MODEL_*/SYNTHESIS_MODEL env resolution) so none of them need
    // `ASK_MODEL_ENV_LOCK` — the env-driven, real-pipeline scenarios live in
    // `resolve_synthesis_model_and_tier_honor_ask_model_env_override` above,
    // under that lock.

    const NON_HAIKU_MODEL: &str = "claude-sonnet-5";

    #[test]
    fn resolve_effort_fast_tier_is_always_none() {
        for level in ["off", "low", "medium", "high", "xhigh", "max", "bogus"] {
            assert_eq!(
                resolve_effort(Some(level), AskTier::Fast, NON_HAIKU_MODEL),
                None,
                "fast tier must never receive an effort value (requested {level:?})"
            );
        }
        assert_eq!(resolve_effort(None, AskTier::Fast, NON_HAIKU_MODEL), None);
    }

    #[test]
    fn resolve_effort_max_tier_is_always_pinned_high() {
        for requested in [None, Some("off"), Some("low"), Some("medium"), Some("bogus")] {
            assert_eq!(
                resolve_effort(requested, AskTier::Max, NON_HAIKU_MODEL).as_deref(),
                Some("high"),
                "max tier must always resolve to high regardless of what was requested ({requested:?})"
            );
        }
    }

    #[test]
    fn resolve_effort_smart_tier_normalizes_the_four_rung_dial() {
        // Exact rungs low/medium/high pass through unchanged (case-insensitive,
        // trimmed). `off` is handled separately below — it does NOT pass
        // through as the literal string "off".
        for level in ["low", "medium", "high"] {
            assert_eq!(
                resolve_effort(Some(level), AskTier::Smart, NON_HAIKU_MODEL).as_deref(),
                Some(level),
                "smart tier should pass {level:?} through unchanged"
            );
            assert_eq!(
                resolve_effort(Some(&level.to_ascii_uppercase()), AskTier::Smart, NON_HAIKU_MODEL)
                    .as_deref(),
                Some(level),
                "uppercase {level:?} should normalize to lowercase"
            );
            assert_eq!(
                resolve_effort(Some(&format!("  {level}  ")), AskTier::Smart, NON_HAIKU_MODEL)
                    .as_deref(),
                Some(level),
                "whitespace around {level:?} should be trimmed"
            );
        }
        // Legacy Anthropic-only rungs (old UI / in-flight clients) collapse to
        // `high` rather than erroring.
        assert_eq!(
            resolve_effort(Some("xhigh"), AskTier::Smart, NON_HAIKU_MODEL).as_deref(),
            Some("high")
        );
        assert_eq!(
            resolve_effort(Some("max"), AskTier::Smart, NON_HAIKU_MODEL).as_deref(),
            Some("high")
        );
        assert_eq!(
            resolve_effort(Some("XHIGH"), AskTier::Smart, NON_HAIKU_MODEL).as_deref(),
            Some("high")
        );
        // No request ⇒ the documented default, `medium`.
        assert_eq!(resolve_effort(None, AskTier::Smart, NON_HAIKU_MODEL).as_deref(), Some("medium"));
        // Blank / unrecognized ⇒ the same safe default as no request at all.
        assert_eq!(
            resolve_effort(Some(""), AskTier::Smart, NON_HAIKU_MODEL).as_deref(),
            Some("medium")
        );
        assert_eq!(
            resolve_effort(Some("   "), AskTier::Smart, NON_HAIKU_MODEL).as_deref(),
            Some("medium")
        );
        assert_eq!(
            resolve_effort(Some("gpt-4o; DROP TABLE"), AskTier::Smart, NON_HAIKU_MODEL).as_deref(),
            Some("medium")
        );
    }

    /// THE regression this whole review round exists to catch: the web
    /// composer's dial sends the literal wire value `"off"` for its lowest
    /// rung (`effortForWire` in `web/lib/askScope.ts`). Before this fix,
    /// `resolve_effort(Some("off"), "smart")` returned `Some("off".to_string())`
    /// — a 400 on every live Smart ask with the dial at Off. `"off"` must
    /// resolve to `None`, identically to Fast's "no dial at all".
    #[test]
    fn resolve_effort_smart_tier_off_resolves_to_none_not_the_literal_string() {
        assert_eq!(resolve_effort(Some("off"), AskTier::Smart, NON_HAIKU_MODEL), None);
        assert_eq!(resolve_effort(Some("OFF"), AskTier::Smart, NON_HAIKU_MODEL), None);
        assert_eq!(resolve_effort(Some("  off  "), AskTier::Smart, NON_HAIKU_MODEL), None);
    }

    /// Defensive second layer, restored after review found it had been
    /// dropped by the tier-aware rewrite (see [`is_haiku_model`]'s doc
    /// comment): `resolve_effort` must return `None` whenever the RESOLVED
    /// model id is Haiku-class, regardless of tier — including Max, which
    /// otherwise always pins `high`.
    ///
    /// Covers both scenarios review flagged:
    ///   * `SYNTHESIS_MODEL=claude-haiku-4-5` with no per-request `model`
    ///     (`req.model: None`) — `resolve_ask_tier(None)` defaults to
    ///     [`AskTier::Smart`] (proved in the test above), so the
    ///     `(AskTier::Smart, level: None)` case below, paired with a literal
    ///     Haiku id, is that exact shape without needing to mutate
    ///     `SYNTHESIS_MODEL` itself (which would race the OTHER test that
    ///     already owns it, `synthesis_model_and_aux_model_are_
    ///     independently_overridable`).
    ///   * `ASK_MODEL_SMART`/`ASK_MODEL_MAX` misconfigured to a Haiku id —
    ///     exercised end-to-end through the real env vars in
    ///     `resolve_synthesis_model_and_tier_honor_ask_model_env_override`
    ///     above, under `ASK_MODEL_ENV_LOCK`.
    #[test]
    fn resolve_effort_is_none_for_any_haiku_model_regardless_of_tier() {
        for model in [
            "claude-haiku-4-5",
            "claude-haiku-4-5-20251001",
            "claude-3-5-haiku-20241022",
            "CLAUDE-HAIKU-4-5", // case-insensitive match
        ] {
            for tier in [AskTier::Fast, AskTier::Smart, AskTier::Max] {
                for level in [None, Some("off"), Some("low"), Some("medium"), Some("high")] {
                    assert_eq!(
                        resolve_effort(level, tier, model),
                        None,
                        "effort must never be resolved for Haiku model {model:?} \
                         (tier {tier:?}, requested {level:?})"
                    );
                }
            }
        }
    }

    #[test]
    fn journal_compile_model_defaults_to_capable_tier() {
        // Compiling a journal is synthesis-grade. Guard the constant directly
        // so a future "just reuse DEFAULT_EXTRACT_MODEL" edit can't silently
        // cheapen it.
        assert_eq!(DEFAULT_JOURNAL_COMPILE_MODEL, "z-ai/glm-5.3-flash");

        let prev = std::env::var("JOURNAL_COMPILE_MODEL").ok();
        std::env::remove_var("JOURNAL_COMPILE_MODEL");
        assert_eq!(journal_compile_model(), DEFAULT_JOURNAL_COMPILE_MODEL);
        std::env::set_var("JOURNAL_COMPILE_MODEL", "claude-sonnet-5-preview");
        assert_eq!(journal_compile_model(), "claude-sonnet-5-preview");
        match prev {
            Some(v) => std::env::set_var("JOURNAL_COMPILE_MODEL", v),
            None => std::env::remove_var("JOURNAL_COMPILE_MODEL"),
        }
    }

    // SOURCE_EXTRACTION_FALLBACK_MODEL is read by no other unit test.
    #[test]
    fn source_extraction_fallback_kill_switch() {
        std::env::remove_var("SOURCE_EXTRACTION_FALLBACK_MODEL");
        assert_eq!(source_extraction_fallback_model(), "deepseek/deepseek-v4.1-flash");
        std::env::set_var("SOURCE_EXTRACTION_FALLBACK_MODEL", "z-ai/glm-5.3-flash\n");
        assert_eq!(source_extraction_fallback_model(), "z-ai/glm-5.3-flash");
        let restored = crate::llm::openai::candidate_models(
            DEFAULT_EXTRACT_MODEL,
            Some(&source_extraction_fallback_model()),
        );
        assert_eq!(restored, vec![DEFAULT_EXTRACT_MODEL.to_string(), "z-ai/glm-5.3-flash".to_string()]);
        std::env::set_var("SOURCE_EXTRACTION_FALLBACK_MODEL", "  ");
        assert_eq!(source_extraction_fallback_model(), "deepseek/deepseek-v4.1-flash");
        std::env::remove_var("SOURCE_EXTRACTION_FALLBACK_MODEL");
    }

    // JEV_ASK_FALLBACK_MODEL is read by no other unit test.
    #[test]
    fn jev_ask_fallback_keeps_reasoning_off() {
        std::env::remove_var("JEV_ASK_FALLBACK_MODEL");
        assert_eq!(jev_ask_fallback_model(), "deepseek/deepseek-v4.1-flash");
        let models = crate::llm::openai::candidate_models(MODEL_FAST, Some(&jev_ask_fallback_model()));
        assert_eq!(models, vec![MODEL_FAST.to_string(), "deepseek/deepseek-v4.1-flash".to_string()]);
        assert!(!crate::llm::openai::any_requires_mandatory_reasoning(&models));
        let global = crate::llm::openai::candidate_models(MODEL_FAST, None);
        assert!(crate::llm::openai::any_requires_mandatory_reasoning(&global), "the global fallback forces a reasoning floor");
        std::env::set_var("JEV_ASK_FALLBACK_MODEL", " z-ai/glm-5.3-flash ");
        assert_eq!(jev_ask_fallback_model(), "z-ai/glm-5.3-flash");
        std::env::remove_var("JEV_ASK_FALLBACK_MODEL");
    }

    #[test]
    fn model_from_env_falls_back_when_unset() {
        // A var that is virtually guaranteed not to be set in CI.
        assert_eq!(
            model_from_env("CS_TEST_DEFINITELY_UNSET_MODEL_VAR", "fallback"),
            "fallback"
        );
    }

    #[test]
    fn parse_from_env_falls_back_on_garbage() {
        std::env::set_var("CS_TEST_PARSE_GARBAGE", "not-a-number");
        assert_eq!(parse_from_env::<u32>("CS_TEST_PARSE_GARBAGE", 99), 99);
        std::env::remove_var("CS_TEST_PARSE_GARBAGE");
    }

    #[test]
    fn parse_from_env_reads_valid_value() {
        std::env::set_var("CS_TEST_PARSE_VALID", "1234");
        assert_eq!(parse_from_env::<u32>("CS_TEST_PARSE_VALID", 99), 1234);
        std::env::remove_var("CS_TEST_PARSE_VALID");
    }

    // These two vars are read by no other test, so a serialized set/reset here is
    // race-free under cargo's parallel runner.
    #[test]
    fn summary_max_tokens_reject_zero_and_fall_back_to_default() {
        let prev_map = std::env::var("SUMMARY_MAP_MAX_TOKENS").ok();
        let prev_reduce = std::env::var("SUMMARY_REDUCE_MAX_TOKENS").ok();

        // A zero (bad env) must not be forwarded to the LLM — fall back to default.
        std::env::set_var("SUMMARY_MAP_MAX_TOKENS", "0");
        std::env::set_var("SUMMARY_REDUCE_MAX_TOKENS", "0");
        assert_eq!(summary_map_max_tokens(), DEFAULT_SUMMARY_MAP_MAX_TOKENS);
        assert_eq!(summary_reduce_max_tokens(), DEFAULT_SUMMARY_REDUCE_MAX_TOKENS);

        // A valid nonzero value is honored.
        std::env::set_var("SUMMARY_MAP_MAX_TOKENS", "777");
        assert_eq!(summary_map_max_tokens(), 777);

        match prev_map {
            Some(v) => std::env::set_var("SUMMARY_MAP_MAX_TOKENS", v),
            None => std::env::remove_var("SUMMARY_MAP_MAX_TOKENS"),
        }
        match prev_reduce {
            Some(v) => std::env::set_var("SUMMARY_REDUCE_MAX_TOKENS", v),
            None => std::env::remove_var("SUMMARY_REDUCE_MAX_TOKENS"),
        }
    }

    // ── Synthesis / digest `max_tokens` (production regression, 2026-08) ──────

    #[test]
    fn synthesis_and_digest_max_tokens_defaults_are_raised() {
        // Guards the intentional bump (2048 -> 4096, 1500 -> 4000) — a
        // regression here silently reintroduces the truncation bug.
        assert_eq!(DEFAULT_SYNTHESIS_MAX_TOKENS, 4096);
        assert_eq!(DEFAULT_DIGEST_MAX_TOKENS, 4000);
    }

    // These vars are read by no other test, so a serialized set/reset here is
    // race-free under cargo's parallel runner.
    #[test]
    fn synthesis_max_tokens_reads_env_and_falls_back_to_default() {
        let prev = std::env::var("SYNTHESIS_MAX_TOKENS").ok();

        std::env::remove_var("SYNTHESIS_MAX_TOKENS");
        assert_eq!(synthesis_max_tokens(), DEFAULT_SYNTHESIS_MAX_TOKENS);

        std::env::set_var("SYNTHESIS_MAX_TOKENS", "8192");
        assert_eq!(synthesis_max_tokens(), 8192);

        std::env::set_var("SYNTHESIS_MAX_TOKENS", "not-a-number");
        assert_eq!(
            synthesis_max_tokens(),
            DEFAULT_SYNTHESIS_MAX_TOKENS,
            "unparseable env falls back to the default"
        );

        // CodeRabbit (PR #452): a parsed `0` must fall back too — it would
        // otherwise be forwarded as the request's `max_tokens`, which breaks
        // the LLM call (an empty completion is requested).
        std::env::set_var("SYNTHESIS_MAX_TOKENS", "0");
        assert_eq!(
            synthesis_max_tokens(),
            DEFAULT_SYNTHESIS_MAX_TOKENS,
            "a zero env value must fall back to the default, not be forwarded"
        );

        match prev {
            Some(v) => std::env::set_var("SYNTHESIS_MAX_TOKENS", v),
            None => std::env::remove_var("SYNTHESIS_MAX_TOKENS"),
        }
    }

    #[test]
    fn digest_max_tokens_reads_env_and_falls_back_to_default() {
        let prev = std::env::var("DIGEST_MAX_TOKENS").ok();

        std::env::remove_var("DIGEST_MAX_TOKENS");
        assert_eq!(digest_max_tokens(), DEFAULT_DIGEST_MAX_TOKENS);

        std::env::set_var("DIGEST_MAX_TOKENS", "6000");
        assert_eq!(digest_max_tokens(), 6000);

        std::env::set_var("DIGEST_MAX_TOKENS", "not-a-number");
        assert_eq!(
            digest_max_tokens(),
            DEFAULT_DIGEST_MAX_TOKENS,
            "unparseable env falls back to the default"
        );

        // CodeRabbit (PR #452): see the matching zero-guard assertion in
        // `synthesis_max_tokens_reads_env_and_falls_back_to_default` above.
        std::env::set_var("DIGEST_MAX_TOKENS", "0");
        assert_eq!(
            digest_max_tokens(),
            DEFAULT_DIGEST_MAX_TOKENS,
            "a zero env value must fall back to the default, not be forwarded"
        );

        match prev {
            Some(v) => std::env::set_var("DIGEST_MAX_TOKENS", v),
            None => std::env::remove_var("DIGEST_MAX_TOKENS"),
        }
    }
}

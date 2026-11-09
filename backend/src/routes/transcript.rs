//! `POST /v1/transcript/cleanup` — conservatively clean a meeting transcript.
//!
//! Uses the ocean's known entity/people names as a correction dictionary and a
//! single Claude **Haiku** call to fix only proper nouns and clear speech-to-text
//! mis-transcriptions, preserving everything else verbatim (wording, filler words,
//! sentence structure, and speaker-label line prefixes). The endpoint NEVER 500s
//! on an LLM hiccup: on any failure, empty/garbage output, or an implausibly short
//! result it logs and returns the ORIGINAL transcript unchanged.
//!
//! # Consumer-plane RLS scoping (org-tenancy OPS-4 phase B.5)
//!
//! One query here touches an RLS-covered table (migration
//! `0083_consumer_rls`): the known-names dictionary load, `Store::list_nodes`
//! over `nodes`. It runs inside a `begin_account_scoped` transaction scoped to
//! the OCEAN'S OWNING account — the value this handler's own authorization
//! step already resolves via `Store::graph_account_id`, i.e. migration 0084's
//! audited `resolve_graph_account_id` SECURITY DEFINER bootstrap (which stays
//! deliberately unscoped: it is how that account is discovered). The ocean's
//! owner rather than `user_ctx.account_id` because the gate here is graph
//! REACHABILITY, which is multi-account by design (Model B) — a legitimate
//! caller may sit in a different account than the ocean's owner, and scoping
//! to their session account would empty the dictionary for exactly those
//! callers.
//!
//! The transaction is committed BEFORE the Haiku call: the dictionary read is
//! the last database work this handler does, and holding a pooled connection
//! across a multi-second LLM round trip is what PR #291's per-statement
//! rationale rules out.
//!
//! Not scoped, deliberately: `ReachabilityResolver::graph_reachable_by_user`
//! (permanent multi-account carve-out) and `llm::usage::record`, whose
//! `llm_usage` write is a separately-tracked conversion unit in the
//! rls-coverage manifest (`backend/src/llm/usage.rs`).

use axum::{extract::State, http::StatusCode, response::{IntoResponse, Response}, Extension, Json};
use serde_json::json;
use std::sync::Arc;
use utoipa::path as utoipa_path;
use uuid::Uuid;

use crate::llm::model_config;
use crate::llm::{CompletionRequest, Message, Role};
use crate::middleware::auth::UserContext;
use crate::tenancy::extractor::Tenant;
use crate::tenancy::reachability::ReachabilityResolver;
use crate::AppState;

// ── Prompt loaded at compile time ─────────────────────────────────────────────

const CLEANUP_SYSTEM_PROMPT: &str = include_str!("../../prompts/cleanup_transcript_v1.txt");

// ── Tuning constants ──────────────────────────────────────────────────────────

/// How many existing entity/people names to feed the model as a correction
/// dictionary.
const KNOWN_NAMES_LIMIT: i64 = 200;

/// Hard upper bound on the transcript we will accept (characters). Anything
/// larger is rejected gracefully (413) rather than sent to the model — keeps the
/// call cheap and bounded. ~200k chars ≈ a very long meeting; comfortably under
/// the model's context window once the prompt + known-names are added.
const MAX_TRANSCRIPT_CHARS: usize = 200_000;

/// Minimum fraction of the original length the cleaned text must retain to be
/// considered plausible. A conservative proofread keeps essentially all of the
/// text; a result under half the input length means the model summarized, refused,
/// or otherwise misbehaved — so we reject it and fall back to the original.
const MIN_PLAUSIBLE_LEN_RATIO: f64 = 0.5;

// ── Request / response types ──────────────────────────────────────────────────

/// Request body for transcript cleanup.
#[derive(serde::Deserialize, utoipa::ToSchema)]
pub struct CleanupTranscriptRequest {
    /// ID of the graph (ocean) whose known names are used as the correction
    /// dictionary. Must belong to the caller's account.
    pub graph_id: Uuid,
    /// Raw meeting transcript to clean.
    pub transcript: String,
}

/// Response body — the cleaned transcript (or the original, on fallback).
#[derive(serde::Serialize, utoipa::ToSchema)]
pub struct CleanupTranscriptResponse {
    /// The conservatively cleaned transcript. Equal to the input transcript when
    /// the LLM call failed or returned an implausible result (graceful fallback).
    pub transcript: String,
}

// ── Handler ───────────────────────────────────────────────────────────────────

/// Conservatively clean up a meeting transcript.
///
/// Authorizes the target graph exactly like atom ingest (graph must belong to a
/// personal ocean reachable by the caller), loads up to ~200 known entity/people
/// names, then runs a single Claude Haiku pass that fixes only proper nouns and
/// clear mis-transcriptions while preserving everything else verbatim. Falls back
/// to the original transcript on any LLM failure or implausible output.
#[utoipa_path(
    post,
    path = "/v1/transcript/cleanup",
    tag = "transcript",
    security(("bearer_token" = [])),
    request_body = CleanupTranscriptRequest,
    responses(
        (status = 200, description = "Cleaned transcript (or original on fallback)", body = CleanupTranscriptResponse),
        (status = 400, description = "Bad request"),
        (status = 401, description = "Unauthorized"),
        (status = 403, description = "Forbidden — graph not owned by caller's account"),
        (status = 413, description = "Transcript too large"),
        (status = 422, description = "Unprocessable entity — missing or invalid fields"),
        (status = 503, description = "Service not available"),
    )
)]
pub async fn cleanup_transcript(
    State(state): State<Arc<AppState>>,
    tenant: Tenant,
    Extension(user_ctx): Extension<UserContext>,
    Json(req): Json<CleanupTranscriptRequest>,
) -> Response {
    // ── 1. Validate basic input ───────────────────────────────────────────────
    if req.transcript.trim().is_empty() {
        return (
            StatusCode::UNPROCESSABLE_ENTITY,
            Json(json!({"error": "transcript must not be empty"})),
        )
            .into_response();
    }
    // Cheap, bounded: reject absurdly long input rather than sending it to the LLM.
    if req.transcript.chars().count() > MAX_TRANSCRIPT_CHARS {
        return (
            StatusCode::PAYLOAD_TOO_LARGE,
            Json(json!({
                "error": format!("transcript exceeds maximum of {MAX_TRANSCRIPT_CHARS} characters")
            })),
        )
            .into_response();
    }

    // ── 2. Require a Store + LLM ──────────────────────────────────────────────
    // Content-plane store from the `Tenant` extractor (org-tenancy PR 1.6; was
    // ambient `state.store`) — `graphs`/`nodes` are tenant-tree tables.
    let store = match &tenant.store {
        Some(s) => s,
        None => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({"error": "service unavailable"})),
            )
                .into_response()
        }
    };
    let Some(llm) = state.llm.clone() else {
        // No LLM configured — nothing to clean. Return the transcript untouched
        // rather than erroring; cleanup is a best-effort enhancement.
        return (
            StatusCode::OK,
            Json(CleanupTranscriptResponse { transcript: req.transcript }),
        )
            .into_response();
    };

    // ── 3. Authorize the target graph (mirrors ingest::ingest_atom) ───────────
    // The graph must exist and be reachable by the caller. Ingest goes further and
    // requires a *personal* ocean; cleanup is read-only (it writes nothing), so
    // reachability + ownership is the bar. We resolve the owning account first so
    // a non-existent / cross-account graph is indistinguishable (no info leak).
    // The resolved owner doubles as the RLS scope for the dictionary load
    // below (org-tenancy OPS-4 phase B.5; see the module doc) — this lookup
    // itself is 0084's SECURITY DEFINER bootstrap and stays unscoped.
    let owner_account = match store.graph_account_id(req.graph_id).await {
        Ok(Some(a)) => a,
        Ok(None) => {
            return (
                StatusCode::FORBIDDEN,
                Json(json!({"error": "graph not found or access denied"})),
            )
                .into_response();
        }
        Err(e) => {
            tracing::error!(error = %e, "DB error checking graph authorization");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            )
                .into_response();
        }
    };
    match ReachabilityResolver::graph_reachable_by_user(
        tenant.decision.as_ref(),
        store.as_ref(),
        req.graph_id,
        user_ctx.user_id,
    )
    .await
    {
        Ok(true) => {}
        Ok(false) => {
            return (
                StatusCode::FORBIDDEN,
                Json(json!({"error": "graph not found or access denied"})),
            )
                .into_response();
        }
        Err(e) => {
            tracing::error!(error = %e, "DB error checking graph reachability");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            )
                .into_response();
        }
    }

    // ── 4. Load known names (reuses the extract worker's query) ───────────────
    // Same `list_nodes(graph_id, Some("entity"), 200, 0)` the extractor uses for
    // canonicalization — the titles are the people/org/topic/project names that
    // already exist in this ocean.
    //
    // RLS scoping (org-tenancy OPS-4 phase B.5): on a Postgres plane this runs
    // through `list_nodes_scoped` inside a `begin_account_scoped` transaction
    // on the ocean's owning account, committed before the LLM call below.
    // A pool-less `Tenant` keeps the plain method. Both arms stay non-fatal: a failure degrades to an empty
    // dictionary exactly as before, never a 5xx.
    let known_names: Vec<String> = load_known_names(&tenant, store.as_ref(), owner_account, user_ctx.user_id, req.graph_id)
        .await
        .map(|nodes| nodes.into_iter().map(|n| n.title).collect())
        .unwrap_or_else(|e| {
            // Non-fatal: cleanup still works without the dictionary, just with less
            // name-correction power.
            tracing::warn!(error = %e, "transcript cleanup: failed to load known names — proceeding without dictionary");
            Vec::new()
        });

    // ── 5. Build the request + call Claude Haiku (same client/model as extract) ─
    let request = build_cleanup_request(&req.transcript, &known_names);
    // Usage-metering PR4: routes through `metered_complete` (reserve before
    // the call, finalize on success). `llm_usage` is a tenant-tree table
    // (org-tenancy PR 1.6): the accounting row lands on the caller's content
    // plane, mirroring `nodes.rs::node_digest`'s identical move in PR 1.5.
    let usage_ctx = tenant.pool.clone().map(|pool| {
        crate::llm::usage::UsageContext::new(pool, Some(user_ctx.account_id), Some(req.graph_id))
            .with_user(user_ctx.user_id)
            .with_attributed_user(user_ctx.user_id)
    });
    let cleaned = match crate::llm::usage::metered_complete(
        llm.as_ref(),
        request,
        usage_ctx.as_ref(),
        "transcript_cleanup",
    )
    .await
    {
        Ok(resp) => resp.content,
        Err(e) => {
            // Graceful fallback: never 500 on an LLM hiccup — return the original.
            tracing::warn!(error = %e, "transcript cleanup: LLM call failed — returning original transcript");
            return (
                StatusCode::OK,
                Json(CleanupTranscriptResponse { transcript: req.transcript }),
            )
                .into_response();
        }
    };

    // ── 6. Sanity-check the result; fall back to the original if implausible ──
    let final_transcript = if is_plausible_cleanup(&req.transcript, &cleaned) {
        cleaned
    } else {
        tracing::warn!(
            original_len = req.transcript.chars().count(),
            cleaned_len = cleaned.chars().count(),
            "transcript cleanup: implausible result — returning original transcript"
        );
        req.transcript
    };

    (
        StatusCode::OK,
        Json(CleanupTranscriptResponse { transcript: final_transcript }),
    )
        .into_response()
}

// ── Helpers ───────────────────────────────────────────────────────────────────

/// The ocean's known entity names, read under the request's RLS scope when
/// there is one (org-tenancy OPS-4 phase B.5 — see the module doc).
///
/// The transaction is committed rather than rolled back even though this is a
/// read: nothing was written either way, and committing keeps the shape
/// identical to every other converted route in this codebase. A commit failure
/// surfaces as `Err` and the caller degrades to an empty dictionary, same as
/// any other read failure.
async fn load_known_names(
    tenant: &Tenant,
    store: &dyn crate::store::Store,
    owner_account: Uuid,
    user_id: Uuid,
    graph_id: Uuid,
) -> Result<Vec<crate::store::Node>, crate::store::StoreError> {
    let Some(pool) = tenant.pool.as_ref() else {
        return store
            .list_nodes(graph_id, Some("entity"), KNOWN_NAMES_LIMIT, 0)
            .await;
    };
    let mut tx = crate::tenancy::scope::begin_tenant_scoped(pool, owner_account, user_id).await?;
    let nodes = store
        .list_nodes_scoped(&mut tx, graph_id, Some("entity"), KNOWN_NAMES_LIMIT, 0)
        .await?;
    tx.commit().await?;
    Ok(nodes)
}

/// Build the Haiku cleanup request: the transcript plus the ocean's known names
/// appended as a `## Known names` dictionary, with the conservative system prompt.
/// Uses the same model resolver (`model_config::extract_model`, i.e. Haiku) the
/// extract worker uses so cleanup and extraction stay on the same cheap model.
fn build_cleanup_request(transcript: &str, known_names: &[String]) -> CompletionRequest {
    let user_content = if known_names.is_empty() {
        transcript.to_string()
    } else {
        let mut s = transcript.to_string();
        s.push_str("\n\n## Known names\n");
        for name in known_names {
            s.push_str("- ");
            s.push_str(name);
            s.push('\n');
        }
        s
    };
    CompletionRequest {
        messages: vec![Message {
            role: Role::User,
            content: user_content,
        }],
        model: model_config::extract_model(),
        // Cleanup echoes the (possibly long) transcript back, so the token cap must
        // scale with input rather than reuse the small extraction cap. ~1 token per
        // 3 chars of UTF-8 text, padded, then clamped so a pathological input can't
        // request an unbounded completion.
        max_tokens: cleanup_max_tokens(transcript),
        system: Some(CLEANUP_SYSTEM_PROMPT.to_string()),
        tool_schema: None,
        effort: None,
        reasoning: crate::llm::ReasoningSetting::Off,
    }
}

/// Output-token budget for the cleanup call. The model returns roughly the whole
/// transcript, so the cap is derived from input length (≈1 token / 3 chars, ×1.3
/// headroom) and clamped to a sane floor/ceiling.
fn cleanup_max_tokens(transcript: &str) -> u32 {
    let chars = transcript.chars().count();
    let est = ((chars as f64 / 3.0) * 1.3).ceil() as u32;
    est.clamp(512, 64_000)
}

/// A cleaned transcript is *plausible* only if it is non-empty (after trimming)
/// and at least `MIN_PLAUSIBLE_LEN_RATIO` of the original length. A conservative
/// proofread changes almost nothing about the length; a dramatically shorter
/// result means the model summarized, refused, or returned garbage — reject it.
fn is_plausible_cleanup(original: &str, cleaned: &str) -> bool {
    if cleaned.trim().is_empty() {
        return false;
    }
    let original_len = original.chars().count();
    if original_len == 0 {
        // Caller already rejected empty input, but guard against div-by-zero.
        return false;
    }
    let cleaned_len = cleaned.chars().count();
    (cleaned_len as f64) >= (original_len as f64) * MIN_PLAUSIBLE_LEN_RATIO
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // ── Length / plausibility sanity guard ────────────────────────────────────

    #[test]
    fn plausible_when_roughly_same_length() {
        let original = "You: hello there\nOthers: hi, how are you doing today";
        // A realistic conservative fix: one name corrected, length essentially equal.
        let cleaned = "You: hello there\nOthers: hi, how are you doing today";
        assert!(is_plausible_cleanup(original, cleaned));
    }

    #[test]
    fn implausible_when_dramatically_shorter() {
        let original = "You: ".to_string() + &"word ".repeat(200);
        // Model summarized down to a sentence — far under 50% of the input length.
        let cleaned = "They talked about the project.";
        assert!(!is_plausible_cleanup(&original, cleaned));
    }

    #[test]
    fn implausible_when_empty_or_whitespace() {
        let original = "You: something was said here";
        assert!(!is_plausible_cleanup(original, ""));
        assert!(!is_plausible_cleanup(original, "   \n  \t "));
    }

    #[test]
    fn plausible_at_exactly_half_length() {
        // 100 chars in, exactly 50 chars out — the boundary is inclusive (>=).
        let original = "a".repeat(100);
        let cleaned = "b".repeat(50);
        assert!(is_plausible_cleanup(&original, &cleaned));
    }

    #[test]
    fn implausible_just_below_half_length() {
        let original = "a".repeat(100);
        let cleaned = "b".repeat(49);
        assert!(!is_plausible_cleanup(&original, &cleaned));
    }

    #[test]
    fn longer_cleaned_is_plausible() {
        // A fix can lengthen text (e.g. expanding a garbled token); never reject for length growth.
        let original = "You: cora swarm is grate";
        let cleaned = "You: CoralSwarm is great, really really great";
        assert!(is_plausible_cleanup(original, cleaned));
    }

    // ── Request building ──────────────────────────────────────────────────────

    #[test]
    fn request_uses_haiku_extract_model_and_prompt() {
        let req = build_cleanup_request("You: hello", &["Alex".to_string()]);
        assert_eq!(req.model, model_config::extract_model());
        assert_eq!(req.system.as_deref(), Some(CLEANUP_SYSTEM_PROMPT));
        assert!(req.tool_schema.is_none(), "cleanup uses free-text output, not forced tools");
        // Known names are appended as a dictionary.
        assert!(req.messages[0].content.contains("## Known names"));
        assert!(req.messages[0].content.contains("Alex"));
        assert!(req.messages[0].content.contains("You: hello"));
    }

    #[test]
    fn request_omits_dictionary_when_no_known_names() {
        let req = build_cleanup_request("You: hello", &[]);
        assert!(!req.messages[0].content.contains("## Known names"));
        assert_eq!(req.messages[0].content, "You: hello");
    }

    // ── Token budget ──────────────────────────────────────────────────────────

    #[test]
    fn max_tokens_has_floor_and_ceiling() {
        // Tiny input → floor.
        assert_eq!(cleanup_max_tokens("hi"), 512);
        // Huge input → clamped to the ceiling, never unbounded.
        let huge = "x".repeat(1_000_000);
        assert_eq!(cleanup_max_tokens(&huge), 64_000);
    }

    #[test]
    fn max_tokens_scales_with_input() {
        // A mid-size transcript lands between the floor and ceiling and grows with length.
        let small = "x".repeat(6_000);
        let large = "x".repeat(30_000);
        let small_budget = cleanup_max_tokens(&small);
        let large_budget = cleanup_max_tokens(&large);
        assert!(small_budget > 512);
        assert!(large_budget > small_budget);
        assert!(large_budget <= 64_000);
    }

    // ── Prompt sanity ─────────────────────────────────────────────────────────

    #[test]
    fn system_prompt_loaded_and_conservative() {
        assert!(!CLEANUP_SYSTEM_PROMPT.is_empty(), "system prompt must not be empty");
        // Key conservative guarantees the prompt must carry.
        assert!(CLEANUP_SYSTEM_PROMPT.contains("Known names"));
        assert!(CLEANUP_SYSTEM_PROMPT.to_lowercase().contains("speaker"));
        assert!(CLEANUP_SYSTEM_PROMPT.to_lowercase().contains("verbatim"));
    }
}

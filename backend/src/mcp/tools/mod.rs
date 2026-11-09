//! Tool router assembly + the shared auth-extraction helper every tool body
//! calls first (`docs/plans/remote-mcp-v1.md` §4.5 multi-tenancy rule: never
//! trust caller-supplied ids — always re-derive identity from the verified
//! token).
//!
//! # Consumer-plane RLS scoping (org-tenancy OPS-4, MCP surface)
//! This module also owns the shared half of the scoping seam both tool
//! modules use: [`ScopedTx`], [`scoped_store`], [`commit_read_tx`],
//! [`commit_write_tx`]. It is the MCP analogue of the file-local
//! `begin_scoped_tx!`/`scoped_store!`/`commit_*_tx` trio the converted route
//! files each carry (`routes::{nodes,relationships,…}`, phase B.1/B.2) —
//! hoisted here rather than duplicated because `tools/read.rs` and
//! `tools/write.rs` are the only two consumers and both need the identical
//! shape, and because the MCP flavour returns a `CallToolResult` tool error
//! (`{"error":{"code","message"}}`) instead of an HTTP response. Only the
//! transaction OPEN stays per-file (see the comment on [`ScopedTx`]).
//!
//! See each tool module's own doc for its per-tool scope-account choice and
//! its list of deliberate exceptions.

pub(crate) mod read;
/// The 2 tenant-selection tools (`docs/plans/claim-based-tenancy.md` §Phase
/// 3, PR 3.2) — control-plane-only, never touches `content_tenant`/`ScopedTx`
/// (see this module's own doc for why it is separate from `read`/`write`).
pub(crate) mod tenancy;
pub(crate) mod write;

use std::sync::Arc;

use rmcp::model::CallToolResult;
use rmcp::service::RequestContext;
use rmcp::ErrorData as McpError;
use rmcp::RoleServer;
use serde_json::json;

use crate::store::{PostgresStore, Store};
use crate::tenancy::extractor::Tenant;
use crate::tenancy::runtime::TenancyDecision;
use crate::AppState;

use super::auth::McpAuthContext;

/// Reef types accepted by `list_reefs`'/`update_reef`'s `type` field. Mirrors
/// `routes::nodes::ALLOWED_TYPES` (private to that module, so duplicated here
/// rather than exposed cross-module for one constant — keep in sync if the
/// reef type vocabulary ever changes). Shared by `tools/read.rs` and
/// `tools/write.rs` so the two modules don't each keep their own copy.
pub(crate) const ALLOWED_REEF_TYPES: &[&str] = &["entity", "decision", "thread", "reference"];

/// Tool names that require write scope (Phase 3, `docs/plans/remote-mcp-v1.md`
/// §5). Checked independently at TWO points (`server::CoralSwarmMcp::list_tools`
/// filters these out of the advertised catalog without write scope;
/// `call_tool` rejects a call to one of these names without write scope even
/// if the client calls it unlisted) — see plan §4.2.
pub(crate) const WRITE_TOOLS: &[&str] =
    &["add_context", "confirm_reef", "reject_reef", "update_reef", "promote"];

/// The two tenant-selection tools (`tools::tenancy`, Phase 3 PR 3.2) that stay
/// callable REGARDLESS of whether a selection currently exists — see that
/// module's own doc for why neither is ever gated on the other. Consulted by
/// `server::CoralSwarmMcp`'s Phase 3 PR 3.3 catalogue gate (decision 5: ≥1
/// org membership with no active selection narrows the catalogue to exactly
/// these two), checked independently at the SAME two points [`WRITE_TOOLS`]
/// is: `list_tools` (hides everything else) and `call_tool` (rejects a call
/// to anything else even if a hostile/caching client names it directly).
pub(crate) const TENANCY_SELECTION_TOOLS: &[&str] =
    &[tenancy::LIST_TENANTS_TOOL_NAME, tenancy::SELECT_TENANT_TOOL_NAME];

/// Extract the verified [`McpAuthContext`] the `mcp_auth` HTTP middleware
/// attached to this request (`mcp/auth.rs`). The streamable-HTTP transport
/// rides `http::request::Parts` (and, inside it, the axum `Extension`
/// extensions the middleware inserted into) through to every JSON-RPC
/// message's [`RequestContext::extensions`] — see `mod.rs`'s module doc on
/// per-request identity plumbing.
///
/// Returns a **protocol-level** error (never a tool error) when absent: this
/// should be unreachable in production (the middleware runs on every request
/// before the streamable-HTTP service is ever invoked), so its absence means
/// a plumbing bug, not a caller mistake — callers can never see this via a
/// well-formed request through the real `/mcp` router.
pub(crate) fn auth(ctx: &RequestContext<RoleServer>) -> Result<McpAuthContext, McpError> {
    ctx.extensions
        .get::<axum::http::request::Parts>()
        .and_then(|parts| parts.extensions.get::<McpAuthContext>())
        .cloned()
        .ok_or_else(|| McpError::internal_error("missing MCP auth context", None))
}

/// Resolve the CONTENT-plane handles for this call from the auth context's
/// [`TenancyDecision`] (org-tenancy PR 2.3) — the MCP analogue of the axum
/// `Tenant` extractor (`tenancy::extractor`), which tool bodies can't use
/// directly (they run inside rmcp's dispatcher, not as axum handlers; the
/// decision rides [`McpAuthContext`] instead of a request extension).
///
/// Resolution rule, matching the extractor's arms exactly:
///
/// * `None` / `Some(Personal)` — legacy `state.db`/`state.store`. `None` is
///   the flag-off state (`mcp_auth` only ever stamps a decision when the
///   tenancy runtime is enabled), so flag-off behavior stays byte-identical.
///   Unlike the axum extractor there is no "absent decision + enabled
///   runtime ⇒ 500" tripwire arm here: `mcp_auth` itself is the single
///   stamping site AND the only way into a tool body (dispatch without it
///   already hard-fails in `tools::auth`), so the extractor's
///   mounted-outside-the-gate deploy-bug class cannot occur on this path.
/// * `Some(Org(ctx))` — a fresh [`PostgresStore`] over `ctx.pool` (the
///   fingerprint-verified tenant pool) plus that pool. Cheap per call (one
///   `PgPool` clone, no I/O — same note as the extractor's Org arm).
pub(crate) fn content_tenant(state: &AppState, auth: &McpAuthContext) -> Tenant {
    match &auth.decision {
        Some(TenancyDecision::Org(ctx)) => Tenant {
            store: Some(Arc::new(PostgresStore::new(ctx.pool.clone())) as Arc<dyn Store>),
            pool: Some(ctx.pool.clone()),
            decision: auth.decision.clone(),
            class: crate::route_class::RouteClass::Other,
        },
        _ => Tenant {
            store: state.store.clone(),
            pool: state.db.clone(),
            decision: auth.decision.clone(),
            class: crate::route_class::RouteClass::Other,
        },
    }
}

// ── Consumer-plane RLS scoping seam (org-tenancy OPS-4, MCP surface) ─────────

/// One tool call's account-scoped transaction. The `begin_scoped_tx!` macros
/// in `read.rs`/`write.rs` always yield `Some`; with no pool they return a
/// `service_unavailable` tool error instead. A `None` (passed by callers that
/// have no transaction) makes [`scoped_store!`] run the plain unscoped Store
/// methods.
pub(crate) type ScopedTx = Option<sqlx::Transaction<'static, sqlx::Postgres>>;

// The transaction OPEN itself is a file-local `begin_scoped_tx!` macro in each
// of `read.rs`/`write.rs` rather than a shared helper here, deliberately:
// `scripts/check_rls_coverage.sh` verifies that a file marked `converted` in
// the manifest literally contains a `begin_account_scoped` call, and a macro
// (or helper-fn) expansion does not put that text in the calling file. Every
// converted route file carries its own copy for the same reason. The two
// copies are byte-identical apart from their doc comments; everything AFTER
// the open — dispatch, commit — is shared here.

/// Commit a read-only tool's scoped transaction. Failures are logged and
/// swallowed: the response data is already materialized, and a read-only
/// `SET LOCAL` transaction only fails to commit when a tolerated best-effort
/// sub-query already aborted it (those sub-results then fell back to their
/// existing best-effort defaults — same degradation as before this change).
/// Mirrors `routes::relationships::commit_read_tx`.
pub(crate) async fn commit_read_tx(tx: ScopedTx) {
    if let Some(tx) = tx {
        if let Err(e) = tx.commit().await {
            tracing::warn!(error = %e, "read-only scoped transaction commit failed");
        }
    }
}

/// Commit a WRITE tool's scoped transaction before responding — a commit
/// failure means nothing was persisted, so it must surface as a tool error,
/// never a false success. `Err` is a ready-to-return tool error.
pub(crate) async fn commit_write_tx(tx: ScopedTx, log_context: &str) -> Result<(), CallToolResult> {
    if let Some(tx) = tx {
        if let Err(e) = tx.commit().await {
            tracing::error!(error = %e, tool = log_context, "scoped transaction commit failed");
            return Err(tool_error("service_unavailable", "internal error"));
        }
    }
    Ok(())
}

/// Dispatch one `Store` call through the scoped transaction, or the plain
/// unscoped method when `$tx` is `None`. Identical in shape to the converted
/// route files' own `scoped_store!`.
macro_rules! scoped_store {
    ($store:expr, $tx:expr, $unscoped:ident / $scoped:ident ( $($arg:expr),* $(,)? )) => {
        match $tx.as_mut() {
            Some(tx) => $store.$scoped(tx, $($arg),*).await,
            None => $store.$unscoped($($arg),*).await,
        }
    };
}
pub(crate) use scoped_store;

/// Build a tool-error `CallToolResult` in the plan's error shape:
/// `{"error":{"code","message"}}` (§3 conventions). `code` is one of
/// `invalid_params | not_found | rate_limited | insufficient_scope |
/// service_unavailable | synthesis_failed | transcript_unavailable |
/// tenant_selection_required`. The last is Phase 3 PR 3.3's addition
/// (`server::CoralSwarmMcp::call_tool`'s selection gate) — deliberately its
/// OWN code rather than reusing `insufficient_scope`: the two are different
/// signals (a real permission denial vs. "choose a tenant first, then
/// retry"), and collapsing them would make a caller's next action ambiguous.
pub(crate) fn tool_error(code: &str, message: impl std::fmt::Display) -> CallToolResult {
    CallToolResult::structured_error(json!({
        "error": { "code": code, "message": message.to_string() }
    }))
}

/// The MCP form of the web's `402 PAYMENT_REQUIRED`: the `payment_required`
/// tool error, plus `error.refusal_reason` (the funding policy's stable code:
/// `free_plan`, `credits_frozen`, `extra_usage_off`, `member_cap_reached`,
/// `monthly_limit_reached`, `extra_balance_exhausted`). `code` and `message`
/// keep their pre-extra-usage values, so existing clients are unaffected.
pub(crate) fn payment_required_error(
    message: impl std::fmt::Display,
    reason: crate::billing::funding_policy::RefusalReason,
) -> CallToolResult {
    CallToolResult::structured_error(json!({
        "error": {
            "code": "payment_required",
            "message": message.to_string(),
            "refusal_reason": reason.code(),
        }
    }))
}

// ── structuredContent-only negotiation (Track B, search/ask retrieval roadmap) ─
//
// `CallToolResult::structured()` (rmcp) ships `structuredContent` AND the
// identical JSON re-serialized into a text `ContentBlock` — the MCP spec
// (2025-06-18) only says a server that returns `structuredContent` SHOULD
// also return equivalent text "for backwards compatibility", not MUST. For a
// caller that reads `structuredContent` directly (Claude Code, and any other
// structured-content-aware client), that text block is pure duplicated
// payload — for a large `ask_ocean`/`search_atoms` response this roughly
// doubles the tokens shipped into the calling LLM's context for zero benefit.
//
// The identity signal used to negotiate this is [`McpAuthContext::mcp_client_name`]
// (User-Agent-derived), NOT rmcp's own MCP-protocol `client_info`. That is a
// deliberate choice, not a limitation: SEP-2575 (MCP `2026-07-28`, rmcp
// 3.1.2 — the version this backend runs) DOES put a real per-request
// `client_info` on every call, including in stateless mode, readable off the
// request's raw `_meta` (`RequestContext::meta.client_info()` — NOT the
// `RequestContext::client_info()` convenience method, whose stateless-mode
// legacy fallback is a non-`None` synthetic default; see
// `McpAuthContext::mcp_client_name`'s doc for the verified trap); see
// `services::platform::detect_platform`,
// which reads exactly this for `platform_detected`). Reclassifying a caller
// by clientInfo instead of `mcp_client_name` would change the wire shape
// (`structuredContent`-only vs. dual-payload) that a given client gets back
// — a bigger, response-shape-changing blast radius than negotiating off the
// same signal every other identity decision in this file already uses.
// `McpAuthContext::mcp_client_name`'s own doc comment (`mcp/auth.rs`) covers
// the equivalent choice for the atom-provenance stamp — this reuses that
// existing, already-threaded-through signal rather than introducing a
// second identity source for one negotiation.
//
// Scope: only [`tool_success`] negotiates. Tool-level errors
// (`CallToolResult::structured_error`, `tool_error`) are NOT negotiated —
// error payloads are small (`{"error":{"code","message"}}`), so the
// duplication there is a few dozen bytes, not worth threading
// `mcp_client_name` through ~90 error call sites (several of which are free
// functions with no `McpAuthContext` in scope at all, e.g.
// `ask_error_to_tool_error`). The token cost this task targets lives
// entirely in large SUCCESS payloads (`ask_ocean` citations, `search_atoms`
// results, listings) — `tool_success` is where all of those funnel through.

/// Env var: additional case-insensitive substrings (comma-separated),
/// ADDED to [`DEFAULT_STRUCTURED_ONLY_CLIENT_HINTS`] (never replacing it —
/// same additive-override idiom as `McpConfig::from_env`'s
/// `MCP_ALLOWED_HOSTS`, `mcp/mod.rs`), matched against
/// [`super::auth::McpAuthContext::mcp_client_name`] to decide whether a
/// caller is treated as structured-content-capable.
///
/// **No real captured `mcp_client_name` value for any live MCP client
/// exists anywhere in this codebase or its history** (checked: git log,
/// `docs/plans/remote-mcp-v1.md`, the MCP audit log schema) — every
/// `"claude-code/1.0"`-shaped string in this crate is an illustrative test
/// literal (`mcp/auth.rs`'s own tests, `mcp/tools/write.rs`'s provenance
/// tests), not an observed production User-Agent. This env var exists
/// specifically so the real value(s) can be added — or the built-in default
/// corrected/removed — once observed (e.g. via `atoms.mcp_client_name` on
/// `add_context`-ingested rows) without a code change or redeploy.
pub(crate) const MCP_STRUCTURED_ONLY_CLIENTS_ENV: &str = "MCP_STRUCTURED_ONLY_CLIENTS";

/// Env var (truthy: `1`/`true`/`TRUE`/`True`/`yes`/`on`): unconditionally
/// force EVERY caller back into today's dual-payload behavior, overriding
/// both [`DEFAULT_STRUCTURED_ONLY_CLIENT_HINTS`] and
/// `MCP_STRUCTURED_ONLY_CLIENTS`. A safety valve — if the negotiation ever
/// misbehaves for some real client in production (e.g. a client that reads
/// `content` and ignores `structuredContent`, contrary to what its
/// User-Agent implied), this disables it instantly with just an env var flip
/// + restart, no code change.
pub(crate) const MCP_STRUCTURED_ONLY_DISABLE_ENV: &str = "MCP_STRUCTURED_ONLY_DISABLE";

/// Built-in best-effort default: `mcp_client_name` substrings (matched
/// case-insensitively) assumed to belong to a structured-content-capable
/// client. Deliberately narrow — an unverified guess (see
/// [`MCP_STRUCTURED_ONLY_CLIENTS_ENV`]'s doc comment) should not be allowed
/// to silently misfire broadly; `MCP_STRUCTURED_ONLY_CLIENTS` is how this
/// gets corrected once real values are known. Empty `mcp_client_name` (no
/// `User-Agent` header) NEVER matches regardless of this list — see
/// [`structured_only_client`].
const DEFAULT_STRUCTURED_ONLY_CLIENT_HINTS: &[&str] = &["claude-code"];

/// Whether a caller identified by `client_name` (`McpAuthContext::mcp_client_name`,
/// `None` when the request had no/unreadable `User-Agent` header) should get
/// a `structuredContent`-only [`tool_success`] response instead of today's
/// dual `structuredContent` + text-duplicate response.
///
/// The safe default — dual payload — wins whenever there's any doubt:
/// `MCP_STRUCTURED_ONLY_DISABLE` set, `client_name` absent, or `client_name`
/// matching none of the known hints. Only an explicit, positive match flips
/// to structured-only.
fn structured_only_client(client_name: Option<&str>) -> bool {
    if super::env_truthy(MCP_STRUCTURED_ONLY_DISABLE_ENV) {
        return false;
    }
    let Some(name) = client_name else {
        return false;
    };
    let name = name.to_ascii_lowercase();
    if DEFAULT_STRUCTURED_ONLY_CLIENT_HINTS
        .iter()
        .any(|hint| name.contains(hint))
    {
        return true;
    }
    std::env::var(MCP_STRUCTURED_ONLY_CLIENTS_ENV)
        .ok()
        .map(|raw| {
            raw.split(',')
                .map(|s| s.trim().to_ascii_lowercase())
                .filter(|s| !s.is_empty())
                .any(|hint| name.contains(&hint))
        })
        .unwrap_or(false)
}

/// Build a successful tool result. For a known structured-content-capable
/// caller (see [`structured_only_client`]), returns `structuredContent`
/// only (`content: []` — the MCP spec's backwards-compat text block is a
/// SHOULD, not a MUST, and `content` is a plain array so an empty one is
/// valid per rmcp's own lenient `Deserialize` impl, which already treats a
/// missing/absent `content` field as `[]`). Every other caller — unknown
/// client, absent `User-Agent`, or the `MCP_STRUCTURED_ONLY_DISABLE` safety
/// valve — gets today's unchanged dual `structuredContent` + text-duplicate
/// behavior (ChatGPT compatibility — plan §3 conventions).
pub(crate) fn tool_success(value: serde_json::Value, client_name: Option<&str>) -> CallToolResult {
    if structured_only_client(client_name) {
        let mut result = CallToolResult::default();
        result.structured_content = Some(value);
        result.is_error = Some(false);
        result
    } else {
        CallToolResult::structured(value)
    }
}

#[cfg(test)]
mod negotiation_tests {
    use super::*;

    // `structured_only_client`/`tool_success` mutate/read process env vars
    // (`MCP_STRUCTURED_ONLY_CLIENTS`, `MCP_STRUCTURED_ONLY_DISABLE`), which
    // `cargo test`'s default in-binary thread-pool parallelism turns into a
    // real race against any other test touching the same vars — mirrors
    // `mcp::test_support::env_lock()` (`mcp/mod.rs`), reused here rather than
    // a second lock so these tests and `mcp::mod`'s `from_env` tests can
    // never race each other either.
    fn guard() -> tokio::sync::MutexGuard<'static, ()> {
        // Plain #[test] (not #[tokio::test]) fns — `blocking_lock()` mirrors
        // `mcp::mod`'s own `from_env` tests' `env_lock().blocking_lock()` use.
        super::super::test_support::env_lock().blocking_lock()
    }

    fn clear_env() {
        // SAFETY: serialized on `guard()`.
        unsafe {
            std::env::remove_var(MCP_STRUCTURED_ONLY_CLIENTS_ENV);
            std::env::remove_var(MCP_STRUCTURED_ONLY_DISABLE_ENV);
        }
    }

    #[test]
    fn unknown_client_stays_dual_payload() {
        let _g = guard();
        clear_env();
        assert!(!structured_only_client(Some("some-other-tool/9.9")));
        assert!(!structured_only_client(None));
        clear_env();
    }

    #[test]
    fn built_in_hint_matches_case_insensitively() {
        let _g = guard();
        clear_env();
        assert!(structured_only_client(Some("claude-code/1.0")));
        assert!(structured_only_client(Some("Claude-Code/2.1 (macOS)")));
        assert!(structured_only_client(Some("CLAUDE-CODE")));
        clear_env();
    }

    #[test]
    fn env_allowlist_adds_without_replacing_builtin() {
        let _g = guard();
        clear_env();
        // SAFETY: serialized on `guard()`.
        unsafe {
            std::env::set_var(MCP_STRUCTURED_ONLY_CLIENTS_ENV, "cursor, my-harness");
        }
        assert!(structured_only_client(Some("cursor/3.0")));
        assert!(structured_only_client(Some("my-harness/0.1")));
        // built-in default is still active alongside the env addition
        assert!(structured_only_client(Some("claude-code/1.0")));
        assert!(!structured_only_client(Some("totally-unrelated/1.0")));
        clear_env();
    }

    #[test]
    fn disable_env_forces_dual_payload_even_for_a_known_hint() {
        let _g = guard();
        clear_env();
        // SAFETY: serialized on `guard()`.
        unsafe {
            std::env::set_var(MCP_STRUCTURED_ONLY_DISABLE_ENV, "1");
        }
        assert!(!structured_only_client(Some("claude-code/1.0")));
        clear_env();
    }

    #[test]
    fn tool_success_structured_only_shape() {
        let _g = guard();
        clear_env();
        let value = json!({"foo": "bar"});
        let r = tool_success(value.clone(), Some("claude-code/1.0"));
        assert_eq!(r.structured_content, Some(value));
        assert!(r.content.is_empty(), "expected no text content block: {:?}", r.content);
        assert_eq!(r.is_error, Some(false));
        clear_env();
    }

    #[test]
    fn tool_success_dual_payload_shape_unchanged_for_unknown_client() {
        let _g = guard();
        clear_env();
        let value = json!({"foo": "bar"});
        let r = tool_success(value.clone(), Some("some-other-tool/9.9"));
        assert_eq!(r.structured_content, Some(value.clone()));
        assert_eq!(r.content.len(), 1, "expected the legacy text duplicate block");
        let text = r.content[0].as_text().expect("text content block");
        let parsed: serde_json::Value =
            serde_json::from_str(&text.text).expect("text block is JSON");
        assert_eq!(parsed, value, "text block must still equal structuredContent verbatim");
        assert_eq!(r.is_error, Some(false));
        clear_env();
    }

    #[test]
    fn tool_success_dual_payload_shape_unchanged_when_no_client_name() {
        let _g = guard();
        clear_env();
        let r = tool_success(json!({"x": 1}), None);
        assert_eq!(r.content.len(), 1);
        clear_env();
    }
}

#[cfg(test)]
mod payment_required_tests {
    use super::*;
    use crate::billing::funding_policy::RefusalReason;

    /// `payment_required` keeps `error.code` and `error.message` and adds
    /// `error.refusal_reason` (the same code the web's 402 carries).
    #[test]
    fn payment_required_carries_the_refusal_reason_beside_the_old_fields() {
        for reason in RefusalReason::ALL {
            let result = payment_required_error("insufficient credits", reason);
            assert_eq!(result.is_error, Some(true));
            assert_eq!(
                result.structured_content.expect("structured error"),
                json!({
                    "error": {
                        "code": "payment_required",
                        "message": "insufficient credits",
                        "refusal_reason": reason.code(),
                    }
                })
            );
        }
        // Other tool errors are untouched.
        assert_eq!(
            tool_error("not_found", "x").structured_content.unwrap(),
            json!({"error": {"code": "not_found", "message": "x"}})
        );
    }
}

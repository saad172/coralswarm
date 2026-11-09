//! Remote-MCP audit logging (`docs/plans/remote-mcp-v1.md` §4.4): a
//! `target="mcp.tool_call"` tracing line (real-time, Fly logs) plus a
//! best-effort append-only `mcp_tool_calls` row (migration 0058). Both fire
//! for EVERY dispatched call — denied calls (scope/tenancy/rate-limit) are
//! always logged, since they're the security signal this module exists for.
//!
//! **Never logs or stores raw tool arguments, results, or token material** —
//! only ids/sizes/enums the caller explicitly chooses to summarize
//! (`args_summary`) and an irreversible digest of the canonicalized arguments
//! (`args_digest`, integrity/dedup only). A DB write failure here must never
//! fail the tool call it's describing (mirrors `routes::activity`'s
//! `insert_activity_event`) — logged at `warn` and dropped.

use serde_json::Value;
use sha2::{Digest, Sha256};
use uuid::Uuid;

/// Outcome discriminant persisted to `mcp_tool_calls.outcome` and the
/// tracing line. Mirrors migration 0058's documented value set.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AuditOutcome {
    Ok,
    Error,
    DeniedScope,
    /// Reserved for a dispatcher-level tenancy denial. Phase 2's read tools
    /// each perform their OWN reachability/ownership guard inline (§4.5) and
    /// surface a miss as an ordinary `not_found` tool error (IDOR non-leak
    /// convention) rather than signaling back to `server::CoralSwarmMcp` that
    /// the miss was specifically tenancy-related — the dispatcher has no way
    /// to distinguish "genuinely absent" from "not yours" from a tool's
    /// `CallToolResult` alone, so this variant is never constructed in
    /// Phase 2. Kept in the enum (rather than removed) because the audit
    /// table/tracing schema documents it as a first-class outcome (§4.4);
    /// wiring a real signal into it is a candidate follow-up, not a Phase 2
    /// requirement.
    #[allow(dead_code)]
    DeniedTenancy,
    RateLimited,
}

impl AuditOutcome {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            AuditOutcome::Ok => "ok",
            AuditOutcome::Error => "error",
            AuditOutcome::DeniedScope => "denied_scope",
            AuditOutcome::DeniedTenancy => "denied_tenancy",
            AuditOutcome::RateLimited => "rate_limited",
        }
    }
}

/// One tool-call audit entry. Constructed by `mcp::server`'s dispatch gate
/// around every `tools/call`, win or lose.
pub(crate) struct AuditEntry {
    pub request_id: Option<String>,
    pub user_id: Option<Uuid>,
    pub account_id: Option<Uuid>,
    pub client_id: Option<String>,
    pub token_jti: Option<String>,
    pub tool: String,
    /// The scope this call was actually authorized under: `"read"` |
    /// `"write"`, or `None` when the call was denied before scope resolution
    /// could attribute one (shouldn't normally happen — auth always runs
    /// first — but kept optional defensively).
    pub scope_used: Option<&'static str>,
    pub graph_id: Option<Uuid>,
    pub target_id: Option<Uuid>,
    /// sha256 of the canonicalized call arguments (see [`args_digest`]) —
    /// irreversible, integrity/dedup signal only. `None` when the call never
    /// reached argument parsing (e.g. a pre-dispatch scope/rate-limit denial
    /// recorded before the tool router ever saw the arguments).
    pub args_digest: Option<String>,
    /// Ids/sizes/enums ONLY — never free-text argument content. Empty object
    /// when the caller has nothing worth summarizing.
    pub args_summary: Value,
    pub outcome: AuditOutcome,
    pub error_code: Option<&'static str>,
    pub duration_ms: u64,
    /// Raw request byte size (for the tracing line's `request_size` /ish
    /// signal) — never the bytes themselves.
    pub request_bytes: Option<usize>,
}

/// sha256 of the canonicalized (key-sorted, recursively) JSON arguments,
/// hex-encoded. Irreversible; lets an operator confirm "was this the exact
/// same call retried" without the arguments ever being stored or logged.
pub(crate) fn args_digest(args: &Value) -> String {
    let canonical = canonicalize(args);
    let bytes = serde_json::to_vec(&canonical).unwrap_or_default();
    let mut hasher = Sha256::new();
    hasher.update(&bytes);
    hex::encode(hasher.finalize())
}

/// Recursively sort object keys so semantically-identical argument sets
/// always hash identically regardless of client-supplied key order.
fn canonicalize(v: &Value) -> Value {
    match v {
        Value::Object(map) => {
            let mut sorted: std::collections::BTreeMap<String, Value> = std::collections::BTreeMap::new();
            for (k, val) in map {
                sorted.insert(k.clone(), canonicalize(val));
            }
            Value::Object(sorted.into_iter().collect())
        }
        Value::Array(arr) => Value::Array(arr.iter().map(canonicalize).collect()),
        other => other.clone(),
    }
}

/// Defensive ceiling on `AuditEntry.tool`, independent of and in addition to
/// `server::CoralSwarmMcp::call_tool`'s primary 64-char `invalid_params`
/// rejection (security review 2026-07-15) — this is the SECOND layer: even if
/// some future call site ever constructs an `AuditEntry` directly (bypassing
/// that dispatch-time check), an attacker-controlled tool name can never grow
/// the `mcp.tool_call` tracing line or the `mcp_tool_calls.tool` TEXT column
/// without bound. 128 (double the real 64-char tool-name bound) leaves room
/// to see what a truncated hostile name looked like without the column itself
/// becoming unbounded.
const MAX_AUDIT_TOOL_NAME_LEN: usize = 128;

/// Truncate `tool` to at most [`MAX_AUDIT_TOOL_NAME_LEN`] chars.
/// Char-boundary-safe (`chars().take` rather than a byte `String::truncate`,
/// which can panic mid-multi-byte-codepoint) — a hostile tool name is
/// arbitrary attacker-controlled UTF-8, never assumed ASCII.
fn truncate_tool_name(tool: String) -> String {
    if tool.chars().count() <= MAX_AUDIT_TOOL_NAME_LEN {
        tool
    } else {
        tool.chars().take(MAX_AUDIT_TOOL_NAME_LEN).collect()
    }
}

/// Emit the `mcp.tool_call` tracing line and (best-effort, non-blocking)
/// append a row to `mcp_tool_calls` on `pool`. Never fails the call this
/// describes.
///
/// `pool` is the CONTENT-plane pool the caller resolved for this request
/// (org-tenancy PR 2.3): the org's TENANT pool for an org-scoped call — the
/// audit row is content-plane provenance, and the tenant tree carries its own
/// `mcp_tool_calls` table (with no cross-plane FKs, unlike the consumer
/// table's `account_id` FK to `accounts`, which an org `account_id` would
/// still satisfy but whose row belongs with the org's content) — or the
/// consumer `state.db` pool for personal/flag-off calls (byte-identical to
/// the pre-2.3 behavior, where this always wrote to `state.db`). `None`
/// (tests with no Postgres) skips the row; the tracing line always
/// fires.
pub(crate) fn record(pool: Option<sqlx::PgPool>, entry: AuditEntry) {
    let entry = AuditEntry {
        tool: truncate_tool_name(entry.tool),
        ..entry
    };
    tracing::info!(
        target: "mcp.tool_call",
        request_id = entry.request_id.as_deref().unwrap_or(""),
        tool_name = %entry.tool,
        user_id = entry.user_id.map(|u| u.to_string()).unwrap_or_default(),
        account_id = entry.account_id.map(|a| a.to_string()).unwrap_or_default(),
        client_id = entry.client_id.as_deref().unwrap_or(""),
        scope_used = entry.scope_used.unwrap_or(""),
        duration_ms = entry.duration_ms,
        outcome = entry.outcome.as_str(),
        error_code = entry.error_code.unwrap_or(""),
        request_bytes = entry.request_bytes.unwrap_or(0),
        "mcp tool call"
    );

    let Some(pool) = pool else {
        return;
    };

    let tool = entry.tool.clone();
    let scope_used = entry.scope_used.map(str::to_string);
    let outcome = entry.outcome.as_str();
    let error_code = entry.error_code.map(str::to_string);
    let args_digest = entry.args_digest.clone();
    let args_summary = entry.args_summary.clone();
    let account_id = entry.account_id;
    let user_id = entry.user_id;
    let client_id = entry.client_id.clone();
    let token_jti = entry.token_jti.clone();
    let graph_id = entry.graph_id;
    let target_id = entry.target_id;
    let duration_ms = entry.duration_ms as i64;
    let request_id = entry.request_id.clone();

    tokio::spawn(async move {
        let insert = sqlx::query(
            "INSERT INTO mcp_tool_calls \
                (account_id, user_id, client_id, token_jti, tool, scope_used, \
                 graph_id, target_id, args_digest, args_summary, outcome, error_code, \
                 duration_ms, request_id) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)",
        )
        .bind(account_id)
        .bind(user_id)
        .bind(client_id)
        .bind(token_jti)
        .bind(&tool)
        .bind(scope_used)
        .bind(graph_id)
        .bind(target_id)
        .bind(args_digest)
        .bind(args_summary)
        .bind(outcome)
        .bind(error_code)
        .bind(duration_ms)
        .bind(request_id);

        // RLS scoping (org-tenancy conversion, pre-OPS-4; extended by tenant
        // RLS Phase 2): `mcp_tool_calls` carries an `account_id`-keyed policy
        // (migration 0083) on the CONSUMER plane, so an attributed row is
        // written inside a scoped transaction. On an org-scoped call `pool`
        // is the TENANT pool, where `migrations_tenant/0008_tenant_rls`
        // instead covers this table by NULLABLE `graph_id` (§6(c)) — a NULL
        // `graph_id` (e.g. a pre-auth denial with no resolved ocean) is hidden
        // regardless of any GUC, an accepted residual per that migration's own
        // header, same spirit as 0083's NULL-account rows here. When a
        // `graph_id` IS resolved, `app.user_id` is what admits it into a
        // member ocean's audit trail, so:
        //   - a real acting user (`user_id: Some`) uses `begin_tenant_scoped`;
        //   - an attributed ACCOUNT with no bound user (`user_id: None` — a
        //     scope-less/service token call) uses `begin_tenant_system_scoped`:
        //     there is no honest per-member identity to give an audit row that
        //     isn't acting as any one member, the same reasoning as the
        //     background workers (see `docs/tenant-rls-cutover-runbook.md`).
        //     Harmless on the consumer plane either way (neither GUC is read
        //     by 0083's policy).
        // An UNattributed row (`account_id = None` — pre-auth denials) keeps
        // the direct path: post-OPS-4 the strict policy refuses NULL-account
        // rows for the app role on the consumer plane, an accepted
        // fail-closed consequence per 0083's header (the tracing line above
        // always fires regardless).
        let res = match account_id {
            Some(acct) => {
                let scoped_tx = match user_id {
                    Some(uid) => crate::tenancy::scope::begin_tenant_scoped(&pool, acct, uid).await,
                    None => crate::tenancy::scope::begin_tenant_system_scoped(&pool, acct).await,
                };
                match scoped_tx {
                    Ok(mut tx) => match insert.execute(&mut *tx).await {
                        Ok(_) => tx.commit().await,
                        Err(e) => Err(e),
                    },
                    Err(e) => {
                        tracing::warn!(error = %e, tool = %tool, "mcp_tool_calls scoped tx failed (non-fatal)");
                        return;
                    }
                }
            }
            None => insert.execute(&pool).await.map(|_| ()),
        };
        if let Err(e) = res {
            tracing::warn!(error = %e, tool = %tool, "mcp_tool_calls insert failed (non-fatal)");
        }
    });
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn args_digest_is_stable_regardless_of_key_order() {
        let a = json!({"ocean_id": "x", "top_k": 20});
        let b = json!({"top_k": 20, "ocean_id": "x"});
        assert_eq!(args_digest(&a), args_digest(&b));
    }

    #[test]
    fn args_digest_differs_for_different_values() {
        let a = json!({"top_k": 20});
        let b = json!({"top_k": 21});
        assert_ne!(args_digest(&a), args_digest(&b));
    }

    #[test]
    fn outcome_as_str_matches_migration_value_set() {
        assert_eq!(AuditOutcome::Ok.as_str(), "ok");
        assert_eq!(AuditOutcome::Error.as_str(), "error");
        assert_eq!(AuditOutcome::DeniedScope.as_str(), "denied_scope");
        assert_eq!(AuditOutcome::DeniedTenancy.as_str(), "denied_tenancy");
        assert_eq!(AuditOutcome::RateLimited.as_str(), "rate_limited");
    }

    // ── truncate_tool_name (security review 2026-07-15, defense-in-depth
    // second layer beneath `server::CoralSwarmMcp::call_tool`'s primary
    // 64-char rejection) ───────────────────────────────────────────────────

    #[test]
    fn truncate_tool_name_leaves_short_names_untouched() {
        assert_eq!(truncate_tool_name("ask_ocean".to_string()), "ask_ocean");
    }

    #[test]
    fn truncate_tool_name_caps_an_oversized_name_at_128_chars() {
        let hostile = "x".repeat(10_000);
        let truncated = truncate_tool_name(hostile);
        assert_eq!(truncated.chars().count(), MAX_AUDIT_TOOL_NAME_LEN);
    }

    #[test]
    fn truncate_tool_name_is_char_boundary_safe_on_multibyte_input() {
        // Each "é" is a 2-byte UTF-8 codepoint — a naive byte-index
        // `String::truncate` at an odd boundary would panic here.
        let hostile = "é".repeat(10_000);
        let truncated = truncate_tool_name(hostile);
        assert_eq!(truncated.chars().count(), MAX_AUDIT_TOOL_NAME_LEN);
    }
}

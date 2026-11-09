//! The 4 write-scope MCP tools (`docs/plans/remote-mcp-v1.md` §3.2, §5 Phase 3).
//!
//! Every tool body re-derives authorization from the verified
//! [`McpAuthContext`] extracted via `super::auth` — never from caller-supplied
//! ids alone (§4.5 multi-tenancy rule) — and never re-implements a
//! guarded-UPDATE the routes already own: `add_context` calls
//! `ingest::submit`; `confirm_reef`/`reject_reef`/
//! `update_reef` call the `confirm_node_impl`/`reject_node_impl`/
//! `patch_node_impl` extractions in `routes::nodes` (precedent:
//! `ingest::submit`'s own doc comment, which endorses exactly this "shared
//! by the HTTP handler and other callers" shape).
//!
//! Write-scope enforcement itself (whether a caller may invoke ANY of these
//! four at all) lives one layer up, in `server::CoralSwarmMcp::list_tools` /
//! `call_tool` (`WRITE_TOOLS`, plan §4.2) — nothing in this file re-checks
//! `auth.scopes.write`.
//!
//! Annotations (plan §3.1): all four are `readOnlyHint: false`.
//! `destructiveHint` defaults to `true` once `readOnlyHint` is `false`, so
//! `add_context`/`confirm_reef` set it explicitly `false` (net-new data /
//! reversible-in-practice state flip); `reject_reef`/`update_reef` leave it
//! `true` (no undo surface exposed in v1 — `update_reef`'s before/after echo
//! is the recovery affordance). `idempotentHint`: `add_context` is `false`
//! (repeat calls without an explicit `source_id` mint a new coral each time);
//! the other three are `true` (repeat confirm/reject/patch of the same
//! target with the same args converges on the same state — confirm/reject
//! even converge on an explicit `not_found` on the second call, which is
//! itself idempotent behavior per plan §3.2).
//!
//! # Consumer-plane RLS scoping (org-tenancy OPS-4, MCP surface)
//! `confirm_reef`/`reject_reef`/`update_reef` each run every RLS-covered
//! query (migration `0083_consumer_rls`) inside ONE
//! [`crate::tenancy::scope::begin_account_scoped`] transaction PER TOOL CALL,
//! committed only on success, so the tools keep working — and keep their
//! all-or-nothing write semantics — once OPS-4 switches the app's connection
//! role to the non-`BYPASSRLS` `coralswarm_app`. The shared half of the seam
//! (`ScopedTx` / `scoped_store!` / `commit_write_tx`) lives in [`super`]; the
//! transaction OPEN is the file-local `begin_scoped_tx!` below (see that
//! macro's doc for why it is not shared). The guarded-UPDATE bodies
//! themselves come from `routes::nodes`'
//! `*_impl_scoped` twins, which phase B.2 introduced FOR this conversion (see
//! `patch_node_impl`'s own doc comment there).
//!
//! All three are reef-id-anchored, so all three scope by the CALLER'S ACTIVE
//! account — which is also exactly the account each one's guarded UPDATE
//! already filters by (`node_owned_by_account` / `set_node_review_status`'s
//! `g.account_id = $2` join), so RLS agrees with the query's own predicate
//! rather than narrowing it. This is STRICTER than reachability by design
//! (plan §3.2 #14: "active-account authorization … keep parity") and
//! unchanged by this conversion.
//!
//! `add_context` opens no transaction of its own: it delegates wholesale to
//! `ingest::submit`, which was converted in phase B.1 and
//! opens its own per-request scoped transaction internally. Its one direct
//! query, `caller_display_name`, reads `users` — a control-plane table that
//! is not in 0083's roster — and so is deliberately unscoped.
//!
//! As in `tools::read`, with no pool (`tenant.pool == None`) opening the
//! scoped transaction fails and the tool returns a `service_unavailable`
//! error; there is no unscoped fallback.

use axum::response::IntoResponse as _;
use chrono::{DateTime, Utc};
use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::CallToolResult;
use rmcp::service::RequestContext;
use rmcp::{tool, tool_router, ErrorData as McpError, RoleServer};
use schemars::JsonSchema;
use serde::Deserialize;
use serde_json::json;
use uuid::Uuid;

use crate::ingest::atom::{
    add_context_filing, validate_provenance_label, IngestAtomRequest,
    McpClientInfo, ProvenanceLabelError, SessionKind, PROVENANCE_LABEL_CAP,
};
use crate::routes::nodes::{
    confirm_node_impl, confirm_node_impl_scoped, node_writable_by, patch_node_impl,
    patch_node_with_identity_impl_scoped, ReefWriter,
    reject_node_impl, reject_node_impl_scoped, ConfirmRequest, PatchNodeError, RejectRequest,
    ReviewError,
};
use crate::store::Node;
use crate::AppState;

use super::super::server::{model_only_meta, CoralSwarmMcp};
use super::{auth, commit_write_tx, scoped_store, tool_error, tool_success};

/// Open this tool call's account-scoped transaction
/// ([`crate::tenancy::scope::begin_account_scoped`] via
/// [`crate::tenancy::extractor::Tenant`]), or early-return a ready-made
/// `service_unavailable` tool error. Byte-for-byte the same macro
/// `tools::read` carries — file-local in both for the reason recorded above
/// [`super::ScopedTx`] (the coverage gate greps each converted file for a
/// literal `begin_account_scoped` call).
macro_rules! begin_scoped_tx {
    ($tenant:expr, $account_id:expr, $log_context:literal) => {
        {
            match $tenant.begin_member_scoped($account_id).await {
                Ok(tx) => Some(tx),
                Err(e) => {
                    tracing::error!(
                        error = %e,
                        tool = $log_context,
                        "failed to open account-scoped transaction"
                    );
                    return Ok(tool_error("service_unavailable", "internal error"));
                }
            }
        }
    };
}

// ── Caps (plan §3.2 #11-14; the HTTP routes/`ingest::submit` have none of
// these — they exist ONLY at this MCP boundary to cap the blast radius of an
// untrusted, prompt-injectable caller) ────────────────────────────────────────

/// `add_context.content` cap, in chars.
const ADD_CONTEXT_CONTENT_CAP: usize = 100_000;
/// `add_context.source_id` cap, in chars (when the caller supplies one).
const ADD_CONTEXT_SOURCE_ID_CAP: usize = 512;
/// `add_context.participants` cap: max entries.
const ADD_CONTEXT_MAX_PARTICIPANTS: usize = 100;
/// `add_context.participants` cap: max chars per entry.
const ADD_CONTEXT_PARTICIPANT_CAP: usize = 256;
/// `add_context.session_id` / `add_context.checkpoint_id` cap, in chars
/// (coding-session capture Phase 1) — same cap for both since they're the
/// same kind of thing (an opaque caller-chosen identifier folded into the
/// `"cc:{session_id}:{checkpoint_id}"` `source_id` convention).
const ADD_CONTEXT_SESSION_ID_CAP: usize = 512;
/// `confirm_reef.rationale` / `reject_reef.reason` cap, in bytes (~2 KB).
const REVIEW_TEXT_CAP: usize = 2 * 1024;
/// `confirm_reef.title` cap, in chars.
const CONFIRM_TITLE_CAP: usize = 512;

/// `add_context.source_type` allowlist. NOT the same list `ingest::submit`
/// itself enforces (that route accepts any non-empty string) — this is an
/// MCP-only narrowing (plan §3.2 #11 "allowlisted incl. new `mcp`") chosen
/// conservatively to keep LLM-driven writes from spoofing a provenance-looking
/// source type (e.g. claiming `"slack"` for content that never came from a
/// Slack integration). `"mcp"` is the recommended default for anything that
/// doesn't fit an existing category. Not exhaustive of every source type the
/// product recognizes elsewhere (see `ChunkStrategy::for_source_type`) — just
/// the ones a legitimate `add_context` call plausibly wants; extend here if a
/// real use case needs another.
///
/// Deliberately EXCLUDES every SESSION source type — e.g. `"coding_session"`
/// ([`crate::ingest::atom::CODING_SESSION_SOURCE_TYPE`]) — by the same anti-spoofing logic that motivates this list. Those are
/// derived from a `session_id` plus a declared `session_kind`, never chosen:
/// see [`crate::ingest::atom::SessionKind`]. A caller who sends `session_id`
/// gets the derived label regardless of what they put here, and a caller who
/// doesn't cannot claim it.
//
// GitHub connector PR A (docs/connectors/github.md §3.5) — this is the
// fourth "switch site" the connector doc names, but registration here means
// the OPPOSITE of the other three (`ChunkStrategy::for_source_type`,
// `services::atom_title::pretty_source_type`, `store::
// journal_block_title_fallback`): those three make a source type RENDERABLE;
// ADD_CONTEXT_SOURCE_TYPES makes one CLAIMABLE by an arbitrary MCP caller.
// GitHub content is produced exclusively by the real ingest pipeline
// (installation-token-backed REST reads, PR B) — never by an LLM-driven
// `add_context` call — so the six §3.5 types are deliberately ABSENT from
// this allowlist, same anti-spoofing posture the doc comment above already
// states for `"slack"` (also absent, also illustrated above as the exact
// kind of claim this list exists to prevent). Un-listed here is the correct,
// tested outcome — see
// `add_context_rejects_every_github_source_type_as_a_caller_chosen_label`.
const ADD_CONTEXT_SOURCE_TYPES: &[&str] =
    &["note", "mcp", "email", "chat", "document", "meeting", "transcript"];

// ── Input schemas ─────────────────────────────────────────────────────────────

/// `promote`'s input. Mirrors [`crate::routes::nodes::PromoteRequest`] with
/// MCP-facing names (`ocean` rather than `graph`, matching every other tool in
/// this surface).
#[derive(Debug, Deserialize, JsonSchema)]
struct PromoteInput {
    /// The reef to publish, living in one of your own private oceans.
    reef_id: Uuid,
    /// The shared (team/org) ocean to publish it into.
    target_ocean_id: Uuid,
    /// Corals to copy alongside the reef. Omit to copy every coral attached to
    /// the reef; pass an empty list to promote the reef alone; pass ids to copy
    /// exactly those. Every id must already be attached to the source reef or
    /// the whole request is rejected before anything is promoted.
    #[serde(default)]
    coral_ids: Option<Vec<Uuid>>,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct AddContextInput {
    /// The ocean to ingest into. Must be one you can write to — your own
    /// private ocean (`writable: true` in `list_oceans`). A shared org ocean is
    /// never a legal target; publish into one with `promote` instead.
    ///
    /// Omit to default to your own primary ocean, which in an org context is
    /// your member-private ocean rather than the org commons (see
    /// `read::resolve_primary_ocean`). Omitting is the recommended usage: the
    /// commons is the id callers most often picked by hand, and it is the one
    /// id that can never succeed.
    #[serde(default)]
    ocean_id: Option<Uuid>,
    /// Text to chunk, embed, and extract entities/decisions from (max 100,000
    /// chars).
    content: String,
    /// Provenance label, default "note". One of: note, mcp, email, chat,
    /// document, meeting, transcript. Ignored entirely when `session_id` is
    /// supplied — see that field.
    #[serde(default)]
    source_type: Option<String>,
    /// Provider-native id, unique within (ocean, source_type) (1-512 chars
    /// when supplied). Omit to have the server generate one (`mcp:{uuid}`) —
    /// the response echoes whichever was used, enabling idempotent re-ingest
    /// by re-supplying it.
    #[serde(default)]
    source_id: Option<String>,
    /// Names/emails of participants (max 100, each max 256 chars). Omit this
    /// field entirely to have the server default it to the authenticated
    /// caller's own display name (falling back to email if no display name
    /// is on file); an explicitly supplied list (including an empty one) is
    /// always used verbatim.
    #[serde(default)]
    participants: Option<Vec<String>>,
    /// Original timestamp of the content (RFC 3339), default now.
    #[serde(default)]
    timestamp: Option<DateTime<Utc>>,
    /// Visibility scope: "private" (default) or "account".
    #[serde(default)]
    scope: Option<String>,
    /// Optional project/repo label (max 256 chars), e.g. "coralswarm" or
    /// "acme/backend" — lets a single personal ocean fed by several unrelated
    /// local projects distinguish which project a coral came from. Stamped
    /// onto `atoms.project` (migration 0072); omit for context with no
    /// specific project association.
    #[serde(default)]
    project: Option<String>,
    /// Coding-agent session identifier (e.g. a Claude Code session uuid),
    /// coding-session capture Phase 1. When present, this call is treated as
    /// a checkpoint save within that session: a `coding_sessions` row is
    /// lazily upserted (bumping its `last_activity_at`) and the resulting
    /// `source_id` becomes `"cc:{session_id}:{checkpoint_id}"` — `source_id`
    /// above is ignored when `session_id` is supplied (max 512 chars).
    /// `source_type` is likewise ignored and derived from `session_kind`
    /// below: carrying a `session_id` is what MAKES a coral a session coral,
    /// so the two can never disagree.
    #[serde(default)]
    session_id: Option<String>,
    /// Which kind of session this is: "coding" (default), "meeting" or
    /// "agent". Only meaningful alongside session_id. Determines the
    /// coral's source_type ("coding_session" / "meeting_session" /
    /// "agent_session"), which you cannot set directly.
    /// "coding" represents an interactive coding session; "agent" represents
    /// an explicitly enabled agent capture workflow. For a session checkpoint
    /// explicitly declared "agent", platform is required unless the MCP client
    /// or User-Agent identifies it. "meeting" represents a meeting session.
    ///
    /// When omitted entirely (not sent), the server INFERS "agent" instead of
    /// defaulting straight to "coding": a non-empty `agent_name`, a
    /// `parent_session_id`, or an MCP client whose clientInfo names a known
    /// agent framework (e.g. `openclaw`) are each enough on their own.
    /// Everything else infers "coding". A value you DO send always wins over
    /// inference, in both directions — declaring "coding" alongside
    /// `agent_name` stays "coding". An inferred "agent" that ends up with no
    /// platform declared or detected anywhere is filed as "coding" instead of
    /// being rejected — the `platform`-required rule below only ever rejects
    /// a session_kind you explicitly asked for.
    #[serde(default)]
    session_kind: Option<String>,
    /// What you call yourself as an agent (`grokbot`, `pr-reviewer`,
    /// `cs-worker-3`; max 256 chars). Free text, refinable on later
    /// checkpoints. When `parent_session_id` is set this is recorded as your
    /// `subagent_name`; the effective `agent_name` is inherited from the root
    /// agent and cannot be overridden by a subagent.
    ///
    /// Naming your agent is what makes this an agent session: with no
    /// `session_kind` sent at all, a non-empty `agent_name` alone infers
    /// "agent" (see `session_kind` above).
    #[serde(default)]
    agent_name: Option<String>,
    /// Where you run: one of claude-code, cursor, codex, grok, openai,
    /// gemini-cli, copilot, windsurf, opencode, hermes, pi, openclaw — or
    /// any other value, which is kept as a normalized slug (spellings like
    /// "Claude Code" / "cursor-cloud" are canonicalized server-side).
    /// Optional — omit it and the platform is otherwise detected from your
    /// MCP client's own `clientInfo` (then, failing that, your User-Agent),
    /// so a compliant MCP client is identified with no parameter at all.
    /// REQUIRED only when session_kind is "agent" AND neither of those
    /// detected anything; the call is rejected otherwise. Optional for the
    /// other kinds regardless. Subagents inherit the root agent's platform.
    #[serde(default)]
    platform: Option<String>,
    /// What you are working on — free text or a story id such as `CS-042`
    /// (max 256 chars). Refinable on later checkpoints. A subagent may
    /// declare its own slice of the work; it inherits the root's task when
    /// omitted.
    #[serde(default)]
    task: Option<String>,
    /// The `session_id` of the agent session that spawned you, when you are a
    /// subagent. Fixed at your first checkpoint: the server rolls your
    /// session up to that agent's root (`root_session_id`) and copies its
    /// `agent_name` / `platform` onto yours, so filters on the root agent
    /// match every subagent's corals too. The parent must already exist in
    /// the same ocean and be an "agent" session; the call is rejected
    /// otherwise. The identifier is supplied by the explicitly enabled capture
    /// workflow; it is not inferred from conversation text.
    #[serde(default)]
    parent_session_id: Option<String>,
    /// Caller-supplied checkpoint identifier within the session. Omit to
    /// have the server generate one (a fresh UUID per call) — only meaningful
    /// alongside `session_id`; ignored otherwise (max 512 chars).
    #[serde(default)]
    checkpoint_id: Option<String>,
    /// Human-readable session title. Best-effort; a later call may refine an
    /// earlier omitted/generic title (max 256 chars, truncated if longer).
    #[serde(default)]
    session_title: Option<String>,
    /// Working-directory path on the caller's machine at capture time (max
    /// 256 chars, truncated if longer).
    #[serde(default)]
    repo_path: Option<String>,
    /// RAW git remote URL as reported by the caller (e.g.
    /// `"https://github.com/org/repo.git"` or `"git@github.com:org/repo.git"`).
    /// ALWAYS re-normalized server-side — credentials embedded in the URL
    /// (e.g. `https://user:token@host/...`) are unconditionally stripped
    /// before anything is stored, regardless of what the caller sends.
    #[serde(default)]
    remote: Option<String>,
    /// Caller machine hostname (max 256 chars, truncated if longer).
    #[serde(default)]
    hostname: Option<String>,
    /// Coding-agent harness version string (max 256 chars, truncated if
    /// longer).
    #[serde(default)]
    harness_version: Option<String>,
    /// Model identifier the harness was using at capture time (max 256
    /// chars, truncated if longer).
    #[serde(default)]
    model: Option<String>,
    /// Current git branch at capture time (max 256 chars, truncated if
    /// longer). Last reported value is stored on the session row; each
    /// checkpoint also rides the embedded contextual chunk header.
    #[serde(default)]
    branch: Option<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct ConfirmReefInput {
    /// The pending decision reef to confirm.
    reef_id: Uuid,
    /// Optional title edit applied on confirm (max 512 chars).
    #[serde(default)]
    title: Option<String>,
    /// Optional rationale edit applied on confirm (max ~2 KB).
    #[serde(default)]
    rationale: Option<String>,
    /// Optional decided_at edit (RFC 3339) applied on confirm.
    #[serde(default)]
    decided_at: Option<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct RejectReefInput {
    /// The pending decision reef to reject.
    reef_id: Uuid,
    /// Optional reason recorded on reject (max ~2 KB), e.g. "wrong",
    /// "not_a_decision", "duplicate".
    #[serde(default)]
    reason: Option<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct UpdateReefInput {
    /// The reef to update.
    reef_id: Uuid,
    /// New title. At least one of title/type must be provided.
    #[serde(default)]
    title: Option<String>,
    /// New type: entity, thread, or reference (not "decision" — use
    /// confirm_reef for that). At least one of title/type must be provided.
    #[serde(default, rename = "type")]
    reef_type: Option<String>,
    /// Optimistic-concurrency guard: if supplied, the update is rejected
    /// (invalid_params) when the reef's current version doesn't match — read
    /// it back off a prior update_reef call's before/after `version`. Omit to
    /// keep last-writer-wins behavior.
    #[serde(default)]
    expected_version: Option<i64>,
    /// Existing member that independently supports this explicit human name.
    /// Omit for an ordinary metadata-only update.
    #[serde(default)]
    support_atom_id: Option<Uuid>,
}

// ── Shared helpers ────────────────────────────────────────────────────────────

/// `{title, type, scope, atom_count, updated_at, version}` — the
/// `before`/`after` snapshot shape `update_reef` echoes (plan §3.2 #14).
/// `version` is looked up separately (`Node` itself doesn't carry it) so a
/// caller can read it back and pass it as `expected_version` on a later call.
fn node_snapshot(n: &Node, version: Option<i64>) -> serde_json::Value {
    json!({
        "title": n.title,
        "type": n.node_type,
        "scope": n.scope,
        "atom_count": n.atom_count,
        "updated_at": n.updated_at,
        "version": version,
    })
}

fn patch_error_to_tool_error(e: PatchNodeError) -> CallToolResult {
    match e {
        PatchNodeError::NoFields => {
            tool_error("invalid_params", "at least one of title or type must be provided")
        }
        PatchNodeError::InvalidType => tool_error(
            "invalid_params",
            "type must be one of: entity, decision, thread, reference",
        ),
        // IDOR non-leak convention (plan §3 conventions): ownership failure
        // and "the update itself found nothing" both surface identically —
        // never a distinct "forbidden" code that would confirm the reef
        // exists under another tenant.
        PatchNodeError::NotOwned | PatchNodeError::NotFound => {
            tool_error("not_found", "reef not found or access denied")
        }
        PatchNodeError::EmptyTitle => tool_error("invalid_params", "title must not be empty"),
        PatchNodeError::TitleTooLong => tool_error(
            "invalid_params",
            format!(
                "title must be at most {} chars",
                crate::routes::nodes::PATCH_TITLE_CAP
            ),
        ),
        PatchNodeError::DecisionTypeNotAllowed => tool_error(
            "invalid_params",
            "changing a reef's type to 'decision' isn't supported via update_reef — use \
             confirm_reef instead",
        ),
        PatchNodeError::Conflict(msg) => tool_error("invalid_params", msg),
        PatchNodeError::Store(e) => {
            tracing::error!(error = %e, "update_reef store error");
            tool_error("service_unavailable", "internal error")
        }
    }
}

fn review_error_to_tool_error(e: ReviewError) -> CallToolResult {
    match e {
        ReviewError::NotFound => {
            tool_error("not_found", "no pending decision candidate with that id")
        }
        ReviewError::Store(e) => {
            tracing::error!(error = %e, "reef review store error");
            tool_error("service_unavailable", "internal error")
        }
    }
}

/// Filing + hint inputs for a successful `add_context` — computed from the
/// same signals `ingest::submit` uses, then echoed so the caller can see
/// what actually landed (a note vs a session) without inspecting `source_id`
/// prefixes.
struct AddContextSuccessMeta {
    session_id: Option<String>,
    filed_as: String,
    session_kind: Option<&'static str>,
    platform_detected: Option<String>,
}

/// Machine-readable nudge when a save landed as a note because `session_id`
/// was omitted. The write still succeeds — the coral is worth keeping — but
/// this is the moment an unidentified agent actually listens.
fn unattributed_save_hint(platform_detected: Option<&str>) -> serde_json::Value {
    json!({
        "code": "unattributed_save",
        "message": "Saved without session attribution. If you are an autonomous agent, adopt \
                    recipe.session_id and pass session_kind=agent, agent_name, and platform \
                    on the next milestone. Ignore this if the save intentionally has no session.",
        "recipe": {
            "session_id": Uuid::new_v4().to_string(),
            "session_kind": "agent",
            "agent_name": serde_json::Value::Null,
            "platform": platform_detected,
            "checkpoint_id": serde_json::Value::Null,
        },
    })
}

/// Translate `ingest::submit`'s `axum::Response` into a tool result.
/// `ingest::submit` is a plain HTTP-handler-shaped function (unlike
/// `services::transcripts::get_transcript`, which Phase 2 gave a typed
/// `Result` return) — the plan names it directly as
/// `add_context`'s reuse target (§3.2 #11), so rather than fork a second,
/// typed copy of its ~250 lines of guarded logic, this parses the ONE
/// response shape it already produces (JSON body + status code) back into the
/// MCP error-code convention. `10 MiB` here is just a generous decode ceiling
/// for `to_bytes`, not a real limit — the actual request already passed
/// through the `/mcp` router's 2 MiB `DefaultBodyLimit` before this ever runs,
/// and the response body itself is always small JSON.
async fn ingest_response_to_tool_result(
    resp: axum::response::Response,
    ocean_id: Uuid,
    source_id: String,
    client_name: Option<&str>,
    echo: AddContextSuccessMeta,
) -> CallToolResult {
    use axum::http::StatusCode;

    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), 10 * 1024 * 1024)
        .await
        .unwrap_or_default();
    let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap_or_else(|_| json!({}));

    if status == StatusCode::OK {
        let hint = if echo.session_id.is_none() {
            Some(unattributed_save_hint(echo.platform_detected.as_deref()))
        } else {
            None
        };
        return tool_success(
            json!({
                "atom_ids": body.get("atom_ids").cloned().unwrap_or_else(|| json!([])),
                "chunks_created": body.get("chunks_created").cloned().unwrap_or_else(|| json!(0)),
                "jobs_enqueued": body.get("jobs_enqueued").cloned().unwrap_or_else(|| json!(0)),
                // How many chunks at an already-used position were skipped as
                // duplicates (checked+fixed items #5) — makes a re-supplied
                // `source_id` with edited content self-explanatory instead of
                // silently discarding the edit.
                "skipped_duplicate_chunks": body.get("skipped_duplicate_chunks").cloned().unwrap_or_else(|| json!(0)),
                "ocean_id": ocean_id,
                "source_id": source_id,
                "filed_as": echo.filed_as,
                "session_kind": echo.session_kind,
                "session_id": echo.session_id,
                "hint": hint,
            }),
            client_name,
        );
    }

    // Mirror the HTTP route's exact message (plan §3.2 #11) rather than
    // re-wording it — this is what makes the own-private-ocean-only rejection
    // ("can only ingest into your own private ocean; share via promotion",
    // widened by member-private-oceans PR 5 to also admit `kind='member'`)
    // visible to the MCP caller verbatim.
    let message = body
        .get("error")
        .and_then(|v| v.as_str())
        .unwrap_or("ingest failed")
        .to_string();

    let code = match status {
        StatusCode::BAD_REQUEST | StatusCode::UNPROCESSABLE_ENTITY => "invalid_params",
        // 403/404 from the route both mean "not reachable/owned" — IDOR
        // non-leak convention, collapse to not_found.
        StatusCode::FORBIDDEN | StatusCode::NOT_FOUND => "not_found",
        // 409 is the `session_kind` conflict (capture blocker B1): the
        // caller reused one `(ocean, session_id)` for two kinds of session.
        // It MUST NOT fall through to `service_unavailable` below — that
        // code reads as transient and tells a model to retry, and this
        // failure is permanent by construction (a session's kind is fixed at
        // its first checkpoint and nothing later can move it, so the
        // identical call fails identically forever). `invalid_params` is the
        // closest member of this file's deliberately tiny code vocabulary
        // that means "your request was wrong; change it, don't repeat it" —
        // and the message carried through from the route already names the
        // stored kind, which is the one fact the caller needs to fix it.
        StatusCode::CONFLICT => "invalid_params",
        _ => "service_unavailable",
    };
    tool_error(code, message)
}

/// Human-readable label for the authenticated caller, for stamping
/// `atoms.author_name` and for defaulting `participants` when the caller
/// omits it entirely — without this, MCP-ingested atoms recorded WHO via
/// `atoms.ingested_by` only, and every read path (ocean UI, `ask_ocean`
/// citations) that derives a person label reads `author_name`, so those
/// atoms showed no attribution at all.
///
/// Prefers `users.name` (migration 0067 — populated from Clerk's OIDC `name`
/// claim / Backend API by `clerk_provision::ensure_user` /
/// `sync_user_display_name`); falls back to `users.email` when `name` is
/// `NULL` (unset, or a token/identity that never surfaced one — mirrors
/// `self_context::build_identity` and `routes::escrow::post_recovery_log`'s
/// own email-as-identity lookups, which predate the `name` column). A single
/// `COALESCE` query rather than two round-trips.
///
/// `None` when there's no Postgres pool (shouldn't happen for a real `/mcp`
/// request — `mcp_auth` itself requires one to resolve identity at all) or on any
/// DB error; fails soft (`author_id` alone still lands) rather than ever
/// blocking the ingest.
async fn caller_display_name(state: &AppState, user_id: Uuid) -> Option<String> {
    let pool = state.db.as_ref()?;
    match sqlx::query_scalar::<_, Option<String>>("SELECT COALESCE(name, email) FROM users WHERE id = $1")
        .bind(user_id)
        .fetch_optional(pool)
        .await
    {
        Ok(label) => label.flatten(),
        Err(e) => {
            tracing::warn!(error = %e, user_id = %user_id, "add_context caller display-name lookup failed");
            None
        }
    }
}

// ── Tools ─────────────────────────────────────────────────────────────────────

#[tool_router(router = write_tool_router, vis = "pub(crate)")]
impl CoralSwarmMcp {
    /// 11. `add_context` — "Add context".
    #[tool(
        title = "Add context",
        description = "Save specific text that the user selected or explicitly identified for \
            saving as a private note. A broad request to capture a conversation or future \
            messages does not identify a note: ask which specific snippet or summary to save \
            before calling this tool. Conversation-wide capture, background recording, and \
            automatic future synchronization are not capabilities of this tool. \
            Selected content must exclude secrets, tokens, and credentials; obtain a \
            redacted selection before saving sensitive text. \
            \
            content is the only required input for a standalone note; it contains exactly \
            the identified text. Session attribution is optional and belongs to an explicitly \
            enabled coding or agent capture workflow. session_id groups a checkpoint under a \
            session, and session_kind controls its type; without session_id the content is a \
            note. checkpoint_id is generated when omitted. An explicitly declared agent \
            checkpoint requires platform when MCP client identity cannot identify it; other \
            provenance follows the documented input conditions. \
            \
            Writes only to the authenticated caller's own writable private ocean. Omitting \
            ocean_id uses that caller's primary ocean, including their member-private ocean \
            in an organization. A shared organization ocean is populated through promote. \
            list_oceans reports writable targets. The save triggers embedding and entity \
            extraction, is attributed to the authenticated caller, and defaults participants \
            to that caller when omitted. Refused for a token that requested only coralswarm:read (a \
            token carrying product scopes needs coralswarm:write); writes do \
            not authorize saving unspecified content.",
        annotations(read_only_hint = false, destructive_hint = false, idempotent_hint = false, open_world_hint = false),
        meta = model_only_meta()
    )]
    async fn add_context(
        &self,
        ctx: RequestContext<RoleServer>,
        Parameters(input): Parameters<AddContextInput>,
    ) -> Result<CallToolResult, McpError> {
        let a = auth(&ctx)?;

        if input.content.chars().count() > ADD_CONTEXT_CONTENT_CAP {
            return Ok(tool_error(
                "invalid_params",
                format!("content must be at most {ADD_CONTEXT_CONTENT_CAP} chars"),
            ));
        }

        let source_type = input.source_type.unwrap_or_else(|| "note".to_string());
        if !ADD_CONTEXT_SOURCE_TYPES.contains(&source_type.as_str()) {
            return Ok(tool_error(
                "invalid_params",
                format!(
                    "source_type must be one of: {}",
                    ADD_CONTEXT_SOURCE_TYPES.join(", ")
                ),
            ));
        }

        // `source_id` is IGNORED when `session_id` is supplied (the
        // "cc:{session_id}:{checkpoint_id}" convention takes over below), so
        // it's only validated in the plain-source_id path.
        if input.session_id.is_none() {
            if let Some(sid) = &input.source_id {
                if sid.is_empty() || sid.chars().count() > ADD_CONTEXT_SOURCE_ID_CAP {
                    return Ok(tool_error(
                        "invalid_params",
                        format!("source_id must be 1-{ADD_CONTEXT_SOURCE_ID_CAP} chars"),
                    ));
                }
            }
        }

        // ── Coding-session capture Phase 1 (audit A2): `session_id` and
        // `checkpoint_id` are IDENTIFIERS, not free-text machine labels — an
        // over-cap or empty value is rejected here rather than silently
        // truncated (truncating an identifier risks merging two distinct
        // sessions, or splitting one caller's session across two rows). The
        // OTHER coding-session fields (title/repo_path/remote/hostname/
        // harness_version/model/branch) are free-text provenance and are
        // truncated, never rejected, by `ingest::submit` itself
        // (audit A10) — this tool passes them through unvalidated.
        if let Some(sid) = &input.session_id {
            if sid.is_empty() || sid.chars().count() > ADD_CONTEXT_SESSION_ID_CAP {
                return Ok(tool_error(
                    "invalid_params",
                    format!("session_id must be 1-{ADD_CONTEXT_SESSION_ID_CAP} chars"),
                ));
            }
        }
        if let Some(cid) = &input.checkpoint_id {
            if cid.is_empty() || cid.chars().count() > ADD_CONTEXT_SESSION_ID_CAP {
                return Ok(tool_error(
                    "invalid_params",
                    format!("checkpoint_id must be 1-{ADD_CONTEXT_SESSION_ID_CAP} chars"),
                ));
            }
        }
        // `session_kind` is re-validated by `ingest::submit` — the single
        // choke point that actually derives `source_type` from it, and where
        // the invariant lives. Checking it here too is not redundancy for its
        // own sake: this boundary owes an untrusted, prompt-injectable caller
        // an `invalid_params` tool error naming the allowed values, rather
        // than the raw 422 body of an internal route it never sees.
        if let Some(kind) = &input.session_kind {
            if SessionKind::parse(kind).is_none() {
                return Ok(tool_error(
                    "invalid_params",
                    format!("session_kind must be one of: {}", SessionKind::allowed_values()),
                ));
            }
        }

        // Shared with the REST `/v1/ingest/atom` route (`routes::ingest`) so
        // both entry points that accept a `project` label enforce the exact
        // same cap AND reject an embedded NUL byte the same way — Postgres
        // `TEXT` columns refuse a NUL outright, so without this an
        // otherwise-valid-looking `project` would fail the ingest with an
        // opaque DB error instead of a clean `invalid_params` (CodeRabbit
        // review).
        if let Err(e) = validate_provenance_label(&input.project) {
            let message = match e {
                ProvenanceLabelError::ContainsNul => "project must not contain NUL bytes".to_string(),
                ProvenanceLabelError::TooLong => {
                    format!("project must be at most {PROVENANCE_LABEL_CAP} chars")
                }
            };
            return Ok(tool_error("invalid_params", message));
        }

        // `None` (field omitted entirely) is later defaulted to the caller's
        // own identity below; `Some(_)` — including an explicitly empty list
        // — is caller-supplied and used verbatim, so caps only apply here.
        let participants_omitted = input.participants.is_none();
        let mut participants = input.participants.unwrap_or_default();
        if !participants_omitted {
            if participants.len() > ADD_CONTEXT_MAX_PARTICIPANTS {
                return Ok(tool_error(
                    "invalid_params",
                    format!("participants must have at most {ADD_CONTEXT_MAX_PARTICIPANTS} entries"),
                ));
            }
            if participants
                .iter()
                .any(|p| p.chars().count() > ADD_CONTEXT_PARTICIPANT_CAP)
            {
                return Ok(tool_error(
                    "invalid_params",
                    format!("each participant must be at most {ADD_CONTEXT_PARTICIPANT_CAP} chars"),
                ));
            }
        }

        // Strip NUL bytes — Postgres TEXT columns reject them outright, and an
        // untrusted caller has no other reason to send one.
        let content = input.content.replace('\0', "");

        // Echo whichever source_id was actually used (plan §3.2 #11):
        // generated ids let a caller who DIDN'T supply one still re-ingest
        // idempotently by supplying it back on a retry.
        //
        // Coding-session capture Phase 1 (audit A2): when `session_id` is
        // supplied, the `source_id` convention takes over entirely —
        // `"cc:{session_id}:{checkpoint_id}"` — regardless of whatever
        // `input.source_id` may also have been set to (validated as ignored,
        // above). `ingest::submit`'s existing `"{source_id}:chunk:{n}"`
        // chunk-suffixing mechanism (routes/ingest.rs) then applies exactly
        // as it does for any other source, with no changes needed there.
        let source_id = if let Some(session_id) = &input.session_id {
            let checkpoint_id = input
                .checkpoint_id
                .clone()
                .unwrap_or_else(|| Uuid::new_v4().to_string());
            format!("cc:{session_id}:{checkpoint_id}")
        } else {
            input
                .source_id
                .clone()
                .unwrap_or_else(|| format!("mcp:{}", Uuid::new_v4()))
        };

        // Attribute the coral to the authenticated caller (never a
        // caller-supplied identity — `a.user` is re-derived from the
        // verified token, same multi-tenancy rule as everything else in this
        // file). `author_id` is the stable user id; `author_name` is the best
        // human label available (email — see `caller_display_name`), `None`
        // only when it genuinely couldn't be resolved.
        let author_id = Some(a.user.user_id.to_string());
        let caller_label = caller_display_name(&self.state, a.user.user_id).await;
        if participants_omitted {
            if let Some(label) = &caller_label {
                participants.push(label.clone());
            }
        }

        // ── Write destination ────────────────────────────────────────────────
        // An omitted `ocean_id` defaults to the caller's own primary ocean —
        // in an org context their MEMBER-PRIVATE ocean, not the org commons
        // (`read::resolve_primary_ocean`, hardened for exactly this by
        // member-private-oceans PR #344).
        //
        // This DEFAULTS an id; it never AUTHORIZES one. `ingest::submit`
        // below re-checks the full destination predicate no matter how the id
        // arrived, so a defaulted id is trusted exactly as little as a
        // caller-supplied one and the fail-closed behavior is unchanged. The
        // `service_unavailable` arm mirrors `list_oceans`' identical guard.
        let tenant = super::content_tenant(&self.state, &a);
        let ocean_id = match input.ocean_id {
            Some(id) => id,
            None => {
                let Some(store) = tenant.store.as_ref() else {
                    return Ok(tool_error("service_unavailable", "no database configured"));
                };
                match super::read::resolve_primary_ocean(
                    tenant.decision.as_ref(),
                    store.as_ref(),
                    a.user.user_id,
                )
                .await
                {
                    Ok(id) => id,
                    Err(e) => return Ok(e),
                }
            }
        };

        // SEP-2575 `clientInfo` (`{name, version}`), carried on THIS
        // request's own `_meta` — available even though `/mcp` runs
        // stateless (no session to remember an earlier `initialize`),
        // because 2026-07-28 puts it on every request, not just
        // `initialize`.
        //
        // Reads `ctx.meta.client_info()` (`RequestMetaObject`, the raw
        // per-request `_meta` map) rather than the more obvious
        // `ctx.client_info()` (`RequestContext`'s own convenience method):
        // the latter has a legacy fallback — `peer.peer_info().client_info`
        // — for pre-2026-07-28 sessions, and in OUR stateless config that
        // fallback is NOT empty/`None` the way its doc reads at a glance.
        // Verified against rmcp 3.1.2 (`peer_info_for_stateless_request`,
        // `transport/streamable_http_server/tower.rs`): every stateless
        // request synthesizes a placeholder `InitializeRequestParams` with
        // `client_info: Implementation::default()`, and
        // `Implementation::default()` is `Implementation::from_build_env()`
        // — `{name: "rmcp", version: "3.1.2"}` (rmcp's OWN crate name/
        // version, `env!("CARGO_CRATE_NAME")`/`env!("CARGO_PKG_VERSION")`
        // evaluated inside the rmcp crate), not a blank/absent value. A
        // real end-to-end test against this exact server caught it:
        // `ctx.client_info()` returned `Some(Implementation{name:"rmcp",..})`
        // for a request that sent no `_meta.clientInfo` at all, which would
        // have silently detected every such caller (every pre-SEP-2575
        // client in production today) as platform `"rmcp"`.
        // `ctx.meta.client_info()` has no such fallback — it decodes
        // `_meta.io.modelcontextprotocol/clientInfo` directly and is `None`
        // whenever this request's own JSON-RPC body didn't carry that key,
        // which is the only correct semantics here (same reasoning
        // `mcp/widget_trace.rs`'s `summarize` already applies by parsing
        // `_meta` straight out of the raw request JSON instead of going
        // through rmcp's typed peer-info API).
        //
        // See `IngestAtomRequest::client_info` for how `ingest::submit`
        // uses the result (platform detection ahead of `trusted_user_agent`,
        // `harness_version` fallback) and `McpAuthContext::mcp_client_name`'s
        // doc for why the OTHER User-Agent-derived signals on this request
        // deliberately do NOT switch to this source.
        let client_info = ctx.meta.client_info().map(|info| McpClientInfo {
            name: info.name,
            version: info.version,
        });

        // Detected on every call: session checkpoints log it (clientinfo-
        // platform-detection decision 5), and an unattributed note uses it
        // to fill the hint recipe's `platform` so the next save can copy it.
        let platform_detected = crate::services::platform::detect_platform(
            client_info.as_ref().map(|ci| ci.name.as_str()),
            a.mcp_client_name.as_deref(),
        );
        if input.session_id.is_some() {
            tracing::info!(
                client_name = client_info.as_ref().map(|ci| ci.name.as_str()).unwrap_or(""),
                client_version = client_info.as_ref().map(|ci| ci.version.as_str()).unwrap_or(""),
                user_agent = a.mcp_client_name.as_deref().unwrap_or(""),
                platform_detected = platform_detected.as_deref().unwrap_or(""),
                "add_context: resolved MCP client identity for a session checkpoint"
            );
        }

        let (filed_as, echoed_session_kind) = add_context_filing(
            input.session_id.is_some(),
            input.session_kind.as_deref(),
            input.agent_name.as_deref(),
            input.parent_session_id.as_deref(),
            client_info.as_ref().map(|ci| ci.name.as_str()),
            input.platform.as_deref(),
            a.mcp_client_name.as_deref(),
            &source_type,
        )
        .unwrap_or_else(|()| (source_type.clone(), None));

        let req = IngestAtomRequest {
            graph_id: ocean_id,
            source_type,
            source_id: source_id.clone(),
            content,
            participants,
            timestamp: input.timestamp.unwrap_or_else(Utc::now),
            scope: input.scope,
            fts_language: None,
            channel_id: None,
            channel_name: None,
            author_id,
            author_name: caller_label,
            project: input.project,
            mcp_client_name: a.mcp_client_name.clone(),
            raw_content: None,
            duration_ms: None,
            segments: None,
            session_id: input.session_id.clone(),
            session_kind: input.session_kind.clone(),
            session_title: input.session_title,
            repo_path: input.repo_path,
            remote: input.remote,
            hostname: input.hostname,
            harness_version: input.harness_version,
            model: input.model,
            branch: input.branch,
            agent_name: input.agent_name,
            platform: input.platform,
            task: input.task,
            parent_session_id: input.parent_session_id,
            // The auth layer's User-Agent capture — the same value stamped
            // as `mcp_client_name`, but carried on the transport-only field
            // `ingest::submit` trusts for platform detection (as a fallback
            // behind `client_info` below).
            trusted_user_agent: a.mcp_client_name.clone(),
            client_info,
            // MCP `add_context` never writes a `github_code` atom (that
            // source type is producer-only, backend workers via
            // `routes::ingest::ingest_atom` directly).
            code_path: None,
        };

        // `anchor_atom_ids: &[]` — anchoring is an in-process-only concern
        // (chat-correction auto-save); MCP callers, like the HTTP route,
        // never anchor (plan §3.2 #11 reuse note).
        //
        // Real tenant handles (org-tenancy PR 2.3 — this closes adversarial
        // review P1-2b): `mcp_auth` now resolves a `TenancyDecision` from the
        // verified org claim, so an org-scoped caller's ingest runs against
        // its TENANT store — where ADR-0007 D7 then refuses structurally
        // (org oceans are promotion-fed, never direct-ingest; the caller's
        // personal ocean has no row on the tenant plane, so reachability
        // says not-found, and the org's own graphs fail the destination
        // predicate — `kind='personal'`, or `kind='member'` owned by the
        // caller and not archived, widened from a bare `kind == "personal"`
        // by member-private-oceans PR 5). An org token can no longer reach
        // the consumer plane through this tool. Personal/flag-off resolves to
        // the same consumer handles `Tenant::legacy` produced before —
        // byte-identical.
        //
        // No scoped transaction opened here (module doc, RLS section):
        // `ingest::submit` was converted in phase B.1 and opens its OWN
        // per-request `begin_account_scoped` transaction internally, so
        // wrapping a second one around it here would only nest — and hold —
        // a redundant pooled connection across the whole ingest (which
        // itself interleaves R2 writes and job enqueues).
        // `tenant` is built ABOVE, at the destination-resolution block — the
        // same handle, hoisted rather than rebuilt, so the ocean defaulting
        // and the ingest it feeds provably share one tenancy binding.
        let resp = crate::ingest::submit(crate::ingest::IngestRequest::private(crate::ingest::Member::new(&self.state, &tenant, &a.user, req))).await.into_response();
        Ok(ingest_response_to_tool_result(
            resp,
            // The RESOLVED id, not `input.ocean_id` — the response must echo
            // the ocean actually written to, which is the whole point when the
            // caller omitted it.
            ocean_id,
            source_id,
            a.mcp_client_name.as_deref(),
            AddContextSuccessMeta {
                session_id: input.session_id.clone(),
                filed_as,
                session_kind: echoed_session_kind,
                platform_detected,
            },
        )
        .await)
    }

    /// 12. `confirm_reef` — "Confirm decision".
    #[tool(
        title = "Confirm decision",
        description = "Confirm a pending decision reef, optionally editing its title, rationale, \
            or decided_at. Only reefs in your own private ocean; a reef in a shared ocean or \
            another member's ocean returns not_found. Repeat calls on an already-decided reef \
            return not_found. Refused for read-only tokens.",
        annotations(read_only_hint = false, destructive_hint = false, idempotent_hint = true, open_world_hint = false),
        meta = model_only_meta()
    )]
    async fn confirm_reef(
        &self,
        ctx: RequestContext<RoleServer>,
        Parameters(input): Parameters<ConfirmReefInput>,
    ) -> Result<CallToolResult, McpError> {
        let a = auth(&ctx)?;
        // Content-plane handles resolved from the request's tenancy decision
        // (org-tenancy PR 2.3): tenant store for org tokens, legacy `state`
        // handles for personal/flag-off — see `super::content_tenant`.
        let tenant = super::content_tenant(&self.state, &a);
        let Some(store) = tenant.store.as_ref() else {
            return Ok(tool_error("service_unavailable", "no database configured"));
        };

        if let Some(t) = &input.title {
            if t.chars().count() > CONFIRM_TITLE_CAP {
                return Ok(tool_error(
                    "invalid_params",
                    format!("title must be at most {CONFIRM_TITLE_CAP} chars"),
                ));
            }
        }
        if let Some(r) = &input.rationale {
            if r.len() > REVIEW_TEXT_CAP {
                return Ok(tool_error(
                    "invalid_params",
                    format!("rationale must be at most {REVIEW_TEXT_CAP} bytes"),
                ));
            }
        }
        // Validated up front rather than trusted through to the DB: the only
        // existing guard (`decisions_require_decided_at` CHECK) asserts
        // non-null, not parseable — an unparseable value used to get a 200 OK
        // while silently becoming `null` on echo-back (checked+fixed items
        // #1).
        if let Some(d) = &input.decided_at {
            if DateTime::parse_from_rfc3339(d).is_err() {
                return Ok(tool_error("invalid_params", "decided_at must be RFC 3339"));
            }
        }

        let req = ConfirmRequest {
            title: input.title,
            rationale: input.rationale,
            decided_at: input.decided_at,
        };

        // Reef-id-anchored: scope by the caller's ACTIVE account, which is
        // also the account this tool's guarded UPDATE already filters by
        // (module doc, RLS section). The write scope is the caller's own
        // private ocean only (`ReefWriter::private_only`): shared oceans
        // change through `promote`, never a direct MCP write.
        let writer = ReefWriter::private_only(&a.user);
        let mut scoped_tx = begin_scoped_tx!(tenant, a.user.account_id, "confirm_reef");
        let result = if scoped_tx.is_some() {
            confirm_node_impl_scoped(store.as_ref(), &mut scoped_tx, &writer, input.reef_id, req, "mcp").await
        } else {
            // No pool: the unscoped entry point `routes::nodes` keeps for
            // exactly this case.
            confirm_node_impl(store.as_ref(), &writer, input.reef_id, req, "mcp").await
        };

        match result {
            Ok(state) => {
                if let Err(e) = commit_write_tx(scoped_tx, "confirm_reef").await {
                    return Ok(e);
                }
                Ok(tool_success(
                    json!({
                        "reef_id": state.node_id,
                        "review_status": state.review_status.unwrap_or_else(|| "confirmed".to_string()),
                        "title": state.title,
                        "rationale": state.rationale,
                        "decided_at": state.decided_at,
                    }),
                    a.mcp_client_name.as_deref(),
                ))
            }
            // The transaction is dropped un-committed here, so a partial
            // write can never survive a failed confirm.
            Err(e) => Ok(review_error_to_tool_error(e)),
        }
    }

    /// 13. `reject_reef` — "Reject decision".
    #[tool(
        title = "Reject decision",
        description = "Reject a pending decision reef, optionally recording a reason. Only \
            reefs in your own private ocean; a reef in a shared ocean or another member's ocean \
            returns not_found. Repeat calls on an already-decided reef return not_found. \
            Refused for read-only tokens.",
        annotations(read_only_hint = false, destructive_hint = true, idempotent_hint = true, open_world_hint = false),
        meta = model_only_meta()
    )]
    async fn reject_reef(
        &self,
        ctx: RequestContext<RoleServer>,
        Parameters(input): Parameters<RejectReefInput>,
    ) -> Result<CallToolResult, McpError> {
        let a = auth(&ctx)?;
        // Content-plane handles resolved from the request's tenancy decision
        // (org-tenancy PR 2.3): tenant store for org tokens, legacy `state`
        // handles for personal/flag-off — see `super::content_tenant`.
        let tenant = super::content_tenant(&self.state, &a);
        let Some(store) = tenant.store.as_ref() else {
            return Ok(tool_error("service_unavailable", "no database configured"));
        };

        if let Some(r) = &input.reason {
            if r.len() > REVIEW_TEXT_CAP {
                return Ok(tool_error(
                    "invalid_params",
                    format!("reason must be at most {REVIEW_TEXT_CAP} bytes"),
                ));
            }
        }

        let req = RejectRequest {
            reason: input.reason,
        };

        // Reef-id-anchored: scope by the caller's ACTIVE account (module doc,
        // RLS section); write scope is the caller's own private ocean only.
        let writer = ReefWriter::private_only(&a.user);
        let mut scoped_tx = begin_scoped_tx!(tenant, a.user.account_id, "reject_reef");
        let result = if scoped_tx.is_some() {
            reject_node_impl_scoped(store.as_ref(), &mut scoped_tx, &writer, input.reef_id, req, "mcp").await
        } else {
            // No pool.
            reject_node_impl(store.as_ref(), &writer, input.reef_id, req, "mcp").await
        };

        match result {
            Ok(state) => {
                if let Err(e) = commit_write_tx(scoped_tx, "reject_reef").await {
                    return Ok(e);
                }
                Ok(tool_success(
                    json!({
                        "reef_id": state.node_id,
                        "review_status": state.review_status.unwrap_or_else(|| "rejected".to_string()),
                    }),
                    a.mcp_client_name.as_deref(),
                ))
            }
            Err(e) => Ok(review_error_to_tool_error(e)),
        }
    }

    /// 14. `update_reef` — "Update reef".
    #[tool(
        title = "Update reef",
        description = "Update a reef's title and/or type (not to \"decision\" — use confirm_reef \
            for that). Only reefs in your own private ocean; a reef in a shared ocean or another \
            member's ocean returns not_found. At least one of title/type is required. Returns \
            before AND after \
            snapshots, each including `version` — pass the value back as `expected_version` on a \
            later call to guard against concurrent edits. Refused for read-only tokens.",
        annotations(read_only_hint = false, destructive_hint = true, idempotent_hint = true, open_world_hint = false),
        meta = model_only_meta()
    )]
    async fn update_reef(
        &self,
        ctx: RequestContext<RoleServer>,
        Parameters(input): Parameters<UpdateReefInput>,
    ) -> Result<CallToolResult, McpError> {
        let a = auth(&ctx)?;
        // Content-plane handles resolved from the request's tenancy decision
        // (org-tenancy PR 2.3): tenant store for org tokens, legacy `state`
        // handles for personal/flag-off — see `super::content_tenant`.
        let tenant = super::content_tenant(&self.state, &a);
        let Some(store) = tenant.store.as_ref() else {
            return Ok(tool_error("service_unavailable", "no database configured"));
        };

        if input.expected_version.is_some_and(|v| v < 1)
            || (input.support_atom_id.is_some() && input.title.is_none())
        {
            return Ok(tool_error("invalid_params", "invalid guarded identity update"));
        }
        if input.title.is_none() && input.reef_type.is_none() {
            return Ok(tool_error(
                "invalid_params",
                "at least one of title or type must be provided",
            ));
        }
        // Empty/oversized title, invalid type, and `type == "decision"` are
        // all rejected inside `patch_node_impl` -> `validate_patch_fields`
        // (`routes::nodes`) now — the single shared guard both this tool and
        // the REST `PATCH /v1/nodes/{id}` route go through (checked+fixed
        // items #2/#4, widened to cover both callers).

        // Reef-id-anchored: scope by the caller's ACTIVE account — the same
        // account the ownership guard and the guarded UPDATE below already
        // filter by (module doc, RLS section). ONE transaction spans the
        // ownership check, the before-snapshot, the UPDATE and the
        // after-snapshot, so the echoed before/after pair can never straddle
        // a concurrent writer's commit the way four independent statements
        // could.
        let mut scoped_tx = begin_scoped_tx!(tenant, a.user.account_id, "update_reef");

        // Write-scope guard BEFORE the before-snapshot read: `Store::get_node`
        // has no account filtering of its own (see its doc comment) — it is
        // only safe to call once the reef is proven to sit in the caller's
        // own private ocean. Account ownership alone is not enough: every
        // ocean in an org shares one account (settings 2.10). Same predicate
        // `patch_node_*` re-checks below.
        let writer = ReefWriter::private_only(&a.user);
        match node_writable_by(store.as_ref(), &mut scoped_tx, input.reef_id, &writer).await {
            Ok(true) => {}
            Ok(false) => return Ok(tool_error("not_found", "reef not found or access denied")),
            Err(e) => {
                tracing::error!(error = %e, "update_reef ownership check error");
                return Ok(tool_error("service_unavailable", "internal error"));
            }
        }

        let before = match scoped_store!(store, scoped_tx, get_node / get_node_scoped(input.reef_id))
        {
            Ok(Some(n)) => n,
            Ok(None) => return Ok(tool_error("not_found", "reef not found or access denied")),
            Err(e) => {
                tracing::error!(error = %e, "update_reef before-snapshot error");
                return Ok(tool_error("service_unavailable", "internal error"));
            }
        };
        // Best-effort: `version` is snapshot enrichment, not core to the
        // update — a lookup failure here must never block the write below.
        //
        // Under a shared transaction a failed statement aborts the whole
        // transaction, so a `version` lookup that errors would take the
        // UPDATE down with it. In practice this query cannot fail
        // independently of the ones around it (same table, same row, same
        // scope, and ownership was just proven), so the tolerated-failure
        // contract is preserved for every reachable case; the residue is a
        // connection-level failure, which would have failed the whole call
        // anyway.
        let before_version = match scoped_store!(
            store,
            scoped_tx,
            node_version / node_version_scoped(input.reef_id)
        ) {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!(error = %e, "update_reef before-version lookup failed");
                None
            }
        };

        let after = if scoped_tx.is_some() {
            patch_node_with_identity_impl_scoped(
                store.as_ref(),
                &mut scoped_tx,
                &writer,
                input.reef_id,
                input.title.as_deref(),
                input.reef_type.as_deref(),
                input.expected_version,
                input.support_atom_id,
            )
            .await
        } else {
            if input.support_atom_id.is_some() {
                return Ok(tool_error("invalid_params", "supported rename requires an authenticated PostgreSQL scope"));
            }
            // No pool.
            patch_node_impl(
                store.as_ref(),
                &writer,
                input.reef_id,
                input.title.as_deref(),
                input.reef_type.as_deref(),
                input.expected_version,
            )
            .await
        };
        // The transaction is dropped un-committed on any error path below, so
        // a rejected/conflicting patch can never leave a partial write.
        let after = match after {
            Ok(n) => n,
            Err(e) => return Ok(patch_error_to_tool_error(e)),
        };
        let after_version = match scoped_store!(
            store,
            scoped_tx,
            node_version / node_version_scoped(input.reef_id)
        ) {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!(error = %e, "update_reef after-version lookup failed");
                None
            }
        };

        if let Err(e) = commit_write_tx(scoped_tx, "update_reef").await {
            return Ok(e);
        }

        Ok(tool_success(
            json!({
                "reef_id": input.reef_id,
                "before": node_snapshot(&before, before_version),
                "after": node_snapshot(&after, after_version),
            }),
            a.mcp_client_name.as_deref(),
        ))
    }

    /// 15. `promote` — "Promote reef to a shared ocean".
    ///
    /// The missing half of the write story. `add_context` refuses a shared org
    /// ocean with "can only ingest into your own private ocean; **share via
    /// promotion**" — but promotion existed only as HTTP routes
    /// (`/graphs/{id}/promote-source`, `/nodes/{id}/promote`) and was exposed
    /// as no MCP tool at all, so the remedy the error names was unreachable
    /// from the connection that raised it. An agent could put knowledge into a
    /// private ocean and had no way to publish it to the team.
    #[tool(
        title = "Promote reef to a shared ocean",
        description = "Publish a reef from your own private ocean into a shared (team/org) \
            ocean, copying its corals. This is the ONLY way content reaches a shared ocean — \
            add_context cannot write to one directly. Pass the source reef_id and the \
            target_ocean_id; omit coral_ids to copy every coral on the reef, or pass an \
            explicit list (an empty list promotes the reef alone). Re-promoting an \
            already-promoted reef re-publishes it rather than duplicating it. Refused for \
            read-only tokens.",
        annotations(read_only_hint = false, destructive_hint = false, idempotent_hint = true, open_world_hint = false),
        meta = model_only_meta()
    )]
    async fn promote(
        &self,
        ctx: RequestContext<RoleServer>,
        Parameters(input): Parameters<PromoteInput>,
    ) -> Result<CallToolResult, McpError> {
        let a = auth(&ctx)?;

        // Delegated to the HTTP handler verbatim rather than through an
        // `*_impl` extraction (the pattern the other write tools in this file
        // use). Deliberate: `promote_node` still runs every one of its gates
        // — ownership, LIVE org membership, target-is-shared, coral-attachment,
        // and (as of "Promotion never crosses a database") the same-pool-only
        // boundary itself — and calling the handler leaves all of them running
        // exactly as the REST route runs them, byte for byte, rather than
        // duplicating that logic in an `_impl` carve-out. Promotion publishes a
        // reef from the caller's own member ocean into a shared team/org ocean
        // WITHIN THE SAME TENANT DATABASE; a request whose source resolves to a
        // different plane (the old personal-consumer-DB → org-tenant-DB case)
        // is refused with a 403, mapped below like any other error status —
        // there is no cross-DB decrypt/re-encrypt path left to delegate to.
        //
        // This is only possible because `promote_node` returns a CONCRETE
        // `(StatusCode, Json<Value>)` rather than an erased `Response`, so the
        // status is readable here for the error mapping below. If it is ever
        // changed to return `impl IntoResponse`, this call site breaks at
        // compile time and should be revisited then — not silently patched
        // with a body-sniffing status guess.
        let tenant = super::content_tenant(&self.state, &a);
        let (status, axum::Json(body)) = crate::routes::nodes::promote_node(
            axum::extract::State(self.state.clone()),
            tenant,
            axum::Extension(a.user.clone()),
            axum::extract::Path(input.reef_id),
            axum::Json(crate::routes::nodes::PromoteRequest {
                target_graph_id: input.target_ocean_id,
                coral_ids: input.coral_ids,
            }),
        )
        .await;

        use axum::http::StatusCode;
        if status == StatusCode::OK || status == StatusCode::CREATED {
            return Ok(tool_success(
                json!({
                    "reef_id": input.reef_id,
                    "target_ocean_id": input.target_ocean_id,
                    // 201 = newly promoted, 200 = re-published an existing
                    // promotion. Surfaced because the two are indistinguishable
                    // from the body alone, and "did this create something new?"
                    // is exactly what a caller retrying needs to know.
                    "created": status == StatusCode::CREATED,
                    "promoted_reef": body,
                }),
                a.mcp_client_name.as_deref(),
            ));
        }

        // Same mapping (and same IDOR non-leak collapse of 403/404 into
        // `not_found`) that `ingest_response_to_tool_result` applies, so the
        // two write paths cannot disagree about what a given status means.
        let message = body
            .get("error")
            .and_then(|v| v.as_str())
            .unwrap_or("promote failed")
            .to_string();
        let code = match status {
            StatusCode::BAD_REQUEST | StatusCode::UNPROCESSABLE_ENTITY => "invalid_params",
            StatusCode::FORBIDDEN | StatusCode::NOT_FOUND => "not_found",
            _ => "service_unavailable",
        };
        Ok(tool_error(code, message))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    /// GitHub connector PR A — the fourth switch site, in reverse: none of
    /// the six §3.5 source types may be claimed by an `add_context` caller
    /// (see `ADD_CONTEXT_SOURCE_TYPES`'s own doc comment for why absence is
    /// correct here, unlike the other three switch sites where presence is
    /// correct). A pure allowlist check — no MCP call needed — mirrors how
    /// cheaply the other three sites' registration tests run.
    #[test]
    fn add_context_rejects_every_github_source_type_as_a_caller_chosen_label() {
        for t in [
            "github_pr",
            "github_issue",
            "github_release",
            "github_commit",
            "github_code",
            "github_code_summary",
        ] {
            assert!(
                !ADD_CONTEXT_SOURCE_TYPES.contains(&t),
                "add_context must never let a caller claim GitHub provenance for {t} — that \
                 content is produced exclusively by the real ingest pipeline"
            );
        }
    }

    // ── confirm_reef / reject_reef idempotency ────────────────────────────────

    // ── update_reef before/after ──────────────────────────────────────────────

    // ── add_context → atom lands in the store ─────────────────────────────────

    // ── Tenancy: user B cannot confirm/reject/update user A's reef, nor ingest
    // into A's ocean ────────────────────────────────────────────────────────

    // ── caller_display_name: name-vs-email preference (migration 0067) ───────
    //
    // These two exercise `caller_display_name`
    // DIRECTLY against a REAL Postgres pool (gated on `RELINFER_TEST_DATABASE_URL`,
    // same convention as `workers::embed::tests::keyword_search_finds_atom_after_content_tsv_uses_simple_regconfig`)
    // rather than through the full `add_context` tool-call harness: `caller_display_name`
    // needs `state.db` to run its `COALESCE(name, email)` query at all. The full end-to-end version of both cases (through `add_context`
    // + `get_atom`) lives in `tests/mcp_pg_e2e.rs`'s
    // `add_context_prefers_users_name_over_email_when_both_present_...` and
    // `add_context_stamps_caller_author_and_defaults_participants_...` tests.

    /// Build a minimal `AppState` wired to a real pool, every other field at
    /// its inert test default.
    fn app_state_with_pool(pool: sqlx::PgPool) -> Arc<crate::AppState> {
        Arc::new(crate::AppState {
            db: Some(pool),
            store: None,
            resend_api_key: None,
            r2: None,
            http_client: reqwest::Client::new(),
            embedder: Arc::new(crate::StubEmbedder),
            llm: None,
            rate_limiter: Arc::new(crate::rate_limit::InMemoryRateLimiter::new()),
            // W2: inert in tests — admission control only runs as router
            // middleware, and these states never front one.
            admission: Default::default(),
            in_flight: std::sync::Arc::new(Default::default()),
            metrics: std::sync::Arc::new(crate::telemetry::NoopMetrics),
            dek_cache: crate::dek_cache::DekCache::new(),
            connector_flag_cache: crate::connector_flags::ConnectorFlagCache::new(),
            tenant_runtime: None,
            env: crate::AppEnv::default(),
            task_tracker: tokio_util::task::TaskTracker::new(),
            worker_activity: crate::workers::tenant_harness::TenantActivity::new(),
            mcp_cancellation_token: tokio_util::sync::CancellationToken::new(),
            mcp_task_manager: crate::mcp::server::McpTaskRegistry::new(),
            mcp_ask_in_flight: crate::mcp::server::AskInFlight::new(),
            stripe: None,
        })
    }

    async fn connect_test_pool() -> Option<sqlx::PgPool> {
        let url = std::env::var("RELINFER_TEST_DATABASE_URL").ok()?;
        Some(
            sqlx::postgres::PgPoolOptions::new()
                .max_connections(4)
                .connect(&url)
                .await
                .expect("connect to test Postgres"),
        )
    }

    #[tokio::test]
    async fn caller_display_name_prefers_name_over_email_against_real_postgres() {
        let Some(pool) = connect_test_pool().await else {
            eprintln!("skipping: set RELINFER_TEST_DATABASE_URL to run the live caller_display_name test");
            return;
        };
        let email = format!("{}@test.local", Uuid::new_v4());
        let user_id: Uuid = sqlx::query_scalar(
            "INSERT INTO users (email, name) VALUES ($1, $2) RETURNING id",
        )
        .bind(&email)
        .bind("Ada Lovelace")
        .fetch_one(&pool)
        .await
        .expect("insert user with name");

        let state = app_state_with_pool(pool.clone());
        let label = caller_display_name(&state, user_id).await;
        assert_eq!(
            label.as_deref(),
            Some("Ada Lovelace"),
            "name must be preferred over email when both are present"
        );

        sqlx::query("DELETE FROM users WHERE id = $1")
            .bind(user_id)
            .execute(&pool)
            .await
            .ok();
    }

    #[tokio::test]
    async fn caller_display_name_falls_back_to_email_when_name_is_null_against_real_postgres() {
        let Some(pool) = connect_test_pool().await else {
            eprintln!("skipping: set RELINFER_TEST_DATABASE_URL to run the live caller_display_name test");
            return;
        };
        let email = format!("{}@test.local", Uuid::new_v4());
        let user_id: Uuid = sqlx::query_scalar("INSERT INTO users (email) VALUES ($1) RETURNING id")
            .bind(&email)
            .fetch_one(&pool)
            .await
            .expect("insert user with no name");

        let state = app_state_with_pool(pool.clone());
        let label = caller_display_name(&state, user_id).await;
        assert_eq!(
            label.as_deref(),
            Some(email.as_str()),
            "must fall back to email when name is NULL"
        );

        sqlx::query("DELETE FROM users WHERE id = $1")
            .bind(user_id)
            .execute(&pool)
            .await
            .ok();
    }
}

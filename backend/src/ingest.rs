//! The one ingest door (ADR-0016). [`submit`] takes an [`IngestRequest`] with
//! a typed [`Destination`], checks it, and writes the source through
//! [`capture::store_capture_tx`] in one tenant-DB transaction.
//!
//! HTTP `/v1/ingest/atom`, MCP `add_context` and the connectors that act for
//! one member write to that member's private ocean. Slack events, huddles,
//! Google Chat and GitHub write where an admin routed their source, under an
//! [`OrgSharedGrant`].

pub mod atom;
pub(crate) mod capture;
mod grant;
#[cfg(test)]
mod pg_tests;

pub(crate) use grant::{OrgSharedGrant, RepoRefusal};

use atom::{
    apply_agent_platform_safety_valve, basename_for_fallback_key, build_contextual_header,
    effective_harness_version, effective_root_platform, merge_chunk_speakers, provenance_label_error,
    resolve_undeclared_session_kind, truncate_machine_label, validate_provenance_label, IngestAtomRequest,
    IngestAtomResponse, KindSource, SessionKind, ALLOWED_LANGUAGES, CODING_SESSION_ID_CAP,
    CODING_SESSION_LABEL_CAP, CODING_SESSION_REMOTE_RAW_CAP,
};
use capture::{embed_key, AtomFields, CaptureHints, Prepared};

use axum::{http::StatusCode, response::{IntoResponse, Response}, Json};
use chrono::Utc;
use serde_json::json;
use std::sync::Arc;
use uuid::Uuid;

use coralswarm_core::chunker::{chunk, ChunkStrategy};

use sqlx::PgPool;

use crate::{
    crypto::{resolve_dek_cached_dispatch, CryptoError},
    jobs,
    middleware::auth::UserContext,
    routes::activity::insert_activity_event,
    source_evidence::CaptureSource,
    source_origin::SourceOrigin,
    store::Store,
    tenancy::extractor::Tenant,
    tenancy::reachability::ReachabilityResolver,
    tenancy::TenantId,
    AppState,
};

/// Where a request's corals land, and who writes them. Every request names
/// one: there is no trusted-connector bypass.
pub(crate) enum Destination<'a> {
    /// The member's own private ocean, `atom.graph_id`. [`submit`] applies
    /// ADR-0007 D7 ([`is_own_writable_ocean`]).
    Private(Member<'a>),
    /// A shared org ocean an admin routed the source into. [`submit`] checks
    /// that the grant still exists and points at the ocean.
    OrgShared(Routed<'a>),
}

/// A member capturing into their own private ocean: HTTP, MCP, and the
/// connectors that act for one member.
pub(crate) struct Member<'a> {
    pub(crate) state: &'a Arc<AppState>,
    pub(crate) tenant: &'a Tenant,
    pub(crate) user: &'a UserContext,
    pub(crate) atom: IngestAtomRequest,
    /// The admitted connector's source namespace. `None` records the source
    /// under `declared:user:{user_id}`.
    pub(crate) provider_namespace: Option<&'a str>,
    /// A trusted connector's chunk strategy and source anchor. Never part of
    /// [`IngestAtomRequest`], so an HTTP client cannot supply them.
    pub(crate) hints: CaptureHints,
}

impl<'a> Member<'a> {
    /// A capture with no connector namespace or hints.
    pub(crate) fn new(state: &'a Arc<AppState>, tenant: &'a Tenant, user: &'a UserContext, atom: IngestAtomRequest) -> Self {
        Self { state, tenant, user, atom, provider_namespace: None, hints: CaptureHints::default() }
    }
}

/// A connector capturing into an ocean its routing grant names. There is no
/// member to act as, so the write runs as the tenant's system actor.
pub(crate) struct Routed<'a> {
    pub(crate) ocean: Uuid,
    pub(crate) grant: &'a OrgSharedGrant,
    pub(crate) writer: SystemWriter<'a>,
    /// The connector's prepared source. Its `provider_namespace` must be the
    /// grant's.
    pub(crate) source: CaptureSource<'a>,
    pub(crate) chunks: &'a Prepared,
    pub(crate) atoms: AtomFields<'a>,
    /// Hash of the source text, for the embed job's [`embed_key`].
    pub(crate) content_hash: &'a str,
    pub(crate) sync_ops: Option<&'a dyn Store>,
}

/// The tenant plane and keys a [`Routed`] capture is written with. The account
/// is the grant's.
#[derive(Clone, Copy)]
pub(crate) struct SystemWriter<'a> {
    pub(crate) pool: &'a PgPool,
    pub(crate) tenant: &'a TenantId,
    pub(crate) dek: &'a [u8; 32],
    pub(crate) r2: Option<&'a coralswarm_core::r2::R2Client>,
}

/// One source to ingest.
pub(crate) struct IngestRequest<'a> {
    pub(crate) destination: Destination<'a>,
    /// Corals whose reefs (within the destination ocean) the new corals are
    /// linked onto. In-process only; HTTP clients cannot drive reef linking.
    pub(crate) anchors: &'a [Uuid],
    pub(crate) origin: Option<&'a SourceOrigin>,
}

impl<'a> IngestRequest<'a> {
    /// A request with no anchors or origin.
    pub(crate) fn new(destination: Destination<'a>) -> Self {
        Self { destination, anchors: &[], origin: None }
    }

    /// [`IngestRequest::new`] into the member's own private ocean.
    pub(crate) fn private(member: Member<'a>) -> Self {
        Self::new(Destination::Private(member))
    }
}

/// What [`submit`] stored. Responds as `/v1/ingest/atom`'s 200 body.
pub(crate) struct Ingested {
    pub(crate) body: IngestAtomResponse,
    /// Chunk atoms linked to the source version: written or adopted.
    pub(crate) linked: usize,
}

impl IntoResponse for Ingested {
    fn into_response(self) -> Response {
        (StatusCode::OK, Json(self.body)).into_response()
    }
}

/// ADR-0007 **D7's destination invariant**: is `g` an ocean `user_id` may ingest
/// directly into? The single source of truth for that question — the ingest
/// route enforces it, and MCP's `list_oceans` reports it as an advisory
/// `writable` flag, so the two cannot drift apart.
///
/// True for exactly two shapes:
/// * `kind='personal'` — the consumer plane's own ocean.
/// * `kind='member'` AND owned by this caller AND not archived — a member's
///   private ocean inside an org tenant (member-private-oceans PR 5, plan
///   §3/§7 item 5).
///
/// **The kind check alone is NOT sufficient**, which is why this takes a
/// `user_id` at all: a kind-only widening would let a caller ingest into ANY
/// member ocean in the account, not just their own. The reachability check the
/// ingest route runs a few lines earlier already enforces the identical
/// predicate (PR 2's tenant-arm SQL function), but this is the one place D7's
/// destination invariant actually lives, and it must hold even if reachability
/// is ever satisfied by a path that skips that call.
///
/// Deliberately **no `is_primary` requirement** (matching the original
/// `kind == "personal"` arm, which never checked it): any of the caller's own
/// non-archived member oceans — primary or an allowed secondary (plan §5) — is
/// a valid explicit destination. `is_primary` decides only the *default* ocean
/// (`resolve_primary_graph` / MCP's `resolve_primary_ocean`), never whether a
/// supplied id belongs to you.
///
/// Note what is absent: `kind='org'`/`'team'`. A shared ocean is **never** a
/// legal direct-ingest target — it is promotion-fed (`promote_node`), which is
/// what the rejection message means by "share via promotion".
pub fn is_own_writable_ocean(g: &crate::store::Graph, user_id: Uuid) -> bool {
    g.kind == "personal"
        || (g.kind == "member" && g.owner_user_id == Some(user_id) && g.archived_at.is_none())
}

/// Why [`submit`] refused. HTTP and MCP answer with its [`IntoResponse`],
/// which keeps the status and `{"error": ..}` body each refusal always had.
/// In-process connectors match on the variant instead of a status code.
#[derive(Debug)]
pub(crate) enum IngestError {
    /// 400: the request is malformed.
    Malformed(String),
    /// 403: the member may not write to this ocean, or no routing grant
    /// allows it. On the routed path the grant is resolved afresh on the
    /// next attempt, so a connector retries this.
    Forbidden(String),
    /// 409: a session identity the request contradicts.
    Conflict(String),
    /// 422: the request can never succeed as sent.
    Invalid(String),
    /// [`Self::Invalid`] with a machine-readable `code` in the body, for the
    /// agent-session shapes an MCP client acts on.
    InvalidCoded { error: String, code: &'static str },
    /// 503: a dependency is down; retry later.
    Unavailable(String),
    /// 500: a failure mid-way; retry.
    Internal(String),
    /// The capture core failed; nothing is committed. Carries the core's own
    /// permanence (a malformed blob key) and whether row-level security
    /// refused the write. Never an object-store refusal: [`Self::capture`]
    /// turns that into [`Self::StorageUnavailable`].
    Capture(capture::FunnelError),
    /// The capture ran but was withheld; `cleanup_status` is what became of
    /// its uploads. 403 when the caller's capture authorization lapsed, else
    /// 500.
    Uncommitted { error: &'static str, authorization_lapsed: bool, cleanup_status: &'static str },
    /// 503: the object store refused a body, so nothing was stored; the same
    /// request succeeds once the store is back. `cleanup_status` is what
    /// became of the uploads that did land. The body carries
    /// `"code": "storage_unavailable"` for a client deciding to retry.
    StorageUnavailable { cleanup_status: &'static str },
}

impl IngestError {
    /// The error a failed capture answers with: the one place an object-store
    /// refusal ([`capture::FunnelError::storage`]) becomes
    /// [`Self::StorageUnavailable`], on every arm, so one outage has one
    /// shape. `cleanup_status` is what became of the capture's uploads.
    fn capture(e: capture::FunnelError, cleanup_status: &'static str) -> Self {
        if e.storage {
            Self::StorageUnavailable { cleanup_status }
        } else {
            Self::Capture(e)
        }
    }

    /// The same request fails the same way on every retry.
    pub(crate) fn is_permanent(&self) -> bool {
        match self {
            Self::Malformed(_) | Self::Conflict(_) | Self::Invalid(_) | Self::InvalidCoded { .. } => true,
            Self::Capture(e) => e.permanent,
            Self::Forbidden(_)
            | Self::Unavailable(_)
            | Self::Internal(_)
            | Self::Uncommitted { .. }
            | Self::StorageUnavailable { .. } => false,
        }
    }

    /// This item can never be stored as sent (HTTP 422): a connector that
    /// walks a provider's history drops it and moves its cursor past it,
    /// where a transient failure would hold the cursor. Narrower than
    /// [`Self::is_permanent`] on purpose: the connectors that drop on it have
    /// always retried a malformed request or a session conflict.
    pub(crate) fn is_invalid(&self) -> bool {
        match self {
            Self::Invalid(_) | Self::InvalidCoded { .. } => true,
            Self::Capture(e) => e.permanent && !e.rls_refused,
            Self::Malformed(_)
            | Self::Forbidden(_)
            | Self::Conflict(_)
            | Self::Unavailable(_)
            | Self::Internal(_)
            | Self::Uncommitted { .. }
            | Self::StorageUnavailable { .. } => false,
        }
    }

    /// The writer may not write to this ocean (HTTP 403): the member lost
    /// it, no routing grant allows it, or row-level security refused it.
    pub(crate) fn is_forbidden(&self) -> bool {
        match self {
            Self::Forbidden(_) => true,
            Self::Capture(e) => e.rls_refused,
            Self::Uncommitted { authorization_lapsed, .. } => *authorization_lapsed,
            Self::Malformed(_)
            | Self::Conflict(_)
            | Self::Invalid(_)
            | Self::InvalidCoded { .. }
            | Self::Unavailable(_)
            | Self::Internal(_)
            | Self::StorageUnavailable { .. } => false,
        }
    }

    fn status(&self) -> StatusCode {
        match self {
            Self::Malformed(_) => StatusCode::BAD_REQUEST,
            Self::Forbidden(_) => StatusCode::FORBIDDEN,
            Self::Conflict(_) => StatusCode::CONFLICT,
            Self::Invalid(_) | Self::InvalidCoded { .. } => StatusCode::UNPROCESSABLE_ENTITY,
            Self::Unavailable(_) => StatusCode::SERVICE_UNAVAILABLE,
            Self::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Self::Capture(e) if e.rls_refused => StatusCode::FORBIDDEN,
            Self::Capture(e) if e.permanent => StatusCode::UNPROCESSABLE_ENTITY,
            Self::Capture(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Self::Uncommitted { authorization_lapsed: true, .. } => StatusCode::FORBIDDEN,
            Self::Uncommitted { .. } => StatusCode::INTERNAL_SERVER_ERROR,
            Self::StorageUnavailable { .. } => StatusCode::SERVICE_UNAVAILABLE,
        }
    }

    fn message(&self) -> String {
        match self {
            Self::Malformed(m) | Self::Forbidden(m) | Self::Conflict(m) | Self::Invalid(m) | Self::Unavailable(m) | Self::Internal(m) => {
                m.clone()
            }
            Self::InvalidCoded { error, .. } => error.clone(),
            Self::Capture(e) => e.to_string(),
            Self::Uncommitted { error, .. } => (*error).to_string(),
            Self::StorageUnavailable { .. } => "object storage is unavailable; nothing was stored, retry ingest".to_string(),
        }
    }
}

impl std::fmt::Display for IngestError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let kind = match self {
            Self::Malformed(_) => "malformed",
            Self::Forbidden(_) => "forbidden",
            Self::Conflict(_) => "conflict",
            Self::Invalid(_) | Self::InvalidCoded { .. } => "invalid",
            Self::Unavailable(_) => "unavailable",
            Self::Internal(_) => "internal",
            Self::Capture(_) => "capture failed",
            Self::Uncommitted { .. } => "not committed",
            Self::StorageUnavailable { .. } => "storage unavailable",
        };
        write!(f, "ingest {kind}: {}", self.message())
    }
}

impl std::error::Error for IngestError {}

impl IntoResponse for IngestError {
    fn into_response(self) -> Response {
        let body = match &self {
            Self::InvalidCoded { error, code } => json!({"error": error, "code": code}),
            Self::Uncommitted { cleanup_status, .. } => json!({"error": self.message(), "cleanup_status": cleanup_status}),
            Self::StorageUnavailable { cleanup_status } => {
                json!({"error": self.message(), "code": "storage_unavailable", "cleanup_status": cleanup_status})
            }
            _ => json!({"error": self.message()}),
        };
        (self.status(), Json(body)).into_response()
    }
}

/// Ingest one source. `Ok` is what was stored; `Err` is why it was refused.
/// HTTP callers answer with either.
pub(crate) async fn submit(request: IngestRequest<'_>) -> Result<Ingested, IngestError> {
    let IngestRequest { destination, anchors, origin } = request;
    if let Some(origin) = origin {
        if let Err(error) = origin.validate() {
            return Err(IngestError::Invalid(error));
        }
    }
    match destination {
        Destination::Private(member) => submit_private(member, anchors, origin).await,
        Destination::OrgShared(routed) => submit_routed(routed, anchors, origin).await,
    }
}

async fn submit_private(
    member: Member<'_>,
    anchor_atom_ids: &[Uuid],
    origin: Option<&SourceOrigin>,
) -> Result<Ingested, IngestError> {
    let Member { state, tenant, user: user_ctx, atom: mut req, provider_namespace, hints } = member;
    // ── 0. Session identity, declared kind, derived source type ──────────────
    // `session_id` is an identifier — rejected (422) when empty/over-cap/
    // NUL-bearing, same shape as `source_id`'s validation (Phase 1, audit
    // A1/A2/A5/A10). Every other coding-session field is machine-generated
    // free-text provenance and is TRUNCATED, never rejected — see
    // [`truncate_machine_label`] (A10).
    if let Some(sid) = &req.session_id {
        if sid.is_empty() || sid.chars().count() > CODING_SESSION_ID_CAP {
            return Err(IngestError::Invalid(format!("session_id must be 1-{CODING_SESSION_ID_CAP} chars").into()));
        }
        if sid.contains('\0') {
            return Err(IngestError::Invalid("session_id must not contain NUL bytes".into()));
        }
    }
    // `parent_session_id` is an identifier too (an exact-match key against
    // another `coding_sessions` row), so it gets `session_id`'s reject-don't-
    // truncate treatment. An empty string is normalized to "no parent".
    if req.parent_session_id.as_deref().is_some_and(str::is_empty) {
        req.parent_session_id = None;
    }
    if let Some(pid) = &req.parent_session_id {
        if pid.chars().count() > CODING_SESSION_ID_CAP {
            return Err(IngestError::Invalid(format!("parent_session_id must be 1-{CODING_SESSION_ID_CAP} chars").into()));
        }
        if pid.contains('\0') {
            return Err(IngestError::Invalid("parent_session_id must not contain NUL bytes".into()));
        }
    }
    // The KIND — declared or inferred, validated against the closed set
    // BEFORE it can decide anything (session-kind-inference plan). A
    // DECLARED value is rejected (422) rather than defaulted on an unknown
    // spelling: `atoms.source_type` is INSERT-only across sync, so a typo'd
    // kind silently coerced to the default would land a coral that can never
    // be relabelled. Validated even when `session_id` is absent (where the
    // kind has nothing to label) so a caller finds out its vocabulary is
    // wrong on the first call, not on the first call that happens to carry a
    // session.
    //
    // An ABSENT `session_kind` no longer defaults blindly to `coding`: with a
    // `session_id` present, [`resolve_undeclared_session_kind`] infers
    // `agent` from a structural tell (`agent_name` / `parent_session_id`) or
    // a known agent framework's clientInfo, falling back to
    // [`SessionKind::DEFAULT`] only when neither applies. `kind_source`
    // records which path produced the value — the platform safety valve two
    // steps down (0a) reads it to decide whether a missing platform is the
    // existing 422 (a caller that ASKED for `agent`) or a silent downgrade (a
    // caller that never mentioned `session_kind` at all).
    let (mut session_kind, kind_source): (SessionKind, KindSource) = match req.session_kind.as_deref() {
        Some(raw) => match SessionKind::parse(raw) {
            Some(k) => (k, KindSource::Declared),
            None => {
                return Err(IngestError::Invalid(format!(
                            "session_kind must be one of: {}",
                            SessionKind::allowed_values()
                        ).into()));
            }
        },
        None if req.session_id.is_some() => (
            resolve_undeclared_session_kind(
                req.agent_name.as_deref(),
                req.parent_session_id.is_some(),
                req.client_info.as_ref().map(|ci| ci.name.as_str()),
            ),
            KindSource::Inferred,
        ),
        // No `session_id` at all: inference is a session-kind property
        // (decision 1) with nothing to infer about, and nothing downstream
        // reads `session_kind` either. `Declared` here is a don't-care —
        // every site that branches on `kind_source` also requires a
        // `session_id`.
        None => (SessionKind::DEFAULT, KindSource::Declared),
    };

    // ── 0a. Agent identity: platform policy (SessionKind::Agent) ─────────────
    // The declared `platform` is canonicalized (aliases collapse, unknown
    // values pass through as a slug) and the request's clientInfo/User-Agent
    // is read independently into `platform_detected` — never merged, so a
    // declared value is never silently rewritten and the read side can
    // expose `platform_verified`. For an AGENT session with no parent, a
    // platform is mandatory: declared wins, detected is the fallback. A
    // subagent (`parent_session_id` set) satisfies the rule by inheriting its
    // root's platform below, and the other kinds are never gated.
    //
    // Neither is a 422 for a DECLARED agent kind that's missing both — see
    // [`apply_agent_platform_safety_valve`]. An INFERRED agent kind in that
    // same shape degrades to `coding` instead: nothing that saves today may
    // start failing a checkpoint the caller never asked to label `agent`
    // just because the server got smarter about filing it.
    let declared_platform = req
        .platform
        .as_deref()
        .and_then(crate::services::platform::canonicalize_platform);
    let platform_detected = crate::services::platform::detect_platform(
        req.client_info.as_ref().map(|ci| ci.name.as_str()),
        req.trusted_user_agent.as_deref(),
    );
    if req.session_id.is_some() {
        match apply_agent_platform_safety_valve(
            session_kind,
            kind_source,
            req.parent_session_id.is_some(),
            declared_platform.as_deref(),
            platform_detected.as_deref(),
        ) {
            Ok(resolved) => {
                if resolved != session_kind {
                    tracing::warn!(
                        session_id = req.session_id.as_deref().unwrap_or_default(),
                        "session_kind inferred as agent (agent_name / parent_session_id / an \
                         agent-framework clientInfo) but no platform was declared or detected; \
                         degrading to coding rather than failing a checkpoint the caller never \
                         labeled agent"
                    );
                }
                session_kind = resolved;
            }
            Err(()) => {
                return Err(IngestError::InvalidCoded {
                    error: format!(
                        "platform is required for session_kind=agent (the request's MCP clientInfo \
                         and User-Agent did not identify a known harness); known values: {}",
                        crate::services::platform::known_platforms_list()
                    ),
                    code: "agent_platform_required",
                });
            }
        }
    }

    // A coral that carries a `session_id` IS a session coral, whatever the
    // caller labelled it — see [`CODING_SESSION_SOURCE_TYPE`] for why this is
    // derived rather than trusted. Before that rule, every captured
    // checkpoint was stamped `"note"`/`"chat"` on the harness primer's
    // instruction, which made it indistinguishable from a hand-typed note in
    // the corals list, the Sources view, ask citations and the by-source
    // breakdown.
    //
    // Capture blocker B1 changed WHICH label is derived, never WHETHER it is:
    // the source of truth moved from "always `coding_session`" to
    // "[`SessionKind::source_type`] of the resolved (declared or inferred,
    // and possibly platform-degraded) kind". With no `session_kind` on the
    // wire and a structural/framework tell absent — every caller that
    // existed before this change: the MCP plugin, the `cs` CLI, the Claude
    // Code harness primer — this is byte-identical to the old line.
    //
    // Deliberately AFTER 0a's platform safety valve, not before: a
    // `session_kind` that gets silently downgraded from `agent` to `coding`
    // there must derive `coding_session`, never `agent_session` — reading
    // `session_kind` here before the valve runs would stamp the pre-degrade
    // kind's label on corals whose row ends up filed under the post-degrade
    // one.
    if req.session_id.is_some() {
        req.source_type = session_kind.source_type().to_string();
    }

    // A parent link only makes sense on an agent session: a child's corals
    // must carry the same `agent_session` label as the tree it rolls up to.
    if req.parent_session_id.is_some() && req.session_id.is_some() && session_kind != SessionKind::Agent
    {
        return Err(IngestError::Invalid("parent_session_id is only valid with session_kind=agent".into()));
    }
    if let (Some(sid), Some(pid)) = (req.session_id.as_deref(), req.parent_session_id.as_deref()) {
        if sid == pid {
            return Err(IngestError::Invalid("parent_session_id must not equal session_id".into()));
        }
    }

    // ── 1. Require a DB pool + store ──────────────────────────────────────────
    // `pool` is still needed for the crypto / job-queue / activity-log helpers
    // (resolve_dek_cached_dispatch, jobs::enqueue, insert_activity_event) that
    // aren't yet behind the Store trait; atom/graph SQL goes through `store`.
    // Both are resolved through the `Tenant` extractor (org-tenancy PR 1.4):
    // Personal → the same `state.db`/`state.store` this always used; Org → the
    // caller's fingerprint-verified tenant pool/store, never the ambient
    // consumer plane.
    let Some(pool) = tenant.pool.clone() else {
        tracing::error!("ingest: no content-plane pool configured");
        return Err(IngestError::Unavailable("service unavailable".into()));
    };
    let Some(store) = &tenant.store else {
        return Err(IngestError::Unavailable("service unavailable".into()));
    };

    // Org-prefixed blob keys (org-tenancy PR 2.4): every blob key built in
    // this request — the chunk atoms AND the raw-transcript blob — derives
    // from this one server-side value (`TenantContext.org_id`, verified by
    // the binding gate; never client input). Personal/flag-off → `None` →
    // legacy 2-segment `{graph_id}/{atom_id}`, byte-identical. Today an Org
    // decision structurally 403s at the reachability check below (ADR-0007
    // D7: ingest lands in personal oceans only, which have no tenant-plane
    // row), so this is `None` in practice — threading it anyway means that
    // if org-graph ingest is ever permitted, keys get the org prefix
    // automatically instead of silently landing unprefixed.
    let blob_org_id: Option<&str> = tenant.org_context().map(|ctx| ctx.org_id.as_str());

    // ── 2. Validate basic input ───────────────────────────────────────────────
    // `source_type` is required EXCEPT on a SESSION capture, where step 0
    // above derives it unconditionally from the declared kind
    // ([`SessionKind::source_type`]) — 422-ing a caller for omitting a field
    // whose value is about to be overwritten anyway would be a contract that
    // only punishes clients for reading the docs correctly.
    if req.source_type.is_empty() && req.session_id.is_none() {
        return Err(IngestError::Invalid("source_type must not be empty".into()));
    }
    if req.source_id.is_empty() {
        return Err(IngestError::Invalid("source_id must not be empty".into()));
    }
    if req.content.trim().is_empty() {
        return Err(IngestError::Invalid("content must not be empty".into()));
    }
    if let Err(e) = validate_provenance_label(&req.project) {
        return Err(IngestError::Invalid(provenance_label_error("project", e)));
    }
    if let Err(e) = validate_provenance_label(&req.mcp_client_name) {
        return Err(IngestError::Invalid(provenance_label_error("mcp_client_name", e)));
    }

    let session_title = truncate_machine_label(req.session_title.clone(), CODING_SESSION_LABEL_CAP);
    let session_repo_path = truncate_machine_label(req.repo_path.clone(), CODING_SESSION_LABEL_CAP);
    let session_hostname = truncate_machine_label(req.hostname.clone(), CODING_SESSION_LABEL_CAP);
    let session_harness_version = truncate_machine_label(
        effective_harness_version(req.harness_version.clone(), req.client_info.as_ref()),
        CODING_SESSION_LABEL_CAP,
    );
    let session_model = truncate_machine_label(req.model.clone(), CODING_SESSION_LABEL_CAP);
    let session_branch = truncate_machine_label(req.branch.clone(), CODING_SESSION_LABEL_CAP);
    let session_agent_name = truncate_machine_label(req.agent_name.clone(), CODING_SESSION_LABEL_CAP);
    let session_task = truncate_machine_label(req.task.clone(), CODING_SESSION_LABEL_CAP);

    // Server-side remote re-normalization — ALWAYS runs, unconditionally,
    // regardless of what the caller claims the value already is (audit
    // P0-1/A5 defense-in-depth: a credential-bearing remote must be
    // structurally unable to reach storage even from a buggy/compromised
    // client that thinks its own value is "already sanitized"). The raw
    // input is truncated first so a pathological length can't do unbounded
    // work inside the normalizer.
    let session_remote_raw_truncated =
        truncate_machine_label(req.remote.clone(), CODING_SESSION_REMOTE_RAW_CAP);
    let session_remote_sanitized = session_remote_raw_truncated
        .as_deref()
        .and_then(crate::project_key::normalize_remote);
    // Fallback: no usable remote → derive the key from the repo folder
    // basename instead (the normalizer's fallback path, never URL-parsed).
    let session_project_key = session_remote_sanitized.clone().or_else(|| {
        session_repo_path
            .as_deref()
            .and_then(basename_for_fallback_key)
            .as_deref()
            .and_then(crate::project_key::normalize_fallback)
    });

    // ── 3. Authorization. Ingest writes to the TARGET graph's own account, not
    // the caller's active account (which is the org account in org context).
    // Per ADR-0007 D7, as amended by member-private-oceans PR 5 (plan §3/§7
    // item 5): ingest always lands in the caller's own PRIVATE ocean on
    // whichever plane they are currently on — the personal ocean on the
    // consumer plane, or their own `kind='member'` ocean on an org tenant
    // plane — never a shared ocean; those are populated via promotion, never
    // direct ingest. See the widened kind gate below (`:720`-area) for the
    // full predicate.
    let ingest_account_id = match store.graph_account_id(req.graph_id).await {
        Ok(Some(a)) => a,
        Ok(None) => {
            return Err(IngestError::Forbidden("graph not found or access denied".into()));
        }
        Err(e) => {
            tracing::error!(error = %e, "DB error checking graph authorization");
            return Err(IngestError::Internal("internal error".into()));
        }
    };
    // Threads the request's REAL decision now (org-tenancy PR 1.4 — both call
    // sites, the HTTP handler and `routes::queries`'s chat-correction
    // auto-save, are migrated to the `Tenant` extractor). ADR-0007 D7, as
    // amended by member-private-oceans PR 5, says ingest ALWAYS lands in the
    // caller's own PRIVATE ocean on whichever plane they are on (enforced a
    // few lines below by the widened `g.kind` check). Under `Personal` that
    // is the consumer-plane personal ocean, unchanged: a personal graph has no
    // row in an org tenant plane's `graphs` table at all, so
    // `ReachabilityResolver` correctly returns `Ok(false)` there without a
    // special case. Under `Org` it is the caller's OWN `kind='member'` ocean —
    // `ReachabilityResolver`'s tenant arm (PR 2) already constrains that to
    // `owner_user_id = user_ctx.user_id AND archived_at IS NULL`, so a
    // `graph_id` naming another member's `kind='member'` ocean, or an archived
    // one, is rejected right here, before the redundant re-check below ever
    // runs. That is the intended v1 "context-scoped only" behavior: ingest from an org
    // subdomain can only ever land in that org's own tenant content, never
    // reach back across the split into the caller's personal ocean — the
    // caller must switch to the apex/personal context to ingest there.
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
            return Err(IngestError::Forbidden("graph not found or access denied".into()));
        }
        Err(e) => {
            tracing::error!(error = %e, "DB error checking graph reachability");
            return Err(IngestError::Internal("internal error".into()));
        }
    }
    // ── 3b. RLS scoping (org-tenancy B.1) ─────────────────────────────────────
    // Everything above is bootstrap/authorization: `graph_account_id` is the
    // PR #284 SECURITY DEFINER carve-out that DISCOVERS the account to scope
    // with, and the reachability check is the deliberate multi-account
    // SECURITY DEFINER carve-out — both run unscoped by design. From here on,
    // every RLS-covered-table statement in this request (graphs/atoms/
    // node_atoms/meeting_transcripts/coding_sessions) runs inside this ONE
    // `begin_account_scoped` transaction on the TARGET graph's own account
    // (`ingest_account_id` — ingest writes to the graph's account, not the
    // caller's active account), committed before the response. The embed job
    // is enqueued in this same transaction (ADR-0016 §4), so it commits or
    // rolls back with the atoms it embeds and a worker can never claim it
    // before they are visible. The meeting-summary job and the activity event
    // run AFTER the commit.
    let mut scoped_tx = match tenant.begin_member_scoped(ingest_account_id).await {
        Ok(tx) => tx,
        Err(e) => {
            tracing::error!(error = %e, "ingest: begin_account_scoped failed");
            return Err(IngestError::Internal("internal error".into()));
        }
    };

    // Fetch once, after authorization, for B4 and D7's destination checks.
    let graph = match store
        .get_graph_scoped(&mut scoped_tx, req.graph_id, ingest_account_id)
        .await
    {
        Ok(Some(g)) => g,
        Ok(None) => {
            return Err(IngestError::Malformed("can only ingest into your own private ocean; share via promotion".into()));
        }
        Err(e) => {
            tracing::error!(error = %e, "DB error fetching graph kind");
            return Err(IngestError::Internal("internal error".into()));
        }
    };

    // D7's destination invariant, via the single shared predicate
    // [`is_own_writable_ocean`] — see that function for why each clause is
    // load-bearing. Extracted (not reworded) so the advisory `writable`
    // flag MCP's `list_oceans` reports cannot drift from the rule enforced
    // here: both call this one function, so there is no second copy to
    // fall out of sync.
    if !is_own_writable_ocean(&graph, user_ctx.user_id) {
        return Err(IngestError::Malformed("can only ingest into your own private ocean; share via promotion".into()));
    }

    // A session lock precedes every source/item/version lock in this writer.
    // No second transaction/pool acquisition while this request holds it.
    let existing_capture_session = if let Some(id) = &req.session_id {
        if crate::capture_context::lock_session_tx(&mut scoped_tx, req.graph_id, id).await.is_err() {
            return Err(IngestError::Internal("session identity unavailable".into()));
        }
        match store.get_coding_session_scoped(&mut scoped_tx, req.graph_id, id).await {
            Ok(row)=>row,
            Err(_)=>return Err(IngestError::Internal("session identity unavailable".into())),
        }
    } else {None};
    if let Some(existing)=&existing_capture_session {
        if existing.session_kind != session_kind.as_str()
            || req.parent_session_id.as_ref().is_some_and(|parent| existing.identity.parent_session_id.as_ref()!=Some(parent)) {
            return Err(IngestError::Conflict("session kind and parent are fixed at first capture".into()));
        }
    }
    let capture_parent = existing_capture_session.as_ref()
        .and_then(|row| row.identity.parent_session_id.clone()).or_else(|| req.parent_session_id.clone());
    let capture_root = if let Some(id)=&req.session_id {
        match crate::capture_context::validate_ancestry_tx(&mut scoped_tx,req.graph_id,id,capture_parent.as_deref()).await {
            Ok(root)=>root,
            Err(_)=>return Err(IngestError::Invalid("session ancestry unavailable, cyclic or too deep".into())),
        }
    } else {None};

    // ── 3c. Agent identity: subagent roll-up (SessionKind::Agent) ────────────
    // Resolved HERE — inside the scoped transaction (the parent row is
    // RLS-covered) and before any write — so a child pointing at nothing is a
    // clean 422 with no atoms committed. A child inherits its ROOT's
    // `agent_name` / `platform` / `platform_detected` and cannot override
    // them (its own declared name is kept as `subagent_name`); `task` may be
    // declared per child and otherwise defaults to the root's. Depth is
    // unbounded: a grandchild rolls up to the same root through its parent's
    // `root_session_id`. A session with no parent is its own root.
    let session_identity: crate::store::SessionIdentity = match (&req.session_id, &capture_parent) {
        (Some(_), Some(parent_id)) => {
            let parent = match store
                .get_coding_session_scoped(&mut scoped_tx, req.graph_id, parent_id)
                .await
            {
                Ok(Some(p)) => p,
                Ok(None) => {
                    return Err(IngestError::InvalidCoded {
                        error: "parent_session_id not found: no session with that id exists \
                                in this ocean (the parent agent must checkpoint first)"
                            .into(),
                        code: "parent_session_not_found",
                    });
                }
                Err(e) => {
                    tracing::error!(error = %e, "DB error resolving parent_session_id");
                    return Err(IngestError::Internal("internal error".into()));
                }
            };
            if parent.session_kind != SessionKind::Agent.as_str() {
                return Err(IngestError::InvalidCoded {
                    error: format!(
                        "parent_session_id must reference an agent session; \
                         \"{}\" is a {} session",
                        parent.session_id, parent.session_kind
                    ),
                    code: "parent_session_not_agent",
                });
            }
            let root_id = capture_root.clone().expect("validated parent root");
            // The root's task is the tree's default; the parent's row already
            // carries the root's task when the parent is a direct child that
            // declared none, but a nested parent may have its own slice, so
            // look the root up when the parent is not it.
            let root_task = if root_id == parent.session_id {
                parent.identity.task.clone()
            } else {
                match store
                    .get_coding_session_scoped(&mut scoped_tx, req.graph_id, &root_id)
                    .await
                {
                    Ok(Some(root)) => root.identity.task,
                    Ok(None) => parent.identity.task.clone(),
                    Err(e) => {
                        tracing::error!(error = %e, "DB error resolving root_session_id");
                        return Err(IngestError::Internal("internal error".into()));
                    }
                }
            };
            crate::store::SessionIdentity {
                agent_name: parent.identity.agent_name.clone(),
                platform: parent.identity.platform.clone(),
                platform_detected: parent.identity.platform_detected.clone(),
                task: session_task.clone().or(root_task),
                parent_session_id: Some(parent.session_id.clone()),
                root_session_id: Some(root_id),
                subagent_name: session_agent_name.clone(),
            }
        }
        (Some(session_id), None) => crate::store::SessionIdentity {
            agent_name: session_agent_name.clone(),
            // Agent kind (declared OR inferred — [`effective_root_platform`]
            // does not distinguish, by design; decision 6): declared wins,
            // detected is the fallback (the 422/downgrade for "neither"
            // already ran in step 0a). Other kinds: declared only — never
            // back-filled from detection, so a coding session's `platform`
            // keeps the canonicalized declared value. Reads fall back to
            // `platform_detected` via `SessionIdentity::effective_platform`,
            // so a coding session that declared nothing still lists, filters
            // and facets under the platform its User-Agent identified.
            platform: effective_root_platform(session_kind, declared_platform.as_deref(), platform_detected.as_deref()),
            platform_detected: platform_detected.clone(),
            task: session_task.clone(),
            parent_session_id: None,
            root_session_id: Some(session_id.clone()),
            subagent_name: None,
        },
        (None, _) => crate::store::SessionIdentity::default(),
    };

    // ── 4. Chunk the content ──────────────────────────────────────────────────
    // `code_path` is the one seam `for_source_type` cannot cover on its own
    // (see `IngestAtomRequest::code_path`'s own doc) — `github_code`'s
    // backfill/event-ingest writers always set it; every other caller leaves
    // it `None` and gets the ordinary `for_source_type` routing unchanged.
    let strategy = crate::ingest::capture::chunk_strategy(
        &req.source_type,
        req.code_path.as_deref(),
        hints.chunk.as_ref(),
    );
    let chunks = chunk(&req.content, strategy);
    let chunks_created = chunks.len();

    // ── 5. Content hash for idempotency ──────────────────────────────────────
    // ONE definition shared with its reader (`services::source_reingest`,
    // the Calendar event-edit fix): this value is also persisted to
    // `atoms.content_hash` at step 7, and a writer/reader drift would make
    // every re-fetched source look edited.
    let content_hash = crate::services::source_reingest::content_hash_hex(&req.content);

    // ── 6. Resolve and validate scope (default to "private") ─────────────────
    let scope = req.scope.as_deref().unwrap_or("private");
    if scope != "private" && scope != "account" {
        return Err(IngestError::Invalid("scope must be \"private\" or \"account\"".into()));
    }

    // ── 6b. Resolve and validate fts_language (default to "english") ──────────
    let fts_language = req.fts_language.as_deref().unwrap_or("english");
    if !ALLOWED_LANGUAGES.contains(&fts_language) {
        return Err(IngestError::Malformed(format!(
                    "fts_language \"{}\" is not supported; allowed values: {}",
                    fts_language,
                    ALLOWED_LANGUAGES.join(", ")
                ).into()));
    }

    // ── 6a. Resolve account DEK (once per request) ────────────────────────────
    // Use resolve_dek_cached[_dispatch] so that repeat requests within the
    // 5-minute TTL skip the DB round-trip. Infrastructure failures (missing
    // KEK, DB down) return 503 while data-level failures (bad wrapped key
    // bytes) return 500. Dispatches on tenancy (org-tenancy PR 1.4): before
    // member-private-oceans PR 5, the `g.kind == "personal"` check above meant
    // `ingest_account_id` could only ever be a personal (consumer-plane)
    // account here, so the `Org` arm below was unreachable in practice. PR 5's
    // widened `kind IN ("personal", "member")` gate makes the `Org` arm a REAL
    // path: a `kind='member'` ingest resolves `ingest_account_id` to the ORG
    // account, and `resolve_dek_cached_dispatch`'s `Org` arm correctly reads
    // the org's own DEK off `tenant.decision`'s `TenantContext` rather than the
    // consumer pool. This is exactly why dispatching explicitly (rather than
    // hard-coding `resolve_dek_cached`) mattered even before it was exercised —
    // and it fails closed (`CryptoError::TenantMismatch`) if the decision and
    // `ingest_account_id` were ever to disagree.
    let dek: [u8; 32] = match resolve_dek_cached_dispatch(
        tenant.decision.as_ref(),
        &pool,
        ingest_account_id,
        &state.dek_cache,
    )
    .await
    {
        Ok(Some(d)) => d,
        Ok(None) => {
            // Account exists but has no DEK — provisioning incomplete.
            tracing::warn!(account_id = %ingest_account_id, "account has no DEK — run key rotation first");
            return Err(IngestError::Unavailable("encryption service unavailable".into()));
        }
        Err(ref e @ (CryptoError::MasterKekNotSet(_) | CryptoError::MasterKekInvalid(_) | CryptoError::DbError(_))) => {
            tracing::warn!(error = %e, "master KEK or DB error resolving DEK");
            return Err(IngestError::Unavailable("encryption service unavailable".into()));
        }
        Err(e) => {
            tracing::warn!(error = %e, "DEK resolution failed");
            return Err(IngestError::Internal("encryption error".into()));
        }
    };

    // ── 6c. Contextual chunk header (cs-phase0-chunk-headers) ─────────────────
    // Computed once per request (every chunk shares the same meeting/session)
    // and prepended to each chunk's persisted text below, so it's embedded AND
    // tsvectored along with the body — see `build_contextual_header`'s doc
    // comment for source-type scoping. The capture core records its byte
    // length (including the "\n\n" separator) in `atoms.header_len` so
    // citation/preview code can strip it back out (`strip_header_prefix`).
    //
    // Coding-session checkpoints (`session_id.is_some()`) get a DIFFERENT
    // header — `build_coding_session_header` (audit A3) — carrying
    // session_title/repo_path/branch instead of the meeting-shaped one, so
    // this per-checkpoint context is embedded/FTS-indexed text rather than a
    // new `atoms` column (D4 reject). The two headers are mutually exclusive:
    // `add_context` never sets both `session_id` and a meeting-shaped
    // `source_type` in the same call.
    let participants = merge_chunk_speakers(&req.participants, &chunks);
    let header = if req.session_id.is_some() {
        coralswarm_core::chunker::build_coding_session_header(
            session_title.as_deref(),
            session_repo_path.as_deref(),
            session_branch.as_deref(),
        )
    } else {
        build_contextual_header(
            &req.source_type,
            req.raw_content.as_deref(),
            req.timestamp,
            &participants,
        )
        .or_else(|| {
            if coralswarm_core::chunker::provenance_source(&req.source_type) {
                coralswarm_core::chunker::build_provenance_header(
                    None,
                    Some(req.timestamp),
                    req.project.as_deref(),
                )
            } else {
                None
            }
        })
    };

    // Captured context makes session identity load-bearing for every cloud
    // session kind. Establish/read back first-fixed lineage BEFORE any source
    // snapshot or R2 upload; a failure rolls back without orphan-success.
    let fixed_capture_session = if let Some(session_id)=&req.session_id {
        let stored = match store.upsert_coding_session_scoped(
            &mut scoped_tx,req.graph_id,session_id,session_kind.as_str(),
            session_project_key.as_deref(),session_title.as_deref(),session_repo_path.as_deref(),
            session_remote_sanitized.as_deref(),session_hostname.as_deref(),
            session_harness_version.as_deref(),session_model.as_deref(),session_branch.as_deref(),&session_identity,
        ).await {
            Ok(kind)=>kind,
            Err(_)=>return Err(IngestError::Internal("session identity could not be preserved; retry ingest".into())),
        };
        if stored!=session_kind.as_str() {
            return Err(IngestError::Conflict("session kind is fixed at first capture".into()));
        }
        match store.get_coding_session_scoped(&mut scoped_tx,req.graph_id,session_id).await {
            Ok(Some(row)) if row.identity.parent_session_id==capture_parent && row.identity.root_session_id.as_ref().is_none_or(|id|Some(id)==capture_root.as_ref())=>Some(row),
            _=>return Err(IngestError::Conflict("session lineage changed concurrently; retry capture".into())),
        }
    } else {None};

    // Preserve the admitted text before writing retrieval chunks. Source locks
    // precede version/chunk locks everywhere, including concurrent deletion.
    // The generic HTTP shape does not prove a connector/mailbox identity:
    // declare that limitation explicitly rather than guessing a namespace.
    let is_transcript = matches!(ChunkStrategy::for_source_type(&req.source_type), ChunkStrategy::Transcript);
    let source_text = if is_transcript {
        req.raw_content.as_deref().filter(|s| !s.trim().is_empty())
            .unwrap_or(req.content.as_str())
    } else {
        req.content.as_str()
    };
    let mut source_metadata = json!({
        "channel_id": req.channel_id, "channel_name": req.channel_name,
        "author_id": req.author_id, "author_name": req.author_name,
        "project": req.project, "session_id": req.session_id,
        "session_title": session_title, "session_kind": req.session_id.as_ref().map(|_| session_kind.as_str()),
        "branch": session_branch, "project_key": session_project_key,
        "capture_extent": if req.session_id.is_some() { "checkpoint" } else { "source_item" },
    });
    let captured_identity = fixed_capture_session.as_ref().map(|session| &session.identity);
    let captured_context=crate::capture_context::CaptureContext {
        revision:1, session_id:req.session_id.clone(),
        session_kind:fixed_capture_session.as_ref().map(|s|s.session_kind.clone()),
        session_title:session_title.clone(), project:req.project.clone(), project_key:session_project_key.clone(),
        repo_path:session_repo_path.clone(),remote_sanitized:session_remote_sanitized.clone(),
        branch:session_branch.clone(),hostname:session_hostname.clone(),
        harness_version:session_harness_version.clone(),model:session_model.clone(),
        agent_name:captured_identity.map_or_else(||session_agent_name.clone(),|identity|identity.agent_name.clone()),
        platform:captured_identity.map_or_else(||declared_platform.clone(),|identity|identity.platform.clone()),
        platform_detected:captured_identity.map_or_else(||platform_detected.clone(),|identity|identity.platform_detected.clone()),
        task:captured_identity.map_or_else(||req.task.clone(),|identity|identity.task.clone()),
        subagent_name:captured_identity.and_then(|identity|identity.subagent_name.clone()),
        parent_session_id:captured_identity.and_then(|identity|identity.parent_session_id.clone()),
        root_session_id:captured_identity.and_then(|identity|identity.root_session_id.clone()).or(capture_root),
    };
    if captured_context.validate().is_err() {
        return Err(IngestError::Invalid("invalid reported capture context".into()));
    }
    source_metadata["capture_context"]=json!(captured_context);
    // A trusted connector's anchor (title, issue key, permalink): the keys
    // source extraction reads to name the source in every atom.
    hints.record_anchor(&mut source_metadata);
    // Demo classification is a content fact, supplied only by trusted adapter
    // code. Other mutable attribution stays outside the content fingerprint.
    if origin.is_some_and(|o|o.transport == crate::source_origin::CaptureTransport::Synthetic) {
        source_metadata["synthetic_demo"] = json!(true);
    }
    let declared_namespace = format!("declared:user:{}", user_ctx.user_id);
    let capture = crate::source_evidence::CaptureSource {
        source_type: &req.source_type,
        provider_namespace: provider_namespace.unwrap_or(&declared_namespace),
        source_id: &req.source_id,
        normalized_text: source_text,
        representation: if req.session_id.is_some() { "generated_summary" } else if is_transcript && req.raw_content.as_ref().is_some_and(|raw|!raw.trim().is_empty()) { "original_text" } else { "normalized_text" },
        metadata: &source_metadata,
        occurred_at: Some(req.timestamp), participants: &participants,
        ingested_by: Some(user_ctx.user_id), scope,
        duration_ms: req.duration_ms,
        segments: req.segments.as_ref().filter(|v| v.as_array().is_some_and(|a| !a.is_empty())),
    };
    if !crate::source_evidence::still_admitted_in_tx_for_capture(state,tenant,user_ctx,req.graph_id,ingest_account_id,&mut scoped_tx).await {
        return Err(IngestError::Forbidden("capture authorization changed".into()));
    }
    let mut upload_journal=match crate::capture_upload_cleanup::UploadJournal::begin(&mut scoped_tx,tenant,ingest_account_id,req.graph_id).await {
        Ok(journal)=>journal,
        Err(_)=>return Err(IngestError::Internal("capture cleanup could not be prepared".into())),
    };
    let cleanup_blobs=state.r2.as_deref().map(|r|r as &dyn crate::services::blob_store::PromotionBlobStore);
    // ── 7–8c. The capture core: version, chunk atoms (fallback titles,
    // excerpts, symbols, provenance, header length), bodies, reconcile/links
    // and the embed job, all in this request's scoped transaction. See
    // [`capture`]. An upload failure aborts the capture (ADR-0016 §4).
    let prepared = crate::ingest::capture::Prepared::new(
        &req.source_id,
        &req.source_type,
        &req.content,
        &chunks,
        &match &header {
            Some(h) => crate::ingest::capture::Header::Recorded(h.clone()),
            None => crate::ingest::capture::Header::None,
        },
        hints.title.as_deref(),
    );
    let atom_fields = crate::ingest::capture::AtomFields {
        scope,
        fts_language,
        content_hash: Some(&content_hash),
        channel_id: req.channel_id.as_deref(),
        channel_name: req.channel_name.as_deref(),
        author_id: req.author_id.as_deref(),
        author_name: req.author_name.as_deref(),
        project: req.project.as_deref(),
        mcp_client_name: req.mcp_client_name.as_deref(),
        ..crate::ingest::capture::AtomFields::new(&req.source_type, &participants, req.timestamp, Some(user_ctx.user_id))
    };
    let stored = match crate::ingest::capture::store_capture_tx(
        &mut scoped_tx,
        crate::ingest::capture::Destination {
            graph_id: req.graph_id,
            account_id: ingest_account_id,
            org_id: blob_org_id,
            dek: &dek,
            r2: state.r2.as_deref(),
        },
        &crate::ingest::capture::CaptureRequest {
            source: capture,
            origin,
            chunks: &prepared,
            atoms: atom_fields,
            embed_key: embed_key(req.graph_id, &req.source_type, &req.source_id, &content_hash),
            sync_ops: Some(store.as_ref()),
        },
        &mut upload_journal.keys,
    )
    .await
    {
        Ok(stored) => stored,
        Err(e) => {
            let cleanup_status = upload_journal.rollback_and_retain(scoped_tx, tenant, cleanup_blobs).await;
            if e.rls_refused {
                // SQLSTATE 42501 here is Postgres refusing the row under
                // RLS — under member scope that means this actor may not
                // write to this ocean, or (m365 enterprise train PR 4.1,
                // migration 0064) the supplied `timestamp` predates the
                // actor's `history_from` window for it: the INSERT's
                // RETURNING must be able to SEE the new row, and the
                // windowed SELECT policy hides pre-window rows. A caller
                // mistake, not a server fault — 403, not 500.
                tracing::warn!(error = %e, graph_id = %req.graph_id, "atom insert refused by row-level security");
                return Err(IngestError::Forbidden("not permitted to write this content to this \
                                          ocean (wrong ocean, or the content's timestamp \
                                          predates your access window)".into()));
            }
            let e = match IngestError::capture(e, cleanup_status) {
                IngestError::Capture(e) => e,
                outage => {
                    tracing::warn!(error = %outage, graph_id = %req.graph_id, "ingest capture aborted: object store unavailable");
                    return Err(outage);
                }
            };
            tracing::error!(error = %e, "ingest capture failed");
            let error = match e.stage {
                "source version" => "source could not be preserved; retry ingest",
                "chunk reconcile" => "source evidence could not be preserved; retry ingest",
                _ => "internal error",
            };
            return Err(IngestError::Uncommitted { error, authorization_lapsed: false, cleanup_status });
        }
    };
    let new_atom_ids: Vec<Uuid> = stored.inserted.clone();

    // ── 8a. Anchor onto cited-coral reefs (chat correction) ──────────────────
    // `req.content` is never mutated before this point, so it is still the
    // exact statement `classify_capture` produced.
    apply_anchors(store.as_ref(), &mut scoped_tx, req.graph_id, &req.content, &new_atom_ids, anchor_atom_ids).await;

    // Existing positions this request did not rewrite (unchanged, or not
    // this source's to rewrite).
    let skipped_duplicate_chunks = stored.duplicates();
    let replaced_chunks = stored.reconciled.replaced.len();
    let retired_chunks = stored.reconciled.retired.len();
    let written_atom_ids = stored.written();

    let (processing_status, processing_reason) = processing_status(&mut scoped_tx, req.graph_id, stored.version_id).await;
    // The summary job (8f) runs on the managed_enrichment purpose
    // (`workers::summary`): the transcript row stays `pending` until the
    // worker claims it, and a managed hold hands it back as `pending`.
    if !crate::source_evidence::still_admitted_in_tx_for_capture(state,tenant,user_ctx,req.graph_id,ingest_account_id,&mut scoped_tx).await {
        let cleanup_status=upload_journal.rollback_and_retain(scoped_tx,tenant,cleanup_blobs).await;
        return Err(IngestError::Uncommitted {
            error: "capture authorization changed; evidence not committed",
            authorization_lapsed: true,
            cleanup_status,
        });
    }
    // Release the capture transaction before retry persistence/transport. An
    // I/O COMMIT failure is independently withheld unless this database proves
    // aborted; committed evidence is never removed on reference absence alone.
    if let Err(e) = scoped_tx.commit().await {
        let cleanup_status=upload_journal.retain_and_attempt(tenant,cleanup_blobs).await;
        tracing::error!(error = %e, "ingest: scoped transaction commit failed");
        return Err(IngestError::Uncommitted { error: "internal error", authorization_lapsed: false, cleanup_status });
    }

    // ── 8f. Post-commit: the meeting-summary job ──────────────────────────────
    enqueue_meeting_summary(&pool, &stored, req.graph_id, &req.source_id, ingest_account_id).await;

    // ── 10. Fire-and-forget activity event ───────────────────────────────────
    // Only when at least one atom was actually inserted. `new_atom_ids` is
    // the post-R2-upload, post-`ON CONFLICT DO NOTHING` set from step 7/8
    // above (`insert_atoms_scoped` only returns rows it actually inserted),
    // so it is the true "did anything land" signal — not a proxy like
    // `chunks_created`, which counts chunks the caller *sent*, duplicates
    // included. A fully-duplicate re-ingest (every chunk collides with an
    // existing `(graph_id, source_type, source_id)` row) must emit NO event:
    // before this guard, the emitter fired unconditionally with
    // `atom_count: 0`, and a duplicate-heavy re-ingest storm (e.g. repeated
    // Teams-channel syncs) produced dozens of zero-content "N items
    // captured" journal entries for hours where nothing was actually
    // ingested (ticket 18). Pre-existing zero-count rows are left to age out
    // of the 14-day journal window naturally rather than being purged — the
    // journal only ever reads the trailing 14 days, so a one-off backfill
    // buys nothing a few days of retention doesn't already erase, and a
    // purge would need its own care not to touch legitimate `atom_count: 0`
    // rows from a source with no atom-producing content.
    if !written_atom_ids.is_empty() {
        insert_activity_event(
            pool.clone(),
            ingest_account_id,
            user_ctx.user_id,
            Some(req.graph_id),
            "atom_ingested",
            serde_json::json!({
                "atom_count": written_atom_ids.len(),
                "chunks_created": chunks_created,
                "source_type": req.source_type,
            }),
        );
    }

    // ── 11. Return response ───────────────────────────────────────────────────
    Ok(Ingested {
        body: IngestAtomResponse {
            atom_ids: written_atom_ids,
            jobs_enqueued: stored.enqueued_in_transaction,
            chunks_created,
            skipped_duplicate_chunks,
            replaced_chunks,
            retired_chunks,
            processing_status,
            processing_reason,
        },
        linked: stored.reconciled.linked.len(),
    })
}

#[cfg(feature = "test-support")]
#[doc(hidden)]
pub async fn test_ingest_with_source_context(
    state: &Arc<AppState>, tenant: &Tenant, user: &UserContext, request: IngestAtomRequest, namespace: &str,
) -> Response {
    submit(IngestRequest::private(Member { provider_namespace: Some(namespace), ..Member::new(state, tenant, user, request) }))
        .await
        .into_response()
}

#[cfg(feature = "test-support")]
#[doc(hidden)]
pub async fn test_ingest_with_origin_context(
    state: &Arc<AppState>, tenant: &Tenant, user: &UserContext, request: IngestAtomRequest,
    namespace: Option<&str>, origin: &SourceOrigin,
) -> Response {
    submit(IngestRequest {
        origin: Some(origin),
        ..IngestRequest::private(Member { provider_namespace: namespace, ..Member::new(state, tenant, user, request) })
    })
    .await
    .into_response()
}

/// Test seam: a routed capture pauses here, after its rows and uploads and
/// before the grant check and commit, so a test can act on the routing row
/// while the capture is open. Set through [`BEFORE_SEAL`]; absent in
/// production builds.
#[cfg(test)]
#[derive(Default)]
pub(super) struct Pause {
    /// The capture has reached the seam.
    pub(super) reached: tokio::sync::Notify,
    /// The test lets it go on to the seal.
    pub(super) resume: tokio::sync::Notify,
}

#[cfg(test)]
tokio::task_local! {
    pub(super) static BEFORE_SEAL: Arc<Pause>;
}

/// [`submit`] for a [`Routed`] capture: the capture core in one system-scoped
/// transaction on the grant's account, then the grant check as the last
/// statement before the commit. An upload failure aborts, and the embed job
/// commits with the capture (ADR-0016 §4).
///
/// The check runs last on purpose. It share-locks the routing row, and the
/// lock is held only from the check to the commit, never across the capture's
/// uploads: the row's own bookkeeping (a backfill cursor, an attempt stamp,
/// an error code) does not queue behind an open capture, and an admin's
/// re-point waits only for that instant. A route that changed while the
/// capture ran fails the check and the whole capture rolls back, so nothing
/// lands under a routing that no longer exists. What the capture did before
/// the check (rows, uploads) is wasted for a refused write; refusals are rare
/// and the uploads are cleaned up below.
async fn submit_routed(
    routed: Routed<'_>,
    anchor_atom_ids: &[Uuid],
    origin: Option<&SourceOrigin>,
) -> Result<Ingested, IngestError> {
    let Routed { ocean, grant, writer, source, chunks, atoms, content_hash, sync_ops } = routed;
    if source.provider_namespace != grant.namespace() {
        return Err(IngestError::Invalid(format!(
            "source namespace {} is not its grant's ({})",
            source.provider_namespace,
            grant.namespace()
        )));
    }
    // The in-memory half of the grant check, before any work: an ocean the
    // grant never named needs no transaction to refuse.
    if !grant.oceans().contains(&ocean) {
        return Err(IngestError::Forbidden(format!("no routing grant allows writing to ocean {ocean}")));
    }
    let (source_type, source_id, text) = (source.source_type, source.source_id, source.normalized_text);
    let account_id = grant.account_id();
    let org_id = match writer.tenant {
        TenantId::Consumer => None,
        TenantId::Org(org) => Some(org.as_str()),
    };
    let mut uploads = Vec::new();
    let mut commit_outcome_unknown = false;
    let result = async {
        let mut tx = crate::tenancy::scope::begin_tenant_system_scoped(writer.pool, account_id)
            .await
            .map_err(|e| IngestError::Internal(format!("scoped tx: {e}")))?;
        let stored = crate::ingest::capture::store_capture_tx(
            &mut tx,
            crate::ingest::capture::Destination { graph_id: ocean, account_id, org_id, dek: writer.dek, r2: writer.r2 },
            &crate::ingest::capture::CaptureRequest {
                source,
                origin,
                chunks,
                atoms,
                embed_key: embed_key(ocean, source_type, source_id, content_hash),
                sync_ops,
            },
            &mut uploads,
        )
        .await
        .map_err(IngestError::Capture)?;
        if !anchor_atom_ids.is_empty() {
            let store = crate::store::PostgresStore::new(writer.pool.clone());
            apply_anchors(&store, &mut tx, ocean, text, &stored.inserted, anchor_atom_ids).await;
        }
        let (processing_status, processing_reason) = processing_status(&mut tx, ocean, stored.version_id).await;
        #[cfg(test)]
        if let Ok(pause) = BEFORE_SEAL.try_with(Arc::clone) {
            pause.reached.notify_one();
            pause.resume.notified().await;
        }
        // The seal: the routing row, share-locked from here to the commit.
        match grant.verify_tx(&mut tx, ocean).await {
            Ok(true) => {}
            Ok(false) => return Err(IngestError::Forbidden(format!("no routing grant allows writing to ocean {ocean}"))),
            Err(e) => return Err(IngestError::Internal(format!("routing grant check: {e}"))),
        }
        if let Err(e) = tx.commit().await {
            commit_outcome_unknown = !matches!(&e, sqlx::Error::Database(_));
            return Err(IngestError::Internal(format!("commit: {e}")));
        }
        enqueue_meeting_summary(writer.pool, &stored, ocean, source_id, account_id).await;
        let atom_ids = stored.written();
        Ok(Ingested {
            body: IngestAtomResponse {
                jobs_enqueued: stored.enqueued_in_transaction,
                atom_ids,
                chunks_created: chunks.len(),
                skipped_duplicate_chunks: stored.duplicates(),
                replaced_chunks: stored.reconciled.replaced.len(),
                retired_chunks: stored.reconciled.retired.len(),
                processing_status,
                processing_reason,
            },
            linked: stored.reconciled.linked.len(),
        })
    }
    .await;
    let mut cleanup_status = "retained";
    if result.is_err() && !commit_outcome_unknown {
        let gone = crate::services::blob_store::cleanup_uncommitted_uploads(writer.r2, &uploads).await;
        cleanup_status = if gone { "completed" } else { "incomplete" };
    }
    result.map_err(|e| match e {
        IngestError::Capture(e) => IngestError::capture(e, cleanup_status),
        other => other,
    })
}

/// An existing source version to re-chunk in place, and the keys its
/// bodies are sealed with. See [`rechunk_existing_version_tx`].
#[derive(Clone, Copy)]
pub(crate) struct ExistingVersion<'a> {
    pub(crate) graph_id: Uuid,
    /// Owner account of `graph_id` (the DEK's account).
    pub(crate) account_id: Uuid,
    /// Blob-key org segment (`None` on the consumer plane).
    pub(crate) org_id: Option<&'a str>,
    pub(crate) dek: &'a [u8; 32],
    /// `None` = dev/inline mode.
    pub(crate) r2: Option<&'a coralswarm_core::r2::R2Client>,
    pub(crate) source_item_id: Uuid,
    pub(crate) version_id: Uuid,
    /// Source id before `:chunk:N`.
    pub(crate) source_id: &'a str,
}

/// Maintenance, not ingest: re-chunk a source version that is already
/// stored, with today's chunker, without minting a version or queueing
/// extraction (the version's text is unchanged, only its chunking is). The
/// recovery for chunks over the embedder's input limit
/// (`chunk_reconcile::backfill::rechunk_oversized`, its only caller). It
/// inserts and replaces chunk atoms, so it is the door's like [`submit`]:
/// the core it runs ([`capture::rechunk_version_tx`]) is private to `ingest`,
/// and `tests/capture_funnel_guard.rs` admits only the backfill as a caller.
///
/// The caller holds the source's identity lock, has checked the version is
/// still current, and owns the commit; `uploads` lists every object written,
/// for its rollback cleanup.
pub(crate) async fn rechunk_existing_version_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    version: ExistingVersion<'_>,
    prepared: &Prepared,
    atoms: &AtomFields<'_>,
    uploads: &mut Vec<String>,
) -> Result<crate::chunk_reconcile::Reconciled, capture::FunnelError> {
    let dest = capture::Destination {
        graph_id: version.graph_id,
        account_id: version.account_id,
        org_id: version.org_id,
        dek: version.dek,
        r2: version.r2,
    };
    capture::rechunk_version_tx(tx, dest, version.source_item_id, version.version_id, version.source_id, prepared, atoms, uploads)
        .await
}

/// The version's extraction state, as the ingest response reports it.
async fn processing_status(
    tx: &mut sqlx::Transaction<'static, sqlx::Postgres>,
    graph_id: Uuid,
    version_id: Uuid,
) -> (String, Option<&'static str>) {
    let status: String = sqlx::query_scalar("SELECT extraction_status FROM source_versions WHERE graph_id=$1 AND version_id=$2")
        .bind(graph_id)
        .bind(version_id)
        .fetch_one(&mut **tx)
        .await
        .unwrap_or_else(|_| "unknown".into());
    let reason = (status == "blocked").then_some("managed_admission_required");
    (status, reason)
}

/// Enqueue `GenerateMeetingSummary` for a meeting-kind source whose content
/// changed: a transcript the capture projected into `meeting_transcripts`
/// (`source_evidence` decides that by source type and representation). Runs
/// after the commit (`jobs` is not RLS-covered), so the worker can never claim
/// it before the transcript row is visible. The summary worker runs on the
/// managed_enrichment purpose: the transcript row stays `pending` until it
/// claims the job, and a managed hold hands it back as `pending`.
async fn enqueue_meeting_summary(
    pool: &PgPool,
    stored: &crate::ingest::capture::Stored,
    graph_id: Uuid,
    source_id: &str,
    account_id: Uuid,
) {
    if !(stored.version_changed && stored.meeting_projection) {
        return;
    }
    let key = format!("summary:{graph_id}:{source_id}");
    if let Err(e) = jobs::queue::enqueue(
        pool,
        &jobs::JobKind::GenerateMeetingSummary { graph_id, source_id: source_id.to_string(), allow_cloud_override: false },
        Some(account_id),
        Some(&key),
        0,
    )
    .await
    {
        tracing::warn!(error = %e, "failed to enqueue GenerateMeetingSummary job");
    }
}

/// Link the new atoms onto the anchors' reefs and, for an anchored
/// correction, write its supersession edges. Both are best-effort and no-ops
/// without anchors.
async fn apply_anchors(
    store: &dyn Store,
    tx: &mut sqlx::Transaction<'static, sqlx::Postgres>,
    graph_id: Uuid,
    content: &str,
    new_atom_ids: &[Uuid],
    anchor_atom_ids: &[Uuid],
) {
    anchor_atoms_to_reefs_scoped(store, tx, graph_id, new_atom_ids, anchor_atom_ids).await;
    if let Some(&seed_atom_id) = new_atom_ids.first() {
        write_anchored_supersession_edges_scoped(store, tx, graph_id, content, seed_atom_id, anchor_atom_ids).await;
    }
}

// ── Best-effort-on-a-shared-transaction SAVEPOINT discipline ────────────────
//
// Every best-effort step of [`submit`] runs against its ONE
// request-scoped `scoped_tx` (org-tenancy B.1) and is documented as
// "best-effort — a failure here never fails the ingest". Postgres does not
// actually support that on a bare transaction: ANY statement error aborts the
// WHOLE transaction (25P02 `in_failed_sql_transaction`), so a `if let Err(e) =
// ... { warn!(...) }` that does not roll back does not skip the failure — it
// silently poisons every later statement on `scoped_tx`, including the
// header_len UPDATE, the provenance stamp, the transcript upsert, and the
// final COMMIT, discarding atoms whose blobs are already durable in R2 and
// turning "one best-effort feature degraded" into "the whole ingest 500s".
//
// The fix used here is the same SAVEPOINT/ROLLBACK TO
// SAVEPOINT discipline `Store::link_node_atom_scoped` and
// `batch_apply_remote_ops_scoped` already use for exactly this reason: open a
// named savepoint before the step's writes, RELEASE it on success (folding
// the writes into the outer transaction, pending its own COMMIT), or ROLLBACK
// TO it on failure (discarding only this step's partial writes and leaving
// the outer transaction healthy for everything after it).
//
// `$txref` must be an expression of type `&mut sqlx::Transaction<'static,
// sqlx::Postgres>` — the `tx: &mut Transaction` parameter the two helper
// functions below carry.
macro_rules! open_best_effort_savepoint {
    ($txref:expr, $sp:literal, $step:literal) => {{
        match sqlx::query(concat!("SAVEPOINT ", $sp))
            .execute(&mut **$txref)
            .await
        {
            Ok(_) => true,
            Err(e) => {
                tracing::warn!(error = %e, step = $step, "failed to establish savepoint for best-effort ingest step; skipping");
                false
            }
        }
    }};
}

macro_rules! release_best_effort_savepoint {
    ($txref:expr, $sp:literal, $step:literal) => {{
        if let Err(e) = sqlx::query(concat!("RELEASE SAVEPOINT ", $sp))
            .execute(&mut **$txref)
            .await
        {
            tracing::warn!(error = %e, step = $step, "failed to release savepoint");
        }
    }};
}

macro_rules! rollback_best_effort_savepoint {
    ($txref:expr, $sp:literal, $step:literal, $err:expr) => {{
        tracing::warn!(error = %$err, step = $step, "best-effort ingest step failed — rolling back to savepoint");
        if let Err(e2) = sqlx::query(concat!("ROLLBACK TO SAVEPOINT ", $sp))
            .execute(&mut **$txref)
            .await
        {
            tracing::error!(error = %e2, step = $step,
                "failed to roll back savepoint after best-effort step failure — outer transaction may be poisoned");
        } else if let Err(e2) = sqlx::query(concat!("RELEASE SAVEPOINT ", $sp))
            .execute(&mut **$txref)
            .await
        {
            tracing::warn!(error = %e2, step = $step, "failed to release savepoint after rollback");
        }
    }};
}

/// Link the freshly-created atoms onto every reef **in `graph_id`** that any
/// `anchor_atom_ids` coral already belongs to. The `graph_id` scope is enforced by
/// `node_ids_for_atoms_scoped` (tenant boundary): anchors can only reach reefs in
/// the ocean the new atoms landed in. Best-effort and idempotent: a failure to
/// anchor never fails the ingest, since the atom + its embedding are already
/// durable and entity extraction will still link the obvious entities.
///
/// Runs on the caller's `begin_account_scoped` transaction (org-tenancy B.1) —
/// `node_atoms`/`atoms`/`nodes` are all RLS-covered, so the ingest path threads
/// its request transaction through here.
///
/// **SAVEPOINT, not a bare best-effort write on the shared `tx`.** This step
/// (like every other "best-effort, never fails the ingest" step in this file)
/// used to run its store calls directly against the caller's shared
/// `scoped_tx` and just log-and-return on error. That does NOT actually skip
/// the failure: Postgres aborts the WHOLE transaction on any statement error
/// (25P02 `in_failed_sql_transaction`), so every later statement on the same
/// `scoped_tx` — the header_len UPDATE, the provenance stamp, the transcript
/// upsert, and the final COMMIT — would also fail, discarding atoms whose
/// blobs are already durable in R2 and turning "one best-effort feature
/// degraded" into "the whole ingest 500s". A `SAVEPOINT`/`ROLLBACK TO
/// SAVEPOINT` pair (the same discipline `Store::link_node_atom_scoped` and
/// `batch_apply_remote_ops_scoped` already use for exactly this reason) scopes
/// a failure to just this step's own writes and leaves the outer transaction
/// healthy for everything after it.
async fn anchor_atoms_to_reefs_scoped(
    store: &dyn crate::store::Store,
    tx: &mut sqlx::Transaction<'static, sqlx::Postgres>,
    graph_id: Uuid,
    new_atom_ids: &[Uuid],
    anchor_atom_ids: &[Uuid],
) {
    if new_atom_ids.is_empty() || anchor_atom_ids.is_empty() {
        return;
    }
    if !open_best_effort_savepoint!(tx, "anchor_atoms_to_reefs_sp", "anchor") {
        return;
    }

    let node_ids = match store
        .node_ids_for_atoms_scoped(tx, graph_id, anchor_atom_ids)
        .await
    {
        Ok(ids) => ids,
        Err(e) => {
            rollback_best_effort_savepoint!(tx, "anchor_atoms_to_reefs_sp", "anchor:node_ids", e);
            return;
        }
    };
    if node_ids.is_empty() {
        release_best_effort_savepoint!(tx, "anchor_atoms_to_reefs_sp", "anchor:noop");
        return;
    }
    let pairs: Vec<(Uuid, Uuid)> = node_ids
        .iter()
        .flat_map(|n| new_atom_ids.iter().map(move |a| (*n, *a)))
        .collect();
    if let Err(e) = store.link_node_atoms_scoped(tx, &pairs).await {
        rollback_best_effort_savepoint!(tx, "anchor_atoms_to_reefs_sp", "anchor:link", e);
        return;
    }
    release_best_effort_savepoint!(tx, "anchor_atoms_to_reefs_sp", "anchor:done");
    tracing::debug!(
        reefs = node_ids.len(),
        atoms = new_atom_ids.len(),
        "anchor: linked correction atoms onto cited-coral reefs"
    );
}

/// graph-temporal-memory plan §3.4a: write supersession edges **directly from
/// a chat-originated correction's anchors, deterministically, at ingest
/// time** — no blocking, no cosine ranking, no LLM adjudication. §3.4's
/// background conflict-detection worker exists for drift discovered IN THE
/// CORPUS; this path already knows exactly which reefs it contradicts,
/// because `anchor_atom_ids` is sourced from what the corrected answer cited
/// (`routes::queries::save_correction_note`), so it bypasses §3.4 entirely.
///
/// Only ever does anything when `anchor_atom_ids` is non-empty — the SAME
/// in-process-only correction signal [`anchor_atoms_to_reefs_scoped`] gates
/// on; see that function's doc comment. The HTTP path always passes `&[]`.
///
/// The correction statement (`statement` — `req.content`, already the
/// self-contained declarative sentence `classify_capture` produced) becomes
/// its own `type = 'decision'` claim reef (§3.3: the widened extraction
/// prompt's "durable assertion", written here synchronously and
/// deterministically rather than waiting on the async LLM extraction pass
/// this same atom will ALSO go through — `create_decision_candidate_and_link_atom_scoped`
/// is idempotent on `(graph_id, 'decision', lower(title))`, so a later
/// re-extraction producing the identical statement folds into this same
/// reef rather than duplicating it). That new claim reef then supersedes
/// every reef `anchor_atom_ids` resolves to.
///
/// **`kind` vocabulary (§3.4a's own warning, verbatim):** `classify_capture`'s
/// `is_correction: true` is conversation-scoped ("this message fixes
/// something said earlier") and is NOT the same predicate as
/// `supersession_edge.kind = 'correction'` ("never true"). `kind` therefore
/// defaults to [`crate::store::SupersessionKind::Change`] and escalates to
/// [`crate::store::SupersessionKind::Correction`] ONLY on explicit correction
/// language in `statement`, via
/// [`crate::services::supersession::classify_supersession_kind`] — the SAME
/// classifier §3.5's other producer uses. One rule, applied at two sites, so
/// "we no longer use Fly.io" (a real change) and "it's actually Elmbrook, not
/// Elmbrok" (a correction) are never conflated.
///
/// The one gate this must never skip: [`Store::is_non_supersession_pair_scoped`]
/// (D3) — if a human already rejected this exact pairing, the write is
/// skipped and logged rather than silently re-creating an overturned
/// verdict. This is the only path in the system where a single chat message
/// demotes existing evidence with no second party involved, which is exactly
/// why the check is mandatory here rather than advisory.
///
/// Best-effort throughout, like [`anchor_atoms_to_reefs_scoped`] above: any
/// failure is logged and never fails the ingest — the atom + its embedding
/// are already durable, and a missed supersession write is recoverable by
/// §3.4's background sweep or a later re-run, whereas failing an already-
/// accepted user correction is not.
async fn write_anchored_supersession_edges_scoped(
    store: &dyn crate::store::Store,
    tx: &mut sqlx::Transaction<'static, sqlx::Postgres>,
    graph_id: Uuid,
    statement: &str,
    seed_atom_id: Uuid,
    anchor_atom_ids: &[Uuid],
) {
    if anchor_atom_ids.is_empty() {
        return;
    }
    // Outer savepoint: guards the reef resolution + claim-reef write below
    // from poisoning the CALLER's shared `scoped_tx` (see the SAVEPOINT
    // discipline note above `anchor_atoms_to_reefs_scoped`). The per-anchor
    // loop further down opens its OWN nested savepoint per iteration so one
    // anchor's failure can never mask or poison the next anchor's attempt —
    // without it, a mid-loop error would abort every subsequent iteration's
    // queries too, masquerading as "not a supersession" instead of the
    // transaction-aborted state it actually was.
    if !open_best_effort_savepoint!(tx, "anchored_supersession_sp", "anchored_supersession") {
        return;
    }
    let anchor_node_ids = match store
        .node_ids_for_atoms_scoped(tx, graph_id, anchor_atom_ids)
        .await
    {
        Ok(ids) => ids,
        Err(e) => {
            rollback_best_effort_savepoint!(
                tx,
                "anchored_supersession_sp",
                "anchored_supersession:node_ids",
                e
            );
            return;
        }
    };
    if anchor_node_ids.is_empty() {
        release_best_effort_savepoint!(
            tx,
            "anchored_supersession_sp",
            "anchored_supersession:noop"
        );
        return;
    }

    let kind = crate::services::supersession::classify_supersession_kind(statement);
    let body = serde_json::json!({
        "decided_at":    Utc::now().to_rfc3339(),
        "rationale":     serde_json::Value::Null,
        "confidence":    1.0,
        "review_status": "confirmed",
    })
    .to_string();
    let superseder_id = match store
        .create_decision_candidate_and_link_atom_scoped(tx, graph_id, statement, &body, seed_atom_id)
        .await
    {
        Ok(id) => id,
        Err(e) => {
            rollback_best_effort_savepoint!(
                tx,
                "anchored_supersession_sp",
                "anchored_supersession:claim_reef",
                e
            );
            return;
        }
    };

    for (i, anchor_id) in anchor_node_ids.into_iter().enumerate() {
        if anchor_id == superseder_id {
            // Idempotent re-run (or a correction that lexically matches its
            // own anchor's title) landed on its own reef — nothing to supersede.
            continue;
        }
        let anchor_sp = format!("anchored_supersession_anchor_sp_{i}");
        if sqlx::query(&format!("SAVEPOINT {anchor_sp}"))
            .execute(&mut **tx)
            .await
            .is_err()
        {
            // Can't establish the per-anchor savepoint — the OUTER savepoint
            // itself must already be aborted (nothing else runs statements on
            // `tx` between here and the last successful step), so there is
            // nothing left to roll back to but the outer one. Stop the loop
            // rather than attempt more anchors doomed by the same abort.
            tracing::warn!(
                superseder_id = %superseder_id,
                anchor_id = %anchor_id,
                "anchored correction: failed to open per-anchor savepoint — stopping"
            );
            break;
        }
        match store
            .is_non_supersession_pair_scoped(tx, superseder_id, anchor_id)
            .await
        {
            Ok(true) => {
                tracing::info!(
                    superseder_id = %superseder_id,
                    anchor_id = %anchor_id,
                    "anchored correction: pair previously marked non-supersession by a human — skipping write"
                );
                if let Err(e) = sqlx::query(&format!("RELEASE SAVEPOINT {anchor_sp}"))
                    .execute(&mut **tx)
                    .await
                {
                    tracing::warn!(error = %e, "anchored correction: failed to release per-anchor savepoint");
                }
                continue;
            }
            Ok(false) => {}
            Err(e) => {
                // Fail closed: a gate-check error must never fall through to a
                // write it couldn't actually verify.
                tracing::warn!(
                    error = %e,
                    superseder_id = %superseder_id,
                    anchor_id = %anchor_id,
                    "anchored correction: non_supersession_pair gate check failed — skipping write"
                );
                if sqlx::query(&format!("ROLLBACK TO SAVEPOINT {anchor_sp}"))
                    .execute(&mut **tx)
                    .await
                    .is_ok()
                {
                    let _ = sqlx::query(&format!("RELEASE SAVEPOINT {anchor_sp}"))
                        .execute(&mut **tx)
                        .await;
                }
                continue;
            }
        }
        if let Err(e) = store
            .upsert_supersession_edge_with_kind_scoped(tx, superseder_id, anchor_id, kind)
            .await
        {
            tracing::warn!(
                error = %e,
                superseder_id = %superseder_id,
                anchor_id = %anchor_id,
                "anchored correction: supersession edge write failed"
            );
            if sqlx::query(&format!("ROLLBACK TO SAVEPOINT {anchor_sp}"))
                .execute(&mut **tx)
                .await
                .is_ok()
            {
                let _ = sqlx::query(&format!("RELEASE SAVEPOINT {anchor_sp}"))
                    .execute(&mut **tx)
                    .await;
            }
        } else {
            if let Err(e) = sqlx::query(&format!("RELEASE SAVEPOINT {anchor_sp}"))
                .execute(&mut **tx)
                .await
            {
                tracing::warn!(error = %e, "anchored correction: failed to release per-anchor savepoint");
            }
            tracing::info!(
                superseder_id = %superseder_id,
                anchor_id = %anchor_id,
                kind = %kind,
                "anchored correction: supersession edge written"
            );
        }
    }
    release_best_effort_savepoint!(
        tx,
        "anchored_supersession_sp",
        "anchored_supersession:done"
    );
}

/// ADR-0007 D7's destination predicate, [`is_own_writable_ocean`], for every
/// ocean shape.
#[cfg(test)]
mod writable_ocean_tests {
    use super::is_own_writable_ocean;
    use crate::store::Graph;
    use chrono::Utc;
    use uuid::Uuid;

    fn graph(kind: &str, owner: Option<Uuid>, archived: bool) -> Graph {
        Graph {
            id: Uuid::new_v4(),
            name: "o".to_string(),
            kind: kind.to_string(),
            is_primary: false,
            created_at: Utc::now(),
            owner_user_id: owner,
            archived_at: archived.then(Utc::now),
            team_id: None,
            my_team_role: None,
        }
    }

    #[test]
    fn personal_ocean_is_writable() {
        let me = Uuid::new_v4();
        assert!(is_own_writable_ocean(&graph("personal", None, false), me));
    }

    #[test]
    fn own_live_member_ocean_is_writable() {
        let me = Uuid::new_v4();
        assert!(is_own_writable_ocean(&graph("member", Some(me), false), me));
    }

    /// The clause that makes the `user_id` parameter load-bearing: a kind-only
    /// check would hand every member ocean in the account to every member.
    #[test]
    fn another_members_ocean_is_not_writable() {
        let me = Uuid::new_v4();
        let someone_else = Uuid::new_v4();
        assert!(!is_own_writable_ocean(
            &graph("member", Some(someone_else), false),
            me
        ));
    }

    #[test]
    fn own_archived_member_ocean_is_not_writable() {
        let me = Uuid::new_v4();
        assert!(!is_own_writable_ocean(&graph("member", Some(me), true), me));
    }

    /// The case this whole change exists for: a shared ocean is promotion-fed,
    /// never a direct-ingest target — not even for the org's own admin, and not
    /// even when `owner_user_id` happens to match.
    #[test]
    fn shared_oceans_are_never_writable() {
        let me = Uuid::new_v4();
        for kind in ["org", "team"] {
            assert!(
                !is_own_writable_ocean(&graph(kind, None, false), me),
                "{kind} ocean must not be a direct-ingest target"
            );
            assert!(
                !is_own_writable_ocean(&graph(kind, Some(me), false), me),
                "{kind} ocean must not become writable just because owner_user_id matches"
            );
        }
    }

    /// An unrecognized kind must fail CLOSED rather than fall through to
    /// writable — the predicate is an allowlist, and a future ocean kind that
    /// nobody remembered to classify should be un-writable until someone
    /// decides otherwise.
    #[test]
    fn unknown_kind_fails_closed() {
        let me = Uuid::new_v4();
        assert!(!is_own_writable_ocean(&graph("something_new", Some(me), false), me));
    }
}

/// The connectors' drop/retry decisions used to read the HTTP status off
/// [`IngestError`]'s response. They now read [`IngestError::is_invalid`] and
/// [`IngestError::is_forbidden`]; these pin each to the status it replaced,
/// for every variant.
#[cfg(test)]
mod error_predicate_tests {
    use super::*;

    fn capture(permanent: bool, rls_refused: bool) -> IngestError {
        IngestError::Capture(capture::FunnelError { stage: "t", message: "m".into(), permanent, rls_refused, storage: false })
    }

    fn every_shape() -> Vec<IngestError> {
        vec![
            IngestError::Malformed("m".into()),
            IngestError::Forbidden("m".into()),
            IngestError::Conflict("m".into()),
            IngestError::Invalid("m".into()),
            IngestError::InvalidCoded { error: "m".into(), code: "c" },
            IngestError::Unavailable("m".into()),
            IngestError::Internal("m".into()),
            capture(false, false),
            capture(true, false),
            capture(false, true),
            capture(true, true),
            IngestError::Uncommitted { error: "m", authorization_lapsed: false, cleanup_status: "s" },
            IngestError::Uncommitted { error: "m", authorization_lapsed: true, cleanup_status: "s" },
            IngestError::StorageUnavailable { cleanup_status: "s" },
        ]
    }

    #[test]
    fn is_invalid_is_exactly_the_422_set() {
        for e in every_shape() {
            assert_eq!(e.is_invalid(), e.status() == StatusCode::UNPROCESSABLE_ENTITY, "{e:?}");
        }
    }

    #[test]
    fn is_forbidden_is_exactly_the_403_set() {
        for e in every_shape() {
            assert_eq!(e.is_forbidden(), e.status() == StatusCode::FORBIDDEN, "{e:?}");
        }
    }

    /// An object-store outage is the one refusal every caller retries: a
    /// connector's drop arms (`is_invalid`, `is_forbidden`) never take it,
    /// and HTTP and MCP answer it 503 with a code a client can retry on.
    /// Both arms reach it through [`IngestError::capture`], so a storage
    /// failure from any capture stage has the one shape.
    #[test]
    fn a_storage_outage_is_transient_everywhere() {
        for stage in ["source version", "chunk upload", "chunk reconcile"] {
            let failed = capture::FunnelError { stage, message: "m".into(), permanent: false, rls_refused: false, storage: true };
            let e = IngestError::capture(failed, "s");
            assert!(matches!(e, IngestError::StorageUnavailable { cleanup_status: "s" }), "{e:?}");
            assert!(!e.is_permanent() && !e.is_invalid() && !e.is_forbidden(), "{e:?}");
            assert_eq!(e.status(), StatusCode::SERVICE_UNAVAILABLE, "{e:?}");
        }
        let other = capture::FunnelError { stage: "chunk insert", message: "m".into(), permanent: false, rls_refused: false, storage: false };
        assert!(matches!(IngestError::capture(other, "s"), IngestError::Capture(_)), "only a storage failure becomes the outage");
    }

    #[tokio::test]
    async fn a_storage_outage_answers_a_retryable_503() {
        let response = IngestError::StorageUnavailable { cleanup_status: "complete" }.into_response();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        let body: serde_json::Value =
            serde_json::from_slice(&axum::body::to_bytes(response.into_body(), 1 << 16).await.unwrap()).unwrap();
        assert_eq!(
            body,
            json!({"error": "object storage is unavailable; nothing was stored, retry ingest",
                   "code": "storage_unavailable", "cleanup_status": "complete"})
        );
    }

    /// The history walkers drop on `is_invalid` and retry everything else,
    /// including the permanent shapes that answer 400 and 409: that is what
    /// their status branch did, so `is_permanent` is not the same decision.
    #[test]
    fn is_invalid_is_narrower_than_is_permanent() {
        assert!(IngestError::Malformed("m".into()).is_permanent() && !IngestError::Malformed("m".into()).is_invalid());
        assert!(IngestError::Conflict("m".into()).is_permanent() && !IngestError::Conflict("m".into()).is_invalid());
        for e in every_shape() {
            assert!(!e.is_invalid() || e.is_permanent(), "{e:?}");
        }
    }
}

// ── Live-Postgres tests: §3.4a anchored-correction supersession writes ────────
//
// In-crate, not `backend/tests/*_pg.rs`: `write_anchored_supersession_edges_scoped`
// is a private function, and testing it through the full [`submit`]
// path would require standing up `AppState`'s crypto/DEK/tenant machinery for a
// concern that lives entirely in this one function. Mirrors `routes::slack::
// resolve_primary_graph_org_gate_tests`'s established pattern for exercising a
// crate-private function against a real Postgres. Gated on `ORGT_TEST_BASE_URL`,
// same scratch-DB convention as every `backend/tests/*_pg.rs` file.
//
//   ORGT_TEST_BASE_URL=postgres://postgres:postgres@localhost:55987/postgres \
//     cargo test -p coralswarm-backend --lib ingest::anchored_correction_pg_tests -- --nocapture
#[cfg(test)]
mod anchored_correction_pg_tests {
    use super::*;
    use crate::store::{PostgresStore, Store};
    use sqlx::postgres::PgPoolOptions;
    use sqlx::{Connection, Executor, PgConnection};

    fn base_url() -> Option<String> {
        std::env::var("ORGT_TEST_BASE_URL").ok()
    }
    fn with_db(base: &str, db: &str) -> String {
        let (prefix, rest) = base.split_at(base.rfind('/').expect("url has a path /"));
        let params = rest.find('?').map(|i| &rest[i..]).unwrap_or("");
        format!("{prefix}/{db}{params}")
    }
    async fn admin(base: &str) -> PgConnection {
        PgConnection::connect(base).await.expect("connect admin")
    }
    async fn recreate(conn: &mut PgConnection, db: &str) {
        conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .unwrap();
        conn.execute(format!("CREATE DATABASE {db}").as_str())
            .await
            .unwrap();
    }
    async fn control_pool(base: &str, db: &str) -> sqlx::PgPool {
        let pool = PgPoolOptions::new()
            .max_connections(8)
            .connect(&with_db(base, db))
            .await
            .expect("connect control");
        sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();
        pool
    }

    async fn seed_graph(pool: &sqlx::PgPool) -> Uuid {
        let user_id: Uuid = sqlx::query_scalar("INSERT INTO users (email) VALUES ($1) RETURNING id")
            .bind(format!("anchor-{}@test.local", Uuid::new_v4()))
            .fetch_one(pool)
            .await
            .unwrap();
        let account_id: Uuid = sqlx::query_scalar(
            "INSERT INTO accounts (owner_user_id, plan_tier) VALUES ($1, 'free') RETURNING id",
        )
        .bind(user_id)
        .fetch_one(pool)
        .await
        .unwrap();
        sqlx::query_scalar("INSERT INTO graphs (account_id, name) VALUES ($1, 'test') RETURNING id")
            .bind(account_id)
            .fetch_one(pool)
            .await
            .unwrap()
    }

    async fn insert_decision(pool: &sqlx::PgPool, graph_id: Uuid, title: &str, decided_at: &str) -> Uuid {
        let body = serde_json::json!({ "decided_at": decided_at, "review_status": "confirmed" });
        sqlx::query_scalar(
            "INSERT INTO nodes (graph_id, type, title, body) VALUES ($1, 'decision', $2, $3) RETURNING node_id",
        )
        .bind(graph_id)
        .bind(title)
        .bind(body)
        .fetch_one(pool)
        .await
        .unwrap()
    }

    async fn insert_atom(pool: &sqlx::PgPool, graph_id: Uuid, source_id: &str) -> Uuid {
        sqlx::query_scalar(
            "INSERT INTO atoms (graph_id, source_type, source_id, content_blob_key, timestamp) \
             VALUES ($1, 'slack', $2, $3, now()) RETURNING atom_id",
        )
        .bind(graph_id)
        .bind(source_id)
        .bind(format!("blob-{source_id}"))
        .fetch_one(pool)
        .await
        .unwrap()
    }

    async fn link(pool: &sqlx::PgPool, node_id: Uuid, atom_id: Uuid) {
        sqlx::query("INSERT INTO node_atoms (node_id, atom_id) VALUES ($1, $2)")
            .bind(node_id)
            .bind(atom_id)
            .execute(pool)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn anchored_correction_writes_change_by_default_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping anchored_correction_writes_change_by_default_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "anchor_default_change";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let mut conn = admin(&base).await;
        recreate(&mut conn, db).await;
        let pool = control_pool(&base, db).await;
        let store = PostgresStore::new(pool.clone());

        let graph_id = seed_graph(&pool).await;
        let old_claim = insert_decision(&pool, graph_id, "ExamplePortal is hosted on Fly.io", "2026-01-01T00:00:00Z").await;
        let anchor_atom = insert_atom(&pool, graph_id, "anchor-1").await;
        link(&pool, old_claim, anchor_atom).await;
        let correction_atom = insert_atom(&pool, graph_id, "correction-1").await;

        let mut tx = pool.begin().await.unwrap();
        write_anchored_supersession_edges_scoped(
            &store,
            &mut tx,
            graph_id,
            "we no longer use Fly.io",
            correction_atom,
            &[anchor_atom],
        )
        .await;
        tx.commit().await.unwrap();

        let new_claim: Uuid = sqlx::query_scalar(
            "SELECT node_id FROM nodes WHERE graph_id = $1 AND type = 'decision' AND lower(title) = lower($2)",
        )
        .bind(graph_id)
        .bind("we no longer use Fly.io")
        .fetch_one(&pool)
        .await
        .expect("the correction statement must become its own claim reef");

        let row: (Uuid, Uuid, String) = sqlx::query_as(
            "SELECT superseder_id, superseded_id, kind FROM supersession_edge \
             WHERE superseder_id = $1 AND superseded_id = $2",
        )
        .bind(new_claim)
        .bind(old_claim)
        .fetch_one(&pool)
        .await
        .expect("the new claim reef must supersede the anchor reef");
        assert_eq!(row.0, new_claim);
        assert_eq!(row.1, old_claim);
        assert_eq!(row.2, "change", "\"we no longer use Fly.io\" has no explicit correction language ⇒ change");

        pool.close().await;
        conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.ok();
    }

    #[tokio::test]
    async fn anchored_correction_escalates_to_correction_on_explicit_language_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping anchored_correction_escalates_to_correction_on_explicit_language_pg: set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "anchor_correction";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let mut conn = admin(&base).await;
        recreate(&mut conn, db).await;
        let pool = control_pool(&base, db).await;
        let store = PostgresStore::new(pool.clone());

        let graph_id = seed_graph(&pool).await;
        let old_claim = insert_decision(&pool, graph_id, "It's Elmbrok", "2026-01-01T00:00:00Z").await;
        let anchor_atom = insert_atom(&pool, graph_id, "anchor-1").await;
        link(&pool, old_claim, anchor_atom).await;
        let correction_atom = insert_atom(&pool, graph_id, "correction-1").await;

        let mut tx = pool.begin().await.unwrap();
        write_anchored_supersession_edges_scoped(
            &store,
            &mut tx,
            graph_id,
            "it's actually Elmbrook, not Elmbrok",
            correction_atom,
            &[anchor_atom],
        )
        .await;
        tx.commit().await.unwrap();

        let kind: String = sqlx::query_scalar(
            "SELECT se.kind FROM supersession_edge se \
             JOIN nodes n ON n.node_id = se.superseder_id \
             WHERE n.graph_id = $1 AND lower(n.title) = lower($2) AND se.superseded_id = $3",
        )
        .bind(graph_id)
        .bind("it's actually Elmbrook, not Elmbrok")
        .bind(old_claim)
        .fetch_one(&pool)
        .await
        .expect("supersession edge with escalated kind must exist");
        assert_eq!(kind, "correction", "explicit correction language ⇒ kind escalates to correction");

        pool.close().await;
        conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.ok();
    }

    #[tokio::test]
    async fn anchored_correction_respects_the_non_supersession_pair_gate_pg() {
        // D3/§3.7: this is the only path in the system where a single chat
        // message demotes existing evidence with no second party involved —
        // which is exactly why this gate is mandatory here, not advisory.
        let Some(base) = base_url() else {
            eprintln!(
                "skipping anchored_correction_respects_the_non_supersession_pair_gate_pg: set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "anchor_gate_block";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let mut conn = admin(&base).await;
        recreate(&mut conn, db).await;
        let pool = control_pool(&base, db).await;
        let store = PostgresStore::new(pool.clone());

        let graph_id = seed_graph(&pool).await;
        let old_claim = insert_decision(&pool, graph_id, "ExamplePortal is hosted on Fly.io", "2026-01-01T00:00:00Z").await;
        let anchor_atom = insert_atom(&pool, graph_id, "anchor-1").await;
        link(&pool, old_claim, anchor_atom).await;
        let correction_atom = insert_atom(&pool, graph_id, "correction-1").await;
        let statement = "we no longer use Fly.io";

        // Pre-create the exact claim reef this correction will resolve to
        // (idempotent on (graph_id, 'decision', lower(title))), so its id is
        // known ahead of time, then have a human reject that exact pairing —
        // simulating "a detection pass already proposed this and a human
        // said no" before the chat correction ever arrives.
        let body = serde_json::json!({ "decided_at": "2026-05-01T00:00:00Z", "review_status": "confirmed" });
        let preexisting_new_claim: Uuid = sqlx::query_scalar(
            "INSERT INTO nodes (graph_id, type, title, body) VALUES ($1, 'decision', $2, $3) RETURNING node_id",
        )
        .bind(graph_id)
        .bind(statement)
        .bind(body)
        .fetch_one(&pool)
        .await
        .unwrap();
        store
            .mark_non_supersession_pair(preexisting_new_claim, old_claim, None)
            .await
            .unwrap();

        let mut tx = pool.begin().await.unwrap();
        write_anchored_supersession_edges_scoped(
            &store,
            &mut tx,
            graph_id,
            statement,
            correction_atom,
            &[anchor_atom],
        )
        .await;
        tx.commit().await.unwrap();

        let count: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM supersession_edge WHERE superseder_id = $1 AND superseded_id = $2",
        )
        .bind(preexisting_new_claim)
        .bind(old_claim)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(count, 0, "a previously-rejected pair must never be re-written by an anchored correction");

        pool.close().await;
        conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.ok();
    }
}

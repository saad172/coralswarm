//! AI coding-agent session capture — read HTTP surface (coding-session
//! capture Phase 4, audit A8). Mirrors `routes::shares`'s
//! `GET /v1/meetings/shares?graph_id=&source_id=` precedent byte-for-byte
//! (query-param IDOR shape, generic 404 body, `require_store!` macro) — the
//! closest existing "list under a container namespace, scoped by graph_id"
//! HTTP route in this codebase, since `list_meetings` itself has no HTTP
//! counterpart (MCP-only).
//!
//! - `GET /v1/sessions?graph_id=&project=&kind=&since=&limit=&cursor=` — list
//!   captured sessions in an ocean, most-recent-activity first. Same store
//!   method (`Store::list_coding_sessions`) and cursor encoding as the
//!   `list_sessions` MCP tool (`mcp/tools/read.rs`) — the two surfaces stay
//!   byte-for-byte consistent by construction, not by convention alone.
//!
//!   `kind` is the OPTIONAL wire kind (`coding` / `meeting` / `agent`,
//!   [`crate::ingest::atom::SessionKind`]), added by capture unit 07 item 5
//!   so Library's per-kind facets can each ask for their own rows.
//!   `coding_sessions` has held every kind since migration
//!   `0185_coding_sessions_session_kind`, so before this parameter a
//!   "coding sessions" list was really an every-kind list. Omitting `kind`
//!   still returns every kind, unchanged.
//!
//!   **The filter is applied in SQL, never by the client.** This route is
//!   keyset-paginated: a browser that fetched a 20-row page and then dropped
//!   the wrong kinds would render an EMPTY facet whenever the first page
//!   happens to hold no rows of that kind — an empty list that is not
//!   evidence of an empty ocean, which is the `absence-renders-as-success`
//!   failure the per-kind facets were specified to close (unit 07,
//!   Q11=(c)).
//! - `GET /v1/sessions/{session_id}?graph_id=` — one session's metadata plus
//!   its corals (atoms) in chronological order, via `Store::list_session_atoms`
//!   (the `"cc:{session_id}:"` `source_id` prefix convention, escaped by the
//!   SAME `store::session_source_id_prefix` helper the retrieval filters use —
//!   no new LIKE logic here).
//! - `GET /v1/sessions/facets?graph_id=&...&facet=&facet_cursor=` — the
//!   distinct-value vocabulary (with counts) of every OTHER filter on this
//!   list, via `Store::session_facets`. NOT to be confused with `kind` above
//!   (which picks a whole session TYPE): this is the Library filter
//!   suggestions' (`agent_name`/`platform`/`project`/`harness`/`model`/
//!   `repo`/`branch`/`task`) vocabulary, paged per dimension by an opaque
//!   keyset cursor (`next_cursor`), and exists for the identical reason
//!   `kind` moved into SQL —
//!   `SessionFilterBar` used to derive its chip list from whatever page of
//!   rows happened to be loaded, which is correct only by accident and wrong
//!   the moment an ocean outgrows one page. Mounted as a STATIC path segment
//!   ahead of `/sessions/{session_id}`'s param route; axum's router (like
//!   every `matchit`-based router) always prefers a static segment over a
//!   path parameter at the same position regardless of registration order —
//!   proven by a route test in this file's `tests` module rather than
//!   asserted from the docs alone.
//!
//! All three routes are IDOR-guarded by graph reachability (ADR-0007 D9),
//! exactly like every other `meetings`-prefixed route.
//!
//! # Consumer-plane RLS scoping (org-tenancy OPS-4 phase B.5b)
//!
//! `coding_sessions` and `atoms` both carry 0083 policies, so each handler
//! runs its RLS-covered reads inside ONE
//! [`crate::tenancy::scope::begin_account_scoped`] transaction, scoped to the
//! OCEAN'S OWNING account. Not `user.account_id`: these routes gate on graph
//! REACHABILITY, which is multi-account by design (ADR-0007 Model B), so a
//! legitimate caller may sit in a different account from the ocean's owner —
//! the same call B.1 made in `routes::graphs::get_ocean_stats` and B.3b in
//! `routes::shares`. The owner comes from `Store::graph_account_id`, i.e.
//! migration 0084's audited `resolve_graph_account_id` SECURITY DEFINER
//! bootstrap, which stays deliberately unscoped because it is how that account
//! is discovered. `ReachabilityResolver` is likewise the permanent
//! multi-account carve-out and stays unscoped.
//!
//! A `Tenant` with no pool cannot open the scoped transaction, so
//! `begin_owner_scoped_tx!` returns a 500.
//!
//! **Formerly a known gap, now closed** (org-tenancy OPS-4 Store-surface
//! follow-up): `Store::list_session_atoms`/`Store::count_session_atoms` now
//! have `_scoped` twins, and `get_session`'s coral list and count reuse the
//! SAME `scoped_tx` opened for `get_coding_session_scoped` above rather than
//! opening a fresh transaction per call. `list_sessions` (the list route) was
//! already fully converted.

use axum::{
    extract::{Path, Query},
    http::StatusCode,
    Extension, Json,
};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use chrono::{DateTime, Utc};
use serde::Deserialize;
use serde_json::{json, Value};
use uuid::Uuid;

use crate::ingest::atom::{validate_provenance_label, SessionKind};
use crate::store::{FacetCursor, FacetDimension, FacetPageRequest};
use crate::{
    middleware::auth::UserContext, tenancy::extractor::Tenant,
    tenancy::reachability::ReachabilityResolver,
};

// Content-plane store, resolved by the `Tenant` extractor (org-tenancy PR
// 1.6; was ambient `state.store`) — `coding_sessions`/`atoms`/`graphs` are
// tenant-tree tables.
macro_rules! require_store {
    ($tenant:expr) => {
        match &$tenant.store {
            Some(s) => s,
            None => {
                return (
                    StatusCode::SERVICE_UNAVAILABLE,
                    Json(json!({"error": "service unavailable"})),
                )
            }
        }
    };
}

/// Open the request's account-scoped transaction on the OCEAN'S owning account
/// (org-tenancy OPS-4 phase B.5b — see the module doc), or early-return.
/// `Ok(None)` from the bootstrap means "no such graph"; reachability has
/// already passed at both call sites, so that is a race with graph deletion
/// and returns each handler's own existing not-found body — never a new,
/// distinguishable status.
macro_rules! begin_owner_scoped_tx {
    ($tenant:expr, $store:expr, $graph_id:expr, $missing:expr) => {
        {
            let owner = match $store.graph_account_id($graph_id).await {
                Ok(Some(a)) => a,
                Ok(None) => return $missing,
                Err(e) => {
                    tracing::error!(error = %e, "sessions: graph account lookup failed");
                    return (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(json!({"error": "internal error"})),
                    );
                }
            };
            match $tenant.begin_member_scoped(owner).await {
                Ok(tx) => Some(tx),
                Err(e) => {
                    tracing::error!(error = %e, "failed to open account-scoped transaction");
                    return (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(json!({"error": "internal error"})),
                    );
                }
            }
        }
    };
}

/// Dispatch one Store call through the scoped transaction, or the plain
/// unscoped method when `$tx` is `None`. `begin_owner_scoped_tx!` never yields `None`: with
/// no pool it returns a 500 instead.
macro_rules! scoped_store {
    ($store:expr, $tx:expr, $unscoped:ident / $scoped:ident ( $($arg:expr),* $(,)? )) => {
        match $tx.as_mut() {
            Some(tx) => $store.$scoped(tx, $($arg),*).await,
            None => $store.$unscoped($($arg),*).await,
        }
    };
}

/// Close a READ-ONLY handler's scoped transaction. Nothing was written, so a
/// commit failure is logged and never surfaced.
macro_rules! close_read_tx {
    ($tx:expr) => {
        if let Some(tx) = $tx.take() {
            if let Err(e) = tx.commit().await {
                tracing::warn!(error = %e, "read-only scoped transaction commit failed");
            }
        }
    };
}

/// `list_sessions`'s default/max page size — mirrors the MCP tool's own
/// `input.limit.unwrap_or(20).clamp(1, 100)` exactly, so a client hitting
/// either surface sees the same pagination behavior.
const SESSIONS_LIST_DEFAULT_LIMIT: i64 = 20;
const SESSIONS_LIST_MAX_LIMIT: i64 = 100;

/// Page size of each dimension in `GET /v1/sessions/facets` (`count DESC,
/// value ASC`; see [`crate::store::SessionFacets`]). A dimension with more
/// values carries a `next_cursor`.
const SESSIONS_FACETS_CAP: i64 = 200;

/// Max corals returned by the session detail route. A caller that needs the
/// full transcript beyond this cap has no follow-up query today (this route
/// deliberately does not implement its own pagination — see the module doc);
/// `corals_truncated` tells the client that more exist.
const SESSION_DETAIL_CORAL_CAP: i64 = 200;

/// Cap on the `subagents` array of `GET /v1/sessions/{id}` — an orchestrator
/// fanning out more helpers than this is a pathological tree; the response
/// flags the cut (`subagents_truncated`) and the list route
/// (`parent_session_id=` / `roots_only=false`) pages through the rest.
const SESSION_DETAIL_SUBAGENT_CAP: i64 = 200;

// ── Pagination cursor (mirrors `mcp/tools/read.rs`'s `list_sessions` cursor) ──
//
// Deliberately duplicated (not imported from `mcp::tools::read`, which is
// itself a consumer of routes, not the other way around) rather than shared:
// this is generic keyset-pagination encoding, not MCP-specific, and keeping
// each surface's cursor codec independently declared avoids a cross-module
// dependency neither side should have to assume the other won't change. Same
// encoding either way: `"{last_activity_at RFC3339}|{id}"`, opaque to callers.

fn encode_sessions_cursor(last_activity_at: DateTime<Utc>, id: Uuid) -> String {
    format!("{}|{id}", last_activity_at.to_rfc3339())
}

fn decode_sessions_cursor(cursor: &str) -> Option<(DateTime<Utc>, Uuid)> {
    let (ts, id) = cursor.split_once('|')?;
    let ts = DateTime::parse_from_rfc3339(ts).ok()?.with_timezone(&Utc);
    let id = Uuid::parse_str(id).ok()?;
    Some((ts, id))
}

/// Facet cursor wire form: base64url (no padding) of `"{count}|{value}"`.
/// The count is digits only, so the first `|` always ends it.
fn encode_facet_cursor(cursor: &FacetCursor) -> String {
    URL_SAFE_NO_PAD.encode(format!("{}|{}", cursor.count, cursor.value))
}

fn decode_facet_cursor(cursor: &str) -> Option<FacetCursor> {
    let raw = String::from_utf8(URL_SAFE_NO_PAD.decode(cursor).ok()?).ok()?;
    let (count, value) = raw.split_once('|')?;
    Some(FacetCursor { count: count.parse().ok()?, value: value.to_string() })
}

// ── GET /v1/sessions ─────────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct ListSessionsQuery {
    pub graph_id: Uuid,
    #[serde(default)]
    pub project: Option<String>,
    /// Wire session kind (`coding` / `meeting` / `agent`). Validated
    /// against [`SessionKind`]'s closed set and rejected with 400 when
    /// unknown — never silently ignored, because an ignored filter returns
    /// EVERY kind under a heading that promises one, which is worse than an
    /// error.
    #[serde(default)]
    pub kind: Option<String>,
    #[serde(default)]
    pub since: Option<DateTime<Utc>>,
    #[serde(default)]
    pub limit: Option<i64>,
    #[serde(default)]
    pub cursor: Option<String>,
    /// Agent-session filters (migration 0219). `agent_name` is an exact
    /// match on the effective (inherited) name; `platform` is canonicalized
    /// before matching so `?platform=Claude%20Code` finds `claude-code`;
    /// `task` is a case-insensitive substring match.
    #[serde(default)]
    pub agent_name: Option<String>,
    #[serde(default)]
    pub platform: Option<String>,
    #[serde(default)]
    pub task: Option<String>,
    /// Coding-session filters (migration 0220). Apply to every kind.
    /// `harness` / `model` / `branch` are exact matches; `repo` matches
    /// `repo_path` OR `remote_sanitized`.
    #[serde(default)]
    pub harness: Option<String>,
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default)]
    pub repo: Option<String>,
    #[serde(default)]
    pub branch: Option<String>,
    /// Default `true`: list only sessions with no parent, each carrying a
    /// tree-wide `coral_count` and a `subagent_count`. `false` lists every
    /// session flat (subagents included, per-session counts).
    #[serde(default)]
    pub roots_only: Option<bool>,
    /// List the DIRECT children of this session instead (implies
    /// `roots_only=false`).
    #[serde(default)]
    pub parent_session_id: Option<String>,
}

/// Build the store-level filter from the query's agent parameters; shared
/// with the tests so the canonicalization/default rules live in one place.
fn session_list_filter(q: &ListSessionsQuery) -> crate::store::SessionListFilter {
    crate::store::SessionListFilter {
        agent_name: q.agent_name.as_deref().map(str::trim).filter(|s| !s.is_empty()).map(str::to_string),
        platform: q
            .platform
            .as_deref()
            .and_then(crate::services::platform::canonicalize_platform),
        task: q.task.as_deref().map(str::trim).filter(|s| !s.is_empty()).map(str::to_string),
        harness: q.harness.as_deref().map(str::trim).filter(|s| !s.is_empty()).map(str::to_string),
        model: q.model.as_deref().map(str::trim).filter(|s| !s.is_empty()).map(str::to_string),
        repo: q.repo.as_deref().map(str::trim).filter(|s| !s.is_empty()).map(str::to_string),
        branch: q.branch.as_deref().map(str::trim).filter(|s| !s.is_empty()).map(str::to_string),
        roots_only: q.roots_only.unwrap_or(true),
        parent_session_id: q
            .parent_session_id
            .as_deref()
            .filter(|s| !s.is_empty())
            .map(str::to_string),
    }
}

/// The agent-identity fields every session JSON body (list row and detail)
/// carries — one function so the two surfaces cannot drift. `platform` is
/// the EFFECTIVE platform (`SessionIdentity::effective_platform`: declared,
/// falling back to detected) so a session whose harness never sent
/// `platform` — every Cursor coding session, as of this writing — still
/// carries a platform chip; `platform_detected` stays the raw detected value
/// underneath it.
fn identity_json(identity: &crate::store::SessionIdentity) -> Vec<(&'static str, Value)> {
    vec![
        ("agent_name", json!(identity.agent_name)),
        ("platform", json!(identity.effective_platform())),
        ("platform_detected", json!(identity.platform_detected)),
        ("platform_verified", json!(identity.platform_verified())),
        ("task", json!(identity.task)),
        ("root_session_id", json!(identity.root_session_id)),
        ("parent_session_id", json!(identity.parent_session_id)),
        ("subagent_name", json!(identity.subagent_name)),
    ]
}

pub async fn list_sessions(
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
    Query(q): Query<ListSessionsQuery>,
) -> (StatusCode, Json<Value>) {
    let store = require_store!(tenant);

    if let Err(e) = validate_provenance_label(&q.project) {
        let msg = match e {
            crate::ingest::atom::ProvenanceLabelError::ContainsNul => {
                "project must not contain NUL bytes".to_string()
            }
            crate::ingest::atom::ProvenanceLabelError::TooLong => format!(
                "project must be at most {} chars",
                crate::ingest::atom::PROVENANCE_LABEL_CAP
            ),
        };
        return (StatusCode::BAD_REQUEST, Json(json!({"error": msg})));
    }

    // An unrecognised `kind` is a 400, not a silently-dropped filter: the
    // caller asked for one kind of session and would otherwise be handed
    // every kind under that kind's heading.
    if let Some(k) = q.kind.as_deref() {
        if SessionKind::parse(k).is_none() {
            let allowed = SessionKind::ALL
                .iter()
                .map(|k| k.as_str())
                .collect::<Vec<_>>()
                .join(", ");
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": format!("kind must be one of: {allowed}")})),
            );
        }
    }

    // IDOR: the ocean must be reachable by the caller (ADR-0007 D9), same
    // guard + generic 404 body as `routes::shares::list_shares` — through the
    // tenancy-aware resolver (org-tenancy PR 1.6).
    match ReachabilityResolver::graph_reachable_by_user(
        tenant.decision.as_ref(),
        store.as_ref(),
        q.graph_id,
        user.user_id,
    )
    .await
    {
        Ok(true) => {}
        Ok(false) => {
            return (
                StatusCode::NOT_FOUND,
                Json(json!({"error": "ocean not found or access denied"})),
            )
        }
        Err(e) => {
            tracing::error!(error = %e, "list_sessions reachability error");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            );
        }
    }

    let cursor = match q.cursor.as_deref() {
        Some(c) => match decode_sessions_cursor(c) {
            Some(parsed) => Some(parsed),
            None => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(json!({"error": "malformed cursor"})),
                )
            }
        },
        None => None,
    };

    let limit = q
        .limit
        .unwrap_or(SESSIONS_LIST_DEFAULT_LIMIT)
        .clamp(1, SESSIONS_LIST_MAX_LIMIT);

    for (name, value) in [
        ("agent_name", &q.agent_name),
        ("task", &q.task),
        ("parent_session_id", &q.parent_session_id),
        ("harness", &q.harness),
        ("model", &q.model),
        ("repo", &q.repo),
        ("branch", &q.branch),
    ] {
        if let Err(e) = validate_provenance_label(value) {
            let msg = match e {
                crate::ingest::atom::ProvenanceLabelError::ContainsNul => {
                    format!("{name} must not contain NUL bytes")
                }
                crate::ingest::atom::ProvenanceLabelError::TooLong => format!(
                    "{name} must be at most {} chars",
                    crate::ingest::atom::PROVENANCE_LABEL_CAP
                ),
            };
            return (StatusCode::BAD_REQUEST, Json(json!({"error": msg})));
        }
    }
    let filter = session_list_filter(&q);

    // Fetch one extra row to compute `has_more`/`next_cursor` without a
    // second COUNT query — same trick `list_sessions` (MCP) and every other
    // paginated tool/route in this codebase uses.
    let mut scoped_tx = begin_owner_scoped_tx!(
        tenant,
        store,
        q.graph_id,
        (
            StatusCode::NOT_FOUND,
            Json(json!({"error": "ocean not found or access denied"})),
        )
    );
    let rows = match scoped_store!(
        store,
        scoped_tx,
        list_coding_sessions
            / list_coding_sessions_scoped(
                q.graph_id,
                q.project.as_deref(),
                q.kind.as_deref(),
                q.since,
                cursor,
                limit + 1,
                &filter,
            )
    ) {
        Ok(r) => r,
        Err(e) => {
            tracing::error!(error = %e, "list_sessions store error");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            );
        }
    };
    close_read_tx!(scoped_tx);

    let has_more = rows.len() as i64 > limit;
    let page: Vec<_> = rows.into_iter().take(limit as usize).collect();
    let next_cursor = if has_more {
        page.last().map(|s| encode_sessions_cursor(s.last_activity_at, s.id))
    } else {
        None
    };

    let sessions: Vec<_> = page
        .into_iter()
        .map(|s| {
            // Titles-always-present (founder requirement): a list row has no
            // per-checkpoint title on hand (that would be an N+1 fetch per
            // session for a listing endpoint) — `checkpoint_title: None`
            // simply skips that rung and falls through to
            // `project_key`/date, still guaranteeing non-empty.
            let title = crate::services::atom_title::display_session_title(
                crate::services::atom_title::DisplaySessionTitleInput {
                    session_title: s.title.as_deref(),
                    checkpoint_title: None,
                    project_key: s.project_key.as_deref(),
                    timestamp: s.started_at,
                    session_kind: Some(s.session_kind.as_str()),
                    agent_name: s.identity.agent_name.as_deref(),
                    task: s.identity.task.as_deref(),
                },
            );
            let mut row = json!({
                "session_id": s.session_id,
                // The wire kind (`coding`/`meeting`/`agent`), so a
                // client can tell what it is holding without inferring it
                // from the route it happened to call.
                "session_kind": s.session_kind,
                "title": title,
                "project_key": s.project_key,
                "repo_path": s.repo_path,
                "remote_sanitized": s.remote_sanitized,
                "hostname": s.hostname,
                "harness_version": s.harness_version,
                "model": s.model,
                "branch": s.branch,
                "started_at": s.started_at,
                "last_activity_at": s.last_activity_at,
                "coral_count": s.coral_count,
                "subagent_count": s.subagent_count,
            });
            if let Some(obj) = row.as_object_mut() {
                for (k, v) in identity_json(&s.identity) {
                    obj.insert(k.to_string(), if k=="platform_verified" && s.identity.platform.is_none() && s.identity.platform_detected.is_none() {Value::Null}else{v});
                }
            }
            row
        })
        .collect();

    (
        StatusCode::OK,
        Json(json!({
            "sessions": sessions,
            "has_more": has_more,
            "next_cursor": next_cursor,
        })),
    )
}

// ── GET /v1/sessions/facets ──────────────────────────────────────────────────

/// `?facet=&facet_cursor=` on `GET /v1/sessions/facets`, read beside
/// [`ListSessionsQuery`] so the list route never accepts them.
#[derive(Debug, Deserialize)]
pub struct FacetPageQuery {
    #[serde(default)]
    pub facet: Option<String>,
    #[serde(default)]
    pub facet_cursor: Option<String>,
}

/// `None` asks for the first page of every dimension.
fn facet_page_request(q: &FacetPageQuery) -> Result<Option<FacetPageRequest>, String> {
    let Some(facet) = q.facet.as_deref() else {
        return match q.facet_cursor {
            Some(_) => Err("facet_cursor requires facet".into()),
            None => Ok(None),
        };
    };
    let Some(dimension) = FacetDimension::parse(facet) else {
        let allowed = FacetDimension::ALL.iter().map(|d| d.as_str()).collect::<Vec<_>>().join(", ");
        return Err(format!("facet must be one of: {allowed}"));
    };
    let after = match q.facet_cursor.as_deref() {
        Some(c) => Some(decode_facet_cursor(c).ok_or_else(|| "malformed facet_cursor".to_string())?),
        None => None,
    };
    Ok(Some(FacetPageRequest { dimension, after }))
}

fn session_facets_json(facets: &crate::store::SessionFacets) -> Value {
    let mut values = serde_json::Map::new();
    let mut next = serde_json::Map::new();
    for (dim, page) in &facets.pages {
        values.insert(
            dim.as_str().into(),
            json!(page.values.iter().map(|v| json!({"value": v.value, "count": v.count})).collect::<Vec<_>>()),
        );
        next.insert(dim.as_str().into(), json!(page.next.as_ref().map(encode_facet_cursor)));
    }
    json!({"facets": values, "next_cursor": next, "cap": SESSIONS_FACETS_CAP})
}

/// The distinct-value vocabulary (with counts) of every `GET /v1/sessions`
/// filter dimension in one ocean — see the module doc's bullet on this route
/// and [`crate::store::SessionFacets`]'s doc for the faceted-search "all
/// filters except its own" semantics this exists to serve. Without `facet`
/// the response holds the first page of all eight dimensions; with `facet`
/// (and optionally `facet_cursor` from a previous `next_cursor`) it holds
/// only that dimension's next page. Reuses
/// [`ListSessionsQuery`] wholesale (ignoring `since`/`limit`/`cursor`, which
/// this route has no use for) rather than a bespoke query struct, so its
/// validation and [`session_list_filter`] stay single-sourced with
/// `list_sessions` — a facet dimension and the list it filters can never
/// silently drift apart on what counts as a valid/empty value.
pub async fn session_facets(
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
    Query(q): Query<ListSessionsQuery>,
    Query(facet_q): Query<FacetPageQuery>,
) -> (StatusCode, Json<Value>) {
    let store = require_store!(tenant);

    let page = match facet_page_request(&facet_q) {
        Ok(p) => p,
        Err(msg) => return (StatusCode::BAD_REQUEST, Json(json!({"error": msg}))),
    };

    if let Err(e) = validate_provenance_label(&q.project) {
        let msg = match e {
            crate::ingest::atom::ProvenanceLabelError::ContainsNul => {
                "project must not contain NUL bytes".to_string()
            }
            crate::ingest::atom::ProvenanceLabelError::TooLong => format!(
                "project must be at most {} chars",
                crate::ingest::atom::PROVENANCE_LABEL_CAP
            ),
        };
        return (StatusCode::BAD_REQUEST, Json(json!({"error": msg})));
    }

    // Same 400-not-silently-ignored rule as `list_sessions` — see that
    // handler's comment.
    if let Some(k) = q.kind.as_deref() {
        if SessionKind::parse(k).is_none() {
            let allowed = SessionKind::ALL
                .iter()
                .map(|k| k.as_str())
                .collect::<Vec<_>>()
                .join(", ");
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": format!("kind must be one of: {allowed}")})),
            );
        }
    }

    // IDOR: same guard + generic 404 body as `list_sessions`.
    match ReachabilityResolver::graph_reachable_by_user(
        tenant.decision.as_ref(),
        store.as_ref(),
        q.graph_id,
        user.user_id,
    )
    .await
    {
        Ok(true) => {}
        Ok(false) => {
            return (
                StatusCode::NOT_FOUND,
                Json(json!({"error": "ocean not found or access denied"})),
            )
        }
        Err(e) => {
            tracing::error!(error = %e, "session_facets reachability error");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            );
        }
    }

    for (name, value) in [
        ("agent_name", &q.agent_name),
        ("task", &q.task),
        ("parent_session_id", &q.parent_session_id),
        ("harness", &q.harness),
        ("model", &q.model),
        ("repo", &q.repo),
        ("branch", &q.branch),
    ] {
        if let Err(e) = validate_provenance_label(value) {
            let msg = match e {
                crate::ingest::atom::ProvenanceLabelError::ContainsNul => {
                    format!("{name} must not contain NUL bytes")
                }
                crate::ingest::atom::ProvenanceLabelError::TooLong => format!(
                    "{name} must be at most {} chars",
                    crate::ingest::atom::PROVENANCE_LABEL_CAP
                ),
            };
            return (StatusCode::BAD_REQUEST, Json(json!({"error": msg})));
        }
    }
    let filter = session_list_filter(&q);

    let mut scoped_tx = begin_owner_scoped_tx!(
        tenant,
        store,
        q.graph_id,
        (
            StatusCode::NOT_FOUND,
            Json(json!({"error": "ocean not found or access denied"})),
        )
    );
    let facets = match scoped_store!(
        store,
        scoped_tx,
        session_facets
            / session_facets_scoped(
                q.graph_id,
                q.project.as_deref(),
                q.kind.as_deref(),
                &filter,
                page.as_ref(),
                SESSIONS_FACETS_CAP,
            )
    ) {
        Ok(f) => f,
        Err(e) => {
            tracing::error!(error = %e, "session_facets store error");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            );
        }
    };
    close_read_tx!(scoped_tx);

    (StatusCode::OK, Json(session_facets_json(&facets)))
}

// ── GET /v1/sessions/{session_id} ────────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct GetSessionQuery {
    pub graph_id: Uuid,
}

pub async fn get_session(
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
    Path(session_id): Path<String>,
    Query(q): Query<GetSessionQuery>,
) -> (StatusCode, Json<Value>) {
    let store = require_store!(tenant);

    // IDOR: the ocean must be reachable by the caller — same guard + generic
    // 404 body as every other `meetings`-prefixed detail route.
    match ReachabilityResolver::graph_reachable_by_user(
        tenant.decision.as_ref(),
        store.as_ref(),
        q.graph_id,
        user.user_id,
    )
    .await
    {
        Ok(true) => {}
        Ok(false) => {
            return (
                StatusCode::NOT_FOUND,
                Json(json!({"error": "session not found or access denied"})),
            )
        }
        Err(e) => {
            tracing::error!(error = %e, "get_session reachability error");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            );
        }
    }

    let mut scoped_tx = begin_owner_scoped_tx!(
        tenant,
        store,
        q.graph_id,
        (
            StatusCode::NOT_FOUND,
            Json(json!({"error": "session not found or access denied"})),
        )
    );
    let session = match scoped_store!(
        store,
        scoped_tx,
        get_coding_session / get_coding_session_scoped(q.graph_id, &session_id)
    ) {
        Ok(Some(s)) => s,
        Ok(None) => {
            return (
                StatusCode::NOT_FOUND,
                Json(json!({"error": "session not found or access denied"})),
            )
        }
        Err(e) => {
            tracing::error!(error = %e, "get_session store error");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            );
        }
    };
    // `list_session_atoms`/`count_session_atoms` now have `_scoped` twins
    // (org-tenancy OPS-4 Store-surface follow-up) — reuse the SAME
    // `scoped_tx` opened above for `get_coding_session_scoped` instead of
    // opening a fresh one per call.
    let corals = match scoped_store!(
        store,
        scoped_tx,
        list_session_atoms
            / list_session_atoms_scoped(q.graph_id, &session_id, SESSION_DETAIL_CORAL_CAP + 1)
    ) {
        Ok(c) => c,
        Err(e) => {
            tracing::error!(error = %e, "get_session corals load error");
            close_read_tx!(scoped_tx);
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            );
        }
    };
    let corals_truncated = corals.len() as i64 > SESSION_DETAIL_CORAL_CAP;
    // Titles-always-present (founder requirement): every checkpoint row's
    // `title` is now guaranteed non-empty via `display_title` — a raw
    // `atoms.title` of `null` (untiled, or titled only by ingest's
    // synchronous fallback that then got upgraded elsewhere) falls through
    // to `excerpt`, then to a "{kind label} · {date}" label (the kind label
    // matches `session.session_kind` — "Coding session"/"Meeting session"/
    // "Agent session"), never to the `source_id` the client used to fall
    // back to. `first_checkpoint_title`
    // is captured (before the cap/consumption below) for the SESSION-level
    // title's own fallback rung just below.
    let mut first_checkpoint_title: Option<String> = None;
    // Every row here is a checkpoint atom of THIS session by construction
    // (`list_session_atoms` matches the `"cc:{session_id}:"` source_id
    // prefix) — no per-row `source_type` column on `CodingSessionAtom` (see
    // that struct's doc: "deliberately lightweight"), so `source_type` is
    // derived once from `session.session_kind`
    // ([`crate::ingest::atom::SessionKind::source_type`], the ONE place a
    // session's source type is decided) rather than hardcoded to
    // `"coding_session"` — a meeting- or agent-session's checkpoints must not
    // be labelled as coding ones. `unwrap_or` covers a row written by a
    // newer deploy with a kind this binary doesn't recognise yet (same
    // "still list and display" contract as `CodingSession::session_kind`'s
    // own doc comment).
    let checkpoint_source_type = crate::ingest::atom::SessionKind::parse(&session.session_kind)
        .map(crate::ingest::atom::SessionKind::source_type)
        .unwrap_or(crate::ingest::atom::CODING_SESSION_SOURCE_TYPE);
    let corals_json: Vec<_> = corals
        .into_iter()
        .take(SESSION_DETAIL_CORAL_CAP as usize)
        .map(|a| {
            let display_title = crate::services::atom_title::display_title(
                crate::services::atom_title::DisplayTitleInput {
                    title: a.title.as_deref(),
                    excerpt: a.excerpt.as_deref(),
                    stripped_body: None,
                    source_type: checkpoint_source_type,
                    channel_name: None,
                    author_name: None,
                    timestamp: a.timestamp,
                },
            );
            if first_checkpoint_title.is_none() {
                first_checkpoint_title = Some(display_title.clone());
            }
            json!({
                "atom_id": a.atom_id,
                "source_id": a.source_id,
                "timestamp": a.timestamp,
                "created_at": a.created_at,
                "title": display_title,
                "excerpt": a.excerpt,
            })
        })
        .collect();

    // `coral_count` is the FULL persisted total, not `corals_json.len()` (which
    // is capped at SESSION_DETAIL_CORAL_CAP). Sourcing it from a dedicated
    // COUNT keeps the detail page consistent with GET /v1/sessions'
    // `coral_count` for the same session — otherwise a >cap session would show
    // e.g. "corals · 200" on the detail page but its true count in the list.
    // `corals` above stays the truncated display list; `corals_truncated`
    // still signals that `corals` is a prefix of the whole.
    let coral_count = match scoped_store!(
        store,
        scoped_tx,
        count_session_atoms / count_session_atoms_scoped(q.graph_id, &session_id)
    ) {
        Ok(n) => n,
        Err(e) => {
            tracing::error!(error = %e, "get_session coral count error");
            close_read_tx!(scoped_tx);
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            );
        }
    };
    // Subagents rolled up under this session (any depth). Only a ROOT can
    // have any (`root_session_id = session_id`), so the query is skipped for
    // children and non-agent sessions rather than run for nothing.
    let is_root = session
        .identity
        .root_session_id
        .as_deref()
        .map_or(true, |r| r == session.session_id);
    // Fetched cap+1 so the response can say the array is a PREFIX of the tree
    // (`subagents_truncated`) — same one-extra-row trick as `corals`.
    let subagents = if is_root && session.session_kind == SessionKind::Agent.as_str() {
        match scoped_store!(
            store,
            scoped_tx,
            list_session_subagents
                / list_session_subagents_scoped(q.graph_id, &session_id, SESSION_DETAIL_SUBAGENT_CAP + 1)
        ) {
            Ok(rows) => rows,
            Err(e) => {
                tracing::error!(error = %e, "get_session subagents load error");
                close_read_tx!(scoped_tx);
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({"error": "internal error"})),
                );
            }
        }
    } else {
        Vec::new()
    };
    // The reader's team-seat history floor (#1545) on each item, as for every
    // other source list (crate::history_floor).
    let item_floor = crate::history_floor::admits_source_item_at("s", "v");
    let source_items: Vec<Value> = if let Some(tx) = scoped_tx.as_mut() {
        match sqlx::query_scalar::<_, Value>(&format!("SELECT jsonb_build_object('graph_id',s.graph_id,'source_item_id',s.source_item_id,'source_version_id',v.version_id,'checkpoint_atom_id',(SELECT c.atom_id FROM source_version_chunks c WHERE c.graph_id=v.graph_id AND c.source_version_id=v.version_id AND library_atom_permitted(c.graph_id,c.atom_id) ORDER BY c.atom_id LIMIT 1)) FROM source_items s JOIN source_versions v ON v.version_id=s.current_version_id AND v.graph_id=s.graph_id WHERE s.graph_id=$1 AND s.access_state='permitted' AND v.source_metadata->>'session_id'=$2 AND {item_floor} ORDER BY v.captured_at DESC,s.source_item_id LIMIT 200"))
            .bind(q.graph_id).bind(&session_id).fetch_all(&mut **tx).await {
                Ok(rows) => rows,
                Err(_) => return (StatusCode::SERVICE_UNAVAILABLE, Json(json!({"error":"service unavailable"}))),
            }
    } else { Vec::new() };
    let admitted_times: (Option<chrono::DateTime<chrono::Utc>>, Option<chrono::DateTime<chrono::Utc>>) = if let Some(tx) = scoped_tx.as_mut() {
        match sqlx::query_as(&format!("SELECT min(v.occurred_at),max(v.occurred_at) FROM source_items s JOIN source_versions v ON v.version_id=s.current_version_id AND v.graph_id=s.graph_id WHERE s.graph_id=$1 AND s.access_state='permitted' AND v.source_metadata->>'session_id'=$2 AND {item_floor}"))
            .bind(q.graph_id).bind(&session_id).fetch_one(&mut **tx).await {
                Ok(times) => times,
                Err(_) => return (StatusCode::SERVICE_UNAVAILABLE, Json(json!({"error":"service unavailable"}))),
            }
    } else { (Some(session.started_at),Some(session.last_activity_at)) };
    if tenant.pool.is_some() && coral_count == 0 && source_items.is_empty() {
        return (StatusCode::NOT_FOUND, Json(json!({"error":"session not found or access denied"})));
    }
    close_read_tx!(scoped_tx);

    let session_title = crate::services::atom_title::display_session_title(
        crate::services::atom_title::DisplaySessionTitleInput {
            session_title: session.title.as_deref(),
            checkpoint_title: first_checkpoint_title.as_deref(),
            project_key: session.project_key.as_deref(),
            timestamp: session.started_at,
            session_kind: Some(session.session_kind.as_str()),
            agent_name: session.identity.agent_name.as_deref(),
            task: session.identity.task.as_deref(),
        },
    );

    let subagents_truncated = subagents.len() as i64 > SESSION_DETAIL_SUBAGENT_CAP;
    let subagents_json: Vec<Value> = subagents
        .iter()
        .take(SESSION_DETAIL_SUBAGENT_CAP as usize)
        .map(|s| {
            json!({
                "session_id": s.session_id,
                "parent_session_id": s.parent_session_id,
                "subagent_name": s.subagent_name,
                "task": s.task,
                "title": s.title,
                "started_at": s.started_at,
                "last_activity_at": s.last_activity_at,
                "coral_count": s.coral_count,
            })
        })
        .collect();

    let mut body = json!({
        "session_id": session.session_id,
        "session_kind": session.session_kind,
        "title": session_title,
        "project_key": session.project_key,
        "repo_path": session.repo_path,
        "remote_sanitized": session.remote_sanitized,
        "hostname": session.hostname,
        "harness_version": session.harness_version,
        "model": session.model,
        "branch": session.branch,
        "started_at": session.started_at,
        "last_activity_at": session.last_activity_at,
        "coral_count": coral_count,
        "corals": corals_json,
        "corals_truncated": corals_truncated,
        "subagents": subagents_json,
        "subagents_truncated": subagents_truncated,
    });
    if let Some(obj) = body.as_object_mut() {
        for (k, v) in identity_json(&session.identity) {
            obj.insert(k.to_string(), v);
        }
    }

    if let Some(obj) = body.as_object_mut() {
        // Raw session descriptors and family summaries are not certified
        // merely because this session's graph is authorized.
        for field in ["title","project_key","repo_path","remote_sanitized","hostname","harness_version","model","branch","platform","platform_detected","platform_verified","task","agent_name","subagent_name","root_session_id","parent_session_id"] {
            obj.insert(field.into(), Value::Null);
        }
        obj.insert("started_at".into(), json!(admitted_times.0));
        obj.insert("last_activity_at".into(), json!(admitted_times.1));
        obj.insert("subagents".into(), json!([]));
        obj.insert("subagents_truncated".into(), json!(false));
        obj.insert("source_items".into(), json!(source_items));
        obj.insert("projection_status".into(), json!("unverified_lineage"));
        obj.insert("structure_status".into(), json!("unknown"));
    }
    (StatusCode::OK, Json(body))
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // ── GET /v1/sessions — IDOR ──────────────────────────────────────────────

    #[test]
    fn session_list_filter_canonicalizes_platform_and_defaults_roots_only() {
        let q = ListSessionsQuery {
            graph_id: Uuid::new_v4(),
            project: None,
            kind: Some("agent".into()),
            since: None,
            limit: None,
            cursor: None,
            agent_name: Some("  grokbot ".into()),
            platform: Some("Claude Code".into()),
            task: Some(" CS-042 ".into()),
            harness: Some(" claude-code/2.1 ".into()),
            model: Some(" claude-opus ".into()),
            repo: Some(" github.com/org/repo ".into()),
            branch: Some(" feat/filters ".into()),
            roots_only: None,
            parent_session_id: Some(String::new()),
        };
        let f = session_list_filter(&q);
        assert_eq!(f.agent_name.as_deref(), Some("grokbot"));
        assert_eq!(f.platform.as_deref(), Some("claude-code"));
        assert_eq!(f.task.as_deref(), Some("CS-042"));
        assert_eq!(f.harness.as_deref(), Some("claude-code/2.1"));
        assert_eq!(f.model.as_deref(), Some("claude-opus"));
        assert_eq!(f.repo.as_deref(), Some("github.com/org/repo"));
        assert_eq!(f.branch.as_deref(), Some("feat/filters"));
        assert!(f.roots_only, "roots_only defaults to true");
        assert_eq!(f.parent_session_id, None, "empty parent filter is no filter");

        let q = ListSessionsQuery {
            roots_only: Some(false),
            parent_session_id: Some("root-1".into()),
            ..q
        };
        let f = session_list_filter(&q);
        assert!(!f.roots_only);
        assert_eq!(f.parent_session_id.as_deref(), Some("root-1"));
    }

    #[test]
    fn identity_json_reports_platform_verified_from_the_pair() {
        let verified = crate::store::SessionIdentity {
            platform: Some("claude-code".into()),
            platform_detected: Some("claude-code".into()),
            ..Default::default()
        };
        let mismatch = crate::store::SessionIdentity {
            platform: Some("grok".into()),
            platform_detected: Some("claude-code".into()),
            ..Default::default()
        };
        let undetected = crate::store::SessionIdentity {
            platform: Some("grok".into()),
            ..Default::default()
        };
        let get = |id: &crate::store::SessionIdentity| {
            identity_json(id)
                .into_iter()
                .find(|(k, _)| *k == "platform_verified")
                .map(|(_, v)| v)
                .unwrap()
        };
        assert_eq!(get(&verified), json!(true));
        assert_eq!(get(&mismatch), json!(false));
        assert_eq!(get(&undetected), json!(true));
    }

    /// The Cursor case (founder's bug report): a coding session whose
    /// harness never declared a platform but whose User-Agent was detected
    /// must still report a `platform` (the detected value) AND be verified —
    /// there is no declared value for it to contradict. A session that DID
    /// declare a platform, and it disagrees with detection, must keep
    /// reporting the DECLARED value (never silently swapped for the
    /// detected one) and must be unverified.
    #[test]
    fn identity_json_platform_falls_back_to_detected_when_nothing_was_declared() {
        let get_field = |id: &crate::store::SessionIdentity, field: &str| {
            identity_json(id)
                .into_iter()
                .find(|(k, _)| *k == field)
                .map(|(_, v)| v)
                .unwrap()
        };

        let cursor_undeclared = crate::store::SessionIdentity {
            platform: None,
            platform_detected: Some("cursor".into()),
            ..Default::default()
        };
        assert_eq!(get_field(&cursor_undeclared, "platform"), json!("cursor"));
        assert_eq!(get_field(&cursor_undeclared, "platform_detected"), json!("cursor"));
        assert_eq!(get_field(&cursor_undeclared, "platform_verified"), json!(true));

        let declared_disagrees = crate::store::SessionIdentity {
            platform: Some("claude-code".into()),
            platform_detected: Some("cursor".into()),
            ..Default::default()
        };
        assert_eq!(get_field(&declared_disagrees, "platform"), json!("claude-code"));
        assert_eq!(get_field(&declared_disagrees, "platform_detected"), json!("cursor"));
        assert_eq!(get_field(&declared_disagrees, "platform_verified"), json!(false));
    }

    // ── GET /v1/sessions/facets ──────────────────────────────────────────────

    #[test]
    fn facet_cursor_round_trips_values_containing_the_separator() {
        let cursor = FacetCursor { count: 12, value: "a|b é".into() };
        let encoded = encode_facet_cursor(&cursor);
        assert!(!encoded.contains('=') && !encoded.contains('+') && !encoded.contains('/'));
        assert_eq!(decode_facet_cursor(&encoded), Some(cursor));
    }

    // ── GET /v1/sessions/{session_id} — IDOR + not-found ────────────────────

    // ── Cursor codec (pure, no store) ────────────────────────────────────────

    #[test]
    fn cursor_round_trips() {
        let ts = Utc::now();
        let id = Uuid::new_v4();
        let encoded = encode_sessions_cursor(ts, id);
        let (decoded_ts, decoded_id) = decode_sessions_cursor(&encoded).expect("decodes");
        // RFC3339 round-trips to microsecond precision; compare via a small
        // tolerance-free string compare on the re-encoded form instead of
        // struct equality (chrono's `DateTime` sub-nanosecond storage vs
        // parsed value can differ in the internal representation without
        // being semantically different).
        assert_eq!(decoded_ts.to_rfc3339(), ts.to_rfc3339());
        assert_eq!(decoded_id, id);
    }

    #[test]
    fn cursor_decode_rejects_garbage() {
        assert!(decode_sessions_cursor("not-a-cursor").is_none());
        assert!(decode_sessions_cursor("2026-01-01T00:00:00Z").is_none()); // missing `|id`
        assert!(decode_sessions_cursor("not-a-date|not-a-uuid").is_none());
    }
}

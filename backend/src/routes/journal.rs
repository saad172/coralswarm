//! `GET /v1/journal` — the rebuilt `/today` work journal's read endpoint
//! (Today-journal PR C, stacked on PR A's migration `0113_journal_rollups`/
//! `0024_journal_rollups` + `Store` layer, and PR B's compile worker).
//!
//! `GET /v1/journal?from=<rfc3339>&to=<rfc3339>` — for the authenticated
//! member, across every ocean they can currently reach (ADR-0007 D9, same
//! reachability union `GET /v1/graphs` uses), returns one entry per UTC hour
//! in `[from, to)` that has EITHER a compiled [`JournalRollup`](
//! crate::store::JournalRollup) prose entry OR a non-empty live aggregate.
//! Every returned hour ALWAYS carries `aggregate`: it is the fallback for the
//! in-progress hour, any hour the compile sweep (PR B) hasn't reached yet,
//! and every hour compiled before this endpoint shipped — see
//! [`crate::store::Store::journal_range_aggregate`]'s doc comment. Hours with
//! neither a rollup nor any input are simply OMITTED; the `/today` UI (PR D)
//! derives its own gap rows from that absence rather than this endpoint
//! synthesizing empty placeholders for a caller-chosen span.
//!
//! Modeled structurally on `routes/activity.rs` (`require_store!`, tenant
//! store resolution via `Tenant`, `begin_member_scoped`) — the closest
//! existing "per-member, RLS-scoped, date-bounded" read in this codebase.
//!
//! # Block titles (the live aggregate's `blocks[].title`)
//!
//! A coding-session block's title comes from `coding_sessions.title` (the
//! title-sweep feature) when present. Every other source type has no cheap,
//! DB-local session/meeting-level title today — meeting titles are derived
//! from the raw transcript text at RENDER time (`meeting_title_from_raw`,
//! `routes::meetings`), which can require an R2 decrypt round trip and is
//! wired for a single meeting, not a batch aggregate over an arbitrary
//! caller-chosen span. Pulling that into a deterministic, single-transaction
//! read endpoint was judged out of scope for PR C: it would add a Content
//! Blob fetch (with its own DEK-resolution and R2-availability failure modes)
//! to what is otherwise a pure-SQL aggregate, for every non-coding-session
//! block in the range, on every `/today` page load. Falls back instead to any
//! grouped atom's own per-coral headline (`atoms.title`, migration
//! `0097_atoms_title`) when one exists, then to a generic `source_type`
//! label. Revisit if `/today`'s meeting rows read as too generic in practice
//! — see `Store::journal_range_aggregate`'s own doc comment for the exact
//! precedence.
//!
//! # No pool
//!
//! `journal_rollups` (PR A) and `journal_range_aggregate` (this PR) both
//! need a Postgres pool. When `tenant.pool` is absent this handler returns
//! `{"hours": []}` with `200` rather than a 5xx or a 503 — an empty list is
//! exactly what a client treating an omitted hour as a gap row already
//! handles.

use axum::{
    extract::{Path, Query},
    http::StatusCode,
    Extension, Json,
};
use chrono::{DateTime, Duration, TimeZone, Timelike, Utc};
use serde::Deserialize;
use serde_json::{json, Value};
use std::collections::HashMap;
use utoipa::path as utoipa_path;
use uuid::Uuid;

use crate::{
    middleware::auth::UserContext,
    store::{JournalHourItem, JournalHourItemKind},
    tenancy::extractor::Tenant,
    tenancy::reachability::ReachabilityResolver,
    workers::journal_sweep::{local_day_bounds, local_month_bounds, local_week_bounds},
};

// Content-plane store, resolved by the `Tenant` extractor (org-tenancy PR
// 1.4) — `journal_rollups`/`atoms`/`query_history`/`graphs` are tenant-tree
// tables. Same macro as `routes/activity.rs`.
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

/// Hard cap on `to - from` (validation requirement) — `/today` never needs
/// more than a couple of days in one request, and a wide-open span would turn
/// this into an unbounded full-account atom scan. Per-tier caps below (ticket
/// 15) follow the SAME "a caller-chosen span can't turn this into an
/// unbounded scan" reasoning, scaled to what each tier's zoom level actually
/// needs to show (the approved zoom-UI prototype's month view, for example,
/// never needs more than about a year of month-gists at once).
const JOURNAL_MAX_SPAN_HOURS: i64 = 48;
const JOURNAL_MAX_SPAN_HOURS_DAY: i64 = 62 * 24;
const JOURNAL_MAX_SPAN_HOURS_WEEK: i64 = 26 * 7 * 24;
/// 366, not 365 — an inclusive-enough bound to cover exactly "12 months" even
/// when that span crosses a leap day, without special-casing the leap year.
const JOURNAL_MAX_SPAN_HOURS_MONTH: i64 = 366 * 24;

// ── Granularity ────────────────────────────────────────────────────────────────

/// `?granularity=` — ticket 15's tier zoom levels. `Hour` is the DEFAULT
/// (backward-compatible: an omitted/blank param, or the literal value
/// `"hour"`, all resolve here) so every existing caller of `GET /v1/journal`
/// with no `granularity` param keeps getting EXACTLY today's response shape
/// (`{"hours": [...]}`) — see [`list_journal`]'s branch on this below.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Granularity {
    Hour,
    Day,
    Week,
    Month,
}

impl Granularity {
    pub(crate) fn parse(raw: Option<&str>) -> Result<Self, String> {
        match raw.map(str::trim) {
            None | Some("") | Some("hour") => Ok(Granularity::Hour),
            Some("day") => Ok(Granularity::Day),
            Some("week") => Ok(Granularity::Week),
            Some("month") => Ok(Granularity::Month),
            Some(other) => Err(format!(
                "granularity must be one of hour|day|week|month, got \"{other}\""
            )),
        }
    }

    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Granularity::Hour => "hour",
            Granularity::Day => "day",
            Granularity::Week => "week",
            Granularity::Month => "month",
        }
    }

    /// `(max_span_hours, caller-facing label for the span-guard error)`.
    pub(crate) fn span_guard(self) -> (i64, &'static str) {
        match self {
            Granularity::Hour => (JOURNAL_MAX_SPAN_HOURS, "48 hours"),
            Granularity::Day => (JOURNAL_MAX_SPAN_HOURS_DAY, "62 days"),
            Granularity::Week => (JOURNAL_MAX_SPAN_HOURS_WEEK, "26 weeks"),
            Granularity::Month => (JOURNAL_MAX_SPAN_HOURS_MONTH, "12 months"),
        }
    }

    /// The tier ONE BELOW this one — what a period's LIVE (open, uncompiled)
    /// composition is assembled from (`workers::journal_sweep`'s tier-compile
    /// module doc: "no stored row until a period closes"). `None` for `Hour`
    /// — the hour tier has its own, separate live-aggregate mechanism
    /// ([`crate::store::Store::journal_range_aggregate`]), not this one.
    pub(crate) fn child(self) -> Option<Granularity> {
        match self {
            Granularity::Hour => None,
            Granularity::Day => Some(Granularity::Hour),
            Granularity::Week => Some(Granularity::Day),
            Granularity::Month => Some(Granularity::Week),
        }
    }

    /// The period-boundary function for this tier — `None` for `Hour` (plain
    /// UTC truncation, no timezone involved at all).
    pub(crate) fn bounds_of(
        self,
    ) -> Option<fn(DateTime<Utc>, chrono_tz::Tz) -> (DateTime<Utc>, DateTime<Utc>)> {
        match self {
            Granularity::Hour => None,
            Granularity::Day => Some(local_day_bounds),
            Granularity::Week => Some(local_week_bounds),
            Granularity::Month => Some(local_month_bounds),
        }
    }
}

// ── Query params ──────────────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct JournalQueryParams {
    pub from: Option<String>,
    pub to: Option<String>,
    pub granularity: Option<String>,
}

/// Validates and parses `from`/`to` into a `[from, to)` range: both required,
/// both valid RFC3339, `from < to`, and the span at most `granularity`'s own
/// [`Granularity::span_guard`]. Returns a caller-facing error message (not a
/// raw parse error) on failure — kept as a free function so the validation
/// rules are independently unit-testable without spinning up a router.
pub(crate) fn parse_journal_range(
    params: &JournalQueryParams,
    granularity: Granularity,
) -> Result<(DateTime<Utc>, DateTime<Utc>), String> {
    let from_raw = params
        .from
        .as_deref()
        .ok_or_else(|| "from is required".to_string())?;
    let to_raw = params
        .to
        .as_deref()
        .ok_or_else(|| "to is required".to_string())?;

    let from = DateTime::parse_from_rfc3339(from_raw.trim())
        .map_err(|_| "from must be a valid RFC3339 timestamp".to_string())?
        .with_timezone(&Utc);
    let to = DateTime::parse_from_rfc3339(to_raw.trim())
        .map_err(|_| "to must be a valid RFC3339 timestamp".to_string())?
        .with_timezone(&Utc);

    if from >= to {
        return Err("from must be before to".to_string());
    }
    let (max_span_hours, label) = granularity.span_guard();
    if to - from > Duration::hours(max_span_hours) {
        return Err(format!("from/to span must not exceed {label}"));
    }

    Ok((from, to))
}

// ── Handler ───────────────────────────────────────────────────────────────────

/// `GET /v1/journal` — see the module doc for the full response contract.
/// `granularity=hour` (the default — omitted or `"hour"`) is BYTE-IDENTICAL
/// to the pre-ticket-15 response (`{"hours": [...]}`, unchanged code path
/// below); `granularity=day|week|month` (ticket 15) returns
/// `{"granularity": ..., "periods": [...]}` instead — see the
/// [`tier_periods_response`] doc comment for that shape.
#[utoipa_path(
    get,
    path = "/v1/journal",
    tag = "journal",
    security(("bearer_token" = [])),
    params(
        ("from" = String, Query, description = "RFC3339 range start (inclusive)"),
        ("to"   = String, Query, description = "RFC3339 range end (exclusive); max span depends on granularity"),
        ("granularity" = Option<String>, Query, description = "hour (default) | day | week | month"),
    ),
    responses(
        (status = 200, description = "Journal entries at the requested granularity", body = Value),
        (status = 400, description = "Invalid or missing from/to/granularity"),
        (status = 500, description = "Internal error"),
        (status = 503, description = "Database unavailable"),
    )
)]
pub async fn list_journal(
    state: axum::extract::State<std::sync::Arc<crate::AppState>>,
    tenant: Tenant,
    user: Extension<UserContext>,
    original: Option<Extension<crate::tenancy::runtime::TenancyDecision>>,
    query: Query<std::collections::BTreeMap<String, String>>,
) -> axum::response::Response {
    use axum::response::IntoResponse;
    crate::routes::page_read::serve::<crate::read_models::today::JournalRollups>(
        state,
        tenant,
        user,
        original,
        Path(std::collections::BTreeMap::new()),
        query,
    )
    .await
    .into_response()
}

// ── GET /v1/journal/hour/{hour_start}/items ─────────────────────────────────
//
// Ticket 04's Answer ("Grilling: day/week/month rollup architecture"), API
// section: "New drill-down endpoint `GET /v1/journal/hour/{hour_start}/items`
// — the UNCAPPED everything-view (atoms + events + asks grouped, paginated
// ~50/page, member-scoped via `begin_account_scoped` per the RLS lessons)."
//
// For the authenticated member, every `atoms`/`activity_events`/
// `query_history` row in one UTC hour, across every ocean the caller can
// currently reach (same D9 reachability union `GET /v1/journal` uses) unless
// narrowed to one ocean via `?graph_id=`. Grouped into `atoms[]`/
// `activity[]`/`asks[]` by kind, keyset-paginated as ONE combined stream
// ordered `(occurred_at, id)` ascending — a page may mix kinds — with
// `totals` reporting each kind's FULL count for the hour (unaffected by
// pagination) so `/today` can render "N of M shown" per kind. No caps beyond
// this pagination: unlike `GET /v1/journal`'s live aggregate (which the
// compile sweep's prompt budget bounds), this is the escape hatch a caller
// drills into specifically to see everything, so a busy hour is paginated,
// never truncated.
//
// The hour need not have a stored `journal_rollups` row — this endpoint never
// reads that table at all, so it serves the in-progress hour, any hour the
// sweep hasn't reached, and any hour predating the feature identically (see
// ticket 04's Answer, decision 3: "mirrors the live-hour pattern").

/// Sanity floor for `hour_start` — well before any real CoralSwarm data,
/// chosen only to reject obviously-malformed input (e.g. a mis-parsed epoch)
/// rather than encode any real product boundary. Not the same kind of bound
/// as [`JOURNAL_MAX_SPAN_HOURS`]: this endpoint takes one hour, not a range,
/// so there is no span to cap — just a floor and a "not yet happened" ceiling
/// (checked against `now` at request time, see [`parse_hour_start`]).
fn journal_hour_floor() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap()
}

/// Validates and parses the `{hour_start}` path segment: a valid RFC3339
/// timestamp, landing exactly on a UTC hour boundary (`date_trunc('hour',
/// ...)` — the same convention every `journal_rollups`/live-aggregate
/// `hour_start` already uses), no earlier than [`journal_hour_floor`], and no
/// later than `now` — a genuinely future hour has not happened yet, so there
/// is nothing to drill into (the CURRENT in-progress hour is still valid:
/// `hour_start <= now` holds for it by construction). Free function for the
/// same independent-unit-testability reason as `parse_journal_range`.
fn parse_hour_start(raw: &str, now: DateTime<Utc>) -> Result<DateTime<Utc>, String> {
    let hour_start = DateTime::parse_from_rfc3339(raw.trim())
        .map_err(|_| "hour_start must be a valid RFC3339 timestamp".to_string())?
        .with_timezone(&Utc);

    if hour_start.minute() != 0 || hour_start.second() != 0 || hour_start.nanosecond() != 0 {
        return Err("hour_start must be an exact UTC hour boundary".to_string());
    }
    if hour_start < journal_hour_floor() {
        return Err("hour_start is out of range".to_string());
    }
    if hour_start > now {
        return Err("hour_start must not be in the future".to_string());
    }

    Ok(hour_start)
}

/// `journal_hour_items`'s default/max page size. ~50/page per ticket 16, cap
/// 200 — deliberately smaller than `GET /v1/sessions`'s own 100 default/100
/// max, since one page here mixes THREE row kinds via the UNION query rather
/// than one table.
const JOURNAL_HOUR_ITEMS_DEFAULT_LIMIT: i64 = 50;
const JOURNAL_HOUR_ITEMS_MAX_LIMIT: i64 = 200;

// ── Pagination cursor ────────────────────────────────────────────────────────
//
// Same keyset encoding convention as `routes::sessions`'s
// `encode_sessions_cursor`/`decode_sessions_cursor` (`"{occurred_at
// RFC3339}|{id}"`, opaque to callers) — deliberately duplicated rather than
// imported for the same reason that module gives: generic keyset-pagination
// encoding, not a shared dependency either surface should have to assume the
// other won't change.

fn encode_hour_items_cursor(occurred_at: DateTime<Utc>, id: Uuid) -> String {
    format!("{}|{id}", occurred_at.to_rfc3339())
}

fn decode_hour_items_cursor(cursor: &str) -> Option<(DateTime<Utc>, Uuid)> {
    let (ts, id) = cursor.split_once('|')?;
    let ts = DateTime::parse_from_rfc3339(ts).ok()?.with_timezone(&Utc);
    let id = Uuid::parse_str(id).ok()?;
    Some((ts, id))
}

#[derive(Debug, Deserialize)]
pub struct JournalHourItemsQueryParams {
    /// Narrow to one ocean (must be in the caller's reachable set). Absent =
    /// every reachable ocean, matching `GET /v1/journal`'s own no-graph-
    /// filter default.
    pub graph_id: Option<Uuid>,
    #[serde(default)]
    pub cursor: Option<String>,
    #[serde(default)]
    pub limit: Option<i64>,
}

/// `GET /v1/journal/hour/{hour_start}/items` — see the module doc section
/// above for the full response contract.
#[utoipa_path(
    get,
    path = "/v1/journal/hour/{hour_start}/items",
    tag = "journal",
    security(("bearer_token" = [])),
    params(
        ("hour_start" = String, Path, description = "RFC3339 UTC hour boundary"),
        ("graph_id" = Option<Uuid>, Query, description = "Narrow to one reachable ocean"),
        ("cursor" = Option<String>, Query, description = "Opaque keyset pagination cursor"),
        ("limit" = Option<i64>, Query, description = "Page size (default 50, max 200)"),
    ),
    responses(
        (status = 200, description = "One hour's atoms/activity/asks, keyset-paginated", body = Value),
        (status = 400, description = "Invalid hour_start or cursor"),
        (status = 404, description = "graph_id not reachable by the caller"),
        (status = 500, description = "Internal error"),
        (status = 503, description = "Database unavailable"),
    )
)]
pub async fn journal_hour_items(
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
    Path(hour_start_raw): Path<String>,
    Query(params): Query<JournalHourItemsQueryParams>,
) -> (StatusCode, Json<Value>) {
    let store = require_store!(tenant);

    let hour_start = match parse_hour_start(&hour_start_raw, Utc::now()) {
        Ok(h) => h,
        Err(msg) => return (StatusCode::BAD_REQUEST, Json(json!({"error": msg}))),
    };

    let cursor = match params.cursor.as_deref() {
        Some(c) => match decode_hour_items_cursor(c) {
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
    let limit = params
        .limit
        .unwrap_or(JOURNAL_HOUR_ITEMS_DEFAULT_LIMIT)
        .clamp(1, JOURNAL_HOUR_ITEMS_MAX_LIMIT);

    let empty_response = || {
        json!({
            "hour_start": hour_start,
            "atoms": [],
            "activity": [],
            "asks": [],
            "totals": {"atoms": 0, "activity": 0, "asks": 0},
            "has_more": false,
            "next_cursor": Value::Null,
        })
    };

    // No pool — same degrade-to-empty rationale as the module doc's "No pool"
    // section.
    if tenant.pool.is_none() {
        return (StatusCode::OK, Json(empty_response()));
    }

    // The D9 reachability union (ADR-0007) — exactly `list_journal`'s own
    // resolution, so this endpoint's "which oceans" semantics never drifts
    // from the parent hour list it drills down from.
    let reachable = match ReachabilityResolver::list_reachable_graphs(
        tenant.decision.as_ref(),
        store.as_ref(),
        user.user_id,
    )
    .await
    {
        Ok(g) => g,
        Err(e) => {
            tracing::error!(error = %e, "journal_hour_items reachability error");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            );
        }
    };
    if reachable.is_empty() {
        return (StatusCode::OK, Json(empty_response()));
    }

    let graph_ids: Vec<Uuid> = match params.graph_id {
        Some(gid) => {
            if !reachable.iter().any(|g| g.id == gid) {
                return (
                    StatusCode::NOT_FOUND,
                    Json(json!({"error": "ocean not found or access denied"})),
                );
            }
            vec![gid]
        }
        None => reachable.iter().map(|g| g.id).collect(),
    };
    let graph_names: HashMap<Uuid, &str> =
        reachable.iter().map(|g| (g.id, g.name.as_str())).collect();

    let mut tx = match tenant.begin_member_scoped(user.account_id).await {
        Ok(tx) => tx,
        Err(e) => {
            tracing::error!(error = %e, "journal_hour_items: begin_member_scoped failed");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            );
        }
    };

    let totals = match store
        .journal_hour_item_totals(&mut tx, user.user_id, &graph_ids, hour_start)
        .await
    {
        Ok(t) => t,
        Err(e) => {
            tracing::error!(error = %e, "journal_hour_items: journal_hour_item_totals failed");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            );
        }
    };

    // Fetch one extra row to compute `has_more`/`next_cursor` without a
    // second query — same trick `routes::sessions::list_sessions` uses.
    let rows = match store
        .journal_hour_items(
            &mut tx,
            user.user_id,
            &graph_ids,
            hour_start,
            cursor,
            limit + 1,
        )
        .await
    {
        Ok(r) => r,
        Err(e) => {
            tracing::error!(error = %e, "journal_hour_items: journal_hour_items failed");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            );
        }
    };

    if let Err(e) = tx.commit().await {
        tracing::error!(error = %e, "journal_hour_items: scoped commit failed");
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": "internal error"})),
        );
    }

    let has_more = rows.len() as i64 > limit;
    let page: Vec<JournalHourItem> = rows.into_iter().take(limit as usize).collect();
    let next_cursor = if has_more {
        page.last()
            .map(|item| encode_hour_items_cursor(item.occurred_at, item.id))
    } else {
        None
    };

    let mut atoms = Vec::new();
    let mut activity = Vec::new();
    let mut asks = Vec::new();
    for item in page {
        let graph_name = graph_names.get(&item.graph_id).copied().unwrap_or("");
        match item.kind {
            JournalHourItemKind::Atom {
                source_type,
                excerpt,
                group_label,
            } => {
                // Titles-always-present (founder requirement): this row
                // never carried its own `atoms.title` (`group_label` is a
                // SEPARATE session-aware field, already surfaced below), so
                // `display_title` derives a guaranteed non-empty title from
                // what this item already has on hand — `source_type` +
                // `excerpt` + `occurred_at` — zero new queries.
                let title = crate::services::atom_title::display_title(
                    crate::services::atom_title::DisplayTitleInput {
                        title: None,
                        excerpt: excerpt.as_deref(),
                        stripped_body: None,
                        source_type: &source_type,
                        channel_name: None,
                        author_name: None,
                        timestamp: item.occurred_at,
                    },
                );
                atoms.push(json!({
                    "id": item.id,
                    "graph_id": item.graph_id,
                    "graph_name": graph_name,
                    "source_type": source_type,
                    "title": title,
                    "excerpt": excerpt,
                    "group": group_label,
                    "occurred_at": item.occurred_at,
                }));
            }
            JournalHourItemKind::Activity { event_type, label } => {
                activity.push(json!({
                    "id": item.id,
                    "graph_id": item.graph_id,
                    "graph_name": graph_name,
                    "event_type": event_type,
                    "label": label,
                    "created_at": item.occurred_at,
                }));
            }
            JournalHourItemKind::Ask {
                question,
                citation_count,
            } => {
                asks.push(json!({
                    "id": item.id,
                    "graph_id": item.graph_id,
                    "graph_name": graph_name,
                    "question": question,
                    "citation_count": citation_count,
                    "created_at": item.occurred_at,
                }));
            }
        }
    }

    (
        StatusCode::OK,
        Json(json!({
            "hour_start": hour_start,
            "atoms": atoms,
            "activity": activity,
            "asks": asks,
            "totals": {
                "atoms": totals.atoms,
                "activity": totals.activity,
                "asks": totals.asks,
            },
            "has_more": has_more,
            "next_cursor": next_cursor,
        })),
    )
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        body::Body,
        http::{Request as HttpRequest, StatusCode},
        middleware,
        routing::get,
        Router,
    };
    use std::sync::Arc;
    use tower::ServiceExt;

    use crate::AppState;

    fn journal_router(state: Arc<AppState>) -> Router {
        Router::new()
            .route("/v1/journal", get(list_journal))
            .route(
                "/v1/journal/hour/{hour_start}/items",
                get(journal_hour_items),
            )
            .layer(middleware::from_fn_with_state(
                state.clone(),
                crate::middleware::auth::require_auth,
            ))
            .with_state(state)
    }

    #[tokio::test]
    async fn missing_auth_returns_401() {
        let router = journal_router(crate::test_app_state());
        let req = HttpRequest::builder()
            .uri("/v1/journal?from=2026-08-01T00:00:00Z&to=2026-08-01T12:00:00Z")
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn bearer_no_db_returns_503() {
        let router = journal_router(crate::test_app_state());
        let req = HttpRequest::builder()
            .uri("/v1/journal?from=2026-08-01T00:00:00Z&to=2026-08-01T12:00:00Z")
            .header("Authorization", "Bearer sometoken")
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        // require_auth middleware returns 503 when db=None.
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    // ── parse_journal_range ──────────────────────────────────────────────────

    fn params(from: Option<&str>, to: Option<&str>) -> JournalQueryParams {
        JournalQueryParams {
            from: from.map(str::to_string),
            to: to.map(str::to_string),
            granularity: None,
        }
    }

    #[test]
    fn parse_journal_range_requires_from() {
        let err = parse_journal_range(
            &params(None, Some("2026-08-01T12:00:00Z")),
            Granularity::Hour,
        )
        .unwrap_err();
        assert_eq!(err, "from is required");
    }

    #[test]
    fn parse_journal_range_requires_to() {
        let err = parse_journal_range(
            &params(Some("2026-08-01T00:00:00Z"), None),
            Granularity::Hour,
        )
        .unwrap_err();
        assert_eq!(err, "to is required");
    }

    #[test]
    fn parse_journal_range_rejects_malformed_from() {
        let err = parse_journal_range(
            &params(Some("not-a-timestamp"), Some("2026-08-01T12:00:00Z")),
            Granularity::Hour,
        )
        .unwrap_err();
        assert_eq!(err, "from must be a valid RFC3339 timestamp");
    }

    #[test]
    fn parse_journal_range_rejects_malformed_to() {
        let err = parse_journal_range(
            &params(Some("2026-08-01T00:00:00Z"), Some("not-a-timestamp")),
            Granularity::Hour,
        )
        .unwrap_err();
        assert_eq!(err, "to must be a valid RFC3339 timestamp");
    }

    #[test]
    fn parse_journal_range_rejects_from_after_to() {
        let err = parse_journal_range(
            &params(Some("2026-08-01T12:00:00Z"), Some("2026-08-01T00:00:00Z")),
            Granularity::Hour,
        )
        .unwrap_err();
        assert_eq!(err, "from must be before to");
    }

    #[test]
    fn parse_journal_range_rejects_from_equal_to() {
        let err = parse_journal_range(
            &params(Some("2026-08-01T00:00:00Z"), Some("2026-08-01T00:00:00Z")),
            Granularity::Hour,
        )
        .unwrap_err();
        assert_eq!(err, "from must be before to");
    }

    #[test]
    fn parse_journal_range_rejects_span_over_48_hours() {
        let err = parse_journal_range(
            &params(Some("2026-08-01T00:00:00Z"), Some("2026-08-03T00:00:01Z")),
            Granularity::Hour,
        )
        .unwrap_err();
        assert_eq!(err, "from/to span must not exceed 48 hours");
    }

    #[test]
    fn parse_journal_range_accepts_exactly_48_hours() {
        let (from, to) = parse_journal_range(
            &params(Some("2026-08-01T00:00:00Z"), Some("2026-08-03T00:00:00Z")),
            Granularity::Hour,
        )
        .unwrap();
        assert_eq!((to - from).num_hours(), 48);
    }

    #[test]
    fn parse_journal_range_accepts_a_valid_short_range() {
        let (from, to) = parse_journal_range(
            &params(Some("2026-08-01T00:00:00Z"), Some("2026-08-01T12:00:00Z")),
            Granularity::Hour,
        )
        .unwrap();
        assert!(from < to);
    }

    // ── Granularity parsing + per-tier span guards ────────────────────────────

    #[test]
    fn granularity_parse_defaults_to_hour_on_absent_or_blank() {
        assert_eq!(Granularity::parse(None).unwrap(), Granularity::Hour);
        assert_eq!(Granularity::parse(Some("")).unwrap(), Granularity::Hour);
        assert_eq!(Granularity::parse(Some("hour")).unwrap(), Granularity::Hour);
    }

    #[test]
    fn granularity_parse_accepts_day_week_month() {
        assert_eq!(Granularity::parse(Some("day")).unwrap(), Granularity::Day);
        assert_eq!(Granularity::parse(Some("week")).unwrap(), Granularity::Week);
        assert_eq!(
            Granularity::parse(Some("month")).unwrap(),
            Granularity::Month
        );
    }

    #[test]
    fn granularity_parse_rejects_unknown_values() {
        let err = Granularity::parse(Some("year")).unwrap_err();
        assert_eq!(
            err,
            "granularity must be one of hour|day|week|month, got \"year\""
        );
    }

    #[test]
    fn parse_journal_range_day_span_guard_is_62_days() {
        let ok = parse_journal_range(
            &params(Some("2026-01-01T00:00:00Z"), Some("2026-03-03T00:00:00Z")), // exactly 62 days
            Granularity::Day,
        );
        assert!(ok.is_ok(), "62 days must be accepted: {ok:?}");

        let err = parse_journal_range(
            &params(Some("2026-01-01T00:00:00Z"), Some("2026-03-04T00:00:01Z")), // 62 days + 1s over
            Granularity::Day,
        )
        .unwrap_err();
        assert_eq!(err, "from/to span must not exceed 62 days");
    }

    #[test]
    fn parse_journal_range_week_span_guard_is_26_weeks() {
        let err = parse_journal_range(
            &params(Some("2026-01-01T00:00:00Z"), Some("2026-08-01T00:00:00Z")), // > 26 weeks
            Granularity::Week,
        )
        .unwrap_err();
        assert_eq!(err, "from/to span must not exceed 26 weeks");
    }

    #[test]
    fn parse_journal_range_month_span_guard_is_12_months() {
        let err = parse_journal_range(
            &params(Some("2025-01-01T00:00:00Z"), Some("2027-01-01T00:00:00Z")), // 2 years, way over
            Granularity::Month,
        )
        .unwrap_err();
        assert_eq!(err, "from/to span must not exceed 12 months");

        let ok = parse_journal_range(
            &params(Some("2026-01-01T00:00:00Z"), Some("2026-12-31T00:00:00Z")), // under 12 months
            Granularity::Month,
        );
        assert!(ok.is_ok(), "under 12 months must be accepted: {ok:?}");
    }

    // ── journal_hour_items: missing auth / no db ────────────────────────────

    #[tokio::test]
    async fn hour_items_missing_auth_returns_401() {
        let router = journal_router(crate::test_app_state());
        let req = HttpRequest::builder()
            .uri("/v1/journal/hour/2026-08-01T10:00:00Z/items")
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn hour_items_bearer_no_db_returns_503() {
        let router = journal_router(crate::test_app_state());
        let req = HttpRequest::builder()
            .uri("/v1/journal/hour/2026-08-01T10:00:00Z/items")
            .header("Authorization", "Bearer sometoken")
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    // ── parse_hour_start ─────────────────────────────────────────────────────

    fn now() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 8, 2, 15, 30, 0).unwrap()
    }

    #[test]
    fn parse_hour_start_accepts_an_exact_hour_boundary_in_range() {
        let h = parse_hour_start("2026-08-01T10:00:00Z", now()).unwrap();
        assert_eq!(h, Utc.with_ymd_and_hms(2026, 8, 1, 10, 0, 0).unwrap());
    }

    #[test]
    fn parse_hour_start_accepts_the_current_in_progress_hour() {
        // `now()` is 15:30 — the 15:00 hour has started but not closed; the
        // live-hour pattern (ticket 04's Answer, decision 3) says this must
        // still be drillable.
        let h = parse_hour_start("2026-08-02T15:00:00Z", now()).unwrap();
        assert_eq!(h, Utc.with_ymd_and_hms(2026, 8, 2, 15, 0, 0).unwrap());
    }

    #[test]
    fn parse_hour_start_rejects_malformed_timestamp() {
        let err = parse_hour_start("not-a-timestamp", now()).unwrap_err();
        assert_eq!(err, "hour_start must be a valid RFC3339 timestamp");
    }

    #[test]
    fn parse_hour_start_rejects_a_non_boundary_minute() {
        let err = parse_hour_start("2026-08-01T10:15:00Z", now()).unwrap_err();
        assert_eq!(err, "hour_start must be an exact UTC hour boundary");
    }

    #[test]
    fn parse_hour_start_rejects_a_non_boundary_second() {
        let err = parse_hour_start("2026-08-01T10:00:01Z", now()).unwrap_err();
        assert_eq!(err, "hour_start must be an exact UTC hour boundary");
    }

    #[test]
    fn parse_hour_start_rejects_below_the_sanity_floor() {
        let err = parse_hour_start("1970-01-01T00:00:00Z", now()).unwrap_err();
        assert_eq!(err, "hour_start is out of range");
    }

    #[test]
    fn parse_hour_start_rejects_a_future_hour() {
        let err = parse_hour_start("2026-08-02T16:00:00Z", now()).unwrap_err();
        assert_eq!(err, "hour_start must not be in the future");
    }

    // ── hour-items pagination cursor ────────────────────────────────────────

    #[test]
    fn hour_items_cursor_round_trips() {
        let ts = Utc.with_ymd_and_hms(2026, 8, 1, 10, 5, 0).unwrap();
        let id = Uuid::new_v4();
        let encoded = encode_hour_items_cursor(ts, id);
        let (decoded_ts, decoded_id) = decode_hour_items_cursor(&encoded).expect("decodes");
        assert_eq!(decoded_ts, ts);
        assert_eq!(decoded_id, id);
    }

    #[test]
    fn hour_items_cursor_decode_rejects_garbage() {
        assert!(decode_hour_items_cursor("not-a-cursor").is_none());
        assert!(decode_hour_items_cursor("2026-08-01T10:00:00Z").is_none()); // missing `|id`
        assert!(decode_hour_items_cursor("not-a-date|not-a-uuid").is_none());
    }
}

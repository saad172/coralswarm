//! `GET /v1/retractions` and `GET /v1/retractions/{retraction_id}` — the D6
//! UI's read of `retraction_audit_log` (B2 unit 02's FM-5/D7/D8 substrate,
//! `migrations_control/0196_retraction_audit_log.sql`). Built separately
//! from B2 unit 02 (#718), per
//! `docs/plans/capture/02-b2-retract-fixes.md`'s own instruction: "If the
//! report needs a read endpoint that does not exist yet, BUILD IT (a thin,
//! RLS-respecting read of the audit rows) rather than inventing client-side
//! state."
//!
//! # No route-level role/account gate — unlike `routes::query_audit`
//!
//! `routes::query_audit::list_query_audit` is org-admin-only because THAT
//! table's tenant-plane SELECT policy is `tenant_rls_is_system()`-only — RLS
//! cannot express "org admin", so the route enforces it before ever opening
//! a scoped transaction. `retraction_audit_log`'s SELECT policy (migration
//! 0196) is different: it ALREADY encodes the full D8 visibility rule in
//! SQL — `actor_user_id = app.user_id` (author, every ocean) OR (not a
//! personal-ocean row AND the caller is `owner`/`admin` on the row's
//! `account_id`). Opening the transaction with THIS caller's own
//! `app.account_id`/`app.user_id` GUCs (`begin_tenant_scoped`) and letting
//! RLS filter is therefore the COMPLETE authorization step for both routes
//! below — there is nothing left for a route-level check to add.
//!
//! **Correction (independent review of PR #729, finding 5, BLOCKING).** The
//! admin clause above is keyed on `app.user_id` ONLY — it says nothing about
//! `app.account_id`, so a caller who administers TWO accounts had every row
//! from EVERY account they administer blended into one result, with no
//! predicate distinguishing them and no relationship to which host the
//! request arrived on. "RLS filter is therefore the COMPLETE authorization
//! step" is still true for AUTHORIZATION (can this caller see this row at
//! all); it was never true for SCOPING (which account's rows should this
//! particular request return) — that gap is why every query below now also
//! carries an explicit `account_id = $1`, the same belt-and-suspenders shape
//! `services::query_audit::list_page` already uses on top of its own RLS.
//!
//! # Always the CONTROL pool
//!
//! `retraction_audit_log` has no tenant-plane twin (see the migration's own
//! header, "why the CONTROL plane") — every read here goes through
//! `state.control_db()`, never `tenant.pool`, exactly like
//! `services::retraction_audit::record`'s write path.

use std::sync::Arc;

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    Extension, Json,
};
use serde::Deserialize;
use serde_json::json;
use uuid::Uuid;

use crate::middleware::auth::UserContext;
use crate::services::retraction_audit::{self, RetractionAuditRow};
use crate::AppState;

#[derive(Debug, Deserialize)]
pub struct ListRetractionsParams {
    #[serde(default = "default_page")]
    pub page: u32,
    #[serde(default = "default_page_size")]
    pub page_size: u32,
    /// `RetractionHistory`'s "Not fully removed" header filter (the artboard
    /// draws it: `docs/design/capture/RetractionHistory.dc.html`). `true`
    /// returns only groups with at least one row whose outcome is NOT
    /// `removed` — i.e. exactly the `out_of_window`/`not_a_member` entries
    /// ticket 09 Q15=(c) says must stay FINDABLE rather than being shown
    /// once and dismissed. Applied SERVER-side (see
    /// `services::retraction_audit::list_retraction_ids_page`'s own doc for
    /// why a client-side filter over one page would be an
    /// absence-renders-as-success bug). Defaults to `false` — an absent
    /// param behaves exactly as it did before this field existed.
    #[serde(default)]
    pub unreached_only: bool,
}

fn default_page() -> u32 {
    1
}

fn default_page_size() -> u32 {
    20
}

/// Same clamp shape as `routes::query_audit::query_audit_page_bounds` — a
/// free function so an unbounded `?page_size=` is proven safe without a
/// database (see this module's tests).
fn retractions_page_bounds(params: &ListRetractionsParams) -> (i64, i64) {
    let page_size = params.page_size.clamp(1, 50) as i64;
    let page = params.page.max(1) as i64;
    (page_size, (page - 1) * page_size)
}

/// One `retraction_id` group, as returned to the D6 UI — every row this ONE
/// retract call wrote (`services::retraction_audit`'s own "PER-ATOM, not
/// per-batch" doc: several rows, one shared id), reached and unreached
/// together, exactly the shape `SessionRetracted`/`RetractionHistory` render
/// as one list per D6.
///
/// `caller_user_id` decides `is_own` (independent review of PR #729, finding
/// 4, BLOCKING): D8 deliberately admits an org admin to OTHER members'
/// shared-ocean rows, and both `SessionRetracted` and `RetractionHistory`
/// claim "everything YOU have retracted" / "who retracted it, and when" — an
/// admin reading another member's row with no attribution on it cannot tell
/// the two apart. `actor_user_id`/`is_own` are the fix chosen over the
/// alternative the review also offered (scope the list route to
/// `actor_user_id = caller` and put the admin view behind its own surface):
/// D8's own binding text is "org admin → rows … at per-item granularity (who
/// · when · which coral · outcome)" — ONE surface serving both audiences,
/// distinguished by attribution, IS the D8 shape; narrowing the route would
/// contradict the decision this table exists to implement, not fix a bug in
/// it. No name lookup here (same posture as `oceanName`'s own doc: no
/// separate name-resolution service) — `is_own` alone is enough for the
/// frontend to render "you" vs. a neutral "another member" label rather than
/// a raw id, which is the load-bearing fix (today there is no way to tell
/// the two apart AT ALL); resolving an actual display name for the
/// NOT-own case is real, separate work (a member directory join this route
/// does not have) and is left as a named follow-up, not silently implied.
///
/// `truncated` is finding 6's signal: `rows` may have been fetched with one
/// extra row past [`retraction_audit::MAX_ROWS_PER_RETRACTION_GROUP`]
/// (`rows_for_retraction_ids`'s own doc) — if so, the extra row is dropped
/// here and `truncated: true` is set, so the report says "this is
/// incomplete" instead of rendering a silently-partial success.
fn entry_json(retraction_id: Uuid, rows: &[RetractionAuditRow], caller_user_id: Uuid) -> serde_json::Value {
    let retracted_at = rows.iter().map(|r| r.retracted_at).min();
    let actor_user_id = rows.first().and_then(|r| r.actor_user_id);
    let is_own = actor_user_id == Some(caller_user_id);
    let cap = retraction_audit::MAX_ROWS_PER_RETRACTION_GROUP as usize;
    let truncated = rows.len() > cap;
    let shown = if truncated { &rows[..cap] } else { rows };
    json!({
        "retraction_id": retraction_id,
        "retracted_at": retracted_at,
        "actor_user_id": actor_user_id,
        "is_own": is_own,
        "truncated": truncated,
        "rows": shown.iter().map(|r| json!({
            "graph_id": r.graph_id,
            "is_personal_ocean": r.is_personal_ocean,
            "source_type": r.source_type,
            "source_id": r.source_id,
            "atom_id": r.atom_id,
            "surface": r.surface,
            "outcome": r.outcome,
            "survivors": r.manifest.get("survivors").cloned().unwrap_or_else(|| json!([])),
        })).collect::<Vec<_>>(),
    })
}

/// Group a flat row list (already fetched for a known, ordered set of ids)
/// back into one JSON entry per id, preserving `order`'s sequence — the
/// SAME shape [`list_retraction_ids_page`] decided the page's ranking in.
fn group_rows(rows: Vec<RetractionAuditRow>, order: &[Uuid], caller_user_id: Uuid) -> Vec<serde_json::Value> {
    let mut by_id: std::collections::HashMap<Uuid, Vec<RetractionAuditRow>> =
        std::collections::HashMap::new();
    for r in rows {
        by_id.entry(r.retraction_id).or_default().push(r);
    }
    order
        .iter()
        .filter_map(|id| by_id.remove(id).map(|group| entry_json(*id, &group, caller_user_id)))
        .collect()
}

/// Same grouping as [`group_rows`], but for a flat row list with NO known
/// id order (`rows_for_session` returns rows for however many
/// `retraction_id`s happened to touch one session, not a pre-picked page) —
/// groups are discovered from the rows themselves and sorted newest-group-
/// first by each group's own earliest `retracted_at`, the same ranking
/// [`list_retraction_ids_page`] uses. Backs
/// `routes::retractions::get_session_retractions`.
fn group_rows_newest_first(rows: Vec<RetractionAuditRow>, caller_user_id: Uuid) -> Vec<serde_json::Value> {
    let mut by_id: std::collections::HashMap<Uuid, Vec<RetractionAuditRow>> =
        std::collections::HashMap::new();
    let mut order: Vec<Uuid> = Vec::new();
    for r in rows {
        if !by_id.contains_key(&r.retraction_id) {
            order.push(r.retraction_id);
        }
        by_id.entry(r.retraction_id).or_default().push(r);
    }
    let mut groups: Vec<(Uuid, Vec<RetractionAuditRow>)> = order
        .into_iter()
        .filter_map(|id| by_id.remove(&id).map(|g| (id, g)))
        .collect();
    groups.sort_by(|a, b| {
        let a_min = a.1.iter().map(|r| r.retracted_at).min();
        let b_min = b.1.iter().map(|r| r.retracted_at).min();
        b_min.cmp(&a_min)
    });
    groups
        .into_iter()
        .map(|(id, g)| entry_json(id, &g, caller_user_id))
        .collect()
}

/// Paginated retraction history for the caller — every `retraction_id`
/// group RLS lets them see (D8: their own, every ocean, plus anything an
/// ocean they administer was touched by), newest first. Backs
/// `Settings › Retraction history` (`RetractionHistory`, ticket 09 Q15=(c)
/// — a persisted surface, never a dismissable modal alone, which is exactly
/// what a durable paginated GET route makes possible).
#[utoipa::path(
    get,
    path = "/v1/retractions",
    tag = "retractions",
    params(
        ("page" = Option<u32>, Query, description = "Page number (default 1)"),
        ("page_size" = Option<u32>, Query, description = "Page size (default 20, max 50)"),
        ("unreached_only" = Option<bool>, Query, description = "Only groups with at least one non-`removed` outcome (RetractionHistory's \"Not fully removed\" filter). Default false."),
    ),
    security(("bearer_token" = [])),
    responses(
        (status = 200, description = "Retraction groups visible to the caller, newest first", body = serde_json::Value),
        (status = 503, description = "Database not available"),
    )
)]
pub async fn list_retractions(
    State(state): State<Arc<AppState>>,
    Extension(user): Extension<UserContext>,
    Query(params): Query<ListRetractionsParams>,
) -> impl IntoResponse {
    let Some(pool) = state.control_db() else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error": "service unavailable"})),
        )
            .into_response();
    };
    let (page_size, offset) = retractions_page_bounds(&params);

    let mut tx =
        match crate::tenancy::scope::begin_request_tenant_read(pool, user.account_id, user.user_id).await
        {
            Ok(tx) => tx,
            Err(e) => {
                tracing::error!(error = %e, account_id = %user.account_id, "list_retractions: failed to open scoped transaction");
                return (
                    StatusCode::SERVICE_UNAVAILABLE,
                    Json(json!({"error": "service unavailable"})),
                )
                    .into_response();
            }
        };

    let ids =
        match retraction_audit::list_retraction_ids_page(
            &mut tx,
            user.account_id,
            page_size,
            offset,
            params.unreached_only,
        )
        .await
        {
            Ok(ids) => ids,
            Err(e) => {
                tracing::error!(error = %e, account_id = %user.account_id, "list_retractions: id-page read failed");
                tx.rollback().await.ok();
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({"error": "internal error"})),
                )
                    .into_response();
            }
        };
    if ids.is_empty() {
        tx.rollback().await.ok();
        return (
            StatusCode::OK,
            Json(json!({
                "entries": [],
                "page": params.page.max(1),
                "page_size": page_size,
                // Echoed on EVERY response, including this empty one: an
                // empty list under a filter means "nothing matched the
                // filter", which is a different fact from "nothing at all",
                // and the UI must be able to tell them apart without
                // remembering what it asked for.
                "unreached_only": params.unreached_only,
            })),
        )
            .into_response();
    }

    let rows = match retraction_audit::rows_for_retraction_ids(&mut tx, user.account_id, &ids).await {
        Ok(rows) => rows,
        Err(e) => {
            tracing::error!(error = %e, account_id = %user.account_id, "list_retractions: row read failed");
            tx.rollback().await.ok();
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            )
                .into_response();
        }
    };
    // Read-only — rolled back, not committed (same convention as
    // `routes::query_audit::list_query_audit`).
    tx.rollback().await.ok();

    let entries = group_rows(rows, &ids, user.user_id);
    (
        StatusCode::OK,
        Json(json!({
            "entries": entries,
            "page": params.page.max(1),
            "page_size": page_size,
            "unreached_only": params.unreached_only,
        })),
    )
        .into_response()
}

/// One retraction group by id — the D6 UI's re-hydration read:
/// `SessionRetracted` (a state of `SessionDetail`, ticket 09 Q15=(a)) can
/// render this call's outcomes SYNCHRONOUSLY from the retract response's own
/// `retraction_id` field (`routes::deletion::cascade_json`), but a page
/// reload after that — or a link opened later, e.g. from
/// `RetractionHistory` — has nothing left to render from except this table,
/// especially once FM-6 has deleted the `coding_sessions` row the session
/// page would otherwise have re-fetched.
#[utoipa::path(
    get,
    path = "/v1/retractions/{retraction_id}",
    tag = "retractions",
    params(
        ("retraction_id" = Uuid, Path, description = "The retraction group to read back"),
    ),
    security(("bearer_token" = [])),
    responses(
        (status = 200, description = "The retraction group's rows", body = serde_json::Value),
        (status = 404, description = "No visible row for this id (never existed, or RLS hides every row it has — the two are intentionally indistinguishable, same posture as routes::deletion::require_reachable)"),
        (status = 503, description = "Database not available"),
    )
)]
pub async fn get_retraction(
    State(state): State<Arc<AppState>>,
    Extension(user): Extension<UserContext>,
    Path(retraction_id): Path<Uuid>,
) -> impl IntoResponse {
    let Some(pool) = state.control_db() else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error": "service unavailable"})),
        )
            .into_response();
    };

    let mut tx =
        match crate::tenancy::scope::begin_tenant_scoped(pool, user.account_id, user.user_id).await
        {
            Ok(tx) => tx,
            Err(e) => {
                tracing::error!(error = %e, account_id = %user.account_id, %retraction_id, "get_retraction: failed to open scoped transaction");
                return (
                    StatusCode::SERVICE_UNAVAILABLE,
                    Json(json!({"error": "service unavailable"})),
                )
                    .into_response();
            }
        };

    let rows = match retraction_audit::rows_for_one_retraction(&mut tx, user.account_id, retraction_id)
        .await
    {
        Ok(rows) => rows,
        Err(e) => {
            tracing::error!(error = %e, account_id = %user.account_id, %retraction_id, "get_retraction: read failed");
            tx.rollback().await.ok();
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            )
                .into_response();
        }
    };
    tx.rollback().await.ok();

    if rows.is_empty() {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({"error": "retraction not found"})),
        )
            .into_response();
    }
    (
        StatusCode::OK,
        Json(entry_json(retraction_id, &rows, user.user_id)),
    )
        .into_response()
}

/// Every retraction group that ever touched ONE coding session, newest
/// first — the D6 UI's fix for independent review finding 3 (BLOCKING):
/// retracting a session's checkpoints one at a time mints one
/// `retraction_id` PER checkpoint, so `SessionRetracted`'s report (rendered
/// once the session itself is finally gone, FM-6) must not describe only the
/// LAST of those calls. `session_id` is a caller-supplied string, not a
/// `Uuid` (`add_context`'s own validation has no charset restriction beyond
/// a length cap) — see `services::retraction_audit::rows_for_session`'s doc
/// for why this reads the audit log directly rather than calling
/// `DELETE /v1/graphs/{id}/sources` (rejected: that route's match is exact
/// on `atoms.source_id`, and a session's checkpoints never share one).
#[utoipa::path(
    get,
    path = "/v1/retractions/session/{session_id}",
    tag = "retractions",
    params(
        ("session_id" = String, Path, description = "The coding session's own session_id"),
    ),
    security(("bearer_token" = [])),
    responses(
        (status = 200, description = "Every retraction group that named this session, newest first (may be empty)", body = serde_json::Value),
        (status = 503, description = "Database not available"),
    )
)]
pub async fn get_session_retractions(
    State(state): State<Arc<AppState>>,
    Extension(user): Extension<UserContext>,
    Path(session_id): Path<String>,
) -> impl IntoResponse {
    let Some(pool) = state.control_db() else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error": "service unavailable"})),
        )
            .into_response();
    };

    let mut tx =
        match crate::tenancy::scope::begin_tenant_scoped(pool, user.account_id, user.user_id).await
        {
            Ok(tx) => tx,
            Err(e) => {
                tracing::error!(error = %e, account_id = %user.account_id, %session_id, "get_session_retractions: failed to open scoped transaction");
                return (
                    StatusCode::SERVICE_UNAVAILABLE,
                    Json(json!({"error": "service unavailable"})),
                )
                    .into_response();
            }
        };

    let rows = match retraction_audit::rows_for_session(&mut tx, user.account_id, &session_id).await {
        Ok(rows) => rows,
        Err(e) => {
            tracing::error!(error = %e, account_id = %user.account_id, %session_id, "get_session_retractions: read failed");
            tx.rollback().await.ok();
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            )
                .into_response();
        }
    };
    tx.rollback().await.ok();

    let entries = group_rows_newest_first(rows, user.user_id);
    (StatusCode::OK, Json(json!({ "entries": entries }))).into_response()
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retractions_page_bounds_defaults_to_page_1_size_20() {
        let params = ListRetractionsParams { page: 1, page_size: 20, unreached_only: false };
        assert_eq!(retractions_page_bounds(&params), (20, 0));
    }

    #[test]
    fn retractions_page_bounds_clamps_page_size_to_fifty() {
        let params = ListRetractionsParams { page: 1, page_size: 100_000, unreached_only: false };
        let (page_size, _) = retractions_page_bounds(&params);
        assert_eq!(page_size, 50, "an unbounded LIMIT must never reach the store");
    }

    #[test]
    fn retractions_page_bounds_clamps_zero_page_up_to_one() {
        let params = ListRetractionsParams { page: 0, page_size: 20, unreached_only: false };
        let (_, offset) = retractions_page_bounds(&params);
        assert_eq!(offset, 0, "page=0 must not produce a negative offset");
    }

    #[test]
    fn retractions_page_bounds_computes_offset_for_later_pages() {
        let params = ListRetractionsParams { page: 3, page_size: 20, unreached_only: false };
        assert_eq!(retractions_page_bounds(&params), (20, 40));
    }

    fn row(retraction_id: Uuid, graph_id: Uuid, surface: &str, outcome: &str) -> RetractionAuditRow {
        row_by(retraction_id, graph_id, surface, outcome, Uuid::new_v4())
    }

    fn row_by(
        retraction_id: Uuid,
        graph_id: Uuid,
        surface: &str,
        outcome: &str,
        actor_user_id: Uuid,
    ) -> RetractionAuditRow {
        RetractionAuditRow {
            retraction_id,
            retracted_at: chrono::Utc::now(),
            actor_user_id: Some(actor_user_id),
            source_type: "coding_session".to_string(),
            source_id: "cc:test-session:0".to_string(),
            atom_id: None,
            graph_id,
            is_personal_ocean: false,
            surface: surface.to_string(),
            outcome: outcome.to_string(),
            manifest: json!({}),
        }
    }

    /// D6's own negative control (`02-b2-retract-fixes.md`): "an unreached
    /// copy renders in the same list as the reached ones with a named
    /// reason". Proven here at the JSON-shaping layer — `group_rows` must
    /// never split `removed` and `out_of_window`/`not_a_member` rows into
    /// separate entries or drop either kind.
    #[test]
    fn group_rows_keeps_reached_and_unreached_rows_in_one_entry() {
        let rid = Uuid::new_v4();
        let g1 = Uuid::new_v4();
        let g2 = Uuid::new_v4();
        let rows = vec![
            row(rid, g1, "content", "removed"),
            row(rid, g1, "journal_rollup", "out_of_window"),
            row(rid, g2, "content", "not_a_member"),
        ];
        let entries = group_rows(rows, &[rid], Uuid::new_v4());
        assert_eq!(entries.len(), 1, "one retraction_id must render as exactly one entry");
        let entry_rows = entries[0]["rows"].as_array().expect("rows must be an array");
        assert_eq!(entry_rows.len(), 3, "reached and unreached rows must both survive in the SAME list");
        let outcomes: Vec<&str> = entry_rows.iter().map(|r| r["outcome"].as_str().unwrap()).collect();
        assert!(outcomes.contains(&"removed"));
        assert!(outcomes.contains(&"out_of_window"));
        assert!(outcomes.contains(&"not_a_member"));
    }

    #[test]
    fn group_rows_preserves_the_requested_id_order_and_skips_unmatched_ids() {
        let rid1 = Uuid::new_v4();
        let rid2 = Uuid::new_v4();
        let missing = Uuid::new_v4();
        let rows = vec![row(rid2, Uuid::new_v4(), "content", "removed"), row(rid1, Uuid::new_v4(), "content", "removed")];
        // Order argument says rid1 first, then the never-visible `missing` id,
        // then rid2 — the output must follow THIS order and silently drop the
        // id that had zero visible rows (RLS may have hidden all of them).
        let entries = group_rows(rows, &[rid1, missing, rid2], Uuid::new_v4());
        assert_eq!(entries.len(), 2, "an id with zero rows must be dropped, not rendered empty");
        assert_eq!(entries[0]["retraction_id"].as_str().unwrap(), rid1.to_string());
        assert_eq!(entries[1]["retraction_id"].as_str().unwrap(), rid2.to_string());
    }

    #[test]
    fn entry_json_reads_survivors_out_of_the_manifest() {
        let rid = Uuid::new_v4();
        let mut r = row(rid, Uuid::new_v4(), "journal_rollup", "out_of_window");
        let survivor = Uuid::new_v4().to_string();
        r.manifest = json!({"survivors": [survivor.clone()]});
        let entry = entry_json(rid, &[r], Uuid::new_v4());
        let survivors = entry["rows"][0]["survivors"].as_array().expect("survivors must be an array");
        assert_eq!(survivors.len(), 1);
        assert_eq!(survivors[0].as_str().unwrap(), survivor);
    }

    #[test]
    fn entry_json_defaults_survivors_to_an_empty_array_when_absent() {
        let rid = Uuid::new_v4();
        let r = row(rid, Uuid::new_v4(), "content", "removed");
        let entry = entry_json(rid, &[r], Uuid::new_v4());
        assert_eq!(entry["rows"][0]["survivors"].as_array().expect("must be an array").len(), 0);
    }

    /// Finding 4 (BLOCKING): an admin reading THEIR OWN retraction must see
    /// `is_own: true` and the raw `actor_user_id` back — an admin reading
    /// another member's row (D8's own admit-other-members'-rows clause) must
    /// see `is_own: false`, never the same shape rendered as if it were
    /// theirs.
    #[test]
    fn entry_json_marks_is_own_true_only_when_the_caller_is_the_actor() {
        let rid = Uuid::new_v4();
        let author = Uuid::new_v4();
        let admin = Uuid::new_v4();
        let r = row_by(rid, Uuid::new_v4(), "content", "removed", author);

        let as_author = entry_json(rid, &[r.clone()], author);
        assert_eq!(as_author["is_own"].as_bool().unwrap(), true);
        assert_eq!(as_author["actor_user_id"].as_str().unwrap(), author.to_string());

        let as_admin = entry_json(rid, &[r], admin);
        assert_eq!(
            as_admin["is_own"].as_bool().unwrap(),
            false,
            "an admin reading ANOTHER member's row must not see it marked as their own"
        );
        assert_eq!(as_admin["actor_user_id"].as_str().unwrap(), author.to_string());
    }

    /// Finding 6 (HIGH): a group past `MAX_ROWS_PER_RETRACTION_GROUP` renders
    /// exactly `cap` rows and `truncated: true` — never a silently-partial
    /// "complete" report, and never more than the cap in one JSON body.
    #[test]
    fn entry_json_truncates_and_flags_a_group_past_the_row_cap() {
        let rid = Uuid::new_v4();
        let cap = retraction_audit::MAX_ROWS_PER_RETRACTION_GROUP as usize;
        let rows: Vec<RetractionAuditRow> = (0..cap + 1)
            .map(|_| row(rid, Uuid::new_v4(), "content", "removed"))
            .collect();
        let entry = entry_json(rid, &rows, Uuid::new_v4());
        assert_eq!(entry["truncated"].as_bool().unwrap(), true);
        assert_eq!(entry["rows"].as_array().unwrap().len(), cap);
    }

    #[test]
    fn entry_json_does_not_flag_truncation_at_exactly_the_cap() {
        let rid = Uuid::new_v4();
        let cap = retraction_audit::MAX_ROWS_PER_RETRACTION_GROUP as usize;
        let rows: Vec<RetractionAuditRow> = (0..cap)
            .map(|_| row(rid, Uuid::new_v4(), "content", "removed"))
            .collect();
        let entry = entry_json(rid, &rows, Uuid::new_v4());
        assert_eq!(entry["truncated"].as_bool().unwrap(), false);
        assert_eq!(entry["rows"].as_array().unwrap().len(), cap);
    }

    /// Finding 3 (BLOCKING): `group_rows_newest_first` — used by
    /// `get_session_retractions` — must recover EVERY `retraction_id` group
    /// present in an unordered row list (simulating several per-checkpoint
    /// retract calls for the same session) and rank them newest-group-first,
    /// the same ranking the paginated list route uses.
    #[test]
    fn group_rows_newest_first_recovers_every_group_newest_first() {
        let older = Uuid::new_v4();
        let newer = Uuid::new_v4();
        let g = Uuid::new_v4();
        let mut older_row = row(older, g, "content", "removed");
        older_row.retracted_at = chrono::Utc::now() - chrono::Duration::hours(1);
        let mut newer_row = row(newer, g, "content", "removed");
        newer_row.retracted_at = chrono::Utc::now();
        // Deliberately fed OLDEST-first, so a bug that just preserved input
        // order (rather than sorting by each group's own timestamp) would
        // still pass a naively-ordered fixture — this one wouldn't.
        let entries = group_rows_newest_first(vec![older_row, newer_row], Uuid::new_v4());
        assert_eq!(entries.len(), 2, "both retraction_id groups must survive");
        assert_eq!(
            entries[0]["retraction_id"].as_str().unwrap(),
            newer.to_string(),
            "the newer group must sort first"
        );
        assert_eq!(entries[1]["retraction_id"].as_str().unwrap(), older.to_string());
    }

    #[tokio::test]
    async fn list_retractions_with_no_db_returns_503() {
        let state = crate::test_app_state();
        let user = UserContext {
            user_id: uuid::Uuid::new_v4(),
            account_id: uuid::Uuid::new_v4(),
            role: "member".to_string(),
            device_id: "dev-1".to_string(),
        };
        let resp = list_retractions(
            State(state),
            Extension(user),
            Query(ListRetractionsParams { page: 1, page_size: 20, unreached_only: false }),
        )
        .await
        .into_response();
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn get_retraction_with_no_db_returns_503() {
        let state = crate::test_app_state();
        let user = UserContext {
            user_id: uuid::Uuid::new_v4(),
            account_id: uuid::Uuid::new_v4(),
            role: "member".to_string(),
            device_id: "dev-1".to_string(),
        };
        let resp = get_retraction(State(state), Extension(user), Path(Uuid::new_v4()))
            .await
            .into_response();
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    }
}

// ── Live-Postgres tests: the routes' own SQL/grouping/JSON-shaping logic,
// against a real `retraction_audit_log` ─────────────────────────────────────
//
// **These run owner-connected (BYPASSRLS) — same caveat
// `routes::query_audit::query_audit_pg_tests`'s own module doc states for
// the identical reason**: this proves the ROUTE's own logic (pagination,
// grouping several rows under one `retraction_id`, reading `survivors` back
// out of `manifest`, the 404-on-empty-result shape) — NOT the RLS backstop
// itself. D8's actual visibility rule (author-all-oceans + admin-of-ocean,
// personal-author-only) is proven under the REAL, non-BYPASSRLS
// `coralswarm_app` role by
// `tests/consumer_rls_pg.rs::retraction_audit_log_rls_enforces_d8_visibility_and_is_append_only`
// — that test is this one's authorization complement, not a duplicate of it.
//
// In-crate, not `backend/tests/*_pg.rs`, for the same reason
// `routes::query_audit::query_audit_pg_tests` gives: `UserContext` is not
// `pub` outside the crate. Gated on `ORGT_TEST_BASE_URL`, same scratch-DB
// convention as every other `*_pg_tests` module in this codebase.
//
//   ORGT_TEST_BASE_URL=postgres://postgres:postgres@localhost:55987/postgres \
//     cargo test -p coralswarm-backend --lib routes::retractions::retractions_pg_tests -- --nocapture
#[cfg(test)]
mod retractions_pg_tests {
    use super::*;
    use crate::services::connector_audit::Actor;
    use crate::services::deletion::{outcome, SurfaceOutcome};
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
    async fn admin_conn(base: &str) -> PgConnection {
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

    async fn seed_account(pool: &sqlx::PgPool, email: &str) -> (uuid::Uuid, uuid::Uuid) {
        let user_id: uuid::Uuid =
            sqlx::query_scalar("INSERT INTO users (email) VALUES ($1) RETURNING id")
                .bind(email)
                .fetch_one(pool)
                .await
                .unwrap();
        let account_id: uuid::Uuid = sqlx::query_scalar(
            "INSERT INTO accounts (owner_user_id, plan_tier) VALUES ($1, 'free') RETURNING id",
        )
        .bind(user_id)
        .fetch_one(pool)
        .await
        .unwrap();
        (user_id, account_id)
    }

    fn user_ctx(account_id: uuid::Uuid, user_id: uuid::Uuid, role: &str) -> UserContext {
        UserContext {
            user_id,
            account_id,
            role: role.to_string(),
            device_id: "pg-test".to_string(),
        }
    }

    fn surface_outcome(graph_id: uuid::Uuid, is_personal: bool, surface: &'static str, outcome: &'static str) -> SurfaceOutcome {
        SurfaceOutcome {
            graph_id,
            is_personal_ocean: is_personal,
            source_type: "coding_session".to_string(),
            source_id: "cc:pg-test-session:0".to_string(),
            atom_id: Some(uuid::Uuid::new_v4()),
            surface,
            outcome,
            survivors: vec![],
        }
    }

    /// Two retract calls (two `retraction_id`s, one of them a 2-row group
    /// spanning a `removed` + an `out_of_window` surface — D6's own bar,
    /// "reached and unreached in one list") land as two correctly-grouped,
    /// correctly-paginated, newest-first entries; a page_size of 1 returns
    /// only the newer one; `get_retraction` re-hydrates either id directly,
    /// and an unknown id is a 404, never a panic or an empty-but-200 body.
    #[tokio::test]
    async fn list_and_get_retractions_group_and_paginate_real_rows_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping list_and_get_retractions_group_and_paginate_real_rows_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "retractions_route_grouping";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let mut conn = admin_conn(&base).await;
        recreate(&mut conn, db).await;
        let pool = control_pool(&base, db).await;

        let (author, account_id) = seed_account(&pool, "retractions-author@test.local").await;

        let older_id = uuid::Uuid::new_v4();
        retraction_audit::record(
            &pool,
            account_id,
            Actor::User(author),
            older_id,
            &[surface_outcome(uuid::Uuid::new_v4(), true, "content", outcome::REMOVED)],
        )
        .await
        .expect("record (older) must succeed");

        // A real clock tick between the two calls, so `ORDER BY MIN(retracted_at)
        // DESC` has an unambiguous newer/older pair to sort — `retracted_at`
        // defaults to `now()` at insert time, no caller-supplied timestamp.
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        let newer_id = uuid::Uuid::new_v4();
        retraction_audit::record(
            &pool,
            account_id,
            Actor::User(author),
            newer_id,
            &[
                surface_outcome(uuid::Uuid::new_v4(), false, "content", outcome::REMOVED),
                surface_outcome(uuid::Uuid::new_v4(), false, "journal_rollup", outcome::OUT_OF_WINDOW),
            ],
        )
        .await
        .expect("record (newer, 2 rows) must succeed");

        // `state.control_db()` must resolve to THIS test's pool — plain
        // `test_app_state()` leaves `db: None` (right for the 503 unit tests
        // above; wrong here, where the route needs a real connection).
        let state = Arc::new(crate::test_app_state_owned_with_db(pool.clone(), None));
        let user = user_ctx(account_id, author, "member");

        // ── list: two entries, newer group first, its 2 rows in ONE entry ───
        let list_resp = list_retractions(
            State(state.clone()),
            Extension(user.clone()),
            Query(ListRetractionsParams { page: 1, page_size: 20, unreached_only: false }),
        )
        .await
        .into_response();
        assert_eq!(list_resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(list_resp.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let entries = json["entries"].as_array().unwrap();
        assert_eq!(entries.len(), 2, "two retract calls must produce exactly two entries");
        assert_eq!(entries[0]["retraction_id"].as_str().unwrap(), newer_id.to_string(), "newest retraction_id must sort first");
        assert_eq!(entries[0]["rows"].as_array().unwrap().len(), 2, "the newer call's removed + out_of_window rows must be in the SAME entry (D6)");
        assert_eq!(entries[1]["retraction_id"].as_str().unwrap(), older_id.to_string());
        assert_eq!(entries[1]["rows"].as_array().unwrap().len(), 1);

        // ── pagination: page_size=1 returns only the newer entry ────────────
        let page1_resp = list_retractions(
            State(state.clone()),
            Extension(user.clone()),
            Query(ListRetractionsParams { page: 1, page_size: 1, unreached_only: false }),
        )
        .await
        .into_response();
        let body = axum::body::to_bytes(page1_resp.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let entries = json["entries"].as_array().unwrap();
        assert_eq!(entries.len(), 1, "page_size=1 must return exactly one entry");
        assert_eq!(entries[0]["retraction_id"].as_str().unwrap(), newer_id.to_string());

        // ── "Not fully removed": the SERVER-side filter, with its own
        // negative control ───────────────────────────────────────────────────
        //
        // The assertion two blocks up (`entries.len() == 2` unfiltered) IS
        // this test's negative control: both groups are visible without the
        // flag, so a filtered result of 1 proves the `HAVING` selected,
        // rather than the fixture simply not containing the other group. The
        // older group is `removed`-only; the newer one carries the
        // `out_of_window` row, so exactly the newer one must survive the
        // filter. A filter that matched nothing (0 entries) would be the
        // failure this program keeps re-finding — an empty list reading as
        // "nothing left behind".
        let filtered_resp = list_retractions(
            State(state.clone()),
            Extension(user.clone()),
            Query(ListRetractionsParams { page: 1, page_size: 20, unreached_only: true }),
        )
        .await
        .into_response();
        assert_eq!(filtered_resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(filtered_resp.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(
            json["unreached_only"].as_bool(),
            Some(true),
            "the response must echo the filter it applied, so an empty list can never be mistaken for an empty history",
        );
        let entries = json["entries"].as_array().unwrap();
        assert_eq!(
            entries.len(),
            1,
            "unreached_only must keep ONLY the group holding a non-removed outcome — got {entries:?}",
        );
        assert_eq!(
            entries[0]["retraction_id"].as_str().unwrap(),
            newer_id.to_string(),
            "the surviving group must be the out_of_window one, not whichever sorted first",
        );
        assert_eq!(
            entries[0]["rows"].as_array().unwrap().len(),
            2,
            "the filter selects GROUPS, never rows: the matching group keeps its removed row too (D6's one list)",
        );

        // ── get_retraction: re-hydrates the older id directly, with its real
        // outcome value, not a placeholder ───────────────────────────────────
        let get_resp = get_retraction(State(state.clone()), Extension(user.clone()), Path(older_id))
            .await
            .into_response();
        assert_eq!(get_resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(get_resp.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["rows"].as_array().unwrap().len(), 1);
        assert_eq!(json["rows"][0]["outcome"].as_str().unwrap(), "removed");
        assert_eq!(json["rows"][0]["is_personal_ocean"].as_bool().unwrap(), true);

        // ── an id nothing was ever written under is a 404, never a 200 with
        // an empty body ──────────────────────────────────────────────────────
        let missing_resp = get_retraction(State(state), Extension(user), Path(uuid::Uuid::new_v4()))
            .await
            .into_response();
        assert_eq!(missing_resp.status(), StatusCode::NOT_FOUND);

        pool.close().await;
        conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .ok();
    }

    /// Finding 5 (BLOCKING), against REAL Postgres RLS (BYPASSRLS — same
    /// caveat this module's own doc states: this proves the route's own
    /// account_id predicate, not the RLS backstop itself). ONE user
    /// (`admin`) owns TWO separate accounts and writes a retraction row under
    /// each. Before the fix, `list_retraction_ids_page`/`rows_for_
    /// retraction_ids` carried no `account_id` predicate at all, so a
    /// request scoped to account A's `UserContext` (e.g. because that's the
    /// host the request arrived on) returned BOTH accounts' rows blended —
    /// this asserts it now returns EXACTLY account A's row, never B's.
    #[tokio::test]
    async fn list_retractions_never_blends_a_second_account_the_caller_also_administers_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping list_retractions_never_blends_a_second_account_the_caller_also_administers_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "retractions_route_account_scope";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let mut conn = admin_conn(&base).await;
        recreate(&mut conn, db).await;
        let pool = control_pool(&base, db).await;

        let (admin, account_a) = seed_account(&pool, "cross-account-admin@test.local").await;
        // A second account, OWNED BY THE SAME USER — same shape as "an admin
        // who administers two accounts", the exact scenario the finding
        // names. `seed_account` always makes its user the `owner_user_id` of
        // the account it creates, so `admin` genuinely owns/administers both.
        let (_second_owner, account_b) = seed_account(&pool, "second-account-owner@test.local").await;

        let a_retraction = uuid::Uuid::new_v4();
        retraction_audit::record(
            &pool,
            account_a,
            Actor::User(admin),
            a_retraction,
            &[surface_outcome(uuid::Uuid::new_v4(), false, "content", outcome::REMOVED)],
        )
        .await
        .expect("record under account A must succeed");

        let b_retraction = uuid::Uuid::new_v4();
        retraction_audit::record(
            &pool,
            account_b,
            Actor::User(admin),
            b_retraction,
            &[surface_outcome(uuid::Uuid::new_v4(), false, "content", outcome::REMOVED)],
        )
        .await
        .expect("record under account B must succeed");

        let state = Arc::new(crate::test_app_state_owned_with_db(pool.clone(), None));
        // The request's UserContext names account A — the same shape as "the
        // host this request arrived on".
        let user_on_a = user_ctx(account_a, admin, "owner");

        let list_resp = list_retractions(
            State(state.clone()),
            Extension(user_on_a.clone()),
            Query(ListRetractionsParams { page: 1, page_size: 20, unreached_only: false }),
        )
        .await
        .into_response();
        assert_eq!(list_resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(list_resp.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let entries = json["entries"].as_array().unwrap();
        assert_eq!(
            entries.len(),
            1,
            "a request scoped to account A must return ONLY account A's retractions, never account B's blended in"
        );
        assert_eq!(entries[0]["retraction_id"].as_str().unwrap(), a_retraction.to_string());

        // `get_retraction` must refuse the SAME cross-account leak for a
        // direct-by-id read: account A's caller asking for account B's
        // `retraction_id` is a 404, never a 200 with someone else's row.
        let cross_get = get_retraction(State(state), Extension(user_on_a), Path(b_retraction))
            .await
            .into_response();
        assert_eq!(
            cross_get.status(),
            StatusCode::NOT_FOUND,
            "a caller scoped to account A must not be able to read account B's retraction by id"
        );

        pool.close().await;
        conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .ok();
    }

    /// Finding 3 (BLOCKING): retracting a session's checkpoints ONE AT A TIME
    /// mints one `retraction_id` per call — this proves
    /// `get_session_retractions` recovers ALL of them (not just the last),
    /// grouped and ranked newest-first, so `SessionRetracted`'s report can
    /// describe the session's WHOLE retraction history, not only the call
    /// that finally emptied it.
    #[tokio::test]
    async fn get_session_retractions_recovers_every_checkpoint_call_for_one_session_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping get_session_retractions_recovers_every_checkpoint_call_for_one_session_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "retractions_route_session_history";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let mut conn = admin_conn(&base).await;
        recreate(&mut conn, db).await;
        let pool = control_pool(&base, db).await;

        let (author, account_id) = seed_account(&pool, "session-history-author@test.local").await;
        let graph = uuid::Uuid::new_v4();
        let session_id = "pg-test-session-abc";

        // Checkpoint 1 of 2: an ordinary "content" row, own retraction_id.
        let checkpoint1 = uuid::Uuid::new_v4();
        retraction_audit::record(
            &pool,
            account_id,
            Actor::User(author),
            checkpoint1,
            &[SurfaceOutcome {
                graph_id: graph,
                is_personal_ocean: true,
                source_type: "coding_session".to_string(),
                source_id: format!("cc:{session_id}:checkpoint-1"),
                atom_id: Some(uuid::Uuid::new_v4()),
                surface: "content",
                outcome: outcome::REMOVED,
                survivors: vec![],
            }],
        )
        .await
        .expect("record (checkpoint 1) must succeed");

        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        // Checkpoint 2 of 2: the LAST checkpoint — a "content" row AND the
        // session-level "coding_session" surface row FM-6 writes when the
        // session becomes empty, both under this SAME (second) retraction_id.
        let checkpoint2 = uuid::Uuid::new_v4();
        retraction_audit::record(
            &pool,
            account_id,
            Actor::User(author),
            checkpoint2,
            &[
                SurfaceOutcome {
                    graph_id: graph,
                    is_personal_ocean: true,
                    source_type: "coding_session".to_string(),
                    source_id: format!("cc:{session_id}:checkpoint-2"),
                    atom_id: Some(uuid::Uuid::new_v4()),
                    surface: "content",
                    outcome: outcome::REMOVED,
                    survivors: vec![],
                },
                SurfaceOutcome {
                    graph_id: graph,
                    is_personal_ocean: true,
                    source_type: "coding_session".to_string(),
                    source_id: session_id.to_string(),
                    atom_id: None,
                    surface: "coding_session",
                    outcome: outcome::REMOVED,
                    survivors: vec![],
                },
            ],
        )
        .await
        .expect("record (checkpoint 2 + session row) must succeed");

        // A DIFFERENT, unrelated retraction (a different session entirely) —
        // must NOT show up in this session's report.
        let unrelated = uuid::Uuid::new_v4();
        retraction_audit::record(
            &pool,
            account_id,
            Actor::User(author),
            unrelated,
            &[SurfaceOutcome {
                graph_id: graph,
                is_personal_ocean: true,
                source_type: "coding_session".to_string(),
                source_id: "cc:some-other-session:checkpoint-1".to_string(),
                atom_id: Some(uuid::Uuid::new_v4()),
                surface: "content",
                outcome: outcome::REMOVED,
                survivors: vec![],
            }],
        )
        .await
        .expect("record (unrelated session) must succeed");

        let state = Arc::new(crate::test_app_state_owned_with_db(pool.clone(), None));
        let user = user_ctx(account_id, author, "member");

        let resp = get_session_retractions(State(state), Extension(user), Path(session_id.to_string()))
            .await
            .into_response();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let entries = json["entries"].as_array().unwrap();
        assert_eq!(
            entries.len(),
            2,
            "BOTH per-checkpoint retraction_id groups for this session must be returned, not only the last one"
        );
        assert_eq!(
            entries[0]["retraction_id"].as_str().unwrap(),
            checkpoint2.to_string(),
            "newest group (the one that ended the session) must sort first"
        );
        assert_eq!(entries[0]["rows"].as_array().unwrap().len(), 2, "checkpoint 2's group carries BOTH its content row and the FM-6 session row");
        assert_eq!(entries[1]["retraction_id"].as_str().unwrap(), checkpoint1.to_string());
        let all_ids: Vec<&str> = entries
            .iter()
            .map(|e| e["retraction_id"].as_str().unwrap())
            .collect();
        assert!(!all_ids.contains(&unrelated.to_string().as_str()), "an unrelated session's retraction must not appear");

        pool.close().await;
        conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .ok();
    }
}

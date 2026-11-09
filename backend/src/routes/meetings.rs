//! Post-meeting summary + transcript name-correction revert controls
//! (cs-raw-provenance).
//!
//! - `GET /v1/meetings?graph_id=&limit=&cursor=` — metadata-only source list,
//!   newest ingestion first, with graph-bound keyset pagination. Event time and
//!   participants come from exact source/chunk metadata; missing values and
//!   unavailable titles remain null. No transcript/blob fetch occurs.
//! - `GET  /v1/meetings/summary?graph_id=&source_id=` — the verbatim raw
//!   transcript, a `corrected_text` overlay (whole-word, case-insensitive
//!   surface→canonical replacements for the `applied` corrections only), the
//!   correction provenance list, best-effort entity merges / new entities this
//!   meeting produced, and a best-effort nullable `title` + `occurred_at` (cs-
//!   candidate-meeting-labels) so the frontend can show a human label instead
//!   of deriving one from provenance timestamps.
//! - `POST /v1/transcript-corrections/{id}/revert`  — set a correction `reverted`.
//! - `POST /v1/transcript-corrections/{id}/restore` — set a correction `applied`.
//!
//! The raw transcript text is NEVER mutated: corrections are an overlay applied
//! only when building `corrected_text`. All routes are IDOR-guarded by graph
//! reachability (ADR-0007 D9), mirroring the atom/merge routes.
//!
//! # Consumer-plane RLS scoping (org-tenancy OPS-4 phase B.5)
//!
//! Every query here that touches an RLS-covered consumer table
//! (`meeting_transcripts`, `transcript_corrections`, `node_merges`, `nodes`,
//! `atoms` — migration `0083_consumer_rls`) runs inside a
//! [`crate::tenancy::scope::begin_account_scoped`] transaction, through the
//! Store's `_scoped` twins, dispatched by the file-local `begin_scoped_tx!` /
//! `scoped_store!` / `commit_*_tx` trio PR B.2 established. With no pool
//! (`tenant.pool == None`) opening the transaction fails and the handler
//! returns a 500.
//!
//! **Which account, and why:**
//!
//! * The four GRAPH-ANCHORED handlers (`list_meetings`, `meeting_summary`,
//!   `generate_meeting_summary`, `create_speaker_relabel`) scope to the
//!   OCEAN'S OWNING account, resolved through `Store::graph_account_id` —
//!   migration 0084's audited `resolve_graph_account_id` SECURITY DEFINER
//!   bootstrap (PR #284), which stays deliberately unscoped because it is how
//!   that account is discovered. It must be the ocean's owner, not
//!   `user.account_id`: the gate on these routes is graph REACHABILITY, which
//!   is multi-account by design (ADR-0007 Model B), so a legitimate caller may
//!   sit in a different account than the ocean's owner. Same call B.1 made in
//!   `routes::graphs::get_ocean_stats` and B.3b made in `routes::shares`.
//! * The ID-ANCHORED pair (`revert_correction`/`restore_correction`, keyed on
//!   a bare `correction_id`) now scopes to the CORRECTION'S OWNING account,
//!   resolved through `resolve_transcript_correction_account_id` (migration
//!   0105). It previously scoped to the caller's active account — "the only
//!   account knowable before the row is read" — and this doc recorded the
//!   resulting gap rather than silently accepting it: a correction in a
//!   reachable-but-other-account ocean failed closed onto the handler's 404
//!   instead of being revertible, while the graph-anchored summary route listed
//!   it. That bootstrap is the one this note asked for, and the gap is closed.
//!   Authorization is unchanged — `graph_reachable_by_user` still gates it.
//!
//! **`meeting_summary` uses TWO transactions, not one.** Its transcript read
//! is followed by `services::transcripts::resolve_raw_text`, which does an R2
//! blob fetch + AES decrypt, and only then by the corrections/merges/entities
//! reads. Holding one pooled connection across that network round trip is
//! exactly what PR #291's per-statement rationale rules out, so the transcript
//! read commits first and the four post-network reads share a second scoped
//! transaction (they are contiguous, with no I/O between them).
//!
//! **Formerly a known gap, now closed** (org-tenancy OPS-4 Store-surface
//! follow-up): `Store::relabel_summary_owner` (the best-effort summary
//! action-item owner propagation in `create_speaker_relabel`) now has a
//! `_scoped` twin, wired through its OWN fresh scoped transaction — not a
//! reuse of the relabel write's `scoped_tx` (already committed by then) —
//! since it is best-effort and independent of that write, matching this
//! module's general "no DB work across a network wait shares one
//! transaction" per-statement rule.
//!
//! **Declared behavior deltas** (error paths only — every success path stays
//! byte-identical, and `SET LOCAL` on today's BYPASSRLS owner connection is a
//! no-op, so pre-OPS-4 responses are unchanged):
//!
//! * The three graph-anchored handlers now resolve the owning account before
//!   doing any content work. A DB ERROR from that bootstrap lookup returns 500
//!   instead of the previous behavior — same call B.1 made in
//!   `routes::graphs::get_ocean_stats`. A `None` (no such graph) returns each
//!   handler's own existing not-found body; it is reachable only as a race
//!   with graph deletion, since reachability has already passed by then.
//! * `generate_meeting_summary` now resolves that account BEFORE the
//!   `reset_meeting_summary_pending` write rather than after it (the reset is
//!   the scoped statement and needs the scope first). One consequence is an
//!   improvement, recorded rather than assumed: the enqueued job's W.1
//!   `account_id` stamp can no longer be `None` on this path, because a
//!   handler that could not resolve the account no longer reaches the enqueue.
//!
//! The fire-and-forget `insert_activity_event` and `jobs::queue::enqueue` are
//! untouched: the former owns its own pool handle, spawn and scoping
//! (`routes::activity`, already converted on the B.1 lane), and `jobs` is
//! deliberately outside 0083's roster.

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Extension, Json,
};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use chrono::{DateTime, Utc};
use serde::Deserialize;
use serde_json::{json, Value};
use std::sync::Arc;
use uuid::Uuid;

use crate::routes::queries::collapse_snippet;
use crate::store::SummaryResetOutcome;
use crate::{
    middleware::auth::UserContext, tenancy::extractor::Tenant,
    tenancy::reachability::ReachabilityResolver, AppState,
};

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Max characters of a derived meeting `title` (cs-candidate-meeting-labels).
const MEETING_TITLE_CHARS: usize = 80;

/// Per-meeting summary-generation throttle: at most `SUMMARY_GEN_MAX_PER_WINDOW`
/// triggers per `SUMMARY_GEN_WINDOW_SECS`. Bounds LLM spend from repeated Retry /
/// opt-in clicks (each is a full map-reduce pass) without impeding a legitimate
/// retry-after-failure a few seconds later.
const SUMMARY_GEN_MAX_PER_WINDOW: u32 = 5;
const SUMMARY_GEN_WINDOW_SECS: u64 = 60;

// Content-plane store, resolved by the `Tenant` extractor (org-tenancy PR
// 1.6; was ambient `state.store`) — `meeting_transcripts`/
// `transcript_corrections`/`node_merges`/`graphs` are tenant-tree tables.
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

/// Resolve the OCEAN'S owning account (the scope for every graph-anchored
/// handler here — see the module doc's RLS section) via migration 0084's
/// `resolve_graph_account_id` SECURITY DEFINER bootstrap, or early-return.
///
/// `$missing` is the response for "no such graph": reachability has already
/// passed at every call site, so a miss here is a race with graph deletion and
/// must read exactly like the handler's existing not-found arm — never a new,
/// distinguishable status.
macro_rules! require_owner_account {
    ($store:expr, $graph_id:expr, $missing:expr) => {
        match $store.graph_account_id($graph_id).await {
            Ok(Some(a)) => a,
            Ok(None) => return $missing,
            Err(e) => {
                tracing::error!(error = %e, "meetings: graph account lookup failed");
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({"error": "internal error"})),
                );
            }
        }
    };
}

/// Open an account-scoped transaction (org-tenancy OPS-4 phase B.5), or
/// early-return a 500. Always `Some`: with no pool `begin_member_scoped`
/// errors and this returns the 500.
macro_rules! begin_scoped_tx {
    ($tenant:expr, $account_id:expr) => {
        {
            match $tenant.begin_member_scoped($account_id).await {
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
/// unscoped method when `$tx` is `None`. `begin_scoped_tx!` never yields `None`: with
/// no pool it returns a 500 instead.
macro_rules! scoped_store {
    ($store:expr, $tx:expr, $unscoped:ident / $scoped:ident ( $($arg:expr),* $(,)? )) => {
        match $tx.as_mut() {
            Some(tx) => $store.$scoped(tx, $($arg),*).await,
            None => $store.$unscoped($($arg),*).await,
        }
    };
}

/// Commit a WRITE handler's scoped transaction before responding — a commit
/// failure means nothing was persisted, so it must surface as a 500, never a
/// false success.
macro_rules! commit_write_tx {
    ($tx:expr) => {
        if let Some(tx) = $tx.take() {
            if let Err(e) = tx.commit().await {
                tracing::error!(error = %e, "scoped transaction commit failed");
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({"error": "internal error"})),
                );
            }
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

// ── GET /v1/meetings ───────────────────────────────────────────────────────────

#[derive(Deserialize)]
pub struct ListMeetingsQuery {
    pub graph_id: Uuid,
    pub limit: Option<i64>,
    pub cursor: Option<String>,
}

/// The graph is part of the cursor's scope, never its authorization. Every
/// request independently passes reachability before the cursor is inspected.
fn encode_meetings_cursor(graph_id: Uuid, created_at: DateTime<Utc>, source_id: &str) -> String {
    URL_SAFE_NO_PAD.encode(
        serde_json::to_vec(&(1_u8, graph_id, created_at, source_id))
            .expect("meeting cursor contains only serializable scalar values"),
    )
}

fn decode_meetings_cursor(cursor: &str, graph_id: Uuid) -> Option<(DateTime<Utc>, String)> {
    // Bound work on untrusted input before decoding. Source IDs are retained
    // verbatim, including punctuation and Unicode; they are not URL paths.
    if cursor.len() > 16_384 {
        return None;
    }
    let decoded = URL_SAFE_NO_PAD.decode(cursor).ok()?;
    let (version, cursor_graph, created_at, source_id): (u8, Uuid, DateTime<Utc>, String) =
        serde_json::from_slice(&decoded).ok()?;
    (version == 1 && cursor_graph == graph_id && !source_id.contains('\0'))
        .then_some((created_at, source_id))
}

pub async fn list_meetings(
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
    Query(q): Query<ListMeetingsQuery>,
) -> (StatusCode, Json<Value>) {
    let store = require_store!(tenant);
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
            );
        }
        Err(e) => {
            tracing::error!(error = %e, "list_meetings reachability error");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            );
        }
    }

    let cursor = match q.cursor.as_deref() {
        Some(value) => match decode_meetings_cursor(value, q.graph_id) {
            Some(cursor) => Some(cursor),
            None => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(json!({"error": "malformed cursor"})),
                );
            }
        },
        None => None,
    };
    let owner_account = require_owner_account!(
        store,
        q.graph_id,
        (
            StatusCode::NOT_FOUND,
            Json(json!({"error": "ocean not found or access denied"})),
        )
    );
    let mut scoped_tx = begin_scoped_tx!(tenant, owner_account);
    let Some(tx) = scoped_tx.as_mut() else {
        // Meeting transcripts need a scoped transaction.
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error": "service unavailable"})),
        );
    };
    let limit = q.limit.unwrap_or(20).clamp(1, 100);
    type MeetingRow = (
        String,
        Option<i64>,
        DateTime<Utc>,
        String,
        Option<DateTime<Utc>>,
        Option<Vec<String>>,
    );
    // The Store's MCP list uses OFFSET and substitutes "pending" for status.
    // This small metadata query retains the same graph/account/member scope,
    // but reads the actual status and uses a unique tie-breaker so inserts
    // ahead of a page do not duplicate or skip previously listed meetings.
    // Materialize the bounded page BEFORE metadata enrichment. The two atom
    // branches use equality or text_pattern_ops range bounds (migration 0074),
    // avoiding a whole-ocean scan or a dynamic LIKE prefix. A numeric chunk
    // suffix is required: "meeting-a-other" is not evidence for "meeting-a".
    // No string from a source ID is interpreted as a pattern.
    // The reader's team-seat history floor (#1545) admits each listed
    // meeting before the page is cut, so a floored member's pages stay full.
    let rows: Vec<MeetingRow> = match sqlx::query_as(&format!(
        "WITH page AS MATERIALIZED ( \
             SELECT source_id, duration_ms, created_at, summary_status \
             FROM meeting_transcripts t \
             WHERE graph_id = $1 \
               AND ($3::timestamptz IS NULL OR (created_at, source_id) < ($3, $4::text)) \
               AND {floor} \
             ORDER BY created_at DESC, source_id DESC LIMIT $2 \
         ) \
         SELECT p.source_id, p.duration_ms, p.created_at, p.summary_status, \
                evidence.occurred_at, evidence.participants \
         FROM page p \
         LEFT JOIN LATERAL ( \
             SELECT MIN(a.timestamp) AS occurred_at, \
                    ARRAY_AGG(DISTINCT participant ORDER BY participant) \
                        FILTER (WHERE participant IS NOT NULL) AS participants \
             FROM ( \
                 SELECT timestamp, participants FROM atoms \
                 WHERE graph_id = $1 AND source_id = p.source_id \
                 UNION ALL \
                 SELECT timestamp, participants FROM atoms \
                 WHERE graph_id = $1 \
                   AND source_id ~>=~ (p.source_id || ':chunk:') \
                   AND source_id ~<~ (p.source_id || ':chunk;') \
                   AND substring(source_id FROM char_length(p.source_id) + 8) ~ '^[0-9]+$' \
             ) a \
             LEFT JOIN LATERAL UNNEST(a.participants) participant ON true \
         ) evidence ON true \
         ORDER BY p.created_at DESC, p.source_id DESC",
        floor = crate::history_floor::admits_transcript("t")
    ))
    .bind(q.graph_id)
    .bind(limit + 1)
    .bind(cursor.as_ref().map(|(created_at, _)| *created_at))
    .bind(cursor.as_ref().map(|(_, source_id)| source_id.as_str()))
    .fetch_all(&mut **tx)
    .await
    {
        Ok(rows) => rows,
        Err(e) => {
            tracing::error!(error = %e, "list_meetings load error");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            );
        }
    };
    close_read_tx!(scoped_tx);
    let has_more = rows.len() > limit as usize;
    let next_cursor = has_more.then(|| {
        let (source_id, _, created_at, _, _, _) = &rows[limit as usize - 1];
        encode_meetings_cursor(q.graph_id, *created_at, source_id)
    });
    let items: Vec<Value> = rows
        .into_iter()
        .take(limit as usize)
        .map(
            |(source_id, duration_ms, created_at, summary_status, occurred_at, participants)| {
                json!({
                    "source_id": source_id,
                    "duration_ms": duration_ms,
                    "created_at": created_at,
                    "summary_status": summary_status,
                    // Meeting title is derived from the original transcript by
                    // the detail route; a chunk headline is not a meeting title.
                    "title": null,
                    "occurred_at": occurred_at,
                    "participants": participants,
                })
            },
        )
        .collect();
    (
        StatusCode::OK,
        Json(json!({"items": items, "next_cursor": next_cursor, "has_more": has_more})),
    )
}

// ── GET /v1/meetings/summary ───────────────────────────────────────────────────

#[derive(Deserialize)]
pub struct SummaryQuery {
    pub graph_id: Uuid,
    pub source_id: String,
}

pub async fn meeting_summary(
    State(_state): State<Arc<AppState>>,
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
    Query(q): Query<SummaryQuery>,
) -> (StatusCode, Json<Value>) {
    let store = require_store!(tenant);

    // IDOR: the ocean must be reachable by the caller (ADR-0007 D9), through
    // the tenancy-aware resolver (org-tenancy PR 1.6).
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
                Json(json!({"error": "meeting not found or access denied"})),
            )
        }
        Err(e) => {
            tracing::error!(error = %e, "meeting_summary reachability error");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            );
        }
    }

    // RLS scope: the OCEAN'S owning account (module doc). Reachability above
    // may have admitted a reader from another account, so the caller's own
    // account would be the wrong scope.
    let owner_account = require_owner_account!(
        store,
        q.graph_id,
        (
            StatusCode::NOT_FOUND,
            Json(json!({"error": "meeting not found or access denied"})),
        )
    );

    let mut tx = begin_scoped_tx!(tenant, owner_account).expect("cloud scoped transaction");
    // The reader's team-seat history floor (#1545): a meeting before the
    // floor is refused exactly like a source whose access was withdrawn.
    let floor = crate::history_floor::admits_transcript("t");
    let mapping: Result<Option<(uuid::Uuid, uuid::Uuid, Option<chrono::DateTime<chrono::Utc>>)>, sqlx::Error> = sqlx::query_as(&format!(
        "SELECT s.source_item_id,v.version_id,v.occurred_at FROM meeting_transcripts t JOIN source_items s ON s.source_item_id=t.source_item_id AND s.graph_id=t.graph_id JOIN source_versions v ON v.version_id=t.current_version_id AND v.source_item_id=s.source_item_id AND v.graph_id=s.graph_id WHERE t.graph_id=$1 AND t.source_id=$2 AND s.access_state='permitted' AND {floor}",
    )).bind(q.graph_id).bind(&q.source_id).fetch_optional(&mut *tx).await;
    let mapping = match mapping {
        Ok(row) => row,
        Err(_) => return (StatusCode::SERVICE_UNAVAILABLE, Json(json!({"error":"service unavailable"}))),
    };
    if mapping.is_none() {
        let denied: bool = match sqlx::query_scalar(&format!("SELECT EXISTS(SELECT 1 FROM meeting_transcripts t LEFT JOIN source_items s ON s.source_item_id=t.source_item_id AND s.graph_id=t.graph_id WHERE t.graph_id=$1 AND t.source_id=$2 AND (s.access_state<>'permitted' OR NOT {floor}))"))
            .bind(q.graph_id).bind(&q.source_id).fetch_one(&mut *tx).await {
                Ok(v) => v, Err(_) => return (StatusCode::SERVICE_UNAVAILABLE, Json(json!({"error":"service unavailable"}))),
            };
        if denied { return (StatusCode::NOT_FOUND, Json(json!({"error":"meeting not found or access denied"}))); }
    }
    if tx.commit().await.is_err() { return (StatusCode::SERVICE_UNAVAILABLE, Json(json!({"error":"service unavailable"}))); }
    let source_items: Vec<Value> = mapping.as_ref().map(|(item,version,_)| json!({"graph_id":q.graph_id,"source_item_id":item,"source_version_id":version})).into_iter().collect();
    // Stored projections/corrections/summaries have no complete dependency
    // receipt. Exact retained evidence remains available through P4.
    return (StatusCode::OK, Json(json!({
        "source_id":q.source_id,"title":null,"occurred_at":mapping.and_then(|(_,_,at)|at),
        "raw_text":null,"corrected_text":null,"corrections":[],"merges":[],"new_entities":[],
        "summary_status":"unavailable","summary":null,"summary_model":null,"summary_error":null,"segments":null,
        "source_items":source_items,"projection_status":"unverified_lineage","evidence_status":if source_items.is_empty(){"unknown"}else{"retained"},
    })));
}

// ── POST /v1/meetings/summary/generate ─────────────────────────────────────────

#[derive(Deserialize)]
pub struct GenerateSummaryBody {
    pub graph_id: Uuid,
    pub source_id: String,
    /// When `true`, opt THIS meeting into cloud synthesis from the `blocked`
    /// state (the per-meeting consent the UI collects). Defaults to `false`: a
    /// plain Retry.
    #[serde(default)]
    pub allow_cloud: bool,
}

/// (Re)generate a meeting's summary: resets the row to `pending` and enqueues a
/// `GenerateMeetingSummary` job. Serves three client actions — first-time
/// generation for a pre-feature meeting, Retry from `failed`, and the opt-in
/// from `blocked` (`allow_cloud: true`, the user's explicit per-meeting
/// consent to cloud synthesis). IDOR-guarded by graph reachability, exactly like
/// `meeting_summary`.
pub async fn generate_meeting_summary(
    State(state): State<Arc<AppState>>,
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
    Json(body): Json<GenerateSummaryBody>,
) -> (StatusCode, Json<Value>) {
    let store = require_store!(tenant);
    // `jobs` is a tenant-tree table (org-tenancy PR 1.6): the enqueue below
    // must land on the same plane the transcript row lives on, so the worker
    // tenant loop (PR 2.1) picks it up from the right database.
    let Some(pool) = tenant.pool.as_ref() else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error": "service unavailable"})),
        );
    };

    // IDOR: the ocean must be reachable by the caller.
    match ReachabilityResolver::graph_reachable_by_user(
        tenant.decision.as_ref(),
        store.as_ref(),
        body.graph_id,
        user.user_id,
    )
    .await
    {
        Ok(true) => {}
        Ok(false) => {
            return (
                StatusCode::NOT_FOUND,
                Json(json!({"error": "meeting not found or access denied"})),
            )
        }
        Err(e) => {
            tracing::error!(error = %e, "generate_meeting_summary reachability error");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            );
        }
    }

    // Throttle per meeting: a single generate kicks off a full map-reduce LLM
    // pass (Haiku × N chunks + Sonnet reduce). Cap repeat triggers for the same
    // meeting so an impatient/scripted client can't cheaply multiply LLM spend.
    // Keyed on the meeting identity (reachability already checked above).
    let rl_key = format!("summary:gen:{}:{}", body.graph_id, body.source_id);
    if !state
        .rate_limiter
        .check_and_increment_key(&rl_key, SUMMARY_GEN_MAX_PER_WINDOW, SUMMARY_GEN_WINDOW_SECS)
        .await
    {
        return (
            StatusCode::TOO_MANY_REQUESTS,
            Json(
                json!({"error": "too many summary requests for this meeting; please wait a moment"}),
            ),
        );
    }

    // Owning account, used for BOTH the job's `account_id` stamp (org-tenancy
    // W.1) and this request's RLS scope (org-tenancy OPS-4 phase B.5): this
    // handler has no account in scope of its own (reachability is user-, not
    // account-keyed), so resolve it from the graph — `Store::graph_account_id`
    // is backed by the 0084 SECURITY DEFINER bootstrap resolver and stays
    // deliberately unscoped.
    //
    // Resolved BEFORE the reset (it used to sit after it) because the reset is
    // now the scoped write and needs the account first. On a miss — a race
    // with graph deletion, after reachability already passed — this returns the
    // handler's existing 404 rather than proceeding unscoped: pre-conversion,
    // the reset would have run and found no row, producing the SAME
    // `SummaryResetOutcome::NotFound` 404 body.
    let owner_account = require_owner_account!(
        store,
        body.graph_id,
        (
            StatusCode::NOT_FOUND,
            Json(json!({"error": "meeting not found"})),
        )
    );
    let job_account_id = Some(owner_account);

    // Atomically reset the row to `pending` — but only if a job isn't already
    // running. A concurrent Retry while one is running is rejected (409) rather
    // than enqueuing a second job that races the first's `summary` write.
    let mut scoped_tx = begin_scoped_tx!(tenant, owner_account);
    match scoped_store!(
        store,
        scoped_tx,
        reset_meeting_summary_pending
            / reset_meeting_summary_pending_scoped(body.graph_id, &body.source_id)
    ) {
        Ok(SummaryResetOutcome::Reset) => {}
        Ok(SummaryResetOutcome::AlreadyRunning) => {
            return (
                StatusCode::CONFLICT,
                Json(json!({"error": "a summary is already being generated for this meeting"})),
            )
        }
        Ok(SummaryResetOutcome::NotFound) => {
            return (
                StatusCode::NOT_FOUND,
                Json(json!({"error": "meeting not found"})),
            )
        }
        Err(e) => {
            tracing::error!(error = %e, "generate_meeting_summary reset error");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            );
        }
    }
    // Commit the reset BEFORE enqueuing: a worker must never be able to claim
    // a job whose `pending` row isn't visible yet (same ordering PR B.1
    // established for `routes::ingest`'s summary-job enqueue).
    commit_write_tx!(scoped_tx);

    // Enqueue the (re)generation job. `fail()` clears the dedup key when a prior
    // job left 'pending', so a Retry after a failure always queues fresh.
    let key = format!("summary:{}:{}", body.graph_id, body.source_id);
    if let Err(e) = crate::jobs::queue::enqueue(
        pool,
        &crate::jobs::JobKind::GenerateMeetingSummary {
            graph_id: body.graph_id,
            source_id: body.source_id.clone(),
            allow_cloud_override: body.allow_cloud,
        },
        job_account_id,
        Some(&key),
        0,
    )
    .await
    {
        tracing::error!(error = %e, "generate_meeting_summary enqueue error");
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": "could not enqueue summary generation"})),
        );
    }

    (
        StatusCode::OK,
        Json(json!({ "ok": true, "summary_status": "pending" })),
    )
}

/// Best-effort human title for a meeting, derived from the first non-empty
/// line of its verbatim raw transcript text. The Teams backfill path prefixes
/// the calendar event's subject as line one (`"{subject}\n\n{transcript}"`);
/// live-capture prefixes a short label. Falls back to `None` when there's no
/// raw text or the first line is blank — the caller (and frontend) treat that
/// as "no title known" rather than guessing.
///
/// `pub(crate)` (not just used by `meeting_summary` above): `routes::ingest`
/// reuses this exact heuristic to derive the `title` clause of the
/// contextual chunk-header (cs-phase0-chunk-headers) from the same
/// `raw_content` an ingest request may carry, so the two "what's this
/// meeting called" derivations never drift apart.
pub(crate) fn meeting_title_from_raw(raw: &str) -> Option<String> {
    let line = raw.lines().map(str::trim).find(|l| !l.is_empty())?;
    let title = collapse_snippet(line, MEETING_TITLE_CHARS);
    if title.is_empty() {
        None
    } else {
        Some(title)
    }
}

// Raw-text resolution (inline / R2+DEK) has been promoted to
// `services::transcripts::resolve_raw_text` (docs/plans/remote-mcp-v1.md
// §3.3 item 4) so the MCP `get_transcript` tool shares the exact same
// blob/DEK path as this route, rather than duplicating it. See the call site
// above.

// ── POST /v1/transcript-corrections/{id}/{revert|restore} ──────────────────────

pub async fn revert_correction(
    tenant: Tenant,
    user: Extension<UserContext>,
    correction_id: Path<Uuid>,
) -> (StatusCode, Json<Value>) {
    set_correction_status(tenant, user, correction_id, "reverted").await
}

pub async fn restore_correction(
    tenant: Tenant,
    user: Extension<UserContext>,
    correction_id: Path<Uuid>,
) -> (StatusCode, Json<Value>) {
    set_correction_status(tenant, user, correction_id, "applied").await
}

// ── Correction propagation (contract §8.4 / C15) ───────────────────────────────

/// Durably enqueue a `PropagateCorrections` job (`workers::
/// propagate_corrections` runs it) — fired after a relabel, a revert, and a
/// restore all write (§5 of the design this closes: "safe to fire on every
/// correction write, on revert, and on restore").
///
/// **Enqueued in the SAME transaction as the correction write** (CodeRabbit
/// round 2, finding 1): an earlier revision of this used a bare
/// `tokio::spawn` after commit, matching `routes::google_push`'s spawn
/// precedent — but that precedent doesn't transfer. Pub/Sub REDELIVERS a
/// failed Gmail push, so a lost spawned task there just gets re-triggered by
/// the next delivery; a correction has no such redelivery, so a lost spawn
/// (process restart, panic, OOM-kill between commit and the task actually
/// running) would leave the correction durably `applied` while its atoms are
/// NEVER rewritten — permanently, since nothing else re-triggers propagation
/// for that source. `jobs::queue::enqueue_tx`, on the SAME `tx` the caller is
/// about to commit, makes "the correction is durable" and "the propagation
/// work is durably queued" one atomic fact — the same discipline every other
/// content-mutation-adjacent job in this tree already has.
///
/// Idempotency-keyed on `(graph_id, source_id)` so a burst of correction
/// events on the same meeting coalesces into one pending job rather than
/// piling up redundant ones — `propagate()` always reads the FRESH current
/// `applied` set when it actually runs, so one job converges to the same
/// final state N would have.
///
/// Never reached without a pool (`tx` is never opened there — the
/// `scoped_store!` dispatch's unscoped branch never reaches this).
async fn enqueue_propagate_corrections_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    account_id: Uuid,
    graph_id: Uuid,
    source_id: &str,
) {
    let idempotency_key = format!("propagate_corrections:{graph_id}:{source_id}");
    if let Err(e) = crate::jobs::queue::enqueue_tx(
        tx,
        &crate::jobs::JobKind::PropagateCorrections {
            graph_id,
            source_id: source_id.to_string(),
        },
        Some(account_id),
        Some(&idempotency_key),
        0,
    )
    .await
    {
        tracing::warn!(
            error = %e,
            "corrections propagate: failed to enqueue PropagateCorrections job (non-fatal — a \
             later correction event on the same source will retry the enqueue)"
        );
    }
}

// ── POST /v1/transcript-corrections (create a user speaker-relabel) ────────────

#[derive(Deserialize)]
pub struct CreateRelabelBody {
    pub graph_id: Uuid,
    pub source_id: String,
    /// The current speaker label being renamed (e.g. "Speaker 2" / "Others").
    pub surface_form: String,
    /// The new name to show for that speaker.
    pub canonical: String,
    /// `null` → all-segments scope (rename every occurrence of the label);
    /// `n` (1-based) → this-segment scope (rename only the n-th occurrence).
    #[serde(default)]
    pub occurrence: Option<i32>,
}

/// `POST /v1/transcript-corrections` — record a user speaker-relabel as a
/// first-class transcript correction (cs-uxp3c-speaker-relabel). The relabel
/// overlays `corrected_text` and reverts/restores through the SAME routes as an
/// extraction name-correction. IDOR-guarded by graph reachability, mirroring the
/// revert/restore handlers. Never logs the label text (PII).
pub async fn create_speaker_relabel(
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
    Json(body): Json<CreateRelabelBody>,
) -> (StatusCode, Json<Value>) {
    let store = require_store!(tenant);

    let surface = body.surface_form.trim();
    let canonical = body.canonical.trim();
    if surface.is_empty() || canonical.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "surface_form and canonical are required"})),
        );
    }
    // `occurrence`, when present, is a 1-based ordinal; reject <1 rather than
    // silently widening the scope to "all".
    let occurrence = match body.occurrence {
        None => None,
        Some(n) if n >= 1 => Some(n),
        Some(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": "occurrence must be a positive (1-based) ordinal"})),
            )
        }
    };

    // IDOR: the ocean must be reachable by the caller.
    match ReachabilityResolver::graph_reachable_by_user(
        tenant.decision.as_ref(),
        store.as_ref(),
        body.graph_id,
        user.user_id,
    )
    .await
    {
        Ok(true) => {}
        Ok(false) => {
            return (
                StatusCode::NOT_FOUND,
                Json(json!({"error": "meeting not found or access denied"})),
            )
        }
        Err(e) => {
            tracing::error!(error = %e, "create_speaker_relabel reachability error");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            );
        }
    }

    // RLS scope: the OCEAN'S owning account (module doc).
    let owner_account = require_owner_account!(
        store,
        body.graph_id,
        (
            StatusCode::NOT_FOUND,
            Json(json!({"error": "meeting not found or access denied"})),
        )
    );

    let mut scoped_tx = begin_scoped_tx!(tenant, owner_account);
    let correction = match scoped_store!(
        store,
        scoped_tx,
        insert_speaker_relabel
            / insert_speaker_relabel_scoped(
                body.graph_id,
                &body.source_id,
                surface,
                canonical,
                occurrence,
            )
    ) {
        Ok(c) => c,
        Err(e) => {
            tracing::error!(error = %e, "create_speaker_relabel insert error");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            );
        }
    };
    // Contract §8.4 / C15: enqueue the propagation job in the SAME
    // transaction as the correction row above, BEFORE it commits — see
    // `enqueue_propagate_corrections_tx`'s doc comment for why this must be
    // atomic with the correction write rather than a best-effort step after
    // it.
    if let Some(tx) = scoped_tx.as_mut() {
        enqueue_propagate_corrections_tx(tx, owner_account, body.graph_id, &body.source_id).await;
    }
    commit_write_tx!(scoped_tx);

    // Post-#208: an all-segments relabel also refreshes any summary action-item
    // owners that matched the old label. Skipped for this-segment scope (a single
    // turn shouldn't rewrite whole-meeting ownership). Best-effort + no-op before
    // #208 lands (feature-detected in the store).
    //
    // `relabel_summary_owner` now has a `_scoped` twin (org-tenancy OPS-4
    // Store-surface follow-up). Deliberately its OWN fresh scoped
    // transaction (not a reuse of `scoped_tx`, already committed above) —
    // matches the module doc's "runs on its own connection" note: this
    // propagation is best-effort and independent of the relabel write that
    // already succeeded, so it opens, runs, and commits/logs entirely on its
    // own, never failing the request either way.
    //
    // Deliberately NOT `begin_scoped_tx!` here: that macro early-returns a
    // 500 on a failed `begin_account_scoped`, which is correct for a WRITE
    // handler's primary transaction but wrong for this one — the correction
    // above already committed (L771) by the time this runs, so a fatal
    // response here would 500 a request whose real effect already succeeded,
    // inviting a client retry that creates a DUPLICATE correction (flagged in
    // review). A begin failure just means the best-effort propagation is
    // skipped this time, same as any other failure on this path.
    if occurrence.is_none() {
        let mut propagate_tx: Option<sqlx::Transaction<'static, sqlx::Postgres>> = None;
        match tenant.begin_member_scoped(owner_account).await {
            Ok(tx) => propagate_tx = Some(tx),
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    "relabel: summary owner propagation begin_account_scoped failed \
                     (non-fatal, correction already committed) — skipping propagation"
                );
            }
        }

        // `tenant.pool.is_none()` cannot hold here: with no pool the
        // handler's `begin_scoped_tx!` above already returned a 500.
        if tenant.pool.is_none() || propagate_tx.is_some() {
            let result = scoped_store!(
                store,
                propagate_tx,
                relabel_summary_owner
                    / relabel_summary_owner_scoped(
                        body.graph_id,
                        &body.source_id,
                        surface,
                        canonical
                    )
            );
            if let Some(tx) = propagate_tx.take() {
                if let Err(e) = tx.commit().await {
                    tracing::warn!(error = %e, "relabel: summary owner propagation commit failed (non-fatal)");
                }
            }
            match result {
                Ok(n) if n > 0 => {
                    tracing::info!(
                        rows = n,
                        "relabel: propagated to summary action-item owners"
                    )
                }
                Ok(_) => {}
                Err(e) => tracing::warn!(
                    error = %e,
                    "relabel: summary owner propagation failed (non-fatal)"
                ),
            }
        }
    }

    // Contract §8.4 / C15 propagation was already durably enqueued above, in
    // the SAME transaction as the correction insert (unlike the
    // `relabel_summary_owner` propagation above, which is all-segments only
    // — the enqueued job runs for BOTH scopes: an occurrence-scoped
    // (this-segment) relabel still rewrites the atom whose stored content
    // actually contains that turn, since `rewrite_atom_content`'s occurrence
    // counting is per-atom — see `services::corrections`' module doc).

    // 201 Created: this POST creates a new transcript_correction resource.
    (
        StatusCode::CREATED,
        Json(json!({
            "correction_id": correction.correction_id,
            "surface_form":  correction.surface_form,
            "canonical":     correction.canonical,
            "node_id":       correction.node_id,
            "confidence":    correction.confidence,
            "status":        correction.status,
            "kind":          correction.kind,
            "occurrence":    correction.occurrence,
        })),
    )
}

/// The account that OWNS this transcript correction, via migration 0105's
/// SECURITY DEFINER bootstrap — `None` with no pool or
/// when the correction does not exist. Bare account id: correction → graph →
/// account, no content.
async fn correction_owner_account(tenant: &Tenant, correction_id: Uuid) -> Option<Uuid> {
    let pool = tenant.pool.as_ref()?;
    sqlx::query_scalar::<_, Option<Uuid>>("SELECT resolve_transcript_correction_account_id($1)")
        .bind(correction_id)
        .fetch_optional(pool)
        .await
        .ok()
        .flatten()
        .flatten()
}

async fn set_correction_status(
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
    Path(correction_id): Path<Uuid>,
    status: &str,
) -> (StatusCode, Json<Value>) {
    let store = require_store!(tenant);

    // RLS scope for this ID-ANCHORED handler: the correction's OWNING account,
    // via migration 0105's `resolve_transcript_correction_account_id` — the
    // bootstrap this module's doc predicted and recorded as missing.
    //
    // It used to scope to the caller's ACTIVE account, "the only account knowable
    // before the row is read", which made a correction in a reachable-but-
    // other-account ocean fail closed onto this handler's 404 — listed by the
    // graph-anchored summary route, un-revertible here.
    //
    // Authorization is UNCHANGED and does not come from the scope: the
    // `graph_reachable_by_user` check below still gates every correction.
    // Widening RLS visibility only lets that check run. An unresolvable id falls
    // back to the caller's account, preserving the existing non-leaking 404.
    //
    // ONE transaction spans the lookup and the update: they are adjacent, with
    // only the (non-RLS, SECURITY DEFINER) reachability check between them.
    let scope_account = correction_owner_account(&tenant, correction_id)
        .await
        .unwrap_or(user.account_id);
    let mut scoped_tx = begin_scoped_tx!(tenant, scope_account);

    // IDOR: resolve the correction's ocean and require it is reachable. A missing
    // record reads as "not found" (don't leak existence).
    let graph_id = match scoped_store!(
        store,
        scoped_tx,
        transcript_correction_graph_id / transcript_correction_graph_id_scoped(correction_id)
    ) {
        Ok(Some(g)) => g,
        Ok(None) => {
            return (
                StatusCode::NOT_FOUND,
                Json(json!({"error": "correction not found or access denied"})),
            )
        }
        Err(e) => {
            tracing::error!(error = %e, "set_correction_status lookup error");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            );
        }
    };
    match ReachabilityResolver::graph_reachable_by_user(
        tenant.decision.as_ref(),
        store.as_ref(),
        graph_id,
        user.user_id,
    )
    .await
    {
        Ok(true) => {}
        Ok(false) => {
            return (
                StatusCode::NOT_FOUND,
                Json(json!({"error": "correction not found or access denied"})),
            )
        }
        Err(e) => {
            tracing::error!(error = %e, "set_correction_status reachability error");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            );
        }
    }

    // Contract §8.4 / C15: capture this correction's `source_id` now, on the
    // SAME scoped transaction the `graph_id` lookup above already opened —
    // `enqueue_propagate_corrections_tx` needs it, in the SAME transaction,
    // before the status update below commits.
    //
    // A query error here is LOGGED, not swallowed (CodeRabbit round 1,
    // finding 5): `unwrap_or(None)` used to discard it silently, which is
    // doubly wrong — it hides the actual failure AND, since a real (rather
    // than "no row") sqlx error on a Postgres transaction leaves that
    // transaction ABORTED, the very next statement (the status UPDATE
    // below) would then fail too, but with Postgres's generic "current
    // transaction is aborted" message rather than this one's real root
    // cause. Bailing out here with the logged error is both more honest and
    // avoids ever reaching that confusing downstream failure.
    let source_id: Option<String> = match scoped_tx.as_mut() {
        Some(tx) => {
            match sqlx::query_scalar(
                "SELECT source_id FROM transcript_corrections WHERE correction_id = $1",
            )
            .bind(correction_id)
            .fetch_optional(&mut **tx)
            .await
            {
                Ok(id) => id,
                Err(e) => {
                    tracing::error!(error = %e, "set_correction_status: source_id lookup failed");
                    return (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(json!({"error": "internal error"})),
                    );
                }
            }
        }
        None => None,
    };

    match scoped_store!(
        store,
        scoped_tx,
        set_transcript_correction_status
            / set_transcript_correction_status_scoped(correction_id, status)
    ) {
        Ok(()) => {
            // Contract §8.4 / C15, CodeRabbit round 2 finding 1: enqueue the
            // propagation job in the SAME transaction as the status update,
            // BEFORE it commits (`enqueue_propagate_corrections_tx`'s doc
            // comment explains why this must be atomic with the write).
            //
            // The account used (round 1 finding 1) is `graph_id`'s REAL
            // owning account — freshly re-derived here via the same
            // unscoped bootstrap `require_owner_account!` uses
            // (`store.graph_account_id`), NOT `scope_account`.
            // `scope_account` can legitimately be `user.account_id` (the
            // CALLER's own active account, a best-effort fallback for when
            // the correction's owning account could not be resolved — see
            // the comment on its definition above), which is the WRONG
            // account whenever the correction belongs to a different one
            // than the caller's active session. By this point `graph_id`
            // has already passed BOTH RLS-scoped resolution and the
            // explicit `graph_reachable_by_user` check above, so
            // re-deriving its owner fresh here is authoritative regardless
            // of which path `scope_account` took — matching exactly what
            // `create_speaker_relabel`'s `owner_account` already does.
            if let Some(source_id) = &source_id {
                match store.graph_account_id(graph_id).await {
                    Ok(Some(propagate_account)) => {
                        if let Some(tx) = scoped_tx.as_mut() {
                            enqueue_propagate_corrections_tx(
                                tx,
                                propagate_account,
                                graph_id,
                                source_id,
                            )
                            .await;
                        }
                    }
                    Ok(None) => {
                        // Race with graph deletion between the reachability
                        // check and here — nothing to propagate to.
                    }
                    Err(e) => tracing::warn!(
                        error = %e,
                        "set_correction_status: graph account lookup failed for propagation \
                         enqueue (non-fatal)"
                    ),
                }
            }
            commit_write_tx!(scoped_tx);
            (
                StatusCode::OK,
                Json(json!({"ok": true, "correction_id": correction_id, "status": status})),
            )
        }
        Err(e) => {
            tracing::error!(error = %e, "set_correction_status update error");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "update failed"})),
            )
        }
    }
}

// ── corrected_text overlay ─────────────────────────────────────────────────────

/// Apply name corrections to raw transcript text: for each `(surface_form,
/// canonical)` pair, replace WHOLE-WORD, CASE-INSENSITIVE occurrences of
/// `surface_form` with `canonical`. Replacements are applied sequentially in the
/// order given. The raw text is never mutated in storage — this produces a
/// display-only overlay string.
pub(crate) fn apply_corrections(raw: &str, corrections: &[(String, String)]) -> String {
    let mut text = raw.to_string();
    for (surface, canonical) in corrections {
        if surface.trim().is_empty() {
            continue;
        }
        text = replace_whole_word_ci(&text, surface, canonical);
    }
    text
}

/// Apply name corrections with an optional `occurrence` scope (0070): `None`
/// replaces every whole-word occurrence (all-segments relabel / extraction
/// correction), `Some(n)` replaces only the n-th (1-based) whole-word occurrence
/// (a this-segment speaker relabel). An out-of-range `n` (≤0, or larger than the
/// match count) simply leaves the text unchanged for that correction. Sequential
/// like `apply_corrections`, and likewise display-only.
pub(crate) fn apply_scoped_corrections(
    raw: &str,
    corrections: &[(String, String, Option<i32>)],
) -> String {
    let mut text = raw.to_string();
    for (surface, canonical, occurrence) in corrections {
        if surface.trim().is_empty() {
            continue;
        }
        text = match occurrence {
            None => replace_whole_word_ci(&text, surface, canonical),
            Some(n) if *n >= 1 => replace_whole_word_ci_nth(&text, surface, canonical, *n as usize),
            // Non-positive ordinal: nothing to scope to, leave unchanged.
            Some(_) => text,
        };
    }
    text
}

/// Whole-word, case-insensitive replacement of ONLY the `n`-th (1-based)
/// occurrence of `needle` with `replacement` in `haystack`. Every other
/// occurrence — and every non-match — is preserved verbatim. Used for a
/// this-segment speaker relabel, which must not touch the same label elsewhere.
fn replace_whole_word_ci_nth(haystack: &str, needle: &str, replacement: &str, n: usize) -> String {
    let hay: Vec<char> = haystack.chars().collect();
    let need: Vec<char> = needle.chars().collect();
    if need.is_empty() || n == 0 {
        return haystack.to_string();
    }
    let mut out = String::with_capacity(haystack.len());
    let mut i = 0;
    let mut seen = 0usize;
    while i < hay.len() {
        let end = i + need.len();
        let matches = end <= hay.len()
            && chars_eq_ci(&hay[i..end], &need)
            && !is_word_char(if i == 0 { None } else { Some(hay[i - 1]) })
            && !is_word_char(hay.get(end).copied());
        if matches {
            seen += 1;
            if seen == n {
                out.push_str(replacement);
            } else {
                out.extend(&hay[i..end]);
            }
            i = end;
        } else {
            out.push(hay[i]);
            i += 1;
        }
    }
    out
}

/// Whole-word, case-insensitive replacement of `needle` with `replacement` in
/// `haystack`. "Whole word" = the match is not flanked by an alphanumeric or `_`
/// character on either side, so "Jordan" does not match inside "Jordana".
fn replace_whole_word_ci(haystack: &str, needle: &str, replacement: &str) -> String {
    let hay: Vec<char> = haystack.chars().collect();
    let need: Vec<char> = needle.chars().collect();
    if need.is_empty() {
        return haystack.to_string();
    }
    let mut out = String::with_capacity(haystack.len());
    let mut i = 0;
    while i < hay.len() {
        let end = i + need.len();
        let matches = end <= hay.len()
            && chars_eq_ci(&hay[i..end], &need)
            && !is_word_char(if i == 0 { None } else { Some(hay[i - 1]) })
            && !is_word_char(hay.get(end).copied());
        if matches {
            out.push_str(replacement);
            i = end;
        } else {
            out.push(hay[i]);
            i += 1;
        }
    }
    out
}

/// Case-insensitive equality of two char slices (ASCII fast path + Unicode
/// lowercase fallback).
fn chars_eq_ci(a: &[char], b: &[char]) -> bool {
    a.len() == b.len()
        && a.iter()
            .zip(b)
            .all(|(x, y)| x.eq_ignore_ascii_case(y) || x.to_lowercase().eq(y.to_lowercase()))
}

/// A "word" character for whole-word boundary checks. `None` (string edge) is not
/// a word char, so matches at the start/end of the text are allowed.
fn is_word_char(c: Option<char>) -> bool {
    c.is_some_and(|c| c.is_alphanumeric() || c == '_')
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn c(surface: &str, canonical: &str) -> (String, String) {
        (surface.to_string(), canonical.to_string())
    }

    #[test]
    fn applies_whole_word_replacement_everywhere() {
        let raw = "Jordan spoke. Then Jordan left.";
        let out = apply_corrections(raw, &[c("Jordan", "Morgan")]);
        assert_eq!(out, "Morgan spoke. Then Morgan left.");
    }

    #[test]
    fn replacement_is_case_insensitive() {
        let raw = "Jordan Jordan Jordan";
        let out = apply_corrections(raw, &[c("Jordan", "Morgan")]);
        assert_eq!(out, "Morgan Morgan Morgan");
    }

    #[test]
    fn does_not_replace_inside_other_words() {
        // "Jordan" must not match inside "Jordana" or "aJordan" (whole-word only).
        let raw = "Jordana aJordan Jordan";
        let out = apply_corrections(raw, &[c("Jordan", "Morgan")]);
        assert_eq!(out, "Jordana aJordan Morgan");
    }

    #[test]
    fn respects_punctuation_boundaries() {
        let raw = "Hi, Jordan! (Jordan).";
        let out = apply_corrections(raw, &[c("Jordan", "Morgan")]);
        assert_eq!(out, "Hi, Morgan! (Morgan).");
    }

    #[test]
    fn multiple_corrections_applied_in_order() {
        let raw = "Jordan met Saaad about Coral.";
        let out = apply_corrections(raw, &[c("Jordan", "Morgan"), c("Saaad", "Alex")]);
        assert_eq!(out, "Morgan met Alex about Coral.");
    }

    #[test]
    fn reverted_corrections_excluded_from_overlay() {
        // The route filters to status='applied' before calling apply_corrections;
        // a reverted correction is simply not in the list, so its surface form
        // stays verbatim in the corrected text.
        let raw = "Jordan and Saaad spoke.";
        // Only the "Jordan" correction is applied; "Saaad" was reverted (omitted).
        let applied = vec![c("Jordan", "Morgan")];
        let out = apply_corrections(raw, &applied);
        assert_eq!(out, "Morgan and Saaad spoke.");
    }

    #[test]
    fn no_corrections_leaves_text_unchanged() {
        let raw = "Nothing to fix here.";
        assert_eq!(apply_corrections(raw, &[]), raw);
    }

    #[test]
    fn blank_surface_form_is_skipped() {
        let raw = "Keep me intact.";
        let out = apply_corrections(raw, &[c("   ", "X")]);
        assert_eq!(out, raw);
    }

    #[test]
    fn multiword_surface_form_replaced() {
        let raw = "spoke with Bob Smith today";
        let out = apply_corrections(raw, &[c("Bob Smith", "Robert Smith")]);
        assert_eq!(out, "spoke with Robert Smith today");
    }

    // ── apply_scoped_corrections (0070 speaker-relabel scope) ─────────────────

    fn sc(surface: &str, canonical: &str, occ: Option<i32>) -> (String, String, Option<i32>) {
        (surface.to_string(), canonical.to_string(), occ)
    }

    #[test]
    fn scoped_none_relabels_every_occurrence() {
        // A "Speaker 2: … Speaker 2: …" transcript, all-segments scope.
        let raw = "Speaker 2: hi\nSpeaker 2: bye";
        let out = apply_scoped_corrections(raw, &[sc("Speaker 2", "Alice", None)]);
        assert_eq!(out, "Alice: hi\nAlice: bye");
    }

    #[test]
    fn scoped_nth_relabels_only_that_occurrence() {
        // this-segment scope: only the 2nd "Speaker 2" turn becomes "Alice".
        let raw = "Speaker 2: hi\nSpeaker 2: bye\nSpeaker 2: again";
        let out = apply_scoped_corrections(raw, &[sc("Speaker 2", "Alice", Some(2))]);
        assert_eq!(out, "Speaker 2: hi\nAlice: bye\nSpeaker 2: again");
    }

    #[test]
    fn scoped_nth_first_occurrence() {
        let raw = "Others: a\nOthers: b";
        let out = apply_scoped_corrections(raw, &[sc("Others", "Bob", Some(1))]);
        assert_eq!(out, "Bob: a\nOthers: b");
    }

    #[test]
    fn scoped_nth_out_of_range_is_noop() {
        let raw = "Others: a\nOthers: b";
        // Only two occurrences; asking for the 5th changes nothing.
        let out = apply_scoped_corrections(raw, &[sc("Others", "Bob", Some(5))]);
        assert_eq!(out, raw);
    }

    #[test]
    fn scoped_nth_is_case_insensitive_and_whole_word() {
        // "speaker 1" (lowercase) is the 1st occurrence; "Speaker 10" must NOT
        // match "Speaker 1" (whole-word boundary).
        let raw = "speaker 1: x\nSpeaker 10: y\nSpeaker 1: z";
        let out = apply_scoped_corrections(raw, &[sc("Speaker 1", "Kai", Some(2))]);
        assert_eq!(out, "speaker 1: x\nSpeaker 10: y\nKai: z");
    }

    #[test]
    fn scoped_mixed_all_and_single_apply_in_order() {
        let raw = "You: a\nOthers: b\nOthers: c";
        let out =
            apply_scoped_corrections(raw, &[sc("You", "Sam", None), sc("Others", "Lee", Some(1))]);
        assert_eq!(out, "Sam: a\nLee: b\nOthers: c");
    }

    #[test]
    fn scoped_non_positive_ordinal_is_noop() {
        let raw = "Others: a";
        let out = apply_scoped_corrections(raw, &[sc("Others", "Bob", Some(0))]);
        assert_eq!(out, raw);
    }

    // ── meeting_title_from_raw ────────────────────────────────────────────────

    #[test]
    fn title_from_raw_uses_first_line() {
        let raw = "Weekly Standup\n\nAlice: good morning everyone.";
        assert_eq!(
            meeting_title_from_raw(raw).as_deref(),
            Some("Weekly Standup")
        );
    }

    #[test]
    fn title_from_raw_skips_leading_blank_lines() {
        let raw = "\n\n  \nSprint Planning\n\ncontent here";
        assert_eq!(
            meeting_title_from_raw(raw).as_deref(),
            Some("Sprint Planning")
        );
    }

    #[test]
    fn title_from_raw_none_when_blank() {
        assert_eq!(meeting_title_from_raw(""), None);
        assert_eq!(meeting_title_from_raw("   \n  \n"), None);
    }

    #[test]
    fn title_from_raw_truncates_long_first_line() {
        let long_line = "x".repeat(MEETING_TITLE_CHARS + 50);
        let raw = format!("{long_line}\n\nbody");
        let title = meeting_title_from_raw(&raw).unwrap();
        assert!(title.ends_with('…'));
        assert!(title.chars().count() <= MEETING_TITLE_CHARS + 1);
    }
}

//! Scope selection (connectors redesign, layer B3): `PLAN.md` decision 5.
//!
//! One picker for every source, with two modes: "Only what I pick" (the
//! default, nothing selected) and "Everything I can see". This module holds what
//! the adapters share:
//!
//! * the request checks ([`validate`]: PickOnly sources have no Everything and
//!   EverythingOnly sources no pick, both a 422),
//! * paging and searching an item list a provider can only hand over whole
//!   ([`paginate`]),
//! * the mode a member's `connector_sources` row stores ([`write_mode`]) and the
//!   mode a connection from before the catalog reads ([`legacy_mode`]),
//! * reading a legacy handler's JSON answer ([`read_json`]),
//! * Slack's capture admission ([`Admission`], [`slack_channel_admitted`]): the
//!   pick-mode filter `slack_member_capture` applies at every Slack door.
//!
//! # Who keeps "everything"
//!
//! Member capture used to take every channel the member can read. A member with
//! a Slack connection from before the catalog has no `connector_sources` row, or
//! a row written by `connect::keep_legacy_connection_set_up` with no stored mode
//! and setup finished. Both read as "Everything I can see" ([`Admission`]), so
//! nothing changes for them until they choose. A connection made through the new
//! flow has rows with no mode and setup unfinished; that reads as "Only what I
//! pick" with nothing picked, which captures no channel until the member picks
//! some (decision 5).

use axum::body::to_bytes;
use axum::response::Response;
use sqlx::PgPool;
use uuid::Uuid;

use super::{
    pause, AdapterCx, AdapterError, ConnectorAdapter, Page, ScopeItem, ScopeItemsQuery, ScopeMode,
    ScopeSelection, ScopeShape,
};

/// The page size when the request names none, and the most it may name.
pub const DEFAULT_LIMIT: u32 = 50;
pub const MAX_LIMIT: u32 = 200;
/// The most items one `PUT .../scope` may pick.
pub const MAX_PICKED: usize = 1000;
/// The longest search text read.
const MAX_QUERY: usize = 100;

const MAX_BODY: usize = 4 * 1024 * 1024;

// ---------------------------------------------------------------------------
// Request checks
// ---------------------------------------------------------------------------

fn not_allowed(code: &'static str) -> AdapterError {
    AdapterError::Refused { status: 422, code }
}

/// Whether `sel` may be stored for a source of this shape. A 422 otherwise:
/// PickOnly rejects Everything, EverythingOnly rejects pick, and a pick may not
/// carry more items than [`MAX_PICKED`].
pub fn validate(shape: ScopeShape, sel: &ScopeSelection) -> Result<(), AdapterError> {
    match (shape, sel.mode) {
        (ScopeShape::PickOnly { .. }, ScopeMode::Everything) => {
            Err(not_allowed("everything_not_allowed"))
        }
        (ScopeShape::EverythingOnly, ScopeMode::Pick) => Err(not_allowed("pick_not_allowed")),
        (_, ScopeMode::Pick) if sel.item_ids.len() > MAX_PICKED => {
            Err(not_allowed("too_many_items"))
        }
        _ => Ok(()),
    }
}

/// The limit a request asks for, clamped to `1..=MAX_LIMIT`.
pub fn clamp_limit(raw: Option<u32>) -> u32 {
    raw.unwrap_or(DEFAULT_LIMIT).clamp(1, MAX_LIMIT)
}

/// Search text as it is matched: trimmed, lowercased, capped.
fn needle(q: &Option<String>) -> Option<String> {
    let q = q.as_deref()?.trim();
    (!q.is_empty()).then(|| q.chars().take(MAX_QUERY).collect::<String>().to_lowercase())
}

/// Search and page a list a provider can only return whole. The cursor is the
/// offset into the (filtered, label-ordered) list; `total` is the filtered
/// count. A cursor that is not ours starts from the top.
pub fn paginate(mut items: Vec<ScopeItem>, q: &ScopeItemsQuery) -> Page<ScopeItem> {
    if let Some(n) = needle(&q.q) {
        items.retain(|i| i.label.to_lowercase().contains(&n));
    }
    items.sort_by(|a, b| {
        a.label
            .to_lowercase()
            .cmp(&b.label.to_lowercase())
            .then_with(|| a.id.cmp(&b.id))
    });
    let total = items.len();
    let limit = q.limit.clamp(1, MAX_LIMIT) as usize;
    let start = q
        .cursor
        .as_deref()
        .and_then(|c| c.parse::<usize>().ok())
        .unwrap_or(0)
        .min(total);
    let end = (start + limit).min(total);
    let next_cursor = (end < total).then(|| end.to_string());
    Page {
        items: items.drain(start..end).collect(),
        next_cursor,
        total: Some(total as u32),
    }
}

// ---------------------------------------------------------------------------
// Stored mode
// ---------------------------------------------------------------------------

/// The mode a connection from before the catalog reads for a source: Slack took
/// every channel it could see; every other source with items already worked from
/// an explicit list (followed threads, watched chats, picked repos, spaces).
pub fn legacy_mode(provider: &str, shape: ScopeShape) -> ScopeMode {
    match shape {
        ScopeShape::EverythingOnly => ScopeMode::Everything,
        ScopeShape::PickOnly { .. } => ScopeMode::Pick,
        ScopeShape::Items { .. } if provider == "slack" => ScopeMode::Everything,
        ScopeShape::Items { .. } => ScopeMode::Pick,
    }
}

/// Store the mode for one source. Idempotent.
pub async fn write_mode(
    cx: &AdapterCx<'_>,
    adapter: &dyn ConnectorAdapter,
    source: &str,
    mode: ScopeMode,
) -> Result<(), AdapterError> {
    super::connect::keep_legacy_connection_set_up(cx, adapter).await?;
    let pool = cx
        .state
        .control_db()
        .ok_or_else(|| AdapterError::Database("control plane unavailable".into()))?;
    let completes_setup = adapter.sources().iter().any(|spec| {
        spec.key == source && matches!(spec.history, super::HistorySupport::Unsupported { .. })
    });
    let mut tx = crate::tenancy::scope::begin_account_scoped(pool, cx.account_id).await?;
    sqlx::query(
        "INSERT INTO connector_sources \
             (account_id, owner_user_id, provider, source, enabled, scope_mode, setup_completed_at) \
         VALUES ($1, $2, $3, $4, false, $5, CASE WHEN $6 THEN now() END) \
         ON CONFLICT (account_id, owner_user_id, provider, source) DO UPDATE \
             SET scope_mode = $5, setup_completed_at = CASE WHEN $6 THEN COALESCE(connector_sources.setup_completed_at, now()) ELSE connector_sources.setup_completed_at END, updated_at = now()",
    )
    .bind(cx.account_id)
    .bind(cx.user_id)
    .bind(adapter.key())
    .bind(source)
    .bind(match mode {
        ScopeMode::Pick => "pick",
        ScopeMode::Everything => "everything",
    })
    .bind(completes_setup)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    crate::read_models::dirty::connector_changed(cx.state, cx.tenant, cx.account_id, cx.user_id)
        .await
        .map_err(|e| AdapterError::Database(e.to_string()))?;
    Ok(())
}

// ---------------------------------------------------------------------------
// A legacy handler's JSON answer
// ---------------------------------------------------------------------------

/// A legacy handler's success body as JSON; any other status is the refusal the
/// route would have been, by status (`connect::refusal`).
pub async fn read_json(resp: Response) -> Result<serde_json::Value, AdapterError> {
    if !resp.status().is_success() {
        return Err(super::connect::refusal(resp).await);
    }
    let bytes = to_bytes(resp.into_body(), MAX_BODY)
        .await
        .map_err(|e| AdapterError::Database(format!("legacy answer unreadable: {e}")))?;
    if bytes.is_empty() {
        return Ok(serde_json::Value::Null);
    }
    serde_json::from_slice(&bytes)
        .map_err(|e| AdapterError::Database(format!("legacy answer not JSON: {e}")))
}

// ---------------------------------------------------------------------------
// Slack: what member capture admits
// ---------------------------------------------------------------------------

/// A member's `connector_sources` row for Slack channels, as the capture doors
/// read it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StoredSource {
    pub enabled: bool,
    pub paused: bool,
    pub mode: Option<ScopeMode>,
    pub setup_done: bool,
}

/// What member capture takes from this member's Slack index.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Admission {
    /// Off or paused: no channel.
    Blocked,
    /// Every channel the member can read.
    Everything,
    /// Only the channels they picked (`slack_member_channels.selected`).
    Picked,
}

impl Admission {
    /// The rule in the module docs. No row, or a row from before the catalog
    /// (no stored mode, setup finished), keeps "everything".
    pub fn of(row: Option<StoredSource>) -> Self {
        match row {
            None => Self::Everything,
            Some(r) if !r.enabled || r.paused => Self::Blocked,
            Some(r) => match r.mode {
                Some(ScopeMode::Everything) => Self::Everything,
                Some(ScopeMode::Pick) => Self::Picked,
                None if r.setup_done => Self::Everything,
                None => Self::Picked,
            },
        }
    }

    /// Whether a channel with this `selected` flag is captured.
    pub fn takes(self, selected: bool) -> bool {
        match self {
            Self::Blocked => false,
            Self::Everything => true,
            Self::Picked => selected,
        }
    }
}

/// Parse the columns of the `connector_sources` read below.
pub fn stored_from_columns(
    enabled: bool,
    paused: bool,
    mode: Option<&str>,
    setup_done: bool,
) -> StoredSource {
    StoredSource {
        enabled,
        paused,
        mode: mode.and_then(ScopeMode::parse),
        setup_done,
    }
}

/// Every Slack-channels row of one account, keyed by member, read inside the
/// caller's account-scoped transaction.
pub async fn slack_rows_in(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    account_id: Uuid,
) -> Result<std::collections::HashMap<Uuid, StoredSource>, sqlx::Error> {
    let rows: Vec<(Uuid, bool, bool, Option<String>, bool)> = sqlx::query_as(
        "SELECT owner_user_id, enabled, paused_at IS NOT NULL, scope_mode, \
                setup_completed_at IS NOT NULL \
         FROM connector_sources \
         WHERE account_id = $1 AND provider = $2 AND source = $3",
    )
    .bind(account_id)
    .bind(pause::SLACK_CHANNELS.provider)
    .bind(pause::SLACK_CHANNELS.source)
    .fetch_all(&mut **tx)
    .await?;
    Ok(rows
        .into_iter()
        .map(|(user, enabled, paused, mode, done)| {
            (
                user,
                stored_from_columns(enabled, paused, mode.as_deref(), done),
            )
        })
        .collect())
}

/// One member's admission, on the control pool. A lookup error blocks (the
/// pause gate's posture, `pause`).
pub async fn slack_admission(control: &PgPool, account_id: Uuid, user_id: Uuid) -> Admission {
    let read = async {
        let mut tx = crate::tenancy::scope::begin_account_scoped(control, account_id)
            .await
            .map_err(|e| sqlx::Error::Protocol(e.to_string()))?;
        let rows = slack_rows_in(&mut tx, account_id).await?;
        tx.rollback().await.ok();
        Ok::<_, sqlx::Error>(rows.get(&user_id).copied())
    };
    match read.await {
        Ok(row) => Admission::of(row),
        Err(e) => {
            tracing::warn!(error = %e, %account_id, %user_id, "slack scope: could not read connector_sources -- blocking capture");
            Admission::Blocked
        }
    }
}

/// Whether member capture takes one channel for one member right now. The one
/// check the backfill uses; the doorbell and the worker's due list apply the
/// same [`Admission`] in bulk.
pub async fn slack_channel_admitted(
    control: &PgPool,
    account_id: Uuid,
    user_id: Uuid,
    workspace_id: &str,
    channel_id: &str,
) -> bool {
    match slack_admission(control, account_id, user_id).await {
        Admission::Blocked => false,
        Admission::Everything => true,
        Admission::Picked => sqlx::query_scalar::<_, bool>(
            "SELECT selected FROM slack_member_channels \
             WHERE account_id = $1 AND user_id = $2 AND workspace_id = $3 AND channel_id = $4",
        )
        .bind(account_id)
        .bind(user_id)
        .bind(workspace_id)
        .bind(channel_id)
        .fetch_optional(control)
        .await
        .map(|s| s.unwrap_or(false))
        .unwrap_or(false),
    }
}

/// The install/fan-out writer must honor the destination owner's source too.
/// Resolve ownership on the content plane; membership and selections are on
/// control. An installer's pause must never suppress other members' copies or
/// an explicitly routed shared ocean. Author attribution is not ownership.
pub(crate) async fn slack_destination_admitted(
    control: &PgPool,
    content: &PgPool,
    account_id: Uuid,
    graph_id: Uuid,
    workspace_id: &str,
    channel_id: &str,
) -> Result<bool, AdapterError> {
    let mut tx = crate::tenancy::scope::begin_tenant_system_scoped(content, account_id).await?;
    let graph: Option<(String, Option<Uuid>)> =
        sqlx::query_as("SELECT kind, owner_user_id FROM graphs WHERE account_id=$1 AND id=$2")
            .bind(account_id)
            .bind(graph_id)
            .fetch_optional(&mut *tx)
            .await?;
    tx.commit().await?;
    let Some((kind, owner)) = graph else {
        return Ok(false);
    };
    let mut tx = crate::tenancy::scope::begin_account_scoped(control, account_id).await?;
    let owner = match owner {
        Some(owner) => Some(owner),
        None if kind == "personal" => {
            sqlx::query_scalar::<_, Option<Uuid>>("SELECT owner_user_id FROM accounts WHERE id=$1")
                .bind(account_id)
                .fetch_optional(&mut *tx)
                .await?
                .flatten()
        }
        None if matches!(kind.as_str(), "team" | "org") => return Ok(true),
        None => None,
    };
    let Some(owner) = owner else {
        return Ok(false);
    };
    let member: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM account_members WHERE account_id=$1 AND user_id=$2)",
    )
    .bind(account_id)
    .bind(owner)
    .fetch_one(&mut *tx)
    .await?;
    tx.commit().await?;
    if !member {
        return Ok(false);
    }
    Ok(slack_channel_admitted(control, account_id, owner, workspace_id, channel_id).await)
}

/// Replace the member's picked Slack channels. Channels not yet in the routing
/// index are added to it (the member picked them from the live list, so they can
/// read them), so a pick takes effect before the connect-time index refresh has
/// run. Returns how many are picked.
pub async fn slack_set_selected(
    control: &PgPool,
    account_id: Uuid,
    user_id: Uuid,
    workspace_id: &str,
    channel_ids: &[String],
) -> Result<u32, AdapterError> {
    let mut tx = control.begin().await?;
    sqlx::query(
        "UPDATE slack_member_channels SET selected = (channel_id = ANY($4)), updated_at = now() \
         WHERE account_id = $1 AND user_id = $2 AND workspace_id = $3",
    )
    .bind(account_id)
    .bind(user_id)
    .bind(workspace_id)
    .bind(channel_ids)
    .execute(&mut *tx)
    .await?;
    sqlx::query(
        "INSERT INTO slack_member_channels (account_id, user_id, workspace_id, channel_id, selected) \
         SELECT $1, $2, $3, c, true FROM unnest($4::text[]) AS c \
         ON CONFLICT (account_id, user_id, workspace_id, channel_id) \
         DO UPDATE SET selected = true, updated_at = now()",
    )
    .bind(account_id)
    .bind(user_id)
    .bind(workspace_id)
    .bind(channel_ids)
    .execute(&mut *tx)
    .await?;
    let picked: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM slack_member_channels \
         WHERE account_id = $1 AND user_id = $2 AND workspace_id = $3 AND selected",
    )
    .bind(account_id)
    .bind(user_id)
    .bind(workspace_id)
    .fetch_one(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(picked as u32)
}

// ---------------------------------------------------------------------------
// Followed threads (Gmail, Outlook)
// ---------------------------------------------------------------------------

/// The most pages of followed threads one listing reads.
const MAX_THREAD_PAGES: usize = 10;

/// The member's followed threads in Gmail (`"google"`) or Outlook
/// (`"microsoft"`), through the same list the add-ins use
/// (`routes::connectors::list_watched`). A thread the member stopped following
/// stays in that list for deletion propagation; it is not an item here. There
/// is no inbox listing, so pick mode offers followed threads only (decision 5).
pub async fn followed_threads(
    cx: &AdapterCx<'_>,
    provider: &str,
) -> Result<Vec<ScopeItem>, AdapterError> {
    if provider == "google" && cx.tenant.org_context().is_some() {
        // Legacy watched routes are intentionally personal-only/account-wide.
        // The catalog reads the caller's own tenant rows under member RLS.
        let mut tx = cx.tenant.begin_member_scoped(cx.account_id).await?;
        let rows: Vec<(String, chrono::DateTime<chrono::Utc>)> = sqlx::query_as(
            "SELECT thread_id,last_ingested_at FROM gmail_watched_threads WHERE account_id=$1 AND ingested_by=$2 AND follow_replies ORDER BY last_ingested_at DESC,id DESC"
        ).bind(cx.account_id).bind(cx.user_id).fetch_all(&mut *tx).await?;
        tx.rollback().await?;
        return Ok(rows
            .into_iter()
            .map(|(id, saved)| ScopeItem {
                label: id.clone(),
                id,
                meta: Some(saved.to_rfc3339()),
                selected: true,
            })
            .collect());
    }
    use axum::extract::{Path, Query, State};
    use axum::Extension;
    let mut out = Vec::new();
    let mut cursor: Option<Uuid> = None;
    for _ in 0..MAX_THREAD_PAGES {
        let resp = crate::routes::connectors::list_watched(
            State(cx.state.clone()),
            cx.tenant.clone(),
            Extension(cx.user.clone()),
            Path(provider.to_string()),
            Query(crate::routes::connectors::ListWatchedQuery {
                limit: Some(200),
                cursor,
            }),
        )
        .await;
        let body = read_json(resp).await?;
        for t in body["threads"].as_array().into_iter().flatten() {
            if t["following"].as_bool() != Some(true) {
                continue;
            }
            let Some(id) = t["thread_id"].as_str() else {
                continue;
            };
            out.push(ScopeItem {
                id: id.to_string(),
                label: t["title"]
                    .as_str()
                    .filter(|s| !s.is_empty())
                    .unwrap_or(id)
                    .to_string(),
                meta: t["saved_at"].as_str().map(str::to_string),
                selected: true,
            });
        }
        cursor = body["next_cursor"]
            .as_str()
            .and_then(|c| Uuid::parse_str(c).ok());
        if cursor.is_none() {
            break;
        }
    }
    Ok(out)
}

/// Stop following every followed thread the pick does not name. The rows stay
/// (deletion propagation reads them); only `follow_replies` flips, exactly as
/// the add-in's own toggle does.
pub async fn unfollow_threads_not_in(
    cx: &AdapterCx<'_>,
    provider: &str,
    keep: &[String],
) -> Result<(), AdapterError> {
    if provider == "google" && cx.tenant.org_context().is_some() {
        let mut tx = cx.tenant.begin_member_scoped(cx.account_id).await?;
        sqlx::query("UPDATE gmail_watched_threads SET follow_replies=false WHERE account_id=$1 AND ingested_by=$2 AND NOT(thread_id=ANY($3))")
            .bind(cx.account_id).bind(cx.user_id).bind(keep).execute(&mut *tx).await?;
        tx.commit().await?;
        return Ok(());
    }
    let pool = cx
        .tenant
        .pool
        .as_ref()
        .ok_or_else(|| AdapterError::Database("content plane unavailable".into()))?;
    let keep: std::collections::HashSet<&str> = keep.iter().map(String::as_str).collect();
    for item in followed_threads(cx, provider).await? {
        if keep.contains(item.id.as_str()) {
            continue;
        }
        let done = match provider {
            "google" => {
                crate::connectors::google_gmail::set_follow(pool, cx.account_id, &item.id, false)
                    .await
            }
            _ => {
                crate::connectors::microsoft::set_follow(pool, cx.account_id, &item.id, false).await
            }
        };
        done.map_err(|e| AdapterError::Database(e.to_string()))?;
    }
    Ok(())
}

/// Switch the mailbox's existing everything-mode (the worker that walks the
/// whole mailbox) on or off, through its route handler so its own gates
/// (organization flag, ocean resolution, owner-only) apply unchanged.
pub async fn set_mail_everything_mode(
    cx: &AdapterCx<'_>,
    provider: &str,
    on: bool,
) -> Result<(), AdapterError> {
    use axum::extract::State;
    use axum::{Extension, Json};
    let resp = match provider {
        "google" => {
            crate::routes::gmail_everything_mode::set_everything_mode_route(
                State(cx.state.clone()),
                cx.tenant.clone(),
                Extension(cx.user.clone()),
                Json(
                    crate::routes::gmail_everything_mode::SetEverythingModeRequest { enabled: on },
                ),
            )
            .await
        }
        _ => {
            crate::routes::outlook_everything_mode::set_everything_mode_route(
                State(cx.state.clone()),
                cx.tenant.clone(),
                Extension(cx.user.clone()),
                Json(
                    crate::routes::outlook_everything_mode::SetEverythingModeRequest {
                        enabled: on,
                    },
                ),
            )
            .await
        }
    };
    read_json(resp).await.map(|_| ())
}

/// A count off the content plane for the caller, as a scoped read. `None` when
/// the request has no content pool or the read failed: a count that cannot be
/// had is shown as unknown, not as a reason to fail the whole listing.
pub async fn count_for_member(cx: &AdapterCx<'_>, sql: &'static str) -> Option<u32> {
    let pool = cx.tenant.pool.as_ref()?;
    let mut tx = crate::tenancy::scope::begin_tenant_scoped(pool, cx.account_id, cx.user_id)
        .await
        .ok()?;
    let n: Result<i64, _> = sqlx::query_scalar(sql)
        .bind(cx.account_id)
        .bind(cx.user_id)
        .fetch_one(&mut *tx)
        .await;
    tx.rollback().await.ok();
    match n {
        Ok(n) => Some(n as u32),
        Err(e) => {
            tracing::warn!(error = %e, "scope: could not count picked items");
            None
        }
    }
}

/// [`count_for_member`] with the count written onto the runtime.
pub async fn with_member_count(
    cx: &AdapterCx<'_>,
    mut rt: super::SourceRuntime,
    sql: &'static str,
) -> super::SourceRuntime {
    if rt.scope.is_some_and(|s| s.mode == ScopeMode::Pick) {
        if let Some(n) = count_for_member(cx, sql).await {
            if let Some(s) = rt.scope.as_mut() {
                s.count = Some(n);
            }
        }
    }
    rt
}

#[cfg(test)]
mod tests {
    use super::*;

    fn item(id: &str, label: &str) -> ScopeItem {
        ScopeItem {
            id: id.into(),
            label: label.into(),
            meta: None,
            selected: false,
        }
    }

    fn query(q: Option<&str>, cursor: Option<&str>, limit: u32) -> ScopeItemsQuery {
        ScopeItemsQuery {
            q: q.map(str::to_string),
            cursor: cursor.map(str::to_string),
            limit,
        }
    }

    fn sel(mode: ScopeMode, n: usize) -> ScopeSelection {
        ScopeSelection {
            mode,
            item_ids: (0..n).map(|i| i.to_string()).collect(),
        }
    }

    #[test]
    fn modes_are_checked_against_the_shape() {
        let items = ScopeShape::Items { noun: "x" };
        let pick_only = ScopeShape::PickOnly { noun: "x" };
        let every = ScopeShape::EverythingOnly;
        assert!(validate(items, &sel(ScopeMode::Pick, 2)).is_ok());
        assert!(validate(items, &sel(ScopeMode::Everything, 0)).is_ok());
        assert!(validate(pick_only, &sel(ScopeMode::Pick, 1)).is_ok());
        assert!(validate(every, &sel(ScopeMode::Everything, 0)).is_ok());
        for (shape, mode, code) in [
            (pick_only, ScopeMode::Everything, "everything_not_allowed"),
            (every, ScopeMode::Pick, "pick_not_allowed"),
        ] {
            match validate(shape, &sel(mode, 0)) {
                Err(AdapterError::Refused {
                    status: 422,
                    code: c,
                }) => assert_eq!(c, code),
                other => panic!("{other:?}"),
            }
        }
        assert!(matches!(
            validate(items, &sel(ScopeMode::Pick, MAX_PICKED + 1)),
            Err(AdapterError::Refused {
                status: 422,
                code: "too_many_items"
            })
        ));
        assert!(validate(items, &sel(ScopeMode::Pick, MAX_PICKED)).is_ok());
    }

    #[test]
    fn paginate_searches_orders_and_pages() {
        let all: Vec<ScopeItem> = (1..=5)
            .map(|i| item(&format!("C{i}"), &format!("chan-{i}")))
            .collect();
        let first = paginate(all.clone(), &query(None, None, 2));
        assert_eq!(
            first
                .items
                .iter()
                .map(|i| i.id.as_str())
                .collect::<Vec<_>>(),
            ["C1", "C2"]
        );
        assert_eq!(first.total, Some(5));
        assert_eq!(first.next_cursor.as_deref(), Some("2"));
        let last = paginate(all.clone(), &query(None, Some("4"), 2));
        assert_eq!(last.items.len(), 1);
        assert_eq!(last.next_cursor, None);
        let found = paginate(all.clone(), &query(Some("  CHAN-3 "), None, 50));
        assert_eq!(found.items.len(), 1);
        assert_eq!(found.total, Some(1));
        // A cursor past the end, or one that is not ours, never panics.
        assert!(paginate(all.clone(), &query(None, Some("99"), 2))
            .items
            .is_empty());
        assert_eq!(paginate(all, &query(None, Some("zzz"), 2)).items.len(), 2);
    }

    #[test]
    fn limit_is_clamped() {
        assert_eq!(clamp_limit(None), DEFAULT_LIMIT);
        assert_eq!(clamp_limit(Some(0)), 1);
        assert_eq!(clamp_limit(Some(10_000)), MAX_LIMIT);
    }

    #[test]
    fn a_connection_from_before_the_catalog_keeps_what_it_had() {
        let items = ScopeShape::Items { noun: "x" };
        assert_eq!(legacy_mode("slack", items), ScopeMode::Everything);
        assert_eq!(legacy_mode("microsoft", items), ScopeMode::Pick);
        assert_eq!(legacy_mode("github", items), ScopeMode::Pick);
        assert_eq!(
            legacy_mode("google", ScopeShape::PickOnly { noun: "x" }),
            ScopeMode::Pick
        );
        assert_eq!(
            legacy_mode("jira", ScopeShape::EverythingOnly),
            ScopeMode::Everything
        );
    }

    fn row(
        enabled: bool,
        paused: bool,
        mode: Option<ScopeMode>,
        setup_done: bool,
    ) -> Option<StoredSource> {
        Some(StoredSource {
            enabled,
            paused,
            mode,
            setup_done,
        })
    }

    #[test]
    fn slack_admission_table() {
        use Admission::*;
        let cases: Vec<(&str, Option<StoredSource>, Admission)> = vec![
            ("no row: a member from before the catalog", None, Everything),
            (
                "legacy row: on, no mode, setup finished",
                row(true, false, None, true),
                Everything,
            ),
            (
                "new connection: off, nothing chosen",
                row(false, false, None, false),
                Blocked,
            ),
            (
                "new connection, source on, scope not chosen",
                row(true, false, None, false),
                Picked,
            ),
            (
                "picked",
                row(true, false, Some(ScopeMode::Pick), true),
                Picked,
            ),
            (
                "everything",
                row(true, false, Some(ScopeMode::Everything), true),
                Everything,
            ),
            (
                "paused beats everything",
                row(true, true, Some(ScopeMode::Everything), true),
                Blocked,
            ),
            ("paused legacy member", row(true, true, None, true), Blocked),
            (
                "turned off",
                row(false, false, Some(ScopeMode::Everything), true),
                Blocked,
            ),
        ];
        for (name, r, want) in cases {
            assert_eq!(Admission::of(r), want, "{name}");
        }
        assert!(Everything.takes(false) && Everything.takes(true));
        assert!(Picked.takes(true) && !Picked.takes(false));
        assert!(!Blocked.takes(true));
    }
}

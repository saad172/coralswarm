//! Handlers for the `/v1/conversations` resource (cs-ask-chat-a).
//!
//! Persisted chat conversations for the conversational Ask page. A conversation
//! is scoped to one ocean (`graph_id`) and owned by its creator. Conversations
//! are private to the owner; setting `shared = true` exposes a read-only copy to
//! other members of the same ocean.
//!
//! Conversations are created lazily by the streaming query endpoint
//! (`POST /v1/queries/stream`) on the first turn — there is no public create
//! route, so empty orphan conversations never exist. This module covers
//! listing, reading (with messages), rename/pin/share, and delete.
//!
//! # Consumer-plane RLS scoping (org-tenancy OPS-4 phase B.2)
//! Every query here that touches an RLS-covered consumer table
//! (`conversations`/`conversation_messages`/`conversation_oceans`/`graphs`,
//! migration `0083_consumer_rls`) runs inside ONE
//! [`crate::tenancy::scope::begin_account_scoped`] transaction per request,
//! through the Store's `_scoped` twins. Scope-account choice:
//!   * graph-anchored listings (`?graph_id=`) scope by the OCEAN'S owning
//!     account — that is the account `conversations.account_id` carries (the
//!     streaming endpoint stamps `graph_account_id(primary_graph)` on
//!     create), so both own and shared rows in that ocean live under it;
//!   * conversation-id-anchored reads/writes and the cross-ocean listing
//!     scope by the CALLER'S ACTIVE ACCOUNT — the only account known before
//!     the row is loaded (no conversation→account SECURITY DEFINER bootstrap
//!     resolver exists, deliberately). Under live RLS a conversation whose
//!     account differs from the caller's active one is filtered to "absent",
//!     failing closed onto this module's existing non-leaking 404.
//!
//! Deliberately OUTSIDE the transaction: `ReachabilityResolver` (permanent
//! multi-account SECURITY DEFINER carve-out) and `Store::graph_account_id`
//! (the bootstrap resolver). With no pool (`tenant.pool == None`) opening the
//! transaction fails and the handler returns a 500.

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Extension, Json,
};
use serde::Deserialize;
use serde_json::{json, Value};
use std::collections::HashMap;
use uuid::Uuid;

use crate::{
    middleware::auth::UserContext, store::Conversation, store::Store,
    tenancy::extractor::Tenant, tenancy::reachability::ReachabilityResolver,
};


// Reads through the `Tenant` extractor's resolved store (org-tenancy PR 1.5)
// rather than ambient `state.store` — `conversations`/`conversation_messages`/
// `conversation_oceans` are tenant-tree tables.
macro_rules! require_store {
    ($tenant:expr) => {
        match &$tenant.store {
            Some(s) => s,
            None => {
                return (
                    StatusCode::SERVICE_UNAVAILABLE,
                    Json(json!({"error": "service unavailable: no database"})),
                )
                    .into_response()
            }
        }
    };
}

/// The request's one account-scoped transaction. `begin_scoped_tx!` always
/// yields `Some`. See the module doc's RLS section.
pub(crate) type ScopedTx = Option<sqlx::Transaction<'static, sqlx::Postgres>>;

/// Open the request's account-scoped transaction (org-tenancy OPS-4 phase
/// B.2), or early-return a 500. On the org plane this opens on the TENANT
/// pool via `Tenant::begin_member_scoped`, which also sets `app.user_id`.
///
/// That GUC has real policies to satisfy there. Whole-database isolation is
/// real, but it covers the CROSS-ORG boundary only, and it is NOT a reason to
/// skip the scope: `migrations_tenant/0009_tenant_rls` puts member-ocean
/// policies on `conversations`/`conversation_messages`/`conversation_oceans`/
/// `conversation_surfaced_edges`, every conversation-surface table this file
/// reads. Those policies resolve through `tenant_rls_visible_graph_ids()`, and
/// with neither `app.user_id` nor `app.tenant_actor` set
/// `tenant_rls_actor_user_id()` is NULL — so a sidebar listing keeps the org
/// and personal oceans and loses every member and team ocean. The GUC is
/// unevaluated today for a ROLE reason, not a policy one: tenant pools connect
/// as the table owner. Phase 3 (`OPS-4-T`) repoints each tenant DSN at the
/// non-owner `coralswarm_tenant_app` role, which is what makes it bite. See
/// `tenancy::scope`'s module doc.
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
                    )
                        .into_response();
                }
            }
        }
    };
}

/// Receipt reads require the verified actor on both planes; account scoping
/// alone cannot certify private question/context lineage on Personal.
macro_rules! begin_answer_read_tx {
    ($tenant:expr,$account:expr,$user:expr) => {{
        let mut result=begin_scoped_tx!($tenant,$account);
        if let Some(tx)=result.as_mut() {
            if crate::tenancy::scope::set_tenant_user(tx,$user).await.is_err() {
                return (StatusCode::SERVICE_UNAVAILABLE,Json(json!({"error":"service unavailable"}))).into_response();
            }
        }
        result
    }};
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
                )
                    .into_response();
            }
        }
    };
}

/// Commit a read-only handler's scoped transaction. Failures are logged and
/// swallowed: the response data is already materialized, and a read-only
/// `SET LOCAL` transaction only fails to commit when a tolerated best-effort
/// sub-query already aborted it (those sub-results then fell back to their
/// existing best-effort defaults — same degradation as before this change).
async fn commit_read_tx(tx: ScopedTx) {
    if let Some(tx) = tx {
        if let Err(e) = tx.commit().await {
            tracing::warn!(error = %e, "read-only scoped transaction commit failed");
        }
    }
}

// ── Serialization ───────────────────────────────────────────────────────────

/// JSON shape of a conversation summary (sidebar row). `mine` lets the client
/// distinguish own vs shared-by-others without exposing other users' ids.
/// `oceans` carries the conversation's scope descriptor as ocean badges
/// (ADR-0011 story 2): `[{ ocean_id, ocean_name }]`.
fn conv_json(c: &Conversation, mine: bool, oceans: Vec<Value>) -> Value {
    json!({
        "id": c.id,
        "graph_id": c.graph_id,
        "title": c.title,
        "pinned": c.pinned,
        "shared": c.shared,
        "mode": c.mode,
        "oceans": oceans,
        "mine": mine,
        "created_at": c.created_at,
        "updated_at": c.updated_at,
    })
}

/// Resolve `graph_id → display name` for ocean badges (best-effort). Mirrors the
/// helper in the queries route — each name needs the owning account.
///
/// Sequential (was `join_all` pre-B.2): a shared `&mut` scoped transaction
/// cannot be borrowed by concurrent queries. The set of distinct oceans on a
/// sidebar listing is small, so this is a non-issue in practice. The
/// `graph_account_id` lookup stays on the pool (SECURITY DEFINER bootstrap
/// resolver — see the module doc's RLS section); the `graphs` read itself is
/// scoped.
async fn resolve_ocean_names(
    store: &dyn Store,
    scoped_tx: &mut ScopedTx,
    graph_ids: &[Uuid],
) -> HashMap<Uuid, String> {
    let mut out = HashMap::new();
    for &gid in graph_ids {
        let Some(account) = store.graph_account_id(gid).await.ok().flatten() else {
            continue;
        };
        let Some(g) = scoped_store!(store, *scoped_tx, get_graph / get_graph_scoped(gid, account))
            .ok()
            .flatten()
        else {
            continue;
        };
        out.insert(gid, g.name);
    }
    out
}

// ── Request types ───────────────────────────────────────────────────────────

#[derive(Deserialize)]
pub struct PatchConversation {
    pub title: Option<String>,
    pub pinned: Option<bool>,
    pub shared: Option<bool>,
}

// ── Authz helper ────────────────────────────────────────────────────────────

pub(crate) enum Unreadable {
    NotVisible,
    Forbidden,
    NoDatabase,
    Store,
}

/// The `conversations` read runs through `scoped_tx` (caller's active
/// account — see the module doc's RLS section); the reachability check stays
/// on the pool (permanent SECURITY DEFINER carve-out).
pub(crate) async fn readable_conversation(
    tenant: &Tenant,
    scoped_tx: &mut ScopedTx,
    id: Uuid,
    user_id: Uuid,
) -> Result<Conversation, Unreadable> {
    let store = tenant.store.as_ref().ok_or(Unreadable::NoDatabase)?;

    let conv = match scoped_store!(
        store,
        *scoped_tx,
        get_conversation / get_conversation_scoped(id)
    ) {
        Ok(Some(c)) => c,
        Ok(None) => return Err(Unreadable::NotVisible),
        Err(e) => {
            tracing::error!(error = %e, "get_conversation: store error");
            return Err(Unreadable::Store);
        }
    };

    if conv.user_id == user_id {
        return Ok(conv);
    }

    // Not the owner. Reachability is now evaluated FIRST and for every path,
    // because it decides WHICH refusal is correct — and getting that wrong leaks
    // existence.
    //
    // Before the owning-account anchor fix, a conversation in an ocean the caller
    // could not reach was hidden by RLS: the lookup above returned `Ok(None)` and
    // this function answered 404 without ever reaching the branch below. Scoping
    // to the ocean's owner makes that row VISIBLE, so an unconditional 403 here
    // would newly tell an unauthorized caller that the conversation exists — an
    // existence oracle where there used to be none, and exactly what
    // `routes_b2_rls_pg` pins ("filtered to absent, not 403-leaked").
    //
    // So: cannot see the ocean at all -> 404, byte-identical to a missing row.
    // Can see the ocean but the thread is not shared -> an honest 403; existence
    // is not a secret from someone who can already read that ocean, and this is
    // the pre-existing behaviour for a same-account thread owned by someone else.
    let reachable = match ReachabilityResolver::graph_reachable_by_user(
        tenant.decision.as_ref(),
        store.as_ref(),
        conv.graph_id,
        user_id,
    )
    .await
    {
        Ok(r) => r,
        Err(e) => {
            tracing::error!(error = %e, "graph_reachable_by_user: store error");
            return Err(Unreadable::Store);
        }
    };
    if !reachable {
        return Err(Unreadable::NotVisible);
    }
    if conv.shared {
        return Ok(conv);
    }

    Err(Unreadable::Forbidden)
}

async fn load_readable(
    tenant: &Tenant,
    scoped_tx: &mut ScopedTx,
    id: Uuid,
    user_id: Uuid,
) -> Result<Conversation, Response> {
    readable_conversation(tenant, scoped_tx, id, user_id)
        .await
        .map_err(|e| match e {
            Unreadable::NotVisible => (
                StatusCode::NOT_FOUND,
                Json(json!({"error": "conversation not found"})),
            )
                .into_response(),
            Unreadable::Forbidden => {
                (StatusCode::FORBIDDEN, Json(json!({"error": "forbidden"}))).into_response()
            }
            Unreadable::NoDatabase => (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({"error": "service unavailable: no database"})),
            )
                .into_response(),
            Unreadable::Store => (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            )
                .into_response(),
        })
}

/// The account that OWNS this conversation, via migration 0105's SECURITY
/// DEFINER bootstrap — `None` with no pool or when the
/// conversation does not exist.
///
/// `conversations` is covered by 0083, so it cannot be read to discover its own
/// account: that is the chicken-and-egg these bootstrap resolvers exist for, and
/// the same hop `services::promotion` makes with `resolve_node_account_id`.
/// Returns a BARE account id — no content, no membership facts.
pub(crate) async fn conversation_owner_account(tenant: &Tenant, id: Uuid) -> Option<Uuid> {
    let pool = tenant.pool.as_ref()?;
    sqlx::query_scalar::<_, Option<Uuid>>("SELECT resolve_conversation_account_id($1)")
        .bind(id)
        .fetch_optional(pool)
        .await
        .ok()
        .flatten()
        .flatten()
}

// ── Handlers ────────────────────────────────────────────────────────────────

pub(crate) async fn recheck_headline_message_scopes(
    tx:&mut sqlx::Transaction<'_,sqlx::Postgres>,
    messages:&[crate::store::ConversationMessage],
    statuses:&mut HashMap<Uuid,&'static str>,
)->Result<(),sqlx::Error> {
    let ids:Vec<Uuid>=messages.iter().filter(|m|m.role=="assistant"&&statuses.get(&m.id).copied()==Some("complete")).map(|m|m.id).collect();
    if ids.is_empty() {return Ok(());}
    let rows:Vec<(Uuid,Value,Option<Value>)>=sqlx::query_as(r#"
     SELECT m.id,r.support,q.library_scope FROM conversation_messages m
     JOIN answer_support_receipts r ON r.receipt_id=m.id
     LEFT JOIN LATERAL(
      SELECT question.library_scope FROM jsonb_array_elements(r.support) e
      JOIN conversation_messages question ON question.id=(e->>'message_id')::uuid
      WHERE e->>'kind'='user_question' AND question.conversation_id=m.conversation_id
       AND question.seq=m.seq-1 AND question.role='user' AND question.library_scope IS NOT NULL
      LIMIT 1
     ) q ON true
     WHERE m.id=ANY($1) AND EXISTS(SELECT 1 FROM jsonb_array_elements(r.support) e
      WHERE e->>'kind'='legacy_chunk' AND e->>'projection'='headline_v1')
    "#).bind(&ids).fetch_all(&mut **tx).await?;
    let mut groups:HashMap<String,(crate::library_read::LibraryScope,Vec<(Uuid,Value)>)>=HashMap::new();
    for (id,support,scope) in rows {
        let Some(mut scope)=scope.and_then(|v|serde_json::from_value::<crate::library_read::LibraryScope>(v).ok()) else {statuses.insert(id,"unavailable");continue;};
        if scope.normalize().is_err() {statuses.insert(id,"unavailable");continue;}
        let key=serde_json::to_string(&scope).expect("normalized message scope");
        groups.entry(key).or_insert_with(||(scope,Vec::new())).1.push((id,support));
    }
    for (_, (scope,receipts)) in groups {
        if !crate::library_read::validate_objects_tx(tx,&scope).await? {
            for (id,_) in receipts {statuses.insert(id,"unavailable");}
        } else {
            for (id,allowed) in crate::library_read::ask_receipts_in_scope_tx(tx,&scope,&receipts).await? {
                if !allowed {statuses.insert(id,"unavailable");}
            }
        }
    }
    Ok(())
}

/// Keyset cursor for `GET /v1/conversations/{id}`: return the page of messages
/// immediately older than this `seq` (the previous response's `older_before_seq`).
#[derive(Deserialize)]
pub struct MessagePageParams {
    pub before_seq: Option<i32>,
}

/// `GET /v1/conversations/{id}` — a conversation plus one page of its turns,
/// in order: the latest page, or the page older than `?before_seq=`. Every
/// page reruns the reader, scope and live-receipt checks.
pub async fn get_conversation(
    State(state): State<std::sync::Arc<crate::AppState>>,
    tenant: Tenant,
    Extension(user_ctx): Extension<UserContext>,
    Path(id): Path<Uuid>,
    Query(page): Query<MessagePageParams>,
    door: Option<Extension<crate::middleware::auth::DoorAdmission>>,
) -> impl IntoResponse {
    if page.before_seq.is_some_and(|seq| seq < 1) {
        return (StatusCode::BAD_REQUEST,Json(json!({"error":"before_seq must be a positive message seq"}))).into_response();
    }
    // Conversation-id-anchored. Scope to the conversation's OWNING account,
    // discovered via 0105's bootstrap resolver.
    //
    // This used to scope to the CALLER's active account, which hides the row from
    // RLS whenever that differs from the ocean's owner — every org member, and
    // any user with more than one account using the Personal/Org switcher. The
    // sidebar (graph-anchored, so it resolves the owner) rendered the row while
    // this returned 404 for it.
    //
    // Authorization is UNCHANGED and does not come from the scope: `load_readable`
    // still requires the caller to be the owner, or the conversation to be shared
    // AND the ocean reachable by them, else 403. Widening visibility here only
    // lets that check actually run. Falling back to the caller's account when the
    // resolver finds nothing preserves the existing 404 for an unknown id.
    let scope_account = conversation_owner_account(&tenant, id)
        .await
        .unwrap_or(user_ctx.account_id);
    let mut scoped_tx = begin_answer_read_tx!(tenant, scope_account,user_ctx.user_id);
    let conv = match load_readable(&tenant, &mut scoped_tx, id, user_ctx.user_id).await {
        Ok(c) => c,
        Err(resp) => return resp,
    };
    let store = require_store!(tenant);

    let mut library_scope=Value::Null;
    let mut temporal_selection=Value::Null;
    let mut scope_status="unscoped";
    if let Some(tx)=scoped_tx.as_mut() {
        if let Ok(Some(Some(value)))=sqlx::query_scalar::<_,Option<Value>>("SELECT library_scope FROM conversations WHERE id=$1").bind(conv.id).fetch_optional(&mut **tx).await {
            scope_status="unavailable";
            if let Ok(mut scope)=serde_json::from_value::<crate::library_read::LibraryScope>(value) {
                if matches!(crate::library_read::authorize_scope(&tenant,&user_ctx,&mut scope).await,Ok(account) if account==scope_account)
                    && matches!(crate::library_read::validate_objects_tx(tx,&scope).await,Ok(true)) {
                    library_scope=json!(scope);scope_status="resolved";
                }
            }
        }
    }
    if scope_status=="resolved" {
        if let Some(tx)=scoped_tx.as_mut() {
            if let Ok(Some(value))=sqlx::query_scalar::<_,Option<Value>>("SELECT temporal_selection FROM conversations WHERE id=$1 AND user_id=$2")
                .bind(conv.id).bind(user_ctx.user_id).fetch_optional(&mut **tx).await {
                temporal_selection=value.unwrap_or_else(||json!(crate::temporal::TemporalSelection::default()));
            }
        }
    }
    let oceans = conv_oceans(store.as_ref(), &mut scoped_tx, &conv).await;
    // #1496: a pure read, fenced before any message leaves. When this
    // request's own auth read already proved the caller's control-plane
    // membership (`DoorAdmission`, fresh and for exactly this account/user/
    // org), that stands in for the second control transaction. It is only
    // minted for an interactive Clerk session; for every other credential
    // (Clerk OAuth access tokens incl. the add-in clients, PAT, add-in
    // session), slow-path auth, or a stale or mismatched fact, the
    // full re-check runs as before. Graph reachability is re-read
    // either way.
    let door = door.as_ref().map(|Extension(d)| d);
    if tenant.pool.is_some() && !crate::source_evidence::still_admitted_at_door(&state,&tenant,&user_ctx,conv.graph_id,scope_account,door).await {
        return (StatusCode::NOT_FOUND,Json(json!({"error":"conversation not found or access denied"}))).into_response();
    }
    let Some(tx) = scoped_tx.as_mut() else { return (StatusCode::SERVICE_UNAVAILABLE,Json(json!({"error":"service unavailable"}))).into_response(); };
    let (msgs,messages_truncated,older_before_seq) = match crate::services::conversation_page::messages(tx,user_ctx.user_id,conv.id,page.before_seq).await {
        Ok(value) => value,
        Err(_) => return (StatusCode::SERVICE_UNAVAILABLE,Json(json!({"error":"service unavailable"}))).into_response(),
    };

    commit_read_tx(scoped_tx).await;
    let mine = conv.user_id == user_ctx.user_id;
    let mut conversation=conv_json(&conv,mine,oceans);conversation["library_scope"]=library_scope;conversation["scope_status"]=json!(scope_status);
    conversation["temporal_selection"]=temporal_selection;
    (
        StatusCode::OK,
        Json(json!({
            "conversation": conversation,
            "messages": msgs,
            "messages_truncated": messages_truncated,
            "older_before_seq": older_before_seq,
        })),
    )
        .into_response()
}

/// Ocean badges for a single conversation's current scope (ADR-0011 story 2),
/// falling back to its primary `graph_id` when the join is empty.
async fn conv_oceans(
    store: &dyn Store,
    scoped_tx: &mut ScopedTx,
    conv: &Conversation,
) -> Vec<Value> {
    let mut gids = scoped_store!(
        store,
        *scoped_tx,
        list_conversation_oceans / list_conversation_oceans_scoped(conv.id)
    )
    .unwrap_or_default();
    if gids.is_empty() {
        gids.push(conv.graph_id);
    }
    let names = resolve_ocean_names(store, scoped_tx, &gids).await;
    gids.into_iter()
        .map(|gid| {
            json!({
                "ocean_id": gid.to_string(),
                "ocean_name": names.get(&gid).cloned().unwrap_or_default(),
            })
        })
        .collect()
}

/// `PATCH /v1/conversations/{id}` — rename, pin/unpin, or share/unshare.
/// Owner only.
pub async fn patch_conversation(
    tenant: Tenant,
    Extension(user_ctx): Extension<UserContext>,
    Path(id): Path<Uuid>,
    Json(body): Json<PatchConversation>,
) -> impl IntoResponse {
    // Conversation-id-anchored: scope to the conversation's OWNING account via
    // 0105's bootstrap resolver — see `get_conversation` for why the caller's
    // active account was the wrong anchor. One transaction also makes the
    // multi-field patch atomic instead of a sequence of independently-committed
    // writes. Authorization is still `load_readable`'s, unchanged.
    let scope_account = conversation_owner_account(&tenant, id)
        .await
        .unwrap_or(user_ctx.account_id);
    let mut scoped_tx = begin_scoped_tx!(tenant, scope_account);
    let conv = match load_readable(&tenant, &mut scoped_tx, id, user_ctx.user_id).await {
        Ok(c) => c,
        Err(resp) => return resp,
    };
    if conv.user_id != user_ctx.user_id {
        return (
            StatusCode::FORBIDDEN,
            Json(json!({"error": "not the owner"})),
        )
            .into_response();
    }
    let store = require_store!(tenant);

    if let Some(title) = body.title.as_deref() {
        let title = title.trim();
        if title.is_empty() || title.len() > 200 {
            return (
                StatusCode::UNPROCESSABLE_ENTITY,
                Json(json!({"error": "title must be 1–200 characters"})),
            )
                .into_response();
        }
        if let Err(e) = scoped_store!(
            store,
            scoped_tx,
            rename_conversation / rename_conversation_scoped(id, title)
        ) {
            tracing::error!(error = %e, "rename_conversation: store error");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            )
                .into_response();
        }
    }
    if let Some(pinned) = body.pinned {
        if let Err(e) = scoped_store!(
            store,
            scoped_tx,
            set_conversation_pinned / set_conversation_pinned_scoped(id, pinned)
        ) {
            tracing::error!(error = %e, "set_conversation_pinned: store error");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            )
                .into_response();
        }
    }
    if let Some(shared) = body.shared {
        if let Err(e) = scoped_store!(
            store,
            scoped_tx,
            set_conversation_shared / set_conversation_shared_scoped(id, shared)
        ) {
            tracing::error!(error = %e, "set_conversation_shared: store error");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            )
                .into_response();
        }
    }

    // Return the fresh row. On the (should-never-happen) re-read failure the
    // pre-B.2 code returned `{"ok": true}` after per-call-committed writes;
    // under one shared transaction that same degenerate path now surfaces the
    // commit failure as a 500 instead of a false success — the honest
    // transactional equivalent.
    match scoped_store!(
        store,
        scoped_tx,
        get_conversation / get_conversation_scoped(id)
    ) {
        Ok(Some(c)) => {
            let oceans = conv_oceans(store.as_ref(), &mut scoped_tx, &c).await;
            commit_write_tx!(scoped_tx);
            (StatusCode::OK, Json(conv_json(&c, true, oceans))).into_response()
        }
        _ => {
            commit_write_tx!(scoped_tx);
            (StatusCode::OK, Json(json!({"ok": true}))).into_response()
        }
    }
}

/// `DELETE /v1/conversations/{id}` — hard delete (messages cascade). Owner only.
pub async fn delete_conversation(
    tenant: Tenant,
    Extension(user_ctx): Extension<UserContext>,
    Path(id): Path<Uuid>,
) -> impl IntoResponse {
    // Conversation-id-anchored. Scope to the conversation's OWNING account,
    // discovered via 0105's bootstrap resolver.
    //
    // This used to scope to the CALLER's active account, which hides the row from
    // RLS whenever that differs from the ocean's owner — every org member, and
    // any user with more than one account using the Personal/Org switcher. The
    // sidebar (graph-anchored, so it resolves the owner) rendered the row while
    // this returned 404 for it.
    //
    // Authorization is UNCHANGED and does not come from the scope: `load_readable`
    // still requires the caller to be the owner, or the conversation to be shared
    // AND the ocean reachable by them, else 403. Widening visibility here only
    // lets that check actually run. Falling back to the caller's account when the
    // resolver finds nothing preserves the existing 404 for an unknown id.
    let scope_account = conversation_owner_account(&tenant, id)
        .await
        .unwrap_or(user_ctx.account_id);
    let mut scoped_tx = begin_scoped_tx!(tenant, scope_account);
    let conv = match load_readable(&tenant, &mut scoped_tx, id, user_ctx.user_id).await {
        Ok(c) => c,
        Err(resp) => return resp,
    };
    if conv.user_id != user_ctx.user_id {
        return (
            StatusCode::FORBIDDEN,
            Json(json!({"error": "not the owner"})),
        )
            .into_response();
    }
    let store = require_store!(tenant);

    match scoped_store!(
        store,
        scoped_tx,
        delete_conversation / delete_conversation_scoped(id)
    ) {
        Ok(()) => {
            commit_write_tx!(scoped_tx);
            (StatusCode::OK, Json(json!({"ok": true}))).into_response()
        }
        Err(e) => {
            tracing::error!(error = %e, "delete_conversation: store error");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            )
                .into_response()
        }
    }
}

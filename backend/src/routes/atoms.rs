//! Atom (coral) detail endpoint.
//!
//! - `GET /v1/atoms/{atom_id}` — full coral detail with content + linked reefs,
//!   for the UI's drill-through path. IDOR-guarded by the Store layer:
//!   `atom_detail` returns `None` if the atom doesn't exist OR if the atom's
//!   graph doesn't belong to the caller's account.
//!
//! Content resolution: when `content_inline` is populated (dev mode without R2,
//! or atoms that pre-date the R2 split), the inline text is returned directly.
//! When `content_inline` is NULL and R2 is configured, the route decrypts the
//! blob with the account DEK. When R2 isn't configured and `content_inline` is
//! NULL (an edge case that shouldn't happen), the response
//! returns `content: null` rather than failing.

use axum::{
    extract::{Path, State},
    http::StatusCode,
    Extension, Json,
};
use serde_json::{json, Value};
use std::sync::Arc;
use utoipa::path as utoipa_path;
use uuid::Uuid;

use crate::{
    middleware::auth::UserContext, tenancy::extractor::Tenant,
    tenancy::reachability::ReachabilityResolver, AppState,
};

// ── Helpers ───────────────────────────────────────────────────────────────────

// Reads through the `Tenant` extractor's resolved store (org-tenancy PR 1.4)
// rather than ambient `state.store` — `atoms`/`graphs` are tenant-tree tables.
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

// Pin one atom row and its verified source addressing across slow blob I/O.
// One extra sentinel mapping bounds work and fails closed on excessive support.
async fn content_binding(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    graph: Uuid,
    atom: Uuid,
) -> Result<Option<(String, Vec<Uuid>)>, sqlx::Error> {
    let row: Option<(String, Vec<Uuid>)> = sqlx::query_as(
        "SELECT a.xmin::text,ARRAY(SELECT c.source_version_id FROM source_version_chunks c WHERE c.graph_id=a.graph_id AND c.atom_id=a.atom_id ORDER BY c.source_version_id LIMIT 257) FROM atoms a WHERE a.graph_id=$1 AND a.atom_id=$2 AND library_atom_permitted(a.graph_id,a.atom_id)",
    ).bind(graph).bind(atom).fetch_optional(&mut **tx).await?;
    Ok(row.filter(|(_, versions)| versions.len() <= 256))
}
// ── GET /v1/atoms/{atom_id} ───────────────────────────────────────────────────

#[utoipa_path(
    get,
    path = "/v1/atoms/{atom_id}",
    tag = "atoms",
    security(("bearer_token" = [])),
    params(("atom_id" = Uuid, Path, description = "Coral (atom) identifier")),
    responses(
        (status = 200, description = "Coral detail with linked reefs", body = Value),
        (status = 404, description = "Coral not found or access denied"),
        (status = 503, description = "Database unavailable"),
    )
)]
pub async fn get_atom(
    State(state): State<Arc<AppState>>,
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
    Path(atom_id): Path<Uuid>,
) -> (StatusCode, Json<Value>) {
    let store = require_store!(tenant);

    // ── Account bootstrap + RLS scoping (org-tenancy B.1) ─────────────────────
    // This handler operates on the atom's OWN account, not the caller's active
    // account (its content is encrypted under that account's DEK) — so the
    // account_id to scope with must be DISCOVERED from the atom id first. That
    // lookup cannot itself run under RLS (chicken-and-egg), which is exactly
    // what the `resolve_atoms_account_id` SECURITY DEFINER bootstrap carve-out
    // (migration 0084, org-tenancy Phase S) exists for: it returns a bare
    // account uuid and nothing else. Every subsequent RLS-covered read
    // (`atoms` via `atom_graph_id_scoped` / `atom_detail_scoped`) then runs
    // inside ONE `begin_account_scoped` transaction on that account.
    // A pool-less `Tenant` keeps the plain Store path.
    let mut scoped_tx: Option<sqlx::Transaction<'static, sqlx::Postgres>> = None;
    let (atom_graph, atom_acct) = if let Some(pool) = &tenant.pool {
        let accounts: Vec<Uuid> =
            match sqlx::query_scalar("SELECT * FROM resolve_atoms_account_id($1)")
                .bind(&[atom_id][..])
                .fetch_all(pool)
                .await
            {
                Ok(a) => a,
                Err(e) => {
                    tracing::error!(error = %e, "get_atom account bootstrap error");
                    return (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(json!({"error": "internal error"})),
                    );
                }
            };
        let Some(acct) = accounts.first().copied() else {
            return (
                StatusCode::NOT_FOUND,
                Json(json!({"error": "coral not found or access denied"})),
            );
        };
        let mut tx = match tenant.begin_member_scoped(acct).await {
            Ok(tx) => tx,
            Err(e) => {
                tracing::error!(error = %e, "get_atom: begin_account_scoped failed");
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({"error": "internal error"})),
                );
            }
        };
        let g = match store.atom_graph_id_scoped(&mut tx, atom_id).await {
            Ok(Some(g)) => g,
            Ok(None) => {
                return (
                    StatusCode::NOT_FOUND,
                    Json(json!({"error": "coral not found or access denied"})),
                )
            }
            Err(e) => {
                tracing::error!(error = %e, "get_atom lookup error");
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({"error": "internal error"})),
                );
            }
        };
        scoped_tx = Some(tx);
        (g, acct)
    } else {
        let g = match store.atom_graph_id(atom_id).await {
            Ok(Some(g)) => g,
            Ok(None) => {
                return (
                    StatusCode::NOT_FOUND,
                    Json(json!({"error": "coral not found or access denied"})),
                )
            }
            Err(e) => {
                tracing::error!(error = %e, "get_atom lookup error");
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({"error": "internal error"})),
                );
            }
        };
        let a = match store.graph_account_id(g).await {
            Ok(Some(a)) => a,
            Ok(None) => {
                return (
                    StatusCode::NOT_FOUND,
                    Json(json!({"error": "coral not found or access denied"})),
                )
            }
            Err(e) => {
                tracing::error!(error = %e, "get_atom account lookup error");
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({"error": "internal error"})),
                );
            }
        };
        (g, a)
    };

    // Authorize by reachability (ADR-0007 D9). Deliberately UNSCOPED: the
    // reachability check is the PR #284 SECURITY DEFINER carve-out
    // (`resolve_graph_reachable_by_user`) — a multi-account predicate that
    // cannot run under a single `app.account_id` scope by design.
    match ReachabilityResolver::graph_reachable_by_user(
        tenant.decision.as_ref(),
        store.as_ref(),
        atom_graph,
        user.user_id,
    )
    .await
    {
        Ok(true) => {}
        Ok(false) => {
            return (
                StatusCode::NOT_FOUND,
                Json(json!({"error": "coral not found or access denied"})),
            )
        }
        Err(e) => {
            tracing::error!(error = %e, "get_atom reachability error");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            );
        }
    }

    let original_binding = if let Some(tx)=scoped_tx.as_mut() {
        match content_binding(tx,atom_graph,atom_id).await {
            Ok(Some(binding)) => Some(binding),
            Ok(None) => return (StatusCode::NOT_FOUND,Json(json!({"error":"coral not found or access denied"}))),
            Err(_) => return (StatusCode::SERVICE_UNAVAILABLE,Json(json!({"error":"service unavailable"}))),
        }
    } else { None };

    let detail_res = match scoped_tx.as_mut() {
        Some(tx) => store.atom_detail_scoped(tx, atom_id, atom_acct).await,
        None => store.atom_detail(atom_id, atom_acct).await,
    };
    let mut detail = match detail_res {
        Ok(Some(d)) => d,
        Ok(None) => {
            // Don't leak 404 vs 403.
            return (
                StatusCode::NOT_FOUND,
                Json(json!({"error": "coral not found or access denied"})),
            );
        }
        Err(e) => {
            tracing::error!(error = %e, "get_atom DB error");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            );
        }
    };

    // All RLS-covered reads are done — commit before the (non-RLS) DEK/R2
    // content resolution below so the pooled connection isn't held across
    // blob fetches.
    if let Some(tx) = scoped_tx.take() {
        if let Err(e) = tx.commit().await {
            tracing::error!(error = %e, "get_atom: scoped commit failed");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            );
        }
    }

    // Resolve content: inline if present; otherwise decrypt from R2 if
    // configured. If neither path yields content, return null and let the UI
    // decide how to render (dev fallback).
    //
    // Every branch is stripped of the contextual chunk-header
    // (cs-phase0-chunk-headers, `detail.header_len`) before being returned —
    // this endpoint is the drill-through "coral detail" view, so its
    // `content` must be the original text, never the synthetic "Meeting:
    // ..." embedding-time header.
    let content: Option<String> = if let Some(inline) = detail.content_inline.clone() {
        Some(crate::store::strip_header_prefix(&inline, detail.header_len).to_string())
    } else if let Some(ref r2) = state.r2 {
        // Resolve the account DEK via the same path embed/extract workers use,
        // dispatching on tenancy (org-tenancy PR 1.4): an org tenant's DEK
        // lives in ITS OWN `account_keys` table (org-tenancy PR 1.3), never
        // the control-plane `accounts.dek_wrapped` this unconditionally read
        // before the split. `tenant.pool` mirrors `store`'s own presence
        // invariant (`AppState`: "store implies db on cloud" — same as the
        // `.expect` this replaces), and the `Org` arm ignores it anyway,
        // reading `ctx.pool` off the context instead.
        //
        // NOT flag-off byte-identical (P2-1, adversarial review follow-up):
        // this used to call the UNCACHED `crypto::resolve_dek` directly; the
        // dispatch helper's Personal/None arm calls the CACHED
        // `resolve_dek_cached` instead (matching `ingest.rs`'s existing
        // pattern, and consistent with every other DEK-resolving route in
        // this PR). Benign in practice — the cache TTL is 5 minutes and a
        // `rotate_dek` call is rare — but after a DEK rotation this route can
        // now serve a stale DEK for up to that TTL, where it previously
        // always read fresh. Declared here rather than silently changed.
        let dek_pool = tenant
            .pool
            .as_ref()
            .expect("store implies a content-plane pool on cloud");
        match crate::crypto::resolve_dek_cached_dispatch(
            tenant.decision.as_ref(),
            dek_pool,
            atom_acct,
            &state.dek_cache,
        )
        .await
        {
            Ok(Some(dek)) => {
                // org-tenancy PR 4.1 deferred assert (gap-review item 7): the
                // stored `blob_key`'s own org segment must equal THIS
                // request's org context before it is ever fetched — a
                // forged/mismatched/corrupted `blob_key` (org B's prefix
                // reaching an org A request) must never cross, even though
                // reachability/DEK resolution above already confirmed this
                // atom is legitimately visible on org A's own tenant pool.
                let expected_org = tenant.org_context().map(|ctx| ctx.org_id.as_str());
                match coralswarm_core::r2::assert_blob_key_org_scope(
                    &detail.content_blob_key,
                    expected_org,
                ) {
                    Err(e) => {
                        tracing::error!(
                            atom_id = %atom_id,
                            blob_key = %detail.content_blob_key,
                            expected_org = ?expected_org,
                            error = %e,
                            "get_atom: blob_key org-scope mismatch — refusing to fetch \
                             (never crossing to another org's/plane's object)"
                        );
                        None
                    }
                    Ok(()) => match r2.get_atom_blob(&detail.content_blob_key, &dek).await {
                        Ok(bytes) => {
                            let raw = String::from_utf8_lossy(&bytes).into_owned();
                            Some(
                                crate::store::strip_header_prefix(&raw, detail.header_len)
                                    .to_string(),
                            )
                        }
                        Err(e) => {
                            tracing::warn!(
                                atom_id = %atom_id,
                                blob_key = %detail.content_blob_key,
                                error = %e,
                                "get_atom: R2 fetch failed; returning null content"
                            );
                            None
                        }
                    },
                }
            }
            Ok(None) => {
                tracing::warn!(
                    account_id = %atom_acct,
                    "get_atom: account has no DEK; returning null content"
                );
                None
            }
            Err(e) => {
                tracing::warn!(error = %e, "get_atom: DEK resolution failed");
                None
            }
        }
    } else {
        None
    };

    // Blob I/O does not pin a database permission snapshot. Re-read the exact
    // admitted atom and its linked safe titles before emitting any payload.
    if !crate::source_evidence::still_admitted(&state, &tenant, &user, atom_graph, atom_acct)
        .await
    {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({"error":"coral not found or access denied"})),
        );
    }
    let mut tx = match tenant.begin_member_scoped(atom_acct).await {
        Ok(tx) => tx,
        Err(_) => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({"error":"service unavailable"})),
            )
        }
    };
    // Fresh permission must certify the exact fetched payload, not bytes
    // from an earlier row now rebound to a different authorized source.
    detail = match store.atom_detail_scoped(&mut tx, atom_id, atom_acct).await {
        Ok(Some(current)) if current.content_blob_key==detail.content_blob_key
            && current.content_inline==detail.content_inline && current.header_len==detail.header_len
            && current.source_type==detail.source_type && current.source_id==detail.source_id => current,
        Ok(Some(_)) => return (StatusCode::NOT_FOUND,Json(json!({"error":"coral not found or access denied"}))),
        Ok(None) => {
            return (
                StatusCode::NOT_FOUND,
                Json(json!({"error":"coral not found or access denied"})),
            )
        }
        Err(_) => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({"error":"service unavailable"})),
            )
        }
    };
    if !matches!(content_binding(&mut tx,atom_graph,atom_id).await, Ok(binding) if binding==original_binding) {
        return (StatusCode::NOT_FOUND,Json(json!({"error":"coral not found or access denied"})));
    }
    if tx.commit().await.is_err() {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error":"service unavailable"})),
        );
    }

    let linked: Vec<Value> = detail
        .linked_reefs
        .into_iter()
        .map(|r| {
            json!({
                "node_id": r.node_id,
                "type":    r.node_type,
                "title":   r.title,
            })
        })
        .collect();

    // Titles-always-present (founder requirement): `detail.title` (the bare
    // `atoms.title` column) is commonly NULL — `display_title` guarantees a
    // non-empty headline. `content` above is already header-stripped, so it
    // is reused as-is for the `stripped_body` fallback rung.
    let display_title = crate::services::atom_title::display_title(
        crate::services::atom_title::DisplayTitleInput {
            title: detail.title.as_deref(),
            excerpt: detail.excerpt.as_deref(),
            stripped_body: content.as_deref(),
            source_type: &detail.source_type,
            channel_name: detail.channel_name.as_deref(),
            author_name: detail.author_name.as_deref(),
            timestamp: detail.timestamp.unwrap_or(detail.created_at),
        },
    );

    (
        StatusCode::OK,
        Json(json!({
            "atom_id":      detail.atom_id,
            "graph_id":     detail.graph_id,
            "source_type":  detail.source_type,
            "source_id":    detail.source_id,
            "content":      content,
            "participants": detail.participants,
            "timestamp":    detail.timestamp,
            "created_at":   detail.created_at,
            "scope":        detail.scope,
            "fts_language": detail.fts_language,
            "channel_id":   detail.channel_id,
            "channel_name": detail.channel_name,
            "author_id":    detail.author_id,
            "author_name":  detail.author_name,
            "title":        display_title,
            "excerpt":      detail.excerpt,
            "linked_reefs": linked,
        })),
    )
}

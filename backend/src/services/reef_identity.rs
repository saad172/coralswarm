//! Explicit human identity interpretations over one admitted, unchanged member.
//! The source text is never rewritten; ordinary metadata edits confer no authority.

use sqlx::{Acquire, Postgres, Transaction};
use uuid::Uuid;

use crate::store::{Node, Store, StoreError};

/// Apply a human rename and its exact support atomically within the caller's
/// authenticated transaction. Actor/account are server context, never JSON fields.
#[allow(clippy::too_many_arguments)]
pub async fn patch_supported(
    store: &dyn Store,
    tx: &mut Transaction<'_, Postgres>,
    account: Uuid,
    actor: Uuid,
    node: Uuid,
    title: &str,
    node_type: Option<&str>,
    expected_version: Option<i64>,
    support: Uuid,
) -> Result<Node, StoreError> {
    if actor.is_nil()
        || title.trim().is_empty()
        || title.contains('\0')
        || title.chars().count() > crate::routes::nodes::PATCH_TITLE_CAP
        || expected_version.is_some_and(|v| v < 1)
        || node_type.is_some_and(|t| !matches!(t, "entity" | "thread" | "reference"))
    {
        return Err(StoreError::Conflict("invalid supported reef update".into()));
    }
    // A caller may handle an error and retain its outer transaction. A savepoint
    // therefore owns the complete node, authority and sync mutation, including
    // logical expected-version failures that do not abort PostgreSQL themselves.
    let mut inner = tx.begin().await?;
    let result = patch_inner(
        store,
        &mut inner,
        account,
        actor,
        node,
        title,
        node_type,
        expected_version,
        support,
    )
    .await;
    match result {
        Ok(node) => {
            inner.commit().await?;
            Ok(node)
        }
        Err(error) => {
            inner.rollback().await?;
            Err(error)
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn patch_inner(
    store: &dyn Store,
    tx: &mut Transaction<'_, Postgres>,
    account: Uuid,
    actor: Uuid,
    node: Uuid,
    title: &str,
    node_type: Option<&str>,
    expected_version: Option<i64>,
    support: Uuid,
) -> Result<Node, StoreError> {
    let scope: (Option<String>, Option<String>, Option<String>) = sqlx::query_as(
        "SELECT current_setting('app.account_id',true), current_setting('app.user_id',true), current_setting('app.tenant_actor',true)",
    ).fetch_one(&mut **tx).await?;
    // Shared reachability resolvers also exist on CONTROL. The actor RLS
    // primitive identifies the Tenant plane that requires a pre-scoped actor.
    let tenant_plane: bool = sqlx::query_scalar(
        "SELECT to_regprocedure('public.tenant_rls_actor_user_id()') IS NOT NULL",
    ).fetch_one(&mut **tx).await?;
    if (tenant_plane && scope.1.as_deref().filter(|s| !s.is_empty()).is_none())
        || scope.0.as_deref() != Some(account.to_string().as_str())
        || scope.2.as_deref() == Some("system")
        || scope
            .1
            .as_deref()
            .filter(|s| !s.is_empty())
            .is_some_and(|s| s != actor.to_string())
    {
        return Err(StoreError::Conflict(
            "identity actor scope unavailable".into(),
        ));
    }
    // Personal account transactions do not initialize app.user_id; Org already
    // carries the gate-verified actor, whose equality was checked above.
    sqlx::query("SELECT set_config('app.user_id',$1,true)")
        .bind(actor.to_string())
        .execute(&mut **tx)
        .await?;
    let graph_sql = if tenant_plane {
        "SELECT n.graph_id FROM public.nodes n JOIN public.graphs g ON g.id=n.graph_id WHERE n.node_id=$1 AND g.account_id=$2 AND COALESCE(public.resolve_tenant_graph_reachable_by_user(g.id,$2,$3),false)"
    } else {
        "SELECT n.graph_id FROM public.nodes n JOIN public.graphs g ON g.id=n.graph_id WHERE n.node_id=$1 AND g.account_id=$2 AND COALESCE(public.resolve_graph_reachable_by_user(g.id,$3),false)"
    };
    let graph: Option<Uuid> = sqlx::query_scalar(graph_sql)
        .bind(node)
        .bind(account)
        .bind(actor)
        .fetch_optional(&mut **tx)
        .await?;
    let graph = graph.ok_or(StoreError::NotFound)?;
    if !store
        .node_owned_by_account_scoped(tx, node, account)
        .await?
    {
        return Err(StoreError::NotFound);
    }
    let initial: Option<(bool, String)> = sqlx::query_as(
        "SELECT a.source_evidence_bound,a.source_id FROM public.atoms a JOIN public.node_atoms l ON l.atom_id=a.atom_id AND l.node_id=$2 WHERE a.graph_id=$1 AND a.atom_id=$3 AND public.library_atom_permitted($1,a.atom_id)",
    ).bind(graph).bind(node).bind(support).fetch_optional(&mut **tx).await?;
    let (bound, legacy_source_id) = initial.ok_or(StoreError::NotFound)?;
    let mapping: Option<(Uuid, Uuid)> = if bound {
        let candidates: Vec<(Uuid, Uuid, String)> = sqlx::query_as(
            "SELECT s.source_item_id,v.version_id,v.source_id FROM public.source_version_chunks c JOIN public.source_versions v ON v.version_id=c.source_version_id AND v.graph_id=c.graph_id JOIN public.source_items s ON s.source_item_id=v.source_item_id AND s.graph_id=v.graph_id WHERE c.graph_id=$1 AND c.atom_id=$2 AND s.access_state='permitted' AND s.current_version_id=v.version_id ORDER BY s.source_item_id,v.version_id",
        ).bind(graph).bind(support).fetch_all(&mut **tx).await?;
        if candidates.len() != 1 {
            return Err(StoreError::Conflict(
                "support needs one current source binding".into(),
            ));
        }
        let (item, version, source_id) = &candidates[0];
        crate::source_evidence::lock_identity_tx(tx, graph, source_id).await?;
        let locked: Option<Uuid> = sqlx::query_scalar(
            "SELECT v.version_id FROM public.source_items s JOIN public.source_versions v ON v.source_item_id=s.source_item_id AND v.graph_id=s.graph_id WHERE s.graph_id=$1 AND s.source_item_id=$2 AND s.current_version_id=$3 AND v.version_id=$3 AND s.access_state='permitted' FOR UPDATE OF s,v",
        ).bind(graph).bind(item).bind(version).fetch_optional(&mut **tx).await?;
        if locked != Some(*version) {
            return Err(StoreError::NotFound);
        }
        Some((*item, *version))
    } else {
        crate::source_evidence::lock_identity_tx(tx, graph, &legacy_source_id).await?;
        None
    };
    let snapshot: Option<(bool, i64, String)> = sqlx::query_as(
        "SELECT a.source_evidence_bound,a.content_revision,public.reef_identity_atom_fingerprint(a) FROM public.atoms a JOIN public.node_atoms l ON l.atom_id=a.atom_id AND l.node_id=$2 WHERE a.graph_id=$1 AND a.atom_id=$3 AND public.library_atom_permitted($1,a.atom_id) FOR UPDATE OF a FOR KEY SHARE OF l",
    ).bind(graph).bind(node).bind(support).fetch_optional(&mut **tx).await?;
    let (now_bound, revision, fingerprint) = snapshot.ok_or(StoreError::NotFound)?;
    if now_bound != bound {
        return Err(StoreError::Conflict("support binding changed".into()));
    }
    let patched = store
        .patch_node_scoped(tx, node, Some(title), node_type, expected_version)
        .await?
        .ok_or(StoreError::NotFound)?;
    // Equal-title explicit reapply is also a fresh event; inactive authority is
    // never toggled back on. Raw title changes are retired by the SQL trigger.
    sqlx::query("UPDATE public.reef_identity_corrections SET state='superseded' WHERE graph_id=$1 AND node_id=$2 AND state='active'")
        .bind(graph).bind(node).execute(&mut **tx).await?;
    sqlx::query("INSERT INTO public.reef_identity_corrections(graph_id,node_id,label,recorded_by,support_atom_id,support_kind,source_item_id,source_version_id,content_revision,input_sha256) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)")
        .bind(graph).bind(node).bind(title).bind(actor).bind(support)
        .bind(if bound { "bound" } else { "legacy" })
        .bind(mapping.map(|m| m.0)).bind(mapping.map(|m| m.1))
        .bind(revision).bind(fingerprint).execute(&mut **tx).await?;
    Ok(patched)
}

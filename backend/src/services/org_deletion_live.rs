//! Production wiring for [`super::org_deletion`]'s dependencies: the org's
//! tenant database (manifest + export) and R2 (the encrypted export).
//!
//! # The export
//!
//! One NDJSON document, every line a JSON object:
//!
//! * `{"kind":"manifest", ...}` first: org, deletion id, when, the tables
//!   included, and the re-import non-promise;
//! * `{"kind":"source","source_type":..,"source_id":..,"atoms":n}`, one per
//!   distinct captured source: the source manifest (decision 13: members'
//!   own captures in their personal accounts are NOT retracted; this list
//!   is what lets "did a copy of X exist in the deleted org" be answered
//!   later without deleting anything);
//! * `{"kind":"row","table":..,"row":{..}}` for every row of the content
//!   tables, minus derived columns (`embedding`, `content_tsv`);
//! * `{"kind":"content","atom_id":..,"text":..}` for every atom whose content
//!   lives in R2, decrypted with the org's DEK (`bytes_b64` instead of `text`
//!   when it is not UTF-8).
//!
//! The whole document is built in memory, then encrypted and uploaded in
//! one object (R2Client has no multipart surface), under a hard size cap
//! ([`OWNER_EXPORT_MAX_BYTES`] for the owner's download,
//! [`OPERATOR_EXPORT_MAX_BYTES`] for the worker's copy). Streaming is a
//! follow-up.
//!
//! # Two scopes
//!
//! The OWNER's download (`ExportScope::Owner`) carries the shared org oceans
//! and the owner's own member ocean, and the owner's own conversations, with
//! every table filtered by those graph ids explicitly in SQL. It must not
//! lean on row-level security: prod tenant pools connect as the table owner,
//! which bypasses it. The whole-tenant dump (`ExportScope::Operator`) is only
//! the worker's 30-day copy, which only an operator can read.

use std::sync::Arc;

use async_trait::async_trait;
use futures::TryStreamExt;
use base64::Engine;
use serde_json::{json, Value};
use uuid::Uuid;

use super::org_deletion::{ExportScope, ExportStore, OrgDataPlane, OrgDeletion, EXPORT_TOO_LARGE};
use crate::services::blob_store::PromotionBlobStore;
use crate::tenancy::TenantId;
use crate::AppState;

/// The tables exported, in order. A table the tenant tree does not have is
/// skipped (recorded in the manifest as absent).
pub const EXPORT_TABLES: &[&str] = &[
    "graphs",
    "atoms",
    "nodes",
    "node_atoms",
    "relationship_edge",
    "meeting_transcripts",
    "transcript_corrections",
    "conversations",
    "conversation_messages",
];

/// Derived or model-bound columns never exported.
const DROPPED_COLUMNS: &[&str] = &["embedding", "content_tsv"];

pub struct LiveDataPlane {
    pub state: Arc<AppState>,
}

impl LiveDataPlane {
    async fn tenant_pool(&self, d: &OrgDeletion) -> Result<sqlx::PgPool, String> {
        let control = self.state.control_db().ok_or("no control pool")?;
        let row = crate::tenancy::routing::lookup_by_org(control, &d.clerk_org_id)
            .await
            .map_err(|e| format!("routing lookup: {e}"))?
            .ok_or("the org has no routing row (its database is already gone)")?;
        let rt = self.state.tenant_runtime.as_ref().ok_or("no tenant runtime")?;
        rt.registry
            .get_or_build(&d.clerk_org_id, row.account_id, &row.db_secret_ref)
            .await
            .map_err(|e| format!("tenant pool: {e}"))
    }
}

/// The per-source-type counts recorded on the job row.
pub async fn manifest_counts(tenant: &sqlx::PgPool) -> Result<Value, sqlx::Error> {
    let rows: Vec<(String, i64, i64)> = sqlx::query_as(
        "SELECT source_type, count(*)::bigint, count(DISTINCT source_id)::bigint FROM atoms GROUP BY source_type ORDER BY 1",
    )
    .fetch_all(tenant)
    .await?;
    let total: i64 = rows.iter().map(|r| r.1).sum();
    let by: serde_json::Map<String, Value> = rows
        .into_iter()
        .map(|(t, atoms, sources)| (t, json!({"atoms": atoms, "sources": sources})))
        .collect();
    Ok(json!({"atoms": total, "by_source_type": by}))
}

/// Hard cap on an owner download. The document is built in memory and sent in
/// one response, so it is bounded; an org over the cap is pointed at support.
pub const OWNER_EXPORT_MAX_BYTES: usize = 64 * 1024 * 1024;
/// Hard cap on the operator's 30-day copy (built, encrypted and uploaded in
/// memory by the worker). Over it, the deletion parks with an alert instead of
/// exhausting the worker; streaming is the follow-up that lifts it.
pub const OPERATOR_EXPORT_MAX_BYTES: usize = 256 * 1024 * 1024;

fn max_bytes(scope: &ExportScope) -> usize {
    match scope {
        ExportScope::Operator => OPERATOR_EXPORT_MAX_BYTES,
        ExportScope::Owner { .. } => OWNER_EXPORT_MAX_BYTES,
    }
}

fn too_large(scope: &ExportScope, cap_bytes: usize) -> String {
    let cap = cap_bytes / (1024 * 1024);
    match scope {
        ExportScope::Owner { .. } => format!(
            "{EXPORT_TOO_LARGE}: this organization's data is larger than the {cap} MB download limit; \
             contact support and we will send you an operator export"
        ),
        ExportScope::Operator => format!("{EXPORT_TOO_LARGE}: the whole-tenant export exceeds {cap} MB; needs the streaming exporter"),
    }
}

/// The graph ids an owner export may contain, decided in SQL on the tenant
/// pool WITHOUT relying on row-level security (production tenant pools
/// connect as the table owner, which bypasses it): every shared org-ocean
/// (`kind = 'org'`) and the owner's own member ocean. Every other member's
/// member ocean, team oceans and anything unclassified are excluded
/// (fail closed).
pub async fn owner_visible_graphs(tenant: &sqlx::PgPool, owner: Uuid) -> Result<Vec<Uuid>, sqlx::Error> {
    sqlx::query_scalar(
        "SELECT id FROM graphs WHERE kind = 'org' OR (kind = 'member' AND owner_user_id = $1) ORDER BY id",
    )
    .bind(owner)
    .fetch_all(tenant)
    .await
}

/// `WHERE` fragment (with `$1` = visible graph ids, `$2` = owner) restricting
/// `table` to the owner's data; `None` for the operator's whole-tenant copy.
fn owner_filter(table: &str) -> &'static str {
    match table {
        "graphs" => "t.id = ANY($1)",
        "atoms" | "nodes" | "relationship_edge" | "meeting_transcripts" | "transcript_corrections" => {
            "t.graph_id = ANY($1)"
        }
        "node_atoms" => {
            "t.atom_id IN (SELECT a.atom_id FROM atoms a WHERE a.graph_id = ANY($1)) \
             AND t.node_id IN (SELECT n.node_id FROM nodes n WHERE n.graph_id = ANY($1))"
        }
        "conversations" => "t.user_id = $2 AND t.graph_id = ANY($1)",
        "conversation_messages" => {
            "t.conversation_id IN (SELECT c.id FROM conversations c WHERE c.user_id = $2 AND c.graph_id = ANY($1))"
        }
        // A table with no rule is never exported to an owner.
        _ => "FALSE",
    }
}

/// Build the export document from `tenant`. `blob` fetches and decrypts an
/// atom's R2 content (`None` when the org has no blob store or no DEK).
/// `scope` decides whose rows: see [`ExportScope`]. The document is capped at
/// the scope's size limit; over it, an [`EXPORT_TOO_LARGE`] error.
pub async fn build_export<F, Fut>(
    tenant: &sqlx::PgPool,
    d: &OrgDeletion,
    scope: &ExportScope,
    blob: Option<F>,
) -> Result<Vec<u8>, String>
where
    F: Fn(String) -> Fut,
    Fut: std::future::Future<Output = Result<Vec<u8>, String>>,
{
    build_export_with_cap(tenant, d, scope, blob, max_bytes(scope)).await
}

/// [`build_export`] with the size cap injected (tests use a tiny one). Rows are
/// STREAMED from Postgres and appended one at a time, and the export aborts
/// the moment the output passes `cap`, so memory is bounded by the cap plus
/// one row, never by the size of a table.
pub async fn build_export_with_cap<F, Fut>(
    tenant: &sqlx::PgPool,
    d: &OrgDeletion,
    scope: &ExportScope,
    blob: Option<F>,
    cap: usize,
) -> Result<Vec<u8>, String>
where
    F: Fn(String) -> Fut,
    Fut: std::future::Future<Output = Result<Vec<u8>, String>>,
{
    let graphs: Option<Vec<Uuid>> = match scope {
        ExportScope::Operator => None,
        ExportScope::Owner { user_id } => Some(
            owner_visible_graphs(tenant, *user_id).await.map_err(|e| format!("export graphs: {e}"))?,
        ),
    };
    let owner_id: Option<Uuid> = match scope {
        ExportScope::Operator => None,
        ExportScope::Owner { user_id } => Some(*user_id),
    };
    let mut out: Vec<u8> = Vec::new();
    let push = |v: Value, out: &mut Vec<u8>| -> Result<(), String> {
        out.extend_from_slice(v.to_string().as_bytes());
        out.push(b'\n');
        if out.len() > cap {
            return Err(too_large(scope, cap));
        }
        Ok(())
    };
    let mut present = Vec::new();
    let mut absent = Vec::new();
    for t in EXPORT_TABLES {
        let exists: bool = sqlx::query_scalar("SELECT to_regclass($1) IS NOT NULL")
            .bind(format!("public.{t}"))
            .fetch_one(tenant)
            .await
            .map_err(|e| format!("export: {e}"))?;
        if exists { present.push(*t) } else { absent.push(*t) }
    }
    push(
        json!({
            "kind": "manifest",
            "format": "coralswarm-org-export/v1",
            "org": d.clerk_org_id,
            "deletion_id": d.id,
            "exported_at": chrono::Utc::now(),
            "tables": present,
            "absent_tables": absent,
            "scope": match scope { ExportScope::Operator => "operator", ExportScope::Owner { .. } => "owner" },
            "note": "Re-import is not guaranteed and not precluded. Atom content stored in object storage is included decrypted as `content` lines.",
        }),
        &mut out,
    )?;
    if present.contains(&"atoms") {
        let mut sources = match &graphs {
            None => sqlx::query_as::<_, (String, String, i64)>(
                "SELECT source_type, source_id, count(*)::bigint FROM atoms GROUP BY 1, 2 ORDER BY 1, 2",
            )
            .fetch(tenant),
            Some(g) => sqlx::query_as::<_, (String, String, i64)>(
                "SELECT source_type, source_id, count(*)::bigint FROM atoms WHERE graph_id = ANY($1) GROUP BY 1, 2 ORDER BY 1, 2",
            )
            .bind(g.clone())
            .fetch(tenant),
        };
        while let Some((t, id, n)) = sources.try_next().await.map_err(|e| format!("export sources: {e}"))? {
            push(json!({"kind": "source", "source_type": t, "source_id": id, "atoms": n}), &mut out)?;
        }
    }
    let drop_cols = DROPPED_COLUMNS.iter().map(|c| format!(" - '{c}'")).collect::<String>();
    for t in &present {
        // Table names come from the constant list above, never from input.
        let sql = match (&graphs, owner_id) {
            (Some(_), Some(_)) => format!("SELECT to_jsonb(t){drop_cols} FROM public.{t} t WHERE {}", owner_filter(t)),
            _ => format!("SELECT to_jsonb(t){drop_cols} FROM public.{t} t"),
        };
        let mut rows = match (&graphs, owner_id) {
            (Some(g), Some(owner)) => sqlx::query_scalar::<_, Value>(&sql).bind(g.clone()).bind(owner).fetch(tenant),
            _ => sqlx::query_scalar::<_, Value>(&sql).fetch(tenant),
        };
        while let Some(r) = rows.try_next().await.map_err(|e| format!("export {t}: {e}"))? {
            push(json!({"kind": "row", "table": t, "row": r}), &mut out)?;
        }
    }
    if let (Some(blob), true) = (blob, present.contains(&"atoms")) {
        // Keyset-paged (not one big fetch, and not a cursor held open across
        // the slow blob fetches below).
        let mut after = Uuid::nil();
        loop {
            let page: Vec<(Uuid, String)> = sqlx::query_as(
                "SELECT atom_id, content_blob_key FROM atoms WHERE content_blob_key IS NOT NULL AND content_blob_key <> '' \
                  AND atom_id > $1 AND ($2::uuid[] IS NULL OR graph_id = ANY($2)) ORDER BY atom_id LIMIT 500",
            )
            .bind(after)
            .bind(graphs.as_deref())
            .fetch_all(tenant)
            .await
            .map_err(|e| format!("export blob keys: {e}"))?;
            let Some((last, _)) = page.last() else { break };
            after = *last;
            for (atom, key) in page {
                coralswarm_core::r2::assert_blob_key_org_scope(&key, Some(&d.clerk_org_id))
                    .map_err(|_| format!("atom {atom}: blob key is not scoped to this org"))?;
                let bytes = blob(key).await.map_err(|e| format!("atom {atom}: {e}"))?;
                match String::from_utf8(bytes) {
                    Ok(text) => push(json!({"kind": "content", "atom_id": atom, "text": text}), &mut out)?,
                    Err(e) => push(
                        json!({"kind": "content", "atom_id": atom,
                               "bytes_b64": base64::engine::general_purpose::STANDARD.encode(e.into_bytes())}),
                        &mut out,
                    )?,
                }
            }
        }
    }
    Ok(out)
}

#[async_trait]
impl OrgDataPlane for LiveDataPlane {
    async fn source_manifest(&self, d: &OrgDeletion) -> Result<Value, String> {
        let tenant = self.tenant_pool(d).await?;
        manifest_counts(&tenant).await.map_err(|e| format!("manifest: {e}"))
    }

    async fn export_bundle(&self, d: &OrgDeletion, scope: &ExportScope) -> Result<Vec<u8>, String> {
        let tenant = self.tenant_pool(d).await?;
        let tenant_id = TenantId::Org(d.clerk_org_id.clone());
        let dek = crate::crypto::resolve_dek_for_tenant(&tenant_id, &tenant, d.account_id)
            .await
            .map_err(|e| format!("org DEK: {e}"))?;
        let r2 = self.state.r2.clone();
        match (r2, dek) {
            (Some(r2), Some(dek)) => {
                let fetch = move |key: String| {
                    let r2 = r2.clone();
                    async move {
                        let get = r2.get_blob(&key, &dek);
                        match tokio::time::timeout(std::time::Duration::from_secs(30), get).await {
                            Ok(r) => r.map_err(|e| format!("blob: {e}")),
                            Err(_) => Err("blob fetch timed out".to_string()),
                        }
                    }
                };
                build_export(&tenant, d, scope, Some(fetch)).await
            }
            _ => {
                type F = fn(String) -> std::future::Ready<Result<Vec<u8>, String>>;
                build_export::<F, _>(&tenant, d, scope, None).await
            }
        }
    }

    async fn forget_tenant(&self, clerk_org_id: &str, account_id: Uuid) {
        if let Some(rt) = self.state.tenant_runtime.as_ref() {
            rt.registry.evict_org(clerk_org_id).await;
        }
        self.state.dek_cache.invalidate(&TenantId::Org(clerk_org_id.to_string()), account_id);
    }
}

/// Account deletion's Resend mailer, reused.
#[async_trait]
impl super::org_deletion::Mailer for crate::services::account_deletion_live::LiveMailer {
    async fn send(&self, mail: super::org_deletion::Mail) -> Result<(), String> {
        crate::services::account_deletion::Mailer::send(
            self,
            crate::services::account_deletion::Mail {
                to: mail.to,
                subject: mail.subject,
                text: mail.text,
                idempotency_key: mail.idempotency_key,
            },
        )
        .await
    }
}

/// The export in R2, under the per-export key (`PromotionBlobStore`
/// encrypts on put and decrypts on get).
pub struct R2ExportStore {
    pub blobs: Arc<dyn PromotionBlobStore>,
}

#[async_trait]
impl ExportStore for R2ExportStore {
    async fn put(&self, key: &str, plaintext: &[u8], export_key: &[u8; 32]) -> Result<(), String> {
        self.blobs.put_blob(key, plaintext, export_key).await.map_err(|e| format!("export upload: {e}"))
    }
    async fn get(&self, key: &str, export_key: &[u8; 32]) -> Result<Vec<u8>, String> {
        self.blobs.get_blob(key, export_key).await.map_err(|e| format!("export download: {e}"))
    }
    async fn delete(&self, key: &str) -> Result<(), String> {
        self.blobs.delete_blob(key).await.map_err(|e| format!("export delete: {e}"))
    }
}

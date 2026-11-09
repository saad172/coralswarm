//! Immutable source versions and their semantic evidence. All SQL accepts an
//! already scoped transaction; HTTP callers authorize the graph before opening it.
use crate::{
    middleware::auth::UserContext,
    tenancy::{extractor::Tenant, reachability::ReachabilityResolver},
    AppState,
};

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Extension, Json,
};

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};

use chrono::{DateTime, Utc};

use serde::{Deserialize, Serialize};

use serde_json::{json, Value};

use sqlx::{Postgres, Transaction};

use std::sync::Arc;

use uuid::Uuid;

use crate::source_origin::{capture_contract, SourceOrigin};

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct SourceVersion {
    pub version_id: Uuid,
    pub graph_id: Uuid,
    pub source_item_id: Uuid,
    pub source_id: String,
    pub source_type: String,
    pub provider_namespace: String,
    pub representation: String,
    pub source_metadata: Value,
    pub origin: Option<sqlx::types::Json<SourceOrigin>>,
    pub occurred_at: Option<DateTime<Utc>>,
    pub participants: Vec<String>,
    pub ingested_by: Option<Uuid>,
    pub scope: String,
    pub extraction_version: String,
    pub fingerprint_sha256: Option<String>,
    pub captured_at: DateTime<Utc>,
    pub is_legacy_baseline: bool,
    pub raw_blob_key: Option<String>,
    pub raw_inline: Option<String>,
    pub content_sha256: Option<String>,
    pub duration_ms: Option<i64>,
    pub segments: Option<Value>,
    pub extraction_status: String,
    pub extraction_error: Option<String>,
    pub next_start_char: i64,
}

pub async fn load_version_scoped(
    tx: &mut Transaction<'_, Postgres>,
    graph_id: Uuid,
    version_id: Uuid,
) -> Result<Option<SourceVersion>, sqlx::Error> {
    // The reader's team-seat history floor on the version (#1545): this is
    // the loader behind source detail raw text, transcripts and publication.
    sqlx::query_as(&format!(
        r#"
        SELECT v.*
        FROM source_versions v JOIN source_items s ON s.source_item_id=v.source_item_id AND s.graph_id=v.graph_id
        WHERE v.graph_id=$1 AND v.version_id=$2 AND s.access_state='permitted' AND {floor}
        "#,
        floor = crate::history_floor::admits_version("v")
    ))
    .bind(graph_id)
    .bind(version_id)
    .fetch_optional(&mut **tx)
    .await
}

/// Why a capture stage failed: the tracked version capture here, or the
/// chunk reconcile (`chunk_reconcile::reconcile_tx`). The object store's
/// refusal is told apart from every other failure: it is the one the same
/// capture recovers from on its own, once the store is back.
#[derive(Debug)]
pub(crate) enum CaptureFailure {
    /// The snapshot's body could not be written to the object store.
    Storage(String),
    Other(String),
}

impl From<String> for CaptureFailure {
    fn from(e: String) -> Self {
        Self::Other(e)
    }
}

impl From<&str> for CaptureFailure {
    fn from(e: &str) -> Self {
        Self::Other(e.to_owned())
    }
}

impl From<CaptureFailure> for String {
    fn from(e: CaptureFailure) -> Self {
        match e {
            CaptureFailure::Storage(m) | CaptureFailure::Other(m) => m,
        }
    }
}

/// An object-store error, classified by the one storage rule
/// ([`coralswarm_core::r2::R2Error::is_store_unavailable`]).
impl From<coralswarm_core::r2::R2Error> for CaptureFailure {
    fn from(e: coralswarm_core::r2::R2Error) -> Self {
        if e.is_store_unavailable() {
            Self::Storage(e.to_string())
        } else {
            Self::Other(e.to_string())
        }
    }
}

impl CaptureFailure {
    /// The object store refused; the same capture succeeds once it is back.
    pub(crate) fn is_storage(&self) -> bool {
        matches!(self, Self::Storage(_))
    }

    /// The same failure with `what` in front of its message.
    pub(crate) fn context(self, what: &str) -> Self {
        match self {
            Self::Storage(m) => Self::Storage(format!("{what}: {m}")),
            Self::Other(m) => Self::Other(format!("{what}: {m}")),
        }
    }
}

impl std::fmt::Display for CaptureFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Storage(m) | Self::Other(m) => f.write_str(m),
        }
    }
}

pub struct CaptureOutcome {
    pub source_item_id: Uuid,
    pub version_id: Uuid,
    pub uploaded_blob_key: Option<String>,
    pub changed: bool,
    pub meeting_projection: bool,
}

/// Metadata contains content-bearing facts only (for example title, event dates,
/// attendees). Observation times, transport cursors and request IDs belong in
/// transport diagnostics, not the immutable content fingerprint.
pub struct CaptureSource<'a> {
    pub source_type: &'a str,
    pub provider_namespace: &'a str,
    pub source_id: &'a str,
    pub normalized_text: &'a str,
    pub representation: &'a str,
    pub metadata: &'a Value,
    pub occurred_at: Option<DateTime<Utc>>,
    pub participants: &'a [String],
    pub ingested_by: Option<Uuid>,
    pub scope: &'a str,
    pub duration_ms: Option<i64>,
    pub segments: Option<&'a Value>,
}

fn fingerprint_metadata(value: &Value, key: Option<&str>) -> Value {
    match value {
        Value::Object(object) => Value::Object(
            object
                .iter()
                .filter(|(name, _)| {
                    !matches!(
                        name.as_str(),
                        "captured_at"
                            | "observed_at"
                            | "ingested_at"
                            | "imported_at"
                            | "fetched_at"
                            | "request_id"
                            | "cursor"
                            | "next_cursor"
                            | "sync_cursor"
                            | "ingested_by"
                            | "actor_id"
                    )
                })
                .map(|(name, value)| (name.clone(), fingerprint_metadata(value, Some(name))))
                .collect(),
        ),
        Value::Array(values) => {
            let mut values: Vec<Value> = values
                .iter()
                .map(|v| fingerprint_metadata(v, None))
                .collect();
            if matches!(key, Some("attendees" | "participants")) {
                values.sort_by_key(Value::to_string);
                values.dedup();
            }
            Value::Array(values)
        }
        _ => value.clone(),
    }
}

/// Source types whose `occurred_at` is an observation property rather than
/// part of what was said. GitHub derives it from the commit or the
/// `updated_at` a capture happened to read, so a capture that differs only in
/// time is not a new version (see the comparison in
/// `capture_source_version_tracked_tx`).
fn occurred_at_is_observation_time(source_type: &str) -> bool {
    source_type.starts_with("github_")
}

fn fingerprint(
    hash: &str,
    representation: &str,
    metadata: &Value,
    occurred_at: Option<DateTime<Utc>>,
    participants: &[String],
    duration_ms: Option<i64>,
    segments: Option<&Value>,
) -> String {
    // serde_json's object map has deterministic key ordering. Sort attendees as
    // well: delivery order alone is not a change in who participated.
    let mut participants = participants.to_vec();
    participants.sort();
    participants.dedup();
    crate::services::source_reingest::content_hash_hex(
        &json!({
            "text_sha256":hash,"representation":representation,"metadata":fingerprint_metadata(metadata,None),
            "occurred_at":occurred_at,"participants":participants,
            "duration_ms":duration_ms,"segments":segments,
        })
        .to_string(),
    )
}

/// Serialize mutations that share the legacy graph/native-ID boundary. The
/// lock deliberately excludes source type and provider namespace: retrieval
/// chunks and meeting projections do not yet carry that full identity.
pub async fn lock_identity_tx(
    tx: &mut Transaction<'_, Postgres>,
    graph_id: Uuid,
    source_id: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "SELECT pg_advisory_xact_lock(hashtext('source_capture:' || $1::text || ':' || $2))",
    )
    .bind(graph_id.to_string())
    .bind(source_id)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

/// A projection created before source identity existed carries no proof of its
/// provider. Preserve it under the explicit unknown legacy identity before a
/// new provider observation can use the same native ID. Existing bound items
/// are never reassigned on the strength of that ID alone.
async fn preserve_unbound_meeting_tx(
    tx: &mut Transaction<'_, Postgres>,
    graph_id: Uuid,
    source_id: &str,
) -> Result<(), sqlx::Error> {
    let unbound: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM meeting_transcripts WHERE graph_id=$1 AND source_id=$2 AND source_item_id IS NULL)",
    )
    .bind(graph_id)
    .bind(source_id)
    .fetch_one(&mut **tx)
    .await?;
    if !unbound {
        return Ok(());
    }
    // Match the deletion/extraction order: source item, then projection/version.
    let legacy_item: Uuid = sqlx::query_scalar(
        "INSERT INTO source_items(graph_id,source_type,provider_namespace,source_id) VALUES($1,'transcript','legacy:meeting',$2) ON CONFLICT(graph_id,source_type,provider_namespace,source_id) DO UPDATE SET source_id=EXCLUDED.source_id RETURNING source_item_id",
    )
    .bind(graph_id)
    .bind(source_id)
    .fetch_one(&mut **tx)
    .await?;
    type UnboundProjection = (Option<String>, Option<String>, Option<i64>, Option<Value>);
    let projection: Option<UnboundProjection> = sqlx::query_as(
        "SELECT raw_blob_key,raw_inline,duration_ms,segments FROM meeting_transcripts WHERE graph_id=$1 AND source_id=$2 AND source_item_id IS NULL FOR UPDATE",
    )
    .bind(graph_id)
    .bind(source_id)
    .fetch_optional(&mut **tx)
    .await?;
    let Some((blob, inline, duration, segments)) = projection else {
        return Ok(());
    };
    let version = if blob.is_some() || inline.is_some() {
        let version: Uuid = sqlx::query_scalar(
            "INSERT INTO source_versions(graph_id,source_item_id,source_id,source_type,provider_namespace,raw_blob_key,raw_inline,duration_ms,segments,is_legacy_baseline,extraction_version) VALUES($1,$2,$3,'transcript','legacy:meeting',$4,$5,$6,$7,true,'meeting-atoms-v1') RETURNING version_id",
        )
        .bind(graph_id).bind(legacy_item).bind(source_id).bind(blob).bind(inline)
        .bind(duration).bind(segments).fetch_one(&mut **tx).await?;
        sqlx::query("UPDATE source_items SET current_version_id=$2 WHERE source_item_id=$1")
            .bind(legacy_item)
            .bind(version)
            .execute(&mut **tx)
            .await?;
        Some(version)
    } else {
        sqlx::query_scalar("SELECT current_version_id FROM source_items WHERE source_item_id=$1")
            .bind(legacy_item)
            .fetch_one(&mut **tx)
            .await?
    };
    sqlx::query("UPDATE meeting_transcripts SET source_item_id=$3,current_version_id=$4 WHERE graph_id=$1 AND source_id=$2")
        .bind(graph_id).bind(source_id).bind(legacy_item).bind(version).execute(&mut **tx).await?;
    Ok(())
}

/// Capture an admitted source in its caller's authorized transaction. Latest
/// snapshot comparison makes retries idempotent without collapsing A→B→A.
/// SQL and extraction enqueue are atomic; the caller owns final commit handling.
#[allow(clippy::too_many_arguments)]
pub async fn capture_source_version_tx(
    tx: &mut Transaction<'_, Postgres>,
    r2: Option<&coralswarm_core::r2::R2Client>,
    graph_id: Uuid,
    account_id: Uuid,
    capture: &CaptureSource<'_>,
    org_id: Option<&str>,
    dek: &[u8; 32],
) -> Result<CaptureOutcome, String> {
    capture_source_version_with_origin_tx(tx, r2, graph_id, account_id, capture, org_id, dek, None)
        .await
}

/// Adds observed attribution without changing the content fingerprint, source
/// identity, or earlier versions. None leaves existing item attribution intact.
#[allow(clippy::too_many_arguments)]
pub async fn capture_source_version_with_origin_tx(
    tx: &mut Transaction<'_, Postgres>,
    r2: Option<&coralswarm_core::r2::R2Client>,
    graph_id: Uuid,
    account_id: Uuid,
    capture: &CaptureSource<'_>,
    org_id: Option<&str>,
    dek: &[u8; 32],
    origin: Option<&SourceOrigin>,
) -> Result<CaptureOutcome, String> {
    let mut uploads = Vec::new();
    let result=capture_source_version_tracked_tx(tx,r2,graph_id,account_id,capture,org_id,dek,origin,&mut uploads).await;
    if result.is_err() { crate::services::blob_store::cleanup_uncommitted_uploads(r2,&uploads).await; }
    result.map_err(String::from)
}

/// Native capture retains successful new PUT identities even when a later SQL
/// operation fails. The caller releases the transaction before durable cleanup.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn capture_source_version_tracked_tx(
    tx: &mut Transaction<'_, Postgres>,
    r2: Option<&coralswarm_core::r2::R2Client>,
    graph_id: Uuid, account_id: Uuid, capture: &CaptureSource<'_>,
    org_id: Option<&str>, dek: &[u8;32], origin: Option<&SourceOrigin>,
    uploads: &mut Vec<String>,
) -> Result<CaptureOutcome,CaptureFailure> {
    capture_pinned_tx(tx, r2, graph_id, account_id, capture, org_id, dek, origin, uploads,
        crate::workers::source_atoms::active_source_profile()).await
}

/// [`capture_source_version_tx`] with the profile a new pending version is
/// pinned to given explicitly: a managed run's baseline promotion pins its
/// run's profile. Every other capture pins the active source profile
/// (`SOURCE_EXTRACTION_PROFILE`), never the retired `source-atoms-v1`. A
/// byte-identical capture that carries extraction forward keeps the profile
/// it was extracted under.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn capture_source_version_pinned_tx(
    tx: &mut Transaction<'_, Postgres>,
    r2: Option<&coralswarm_core::r2::R2Client>,
    graph_id: Uuid,
    account_id: Uuid,
    capture: &CaptureSource<'_>,
    org_id: Option<&str>,
    dek: &[u8; 32],
    pin: &str,
) -> Result<CaptureOutcome, String> {
    let mut uploads = Vec::new();
    let result = capture_pinned_tx(tx, r2, graph_id, account_id, capture, org_id, dek, None, &mut uploads, pin).await;
    if result.is_err() { crate::services::blob_store::cleanup_uncommitted_uploads(r2, &uploads).await; }
    result.map_err(String::from)
}

/// Test support: a capture pinned to the retired `source-atoms-v1`, the
/// state every version captured before v1 stopped being minted is in. No
/// production path pins v1.
#[cfg(any(test, feature = "test-support"))]
pub async fn capture_historical_v1_tx(
    tx: &mut Transaction<'_, Postgres>,
    graph_id: Uuid,
    account_id: Uuid,
    capture: &CaptureSource<'_>,
    dek: &[u8; 32],
) -> Result<CaptureOutcome, String> {
    capture_source_version_pinned_tx(tx, None, graph_id, account_id, capture, None, dek,
        crate::workers::source_atoms::EXTRACTION_VERSION).await
}

#[allow(clippy::too_many_arguments)]
async fn capture_pinned_tx(
    tx: &mut Transaction<'_, Postgres>,
    r2: Option<&coralswarm_core::r2::R2Client>,
    graph_id: Uuid, account_id: Uuid, capture: &CaptureSource<'_>,
    org_id: Option<&str>, dek: &[u8;32], origin: Option<&SourceOrigin>,
    uploads: &mut Vec<String>,
    pin: &str,
) -> Result<CaptureOutcome,CaptureFailure> {
    let captured_context = crate::capture_context::from_metadata(capture.metadata)?;
    if let Some(context) = &captured_context {
        crate::capture_context::validate_binding_tx(tx, graph_id, context).await?;
    }
    // Old readers return source_metadata verbatim. Lineage references stay in
    // the authoritative column; only the permission-aware history projection
    // may disclose them. The fingerprint still covers the full raw snapshot.
    let mut fingerprint_metadata = capture.metadata.clone();
    if let Some(context) = &captured_context {
        fingerprint_metadata["capture_context"] = serde_json::to_value(context).map_err(|e|e.to_string())?;
    }
    let mut stored_metadata = fingerprint_metadata.clone();
    if let Some(object) = stored_metadata.get_mut("capture_context").and_then(Value::as_object_mut) {
        object.remove("parent_session_id");
        object.remove("root_session_id");
    }
    if let Some(origin) = origin {
        origin.validate()?;
    }
    if capture.source_type.is_empty()
        || capture.source_type.chars().count() > 100
        || capture.provider_namespace.is_empty()
        || capture.provider_namespace.chars().count() > 512
        || capture.source_id.is_empty()
        || !matches!(
            capture.representation,
            "original_text" | "normalized_text" | "provider_summary" | "generated_summary"
        )
    {
        return Err("invalid source identity or representation".into());
    }
    let meeting = matches!(
        coralswarm_core::chunker::ChunkStrategy::for_source_type(capture.source_type),
        coralswarm_core::chunker::ChunkStrategy::Transcript
    ) && matches!(capture.representation, "original_text" | "normalized_text");
    lock_identity_tx(tx, graph_id, capture.source_id)
        .await
        .map_err(|e| e.to_string())?;
    if meeting {
        preserve_unbound_meeting_tx(tx, graph_id, capture.source_id)
            .await
            .map_err(|e| e.to_string())?;
    }

    let source_item_id: Option<Uuid> = sqlx::query_scalar(
        "SELECT source_item_id FROM source_items WHERE graph_id=$1 AND source_type=$2 AND provider_namespace=$3 AND source_id=$4 FOR UPDATE")
        .bind(graph_id).bind(capture.source_type).bind(capture.provider_namespace).bind(capture.source_id)
        .fetch_optional(&mut **tx).await.map_err(|e|e.to_string())?;
    let item = if let Some(id) = source_item_id {
        id
    } else {
        sqlx::query_scalar("INSERT INTO source_items(graph_id,source_type,provider_namespace,source_id) VALUES($1,$2,$3,$4) ON CONFLICT(graph_id,source_type,provider_namespace,source_id) DO UPDATE SET source_id=EXCLUDED.source_id RETURNING source_item_id")
            .bind(graph_id).bind(capture.source_type).bind(capture.provider_namespace).bind(capture.source_id)
            .fetch_one(&mut **tx).await.map_err(|e|e.to_string())?
    };
    if let Some(origin) = origin {
        sqlx::query("UPDATE source_items SET origin=$3,updated_at=now() WHERE source_item_id=$1 AND graph_id=$2 AND origin IS DISTINCT FROM $3")
            .bind(item).bind(graph_id).bind(sqlx::types::Json(origin))
            .execute(&mut **tx).await.map_err(|e|e.to_string())?;
    }
    let mut current_id: Option<Uuid> = sqlx::query_scalar("SELECT current_version_id FROM source_items WHERE source_item_id=$1 AND graph_id=$2 FOR UPDATE")
        .bind(item).bind(graph_id).fetch_one(&mut **tx).await.map_err(|e|e.to_string())?;
    let mut meeting_projection = false;
    if meeting {
        sqlx::query("INSERT INTO meeting_transcripts(graph_id,source_id,source_item_id) VALUES($1,$2,$3) ON CONFLICT(graph_id,source_id) DO NOTHING")
            .bind(graph_id).bind(capture.source_id).bind(item).execute(&mut **tx).await.map_err(|e|e.to_string())?;
        type Projection = (
            Option<Uuid>,
            Option<String>,
            Option<String>,
            Option<i64>,
            Option<Value>,
        );
        let projection:Projection=sqlx::query_as("SELECT source_item_id,raw_blob_key,raw_inline,duration_ms,segments FROM meeting_transcripts WHERE graph_id=$1 AND source_id=$2 FOR UPDATE")
            .bind(graph_id).bind(capture.source_id).fetch_one(&mut **tx).await.map_err(|e|e.to_string())?;
        meeting_projection = projection.0 == Some(item);
        if meeting_projection {
            sqlx::query("UPDATE meeting_transcripts SET source_item_id=$3 WHERE graph_id=$1 AND source_id=$2")
                .bind(graph_id).bind(capture.source_id).bind(item).execute(&mut **tx).await.map_err(|e|e.to_string())?;
            if current_id.is_none() && (projection.1.is_some() || projection.2.is_some()) {
                let baseline:Uuid=sqlx::query_scalar("INSERT INTO source_versions(graph_id,source_item_id,source_id,source_type,provider_namespace,raw_blob_key,raw_inline,duration_ms,segments,is_legacy_baseline,extraction_version) VALUES($1,$2,$3,$8,$9,$4,$5,$6,$7,true,'meeting-atoms-v1') RETURNING version_id")
                    .bind(graph_id).bind(item).bind(capture.source_id).bind(&projection.1).bind(&projection.2).bind(projection.3).bind(&projection.4).bind(capture.source_type).bind(capture.provider_namespace)
                    .fetch_one(&mut **tx).await.map_err(|e|e.to_string())?;
                current_id = Some(baseline);
                sqlx::query(
                    "UPDATE source_items SET current_version_id=$2 WHERE source_item_id=$1",
                )
                .bind(item)
                .bind(baseline)
                .execute(&mut **tx)
                .await
                .map_err(|e| e.to_string())?;
                sqlx::query("UPDATE meeting_transcripts SET current_version_id=$3 WHERE graph_id=$1 AND source_id=$2")
                    .bind(graph_id).bind(capture.source_id).bind(baseline).execute(&mut **tx).await.map_err(|e|e.to_string())?;
            }
        }
    }
    let hash = crate::services::source_reingest::content_hash_hex(capture.normalized_text);
    let snapshot = fingerprint(
        &hash,
        capture.representation,
        &fingerprint_metadata,
        capture.occurred_at,
        capture.participants,
        capture.duration_ms,
        capture.segments,
    );
    let mut carried: Option<(String, i64)> = None;
    if let Some(id) = current_id {
        // A trusted exact-item refetch must compare its retained snapshot even
        // when provider access was previously unknown/restricted. The item is
        // already locked inside the caller's authorized capture transaction;
        // public evidence loading remains permission-gated, and capture never
        // restores access by itself.
        let previous = sqlx::query_as::<_, SourceVersion>(
            "SELECT * FROM source_versions WHERE graph_id=$1 AND source_item_id=$2 AND version_id=$3",
        ).bind(graph_id).bind(item).bind(id).fetch_optional(&mut **tx)
            .await
            .map_err(|e| e.to_string())?
            .ok_or("missing current source version")?;
        let previous_fingerprint = if let Some(value) = previous.fingerprint_sha256.clone() {
            Some(value)
        } else {
            let old_hash = if let Some(hash) = previous.content_sha256.clone() {
                Some(hash)
            } else if let Some(raw) = previous.raw_inline.as_ref() {
                Some(crate::services::source_reingest::content_hash_hex(raw))
            } else if let (Some(r2), Some(key)) = (r2, previous.raw_blob_key.as_ref().filter(|key| artifact_locator_admitted(graph_id,key,org_id))) {
                r2.get_atom_blob(key, dek)
                    .await
                    .ok()
                    .and_then(|v| String::from_utf8(v).ok())
                    .map(|v| crate::services::source_reingest::content_hash_hex(&v))
            } else {
                None
            };
            old_hash.map(|hash| {
                fingerprint(
                    &hash,
                    &previous.representation,
                    &previous.source_metadata,
                    previous.occurred_at,
                    &previous.participants,
                    previous.duration_ms,
                    previous.segments.as_ref(),
                )
            })
        };
        // For a GitHub item, `occurred_at` is derived from the snapshot we
        // happened to take (which head commit, which `updated_at`), not from
        // the bytes: a sweep that re-reads an unchanged file under a newer
        // head would otherwise mint a version per sweep (2026-10-18). Such a
        // capture is the current version; its first observed time stands.
        // Compared against the previous fingerprint recomputed with the
        // previous time, so stored fingerprints stay valid (no backfill).
        // Other sources keep the time in the snapshot: a rescheduled meeting
        // or event can change only its time and that is a real new version.
        let time_only_change = || {
            occurred_at_is_observation_time(capture.source_type)
                && previous.occurred_at != capture.occurred_at
                && previous_fingerprint.as_deref()
                    == Some(
                        fingerprint(
                            &hash,
                            capture.representation,
                            &fingerprint_metadata,
                            previous.occurred_at,
                            capture.participants,
                            capture.duration_ms,
                            capture.segments,
                        )
                        .as_str(),
                    )
        };
        if previous_fingerprint.as_deref() == Some(snapshot.as_str()) || time_only_change() {
            // A failed version is retried only by an explicit retry or a
            // managed run; re-syncing unchanged content must not respend it.
            if matches!(previous.extraction_status.as_str(),"not_started"|"blocked") {
                queue_extraction_tx(tx, graph_id, id, account_id, previous.next_start_char)
                    .await
                    .map_err(|e| e.to_string())?;
            }
            return Ok(CaptureOutcome {
                source_item_id: item,
                version_id: id,
                uploaded_blob_key: None,
                changed: false,
                meeting_projection,
            });
        }
        if carries_extraction(&previous, &hash, capture.representation) {
            carried = Some((previous.extraction_version.clone(), previous.next_start_char));
        }
    }
    let version = Uuid::new_v4();
    let blob = if let Some(r2) = r2 {
        Some(
            r2.put_atom_blob(
                org_id,
                graph_id,
                version,
                capture.normalized_text.as_bytes(),
                dek,
            )
            .await
            .map_err(CaptureFailure::from)?,
        )
    } else {
        None
    };
    if let Some(key)=&blob { uploads.push(key.clone()); }
    let result:Result<(),sqlx::Error>=async {
        let inline=blob.is_none().then_some(capture.normalized_text);
        let (profile, status, cursor) = match &carried {
            Some((profile, cursor)) => (profile.as_str(), "done", *cursor),
            None => (pin, "pending", 0),
        };
        sqlx::query("INSERT INTO source_versions(version_id,graph_id,source_item_id,source_id,source_type,provider_namespace,raw_blob_key,raw_inline,content_sha256,fingerprint_sha256,representation,source_metadata,occurred_at,participants,ingested_by,scope,duration_ms,segments,origin,capture_context,extraction_version,extraction_status,next_start_char) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23)")
            .bind(version).bind(graph_id).bind(item).bind(capture.source_id).bind(capture.source_type).bind(capture.provider_namespace).bind(&blob).bind(inline).bind(&hash).bind(&snapshot)
            .bind(capture.representation).bind(&stored_metadata).bind(capture.occurred_at).bind(capture.participants).bind(capture.ingested_by).bind(capture.scope).bind(capture.duration_ms).bind(capture.segments).bind(origin.map(sqlx::types::Json)).bind(captured_context.as_ref().map(sqlx::types::Json))
            .bind(profile).bind(status).bind(cursor)
            .execute(&mut **tx).await?;
        sqlx::query("UPDATE source_items SET current_version_id=$2,updated_at=now() WHERE source_item_id=$1")
            .bind(item).bind(version).execute(&mut **tx).await?;
        if meeting_projection {
            sqlx::query("UPDATE meeting_transcripts SET current_version_id=$3,raw_blob_key=$4,raw_inline=$5,duration_ms=$6,segments=$7,summary=NULL,summary_status='pending',summary_error=NULL,summary_model=NULL,summary_updated_at=NULL WHERE graph_id=$1 AND source_id=$2 AND source_item_id=$8")
                .bind(graph_id).bind(capture.source_id).bind(version).bind(&blob).bind(inline).bind(capture.duration_ms).bind(capture.segments).bind(item).execute(&mut **tx).await?;
        }
        if carried.is_none() {
            queue_extraction_tx(tx,graph_id,version,account_id,0).await?;
        } else {
            crate::corpus_enrichment::record_carried_receipt_tx(tx,graph_id,version,account_id).await?;
        }
        Ok(())
    }.await;
    if let Err(error) = result {
        return Err(error.to_string().into());
    }
    Ok(CaptureOutcome {
        source_item_id: item,
        version_id: version,
        uploaded_blob_key: blob,
        changed: true,
        meeting_projection,
    })
}

/// A new version of byte-identical content in the same representation as an
/// extracted current version is a metadata-only change (an anchor key, a
/// title). It is recorded as `done` under the previous profile and is never
/// queued, so it costs no extraction. Its facts are the ones already
/// extracted from the same bytes, which readers serve through
/// `library_read::SERVED_VERSIONS_SQL`. Re-extraction under a newer profile
/// stays with the held re-extraction gate.
fn carries_extraction(previous: &SourceVersion, hash: &str, representation: &str) -> bool {
    previous.extraction_status == "done"
        && previous.content_sha256.as_deref() == Some(hash)
        && previous.representation == representation
        && !previous.is_legacy_baseline
}

/// The caller passes newly inserted or content-verified IDs only. Positional
/// legacy duplicates must not be assigned to an unrelated new source snapshot.
pub async fn link_verified_chunks_tx(
    tx: &mut Transaction<'_, Postgres>,
    graph_id: Uuid,
    version_id: Uuid,
    chunk_ids: &[Uuid],
) -> Result<(), sqlx::Error> {
    sqlx::query("INSERT INTO source_version_chunks(graph_id,source_version_id,atom_id) SELECT a.graph_id,v.version_id,a.atom_id FROM atoms a JOIN source_versions v ON v.graph_id=a.graph_id WHERE a.graph_id=$1 AND v.version_id=$2 AND a.atom_id=ANY($3) ON CONFLICT DO NOTHING")
        .bind(graph_id).bind(version_id).bind(chunk_ids).execute(&mut **tx).await?;
    Ok(())
}

pub async fn queue_extraction_tx(
    tx: &mut Transaction<'_, Postgres>,
    graph_id: Uuid,
    version_id: Uuid,
    account_id: Uuid,
    start_char: i64,
) -> Result<(), sqlx::Error> {
    if !crate::corpus_enrichment::enqueue_admission_tx(tx,graph_id,version_id,account_id).await? { return Ok(()); }
    let extraction_version: String = sqlx::query_scalar(
        "SELECT extraction_version FROM source_versions WHERE version_id=$1 AND graph_id=$2",
    )
    .bind(version_id)
    .bind(graph_id)
    .fetch_one(&mut **tx)
    .await?;
    let (kind, prefix) = if extraction_version == "meeting-atoms-v1" {
        (
            crate::jobs::JobKind::ExtractMeetingAtoms {
                graph_id,
                source_version_id: version_id,
                start_char,
            },
            "meeting-atoms",
        )
    } else {
        (
            crate::jobs::JobKind::ExtractSourceAtoms {
                graph_id,
                source_version_id: version_id,
                start_char,
            },
            "source-atoms",
        )
    };
    crate::jobs::queue::enqueue_tx(
        tx,
        &kind,
        Some(account_id),
        Some(&format!("{prefix}:{version_id}:{start_char}")),
        0,
    )
    .await?;

    sqlx::query(
        r#"
        UPDATE source_versions
        SET extraction_status='pending',extraction_error=NULL
        WHERE version_id=$1 AND graph_id=$2
        "#,
    )
    .bind(version_id)
    .bind(graph_id)
    .execute(&mut **tx)
    .await?;

    Ok(())
}

/// Queue state wins while a window is pending/running. A process can die before
/// recording a version failure; a dead-lettered current window remains retryable.
pub(crate) async fn extraction_state(
    tx: &mut Transaction<'_, Postgres>,
    row: &SourceVersion,
) -> Result<(String, Option<String>), sqlx::Error> {
    if row.extraction_status == "done" {
        return Ok(("done".into(), None));
    }

    let job:Option<String>=sqlx::query_scalar(r#"
        SELECT status::text
        FROM jobs
        WHERE kind IN ('ExtractMeetingAtoms','ExtractSourceAtoms') AND payload->>'source_version_id'=$1 AND payload->>'graph_id'=$2 AND COALESCE(payload->>'start_char','0')=$3
        ORDER BY created_at DESC,id DESC
        LIMIT 1
        "#)
 .bind(row.version_id.to_string()).bind(row.graph_id.to_string()).bind(row.next_start_char.to_string()).fetch_optional(&mut **tx).await?;

    Ok(match job.as_deref() {
        Some("pending") => ("pending".into(), None),
        Some("running") => ("running".into(), None),
        Some("dead") => (
            "failed".into(),
            Some("Processing stopped. Retry to continue from the last completed passage.".into()),
        ),
        _ => (row.extraction_status.clone(), row.extraction_error.clone()),
    })
}

#[derive(Deserialize)]
pub struct EvidenceQuery {
    pub graph_id: Uuid,
    pub source_item_id: Uuid,
    pub version_id: Option<Uuid>,
    pub before: Option<String>,
    pub atoms_after: Option<String>,
    pub atom_id: Option<Uuid>,
}

#[derive(sqlx::FromRow)]
struct VersionSummary {
    version_id: Uuid,
    captured_at: DateTime<Utc>,
    is_legacy_baseline: bool,
}

#[derive(Serialize, Deserialize)]
struct VersionCursor {
    graph_id: Uuid,
    source_item_id: Uuid,
    captured_at: DateTime<Utc>,
    version_id: Uuid,
}

#[derive(Serialize, Deserialize)]
struct AtomCursor {
    graph_id: Uuid,
    version_id: Uuid,
    ordinal: i32,
}

fn fail(status: StatusCode, message: &str) -> (StatusCode, Json<Value>) {
    (
        status,
        Json(json!({
        "error":message}
        )),
    )
}

pub(crate) async fn authorize(
    tenant: &Tenant,
    user: &UserContext,
    graph: Uuid,
) -> Result<Uuid, (StatusCode, Json<Value>)> {
    let store = tenant
        .store
        .as_ref()
        .ok_or_else(|| fail(StatusCode::SERVICE_UNAVAILABLE, "service unavailable"))?;

    match ReachabilityResolver::graph_reachable_by_user(
        tenant.decision.as_ref(),
        store.as_ref(),
        graph,
        user.user_id,
    )
    .await
    {
        Ok(true) => {}

        Ok(false) => {
            return Err(fail(
                StatusCode::NOT_FOUND,
                "source not found or access denied",
            ))
        }

        Err(_) => return Err(fail(StatusCode::INTERNAL_SERVER_ERROR, "internal error")),
    }

    store
        .graph_account_id(graph)
        .await
        .map_err(|_| fail(StatusCode::INTERNAL_SERVER_ERROR, "internal error"))?
        .ok_or_else(|| fail(StatusCode::NOT_FOUND, "source not found or access denied"))
}

/// Fresh request admission for content reads after asynchronous blob work.
/// Membership lives on CONTROL; private/team graph reachability lives on the
/// selected content plane. Missing admission data fails closed.
pub async fn still_admitted(
    state: &AppState,
    tenant: &Tenant,
    user: &UserContext,
    graph: Uuid,
    account: Uuid,
) -> bool {
    still_admitted_all(state, tenant, user, &[graph], account).await
}

/// [`still_admitted`] for every graph of one scope: graph reachability is
/// read per graph on the content plane, the CONTROL membership once. A
/// scoped Ask fence used to run the membership read once per graph, three
/// times for a two-ocean scope.
pub async fn still_admitted_all(
    state: &AppState,
    tenant: &Tenant,
    user: &UserContext,
    graphs: &[Uuid],
    account: Uuid,
) -> bool {
    let is_org = matches!(tenant.decision.as_ref(),Some(crate::tenancy::runtime::TenancyDecision::Org(_)));
    let control=state.control_db().or_else(||(!is_org).then_some(tenant.pool.as_ref()).flatten());
    still_admitted_all_with_control_inner(control,tenant,user,graphs,account,None,None).await
}

/// [`still_admitted`], for a READ handler that may let this request's own
/// authentication read stand in for the CONTROL membership/revocation
/// re-check (#1496).
///
/// Everything else is unchanged and still runs: graph reachability and graph
/// ownership on the content plane, the plane/actor consistency checks, and
/// the "no control pool means refuse" rule. Only the control transaction is
/// skipped, and only when `door` covers (`DoorAdmission::covers`) exactly the `(account, user, org)`
/// that transaction would have asked about and is younger than
/// [`DoorAdmission::MAX_AGE`]; otherwise this is byte-for-byte
/// [`still_admitted`].
///
/// Opt-in by design. A fence that guards a write, a promotion, billing or
/// managed admission, or that runs after long asynchronous work it is meant
/// to re-check across, must keep calling [`still_admitted`] /
/// [`still_admitted_in_tx`].
///
/// [`DoorAdmission::MAX_AGE`]: crate::middleware::auth::DoorAdmission::MAX_AGE
pub async fn still_admitted_at_door(
    state: &AppState,
    tenant: &Tenant,
    user: &UserContext,
    graph: Uuid,
    account: Uuid,
    door: Option<&crate::middleware::auth::DoorAdmission>,
) -> bool {
    let is_org = matches!(tenant.decision.as_ref(),Some(crate::tenancy::runtime::TenancyDecision::Org(_)));
    let control=state.control_db().or_else(||(!is_org).then_some(tenant.pool.as_ref()).flatten());
    still_admitted_all_with_control_inner(control,tenant,user,&[graph],account,None,door).await
}

/// Pool-only form of the same central admission fence for bounded background
/// readers that intentionally do not retain the entire application state.
pub(crate) async fn still_admitted_all_with_control(
    control:Option<&sqlx::PgPool>,tenant:&Tenant,user:&UserContext,graphs:&[Uuid],account:Uuid,
)->bool {
    still_admitted_all_with_control_inner(control,tenant,user,graphs,account,Some(std::time::Duration::from_millis(250)),None).await
}

async fn still_admitted_all_with_control_inner(
    control:Option<&sqlx::PgPool>,tenant:&Tenant,user:&UserContext,graphs:&[Uuid],account:Uuid,control_budget:Option<std::time::Duration>,
    door:Option<&crate::middleware::auth::DoorAdmission>,
)->bool {
    for graph in graphs {
        if !matches!(authorize(tenant,user,*graph).await,Ok(current) if current==account){return false;}
    }
    let Some(control)=control else{return false;};
    let org = match tenant.decision.as_ref() {
        Some(crate::tenancy::runtime::TenancyDecision::Org(ctx)) => {
            if ctx.account_id != user.account_id || ctx.user_id != user.user_id {
                return false;
            }
            Some(ctx.org_id.as_str())
        }
        _ => None,
    };
    // #1496: this request's door read already answered exactly the question
    // `control_actor_admitted_tx` would ask, for this `(account, user, org)`,
    // moments ago. Checked AFTER graph reachability, the actor/plane checks
    // and the control-pool presence rule above, so it can only ever replace
    // the control round trip, never one of those refusals.
    if door.is_some_and(|d|d.covers(user,org)) {return true;}
    let admitted = async {
        let mut tx = if let Some(budget)=control_budget {
            match tokio::time::timeout(budget,crate::tenancy::scope::begin_account_scoped(control,user.account_id)).await {
                Ok(Ok(tx))=>tx,_=>return Ok(false),
            }
        } else {crate::tenancy::scope::begin_account_scoped(control,user.account_id).await?};
        if let Some(budget)=control_budget {
            sqlx::query("SELECT set_config('statement_timeout',$1,true),set_config('lock_timeout','100ms',true)")
                .bind(format!("{}ms",budget.as_millis())).execute(&mut *tx).await?;
        }
        let allowed=control_actor_admitted_tx(&mut tx,user,org).await?;
        tx.commit().await?;
        Ok::<_, crate::store::StoreError>(allowed)
    }
    .await;
    matches!(admitted, Ok(true))
}

async fn control_actor_admitted_tx(tx:&mut Transaction<'_,Postgres>,user:&UserContext,org:Option<&str>)->Result<bool,sqlx::Error> {
 control_actor_admitted_tx_inner(tx,user,org,true).await
}

/// `enforce_deletion = false` is the CAPTURE writer's fence: the same
/// membership + revocation predicate WITHOUT the in-app org-deletion refusal,
/// because capture continues through the grace period so an owner's undo loses
/// nothing (`docs/plans/org-offboarding-design.md`). Every reader keeps `true`.
async fn control_actor_admitted_tx_inner(tx:&mut Transaction<'_,Postgres>,user:&UserContext,org:Option<&str>,enforce_deletion:bool)->Result<bool,sqlx::Error> {
 sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM account_members m JOIN accounts a ON a.id=m.account_id WHERE m.account_id=$1 AND m.user_id=$2 AND ($3::text IS NULL OR a.clerk_org_id=$3) AND NOT EXISTS(SELECT 1 FROM org_revocations WHERE clerk_org_id=$3) AND (NOT $4 OR COALESCE(org_deletion_state_for_org($3),'') NOT IN ('requested','grace','awaiting_approval','destroying')))").bind(user.account_id).bind(user.user_id).bind(org).bind(enforce_deletion).fetch_one(&mut **tx).await
}

/// Fresh admission while a trusted writer owns its scoped READ COMMITTED
/// transaction. Never reacquire the same consumer pool while holding session/
/// source/run locks: concurrent captures can otherwise exhaust their own pool.
/// Membership query is the exact shared reader fence, not caller attestation.
pub async fn still_admitted_in_tx(state:&AppState,tenant:&Tenant,user:&UserContext,graph:Uuid,account:Uuid,tx:&mut Transaction<'_,Postgres>)->bool {
 still_admitted_in_tx_inner(state,tenant,user,graph,account,tx,true).await
}

/// [`still_admitted_in_tx`] for the capture INGEST writer only: identical,
/// except that an org with a live in-app deletion still admits its members'
/// capture (grace keeps capturing so an undo loses nothing). Revocation, which
/// the destroy step writes first, still refuses it. Interactive requests never
/// reach ingest through this relaxation: the auth door already refused them.
pub async fn still_admitted_in_tx_for_capture(state:&AppState,tenant:&Tenant,user:&UserContext,graph:Uuid,account:Uuid,tx:&mut Transaction<'_,Postgres>)->bool {
 still_admitted_in_tx_inner(state,tenant,user,graph,account,tx,false).await
}

async fn still_admitted_in_tx_inner(state:&AppState,tenant:&Tenant,user:&UserContext,graph:Uuid,account:Uuid,tx:&mut Transaction<'_,Postgres>,enforce_deletion:bool)->bool {
 let org=match tenant.decision.as_ref() {
  Some(crate::tenancy::runtime::TenancyDecision::Org(ctx))=>{
   if ctx.account_id!=user.account_id || ctx.user_id!=user.user_id || ctx.account_id!=account {return false;}
   Some(ctx.org_id.as_str())
  }, _=>None,
 };
 let graph_sql=if org.is_some() {
  "SELECT EXISTS(SELECT 1 FROM graphs WHERE id=$1 AND account_id=$2 AND resolve_tenant_graph_reachable_by_user($1,$2,$3))"
 }else{
  "SELECT EXISTS(SELECT 1 FROM graphs WHERE id=$1 AND account_id=$2 AND resolve_graph_reachable_by_user($1,$3))"
 };
 let graph_allowed=sqlx::query_scalar::<_,bool>(graph_sql).bind(graph).bind(account).bind(user.user_id).fetch_one(&mut **tx).await.unwrap_or(false);
 if !graph_allowed {return false;}
 if let Some(org)=org {
  let Some(control)=state.control_db() else{return false;};
  let Ok(mut control_tx)=crate::tenancy::scope::begin_account_scoped(control,user.account_id).await else{return false;};
  let allowed=control_actor_admitted_tx_inner(&mut control_tx,user,Some(org),enforce_deletion).await.unwrap_or(false);
  control_tx.rollback().await.is_ok() && allowed
 }else{
  // Canonical Personal Tenant points at CONTROL. Refuse a mismatched plane
  // rather than certify CONTROL membership from a substituted content DB.
  let Some(content)=tenant.pool.as_ref() else{return false;};
  if let Some(control)=state.control_db() {
   let c=control.connect_options();let p=content.connect_options();
   if (c.get_host(),c.get_port(),c.get_database())!=(p.get_host(),p.get_port(),p.get_database()) {return false;}
  }
  control_actor_admitted_tx_inner(tx,user,None,enforce_deletion).await.unwrap_or(false)
 }
}

/// A DEK/org prefix is not a grant to another graph's private artifact.
/// Old retained object IDs may differ from a version ID, but must be UUIDs in
/// the exact canonical graph/tenant key shape used by all trusted writers.
pub(crate) fn artifact_locator_admitted(graph: Uuid, key: &str, org: Option<&str>) -> bool {
    let Some(object) = key.rsplit('/').next().and_then(|id| Uuid::parse_str(id).ok()) else { return false; };
    coralswarm_core::r2::blob_key_for(org, graph, object).is_ok_and(|expected| expected == key)
}
async fn read_artifact(state:&Arc<AppState>,tenant:&Tenant,account:Uuid,graph:Uuid,inline:Option<&str>,key:Option<&str>)->Option<String> {
    if let Some(text)=inline {return Some(text.to_owned());}
    let pool=tenant.pool.as_ref()?;let r2=state.r2.as_ref()?;let key=key?;
    let org=match tenant.decision.as_ref(){Some(crate::tenancy::runtime::TenancyDecision::Org(c))=>Some(c.org_id.as_str()),_=>None};
    if !artifact_locator_admitted(graph,key,org) {return None;}
    let dek=crate::crypto::resolve_dek_cached_dispatch(tenant.decision.as_ref(),pool,account,&state.dek_cache).await.ok()??;
    String::from_utf8(r2.get_atom_blob(key,&dek).await.ok()?).ok()
}
pub(crate) async fn read_text(state:&Arc<AppState>,tenant:&Tenant,account:Uuid,row:&SourceVersion)->Option<String> {
    read_artifact(state,tenant,account,row.graph_id,row.raw_inline.as_deref(),row.raw_blob_key.as_deref()).await
}
/// Shared permission/locator fence for an actually retained unbound transcript.
/// Returned bytes are normalized retained evidence, not a claim of provider
/// provenance. No summary or overlay can substitute for the original artifact.
pub(crate) async fn read_unbound_transcript_for_promotion(state:&Arc<AppState>,tenant:&Tenant,user:&UserContext,graph:Uuid,account:Uuid,id:Uuid)->Option<String> {
    if !still_admitted(state,tenant,user,graph,account).await {return None;}
    let mut tx=tenant.begin_member_scoped(account).await.ok()?;
    let row:Option<(Option<String>,Option<String>,String)>=sqlx::query_as(&format!("SELECT raw_inline,raw_blob_key,xmin::text FROM meeting_transcripts t WHERE graph_id=$1 AND id=$2 AND source_item_id IS NULL AND current_version_id IS NULL AND {floor}", floor = crate::history_floor::admits_transcript("t")))
        .bind(graph).bind(id).fetch_optional(&mut *tx).await.ok()?;
    tx.commit().await.ok()?;let row=row?;
    let text=read_artifact(state,tenant,account,graph,row.0.as_deref(),row.1.as_deref()).await?;
    if !still_admitted(state,tenant,user,graph,account).await {return None;}
    let mut tx=tenant.begin_member_scoped(account).await.ok()?;
    let unchanged:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM meeting_transcripts WHERE graph_id=$1 AND id=$2 AND source_item_id IS NULL AND current_version_id IS NULL AND raw_inline IS NOT DISTINCT FROM $3 AND raw_blob_key IS NOT DISTINCT FROM $4 AND xmin::text=$5) /* history_floor: an unchanged-row recheck of the floored read above */")
        .bind(graph).bind(id).bind(&row.0).bind(&row.1).bind(&row.2).fetch_one(&mut *tx).await.ok()?;
    tx.commit().await.ok()?;unchanged.then_some(text)
}

/// A bounded collection; each atom's passages and same-graph canonical nodes
/// are returned together, preserving reverse navigation without blob locators.
pub async fn atom_json(
    tx: &mut Transaction<'_, Postgres>,
    graph: Uuid,
    version: Uuid,
    after: i32,
) -> Result<Vec<Value>, sqlx::Error> {
    sqlx::query_scalar(
        r#"
        SELECT jsonb_build_object(
            'atom_id', a.semantic_atom_id, 'ordinal', a.ordinal,
            'kind', a.kind, 'statement', a.statement, 'entities', a.entities,
            'extraction_version', a.extraction_version, 'model', a.model,
            'confidence', a.confidence, 'created_at', a.created_at,
            'passages', COALESCE((
                SELECT jsonb_agg(jsonb_build_object(
                    'passage_id', p.passage_id, 'quote', p.quote,
                    'start_char', p.start_char, 'end_char', p.end_char,
                    'start_ms', p.start_ms, 'end_ms', p.end_ms, 'speaker', p.speaker
                ) ORDER BY p.start_char,p.passage_id)
                FROM semantic_atom_passages p WHERE p.semantic_atom_id=a.semantic_atom_id AND p.graph_id=a.graph_id AND p.source_version_id=a.source_version_id
            ), '[]'::jsonb),
            'nodes', COALESCE((
                SELECT jsonb_agg(jsonb_build_object(
                    'node_id', n.node_id, 'graph_id', n.graph_id, 'title', COALESCE(library_node_title(n.graph_id,n.node_id),'Connected node'), 'type', n.type
                ) ORDER BY n.node_id)
                FROM node_semantic_atoms na
                JOIN nodes n ON n.node_id=na.node_id AND n.graph_id=na.graph_id
                WHERE na.semantic_atom_id=a.semantic_atom_id AND na.graph_id=a.graph_id AND library_node_permitted(n.graph_id,n.node_id)
            ), '[]'::jsonb)
        )
        FROM served_semantic_atoms a
        WHERE a.graph_id=$1 AND a.source_version_id=$2 AND a.ordinal>$3
        ORDER BY a.ordinal LIMIT 101
    "#,
    )
    .bind(graph)
    .bind(version)
    .bind(after)
    .fetch_all(&mut **tx)
    .await
}

pub async fn get_evidence(
    State(state): State<Arc<AppState>>,
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
    Query(q): Query<EvidenceQuery>,
) -> (StatusCode, Json<Value>) {
    let account = match authorize(&tenant, &user, q.graph_id).await {
        Ok(a) => a,
        Err(e) => return e,
    };

    let cursor = match q.before.as_ref() {
        None => None,
        Some(s) => match URL_SAFE_NO_PAD
            .decode(s)
            .ok()
            .and_then(|v| serde_json::from_slice::<VersionCursor>(&v).ok())
            .filter(|c| c.graph_id == q.graph_id && c.source_item_id == q.source_item_id)
        {
            Some(c) => Some(c),
            None => return fail(StatusCode::BAD_REQUEST, "malformed cursor"),
        },
    };

    let atom_cursor = match q.atoms_after.as_ref() {
        None => None,
        Some(s) => match URL_SAFE_NO_PAD
            .decode(s)
            .ok()
            .and_then(|v| serde_json::from_slice::<AtomCursor>(&v).ok())
            .filter(|c| c.graph_id == q.graph_id)
        {
            Some(c) => Some(c),
            None => return fail(StatusCode::BAD_REQUEST, "malformed atom cursor"),
        },
    };

    let result =
        async {
            let mut tx = tenant.begin_member_scoped(account).await?;
            // The reader's team-seat history floor (#1545) on the item and on
            // every version this detail lists or opens: an item whose current
            // version is before the floor is not found, and an older version
            // before the floor is neither listed nor readable.
            let item_floor = crate::history_floor::admits_source_item("s");
            let mut source: Option<Value> = sqlx::query_scalar(&format!("SELECT jsonb_build_object('source_item_id',source_item_id,'source_type',source_type,'provider_namespace',provider_namespace,'source_id',source_id,'origin',origin) FROM source_items s WHERE graph_id=$1 AND source_item_id=$2 AND access_state='permitted' AND {item_floor}"))
                .bind(q.graph_id).bind(q.source_item_id).fetch_optional(&mut *tx).await?;
            if let Some(source) = source.as_mut() { attach_capture_contract(source); }
            let current: Option<Option<Uuid>> = sqlx::query_scalar(&format!(
            "SELECT current_version_id FROM source_items s WHERE graph_id=$1 AND source_item_id=$2 AND access_state='permitted' AND {item_floor}",
        )).bind(q.graph_id).bind(&q.source_item_id).fetch_optional(&mut *tx).await?;
            let versions: Vec<VersionSummary> = sqlx::query_as(&format!(
                "SELECT v.version_id,v.captured_at,v.is_legacy_baseline FROM source_versions v \
             JOIN source_items s ON s.source_item_id=v.source_item_id AND s.graph_id=v.graph_id \
             WHERE v.graph_id=$1 AND v.source_item_id=$2 AND s.access_state='permitted' AND {floor} \
               AND ($3::timestamptz IS NULL OR (v.captured_at,v.version_id)<($3,$4)) \
             ORDER BY v.captured_at DESC,v.version_id DESC LIMIT 21",
                floor = crate::history_floor::admits_version("v")
            ))
            .bind(q.graph_id)
            .bind(&q.source_item_id)
            .bind(cursor.as_ref().map(|c| c.captured_at))
            .bind(cursor.as_ref().map(|c| c.version_id))
            .fetch_all(&mut *tx)
            .await?;
            let mut selected = if let Some(id) = q.version_id.or(current.flatten()) {
                load_version_scoped(&mut tx, q.graph_id, id)
                    .await?
                    .filter(|v| v.source_item_id == q.source_item_id)
            } else {
                None
            };
            if let Some(v) = selected.as_mut() {
                let (status, error) = extraction_state(&mut tx, v).await?;
                v.extraction_status = status;
                v.extraction_error = error;
            }
            let mut after = -1;
            if let Some(v) = &selected {
                if let Some(c) = &atom_cursor {
                    if c.version_id != v.version_id {
                        return Ok((current, versions, None, vec![], true, source));
                    }
                    after = c.ordinal;
                } else if let Some(id) = q.atom_id {
                    let ordinal: Option<i32> = sqlx::query_scalar(
                        "SELECT ordinal FROM semantic_atoms \
                     WHERE semantic_atom_id=$1 AND source_version_id=$2 AND graph_id=$3 \
                     /* history_floor: keyed by the selected version, which load_version_scoped floors */",
                    )
                    .bind(id)
                    .bind(v.version_id)
                    .bind(q.graph_id)
                    .fetch_optional(&mut *tx)
                    .await?;
                    let Some(ordinal) = ordinal else {
                        return Ok((current, versions, None, vec![], true, source));
                    };
                    after = ordinal - 1;
                }
            }
            let atoms = if let Some(v) = &selected {
                atom_json(&mut tx, q.graph_id, v.version_id, after).await?
            } else {
                vec![]
            };
            tx.commit().await?;
            Ok::<_, crate::store::StoreError>((current, versions, selected, atoms, false, source))
        }
        .await;

    let (current, mut versions, selected, mut atoms, bad_atom_cursor, source) = match result {
        Ok(v) => v,
        Err(e) => {
            tracing::error!(error=%e,"source evidence read failed");

            return fail(StatusCode::INTERNAL_SERVER_ERROR, "internal error");
        }
    };

    if bad_atom_cursor {
        return fail(
            StatusCode::BAD_REQUEST,
            "atom or cursor does not belong to selected version",
        );
    }

    if current.is_none() || (q.version_id.is_some() && selected.is_none()) {
        return fail(
            StatusCode::NOT_FOUND,
            "source version not found or access denied",
        );
    }

    let next_cursor = if versions.len() > 20 {
        versions.truncate(20);

        versions.last().map(|v| {
            URL_SAFE_NO_PAD.encode(
                serde_json::to_vec(&VersionCursor {
                    graph_id: q.graph_id,
                    source_item_id: q.source_item_id,
                    captured_at: v.captured_at,
                    version_id: v.version_id,
                })
                .unwrap(),
            )
        })
    } else {
        None
    };

    let atoms_next_cursor = if atoms.len() > 100 {
        atoms.truncate(100);

        selected.as_ref().and_then(|v| {
            atoms
                .last()
                .and_then(|a| a["ordinal"].as_i64())
                .map(|ordinal| {
                    URL_SAFE_NO_PAD.encode(
                        serde_json::to_vec(&AtomCursor {
                            graph_id: q.graph_id,
                            version_id: v.version_id,
                            ordinal: ordinal as i32,
                        })
                        .unwrap(),
                    )
                })
        })
    } else {
        None
    };

    for atom in &mut atoms {
        if let Some(obj) = atom.as_object_mut() {
            obj.remove("ordinal");
        }
    }

    let selected = if let Some(v) = selected {
        let text = read_text(&state, &tenant, account, &v).await;
        // Recheck request admission as well as exact source permission after
        // blob I/O, before emitting any metadata, passages or preserved text.
        if !still_admitted(&state, &tenant, &user, q.graph_id, account).await {
            return fail(
                StatusCode::NOT_FOUND,
                "source version not found or access denied",
            );
        }
        let still_permitted=async {
            let mut tx=tenant.begin_member_scoped(account).await?;
            let allowed:bool=sqlx::query_scalar(&format!("SELECT EXISTS(SELECT 1 FROM source_items s JOIN source_versions v ON v.graph_id=s.graph_id AND v.source_item_id=s.source_item_id WHERE s.graph_id=$1 AND s.source_item_id=$2 AND v.version_id=$3 AND s.access_state='permitted' AND {floor})", floor = crate::history_floor::admits_version("v")))
                .bind(q.graph_id).bind(q.source_item_id).bind(v.version_id).fetch_one(&mut *tx).await?;
            tx.commit().await?;Ok::<_,crate::store::StoreError>(allowed)
        }.await;
        if !matches!(still_permitted, Ok(true)) {
            return fail(
                StatusCode::NOT_FOUND,
                "source version not found or access denied",
            );
        }

        Some(json!({
        "version_id":v.version_id,"source_item_id":v.source_item_id,"source_id":v.source_id,"source_type":v.source_type,"provider_namespace":v.provider_namespace,"representation":v.representation,"source_metadata":v.source_metadata,"origin":v.origin,"capture_contract":capture_contract(v.origin.as_ref().map(|o| &o.0)),"occurred_at":v.occurred_at,"participants":v.participants,"extraction_version":v.extraction_version,"captured_at":v.captured_at,"is_legacy_baseline":v.is_legacy_baseline,"raw_text":text,"text_status":if text.is_some(){
        "available"}
        else{
        "unavailable"}
        ,"extraction_status":v.extraction_status,"extraction_error":v.extraction_error,"can_retry":matches!(v.extraction_status.as_str(),"not_started"|"failed"|"blocked"),"atoms":atoms,"atoms_next_cursor":atoms_next_cursor}
        ))
    } else {
        None
    };

    (
        StatusCode::OK,
        Json(json!({
        "graph_id":q.graph_id,"source_item_id":q.source_item_id,"source":source,"versions":versions.iter().map(|v|json!({
        "version_id":v.version_id,"captured_at":v.captured_at,"is_legacy_baseline":v.is_legacy_baseline,"is_current":Some(v.version_id)==current.flatten()}
        )).collect::<Vec<_>>(),"next_cursor":next_cursor,"selected_version":selected}
        )),
    )
}

#[derive(Deserialize)]
pub struct RetryRequest {
    pub graph_id: Uuid,
    pub source_item_id: Uuid,
    pub version_id: Uuid,
}

pub async fn retry_evidence(
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
    Json(q): Json<RetryRequest>,
) -> (StatusCode, Json<Value>) {
    let account = match authorize(&tenant, &user, q.graph_id).await {
        Ok(a) => a,
        Err(e) => return e,
    };

    let result = async {
        let mut tx = tenant.begin_member_scoped(account).await?;

        let row: Option<SourceVersion> = sqlx::query_as(&format!(
            r#"
        SELECT v.* FROM source_versions v
        JOIN source_items s ON s.source_item_id=v.source_item_id AND s.graph_id=v.graph_id
        WHERE v.graph_id=$1 AND v.source_item_id=$2 AND v.version_id=$3 AND s.access_state='permitted' AND {floor} FOR UPDATE OF v
        "#,
            floor = crate::history_floor::admits_version("v")
        ))
        .bind(q.graph_id)
        .bind(&q.source_item_id)
        .bind(q.version_id)
        .fetch_optional(&mut *tx)
        .await?;

        let Some(row) = row else {
            return Ok::<_, crate::store::StoreError>(None);
        };

        let (status, _) = extraction_state(&mut tx, &row).await?;

        let accepted = matches!(status.as_str(), "not_started" | "failed" | "blocked");

        if accepted {
            queue_extraction_tx(
                &mut tx,
                q.graph_id,
                q.version_id,
                account,
                row.next_start_char,
            )
            .await?;
        }

        let queued:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM jobs WHERE payload->>'graph_id'=$1 AND payload->>'source_version_id'=$2 AND status::text IN('pending','running'))")
            .bind(q.graph_id.to_string()).bind(q.version_id.to_string()).fetch_one(&mut *tx).await?;
        tx.commit().await?;
        Ok(Some(accepted && queued))
    }
    .await;

    match result {
        Ok(Some(accepted)) => (StatusCode::OK, Json(json!({"accepted": accepted,"blocked_reason":if accepted {None}else{Some("managed_admission_required_or_run_paused")}}))),
        Ok(None) => fail(
            StatusCode::NOT_FOUND,
            "source version not found or access denied",
        ),
        Err(e) => {
            tracing::error!(error=%e,"source evidence retry failed");

            fail(StatusCode::INTERNAL_SERVER_ERROR, "internal error")
        }
    }
}

#[derive(Deserialize)]
pub struct NodeAtomsQuery {
    pub graph_id: Uuid,
    pub before: Option<Uuid>,
}

pub async fn get_node_atoms(
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
    Path(node_id): Path<Uuid>,
    Query(q): Query<NodeAtomsQuery>,
) -> (StatusCode, Json<Value>) {
    let account = match authorize(&tenant, &user, q.graph_id).await {
        Ok(a) => a,
        Err(e) => return e,
    };

    let result = async {
        let mut tx = tenant.begin_member_scoped(account).await?;
        let exists: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM nodes WHERE node_id=$1 AND graph_id=$2 AND library_node_permitted(graph_id,node_id))",
        ).bind(node_id).bind(q.graph_id).fetch_one(&mut *tx).await?;
        if !exists {
            return Ok::<_, crate::store::StoreError>(None);
        }
        let mut rows: Vec<Value> = sqlx::query_scalar(r#"
            SELECT jsonb_build_object(
                'atom_id', a.semantic_atom_id, 'kind', a.kind, 'statement', a.statement,
                'source_id', v.source_id, 'source_item_id', v.source_item_id, 'source_type', v.source_type, 'provider_namespace', v.provider_namespace, 'representation', v.representation, 'source_version_id', v.version_id,
                'captured_at', v.captured_at, 'is_legacy_baseline', v.is_legacy_baseline,
                'passages', COALESCE((
                    SELECT jsonb_agg(jsonb_build_object(
                        'passage_id', p.passage_id, 'quote', p.quote,
                        'start_char', p.start_char, 'end_char', p.end_char,
                        'start_ms', p.start_ms, 'end_ms', p.end_ms, 'speaker', p.speaker
                    ) ORDER BY p.start_char,p.passage_id)
                    FROM semantic_atom_passages p WHERE p.semantic_atom_id=a.semantic_atom_id AND p.graph_id=a.graph_id AND p.source_version_id=a.source_version_id
                ), '[]'::jsonb)
            )
            FROM node_semantic_atoms na
            JOIN served_semantic_atoms a ON a.semantic_atom_id=na.semantic_atom_id AND a.graph_id=na.graph_id
            JOIN source_versions v ON v.version_id=a.source_version_id AND v.graph_id=a.graph_id
            WHERE na.node_id=$1 AND na.graph_id=$2 AND ($3::uuid IS NULL OR a.semantic_atom_id<$3)
            ORDER BY a.semantic_atom_id DESC LIMIT 21
        "#).bind(node_id).bind(q.graph_id).bind(q.before).fetch_all(&mut *tx).await?;
        tx.commit().await?;
        let next_cursor = if rows.len() > 20 {
            rows.truncate(20);
            rows.last().and_then(|v| v.get("atom_id")).cloned()
        } else {
            None
        };
        Ok(Some(json!({
            "graph_id": q.graph_id, "node_id": node_id,
            "items": rows, "next_cursor": next_cursor,
        })))
    }.await;

    match result {
        Ok(Some(v)) => (StatusCode::OK, Json(v)),
        Ok(None) => fail(StatusCode::NOT_FOUND, "node not found or access denied"),
        Err(e) => {
            tracing::error!(error=%e,"node semantic evidence read failed");

            fail(StatusCode::INTERNAL_SERVER_ERROR, "internal error")
        }
    }
}

#[derive(Deserialize)]
pub struct ItemsQuery {
    pub graph_id: Uuid,
    pub before: Option<String>,
    pub source_type: Option<String>,
    pub atom_id: Option<Uuid>,
}
#[derive(Serialize, Deserialize)]
struct ItemCursor {
    graph_id: Uuid,
    source_type: Option<String>,
    atom_id: Option<Uuid>,
    updated_at: DateTime<Utc>,
    source_item_id: Uuid,
}

/// Inventory is scoped to one explicitly authorized graph and never includes
/// preserved text or blob locators. Current metadata is a version snapshot.
pub async fn list_items(
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
    Query(q): Query<ItemsQuery>,
) -> (StatusCode, Json<Value>) {
    let account = match authorize(&tenant, &user, q.graph_id).await {
        Ok(id) => id,
        Err(e) => return e,
    };
    let cursor = match q.before.as_ref() {
        None => None,
        Some(value) => match URL_SAFE_NO_PAD
            .decode(value)
            .ok()
            .and_then(|v| serde_json::from_slice::<ItemCursor>(&v).ok())
            .filter(|c| {
                c.graph_id == q.graph_id && c.source_type == q.source_type && c.atom_id == q.atom_id
            }) {
            Some(c) => Some(c),
            None => return fail(StatusCode::BAD_REQUEST, "malformed cursor"),
        },
    };
    let result=async {
        let mut tx=tenant.begin_member_scoped(account).await?;
        // The reader's team-seat history floor (#1545): an item is listed at
        // its current version's moment, and an atom's mapped version only
        // when that version is admitted too.
        let value=inventory_page_tx(&mut tx,q.graph_id,q.source_type,q.atom_id,cursor).await?;
        tx.commit().await?;
        Ok::<_,crate::store::StoreError>(value)
    }.await;
    match result {
        Ok(body) => (StatusCode::OK, Json(body)),
        Err(error) => {
            tracing::error!(%error,"source inventory failed");
            fail(StatusCode::INTERNAL_SERVER_ERROR, "internal error")
        }
    }
}

#[derive(Debug, Default, Clone, Serialize, sqlx::FromRow)]
pub struct NodeEvidenceCounts {
    pub node_id: Uuid,
    pub semantic_atom_count: i64,
    pub source_support_count: i64,
    pub unmapped_legacy_chunk_count: i64,
}
/// Count distinct source items across both evidence layers and all preserved
/// versions. Retrieval chunk counts keep their existing independent meaning.
pub async fn node_evidence_counts_tx(
    tx: &mut Transaction<'_, Postgres>,
    graph_id: Uuid,
    node_ids: &[Uuid],
) -> Result<Vec<NodeEvidenceCounts>, sqlx::Error> {
    node_evidence_counts_for_graphs_tx(tx, &[graph_id], node_ids).await
}

/// `$1` graphs, `$2` nodes: [`NodeEvidenceCounts`] for the permitted nodes.
/// Every legacy membership is checked against the node's own graph, with one
/// `library_permitted_atoms` call per graph over the requested nodes' links
/// rather than `library_atom_permitted` per link (#1490; the equivalence is
/// in `crate::store::verdict`). `permitted_links` is exactly the links the
/// per-link probe admitted, and both counts below read only those.
pub const NODE_EVIDENCE_COUNTS_SQL: &str = r#"
        WITH requested AS MATERIALIZED (SELECT node_id,graph_id FROM nodes WHERE graph_id=ANY($1) AND node_id=ANY($2) AND library_node_permitted(graph_id,node_id)),
        links AS MATERIALIZED (SELECT n.node_id,n.graph_id,na.atom_id FROM requested n JOIN node_atoms na ON na.node_id=n.node_id),
        permitted AS MATERIALIZED (SELECT i.graph_id,x.atom_id
            FROM (SELECT l.graph_id,array_agg(DISTINCT l.atom_id) AS ids FROM links l GROUP BY l.graph_id) i
            CROSS JOIN LATERAL library_permitted_atoms(i.graph_id, CASE WHEN cardinality(i.ids) > 10000 THEN NULL ELSE i.ids END) x),
        permitted_links AS MATERIALIZED (SELECT l.node_id,l.graph_id,l.atom_id FROM links l JOIN permitted p ON p.graph_id=l.graph_id AND p.atom_id=l.atom_id),
        semantic AS (SELECT na.node_id,na.semantic_atom_id,a.source_item_id FROM requested n
            JOIN node_semantic_atoms na ON na.node_id=n.node_id AND na.graph_id=n.graph_id
            JOIN served_semantic_atoms a ON a.semantic_atom_id=na.semantic_atom_id AND a.graph_id=na.graph_id),
        sources AS (SELECT node_id,source_item_id FROM semantic UNION
            SELECT pl.node_id,v.source_item_id FROM permitted_links pl
            JOIN source_version_chunks c ON c.atom_id=pl.atom_id AND c.graph_id=pl.graph_id
            JOIN source_versions v ON v.version_id=c.source_version_id AND v.graph_id=c.graph_id
            JOIN source_items si ON si.source_item_id=v.source_item_id AND si.graph_id=v.graph_id WHERE si.access_state='permitted')
        SELECT n.node_id,(SELECT count(*) FROM semantic s WHERE s.node_id=n.node_id) AS semantic_atom_count,
            (SELECT count(*) FROM sources s WHERE s.node_id=n.node_id) AS source_support_count,
            (SELECT count(*) FROM permitted_links pl WHERE pl.node_id=n.node_id AND NOT EXISTS
                (SELECT 1 FROM source_version_chunks c WHERE c.graph_id=pl.graph_id AND c.atom_id=pl.atom_id)) AS unmapped_legacy_chunk_count FROM requested n
    "#;

/// One bounded batch for Library's already-authorized graph and node window
/// ([`NODE_EVIDENCE_COUNTS_SQL`]).
pub async fn node_evidence_counts_for_graphs_tx(
    tx: &mut Transaction<'_, Postgres>,
    graph_ids: &[Uuid],
    node_ids: &[Uuid],
) -> Result<Vec<NodeEvidenceCounts>, sqlx::Error> {
    sqlx::query_as(NODE_EVIDENCE_COUNTS_SQL).bind(graph_ids).bind(node_ids).fetch_all(&mut **tx).await
}

fn attach_capture_contract(value: &mut Value) {
    let origin = serde_json::from_value::<SourceOrigin>(value["origin"].clone()).ok();
    value["capture_contract"] = json!(capture_contract(origin.as_ref()));
}

/// Trusted provider access observation, never inferred from a missing page.
/// The caller owns the account/member/provider transaction. Namespace and graph
/// are both required; no ambient account-wide permission change is accepted.
pub async fn set_namespace_access_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    graph_id: Uuid,
    provider_namespace: &str,
    access_state: &str,
) -> Result<u64, sqlx::Error> {
    if !matches!(access_state,"restricted" | "unknown") || provider_namespace.is_empty() {
        return Err(sqlx::Error::Protocol("invalid trusted source access observation".into()));
    }
    let rows: Vec<Uuid> = sqlx::query_scalar("UPDATE source_items SET access_state=$3,updated_at=now() WHERE graph_id=$1 AND provider_namespace=$2 AND access_state<>$3 RETURNING source_item_id")
        .bind(graph_id).bind(provider_namespace).bind(access_state).fetch_all(&mut **tx).await?;
    if access_state != "permitted" && !rows.is_empty() {
        sqlx::query("INSERT INTO source_retraction_outbox(graph_id,source_item_id,event_kind) SELECT $1,unnest($2::uuid[]),$3 ON CONFLICT DO NOTHING")
            .bind(graph_id).bind(&rows).bind(access_state).execute(&mut **tx).await?;
        sqlx::query("DELETE FROM journal_rollups WHERE graph_id=$1").bind(graph_id).execute(&mut **tx).await?;
        sqlx::query("DELETE FROM query_history WHERE graph_id=$1").bind(graph_id).execute(&mut **tx).await?;
        sqlx::query("DELETE FROM extraction_cache WHERE account_id=(SELECT account_id FROM graphs WHERE id=$1)").bind(graph_id).execute(&mut **tx).await?;
    }
    Ok(rows.len() as u64)
}

/// Restore access only for an exact successfully refetched provider item.
/// A successful reconnect/page is not proof that every namespace item is allowed.
pub async fn set_source_identity_access_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>, graph_id: Uuid,
    source_type: &str, provider_namespace: &str, source_id: &str, access_state: &str,
) -> Result<u64, sqlx::Error> {
    if !matches!(access_state,"permitted" | "restricted" | "unknown") || source_type.is_empty() || provider_namespace.is_empty() || source_id.is_empty() {
        return Err(sqlx::Error::Protocol("invalid trusted source identity access observation".into()));
    }
    let item: Option<Uuid> = sqlx::query_scalar("UPDATE source_items SET access_state=$5,updated_at=now() WHERE graph_id=$1 AND source_type=$2 AND provider_namespace=$3 AND source_id=$4 AND access_state<>$5 RETURNING source_item_id")
        .bind(graph_id).bind(source_type).bind(provider_namespace).bind(source_id).bind(access_state).fetch_optional(&mut **tx).await?;
    if let Some(item) = item {
        if access_state != "permitted" {
            sqlx::query("INSERT INTO source_retraction_outbox(graph_id,source_item_id,event_kind) VALUES($1,$2,$3) ON CONFLICT DO NOTHING")
                .bind(graph_id).bind(item).bind(access_state).execute(&mut **tx).await?;
            sqlx::query("DELETE FROM journal_rollups WHERE graph_id=$1").bind(graph_id).execute(&mut **tx).await?;
            sqlx::query("DELETE FROM query_history WHERE graph_id=$1").bind(graph_id).execute(&mut **tx).await?;
            sqlx::query("DELETE FROM extraction_cache WHERE account_id=(SELECT account_id FROM graphs WHERE id=$1)").bind(graph_id).execute(&mut **tx).await?;
        }
        return Ok(1);
    }
    Ok(0)
}

async fn inventory_page_tx(tx:&mut Transaction<'_,Postgres>,graph:Uuid,source_type:Option<String>,atom:Option<Uuid>,cursor:Option<ItemCursor>) -> Result<Value,crate::store::StoreError> {
        let mut items:Vec<Value>=sqlx::query_scalar(&format!(r#"
            SELECT jsonb_build_object('source_item_id',s.source_item_id,'graph_id',s.graph_id,
                'source_type',s.source_type,'provider_namespace',s.provider_namespace,'source_id',s.source_id,
                'updated_at',s.updated_at,'current_version_id',s.current_version_id,'origin',s.origin,
                'representation',v.representation,'source_metadata',v.source_metadata,'occurred_at',v.occurred_at,
                'captured_at',v.captured_at,'is_legacy_baseline',v.is_legacy_baseline,'extraction_status',v.extraction_status,
                'mapped_version_id',(SELECT mapped.version_id FROM source_version_chunks c JOIN source_versions mapped ON mapped.version_id=c.source_version_id AND mapped.graph_id=c.graph_id WHERE c.atom_id=$5 AND c.graph_id=s.graph_id AND mapped.source_item_id=s.source_item_id AND {mapped_floor} ORDER BY mapped.captured_at DESC,mapped.version_id DESC LIMIT 1))
            FROM source_items s LEFT JOIN source_versions v ON v.version_id=s.current_version_id AND v.graph_id=s.graph_id
            WHERE s.graph_id=$1 AND s.access_state='permitted' AND ($2::text IS NULL OR s.source_type=$2)
              AND {item_floor}
              AND ($3::timestamptz IS NULL OR (s.updated_at,s.source_item_id)<($3,$4))
              AND ($5::uuid IS NULL OR (library_atom_permitted(s.graph_id,$5) AND EXISTS(SELECT 1 FROM source_version_chunks c JOIN source_versions mapped ON mapped.version_id=c.source_version_id AND mapped.graph_id=c.graph_id WHERE c.atom_id=$5 AND c.graph_id=s.graph_id AND mapped.source_item_id=s.source_item_id)))
            ORDER BY s.updated_at DESC,s.source_item_id DESC LIMIT 21
        "#, item_floor = crate::history_floor::admits_source_item_at("s", "v"), mapped_floor = crate::history_floor::admits_version("mapped"))).bind(graph).bind(&source_type).bind(cursor.as_ref().map(|c|c.updated_at)).bind(cursor.as_ref().map(|c|c.source_item_id)).bind(atom).fetch_all(&mut **tx).await?;
        for item in &mut items { attach_capture_contract(item); }
        let next_cursor=if items.len()>20 {
            items.truncate(20);
            items.last().and_then(|v|Some(URL_SAFE_NO_PAD.encode(serde_json::to_vec(&ItemCursor{
                graph_id:graph,source_type:source_type.clone(),atom_id:atom,updated_at:serde_json::from_value(v["updated_at"].clone()).ok()?,source_item_id:serde_json::from_value(v["source_item_id"].clone()).ok()?,
            }).ok()?)))
        }else{None};
        Ok::<_,crate::store::StoreError>(json!({"graph_id":graph,"items":items,"next_cursor":next_cursor}))
}
pub(crate) async fn inventory_first_tx(tx:&mut Transaction<'_,Postgres>,graph:Uuid,atom:Uuid)->Result<Value,crate::store::StoreError> {
    inventory_page_tx(tx,graph,None,Some(atom),None).await
}

/// Indexed metadata/association checks only. No evidence projection or blob reads.
/// Both GET/refresh and worker snapshots use this same selector boundary.
pub(crate) async fn validate_first_selectors_on(
    tx: &mut sqlx::PgConnection, graph: Uuid, item: Uuid,
    version: Option<Uuid>, atom: Option<Uuid>,
) -> Result<i32, crate::read_models::Error> {
    use crate::read_models::{BadRequest, NotFound};
    let floor = crate::history_floor::admits_source_item("s");
    let current: Option<Option<Uuid>> = sqlx::query_scalar(&format!(
        "SELECT current_version_id FROM source_items s WHERE s.graph_id=$1 AND s.source_item_id=$2 AND s.access_state='permitted' AND {floor}"))
        .bind(graph).bind(item).fetch_optional(&mut *tx).await.map_err(crate::store::StoreError::from)?;
    let Some(current) = current else { return Err(NotFound("source not found").into()) };
    let selected = version.or(current);
    if let Some(selected) = selected {
        let floor = crate::history_floor::admits_version("v");
        let admitted: bool = sqlx::query_scalar(&format!(
            "SELECT EXISTS(SELECT 1 FROM source_versions v JOIN source_items s ON s.source_item_id=v.source_item_id AND s.graph_id=v.graph_id WHERE v.graph_id=$1 AND v.version_id=$2 AND v.source_item_id=$3 AND s.access_state='permitted' AND {floor})"))
            .bind(graph).bind(selected).bind(item).fetch_one(&mut *tx).await.map_err(crate::store::StoreError::from)?;
        if !admitted { return Err(NotFound("version not found").into()) }
        if let Some(atom) = atom {
            // history_floor: selected version was admitted above. This checks
            // association only; atom_json alone determines served output.
            let ordinal: Option<i32> = sqlx::query_scalar("SELECT ordinal FROM semantic_atoms WHERE semantic_atom_id=$1 AND source_version_id=$2 AND graph_id=$3 /* history_floor: association only, selected version admitted above */")
                .bind(atom).bind(selected).bind(graph).fetch_optional(&mut *tx).await.map_err(crate::store::StoreError::from)?;
            return ordinal.map(|o| o.saturating_sub(1)).ok_or_else(|| BadRequest("atom is not in selected version").into());
        }
    } else if atom.is_some() {
        return Err(BadRequest("atom has no selected version").into());
    }
    Ok(-1)
}

/// First evidence page from the caller's existing authorized snapshot. The
/// interactive endpoint keeps later version/atom cursor pagination separately.
pub(crate) async fn first_evidence_tx(
    tx: &mut Transaction<'_,Postgres>, blobs: Option<&crate::read_models::blobs::BlobReader<'_>>,
    account:Uuid, graphs:&[Uuid], graph:Uuid, item:Uuid,
    version_id: Option<Uuid>, atom_id: Option<Uuid>,
)->Result<Option<Value>,crate::read_models::Error>{
    if !graphs.contains(&graph){return Err("evidence graph outside verified scope".into())}
    let after = validate_first_selectors_on(tx, graph, item, version_id, atom_id).await?;
    let floor=crate::history_floor::admits_source_item("s");
    let source:Option<Value>=sqlx::query_scalar(&format!("SELECT jsonb_build_object('source_item_id',source_item_id,'source_type',source_type,'provider_namespace',provider_namespace,'source_id',source_id,'origin',origin,'current_version_id',current_version_id) FROM source_items s WHERE graph_id=$1 AND source_item_id=$2 AND access_state='permitted' AND {floor}"))
        .bind(graph).bind(item).fetch_optional(&mut **tx).await?;
    let Some(mut source)=source else{return Ok(None)};
    attach_capture_contract(&mut source);
    let current:Option<Uuid>=serde_json::from_value(source["current_version_id"].clone())?;
    source.as_object_mut().unwrap().remove("current_version_id");
    let versions:Vec<(Uuid,DateTime<Utc>,bool)>=sqlx::query_as(&format!("SELECT v.version_id,v.captured_at,v.is_legacy_baseline FROM source_versions v JOIN source_items s ON s.source_item_id=v.source_item_id AND s.graph_id=v.graph_id WHERE v.graph_id=$1 AND v.source_item_id=$2 AND s.access_state='permitted' AND {version_floor} ORDER BY v.captured_at DESC,v.version_id DESC LIMIT 21",version_floor=crate::history_floor::admits_version("v")))
        .bind(graph).bind(item).fetch_all(&mut **tx).await?;
    let next_cursor=if versions.len()>20{let v=&versions[19];Some(URL_SAFE_NO_PAD.encode(serde_json::to_vec(&VersionCursor{graph_id:graph,source_item_id:item,captured_at:v.1,version_id:v.0})?))}else{None};
    let selected=match version_id.or(current) {
        Some(id)=>load_version_scoped(tx,graph,id).await?.filter(|v|v.source_item_id==item),None=>None,
    };
    let selected=if let Some(mut v)=selected {
        let (status,error)=extraction_state(tx,&v).await?;v.extraction_status=status;v.extraction_error=error;
        let mut atoms=atom_json(tx,graph,v.version_id,after).await?;
        let atoms_next_cursor=if atoms.len()>100{
            atoms.truncate(100);
            let ordinal=atoms.last().and_then(|v|v["ordinal"].as_i64()).ok_or("missing evidence ordinal")? as i32;
            Some(URL_SAFE_NO_PAD.encode(serde_json::to_vec(&AtomCursor{graph_id:graph,version_id:v.version_id,ordinal})?))
        }else{None};
        for a in &mut atoms {if let Some(o)=a.as_object_mut(){o.remove("ordinal");}}
        let text=match blobs {
            Some(reader)=>reader.text(tx,account,graphs,graph,v.raw_inline.as_deref(),v.raw_blob_key.as_deref()).await?,
            None=>v.raw_inline.clone(),
        };
        Some(json!({"version_id":v.version_id,"source_item_id":v.source_item_id,"source_id":v.source_id,"source_type":v.source_type,"provider_namespace":v.provider_namespace,
            "representation":v.representation,"source_metadata":v.source_metadata,"origin":v.origin,"capture_contract":capture_contract(v.origin.as_ref().map(|o|&o.0)),
            "occurred_at":v.occurred_at,"participants":v.participants,"extraction_version":v.extraction_version,"captured_at":v.captured_at,"is_legacy_baseline":v.is_legacy_baseline,
            "raw_text":text,"text_status":if text.is_some(){"available"}else{"unavailable"},"extraction_status":v.extraction_status,"extraction_error":v.extraction_error,
            "can_retry":matches!(v.extraction_status.as_str(),"not_started"|"failed"|"blocked"),"atoms":atoms,"atoms_next_cursor":atoms_next_cursor}))
    }else{None};
    let mut value=json!({"graph_id":graph,"source_item_id":item,"source":source,"versions":versions.iter().take(20).map(|v|json!({"version_id":v.0,"captured_at":v.1,"is_legacy_baseline":v.2,"is_current":Some(v.0)==current})).collect::<Vec<_>>(),"next_cursor":next_cursor,"selected_version":selected});
    if let Some(id) = version_id {
        let selected = &value["selected_version"];
        let summary = json!({"version_id":id,"captured_at":selected["captured_at"],"is_legacy_baseline":selected["is_legacy_baseline"],"is_current":Some(id)==current});
        let summaries = value["versions"].as_array_mut().unwrap();
        if !summaries.iter().any(|v| v["version_id"]==json!(id)) { summaries.push(summary); }
    }
    crate::services::page_titles::project(tx,graphs,graph,&mut value).await?;
    Ok(Some(value))
}

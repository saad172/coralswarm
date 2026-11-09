//! Versioned reported capture metadata. This is not semantic understanding.
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::{Postgres, Transaction};
use uuid::Uuid;

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CaptureContext {
    pub revision: u8,
    pub session_id: Option<String>,
    pub session_kind: Option<String>,
    pub session_title: Option<String>,
    pub project: Option<String>,
    pub project_key: Option<String>,
    pub repo_path: Option<String>,
    pub remote_sanitized: Option<String>,
    pub branch: Option<String>,
    pub hostname: Option<String>,
    pub harness_version: Option<String>,
    pub model: Option<String>,
    pub agent_name: Option<String>,
    pub platform: Option<String>,
    pub platform_detected: Option<String>,
    pub task: Option<String>,
    pub subagent_name: Option<String>,
    pub parent_session_id: Option<String>,
    pub root_session_id: Option<String>,
}

impl CaptureContext {
    pub fn validate(&self) -> Result<(), String> {
        if self.revision != 1 {
            return Err("unsupported capture context revision".into());
        }
        for (value, cap) in [
            (&self.session_id, 512),
            (&self.session_kind, 32),
            (&self.session_title, 512),
            (&self.project, 512),
            (&self.project_key, 1024),
            (&self.repo_path, 1024),
            (&self.remote_sanitized, 1024),
            (&self.branch, 512),
            (&self.hostname, 512),
            (&self.harness_version, 512),
            (&self.model, 512),
            (&self.agent_name, 512),
            (&self.platform, 512),
            (&self.platform_detected, 512),
            (&self.task, 2000),
            (&self.subagent_name, 512),
            (&self.parent_session_id, 512),
            (&self.root_session_id, 512),
        ] {
            if value
                .as_ref()
                .is_some_and(|s| s.contains('\0') || s.chars().count() > cap)
            {
                return Err("invalid captured context field".into());
            }
        }
        // `work` is the retired desktop kind: ingest no longer derives it, but
        // this also validates PERSISTED metadata (`from_metadata`), so it stays
        // until the desktop data purge.
        if self
            .session_kind
            .as_ref()
            .is_some_and(|s| !matches!(s.as_str(), "coding" | "work" | "meeting" | "agent"))
        {
            return Err("invalid captured session kind".into());
        }
        if self.session_id.is_none()
            && (self.parent_session_id.is_some() || self.root_session_id.is_some())
        {
            return Err("captured lineage requires a session".into());
        }
        if self.parent_session_id == self.session_id && self.parent_session_id.is_some() {
            return Err("captured session cannot parent itself".into());
        }
        // This field accepts the normalized repo identity, never a credentialed
        // transport URL. The ingest normalizer removes userinfo/query/fragment.
        if self.remote_sanitized.as_ref().is_some_and(|s| {
            s.contains('@') || s.contains('?') || s.contains('#') || s.contains("://")
        }) {
            return Err("repo remote must be sanitized".into());
        }
        Ok(())
    }
}

pub fn from_metadata(metadata: &Value) -> Result<Option<CaptureContext>, String> {
    match metadata.get("capture_context").filter(|v| !v.is_null()) {
        None => Ok(None),
        Some(value) => {
            let context: CaptureContext = serde_json::from_value(value.clone())
                .map_err(|_| "invalid captured context shape")?;
            context.validate()?;
            Ok(Some(context))
        }
    }
}

/// Before source-item/version locks, serialize checkpoints sharing a session.
/// Uses the existing request transaction, never opens a second pool connection.
pub async fn lock_session_tx(
    tx: &mut Transaction<'_, Postgres>,
    graph: Uuid,
    session: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1,0))")
        .bind(format!("capture-session:{graph}:{session}"))
        .execute(&mut **tx)
        .await?;
    Ok(())
}

/// First-fixed ancestry is structural reported capture lineage, not a graph
/// relationship assertion. Never guess it from a title, filename or date.
pub async fn validate_ancestry_tx(
    tx: &mut Transaction<'_, Postgres>,
    graph: Uuid,
    session: &str,
    parent: Option<&str>,
) -> Result<Option<String>, String> {
    let mut seen = std::collections::HashSet::from([session.to_string()]);
    let mut next = parent.map(str::to_string);
    let mut root = session.to_string();
    for _ in 0..32 {
        let Some(id) = next.take() else {
            return Ok(Some(root));
        };
        if !seen.insert(id.clone()) {
            return Err("captured session ancestry is cyclic".into());
        }
        let row:Option<(String,Option<String>,Option<String>)>=sqlx::query_as("SELECT session_kind,parent_session_id,root_session_id FROM coding_sessions WHERE graph_id=$1 AND session_id=$2")
            .bind(graph).bind(&id).fetch_optional(&mut **tx).await.map_err(|_| "captured ancestry unavailable")?;
        let Some((kind, p, _)) = row else {
            return Err("captured parent unavailable".into());
        };
        if kind != "agent" {
            return Err("captured parent must be an agent session".into());
        }
        root = id;
        next = p;
    }
    Err("captured session ancestry exceeds32 levels".into())
}

pub async fn validate_binding_tx(
    tx: &mut Transaction<'_, Postgres>,
    graph: Uuid,
    context: &CaptureContext,
) -> Result<(), String> {
    let Some(session) = context.session_id.as_deref() else {
        return Ok(());
    };
    let row:Option<(String,Option<String>)>=sqlx::query_as("SELECT session_kind,parent_session_id FROM coding_sessions WHERE graph_id=$1 AND session_id=$2")
        .bind(graph).bind(session).fetch_optional(&mut **tx).await.map_err(|_|"captured session binding unavailable")?;
    let Some((kind, parent)) = row else {
        return Err("captured session binding missing".into());
    };
    if context.session_kind.as_deref() != Some(kind.as_str()) || context.parent_session_id != parent
    {
        return Err("captured session binding disagrees with first-fixed identity".into());
    }
    let root = validate_ancestry_tx(tx, graph, session, parent.as_deref()).await?;
    if context.root_session_id != root {
        return Err("captured root disagrees with first-fixed ancestry".into());
    }
    Ok(())
}

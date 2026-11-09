use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Atom {
    pub atom_id: Uuid,
    pub graph_id: Uuid,
    pub source_type: String,
    pub source_id: String,
    pub content_blob_key: String,
    pub participants: Vec<String>,
    pub timestamp: DateTime<Utc>,
    pub ingested_by: Option<Uuid>,
    pub scope: String,
    /// Structured provenance (see migration 0034). Source-specific: chat atoms
    /// carry channel + author; non-chat sources leave these `None`.
    #[serde(default)]
    pub channel_id: Option<String>,
    #[serde(default)]
    pub channel_name: Option<String>,
    #[serde(default)]
    pub author_id: Option<String>,
    #[serde(default)]
    pub author_name: Option<String>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Graph {
    pub id: Uuid,
    pub account_id: Uuid,
    pub name: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NewAtom {
    pub graph_id: Uuid,
    pub source_type: String,
    pub source_id: String,
    pub content_blob_key: String,
    pub participants: Vec<String>,
    pub timestamp: DateTime<Utc>,
    pub ingested_by: Option<Uuid>,
    pub scope: String,
    /// Structured provenance (see migration 0034); `None` for non-chat sources.
    #[serde(default)]
    pub channel_id: Option<String>,
    #[serde(default)]
    pub channel_name: Option<String>,
    #[serde(default)]
    pub author_id: Option<String>,
    #[serde(default)]
    pub author_name: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_atom_scope_default() {
        let a = NewAtom {
            graph_id: Uuid::new_v4(),
            source_type: "slack".into(),
            source_id: "msg_123".into(),
            content_blob_key: "graph-id/atom-id".into(),
            participants: vec!["alice@example.com".into()],
            timestamp: Utc::now(),
            ingested_by: None,
            scope: "private".into(),
            channel_id: None,
            channel_name: None,
            author_id: None,
            author_name: None,
        };
        assert_eq!(a.scope, "private");
        assert_eq!(a.source_type, "slack");
    }
}

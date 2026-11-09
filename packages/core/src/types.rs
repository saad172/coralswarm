// Shared domain types for Coralswarm.
// Stub — expand as the domain model evolves.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// A node in the knowledge graph.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Node {
    pub id: Uuid,
    pub label: String,
}

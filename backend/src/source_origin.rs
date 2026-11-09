//! Capture attribution is descriptive, never a replacement for source identity
//! or an authorization predicate. These references contain no credentials.
use serde::{Deserialize, Serialize};

macro_rules! bounded_id {
    ($name:ident, $max:expr, $slug:expr) => {
        #[derive(Debug, Clone, PartialEq, Eq, Serialize)]
        #[serde(transparent)]
        pub struct $name(String);
        impl $name {
            pub fn new(value: impl Into<String>) -> Result<Self, String> {
                let value = value.into();
                if value.is_empty()
                    || value.trim() != value
                    || value.chars().count() > $max
                    || value.chars().any(char::is_control)
                    || ($slug
                        && !value.bytes().all(|b| {
                            b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_' || b == b'-'
                        }))
                {
                    return Err(concat!("invalid ", stringify!($name)).into());
                }
                Ok(Self(value))
            }
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }
        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
                Self::new(String::deserialize(d)?).map_err(serde::de::Error::custom)
            }
        }
    };
}
bounded_id!(ServiceId, 100, true);
bounded_id!(ContentKind, 100, true);
bounded_id!(CaptureSurfaceId, 100, true);
bounded_id!(ContainerKind, 100, true);
bounded_id!(ContainerId, 512, false);
bounded_id!(ProviderNamespace, 512, false);
bounded_id!(CaptureConnectionId, 512, false);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CaptureTransport {
    Native,
    Composio,
    Synthetic,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CaptureConnectionKind {
    OauthGrant,
    ConnectorInstall,
}

/// An opaque, non-secret reference within the authorized graph. Not a
/// source_connections publication UUID and never a token or token locator.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CaptureConnectionRef {
    pub kind: CaptureConnectionKind,
    pub id: CaptureConnectionId,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceContainer {
    pub kind: ContainerKind,
    pub id: ContainerId,
    /// Qualifies provider-local IDs; labels never identify a container.
    pub provider_namespace: ProviderNamespace,
    pub label: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceOrigin {
    pub service_id: ServiceId,
    pub transport: CaptureTransport,
    pub content_kind: ContentKind,
    pub capture_surface: Option<CaptureSurfaceId>,
    pub connection: Option<CaptureConnectionRef>,
    pub container: Option<SourceContainer>,
}
impl SourceOrigin {
    pub fn native(service: &str, content_kind: &str, surface: &str) -> Result<Self, String> {
        Ok(Self {
            service_id: ServiceId::new(service)?,
            transport: CaptureTransport::Native,
            content_kind: ContentKind::new(content_kind)?,
            capture_surface: Some(CaptureSurfaceId::new(surface)?),
            connection: None,
            container: None,
        })
    }
    pub fn with_container(
        mut self,
        kind: &str,
        id: &str,
        namespace: &str,
        label: Option<&str>,
    ) -> Result<Self, String> {
        self.container = Some(SourceContainer {
            kind: ContainerKind::new(kind)?,
            id: ContainerId::new(id)?,
            provider_namespace: ProviderNamespace::new(namespace)?,
            label: label.map(str::to_owned),
        });
        self.validate()?;
        Ok(self)
    }
    /// Stable hash of the exact admitted OAuth grant key, never credentials.
    /// Blank/unverified provider account namespaces remain unknown.
    pub fn with_oauth_grant(
        mut self,
        account_id: uuid::Uuid,
        provider: &str,
        workspace: &str,
        owner: Option<uuid::Uuid>,
    ) -> Result<Self, String> {
        if workspace.trim().is_empty() {
            return Ok(self);
        }
        use sha2::{Digest, Sha256};
        ProviderNamespace::new(workspace)?;
        let key = serde_json::to_vec(&(account_id, provider, workspace, owner))
            .map_err(|e| e.to_string())?;
        self.connection = Some(CaptureConnectionRef {
            kind: CaptureConnectionKind::OauthGrant,
            id: CaptureConnectionId::new(format!(
                "grant-key-sha256:{}",
                hex::encode(Sha256::digest(key))
            ))?,
        });
        Ok(self)
    }
    /// Called before SQL, uploads or extraction enqueue. Fields with typed IDs
    /// are bounded at construction/deserialization; labels need separate bounds.
    pub fn validate(&self) -> Result<(), String> {
        if let Some(label) = self.container.as_ref().and_then(|c| c.label.as_ref()) {
            if label.trim().is_empty()
                || label.chars().count() > 256
                || label.chars().any(char::is_control)
            {
                return Err("invalid container label".into());
            }
        }
        Ok(())
    }
}

/// Only for trusted provider adapters. A source_type supplied by HTTP is never
/// enough to attest a service; unsupported provider/surface pairs stay unknown.
pub(crate) fn native_connector_origin(
    provider: &str,
    source_type: &str,
    surface: &str,
) -> Result<Option<SourceOrigin>, String> {
    let pair = match (provider, source_type) {
        ("slack" | "slack_user", "slack") => Some(("slack", "message")),
        ("google_gmail", "gmail") => Some(("gmail", "message")),
        ("google" | "google_calendar", "google_calendar") => Some(("google_calendar", "event")),
        ("microsoft", "outlook") => Some(("outlook", "message")),
        ("microsoft", "microsoft_calendar") => Some(("microsoft_calendar", "event")),
        ("microsoft", "teams" | "teams_channel" | "teams_chat") => {
            Some(("microsoft_teams", "message"))
        }
        ("microsoft", "teams_transcript") => Some(("microsoft_teams", "transcript")),
        _ => None,
    };
    pair.map(|(service, kind)| SourceOrigin::native(service, kind, surface))
        .transpose()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CapabilitySupport {
    Available,
    Unsupported,
    Unknown,
}

/// Code capability of this capture surface, not live provider health or corpus
/// completeness. New services/transports receive unknown until proved.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CaptureContract {
    pub history: CapabilitySupport,
    pub updates: CapabilitySupport,
    pub deletes: CapabilitySupport,
    pub reconnect: CapabilitySupport,
}
pub fn capture_contract(origin: Option<&SourceOrigin>) -> CaptureContract {
    use CapabilitySupport::{Unknown, Unsupported};
    let mut result = CaptureContract {
        history: Unknown,
        updates: Unknown,
        deletes: Unknown,
        reconnect: Unknown,
    };
    if origin.is_some_and(|o| {
        o.service_id.as_str() == "slack"
            && o.transport == CaptureTransport::Native
            && o.content_kind.as_str() == "message"
            && o.capture_surface
                .as_ref()
                .is_some_and(|s| s.as_str() == "slack_message_events")
    }) {
        // The events allowlist excludes ordinary message_changed/deleted.
        result.updates = Unsupported;
        result.deletes = Unsupported;
    }
    if origin.is_some_and(|o| {
        o.transport == CaptureTransport::Native
            && o.service_id.as_str() == "slack"
            && o.content_kind.as_str() == "message"
            && o.capture_surface.as_ref().is_some_and(|s| {
                matches!(
                    s.as_str(),
                    "slack_member_capture" | "slack_history_backfill"
                )
            })
    }) {
        result.history = CapabilitySupport::Available;
        result.updates = Unsupported;
        result.deletes = Unsupported;
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    #[test]
    fn typed_origin_rejects_invalid_ids_and_bounds() {
        assert!(ServiceId::new("Composio Slack").is_err());
        assert!(ContainerId::new(" ").is_err());
        assert!(ProviderNamespace::new("x".repeat(513)).is_err());
        assert!(CaptureConnectionId::new("key\nsecret").is_err());
        let value = json!({"service_id":"linear","transport":"composio","content_kind":"issue",
            "container":{"kind":"project","id":"P1","provider_namespace":"linear:workspace:W1","label":"a".repeat(257)}});
        let origin: SourceOrigin = serde_json::from_value(value).unwrap();
        assert!(origin.validate().is_err());
    }
    #[test]
    fn grant_references_preserve_workspace_and_owner_boundaries() {
        let account = uuid::Uuid::new_v4();
        let owner = uuid::Uuid::new_v4();
        let origin = |workspace, owner| {
            SourceOrigin::native("slack", "message", "slack_member_capture")
                .unwrap()
                .with_oauth_grant(account, "slack_user", workspace, owner)
                .unwrap()
        };
        let a = origin("T1", Some(owner));
        assert_eq!(a.connection, origin("T1", Some(owner)).connection);
        assert_ne!(a.connection, origin("T2", Some(owner)).connection);
        assert_ne!(a.connection, origin("T1", None).connection);
        assert!(origin("", Some(owner)).connection.is_none());
        let synthetic:SourceOrigin=serde_json::from_value(json!({"service_id":"linear","transport":"synthetic","content_kind":"issue","capture_surface":"synthetic_demo"})).unwrap();
        assert_eq!(
            capture_contract(Some(&synthetic)).history,
            CapabilitySupport::Unknown
        );
    }
    #[test]
    fn capability_declaration_is_capture_surface_specific() {
        let origin = |transport, surface| {
            serde_json::from_value::<SourceOrigin>(json!({
            "service_id":"slack","transport":transport,"content_kind":"message","capture_surface":surface
        })).unwrap()
        };
        assert_eq!(capture_contract(None).updates, CapabilitySupport::Unknown);
        assert_eq!(
            capture_contract(Some(&origin("native", "slack_message_events"))).updates,
            CapabilitySupport::Unsupported
        );
        assert_eq!(
            capture_contract(Some(&origin("native", "member_capture"))).updates,
            CapabilitySupport::Unknown
        );
        assert_eq!(
            capture_contract(Some(&origin("composio", "slack_message_events"))).updates,
            CapabilitySupport::Unknown
        );
        assert_eq!(
            capture_contract(Some(&origin("native", "slack_message_events"))).history,
            CapabilitySupport::Unknown
        );
    }
}

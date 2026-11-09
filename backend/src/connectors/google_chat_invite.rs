//! Chat invite classification (org-onboarding plan items 22 and 41).
//!
//! The worker in `workers::google_chat_invite` does the roster walk and the
//! sends. This module decides, for one roster member, which email (if any)
//! they get, and whether a personal invite may call the People API.
//!
//! Chat itself never returns an address. A live directory is the only source.
//! `EXTERNAL` / `MANAGED_EXTERNAL` are not invited (item 42). Directory plus
//! every verified org domain is consulted only when a `HUMAN` row has no
//! affiliation.

use uuid::Uuid;

use super::google_chat::{classify_chat_invite_seat, ChatInviteSeat};

/// `(provider, email)` key for the "create your own account" email.
/// Deliberately not `slack`: that provider's cooldown is a different row.
pub(crate) const CHAT_UNMATCHED_PROVIDER: &str = "google_chat";

/// `(provider, email)` key for the "join this org" email. Distinct from
/// `slack_join` so a Slack invite and a Chat invite do not consume each
/// other's window. The membership class on the token is still
/// [`chat_join_membership_class`], never Slack full membership.
pub(crate) const CHAT_JOIN_PROVIDER: &str = "google_chat_join";

/// The connect nudge claims `member_connect`, the same provider the hourly
/// `workers::member_connect_nudge` claims, so the two cannot both email one
/// person inside one cooldown window.
pub(crate) fn chat_connect_notice_provider() -> &'static str {
    crate::services::unmatched_recipient_email::MEMBER_CONNECT_PROVIDER
}

/// Chat cannot claim Slack full membership. `Unavailable` mints no `cls`
/// claim, which is the same choice the Teams join path makes.
pub(crate) fn chat_join_membership_class() -> crate::connectors::slack::MembershipClass {
    crate::connectors::slack::MembershipClass::Unavailable
}

/// `true` only when the stored grant is a known Chat scope set that already
/// includes `directory.readonly`. A blank grant and a pre-widening grant
/// both refuse the invite walk.
pub(crate) fn chat_invite_directory_grant_is_live(stored_scope: &str) -> bool {
    match super::google::chat_missing_scopes(stored_scope) {
        Some(missing) => !missing
            .iter()
            .any(|scope| scope.ends_with("/directory.readonly")),
        None => false,
    }
}

/// Item 41. A personal invite fetches the directory even when the space's
/// destination graph is a personal ocean rather than a team ocean. The
/// graph kind is intentionally ignored. The fetch happens only when the
/// invite does not already have an address to send — it does not become a
/// reason to walk identity or seats for every personal graph.
pub(crate) fn personal_invite_fetches_directory(graph_kind: &str, known_addresses: usize) -> bool {
    let _ = graph_kind;
    known_addresses == 0
}

/// What the invite worker should send, if anything.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ChatInviteAudience {
    /// No CoralSwarm user at this address.
    Unmatched { email: String },
    /// A CoralSwarm user who is not a member of this org.
    JoinThisOrg { user_id: Uuid, email: String },
    /// An org member who has not connected Chat.
    ConnectNudge { user_id: Uuid, email: String },
}

/// Identity facts the worker looked up. Kept out of this function so the
/// three audiences are testable without a database.
#[derive(Debug, Clone, Copy)]
pub(crate) struct ChatInviteIdentity {
    pub(crate) user_id: Option<Uuid>,
    pub(crate) is_org_member: bool,
    pub(crate) chat_connected: bool,
}

/// Classify one roster member.
///
/// `directory_email` is `Err` when the People API could not be read,
/// `Ok(None)` when it was read and this person is absent, and `Ok(Some)`
/// when it returned an address. `directory_live` is the job-level gate: a
/// failed or empty directory invites nobody, including someone whose
/// affiliation is `INTERNAL`.
#[allow(clippy::too_many_arguments)]
pub(crate) fn classify_chat_invite_audience(
    user_type: &str,
    affiliation: &str,
    resource_name: &str,
    directory_email: Result<Option<&str>, ()>,
    anchor_domains: &[String],
    directory_live: bool,
    account_has_org: bool,
    identity: ChatInviteIdentity,
) -> Option<ChatInviteAudience> {
    if !user_type.eq_ignore_ascii_case("HUMAN") {
        return None;
    }
    let seat = classify_chat_invite_seat(
        user_type,
        affiliation,
        resource_name,
        "",
        directory_email,
        anchor_domains,
    );
    if !matches!(seat, ChatInviteSeat::Eligible) {
        return None;
    }
    if !directory_live {
        return None;
    }
    let email = directory_email
        .ok()
        .flatten()
        .map(str::trim)
        .filter(|value| !value.is_empty())?;
    let email = crate::connectors::slack::normalize_email(email);
    if email.is_empty() {
        return None;
    }
    match identity.user_id {
        None => Some(ChatInviteAudience::Unmatched { email }),
        Some(user_id) if identity.is_org_member && !identity.chat_connected => {
            Some(ChatInviteAudience::ConnectNudge { user_id, email })
        }
        Some(_) if identity.is_org_member => None,
        Some(user_id) if account_has_org => {
            Some(ChatInviteAudience::JoinThisOrg { user_id, email })
        }
        Some(_) => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connectors::slack::MembershipClass;

    fn nobody() -> ChatInviteIdentity {
        ChatInviteIdentity {
            user_id: None,
            is_org_member: false,
            chat_connected: false,
        }
    }

    fn user(member: bool, connected: bool) -> ChatInviteIdentity {
        ChatInviteIdentity {
            user_id: Some(Uuid::nil()),
            is_org_member: member,
            chat_connected: connected,
        }
    }

    fn example_domain() -> Vec<String> {
        vec!["example.com".to_string()]
    }

    #[test]
    fn unmatched_person_gets_the_create_account_email() {
        let audience = classify_chat_invite_audience(
            "HUMAN",
            "INTERNAL",
            "users/1",
            Ok(Some("New.Person@Example.com")),
            &example_domain(),
            true,
            true,
            nobody(),
        );
        assert_eq!(
            audience,
            Some(ChatInviteAudience::Unmatched {
                email: "new.person@example.com".into()
            })
        );
    }

    #[test]
    fn coral_user_who_is_not_an_org_member_gets_the_join_email() {
        let audience = classify_chat_invite_audience(
            "HUMAN",
            "INTERNAL",
            "users/2",
            Ok(Some("dana@example.com")),
            &example_domain(),
            true,
            true,
            user(false, false),
        );
        assert_eq!(
            audience,
            Some(ChatInviteAudience::JoinThisOrg {
                user_id: Uuid::nil(),
                email: "dana@example.com".into(),
            })
        );
    }

    #[test]
    fn org_member_without_chat_gets_the_connect_nudge() {
        let audience = classify_chat_invite_audience(
            "HUMAN",
            "INTERNAL",
            "users/3",
            Ok(Some("sam@example.com")),
            &[],
            true,
            true,
            user(true, false),
        );
        assert_eq!(
            audience,
            Some(ChatInviteAudience::ConnectNudge {
                user_id: Uuid::nil(),
                email: "sam@example.com".into(),
            })
        );
        assert_eq!(
            chat_connect_notice_provider(),
            crate::services::unmatched_recipient_email::MEMBER_CONNECT_PROVIDER
        );
    }

    #[test]
    fn external_and_managed_external_are_not_invited() {
        for affiliation in ["EXTERNAL", "MANAGED_EXTERNAL"] {
            let audience = classify_chat_invite_audience(
                "HUMAN",
                affiliation,
                "users/9",
                Ok(Some("guest@example.com")),
                &example_domain(),
                true,
                true,
                nobody(),
            );
            assert_eq!(audience, None, "{affiliation} must not be invited");
        }
    }

    #[test]
    fn directory_and_domain_apply_only_when_a_human_lacks_affiliation() {
        let off_domain = classify_chat_invite_audience(
            "HUMAN",
            "",
            "users/4",
            Ok(Some("pat@other.test")),
            &example_domain(),
            true,
            true,
            nobody(),
        );
        assert_eq!(
            off_domain, None,
            "a missing affiliation off the anchor domain is not invited"
        );

        let on_domain = classify_chat_invite_audience(
            "HUMAN",
            "",
            "users/4",
            Ok(Some("pat@example.com")),
            &example_domain(),
            true,
            true,
            nobody(),
        );
        assert_eq!(
            on_domain,
            Some(ChatInviteAudience::Unmatched {
                email: "pat@example.com".into()
            })
        );

        let internal_off_domain = classify_chat_invite_audience(
            "HUMAN",
            "INTERNAL",
            "users/5",
            Ok(Some("in@other.test")),
            &example_domain(),
            true,
            true,
            nobody(),
        );
        assert_eq!(
            internal_off_domain,
            Some(ChatInviteAudience::Unmatched {
                email: "in@other.test".into()
            }),
            "INTERNAL does not consult the anchor domain"
        );
    }

    #[test]
    fn a_dead_directory_invites_nobody() {
        let audience = classify_chat_invite_audience(
            "HUMAN",
            "INTERNAL",
            "users/1",
            Ok(Some("new.person@example.com")),
            &example_domain(),
            false,
            true,
            nobody(),
        );
        assert_eq!(audience, None);
    }

    #[test]
    fn a_personal_account_does_not_send_a_join_email() {
        let audience = classify_chat_invite_audience(
            "HUMAN",
            "INTERNAL",
            "users/2",
            Ok(Some("dana@example.com")),
            &[],
            true,
            false,
            user(false, false),
        );
        assert_eq!(audience, None);
    }

    #[test]
    fn join_notice_does_not_claim_slack_full_membership() {
        let class = chat_join_membership_class();
        assert_ne!(class, MembershipClass::FullMember);
        assert_eq!(class.label(), None, "Unavailable mints no cls claim");

        let secret = "test-join-notice-secret-0123456789";
        let token = crate::services::unmatched_recipient_email::mint_join_notice_token(
            secret,
            Uuid::nil(),
            Uuid::nil(),
            "org_acme",
            "dana@example.com",
            Some("workspaces/1"),
            Some("spaces/1"),
            Some("Acme"),
            class,
        )
        .expect("mint");
        let ctx =
            crate::services::unmatched_recipient_email::verify_join_notice_token(secret, &token)
                .expect("verify");
        assert_eq!(ctx.membership_class, None);
    }

    #[test]
    fn notice_providers_are_not_slack() {
        assert_ne!(CHAT_UNMATCHED_PROVIDER, "slack");
        assert_ne!(CHAT_JOIN_PROVIDER, "slack");
        assert_ne!(CHAT_JOIN_PROVIDER, "slack_join");
        assert_ne!(chat_connect_notice_provider(), "slack");
    }

    #[test]
    fn personal_invite_fetches_directory_regardless_of_graph_kind_only_without_addresses() {
        for kind in ["personal", "team", "unspecified"] {
            assert!(
                personal_invite_fetches_directory(kind, 0),
                "{kind} with no addresses fetches"
            );
            assert!(
                !personal_invite_fetches_directory(kind, 1),
                "{kind} that already has an address does not fetch"
            );
        }
    }

    #[test]
    fn directory_grant_requires_directory_readonly() {
        let live = "openid email https://www.googleapis.com/auth/chat.memberships.readonly \
                    https://www.googleapis.com/auth/chat.messages.readonly \
                    https://www.googleapis.com/auth/chat.spaces.readonly \
                    https://www.googleapis.com/auth/directory.readonly";
        assert!(chat_invite_directory_grant_is_live(live));
        assert!(!chat_invite_directory_grant_is_live(
            "https://www.googleapis.com/auth/chat.spaces.readonly"
        ));
        assert!(!chat_invite_directory_grant_is_live(""));
    }
}

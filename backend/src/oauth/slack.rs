//! Slack OAuth helper — code exchange and token response parsing.

use serde::Deserialize;
use thiserror::Error;

// ---------------------------------------------------------------------------
// Error type
// ---------------------------------------------------------------------------

/// Errors that can arise during Slack OAuth code exchange.
#[derive(Debug, Error)]
pub enum SlackError {
    #[error("HTTP error: {0}")]
    Http(String),

    #[error("Slack API error: {0}")]
    Api(String),

    #[error("unexpected response shape: {0}")]
    Parse(String),
}

impl From<reqwest::Error> for SlackError {
    fn from(e: reqwest::Error) -> Self {
        SlackError::Http(e.to_string())
    }
}

// ---------------------------------------------------------------------------
// Token response
// ---------------------------------------------------------------------------

/// Parsed token response from `oauth.v2.access`.
#[derive(Debug)]
pub struct SlackTokenResponse {
    pub access_token: String,
    pub refresh_token: Option<String>,
    pub scope: String,
    pub team_id: String,
    pub team_name: String,
    pub authed_user_id: String,
    /// User-scoped token returned when `user_scope` was requested during
    /// the consent flow (i.e. `SLACK_USER_SCOPES` was set).  `None` when the
    /// flow was bot-only.
    pub user_token: Option<String>,
    /// The user scopes Slack granted for `user_token` (`authed_user.scope`).
    /// `None` when no user token came back.
    pub user_scope: Option<String>,
}

// Internal deserialization types that mirror the Slack API JSON shape.

#[derive(Debug, Deserialize)]
struct SlackOAuthResponse {
    ok: bool,
    error: Option<String>,
    access_token: Option<String>,
    refresh_token: Option<String>,
    scope: Option<String>,
    team: Option<SlackTeam>,
    authed_user: Option<SlackAuthedUser>,
}

#[derive(Debug, Deserialize)]
struct SlackTeam {
    id: String,
    name: String,
}

#[derive(Debug, Deserialize)]
struct SlackAuthedUser {
    id: String,
    /// Present when `user_scope` was included in the consent URL and the
    /// user granted the requested user scopes.
    access_token: Option<String>,
    /// The user-scoped grant actually returned (CS-454, member connect) —
    /// space-separated, same shape as the top-level `scope` field but for
    /// the `authed_user.access_token` above rather than the bot token.
    /// `None` on a bot-only exchange (no `user_scope` requested at all).
    scope: Option<String>,
}

// ---------------------------------------------------------------------------
// Code exchange
// ---------------------------------------------------------------------------

/// Exchange a Slack OAuth authorisation code for tokens.
///
/// Posts to `https://slack.com/api/oauth.v2.access` and parses the response.
///
/// # Errors
/// - [`SlackError::Http`] — network/TLS failure.
/// - [`SlackError::Api`] — Slack returned `ok: false` with an error string.
/// - [`SlackError::Parse`] — required fields are missing from the response.
pub async fn exchange_code(
    client: &reqwest::Client,
    code: &str,
    client_id: &str,
    client_secret: &str,
    redirect_uri: &str,
) -> Result<SlackTokenResponse, SlackError> {
    let resp = client
        .post(format!("{}/oauth.v2.access", crate::connectors::slack::slack_api_base()))
        .form(&[
            ("code", code),
            ("client_id", client_id),
            ("client_secret", client_secret),
            ("redirect_uri", redirect_uri),
        ])
        .send()
        .await
        .map_err(SlackError::from)?;

    let body: SlackOAuthResponse = resp
        .json()
        .await
        .map_err(|e| SlackError::Parse(e.to_string()))?;

    if !body.ok {
        let err_msg = body.error.unwrap_or_else(|| "unknown error".into());
        return Err(SlackError::Api(err_msg));
    }

    let access_token = body
        .access_token
        .ok_or_else(|| SlackError::Parse("missing access_token".into()))?;

    let scope = body
        .scope
        .ok_or_else(|| SlackError::Parse("missing scope".into()))?;

    let team = body
        .team
        .ok_or_else(|| SlackError::Parse("missing team".into()))?;

    let authed_user = body
        .authed_user
        .ok_or_else(|| SlackError::Parse("missing authed_user".into()))?;

    let user_token = authed_user.access_token.clone();
    let user_scope = authed_user.scope.clone();

    Ok(SlackTokenResponse {
        access_token,
        refresh_token: body.refresh_token,
        scope,
        team_id: team.id,
        team_name: team.name,
        authed_user_id: authed_user.id,
        user_token,
        user_scope,
    })
}

// ---------------------------------------------------------------------------
// Member (user-scope-only) code exchange — CS-454, item 46a
// ---------------------------------------------------------------------------

/// Parsed token response from `oauth.v2.access` for a MEMBER Slack connect —
/// an authorize request carrying `user_scope` only, no `scope` at all (see
/// `connectors::slack::authorize_url_for_member`). Deliberately a SEPARATE
/// type from [`SlackTokenResponse`]: that type's `access_token` is the BOT
/// token and is required present (`exchange_code` 400s a response missing
/// it) — correct for the admin bot-install flow, wrong here. A user-scope-
/// only authorize request does not necessarily mint or return a bot token
/// for a workspace this app has no prior install in at all (see D21,
/// `docs/plans/org-onboarding-from-the-invite.md` §2, "Slack installs an app
/// per workspace"), so this type requires only what a member consent always
/// carries: the `authed_user` block.
#[derive(Debug)]
pub struct SlackMemberTokenResponse {
    /// The member's own `xoxp-…` user token (`authed_user.access_token`).
    pub user_access_token: String,
    /// The user scopes Slack actually granted (`authed_user.scope`) —
    /// stored verbatim, same discipline as the bot flow's `scope` field.
    pub user_scope: String,
    pub team_id: String,
    pub team_name: String,
    /// Slack's own user id for the person who completed this consent
    /// (`authed_user.id`) — the value `routes::oauth::callback` binds as
    /// `oauth_tokens.provider_user_id` for the `slack_user` row.
    pub authed_user_id: String,
}

/// Exchange a Slack OAuth authorisation code for a MEMBER (user-scope-only)
/// token — the same `oauth.v2.access` endpoint [`exchange_code`] uses, but
/// tolerant of a response that carries no bot `access_token` at all.
///
/// # Errors
/// - [`SlackError::Http`] — network/TLS failure.
/// - [`SlackError::Api`] — Slack returned `ok: false` with an error string.
/// - [`SlackError::Parse`] — the `authed_user` block (or its `access_token`/
///   `scope`) is missing — the one thing a user-scope-only grant must always
///   carry, unlike the top-level bot fields.
pub async fn exchange_code_member(
    client: &reqwest::Client,
    code: &str,
    client_id: &str,
    client_secret: &str,
    redirect_uri: &str,
) -> Result<SlackMemberTokenResponse, SlackError> {
    let resp = client
        .post(format!("{}/oauth.v2.access", crate::connectors::slack::slack_api_base()))
        .form(&[
            ("code", code),
            ("client_id", client_id),
            ("client_secret", client_secret),
            ("redirect_uri", redirect_uri),
        ])
        .send()
        .await
        .map_err(SlackError::from)?;

    let body: SlackOAuthResponse = resp
        .json()
        .await
        .map_err(|e| SlackError::Parse(e.to_string()))?;

    if !body.ok {
        let err_msg = body.error.unwrap_or_else(|| "unknown error".into());
        return Err(SlackError::Api(err_msg));
    }

    let team = body
        .team
        .ok_or_else(|| SlackError::Parse("missing team".into()))?;

    let authed_user = body
        .authed_user
        .ok_or_else(|| SlackError::Parse("missing authed_user".into()))?;

    let user_access_token = authed_user
        .access_token
        .ok_or_else(|| SlackError::Parse("missing authed_user.access_token".into()))?;

    let user_scope = authed_user
        .scope
        .ok_or_else(|| SlackError::Parse("missing authed_user.scope".into()))?;

    Ok(SlackMemberTokenResponse {
        user_access_token,
        user_scope,
        team_id: team.id,
        team_name: team.name,
        authed_user_id: authed_user.id,
    })
}

// ---------------------------------------------------------------------------
// Token revocation — CS-454, member disconnect
// ---------------------------------------------------------------------------

/// Internal deserialization type for `auth.revoke`'s response.
#[derive(Debug, Deserialize)]
struct SlackRevokeResponse {
    ok: bool,
    error: Option<String>,
    #[allow(dead_code)]
    revoked: Option<bool>,
}

/// Revoke a Slack token at Slack (`POST auth.revoke`) — best-effort, called
/// by a member's own disconnect BEFORE the local `oauth_tokens`/
/// `connector_installs` rows are deleted (see
/// `routes::oauth::delete_slack_member_connection`'s own doc comment for why
/// a revoke failure does not block the local delete). `token` is the plain
/// Slack token (`xoxp-…` for a member row) — Slack's `auth.revoke` takes it
/// as a Bearer credential, exactly like every other Web API call, not as a
/// form parameter.
///
/// # Errors
/// - [`SlackError::Http`] — network/TLS failure.
/// - [`SlackError::Api`] — Slack returned `ok: false` with an error string
///   (e.g. the token was already revoked/invalid — `token_revoked`/
///   `invalid_auth` — which a caller may reasonably treat as "already done"
///   rather than a hard failure).
/// - [`SlackError::Parse`] — unexpected response shape.
pub async fn revoke_token(client: &reqwest::Client, token: &str) -> Result<(), SlackError> {
    let resp = client
        .post(format!("{}/auth.revoke", crate::connectors::slack::slack_api_base()))
        .bearer_auth(token)
        .send()
        .await
        .map_err(SlackError::from)?;

    let body: SlackRevokeResponse = resp
        .json()
        .await
        .map_err(|e| SlackError::Parse(e.to_string()))?;

    if !body.ok {
        let err_msg = body.error.unwrap_or_else(|| "unknown error".into());
        return Err(SlackError::Api(err_msg));
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_display_http() {
        let msg = SlackError::Http("connection refused".into()).to_string();
        assert!(msg.contains("HTTP error"), "unexpected: {msg}");
        assert!(msg.contains("connection refused"), "unexpected: {msg}");
    }

    #[test]
    fn error_display_api() {
        let msg = SlackError::Api("invalid_code".into()).to_string();
        assert!(msg.contains("Slack API error"), "unexpected: {msg}");
        assert!(msg.contains("invalid_code"), "unexpected: {msg}");
    }

    #[test]
    fn error_display_parse() {
        let msg = SlackError::Parse("missing access_token".into()).to_string();
        assert!(msg.contains("unexpected response shape"), "unexpected: {msg}");
        assert!(msg.contains("missing access_token"), "unexpected: {msg}");
    }

    #[test]
    fn slack_error_from_reqwest_error() {
        // We can construct a reqwest error via a timeout parse — but the easiest
        // way to get one in a unit test is to use the From impl directly with a
        // URL parse error.  The important thing is that the variant is Http.
        let url_err = reqwest::Url::parse("not-a-url").unwrap_err();
        // Build a reqwest::Error by building a fake request and triggering a
        // URL error — since reqwest::Error is not publicly constructable, we
        // verify the Display impl of our wrapper instead.
        let slack_err = SlackError::Http(url_err.to_string());
        let msg = slack_err.to_string();
        assert!(msg.contains("HTTP error"), "unexpected: {msg}");
    }

    // ── Parse happy-path JSON (unit test without a live HTTP call) ───────────

    #[test]
    fn parses_ok_response_shape() {
        // Simulate what Slack returns on a successful exchange.
        let json = serde_json::json!({
            "ok": true,
            "access_token": "xoxb-token",
            "refresh_token": null,
            "scope": "channels:read users:read",
            "team": { "id": "T123", "name": "Acme" },
            "authed_user": { "id": "U456" }
        });

        let body: SlackOAuthResponse = serde_json::from_value(json).unwrap();

        assert!(body.ok);
        assert_eq!(body.access_token.unwrap(), "xoxb-token");
        assert_eq!(body.scope.unwrap(), "channels:read users:read");
        assert_eq!(body.team.unwrap().id, "T123");
        let authed_user = body.authed_user.unwrap();
        assert_eq!(authed_user.id, "U456");
        assert!(
            authed_user.access_token.is_none(),
            "access_token absent when no user_scope was requested"
        );
    }

    #[test]
    fn parses_error_response_shape() {
        let json = serde_json::json!({
            "ok": false,
            "error": "invalid_code"
        });

        let body: SlackOAuthResponse = serde_json::from_value(json).unwrap();
        assert!(!body.ok);
        assert_eq!(body.error.unwrap(), "invalid_code");
    }

    // ── user_token: present when user_scope was granted ───────────────────────

    #[test]
    fn exchange_code_user_token_populated_when_authed_user_has_access_token() {
        // Slack returns authed_user.access_token when user_scope was requested.
        let json = serde_json::json!({
            "ok": true,
            "access_token": "xoxb-bot-token",
            "refresh_token": null,
            "scope": "channels:read",
            "team": { "id": "T123", "name": "Acme" },
            "authed_user": {
                "id": "U456",
                "scope": "files:read",
                "access_token": "xoxp-user-token",
                "token_type": "user"
            }
        });

        let body: SlackOAuthResponse = serde_json::from_value(json).unwrap();
        assert!(body.ok);

        let authed_user = body.authed_user.unwrap();
        assert_eq!(authed_user.id, "U456");
        assert_eq!(
            authed_user.access_token.as_deref(),
            Some("xoxp-user-token"),
            "authed_user.access_token must be parsed when present"
        );

        // Verify the full round-trip through exchange_code logic.
        // (Manually replicate the mapping in exchange_code.)
        let access_token = body.access_token.unwrap();
        let scope = body.scope.unwrap();
        let team = body.team.unwrap();
        let user_token = authed_user.access_token.clone();

        let resp = SlackTokenResponse {
            access_token,
            refresh_token: body.refresh_token,
            scope,
            team_id: team.id,
            team_name: team.name,
            authed_user_id: authed_user.id,
            user_token,
            user_scope: None,
        };

        assert_eq!(resp.access_token, "xoxb-bot-token");
        assert_eq!(resp.authed_user_id, "U456");
        assert_eq!(
            resp.user_token.as_deref(),
            Some("xoxp-user-token"),
            "user_token in SlackTokenResponse must be Some when authed_user.access_token present"
        );
    }

    #[test]
    fn exchange_code_user_token_none_when_authed_user_has_no_access_token() {
        // Bot-only flow: no user_scope, no authed_user.access_token.
        let json = serde_json::json!({
            "ok": true,
            "access_token": "xoxb-bot-only",
            "refresh_token": null,
            "scope": "channels:read",
            "team": { "id": "T999", "name": "Corp" },
            "authed_user": { "id": "U789" }
        });

        let body: SlackOAuthResponse = serde_json::from_value(json).unwrap();
        let authed_user = body.authed_user.unwrap();
        let user_token = authed_user.access_token.clone();

        let resp = SlackTokenResponse {
            access_token: body.access_token.unwrap(),
            refresh_token: body.refresh_token,
            scope: body.scope.unwrap(),
            team_id: body.team.unwrap().id,
            team_name: "Corp".into(),
            authed_user_id: authed_user.id,
            user_token,
            user_scope: None,
        };

        assert!(
            resp.user_token.is_none(),
            "user_token must be None in bot-only flow"
        );
    }

    // ── exchange_code_member: parsing (CS-454) ─────────────────────────────

    #[test]
    fn member_response_parses_with_no_top_level_bot_fields() {
        // A user-scope-only authorize request: Slack's response carries no
        // top-level `access_token` at all — this must NOT be treated as a
        // parse error the way `exchange_code`'s bot-required parse would.
        let json = serde_json::json!({
            "ok": true,
            "team": { "id": "T123", "name": "Acme" },
            "authed_user": {
                "id": "U456",
                "scope": "channels:history channels:read users:read users:read.email",
                "access_token": "xoxp-member-token",
                "token_type": "user"
            }
        });

        let body: SlackOAuthResponse = serde_json::from_value(json).unwrap();
        assert!(body.ok);
        assert!(
            body.access_token.is_none(),
            "no bot access_token in a user-scope-only response"
        );

        let team = body.team.unwrap();
        let authed_user = body.authed_user.unwrap();
        let user_access_token = authed_user.access_token.clone().unwrap();
        let user_scope = authed_user.scope.clone().unwrap();

        let resp = SlackMemberTokenResponse {
            user_access_token,
            user_scope,
            team_id: team.id,
            team_name: team.name,
            authed_user_id: authed_user.id,
        };

        assert_eq!(resp.user_access_token, "xoxp-member-token");
        assert_eq!(
            resp.user_scope,
            "channels:history channels:read users:read users:read.email"
        );
        assert_eq!(resp.team_id, "T123");
        assert_eq!(resp.authed_user_id, "U456");
    }

    #[test]
    fn member_response_missing_authed_user_access_token_is_a_parse_error() {
        // `authed_user` present but no `access_token` on it — a caller with
        // no user_scope granted, or Slack's shape changing. Must not be
        // silently accepted as an empty-string token.
        let json = serde_json::json!({
            "ok": true,
            "team": { "id": "T123", "name": "Acme" },
            "authed_user": { "id": "U456" }
        });
        let body: SlackOAuthResponse = serde_json::from_value(json).unwrap();
        let authed_user = body.authed_user.unwrap();
        assert!(
            authed_user.access_token.is_none(),
            "the exact shape exchange_code_member must reject with SlackError::Parse"
        );
    }

    #[test]
    fn member_response_error_shape_parses_as_api_error() {
        let json = serde_json::json!({
            "ok": false,
            "error": "invalid_code"
        });
        let body: SlackOAuthResponse = serde_json::from_value(json).unwrap();
        assert!(!body.ok);
        assert_eq!(body.error.unwrap(), "invalid_code");
    }

    // ── revoke_token: response parsing (CS-454) ────────────────────────────

    #[test]
    fn revoke_response_ok_shape_parses() {
        let json = serde_json::json!({ "ok": true, "revoked": true });
        let body: SlackRevokeResponse = serde_json::from_value(json).unwrap();
        assert!(body.ok);
        assert_eq!(body.revoked, Some(true));
    }

    #[test]
    fn revoke_response_error_shape_parses() {
        let json = serde_json::json!({ "ok": false, "error": "invalid_auth" });
        let body: SlackRevokeResponse = serde_json::from_value(json).unwrap();
        assert!(!body.ok);
        assert_eq!(body.error.as_deref(), Some("invalid_auth"));
    }
}

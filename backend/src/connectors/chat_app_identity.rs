//! Chat app-identity write path (org-onboarding plan item 27).
//!
//! `spaces.messages.create` under app authentication: a service-account
//! assertion scoped to `https://www.googleapis.com/auth/chat.bot`. That
//! scope is not a grant on anyone's OAuth consent, and this module never
//! reads `oauth_tokens`. A content-free outbound notice is not an answer
//! (I11, item 28).

use serde::Deserialize;
use serde::Serialize;

use super::ConnectorError;

/// The only scope this write path mints. Google documents it as app
/// authentication only: user credentials and domain-wide delegation cannot
/// use it.
pub(crate) const CHAT_BOT_SCOPE: &str = "https://www.googleapis.com/auth/chat.bot";

const TOKEN_AUDIENCE: &str = "https://oauth2.googleapis.com/token";
const ASSERTION_TTL_SECS: i64 = 3600;

#[derive(Serialize)]
struct AssertionClaims<'a> {
    iss: &'a str,
    scope: &'static str,
    aud: &'static str,
    iat: i64,
    exp: i64,
}

#[derive(Deserialize)]
struct ServiceAccountKey {
    client_email: String,
    private_key: String,
}

#[derive(Deserialize)]
struct TokenResponse {
    access_token: String,
}

/// A member-delegated Chat message scope. Rejected for this write path.
pub(crate) fn scope_is_delegated_user_message_scope(scope: &str) -> bool {
    scope.split_whitespace().any(|token| {
        let name = token.rsplit('/').next().unwrap_or(token);
        name == "chat.messages" || name == "chat.messages.create"
    })
}

pub(crate) fn chat_bot_configured() -> bool {
    service_account_material().is_some()
}

fn env_nonempty(key: &str) -> Option<String> {
    std::env::var(key)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn service_account_material() -> Option<String> {
    if let Some(raw) = env_nonempty("GOOGLE_CHAT_BOT_SERVICE_ACCOUNT_JSON") {
        return Some(raw);
    }
    let path = env_nonempty("GOOGLE_CHAT_BOT_SERVICE_ACCOUNT_FILE")?;
    match std::fs::read_to_string(&path) {
        Ok(raw) if !raw.trim().is_empty() => Some(raw),
        Ok(_) => None,
        Err(e) => {
            tracing::warn!(error = %e, "chat app identity: service account file could not be read");
            None
        }
    }
}

pub(crate) fn encode_chat_bot_assertion(
    email: &str,
    pem: &str,
    now: i64,
) -> Result<String, ConnectorError> {
    if scope_is_delegated_user_message_scope(CHAT_BOT_SCOPE) {
        return Err(ConnectorError::Internal(
            "chat.bot assertion was built with a delegated user scope".into(),
        ));
    }
    let claims = AssertionClaims {
        iss: email,
        scope: CHAT_BOT_SCOPE,
        aud: TOKEN_AUDIENCE,
        iat: now,
        exp: now + ASSERTION_TTL_SECS,
    };
    let key = encoding_key(pem)?;
    jsonwebtoken::encode(
        &jsonwebtoken::Header::new(jsonwebtoken::Algorithm::RS256),
        &claims,
        &key,
    )
    .map_err(|e| ConnectorError::Internal(format!("failed to mint chat.bot assertion: {e}")))
}

fn encoding_key(pem: &str) -> Result<jsonwebtoken::EncodingKey, ConnectorError> {
    // `from_rsa_pem` accepts both PKCS#1 (`BEGIN RSA PRIVATE KEY`, the test
    // helper) and PKCS#8 (`BEGIN PRIVATE KEY`, a Google service-account key).
    jsonwebtoken::EncodingKey::from_rsa_pem(pem.as_bytes()).map_err(|_| {
        ConnectorError::NotConfigured(
            "GOOGLE_CHAT_BOT service account key is not a valid RSA PEM".into(),
        )
    })
}

pub(crate) fn valid_space_name(space_name: &str) -> bool {
    let Some(id) = space_name.strip_prefix("spaces/") else {
        return false;
    };
    !id.is_empty()
        && id
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
}

fn valid_request_id(request_id: &str) -> bool {
    !request_id.is_empty()
        && request_id.len() <= 64
        && request_id
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-')
}

pub(crate) fn message_create_url(
    api_base: &str,
    space_name: &str,
    request_id: &str,
) -> Result<String, ConnectorError> {
    if !valid_space_name(space_name) || !valid_request_id(request_id) {
        return Err(ConnectorError::Exchange(
            "refusing spaces.messages.create for an unsafe space or request id".into(),
        ));
    }
    Ok(format!(
        "{}/{space_name}/messages?requestId={request_id}",
        api_base.trim_end_matches('/')
    ))
}

pub(crate) fn cold_start_request_id(
    account_id: uuid::Uuid,
    space_name: &str,
    yyyymmdd: u32,
) -> String {
    let space: String = space_name
        .strip_prefix("spaces/")
        .unwrap_or(space_name)
        .chars()
        .filter(|c| c.is_ascii_alphanumeric())
        .take(16)
        .collect();
    let account = account_id.simple_hex();
    format!("cs-{}-{}-{yyyymmdd}", &account[..12], space)
}

trait SimpleHex {
    fn simple_hex(&self) -> String;
}

impl SimpleHex for uuid::Uuid {
    fn simple_hex(&self) -> String {
        self.to_string().replace('-', "")
    }
}

fn chat_api_base() -> String {
    env_nonempty("GOOGLE_CHAT_API_BASE")
        .unwrap_or_else(|| "https://chat.googleapis.com/v1".to_string())
}

fn token_endpoint() -> String {
    env_nonempty("GOOGLE_OAUTH_TOKEN_URL").unwrap_or_else(|| TOKEN_AUDIENCE.to_string())
}

pub(crate) enum AppPost {
    Posted,
    NotInSpace,
}

/// A content-free outbound notice is not an answer (I11, item 28).
/// `text` is the caller's fixed connect sentence. This function does not
/// read a member credential or any captured message.
pub(crate) async fn post_app_message(
    http: &reqwest::Client,
    space_name: &str,
    text: &str,
    request_id: &str,
) -> Result<AppPost, ConnectorError> {
    let token = mint_app_access_token(http).await?;
    let url = message_create_url(&chat_api_base(), space_name, request_id)?;
    let resp = http
        .post(&url)
        .bearer_auth(&token)
        .timeout(std::time::Duration::from_secs(10))
        .json(&serde_json::json!({ "text": text }))
        .send()
        .await
        .map_err(|e| ConnectorError::Exchange(format!("spaces.messages.create failed: {e}")))?;
    let status = resp.status();
    if status.as_u16() == 403 || status.as_u16() == 404 {
        return Ok(AppPost::NotInSpace);
    }
    if status.as_u16() == 409 {
        return Ok(AppPost::Posted);
    }
    if !status.is_success() {
        return Err(ConnectorError::upstream(status, format!(
            "spaces.messages.create status {status}"
        )));
    }
    Ok(AppPost::Posted)
}

async fn mint_app_access_token(http: &reqwest::Client) -> Result<String, ConnectorError> {
    let raw = service_account_material().ok_or_else(|| {
        ConnectorError::NotConfigured(
            "GOOGLE_CHAT_BOT_SERVICE_ACCOUNT_JSON or GOOGLE_CHAT_BOT_SERVICE_ACCOUNT_FILE not configured"
                .into(),
        )
    })?;
    let key: ServiceAccountKey = serde_json::from_str(&raw).map_err(|_| {
        ConnectorError::NotConfigured("GOOGLE_CHAT_BOT service account JSON is not usable".into())
    })?;
    if key.client_email.trim().is_empty() || key.private_key.trim().is_empty() {
        return Err(ConnectorError::NotConfigured(
            "GOOGLE_CHAT_BOT service account JSON is missing client_email or private_key".into(),
        ));
    }
    let assertion = encode_chat_bot_assertion(
        key.client_email.trim(),
        &key.private_key,
        chrono::Utc::now().timestamp(),
    )?;
    let resp = http
        .post(token_endpoint())
        .timeout(std::time::Duration::from_secs(10))
        .form(&[
            ("grant_type", "urn:ietf:params:oauth:grant-type:jwt-bearer"),
            ("assertion", assertion.as_str()),
        ])
        .send()
        .await
        .map_err(|e| ConnectorError::Exchange(format!("chat.bot token exchange failed: {e}")))?;
    if !resp.status().is_success() {
        return Err(ConnectorError::upstream(
            resp.status(),
            format!("chat.bot token exchange status {}", resp.status()),
        ));
    }
    let parsed: TokenResponse = resp
        .json()
        .await
        .map_err(|e| ConnectorError::Exchange(format!("chat.bot token exchange parse: {e}")))?;
    if parsed.access_token.is_empty() {
        return Err(ConnectorError::Exchange(
            "chat.bot token exchange returned no access_token".into(),
        ));
    }
    Ok(parsed.access_token)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cold_start_app_identity_is_not_a_delegated_user_scope() {
        let pem = crate::connectors::github::test_throwaway_rsa_pem();
        let jwt =
            encode_chat_bot_assertion("bot@example.iam.gserviceaccount.com", &pem, 1_700_000_000)
                .expect("mint");
        let claims = claims_json(&jwt);
        assert_eq!(claims["scope"], CHAT_BOT_SCOPE);
        assert_eq!(claims["aud"], TOKEN_AUDIENCE);
        assert!(!scope_is_delegated_user_message_scope(CHAT_BOT_SCOPE));
        assert!(scope_is_delegated_user_message_scope(
            "https://www.googleapis.com/auth/chat.messages.create"
        ));
        assert!(scope_is_delegated_user_message_scope(
            "https://www.googleapis.com/auth/chat.messages"
        ));
        assert!(!scope_is_delegated_user_message_scope(
            "https://www.googleapis.com/auth/chat.messages.readonly"
        ));
    }

    fn claims_json(jwt: &str) -> serde_json::Value {
        let part = jwt.split('.').nth(1).expect("claims");
        let bytes = base64::Engine::decode(&base64::engine::general_purpose::URL_SAFE_NO_PAD, part)
            .expect("decode");
        serde_json::from_slice(&bytes).expect("json")
    }
}

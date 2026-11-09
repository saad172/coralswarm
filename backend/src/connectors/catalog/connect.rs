//! Connect, complete and disconnect (connectors redesign, layer B2).
//!
//! The adapters do not reimplement any provider flow. A browser-redirect
//! provider (Slack, Microsoft, Google, GitHub) connects exactly as the older UI
//! does: a signed `state` from [`mint_state_token`], then the existing
//! `GET /v1/oauth/{provider}/authorize` and `/callback`. What is new is a
//! `ret` claim in that state ([`ReturnTo`]), and a callback that, for a state
//! carrying it, sends the browser to `/connectors/return` instead of the old
//! `/connectors` page. A hosted provider (the Beta toolkits) returns the
//! provider's own link plus a `pending_id` and finishes in `complete_connect`.
//!
//! Disconnect calls the same handler the old UI's Disconnect button calls
//! ([`crate::routes::oauth::delete_connection`]), once per `oauth_tokens`
//! provider literal the connector owns, so token revocation, the provider's
//! purge and the capture stop are the existing code, not a copy of it.
//!
//! # The redirect
//!
//! [`classify`] and [`return_url`] are the whole of the new redirect logic and
//! are pure. The callback is left untouched for states without `ret`; for a
//! state with it, `routes::oauth::callback` runs the old handler and rewrites
//! its answer: a success redirect becomes `ok`, a refusal or failure becomes
//! `error` with a stable code. The destination is built here from constants, the
//! connector key (checked against the registry) and a code restricted to
//! `[a-z0-9_]`, never from anything the browser supplied: there is no open
//! redirect.
//!
//! # Rows
//!
//! After a successful connect the member gets one `connector_sources` row per
//! source, switched off and not set up, so the connector reads `setting_up`
//! (resuming at "Choose sources") until the wizard finishes. A member who
//! already had a connection before the catalog existed has no rows and reads as
//! set up; reconnecting that connection (a dead credential) first records it as
//! set up, so fixing the sign-in does not send them back through the wizard.

use axum::body::to_bytes;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::Response;
use axum::Extension;
use chrono::{Duration, Utc};
use uuid::Uuid;

use super::{AdapterCx, AdapterError, ConnectStart, ConnectorAdapter, ReturnTo};
use crate::oauth::state::STATE_TTL_SECS;
use crate::AppState;

/// The most a legacy handler's body is read for: every answer here is a few
/// hundred bytes of JSON.
const MAX_BODY: usize = 64 * 1024;

// ---------------------------------------------------------------------------
// Starting an OAuth connect
// ---------------------------------------------------------------------------

/// Mint a state carrying `ret` and return the existing authorize URL for it.
///
/// `surface` is the same string the old UI passes to both `mint_state` and
/// `authorize` (`workspace`, `slack_member`, ...); `authorize` refuses a state
/// whose surface differs, so the two cannot drift. All of mint_state's org gates
/// run, because this is that function.
pub async fn oauth_connect(
    cx: &AdapterCx<'_>,
    provider: &str,
    surface: Option<&str>,
    ret: ReturnTo,
) -> Result<ConnectStart, AdapterError> {
    let token = match crate::routes::oauth::mint_state_token(
        cx.state,
        cx.tenant,
        cx.user,
        surface,
        Some(ret),
    )
    .await
    {
        Ok(token) => token,
        Err(resp) => return Err(refusal(resp).await),
    };
    let mut url = format!(
        "{}/v1/oauth/{provider}/authorize?state={}",
        cx.state.api_base_url(),
        urlencoding::encode(&token)
    );
    if let Some(surface) = surface {
        url.push_str("&surface=");
        url.push_str(&urlencoding::encode(surface));
    }
    Ok(ConnectStart {
        authorize_url: url,
        expires_at: Utc::now() + Duration::seconds(STATE_TTL_SECS),
        pending_id: None,
    })
}

// ---------------------------------------------------------------------------
// Reading a legacy handler's answer
// ---------------------------------------------------------------------------

/// The adapter error for a legacy handler's non-success answer. The wire code is
/// chosen from the status alone: the older handlers write sentences in their
/// bodies, and this API ships codes.
pub async fn refusal(resp: Response) -> AdapterError {
    let status = resp.status();
    let body = body_json(resp).await;
    refusal_for(status, body.as_ref())
}

fn refusal_for(status: StatusCode, body: Option<&serde_json::Value>) -> AdapterError {
    let code = body
        .and_then(|b| b.get("code"))
        .and_then(|c| c.as_str())
        .unwrap_or_default();
    match status {
        StatusCode::NOT_FOUND => AdapterError::NotConnected,
        StatusCode::GONE => AdapterError::SignInExpired,
        StatusCode::SERVICE_UNAVAILABLE => AdapterError::NotConfigured,
        StatusCode::CONFLICT if code == "sign_in_pending" => AdapterError::SignInPending,
        StatusCode::CONFLICT => AdapterError::Refused {
            status: 409,
            code: "conflict",
        },
        StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN => AdapterError::Refused {
            status: 403,
            code: "forbidden",
        },
        StatusCode::TOO_MANY_REQUESTS => AdapterError::Refused {
            status: 429,
            code: "rate_limited",
        },
        s if s.is_client_error() => AdapterError::Refused {
            status: 400,
            code: "invalid_request",
        },
        _ => AdapterError::Refused {
            status: 502,
            code: "provider_error",
        },
    }
}

async fn body_json(resp: Response) -> Option<serde_json::Value> {
    let bytes = to_bytes(resp.into_body(), MAX_BODY).await.ok()?;
    serde_json::from_slice(&bytes).ok()
}

/// A legacy handler's answer as a unit result: any 2xx is success.
pub async fn expect_ok(resp: Response) -> Result<(), AdapterError> {
    if resp.status().is_success() {
        Ok(())
    } else {
        Err(refusal(resp).await)
    }
}

// ---------------------------------------------------------------------------
// Disconnecting
// ---------------------------------------------------------------------------

/// Run the existing disconnect for one `oauth_tokens` provider literal as the
/// caller. `Ok(true)` when it removed a connection, `Ok(false)` when the caller
/// had none under that literal.
///
/// The handler revokes the token at the provider where the provider supports it,
/// stops capture (watches, subscriptions, repo sync) and, for Slack, tears down
/// the routing rows; it never touches captured atoms. Its own org rules apply
/// unchanged, so a refusal (Slack's admin-only bot under an org) comes back as a
/// 403, not a partial disconnect.
pub async fn disconnect_oauth(
    cx: &AdapterCx<'_>,
    oauth_provider: &str,
) -> Result<bool, AdapterError> {
    let resp = crate::routes::oauth::delete_connection(
        State(cx.state.clone()),
        cx.tenant.clone(),
        Extension(cx.user.clone()),
        Path(oauth_provider.to_string()),
    )
    .await;
    match expect_ok(resp).await {
        Ok(()) => Ok(true),
        Err(AdapterError::NotConnected) => Ok(false),
        Err(e) => Err(e),
    }
}

/// The result of disconnecting several literals: connected to none of them is
/// "not connected", which the route answers 404.
pub fn any_removed(removed: bool) -> Result<(), AdapterError> {
    if removed {
        Ok(())
    } else {
        Err(AdapterError::NotConnected)
    }
}

// ---------------------------------------------------------------------------
// connector_sources rows
// ---------------------------------------------------------------------------

/// `connector_sources` lives on the control plane for every account.
fn control_unavailable() -> AdapterError {
    AdapterError::Database("control plane unavailable".into())
}

/// Record a new connection: one row per source, off and not set up. Rows that
/// exist are left alone, so reconnecting never resets a member's choices.
pub async fn record_connect(
    state: &AppState,
    account_id: Uuid,
    user_id: Uuid,
    adapter: &dyn ConnectorAdapter,
) -> Result<(), AdapterError> {
    let pool = state.control_db().ok_or_else(control_unavailable)?;
    let mut tx = crate::tenancy::scope::begin_account_scoped(pool, account_id).await?;
    for spec in adapter.sources() {
        sqlx::query(
            "INSERT INTO connector_sources (account_id, owner_user_id, provider, source, enabled) \
             VALUES ($1, $2, $3, $4, false) \
             ON CONFLICT (account_id, owner_user_id, provider, source) DO NOTHING",
        )
        .bind(account_id)
        .bind(user_id)
        .bind(adapter.key())
        .bind(spec.key)
        .execute(&mut *tx)
        .await?;
    }
    tx.commit().await?;
    Ok(())
}

/// Before reconnecting a connection made ahead of the catalog (a grant, no
/// rows), write its rows as they read today: each source as on or off by the
/// provider tables, and set up. Without this the new rows [`record_connect`]
/// writes would send the member back through setup to fix a dead sign-in.
pub async fn keep_legacy_connection_set_up(
    cx: &AdapterCx<'_>,
    adapter: &dyn ConnectorAdapter,
) -> Result<(), AdapterError> {
    let key = adapter.key();
    if adapter.grant(cx).await?.is_none() || cx.facts.sources.iter().any(|r| r.provider == key) {
        return Ok(());
    }
    let pool = cx.state.control_db().ok_or_else(control_unavailable)?;
    let mut tx = crate::tenancy::scope::begin_account_scoped(pool, cx.account_id).await?;
    for spec in adapter.sources() {
        let rt = adapter.source_runtime(cx, spec).await?;
        sqlx::query(
            "INSERT INTO connector_sources \
                 (account_id, owner_user_id, provider, source, enabled, setup_completed_at) \
             VALUES ($1, $2, $3, $4, $5, CASE WHEN $6 THEN now() END) \
             ON CONFLICT (account_id, owner_user_id, provider, source) DO NOTHING",
        )
        .bind(cx.account_id)
        .bind(cx.user_id)
        .bind(key)
        .bind(spec.key)
        .bind(rt.enabled)
        .bind(rt.setup_completed)
        .execute(&mut *tx)
        .await?;
    }
    tx.commit().await?;
    crate::read_models::dirty::connector_changed(cx.state, cx.tenant, cx.account_id, cx.user_id)
        .await
        .map_err(|e| AdapterError::Database(e.to_string()))?;
    Ok(())
}

/// Forget this member's setup state for a connector they disconnected. A later
/// connect starts the wizard afresh. Captured atoms are not touched.
pub async fn clear_sources(
    state: &AppState,
    account_id: Uuid,
    user_id: Uuid,
    provider: &str,
) -> Result<u64, AdapterError> {
    let pool = state.control_db().ok_or_else(control_unavailable)?;
    let mut tx = crate::tenancy::scope::begin_account_scoped(pool, account_id).await?;
    let done = sqlx::query(
        "DELETE FROM connector_sources \
         WHERE account_id = $1 AND owner_user_id = $2 AND provider = $3",
    )
    .bind(account_id)
    .bind(user_id)
    .bind(provider)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(done.rows_affected())
}

// ---------------------------------------------------------------------------
// The callback's redirect to /connectors/return
// ---------------------------------------------------------------------------

/// How a callback ended, as far as the return page is concerned.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    Ok,
    /// A stable code, `[a-z0-9_]{1,64}`.
    Error(String),
}

/// The return page for `provider`, or `None` when `provider` is not a connector
/// (nothing to hand back to). `web_base` is the web origin the callback already
/// computed for its own redirects.
pub fn return_url(web_base: &str, provider: &str, outcome: &Outcome) -> Option<String> {
    super::registry::lookup(provider)?;
    let mut url = format!(
        "{}/connectors/return?provider={provider}&result=",
        web_base.trim_end_matches('/')
    );
    match outcome {
        Outcome::Ok => url.push_str("ok"),
        Outcome::Error(code) => {
            url.push_str("error&code=");
            url.push_str(&safe_code(code));
        }
    }
    Some(url)
}

/// A code is restricted to lowercase ASCII letters, digits and `_`, at most 64
/// of them; anything else collapses to `failed`.
fn safe_code(code: &str) -> String {
    let ok = !code.is_empty()
        && code.len() <= 64
        && code
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_');
    if ok {
        code.to_string()
    } else {
        "failed".to_string()
    }
}

/// What the old callback's answer means for the return page.
///
/// * `provider_error` is the `error` query parameter the provider sent back
///   (the member pressed Cancel): `access_denied` is `denied`, any other value
///   `provider_error`. The old handler answers it with a bare 400.
/// * A redirect is a success when it lands on one of the success pages the old
///   handler uses (`/connectors?connected=`, GitHub's selection page, or the
///   Google Workspace summary with at least one surface connected) and a
///   failure for the expired-state page.
/// * Any 4xx or 5xx is a failure with a code from the status.
pub fn classify(status: u16, location: Option<&str>, provider_error: Option<&str>) -> Outcome {
    if let Some(e) = provider_error.filter(|e| !e.is_empty()) {
        return Outcome::Error(
            if e == "access_denied" {
                "denied"
            } else {
                "provider_error"
            }
            .into(),
        );
    }
    if (300..400).contains(&status) {
        return location.map_or_else(|| Outcome::Error("unexpected".into()), classify_redirect);
    }
    if (200..300).contains(&status) {
        return Outcome::Error("unexpected".into());
    }
    Outcome::Error(
        match status {
            400 => "invalid_request",
            401 | 403 => "forbidden",
            404 => "not_found",
            409 => "conflict",
            429 => "rate_limited",
            502..=504 => "unavailable",
            _ => "failed",
        }
        .into(),
    )
}

fn classify_redirect(location: &str) -> Outcome {
    // Absolute (the old handler prefixes the web origin) or a bare path.
    let parsed = reqwest::Url::parse(location)
        .or_else(|_| reqwest::Url::parse(&format!("http://web.invalid{location}")));
    let Ok(url) = parsed else {
        return Outcome::Error("unexpected".into());
    };
    let query = |k: &str| {
        url.query_pairs()
            .find(|(name, _)| name == k)
            .map(|(_, v)| v.into_owned())
    };
    match url.path() {
        "/connectors" => {
            if let Some(e) = query("error") {
                return Outcome::Error(safe_code(&e.to_ascii_lowercase()));
            }
            if query("google_workspace").is_some() {
                return if query("gws_connected").is_some_and(|c| !c.is_empty()) {
                    Outcome::Ok
                } else if query("gws_failed").is_some() {
                    Outcome::Error("failed".into())
                } else {
                    Outcome::Error("not_granted".into())
                };
            }
            if query("connected").is_some() {
                return Outcome::Ok;
            }
            Outcome::Error("unexpected".into())
        }
        "/connectors/github" if query("installation_id").is_some() => Outcome::Ok,
        _ => Outcome::Error("unexpected".into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn err(code: &str) -> Outcome {
        Outcome::Error(code.into())
    }

    #[test]
    fn return_url_has_exactly_the_documented_shape() {
        assert_eq!(
            return_url("https://app.example.com", "slack", &Outcome::Ok).as_deref(),
            Some("https://app.example.com/connectors/return?provider=slack&result=ok")
        );
        assert_eq!(
            return_url("https://app.example.com/", "jira", &err("denied")).as_deref(),
            Some(
                "https://app.example.com/connectors/return?provider=jira&result=error&code=denied"
            )
        );
    }

    #[test]
    fn return_url_is_never_built_from_untrusted_text() {
        // Only a registered connector key is a provider.
        assert_eq!(
            return_url("https://app.example.com", "evil", &Outcome::Ok),
            None
        );
        assert_eq!(
            return_url("https://app.example.com", "slack&result=ok#", &Outcome::Ok),
            None
        );
        assert_eq!(
            return_url("https://app.example.com", "slack/../x", &Outcome::Ok),
            None
        );
        // A code with anything outside [a-z0-9_] collapses, so it cannot add a
        // parameter, a fragment or a second URL.
        for bad in [
            "a&b=c",
            "x#y",
            "//evil.example",
            "https://evil.example",
            "UPPER",
            "has space",
            "",
            &"a".repeat(65),
        ] {
            assert_eq!(
                return_url("https://app.example.com", "slack", &err(bad)).as_deref(),
                Some("https://app.example.com/connectors/return?provider=slack&result=error&code=failed"),
                "{bad}"
            );
        }
    }

    #[test]
    fn return_url_stays_on_the_web_origin_it_is_given() {
        let url = return_url("http://localhost:13000", "microsoft", &Outcome::Ok).unwrap();
        let parsed = reqwest::Url::parse(&url).unwrap();
        assert_eq!(
            parsed.origin().ascii_serialization(),
            "http://localhost:13000"
        );
        assert_eq!(parsed.path(), "/connectors/return");
    }

    #[test]
    fn the_old_handlers_success_pages_are_ok() {
        for loc in [
            "https://app.example.com/connectors?connected=slack",
            "https://app.example.com/connectors?connected=microsoft",
            "https://app.example.com/connectors/github?installation_id=42",
            "https://app.example.com/connectors?google_workspace=1&gws_connected=calendar,gmail",
            "https://app.example.com/connectors?google_workspace=1&gws_connected=gmail&gws_skipped=chat:unticked",
            "/connectors?connected=slack",
        ] {
            assert_eq!(classify(307, Some(loc), None), Outcome::Ok, "{loc}");
        }
    }

    #[test]
    fn the_old_handlers_failure_pages_are_errors() {
        assert_eq!(
            classify(
                307,
                Some("https://app.example.com/connectors?error=expired"),
                None
            ),
            err("expired")
        );
        // Nothing granted in the Google Workspace flow.
        assert_eq!(
            classify(
                307,
                Some("https://app.example.com/connectors?google_workspace=1&gws_skipped=gmail:unticked"),
                None
            ),
            err("not_granted")
        );
        assert_eq!(
            classify(
                307,
                Some("https://app.example.com/connectors?google_workspace=1&gws_failed=gmail"),
                None
            ),
            err("failed")
        );
        // A redirect nobody expected is an error, never a silent success.
        for loc in [
            "https://app.example.com/somewhere/else",
            "https://app.example.com/connectors",
            "https://app.example.com/connectors/microsoft-teams?onboarding=x",
            "not a url \u{0}",
        ] {
            assert_eq!(classify(307, Some(loc), None), err("unexpected"), "{loc}");
        }
        assert_eq!(classify(307, None, None), err("unexpected"));
    }

    #[test]
    fn refusals_and_provider_denials_carry_a_code() {
        assert_eq!(classify(400, None, Some("access_denied")), err("denied"));
        assert_eq!(
            classify(400, None, Some("anything_else")),
            err("provider_error")
        );
        // A denial wins over whatever the old handler said about it.
        assert_eq!(
            classify(
                307,
                Some("/connectors?connected=slack"),
                Some("access_denied")
            ),
            err("denied")
        );
        assert_eq!(classify(400, None, None), err("invalid_request"));
        assert_eq!(classify(403, None, None), err("forbidden"));
        assert_eq!(classify(503, None, None), err("unavailable"));
        assert_eq!(classify(500, None, None), err("failed"));
        assert_eq!(classify(200, None, None), err("unexpected"));
    }

    #[test]
    fn legacy_statuses_map_to_the_adapter_errors_the_route_documents() {
        use serde_json::json;
        let pending = json!({"code": "sign_in_pending"});
        assert!(matches!(
            refusal_for(StatusCode::CONFLICT, Some(&pending)),
            AdapterError::SignInPending
        ));
        assert!(matches!(
            refusal_for(StatusCode::CONFLICT, None),
            AdapterError::Refused { status: 409, .. }
        ));
        assert!(matches!(
            refusal_for(StatusCode::GONE, None),
            AdapterError::SignInExpired
        ));
        assert!(matches!(
            refusal_for(StatusCode::NOT_FOUND, None),
            AdapterError::NotConnected
        ));
        assert!(matches!(
            refusal_for(StatusCode::SERVICE_UNAVAILABLE, None),
            AdapterError::NotConfigured
        ));
        assert!(matches!(
            refusal_for(StatusCode::FORBIDDEN, None),
            AdapterError::Refused {
                status: 403,
                code: "forbidden"
            }
        ));
        assert!(matches!(
            refusal_for(StatusCode::BAD_GATEWAY, None),
            AdapterError::Refused { status: 502, .. }
        ));
    }
}

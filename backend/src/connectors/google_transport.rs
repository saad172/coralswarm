//! Google-only transport substitution, compiled exclusively for the E2E harness.
//! Production builds never read the override; credentials and scopes are unchanged.
pub(crate) fn endpoint(url: &str) -> String {
    #[cfg(feature = "e2e")]
    if let Ok(base) = std::env::var("E2E_GOOGLE_BASE") {
        if let Ok(parsed) = reqwest::Url::parse(url) {
            if let Some(host) = parsed.host_str() {
                if matches!(host, "accounts.google.com" | "oauth2.googleapis.com" | "www.googleapis.com" | "gmail.googleapis.com" | "chat.googleapis.com" | "workspaceevents.googleapis.com" | "people.googleapis.com" | "meet.googleapis.com") {
                    return format!("{}/{host}{}{}", base.trim_end_matches('/'), parsed.path(), parsed.query().map(|q| format!("?{q}")).unwrap_or_default());
                }
            }
        }
    }
    url.to_owned()
}

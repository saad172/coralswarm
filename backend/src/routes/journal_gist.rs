//! `GET /v1/journal/gist`: the HTTP edge of the Today gist. The compile and
//! the read are `services::journal_gist` (module doc: inputs, Mine merge, Team
//! scope, recompile rule); this file only authorizes and maps answers.
//!
//! Answers 404 while `TODAY_GIST_ENABLED` is unset.
//!
//! # Contract
//!
//! `GET /v1/journal/gist?granularity=day|week|month&anchor=YYYY-MM-DD&who=mine|team&graph_id=&tz=`
//! -> 200 `{period_start, period_end, open, who, granularity, tz, gist:
//! {done[], decided[], open[]} | null, compiled_at, span: {first_activity,
//! last_activity} | null}`. 400 on a bad granularity/anchor/who/tz, and for
//! `who=team` without `graph_id`; 404 when `graph_id` names an ocean the
//! caller cannot read. Full shape: `docs/implementation/journal-gist-api.md`.

use axum::{
    extract::Query,
    response::{IntoResponse, Response},
    Extension,
};
#[cfg(test)]
use chrono_tz::Tz;
use serde::Deserialize;
use utoipa::path as utoipa_path;
use uuid::Uuid;

use crate::{middleware::auth::UserContext, tenancy::extractor::Tenant};

#[derive(Debug, Deserialize)]
pub struct GistParams {
    pub granularity: Option<String>,
    pub anchor: Option<String>,
    pub who: Option<String>,
    pub graph_id: Option<Uuid>,
    pub tz: Option<String>,
}

#[cfg(test)]
fn parse_tz(raw: Option<&str>) -> Result<Option<Tz>, String> {
    match raw.map(str::trim) {
        None | Some("") => Ok(None),
        Some(z) => z
            .parse::<Tz>()
            .map(Some)
            .map_err(|_| "tz must be an IANA time zone name".to_string()),
    }
}

/// `GET /v1/journal/gist`
#[utoipa_path(
    get,
    path = "/v1/journal/gist",
    tag = "journal",
    security(("bearer_token" = [])),
    params(
        ("granularity" = Option<String>, Query, description = "day (default) | week | month"),
        ("anchor" = Option<String>, Query, description = "A local date (YYYY-MM-DD) inside the period; default today"),
        ("who" = Option<String>, Query, description = "mine (default) | team"),
        ("graph_id" = Option<String>, Query, description = "One ocean; required for who=team"),
        ("tz" = Option<String>, Query, description = "IANA zone for the period; default the user's stored zone"),
    ),
    responses(
        (status = 200, description = "The period's gist (null when none is compiled yet)", body = Value),
        (status = 400, description = "Invalid granularity, anchor, who or tz, or who=team without graph_id"),
        (status = 404, description = "Flag off, or graph_id names an ocean the caller cannot read"),
        (status = 500, description = "Internal error"),
        (status = 503, description = "Database unavailable"),
    )
)]
pub async fn get_gist(
    state: axum::extract::State<std::sync::Arc<crate::AppState>>,
    tenant: Tenant,
    user: Extension<UserContext>,
    original: Option<Extension<crate::tenancy::runtime::TenancyDecision>>,
    query: Query<std::collections::BTreeMap<String, String>>,
) -> Response {
    crate::routes::page_read::serve::<crate::read_models::today::JournalGist>(
        state,
        tenant,
        user,
        original,
        axum::extract::Path(std::collections::BTreeMap::new()),
        query,
    )
    .await
    .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tz_parse_accepts_iana_and_blank_and_rejects_garbage() {
        assert_eq!(parse_tz(None).unwrap(), None);
        assert_eq!(parse_tz(Some(" ")).unwrap(), None);
        assert_eq!(
            parse_tz(Some("Europe/Paris")).unwrap(),
            Some(chrono_tz::Europe::Paris)
        );
        assert!(parse_tz(Some("Mars/Base")).is_err());
    }
}

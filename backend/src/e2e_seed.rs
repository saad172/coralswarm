//! The e2e harness's one seeding route (`e2e/README.md`, "Seeding").
//!
//! Compiled only with the `e2e` feature, which `lib.rs` refuses to combine with
//! an optimized build, so no deployable binary carries it.
//!
//! `POST /v1/e2e/seed/extraction` stands in for exactly one thing: managed
//! extraction. A source captured through the real API (`POST /v1/ingest/atom`)
//! waits for the standing admission sweep (60 s) and then an LLM tool call
//! before it has `semantic_atoms`, and Ask cites only those. A scenario has 30 s.
//! This route writes the outcome extraction would have written for the
//! caller's own source, deterministically: one `fact` per sentence, each with a
//! passage whose quote is that sentence verbatim at its real character offsets.
//!
//! It runs as the caller, in the caller's plane, through the same member-scoped
//! transaction (`selected_publication::owner_tx`) that publication uses to
//! write semantic atoms, so
//! RLS and the tenant binding apply exactly as they do to product writes. It
//! never creates the source itself: ingest stays the real route.

use crate::{source_evidence, tenancy::extractor::Tenant, AppState, UserContext};
use axum::{
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    Extension, Json,
};
use serde::Deserialize;
use serde_json::json;
use std::sync::Arc;
use uuid::Uuid;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SeedExtraction {
    pub graph_id: Uuid,
    pub source_type: String,
    pub source_id: String,
}

fn fail(status: StatusCode, message: impl std::fmt::Display) -> Response {
    (status, Json(json!({ "error": message.to_string() }))).into_response()
}

/// `(start_char, end_char, sentence)` for every sentence of `text`, in
/// character (not byte) offsets, the unit `semantic_atom_passages` stores.
fn sentences(text: &str) -> Vec<(i32, i32, String)> {
    let chars: Vec<char> = text.chars().collect();
    let mut out = Vec::new();
    let mut start = 0usize;
    let mut i = 0usize;
    while i <= chars.len() {
        let end_here = i == chars.len()
            || chars[i] == '\n'
            || (matches!(chars[i], '.' | '!' | '?') && (i + 1 == chars.len() || chars[i + 1].is_whitespace()));
        if end_here {
            let stop = if i < chars.len() && chars[i] != '\n' { i + 1 } else { i };
            let mut s = start;
            while s < stop && chars[s].is_whitespace() {
                s += 1;
            }
            let mut e = stop;
            while e > s && chars[e - 1].is_whitespace() {
                e -= 1;
            }
            if e > s {
                out.push((s as i32, e as i32, chars[s..e].iter().collect()));
            }
            start = stop;
        }
        i += 1;
    }
    out
}

pub async fn seed_extraction(
    State(state): State<Arc<AppState>>,
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
    Json(req): Json<SeedExtraction>,
) -> Response {
    let mut tx = match crate::selected_publication::owner_tx(&tenant, &user).await {
        Ok(tx) => tx,
        Err(e) => return fail(StatusCode::INTERNAL_SERVER_ERROR, format!("scope: {e}")),
    };
    // The caller's history floor applies here as on every member read: a seat
    // that may not see this source cannot seed it.
    let version_id: Option<Uuid> = match sqlx::query_scalar(&format!(
        "SELECT s.current_version_id FROM source_items s WHERE s.graph_id=$1 AND s.source_type=$2 AND s.source_id=$3 AND {floor}",
        floor = crate::history_floor::admits_source_item("s")
    ))
    .bind(req.graph_id)
    .bind(&req.source_type)
    .bind(&req.source_id)
    .fetch_optional(&mut *tx)
    .await
    {
        Ok(v) => v.flatten(),
        Err(e) => return fail(StatusCode::INTERNAL_SERVER_ERROR, format!("source lookup: {e}")),
    };
    let Some(version_id) = version_id else {
        return fail(StatusCode::NOT_FOUND, "no such source in this graph for the caller");
    };
    let row = match source_evidence::load_version_scoped(&mut tx, req.graph_id, version_id).await {
        Ok(Some(row)) => row,
        Ok(None) => return fail(StatusCode::NOT_FOUND, "source version not readable by the caller"),
        Err(e) => return fail(StatusCode::INTERNAL_SERVER_ERROR, format!("version load: {e}")),
    };
    if let Err(e) = tx.commit().await {
        return fail(StatusCode::INTERNAL_SERVER_ERROR, format!("commit: {e}"));
    }
    let Some(text) = source_evidence::read_text(&state, &tenant, user.account_id, &row).await else {
        return fail(StatusCode::INTERNAL_SERVER_ERROR, "source text unreadable");
    };
    let passages = sentences(&text);
    if passages.is_empty() {
        return fail(StatusCode::UNPROCESSABLE_ENTITY, "source has no sentences");
    }

    let mut tx = match crate::selected_publication::owner_tx(&tenant, &user).await {
        Ok(tx) => tx,
        Err(e) => return fail(StatusCode::INTERNAL_SERVER_ERROR, format!("scope: {e}")),
    };
    let written = async {
        // Idempotent and race-free: the version row lock serializes
        // concurrent seeds of one source, and a version that already has
        // atoms answers with what it has instead of writing more.
        sqlx::query("SELECT 1 FROM source_versions WHERE graph_id=$1 AND version_id=$2 FOR UPDATE")
            .bind(req.graph_id)
            .bind(version_id)
            .execute(&mut *tx)
            .await?;
        let existing: i64 =
            sqlx::query_scalar(
                "SELECT count(*) FROM semantic_atoms WHERE graph_id=$1 AND source_version_id=$2 /* history_floor: counts atoms of the one version load_version_scoped (admits_version) already admitted for this caller and the row lock above holds */",
            )
                .bind(req.graph_id)
                .bind(version_id)
                .fetch_one(&mut *tx)
                .await?;
        if existing > 0 {
            return Ok::<_, sqlx::Error>(existing as usize);
        }
        for (ordinal, (start, end, quote)) in passages.iter().enumerate() {
            let atom = Uuid::new_v4();
            sqlx::query(
                "INSERT INTO semantic_atoms(semantic_atom_id,graph_id,source_version_id,ordinal,kind,statement,extraction_version,model) \
                 VALUES($1,$2,$3,$4,'fact',$5,$6,'e2e-deterministic-extraction')",
            )
            .bind(atom)
            .bind(req.graph_id)
            .bind(version_id)
            .bind(ordinal as i32)
            .bind(quote)
            .bind(&row.extraction_version)
            .execute(&mut *tx)
            .await?;
            sqlx::query(
                "INSERT INTO semantic_atom_passages(semantic_atom_id,graph_id,source_version_id,start_char,end_char,quote) \
                 VALUES($1,$2,$3,$4,$5,$6)",
            )
            .bind(atom)
            .bind(req.graph_id)
            .bind(version_id)
            .bind(start)
            .bind(end)
            .bind(quote)
            .execute(&mut *tx)
            .await?;
        }
        sqlx::query("UPDATE source_versions SET extraction_status='done' WHERE graph_id=$1 AND version_id=$2")
            .bind(req.graph_id)
            .bind(version_id)
            .execute(&mut *tx)
            .await?;
        Ok(passages.len())
    }
    .await;
    let written = match written {
        Ok(n) => n,
        Err(e) => return fail(StatusCode::INTERNAL_SERVER_ERROR, format!("write: {e}")),
    };
    if let Err(e) = tx.commit().await {
        return fail(StatusCode::INTERNAL_SERVER_ERROR, format!("commit: {e}"));
    }
    tracing::info!(graph = %req.graph_id, version = %version_id, written, "e2e seed: deterministic extraction written");
    (
        StatusCode::OK,
        Json(json!({ "source_version_id": version_id, "semantic_atoms": written })),
    )
        .into_response()
}

/// `GET /v1/e2e/slow`: hold one WORK-lane connection inside the caller's
/// member-scoped transaction for `secs` seconds (default 2, at most 8).
///
/// The page-load stack's lane proof (`e2e/tests/route-class.spec.ts`,
/// design §10 L1): several of these in flight saturate the tenant's work
/// lane, and a `page_read` route issued meanwhile must still answer in
/// milliseconds because it runs on the page lane. It is an `Other`-class
/// route, so it is also what the admission cap counts and, with
/// `CONCURRENCY_ENFORCE=1`, sheds.
pub async fn slow_work(
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
    axum::extract::Query(q): axum::extract::Query<SlowQuery>,
) -> Response {
    let secs = q.secs.unwrap_or(2).min(8);
    let mut tx = match tenant.begin_member_scoped(user.account_id).await {
        Ok(tx) => tx,
        Err(e) => return fail(StatusCode::INTERNAL_SERVER_ERROR, format!("scope: {e}")),
    };
    if let Err(e) = sqlx::query("SELECT pg_sleep($1)")
        .bind(f64::from(secs))
        .execute(&mut *tx)
        .await
    {
        return fail(StatusCode::INTERNAL_SERVER_ERROR, format!("sleep: {e}"));
    }
    if let Err(e) = tx.rollback().await {
        return fail(StatusCode::INTERNAL_SERVER_ERROR, format!("rollback: {e}"));
    }
    (
        StatusCode::OK,
        Json(json!({ "slept_secs": secs, "class": tenant.class.as_str() })),
    )
        .into_response()
}

#[derive(Deserialize)]
pub struct SlowQuery {
    pub secs: Option<u32>,
}

#[cfg(test)]
mod tests {
    use super::sentences;

    #[test]
    fn sentences_carry_character_offsets_of_their_verbatim_text() {
        let text = "Ünïcode first. Second one!\nThird line";
        let got = sentences(text);
        let chars: Vec<char> = text.chars().collect();
        assert_eq!(got.len(), 3);
        for (s, e, quote) in &got {
            assert_eq!(&chars[*s as usize..*e as usize].iter().collect::<String>(), quote);
        }
        assert_eq!(got[0].2, "Ünïcode first.");
        assert_eq!(got[2].2, "Third line");
    }
}

/// E2E-only instrumentation; module and route are absent from release builds.
pub(crate) async fn read_model_computes()->axum::Json<serde_json::Value>{
 axum::Json(crate::read_models::compute_counts())
}

//! Managed history boundaries and explicit baseline promotion. No implicit
//! budgets, provider identity reconstruction, local key reads or free model jobs.
use crate::{
    corpus_enrichment::{self, Run},
    tenancy::extractor::Tenant,
    AppState, UserContext,
};
use axum::{
    extract::{Path, State},
    http::StatusCode,
    Extension, Json,
};
use serde::Deserialize;
use serde_json::{json, Value};
use std::sync::Arc;
use uuid::Uuid;

/// Exhaustive classification forces a conscious admission decision when a
/// new automatic JobKind is introduced. Manual Ask is not an automatic job.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) enum AutomaticPurpose {
    ManagedSource,
    ManagedEmbedding,
    /// The shared graph-level purpose (`managed::enrichment`).
    /// `EntityResolution` moved first (source-agnostic plan PR 5), then
    /// `RegenerateDigest` and `BackfillAtomTitles` (PRs 6-8, with the
    /// sweep-driven `JournalSweep`, which has no `JobKind`), then
    /// `GenerateMeetingSummary` (PR 10). A kind here is also listed in
    /// `managed::MANAGED_ENRICHMENT_JOB_KINDS`, which
    /// `managed_enrichment_kinds_are_listed_once` pins.
    ManagedEnrichment,
    NotPaid,
}
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn automatic_purpose(kind: &crate::jobs::JobKind) -> AutomaticPurpose {
    use crate::jobs::JobKind::*;
    match kind {
        ExtractMeetingAtoms { .. } | ExtractSourceAtoms { .. }
        | ExtractStandingMeetingAtoms { .. } | ExtractStandingSourceAtoms { .. } => AutomaticPurpose::ManagedSource,
        EmbedAtoms { .. } | EmbedAtomsRetry { .. } | ReembedOutdatedChunks { .. }
        | EmbedStatements { .. } => AutomaticPurpose::ManagedEmbedding,
        // Graph-level consumers metered on the shared purpose behind
        // ENRICHMENT_PROCESSING_ENABLED plus each kind's own switch: judged
        // duplicate-reef merging (`workers::entity_resolution`), reef digests
        // (`workers::digest`, DIGESTS_ENABLED) and coral titles
        // (`workers::atom_title_backfill`, ATOM_TITLES_ENABLED) and meeting
        // summaries (`workers::summary`, MEETING_SUMMARIES_ENABLED).
        EntityResolution { .. }
        | RegenerateDigest { .. }
        | BackfillAtomTitles { .. }
        | GenerateMeetingSummary { .. } => AutomaticPurpose::ManagedEnrichment,
        // Deterministic closure over confirmed edges: no model call, so it
        // needs no managed purpose (`workers::inference_closure`).
        InferRelationships { .. }
        | PublishSource { .. }
        | RouteCopresence { .. }
        | SendOutreach { .. }
        | ComputeDrift { .. }
        | SlackNameBackfill { .. }
        | ContentTsvRegconfigRepair { .. }
        | DeleteSourceBlob { .. }
        | PropagateCorrections { .. }
        | SlackHuddleCapture { .. }
        | SlackSeatReconcile { .. }
        | EmailUnmatchedSlackChannelMembers { .. }
        | ChatInvite { .. }
        | TeamsSeatReconcile { .. }
        | EmailUnmatchedTeamsMembers { .. }
        | SlackMessageIngest { .. }
        | GraphMessageIngest { .. }
        | PromoteNode { .. }
        | OceanHealth { .. }
        | HealthViolation { .. }
        | HistoryBackfill { .. }
        | GithubBackfill { .. }
        | GithubEventIngest { .. }
        | IngestReceive(_) => AutomaticPurpose::NotPaid,
    }
}
/// Kinds retired permanently (founder decision 2026-10-18; the per-atom
/// `ExtractEntities`/`ExtractRelationships` pair with source-agnostic plan
/// PR 11, replaced by managed source extraction). They have no
/// `JobKind` variant, no worker and no producer; the admission trigger still
/// dead-letters a hand-inserted row so nothing can wait on a missing worker.
/// Historical rows stay dead and are never requeued. Read by the drift tests
/// that pin it to the trigger's list.
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) const RETIRED_JOB_KINDS: &[&str] = &[
    "ExtractEntities",
    "ExtractRelationships",
    "IngestDoor",
    "ConflictDetection",
    "InferExpertise",
    "ReevaluateKind",
    "ExtractThreads",
    "ExtractDecisions",
];
/// `EmbedAtoms`/`EmbedAtomsRetry` are admitted with no constant fence: every
/// cloud request goes through `managed::embedding::process`, whose real
/// admission (policy, executor, credits, daily `cloud_embedding` budget,
/// per-attempt cap) holds or refuses per batch.
///
/// `ReembedOutdatedChunks` shares that path but stays closed here. Its
/// candidates are atoms with a legacy bge `embedding` (retired 2026-09-22),
/// not the atoms the 2026-10-13 fence left without a cloud vector, and it
/// would draw on the same per-account daily budget as live ingest and the
/// fence backlog, re-enqueueing itself every 600 s on any hold.
///
/// Re-checked 2026-10-18 (source-agnostic plan PR 3), read-only on prod at
/// 19:17Z: every atom that still carries a bge vector already has
/// `embed_model_next = openai/text-embedding-3-small@384` (coralswarm tenant
/// 0 of 21,394; testorg 0 of 19; control 0 of 12,062). Nothing writes bge
/// vectors any more, so the candidate set is empty and cannot grow. Opening
/// it would only add a sweep that scans and finds nothing; it stays closed.
pub(crate) const fn managed_reembed_admitted() -> bool {
    false
}
pub(crate) fn automatic_sweep_blocked(purpose: &str) -> bool {
    if purpose=="ReembedOutdatedChunks" && !managed_reembed_admitted() {
        tracing::debug!(
            purpose,
            reason = "managed_purpose_unsupported",
            "automatic purpose blocked; managed restoration required"
        );
        true
    } else {
        false
    }
}

/// `ReembedOutdatedChunks` stays closed (see [`managed_reembed_admitted`]).
/// Queue's structural outcome is visible, terminal and immune to retry bypass.
pub(crate) async fn block_legacy_paid_job(
    pool: &sqlx::PgPool,
    job: &crate::jobs::queue::JobRow,
) -> bool {
    if !(job.kind=="ReembedOutdatedChunks" && !managed_reembed_admitted()) {
        return false;
    }
    let _ = crate::jobs::queue::could_not_do(
        pool,
        job.id,
        crate::jobs::queue::FailureClass::Structural,
        "managed_purpose_unsupported",
    )
    .await;
    true
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PromoteRequest {
    pub graph_id: Uuid,
    pub unit_id: Uuid,
}
fn failure(code: StatusCode, reason: &str) -> (StatusCode, Json<Value>) {
    (code, Json(json!({"error":reason})))
}

/// Bounded explicit promotion of one previously inventoried holding. The
/// source snapshot and execution owner commit atomically; a refused admission
/// returns an error and rolls back, never degrades to ordinary capture success.
pub async fn promote(
    State(state): State<Arc<AppState>>,
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
    Path(run_id): Path<Uuid>,
    Json(req): Json<PromoteRequest>,
) -> (StatusCode, Json<Value>) {
    let account = match crate::source_evidence::authorize(&tenant, &user, req.graph_id).await {
        Ok(a) => a,
        Err(e) => return e,
    };
    if !crate::source_evidence::still_admitted(&state, &tenant, &user, req.graph_id, account).await {
        return failure(StatusCode::FORBIDDEN, "managed promotion not admitted");
    }
    let standing = async {
        let mut tx = tenant.begin_member_scoped(account).await?;
        let allowed = corpus_enrichment::run_actor_allowed_tx(&mut tx, req.graph_id, run_id, account, &user).await?;
        let kind: Option<crate::managed::standing::RunKind> = if allowed {
            sqlx::query_scalar("SELECT run_kind FROM corpus_enrichment_runs WHERE graph_id=$1 AND run_id=$2")
                .bind(req.graph_id).bind(run_id).fetch_optional(&mut *tx).await?
        } else { None };
        tx.commit().await?;
        Ok::<_, crate::store::StoreError>(kind==Some(crate::managed::standing::RunKind::Standing))
    }.await;
    match standing {
        Ok(true)=>return failure(StatusCode::UNPROCESSABLE_ENTITY,"standing runs do not accept cohort promotion"),
        Ok(false)=>{},
        Err(_)=>return failure(StatusCode::INTERNAL_SERVER_ERROR,"promotion unavailable"),
    }
    let result = promote_one(
        &state,
        &tenant,
        &user,
        run_id,
        req.graph_id,
        account,
        req.unit_id,
    )
    .await;
    match result {
        Ok(v) => (StatusCode::OK, Json(v)),
        Err(reason) => failure(StatusCode::CONFLICT, reason),
    }
}
async fn promote_one(
    state: &Arc<AppState>,
    tenant: &Tenant,
    user: &UserContext,
    run_id: Uuid,
    graph: Uuid,
    account: Uuid,
    unit: Uuid,
) -> Result<Value, &'static str> {
    let mut tx = tenant
        .begin_member_scoped(account)
        .await
        .map_err(|_| "promotion unavailable")?;
    crate::tenancy::scope::set_tenant_user(&mut tx, user.user_id)
        .await
        .map_err(|_| "promotion unavailable")?;
    let run: Run =
        sqlx::query_as("SELECT * FROM corpus_enrichment_runs WHERE run_id=$1 AND graph_id=$2")
            .bind(run_id)
            .bind(graph)
            .fetch_optional(&mut *tx)
            .await
            .map_err(|_| "promotion unavailable")?
            .ok_or("run unavailable")?;
    if run.mode != "execute"
        || run.corpus_kind != "real"
        || run.status != "running"
        // A baseline is extracted under its run's source profile; v1 is
        // retired, so a v1 run promotes nothing.
        || !matches!(run.extraction_version.as_str(), "source-atoms-v2" | "source-atoms-v3")
        || !corpus_enrichment::lifecycle_ready_tx(&mut tx)
            .await
            .map_err(|_| "promotion unavailable")?
        || !corpus_enrichment::run_actor_allowed_tx(&mut tx, graph, run_id, account, user)
            .await
            .map_err(|_| "promotion unavailable")?
    {
        return Err("explicit compatible execute run and lifecycle contract required");
    }
    let (kind,holding,already):(String,Uuid,Option<Uuid>)=sqlx::query_as("SELECT kind,holding_id,promoted_version_id FROM corpus_enrichment_units WHERE run_id=$1 AND graph_id=$2 AND unit_id=$3").bind(run_id).bind(graph).bind(unit).fetch_optional(&mut *tx).await.map_err(|_|"promotion unavailable")?.ok_or("holding unavailable")?;
    if !matches!(kind.as_str(), "legacy_chunk" | "retained_transcript") {
        return Err("only inventoried legacy/transcript holdings can be promoted");
    }
    if let Some(version) = already {
        if !crate::source_evidence::still_admitted_in_tx(
            state, tenant, user, graph, account, &mut tx,
        )
        .await
        {
            return Err("promotion access changed");
        }
        if !corpus_enrichment::source_permitted_tx(&mut tx, graph, version, &run.extraction_version)
            .await
            .map_err(|_| "promotion unavailable")?
        {
            return Err("promoted evidence unavailable");
        }
        tx.commit().await.map_err(|_| "promotion unavailable")?;
        return Ok(
            json!({"graph_id":graph,"run_id":run_id,"unit_id":unit,"source_version_id":version,"idempotent":true}),
        );
    }
    // The manifest establishes cohort membership before any original read.
    let sql = if kind == "legacy_chunk" {
        "SELECT pg_visible_in_snapshot(a.inventory_created_xid,$4::pg_snapshot) AND a.created_at<=$5 AND library_atom_permitted(a.graph_id,a.atom_id) AND NOT a.source_evidence_bound AND a.promoted_from_atom_id IS NULL AND COALESCE((to_jsonb(a)->>'derived_copy_bound')::boolean,false)=false AND NOT EXISTS(SELECT 1 FROM source_connection_atoms d WHERE d.target_atom_id=a.atom_id) AND NOT EXISTS(SELECT 1 FROM source_version_chunks c WHERE c.graph_id=a.graph_id AND c.atom_id=a.atom_id) FROM atoms a WHERE a.graph_id=$1 AND a.atom_id=$2 AND $3::uuid IS NOT NULL"
    } else {
        "SELECT pg_visible_in_snapshot(t.inventory_created_xid,$4::pg_snapshot) AND t.created_at<=$5 AND t.source_item_id IS NULL AND t.current_version_id IS NULL FROM meeting_transcripts t WHERE t.graph_id=$1 AND t.id=$2 AND $3::uuid IS NOT NULL"
    };
    let eligible: Option<bool> = sqlx::query_scalar(sql)
        .bind(graph)
        .bind(holding)
        .bind(unit)
        .bind(&run.cohort_snapshot)
        .bind(run.cohort_cutoff)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|_| "promotion unavailable")?;
    if eligible != Some(true) {
        return Err("holding is unavailable, bound, denied or outside the pinned cohort");
    }
    // Pin the retained row signature before slow I/O; never infer originals
    // from a title, mutable key, transcript summary, or adjacent chunks.
    let signature: Value = if kind == "legacy_chunk" {
        sqlx::query_scalar("SELECT jsonb_build_array(content_inline,content_blob_key,header_len,source_type,source_id,timestamp,created_at,xmin::text) FROM atoms WHERE graph_id=$1 AND atom_id=$2").bind(graph).bind(holding).fetch_one(&mut *tx).await.map_err(|_|"promotion unavailable")?
    } else {
        sqlx::query_scalar("SELECT jsonb_build_array(raw_inline,raw_blob_key,duration_ms,segments,created_at,xmin::text) FROM meeting_transcripts WHERE graph_id=$1 AND id=$2").bind(graph).bind(holding).fetch_one(&mut *tx).await.map_err(|_|"promotion unavailable")?
    };
    tx.commit().await.map_err(|_| "promotion unavailable")?;
    let text = if kind == "legacy_chunk" {
        // The legacy atom reader prefers R2 when configured, even with an
        // inline fallback. Refuse foreign graph locators before that I/O.
        let org = tenant.org_context().map(|ctx| ctx.org_id.as_str());
        if state.r2.is_some() && signature[1].as_str().is_some_and(|key|
            !crate::source_evidence::artifact_locator_admitted(graph, key, org)) {
            return Err("legacy original locator unavailable");
        }
        // Reuse the exact existing permission/locator-fenced public read seam.
        let (code, Json(body)) = crate::routes::atoms::get_atom(
            State(state.clone()),
            tenant.clone(),
            Extension(user.clone()),
            Path(holding),
        )
        .await;
        if code != StatusCode::OK || body["graph_id"] != json!(graph) {
            return Err("legacy original unavailable");
        }
        body["content"]
            .as_str()
            .map(str::to_owned)
            .ok_or("legacy original unavailable")?
    } else {
        crate::source_evidence::read_unbound_transcript_for_promotion(
            state, tenant, user, graph, account, holding,
        )
        .await
        .ok_or("retained transcript original unavailable")?
    };
    if text.is_empty() || text.chars().count() > 8000000 || text.contains('\u{fffd}') {
        return Err("retained normalized text unavailable or invalid");
    }
    if !crate::source_evidence::still_admitted(state, tenant, user, graph, account).await {
        return Err("promotion access changed");
    }
    let mut tx = tenant
        .begin_member_scoped(account)
        .await
        .map_err(|_| "promotion unavailable")?;
    crate::tenancy::scope::set_tenant_user(&mut tx, user.user_id)
        .await
        .map_err(|_| "promotion unavailable")?;
    // Run first, then exact retained row locks: budgets and concurrent reruns
    // cannot steal an already admitted baseline or change its copied bytes.
    let run: Run = sqlx::query_as(
        "SELECT * FROM corpus_enrichment_runs WHERE run_id=$1 AND graph_id=$2 FOR UPDATE",
    )
    .bind(run_id)
    .bind(graph)
    .fetch_one(&mut *tx)
    .await
    .map_err(|_| "promotion unavailable")?;
    if run.status != "running"
        || !corpus_enrichment::lifecycle_ready_tx(&mut tx)
            .await
            .map_err(|_| "promotion unavailable")?
    {
        return Err("managed run paused or contract unavailable");
    }
    let existing:Option<Uuid>=sqlx::query_scalar("SELECT promoted_version_id FROM corpus_enrichment_units WHERE run_id=$1 AND unit_id=$2 FOR UPDATE").bind(run_id).bind(unit).fetch_one(&mut *tx).await.map_err(|_|"promotion unavailable")?;
    if let Some(v) = existing {
        if !crate::source_evidence::still_admitted_in_tx(
            state, tenant, user, graph, account, &mut tx,
        )
        .await
        {
            return Err("promotion access changed");
        }
        if !corpus_enrichment::source_permitted_tx(&mut tx, graph, v, &run.extraction_version)
            .await
            .map_err(|_| "promotion unavailable")?
        {
            return Err("promoted evidence unavailable");
        }
        tx.commit().await.map_err(|_| "promotion unavailable")?;
        return Ok(
            json!({"graph_id":graph,"run_id":run_id,"unit_id":unit,"source_version_id":v,"idempotent":true}),
        );
    }
    let current: Option<Value> = if kind == "legacy_chunk" {
        sqlx::query_scalar("SELECT jsonb_build_array(content_inline,content_blob_key,header_len,source_type,source_id,timestamp,created_at,xmin::text) FROM atoms WHERE graph_id=$1 AND atom_id=$2 AND library_atom_permitted(graph_id,atom_id) AND NOT source_evidence_bound AND promoted_from_atom_id IS NULL AND COALESCE((to_jsonb(atoms)->>'derived_copy_bound')::boolean,false)=false AND NOT EXISTS(SELECT 1 FROM source_connection_atoms d WHERE d.target_atom_id=atoms.atom_id) AND NOT EXISTS(SELECT 1 FROM source_version_chunks c WHERE c.graph_id=atoms.graph_id AND c.atom_id=atoms.atom_id) FOR UPDATE").bind(graph).bind(holding).fetch_optional(&mut *tx).await.map_err(|_|"promotion unavailable")?
    } else {
        sqlx::query_scalar("SELECT jsonb_build_array(raw_inline,raw_blob_key,duration_ms,segments,created_at,xmin::text) FROM meeting_transcripts WHERE graph_id=$1 AND id=$2 AND source_item_id IS NULL AND current_version_id IS NULL FOR UPDATE").bind(graph).bind(holding).fetch_optional(&mut *tx).await.map_err(|_|"promotion unavailable")?
    };
    if current.as_ref() != Some(&signature) {
        return Err("retained holding changed; re-inventory required");
    }
    // One baseline per exact retained holding. Source/event/provider identity
    // stays unknown; no parent session/project context is retroactively invented.
    let source_id = holding.to_string();
    let namespace = format!("legacy:baseline:{kind}:{graph}");
    let metadata = json!({"baseline_holding_kind":kind,"baseline_holding_id":holding,"baseline_retained_at":if kind=="legacy_chunk"{signature[6].clone()}else{signature[4].clone()},"provider_origin_unknown":true,"event_time_unknown":true,"original_source_independence":"unknown"});
    let capture = crate::source_evidence::CaptureSource {
        source_type: if kind == "legacy_chunk" {
            "legacy_chunk"
        } else {
            "transcript"
        },
        provider_namespace: &namespace,
        source_id: &source_id,
        normalized_text: &text,
        representation: "normalized_text",
        metadata: &metadata,
        occurred_at: None,
        participants: &[],
        ingested_by: Some(user.user_id),
        scope: "private",
        duration_ms: None,
        segments: None,
    };
    // Inline immutable copy avoids reusing a mutable old blob locator; no R2
    // upload is required to retain the already-authorized normalized baseline.
    let saved = crate::source_evidence::capture_source_version_pinned_tx(
        &mut tx, None, graph, account, &capture, None, &[0; 32], &run.extraction_version,
    )
    .await
    .map_err(|_| "baseline capture refused")?;
    let fingerprint = crate::services::source_reingest::content_hash_hex(&text);
    sqlx::query(
        "UPDATE source_versions SET is_legacy_baseline=true WHERE graph_id=$1 AND version_id=$2",
    )
    .bind(graph)
    .bind(saved.version_id)
    .execute(&mut *tx)
    .await
    .map_err(|_| "promotion unavailable")?;
    sqlx::query("UPDATE corpus_enrichment_units SET promoted_version_id=$3,promotion_fingerprint=$4,source_item_id=$5,extraction_version=$6 WHERE run_id=$1 AND unit_id=$2").bind(run_id).bind(unit).bind(saved.version_id).bind(fingerprint).bind(saved.source_item_id).bind(&run.extraction_version).execute(&mut *tx).await.map_err(|_|"promotion unavailable")?;
    corpus_enrichment::admit_tx(&mut tx, &run, unit, saved.version_id)
        .await
        .map_err(|_| "managed admission refused")?;
    let admitted: bool = sqlx::query_scalar(
        "SELECT execution_owner FROM corpus_enrichment_units WHERE run_id=$1 AND unit_id=$2",
    )
    .bind(run_id)
    .bind(unit)
    .fetch_one(&mut *tx)
    .await
    .map_err(|_| "promotion unavailable")?;
    if !admitted {
        let done:bool=sqlx::query_scalar("SELECT extraction_status='done' FROM source_versions WHERE graph_id=$1 AND version_id=$2").bind(graph).bind(saved.version_id).fetch_one(&mut *tx).await.map_err(|_|"promotion unavailable")?;
        if !done {
            return Err("managed admission refused; promotion rolled back");
        }
    }
    if kind == "legacy_chunk" {
        crate::source_evidence::link_verified_chunks_tx(
            &mut tx,
            graph,
            saved.version_id,
            &[holding],
        )
        .await
        .map_err(|_| "baseline binding refused")?;
    }
    if !crate::source_evidence::still_admitted_in_tx(state, tenant, user, graph, account, &mut tx)
        .await
    {
        return Err("promotion access changed");
    }
    tx.commit().await.map_err(|_| "promotion unavailable")?;
    Ok(
        json!({"graph_id":graph,"run_id":run_id,"unit_id":unit,"source_item_id":saved.source_item_id,"source_version_id":saved.version_id,"representation":"normalized_text","occurred_at":null,"is_legacy_baseline":true,"idempotent":!saved.changed}),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The admission trigger's quoted kind lists (trigger + startup UPDATE).
    fn sql_kind_lists(sql: &str) -> Vec<Vec<String>> {
        sql.lines()
            .filter_map(|l| l.split_once("kind IN (").map(|(_, rest)| rest))
            .map(|rest| {
                rest.split(')').next().unwrap()
                    .split(',')
                    .map(|k| k.trim().trim_matches('\'').to_owned())
                    .collect()
            })
            .collect()
    }

    #[test]
    fn admission_trigger_lists_match_retired_kinds_on_both_planes() {
        let expected: Vec<String> = RETIRED_JOB_KINDS.iter().map(|k| k.to_string()).collect();
        for sql in [
            include_str!("../migrations_control/0347_managed_meeting_summaries.sql"),
            include_str!("../migrations_tenant/0179_managed_meeting_summaries.sql"),
        ] {
            let lists = sql_kind_lists(sql);
            assert_eq!(lists.len(), 1, "current trigger replacement preserves other blocked kinds");
            for list in lists {
                assert_eq!(list, expected);
            }
        }
    }

    #[test]
    fn managed_embedding_open_for_embed_jobs_and_reembed_stays_closed() {
        use crate::jobs::JobKind;
        assert!(!managed_reembed_admitted());
        assert!(!automatic_sweep_blocked("EmbedAtoms"));
        assert!(!automatic_sweep_blocked("EmbedAtomsRetry"));
        let embed = JobKind::EmbedAtoms { atom_ids: vec![] };
        let retry = JobKind::EmbedAtomsRetry { atom_ids: vec![] };
        let reembed = JobKind::ReembedOutdatedChunks { after_atom_id: None };
        assert_eq!(automatic_purpose(&embed), AutomaticPurpose::ManagedEmbedding);
        assert_eq!(automatic_purpose(&retry), AutomaticPurpose::ManagedEmbedding);
        assert_eq!(automatic_purpose(&reembed), AutomaticPurpose::ManagedEmbedding);
        assert!(automatic_sweep_blocked("ReembedOutdatedChunks"));
    }

    /// Every kind that could move onto `managed_enrichment` (the graph-level
    /// consumers awaiting restoration). A kind mapped to the purpose must be
    /// listed in `MANAGED_ENRICHMENT_JOB_KINDS`, must leave the unsupported
    /// list, and must never be billed on the job axis; an unmapped kind must
    /// not be listed.
    #[test]
    fn managed_enrichment_kinds_are_listed_once() {
        use crate::jobs::JobKind;
        use crate::managed::{is_managed_job_kind, MANAGED_ENRICHMENT_JOB_KINDS, MANAGED_JOB_KINDS};
        use crate::workers::billing_derivation::JOB_AXIS_KINDS;
        let g = Uuid::nil();
        let candidates = [
            JobKind::EntityResolution { graph_id: g },
            JobKind::RegenerateDigest { node_id: g, graph_id: g },
            JobKind::BackfillAtomTitles { after_atom_id: None },
            JobKind::GenerateMeetingSummary { graph_id: g, source_id: String::new(), allow_cloud_override: false },
        ];
        for kind in &candidates {
            let tag = serde_json::to_value(kind).unwrap()["kind"].as_str().unwrap().to_owned();
            let mapped = automatic_purpose(kind) == AutomaticPurpose::ManagedEnrichment;
            assert_eq!(MANAGED_ENRICHMENT_JOB_KINDS.contains(&tag.as_str()), mapped, "{tag}");
            if mapped {
                assert!(!JOB_AXIS_KINDS.contains(&tag.as_str()), "{tag} is billed from receipts only");
                assert!(is_managed_job_kind(&tag));
            }
        }
        for kind in MANAGED_ENRICHMENT_JOB_KINDS {
            assert!(!MANAGED_JOB_KINDS.contains(kind), "{kind} is on one managed purpose");
            assert!(!JOB_AXIS_KINDS.contains(kind));
        }
        // Exactly the kinds restored so far: flipping another is an explicit change.
        assert_eq!(
            MANAGED_ENRICHMENT_JOB_KINDS,
            &["EntityResolution", "RegenerateDigest", "BackfillAtomTitles", "GenerateMeetingSummary"]
        );
        let mapped: Vec<&JobKind> = candidates.iter().filter(|kind| automatic_purpose(kind) == AutomaticPurpose::ManagedEnrichment).collect();
        assert_eq!(mapped.len(), 4);
        assert!(matches!(mapped[0], JobKind::EntityResolution { .. }));
        assert!(matches!(mapped[1], JobKind::RegenerateDigest { .. }));
        assert!(matches!(mapped[2], JobKind::BackfillAtomTitles { .. }));
        assert!(matches!(mapped[3], JobKind::GenerateMeetingSummary { .. }));
        for kind in ["EntityResolution", "RegenerateDigest", "BackfillAtomTitles", "GenerateMeetingSummary", "JournalSweep"] {
            assert!(!automatic_sweep_blocked(kind), "{kind} is managed enrichment, not blocked");
        }
    }

    #[test]
    fn retired_kinds_cannot_be_constructed_or_enqueued() {
        for kind in RETIRED_JOB_KINDS {
            let payload = json!({"atom_id":Uuid::nil(),"graph_id":Uuid::nil(),"node_id":Uuid::nil()});
            let parsed = serde_json::from_value::<crate::jobs::JobKind>(json!({"kind":kind,"payload":payload}));
            assert!(parsed.is_err(), "{kind} must have no JobKind variant, so no Rust path can enqueue it");
        }
    }

    #[test]
    fn classification_table() {
        use crate::jobs::JobKind;
        let g = Uuid::nil();
        assert_eq!(automatic_purpose(&JobKind::InferRelationships { graph_id: g }), AutomaticPurpose::NotPaid);
        assert!(!automatic_sweep_blocked("InferRelationships"));
        for (kind, tag) in [
            (JobKind::EntityResolution { graph_id: g }, "EntityResolution"),
            (JobKind::RegenerateDigest { node_id: g, graph_id: g }, "RegenerateDigest"),
            (JobKind::BackfillAtomTitles { after_atom_id: None }, "BackfillAtomTitles"),
            (JobKind::GenerateMeetingSummary { graph_id: g, source_id: String::new(), allow_cloud_override: false }, "GenerateMeetingSummary"),
        ] {
            assert_eq!(automatic_purpose(&kind), AutomaticPurpose::ManagedEnrichment, "{tag}");
        }
    }
}

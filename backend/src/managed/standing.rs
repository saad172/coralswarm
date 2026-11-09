pub(crate) mod execution;
mod admission;
mod principal;
pub(crate) use principal::{
    FailureStage, InvalidPrincipalBinding, MissingPrincipalData, NoEligibleReason,
    PrincipalDenial, PrincipalDisposition, PrincipalError, PrincipalRequest, RunIneligible,
    StandingOwner, StandingProfile,
};

use sqlx::{Postgres, Transaction};
use uuid::Uuid;

#[derive(Clone, Copy, Debug, PartialEq, Eq, sqlx::Type)]
#[sqlx(type_name = "text", rename_all = "snake_case")]
pub(crate) enum RunKind {
    Cohort,
    Standing,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, sqlx::Type)]
#[sqlx(type_name = "text", rename_all = "snake_case")]
pub(crate) enum StandingPauseState {
    None,
    Operator,
    Processing,
    Both,
}

#[derive(Clone, Copy)]
pub(crate) enum PauseAction {
    OperatorPause,
    OperatorResume,
    ProcessingOff,
    ProcessingOn,
}

impl StandingPauseState {
    fn transition(self, action: PauseAction) -> Self {
        use PauseAction::*;
        use StandingPauseState::*;
        match (self, action) {
            (None | Operator, OperatorPause) => Operator,
            (Processing | Both, OperatorPause) => Both,
            (None | Operator, OperatorResume) => None,
            (Processing | Both, OperatorResume) => Processing,
            (None | Processing, ProcessingOff) => Processing,
            (Operator | Both, ProcessingOff) => Both,
            (None | Processing, ProcessingOn) => None,
            (Operator | Both, ProcessingOn) => Operator,
        }
    }

    fn tuple(self) -> (&'static str, Option<&'static str>) {
        match self {
            Self::None => ("running", None),
            Self::Operator | Self::Both => ("paused", Some("operator_paused")),
            Self::Processing => ("paused", Some("managed_processing_disabled")),
        }
    }
}

pub(crate) async fn change_pause_tx(
    tx: &mut Transaction<'_, Postgres>,
    graph: Uuid,
    run: Uuid,
    action: PauseAction,
) -> Result<(), sqlx::Error> {
    // Every standing sweep calls this for every run, and almost always the
    // state does not change. Reading under a share lock and writing only on a
    // transition keeps the sweep from queueing on the row behind every open
    // standing fence (and every fence behind it) once a minute.
    let (current, current_status, current_reason): (StandingPauseState, String, Option<String>) = sqlx::query_as(
        "SELECT standing_pause_state,status,pause_reason FROM corpus_enrichment_runs WHERE graph_id=$1 AND run_id=$2 AND run_kind='standing' FOR SHARE",
    ).bind(graph).bind(run).fetch_one(&mut **tx).await?;
    let next = current.transition(action);
    let (status, reason) = next.tuple();
    if next == current && current_status == status && current_reason.as_deref() == reason {
        return Ok(());
    }
    // A writer: queue on the gate so fences opened from now on wait for the
    // change (`execution::share_run_gate`).
    execution::exclusive_run_gate(tx, graph).await?;
    sqlx::query("UPDATE corpus_enrichment_runs SET standing_pause_state=$3,status=$4,pause_reason=$5,updated_at=now() WHERE graph_id=$1 AND run_id=$2")
        .bind(graph).bind(run).bind(next).bind(status).bind(reason).execute(&mut **tx).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pause_axes_preserve_each_other_and_actions_are_idempotent() {
        use PauseAction::*;
        use StandingPauseState::*;
        let actions = [OperatorPause, OperatorResume, ProcessingOff, ProcessingOn];
        for (state, expected) in [
            (None, [Operator, None, Processing, None]),
            (Operator, [Operator, None, Both, Operator]),
            (Processing, [Both, Processing, Processing, None]),
            (Both, [Both, Processing, Both, Operator]),
        ] {
            for (action, expected) in actions.into_iter().zip(expected) {
                assert_eq!(state.transition(action), expected);
                assert_eq!(expected.transition(action), expected);
            }
        }
    }
}


pub(crate) async fn hold_unit_tx(
    tx: &mut Transaction<'_, Postgres>, graph: Uuid, version: Uuid, profile: &str, reason: &str,
) -> Result<(), sqlx::Error> {
    let reason = execution::HoldReason::parse(reason)
        .ok_or_else(|| sqlx::Error::Protocol("unsupported standing hold reason".into()))?;
    let state = if reason == execution::HoldReason::SourceUnavailable { "unavailable" } else { "pending" };
    sqlx::query("UPDATE corpus_enrichment_units u SET state=$4,reason=$5,retry_after=CASE WHEN $5='daily_budget_exhausted' THEN (date_trunc('day',now() AT TIME ZONE 'UTC') AT TIME ZONE 'UTC')+interval '1 day' WHEN $5='operator_paused' OR $4='unavailable' THEN NULL ELSE now()+interval '60 seconds' END,standing_claim_until=NULL,updated_at=now() FROM corpus_enrichment_runs r WHERE u.run_id=r.run_id AND r.run_kind='standing' AND u.graph_id=$1 AND u.holding_id=$2 AND u.extraction_version=$3 AND u.execution_owner AND u.state NOT IN ('failed','processed')")
        .bind(graph).bind(version).bind(profile).bind(state).bind(reason.code()).execute(&mut **tx).await?;
    sqlx::query("UPDATE source_versions SET extraction_status='blocked',extraction_error=$3 WHERE graph_id=$1 AND version_id=$2 AND extraction_status NOT IN ('done','failed')")
        .bind(graph).bind(version).bind(reason.code()).execute(&mut **tx).await?;
    sqlx::query("UPDATE corpus_capture_admissions SET state='blocked',reason=$4,updated_at=now() WHERE graph_id=$1 AND source_version_id=$2 AND extraction_version=$3 AND state<>'processed'")
        .bind(graph).bind(version).bind(profile).bind(reason.code()).execute(&mut **tx).await?;
    sqlx::query("SELECT corpus_standing_receipt_event($1,$2,'hold',$3)")
        .bind(graph).bind(version).bind(reason.code()).execute(&mut **tx).await?;
    Ok(())
}

/// A standing owner unit nothing will deliver: queued, unclaimed, and with no
/// pending or running job for its version (#1360).
const UNDELIVERED: &str = "u.execution_owner AND u.state='queued' AND (u.standing_claim_until IS NULL OR u.standing_claim_until<=now()) AND NOT EXISTS(SELECT 1 FROM jobs j WHERE j.payload->>'source_version_id'=u.holding_id::text AND j.status::text IN ('pending','running'))";
/// Hands an undelivered unit to requeue as an infrastructure hold.
const RELEASE_UNDELIVERED: &str = "UPDATE corpus_enrichment_units u SET state='pending',reason='infrastructure_unavailable',retry_after=now(),standing_claim_until=NULL,updated_at=now()";

pub(super) async fn release_undelivered_run_tx(tx: &mut Transaction<'_, Postgres>, run: Uuid) -> Result<u64, sqlx::Error> {
    Ok(sqlx::query(&format!("{RELEASE_UNDELIVERED} WHERE u.run_id=$1 AND {UNDELIVERED}"))
        .bind(run).execute(&mut **tx).await?.rows_affected())
}

pub(super) async fn release_undelivered_version_tx(tx: &mut Transaction<'_, Postgres>, graph: Uuid, version: Uuid) -> Result<u64, sqlx::Error> {
    Ok(sqlx::query(&format!("{RELEASE_UNDELIVERED} FROM corpus_enrichment_runs r WHERE r.run_id=u.run_id AND r.run_kind='standing' AND u.graph_id=$1 AND u.holding_id=$2 AND {UNDELIVERED}"))
        .bind(graph).bind(version).execute(&mut **tx).await?.rows_affected())
}

#[cfg(feature="test-support")]
pub mod probe;

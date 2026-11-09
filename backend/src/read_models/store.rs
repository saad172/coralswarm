//! All persistent state transitions. A generation survives same-timestamp bumps;
//! a UUID claim token prevents a recovered stale worker from publishing/releasing
//! a newer worker's claim. Retry deadlines are independent of dirty deadlines.
use super::*;
use sqlx::{PgPool, Row};
pub async fn read<M: ReadModel>(
    tx: &mut PgConnection,
    scope: &Scope<M::Params>,
) -> Result<Stored<M::Payload>, Error> {
    // `identity` mirrors the refusal clause of `read_model_fence_state` (tenant
    // 0225): a compute snapshot exists and, unless the service actor reads, the
    // row's account and windowed viewer are the request's. The function folds
    // those refusals into 0, and `decode` must tell them from a fence behind
    // the row. `read_model_ocean_stored_certified` (tenant 0233) returns the
    // same columns for the OceansIndex fan-in.
    let row=sqlx::query("SELECT payload::text AS payload,computed_at,dirty_at,computed_control_freshness,read_model_fence_state(account_id,CASE WHEN $5 THEN ARRAY['00000000-0000-0000-0000-000000000000'::uuid] ELSE graph_ids END,model,viewer_user_id,computed_snapshot) fence_state,LEAST(dirty_bumped_at+make_interval(secs=>debounce_seconds),dirty_at+make_interval(secs=>max_delay_seconds)) eta,retry_after, (invalidated_xid IS NULL OR pg_visible_in_snapshot(invalidated_xid,computed_snapshot)) valid, failed_attempts>0 failed, (computed_snapshot IS NOT NULL AND (current_setting('app.tenant_actor',true) IS NOT DISTINCT FROM 'system' OR (account_id IS NOT DISTINCT FROM NULLIF(current_setting('app.account_id',true),'')::uuid AND (viewer_user_id IS NULL OR viewer_user_id IS NOT DISTINCT FROM NULLIF(current_setting('app.user_id',true),'')::uuid)))) identity FROM read_models WHERE model=$1 AND scope_key=$2 AND version=$3 AND account_id=$4")
  .bind(M::NAME).bind(scope.key()).bind(M::VERSION).bind(scope.account_id).bind(M::ACCOUNT_FACTS).fetch_optional(&mut *tx).await.map_err(crate::store::StoreError::from)?;
    let mut stored = decode::<M>(row,scope)?;
    if stored.computed_at.is_some() {
        M::overlay_current(tx, scope, &mut stored.payload).await?;
    }
    Ok(stored)
}
/// Shared envelope certification for indexed reads, including the OceansIndex
/// batch adapter. Extra columns beyond the certified set do not change this
/// decision.
///
/// A computed row is served. Row-local invalidation and the graph or actor
/// fences no longer withhold it: they mark the answer stale and the owed
/// recompute replaces it, so a deleted or newly restricted item can show for up
/// to one recompute. Such a row is also `behind`, which the serving paths
/// (`Stored::owe_refresh`) turn into the same immediate enqueue the withheld
/// path made on every read, so the recompute does not wait for the dirty
/// debounce. Two cases still withhold (`computed_at: None`, empty payload, a
/// refresh ETA):
///
/// * the row is not certified for this request (`identity`): no compute
///   snapshot, or the account or windowed viewer is not the request's. These
///   are the authority refusals `read_model_fence_state` folds into 0;
/// * the row is behind a reduction and its last compute failed (`failed`): the
///   recompute that bounds the exposure may not come soon. The ETA is
///   `retry_after` only while no dirty write has cleared it; dirty writes reset
///   it, so under steady writes the client polls at the 5 s floor until a
///   compute publishes.
///
/// A compute that crashes or panics never reaches `fail`, so `failed` stays
/// false and the behind row keeps being served stale until a compute publishes.
/// Bounding that window belongs to the read-model freshness design.
///
/// Graph-level access loss never reaches this point with the old row: the
/// viewer is rederived per request, a lost graph changes the scope key or
/// refuses the scope, and `read_models` RLS hides rows over graphs the actor
/// cannot see.
pub(crate) fn decode<M:ReadModel>(row:Option<sqlx::postgres::PgRow>,scope:&Scope<M::Params>)->Result<Stored<M::Payload>,Error>{
    let mut stored = Stored {
        model: M::NAME.into(),
        version: M::VERSION,
        payload: M::empty(&scope.params),
        computed_at: None,
        stale: true,
        refresh_eta: Some(Utc::now() + chrono::Duration::seconds(5)),
        behind: false,
    };
    if let Some(row) = row {
        let at: Option<DateTime<Utc>> = row.try_get("computed_at")?;
        let dirty: Option<DateTime<Utc>> = row.try_get("dirty_at")?;
        let retry: Option<DateTime<Utc>> = row.try_get("retry_after")?;
        if let Some(retry) = retry {
            stored.refresh_eta = Some(retry.max(Utc::now() + chrono::Duration::seconds(5)));
        }
        if let Some(at) = at {
            if !row.try_get::<bool, _>("identity")? {
                return Ok(stored);
            }
            let fence: i32 = row.try_get("fence_state")?;
            let behind = row.try_get::<Option<bool>, _>("valid")? != Some(true) || fence == 0;
            stored.behind = behind;
            if behind && row.try_get::<bool, _>("failed")? {
                return Ok(stored);
            }
            // Decode directly into the typed payload once. Decoding JSONB to Value
            // and then from_value rebuilt every large Atlas map a second time.
            let payload:&str=row.try_get("payload")?;
            stored.payload = serde_json::from_str(payload)?;
            stored.computed_at = Some(at);
            stored.stale = behind
                || fence < 2
                || row.try_get::<i64, _>("computed_control_freshness")?
                    < scope.control_freshness
                || dirty.is_some()
                || at + chrono::Duration::from_std(M::MAX_DELAY).unwrap() < Utc::now();
            let eta: Option<DateTime<Utc>> = row.try_get("eta")?;
            stored.refresh_eta = eta.map(|e| retry.map_or(e, |r| r.max(e)));
        }
    }
    Ok(stored)
}
pub async fn enqueue<M: ReadModel>(pool: &PgPool, scope: &Scope<M::Params>) -> Result<(), Error> {
    enqueue_inner::<M>(pool, scope, false).await
}
/// Explicit refresh may override failure backoff. Automatic missing reads may
/// not: client polling must not turn a broken model into an unbounded retry loop.
pub async fn request_refresh<M: ReadModel>(
    pool: &PgPool,
    scope: &Scope<M::Params>,
) -> Result<(), Error> {
    enqueue_inner::<M>(pool, scope, true).await
}
async fn enqueue_inner<M: ReadModel>(
    pool: &PgPool,
    scope: &Scope<M::Params>,
    force: bool,
) -> Result<(), Error> {
    // Explicit member scope on the WORK lane; no PageRead task-local opener here.
    let mut tx =
        crate::tenancy::scope::begin_tenant_scoped(pool, scope.account_id, scope.compute_as)
            .await?;
    // This is also authorization for refresh POST. Recheck after asynchronous handoff
    // so a revoked caller cannot recreate an addressable scope.
    let mut viewer = Viewer::load(&mut tx, scope.account_id, scope.compute_as).await?;
    // Enqueue is content bookkeeping. Preserve the request's certified control
    // revision; a newer revision addresses a different key, and worker compute
    // independently verifies against its actual control snapshot.
    viewer.control_revision = Some(scope.control_revision);
    viewer.control_freshness = scope.control_freshness;
    let current = viewer.scope::<M>(scope.params.clone())?;
    if current.key() != scope.key() {
        return Err("viewer changed before enqueue".into());
    }
    if M::CONTROL_FACTS {
        // A control revision is an authority fence, not a cache dimension that
        // grows forever. Retain only the current certified address for this
        // version/viewer/graph/parameter tuple. Token-fenced publish refuses any
        // superseded claim whose row was removed; other rolling versions stay.
        sqlx::query("DELETE FROM read_models WHERE model=$1 AND version=$2 AND account_id=$3 AND viewer_user_id IS NOT DISTINCT FROM $4 AND graph_ids=$5 AND params=$6 AND scope_key<>$7 AND control_revision<$8")
            .bind(M::NAME).bind(M::VERSION).bind(scope.account_id).bind(scope.class.user_id())
            .bind(scope.class.graphs()).bind(serde_json::to_value(&scope.params)?).bind(scope.key())
            .bind(scope.control_revision).execute(&mut *tx).await?;
    }
    let (generation, due_at): (i64, DateTime<Utc>) = sqlx::query_as(r#"INSERT INTO read_models(model,scope_key,version,account_id,graph_ids,viewer_user_id,compute_as,params,dirty_at,dirty_bumped_at,debounce_seconds,max_delay_seconds,control_revision,control_freshness,immediate_due_at)
 VALUES($1,$2,$3,$4,$5,$6,$7,$8,now()-make_interval(secs=>$10),now()-make_interval(secs=>$9),$9,$10,$12,$13,clock_timestamp())
 ON CONFLICT(model,scope_key) DO UPDATE SET
 dirty_at=COALESCE(read_models.dirty_at,EXCLUDED.dirty_at),dirty_bumped_at=EXCLUDED.dirty_bumped_at,
 immediate_due_at=CASE WHEN read_models.dirty_at IS NULL THEN EXCLUDED.immediate_due_at ELSE COALESCE(read_models.immediate_due_at,EXCLUDED.immediate_due_at) END,
 dirty_generation=read_models.dirty_generation+1,retry_after=CASE WHEN $11 THEN NULL ELSE read_models.retry_after END,compute_as=EXCLUDED.compute_as,
 control_freshness=GREATEST(read_models.control_freshness,EXCLUDED.control_freshness)
 RETURNING dirty_generation,immediate_due_at"#)
 .bind(M::NAME).bind(scope.key()).bind(M::VERSION).bind(scope.account_id).bind(scope.class.graphs())
 .bind(scope.class.user_id()).bind(scope.compute_as).bind(serde_json::to_value(&scope.params)?)
 .bind(M::DEBOUNCE.as_secs() as i32).bind(M::MAX_DELAY.as_secs() as i32).bind(force).bind(scope.control_revision).bind(scope.control_freshness).fetch_one(&mut *tx).await?;
    tx.commit().await?;
    tracing::debug!(model=M::NAME,version=M::VERSION,scope_key=%scope_correlation(scope.key()),generation,revision=scope.control_revision,immediate_due_at=%due_at,"read-model immediate enqueue");
    Ok(())
}
#[derive(Debug, sqlx::FromRow)]
pub struct Claim {
    pub model: String,
    pub scope_key: String,
    pub version: i32,
    pub account_id: Uuid,
    pub graph_ids: Vec<Uuid>,
    pub compute_as: Uuid,
    pub params: Value,
    pub dirty_generation: i64,
    pub claim_token: Uuid,
    pub control_revision: i64,
    pub control_freshness: i64,
    /// Earliest unresolved immediate enqueue (NULL for trigger-only/legacy work).
    pub immediate_due_at: Option<DateTime<Utc>>,
    pub claimed_at: DateTime<Utc>,
    pub first_fill: bool,
    /// Both endpoints measured by this database, never browser/worker clocks.
    pub due_to_claim_ms: Option<f64>,
}
/// Same irreversible fingerprint as the read-only recovery probes. Hash even
/// malformed internal keys so diagnostic paths can never print key contents.
pub fn scope_correlation(key: &str) -> String {
    hex::encode(&Sha256::digest(key.as_bytes())[..8])
}
fn registered_versions() -> (Vec<&'static str>, Vec<i32>) {
    registry::all()
        .iter()
        .map(|m| (m.name(), m.version()))
        .unzip()
}
pub async fn claim(pool: &PgPool) -> Result<Vec<Claim>, Error> {
    let mut tx = crate::tenancy::scope::begin_tenant_system_scoped_without_account(pool).await?;
    registry::sync_triggers(&mut tx).await?;
    // Claim only this binary's versions: rolling older workers must never delete
    // or consume rows belonging to the newer deployment.
    let (names, versions) = registered_versions();
    // Reserve one slot for the oldest eligible row so a continuous stream of
    // first fills cannot starve stored refreshes or eligible failed rows. Fill the other slots empty-first;
    // both passes share eligibility, token leasing and SKIP LOCKED semantics.
    // UPDATE RETURNING has no order: explicitly order the selected four so a
    // stored fairness refresh cannot take admission ahead of selected first fills.
    // The worker bounds concurrent claim lifecycles separately. Selection is unchanged.
    let claims:Vec<Claim>=sqlx::query_as(r#"WITH fair AS MATERIALIZED (
 SELECT r.model,r.scope_key FROM read_models r JOIN unnest($1::text[],$2::int[]) m(name,version) ON m.name=r.model AND m.version=r.version
 WHERE dirty_at IS NOT NULL
 AND LEAST(dirty_bumped_at+make_interval(secs=>debounce_seconds),dirty_at+make_interval(secs=>max_delay_seconds))<=now()
 AND (retry_after IS NULL OR retry_after<=now())
 AND (refresh_claimed_at IS NULL OR refresh_claimed_at<now()-interval '2 minutes')
 ORDER BY r.dirty_at,r.model,r.scope_key LIMIT 1 FOR UPDATE OF r SKIP LOCKED),
 priority AS MATERIALIZED (
 SELECT r.model,r.scope_key FROM read_models r JOIN unnest($1::text[],$2::int[]) m(name,version) ON m.name=r.model AND m.version=r.version
 WHERE dirty_at IS NOT NULL
 AND LEAST(dirty_bumped_at+make_interval(secs=>debounce_seconds),dirty_at+make_interval(secs=>max_delay_seconds))<=now()
 AND (retry_after IS NULL OR retry_after<=now())
 AND (refresh_claimed_at IS NULL OR refresh_claimed_at<now()-interval '2 minutes')
 AND NOT EXISTS (SELECT 1 FROM fair f WHERE f.model=r.model AND f.scope_key=r.scope_key)
 ORDER BY (r.computed_at IS NULL) DESC,(r.failed_attempts>0),r.dirty_at,r.model,r.scope_key
 LIMIT (4-(SELECT count(*) FROM fair)) FOR UPDATE OF r SKIP LOCKED),
 due AS (SELECT * FROM fair UNION ALL SELECT * FROM priority),
 claimed AS (UPDATE read_models r SET refresh_claimed_at=clock_timestamp(),claim_token=gen_random_uuid()
 FROM due WHERE r.model=due.model AND r.scope_key=due.scope_key
 RETURNING r.model,r.scope_key,r.version,r.account_id,r.graph_ids,r.compute_as,r.params,r.dirty_generation,r.claim_token,r.control_revision,r.control_freshness,
 r.immediate_due_at,r.refresh_claimed_at claimed_at,(r.computed_at IS NULL) first_fill,
 CASE WHEN r.immediate_due_at IS NOT NULL THEN GREATEST(0,EXTRACT(epoch FROM (r.refresh_claimed_at-r.immediate_due_at))*1000)::float8 END due_to_claim_ms,
 (r.failed_attempts>0) was_failed,r.dirty_at dispatch_dirty_at)
 SELECT * FROM claimed ORDER BY first_fill DESC,was_failed,dispatch_dirty_at,model,scope_key"#)
 .bind(names).bind(versions).fetch_all(&mut *tx).await?;
    tx.commit().await?;
    for c in &claims {
        tracing::info!(model=%c.model,version=c.version,scope_key=%scope_correlation(&c.scope_key),
            generation=c.dirty_generation,revision=c.control_revision,first_fill=c.first_fill,
            immediate_due_at=?c.immediate_due_at,claimed_at=%c.claimed_at,
            due_to_claim_ms=c.due_to_claim_ms,"read-model refresh claimed");
    }
    Ok(claims)
}
/// Caller supplies only internally claimed identity. No system actor or admin
/// GUC, no writable control pool in ComputeCx. Snapshot and content use the
/// SAME repeatable-read transaction (READ COMMITTED snapshot strings are unsafe).
pub async fn begin_compute(
    pool: &PgPool,
    c: &Claim,
    timeout: Duration,
) -> Result<Transaction<'static, Postgres>, Error> {
    let mut tx = pool
        .begin_with("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
        .await?;
    sqlx::query("SELECT set_config('app.account_id',$1,true),set_config('app.user_id',$2,true),set_config('app.tenant_actor','',true),set_config('app.is_org_admin','',true),set_config('statement_timeout',$3,true),set_config('jit','off',true)")
  .bind(c.account_id.to_string()).bind(c.compute_as.to_string()).bind(timeout.as_millis().to_string()).execute(&mut *tx).await?;
    Ok(tx)
}
pub async fn publish(
    pool: &PgPool,
    c: &Claim,
    payload: Value,
    snapshot: &str,
) -> Result<bool, Error> {
    let mut tx = crate::tenancy::scope::begin_tenant_system_scoped_without_account(pool).await?;
    let result=sqlx::query(r#"UPDATE read_models SET payload=$4,computed_at=clock_timestamp(),computed_snapshot=$5::text::pg_snapshot,computed_control_freshness=$8,
 dirty_at=CASE WHEN dirty_generation=$6 AND (invalidated_xid IS NULL OR pg_visible_in_snapshot(invalidated_xid,$5::text::pg_snapshot)) AND read_model_fence_state(account_id,CASE WHEN $9 THEN ARRAY['00000000-0000-0000-0000-000000000000'::uuid] ELSE graph_ids END,model,viewer_user_id,$5::text::pg_snapshot)=2 THEN NULL ELSE dirty_at END,
 dirty_bumped_at=CASE WHEN dirty_generation=$6 AND (invalidated_xid IS NULL OR pg_visible_in_snapshot(invalidated_xid,$5::text::pg_snapshot)) AND read_model_fence_state(account_id,CASE WHEN $9 THEN ARRAY['00000000-0000-0000-0000-000000000000'::uuid] ELSE graph_ids END,model,viewer_user_id,$5::text::pg_snapshot)=2 THEN NULL ELSE dirty_bumped_at END,
 immediate_due_at=CASE WHEN dirty_generation=$6 AND (invalidated_xid IS NULL OR pg_visible_in_snapshot(invalidated_xid,$5::text::pg_snapshot)) AND read_model_fence_state(account_id,CASE WHEN $9 THEN ARRAY['00000000-0000-0000-0000-000000000000'::uuid] ELSE graph_ids END,model,viewer_user_id,$5::text::pg_snapshot)=2 THEN NULL ELSE immediate_due_at END,
 refresh_claimed_at=NULL,claim_token=NULL,last_error=NULL,failed_attempts=0,retry_after=NULL
 WHERE model=$1 AND scope_key=$2 AND version=$3 AND claim_token=$7"#)
 .bind(&c.model).bind(&c.scope_key).bind(c.version).bind(payload).bind(snapshot).bind(c.dirty_generation).bind(c.claim_token).bind(c.control_freshness).bind(registry::by_name(&c.model).is_some_and(|m|m.account_facts())).execute(&mut *tx).await?;
    tx.commit().await?;
    Ok(result.rows_affected() == 1)
}
/// Back a failed compute's scope off. Returns its consecutive failure count, or
/// `None` when a newer worker holds the claim.
pub async fn fail(
    pool: &PgPool,
    c: &Claim,
    kind: failure::FailureKind,
) -> Result<Option<i32>, Error> {
    let mut tx = crate::tenancy::scope::begin_tenant_system_scoped_without_account(pool).await?;
    // Persist only the failure kind (a code literal). Never DB errors, SQL or
    // parameters: they can contain private content.
    let attempts=sqlx::query_scalar("UPDATE read_models SET refresh_claimed_at=NULL,claim_token=NULL,last_error=$5,failed_attempts=LEAST(failed_attempts+1,16),retry_after=now()+make_interval(secs=>LEAST(3600,30*power(2,LEAST(failed_attempts,7)))::int) WHERE model=$1 AND scope_key=$2 AND version=$3 AND claim_token=$4 RETURNING failed_attempts")
 .bind(&c.model).bind(&c.scope_key).bind(c.version).bind(c.claim_token).bind(kind.as_str()).fetch_optional(&mut *tx).await?;
    tx.commit().await?;
    Ok(attempts)
}
/// Retire a permanently superseded address, only after the worker certified a
/// strictly newer control revision. An enqueue or replacement claim wins over
/// this compute's old observation; a lost retirement leaves lease recovery intact.
pub async fn retire(pool: &PgPool, c: &Claim) -> Result<bool, Error> {
    let mut tx = crate::tenancy::scope::begin_tenant_system_scoped_without_account(pool).await?;
    let result = sqlx::query("DELETE FROM read_models WHERE model=$1 AND scope_key=$2 AND version=$3 AND claim_token=$4 AND dirty_generation=$5 AND control_revision=$6")
        .bind(&c.model).bind(&c.scope_key).bind(c.version).bind(c.claim_token)
        .bind(c.dirty_generation).bind(c.control_revision).execute(&mut *tx).await?;
    tx.commit().await?;
    Ok(result.rows_affected() == 1)
}
/// Dirty but not yet due is known work, so don't let the harness idle-backoff
/// past its deadline. Empty tenants retain the harness's dormant scheduling.
pub async fn next_due(pool: &PgPool) -> Result<Option<f64>, Error> {
    let mut tx = crate::tenancy::scope::begin_tenant_system_scoped_without_account(pool).await?;
    // The scheduler must consider exactly the same names/versions as claim;
    // retained rows from other rolling binaries must not prevent idle backoff.
    let (names, versions) = registered_versions();
    let due=sqlx::query_scalar("SELECT EXTRACT(epoch FROM min(GREATEST(COALESCE(r.retry_after,'-infinity'),COALESCE(r.refresh_claimed_at+interval '2 minutes','-infinity'),LEAST(r.dirty_bumped_at+make_interval(secs=>r.debounce_seconds),r.dirty_at+make_interval(secs=>r.max_delay_seconds))))-now())::float8 FROM read_models r JOIN unnest($1::text[],$2::int[]) m(name,version) ON m.name=r.model AND m.version=r.version WHERE r.dirty_at IS NOT NULL")
 .bind(names).bind(versions).fetch_one(&mut *tx).await?;
    tx.rollback().await?;
    Ok(due)
}

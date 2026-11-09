//! Regression seams: real claim SQL, worker settlement, control triggers and RLS.
use super::*;

#[tokio::test]
async fn obsolete_revision_retires_on_worker_tick_and_current_scope_survives() {
    use rm::connectors::ConnectorsList as C;
    for org in [false, true] {
        let f = Fixture::new(org).await;
        let old = f.domain_scope::<C>(Default::default()).await;
        rm::store::enqueue::<C>(&f.app, &old).await.unwrap();
        // Change a real authority input, without enqueueing the replacement:
        // enqueue's same-tuple pruning must not hide the worker regression.
        let mut control = begin_tenant_scoped(&f.control, f.account, f.user)
            .await
            .unwrap();
        sqlx::query("UPDATE account_members SET role='admin' WHERE account_id=$1 AND user_id=$2")
            .bind(f.account)
            .bind(f.user)
            .execute(&mut *control)
            .await
            .unwrap();
        control.commit().await.unwrap();
        let current = f.domain_scope::<C>(Default::default()).await;
        assert_ne!(old.key(), current.key());
        let tick = read_model_refresh::tick(&f.worker(), &f.control)
            .await
            .unwrap();
        assert_eq!(tick.processed, 1);
        let retained: i64 =
            sqlx::query_scalar("SELECT count(*) FROM read_models WHERE scope_key=$1")
                .bind(old.key())
                .fetch_one(&f.owner)
                .await
                .unwrap();
        assert_eq!(
            retained, 0,
            "obsolete control revision must leave the queue, org={org}"
        );
        rm::store::enqueue::<C>(&f.app, &current).await.unwrap();
        read_model_refresh::tick(&f.worker(), &f.control)
            .await
            .unwrap();
        let mut tx = begin_tenant_scoped(&f.app, f.account, f.user)
            .await
            .unwrap();
        assert!(rm::store::read::<C>(&mut tx, &current)
            .await
            .unwrap()
            .computed_at
            .is_some());
        tx.rollback().await.unwrap();
        f.close().await;
    }
}

#[tokio::test]
async fn cold_queue_priority_preserves_fairness_backoff_and_skip_locked() {
    for org in [false, true] {
        let f = Fixture::new(org).await;
        // A backlog much older than the 60-second backdate of a page miss.
        sqlx::query("INSERT INTO read_models(model,scope_key,version,account_id,graph_ids,compute_as,params,payload,computed_at,computed_snapshot,dirty_at,dirty_bumped_at) SELECT 'workspace_counts','stored-'||n,1,$1,$2,$3,'{}','{}',now(),pg_current_snapshot(),now()-interval '21 hours'+n*interval '1 second',now()-interval '21 hours' FROM generate_series(0,11) n")
            .bind(f.account).bind(vec![f.graph]).bind(f.user).execute(&f.owner).await.unwrap();
        sqlx::query("INSERT INTO read_models(model,scope_key,version,account_id,graph_ids,compute_as,failed_attempts,retry_after,dirty_at,dirty_bumped_at) SELECT 'workspace_counts',key,1,$1,$2,$3,attempts,retry,now()-age,now()-age FROM (VALUES ('empty-a',0,NULL::timestamptz,interval '61 seconds'),('empty-b',0,NULL::timestamptz,interval '62 seconds'),('empty-retry',1,now()-interval '1 second',interval '18 hours'),('backoff',0,now()+interval '1 hour',interval '19 hours'),('locked',0,NULL::timestamptz,interval '20 hours')) v(key,attempts,retry,age)")
            .bind(f.account).bind(vec![f.graph]).bind(f.user).execute(&f.owner).await.unwrap();
        f.enqueue().await;
        let key = f.scope().await.key().to_owned();
        let mut lock = f.owner.begin().await.unwrap();
        sqlx::query("SELECT 1 FROM read_models WHERE scope_key='locked' FOR UPDATE")
            .execute(&mut *lock)
            .await
            .unwrap();
        let claimed = rm::store::claim(&f.app).await.unwrap();
        let keys: std::collections::BTreeSet<_> =
            claimed.iter().map(|c| c.scope_key.as_str()).collect();
        assert_eq!(
            keys,
            ["stored-0", "empty-a", "empty-b", &key].into(),
            "empty pages get three slots; oldest stored answer still progresses"
        );
        let next = rm::store::claim(&f.app).await.unwrap();
        assert_eq!(next.len(), 4);
        assert!(next.iter().all(|c| !keys.contains(c.scope_key.as_str())
            && c.scope_key != "backoff"
            && c.scope_key != "locked"));
        assert!(
            next.iter().any(|c| c.scope_key == "empty-retry"),
            "eligible retries still progress after first fills"
        );
        lock.rollback().await.unwrap();
        sqlx::query("DELETE FROM read_models")
            .execute(&f.owner)
            .await
            .unwrap();
        // The fairness slot also protects an old eligible failed first fill
        // from an ongoing stream of clean first fills.
        sqlx::query("INSERT INTO read_models(model,scope_key,version,account_id,graph_ids,compute_as,failed_attempts,dirty_at,dirty_bumped_at) SELECT 'workspace_counts','fair-'||n,1,$1,$2,$3,CASE WHEN n=0 THEN 1 ELSE 0 END,now()-interval '1 minute'-CASE WHEN n=0 THEN interval '1 day' ELSE interval '0' END,now()-interval '1 minute' FROM generate_series(0,8) n")
            .bind(f.account).bind(vec![f.graph]).bind(f.user).execute(&f.owner).await.unwrap();
        let fair = rm::store::claim(&f.app).await.unwrap();
        assert_eq!(fair.len(), 4);
        assert!(
            fair.iter().any(|c| c.scope_key == "fair-0"),
            "eligible failed empty rows must not starve"
        );
        f.close().await;
    }
}

#[tokio::test]
async fn control_bump_attribution_is_aggregated_private_and_expires() {
    let f = Fixture::new(false).await;
    sqlx::query("INSERT INTO oauth_tokens(account_id,provider,scope,owner_user_id,access_token) VALUES($1,'google_gmail','gmail.readonly',$2,decode('00','hex'))")
        .bind(f.account).bind(f.user).execute(&f.owner).await.unwrap();
    sqlx::query("DELETE FROM read_model_control_bumps")
        .execute(&f.owner)
        .await
        .unwrap();
    let before: i64 =
        sqlx::query_scalar("SELECT revision FROM read_model_control_revisions WHERE account_id=$1")
            .bind(f.account)
            .fetch_one(&f.owner)
            .await
            .unwrap();
    for _ in 0..2 {
        sqlx::query("UPDATE account_members SET role=CASE role WHEN 'owner' THEN 'admin' ELSE 'owner' END WHERE account_id=$1 AND user_id=$2")
            .bind(f.account).bind(f.user).execute(&f.owner).await.unwrap();
    }
    let counter: (i64, i64, bool) = sqlx::query_as("SELECT count(*),sum(bumps)::bigint,bool_and(first_at<=last_at AND bucket_start=date_trunc('hour',last_at)) FROM read_model_control_bumps WHERE account_id=$1 AND table_name='account_members' AND operation='UPDATE'")
        .bind(f.account).fetch_one(&f.owner).await.unwrap();
    assert_eq!(counter, (1, 2, true));
    sqlx::query("UPDATE account_members SET role=role WHERE account_id=$1 AND user_id=$2")
        .bind(f.account)
        .bind(f.user)
        .execute(&f.owner)
        .await
        .unwrap();
    let after: i64 =
        sqlx::query_scalar("SELECT revision FROM read_model_control_revisions WHERE account_id=$1")
            .bind(f.account)
            .fetch_one(&f.owner)
            .await
            .unwrap();
    assert_eq!(
        after,
        before + 2,
        "instrumentation never changes authority decisions"
    );
    let count: i64 = sqlx::query_scalar("SELECT sum(bumps)::bigint FROM read_model_control_bumps")
        .fetch_one(&f.owner)
        .await
        .unwrap();
    assert_eq!(count, 2, "no-op statements must not count");
    let mut tx = begin_tenant_scoped(&f.app, f.account, f.user)
        .await
        .unwrap();
    assert!(
        sqlx::query("SELECT * FROM read_model_control_bumps")
            .fetch_all(&mut *tx)
            .await
            .is_err(),
        "app cannot enumerate attribution"
    );
    tx.rollback().await.unwrap();
    // A rolled-back authority write contributes no bump.
    let mut rolled_back = f.owner.begin().await.unwrap();
    sqlx::query("UPDATE account_members SET role='member' WHERE account_id=$1 AND user_id=$2")
        .bind(f.account)
        .bind(f.user)
        .execute(&mut *rolled_back)
        .await
        .unwrap();
    rolled_back.rollback().await.unwrap();
    // Ordinary token refresh changes advance freshness only.
    sqlx::query("UPDATE oauth_tokens SET access_token=decode('01','hex') WHERE account_id=$1")
        .bind(f.account)
        .execute(&f.owner)
        .await
        .unwrap();
    let count: i64 = sqlx::query_scalar("SELECT sum(bumps)::bigint FROM read_model_control_bumps")
        .fetch_one(&f.owner)
        .await
        .unwrap();
    assert_eq!(
        count, 2,
        "rollback and operational freshness must not count as authority"
    );
    // Reusing yesterday's slot resets its count instead of growing a log.
    sqlx::query("UPDATE read_model_control_bumps SET bucket_start=bucket_start-interval '24 hours',bumps=900,first_at=first_at-interval '24 hours',last_at=last_at-interval '24 hours'")
        .execute(&f.owner).await.unwrap();
    sqlx::query("INSERT INTO read_model_control_bumps(account_id,table_name,operation,slot,bucket_start,bumps,first_at,last_at) SELECT $1,'expired-'||n,'UPDATE',0,now()-interval '26 hours',1,now()-interval '26 hours',now()-interval '26 hours' FROM generate_series(1,65) n")
        .bind(f.account).execute(&f.owner).await.unwrap();
    sqlx::query("UPDATE account_members SET role='admin' WHERE account_id=$1 AND user_id=$2")
        .bind(f.account)
        .bind(f.user)
        .execute(&f.owner)
        .await
        .unwrap();
    let expired: i64 = sqlx::query_scalar("SELECT count(*) FROM read_model_control_bumps WHERE bucket_start<now()-interval '24 hours'")
        .fetch_one(&f.owner).await.unwrap();
    assert_eq!(
        expired, 1,
        "cleanup is capped at 64, not an unbounded trigger scan"
    );
    let reset: i64 = sqlx::query_scalar(
        "SELECT bumps FROM read_model_control_bumps WHERE table_name='account_members'",
    )
    .fetch_one(&f.owner)
    .await
    .unwrap();
    assert_eq!(reset, 1);
    sqlx::query("UPDATE account_members SET role='owner' WHERE account_id=$1 AND user_id=$2")
        .bind(f.account)
        .bind(f.user)
        .execute(&f.owner)
        .await
        .unwrap();
    let retained: i64 = sqlx::query_scalar("SELECT count(*) FROM read_model_control_bumps")
        .fetch_one(&f.owner)
        .await
        .unwrap();
    assert_eq!(
        retained, 1,
        "remaining expired bucket is cleaned on the next authority write"
    );
    f.close().await;
}

#[tokio::test]
async fn retirement_requires_claim_version_revision_and_generation_ownership() {
    use rm::connectors::ConnectorsList as C;
    for org in [false, true] {
        let f = Fixture::new(org).await;
        let old = f.domain_scope::<C>(Default::default()).await;
        rm::store::enqueue::<C>(&f.app, &old).await.unwrap();
        let mut claim = f.claim().await;
        let token = claim.claim_token;
        claim.claim_token = Uuid::new_v4();
        assert!(!rm::store::retire(&f.app, &claim).await.unwrap());
        claim.claim_token = token;
        claim.version += 1;
        assert!(!rm::store::retire(&f.app, &claim).await.unwrap());
        claim.version -= 1;
        claim.control_revision += 1;
        assert!(!rm::store::retire(&f.app, &claim).await.unwrap());
        claim.control_revision -= 1;
        // A request enqueues after the worker's snapshot but keeps its token.
        rm::store::enqueue::<C>(&f.app, &old).await.unwrap();
        assert!(
            !rm::store::retire(&f.app, &claim).await.unwrap(),
            "generation protects intervening work"
        );
        sqlx::query("UPDATE read_models SET refresh_claimed_at=now()-interval '3 minutes' WHERE scope_key=$1")
            .bind(old.key()).execute(&f.owner).await.unwrap();
        let newer = f.claim().await;
        assert!(
            !rm::store::retire(&f.app, &claim).await.unwrap(),
            "recovered claim cannot delete replacement"
        );
        assert!(rm::store::retire(&f.app, &newer).await.unwrap());
        assert!(!rm::store::retire(&f.app, &newer).await.unwrap());
        f.close().await;
    }
}

#[tokio::test]
async fn reversible_graph_refusal_backs_off_and_recovers() {
    for org in [false, true] {
        let f = Fixture::new(org).await;
        f.enqueue().await;
        sqlx::query("UPDATE graphs SET archived_at=now() WHERE id=$1")
            .bind(f.graph)
            .execute(&f.owner)
            .await
            .unwrap();
        read_model_refresh::tick(&f.worker(), &f.control)
            .await
            .unwrap();
        let state: (i32, String, bool) =
            sqlx::query_as("SELECT failed_attempts,last_error,retry_after>now() FROM read_models")
                .fetch_one(&f.owner)
                .await
                .unwrap();
        assert_eq!(state, (1, "scope_unaddressable".into(), true));
        sqlx::query("UPDATE graphs SET archived_at=NULL WHERE id=$1")
            .bind(f.graph)
            .execute(&f.owner)
            .await
            .unwrap();
        f.enqueue().await;
        read_model_refresh::tick(&f.worker(), &f.control)
            .await
            .unwrap();
        assert!(
            f.answer().await.computed_at.is_some(),
            "temporary refusal must recover, org={org}"
        );
        f.close().await;
    }
}

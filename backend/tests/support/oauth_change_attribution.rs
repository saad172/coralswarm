//! Test actual OAuth writer shapes without changing authority semantics.
use super::*;
use coralswarm_backend::{mark_credential_invalid, CredentialDeathReason, TokenOwner};

#[tokio::test]
async fn oauth_changed_columns_preserve_latest_signal_authority_and_unknowns() {
    let f = Fixture::new(false).await;
    sqlx::query("INSERT INTO oauth_tokens(account_id,provider,scope,owner_user_id,access_token) VALUES($1,'google_gmail','gmail.readonly',$2,decode('00','hex'))")
        .bind(f.account).bind(f.user).execute(&f.owner).await.unwrap();
    async fn counters(f: &Fixture) -> (i64, i64) {
        sqlx::query_as(
            "SELECT revision,freshness FROM read_model_control_revisions WHERE account_id=$1",
        )
        .bind(f.account)
        .fetch_one(&f.owner)
        .await
        .unwrap()
    }
    let base = counters(&f).await;
    sqlx::query(
        "UPDATE oauth_tokens SET scope=scope,updated_at=clock_timestamp() WHERE account_id=$1",
    )
    .bind(f.account)
    .execute(&f.owner)
    .await
    .unwrap();
    assert_eq!(counters(&f).await, base);
    sqlx::query("UPDATE oauth_tokens SET access_token=decode('01','hex'),expires_at=now()+interval '1 hour' WHERE account_id=$1")
        .bind(f.account).execute(&f.owner).await.unwrap();
    assert_eq!(counters(&f).await, (base.0, base.1 + 1));
    // Exercise the real owner-exact writer, retaining the latest-signal contract.
    let mut observations = Vec::new();
    for _ in 0..2 {
        assert_eq!(
            mark_credential_invalid(
                &f.app,
                f.account,
                TokenOwner::Member(f.user),
                "google_gmail",
                CredentialDeathReason::GoogleInvalidGrant,
                None
            )
            .await
            .unwrap(),
            1
        );
        observations.push(
            sqlx::query_scalar::<_, chrono::DateTime<chrono::Utc>>(
                "SELECT credential_invalid_at FROM oauth_tokens WHERE account_id=$1",
            )
            .bind(f.account)
            .fetch_one(&f.owner)
            .await
            .unwrap(),
        );
    }
    assert!(
        observations[1] > observations[0],
        "latest definite signal still advances"
    );
    let invalid = counters(&f).await;
    assert_eq!(
        invalid.0,
        base.0 + 2,
        "observed writer remains conservative until production attribution proves it"
    );
    let last: chrono::DateTime<chrono::Utc> =
        sqlx::query_scalar("SELECT credential_invalid_at FROM oauth_tokens WHERE account_id=$1")
            .bind(f.account)
            .fetch_one(&f.owner)
            .await
            .unwrap();
    assert!(
        last <= chrono::Utc::now(),
        "latest observation contract retained"
    );
    let observed:i64=sqlx::query_scalar("SELECT statements FROM read_model_oauth_changes WHERE account_id=$1 AND column_name='credential_invalid_at' AND semantic_class='credential_observation'")
        .bind(f.account).fetch_one(&f.owner).await.unwrap();
    assert_eq!(observed, 1);
    // Scope, provider identity, owner, invalid -> live, and future schema fields
    // remain authority changes. A reason-only observation also stays authority.
    for set in [
        "credential_invalid_reason='another_definite_signal'",
        "scope='gmail.modify'",
        "provider_user_id='synthetic-id'",
        "provider_workspace_id='synthetic-workspace'",
        "owner_user_id=NULL",
        "provider='google'",
        "credential_invalid_at=NULL,credential_invalid_reason=NULL",
    ] {
        let before = counters(&f).await;
        sqlx::query(&format!(
            "UPDATE oauth_tokens SET {set} WHERE account_id=$1"
        ))
        .bind(f.account)
        .execute(&f.owner)
        .await
        .unwrap();
        assert_eq!(counters(&f).await, (before.0 + 1, before.1 + 1), "{set}");
    }
    sqlx::query("ALTER TABLE oauth_tokens ADD COLUMN future_authority_test text")
        .execute(&f.owner)
        .await
        .unwrap();
    let before = counters(&f).await;
    sqlx::query("UPDATE oauth_tokens SET future_authority_test='synthetic' WHERE account_id=$1")
        .bind(f.account)
        .execute(&f.owner)
        .await
        .unwrap();
    assert_eq!(counters(&f).await, (before.0 + 1, before.1 + 1));
    let unknown:i64=sqlx::query_scalar("SELECT statements FROM read_model_oauth_changes WHERE account_id=$1 AND column_name='_unknown' AND semantic_class='unknown'").bind(f.account).fetch_one(&f.owner).await.unwrap();
    assert_eq!(unknown, 1);
    let mut rollback = f.owner.begin().await.unwrap();
    sqlx::query("UPDATE oauth_tokens SET scope='rolled-back' WHERE account_id=$1")
        .bind(f.account)
        .execute(&mut *rollback)
        .await
        .unwrap();
    rollback.rollback().await.unwrap();
    assert_eq!(counters(&f).await, (before.0 + 1, before.1 + 1));
    // No row values or dynamically introduced names can appear in metadata.
    let names:Vec<String>=sqlx::query_scalar("SELECT column_name FROM information_schema.columns WHERE table_name='read_model_oauth_changes' ORDER BY ordinal_position").fetch_all(&f.owner).await.unwrap();
    assert_eq!(
        names,
        vec![
            "account_id",
            "column_name",
            "semantic_class",
            "slot",
            "bucket_start",
            "statements",
            "first_at",
            "last_at"
        ]
    );
    let private:(bool,i64,bool)=sqlx::query_as("SELECT relrowsecurity,(SELECT count(*) FROM pg_policies WHERE tablename='read_model_oauth_changes'),NOT has_table_privilege('coralswarm_app','read_model_oauth_changes','SELECT,INSERT,UPDATE,DELETE') FROM pg_class WHERE oid='read_model_oauth_changes'::regclass").fetch_one(&f.owner).await.unwrap();
    assert_eq!(private, (true, 0, true));
    let mut tx = begin_tenant_scoped(&f.app, f.account, f.user)
        .await
        .unwrap();
    assert!(sqlx::query("SELECT * FROM read_model_oauth_changes")
        .execute(&mut *tx)
        .await
        .is_err());
    tx.rollback().await.unwrap();
    // A reused UTC slot resets; cleanup is bounded to 64 rows per statement.
    sqlx::query("UPDATE read_model_oauth_changes SET bucket_start=bucket_start-interval '24 hours',statements=900,first_at=first_at-interval '24 hours',last_at=last_at-interval '24 hours'").execute(&f.owner).await.unwrap();
    sqlx::query("UPDATE oauth_tokens SET scope='after-expiry' WHERE account_id=$1")
        .bind(f.account)
        .execute(&f.owner)
        .await
        .unwrap();
    let retained: (i64, i64) =
        sqlx::query_as("SELECT count(*),sum(statements)::bigint FROM read_model_oauth_changes")
            .fetch_one(&f.owner)
            .await
            .unwrap();
    assert_eq!(retained, (1, 1));

    let function:(bool,bool)=sqlx::query_as("SELECT NOT has_function_privilege('coralswarm_app','read_model_oauth_change_attribution()','EXECUTE'),NOT has_function_privilege('coralswarm_tenant_app','read_model_oauth_change_attribution()','EXECUTE')").fetch_one(&f.owner).await.unwrap();
    assert_eq!(function, (true, true));
    sqlx::query("INSERT INTO read_model_oauth_changes(account_id,column_name,semantic_class,slot,bucket_start,statements,first_at,last_at) SELECT $1,c,s,23,now()-interval '26 hours',1,now()-interval '26 hours',now()-interval '26 hours' FROM unnest(ARRAY['id','account_id','provider','scope','access_token','refresh_token','token_type','expires_at','provider_user_id','provider_workspace_id','created_at','updated_at','user_token_encrypted','owner_user_id','dek_migrated_at','credential_invalid_at','credential_invalid_reason','_unknown','_noop']) c CROSS JOIN unnest(ARRAY['authority','operational','credential_observation','unknown','noop']) s LIMIT 65").bind(f.account).execute(&f.owner).await.unwrap();
    sqlx::query("UPDATE oauth_tokens SET scope=scope WHERE account_id=$1")
        .bind(f.account)
        .execute(&f.owner)
        .await
        .unwrap();
    let expired:i64=sqlx::query_scalar("SELECT count(*) FROM read_model_oauth_changes WHERE bucket_start<now()-interval '24 hours'").fetch_one(&f.owner).await.unwrap();
    assert_eq!(
        expired, 1,
        "one indexed cleanup removes at most 64 expired dimensions"
    );
    sqlx::query("UPDATE oauth_tokens SET scope=scope WHERE account_id=$1")
        .bind(f.account)
        .execute(&f.owner)
        .await
        .unwrap();
    let expired:i64=sqlx::query_scalar("SELECT count(*) FROM read_model_oauth_changes WHERE bucket_start<now()-interval '24 hours'").fetch_one(&f.owner).await.unwrap();
    assert_eq!(expired, 0);

    sqlx::query("INSERT INTO oauth_tokens(account_id,provider,scope,owner_user_id,access_token) VALUES($1,'google_chat','scope',$2,decode('00','hex'))").bind(f.account).bind(f.user).execute(&f.owner).await.unwrap();
    let before:i64=sqlx::query_scalar("SELECT sum(statements)::bigint FROM read_model_oauth_changes WHERE account_id=$1 AND column_name='scope'").bind(f.account).fetch_one(&f.owner).await.unwrap();
    let rows =
        sqlx::query("UPDATE oauth_tokens SET scope=scope||' synthetic_bulk' WHERE account_id=$1")
            .bind(f.account)
            .execute(&f.owner)
            .await
            .unwrap()
            .rows_affected();
    assert_eq!(rows, 2);
    let after:i64=sqlx::query_scalar("SELECT sum(statements)::bigint FROM read_model_oauth_changes WHERE account_id=$1 AND column_name='scope'").bind(f.account).fetch_one(&f.owner).await.unwrap();
    assert_eq!(
        after,
        before + 1,
        "bulk changes count statements, never token rows"
    );
    f.close().await;
}

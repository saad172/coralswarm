//! Cached add-on metadata follows the existing actor-bound personal-org gate.
use super::*;
use rm::connectors::ConnectorDetail as D;

fn params() -> std::collections::BTreeMap<String, String> {
    [("provider".into(), "google".into())].into()
}
fn tokens(payload: &Value) -> &Vec<Value> {
    payload["addin_tokens"]["tokens"].as_array().unwrap()
}
async fn changed(f: &Fixture, before: &rm::Scope<rm::connectors::DetailParams>, expected: usize) {
    let current = f.domain_scope::<D>(params()).await;
    assert_ne!(
        before.key(),
        current.key(),
        "mapping changes must fence cached metadata"
    );
    let mut tx = begin_tenant_scoped(&f.app, f.account, f.user)
        .await
        .unwrap();
    let pending = rm::store::read::<D>(&mut tx, &current).await.unwrap();
    assert!(
        pending.computed_at.is_none(),
        "old metadata must be unreachable before refresh"
    );
    tx.rollback().await.unwrap();
    assert_eq!(tokens(&f.domain::<D>(params()).await).len(), expected);
}

#[tokio::test]
async fn personal_org_metadata_and_mapping_authority() {
    let f = Fixture::new(false).await;
    let actor = format!("user_addon_{}", Uuid::new_v4());
    let other = format!("user_addon_other_{}", Uuid::new_v4());
    let org = format!("org_addon_{}", Uuid::new_v4());
    for (id, clerk) in [(f.user, &actor), (f.other, &other)] {
        sqlx::query("UPDATE users SET clerk_user_id=$2 WHERE id=$1")
            .bind(id)
            .bind(clerk)
            .execute(&f.owner)
            .await
            .unwrap();
    }
    let foreign: Uuid = sqlx::query_scalar("SELECT id FROM accounts WHERE owner_user_id=$1")
        .bind(f.other)
        .fetch_one(&f.owner)
        .await
        .unwrap();
    let own: Uuid = sqlx::query_scalar("INSERT INTO addin_tokens(account_id,owner_user_id,token_hash,name) VALUES($1,$2,'fixture-own-secret-hash','Own token') RETURNING id")
        .bind(f.account).bind(f.user).fetch_one(&f.owner).await.unwrap();
    for (account, user, revoked) in [
        (f.account, f.other, false),
        (foreign, f.user, false),
        (f.account, f.user, true),
    ] {
        sqlx::query("INSERT INTO addin_tokens(account_id,owner_user_id,token_hash,revoked_at) VALUES($1,$2,$3,CASE WHEN $4 THEN now() END)")
            .bind(account).bind(user).bind(Uuid::new_v4().to_string()).bind(revoked)
            .execute(&f.owner).await.unwrap();
    }
    let legacy = f.domain::<D>(params()).await;
    assert_eq!(tokens(&legacy).len(), 1);
    assert_eq!(tokens(&legacy)[0]["id"], own.to_string());
    sqlx::query("INSERT INTO personal_orgs(clerk_org_id,clerk_user_id) VALUES($1,$2)")
        .bind(&org)
        .bind(&actor)
        .execute(&f.owner)
        .await
        .unwrap();
    let mut adoption = begin_tenant_scoped(&f.owner, f.account, f.user)
        .await
        .unwrap();
    sqlx::query("SET CONSTRAINTS account_members_account_org_fk DEFERRED")
        .execute(&mut *adoption)
        .await
        .unwrap();
    sqlx::query("SELECT billing_enter_account($1)")
        .bind(f.account)
        .execute(&mut *adoption)
        .await
        .unwrap();
    sqlx::query("UPDATE accounts SET clerk_org_id=$2 WHERE id=$1")
        .bind(f.account)
        .bind(&org)
        .execute(&mut *adoption)
        .await
        .unwrap();
    sqlx::query("UPDATE account_members SET account_is_org_account=true,seat_tier_id=(SELECT id FROM seat_tiers WHERE code='standard' AND is_active=true) WHERE account_id=$1")
        .bind(f.account).execute(&mut *adoption).await.unwrap();
    adoption.commit().await.unwrap();
    let adopted = f.domain::<D>(params()).await;
    assert_eq!(
        tokens(&adopted).len(),
        1,
        "adopted personal org must retain own token metadata"
    );
    assert_eq!(tokens(&adopted)[0]["id"], own.to_string());
    assert_eq!(tokens(&adopted)[0].as_object().unwrap().len(), 4);
    assert!(!adopted.to_string().contains("fixture-own-secret-hash"));

    let before = f.domain_scope::<D>(params()).await;
    sqlx::query("UPDATE personal_orgs SET clerk_user_id=$2 WHERE clerk_org_id=$1")
        .bind(&org)
        .bind(&other)
        .execute(&f.owner)
        .await
        .unwrap();
    changed(&f, &before, 0).await; // Another actor's personal org is not ours.
    let before = f.domain_scope::<D>(params()).await;
    sqlx::query("UPDATE personal_orgs SET clerk_user_id=$2 WHERE clerk_org_id=$1")
        .bind(&org)
        .bind(&actor)
        .execute(&f.owner)
        .await
        .unwrap();
    changed(&f, &before, 1).await;
    let before = f.domain_scope::<D>(params()).await;
    sqlx::query("DELETE FROM personal_orgs WHERE clerk_org_id=$1")
        .bind(&org)
        .execute(&f.owner)
        .await
        .unwrap();
    changed(&f, &before, 0).await; // Unmapped real org remains ineligible.
    let before = f.domain_scope::<D>(params()).await;
    sqlx::query("INSERT INTO personal_orgs(clerk_org_id,clerk_user_id) VALUES($1,$2)")
        .bind(&org)
        .bind(&actor)
        .execute(&f.owner)
        .await
        .unwrap();
    changed(&f, &before, 1).await;
    f.close().await;
}

#[tokio::test]
async fn real_org_metadata_is_absent() {
    let f = Fixture::new(true).await;
    sqlx::query("INSERT INTO addin_tokens(account_id,owner_user_id,token_hash) VALUES($1,$2,$3)")
        .bind(f.account)
        .bind(f.user)
        .bind(Uuid::new_v4().to_string())
        .execute(&f.control)
        .await
        .unwrap();
    assert!(tokens(&f.domain::<D>(params()).await).is_empty());
    f.close().await;
}
